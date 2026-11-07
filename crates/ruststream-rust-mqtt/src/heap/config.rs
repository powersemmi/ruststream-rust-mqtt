//! [`MqttConfig`], the connection settings of a broker.

use alloc::borrow::ToOwned;
use alloc::format;
use alloc::string::String;
use core::time::Duration;

use crate::endpoint::{EndpointError, split};
use crate::heap::error::MqttError;

/// The keep-alive a broker asks for unless the configuration names another.
const DEFAULT_KEEP_ALIVE: Duration = Duration::from_secs(60);

/// The connection settings of an [`MqttBroker`](crate::MqttBroker): where it connects and as
/// whom.
///
/// Building one performs no I/O. The address is read when the broker connects, so a value the
/// broker cannot connect to is reported by `connect`.
///
/// # Examples
///
/// ```
/// use core::time::Duration;
///
/// use ruststream_rust_mqtt::{MqttBroker, MqttConfig};
///
/// /// The board's broker: the server on the plant network, as this sensor.
/// fn broker() -> MqttBroker {
///     let config = MqttConfig::new("192.168.1.10:1883")
///         .client_id("sensor-7")
///         .keep_alive(Duration::from_secs(30))
///         .credentials("sensor-7", "secret");
///     let (broker, _runner) = MqttBroker::new(config);
///     broker
/// }
///
/// let _ = broker();
/// ```
#[derive(Clone, PartialEq, Eq)]
#[must_use]
pub struct MqttConfig {
    address: String,
    client_id: String,
    keep_alive: Duration,
    credentials: Option<(String, String)>,
    clean_start: bool,
    session_expiry: Duration,
}

impl core::fmt::Debug for MqttConfig {
    // The password never reaches a log line.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MqttConfig")
            .field("address", &self.address)
            .field("client_id", &self.client_id)
            .field("keep_alive", &self.keep_alive)
            .field("user", &self.credentials.as_ref().map(|(user, _)| user))
            .field("clean_start", &self.clean_start)
            .field("session_expiry", &self.session_expiry)
            .finish()
    }
}

impl MqttConfig {
    /// The server at `address`: `host:port`, or `mqtt://host[:port]` with the port defaulting
    /// to 1883.
    ///
    /// The client identifier starts empty, which asks the server to assign one; a session that
    /// outlives a connection needs one of its own.
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            client_id: String::new(),
            keep_alive: DEFAULT_KEEP_ALIVE,
            credentials: None,
            clean_start: true,
            session_expiry: Duration::ZERO,
        }
    }

    /// The client identifier the broker connects as.
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = client_id.into();
        self
    }

    /// The keep-alive interval, in whole seconds: the runner pings the server when nothing else
    /// went out for that long. Zero turns pings off. The default is 60 seconds.
    pub const fn keep_alive(mut self, keep_alive: Duration) -> Self {
        self.keep_alive = keep_alive;
        self
    }

    /// The user name and password the broker authenticates with.
    pub fn credentials(mut self, user: impl Into<String>, password: impl Into<String>) -> Self {
        self.credentials = Some((user.into(), password.into()));
        self
    }

    /// Resumes the session the server keeps for this client identifier instead of starting a
    /// clean one, and asks the server to keep it for `expiry` after the connection ends.
    ///
    /// The server then redelivers the `QoS` 1 and 2 messages a previous connection left
    /// unacknowledged.
    pub const fn persistent_session(mut self, expiry: Duration) -> Self {
        self.clean_start = false;
        self.session_expiry = expiry;
        self
    }

    /// The address as configured.
    #[must_use]
    pub fn address(&self) -> &str {
        &self.address
    }

    pub(crate) fn client_identifier(&self) -> &str {
        &self.client_id
    }

    pub(crate) const fn keep_alive_interval(&self) -> Duration {
        self.keep_alive
    }

    pub(crate) fn credentials_pair(&self) -> Option<(&str, &str)> {
        self.credentials
            .as_ref()
            .map(|(user, password)| (user.as_str(), password.as_str()))
    }

    pub(crate) const fn starts_clean(&self) -> bool {
        self.clean_start
    }

    pub(crate) const fn session_expiry(&self) -> Duration {
        self.session_expiry
    }

    /// The host and the port the address names.
    ///
    /// # Errors
    ///
    /// [`MqttError::Connect`] where the address carries a scheme other than `mqtt`, or a port
    /// that is not a number.
    pub(crate) fn endpoint(&self) -> Result<(&str, u16), MqttError> {
        split(&self.address).map_err(|error| MqttError::Connect {
            address: self.address.clone(),
            reason: match error {
                EndpointError::Scheme(scheme) => format!(
                    "the scheme {scheme:?} is not one this broker connects with: write \
                     mqtt://host:port or host:port"
                ),
                EndpointError::Port => "the port is not a number from 0 to 65535".to_owned(),
                EndpointError::NoHost => "the address names no host".to_owned(),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_address_names_a_host_and_a_port() {
        assert_eq!(
            MqttConfig::new("192.168.1.10:1884").endpoint(),
            Ok(("192.168.1.10", 1884))
        );
        assert_eq!(
            MqttConfig::new("mqtt://broker.local").endpoint(),
            Ok(("broker.local", 1883))
        );
        assert_eq!(
            MqttConfig::new("mqtt://[::1]:1885/").endpoint(),
            Ok(("::1", 1885))
        );
        assert_eq!(MqttConfig::new("[::1]").endpoint(), Ok(("::1", 1883)));
    }

    #[test]
    fn an_address_the_broker_cannot_reach_is_refused_with_the_reason() {
        let tls = MqttConfig::new("mqtts://broker.local:8883");
        assert!(
            matches!(tls.endpoint(), Err(MqttError::Connect { reason, .. }) if reason.contains("mqtts"))
        );
        assert!(MqttConfig::new("broker:port").endpoint().is_err());
        assert!(MqttConfig::new(":1883").endpoint().is_err());
    }

    #[test]
    fn the_password_stays_out_of_the_debug_form() {
        let config = MqttConfig::new("broker:1883").credentials("svc", "hunter2");
        assert!(!alloc::format!("{config:?}").contains("hunter2"));
    }
}
