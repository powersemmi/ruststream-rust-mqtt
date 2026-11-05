//! [`MqttConfig`], the connection settings of a broker of the device variant.

use core::time::Duration;

use crate::endpoint::{EndpointError, split};
use crate::fixed::error::MqttError;

/// The keep-alive a broker asks for unless the configuration names another.
const DEFAULT_KEEP_ALIVE: Duration = Duration::from_secs(60);

/// The connection settings of an [`MqttBroker`](crate::MqttBroker): where it connects and as
/// whom.
///
/// Every text is a literal of the firmware, so the settings hold no buffer of their own. Building
/// them performs no I/O. The address is read when the broker connects, so a value the broker
/// cannot connect to is reported by `connect`.
///
/// # Examples
///
/// ```
/// use core::time::Duration;
///
/// use ruststream_rust_mqtt::MqttConfig;
///
/// /// The valve keeps its session for an hour, so commands sent while it reboots wait for it.
/// const VALVE: MqttConfig = MqttConfig::new("mqtt://192.168.1.10:1883")
///     .client_id("valve-1")
///     .credentials("valve-1", "s3cret")
///     .keep_alive(Duration::from_secs(30))
///     .persistent_session(Duration::from_secs(3600));
/// # let _ = VALVE;
/// ```
#[derive(Clone, Copy, PartialEq, Eq)]
#[must_use]
pub struct MqttConfig {
    address: &'static str,
    client_id: &'static str,
    keep_alive: Duration,
    credentials: Option<(&'static str, &'static str)>,
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
            .field("user", &self.credentials.map(|(user, _)| user))
            .field("clean_start", &self.clean_start)
            .field("session_expiry", &self.session_expiry)
            .finish()
    }
}

impl MqttConfig {
    /// Settings for the server at `address`: `mqtt://host:port`, `host:port`, or `host` for port
    /// 1883. The session starts clean, the server assigns the client identifier, and a ping goes
    /// out after 60 seconds without traffic.
    pub const fn new(address: &'static str) -> Self {
        Self {
            address,
            client_id: "",
            keep_alive: DEFAULT_KEEP_ALIVE,
            credentials: None,
            clean_start: true,
            session_expiry: Duration::ZERO,
        }
    }

    /// The client identifier the session opens with. A persistent session is found again by it.
    pub const fn client_id(mut self, client_id: &'static str) -> Self {
        self.client_id = client_id;
        self
    }

    /// How long the connection stays quiet before the broker sends a ping. Zero turns pings off.
    pub const fn keep_alive(mut self, keep_alive: Duration) -> Self {
        self.keep_alive = keep_alive;
        self
    }

    /// The user name and password the session opens with.
    pub const fn credentials(mut self, user: &'static str, password: &'static str) -> Self {
        self.credentials = Some((user, password));
        self
    }

    /// Resumes the session the server keeps for this client, and asks it to keep the session for
    /// `expiry` after the connection ends: the subscriptions and the `QoS` 1 and 2 messages that
    /// arrive meanwhile wait for the client.
    pub const fn persistent_session(mut self, expiry: Duration) -> Self {
        self.clean_start = false;
        self.session_expiry = expiry;
        self
    }

    /// The address the broker connects to.
    #[must_use]
    pub const fn address(&self) -> &'static str {
        self.address
    }

    pub(crate) const fn client_identifier(&self) -> &'static str {
        self.client_id
    }

    pub(crate) const fn keep_alive_interval(&self) -> Duration {
        self.keep_alive
    }

    pub(crate) const fn credentials_pair(&self) -> Option<(&'static str, &'static str)> {
        self.credentials
    }

    pub(crate) const fn starts_clean(&self) -> bool {
        self.clean_start
    }

    pub(crate) const fn session_expiry(&self) -> Duration {
        self.session_expiry
    }

    /// The host and the port the broker connects to.
    pub(crate) fn endpoint(&self) -> Result<(&'static str, u16), MqttError> {
        split(self.address).map_err(|error| MqttError::Connect {
            address: self.address,
            reason: match error {
                EndpointError::Scheme(_) => {
                    "the scheme is not one this broker connects with: write mqtt://host:port or \
                     host:port"
                }
                EndpointError::Port => "the port is not a number from 0 to 65535",
                EndpointError::NoHost => "the address names no host",
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_address_the_broker_cannot_reach_is_refused_before_io() {
        assert!(matches!(
            MqttConfig::new("mqtts://broker.local:8883").endpoint(),
            Err(MqttError::Connect { reason, .. }) if reason.contains("scheme")
        ));
        assert_eq!(
            MqttConfig::new("mqtt://broker.local").endpoint(),
            Ok(("broker.local", 1883))
        );
    }
}
