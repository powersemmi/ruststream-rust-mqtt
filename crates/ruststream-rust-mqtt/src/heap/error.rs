//! The crate-level error type.

use alloc::string::String;

/// An error of the MQTT broker.
///
/// One enum for the whole crate, variants by source. The client's own error types stay inside the
/// crate: a variant carries what the service needs to act on, as text where the client's error
/// borrows from its buffers.
///
/// # Examples
///
/// ```
/// use ruststream_rust_mqtt::MqttError;
///
/// /// A board retries what the network caused and reports what its own settings caused.
/// fn worth_retrying(error: &MqttError) -> bool {
///     match error {
///         MqttError::Connect { .. } | MqttError::Connection(_) | MqttError::NotConnected => true,
///         MqttError::Publish { .. } | MqttError::Subscribe { .. } | MqttError::Invalid(_) => false,
///         _ => false,
///     }
/// }
///
/// assert!(worth_retrying(&MqttError::Connection("reset by peer".into())));
/// assert!(!worth_retrying(&MqttError::Invalid("a share group with a slash".into())));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum MqttError {
    /// Establishing the connection failed: the network refused it, the server answered the
    /// CONNECT with an error, or the address does not parse.
    #[error("mqtt connection to {address} failed: {reason}")]
    Connect {
        /// The address the broker connects to, as configured.
        address: String,
        /// What went wrong.
        reason: String,
    },

    /// The server refused a subscription, or the connection ended before it answered.
    #[error("mqtt subscribe to {filter:?} failed: {reason}")]
    Subscribe {
        /// The topic filter the subscription asked for, as sent on the wire.
        filter: String,
        /// What went wrong.
        reason: String,
    },

    /// The connection ended while the service was running.
    #[error("mqtt connection lost: {0}")]
    Connection(String),

    /// A publish was refused before it reached the server: the topic is not one a publish can
    /// name, or a header is not text an MQTT string carries.
    #[error("mqtt publish to {topic:?} refused: {reason}")]
    Publish {
        /// The topic the message targeted.
        topic: String,
        /// Why it was refused.
        reason: String,
    },

    /// The broker is used before `connect`, after `shutdown`, or after its runner stopped.
    #[error("the mqtt broker is not connected")]
    NotConnected,

    /// A descriptor or a setting is invalid.
    #[error("invalid mqtt setting: {0}")]
    Invalid(String),
}
