//! The error of the device variant.

use ruststream::AckError;
use ruststream_core::Cause;

/// An error of the MQTT broker.
///
/// Each variant holds only what a device keeps without a heap: the address and the filters are
/// literals of the firmware, and a server's refusal is its reason code.
///
/// # Examples
///
/// ```
/// use ruststream::ConnectedBroker;
/// use ruststream_rust_mqtt::{ConnectedMqttBroker, MqttError};
///
/// /// Ends the session, and tells a lost connection apart from a clean end.
/// async fn power_down(connected: ConnectedMqttBroker) -> bool {
///     match connected.shutdown().await {
///         Ok(_) => true,
///         Err(MqttError::Connection(_) | MqttError::Disconnected { .. }) => false,
///         Err(_) => false,
///     }
/// }
/// # let _ = power_down;
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum MqttError {
    /// The connection could not be opened.
    #[error("mqtt connection to {address} failed: {reason}")]
    Connect {
        /// The address the broker connects to.
        address: &'static str,
        /// What failed.
        reason: &'static str,
    },
    /// The server refused the connection.
    #[error("the mqtt server at {address} refused the connection with reason code {code:#04x}")]
    Refused {
        /// The address the broker connects to.
        address: &'static str,
        /// The CONNACK reason code.
        code: u8,
    },
    /// The server refused a subscription.
    #[error("mqtt subscribe refused with reason code {code:#04x}")]
    Subscribe {
        /// The SUBACK reason code.
        code: u8,
    },
    /// The connection was lost.
    #[error("mqtt connection lost: {0}")]
    Connection(&'static str),
    /// The server ended the session with a DISCONNECT.
    #[error("the mqtt server ended the session with reason code {code:#04x}")]
    Disconnected {
        /// The DISCONNECT reason code.
        code: u8,
    },
    /// A publish was refused before it was sent.
    #[error("mqtt publish refused: {0}")]
    Publish(&'static str),
    /// A packet does not fit the broker's buffers or the largest packet the server takes.
    #[error("an mqtt packet of {size} bytes does not fit the {capacity} bytes a packet may hold")]
    PacketTooLarge {
        /// The size of the packet.
        size: usize,
        /// The largest packet the broker's buffers and the server take.
        capacity: usize,
    },
    /// The broker is not connected.
    #[error("the mqtt broker is not connected")]
    NotConnected,
    /// A setting the broker cannot use.
    #[error("invalid mqtt setting: {0}")]
    Invalid(&'static str),
    /// Every subscription the broker's memory holds is open.
    #[error("the mqtt broker holds {capacity} subscriptions and all of them are open")]
    NoFreeSubscription {
        /// How many subscriptions the broker's memory holds.
        capacity: usize,
    },
}

impl MqttError {
    /// The error as the cause a settlement reports, which holds no allocation.
    pub(crate) const fn cause(self) -> Cause {
        let (kind, code) = match self {
            Self::Connect { .. } => ("the mqtt connection failed", 1),
            Self::Refused { .. } => ("the mqtt server refused the connection", 2),
            Self::Subscribe { .. } => ("the mqtt server refused a subscription", 3),
            Self::Connection(_) => ("the mqtt connection was lost", 4),
            Self::Disconnected { .. } => ("the mqtt server ended the session", 5),
            Self::Publish(_) => ("an mqtt publish was refused", 6),
            Self::PacketTooLarge { .. } => ("an mqtt packet is too large", 7),
            Self::NotConnected => ("the mqtt broker is not connected", 8),
            Self::Invalid(_) => ("an mqtt setting is invalid", 9),
            Self::NoFreeSubscription { .. } => ("every mqtt subscription is open", 10),
        };
        Cause::new(kind, code)
    }
}

impl From<MqttError> for AckError {
    fn from(error: MqttError) -> Self {
        Self::Broker(error.cause())
    }
}
