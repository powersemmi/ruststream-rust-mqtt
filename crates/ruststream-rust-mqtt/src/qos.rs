//! [`Qos`], the delivery guarantee of a subscription or a publish.

use rust_mqtt::types::QoS;

/// The quality of service of a subscription or a publish.
///
/// # Examples
///
/// ```
/// use ruststream_rust_mqtt::{MqttTopic, Qos};
///
/// /// Commands to an actuator arrive once and only once.
/// fn commands() -> MqttTopic {
///     MqttTopic::new("actuators/valve-1/set").qos(Qos::ExactlyOnce)
/// }
///
/// assert_eq!(commands().topic(), "actuators/valve-1/set");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub enum Qos {
    /// Fire and forget: a delivery carries no acknowledgement, and settling one reports
    /// [`AckError::Unsupported`](ruststream::AckError::Unsupported).
    AtMostOnce,
    /// Acknowledged delivery: the handler's settlement sends the PUBACK. The default.
    #[default]
    AtLeastOnce,
    /// The exactly-once handshake: the handler's settlement sends the PUBREC, and the broker
    /// completes the handshake when the server releases the message.
    ExactlyOnce,
}

impl Qos {
    pub(crate) const fn to_client(self) -> QoS {
        match self {
            Self::AtMostOnce => QoS::AtMostOnce,
            Self::AtLeastOnce => QoS::AtLeastOnce,
            Self::ExactlyOnce => QoS::ExactlyOnce,
        }
    }

    /// The lesser of two qualities of service, the one a server delivers at.
    #[cfg(feature = "testing")]
    pub(crate) fn lesser(self, other: Self) -> Self {
        self.min(other)
    }
}
