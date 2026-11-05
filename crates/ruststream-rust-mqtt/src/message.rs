//! [`MqttMessage`], one delivery, and its settlement.

use alloc::boxed::Box;
#[cfg(feature = "testing")]
use alloc::string::String;
use core::fmt;

use rust_mqtt::types::{PacketIdentifier, TopicName};
#[cfg(feature = "testing")]
use ruststream::Bytes;
use ruststream::{AckError, HeaderMap, IncomingMessage, Partitioned, Settle};

use crate::error::MqttError;
#[cfg(feature = "testing")]
use crate::in_process::InFlight;
use crate::link::{Command, Link, Shared};
use crate::qos::Qos;

/// The header a delivery's partition key is read from: a publisher that wants per-key ordering
/// in keyed workers sets it as a user property.
pub const PARTITION_KEY_HEADER: &str = "partition-key";

/// The topic and the payload of a delivery, as its transport handed them over.
pub(crate) enum Body {
    /// Received from a server: the client's own buffers, moved in without a copy.
    Wire {
        topic: TopicName<'static>,
        payload: rust_mqtt::Bytes<'static>,
    },
    /// Delivered by the in-process transport.
    #[cfg(feature = "testing")]
    Local { topic: String, payload: Bytes },
}

/// Who a delivery's settlement goes to.
pub(crate) enum Acker {
    /// The runner, which sends the acknowledgement packet for this packet identifier.
    Wire {
        link: Shared<Link>,
        packet: PacketIdentifier,
    },
    /// The in-process transport, which only needs the session to be open.
    #[cfg(feature = "testing")]
    Local(Shared<crate::in_process::Bus>),
}

/// One delivery of an MQTT subscription.
///
/// The handler's settlement is the acknowledgement: `ack` sends the PUBACK of a `QoS` 1 delivery
/// and the PUBREC of a `QoS` 2 one (the broker completes the handshake when the server releases
/// it). A `QoS` 0 delivery carries no acknowledgement and answers
/// [`AckError::Unsupported`]. MQTT has no negative acknowledgement, so `nack(true)` answers
/// [`AckError::Unsupported`] too and leaves the message unacknowledged, for the server to
/// redeliver when a persistent session resumes; `nack(false)` acknowledges it, which is how a
/// delivery is dropped.
///
/// Headers are the MQTT 5 user properties; the content type, response topic and correlation
/// data arrive as the `content-type`, `reply-to` and `correlation-id` headers.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts};
/// use ruststream::{Deserialized, subscriber};
/// use ruststream_rust_mqtt::{MqttBroker, MqttTopic};
///
/// /// The raw bytes of a reading, as the sensor sends them.
/// #[derive(Deserialized)]
/// struct Reading<'a>(&'a [u8]);
///
/// /// Acknowledges a reading once it is stored: the PUBACK leaves with the outcome.
/// #[subscriber(MqttTopic::new("sensors/temp"))]
/// async fn store(reading: &Reading<'_>) -> HandlerOutcome {
///     let _ = reading.0;
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app(runtime = embassy)]
/// pub fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
///     EmbeddedApp::new().with_broker(broker, |b| b.include(store))
/// }
/// # }
/// # fn main() {}
/// ```
pub struct MqttMessage {
    body: Body,
    headers: HeaderMap,
    qos: Qos,
    acker: Option<Acker>,
    #[cfg(feature = "testing")]
    in_flight: Option<InFlight>,
}

impl fmt::Debug for MqttMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttMessage")
            .field("topic", &self.topic())
            .field("qos", &self.qos)
            .field("payload_len", &self.payload().len())
            .finish_non_exhaustive()
    }
}

impl MqttMessage {
    pub(crate) fn new(body: Body, headers: HeaderMap, qos: Qos, acker: Option<Acker>) -> Self {
        Self {
            body,
            headers,
            qos,
            acker,
            #[cfg(feature = "testing")]
            in_flight: None,
        }
    }

    #[cfg(feature = "testing")]
    pub(crate) fn counted(mut self, in_flight: Option<InFlight>) -> Self {
        self.in_flight = in_flight;
        self
    }

    /// The topic this delivery was published to, which a wildcard filter does not name.
    #[must_use]
    pub fn topic(&self) -> &str {
        match &self.body {
            Body::Wire { topic, .. } => topic.as_ref().as_str(),
            #[cfg(feature = "testing")]
            Body::Local { topic, .. } => topic,
        }
    }

    /// The quality of service the server delivered this message at: the lesser of the
    /// subscription's and the publisher's.
    #[must_use]
    pub const fn qos(&self) -> Qos {
        self.qos
    }

    /// Hands the settlement to the transport.
    async fn settle(self) -> Result<(), AckError> {
        let refused = || AckError::Broker(Box::new(MqttError::NotConnected));
        match &self.acker {
            None => Err(AckError::Unsupported),
            Some(Acker::Wire { link, packet }) => {
                if !link.is_open() {
                    return Err(refused());
                }
                // A full queue of commands means the runner is behind: the acknowledgement waits
                // its turn. The runner drains the queue when it stops, so this never waits on a
                // runner that has gone.
                link.commands
                    .send(Command::Ack {
                        packet: *packet,
                        qos: self.qos,
                    })
                    .await;
                Ok(())
            }
            #[cfg(feature = "testing")]
            Some(Acker::Local(bus)) => {
                if bus.is_open() {
                    Ok(())
                } else {
                    Err(refused())
                }
            }
        }
    }
}

impl IncomingMessage for MqttMessage {
    fn payload(&self) -> &[u8] {
        match &self.body {
            Body::Wire { payload, .. } => payload,
            #[cfg(feature = "testing")]
            Body::Local { payload, .. } => payload,
        }
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    fn partition_key(&self) -> Option<&[u8]> {
        self.headers.get(PARTITION_KEY_HEADER)
    }
}

impl Settle for MqttMessage {
    async fn ack(self) -> Result<(), AckError> {
        self.settle().await
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        if requeue {
            // MQTT has no negative acknowledgement: an unacknowledged message comes back only
            // when a persistent session resumes. Acknowledging it here would lose it.
            Err(AckError::Unsupported)
        } else {
            self.settle().await
        }
    }
}

impl Partitioned for MqttMessage {
    fn partition_key(&self) -> Option<&[u8]> {
        IncomingMessage::partition_key(self)
    }
}
