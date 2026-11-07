//! [`MqttMessage`], one delivery of the device variant, and its settlement.

use core::fmt;

use ruststream::{AckError, Headers, IncomingMessage, Partitioned, Settle};

use crate::PARTITION_KEY_HEADER;
use crate::fixed::error::MqttError;
use crate::fixed::state::{Command, InboundLease, Link, Mode};
use crate::qos::Qos;

/// One delivery of an MQTT subscription.
///
/// It holds the receive buffer the PUBLISH arrived in, and its topic and payload borrow from it:
/// nothing of the packet is copied but its headers. The buffer goes back to the broker when the
/// handler settles the delivery or drops it.
///
/// The handler's settlement is the acknowledgement: `ack` sends the PUBACK of a `QoS` 1 delivery
/// and the PUBREC of a `QoS` 2 one (the broker completes the handshake when the server releases
/// it). A `QoS` 0 delivery carries no acknowledgement and answers [`AckError::Unsupported`], and
/// so does a delivery that reached a second subscription with the same packet, whose first
/// subscription owns the acknowledgement. MQTT has no negative acknowledgement, so `nack(true)`
/// answers [`AckError::Unsupported`] too and leaves the message unacknowledged, for the server to
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
/// /// The raw bytes of a reading, borrowed from the broker's receive buffer.
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
pub struct MqttMessage<
    const MESSAGES: usize = 4,
    const PACKET: usize = 1024,
    const SUBSCRIPTIONS: usize = 8,
> {
    #[cfg(feature = "testing")]
    _counted: Option<crate::fixed::testing::Counted<MESSAGES, PACKET, SUBSCRIPTIONS>>,
    link: &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
    inbound: InboundLease<MESSAGES, PACKET>,
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> fmt::Debug
    for MqttMessage<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttMessage")
            .field("topic", &self.topic())
            .field("qos", &self.inbound.qos)
            .field("payload_len", &self.payload().len())
            .finish_non_exhaustive()
    }
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize>
    MqttMessage<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    pub(crate) fn new(
        inbound: InboundLease<MESSAGES, PACKET>,
        link: &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
    ) -> Self {
        Self {
            #[cfg(feature = "testing")]
            _counted: inbound
                .counted
                .then(|| crate::fixed::testing::Counted::new(link)),
            link,
            inbound,
        }
    }

    /// The topic this delivery was published to, which a wildcard filter does not name.
    #[must_use]
    pub fn topic(&self) -> &str {
        self.inbound.topic()
    }

    /// The quality of service the server delivered this message at: the lesser of the
    /// subscription's and the publisher's.
    #[must_use]
    pub fn qos(&self) -> Qos {
        self.inbound.qos
    }

    /// Hands the settlement to the transport.
    async fn settle(self) -> Result<(), AckError> {
        let (packet, qos, session) = (self.inbound.packet, self.inbound.qos, self.inbound.session);
        let link = self.link;
        // The buffer goes back before the acknowledgement waits for room in the runner's queue,
        // so a full queue never holds a buffer the runner needs to read with.
        drop(self);
        if packet == 0 {
            return Err(AckError::Unsupported);
        }
        match link.mode() {
            None => Err(MqttError::NotConnected.into()),
            Some(Mode::Wire) => {
                if link.session() != session {
                    // The session the delivery arrived in is gone, and the packet with it.
                    return Err(MqttError::NotConnected.into());
                }
                link.commands
                    .send(Command::Ack {
                        packet,
                        qos,
                        session,
                    })
                    .await;
                Ok(())
            }
            #[cfg(feature = "testing")]
            Some(Mode::InProcess) => Ok(()),
        }
    }
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> IncomingMessage
    for MqttMessage<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    fn payload(&self) -> &[u8] {
        self.inbound.payload()
    }

    fn headers(&self) -> &Headers {
        &self.inbound.headers
    }

    fn partition_key(&self) -> Option<&[u8]> {
        self.inbound.headers.get(PARTITION_KEY_HEADER)
    }
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> Settle
    for MqttMessage<MESSAGES, PACKET, SUBSCRIPTIONS>
{
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

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> Partitioned
    for MqttMessage<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    fn partition_key(&self) -> Option<&[u8]> {
        IncomingMessage::partition_key(self)
    }
}
