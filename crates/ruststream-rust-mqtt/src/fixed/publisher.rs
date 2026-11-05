//! [`MqttPublisher`] and its [`MqttPublish`] policy, in the device variant.

use core::fmt;
use core::future::{Future, poll_fn, ready};

use ruststream::{
    DefaultPublish, Lend, OutgoingMessage, PairError, PublishMessageError, PublishOutcome,
    PublishPolicy, Publisher,
};

use crate::fixed::broker::ConnectedMqttBroker;
use crate::fixed::error::MqttError;
use crate::fixed::source::validate_topic;
use crate::fixed::state::{Command, Link, Mode, Sent};
use crate::fixed::wire::{self, Outgoing};
use crate::options::MqttPublishOptions;
use crate::qos::Qos;

/// Publishes messages to MQTT topics through the broker's connection.
///
/// A publish is encoded straight into one of the broker's send buffers, which the runner writes:
/// `Ok` means the PUBLISH packet went out into the session, not that the server confirmed it. The
/// publisher lends the bytes, and nothing is allocated. A publisher refuses every publish after
/// `shutdown` with [`MqttError::NotConnected`], and a refused publish hands its message back.
///
/// Headers go out as MQTT 5 user properties. The `content-type`, `reply-to` and
/// `correlation-id` headers take the matching properties (content type, response topic,
/// correlation data), and a textual content type sets the payload format indicator.
///
/// # Examples
///
/// ```
/// use ruststream::{OutgoingMessage, PublishMessageError, Publisher};
/// use ruststream_rust_mqtt::{MqttError, MqttPublisher};
///
/// /// The board announces its state once it is up.
/// async fn announce(publisher: &MqttPublisher) -> Result<(), MqttError> {
///     let online = OutgoingMessage::with_payload("valves/v1/state", &b"online"[..]);
///     publisher.publish(online, None).await.map_err(PublishMessageError::into_error)
/// }
/// # let _ = announce;
/// ```
#[derive(Clone, Copy)]
pub struct MqttPublisher<
    const MESSAGES: usize = 4,
    const PACKET: usize = 1024,
    const SUBSCRIPTIONS: usize = 8,
> {
    link: &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
    qos: Qos,
    retain: bool,
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> fmt::Debug
    for MqttPublisher<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttPublisher")
            .field("qos", &self.qos)
            .field("retain", &self.retain)
            .finish_non_exhaustive()
    }
}

/// Gives a packet identifier back unless the runner took over the publish that holds it.
struct PacketHeld<'a, const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> {
    link: &'a Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
    packet: Option<u16>,
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> Drop
    for PacketHeld<'_, MESSAGES, PACKET, SUBSCRIPTIONS>
{
    fn drop(&mut self) {
        if let Some(packet) = self.packet {
            self.link.release_packet(packet);
        }
    }
}

/// Tells the runner a publisher stopped waiting for its answer, so the send buffer comes back
/// to the pool once it is written.
struct AnswerAwaited<'a, const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> {
    link: &'a Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
    index: Option<usize>,
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> Drop
    for AnswerAwaited<'_, MESSAGES, PACKET, SUBSCRIPTIONS>
{
    fn drop(&mut self) {
        if let Some(index) = self.index {
            self.link.abandon_answer(index);
        }
    }
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize>
    MqttPublisher<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    async fn send(
        &self,
        msg: &OutgoingMessage<'_, &[u8]>,
        qos: Qos,
        retain: bool,
    ) -> Result<(), MqttError> {
        validate_topic(msg.name()).map_err(|_| MqttError::Publish("not a valid MQTT topic"))?;
        wire::check_headers(msg.headers())?;
        let link = self.link;
        match link.mode() {
            None => Err(MqttError::NotConnected),
            Some(Mode::Wire) => Self::send_on_wire(link, msg, qos, retain).await,
            #[cfg(feature = "testing")]
            Some(Mode::InProcess) => link.harness.publish(link, msg, qos, retain),
        }
    }

    async fn send_on_wire(
        link: &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
        msg: &OutgoingMessage<'_, &[u8]>,
        qos: Qos,
        retain: bool,
    ) -> Result<(), MqttError> {
        // Read before the packet identifier is taken: a session that ends meanwhile takes its
        // identifiers with it, and the runner refuses a packet encoded for it.
        let session = link.session();
        let mut held = PacketHeld { link, packet: None };
        let packet = if qos == Qos::AtMostOnce {
            0
        } else {
            let packet = link.take_packet().await.ok_or(MqttError::NotConnected)?;
            held.packet = Some(packet);
            packet
        };
        let mut buffer = link.outbound.take().await;
        buffer.len = wire::publish(
            &mut buffer.bytes,
            link.server_maximum(),
            &Outgoing {
                topic: msg.name(),
                payload: msg.payload(),
                headers: msg.headers(),
                qos,
                retain,
                packet,
            },
        )?;
        buffer.sent = Sent::Publish;
        buffer.session = session;
        let index = buffer.index;
        link.expect_answer(index);
        let mut awaited = AnswerAwaited {
            link,
            index: Some(index),
        };
        link.commands.send(Command::Send(buffer)).await;
        // From here the session owns the identifier: the server's acknowledgement frees it.
        held.packet = None;
        // A runner that stopped before this publish was queued drained the queue already; one
        // that stops after answers it as it goes.
        if !link.is_open() {
            return Err(MqttError::NotConnected);
        }
        let answer = poll_fn(|cx| link.poll_answer(index, cx)).await;
        awaited.index = None;
        answer
    }
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> Publisher
    for MqttPublisher<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    // The bytes are encoded into the broker's send buffer, so the publisher keeps nothing of the
    // caller's buffer past the call.
    type Payload = Lend;
    type Error = MqttError;
    type Options = MqttPublishOptions;

    async fn publish<'msg>(
        &self,
        msg: OutgoingMessage<'msg, &'msg [u8]>,
        options: Option<&Self::Options>,
    ) -> PublishOutcome<'msg, MqttError, Lend> {
        let (qos, retain) = MqttPublishOptions::resolve(options, self.qos, self.retain);
        match self.send(&msg, qos, retain).await {
            Ok(()) => Ok(()),
            Err(error) => Err(PublishMessageError::with_message(error, msg)),
        }
    }
}

/// The publish policy of [`MqttPublisher`]: the quality of service and the retain flag as pure
/// declaration, constructible before the broker connects.
///
/// It is the broker's default policy, so a reply or a retry copy published by the runtime uses
/// it: at least once, not retained.
///
/// # Examples
///
/// ```
/// use ruststream::PublishPolicy;
/// use ruststream_rust_mqtt::{ConnectedMqttBroker, MqttPublish, MqttPublisher, Qos};
///
/// /// The state publisher: exactly once and retained, so a dashboard that starts later reads it.
/// async fn states(connected: &ConnectedMqttBroker) -> Option<MqttPublisher> {
///     MqttPublish::default()
///         .qos(Qos::ExactlyOnce)
///         .retain(true)
///         .pair(connected)
///         .await
///         .ok()
/// }
/// # let _ = states;
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[must_use]
pub struct MqttPublish {
    qos: Qos,
    retain: bool,
}

impl MqttPublish {
    /// Publishes at `qos`.
    pub const fn qos(mut self, qos: Qos) -> Self {
        self.qos = qos;
        self
    }

    /// Publishes retained, so the server keeps the last message of each topic for subscribers
    /// that come later.
    pub const fn retain(mut self, retain: bool) -> Self {
        self.retain = retain;
        self
    }
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize>
    PublishPolicy<ConnectedMqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS>> for MqttPublish
{
    type Live = MqttPublisher<MESSAGES, PACKET, SUBSCRIPTIONS>;

    fn pair(
        self,
        connected: &ConnectedMqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS>,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(MqttPublisher {
            link: connected.link,
            qos: self.qos,
            retain: self.retain,
        }))
    }
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> DefaultPublish
    for ConnectedMqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    type Policy = MqttPublish;
}
