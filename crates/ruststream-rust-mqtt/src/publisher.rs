//! [`MqttPublisher`], its [`MqttPublish`] policy, and the per-message settings.

use alloc::borrow::ToOwned;
use core::fmt;
use core::future::{Future, ready};
use core::mem;

use ruststream::runtime::{PublishBuilder, PublishSink};
use ruststream::{
    HeaderMap, Lend, OutgoingMessage, PairError, PublishMessageError, PublishOutcome,
    PublishPolicy, Publisher,
};

use crate::broker::{Backend, ConnectedMqttBroker};
use crate::error::MqttError;
use crate::link::{Command, Link, Shared};
use crate::qos::Qos;
use crate::topic::{is_mqtt_text, is_topic_name};

/// Refuses a topic no server takes, before the message leaves the publisher.
pub(crate) fn check_topic(topic: &str) -> Result<(), MqttError> {
    let reason = if topic.is_empty() {
        "an MQTT topic must not be empty"
    } else if !is_topic_name(topic) {
        "not a valid MQTT topic (wildcards are subscribe-only)"
    } else if !is_mqtt_text(topic) {
        "not a valid MQTT topic (it carries a control character or a Unicode non-character)"
    } else {
        return Ok(());
    };
    Err(MqttError::Publish {
        topic: topic.to_owned(),
        reason: reason.to_owned(),
    })
}

/// Refuses headers no MQTT property carries, before the message leaves the publisher: a name and
/// a value must be UTF-8 text with no control character, except the correlation data, which is
/// binary on the wire.
pub(crate) fn check_headers(topic: &str, headers: &HeaderMap) -> Result<(), MqttError> {
    for (name, value) in headers.iter() {
        if name == "correlation-id" {
            continue;
        }
        let text = core::str::from_utf8(value).ok();
        if !(is_mqtt_text(name) && text.is_some_and(is_mqtt_text)) {
            return Err(MqttError::Publish {
                topic: topic.to_owned(),
                reason: alloc::format!(
                    "header {name:?} is not text an MQTT property carries: its name and value \
                     must be UTF-8 with no control character and no Unicode non-character"
                ),
            });
        }
    }
    Ok(())
}

/// Publishes messages to MQTT topics through the broker's connection.
///
/// A publish is staged for the runner, which sends it: `Ok` means the PUBLISH packet went out
/// into the session, not that the server confirmed it. For `QoS` 1 and 2 the session keeps the
/// message until the server acknowledges it. A publisher refuses every publish after `shutdown`
/// with [`MqttError::NotConnected`], and a refused publish hands its message back.
///
/// The publisher lends the bytes: they are copied into a staging slot the broker owns, whose
/// buffers keep their capacity, so a publish allocates nothing once the slots have held the
/// largest message the service sends.
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
///     let online = OutgoingMessage::new("valves/v1/state", b"online");
///     publisher.publish(online, None).await.map_err(PublishMessageError::into_error)
/// }
/// # let _ = announce;
/// ```
#[derive(Clone)]
pub struct MqttPublisher {
    backend: Backend,
    qos: Qos,
    retain: bool,
}

impl fmt::Debug for MqttPublisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttPublisher")
            .field("qos", &self.qos)
            .field("retain", &self.retain)
            .finish_non_exhaustive()
    }
}

impl Publisher for MqttPublisher {
    // The bytes are copied into the broker's staging slot, so the publisher keeps nothing of
    // the caller's buffer past the call.
    type Payload = Lend;
    type Error = MqttError;
    type Options = MqttPublishOptions;

    async fn publish<'msg>(
        &self,
        msg: OutgoingMessage<'msg>,
        options: Option<&Self::Options>,
    ) -> PublishOutcome<'msg, MqttError, Lend> {
        let (qos, retain) = MqttPublishOptions::resolve(options, self.qos, self.retain);
        if let Err(error) =
            check_topic(msg.name()).and_then(|()| check_headers(msg.name(), msg.headers()))
        {
            return Err(PublishMessageError::with_message(error, msg));
        }
        match &self.backend {
            Backend::Wire(link) => send(link, qos, retain, msg).await,
            #[cfg(feature = "testing")]
            Backend::InProcess(bus) => bus
                .publish(&msg, qos, retain)
                .map_err(|error| PublishMessageError::with_message(error, msg)),
        }
    }
}

/// Stages one publish for the runner and waits for its answer.
async fn send<'msg>(
    link: &Shared<Link>,
    qos: Qos,
    retain: bool,
    msg: OutgoingMessage<'msg>,
) -> PublishOutcome<'msg, MqttError, Lend> {
    if !link.is_open() {
        return Err(PublishMessageError::with_message(
            MqttError::NotConnected,
            msg,
        ));
    }
    let index = link.free.receive().await;
    let (name, payload, headers) = msg.into_parts();
    let slot = &link.slots[index];
    slot.staged.lock(|staged| {
        let mut staged = staged.borrow_mut();
        staged.topic.clear();
        staged.topic.push_str(name);
        staged.payload.clear();
        staged.payload.extend_from_slice(payload);
        staged.headers = headers;
        staged.qos = qos;
        staged.retain = retain;
    });
    slot.done.reset();
    link.commands.send(Command::Publish(index)).await;
    // A runner that stopped meanwhile answers every slot as it goes; one that stopped before
    // this publish was staged left the broker closed, which this check reads.
    let answer = if link.is_open() {
        slot.done.wait().await
    } else {
        Err(MqttError::NotConnected)
    };
    let headers = slot
        .staged
        .lock(|staged| mem::take(&mut staged.borrow_mut().headers));
    // The slot is back in the pool before the answer leaves, so the next publish finds it.
    let _ = link.free.try_send(index);
    answer.map_err(|error| {
        PublishMessageError::with_message(error, OutgoingMessage::assembled(name, payload, headers))
    })
}

/// The two settings MQTT carries on every PUBLISH packet, as one message asked for them.
///
/// This is [`Publisher::Options`] of every publisher this crate hands out. A setting left unset
/// keeps what the [`MqttPublish`] policy fixed at the mount site, so a call carries only what it
/// changed. A service writes [the steps](MqttPublishSteps) rather than this type; a test names it
/// to assert what a publish carried.
///
/// # Examples
///
/// ```
/// use ruststream_rust_mqtt::{MqttPublishOptions, Qos};
///
/// /// What a retained, exactly-once state announcement carries.
/// fn announcement() -> MqttPublishOptions {
///     MqttPublishOptions::default().qos(Qos::ExactlyOnce).retain(true)
/// }
///
/// assert_ne!(announcement(), MqttPublishOptions::default());
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[must_use]
pub struct MqttPublishOptions {
    qos: Option<Qos>,
    retain: Option<bool>,
}

impl MqttPublishOptions {
    /// Sends this one message at `qos` instead of the policy's.
    pub const fn qos(mut self, qos: Qos) -> Self {
        self.qos = Some(qos);
        self
    }

    /// Sends this one message retained, or explicitly not retained, whatever the policy
    /// declares.
    pub const fn retain(mut self, retain: bool) -> Self {
        self.retain = Some(retain);
        self
    }

    /// Resolves one call's settings over the defaults its publisher was paired with.
    pub(crate) fn resolve(options: Option<&Self>, qos: Qos, retain: bool) -> (Qos, bool) {
        let options = options.copied().unwrap_or_default();
        (options.qos.unwrap_or(qos), options.retain.unwrap_or(retain))
    }
}

/// The per-message steps of an MQTT publish, on the publish builder.
///
/// Both settings MQTT carries on a PUBLISH packet are reopened at the call site:
/// `out.message(&state).retain(true).publish()`. The bound is on the sink's
/// [options type](Publisher::Options), so these steps appear on a builder over an MQTT publisher
/// and on no other broker's. A handler body that takes one imports this crate's prelude and
/// bounds its slot with `Out<impl Publisher<Options = MqttPublishOptions>, Marker>`.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts, Out};
/// use ruststream::{Deserialized, OutSlot, Outgoing, Publisher, Serialized, subscriber};
/// use ruststream_rust_mqtt::prelude::*;
///
/// #[derive(Deserialized)]
/// struct Reading<'a>(&'a [u8]);
///
/// /// The state a reading puts the valve in, as bytes on the wire.
/// #[derive(Outgoing, Serialized)]
/// #[outgoing(name = "valves/v1/state")]
/// struct State(&'static [u8]);
///
/// #[derive(OutSlot)]
/// #[publishes(State)]
/// struct States;
///
/// /// The state is retained, so a dashboard that starts later reads the last one.
/// #[subscriber(MqttTopic::new("valves/v1/position"))]
/// async fn track(
///     reading: &Reading<'_>,
///     Out(states): Out<impl Publisher<Options = MqttPublishOptions>, States>,
/// ) -> HandlerOutcome {
///     let state = State(if reading.0 == b"0" { b"closed" } else { b"open" });
///     if states.message(&state).retain(true).publish().await.is_err() {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app(runtime = embassy)]
/// pub fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
///     EmbeddedApp::new().with_broker(broker, |b| b.include(track).out(States, MqttPublish::default()))
/// }
/// # }
/// # fn main() {}
/// ```
pub trait MqttPublishSteps {
    /// Sends this one message at `qos` instead of the publisher's own.
    #[must_use]
    fn qos(self, qos: Qos) -> Self;

    /// Sends this one message retained, or explicitly not retained, whatever the publisher's
    /// policy declares.
    #[must_use]
    fn retain(self, retain: bool) -> Self;
}

impl<Sink, Body, Enc, Hdrs, Dest> MqttPublishSteps for PublishBuilder<Sink, Body, Enc, Hdrs, Dest>
where
    Sink: PublishSink<Options = MqttPublishOptions>,
{
    fn qos(mut self, qos: Qos) -> Self {
        self.options_mut()
            .get_or_insert_with(MqttPublishOptions::default)
            .qos = Some(qos);
        self
    }

    fn retain(mut self, retain: bool) -> Self {
        self.options_mut()
            .get_or_insert_with(MqttPublishOptions::default)
            .retain = Some(retain);
        self
    }
}

/// The publish policy of [`MqttPublisher`]: the quality of service and the retain flag as pure
/// declaration, paired with the connected broker after `connect`.
///
/// It is the broker's default policy, so a reply or a retry copy the runtime publishes uses it.
///
/// # Examples
///
/// ```
/// use ruststream_rust_mqtt::{MqttPublish, Qos};
///
/// /// Every state this mount publishes is retained and sent exactly once.
/// fn states() -> MqttPublish {
///     MqttPublish::default().qos(Qos::ExactlyOnce).retain(true)
/// }
/// # let _ = states();
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[must_use]
pub struct MqttPublish {
    qos: Qos,
    retain: bool,
}

impl MqttPublish {
    /// Sets the quality of service. Defaults to [`Qos::AtLeastOnce`].
    pub const fn qos(mut self, qos: Qos) -> Self {
        self.qos = qos;
        self
    }

    /// Publishes messages retained: the server keeps the last one per topic and hands it to every
    /// new subscription that is not shared.
    pub const fn retain(mut self, retain: bool) -> Self {
        self.retain = retain;
        self
    }
}

impl PublishPolicy<ConnectedMqttBroker> for MqttPublish {
    type Live = MqttPublisher;

    fn pair(
        self,
        connected: &ConnectedMqttBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(MqttPublisher {
            backend: connected.backend.clone(),
            qos: self.qos,
            retain: self.retain,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_topic_a_server_closes_the_connection_on_is_refused() {
        assert!(check_topic("devices/a\nb").is_err());
        assert!(check_topic("devices/\u{fdd0}").is_err());
        assert!(check_topic("devices/+/state").is_err());
        assert!(check_topic("").is_err());
        assert!(check_topic("devices/zürich").is_ok());
    }

    #[test]
    fn a_call_that_names_nothing_publishes_under_the_policy() {
        assert_eq!(
            MqttPublishOptions::resolve(None, Qos::AtLeastOnce, false),
            (Qos::AtLeastOnce, false)
        );
    }

    #[test]
    fn a_named_setting_wins_and_an_unnamed_one_keeps_the_policy_value() {
        let options = MqttPublishOptions::default().retain(true);
        assert_eq!(
            MqttPublishOptions::resolve(Some(&options), Qos::ExactlyOnce, false),
            (Qos::ExactlyOnce, true)
        );
    }
}
