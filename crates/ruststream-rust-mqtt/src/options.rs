//! [`MqttPublishOptions`] and [`MqttPublishSteps`]: the settings one publish carries, and the
//! steps that set them on a publish builder.

use ruststream::runtime::{PublishBuilder, PublishSink};

use crate::qos::Qos;

/// The two settings MQTT carries on every PUBLISH packet, as one message asked for them.
///
/// This is [`Publisher::Options`](ruststream::Publisher::Options) of every publisher this crate hands out. A setting left unset
/// keeps what the [`MqttPublish`](crate::MqttPublish) policy fixed at the mount site, so a call carries only what it
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
/// [options type](ruststream::Publisher::Options), so these steps appear on a builder over an MQTT publisher
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
