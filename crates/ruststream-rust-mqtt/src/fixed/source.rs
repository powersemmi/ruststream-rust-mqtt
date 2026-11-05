//! [`MqttTopic`] and [`MqttFilter`]: the two subscription descriptors of the device variant.
//!
//! MQTT names two things. A publisher names a *topic*, which carries no wildcard. A subscriber
//! names a *topic filter*, where `+` matches one level and `#` matches the rest. The crate keeps
//! them apart because a retry copy is a publish: a subscription on a topic is reached again by
//! publishing to that topic, and a subscription on a wildcard filter is reached by none of the
//! topics it matches. [`shared`](MqttTopic::shared) makes either an MQTT 5 shared subscription
//! (`$share/<group>/<filter>`), which is how competing consumers are expressed.

use core::future::{Future, ready};

use ruststream::{
    AddressedCopies, FromName, NamedCopies, RedeliveryAddress, RedeliveryAddressed,
    SubscriptionSource,
};

use crate::fixed::broker::ConnectedMqttBroker;
use crate::fixed::error::MqttError;
use crate::fixed::state::Name;
use crate::fixed::subscriber::MqttSubscriber;
use crate::qos::Qos;
use crate::topic::{is_mqtt_text, is_topic_filter, is_topic_name};

/// Refuses a share group name a server refuses.
fn validate_share_group(group: Option<&str>) -> Result<(), MqttError> {
    if let Some(group) = group
        && (group.is_empty() || group.contains(['/', '+', '#']) || !is_mqtt_text(group))
    {
        return Err(MqttError::Invalid("not a valid share group name"));
    }
    Ok(())
}

/// Refuses a topic no server takes.
pub(crate) fn validate_topic(topic: &str) -> Result<(), MqttError> {
    if topic.is_empty() {
        return Err(MqttError::Invalid("an MQTT topic must not be empty"));
    }
    if !is_topic_name(topic) {
        return Err(MqttError::Invalid(
            "not an MQTT topic: wildcards are subscribe-only, so a filter carrying '+' or '#' \
             subscribes with MqttFilter instead",
        ));
    }
    if !is_mqtt_text(topic) {
        return Err(MqttError::Invalid(
            "not an MQTT topic: it carries a control character or a Unicode non-character",
        ));
    }
    Ok(())
}

/// A subscription to an MQTT topic filter, which may carry the wildcards `+` and `#`.
///
/// A filter matches many topics, so no publish reaches the subscription again: a retry copy goes
/// where the mount site names it, with `out_retry(policy).to(..)`.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts};
/// use ruststream::{Deserialized, subscriber};
/// use ruststream_rust_mqtt::{MqttBroker, MqttFilter, Qos};
///
/// #[derive(Deserialized)]
/// struct Reading<'a>(&'a [u8]);
///
/// /// Every sensor's readings, at least once.
/// #[subscriber(MqttFilter::new("sensors/+/temp").qos(Qos::AtLeastOnce))]
/// async fn on_reading(reading: &Reading<'_>) -> HandlerOutcome {
///     let _ = reading.0;
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app(runtime = embassy)]
/// pub fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
///     EmbeddedApp::new().with_broker(broker, |b| b.include(on_reading))
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub struct MqttFilter {
    filter: &'static str,
    qos: Qos,
    shared: Option<&'static str>,
}

impl MqttFilter {
    /// A subscription to `filter`, at least once, not shared.
    pub const fn new(filter: &'static str) -> Self {
        Self {
            filter,
            qos: Qos::AtLeastOnce,
            shared: None,
        }
    }

    /// The quality of service the subscription asks the server for.
    pub const fn qos(mut self, qos: Qos) -> Self {
        self.qos = qos;
        self
    }

    /// Makes the subscription a member of the share group `group`: the server hands each
    /// message to one member of the group.
    pub const fn shared(mut self, group: &'static str) -> Self {
        self.shared = Some(group);
        self
    }

    /// The filter this subscription names.
    #[must_use]
    pub const fn filter(&self) -> &'static str {
        self.filter
    }

    pub(crate) fn validate(&self) -> Result<(), MqttError> {
        if !is_topic_filter(self.filter) || !is_mqtt_text(self.filter) {
            return Err(MqttError::Invalid("not a valid MQTT topic filter"));
        }
        validate_share_group(self.shared)
    }

    async fn open<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize>(
        self,
        connected: &ConnectedMqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS>,
    ) -> Result<MqttSubscriber<MESSAGES, PACKET, SUBSCRIPTIONS>, MqttError> {
        self.validate()?;
        connected
            .open(Name::Static(self.filter), self.shared, self.qos)
            .await
    }
}

impl FromName for MqttFilter {
    fn from_name(name: impl Into<&'static str>) -> Self {
        Self::new(name.into())
    }
}

/// A subscription to one MQTT topic, which carries no wildcard.
///
/// One topic is one destination, so a retry copy reaches the subscription again by a publish
/// to the topic, and the mount site names nothing.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts};
/// use ruststream::{Deserialized, subscriber};
/// use ruststream_rust_mqtt::{MqttBroker, MqttTopic, Qos};
///
/// #[derive(Deserialized)]
/// struct Command<'a>(&'a [u8]);
///
/// /// The valve's commands, exactly once.
/// #[subscriber(MqttTopic::new("valves/v1/set").qos(Qos::ExactlyOnce))]
/// async fn set(command: &Command<'_>) -> HandlerOutcome {
///     let _ = command.0;
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app(runtime = embassy)]
/// pub fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
///     EmbeddedApp::new().with_broker(broker, |b| b.include(set))
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub struct MqttTopic(MqttFilter);

impl MqttTopic {
    /// A subscription to `topic`, at least once, not shared.
    pub const fn new(topic: &'static str) -> Self {
        Self(MqttFilter::new(topic))
    }

    /// The quality of service the subscription asks the server for.
    pub const fn qos(self, qos: Qos) -> Self {
        Self(self.0.qos(qos))
    }

    /// Makes the subscription a member of the share group `group`: the server hands each
    /// message to one member of the group.
    pub const fn shared(self, group: &'static str) -> Self {
        Self(self.0.shared(group))
    }

    /// The topic this subscription names.
    #[must_use]
    pub const fn topic(&self) -> &'static str {
        self.0.filter
    }

    pub(crate) fn validate(&self) -> Result<(), MqttError> {
        validate_topic(self.0.filter)?;
        validate_share_group(self.0.shared)
    }
}

impl FromName for MqttTopic {
    fn from_name(name: impl Into<&'static str>) -> Self {
        Self::new(name.into())
    }
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize>
    SubscriptionSource<ConnectedMqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS>> for MqttTopic
{
    type Subscriber = MqttSubscriber<MESSAGES, PACKET, SUBSCRIPTIONS>;
    // One topic is one destination, so the descriptor says where a copy of a delivery reaches
    // this subscription again and the mount site names nothing.
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        self.topic()
    }

    fn static_name(&self) -> Option<&'static str> {
        Some(self.topic())
    }

    fn subscribe(
        self,
        connected: &ConnectedMqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS>,
    ) -> impl Future<Output = Result<Self::Subscriber, MqttError>> {
        let checked = self.validate();
        async move {
            checked?;
            self.0.open(connected).await
        }
    }
}

// The plain topic, never the share group's wire form: `$share/<group>/<topic>` is a
// subscribe-side name, and a publish to it would reach a topic of that literal spelling.
impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize>
    RedeliveryAddressed<ConnectedMqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS>> for MqttTopic
{
    fn redelivery_address(
        &self,
        _connected: &ConnectedMqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS>,
    ) -> impl Future<Output = Result<RedeliveryAddress, MqttError>> {
        ready(
            self.validate()
                .map(|()| RedeliveryAddress::new(self.topic())),
        )
    }
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize>
    SubscriptionSource<ConnectedMqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS>> for MqttFilter
{
    type Subscriber = MqttSubscriber<MESSAGES, PACKET, SUBSCRIPTIONS>;
    // A filter matches many topics, so the descriptor addresses no copy and the mount site names
    // where one goes.
    type Copies = NamedCopies;

    fn name(&self) -> &str {
        self.filter
    }

    fn static_name(&self) -> Option<&'static str> {
        Some(self.filter)
    }

    fn subscribe(
        self,
        connected: &ConnectedMqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS>,
    ) -> impl Future<Output = Result<Self::Subscriber, MqttError>> {
        self.open(connected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_invalid_filter_or_topic_is_refused_before_io() {
        assert!(MqttFilter::new("a/#/b").validate().is_err());
        assert!(MqttFilter::new("").validate().is_err());
        assert!(MqttFilter::new("a").shared("g/1").validate().is_err());
        assert!(MqttTopic::new("a/+").validate().is_err());
        assert!(MqttTopic::new("a/\n").validate().is_err());
        assert!(MqttTopic::new("a/b").shared("g").validate().is_ok());
    }
}
