//! [`MqttTopic`] and [`MqttFilter`]: the two subscription descriptors.
//!
//! MQTT names two things. A publisher names a *topic*, which carries no wildcard. A subscriber
//! names a *topic filter*, where `+` matches one level and `#` matches the rest. The crate keeps
//! them apart because a retry copy is a publish: a subscription on a topic is reached again by
//! publishing to that topic, and a subscription on a wildcard filter is reached by none of the
//! topics it matches. [`shared`](MqttTopic::shared) makes either an MQTT 5 shared subscription
//! (`$share/<group>/<filter>`), which is how competing consumers are expressed.

use alloc::borrow::{Cow, ToOwned};
use alloc::format;
use alloc::string::String;
use core::fmt;
use core::future::{Future, ready};

use ruststream::{
    AddressedCopies, FromName, NamedCopies, RedeliveryAddress, RedeliveryAddressed,
    SubscriptionSource,
};

use crate::heap::broker::ConnectedMqttBroker;
use crate::heap::error::MqttError;
use crate::heap::subscriber::MqttSubscriber;
use crate::qos::Qos;
use crate::topic::{is_mqtt_text, is_topic_filter, is_topic_name};

/// Refuses a share group no server accepts, before any I/O.
fn validate_share_group(group: Option<&String>) -> Result<(), MqttError> {
    if let Some(group) = group
        && (group.is_empty() || group.contains(['/', '+', '#']) || !is_mqtt_text(group))
    {
        return Err(MqttError::Invalid(format!(
            "{group:?} is not a valid share group name"
        )));
    }
    Ok(())
}

/// A subscription descriptor for one MQTT topic filter, wildcards included.
///
/// One such subscription reads every topic its filter matches, so it addresses none of them: the
/// mount site names where a deferred retry copy goes. [`MqttTopic`] is the descriptor of a
/// subscription that names a single topic and answers for its own copies.
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
/// /// Every sensor of the floor reports here; the share group spreads them over the boards.
/// #[subscriber(MqttFilter::new("floor-1/+/temp").qos(Qos::AtLeastOnce).shared("loggers"))]
/// async fn log(reading: &Reading<'_>) -> HandlerOutcome {
///     let _ = reading.0;
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app(runtime = embassy)]
/// pub fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
///     EmbeddedApp::new().with_broker(broker, |b| b.include(log))
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct MqttFilter {
    filter: String,
    qos: Qos,
    shared: Option<String>,
}

impl MqttFilter {
    /// Names the topic filter, with wildcards as the protocol defines them.
    pub fn new(filter: impl Into<String>) -> Self {
        Self {
            filter: filter.into(),
            qos: Qos::default(),
            shared: None,
        }
    }

    /// Sets the quality of service the subscription asks for. Defaults to
    /// [`Qos::AtLeastOnce`].
    pub const fn qos(mut self, qos: Qos) -> Self {
        self.qos = qos;
        self
    }

    /// Makes this an MQTT 5 shared subscription in `group`: the server hands each matching
    /// message to one member of the group instead of to every subscription.
    pub fn shared(mut self, group: impl Into<String>) -> Self {
        self.shared = Some(group.into());
        self
    }

    /// The topic filter, without the share group.
    #[must_use]
    pub fn filter(&self) -> &str {
        &self.filter
    }

    pub(crate) const fn qos_value(&self) -> Qos {
        self.qos
    }

    /// The filter as sent on the wire: `$share/<group>/<filter>` for a share group.
    pub(crate) fn wire_filter(&self) -> String {
        self.shared.as_ref().map_or_else(
            || self.filter.clone(),
            |group| format!("$share/{group}/{}", self.filter),
        )
    }

    /// Refuses a descriptor that cannot form a subscription, before any I/O.
    pub(crate) fn validate(&self) -> Result<(), MqttError> {
        if !is_topic_filter(&self.filter) || !is_mqtt_text(&self.filter) {
            return Err(MqttError::Invalid(format!(
                "{:?} is not a valid MQTT topic filter",
                self.filter
            )));
        }
        validate_share_group(self.shared.as_ref())
    }
}

/// A topic filter is all this descriptor needs, so the mount site may supply it: a definition
/// written `#[subscriber(MqttFilter)]` takes its filter from `name(..)` there.
impl FromName for MqttFilter {
    fn from_name(name: impl Into<Cow<'static, str>>) -> Self {
        Self::new(name.into().into_owned())
    }
}

/// A subscription descriptor for one MQTT topic.
///
/// A topic carries no wildcard, so it is a name a publisher can use: the subscription is reached
/// again by publishing to it, share group or not. That is what lets this descriptor answer for
/// its own retry copies; a subscription written with `+` or `#` takes [`MqttFilter`].
///
/// # Examples
///
/// ```
/// # mod demo {
/// use core::time::Duration;
///
/// use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts};
/// use ruststream::{Deserialized, subscriber};
/// use ruststream_rust_mqtt::{MqttBroker, MqttTopic, Qos};
///
/// #[derive(Deserialized)]
/// struct Command<'a>(&'a [u8]);
///
/// /// A valve that is still moving asks for the command again in a second.
/// #[subscriber(MqttTopic::new("valves/v1/set").qos(Qos::ExactlyOnce))]
/// async fn set(command: &Command<'_>) -> HandlerOutcome {
///     if command.0.is_empty() {
///         return HandlerOutcome::retry_after(Duration::from_secs(1));
///     }
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
#[derive(Clone, PartialEq, Eq)]
#[must_use]
pub struct MqttTopic(MqttFilter);

impl fmt::Debug for MqttTopic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttTopic")
            .field("topic", &self.0.filter)
            .field("qos", &self.0.qos)
            .field("shared", &self.0.shared)
            .finish()
    }
}

impl MqttTopic {
    /// Names the topic. A value carrying a wildcard is refused when the subscription opens,
    /// naming [`MqttFilter`] as the descriptor that takes one.
    pub fn new(topic: impl Into<String>) -> Self {
        Self(MqttFilter::new(topic))
    }

    /// Sets the quality of service the subscription asks for. Defaults to
    /// [`Qos::AtLeastOnce`].
    pub const fn qos(mut self, qos: Qos) -> Self {
        self.0.qos = qos;
        self
    }

    /// Makes this an MQTT 5 shared subscription in `group`: the server hands each message to one
    /// member of the group.
    pub fn shared(mut self, group: impl Into<String>) -> Self {
        self.0 = self.0.shared(group);
        self
    }

    /// The topic this subscribes to, without the share group.
    #[must_use]
    pub fn topic(&self) -> &str {
        self.0.filter()
    }

    pub(crate) fn into_filter(self) -> MqttFilter {
        self.0
    }

    /// Refuses a value no publisher could name, before any I/O.
    pub(crate) fn validate(&self) -> Result<(), MqttError> {
        let topic = &self.0.filter;
        if topic.is_empty() {
            return Err(MqttError::Invalid(
                "an MQTT topic must not be empty".to_owned(),
            ));
        }
        if !is_topic_name(topic) {
            return Err(MqttError::Invalid(format!(
                "{topic:?} is not an MQTT topic: wildcards are subscribe-only, so a filter \
                 carrying '+' or '#' subscribes with MqttFilter instead"
            )));
        }
        if !is_mqtt_text(topic) {
            return Err(MqttError::Invalid(format!(
                "{topic:?} is not an MQTT topic: it carries a control character or a Unicode \
                 non-character"
            )));
        }
        validate_share_group(self.0.shared.as_ref())
    }

    /// The topic a publisher reaches this subscription on.
    fn address(&self) -> Result<RedeliveryAddress, MqttError> {
        self.validate()?;
        Ok(RedeliveryAddress::new(self.0.filter.clone()))
    }
}

/// A topic is all this descriptor needs, so the mount site may supply it: a definition written
/// `#[subscriber(MqttTopic)]` takes its topic from `name(..)` there.
impl FromName for MqttTopic {
    fn from_name(name: impl Into<Cow<'static, str>>) -> Self {
        Self::new(name.into().into_owned())
    }
}

impl SubscriptionSource<ConnectedMqttBroker> for MqttTopic {
    type Subscriber = MqttSubscriber;
    // One topic is one destination, so the descriptor says where a copy of a delivery reaches
    // this subscription again and the mount site names nothing.
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        self.topic()
    }

    fn subscribe(
        self,
        connected: &ConnectedMqttBroker,
    ) -> impl Future<Output = Result<MqttSubscriber, MqttError>> {
        let checked = self.validate();
        let filter = self.into_filter();
        async move {
            checked?;
            connected.open(&filter).await
        }
    }
}

// The plain topic, never the share group's wire form: `$share/<group>/<topic>` is a
// subscribe-side name, and a publish to it would reach a topic of that literal spelling.
impl RedeliveryAddressed<ConnectedMqttBroker> for MqttTopic {
    fn redelivery_address(
        &self,
        _connected: &ConnectedMqttBroker,
    ) -> impl Future<Output = Result<RedeliveryAddress, MqttError>> {
        ready(self.address())
    }
}

impl SubscriptionSource<ConnectedMqttBroker> for MqttFilter {
    type Subscriber = MqttSubscriber;
    // A filter matches many topics, so the descriptor addresses no copy and the mount site names
    // where one goes.
    type Copies = NamedCopies;

    fn name(&self) -> &str {
        self.filter()
    }

    async fn subscribe(self, connected: &ConnectedMqttBroker) -> Result<MqttSubscriber, MqttError> {
        self.validate()?;
        connected.open(&self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_invalid_filter_is_refused_before_io() {
        assert!(MqttFilter::new("a/#/b").validate().is_err());
        assert!(MqttFilter::new("").validate().is_err());
        assert!(MqttFilter::new("devices/\n/+").validate().is_err());
    }

    #[test]
    fn an_invalid_share_group_is_refused_before_io() {
        assert!(MqttFilter::new("a").shared("g/1").validate().is_err());
        assert!(MqttFilter::new("a").shared("").validate().is_err());
        assert!(MqttTopic::new("a").shared("g+1").validate().is_err());
    }

    #[test]
    fn a_share_group_is_spelled_on_the_wire_only() {
        let filter = MqttFilter::new("orders/+").shared("workers");
        assert_eq!(filter.filter(), "orders/+");
        assert_eq!(filter.wire_filter(), "$share/workers/orders/+");
    }

    #[test]
    fn a_topic_is_the_address_its_retry_copies_go_to() {
        assert_eq!(
            MqttTopic::new("devices/dev42/telemetry").address(),
            Ok(RedeliveryAddress::new("devices/dev42/telemetry"))
        );
        assert_eq!(
            MqttTopic::new("jobs").shared("workers").address(),
            Ok(RedeliveryAddress::new("jobs"))
        );
        for wildcard in ["devices/+/telemetry", "devices/#"] {
            let refused = MqttTopic::new(wildcard)
                .address()
                .expect_err("a filter is not an address");
            assert!(alloc::format!("{refused}").contains("MqttFilter"));
        }
        assert!(MqttTopic::new("").address().is_err());
    }

    #[test]
    fn a_descriptor_built_from_a_name_is_the_plain_one() {
        assert_eq!(
            MqttFilter::from_name("devices/+/telemetry"),
            MqttFilter::new("devices/+/telemetry")
        );
        assert_eq!(
            MqttTopic::from_name("devices/dev42/telemetry"),
            MqttTopic::new("devices/dev42/telemetry")
        );
    }
}
