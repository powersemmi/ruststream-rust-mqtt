//! The broker's in-process mode, behind the `testing` feature: the transport the test harness
//! connects the broker through instead of the network.
//!
//! The connected broker, its publishers, its subscribers and its deliveries are the production
//! types, holding this transport in place of the runner, so a service's descriptors and publish
//! policies run against it unchanged. A publish goes through the same checks a live one does.
//!
//! The transport is a model of the server. It keeps the session's subscriptions, one per wire
//! filter, and the retained message of each topic. A publish reaches every subscription whose
//! filter matches its topic, at the lesser of the two qualities of service; a share group takes
//! one copy, which its members take in turn. A subscription that is not shared receives the
//! retained messages its filter matches when it opens. An acknowledgement after the broker shut
//! down is refused.
//!
//! What belongs to the server and is left to the live mode: a persistent session and its
//! redelivery, the receive-maximum window, the protocol handshakes behind `QoS` 1 and 2, and a
//! server's access control.

use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;
use core::cell::{Cell, RefCell};
use core::future::{Future, ready};

use embassy_sync::blocking_mutex::Mutex;
use ruststream::testing::{Coordinator, InProcess, TestableBroker};
use ruststream::{Bytes, HeaderMap, OutgoingMessage, RawMessage};

use crate::heap::broker::{Backend, ConnectedMqttBroker, DELIVERIES_PER_SUBSCRIPTION, MqttBroker};
use crate::heap::error::MqttError;
use crate::heap::link::{RawLock, Shared};
use crate::heap::message::{Acker, Body, MqttMessage};
use crate::heap::publisher::{check_headers, check_topic};
use crate::heap::queue::Queue;
use crate::heap::routes::{Member, Routes};
use crate::heap::source::MqttFilter;
use crate::heap::subscriber::{Detach, MqttSubscriber};
use crate::qos::Qos;
use crate::topic::matches;

/// The harness's count of one delivery in flight: counted when it is made, released when it is
/// dropped, whether it was settled or not.
pub(crate) struct InFlight {
    coordinator: Coordinator,
}

impl InFlight {
    fn new(coordinator: &Coordinator) -> Self {
        coordinator.enqueued();
        Self {
            coordinator: coordinator.clone(),
        }
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.coordinator.consumed();
    }
}

/// A message the server keeps for a topic.
#[derive(Clone)]
struct Retained {
    payload: Bytes,
    headers: HeaderMap,
    qos: Qos,
}

/// What the server keeps.
#[derive(Default)]
struct Server {
    routes: Routes,
    retained: BTreeMap<String, Retained>,
    log: BTreeMap<String, Vec<RawMessage>>,
}

/// The in-process transport: the server this session is connected to.
pub(crate) struct Bus {
    server: Mutex<RawLock, RefCell<Server>>,
    coordinator: Mutex<RawLock, RefCell<Option<Coordinator>>>,
    open: Mutex<RawLock, Cell<bool>>,
}

impl Bus {
    fn new() -> Self {
        Self {
            server: Mutex::new(RefCell::new(Server::default())),
            coordinator: Mutex::new(RefCell::new(None)),
            open: Mutex::new(Cell::new(true)),
        }
    }

    /// Whether the session is still open, which is what an acknowledgement needs.
    pub(crate) fn is_open(&self) -> bool {
        self.open.lock(Cell::get)
    }

    /// Ends the session, as a DISCONNECT does: every subscription's stream ends.
    pub(crate) fn close(&self) {
        self.open.lock(|open| open.set(false));
        let queues = self
            .server
            .lock(|server| server.borrow_mut().routes.drain_queues());
        for queue in queues {
            queue.close(None);
        }
    }

    fn coordinator(&self) -> Option<Coordinator> {
        self.coordinator.lock(|held| held.borrow().clone())
    }

    /// Opens a subscription and hands it the retained messages its filter matches.
    pub(crate) fn subscribe(self: &Shared<Self>, filter: &MqttFilter) -> MqttSubscriber {
        let queue = Shared::new(Queue::new(DELIVERIES_PER_SUBSCRIPTION));
        let wire_filter = filter.wire_filter();
        let qos = filter.qos_value();
        let (joined, retained) = self.server.lock(|server| {
            let mut server = server.borrow_mut();
            let joined = server.routes.join(&wire_filter, qos, Shared::clone(&queue));
            // A share group hands a retained message to none of its members, and a member that
            // joins a route already there opens no new subscription on the server.
            let shared = wire_filter.starts_with("$share/");
            let retained: Vec<(String, Retained)> = if joined.opened && !shared {
                server
                    .retained
                    .iter()
                    .filter(|(topic, _)| matches(topic, filter.filter()))
                    .map(|(topic, retained)| (topic.clone(), retained.clone()))
                    .collect()
            } else {
                Vec::new()
            };
            (joined, retained)
        });
        for (topic, retained) in retained {
            let message = self.message(
                topic,
                retained.payload,
                retained.headers,
                retained.qos.lesser(qos),
            );
            Self::hand_over(&queue, message);
        }
        MqttSubscriber::new(queue, Detach::Local(Shared::clone(self), joined.member))
    }

    /// Takes a subscription of the app out.
    pub(crate) fn leave(&self, member: Member) {
        self.server
            .lock(|server| server.borrow_mut().routes.leave(member));
    }

    /// Takes a publish of this session.
    pub(crate) fn publish(
        self: &Shared<Self>,
        message: &OutgoingMessage<'_>,
        qos: Qos,
        retain: bool,
    ) -> Result<(), MqttError> {
        if !self.is_open() {
            return Err(MqttError::NotConnected);
        }
        check_headers(message.name(), message.headers())?;
        self.receive(message, qos, retain);
        Ok(())
    }

    /// Takes a publish of another client of the server: at the default quality of service and
    /// not retained.
    fn inject(self: &Shared<Self>, message: &OutgoingMessage<'_>) -> Result<(), MqttError> {
        check_topic(message.name())?;
        check_headers(message.name(), message.headers())?;
        self.receive(message, Qos::default(), false);
        Ok(())
    }

    /// What the server does with a PUBLISH it received: logs it, keeps it when it is retained,
    /// and sends it to every subscription its topic matches.
    fn receive(self: &Shared<Self>, message: &OutgoingMessage<'_>, qos: Qos, retain: bool) {
        let topic = message.name();
        let payload = Bytes::copy_from_slice(message.payload());
        let headers = message.headers().clone();
        let mut targets: Vec<(Shared<Queue>, Qos)> = Vec::new();
        self.server.lock(|server| {
            let mut server = server.borrow_mut();
            server.log.entry(topic.into()).or_default().push(
                RawMessage::new(String::from(topic), payload.clone()).with_headers(headers.clone()),
            );
            if retain {
                // An empty retained payload is how a publisher clears the topic.
                if payload.is_empty() {
                    server.retained.remove(topic);
                } else {
                    server.retained.insert(
                        topic.into(),
                        Retained {
                            payload: payload.clone(),
                            headers: headers.clone(),
                            qos,
                        },
                    );
                }
            }
            server.routes.reach(&[], topic, |route, queue| {
                targets.push((Shared::clone(queue), route.qos.lesser(qos)));
            });
        });
        for (queue, qos) in targets {
            let message = self.message(topic.into(), payload.clone(), headers.clone(), qos);
            Self::hand_over(&queue, message);
        }
    }

    /// One delivery of this transport.
    fn message(
        self: &Shared<Self>,
        topic: String,
        payload: Bytes,
        headers: HeaderMap,
        qos: Qos,
    ) -> MqttMessage {
        let acker = (qos != Qos::AtMostOnce).then(|| Acker::Local(Shared::clone(self)));
        MqttMessage::new(Body::Local { topic, payload }, headers, qos, acker)
            .counted(self.coordinator().as_ref().map(InFlight::new))
    }

    /// Puts a delivery into its subscription's queue.
    fn hand_over(queue: &Queue, message: MqttMessage) {
        if let Err(refused) = queue.push(message) {
            // A server stops sending to a client whose window is full; the test outran the
            // capacity its handlers drain, which is a test to fix rather than a message to lose.
            panic!(
                "the in-process MQTT subscription holds {DELIVERIES_PER_SUBSCRIPTION} deliveries \
                 and refused the one to {:?}: settle the deliveries before publishing more",
                refused.topic()
            );
        }
    }
}

// The compio harness finds a broker by its type, so the broker lists itself.
// The options are checked as `connect` checks them, so a configuration a service could not
// connect with is not one a test connects with either.
ruststream::register_testable_broker!(MqttBroker);

impl InProcess for MqttBroker {
    fn connect_in_process(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        ready(self.config().endpoint().map(|_| ConnectedMqttBroker {
            backend: Backend::InProcess(Shared::new(Bus::new())),
        }))
    }
}

/// The harness's view of the in-process transport.
///
/// # Panics
///
/// `inject` and `published` panic on a broker connected with `connect`: the harness drives only
/// the transport `connect_in_process` produced, and a live connection has no log to read.
impl TestableBroker for ConnectedMqttBroker {
    fn install_coordinator(&self, coordinator: Coordinator) {
        if let Backend::InProcess(bus) = &self.backend {
            bus.coordinator.lock(|held| {
                let mut held = held.borrow_mut();
                if held.is_none() {
                    *held = Some(coordinator);
                }
            });
        }
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        let bus = self.bus("inject");
        if let Err(error) = bus.inject(&message) {
            panic!(
                "the injected message to {:?} is not one a server takes: {error}",
                message.name()
            );
        }
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.bus("published")
            .server
            .lock(|server| server.borrow().log.get(name).cloned().unwrap_or_default())
    }

    /// Every subscription whose filter matches the destination; the subscriptions the app
    /// opened on one filter are one subscription to the server, and the answer names the one
    /// whose turn it is.
    fn routes(&self, destination: &str, subscriptions: &[&str]) -> Vec<usize> {
        let mut routed = Vec::new();
        let mut answered: Vec<&str> = Vec::new();
        for filter in subscriptions {
            if !matches(destination, filter) || answered.contains(filter) {
                continue;
            }
            answered.push(filter);
            // The subscriptions the app opened under this name, in the order it opened them.
            let positions: Vec<usize> = subscriptions
                .iter()
                .enumerate()
                .filter(|(_, name)| *name == filter)
                .map(|(position, _)| position)
                .collect();
            let turn = self
                .bus("routes")
                .server
                .lock(|server| server.borrow().routes.next_taker(filter));
            match turn {
                Some(turn) => routed.extend(positions.get(turn)),
                // A filter this connection has not opened answers by its name alone.
                None => routed.extend(positions),
            }
        }
        routed
    }
}

impl ConnectedMqttBroker {
    fn bus(&self, operation: &str) -> &Shared<Bus> {
        match &self.backend {
            Backend::InProcess(bus) => bus,
            Backend::Wire(_) => panic!(
                "`{operation}` drives the in-process transport, and this broker was connected \
                 with `connect`: the harness connects it with `connect_in_process`"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use core::pin::pin;
    use core::task::Poll;

    use futures::{StreamExt, poll};
    use ruststream::{
        AckError, ConnectedBroker, IncomingMessage, OutgoingMessage, PublishPolicy, Publisher,
        Settle, Subscriber, SubscriptionSource,
    };

    use super::*;
    use crate::heap::publisher::MqttPublish;
    use crate::heap::source::MqttTopic;
    use crate::{MqttConfig, MqttPublisher};

    async fn connected() -> ConnectedMqttBroker {
        let (broker, _runner) = MqttBroker::new(MqttConfig::new("127.0.0.1:1883"));
        broker
            .connect_in_process()
            .await
            .expect("the in-process transport connects")
    }

    async fn publisher(connected: &ConnectedMqttBroker, policy: MqttPublish) -> MqttPublisher {
        policy.pair(connected).await.expect("the policy pairs")
    }

    async fn send(publisher: &MqttPublisher, topic: &str, payload: &'static [u8]) {
        publisher
            .publish(OutgoingMessage::new(topic, payload), None)
            .await
            .expect("the server takes the publish");
    }

    /// What the subscription's stream yields right now: the delivery's payload and quality of
    /// service, or `None` where nothing waits.
    async fn next(subscriber: &mut MqttSubscriber) -> Option<(Vec<u8>, Qos)> {
        let mut deliveries = pin!(subscriber.stream());
        match poll!(deliveries.next()) {
            Poll::Ready(Some(Ok(delivery))) => Some((delivery.payload().to_vec(), delivery.qos())),
            Poll::Ready(other) => panic!("the stream ended or failed: {}", other.is_some()),
            Poll::Pending => None,
        }
    }

    #[test]
    fn a_delivery_arrives_at_the_lesser_of_the_subscription_and_the_publish_quality() {
        embassy_futures::block_on(async {
            let connected = connected().await;
            let mut exactly = MqttTopic::new("q/t")
                .qos(Qos::ExactlyOnce)
                .subscribe(&connected)
                .await
                .expect("the subscription opens");
            let mut at_most = MqttTopic::new("q/u")
                .qos(Qos::AtMostOnce)
                .subscribe(&connected)
                .await
                .expect("the subscription opens");
            let once = publisher(&connected, MqttPublish::default()).await;

            send(&once, "q/t", b"a").await;
            send(&once, "q/u", b"b").await;

            assert_eq!(
                next(&mut exactly).await,
                Some((b"a".to_vec(), Qos::AtLeastOnce))
            );
            assert_eq!(
                next(&mut at_most).await,
                Some((b"b".to_vec(), Qos::AtMostOnce))
            );
        });
    }

    #[test]
    fn a_retained_message_reaches_a_subscription_that_opens_later() {
        embassy_futures::block_on(async {
            let connected = connected().await;
            let retained = publisher(&connected, MqttPublish::default().retain(true)).await;
            send(&retained, "state/door", b"open").await;

            let mut late = MqttTopic::new("state/door")
                .subscribe(&connected)
                .await
                .expect("the subscription opens");
            assert_eq!(
                next(&mut late).await,
                Some((b"open".to_vec(), Qos::AtLeastOnce))
            );
        });
    }

    #[test]
    fn a_retained_message_is_replaced_by_the_next_one_and_cleared_by_an_empty_payload() {
        embassy_futures::block_on(async {
            let connected = connected().await;
            let retained = publisher(&connected, MqttPublish::default().retain(true)).await;
            send(&retained, "state/door", b"open").await;
            send(&retained, "state/door", b"closed").await;

            let mut late = MqttTopic::new("state/door")
                .subscribe(&connected)
                .await
                .expect("the subscription opens");
            assert_eq!(
                next(&mut late).await,
                Some((b"closed".to_vec(), Qos::AtLeastOnce))
            );
            assert_eq!(next(&mut late).await, None);

            send(&retained, "state/door", b"").await;
            let mut after = MqttTopic::new("state/door")
                .subscribe(&connected)
                .await
                .expect("the subscription opens");
            // The clearing publish reaches the first subscription, and the topic holds nothing.
            assert_eq!(next(&mut late).await, Some((Vec::new(), Qos::AtLeastOnce)));
            assert_eq!(next(&mut after).await, None);
        });
    }

    #[test]
    fn a_publish_that_is_not_retained_leaves_nothing_for_a_later_subscription() {
        embassy_futures::block_on(async {
            let connected = connected().await;
            let plain = publisher(&connected, MqttPublish::default()).await;
            send(&plain, "state/door", b"open").await;

            let mut late = MqttTopic::new("state/door")
                .subscribe(&connected)
                .await
                .expect("the subscription opens");
            assert_eq!(next(&mut late).await, None);
        });
    }

    #[test]
    fn a_retained_message_is_handed_over_at_the_lesser_quality_and_matched_by_the_filter() {
        embassy_futures::block_on(async {
            let connected = connected().await;
            let retained = publisher(
                &connected,
                MqttPublish::default().qos(Qos::ExactlyOnce).retain(true),
            )
            .await;
            send(&retained, "state/door", b"open").await;
            send(&retained, "state/window", b"shut").await;

            let mut all = MqttFilter::new("state/+")
                .qos(Qos::AtMostOnce)
                .subscribe(&connected)
                .await
                .expect("the subscription opens");
            assert_eq!(
                next(&mut all).await,
                Some((b"open".to_vec(), Qos::AtMostOnce))
            );
            assert_eq!(
                next(&mut all).await,
                Some((b"shut".to_vec(), Qos::AtMostOnce))
            );
        });
    }

    #[test]
    fn a_share_group_is_handed_no_retained_message() {
        embassy_futures::block_on(async {
            let connected = connected().await;
            let retained = publisher(&connected, MqttPublish::default().retain(true)).await;
            send(&retained, "state/door", b"open").await;

            let mut member = MqttTopic::new("state/door")
                .shared("g")
                .subscribe(&connected)
                .await
                .expect("the subscription opens");
            assert_eq!(next(&mut member).await, None);
        });
    }

    #[test]
    fn a_quality_zero_delivery_settles_as_unsupported() {
        embassy_futures::block_on(async {
            let connected = connected().await;
            let mut subscriber = MqttTopic::new("q/u")
                .qos(Qos::AtMostOnce)
                .subscribe(&connected)
                .await
                .expect("the subscription opens");
            let once = publisher(&connected, MqttPublish::default()).await;
            send(&once, "q/u", b"b").await;

            let mut deliveries = pin!(subscriber.stream());
            let Some(Ok(delivery)) = deliveries.next().await else {
                panic!("the delivery arrives");
            };
            assert!(matches!(delivery.ack().await, Err(AckError::Unsupported)));
        });
    }

    #[test]
    fn a_settlement_after_the_session_ended_is_refused() {
        embassy_futures::block_on(async {
            let connected = connected().await;
            let mut subscriber = MqttTopic::new("q/t")
                .subscribe(&connected)
                .await
                .expect("the subscription opens");
            let once = publisher(&connected, MqttPublish::default()).await;
            send(&once, "q/t", b"a").await;
            let delivery = {
                let mut deliveries = pin!(subscriber.stream());
                deliveries
                    .next()
                    .await
                    .expect("a delivery")
                    .expect("not an error")
            };

            connected
                .clone()
                .shutdown()
                .await
                .expect("the session ends");
            assert!(matches!(delivery.ack().await, Err(AckError::Broker(_))));
            // The publisher made before the shutdown errors rather than succeeding silently.
            assert!(
                once.publish(OutgoingMessage::new("q/t", b"late"), None)
                    .await
                    .is_err()
            );
            assert!(next_ended(&mut subscriber).await);
        });
    }

    async fn next_ended(subscriber: &mut MqttSubscriber) -> bool {
        let mut deliveries = pin!(subscriber.stream());
        matches!(poll!(deliveries.next()), Poll::Ready(None))
    }

    #[test]
    fn an_inject_reaches_only_the_subscriptions_its_topic_matches() {
        embassy_futures::block_on(async {
            let connected = connected().await;
            let mut hit = MqttFilter::new("a/+")
                .subscribe(&connected)
                .await
                .expect("the subscription opens");
            let mut miss = MqttFilter::new("b/+")
                .subscribe(&connected)
                .await
                .expect("the subscription opens");
            connected.inject(OutgoingMessage::new("a/1", b"x"));
            assert_eq!(
                next(&mut hit).await,
                Some((b"x".to_vec(), Qos::AtLeastOnce))
            );
            assert_eq!(next(&mut miss).await, None);
        });
    }
}
