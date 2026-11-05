//! Which subscription a publish reaches: topics, wildcard filters, `$` topics, share groups and
//! one filter mounted twice, with the quality of service each delivery arrives at.

use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts};
use ruststream::testing::Outcome;
use ruststream::testing::embedded::TestApp;
use ruststream::{Deserialized, Outgoing, RawMessage, Serialized, subscriber};
use ruststream_rust_mqtt::{MqttBroker, MqttConfig, MqttFilter, MqttPublish, MqttTopic, Qos};

/// A frame as its bytes.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

/// Which handler took a delivery, and what it carried.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "took/a")]
struct TookA(Vec<u8>);

#[derive(Outgoing, Serialized)]
#[outgoing(name = "took/b")]
struct TookB(Vec<u8>);

#[derive(Outgoing, Serialized)]
#[outgoing(name = "took/c")]
struct TookC(Vec<u8>);

/// What the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// The payloads of the published messages, in publish order.
fn payloads(messages: &[RawMessage]) -> Vec<Vec<u8>> {
    messages
        .iter()
        .map(|message| message.payload().to_vec())
        .collect()
}

fn broker() -> MqttBroker {
    let (broker, _runner) =
        MqttBroker::new(MqttConfig::new("192.168.1.10:1883").client_id("router-1"));
    broker
}

#[subscriber(MqttTopic::new("plant/line1/temp"), reply)]
async fn on_topic(frame: &Frame<'_>) -> TookA {
    TookA(frame.0.to_vec())
}

#[subscriber(MqttFilter::new("plant/+/temp"), reply)]
async fn on_plus(frame: &Frame<'_>) -> TookB {
    TookB(frame.0.to_vec())
}

#[subscriber(MqttFilter::new("plant/#"), reply)]
async fn on_hash(frame: &Frame<'_>) -> TookC {
    TookC(frame.0.to_vec())
}

#[ruststream::app(runtime = embassy)]
fn plain_app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| {
        b.include(on_topic)
            .out_reply(MqttPublish::default())
            .include(on_plus)
            .out_reply(MqttPublish::default())
            .include(on_hash)
            .out_reply(MqttPublish::default())
    })
}

#[test]
fn a_publish_reaches_every_subscription_whose_filter_matches_its_topic() {
    embassy_futures::block_on(async {
        let tb = TestApp::start(plain_app(broker()))
            .await
            .expect("the app starts");
        let mqtt = tb.broker::<MqttBroker>();
        mqtt.message(&Sent(b"21"))
            .to("plant/line1/temp")
            .publish()
            .await
            .expect("the harness publishes");
        mqtt.message(&Sent(b"7"))
            .to("plant/line2/temp")
            .publish()
            .await
            .expect("the harness publishes");
        mqtt.message(&Sent(b"x"))
            .to("plant/line2/humidity/raw")
            .publish()
            .await
            .expect("the harness publishes");

        mqtt.published::<()>("took/a")
            .assert_called_once()
            .with_raw(b"21");
        mqtt.published::<()>("took/b")
            .assert_called(2)
            .with_raw(b"7");
        mqtt.published::<()>("took/c")
            .assert_called(3)
            .with_raw(b"x");
        tb.shutdown().await.expect("the app stops");
    });
}

#[subscriber(MqttFilter::new("#"))]
async fn on_everything(frame: &Frame<'_>) -> HandlerOutcome {
    let _ = frame.0;
    HandlerOutcome::ack()
}

#[subscriber(MqttFilter::new("$SYS/#"))]
async fn on_system(frame: &Frame<'_>) -> HandlerOutcome {
    let _ = frame.0;
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn system_app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| b.include(on_everything).include(on_system))
}

#[test]
fn a_dollar_topic_is_reached_only_by_a_dollar_filter() {
    embassy_futures::block_on(async {
        let tb = TestApp::start(system_app(broker()))
            .await
            .expect("the app starts");
        let mqtt = tb.broker::<MqttBroker>();
        mqtt.message(&Sent(b"up"))
            .to("$SYS/broker/uptime")
            .publish()
            .await
            .expect("the harness publishes");
        mqtt.subscriber("$SYS/#")
            .assert_called_once()
            .with_raw(b"up");
        mqtt.subscriber("#").assert_not_called();

        mqtt.message(&Sent(b"on"))
            .to("lamps/1")
            .publish()
            .await
            .expect("the harness publishes");
        mqtt.subscriber("#").assert_called_once().with_raw(b"on");
        mqtt.subscriber("$SYS/#").assert_called_once();
        tb.shutdown().await.expect("the app stops");
    });
}

#[subscriber(MqttTopic::new("jobs").shared("workers"), reply)]
async fn worker_a(frame: &Frame<'_>) -> TookA {
    TookA(frame.0.to_vec())
}

#[subscriber(MqttTopic::new("jobs").shared("workers"), reply)]
async fn worker_b(frame: &Frame<'_>) -> TookB {
    TookB(frame.0.to_vec())
}

#[ruststream::app(runtime = embassy)]
fn group_app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| {
        b.include(worker_a)
            .out_reply(MqttPublish::default())
            .include(worker_b)
            .out_reply(MqttPublish::default())
    })
}

#[test]
fn the_members_of_a_share_group_take_the_deliveries_in_turn() {
    embassy_futures::block_on(async {
        let tb = TestApp::start(group_app(broker()))
            .await
            .expect("the app starts");
        let mqtt = tb.broker::<MqttBroker>();
        for body in [b"1", b"2", b"3", b"4", b"5"] {
            mqtt.message(&Sent(body))
                .to("jobs")
                .publish()
                .await
                .expect("the harness publishes");
        }

        // The group is one subscription on the server, so each publish goes to one member.
        let first = mqtt.published::<()>("took/a");
        let second = mqtt.published::<()>("took/b");
        assert_eq!(first.messages().len(), 3);
        assert_eq!(second.messages().len(), 2);
        assert_eq!(
            payloads(first.messages()),
            [b"1".to_vec(), b"3".to_vec(), b"5".to_vec()]
        );
        assert_eq!(payloads(second.messages()), [b"2".to_vec(), b"4".to_vec()]);
        tb.shutdown().await.expect("the app stops");
    });
}

#[subscriber(MqttFilter::new("jobs/+").shared("workers"))]
async fn group_member_a(frame: &Frame<'_>) -> HandlerOutcome {
    let _ = frame.0;
    HandlerOutcome::ack()
}

#[subscriber(MqttFilter::new("jobs/+").shared("workers"))]
async fn group_member_b(frame: &Frame<'_>) -> HandlerOutcome {
    let _ = frame.0;
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn group_filter_app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| {
        b.include(group_member_a).include(group_member_b)
    })
}

#[test]
fn a_share_group_on_a_wildcard_filter_matches_by_the_filter_behind_the_group() {
    embassy_futures::block_on(async {
        let tb = TestApp::start(group_filter_app(broker()))
            .await
            .expect("the app starts");
        let mqtt = tb.broker::<MqttBroker>();
        for body in [b"1", b"2", b"3"] {
            mqtt.message(&Sent(body))
                .to("jobs/42")
                .publish()
                .await
                .expect("the harness publishes");
        }
        // Both mounts carry one name; the group's three deliveries are all handled once.
        mqtt.subscriber("jobs/+").assert_called(3);
        tb.shutdown().await.expect("the app stops");
    });
}

#[subscriber(MqttTopic::new("echo"), reply)]
async fn echo_first(frame: &Frame<'_>) -> TookA {
    TookA(frame.0.to_vec())
}

#[subscriber(MqttTopic::new("echo"), reply)]
async fn echo_second(frame: &Frame<'_>) -> TookB {
    TookB(frame.0.to_vec())
}

#[ruststream::app(runtime = embassy)]
fn twice_app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| {
        b.include(echo_first)
            .out_reply(MqttPublish::default())
            .include(echo_second)
            .out_reply(MqttPublish::default())
    })
}

#[test]
fn one_filter_mounted_twice_is_one_subscription_whose_mounts_take_turns() {
    embassy_futures::block_on(async {
        let tb = TestApp::start(twice_app(broker()))
            .await
            .expect("the app starts");
        let mqtt = tb.broker::<MqttBroker>();
        for body in [b"1", b"2", b"3", b"4"] {
            mqtt.message(&Sent(body))
                .to("echo")
                .publish()
                .await
                .expect("the harness publishes");
        }
        // The server holds one subscription per filter, so the mounts share it rather than
        // each receiving every message.
        mqtt.published::<()>("took/a").assert_called(2);
        mqtt.published::<()>("took/b").assert_called(2);
        tb.shutdown().await.expect("the app stops");
    });
}

#[subscriber(MqttTopic::new("levels/never").qos(Qos::AtMostOnce))]
async fn never(frame: &Frame<'_>) -> HandlerOutcome {
    let _ = frame.0;
    HandlerOutcome::ack()
}

#[subscriber(MqttTopic::new("levels/once").qos(Qos::AtLeastOnce))]
async fn once(frame: &Frame<'_>) -> HandlerOutcome {
    let _ = frame.0;
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn qos_app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| b.include(never).include(once))
}

#[test]
fn a_quality_of_service_zero_delivery_settles_as_unsupported_and_the_harness_accepts_it() {
    embassy_futures::block_on(async {
        let tb = TestApp::start(qos_app(broker()))
            .await
            .expect("the app starts");
        let mqtt = tb.broker::<MqttBroker>();
        // The harness publishes at quality 1, so a subscription at 0 is delivered at 0.
        mqtt.message(&Sent(b"a"))
            .to("levels/never")
            .publish()
            .await
            .expect("the harness publishes");
        mqtt.message(&Sent(b"b"))
            .to("levels/once")
            .publish()
            .await
            .expect("the harness publishes");
        mqtt.subscriber("levels/never")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        mqtt.subscriber("levels/once")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        assert_eq!(mqtt.subscriber("levels/never").outcomes(), [Outcome::Ack]);
        tb.shutdown().await.expect("the app stops");
    });
}
