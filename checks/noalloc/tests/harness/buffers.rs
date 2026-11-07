//! The memory of the broker: receive buffers come back when a delivery is settled, a publish that
//! does not fit the packet size is refused with its message handed back, and every subscription a
//! publish matches receives its own copy of the bytes.

use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts, Out};
use ruststream::testing::Outcome;
use ruststream::testing::embedded::TestApp;
use ruststream::{Deserialized, OutSlot, Outgoing, Publisher, Serialized, subscriber};
use ruststream_rust_mqtt::MqttState;
use ruststream_rust_mqtt::prelude::*;

/// A frame as its bytes, borrowed from the receive buffer.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

/// What the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// What a handler saw, sent on to a topic the test reads from the publish log.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "seen/exact")]
struct SeenExact(Vec<u8>);

#[derive(Outgoing, Serialized)]
#[outgoing(name = "seen/filter")]
struct SeenFilter(Vec<u8>);

/// A broker with the default memory: four receive buffers of up to 1 KiB.
fn broker() -> MqttBroker {
    let state = Box::leak(Box::new(MqttState::new()));
    let (broker, _runner) = MqttBroker::new(state, MqttConfig::new("10.0.0.2:1883"));
    broker
}

#[subscriber(MqttTopic::new("lane/a"), reply)]
async fn on_exact(frame: &Frame<'_>) -> SeenExact {
    SeenExact(frame.0.to_vec())
}

#[subscriber(MqttFilter::new("lane/#"), reply)]
async fn on_filter(frame: &Frame<'_>) -> SeenFilter {
    SeenFilter(frame.0.to_vec())
}

#[ruststream::app(runtime = embassy)]
fn lane_app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| {
        b.include(on_exact)
            .out_reply(MqttPublish::default())
            .include(on_filter)
            .out_reply(MqttPublish::default())
    })
}

/// How many publishes the run makes: ten times the broker's four receive buffers, and each
/// publish takes two of them while its handlers run.
const RUN: usize = 40;

#[test]
fn a_run_of_many_times_the_buffers_is_delivered_because_settled_buffers_come_back() {
    embassy_futures::block_on(async {
        let tb = TestApp::start(lane_app(broker()))
            .await
            .expect("the app starts");
        let mqtt = tb.broker::<MqttBroker>();
        for _ in 0..RUN {
            mqtt.message(&Sent(b"tick"))
                .to("lane/a")
                .publish()
                .await
                .expect("the harness publishes");
        }
        // The topic and the filter both match every publish, and each takes its own copy.
        mqtt.subscriber("lane/a").assert_called(RUN);
        mqtt.subscriber("lane/#").assert_called(RUN);
        mqtt.published::<()>("seen/exact").assert_called(RUN);
        mqtt.published::<()>("seen/filter").assert_called(RUN);
        assert_eq!(mqtt.subscriber("lane/a").outcomes(), [Outcome::Ack; RUN]);
        tb.shutdown().await.expect("the app stops");
    });
}

#[test]
fn a_filter_matches_a_topic_the_exact_subscription_does_not() {
    embassy_futures::block_on(async {
        let tb = TestApp::start(lane_app(broker()))
            .await
            .expect("the app starts");
        let mqtt = tb.broker::<MqttBroker>();
        mqtt.message(&Sent(b"a"))
            .to("lane/a")
            .publish()
            .await
            .expect("the harness publishes");
        mqtt.message(&Sent(b"b"))
            .to("lane/b")
            .publish()
            .await
            .expect("the harness publishes");
        mqtt.subscriber("lane/a")
            .assert_called_once()
            .with_raw(b"a");
        mqtt.subscriber("lane/#").assert_called(2);
        tb.shutdown().await.expect("the app stops");
    });
}

/// Payloads of every shape reach the handler as the bytes published: empty, zero bytes, all byte
/// values and a payload that fills the packet.
#[test]
fn a_payload_is_the_bytes_published() {
    static ALL: [u8; 256] = {
        let mut bytes = [0u8; 256];
        let mut at = 0;
        while at < 256 {
            bytes[at] = at as u8;
            at += 1;
        }
        bytes
    };
    static BIG: [u8; 900] = [0xA5; 900];
    embassy_futures::block_on(async {
        let tb = TestApp::start(lane_app(broker()))
            .await
            .expect("the app starts");
        let mqtt = tb.broker::<MqttBroker>();
        for payload in [&b""[..], b"\0\0\0", &ALL, &BIG] {
            mqtt.message(&Sent(payload))
                .to("lane/a")
                .publish()
                .await
                .expect("the harness publishes");
        }
        let seen = mqtt.published::<()>("seen/exact");
        seen.assert_called(4);
        let log = mqtt.published::<()>("seen/filter").assert_called(4);
        let payloads: Vec<Vec<u8>> = log
            .messages()
            .iter()
            .map(|message| message.payload().to_vec())
            .collect();
        assert_eq!(
            payloads,
            [Vec::new(), vec![0, 0, 0], ALL.to_vec(), BIG.to_vec()]
        );
        tb.shutdown().await.expect("the app stops");
    });
}

/// A reply to a topic every request names, sized by the request.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "size/out")]
struct Answer(Vec<u8>);

#[derive(OutSlot)]
#[publishes(Answer)]
struct Answers;

/// Publishes as many bytes as the request says, and drops the request when the broker refuses
/// the publish and hands the message back.
#[subscriber(MqttTopic::new("size/ask"))]
async fn answer(
    ask: &Frame<'_>,
    Out(answers): Out<impl Publisher<Options = MqttPublishOptions>, Answers>,
) -> HandlerOutcome {
    let wanted = core::str::from_utf8(ask.0)
        .ok()
        .and_then(|text| text.parse::<usize>().ok())
        .unwrap_or(0);
    match answers.message(&Answer(vec![7; wanted])).publish().await {
        Ok(()) => HandlerOutcome::ack(),
        Err(error) if error.unsent().is_some() => HandlerOutcome::drop(),
        Err(_) => HandlerOutcome::retry(),
    }
}

/// The memory of a small device: four buffers of 64 bytes.
type Small = MqttBroker<4, 64, 8>;

fn small() -> Small {
    let state = Box::leak(Box::new(MqttState::<4, 64, 8>::new()));
    let (broker, _runner) = MqttBroker::new(state, MqttConfig::new("10.0.0.2:1883"));
    broker
}

#[ruststream::app(runtime = embassy)]
fn small_app(broker: Small) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| {
        b.include(answer).out(Answers, MqttPublish::default())
    })
}

#[test]
fn a_publish_larger_than_the_packet_is_refused_and_hands_its_message_back() {
    embassy_futures::block_on(async {
        let tb = TestApp::start(small_app(small()))
            .await
            .expect("the app starts");
        let mqtt = tb.broker::<Small>();
        // The largest payload the packet holds is found by asking: the sizes that fit are the
        // prefix of the run, the sizes that do not are refused and dropped.
        let mut limit = None;
        let mut refused_from = None;
        for size in 0..=70usize {
            let before = mqtt.published::<()>("size/out").messages().len();
            mqtt.message(&Sent(Box::leak(
                size.to_string().into_bytes().into_boxed_slice(),
            )))
            .to("size/ask")
            .publish()
            .await
            .expect("the harness publishes");
            let took = mqtt.published::<()>("size/out").messages().len() > before;
            let last = *mqtt
                .subscriber("size/ask")
                .outcomes()
                .last()
                .expect("the request was settled");
            if took {
                assert!(
                    refused_from.is_none(),
                    "{size} bytes fit after a smaller refusal"
                );
                assert_eq!(last, Outcome::Ack, "a publish of {size} bytes was taken");
                limit = Some(size);
            } else {
                assert_eq!(last, Outcome::Drop, "a publish of {size} bytes was refused");
                refused_from.get_or_insert(size);
            }
        }
        let limit = limit.expect("some payload fits");
        assert_eq!(refused_from, Some(limit + 1));
        // The packet holds the fixed header, the topic and the properties besides the payload.
        assert!((40..64).contains(&limit), "the limit is {limit}");
        let taken = mqtt.published::<()>("size/out").messages().len();
        assert_eq!(taken, limit + 1, "sizes 0 through the limit were taken");
        tb.shutdown().await.expect("the app stops");
    });
}

#[test]
#[should_panic(expected = "is not one a server takes")]
fn the_harness_refuses_to_inject_a_message_larger_than_the_packet() {
    embassy_futures::block_on(async {
        let tb = TestApp::start(small_app(small()))
            .await
            .expect("the app starts");
        static LARGE: [u8; 200] = [1; 200];
        let _ = tb
            .broker::<Small>()
            .message(&Sent(&LARGE))
            .to("size/ask")
            .publish()
            .await;
    });
}
