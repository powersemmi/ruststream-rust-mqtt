//! What a device service publishes: a reply, an `Out` slot and its per-message options, and the
//! publishes the server refuses.

use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts, Out};
use ruststream::testing::embedded::TestApp;
use ruststream::{Deserialized, OutSlot, Outgoing, Publisher, Serialized, subscriber};
use ruststream_rust_mqtt::prelude::*;

use crate::support::state;

/// A command as its bytes.
#[derive(Deserialized)]
struct Command<'a>(&'a [u8]);

/// What the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// The state a valve reports, kept by the server for whoever subscribes next.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "valves/v1/state")]
struct State(Vec<u8>);

/// A report to a topic no publisher may name.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "valves/+/state")]
struct Wildcard(Vec<u8>);

/// A report to an empty topic.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "")]
struct Nameless(Vec<u8>);

/// A report to a topic with a control character in it.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "valves/\u{7}/state")]
struct Bell(Vec<u8>);

#[derive(OutSlot)]
#[publishes(State, Wildcard, Nameless, Bell)]
struct Reports;

fn broker() -> MqttBroker {
    let (broker, _runner) = MqttBroker::new(
        state(),
        MqttConfig::new("192.168.1.10:1883").client_id("valve-1"),
    );
    broker
}

/// Echoes the command as the valve's retained state.
#[subscriber(MqttTopic::new("valves/v1/set"), reply)]
async fn set(command: &Command<'_>) -> State {
    State(command.0.to_vec())
}

#[ruststream::app(runtime = embassy)]
fn retained_reply_app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| b.include(set).out_reply(MqttPublish::default()))
}

#[test]
fn a_reply_is_published_to_the_destination_its_type_names() {
    embassy_futures::block_on(async {
        let tb = TestApp::start(retained_reply_app(broker()))
            .await
            .expect("the app starts");
        tb.broker::<MqttBroker>()
            .message(&Sent(b"open"))
            .to("valves/v1/set")
            .publish()
            .await
            .expect("the harness publishes");

        tb.broker::<MqttBroker>()
            .published::<()>("valves/v1/state")
            .assert_called_once()
            .with_raw(b"open");
        tb.shutdown().await.expect("the app stops");
    });
}

/// Reports through the slot: what the command says decides which report goes out, and how.
#[subscriber(MqttTopic::new("valves/v1/report"))]
async fn report(
    command: &Command<'_>,
    Out(reports): Out<impl Publisher<Options = MqttPublishOptions>, Reports>,
) -> HandlerOutcome {
    let body = command.0.to_vec();
    let sent = match command.0 {
        b"retained" => reports
            .message(&State(body))
            .retain(true)
            .publish()
            .await
            .map_err(|error| error.unsent().is_some()),
        b"exactly-once" => reports
            .message(&State(body))
            .qos(Qos::ExactlyOnce)
            .retain(true)
            .publish()
            .await
            .map_err(|error| error.unsent().is_some()),
        b"plain" => reports
            .message(&State(body))
            .publish()
            .await
            .map_err(|error| error.unsent().is_some()),
        b"wildcard" => reports
            .message(&Wildcard(body))
            .publish()
            .await
            .map_err(|error| error.unsent().is_some()),
        b"nameless" => reports
            .message(&Nameless(body))
            .publish()
            .await
            .map_err(|error| error.unsent().is_some()),
        _ => reports
            .message(&Bell(body))
            .publish()
            .await
            .map_err(|error| error.unsent().is_some()),
    };
    match sent {
        Ok(()) => HandlerOutcome::ack(),
        // A refusal that hands the message back is dropped; one that does not is retried.
        Err(true) => HandlerOutcome::drop(),
        Err(false) => HandlerOutcome::retry(),
    }
}

#[ruststream::app(runtime = embassy)]
fn slot_app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| {
        b.include(report).out(Reports, MqttPublish::default())
    })
}

async fn command(tb: &TestApp, body: &'static [u8]) {
    tb.broker::<MqttBroker>()
        .message(&Sent(body))
        .to("valves/v1/report")
        .publish()
        .await
        .expect("the harness publishes");
}

#[test]
fn a_step_sets_the_options_of_one_publish() {
    embassy_futures::block_on(async {
        let tb = TestApp::start(slot_app(broker()))
            .await
            .expect("the app starts");
        command(&tb, b"retained").await;
        tb.out::<Reports>()
            .assert_called_once()
            .with_raw(b"retained")
            .with_options(&MqttPublishOptions::default().retain(true));

        command(&tb, b"exactly-once").await;
        tb.out::<Reports>()
            .assert_called(2)
            .with_raw(b"exactly-once")
            .with_options(
                &MqttPublishOptions::default()
                    .qos(Qos::ExactlyOnce)
                    .retain(true),
            );

        command(&tb, b"plain").await;
        tb.out::<Reports>()
            .assert_called(3)
            .with_raw(b"plain")
            .assert_options_default();
        tb.shutdown().await.expect("the app stops");
    });
}

#[test]
fn a_topic_no_publisher_may_name_is_refused_with_the_message_handed_back() {
    embassy_futures::block_on(async {
        let tb = TestApp::start(slot_app(broker()))
            .await
            .expect("the app starts");
        for body in [&b"wildcard"[..], b"nameless", b"bell"] {
            command(&tb, body).await;
            tb.broker::<MqttBroker>()
                .subscriber("valves/v1/report")
                .settled(HandlerOutcome::drop());
        }
        tb.broker::<MqttBroker>()
            .subscriber("valves/v1/report")
            .assert_called(3);
        tb.shutdown().await.expect("the app stops");
    });
}
