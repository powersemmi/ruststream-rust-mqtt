//! A host service on the compio runtime, run by the compio `TestApp` with the broker connected in
//! process.

#![cfg(feature = "compio")]

use core::time::Duration;

use compio::runtime::Runtime;
use ruststream::runtime::compio::{AppInfo, RustStream};
use ruststream::runtime::{HandlerOutcome, Out, RETRY_COUNT_HEADER};
use ruststream::testing::Outcome;
use ruststream::testing::compio::TestApp;
use ruststream::{Deserialized, OutSlot, Outgoing, Publisher, Serialized, subscriber};
use ruststream_rust_mqtt::prelude::*;

/// A level as its bytes.
#[derive(Deserialized)]
struct Level<'a>(&'a [u8]);

/// What the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// What the display shows.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "display")]
struct Shown(Vec<u8>);

/// The alarm a full tank raises.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "alarms")]
struct Alarm(Vec<u8>);

#[derive(OutSlot)]
#[publishes(Alarm)]
struct Alarms;

fn broker() -> MqttBroker {
    MqttBroker::compio(MqttConfig::new("127.0.0.1:1883").client_id("tank-1"))
}

/// Shows every level, in percent.
#[subscriber(MqttTopic::new("tank/level"), reply)]
async fn show(level: &Level<'_>) -> Shown {
    Shown([level.0, b" %"].concat())
}

/// Raises a retained alarm for every full tank.
#[subscriber(MqttFilter::new("tank/+/full"))]
async fn watch(
    level: &Level<'_>,
    Out(alarms): Out<impl Publisher<Options = MqttPublishOptions>, Alarms>,
) -> HandlerOutcome {
    match alarms
        .message(&Alarm(level.0.to_vec()))
        .retain(true)
        .publish()
        .await
    {
        Ok(()) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::retry(),
    }
}

/// Asks for a retry on a first delivery and acknowledges the copy that carries its retry count.
#[subscriber(MqttTopic::new("tank/refill"))]
async fn refill(level: &Level<'_>, ctx: &mut Context<'_>) -> HandlerOutcome {
    let _ = level.0;
    match ctx.headers().get_str(RETRY_COUNT_HEADER) {
        Some("1") => HandlerOutcome::ack(),
        _ => HandlerOutcome::retry_after(Duration::from_secs(5)),
    }
}

fn app() -> RustStream {
    RustStream::new(AppInfo::new("tank", "0.1.0")).with_broker(broker(), |b| {
        b.include(show)
            .out_reply(MqttPublish::default())
            .include(watch)
            .out(Alarms, MqttPublish::default())
            .out_retry(MqttPublish::default())
            .to("tank/retries")
            .include(refill)
            .out_retry(MqttPublish::default())
    })
}

#[test]
fn a_delivery_reaches_its_handler_and_is_acknowledged() {
    Runtime::new()
        .expect("a compio runtime starts")
        .block_on(async {
            let tb = TestApp::start(app()).await.expect("the app starts");
            tb.broker::<MqttBroker>()
                .message(&Sent(b"80"))
                .to("tank/level")
                .publish()
                .await
                .expect("the harness publishes");
            tb.broker::<MqttBroker>()
                .subscriber("tank/level")
                .assert_called_once()
                .with_raw(b"80")
                .settled(HandlerOutcome::ack());
            tb.shutdown().await.expect("the app stops");
        });
}

#[test]
fn a_reply_reaches_its_destination() {
    Runtime::new()
        .expect("a compio runtime starts")
        .block_on(async {
            let tb = TestApp::start(app()).await.expect("the app starts");
            tb.broker::<MqttBroker>()
                .message(&Sent(b"80"))
                .to("tank/level")
                .publish()
                .await
                .expect("the harness publishes");
            tb.broker::<MqttBroker>()
                .published::<()>("display")
                .assert_called_once()
                .with_raw(b"80 %");
            tb.shutdown().await.expect("the app stops");
        });
}

#[test]
fn a_slot_publish_carries_the_options_its_step_set() {
    Runtime::new()
        .expect("a compio runtime starts")
        .block_on(async {
            let tb = TestApp::start(app()).await.expect("the app starts");
            tb.broker::<MqttBroker>()
                .message(&Sent(b"full"))
                .to("tank/3/full")
                .publish()
                .await
                .expect("the harness publishes");
            tb.out::<Alarms>()
                .assert_called_once()
                .with_raw(b"full")
                .with_options(&MqttPublishOptions::default().retain(true));
            tb.shutdown().await.expect("the app stops");
        });
}

#[test]
fn a_retry_is_redelivered_only_once_its_delay_has_passed() {
    Runtime::new()
        .expect("a compio runtime starts")
        .block_on(async {
            let tb = TestApp::start(app()).await.expect("the app starts");
            tb.broker::<MqttBroker>()
                .message(&Sent(b"job"))
                .to("tank/refill")
                .publish()
                .await
                .expect("the harness publishes");

            tb.advance(Duration::from_secs(4))
                .await
                .expect("the clock moves");
            tb.broker::<MqttBroker>()
                .subscriber("tank/refill")
                .assert_called_once()
                .settled(HandlerOutcome::retry_after(Duration::from_secs(5)));

            tb.advance(Duration::from_secs(1))
                .await
                .expect("the clock moves");
            let broker = tb.broker::<MqttBroker>();
            let calls = broker.subscriber("tank/refill").assert_called(2);
            assert_eq!(calls.outcomes().last(), Some(&Outcome::Ack));
            tb.shutdown().await.expect("the app stops");
        });
}
