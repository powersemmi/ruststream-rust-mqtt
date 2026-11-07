//! A deferred retry of a device service's handler: the copy goes back to the topic of an
//! `MqttTopic` subscription, and to the topic the mount names for an `MqttFilter`.

use core::time::Duration;

use ruststream::runtime::{EmbeddedApp, HandlerOutcome, MountSteps, Mounts, RETRY_COUNT_HEADER};
use ruststream::testing::Outcome;
use ruststream::testing::embedded::TestApp;
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};
use ruststream_rust_mqtt::prelude::*;

use crate::support::state;

/// A job as its bytes.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// What the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// The delay the handler asks its retry for.
const LATER: Duration = Duration::from_secs(5);

fn broker() -> MqttBroker {
    let (broker, _runner) = MqttBroker::new(
        state(),
        MqttConfig::new("192.168.1.10:1883").client_id("worker-1"),
    );
    broker
}

/// Asks for a retry on a first delivery and acknowledges the copy that carries its retry count.
#[subscriber(MqttTopic::new("jobs/run"))]
async fn on_topic(job: &Job<'_>, ctx: &mut Context<'_>) -> HandlerOutcome {
    let _ = job.0;
    match ctx.headers().get_str(RETRY_COUNT_HEADER) {
        Some("1") => HandlerOutcome::ack(),
        _ => HandlerOutcome::retry_after(LATER),
    }
}

#[ruststream::app(runtime = embassy)]
fn topic_app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| {
        b.include(on_topic)
            .out_retry(MqttPublish::default())
            .retry_capacity::<1>()
    })
}

#[test]
fn a_topic_subscription_gets_its_copy_back_on_its_own_topic_after_the_delay() {
    embassy_futures::block_on(async {
        let tb = TestApp::start(topic_app(broker()))
            .await
            .expect("the app starts");
        tb.broker::<MqttBroker>()
            .message(&Sent(b"job"))
            .to("jobs/run")
            .publish()
            .await
            .expect("the harness publishes");

        tb.advance(Duration::from_secs(4))
            .await
            .expect("the clock moves");
        tb.broker::<MqttBroker>()
            .subscriber("jobs/run")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(LATER));

        tb.advance(Duration::from_secs(1))
            .await
            .expect("the clock moves");
        let jobs = tb.broker::<MqttBroker>();
        let calls = jobs.subscriber("jobs/run").assert_called(2);
        assert_eq!(
            calls.outcomes().last(),
            Some(&Outcome::Ack),
            "the copy carries its retry count to the handler"
        );
        // The copy is a publish to the topic the descriptor reported.
        jobs.published::<()>("jobs/run").assert_called(2);
        tb.shutdown().await.expect("the app stops");
    });
}

/// Asks for a retry on a first delivery, as [`on_topic`] does, on a wildcard filter.
#[subscriber(MqttFilter::new("jobs/+/run"))]
async fn on_filter(job: &Job<'_>, ctx: &mut Context<'_>) -> HandlerOutcome {
    let _ = job.0;
    match ctx.headers().get_str(RETRY_COUNT_HEADER) {
        Some("1") => HandlerOutcome::ack(),
        _ => HandlerOutcome::retry_after(LATER),
    }
}

#[ruststream::app(runtime = embassy)]
fn filter_app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| {
        b.include(on_filter)
            .out_retry(MqttPublish::default())
            .to("jobs/again/run")
            .retry_capacity::<1>()
    })
}

#[test]
fn a_filter_subscription_gets_its_copy_on_the_topic_the_mount_names() {
    embassy_futures::block_on(async {
        let tb = TestApp::start(filter_app(broker()))
            .await
            .expect("the app starts");
        tb.broker::<MqttBroker>()
            .message(&Sent(b"job"))
            .to("jobs/first/run")
            .publish()
            .await
            .expect("the harness publishes");

        tb.advance(Duration::from_secs(5))
            .await
            .expect("the clock moves");
        let jobs = tb.broker::<MqttBroker>();
        jobs.subscriber("jobs/+/run").assert_called(2);
        jobs.published::<()>("jobs/again/run")
            .assert_called_once()
            .with_raw(b"job");
        tb.shutdown().await.expect("the app stops");
    });
}

/// Asks for the delivery to be put back.
#[subscriber(MqttTopic::new("jobs/requeue"))]
async fn on_requeue(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    HandlerOutcome::retry()
}

#[ruststream::app(runtime = embassy)]
fn requeue_app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| b.include(on_requeue))
}

#[test]
fn a_requeue_is_unsupported_and_the_harness_accepts_it() {
    embassy_futures::block_on(async {
        let tb = TestApp::start(requeue_app(broker()))
            .await
            .expect("the app starts");
        tb.broker::<MqttBroker>()
            .message(&Sent(b"job"))
            .to("jobs/requeue")
            .publish()
            .await
            .expect("the harness publishes");
        // The transport cannot put a delivery back: the message stays unacknowledged, and the
        // settlement answers `Unsupported`, which the harness accepts.
        tb.broker::<MqttBroker>()
            .subscriber("jobs/requeue")
            .assert_called_once()
            .settled(HandlerOutcome::retry());
        tb.shutdown().await.expect("the app stops");
    });
}
