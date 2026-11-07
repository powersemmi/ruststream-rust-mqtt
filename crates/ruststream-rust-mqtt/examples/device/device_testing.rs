//! Testing a device service on the host: the embedded `TestApp` connects the broker in process
//! over the memory the test declares, and the test drives the production app with the calls of
//! any other harness test.
//!
//! ```text
//! cd checks/noalloc && cargo test --example device_testing
//! ```

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use core::time::Duration;

use ruststream::runtime::{MountSteps, RETRY_COUNT_HEADER};
use ruststream::{Deserialized, Outgoing, Serialized};
use ruststream_rust_mqtt::prelude::*;

/// A job as the controller sends its bytes.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// What the board answers for a job, as the bytes that leave.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "jobs/done")]
struct Done(&'static [u8]);

/// Answers every job.
#[subscriber(MqttTopic::new("jobs/run"), reply)]
async fn run(job: &Job<'_>) -> Done {
    let _ = job.0;
    Done(b"done")
}

#[ruststream::app(runtime = embassy)]
fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| b.include(run).out_reply(MqttPublish::default()))
}

/// Asks for a job again in five seconds, once.
#[subscriber(MqttTopic::new("jobs/later"))]
async fn later(job: &Job<'_>, ctx: &mut Context<'_>) -> HandlerOutcome {
    let _ = job.0;
    match ctx.headers().get_str(RETRY_COUNT_HEADER) {
        Some(_) => HandlerOutcome::ack(),
        None => HandlerOutcome::retry_after(Duration::from_secs(5)),
    }
}

#[ruststream::app(runtime = embassy)]
fn retrying_app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| b.include(later).retry_capacity::<2>())
}

fn main() {}

#[cfg(test)]
mod tests {
    use embassy_futures::block_on;
    use ruststream::testing::Outcome;
    use ruststream::testing::embedded::TestApp;
    use ruststream_rust_mqtt::MqttState;
    use static_cell::ConstStaticCell;

    use super::*;

    /// What the controller publishes, to the topic it names.
    #[derive(Outgoing, Serialized)]
    struct Sent(&'static [u8]);

    /// The broker the production `main` builds, over the memory the test declares: the runner is
    /// not needed, because the in-process transport replaces the connection.
    fn board(memory: &'static mut MqttState) -> MqttBroker {
        let (broker, _runner) = MqttBroker::new(memory, MqttConfig::new("192.168.1.10:1883"));
        broker
    }

    // --8<-- [start:test]
    #[test]
    fn a_job_is_answered() {
        static MEMORY: ConstStaticCell<MqttState> = ConstStaticCell::new(MqttState::new());
        block_on(async {
            let tb = TestApp::start(app(board(MEMORY.take())))
                .await
                .expect("the app starts");
            let jobs = tb.broker::<MqttBroker>();
            jobs.message(&Sent(b"mix"))
                .to("jobs/run")
                .publish()
                .await
                .expect("the harness publishes");

            jobs.subscriber("jobs/run")
                .assert_called_once()
                .with_raw(b"mix")
                .settled(HandlerOutcome::ack());
            jobs.published::<()>("jobs/done")
                .assert_called_once()
                .with_raw(b"done");
            tb.shutdown().await.expect("the app stops");
        });
    }
    // --8<-- [end:test]

    // --8<-- [start:advance]
    #[test]
    fn a_job_is_asked_for_again_when_its_delay_has_passed() {
        static MEMORY: ConstStaticCell<MqttState> = ConstStaticCell::new(MqttState::new());
        block_on(async {
            let tb = TestApp::start(retrying_app(board(MEMORY.take())))
                .await
                .expect("the app starts");
            let jobs = tb.broker::<MqttBroker>();
            jobs.message(&Sent(b"mix"))
                .to("jobs/later")
                .publish()
                .await
                .expect("the harness publishes");

            tb.advance(Duration::from_secs(4))
                .await
                .expect("the clock moves");
            jobs.subscriber("jobs/later").assert_called_once();
            tb.advance(Duration::from_secs(1))
                .await
                .expect("the clock moves");
            let calls = jobs.subscriber("jobs/later").assert_called(2);
            assert_eq!(calls.outcomes().last(), Some(&Outcome::Ack));
            tb.shutdown().await.expect("the app stops");
        });
    }
    // --8<-- [end:advance]
}
