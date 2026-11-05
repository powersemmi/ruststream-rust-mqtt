//! Testing a device service on the host: the embedded `TestApp` connects the broker in process,
//! and the test drives the app with the calls of any other harness test.
//!
//! ```text
//! cargo test --example mqtt_testing_embedded --features testing
//! ```

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use core::time::Duration;

use ruststream::Outgoing;
use ruststream::runtime::{MountSteps, RETRY_COUNT_HEADER};
use ruststream_rust_mqtt::prelude::*;
use serde::{Deserialize, Serialize};

/// A job for the board. The sender names the topic.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Job {
    id: u32,
}

/// What the board answers for a job.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "jobs/done")]
struct Done {
    id: u32,
}

/// Answers every job.
#[subscriber(MqttTopic::new("jobs/run"), reply)]
async fn run(job: &Job) -> Done {
    Done { id: job.id }
}

#[ruststream::app(runtime = embassy)]
fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| b.include(run).out_reply(MqttPublish::default()))
}

/// Asks for a job again in five seconds, once.
#[subscriber(MqttTopic::new("jobs/later"))]
async fn later(job: &Job, ctx: &mut Context<'_>) -> HandlerOutcome {
    let _ = job.id;
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
    use std::error::Error;
    use std::time::Duration;

    use embassy_futures::block_on;
    use ruststream::testing::Outcome;
    use ruststream::testing::embedded::TestApp;

    use super::*;

    fn board() -> MqttBroker {
        let (broker, _runner) = MqttBroker::new(MqttConfig::new("192.168.1.10:1883"));
        broker
    }

    // --8<-- [start:test]
    #[test]
    fn a_job_is_answered() -> Result<(), Box<dyn Error>> {
        block_on(async {
            let tb = TestApp::start(app(board())).await?;
            let jobs = tb.broker::<MqttBroker>();
            jobs.message(&Job { id: 7 })
                .to("jobs/run")
                .publish()
                .await?;

            jobs.subscriber("jobs/run")
                .assert_called_once()
                .with(&Job { id: 7 })
                .settled(HandlerOutcome::ack());
            jobs.published::<Done>("jobs/done")
                .assert_called_once()
                .with(&Done { id: 7 });
            tb.shutdown().await?;
            Ok(())
        })
    }
    // --8<-- [end:test]

    // --8<-- [start:advance]
    #[test]
    fn a_job_is_asked_for_again_when_its_delay_has_passed() -> Result<(), Box<dyn Error>> {
        block_on(async {
            let tb = TestApp::start(retrying_app(board())).await?;
            let jobs = tb.broker::<MqttBroker>();
            jobs.message(&Job { id: 7 })
                .to("jobs/later")
                .publish()
                .await?;

            tb.advance(Duration::from_secs(4)).await?;
            jobs.subscriber("jobs/later").assert_called_once();
            tb.advance(Duration::from_secs(1)).await?;
            let calls = jobs.subscriber("jobs/later").assert_called(2);
            assert_eq!(calls.outcomes().last(), Some(&Outcome::Ack));
            tb.shutdown().await?;
            Ok(())
        })
    }
    // --8<-- [end:advance]
}
