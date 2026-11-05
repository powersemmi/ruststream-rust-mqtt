//! Testing a service on a compio host: the compio `TestApp` runs the app on the test's own compio
//! runtime, with the broker connected in process.
//!
//! ```text
//! cargo test --example mqtt_testing_compio --features testing,compio
//! ```

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use ruststream::Outgoing;
use ruststream::runtime::compio::{App, AppInfo, RustStream};
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

#[ruststream::app(runtime = compio, cores = 1)]
fn app() -> impl App {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(
        MqttBroker::compio(MqttConfig::new("mqtt://localhost:1883")),
        |b| b.include(run).out_reply(MqttPublish::default()),
    )
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use compio::runtime::Runtime;
    use ruststream::testing::compio::TestApp;

    use super::*;

    // --8<-- [start:test]
    #[test]
    fn a_job_is_answered_on_compio() -> Result<(), Box<dyn Error>> {
        Runtime::new()?.block_on(async {
            let tb = TestApp::start(app()).await?;
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
}
