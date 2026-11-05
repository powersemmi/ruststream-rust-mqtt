//! A shared subscription: the server hands each message to one member of the group, so boards
//! that run the same service share the work.
//!
//! ```text
//! cargo test --example mqtt_subscriptions_shared --features testing
//! ```

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use ruststream::Outgoing;
use ruststream_rust_mqtt::prelude::*;
use serde::{Deserialize, Serialize};

/// A job for any board of the group.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "jobs/render")]
struct Job {
    id: u32,
}

// --8<-- [start:descriptor]
/// Every board runs this service; the server gives each job to one of them.
#[subscriber(MqttTopic::new("jobs/render").shared("renderers"))]
async fn render(job: &Job) -> HandlerOutcome {
    let _ = job.id;
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| b.include(render))
}
// --8<-- [end:descriptor]

fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use embassy_futures::block_on;
    use ruststream::testing::embedded::TestApp;

    use super::*;

    #[test]
    fn a_member_of_the_group_takes_the_jobs_sent_to_the_topic() -> Result<(), Box<dyn Error>> {
        block_on(async {
            let (broker, _runner) = MqttBroker::new(MqttConfig::new("192.168.1.10:1883"));
            let tb = TestApp::start(app(broker)).await?;
            let board = tb.broker::<MqttBroker>();
            board.message(&Job { id: 1 }).publish().await?;
            board.message(&Job { id: 2 }).publish().await?;

            board
                .subscriber("jobs/render")
                .assert_called(2)
                .with(&Job { id: 2 });
            tb.shutdown().await?;
            Ok(())
        })
    }
}
