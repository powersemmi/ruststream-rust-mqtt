//! One filter mounted twice is one subscription on the server, so its two handlers take the
//! messages in turn, as the members of a share group do.
//!
//! ```text
//! cargo test --example mqtt_subscriptions_twice --features testing
//! ```

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use ruststream::Outgoing;
use ruststream_rust_mqtt::prelude::*;
use serde::{Deserialize, Serialize};

/// A job for the board.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Job {
    id: u32,
}

// --8<-- [start:descriptor]
/// The first of two handlers on the same filter.
#[subscriber(MqttFilter::new("jobs/+"))]
async fn first(job: &Job) -> HandlerOutcome {
    let _ = job.id;
    HandlerOutcome::ack()
}

/// The second takes every other message the filter matches.
#[subscriber(MqttFilter::new("jobs/+"))]
async fn second(job: &Job) -> HandlerOutcome {
    let _ = job.id;
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| b.include(first).include(second))
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
    fn the_two_handlers_take_the_messages_in_turn() -> Result<(), Box<dyn Error>> {
        block_on(async {
            let (broker, _runner) = MqttBroker::new(MqttConfig::new("192.168.1.10:1883"));
            let tb = TestApp::start(app(broker)).await?;
            let board = tb.broker::<MqttBroker>();
            for id in 1..=4 {
                board
                    .message(&Job { id })
                    .to("jobs/render")
                    .publish()
                    .await?;
            }

            // Four messages, each handled once, however the two handlers share them.
            board.subscriber("jobs/+").assert_called(4);
            tb.shutdown().await?;
            Ok(())
        })
    }
}
