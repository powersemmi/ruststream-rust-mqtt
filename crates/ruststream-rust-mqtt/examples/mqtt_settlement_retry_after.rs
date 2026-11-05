//! `retry_after` on MQTT: the runtime publishes a copy of the delivery after the delay. A topic
//! subscription is reached again on its own topic; a filter reaches no topic, so the mount names
//! where the copy goes.
//!
//! ```text
//! cargo test --example mqtt_settlement_retry_after --features testing
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

// --8<-- [start:topic]
/// The valve is still moving: the job is asked for again in five seconds. A copy that carries its
/// retry count is the second attempt, and it is acknowledged.
#[subscriber(MqttTopic::new("jobs/run"))]
async fn run(job: &Job, ctx: &mut Context<'_>) -> HandlerOutcome {
    let _ = job.id;
    match ctx.headers().get_str(RETRY_COUNT_HEADER) {
        Some(_) => HandlerOutcome::ack(),
        None => HandlerOutcome::retry_after(Duration::from_secs(5)),
    }
}

#[ruststream::app(runtime = embassy)]
fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    // Room for two copies waiting at once.
    EmbeddedApp::new().with_broker(broker, |b| b.include(run).retry_capacity::<2>())
}
// --8<-- [end:topic]

// --8<-- [start:filter]
/// A filter reads many topics and addresses none of them, so the mount site names where a copy
/// goes: `jobs/retry` matches the filter, and the copy comes back to the handler.
#[subscriber(MqttFilter::new("jobs/+"))]
async fn reconcile(job: &Job, ctx: &mut Context<'_>) -> HandlerOutcome {
    let _ = job.id;
    match ctx.headers().get_str(RETRY_COUNT_HEADER) {
        Some(_) => HandlerOutcome::ack(),
        None => HandlerOutcome::retry_after(Duration::from_secs(5)),
    }
}

#[ruststream::app(runtime = embassy)]
fn filter_app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| {
        b.include(reconcile)
            .out_retry(MqttPublish::default())
            .to("jobs/retry")
            .retry_capacity::<2>()
    })
}
// --8<-- [end:filter]

fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use std::time::Duration;

    use embassy_futures::block_on;
    use ruststream::testing::Outcome;
    use ruststream::testing::embedded::TestApp;

    use super::*;

    #[test]
    fn a_topic_subscription_is_reached_again_on_its_own_topic() -> Result<(), Box<dyn Error>> {
        block_on(async {
            let (broker, _runner) = MqttBroker::new(MqttConfig::new("192.168.1.10:1883"));
            let tb = TestApp::start(app(broker)).await?;
            let board = tb.broker::<MqttBroker>();
            board
                .message(&Job { id: 1 })
                .to("jobs/run")
                .publish()
                .await?;

            tb.advance(Duration::from_secs(4)).await?;
            board.subscriber("jobs/run").assert_called_once();
            tb.advance(Duration::from_secs(1)).await?;
            let calls = board.subscriber("jobs/run").assert_called(2);
            assert_eq!(calls.outcomes().last(), Some(&Outcome::Ack));
            tb.shutdown().await?;
            Ok(())
        })
    }

    #[test]
    fn a_filter_subscription_is_reached_again_on_the_named_topic() -> Result<(), Box<dyn Error>> {
        block_on(async {
            let (broker, _runner) = MqttBroker::new(MqttConfig::new("192.168.1.10:1883"));
            let tb = TestApp::start(filter_app(broker)).await?;
            let board = tb.broker::<MqttBroker>();
            board.message(&Job { id: 1 }).to("jobs/a").publish().await?;

            tb.advance(Duration::from_secs(5)).await?;
            let calls = board.subscriber("jobs/+").assert_called(2);
            assert_eq!(calls.outcomes().last(), Some(&Outcome::Ack));
            board.published::<Job>("jobs/retry").assert_called_once();
            tb.shutdown().await?;
            Ok(())
        })
    }
}
