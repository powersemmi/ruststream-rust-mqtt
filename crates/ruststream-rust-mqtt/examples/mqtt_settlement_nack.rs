//! A negative acknowledgement on MQTT: the protocol has none, so a retry leaves the delivery
//! unacknowledged and the runtime reports the settlement as unsupported.
//!
//! ```text
//! cargo test --example mqtt_settlement_nack --features testing
//! ```

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use ruststream::Outgoing;
use ruststream_rust_mqtt::prelude::*;
use serde::{Deserialize, Serialize};

/// A job for the board to run.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "jobs/run")]
struct Job {
    id: u32,
}

// --8<-- [start:handler]
/// A job id of zero is not ready to run. `retry` asks for a negative acknowledgement, which MQTT
/// does not have: the delivery stays unacknowledged, and the server sends it again when a
/// persistent session resumes.
#[subscriber(MqttTopic::new("jobs/run"))]
async fn run(job: &Job) -> HandlerOutcome {
    if job.id == 0 {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| b.include(run))
}
// --8<-- [end:handler]

fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use embassy_futures::block_on;
    use ruststream::testing::Outcome;
    use ruststream::testing::embedded::TestApp;

    use super::*;

    #[test]
    fn a_retry_is_not_redelivered_by_the_transport() -> Result<(), Box<dyn Error>> {
        block_on(async {
            let (broker, _runner) = MqttBroker::new(MqttConfig::new("192.168.1.10:1883"));
            let tb = TestApp::start(app(broker)).await?;
            let board = tb.broker::<MqttBroker>();
            board.message(&Job { id: 0 }).publish().await?;
            board.message(&Job { id: 7 }).publish().await?;

            // The first delivery asked for a negative acknowledgement and came back as one
            // call: nothing in the transport sends it again.
            let calls = board.subscriber("jobs/run").assert_called(2);
            assert_eq!(calls.outcomes(), [Outcome::Nack, Outcome::Ack]);
            tb.shutdown().await?;
            Ok(())
        })
    }
}
