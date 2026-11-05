//! `MqttFilter` subscribes to every topic its wildcards match; `+` matches one level and `#`
//! the rest.
//!
//! ```text
//! cargo test --example mqtt_subscriptions_filter --features testing
//! ```

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use ruststream::Outgoing;
use ruststream_rust_mqtt::prelude::*;
use serde::{Deserialize, Serialize};

/// A reading of one sensor of the floor, in degrees. The sender names the topic.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Reading {
    celsius: i16,
}

// --8<-- [start:descriptor]
/// Every sensor of the first floor reports on a topic of its own.
#[subscriber(MqttFilter::new("floor-1/+/temp"))]
async fn log(reading: &Reading) -> HandlerOutcome {
    let _ = reading.celsius;
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| b.include(log))
}
// --8<-- [end:descriptor]

fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use embassy_futures::block_on;
    use ruststream::testing::Outcome;
    use ruststream::testing::embedded::TestApp;

    use super::*;

    #[test]
    fn the_filter_takes_every_topic_it_matches() -> Result<(), Box<dyn Error>> {
        block_on(async {
            let (broker, _runner) = MqttBroker::new(MqttConfig::new("192.168.1.10:1883"));
            let tb = TestApp::start(app(broker)).await?;
            let floor = tb.broker::<MqttBroker>();
            for topic in ["floor-1/a/temp", "floor-1/b/temp", "floor-2/a/temp"] {
                floor
                    .message(&Reading { celsius: 21 })
                    .to(topic)
                    .publish()
                    .await?;
            }

            // Two of the three topics match; the second floor's does not.
            let calls = floor.subscriber("floor-1/+/temp").assert_called(2);
            assert_eq!(calls.outcomes(), [Outcome::Ack, Outcome::Ack]);
            tb.shutdown().await?;
            Ok(())
        })
    }
}
