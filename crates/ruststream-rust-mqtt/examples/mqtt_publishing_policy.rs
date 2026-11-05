//! The publish policy of a mount: `MqttPublish` fixes the quality of service and the retain flag
//! of every message the mount publishes, a reply included.
//!
//! ```text
//! cargo test --example mqtt_publishing_policy --features testing
//! ```

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use ruststream::Outgoing;
use ruststream_rust_mqtt::prelude::*;
use serde::{Deserialize, Serialize};

/// A reading in millivolts, as the sensor sends it.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "sensors/temp")]
struct Reading {
    millivolts: u16,
}

/// What the board reports for a reading.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "reports/temp")]
struct Report {
    celsius: i16,
}

// --8<-- [start:policy]
/// Reports every reading in degrees: the return value is the reply.
#[subscriber(MqttTopic::new("sensors/temp"), reply)]
async fn report(reading: &Reading) -> Report {
    Report {
        celsius: i16::try_from((i32::from(reading.millivolts) - 500) / 10).unwrap_or(i16::MAX),
    }
}

#[ruststream::app(runtime = embassy)]
fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| {
        // Reports are sent exactly once.
        b.include(report)
            .out_reply(MqttPublish::default().qos(Qos::ExactlyOnce))
    })
}
// --8<-- [end:policy]

fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use embassy_futures::block_on;
    use ruststream::testing::embedded::TestApp;

    use super::*;

    #[test]
    fn the_reply_reaches_its_topic_under_the_policy() -> Result<(), Box<dyn Error>> {
        block_on(async {
            let (broker, _runner) = MqttBroker::new(MqttConfig::new("192.168.1.10:1883"));
            let tb = TestApp::start(app(broker)).await?;
            let board = tb.broker::<MqttBroker>();
            board
                .message(&Reading { millivolts: 700 })
                .publish()
                .await?;

            board
                .published::<Report>("reports/temp")
                .assert_called_once()
                .with(&Report { celsius: 20 });
            tb.shutdown().await?;
            Ok(())
        })
    }
}
