//! A delivery at `QoS` 0: the handler settles it like any other, and no acknowledgement packet
//! is sent, because the protocol has none.
//!
//! ```text
//! cargo test --example mqtt_settlement_qos0 --features testing
//! ```

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use ruststream::Outgoing;
use ruststream_rust_mqtt::prelude::*;
use serde::{Deserialize, Serialize};

/// A temperature reading, in degrees.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "sensors/temp")]
struct Reading {
    celsius: i16,
}

// --8<-- [start:handler]
/// Telemetry at `QoS` 0: the next reading replaces a lost one, so the subscription asks for no
/// acknowledgement. The outcome still settles the delivery in the service; the runtime has no
/// packet to send for it and logs that it is unsupported.
#[subscriber(MqttTopic::new("sensors/temp").qos(Qos::AtMostOnce))]
async fn store(reading: &Reading) -> HandlerOutcome {
    if reading.celsius < -40 {
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| b.include(store))
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
    fn a_qos_0_delivery_is_settled_by_the_handler() -> Result<(), Box<dyn Error>> {
        block_on(async {
            let (broker, _runner) = MqttBroker::new(MqttConfig::new("192.168.1.10:1883"));
            let tb = TestApp::start(app(broker)).await?;
            let sensor = tb.broker::<MqttBroker>();
            sensor.message(&Reading { celsius: 21 }).publish().await?;
            sensor.message(&Reading { celsius: -99 }).publish().await?;

            let calls = sensor.subscriber("sensors/temp").assert_called(2);
            assert_eq!(calls.outcomes(), [Outcome::Ack, Outcome::Drop]);
            tb.shutdown().await?;
            Ok(())
        })
    }
}
