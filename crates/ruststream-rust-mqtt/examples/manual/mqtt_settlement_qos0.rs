//! The service of `examples/mqtt_settlement_qos0.rs` written without `#[subscriber]`: each handler
//! implements `Handle`, and an `Err` outcome settles the delivery as the macro form's return does.
//!
//! ```text
//! cargo test --example manual_mqtt_settlement_qos0 --features testing
//! ```

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use std::future::{Future, ready};

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
struct Store;

impl Handle<Reading> for Store {
    fn handle(
        &self,
        reading: &Reading,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        ready(if reading.celsius < -40 {
            Err(HandlerOutcome::drop())
        } else {
            Ok(())
        })
    }
}

fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| {
        let temp = MqttTopic::new("sensors/temp").qos(Qos::AtMostOnce);
        b.include(
            subscriber("sensors/temp", Store)
                .map_source(|_| temp)
                .build(),
        )
    })
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
