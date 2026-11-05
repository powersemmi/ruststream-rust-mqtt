//! A service on a compio host: `MqttBroker::compio` starts the runner on the core that connects
//! the broker, over compio's TCP stream, so the host spawns nothing by hand.
//!
//! ```text
//! cargo test --example mqtt_network_compio --features testing,compio
//! ```

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use ruststream::Outgoing;
use ruststream::runtime::compio::{App, AppInfo, RustStream};
use ruststream_rust_mqtt::prelude::*;
use serde::{Deserialize, Serialize};

/// A temperature reading, in degrees.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "sensors/temp")]
struct Reading {
    celsius: i16,
}

// --8<-- [start:app]
/// Takes every reading the sensors publish.
#[subscriber(MqttTopic::new("sensors/temp"))]
async fn store(reading: &Reading) -> HandlerOutcome {
    let _ = reading.celsius;
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = compio, cores = 1)]
fn app() -> impl App {
    let config = MqttConfig::new("mqtt://localhost:1883").client_id("collector");
    RustStream::new(AppInfo::new("collector", "0.1.0"))
        .with_broker(MqttBroker::compio(config), |b| b.include(store))
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use compio::runtime::Runtime;
    use ruststream::testing::compio::TestApp;

    use super::*;

    #[test]
    fn a_reading_reaches_the_handler_on_compio() -> Result<(), Box<dyn Error>> {
        Runtime::new()?.block_on(async {
            let tb = TestApp::start(app()).await?;
            let sensor = tb.broker::<MqttBroker>();
            sensor.message(&Reading { celsius: 21 }).publish().await?;

            sensor
                .subscriber("sensors/temp")
                .assert_called_once()
                .with(&Reading { celsius: 21 })
                .settled(HandlerOutcome::ack());
            tb.shutdown().await?;
            Ok(())
        })
    }
}
