//! A device service: the board builds the broker with its runner, spawns the runner's task over
//! its network stack, and spawns the app's task with the broker.
//!
//! ```text
//! cargo test --example mqtt_device --features testing,embassy
//! ```

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use embassy_executor::Spawner;
use embassy_net::Stack;
use ruststream::Outgoing;
use ruststream_rust_mqtt::prelude::*;
use ruststream_rust_mqtt::{EmbassyNetwork, MqttRunner};
use serde::{Deserialize, Serialize};

/// A temperature reading, in degrees.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "sensors/temp")]
struct Reading {
    celsius: i16,
}

// --8<-- [start:device]
/// Takes every reading the sensors publish.
#[subscriber(MqttTopic::new("sensors/temp"))]
async fn store(reading: &Reading) -> HandlerOutcome {
    let _ = reading.celsius;
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| b.include(store))
}
// --8<-- [end:device]

// --8<-- [start:runner]
/// The runner owns the connection, and the app starts only while it runs.
#[embassy_executor::task]
async fn mqtt(runner: MqttRunner, stack: Stack<'static>) -> ! {
    let mut rx = [0; 1024];
    let mut tx = [0; 1024];
    runner
        .run(EmbassyNetwork::new(stack, &mut rx, &mut tx))
        .await
}

/// What the board's `main` does once its network stack is up.
fn start(spawner: Spawner, stack: Stack<'static>) {
    let config = MqttConfig::new("192.168.1.10:1883").client_id("sensor-7");
    let (broker, runner) = MqttBroker::new(config);
    spawner.spawn(mqtt(runner, stack).expect("the pool holds the runner's task"));
    spawner.spawn(app::task(broker).expect("the pool holds the app's task"));
}
// --8<-- [end:runner]

// The firmware's `main` is an `#[embassy_executor::main]` that calls `start`.
fn main() {
    let _ = start;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use embassy_futures::block_on;
    use ruststream::testing::embedded::TestApp;

    use super::*;

    #[test]
    fn a_reading_reaches_the_handler_of_the_device() -> Result<(), Box<dyn Error>> {
        block_on(async {
            let (broker, _runner) = MqttBroker::new(MqttConfig::new("192.168.1.10:1883"));
            let tb = TestApp::start(app(broker)).await?;
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
