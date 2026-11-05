//! The service of `examples/mqtt_device.rs` written without `#[subscriber]`: each handler
//! implements `Handle`, and an `Err` outcome settles the delivery as the macro form's return does.
//! The app's task is written by hand beside the runner's.
//!
//! ```text
//! cargo test --example manual_mqtt_device --features testing,embassy
//! ```

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use std::future::{Future, ready};

use embassy_executor::Spawner;
use embassy_net::Stack;
use ruststream::Outgoing;
use ruststream::runtime::Control;
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
struct Store;

impl Handle<Reading> for Store {
    fn handle(
        &self,
        _reading: &Reading,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        ready(Ok(()))
    }
}

fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| {
        b.include(
            subscriber("sensors/temp", Store)
                .map_source(|_| MqttTopic::new("sensors/temp"))
                .build(),
        )
    })
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

/// The app's control: `stop` asks it to shut down, and `health` reads where it is.
static CONTROL: Control = Control::new();

/// Runs the app until the control stops it.
#[embassy_executor::task]
async fn service(broker: MqttBroker) {
    let _ = app(broker).run(&CONTROL).await;
}

/// What the board's `main` does once its network stack is up.
fn start(spawner: Spawner, stack: Stack<'static>) {
    let config = MqttConfig::new("192.168.1.10:1883").client_id("sensor-7");
    let (broker, runner) = MqttBroker::new(config);
    spawner.spawn(mqtt(runner, stack).expect("the pool holds the runner's task"));
    spawner.spawn(service(broker).expect("the pool holds the app's task"));
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
