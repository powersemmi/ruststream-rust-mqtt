//! The service of `examples/mqtt_subscriptions_qos.rs` written without `#[subscriber]`: each
//! handler implements `Handle`, and an `Err` outcome settles the delivery as the macro form's
//! return does.
//!
//! ```text
//! cargo test --example manual_mqtt_subscriptions_qos --features testing
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

/// A command for the valve.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "valves/v1/set")]
struct Command {
    open: bool,
}

// --8<-- [start:descriptor]
/// Telemetry is read at most once: the next reading replaces a lost one.
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

/// A valve command is applied exactly once.
struct Set;

impl Handle<Command> for Set {
    fn handle(
        &self,
        _command: &Command,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        ready(Ok(()))
    }
}

fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| {
        let temp = MqttTopic::new("sensors/temp").qos(Qos::AtMostOnce);
        let valve = MqttTopic::new("valves/v1/set").qos(Qos::ExactlyOnce);
        b.include(
            subscriber("sensors/temp", Store)
                .map_source(|_| temp)
                .build(),
        )
        .include(
            subscriber("valves/v1/set", Set)
                .map_source(|_| valve)
                .build(),
        )
    })
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
    fn each_subscription_receives_its_own_topic() -> Result<(), Box<dyn Error>> {
        block_on(async {
            let (broker, _runner) = MqttBroker::new(MqttConfig::new("192.168.1.10:1883"));
            let tb = TestApp::start(app(broker)).await?;
            let board = tb.broker::<MqttBroker>();
            board.message(&Reading { celsius: 21 }).publish().await?;
            board.message(&Command { open: true }).publish().await?;

            board
                .subscriber("sensors/temp")
                .assert_called_once()
                .with(&Reading { celsius: 21 });
            board
                .subscriber("valves/v1/set")
                .assert_called_once()
                .with(&Command { open: true });
            tb.shutdown().await?;
            Ok(())
        })
    }
}
