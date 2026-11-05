//! The per-message steps of an MQTT publish: `qos(..)` and `retain(..)` on the publish builder reopen
//! the settings the mount's policy fixed.
//!
//! ```text
//! cargo test --example mqtt_publishing_steps --features testing
//! ```

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use ruststream::Outgoing;
use ruststream_rust_mqtt::prelude::*;
use serde::{Deserialize, Serialize};

/// A valve's position, as the sensor reports it.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "valves/v1/position")]
struct Position {
    percent: u8,
}

/// The state the service publishes for the valve.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "valves/v1/state")]
struct State {
    open: bool,
}

/// The slot the states leave through.
#[derive(OutSlot)]
#[publishes(State)]
struct States;

// --8<-- [start:steps]
/// A state change is sent exactly once, and retained so a dashboard that starts later reads it.
#[subscriber(MqttTopic::new("valves/v1/position"))]
async fn track(
    position: &Position,
    Out(states): Out<impl Publisher<Options = MqttPublishOptions>, States>,
) -> HandlerOutcome {
    let state = State {
        open: position.percent > 0,
    };
    if states
        .message(&state)
        .qos(Qos::ExactlyOnce)
        .retain(true)
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| {
        b.include(track).out(States, MqttPublish::default())
    })
}
// --8<-- [end:steps]

fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use embassy_futures::block_on;
    use ruststream::testing::embedded::TestApp;

    use super::*;

    #[test]
    fn the_steps_set_what_the_publish_carries() -> Result<(), Box<dyn Error>> {
        block_on(async {
            let (broker, _runner) = MqttBroker::new(MqttConfig::new("192.168.1.10:1883"));
            let tb = TestApp::start(app(broker)).await?;
            let board = tb.broker::<MqttBroker>();
            board.message(&Position { percent: 80 }).publish().await?;

            tb.out::<States>()
                .assert_called_once()
                .with_raw(br#"{"open":true}"#)
                .with_options(
                    &MqttPublishOptions::default()
                        .qos(Qos::ExactlyOnce)
                        .retain(true),
                );
            tb.shutdown().await?;
            Ok(())
        })
    }
}
