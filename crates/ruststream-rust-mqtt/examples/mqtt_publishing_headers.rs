//! Headers on MQTT: a serde struct is the header contract, and each field travels as an MQTT 5
//! user property, so the handler reads the properties of a delivery and the service writes its own.
//!
//! ```text
//! cargo test --example mqtt_publishing_headers --features testing
//! ```

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use ruststream::Outgoing;
use ruststream_rust_mqtt::prelude::*;
use serde::{Deserialize, Serialize};

/// What travels in the user properties of a message.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Trace {
    trace_id: String,
}

/// A command for the valve; publishing one carries its `Trace`.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "valves/v1/set", headers = Trace)]
struct Command {
    open: bool,
}

/// The state the service publishes for the valve, under the same `Trace`.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "valves/v1/state", headers = Trace)]
struct State {
    open: bool,
}

/// The slot the states leave through.
#[derive(OutSlot)]
#[publishes(State)]
struct States;

// --8<-- [start:headers]
/// The state goes out with the trace the command arrived with.
#[subscriber(MqttTopic::new("valves/v1/set"))]
async fn set(
    command: &Command,
    Headers(trace): Headers<Trace>,
    Out(states): Out<impl Publisher<Options = MqttPublishOptions>, States>,
) -> HandlerOutcome {
    let state = State { open: command.open };
    if states
        .message(&state)
        .with_headers(&trace)
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
        b.include(set).out(States, MqttPublish::default())
    })
}
// --8<-- [end:headers]

fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use embassy_futures::block_on;
    use ruststream::testing::embedded::TestApp;

    use super::*;

    #[test]
    fn the_state_carries_the_trace_of_the_command() -> Result<(), Box<dyn Error>> {
        block_on(async {
            let (broker, _runner) = MqttBroker::new(MqttConfig::new("192.168.1.10:1883"));
            let tb = TestApp::start(app(broker)).await?;
            let trace = Trace {
                trace_id: "t-17".to_owned(),
            };
            tb.broker::<MqttBroker>()
                .message(&Command { open: true })
                .with_headers(&trace)
                .publish()
                .await?;

            tb.out::<States>()
                .assert_called_once()
                .with_header("trace_id", "t-17");
            tb.shutdown().await?;
            Ok(())
        })
    }
}
