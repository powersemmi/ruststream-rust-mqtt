//! The service of `examples/mqtt_settlement_ack.rs` written without `#[subscriber]`: each handler
//! implements `Handle`, and an `Err` outcome settles the delivery as the macro form's return does.
//!
//! ```text
//! cargo test --example manual_mqtt_settlement_ack --features testing
//! ```

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use std::future::{Future, ready};

use ruststream::Outgoing;
use ruststream_rust_mqtt::prelude::*;
use serde::{Deserialize, Serialize};

/// A command sent to one device.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "devices/dev42/commands")]
struct Command {
    action: String,
}

// --8<-- [start:handler]
/// The subscription reads at `QoS` 1, so returning from the handler acknowledges the delivery with
/// a PUBACK. A command the device does not know is dropped: on MQTT that acknowledges it as well,
/// and the server never sends it again.
struct Apply;

impl Handle<Command> for Apply {
    fn handle(
        &self,
        command: &Command,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        ready(match command.action.as_str() {
            "reboot" | "reset" => Ok(()),
            _ => Err(HandlerOutcome::drop()),
        })
    }
}

fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| {
        let commands = MqttTopic::new("devices/dev42/commands").qos(Qos::AtLeastOnce);
        b.include(
            subscriber("devices/dev42/commands", Apply)
                .map_source(|_| commands)
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

    fn command(action: &str) -> Command {
        Command {
            action: action.to_owned(),
        }
    }

    #[test]
    fn a_known_command_is_acknowledged_and_an_unknown_one_dropped() -> Result<(), Box<dyn Error>> {
        block_on(async {
            let (broker, _runner) = MqttBroker::new(MqttConfig::new("192.168.1.10:1883"));
            let tb = TestApp::start(app(broker)).await?;
            let device = tb.broker::<MqttBroker>();
            device.message(&command("reboot")).publish().await?;
            device.message(&command("self-destruct")).publish().await?;

            // Both answers settle the delivery, so neither comes back.
            let calls = device.subscriber("devices/dev42/commands").assert_called(2);
            assert_eq!(calls.outcomes(), [Outcome::Ack, Outcome::Drop]);
            tb.shutdown().await?;
            Ok(())
        })
    }
}
