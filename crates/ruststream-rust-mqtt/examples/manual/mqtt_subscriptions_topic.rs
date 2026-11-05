//! The service of `examples/mqtt_subscriptions_topic.rs` written without `#[subscriber]`: each
//! handler implements `Handle`, and an `Err` outcome settles the delivery as the macro form's
//! return does.
//!
//! ```text
//! cargo test --example manual_mqtt_subscriptions_topic --features testing
//! ```

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use std::future::{Future, ready};

use ruststream::Outgoing;
use ruststream_rust_mqtt::prelude::*;
use serde::{Deserialize, Serialize};

/// A command for the valve. The sender names the topic.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Command {
    open: bool,
}

// --8<-- [start:descriptor]
/// The valve listens on its own topic.
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
        b.include(
            subscriber("valves/v1/set", Set)
                .map_source(|_| MqttTopic::new("valves/v1/set"))
                .build(),
        )
    })
}
// --8<-- [end:descriptor]

/// The same handler on a topic that carries a wildcard.
fn wildcard_app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| {
        b.include(
            subscriber("valves/+/set", Set)
                .map_source(|_| MqttTopic::new("valves/+/set"))
                .build(),
        )
    })
}

fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use embassy_futures::block_on;
    use ruststream::testing::embedded::TestApp;

    use super::*;

    #[test]
    fn the_handler_takes_its_topic_and_no_other() -> Result<(), Box<dyn Error>> {
        block_on(async {
            let (broker, _runner) = MqttBroker::new(MqttConfig::new("192.168.1.10:1883"));
            let tb = TestApp::start(app(broker)).await?;
            let valve = tb.broker::<MqttBroker>();
            for topic in ["valves/v1/set", "valves/v2/set"] {
                valve
                    .message(&Command { open: true })
                    .to(topic)
                    .publish()
                    .await?;
            }

            valve
                .subscriber("valves/v1/set")
                .assert_called_once()
                .with(&Command { open: true });
            tb.shutdown().await?;
            Ok(())
        })
    }

    #[test]
    fn a_topic_with_a_wildcard_is_refused_at_startup() {
        block_on(async {
            let (broker, _runner) = MqttBroker::new(MqttConfig::new("192.168.1.10:1883"));
            let started = TestApp::start(wildcard_app(broker)).await;
            assert!(started.is_err(), "a wildcard is a filter, not a topic");
        });
    }
}
