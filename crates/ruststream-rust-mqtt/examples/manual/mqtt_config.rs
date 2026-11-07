//! The service of `examples/mqtt_config.rs` written without `#[subscriber]`: each handler
//! implements `Handle`, and an `Err` outcome settles the delivery as the macro form's return does.
//!
//! ```text
//! cargo test --example manual_mqtt_config --features testing
//! ```

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use core::time::Duration;
use std::future::{Future, ready};

use ruststream::Outgoing;
use ruststream_rust_mqtt::MqttRunner;
use ruststream_rust_mqtt::prelude::*;
use serde::{Deserialize, Serialize};

/// A command for the valve.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "valves/v1/set")]
struct Command {
    open: bool,
}

// --8<-- [start:config]
/// The server on the plant network, as this valve, with the password the deployment hands the
/// service when it starts. The server keeps the session for ten minutes after the connection
/// ends and redelivers what the valve had not acknowledged.
fn board(password: String) -> (MqttBroker, MqttRunner) {
    MqttBroker::new(
        MqttConfig::new("mqtt://192.168.1.10")
            .client_id("valve-1")
            .keep_alive(Duration::from_secs(30))
            .credentials("valve-1", password)
            .persistent_session(Duration::from_secs(600)),
    )
}
// --8<-- [end:config]

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
        let valve = MqttTopic::new("valves/v1/set").qos(Qos::ExactlyOnce);
        b.include(
            subscriber("valves/v1/set", Set)
                .map_source(|_| valve)
                .build(),
        )
    })
}

fn main() {}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::RandomState;
    use std::error::Error;
    use std::hash::{BuildHasher, Hasher};

    use embassy_futures::block_on;
    use ruststream::testing::embedded::TestApp;

    use super::*;

    /// A password made up at run time, so no secret is written into the source.
    fn made_up_password() -> String {
        RandomState::new().build_hasher().finish().to_string()
    }

    #[test]
    fn the_app_starts_with_the_boards_settings() -> Result<(), Box<dyn Error>> {
        block_on(async {
            let (broker, _runner) = board(made_up_password());
            let tb = TestApp::start(app(broker)).await?;
            tb.broker::<MqttBroker>()
                .message(&Command { open: true })
                .publish()
                .await?;

            tb.broker::<MqttBroker>()
                .subscriber("valves/v1/set")
                .assert_called_once();
            tb.shutdown().await?;
            Ok(())
        })
    }

    #[test]
    fn an_address_with_another_scheme_fails_at_startup() {
        block_on(async {
            let (broker, _runner) = MqttBroker::new(MqttConfig::new("mqtts://192.168.1.10:8883"));
            let started = TestApp::start(app(broker)).await;
            assert!(started.is_err(), "the broker connects over plain TCP");
        });
    }
}
