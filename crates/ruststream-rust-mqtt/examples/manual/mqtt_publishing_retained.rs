//! The service of `examples/mqtt_publishing_retained.rs` written without `#[subscriber]`: each
//! handler implements `Handle`, and an `Err` outcome settles the delivery as the macro form's
//! return does.
//!
//! ```text
//! cargo test --example manual_mqtt_publishing_retained --features testing
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

// --8<-- [start:retained]
/// The valve's state is retained, so a subscriber that joins later reads the latest one.
struct Track;

impl<Entry> Handle<Position, (), Outs<(Entry,)>> for Track
where
    Entry: OutEntry<States, Wire: Publisher<Options = MqttPublishOptions>>,
{
    async fn handle(
        &self,
        position: &Position,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let state = State {
            open: position.percent > 0,
        };
        outs.get(States)
            .message(&state)
            .retain(true)
            .publish()
            .await
            .map_err(|_| HandlerOutcome::retry())
    }
}

fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| {
        b.include(
            subscriber("valves/v1/position", Track)
                .map_source(|_| MqttTopic::new("valves/v1/position"))
                .build(),
        )
        .out(States, MqttPublish::default())
    })
}
// --8<-- [end:retained]

fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use embassy_futures::block_on;
    use ruststream::testing::embedded::TestApp;

    use super::*;

    #[test]
    fn the_state_is_published_retained() -> Result<(), Box<dyn Error>> {
        block_on(async {
            let (broker, _runner) = MqttBroker::new(MqttConfig::new("192.168.1.10:1883"));
            let tb = TestApp::start(app(broker)).await?;
            let board = tb.broker::<MqttBroker>();
            board.message(&Position { percent: 80 }).publish().await?;

            tb.out::<States>()
                .assert_called_once()
                .with_raw(br#"{"open":true}"#)
                .with_options(&MqttPublishOptions::default().retain(true));
            tb.shutdown().await?;
            Ok(())
        })
    }
}
