//! The service of `examples/mqtt_subscriptions_twice.rs` written without `#[subscriber]`: each
//! handler implements `Handle`, and an `Err` outcome settles the delivery as the macro form's
//! return does.
//!
//! ```text
//! cargo test --example manual_mqtt_subscriptions_twice --features testing
//! ```

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use std::future::{Future, ready};

use ruststream::Outgoing;
use ruststream_rust_mqtt::prelude::*;
use serde::{Deserialize, Serialize};

/// A job for the board.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Job {
    id: u32,
}

// --8<-- [start:descriptor]
/// The first of two handlers on the same filter.
struct First;

impl Handle<Job> for First {
    fn handle(
        &self,
        _job: &Job,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        ready(Ok(()))
    }
}

/// The second takes every other message the filter matches.
struct Second;

impl Handle<Job> for Second {
    fn handle(
        &self,
        _job: &Job,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        ready(Ok(()))
    }
}

fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| {
        b.include(
            subscriber("jobs/+", First)
                .map_source(|_| MqttFilter::new("jobs/+"))
                .build(),
        )
        .include(
            subscriber("jobs/+", Second)
                .map_source(|_| MqttFilter::new("jobs/+"))
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
    fn the_two_handlers_take_the_messages_in_turn() -> Result<(), Box<dyn Error>> {
        block_on(async {
            let (broker, _runner) = MqttBroker::new(MqttConfig::new("192.168.1.10:1883"));
            let tb = TestApp::start(app(broker)).await?;
            let board = tb.broker::<MqttBroker>();
            for id in 1..=4 {
                board
                    .message(&Job { id })
                    .to("jobs/render")
                    .publish()
                    .await?;
            }

            // Four messages, each handled once, however the two handlers share them.
            board.subscriber("jobs/+").assert_called(4);
            tb.shutdown().await?;
            Ok(())
        })
    }
}
