//! A device service without a heap: the broker's memory is a `static`, a command borrows its
//! payload from the receive buffer, and the reply is encoded into a send buffer.
//!
//! ```text
//! cd checks/noalloc && cargo test --example device_service
//! ```

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use embassy_executor::Spawner;
use embassy_net::Stack;
use ruststream::{Deserialized, Outgoing, Serialized};
use ruststream_rust_mqtt::prelude::*;
use ruststream_rust_mqtt::{EmbassyNetwork, MqttRunner, MqttState};
use static_cell::ConstStaticCell;

// --8<-- [start:device]
/// A command as the controller sends its bytes, borrowed from the broker's receive buffer.
#[derive(Deserialized)]
struct Command<'a>(&'a [u8]);

/// The state the valve reports, as the bytes that leave.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "valves/v1/state")]
struct State(&'static [u8]);

/// Moves the valve and reports where it is.
#[subscriber(MqttTopic::new("valves/v1/set"), reply)]
async fn set(command: &Command<'_>) -> State {
    match command.0 {
        b"open" => State(b"opened"),
        _ => State(b"closed"),
    }
}

#[ruststream::app(runtime = embassy)]
fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| b.include(set).out_reply(MqttPublish::default()))
}
// --8<-- [end:device]

// --8<-- [start:board]
/// The broker's memory: four deliveries of up to 1 KiB, and eight subscriptions.
static MQTT: ConstStaticCell<MqttState> = ConstStaticCell::new(MqttState::new());

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
    let config = MqttConfig::new("192.168.1.10:1883").client_id("valve-1");
    let (broker, runner) = MqttBroker::new(MQTT.take(), config);
    spawner.spawn(mqtt(runner, stack).expect("the pool holds the runner's task"));
    spawner.spawn(app::task(broker).expect("the pool holds the app's task"));
}
// --8<-- [end:board]

// The firmware's `main` is an `#[embassy_executor::main]` that calls `start`.
fn main() {
    let _ = start;
}

#[cfg(test)]
mod tests {
    use embassy_futures::block_on;
    use ruststream::testing::embedded::TestApp;

    use super::*;

    /// What the controller publishes, to the topic it names.
    #[derive(Outgoing, Serialized)]
    struct Sent(&'static [u8]);

    #[test]
    fn a_command_is_answered_with_the_state_of_the_valve() {
        static MEMORY: ConstStaticCell<MqttState> = ConstStaticCell::new(MqttState::new());
        block_on(async {
            let (broker, _runner) = MqttBroker::new(
                MEMORY.take(),
                MqttConfig::new("192.168.1.10:1883").client_id("valve-1"),
            );
            let tb = TestApp::start(app(broker)).await.expect("the app starts");
            let valve = tb.broker::<MqttBroker>();
            valve
                .message(&Sent(b"open"))
                .to("valves/v1/set")
                .publish()
                .await
                .expect("the harness publishes");

            valve
                .subscriber("valves/v1/set")
                .assert_called_once()
                .with_raw(b"open")
                .settled(HandlerOutcome::ack());
            valve
                .published::<()>("valves/v1/state")
                .assert_called_once()
                .with_raw(b"opened");
            tb.shutdown().await.expect("the app stops");
        });
    }
}
