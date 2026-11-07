//! The service of `examples/device/device_service.rs` written without `#[subscriber]` and the
//! derives: the lanes they write are spelled out, and the handler implements `Handle`. The app's
//! task is written by hand beside the runner's.
//!
//! ```text
//! cd checks/noalloc && cargo test --example manual_device_service
//! ```

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use core::convert::Infallible;
use core::future::{Future, ready};

use embassy_executor::Spawner;
use embassy_net::Stack;
use ruststream::runtime::Control;
use ruststream_rust_mqtt::prelude::*;
use ruststream_rust_mqtt::{EmbassyNetwork, MqttRunner, MqttState};
use static_cell::ConstStaticCell;

// --8<-- [start:device]
/// A command as the controller sends its bytes, borrowed from the broker's receive buffer.
struct Command<'a>(&'a [u8]);

// `#[derive(Deserialized)]` by hand: the view borrows the payload, and `&Command<'_>` takes the
// lane that reads no codec.
impl Deserialized for Command<'_> {
    type Output<'a> = Command<'a>;
    type Error = Infallible;

    fn from_payload(payload: &[u8]) -> Result<Command<'_>, Self::Error> {
        Ok(Command(payload))
    }
}

impl Input for Command<'_> {
    type Axis = SoloDeserialized<Command<'static>>;
}

/// The state the valve reports, as the bytes that leave.
struct State(&'static [u8]);

// `#[derive(Serialized)]` by hand: the bytes, the wire of a typed publish, the shape of a reply.
impl Serialized for State {
    type Error = Infallible;

    fn wire_bytes(&self, _buf: &mut EncodeBuf) -> Result<WireBytes<'_>, Infallible> {
        Ok(WireBytes::Own(self.0))
    }
}

impl MessageWire for State {
    type Wire = SerializedWire;
}

impl ReplyShape for State {
    type Body = Self;
    type Headers = ();
    type Wire = SerializedReply;
}

// `#[derive(Outgoing)]` with no `name`, by hand: the mount names the destination.
impl OutgoingDestination for State {
    type Form = CallerName;
}

impl MessageHeaders for State {
    type Contract = NoHeaders;
}

/// Moves the valve and reports where it is.
struct Set;

impl<'p> Handle<Command<'p>, State> for Set {
    fn handle(
        &self,
        command: &Command<'p>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<State, HandlerOutcome>> {
        ready(Ok(match command.0 {
            b"open" => State(b"opened"),
            _ => State(b"closed"),
        }))
    }
}

fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| {
        b.include(
            subscriber("valves/v1/set", Set)
                .map_source(|_| MqttTopic::new("valves/v1/set"))
                .reply()
                .to("valves/v1/state")
                .build(),
        )
        .out_reply(MqttPublish::default())
    })
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

/// The app's control: `stop` asks it to shut down, and `health` reads where it is.
static CONTROL: Control = Control::new();

/// Runs the app until the control stops it.
#[embassy_executor::task]
async fn service(broker: MqttBroker) {
    let _ = app(broker).run(&CONTROL).await;
}

/// What the board's `main` does once its network stack is up.
fn start(spawner: Spawner, stack: Stack<'static>) {
    let config = MqttConfig::new("192.168.1.10:1883").client_id("valve-1");
    let (broker, runner) = MqttBroker::new(MQTT.take(), config);
    spawner.spawn(mqtt(runner, stack).expect("the pool holds the runner's task"));
    spawner.spawn(service(broker).expect("the pool holds the app's task"));
}
// --8<-- [end:board]

// The firmware's `main` is an `#[embassy_executor::main]` that calls `start`.
fn main() {
    let _ = start;
}

#[cfg(test)]
mod tests {
    use embassy_futures::block_on;
    use ruststream::Outgoing;
    use ruststream::testing::embedded::TestApp;

    use super::*;

    /// What the controller publishes, to the topic it names.
    #[derive(Outgoing, ruststream::Serialized)]
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
