//! The memory of a device broker: three numbers in the type of its `MqttState`, which fix how many
//! deliveries are held at once, how large a packet is, and how many subscriptions are open.
//!
//! ```text
//! cd checks/noalloc && cargo test --example device_memory
//! ```

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use ruststream::{Deserialized, Outgoing, Serialized};
use ruststream_rust_mqtt::MqttState;
use ruststream_rust_mqtt::prelude::*;
use static_cell::ConstStaticCell;

// --8<-- [start:state]
/// Six deliveries of up to 512 bytes, and four subscriptions: about 4 KiB of the board's RAM.
type Memory = MqttState<6, 512, 4>;

static MQTT: ConstStaticCell<Memory> = ConstStaticCell::new(MqttState::new());

/// The broker over that memory, named by the same three numbers.
type Mqtt = MqttBroker<6, 512, 4>;
// --8<-- [end:state]

/// A frame of the line, borrowed from the receive buffer.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

/// How many bytes a frame held.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "line/size")]
struct Size(&'static [u8]);

/// Measures every frame of the line.
#[subscriber(MqttTopic::new("line/frame"), reply)]
async fn measure(frame: &Frame<'_>) -> Size {
    if frame.0.len() > 256 {
        Size(b"large")
    } else {
        Size(b"small")
    }
}

#[ruststream::app(runtime = embassy)]
fn app(broker: Mqtt) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| {
        b.include(measure).out_reply(MqttPublish::default())
    })
}

fn main() {
    let _ = (&MQTT, app);
}

#[cfg(test)]
mod tests {
    use embassy_futures::block_on;
    use ruststream::testing::embedded::TestApp;

    use super::*;

    /// What the line publishes, to the topic it names.
    #[derive(Outgoing, Serialized)]
    struct Sent(&'static [u8]);

    static LARGE: [u8; 400] = [7; 400];

    #[test]
    fn more_frames_than_buffers_are_delivered_because_a_settled_buffer_comes_back() {
        static MEMORY: ConstStaticCell<Memory> = ConstStaticCell::new(MqttState::new());
        block_on(async {
            let (broker, _runner) = Mqtt::new(MEMORY.take(), MqttConfig::new("10.0.0.2:1883"));
            let tb = TestApp::start(app(broker)).await.expect("the app starts");
            let line = tb.broker::<Mqtt>();
            for _ in 0..30 {
                line.message(&Sent(&LARGE))
                    .to("line/frame")
                    .publish()
                    .await
                    .expect("the harness publishes");
            }

            line.subscriber("line/frame").assert_called(30);
            line.published::<()>("line/size")
                .assert_called(30)
                .with_raw(b"large");
            tb.shutdown().await.expect("the app stops");
        });
    }
}
