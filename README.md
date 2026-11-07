<h1 align="center">ruststream-rust-mqtt</h1>

<p align="center">
  <i>The MQTT 5 broker of the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: no heap on a device with embassy, and a host on compio.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-rust-mqtt/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-rust-mqtt/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/ruststream-rust-mqtt"><img src="https://img.shields.io/crates/v/ruststream-rust-mqtt.svg" alt="crates.io"></a>
  <a href="https://docs.rs/ruststream-rust-mqtt"><img src="https://img.shields.io/docsrs/ruststream-rust-mqtt" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/MSRV-1.95-blue.svg" alt="MSRV 1.95">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
</p>

---

`ruststream-rust-mqtt` connects a RustStream service to an MQTT 5 server. Handlers, routing, codecs
and middleware come from the framework; this crate is the transport.

On a device the crate needs no heap. The board declares the broker's memory as a `static`, a PUBLISH
is read straight into a receive buffer of it, and the handler borrows its payload from that buffer.
A host on compio, or a device that has a heap, takes the heap variant over the
[`rust-mqtt`](https://crates.io/crates/rust-mqtt) client.

## Features

- **No heap on a device:** `MqttState<MESSAGES, PACKET, SUBSCRIPTIONS>` is every buffer the broker
  owns, as one value of known size. Nothing is allocated, at startup or per message.
- **Deliveries that borrow:** a handler reads its payload as bytes borrowed from the receive
  buffer, and a reply is encoded straight into a send buffer.
- **Two runtimes:** a device on embassy, where the connection's `MqttRunner` is a task of the board
  over `EmbassyNetwork`, and a host on compio, where `MqttBroker::compio` starts the runner itself.
- **A `Network` of the board's own:** the runner connects over any link that reads and writes
  through `embedded-io-async`.
- **Topics and filters:** `MqttTopic` for a topic, `MqttFilter` for the `+` and `#` wildcards, each
  with a quality of service and an optional share group.
- **QoS-aware acknowledgement:** an `ack` sends the PUBACK of QoS 1 and the PUBREC of QoS 2; QoS 0
  and `retry()` report `Unsupported`, because MQTT has no packet for them.
- **Retries within the session:** `retry_after` publishes a copy after a delay, to the topic of an
  `MqttTopic` or to the topic the mount names for an `MqttFilter`.
- **Publishing:** `MqttPublish` fixes the QoS and the retain flag of a mount, and the steps
  `qos(..)` and `retain(..)` change one message.
- **Headers as user properties,** with `content-type`, `reply-to` and `correlation-id` as the
  native MQTT 5 properties.
- **Persistent sessions:** the server keeps the session of a client id and redelivers what was
  left unacknowledged.
- **Tests run the production app** (feature `testing`): the embedded and the compio `TestApp`
  connect `MqttBroker` in process, with no server.

## Cargo features

All are off by default.

| Feature   | Adds |
| --------- | ---- |
| `embassy` | `EmbassyNetwork`, the runner's network over an embassy-net stack. |
| `alloc`   | The heap variant: the broker runs over the `rust-mqtt` client and its settings hold owned strings. |
| `compio`  | `CompioNetwork` and `MqttBroker::compio` for a host on compio; implies `alloc`. |
| `testing` | The in-process transport the embedded and the compio `TestApp` connect. |

## Install

```toml
[dependencies]
# A device on embassy, without a heap.
ruststream = { version = "0.8", default-features = false, features = ["embassy", "macros"] }
ruststream-rust-mqtt = { version = "0.8", features = ["embassy"] }
static_cell = "2"

[dev-dependencies]
ruststream-rust-mqtt = { version = "0.8", features = ["embassy", "testing"] }
```

A compio host takes `features = ["compio", "macros", "json"]` on `ruststream` and
`features = ["compio"]` on this crate. A device that has a heap takes `features = ["alloc", "embassy"]`
on this crate and the `alloc` feature on `ruststream`.

## Write a device service

```rust
use embassy_executor::Spawner;
use embassy_net::Stack;
use ruststream::{Deserialized, Outgoing, Serialized};
use ruststream_rust_mqtt::prelude::*;
use ruststream_rust_mqtt::{EmbassyNetwork, MqttRunner, MqttState};
use static_cell::ConstStaticCell;

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
```

`MqttState<MESSAGES, PACKET, SUBSCRIPTIONS>` fixes the memory: how many deliveries are held at once
(default 4), the largest packet in bytes (1024) and how many subscriptions are open (8). The memory
is about `(MESSAGES + 2) * PACKET` bytes. While every receive buffer is held, the runner stops
reading the connection.

## Write a compio service

The heap variant: the broker starts its runner on the core that connects it.

```rust
use ruststream::Outgoing;
use ruststream::runtime::compio::{App, AppInfo, RustStream};
use ruststream_rust_mqtt::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "sensors/temp")]
struct Reading {
    celsius: i16,
}

#[subscriber(MqttTopic::new("sensors/temp"))]
async fn store(reading: &Reading) -> HandlerOutcome {
    let _ = reading.celsius;
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = compio, cores = 1)]
fn app() -> impl App {
    let config = MqttConfig::new("mqtt://localhost:1883").client_id("collector");
    RustStream::new(AppInfo::new("collector", "0.1.0"))
        .with_broker(MqttBroker::compio(config), |b| b.include(store))
}
```

`#[ruststream::app(runtime = compio)]` generates `main`, so the binary understands `run`.

## Test it

The embedded `TestApp` runs the device service on the host with `MqttBroker` in process, with no
server and no runner. The test declares its own memory, as the board does:

```rust
use ruststream::testing::embedded::TestApp;

#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

static MEMORY: ConstStaticCell<MqttState> = ConstStaticCell::new(MqttState::new());

embassy_futures::block_on(async {
    let (broker, _runner) = MqttBroker::new(MEMORY.take(), MqttConfig::new("192.168.1.10:1883"));
    let tb = TestApp::start(app(broker)).await?;
    let valve = tb.broker::<MqttBroker>();
    valve.message(&Sent(b"open")).to("valves/v1/set").publish().await?;

    valve
        .subscriber("valves/v1/set")
        .assert_called_once()
        .with_raw(b"open")
        .settled(HandlerOutcome::ack());
    valve
        .published::<()>("valves/v1/state")
        .assert_called_once()
        .with_raw(b"opened");
    tb.shutdown().await?;
    Ok::<_, Box<dyn std::error::Error>>(())
})
```

## Documentation

- [Guides, components and architecture](https://powersemmi.github.io/ruststream-rust-mqtt/latest/)
- [API reference](https://docs.rs/ruststream-rust-mqtt)
- [RustStream](https://powersemmi.github.io/ruststream/latest/), the framework

## License

Apache-2.0. See [LICENSE](LICENSE).
