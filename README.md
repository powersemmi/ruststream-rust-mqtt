<h1 align="center">ruststream-rust-mqtt</h1>

<p align="center">
  <i>The MQTT 5 broker of the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework over the <code>rust-mqtt</code> client, for the embassy and compio runtimes.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-rust-mqtt/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-rust-mqtt/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/ruststream-rust-mqtt"><img src="https://img.shields.io/crates/v/ruststream-rust-mqtt.svg" alt="crates.io"></a>
  <a href="https://docs.rs/ruststream-rust-mqtt"><img src="https://img.shields.io/docsrs/ruststream-rust-mqtt" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/MSRV-1.95-blue.svg" alt="MSRV 1.95">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
</p>

---

`ruststream-rust-mqtt` connects a RustStream service to an MQTT 5 server over
[`rust-mqtt`](https://crates.io/crates/rust-mqtt). Handlers, routing, codecs and middleware come
from the framework; this crate is the transport.

## Features

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

## Install

```toml
[dependencies]
# A device on embassy.
ruststream = { version = "0.8", default-features = false, features = ["embassy", "macros", "json"] }
ruststream-rust-mqtt = { version = "0.8", features = ["embassy"] }

[dev-dependencies]
ruststream-rust-mqtt = { version = "0.8", features = ["testing"] }
```

A compio host takes `features = ["compio", "macros", "json"]` on `ruststream` and
`features = ["compio"]` on this crate.

## Write a device service

```rust
use embassy_executor::Spawner;
use embassy_net::Stack;
use ruststream::Outgoing;
use ruststream_rust_mqtt::prelude::*;
use ruststream_rust_mqtt::{EmbassyNetwork, MqttRunner};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "sensors/temp")]
struct Reading {
    celsius: i16,
}

/// Takes every reading the sensors publish.
#[subscriber(MqttTopic::new("sensors/temp"))]
async fn store(reading: &Reading) -> HandlerOutcome {
    let _ = reading.celsius;
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
pub fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| b.include(store))
}

/// The runner owns the connection, and the app starts only while it runs.
#[embassy_executor::task]
async fn mqtt(runner: MqttRunner, stack: Stack<'static>) -> ! {
    let mut rx = [0; 1024];
    let mut tx = [0; 1024];
    runner.run(EmbassyNetwork::new(stack, &mut rx, &mut tx)).await
}

/// What the board's `main` does once its network stack is up.
fn start(spawner: Spawner, stack: Stack<'static>) {
    let config = MqttConfig::new("192.168.1.10:1883").client_id("sensor-7");
    let (broker, runner) = MqttBroker::new(config);
    spawner.spawn(mqtt(runner, stack).expect("the pool holds the runner's task"));
    spawner.spawn(app::task(broker).expect("the pool holds the app's task"));
}
```

## Write a compio service

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
server and no runner:

```rust
use ruststream::testing::embedded::TestApp;

embassy_futures::block_on(async {
    let (broker, _runner) = MqttBroker::new(MqttConfig::new("192.168.1.10:1883"));
    let tb = TestApp::start(app(broker)).await?;
    let sensor = tb.broker::<MqttBroker>();
    sensor.message(&Reading { celsius: 21 }).publish().await?;

    sensor
        .subscriber("sensors/temp")
        .assert_called_once()
        .with(&Reading { celsius: 21 })
        .settled(HandlerOutcome::ack());
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
