//! An MQTT 5 broker for RustStream, for devices on the embassy runtime and for hosts on the compio
//! runtime.
//!
//! The crate has two variants, which follow the framework's own:
//!
//! - The device variant, the default: no heap. The board declares the broker's memory, an
//!   `MqttState` of fixed size, as a `static`. A delivery holds the receive buffer its PUBLISH
//!   arrived in and borrows its topic and payload from it, and a publish is encoded straight into
//!   a send buffer. The crate speaks the protocol itself through the `zzq-proto` codec.
//! - The heap variant, with the `alloc` feature (which `compio` turns on): the broker runs over the
//!   rust-mqtt client, and its settings and descriptors hold owned strings.
//!
//! The items:
//!
//! - [`MqttBroker`], [`ConnectedMqttBroker`], [`ClosedMqttBroker`]: the broker's lifecycle.
//!   [`MqttBroker::new`] returns the broker with the [`MqttRunner`] that owns its connection; a
//!   device spawns the runner as a task of its own.
//! - [`MqttConfig`]: where the broker connects and as whom.
//! - [`MqttTopic`], [`MqttFilter`], [`Qos`]: the subscription descriptors, for one topic or for a
//!   filter with wildcards, and their quality of service.
//! - [`MqttSubscriber`], [`MqttMessage`]: a subscription's deliveries; the handler's settlement
//!   sends the acknowledgement.
//! - [`MqttPublish`], [`MqttPublisher`], [`MqttPublishOptions`], [`MqttPublishSteps`]: the publish
//!   policy, the live publisher, and the per-message quality of service and retain flag.
//! - [`Network`]: the link the runner connects over; the crate implements it over embassy-net
//!   (feature `embassy`) and over compio (feature `compio`).
//! - `MqttState` (device variant): the memory of a device broker, declared as a `static`; its
//!   three numbers fix the deliveries held at once, the largest packet and the open subscriptions.
//! - [`prelude`]: the crate's types beside the framework's prelude.
//!
//! # Where things are
//!
//! The site at <https://powersemmi.github.io/ruststream-rust-mqtt/latest/> teaches each topic on a
//! page of its own:
//!
//! - Start with [getting started on a device](https://powersemmi.github.io/ruststream-rust-mqtt/latest/guides/getting-started-device/)
//!   or [on a compio host](https://powersemmi.github.io/ruststream-rust-mqtt/latest/guides/getting-started-compio/).
//! - The broker, its runner and the two variants: [`MqttBroker`](https://powersemmi.github.io/ruststream-rust-mqtt/latest/components/broker/).
//! - The device broker's memory: [`MqttState`](https://powersemmi.github.io/ruststream-rust-mqtt/latest/components/broker/memory/).
//! - The settings: [`MqttConfig`](https://powersemmi.github.io/ruststream-rust-mqtt/latest/components/broker/configuration/).
//! - The link: [`Network`](https://powersemmi.github.io/ruststream-rust-mqtt/latest/components/broker/network/).
//! - Subscriptions: [`MqttTopic`](https://powersemmi.github.io/ruststream-rust-mqtt/latest/components/subscriptions/)
//!   and [`MqttFilter`](https://powersemmi.github.io/ruststream-rust-mqtt/latest/components/subscriptions/mqtt-filter/).
//! - Settlement: [acknowledgement](https://powersemmi.github.io/ruststream-rust-mqtt/latest/components/settlement/)
//!   and [retry after](https://powersemmi.github.io/ruststream-rust-mqtt/latest/components/settlement/retry-after/).
//! - Publishing: [`MqttPublish`](https://powersemmi.github.io/ruststream-rust-mqtt/latest/components/publishing/).
//! - Tests: [the embedded `TestApp`](https://powersemmi.github.io/ruststream-rust-mqtt/latest/components/testing/)
//!   and [the live stand](https://powersemmi.github.io/ruststream-rust-mqtt/latest/components/testing/live/).
//! - [What a delivery and a publish cost](https://powersemmi.github.io/ruststream-rust-mqtt/latest/architecture/cost/).
//!
//! # Examples
//!
//! A device service: the board declares the broker's memory, builds the broker and its runner,
//! spawns the runner's task, and hands the broker to the app.
//!
//! ```
//! # #[cfg(not(feature = "alloc"))]
//! # mod demo {
//! use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts};
//! use ruststream::{Deserialized, subscriber};
//! use ruststream_rust_mqtt::{MqttBroker, MqttConfig, MqttRunner, MqttState, MqttTopic, Network};
//! use static_cell::ConstStaticCell;
//!
//! /// A reading as the sensor sends its bytes, borrowed from the broker's receive buffer.
//! #[derive(Deserialized)]
//! struct Reading<'a>(&'a [u8]);
//!
//! #[subscriber(MqttTopic::new("sensors/temp"))]
//! async fn on_reading(reading: &Reading<'_>) -> HandlerOutcome {
//!     let _ = reading.0;
//!     HandlerOutcome::ack()
//! }
//!
//! #[ruststream::app(runtime = embassy)]
//! pub fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
//!     EmbeddedApp::new().with_broker(broker, |b| b.include(on_reading))
//! }
//!
//! /// The broker's memory: four deliveries of up to 1 KiB, and eight subscriptions.
//! static MQTT: ConstStaticCell<MqttState> = ConstStaticCell::new(MqttState::new());
//!
//! /// The body of the board's MQTT task.
//! pub async fn mqtt(runner: MqttRunner, network: impl Network) -> ! {
//!     runner.run(network).await
//! }
//!
//! /// What `main` does before it spawns the two tasks.
//! pub fn board() -> (MqttBroker, MqttRunner) {
//!     MqttBroker::new(MQTT.take(), MqttConfig::new("192.168.1.10:1883").client_id("sensor-7"))
//! }
//! # }
//! # fn main() {}
//! ```
//!
//! A host service on compio builds the heap variant, and the broker starts its runner itself:
//!
//! ```
//! # #[cfg(feature = "compio")]
//! # mod demo {
//! use ruststream_rust_mqtt::{MqttBroker, MqttConfig};
//!
//! /// The gateway's broker; the app connects it on the core that runs it.
//! pub fn broker() -> MqttBroker {
//!     MqttBroker::compio(MqttConfig::new("mqtt://broker.local:1883").client_id("gateway"))
//! }
//! # }
//! # fn main() {}
//! ```

#![cfg_attr(not(feature = "compio"), no_std)]
// Without a multi-thread runtime the broker's state is shared by `Rc` or by a `static`, so its
// futures are not `Send` by design.
#![allow(clippy::future_not_send)]

#[cfg(any(feature = "alloc", feature = "testing"))]
extern crate alloc;

// The framework holds one variant per build: a device build whose framework turned `alloc` on
// through another crate needs this crate's heap variant too.
#[cfg(all(ruststream_alloc, not(feature = "alloc")))]
compile_error!(
    "the framework is the heap variant in this build (a feature such as `json` turned `alloc` on): \
     enable the `alloc` feature of ruststream-rust-mqtt as well"
);

#[cfg(feature = "embassy")]
mod embassy;
mod endpoint;
#[cfg(not(feature = "alloc"))]
mod fixed;
#[cfg(feature = "alloc")]
mod heap;
mod network;
mod options;
pub mod prelude;
mod qos;
mod topic;

#[cfg(feature = "embassy")]
pub use embassy::{EmbassyConnectError, EmbassyNetwork};
#[cfg(not(feature = "alloc"))]
pub use fixed::{
    ClosedMqttBroker, ConnectedMqttBroker, MqttBatch, MqttBroker, MqttConfig, MqttError,
    MqttFilter, MqttMessage, MqttPublish, MqttPublisher, MqttRunner, MqttState, MqttSubscriber,
    MqttTopic,
};
#[cfg(feature = "compio")]
pub use heap::compio::{CompioNetwork, CompioStream};
#[cfg(feature = "alloc")]
pub use heap::{
    ClosedMqttBroker, ConnectedMqttBroker, MqttBroker, MqttConfig, MqttError, MqttFilter,
    MqttMessage, MqttPublish, MqttPublisher, MqttRunner, MqttSubscriber, MqttTopic,
};
pub use network::Network;
pub use options::{MqttPublishOptions, MqttPublishSteps};
pub use qos::Qos;

/// The header a delivery's partition key is read from: a publisher that wants per-key ordering
/// in keyed workers sets it as a user property.
pub const PARTITION_KEY_HEADER: &str = "partition-key";
