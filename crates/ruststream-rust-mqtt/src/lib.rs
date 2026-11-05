//! An MQTT 5 broker for RustStream over the [rust-mqtt](https://docs.rs/rust-mqtt) client, for
//! devices on the embassy runtime and for hosts on the compio runtime.
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
//! - [`prelude`]: the crate's types beside the framework's prelude.
//!
//! The crate builds the framework's `alloc` variant: the client, rust-mqtt 0.6, gives each field it
//! receives a heap buffer of its own, so a delivery holds two of them, its topic and its payload,
//! moved in without a copy.
//!
//! # Examples
//!
//! A device service: the board builds the broker and its runner, spawns the runner's task, and
//! hands the broker to the app.
//!
//! ```
//! # mod demo {
//! use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts};
//! use ruststream::{Deserialized, subscriber};
//! use ruststream_rust_mqtt::{MqttBroker, MqttConfig, MqttRunner, MqttTopic, Network};
//!
//! /// A reading as the sensor sends its bytes.
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
//! /// The body of the board's MQTT task.
//! pub async fn mqtt(runner: MqttRunner, network: impl Network) -> ! {
//!     runner.run(network).await
//! }
//!
//! /// What `main` does before it spawns the two tasks.
//! pub fn board() -> (MqttBroker, MqttRunner) {
//!     MqttBroker::new(MqttConfig::new("192.168.1.10:1883").client_id("sensor-7"))
//! }
//! # }
//! # fn main() {}
//! ```

#![cfg_attr(not(feature = "compio"), no_std)]
// Without a multi-thread runtime the broker's state is shared by `Rc`, so its futures are not
// `Send` by design.
#![allow(clippy::future_not_send)]

extern crate alloc;

mod broker;
#[cfg(feature = "compio")]
mod compio;
mod config;
#[cfg(feature = "embassy")]
mod embassy;
mod error;
#[cfg(feature = "testing")]
mod in_process;
mod link;
mod message;
pub mod prelude;
mod publisher;
mod qos;
mod queue;
mod routes;
mod runner;
mod source;
mod subscriber;
mod topic;

pub use broker::{ClosedMqttBroker, ConnectedMqttBroker, MqttBroker};
#[cfg(feature = "compio")]
pub use compio::{CompioNetwork, CompioStream};
pub use config::MqttConfig;
#[cfg(feature = "embassy")]
pub use embassy::{EmbassyConnectError, EmbassyNetwork};
pub use error::MqttError;
pub use message::{MqttMessage, PARTITION_KEY_HEADER};
pub use publisher::{MqttPublish, MqttPublishOptions, MqttPublishSteps, MqttPublisher};
pub use qos::Qos;
pub use runner::{MqttRunner, Network};
pub use source::{MqttFilter, MqttTopic};
pub use subscriber::MqttSubscriber;
