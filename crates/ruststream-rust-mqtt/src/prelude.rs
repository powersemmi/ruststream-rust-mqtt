//! The crate's types beside the framework's prelude: one glob brings what a service writes.
//!
//! # Examples
//!
//! ```
//! # mod demo {
//! use ruststream_rust_mqtt::prelude::*;
//!
//! #[derive(Deserialized)]
//! struct Command<'a>(&'a [u8]);
//!
//! #[subscriber(MqttTopic::new("valves/v1/set").qos(Qos::ExactlyOnce))]
//! async fn set(command: &Command<'_>) -> HandlerOutcome {
//!     let _ = command.0;
//!     HandlerOutcome::ack()
//! }
//!
//! #[ruststream::app(runtime = embassy)]
//! pub fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
//!     EmbeddedApp::new().with_broker(broker, |b| b.include(set))
//! }
//! # }
//! # fn main() {}
//! ```

pub use ruststream::prelude::*;

pub use crate::{
    MqttBroker, MqttConfig, MqttFilter, MqttPublish, MqttPublish as Publish, MqttPublishOptions,
    MqttPublishSteps, MqttTopic, Qos,
};
