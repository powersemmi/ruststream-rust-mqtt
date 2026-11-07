//! The device variant: the broker over the `zzq-proto` codec, with every buffer in an
//! [`MqttState`] the board declares as a `static`. A delivery holds the receive buffer its PUBLISH
//! arrived in and borrows its topic and payload from it; a publish is encoded straight into a send
//! buffer.

mod broker;
mod config;
mod error;
mod lease;
mod message;
mod publisher;
mod runner;
mod source;
mod state;
mod subscriber;
#[cfg(feature = "testing")]
mod testing;
mod wire;

pub use broker::{ClosedMqttBroker, ConnectedMqttBroker, MqttBroker};
pub use config::MqttConfig;
pub use error::MqttError;
pub use message::MqttMessage;
pub use publisher::{MqttPublish, MqttPublisher};
pub use runner::MqttRunner;
pub use source::{MqttFilter, MqttTopic};
pub use state::MqttState;
pub use subscriber::{MqttBatch, MqttSubscriber};
