//! The conformance suites of the local family against the device variant: the routing suite
//! against the in-process transport, and the lifecycle, refusal and readiness suites against the
//! live stand.

use ruststream::PublishPolicy;
use ruststream_conformance::harness;
use ruststream_conformance::lifecycle::Unsent;
use ruststream_rust_mqtt::{
    ConnectedMqttBroker, MqttBroker, MqttConfig, MqttPublish, MqttPublisher, MqttState, MqttTopic,
};

use crate::support::{leak, live_broker, stand};

/// The in-process transport needs no runner: the harness connects it in process. The suite
/// publishes a run of messages before it reads them, so the memory holds 32 deliveries.
fn in_process() -> MqttBroker<32> {
    let state = Box::leak(Box::new(MqttState::<32>::new()));
    let (broker, _runner) = MqttBroker::new(state, MqttConfig::new("mqtt://127.0.0.1:1883"));
    broker
}

fn publisher(connected: &ConnectedMqttBroker) -> MqttPublisher {
    futures::executor::block_on(MqttPublish::default().pair(connected)).expect("the policy pairs")
}

fn topic(name: &str) -> MqttTopic {
    MqttTopic::new(leak(name.to_owned()))
}

#[tokio::test(flavor = "current_thread")]
async fn the_in_process_transport_passes_the_routing_suite() {
    harness::run_local_suite(in_process).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_live_broker_keeps_the_lifecycle() {
    let Some(url) = stand() else { return };
    harness::local_lifecycle(|| live_broker(&url), topic, publisher).await;
}

#[allow(clippy::redundant_closure)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_live_broker_returns_the_message_of_a_refused_publish() {
    let Some(url) = stand() else { return };
    harness::local_refused_publish_returns_message(
        || live_broker(&url),
        publisher,
        Unsent::Returned,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_live_broker_keeps_the_readiness_contract() {
    let Some(url) = stand() else { return };
    harness::local_readiness(|| live_broker(&url), topic, publisher).await;
}
