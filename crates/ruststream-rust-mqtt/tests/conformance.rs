//! The conformance suites of the local family: the routing suite against the in-process
//! transport, and the lifecycle, refusal and readiness suites against the live stand.

use ruststream::PublishPolicy;
use ruststream::conformance::harness;
use ruststream::conformance::lifecycle::Unsent;
use ruststream_rust_mqtt::{
    ConnectedMqttBroker, MqttBroker, MqttConfig, MqttPublish, MqttPublisher, MqttTopic,
};

use crate::support::{live_broker, stand};

/// The in-process transport needs no runner: the harness connects it in process.
fn in_process() -> MqttBroker {
    let (broker, _runner) = MqttBroker::new(MqttConfig::new("mqtt://127.0.0.1:1883"));
    broker
}

fn publisher(connected: &ConnectedMqttBroker) -> MqttPublisher {
    futures::executor::block_on(MqttPublish::default().pair(connected)).expect("the policy pairs")
}

#[tokio::test(flavor = "current_thread")]
async fn the_in_process_transport_passes_the_routing_suite() {
    harness::run_local_suite(in_process).await;
}

// The closures stay: their bounds are higher-ranked, and a bare path binds one lifetime.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_live_broker_keeps_the_lifecycle() {
    let Some(url) = stand() else { return };
    harness::local_lifecycle(|| live_broker(&url), |name| MqttTopic::new(name), publisher).await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
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

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_live_broker_keeps_the_readiness_contract() {
    let Some(url) = stand() else { return };
    harness::local_readiness(|| live_broker(&url), |name| MqttTopic::new(name), publisher).await;
}
