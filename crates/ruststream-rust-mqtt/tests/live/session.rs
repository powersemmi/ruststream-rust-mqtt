//! Deliveries that outlive a connection: a persistent session redelivers what the handler left
//! unacknowledged, and does not redeliver what it acknowledged.

use core::time::Duration;

use ruststream::{ConnectedBroker, IncomingMessage, Settle};
use ruststream_rust_mqtt::{MqttConfig, MqttPublish, Qos};

use crate::support::{
    connect, live_broker, live_broker_with, next, open, publish, publisher_of, stand, topic,
    unique_client_id,
};

/// A persistent session of the client `id`, kept for a minute after the connection ends.
fn persistent(url: &str, id: &str) -> MqttConfig {
    MqttConfig::new(url)
        .client_id(id)
        .persistent_session(Duration::from_secs(60))
}

async fn redelivers_what_was_not_acknowledged(qos: Qos) {
    let Some(url) = stand() else { return };
    let topic = topic(&format!("redelivery-{qos:?}"));
    let id = unique_client_id("redelivery");

    let first = connect(live_broker_with(persistent(&url, &id))).await;
    let mut subscriber = open(&first, &topic, qos).await;
    let sender = connect(live_broker(&url)).await;
    let publisher = publisher_of(&sender, MqttPublish::default().qos(qos)).await;
    publish(&publisher, &topic, b"left unacknowledged").await;
    let delivery = next(&mut subscriber).await;
    assert_eq!(delivery.payload(), b"left unacknowledged");
    // The handler never settles it: the delivery is dropped, and the session ends.
    drop(delivery);
    drop(subscriber);
    first.shutdown().await.expect("the session ends");

    let second = connect(live_broker_with(persistent(&url, &id))).await;
    let mut subscriber = open(&second, &topic, qos).await;
    let again = next(&mut subscriber).await;
    assert_eq!(again.payload(), b"left unacknowledged");
    again.ack().await.expect("the redelivery acknowledges");
    drop(subscriber);
    second.shutdown().await.expect("the session ends");
    sender.shutdown().await.expect("the sender ends");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_persistent_session_redelivers_a_qos_1_delivery_left_unacknowledged() {
    redelivers_what_was_not_acknowledged(Qos::AtLeastOnce).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_persistent_session_redelivers_a_qos_2_delivery_left_unacknowledged() {
    redelivers_what_was_not_acknowledged(Qos::ExactlyOnce).await;
}

/// The server keeps what was published to a persistent session's subscriptions while the client
/// was away, and sends it right after the CONNACK, before the app opens its subscriptions again:
/// the runner holds those deliveries for the subscription that takes them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn what_arrived_while_the_client_was_away_reaches_the_subscription_that_reopens() {
    let Some(url) = stand() else { return };
    let topic = topic("offline");
    let id = unique_client_id("offline");

    let first = connect(live_broker_with(persistent(&url, &id))).await;
    let subscriber = open(&first, &topic, Qos::AtLeastOnce).await;
    drop(subscriber);
    first.shutdown().await.expect("the session ends");

    let sender = connect(live_broker(&url)).await;
    let publisher = publisher_of(&sender, MqttPublish::default().qos(Qos::AtLeastOnce)).await;
    publish(&publisher, &topic, b"sent while away").await;

    let second = connect(live_broker_with(persistent(&url, &id))).await;
    // An app does its other startup work between connecting a broker and opening the broker's
    // subscriptions; the server's queued message arrives meanwhile.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut subscriber = open(&second, &topic, Qos::AtLeastOnce).await;
    let kept = next(&mut subscriber).await;
    assert_eq!(kept.payload(), b"sent while away");
    kept.ack().await.expect("the kept delivery acknowledges");
    drop(subscriber);
    second.shutdown().await.expect("the session ends");
    sender.shutdown().await.expect("the sender ends");
}
