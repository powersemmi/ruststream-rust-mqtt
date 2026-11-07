//! Deliveries that outlive a connection: a persistent session redelivers what the handler left
//! unacknowledged, and does not redeliver what it acknowledged.

use core::time::Duration;

use ruststream::{ConnectedBroker, IncomingMessage, Settle};
use ruststream_rust_mqtt::{MqttConfig, MqttPublish, Qos};

use crate::support::{
    broker_with, connect, leak, live_broker, next, open, publish, publisher_of, stand, topic,
    unique_client_id,
};

/// A persistent session of the client `id`, kept for a minute after the connection ends.
fn persistent(url: &str, id: &'static str) -> MqttConfig {
    MqttConfig::new(leak(url.to_owned()))
        .client_id(id)
        .persistent_session(Duration::from_secs(60))
}

async fn redelivers_what_was_not_acknowledged(qos: Qos, name: &str) {
    let Some(url) = stand() else { return };
    let topic = topic(name);
    let id = unique_client_id("redelivery");

    let first = connect(broker_with::<4, 1024, 8>(persistent(&url, id))).await;
    let mut subscriber = open(&first, topic, qos).await;
    let sender = connect(live_broker(&url)).await;
    let publisher = publisher_of(&sender, MqttPublish::default().qos(qos)).await;
    publish(&publisher, topic, b"left unacknowledged").await;
    let delivery = next(&mut subscriber).await;
    assert_eq!(delivery.payload(), b"left unacknowledged");
    // The handler never settles it: the delivery is dropped, and the session ends.
    drop(delivery);
    drop(subscriber);
    first.shutdown().await.expect("the session ends");

    let second = connect(broker_with::<4, 1024, 8>(persistent(&url, id))).await;
    let mut subscriber = open(&second, topic, qos).await;
    let again = next(&mut subscriber).await;
    assert_eq!(again.payload(), b"left unacknowledged");
    // Acknowledged after the server's redelivery: a QoS 2 handshake runs to its end.
    again.ack().await.expect("the redelivery acknowledges");
    publish(&publisher, topic, b"marker").await;
    let marker = next(&mut subscriber).await;
    assert_eq!(marker.payload(), b"marker");
    marker.ack().await.expect("the marker acknowledges");
    drop(subscriber);
    second.shutdown().await.expect("the session ends");

    // The settled delivery is not sent a third time.
    let third = connect(broker_with::<4, 1024, 8>(persistent(&url, id))).await;
    let mut subscriber = open(&third, topic, qos).await;
    publish(&publisher, topic, b"after").await;
    let after = next(&mut subscriber).await;
    assert_eq!(after.payload(), b"after");
    after.ack().await.expect("the delivery acknowledges");
    drop(subscriber);
    third.shutdown().await.expect("the session ends");
    sender.shutdown().await.expect("the sender ends");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_persistent_session_redelivers_a_qos_1_delivery_left_unacknowledged() {
    redelivers_what_was_not_acknowledged(Qos::AtLeastOnce, "redelivery-1").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_persistent_session_redelivers_a_qos_2_delivery_left_unacknowledged() {
    redelivers_what_was_not_acknowledged(Qos::ExactlyOnce, "redelivery-2").await;
}

/// The server keeps what was published to a persistent session's subscriptions while the client
/// was away, and sends it right after the CONNACK, before the app opens its subscriptions again:
/// the runner holds those deliveries for the subscription that takes them.
async fn kept_deliveries_wait_for_the_subscription(qos: Qos, name: &str) {
    let Some(url) = stand() else { return };
    let topic = topic(name);
    let id = unique_client_id("offline");

    let first = connect(broker_with::<4, 1024, 8>(persistent(&url, id))).await;
    let subscriber = open(&first, topic, qos).await;
    drop(subscriber);
    first.shutdown().await.expect("the session ends");

    let sender = connect(live_broker(&url)).await;
    let publisher = publisher_of(&sender, MqttPublish::default().qos(qos)).await;
    for index in 0..3u32 {
        publish(&publisher, topic, &index.to_be_bytes()).await;
    }

    let second = connect(broker_with::<4, 1024, 8>(persistent(&url, id))).await;
    // An app does its other startup work between connecting a broker and opening the broker's
    // subscriptions; the server's queued messages arrive meanwhile.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut subscriber = open(&second, topic, qos).await;
    for index in 0..3u32 {
        let kept = next(&mut subscriber).await;
        assert_eq!(kept.payload(), index.to_be_bytes());
        kept.ack().await.expect("the kept delivery acknowledges");
    }
    drop(subscriber);
    second.shutdown().await.expect("the session ends");
    sender.shutdown().await.expect("the sender ends");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn qos_1_messages_kept_while_the_client_was_away_reach_the_subscription_that_reopens() {
    kept_deliveries_wait_for_the_subscription(Qos::AtLeastOnce, "offline-1").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn qos_2_messages_kept_while_the_client_was_away_reach_the_subscription_that_reopens() {
    kept_deliveries_wait_for_the_subscription(Qos::ExactlyOnce, "offline-2").await;
}
