//! What a message is on its way through the server: its quality of service, its retain flag and
//! its headers.

use ruststream::Headers;
use ruststream::{AckError, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, Settle};
use ruststream_rust_mqtt::{MqttPublish, Qos};

use crate::support::{
    WAIT, connect, live_broker, next, next_if_any, open, publish, publisher_of, stand, topic,
};

async fn round_trip(qos: Qos, name: &str) {
    let Some(url) = stand() else { return };
    let topic = topic(name);
    let receiver = connect(live_broker(&url)).await;
    let mut subscriber = open(&receiver, topic, qos).await;
    let sender = connect(live_broker(&url)).await;
    let publisher = publisher_of(&sender, MqttPublish::default().qos(qos)).await;
    publish(&publisher, topic, b"through the server").await;

    let delivery = next(&mut subscriber).await;
    assert_eq!(delivery.payload(), b"through the server");
    assert_eq!(delivery.topic(), topic);
    assert_eq!(delivery.qos(), qos);
    match qos {
        // Nothing acknowledges a delivery at most once.
        Qos::AtMostOnce => assert!(matches!(delivery.ack().await, Err(AckError::Unsupported))),
        Qos::AtLeastOnce | Qos::ExactlyOnce => {
            delivery.ack().await.expect("the delivery acknowledges");
        }
    }
    drop(subscriber);
    receiver.shutdown().await.expect("the receiver ends");
    sender.shutdown().await.expect("the sender ends");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_qos_0_message_goes_through_the_server() {
    round_trip(Qos::AtMostOnce, "qos-0").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_qos_1_message_goes_through_the_server() {
    round_trip(Qos::AtLeastOnce, "qos-1").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_qos_2_message_goes_through_the_server() {
    round_trip(Qos::ExactlyOnce, "qos-2").await;
}

/// The server delivers at the lesser of the two: a `QoS` 2 publish to a `QoS` 0 subscription
/// arrives at most once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delivery_is_made_at_the_lesser_quality_of_service() {
    let Some(url) = stand() else { return };
    let topic = topic("lesser-qos");
    let receiver = connect(live_broker(&url)).await;
    let mut subscriber = open(&receiver, topic, Qos::AtMostOnce).await;
    let sender = connect(live_broker(&url)).await;
    let publisher = publisher_of(&sender, MqttPublish::default().qos(Qos::ExactlyOnce)).await;
    publish(&publisher, topic, b"downgraded").await;

    let delivery = next(&mut subscriber).await;
    assert_eq!(delivery.qos(), Qos::AtMostOnce);
    drop(delivery);
    drop(subscriber);
    receiver.shutdown().await.expect("the receiver ends");
    sender.shutdown().await.expect("the sender ends");
}

/// A packet that fills most of a receive buffer is read whole, at every quality of service.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_payload_that_nearly_fills_a_buffer_arrives_whole() {
    let Some(url) = stand() else { return };
    let topic = topic("big-payload");
    let payload: Vec<u8> = (0..900u32).map(|byte| (byte % 251) as u8).collect();
    let receiver = connect(live_broker(&url)).await;
    let mut subscriber = open(&receiver, topic, Qos::ExactlyOnce).await;
    let sender = connect(live_broker(&url)).await;
    let publisher = publisher_of(&sender, MqttPublish::default().qos(Qos::ExactlyOnce)).await;
    publish(&publisher, topic, &payload).await;

    let delivery = next(&mut subscriber).await;
    assert_eq!(delivery.payload(), payload);
    delivery.ack().await.expect("the delivery acknowledges");
    drop(subscriber);
    receiver.shutdown().await.expect("the receiver ends");
    sender.shutdown().await.expect("the sender ends");
}

/// The server keeps the last retained message of a topic for a subscriber that comes later, and an
/// empty retained message clears it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retained_message_waits_for_a_subscriber_that_comes_later() {
    let Some(url) = stand() else { return };
    let topic = topic("retained");
    let sender = connect(live_broker(&url)).await;
    let retained = publisher_of(&sender, MqttPublish::default().retain(true)).await;
    publish(&retained, topic, b"first").await;
    publish(&retained, topic, b"last").await;

    let late = connect(live_broker(&url)).await;
    let mut subscriber = open(&late, topic, Qos::AtLeastOnce).await;
    let delivery = next(&mut subscriber).await;
    assert_eq!(delivery.payload(), b"last");
    delivery
        .ack()
        .await
        .expect("the retained delivery acknowledges");
    assert!(
        next_if_any(&mut subscriber).await.is_none(),
        "the server keeps one retained message per topic"
    );
    drop(subscriber);

    publish(&retained, topic, b"").await;
    let after = connect(live_broker(&url)).await;
    let mut subscriber = open(&after, topic, Qos::AtLeastOnce).await;
    assert!(
        next_if_any(&mut subscriber).await.is_none(),
        "an empty retained message clears the topic"
    );
    drop(subscriber);
    late.shutdown().await.expect("the late subscriber ends");
    after.shutdown().await.expect("the last subscriber ends");
    sender.shutdown().await.expect("the sender ends");
}

/// User properties, the content type, the response topic and the correlation data go through
/// the server as headers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn headers_go_through_the_server() {
    let Some(url) = stand() else { return };
    let topic = topic("headers");
    let receiver = connect(live_broker(&url)).await;
    let mut subscriber = open(&receiver, topic, Qos::AtLeastOnce).await;
    let sender = connect(live_broker(&url)).await;
    let publisher = publisher_of(&sender, MqttPublish::default().qos(Qos::AtLeastOnce)).await;

    let mut headers = Headers::new();
    for (name, value) in [
        ("site", "plant-3"),
        ("shift", "night"),
        ("content-type", "application/json"),
        ("reply-to", "answers/plant-3"),
        ("correlation-id", "7f3a"),
    ] {
        headers.insert(name, value).expect("the headers fit");
    }
    tokio::time::timeout(
        WAIT,
        publisher.publish(
            OutgoingMessage::new(topic, b"{}").with_headers(headers),
            None,
        ),
    )
    .await
    .expect("the publish finishes in time")
    .expect("the publish goes out");

    let delivery = next(&mut subscriber).await;
    assert_eq!(delivery.payload(), b"{}");
    assert_eq!(delivery.headers().get_str("site"), Some("plant-3"));
    assert_eq!(delivery.headers().get_str("shift"), Some("night"));
    assert_eq!(
        delivery.headers().get_str("content-type"),
        Some("application/json")
    );
    assert_eq!(
        delivery.headers().get_str("reply-to"),
        Some("answers/plant-3")
    );
    assert_eq!(delivery.headers().get("correlation-id"), Some(&b"7f3a"[..]));
    delivery.ack().await.expect("the delivery acknowledges");
    drop(subscriber);
    receiver.shutdown().await.expect("the receiver ends");
    sender.shutdown().await.expect("the sender ends");
}
