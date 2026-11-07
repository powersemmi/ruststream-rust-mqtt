//! The runner's flow control against the live stand: windows larger than the session's
//! maximums, receive buffers all held by handlers, and the keep-alive.

use core::time::Duration;

use ruststream::{ConnectedBroker, IncomingMessage, Settle};
use ruststream_rust_mqtt::{MqttConfig, MqttPublish, Qos};

use crate::support::{
    broker_with, connect, leak, live_broker, next, next_if_any, open, publish, publisher_of, stand,
    topic, unique_client_id,
};

/// More messages in a row than the window of 16 in flight and the server's receive maximum hold:
/// each one is sent, delivered and acknowledged, so neither window fills up for good.
async fn more_than_a_window_in_a_row(qos: Qos, name: &str) {
    let Some(url) = stand() else { return };
    let topic = topic(name);
    let receiver = connect(live_broker(&url)).await;
    let mut subscriber = open(&receiver, topic, qos).await;
    let sender = connect(live_broker(&url)).await;
    let publisher = publisher_of(&sender, MqttPublish::default().qos(qos)).await;
    for index in 0..120u32 {
        publish(&publisher, topic, &index.to_be_bytes()).await;
    }
    for index in 0..120u32 {
        let delivery = next(&mut subscriber).await;
        assert_eq!(delivery.payload(), index.to_be_bytes());
        delivery.ack().await.expect("the delivery acknowledges");
    }
    drop(subscriber);
    receiver.shutdown().await.expect("the receiver ends");
    sender.shutdown().await.expect("the sender ends");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn more_qos_1_messages_than_a_window_go_out_and_come_back() {
    more_than_a_window_in_a_row(Qos::AtLeastOnce, "window-1").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn more_qos_2_messages_than_a_window_complete_their_handshakes() {
    more_than_a_window_in_a_row(Qos::ExactlyOnce, "window-2").await;
}

/// The broker announces a receive maximum one below its receive buffers. While handlers hold
/// every delivery the server may send, the runner reads nothing more; once they settle, the rest
/// arrives in order and nothing is lost.
async fn held_deliveries_stop_the_reading(qos: Qos, name: &str) {
    let Some(url) = stand() else { return };
    let topic = topic(name);
    let receiver = connect(live_broker(&url)).await;
    let mut subscriber = open(&receiver, topic, qos).await;
    let sender = connect(live_broker(&url)).await;
    let publisher = publisher_of(&sender, MqttPublish::default().qos(qos)).await;
    for index in 0..12u32 {
        publish(&publisher, topic, &index.to_be_bytes()).await;
    }

    // The memory holds 4 buffers: 3 deliveries are in hand, and the fourth waits at the server.
    let mut held = Vec::new();
    for index in 0..3u32 {
        let delivery = next(&mut subscriber).await;
        assert_eq!(delivery.payload(), index.to_be_bytes());
        held.push(delivery);
    }
    assert!(
        next_if_any(&mut subscriber).await.is_none(),
        "nothing more is delivered while the handlers hold every buffer"
    );
    for delivery in held {
        delivery.ack().await.expect("the delivery acknowledges");
    }
    for index in 3..12u32 {
        let delivery = next(&mut subscriber).await;
        assert_eq!(delivery.payload(), index.to_be_bytes());
        delivery.ack().await.expect("the delivery acknowledges");
    }
    assert!(next_if_any(&mut subscriber).await.is_none());
    drop(subscriber);
    receiver.shutdown().await.expect("the receiver ends");
    sender.shutdown().await.expect("the sender ends");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn qos_1_deliveries_held_by_handlers_stop_the_reading_and_resume_when_settled() {
    held_deliveries_stop_the_reading(Qos::AtLeastOnce, "held-1").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn qos_2_deliveries_held_by_handlers_stop_the_reading_and_resume_when_settled() {
    held_deliveries_stop_the_reading(Qos::ExactlyOnce, "held-2").await;
}

/// A subscription queue is as long as the memory's buffers, so a burst larger than that waits at
/// the server for the handler, and arrives whole.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_burst_larger_than_the_memory_is_delivered_without_loss() {
    let Some(url) = stand() else { return };
    let topic = topic("burst");
    let config = MqttConfig::new(leak(url.clone())).client_id(unique_client_id("burst"));
    let receiver = connect(broker_with::<2, 512, 2>(config)).await;
    let mut subscriber = open(&receiver, topic, Qos::AtLeastOnce).await;
    let sender = connect(live_broker(&url)).await;
    let publisher = publisher_of(&sender, MqttPublish::default().qos(Qos::AtLeastOnce)).await;
    for index in 0..40u32 {
        publish(&publisher, topic, &index.to_be_bytes()).await;
    }
    for index in 0..40u32 {
        let delivery = next(&mut subscriber).await;
        assert_eq!(delivery.payload(), index.to_be_bytes());
        delivery.ack().await.expect("the delivery acknowledges");
    }
    drop(subscriber);
    receiver.shutdown().await.expect("the receiver ends");
    sender.shutdown().await.expect("the sender ends");
}

/// With a keep-alive of one second, a connection that carries nothing for three is still up: the
/// runner pings, and the server does not close it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_connection_is_kept_alive_by_pings() {
    let Some(url) = stand() else { return };
    let topic = topic("keep-alive");
    let config = MqttConfig::new(leak(url.clone()))
        .client_id(unique_client_id("keep-alive"))
        .keep_alive(Duration::from_secs(1));
    let receiver = connect(broker_with::<4, 1024, 8>(config)).await;
    let mut subscriber = open(&receiver, topic, Qos::AtLeastOnce).await;
    // The subject is the wall clock of the server's keep-alive timer, 1.5 times the interval.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let sender = connect(live_broker(&url)).await;
    let publisher = publisher_of(&sender, MqttPublish::default()).await;
    publish(&publisher, topic, b"still here").await;
    let delivery = next(&mut subscriber).await;
    assert_eq!(delivery.payload(), b"still here");
    delivery.ack().await.expect("the delivery acknowledges");
    drop(subscriber);
    receiver.shutdown().await.expect("the receiver ends");
    sender.shutdown().await.expect("the sender ends");
}
