//! The runner's flow control against the live stand: windows larger than the session's
//! maximums, a full subscription queue, and the keep-alive.

use core::time::Duration;

use ruststream::{ConnectedBroker, IncomingMessage, Settle};
use ruststream_rust_mqtt::{MqttConfig, MqttPublish, Qos};

use crate::support::{
    connect, live_broker, live_broker_with, next, open, publish, publisher_of, stand, topic,
    unique_client_id,
};

/// More messages in a row than the session's windows hold (32 in flight each way): each one is
/// sent, delivered and acknowledged, so neither window fills up for good.
async fn more_than_a_window_in_a_row(qos: Qos) {
    let Some(url) = stand() else { return };
    let topic = topic(&format!("window-{qos:?}"));
    let receiver = connect(live_broker(&url)).await;
    let mut subscriber = open(&receiver, &topic, qos).await;
    let sender = connect(live_broker(&url)).await;
    let publisher = publisher_of(&sender, MqttPublish::default().qos(qos)).await;
    for index in 0..80u32 {
        publish(&publisher, &topic, &index.to_be_bytes()).await;
    }
    for index in 0..80u32 {
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
    more_than_a_window_in_a_row(Qos::AtLeastOnce).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn more_qos_2_messages_than_a_window_complete_their_handshakes() {
    more_than_a_window_in_a_row(Qos::ExactlyOnce).await;
}

/// A subscription holds 16 deliveries; the runner stops reading while it is full and loses none.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_subscription_holds_the_runner_back_without_losing_a_message() {
    let Some(url) = stand() else { return };
    let topic = topic("full-queue");
    let receiver = connect(live_broker(&url)).await;
    let mut subscriber = open(&receiver, &topic, Qos::AtLeastOnce).await;
    let sender = connect(live_broker(&url)).await;
    let publisher = publisher_of(&sender, MqttPublish::default()).await;
    for index in 0..40u32 {
        publish(&publisher, &topic, &index.to_be_bytes()).await;
    }
    // Every message is on its way before the handler takes the first.
    tokio::time::sleep(Duration::from_millis(300)).await;
    for index in 0..40u32 {
        let delivery = next(&mut subscriber).await;
        assert_eq!(delivery.payload(), index.to_be_bytes());
        delivery.ack().await.expect("the delivery acknowledges");
    }
    drop(subscriber);
    receiver.shutdown().await.expect("the receiver ends");
    sender.shutdown().await.expect("the sender ends");
}

/// With a keep-alive of one second, a connection that carries nothing for three is still up:
/// the runner pings, and the server does not close it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_connection_is_kept_alive_by_pings() {
    let Some(url) = stand() else { return };
    let topic = topic("keep-alive");
    let config = MqttConfig::new(&url)
        .client_id(unique_client_id("keep-alive"))
        .keep_alive(Duration::from_secs(1));
    let receiver = connect(live_broker_with(config)).await;
    let mut subscriber = open(&receiver, &topic, Qos::AtLeastOnce).await;
    // The subject is the wall clock of the server's keep-alive timer, 1.5 times the interval.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let sender = connect(live_broker(&url)).await;
    let publisher = publisher_of(&sender, MqttPublish::default()).await;
    publish(&publisher, &topic, b"still here").await;
    let delivery = next(&mut subscriber).await;
    assert_eq!(delivery.payload(), b"still here");
    delivery.ack().await.expect("the delivery acknowledges");
    drop(subscriber);
    receiver.shutdown().await.expect("the receiver ends");
    sender.shutdown().await.expect("the sender ends");
}
