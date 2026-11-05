//! The edges of a session against the live stand: credentials, an address nothing listens on,
//! what the handles answer after a shutdown, packets that do not fit, and a publish dropped
//! while it waits.

use futures::{FutureExt, StreamExt};
use ruststream::{
    AckError, Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, Settle,
    Subscriber,
};
use ruststream_rust_mqtt::{MqttConfig, MqttError, MqttPublish, Qos};

use crate::support::{
    auth_stand, broker_with, connect, leak, live_broker, next, open, publish, publisher_of, stand,
    topic, unique_client_id,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_right_credentials_connect_and_wrong_ones_are_refused() {
    let Some((url, password)) = auth_stand() else {
        return;
    };
    let url = leak(url);
    let good = MqttConfig::new(url)
        .client_id(unique_client_id("auth-good"))
        .credentials("tester", leak(password));
    let connected = connect(broker_with::<4, 1024, 8>(good)).await;
    connected.shutdown().await.expect("the session ends");

    let bad = MqttConfig::new(url)
        .client_id(unique_client_id("auth-bad"))
        .credentials("tester", "not the password");
    let refused = broker_with::<4, 1024, 8>(bad).connect().await;
    assert!(
        matches!(refused, Err(MqttError::Refused { code: 0x87, .. })),
        "a wrong password is refused with `not authorized`: {refused:?}"
    );

    // The listener asks for credentials, so a client that sends none is refused too.
    let anonymous = MqttConfig::new(url).client_id(unique_client_id("auth-none"));
    let refused = broker_with::<4, 1024, 8>(anonymous).connect().await;
    assert!(
        matches!(refused, Err(MqttError::Refused { .. })),
        "{refused:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_address_nothing_listens_on_is_refused_at_connect() {
    // Port 9 (discard) is closed on the test host: the TCP connect itself is refused.
    let config = MqttConfig::new("127.0.0.1:9").client_id(unique_client_id("nowhere"));
    let refused = broker_with::<4, 1024, 8>(config).connect().await;
    assert!(
        matches!(refused, Err(MqttError::Connect { .. })),
        "a refused TCP connect is a connect error: {refused:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn after_a_shutdown_a_publish_is_refused_and_a_stream_ends() {
    let Some(url) = stand() else { return };
    let topic = topic("after-shutdown");
    let connected = connect(live_broker(&url)).await;
    let mut subscriber = open(&connected, topic, Qos::AtLeastOnce).await;
    let publisher = publisher_of(&connected, MqttPublish::default()).await;
    connected.shutdown().await.expect("the session ends");

    let refused = publisher
        .publish(OutgoingMessage::new(topic, b"too late"), None)
        .await
        .expect_err("a publish after shutdown is refused");
    assert!(matches!(refused.error(), MqttError::NotConnected));
    let message = refused
        .into_message()
        .expect("the refusal hands the message back");
    assert_eq!(message.payload(), b"too late");

    let mut deliveries = core::pin::pin!(subscriber.stream());
    assert!(
        deliveries.next().await.is_none(),
        "the stream ends with the session"
    );
}

/// A delivery the handler settles after the session ended names a packet the server no longer
/// waits for: the acknowledgement is refused, not sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delivery_acknowledged_after_the_session_ended_is_refused() {
    let Some(url) = stand() else { return };
    let topic = topic("late-ack");
    let receiver = connect(live_broker(&url)).await;
    let mut subscriber = open(&receiver, topic, Qos::AtLeastOnce).await;
    let sender = connect(live_broker(&url)).await;
    let publisher = publisher_of(&sender, MqttPublish::default()).await;
    publish(&publisher, topic, b"held").await;
    let delivery = next(&mut subscriber).await;
    receiver.shutdown().await.expect("the session ends");

    let refused = delivery
        .ack()
        .await
        .expect_err("the late acknowledgement is refused");
    assert!(
        matches!(refused, AckError::Broker(_)),
        "the refusal names the lost session, not an unsupported settlement: {refused:?}"
    );
    sender.shutdown().await.expect("the sender ends");
}

/// A publish larger than the state's packet is refused with its message and the sizes, however
/// often it is tried: a refusal returns the send buffer and the packet identifier it took.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_too_large_for_the_state_is_refused_with_its_message() {
    let Some(url) = stand() else { return };
    let topic = topic("too-large");
    let receiver = connect(live_broker(&url)).await;
    let mut subscriber = open(&receiver, topic, Qos::ExactlyOnce).await;
    let sender = connect(live_broker(&url)).await;
    let publisher = publisher_of(&sender, MqttPublish::default().qos(Qos::ExactlyOnce)).await;
    let payload = vec![7u8; 2000];
    // More refusals than the two send buffers and the 16 identifiers: none leaks.
    for _ in 0..40 {
        let refused = publisher
            .publish(OutgoingMessage::new(topic, &payload), None)
            .await
            .expect_err("a packet of 2 KiB does not fit 1 KiB");
        assert!(
            matches!(
                refused.error(),
                MqttError::PacketTooLarge { size, capacity: 1024 } if *size > 2000
            ),
            "{:?}",
            refused.error()
        );
        let message = refused.into_message().expect("the message comes back");
        assert_eq!(message.payload(), &payload[..]);
    }
    publish(&publisher, topic, b"fits").await;
    let delivery = next(&mut subscriber).await;
    assert_eq!(delivery.payload(), b"fits");
    delivery.ack().await.expect("the delivery acknowledges");
    drop(subscriber);
    receiver.shutdown().await.expect("the receiver ends");
    sender.shutdown().await.expect("the sender ends");
}

/// The CONNECT announces the largest packet the receive buffers hold, so the server keeps a larger
/// one from the session: it is dropped there, and the session goes on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delivery_larger_than_the_receive_buffer_is_kept_back_by_the_server() {
    let Some(url) = stand() else { return };
    let topic = topic("delivery-too-large");
    let config = MqttConfig::new(leak(url.clone())).client_id(unique_client_id("small"));
    let receiver = connect(broker_with::<4, 256, 8>(config)).await;
    let mut subscriber = open(&receiver, topic, Qos::AtLeastOnce).await;
    let sender = connect(live_broker(&url)).await;
    let publisher = publisher_of(&sender, MqttPublish::default().qos(Qos::AtLeastOnce)).await;
    publish(&publisher, topic, &[1u8; 600]).await;
    publish(&publisher, topic, b"small").await;

    let delivery = next(&mut subscriber).await;
    assert_eq!(delivery.payload(), b"small");
    delivery.ack().await.expect("the delivery acknowledges");
    drop(subscriber);
    receiver
        .shutdown()
        .await
        .expect("the session was never lost");
    sender.shutdown().await.expect("the sender ends");
}

/// A publish dropped while it waits for the runner gives its send buffer and identifier back: the
/// broker still publishes after more drops than it has buffers and identifiers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancelled_publish_leaves_the_broker_able_to_publish() {
    let Some(url) = stand() else { return };
    let topic = topic("cancelled");
    let receiver = connect(live_broker(&url)).await;
    let mut subscriber = open(&receiver, topic, Qos::AtLeastOnce).await;
    let sender = connect(live_broker(&url)).await;
    let publisher = publisher_of(&sender, MqttPublish::default().qos(Qos::AtLeastOnce)).await;

    // One poll takes the buffer and queues the packet, then the future is dropped on its wait for
    // the answer; with the runner running on another thread some drops land earlier, on a wait
    // for a buffer or an identifier.
    for index in 0..200u32 {
        let payload = index.to_be_bytes();
        let _ = publisher
            .publish(OutgoingMessage::new(topic, &payload), None)
            .now_or_never();
    }
    publish(&publisher, topic, b"marker").await;

    // The dropped publishes that were queued were written all the same; the marker closes the run.
    let mut seen = 0u32;
    loop {
        let delivery = next(&mut subscriber).await;
        let marker = delivery.payload() == b"marker";
        delivery.ack().await.expect("the delivery acknowledges");
        if marker {
            break;
        }
        seen += 1;
    }
    assert!(seen <= 200);
    // And the broker goes on: another run of publishes passes through.
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
