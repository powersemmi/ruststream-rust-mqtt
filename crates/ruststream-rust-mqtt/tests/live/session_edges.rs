//! The edges of a session against the live stand: credentials, an address nothing listens on,
//! and what the handles answer after a shutdown.

use futures::StreamExt;
use ruststream::{Broker, ConnectedBroker, OutgoingMessage, Publisher, Subscriber};
use ruststream_rust_mqtt::{MqttConfig, MqttError, MqttPublish, Qos};

use crate::support::{
    auth_stand, connect, live_broker, live_broker_with, made_up_password, open, publisher_of,
    stand, topic, unique_client_id,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_right_credentials_connect_and_wrong_ones_are_refused() {
    let Some((url, password)) = auth_stand() else {
        return;
    };
    let good = MqttConfig::new(&url)
        .client_id(unique_client_id("auth-good"))
        .credentials("tester", password);
    let connected = connect(live_broker_with(good)).await;
    connected.shutdown().await.expect("the session ends");

    let bad = MqttConfig::new(&url)
        .client_id(unique_client_id("auth-bad"))
        .credentials("tester", made_up_password());
    let refused = live_broker_with(bad).connect().await;
    assert!(
        matches!(refused, Err(MqttError::Connect { .. })),
        "a wrong password is refused at connect: {refused:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_address_nothing_listens_on_is_refused_at_connect() {
    // Port 9 (discard) is closed on the test host: the TCP connect itself is refused.
    let config = MqttConfig::new("127.0.0.1:9").client_id(unique_client_id("nowhere"));
    let refused = live_broker_with(config).connect().await;
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
    let mut subscriber = open(&connected, &topic, Qos::AtLeastOnce).await;
    let publisher = publisher_of(&connected, MqttPublish::default()).await;
    connected.shutdown().await.expect("the session ends");

    let refused = publisher
        .publish(OutgoingMessage::new(&topic, b"too late"), None)
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
