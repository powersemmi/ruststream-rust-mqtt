//! What the suites share: the network the live suites run the runner over, and the stand's
//! address.

use core::pin::pin;
use core::time::Duration;
use std::env;

use embedded_io_adapters::tokio_1::FromTokio;
use futures::StreamExt;
use ruststream::{
    Broker, OutgoingMessage, PublishPolicy, Publisher, Subscriber, SubscriptionSource,
};
use ruststream_rust_mqtt::{
    ConnectedMqttBroker, MqttBroker, MqttConfig, MqttMessage, MqttPublish, MqttPublisher,
    MqttSubscriber, MqttTopic, Network, Qos,
};
use tokio::net::TcpStream;
use tokio::time::timeout;

/// The runner's network on a tokio host: a TCP stream and tokio's clock. A tokio read is
/// cancel-safe, which is what the runner asks of a stream.
pub(crate) struct TokioNetwork;

impl Network for TokioNetwork {
    type Stream<'net> = FromTokio<TcpStream>;
    type Error = std::io::Error;

    async fn connect(
        &mut self,
        host: &str,
        port: u16,
    ) -> Result<FromTokio<TcpStream>, std::io::Error> {
        let stream = TcpStream::connect((host, port)).await?;
        stream.set_nodelay(true)?;
        Ok(FromTokio::new(stream))
    }

    async fn sleep(duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

/// The stand's anonymous listener, or `None` where no stand runs.
///
/// # Panics
///
/// Where `RUSTSTREAM_REQUIRE_LIVE` is set and the stand's address is not: the recipe that starts
/// the stand sets both, so a live test that would skip itself there is a fault.
pub(crate) fn stand() -> Option<String> {
    let url = env::var("MQTT_TEST_URL").ok();
    assert!(
        url.is_some() || env::var_os("RUSTSTREAM_REQUIRE_LIVE").is_none(),
        "RUSTSTREAM_REQUIRE_LIVE is set, so MQTT_TEST_URL must name the stand"
    );
    url
}

/// The stand's authenticated listener and the password of its user `tester`, or `None` where no
/// stand runs.
///
/// # Panics
///
/// Where `RUSTSTREAM_REQUIRE_LIVE` is set and either variable is not.
pub(crate) fn auth_stand() -> Option<(String, String)> {
    let url = env::var("MQTT_TEST_AUTH_URL").ok();
    let password = env::var("MQTT_TEST_AUTH_PASSWORD").ok();
    assert!(
        (url.is_some() && password.is_some()) || env::var_os("RUSTSTREAM_REQUIRE_LIVE").is_none(),
        "RUSTSTREAM_REQUIRE_LIVE is set, so MQTT_TEST_AUTH_URL and MQTT_TEST_AUTH_PASSWORD must \
         name the authenticated listener"
    );
    url.zip(password)
}

/// A client identifier no other test of this run uses.
pub(crate) fn unique_client_id(label: &str) -> String {
    static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("rs-rust-mqtt-{label}-{}-{id}", std::process::id())
}

/// A broker on the stand, as a client of its own, whose runner runs on a thread of its own.
///
/// The client keeps a predicate that is not `Sync`, so the runner is not a task a multi-thread
/// runtime moves: it gets a current-thread runtime, as a device gives it a task.
pub(crate) fn live_broker(url: &str) -> MqttBroker {
    live_broker_with(MqttConfig::new(url).client_id(unique_client_id("live")))
}

/// A broker with the given settings, run like [`live_broker`].
pub(crate) fn live_broker_with(config: MqttConfig) -> MqttBroker {
    let (broker, runner) = MqttBroker::new(config);
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the runner's runtime builds")
            .block_on(runner.run(TokioNetwork))
    });
    broker
}

/// How long a live test waits for something the stand owes it.
pub(crate) const WAIT: Duration = Duration::from_secs(10);

/// A topic no other test of this run publishes to.
pub(crate) fn topic(test: &str) -> String {
    format!("ruststream-rust-mqtt/{}/{test}", std::process::id())
}

/// Connects the broker.
pub(crate) async fn connect(broker: MqttBroker) -> ConnectedMqttBroker {
    timeout(WAIT, broker.connect())
        .await
        .expect("the broker connects in time")
        .expect("the broker connects")
}

/// Opens a subscription to `topic` at `qos`.
pub(crate) async fn open(connected: &ConnectedMqttBroker, topic: &str, qos: Qos) -> MqttSubscriber {
    timeout(WAIT, MqttTopic::new(topic).qos(qos).subscribe(connected))
        .await
        .expect("the subscription opens in time")
        .expect("the subscription opens")
}

/// A publisher under `policy`.
pub(crate) async fn publisher_of(
    connected: &ConnectedMqttBroker,
    policy: MqttPublish,
) -> MqttPublisher {
    policy.pair(connected).await.expect("the policy pairs")
}

/// Publishes `payload` to `topic`.
pub(crate) async fn publish(publisher: &MqttPublisher, topic: &str, payload: &[u8]) {
    timeout(
        WAIT,
        publisher.publish(OutgoingMessage::new(topic, payload), None),
    )
    .await
    .expect("the publish finishes in time")
    .expect("the publish goes out");
}

/// The next delivery of a subscription.
pub(crate) async fn next(subscriber: &mut MqttSubscriber) -> MqttMessage {
    let mut deliveries = pin!(subscriber.stream());
    timeout(WAIT, deliveries.next())
        .await
        .expect("a delivery arrives in time")
        .expect("the stream is open")
        .expect("the delivery is not an error")
}
