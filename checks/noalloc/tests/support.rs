//! What the suites share: the network the live suites run the runner over, the stand's address,
//! and a fresh broker memory per broker.

use core::pin::pin;
use core::time::Duration;
use std::collections::hash_map::RandomState;
use std::env;
use std::hash::{BuildHasher, Hasher};

use embedded_io_adapters::tokio_1::FromTokio;
use futures::StreamExt;
use ruststream::{Broker, OutgoingMessage, PublishPolicy, Publisher, Subscriber};
use ruststream_rust_mqtt::{
    ConnectedMqttBroker, MqttBroker, MqttConfig, MqttMessage, MqttPublish, MqttPublisher,
    MqttState, MqttSubscriber, MqttTopic, Network, Qos,
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

/// A text the test keeps for the rest of the process, as a firmware's literals live.
pub(crate) fn leak(text: String) -> &'static str {
    Box::leak(text.into_boxed_str())
}

/// A password made up at run time, so no secret is written into the source.
pub(crate) fn made_up_password() -> String {
    RandomState::new().build_hasher().finish().to_string()
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

/// A fresh memory for one broker: a test builds many brokers, a firmware one.
pub(crate) fn state<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize>()
-> &'static mut MqttState<MESSAGES, PACKET, SUBSCRIPTIONS> {
    Box::leak(Box::new(MqttState::new()))
}

/// A client identifier no other test of this run uses.
pub(crate) fn unique_client_id(label: &str) -> &'static str {
    static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    leak(format!("rs-noalloc-{label}-{}-{id}", std::process::id()))
}

/// A broker with the given settings over a memory of the shape the test names, whose runner runs
/// on a thread of its own.
pub(crate) fn broker_with<
    const MESSAGES: usize,
    const PACKET: usize,
    const SUBSCRIPTIONS: usize,
>(
    config: MqttConfig,
) -> MqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS> {
    let (broker, runner) = MqttBroker::new(state(), config);
    // A thread of its own: the runner's future names the stream through a borrow of the network,
    // which a spawned task cannot prove `Send` for.
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the runner's runtime builds")
            .block_on(runner.run(TokioNetwork))
    });
    broker
}

/// A broker on the stand at `url` whose runner runs on a thread of its own.
pub(crate) fn live_broker(url: &str) -> MqttBroker {
    broker_with(MqttConfig::new(leak(url.to_owned())).client_id(unique_client_id("live")))
}

/// How long a live test waits for something the stand owes it.
pub(crate) const WAIT: Duration = Duration::from_secs(10);

/// How long a live test waits to be sure something does not come.
pub(crate) const QUIET: Duration = Duration::from_millis(500);

/// A topic no other test of this run publishes to.
pub(crate) fn topic(test: &str) -> &'static str {
    leak(format!(
        "ruststream-rust-mqtt-noalloc/{}/{test}",
        std::process::id()
    ))
}

/// Connects the broker.
pub(crate) async fn connect<
    const MESSAGES: usize,
    const PACKET: usize,
    const SUBSCRIPTIONS: usize,
>(
    broker: MqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS>,
) -> ConnectedMqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS> {
    timeout(WAIT, broker.connect())
        .await
        .expect("the broker connects in time")
        .expect("the broker connects")
}

/// Opens a subscription to `topic` at `qos`.
pub(crate) async fn open<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize>(
    connected: &ConnectedMqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS>,
    topic: &'static str,
    qos: Qos,
) -> MqttSubscriber<MESSAGES, PACKET, SUBSCRIPTIONS> {
    open_source(connected, MqttTopic::new(topic).qos(qos)).await
}

/// Opens the subscription a descriptor names.
pub(crate) async fn open_source<
    const MESSAGES: usize,
    const PACKET: usize,
    const SUBSCRIPTIONS: usize,
    Source: ruststream::SubscriptionSource<
            ConnectedMqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS>,
            Subscriber = MqttSubscriber<MESSAGES, PACKET, SUBSCRIPTIONS>,
        >,
>(
    connected: &ConnectedMqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS>,
    source: Source,
) -> MqttSubscriber<MESSAGES, PACKET, SUBSCRIPTIONS> {
    timeout(WAIT, source.subscribe(connected))
        .await
        .expect("the subscription opens in time")
        .expect("the subscription opens")
}

/// A publisher under `policy`.
pub(crate) async fn publisher_of<
    const MESSAGES: usize,
    const PACKET: usize,
    const SUBSCRIPTIONS: usize,
>(
    connected: &ConnectedMqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS>,
    policy: MqttPublish,
) -> MqttPublisher<MESSAGES, PACKET, SUBSCRIPTIONS> {
    policy.pair(connected).await.expect("the policy pairs")
}

/// Publishes `payload` to `topic`.
pub(crate) async fn publish<
    const MESSAGES: usize,
    const PACKET: usize,
    const SUBSCRIPTIONS: usize,
>(
    publisher: &MqttPublisher<MESSAGES, PACKET, SUBSCRIPTIONS>,
    topic: &str,
    payload: &[u8],
) {
    timeout(
        WAIT,
        publisher.publish(OutgoingMessage::new(topic, payload), None),
    )
    .await
    .expect("the publish finishes in time")
    .expect("the publish goes out");
}

/// The next delivery of a subscription.
pub(crate) async fn next<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize>(
    subscriber: &mut MqttSubscriber<MESSAGES, PACKET, SUBSCRIPTIONS>,
) -> MqttMessage<MESSAGES, PACKET, SUBSCRIPTIONS> {
    let mut deliveries = pin!(subscriber.stream());
    timeout(WAIT, deliveries.next())
        .await
        .expect("a delivery arrives in time")
        .expect("the stream is open")
        .expect("the delivery is not an error")
}

/// The next delivery if one arrives within [`QUIET`], else `None`.
pub(crate) async fn next_if_any<
    const MESSAGES: usize,
    const PACKET: usize,
    const SUBSCRIPTIONS: usize,
>(
    subscriber: &mut MqttSubscriber<MESSAGES, PACKET, SUBSCRIPTIONS>,
) -> Option<MqttMessage<MESSAGES, PACKET, SUBSCRIPTIONS>> {
    let mut deliveries = pin!(subscriber.stream());
    let arrived = timeout(QUIET, deliveries.next()).await.ok()?;
    Some(
        arrived
            .expect("the stream is open")
            .expect("the delivery is not an error"),
    )
}
