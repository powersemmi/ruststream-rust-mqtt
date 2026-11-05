//! What the suites share: the network the live suites run the runner over, the stand's address,
//! and a fresh broker memory per broker.

use core::time::Duration;
use std::env;

use embedded_io_adapters::tokio_1::FromTokio;
use ruststream_rust_mqtt::{MqttBroker, MqttConfig, MqttState, Network};
use tokio::net::TcpStream;

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

/// A fresh memory for one broker: a test builds many brokers, a firmware one.
pub(crate) fn state() -> &'static mut MqttState {
    Box::leak(Box::new(MqttState::new()))
}

/// A client identifier no other test of this run uses.
pub(crate) fn unique_client_id(label: &str) -> &'static str {
    static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    leak(format!("rs-noalloc-{label}-{}-{id}", std::process::id()))
}

/// A broker on the stand at `url` whose runner runs on the test's runtime.
pub(crate) fn live_broker(url: &str) -> MqttBroker {
    let config = MqttConfig::new(leak(url.to_owned())).client_id(unique_client_id("live"));
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
