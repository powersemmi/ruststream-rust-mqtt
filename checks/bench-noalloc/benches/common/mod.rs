//! Shared parts of the device variant's code-cost benchmarks.
//!
//! The scenarios are those of the heap variant's (`crates/ruststream-rust-mqtt-bench`): the same
//! body, the same message count, the same method (the slope between a run of N and of 2N
//! deliveries is the steady state, the run of one is the cold start). The service is a device
//! app, `#[ruststream::app(runtime = embassy)]`, over the device variant's `MqttBroker`, and its
//! runner runs on a tokio current-thread runtime of this thread: the runner and the app are one
//! joined future, so everything the service does is on the thread the tools count. The tokio
//! reactor under the socket is therefore in the numbers, as compio's is in the heap variant's.
//!
//! The service is started and brought to `Running`, the feeder fills the stand's queue while
//! nothing is polled, and the drain takes it. Waiting on the socket is not counted.

#![allow(dead_code, missing_docs)]

use core::time::Duration;
use std::env;
use std::future::Future;
use std::hint::black_box;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use embassy_sync::signal::Signal;
use embedded_io_adapters::tokio_1::FromTokio;
use gungraun::{Callgrind, Dhat, EntryPoint, LibraryBenchmarkConfig};
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::Packet;
use rumqttc::v5::{AsyncClient, Event, MqttOptions};
use ruststream::runtime::{Control, Health};
use ruststream_rust_mqtt::{MqttBroker, MqttConfig, MqttRunner, MqttState, Network};
use static_cell::StaticCell;
use tokio::net::TcpStream;
use tokio::runtime::Builder;

pub const INPUT: &str = "bench/orders";
const BODY: &[u8] = br#"{"id":1000000,"quantity":37}"#;

pub const MESSAGES: usize = messages(option_env!("RUSTSTREAM_BENCH_MESSAGES"));
const DEFAULT_MESSAGES: usize = 500;

const fn messages(configured: Option<&str>) -> usize {
    let Some(text) = configured else {
        return DEFAULT_MESSAGES;
    };
    let bytes = text.as_bytes();
    let mut count = 0usize;
    let mut index = 0;
    while index < bytes.len() {
        let digit = bytes[index];
        assert!(
            digit.is_ascii_digit(),
            "RUSTSTREAM_BENCH_MESSAGES must be a number"
        );
        count = count * 10 + (digit - b'0') as usize;
        assert!(
            count <= DEFAULT_MESSAGES,
            "RUSTSTREAM_BENCH_MESSAGES must be at most 500"
        );
        index += 1;
    }
    assert!(count > 0, "RUSTSTREAM_BENCH_MESSAGES must be positive");
    count
}

pub fn config() -> LibraryBenchmarkConfig {
    let mut config = LibraryBenchmarkConfig::default();
    config.tool(callgrind()).tool(dhat());
    config
}

fn callgrind() -> Callgrind {
    let mut callgrind = Callgrind::with_args([
        "--collect-atstart=no",
        &format!("--toggle-collect={REGION}"),
    ]);
    callgrind.entry_point(EntryPoint::None);
    callgrind
}

#[inline(never)]
pub fn measure<T>(body: impl FnOnce() -> T) -> T {
    black_box(body())
}

fn dhat() -> Dhat {
    let mut dhat = Dhat::with_args(["--num-callers=128"]);
    dhat.entry_point(EntryPoint::Custom(REGION.to_owned()));
    dhat
}

const REGION: &str = "*common::measure*";

fn url() -> String {
    env::var("MQTT_TEST_URL").unwrap_or_else(|_| "mqtt://127.0.0.1:1893".to_owned())
}

/// Counts deliveries down; the last wakes the waiter. A static, as a firmware keeps its state.
static REMAINING: AtomicUsize = AtomicUsize::new(0);
static DRAINED: Signal<CriticalSectionRawMutex, ()> = Signal::new();

/// Records one handled delivery.
pub fn arrived() {
    if REMAINING.fetch_sub(1, Ordering::Relaxed) == 1 {
        DRAINED.signal(());
    }
}

/// The runner's network: a TCP stream and tokio's clock.
pub struct TokioNetwork;

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

/// The device broker's memory, as a board declares it.
pub fn broker() -> (MqttBroker, MqttRunner) {
    static MQTT: StaticCell<MqttState> = StaticCell::new();
    let address = url();
    let address: &'static str = Box::leak(
        address
            .strip_prefix("mqtt://")
            .unwrap_or(&address)
            .to_owned()
            .into_boxed_str(),
    );
    let id: &'static str =
        Box::leak(format!("rs-bench-noalloc-{}", std::process::id()).into_boxed_str());
    MqttBroker::new(
        MQTT.init(MqttState::new()),
        MqttConfig::new(address).client_id(id),
    )
}

fn fill(count: usize) {
    thread::spawn(move || {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(async move {
                let url = url();
                let rest = url.strip_prefix("mqtt://").unwrap_or(&url);
                let (host, port) = rest.rsplit_once(':').expect("host and port");
                let mut options = MqttOptions::new(
                    format!("rs-bench-noalloc-feeder-{}", std::process::id()),
                    host.to_owned(),
                    port.parse().expect("port"),
                );
                options.set_max_packet_size(Some(1 << 20));
                let (client, mut events) = AsyncClient::new(options, 64);
                let publishing = tokio::spawn(async move {
                    for _ in 0..count {
                        client
                            .publish(INPUT, QoS::AtLeastOnce, false, BODY.to_vec())
                            .await
                            .expect("the feeder publishes");
                    }
                    client
                });
                let mut acknowledged = 0;
                while acknowledged < count {
                    if let Event::Incoming(Packet::PubAck(_)) =
                        events.poll().await.expect("the feeder's connection holds")
                    {
                        acknowledged += 1;
                    }
                }
                let client = publishing.await.expect("feeder task");
                client.disconnect().await.expect("disconnect");
            });
    })
    .join()
    .expect("the feeder thread ends");
}

/// The app and its runner as one future, polled by the benchmark body.
pub type Joined = Pin<Box<dyn Future<Output = ()>>>;

/// Starts the service, fills its subscription, drains it: the shape of every scenario.
///
/// `app` is the service's run; the first region is the cold start (to `Running`), the second the
/// deliveries. The fill between them is in neither.
pub fn start_and_drain(
    messages: usize,
    control: &'static Control,
    app: impl Future<Output = ()> + 'static,
    runner: MqttRunner,
) {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let mut joined: Joined = Box::pin(async move {
        futures_join(app, async move {
            runner.run(TokioNetwork).await;
        })
        .await;
    });
    REMAINING.store(messages, Ordering::Release);
    DRAINED.reset();
    measure(|| {
        runtime.block_on(poll_until(&mut joined, async {
            let mut seen = Health::Starting;
            while seen != Health::Running {
                seen = control.changed(seen).await;
            }
            // Running is reported as the subscriptions are handed to the runner; the SUBACK comes
            // back over the socket. A message published before it is lost on a clean session, so
            // the runner gets time to finish the handshake. Time is not instructions: the sleep
            // costs the one timer it arms, the same in every run.
            tokio::time::sleep(Duration::from_millis(200)).await;
        }));
    });
    fill(messages);
    assert_eq!(
        REMAINING.load(Ordering::Acquire),
        messages,
        "the subscription was consumed while it was being filled"
    );
    measure(|| runtime.block_on(poll_until(&mut joined, DRAINED.wait())));
    control.stop();
    // The runner never returns, so the app ending is the end of the run.
    runtime.block_on(poll_until(&mut joined, async {
        let mut seen = control.health();
        while seen != Health::Stopped {
            seen = control.changed(seen).await;
        }
    }));
}

/// Polls `joined` beside `until` and returns when `until` does. `joined` is never ready first.
async fn poll_until(joined: &mut Joined, until: impl Future<Output = ()>) {
    let mut until = core::pin::pin!(until);
    core::future::poll_fn(|cx| {
        let _ = joined.as_mut().poll(cx);
        until.as_mut().poll(cx)
    })
    .await;
}

/// Two futures run to completion together.
async fn futures_join(first: impl Future<Output = ()>, second: impl Future<Output = ()>) {
    embassy_futures::join::join(first, second).await;
}
