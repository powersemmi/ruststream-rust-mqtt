//! Shared parts of the crate's code-cost benchmarks: what is measured, and how the measurement is
//! kept to one region.
//!
//! The scenarios are the ones of `ruststream-rumqttc`'s benchmarks, with the same payload, the same
//! message count and the same method, so the two brokers' numbers are read side by side. The method
//! is the core's, described on the
//! [RustStream benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/).
//!
//! # What a scenario looks like
//!
//! A scenario runs the service a user writes on the real broker: the compio app, started through
//! [`RustStream::start`] on [`MqttBroker::compio`], connected to the mosquitto of the compose stand
//! (`MQTT_TEST_URL`, `mqtt://127.0.0.1:1893` when unset). What comes out is what a message costs on
//! the service's core: the framework, this crate, and the `rust-mqtt` client's work, since the
//! crate drives the client from a task of the service's own runtime.
//!
//! # Steady state and cold start
//!
//! Every scenario is measured over one delivery, over [`MESSAGES`] deliveries and over twice as
//! many. The slope between the last two is the steady-state cost of a message: everything that
//! happens once is in both totals and cancels in the subtraction. The one-delivery run is the cold
//! start: the connect, the subscription and the first delivery.
//!
//! # The fill
//!
//! Between the two regions a feeder on a thread and a runtime of its own publishes the run's
//! messages at `QoS` 1 and waits for every acknowledgement of the broker. The service's runtime
//! runs only inside `block_on`, so nothing is consumed while the feeder publishes: the stand holds
//! the messages for the live session, and the drain region takes them all. The feeder's work is on
//! its own thread and is never counted.
//!
//! # What is counted
//!
//! Collection starts switched off and is switched on for [`measure`], which every body wraps its
//! work in. Everything the service's thread runs inside the region is counted; waiting on the
//! socket is not, since valgrind counts instructions and not time. DHAT is pointed at the same
//! frame; the number read is `Total blocks`, allocations per run.

// Each benchmark target compiles this module on its own and uses the part it needs; what another
// target uses looks unused here.
#![allow(dead_code)]

use std::env;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use compio::runtime::Runtime;
use gungraun::{Callgrind, Dhat, DhatMetric, EntryPoint, LibraryBenchmarkConfig};
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::Packet;
use rumqttc::v5::{AsyncClient, Event, MqttOptions};
use ruststream::runtime::compio::{AppInfo, RunningApp};
use ruststream_rust_mqtt::{MqttBroker, MqttConfig};
use serde::Deserialize;
use tokio::runtime::Builder;
use tokio::sync::Notify;

// A benchmark measures what ships, and the framework's harness feature changes the dispatch path:
// a number taken with it on is not the production path. The benchmark lives in a package of its
// own for the same reason: the broker crate's dev-dependencies enable that feature through the
// harness, and a benchmark inside that package would link it.
#[cfg(feature = "testing")]
compile_error!(
    "benchmarks must be built without the `testing` feature; run them through `just bench-code`"
);

/// The topic every scenario delivers on.
pub const INPUT: &str = "bench/orders";

/// The values every body carries. Fixed, so that every delivery of a run costs the same.
const ID: u64 = 1_000_000;
const QUANTITY: u32 = 37;

/// The payload every scenario decodes: two integer fields, so a decode allocates nothing and the
/// number is about the crate and the framework rather than about the JSON parser's strings.
#[derive(Debug, Deserialize)]
pub struct Order {
    pub id: u64,
    pub quantity: u32,
}

/// Deliveries per measured run, as the sibling's: `RUSTSTREAM_BENCH_MESSAGES` at build time sets a
/// smaller count for a shorter run, and the default is also the largest, so the longest run, twice
/// this, fits the stand's queue of a session.
pub const MESSAGES: usize = messages(option_env!("RUSTSTREAM_BENCH_MESSAGES"));

/// The count a run measures when nothing names one.
const DEFAULT_MESSAGES: usize = 500;

/// The configured count, or the default. A value that is not a number from 1 to the default is a
/// build error naming the variable.
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
            "RUSTSTREAM_BENCH_MESSAGES must be a number of deliveries"
        );
        count = count * 10 + (digit - b'0') as usize;
        assert!(
            count <= DEFAULT_MESSAGES,
            "RUSTSTREAM_BENCH_MESSAGES must be at most 500"
        );
        index += 1;
    }
    assert!(
        count > 0,
        "RUSTSTREAM_BENCH_MESSAGES must be a positive number of deliveries"
    );
    count
}

/// The measurement configuration every scenario shares: callgrind and DHAT inside the measured
/// region. The allocation count is reported, not gated: the floor is stated once the numbers are
/// settled.
pub fn config() -> LibraryBenchmarkConfig {
    let mut config = LibraryBenchmarkConfig::default();
    config.tool(callgrind()).tool(dhat());
    config
}

/// The same with a hard limit on the allocations of the run: the cold part once, plus the steady
/// rate over the run's own deliveries.
pub fn config_limited(steady: u64, cold: u64, messages: usize) -> LibraryBenchmarkConfig {
    let mut config = LibraryBenchmarkConfig::default();
    config
        .tool(callgrind())
        .tool(dhat().hard_limits([(DhatMetric::TotalBlocks, cold + steady * messages as u64)]));
    config
}

/// Callgrind collecting inside the measured region alone.
fn callgrind() -> Callgrind {
    let mut callgrind = Callgrind::with_args([
        "--collect-atstart=no",
        &format!("--toggle-collect={REGION}"),
    ]);
    callgrind.entry_point(EntryPoint::None);
    callgrind
}

/// The measured region: everything this runs is counted, nothing around it is.
#[inline(never)]
pub fn measure<T>(body: impl FnOnce() -> T) -> T {
    // `black_box` runs after the body returns, so the call cannot become a tail jump: DHAT
    // attributes an allocation to this region only while this frame is on the stack.
    black_box(body())
}

/// DHAT with a stack window deep enough to reach the measured frame from a publish inside a
/// dispatched handler.
fn dhat() -> Dhat {
    let mut dhat = Dhat::with_args(["--num-callers=128"]);
    dhat.entry_point(EntryPoint::Custom(REGION.to_owned()));
    dhat
}

/// The frame both tools are pointed at.
const REGION: &str = "*common::measure*";

/// The broker the stand exposes.
fn url() -> String {
    env::var("MQTT_TEST_URL").unwrap_or_else(|_| "mqtt://127.0.0.1:1893".to_owned())
}

/// Client identifiers of this process, so a run never shares a session with another.
fn client_id(role: &str) -> String {
    format!("ruststream-rust-mqtt-bench-{role}-{}", std::process::id())
}

/// Counts deliveries down and wakes the benchmark body when the last one has been handled.
///
/// Handlers reach it as the application state. What a delivery pays for it is one relaxed
/// decrement and the branch that reads it.
#[derive(Clone, Debug)]
pub struct Latch(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    remaining: AtomicUsize,
    drained: Notify,
}

impl Default for Latch {
    fn default() -> Self {
        Self(Arc::new(Inner {
            remaining: AtomicUsize::new(0),
            drained: Notify::new(),
        }))
    }
}

impl Latch {
    /// Arms the latch for `count` deliveries.
    pub fn expect(&self, count: usize) {
        self.0.remaining.store(count, Ordering::Release);
    }

    /// Records one handled delivery, waking the waiter on the last one.
    pub fn arrived(&self) {
        if self.0.remaining.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.0.drained.notify_one();
        }
    }

    /// How many deliveries the latch is still waiting for.
    pub fn remaining(&self) -> usize {
        self.0.remaining.load(Ordering::Acquire)
    }

    /// Resolves once every expected delivery has been handled.
    pub async fn drained(&self) {
        while self.0.remaining.load(Ordering::Acquire) > 0 {
            self.0.drained.notified().await;
        }
    }
}

/// The JSON body every delivery carries: the two fields a handler reads.
pub fn json_body() -> Vec<u8> {
    format!("{{\"id\":{ID},\"quantity\":{QUANTITY}}}").into_bytes()
}

/// A service that is built but not started, and how many messages its run takes.
///
/// The start is part of the measurement rather than of the setup, because the cold number is what
/// starting costs. It is held as a boxed call so that every scenario hands over the same type; the
/// one indirect call it adds lands in the cold number and nowhere else.
pub struct Pending {
    runtime: Runtime,
    latch: Latch,
    start: Box<dyn FnOnce(&Runtime) -> RunningApp>,
    messages: usize,
}

/// The broker of the stand, for the compio runtime, under a client identifier of this process.
pub fn broker() -> MqttBroker {
    let address = url();
    let address = address.strip_prefix("mqtt://").unwrap_or(&address);
    MqttBroker::compio(MqttConfig::new(address).client_id(client_id("service")))
}

/// The latch a scenario's app carries as its state, and the app's identity.
pub fn app_info() -> AppInfo {
    AppInfo::new("bench", "0.0.0")
}

/// A service ready to be started by the body: `start` runs the app's start on the runtime and
/// hands back the running service.
pub fn pending(
    latch: Latch,
    messages: usize,
    app: impl AsyncFnOnce() -> RunningApp + 'static,
) -> Pending {
    Pending {
        runtime: Runtime::new().expect("a compio runtime"),
        latch,
        start: Box::new(move |runtime| runtime.block_on(app())),
        messages,
    }
}

/// Publishes `count` bodies on [`INPUT`] at `QoS` 1 from a thread and a runtime of its own, and
/// returns once the broker acknowledged every one of them.
///
/// The raw client rather than this crate's publisher: the feeder is setup, and what it must
/// guarantee is that the broker holds every message before the drain starts, which a `PUBACK`
/// count says directly.
fn fill(count: usize) {
    thread::spawn(move || {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime")
            .block_on(async move {
                let (host, port) = endpoint(&url());
                let mut options = MqttOptions::new(client_id("feeder"), host, port);
                options.set_max_packet_size(Some(1 << 20));
                let (client, mut events) = AsyncClient::new(options, 64);
                let body = json_body();
                let publishing = tokio::spawn(async move {
                    for _ in 0..count {
                        client
                            .publish(INPUT, QoS::AtLeastOnce, false, body.clone())
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
                let client = publishing.await.expect("the feeder's task ends");
                client.disconnect().await.expect("the feeder disconnects");
            });
    })
    .join()
    .expect("the feeder thread ends");
}

/// The host and port of an `mqtt://` URL.
fn endpoint(url: &str) -> (String, u16) {
    let rest = url.strip_prefix("mqtt://").unwrap_or(url);
    let (host, port) = rest
        .rsplit_once(':')
        .expect("the broker URL names a host and a port");
    (
        host.to_owned(),
        port.parse().expect("the broker URL's port is a number"),
    )
}

/// Starts the service, fills its subscription, and drains it: the shape of every scenario here.
///
/// Two measured regions, and the fill between them is in neither. The first is the cold start,
/// the second the deliveries.
pub fn start_and_drain(pending: Pending) {
    let Pending {
        runtime,
        latch,
        start,
        messages,
    } = pending;
    let running = measure(|| start(&runtime));
    latch.expect(messages);
    fill(messages);
    assert_eq!(
        latch.remaining(),
        messages,
        "the subscription was consumed while it was being filled, so the measured region would be \
         short"
    );
    measure(|| runtime.block_on(latch.drained()));
    runtime
        .block_on(running.shutdown())
        .expect("the service stops");
}
