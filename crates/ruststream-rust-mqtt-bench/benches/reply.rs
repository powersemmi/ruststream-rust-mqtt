// The harness macros generate the group module, its items and the paths between them, and a
// benchmark function takes its setup value by value because the harness owns the drop; the
// crate's lints are written for the library surface, not for generated benchmark scaffolding.
#![allow(
    missing_docs,
    unused_qualifications,
    unreachable_pub,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value,
    // The app runs on the one core of the measured run, the thread that drives it.
    clippy::future_not_send
)]
//! Replying: a delivery at `QoS` 1 is decoded and acknowledged, and the value the handler returns
//! is encoded and handed to this crate's publisher under its default policy, which sends the
//! PUBLISH to the topic the reply type declares. The cost of a publish is this scenario's total
//! less the consuming scenario's.

mod common;

use std::convert::Infallible;
use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::runtime::compio::RustStream;
use ruststream::{Outgoing, subscriber};
use ruststream_rust_mqtt::{MqttTopic, Qos};
use serde::Serialize;

/// A reply with a destination of its own: the mount site adds nothing to it.
#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "bench/confirmations")]
struct Confirmation {
    id: u64,
}

#[subscriber(MqttTopic::new("bench/orders").qos(Qos::AtLeastOnce), reply)]
async fn confirm(order: &Order, ctx: &mut Context<'_, (), Latch>) -> Confirmation {
    ctx.state().arrived();
    Confirmation {
        id: black_box(order.id),
    }
}

fn app(messages: usize) -> Pending {
    let latch = Latch::default();
    let state = latch.clone();
    let app = RustStream::new(common::app_info())
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(common::broker(), |b| b.include(confirm));
    common::pending(latch, messages, async move || {
        app.start().await.expect("the service starts")
    })
}

#[library_benchmark(config = common::config())]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = reply_group; benchmarks = service);
main!(library_benchmark_groups = reply_group);
