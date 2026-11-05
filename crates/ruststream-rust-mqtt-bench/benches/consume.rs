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
//! Consuming a small JSON body at `QoS` 1 on compio: the subscription yields the crate's message,
//! the dispatcher decodes it into a struct, the handler reads a field, and the runtime
//! acknowledges it with a `PUBACK`.

mod common;

use std::convert::Infallible;
use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::runtime::HandlerOutcome;
use ruststream::runtime::compio::RustStream;
use ruststream::subscriber;
use ruststream_rust_mqtt::{MqttTopic, Qos};

#[subscriber(MqttTopic::new("bench/orders").qos(Qos::AtLeastOnce))]
async fn consume(order: &Order, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

fn app(messages: usize) -> Pending {
    let latch = Latch::default();
    let state = latch.clone();
    let app = RustStream::new(common::app_info())
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(common::broker(), |b| b.include(consume));
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

library_benchmark_group!(name = consume_group; benchmarks = service);
main!(library_benchmark_groups = consume_group);
