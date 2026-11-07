#![allow(missing_docs, unreachable_pub)]
//! Consuming a small body at `QoS` 1 on the device variant: the subscription yields the crate's
//! message borrowed from the receive buffer, the handler reads it, and the runtime acknowledges
//! it with a `PUBACK`.

mod common;

use std::hint::black_box;

use common::{MESSAGES, arrived, broker, config, start_and_drain};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts};
use ruststream::{Deserialized, subscriber};
use ruststream_rust_mqtt::{MqttBroker, MqttTopic, Qos};

/// A body as its bytes, borrowed from the receive buffer.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

#[subscriber(MqttTopic::new("bench/orders").qos(Qos::AtLeastOnce))]
async fn consume(frame: &Frame<'_>) -> HandlerOutcome {
    black_box(frame.0.len());
    arrived();
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn service(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| b.include(consume))
}

#[library_benchmark(config = config())]
#[bench::first(1)]
#[bench::base(MESSAGES)]
#[bench::twice(2 * MESSAGES)]
fn run(messages: usize) {
    let (mqtt, runner) = broker();
    let app = async {
        service(mqtt)
            .run(&service::CONTROL)
            .await
            .expect("the service runs");
    };
    start_and_drain(messages, &service::CONTROL, app, runner);
}

library_benchmark_group!(name = consume_group; benchmarks = run);
main!(library_benchmark_groups = consume_group);
