#![allow(missing_docs, unreachable_pub)]
//! Replying on the device variant: a delivery at `QoS` 1 is acknowledged, and the value the
//! handler returns is encoded into a send buffer and written by the runner as a PUBLISH to the
//! topic the reply type declares. The cost of a publish is this scenario's total less the
//! consuming scenario's.

mod common;

use std::hint::black_box;

use common::{MESSAGES, arrived, broker, config, start_and_drain};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::runtime::{EmbeddedApp, Mounts};
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};
use ruststream_rust_mqtt::{MqttBroker, MqttTopic, Qos};

#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

/// A reply with a destination of its own, as bytes.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "bench/confirmations")]
struct Confirmation(&'static [u8]);

#[subscriber(MqttTopic::new("bench/orders").qos(Qos::AtLeastOnce), reply)]
async fn confirm(frame: &Frame<'_>) -> Confirmation {
    black_box(frame.0.len());
    arrived();
    Confirmation(b"done")
}

#[ruststream::app(runtime = embassy)]
fn service(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| b.include(confirm))
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

library_benchmark_group!(name = reply_group; benchmarks = run);
main!(library_benchmark_groups = reply_group);
