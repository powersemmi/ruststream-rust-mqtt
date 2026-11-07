//! The service of `examples/device/device_testing.rs` written without `#[subscriber]` and the
//! derives; the tests are the same.
//!
//! ```text
//! cd checks/noalloc && cargo test --example manual_device_testing
//! ```

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use core::convert::Infallible;
use core::future::{Future, ready};
use core::time::Duration;

use ruststream::runtime::{MountSteps, RETRY_COUNT_HEADER};
use ruststream_rust_mqtt::prelude::*;

/// A job as the controller sends its bytes.
struct Job<'a>(&'a [u8]);

impl Deserialized for Job<'_> {
    type Output<'a> = Job<'a>;
    type Error = Infallible;

    fn from_payload(payload: &[u8]) -> Result<Job<'_>, Self::Error> {
        Ok(Job(payload))
    }
}

impl Input for Job<'_> {
    type Axis = SoloDeserialized<Job<'static>>;
}

/// What the board answers for a job, as the bytes that leave.
struct Done(&'static [u8]);

impl Serialized for Done {
    type Error = Infallible;

    fn wire_bytes(&self, _buf: &mut EncodeBuf) -> Result<WireBytes<'_>, Infallible> {
        Ok(WireBytes::Own(self.0))
    }
}

impl MessageWire for Done {
    type Wire = SerializedWire;
}

impl ReplyShape for Done {
    type Body = Self;
    type Headers = ();
    type Wire = SerializedReply;
}

impl OutgoingDestination for Done {
    type Form = CallerName;
}

impl MessageHeaders for Done {
    type Contract = NoHeaders;
}

/// Answers every job.
struct Run;

impl<'p> Handle<Job<'p>, Done> for Run {
    fn handle(
        &self,
        job: &Job<'p>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Done, HandlerOutcome>> {
        let _ = job.0;
        ready(Ok(Done(b"done")))
    }
}

fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| {
        b.include(
            subscriber("jobs/run", Run)
                .map_source(|_| MqttTopic::new("jobs/run"))
                .reply()
                .to("jobs/done")
                .build(),
        )
        .out_reply(MqttPublish::default())
    })
}

/// Asks for a job again in five seconds, once.
struct Later;

impl<'p> Handle<Job<'p>> for Later {
    fn handle(
        &self,
        job: &Job<'p>,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let _ = job.0;
        ready(match ctx.headers().get_str(RETRY_COUNT_HEADER) {
            Some(_) => Ok(()),
            None => Err(HandlerOutcome::retry_after(Duration::from_secs(5))),
        })
    }
}

fn retrying_app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| {
        b.include(
            subscriber("jobs/later", Later)
                .map_source(|_| MqttTopic::new("jobs/later"))
                .build(),
        )
        .retry_capacity::<2>()
    })
}

fn main() {}

#[cfg(test)]
mod tests {
    use embassy_futures::block_on;
    use ruststream::testing::Outcome;
    use ruststream::testing::embedded::TestApp;
    use ruststream::{Outgoing, Serialized as SerializedDerive};
    use ruststream_rust_mqtt::MqttState;
    use static_cell::ConstStaticCell;

    use super::*;

    /// What the controller publishes, to the topic it names.
    #[derive(Outgoing, SerializedDerive)]
    struct Sent(&'static [u8]);

    /// The broker the production `main` builds, over the memory the test declares: the runner is
    /// not needed, because the in-process transport replaces the connection.
    fn board(memory: &'static mut MqttState) -> MqttBroker {
        let (broker, _runner) = MqttBroker::new(memory, MqttConfig::new("192.168.1.10:1883"));
        broker
    }

    // --8<-- [start:test]
    #[test]
    fn a_job_is_answered() {
        static MEMORY: ConstStaticCell<MqttState> = ConstStaticCell::new(MqttState::new());
        block_on(async {
            let tb = TestApp::start(app(board(MEMORY.take())))
                .await
                .expect("the app starts");
            let jobs = tb.broker::<MqttBroker>();
            jobs.message(&Sent(b"mix"))
                .to("jobs/run")
                .publish()
                .await
                .expect("the harness publishes");

            jobs.subscriber("jobs/run")
                .assert_called_once()
                .with_raw(b"mix")
                .settled(HandlerOutcome::ack());
            jobs.published::<()>("jobs/done")
                .assert_called_once()
                .with_raw(b"done");
            tb.shutdown().await.expect("the app stops");
        });
    }
    // --8<-- [end:test]

    // --8<-- [start:advance]
    #[test]
    fn a_job_is_asked_for_again_when_its_delay_has_passed() {
        static MEMORY: ConstStaticCell<MqttState> = ConstStaticCell::new(MqttState::new());
        block_on(async {
            let tb = TestApp::start(retrying_app(board(MEMORY.take())))
                .await
                .expect("the app starts");
            let jobs = tb.broker::<MqttBroker>();
            jobs.message(&Sent(b"mix"))
                .to("jobs/later")
                .publish()
                .await
                .expect("the harness publishes");

            tb.advance(Duration::from_secs(4))
                .await
                .expect("the clock moves");
            jobs.subscriber("jobs/later").assert_called_once();
            tb.advance(Duration::from_secs(1))
                .await
                .expect("the clock moves");
            let calls = jobs.subscriber("jobs/later").assert_called(2);
            assert_eq!(calls.outcomes().last(), Some(&Outcome::Ack));
            tb.shutdown().await.expect("the app stops");
        });
    }
    // --8<-- [end:advance]
}
