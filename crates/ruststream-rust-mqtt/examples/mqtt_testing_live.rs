//! A live test against a real server: the service runs on compio and connects to the stand named
//! by `MQTT_TEST_URL`, and the test talks to it as another client of the server.
//!
//! ```text
//! cargo test --example mqtt_testing_live --features testing,compio
//! ```

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use ruststream::Outgoing;
use ruststream::runtime::compio::{App, AppInfo, RustStream};
use ruststream_rust_mqtt::prelude::*;
use serde::{Deserialize, Serialize};

/// A job for the service, as its payload.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "live/jobs")]
struct Job {
    id: u32,
}

/// What the service answers for a job.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "live/done")]
struct Done {
    id: u32,
}

/// Answers every job.
#[subscriber(MqttTopic::new("live/jobs"), reply)]
async fn run(job: &Job) -> Done {
    Done { id: job.id }
}

#[ruststream::app(runtime = compio, cores = 1)]
fn app() -> impl App {
    let config = MqttConfig::new(std::env::var("MQTT_TEST_URL").unwrap_or_default());
    RustStream::new(AppInfo::new("worker", "0.1.0"))
        .with_broker(MqttBroker::compio(config.client_id("worker")), |b| {
            b.include(run).out_reply(MqttPublish::default())
        })
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use compio::runtime::Runtime;
    use futures::StreamExt;
    use ruststream::{
        Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, PublishMessageError,
        PublishPolicy, Publisher, Settle, Subscriber, SubscriptionSource,
    };

    use super::*;

    // --8<-- [start:test]
    #[test]
    fn a_job_is_answered_through_the_server() -> Result<(), Box<dyn Error>> {
        // The stand is `just test-brokers`; a run without one has nothing to test.
        let Ok(url) = std::env::var("MQTT_TEST_URL") else {
            return Ok(());
        };
        Runtime::new()?.block_on(async {
            let running = app().start().await?;

            // The test is another client of the same server.
            let client = MqttBroker::compio(MqttConfig::new(url).client_id("tester"))
                .connect()
                .await?;
            let mut answers = MqttTopic::new("live/done").subscribe(&client).await?;
            let jobs = MqttPublish::default().pair(&client).await?;
            jobs.publish(OutgoingMessage::new("live/jobs", br#"{"id":7}"#), None)
                .await
                .map_err(PublishMessageError::into_error)?;

            let mut deliveries = core::pin::pin!(answers.stream());
            let answer = deliveries.next().await.ok_or("the subscription ended")??;
            assert_eq!(answer.payload(), br#"{"id":7}"#);
            answer.ack().await?;
            client.shutdown().await?;
            running.shutdown().await?;
            Ok(())
        })
    }
    // --8<-- [end:test]
}
