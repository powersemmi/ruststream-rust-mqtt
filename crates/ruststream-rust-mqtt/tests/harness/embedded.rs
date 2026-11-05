//! A device service written with the attributes, run by the embedded `TestApp`.

use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts};
use ruststream::testing::embedded::TestApp;
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};
use ruststream_rust_mqtt::{MqttBroker, MqttConfig, MqttFilter, MqttTopic, Qos};

/// A reading as the sensor sends its bytes.
#[derive(Deserialized)]
struct Reading<'a>(&'a [u8]);

#[subscriber(MqttTopic::new("sensors/temp"))]
async fn on_reading(reading: &Reading<'_>) -> HandlerOutcome {
    let _ = reading.0;
    HandlerOutcome::ack()
}

#[subscriber(MqttFilter::new("alarms/+").qos(Qos::AtMostOnce))]
async fn on_alarm(alarm: &Reading<'_>) -> HandlerOutcome {
    let _ = alarm.0;
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| b.include(on_reading).include(on_alarm))
}

fn broker() -> MqttBroker {
    let (broker, _runner) =
        MqttBroker::new(MqttConfig::new("192.168.1.10:1883").client_id("sensor-7"));
    broker
}

#[derive(Outgoing, Serialized)]
#[outgoing(name = "sensors/temp")]
struct Temperature(&'static [u8]);

#[derive(Outgoing, Serialized)]
#[outgoing(name = "alarms/door")]
struct Door(&'static [u8]);

#[test]
fn a_reading_reaches_its_handler_and_is_acknowledged() {
    embassy_futures::block_on(async {
        let tb = TestApp::start(app(broker())).await.expect("the app starts");
        tb.broker::<MqttBroker>()
            .message(&Temperature(b"21"))
            .publish()
            .await
            .expect("the harness publishes");
        tb.broker::<MqttBroker>()
            .subscriber("sensors/temp")
            .assert_called_once()
            .with_raw(b"21")
            .settled(HandlerOutcome::ack());
        tb.shutdown().await.expect("the app stops");
    });
}

#[test]
fn a_wildcard_filter_takes_every_topic_it_matches() {
    embassy_futures::block_on(async {
        let tb = TestApp::start(app(broker())).await.expect("the app starts");
        tb.broker::<MqttBroker>()
            .message(&Door(b"open"))
            .publish()
            .await
            .expect("the harness publishes");
        tb.broker::<MqttBroker>()
            .subscriber("alarms/+")
            .assert_called_once()
            .with_raw(b"open");
        tb.broker::<MqttBroker>()
            .subscriber("sensors/temp")
            .assert_not_called();
        tb.shutdown().await.expect("the app stops");
    });
}
