//! Headers through the in-process server: user properties and the three properties that have a
//! header of their own, and a header that is not MQTT text.

use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts, Out};
use ruststream::testing::embedded::TestApp;
use ruststream::{Deserialized, HeaderMap, OutSlot, Outgoing, Publisher, Serialized, subscriber};
use ruststream_rust_mqtt::prelude::*;

/// A frame as its bytes.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

/// What the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// The frame again, sent on with its headers.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "forwarded")]
struct Forwarded(Vec<u8>);

#[derive(OutSlot)]
#[publishes(Forwarded)]
struct Relay;

fn broker() -> MqttBroker {
    let (broker, _runner) =
        MqttBroker::new(MqttConfig::new("192.168.1.10:1883").client_id("relay-1"));
    broker
}

/// Sends every frame on with the headers it arrived with; a frame that carries a header the
/// protocol cannot send is dropped when the publish refuses it and hands the message back.
#[subscriber(MqttTopic::new("inbox"))]
async fn forward(
    frame: &Frame<'_>,
    ctx: &mut Context<'_>,
    Out(relay): Out<impl Publisher<Options = MqttPublishOptions>, Relay>,
) -> HandlerOutcome {
    let mut headers = HeaderMap::new();
    for (name, value) in ctx.headers().iter() {
        headers.insert(name.to_owned(), ruststream::Bytes::copy_from_slice(value));
    }
    match relay
        .message(&Forwarded(frame.0.to_vec()))
        .with_headers(headers)
        .publish()
        .await
    {
        Ok(()) => HandlerOutcome::ack(),
        Err(error) if error.unsent().is_some() => HandlerOutcome::drop(),
        Err(_) => HandlerOutcome::retry(),
    }
}

/// Sends the frame on with a header the protocol cannot carry, the kind the payload names.
#[subscriber(MqttTopic::new("faulty"))]
async fn corrupt(
    frame: &Frame<'_>,
    Out(relay): Out<impl Publisher<Options = MqttPublishOptions>, Relay>,
) -> HandlerOutcome {
    let (name, value): (&str, &[u8]) = match frame.0 {
        b"binary" => ("blob", b"\xff\xfe"),
        b"newline" => ("note", b"line\nbreak"),
        _ => ("bad\tname", b"fine"),
    };
    let mut headers = HeaderMap::new();
    headers.insert(name.to_owned(), ruststream::Bytes::copy_from_slice(value));
    match relay
        .message(&Forwarded(frame.0.to_vec()))
        .with_headers(headers)
        .publish()
        .await
    {
        Ok(()) => HandlerOutcome::ack(),
        Err(error) if error.unsent().is_some() => HandlerOutcome::drop(),
        Err(_) => HandlerOutcome::retry(),
    }
}

#[ruststream::app(runtime = embassy)]
fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| {
        b.include(forward)
            .out(Relay, MqttPublish::default())
            .include(corrupt)
            .out(Relay, MqttPublish::default())
    })
}

async fn send(tb: &TestApp, headers: HeaderMap) {
    tb.broker::<MqttBroker>()
        .message(&Sent(b"payload"))
        .to("inbox")
        .with_headers(headers)
        .publish()
        .await
        .expect("the harness publishes");
}

#[test]
fn user_properties_and_the_three_named_properties_round_trip() {
    embassy_futures::block_on(async {
        let tb = TestApp::start(app(broker())).await.expect("the app starts");
        let headers: HeaderMap = [
            ("site", "plant-3"),
            ("content-type", "application/json"),
            ("reply-to", "answers/plant-3"),
            ("correlation-id", "7f3a"),
        ]
        .into_iter()
        .collect();
        send(&tb, headers).await;

        tb.out::<Relay>()
            .assert_called_once()
            .with_raw(b"payload")
            .with_header("site", "plant-3")
            .with_header("content-type", "application/json")
            .with_header("reply-to", "answers/plant-3")
            .with_header("correlation-id", "7f3a");
        tb.shutdown().await.expect("the app stops");
    });
}

#[test]
fn a_header_that_is_not_mqtt_text_refuses_the_publish_and_hands_the_message_back() {
    embassy_futures::block_on(async {
        let tb = TestApp::start(app(broker())).await.expect("the app starts");
        for body in [&b"binary"[..], b"newline", b"tab"] {
            tb.broker::<MqttBroker>()
                .message(&Sent(body))
                .to("faulty")
                .publish()
                .await
                .expect("the harness publishes");
            tb.broker::<MqttBroker>()
                .subscriber("faulty")
                .settled(HandlerOutcome::drop());
        }
        // The server took none of them.
        tb.broker::<MqttBroker>()
            .published::<()>("forwarded")
            .assert_not_called();
        tb.shutdown().await.expect("the app stops");
    });
}

#[test]
fn a_correlation_id_is_binary_and_is_not_checked_as_text() {
    embassy_futures::block_on(async {
        let tb = TestApp::start(app(broker())).await.expect("the app starts");
        let mut headers = HeaderMap::new();
        headers.insert(
            "correlation-id".to_owned(),
            ruststream::Bytes::copy_from_slice(b"\xff\x00\x01"),
        );
        send(&tb, headers).await;
        tb.out::<Relay>()
            .assert_called_once()
            .with_header("correlation-id", b"\xff\x00\x01");
        tb.shutdown().await.expect("the app stops");
    });
}
