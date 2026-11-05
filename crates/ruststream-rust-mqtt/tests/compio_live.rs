//! The compio transport against the live stand: the broker starts its runner on the core that
//! connects it, and a publish comes back through its own subscription.

#![cfg(feature = "compio")]

use futures::StreamExt;
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, PublishPolicy, Publisher, Settle,
    Subscriber, SubscriptionSource,
};
use ruststream_rust_mqtt::{MqttBroker, MqttConfig, MqttPublish, MqttTopic, Qos};

use crate::support::stand;

#[test]
fn a_publish_comes_back_through_a_subscription_on_compio() {
    let Some(url) = stand() else { return };
    let runtime = compio::runtime::Runtime::new().expect("a compio runtime starts");
    runtime.block_on(async {
        let broker = MqttBroker::compio(
            MqttConfig::new(url).client_id(format!("compio-live-{}", std::process::id())),
        );
        let connected = broker
            .connect()
            .await
            .expect("the broker connects on compio");
        let topic = format!("compio/live/{}", std::process::id());
        let mut subscriber = MqttTopic::new(topic.clone())
            .qos(Qos::AtLeastOnce)
            .subscribe(&connected)
            .await
            .expect("the subscription opens");
        let publisher = MqttPublish::default()
            .pair(&connected)
            .await
            .expect("the policy pairs");
        publisher
            .publish(OutgoingMessage::new(&topic, b"over io_uring"), None)
            .await
            .expect("the publish goes out");
        let delivery = {
            let mut deliveries = core::pin::pin!(subscriber.stream());
            deliveries
                .next()
                .await
                .expect("the stream yields")
                .expect("the delivery arrives")
        };
        assert_eq!(delivery.payload(), b"over io_uring");
        delivery.ack().await.expect("a QoS 1 delivery acknowledges");
        drop(subscriber);
        connected.shutdown().await.expect("the session ends");
    });
}
