//! Which subscription a delivery reaches: overlapping filters of one session, and share groups.

use ruststream::{AckError, ConnectedBroker, IncomingMessage, Settle};
use ruststream_rust_mqtt::{MqttFilter, MqttPublish, MqttSubscriber, MqttTopic, Qos};

use crate::support::{
    connect, leak, live_broker, next, next_if_any, open, open_source, publish, publisher_of, stand,
    topic,
};

/// One PUBLISH that matches two subscriptions of the session reaches both of them, each with the
/// whole message, and a message only one of them matches reaches that one alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_packet_reaches_two_overlapping_subscriptions() {
    let Some(url) = stand() else { return };
    let base = topic("overlap");
    let exact = leak(format!("{base}/a"));
    let other = leak(format!("{base}/b"));
    let wildcard = leak(format!("{base}/+"));
    let receiver = connect(live_broker(&url)).await;
    let mut on_exact = open(&receiver, exact, Qos::AtLeastOnce).await;
    let mut on_wildcard =
        open_source(&receiver, MqttFilter::new(wildcard).qos(Qos::AtLeastOnce)).await;
    let sender = connect(live_broker(&url)).await;
    let publisher = publisher_of(&sender, MqttPublish::default().qos(Qos::AtLeastOnce)).await;

    publish(&publisher, exact, b"both").await;
    let first = next(&mut on_exact).await;
    let second = next(&mut on_wildcard).await;
    assert_eq!((first.payload(), first.topic()), (&b"both"[..], exact));
    assert_eq!((second.payload(), second.topic()), (&b"both"[..], exact));
    // One packet carries one acknowledgement; the subscription that holds a copy of it has none to
    // send.
    let settled = [first.ack().await, second.ack().await];
    assert!(
        settled.iter().any(Result::is_ok),
        "one holds the acknowledgement"
    );
    assert!(
        settled
            .iter()
            .all(|result| matches!(result, Ok(()) | Err(AckError::Unsupported))),
        "the other has none to send: {settled:?}"
    );

    // Neither got the message twice, and the wildcard alone takes the topic the exact one lacks.
    publish(&publisher, other, b"only the wildcard").await;
    let alone = next(&mut on_wildcard).await;
    assert_eq!(alone.payload(), b"only the wildcard");
    alone.ack().await.expect("the delivery acknowledges");
    assert!(next_if_any(&mut on_exact).await.is_none());
    assert!(next_if_any(&mut on_wildcard).await.is_none());
    drop((on_exact, on_wildcard));
    receiver.shutdown().await.expect("the receiver ends");
    sender.shutdown().await.expect("the sender ends");
}

/// Collects what the subscribers of a group receive until none has more, acknowledging each.
async fn drain_group(members: &mut [&mut MqttSubscriber]) -> Vec<Vec<u32>> {
    let mut received = vec![Vec::new(); members.len()];
    loop {
        let mut any = false;
        for (index, member) in members.iter_mut().enumerate() {
            if let Some(delivery) = next_if_any(member).await {
                let bytes: [u8; 4] = delivery.payload().try_into().expect("a counter");
                received[index].push(u32::from_be_bytes(bytes));
                delivery.ack().await.expect("the delivery acknowledges");
                any = true;
            }
        }
        if !any {
            return received;
        }
    }
}

fn assert_shared_out(received: &[Vec<u32>], total: u32) {
    let mut all: Vec<u32> = received.iter().flatten().copied().collect();
    all.sort_unstable();
    assert_eq!(
        all,
        (0..total).collect::<Vec<_>>(),
        "each message reaches one member"
    );
    assert!(
        received.iter().all(|member| !member.is_empty()),
        "every member took a share: {received:?}"
    );
}

/// The members of a share group, each a client of its own, split the messages between them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clients_in_a_share_group_split_the_messages() {
    let Some(url) = stand() else { return };
    let topic = topic("share-clients");
    let group = leak(format!("group-{}", std::process::id()));
    let first = connect(live_broker(&url)).await;
    let second = connect(live_broker(&url)).await;
    let mut one = open_source(&first, MqttTopic::new(topic).shared(group)).await;
    let mut two = open_source(&second, MqttTopic::new(topic).shared(group)).await;
    let sender = connect(live_broker(&url)).await;
    let publisher = publisher_of(&sender, MqttPublish::default().qos(Qos::AtLeastOnce)).await;
    for index in 0..20u32 {
        publish(&publisher, topic, &index.to_be_bytes()).await;
    }
    let received = drain_group(&mut [&mut one, &mut two]).await;
    assert_shared_out(&received, 20);
    drop((one, two));
    first.shutdown().await.expect("the first member ends");
    second.shutdown().await.expect("the second member ends");
    sender.shutdown().await.expect("the sender ends");
}

/// Two subscriptions of one session in one group share one subscription at the server; they take
/// the messages in turn, and none reaches both.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriptions_of_one_client_in_a_share_group_take_turns() {
    let Some(url) = stand() else { return };
    let topic = topic("share-local");
    let group = leak(format!("local-{}", std::process::id()));
    let receiver = connect(live_broker(&url)).await;
    let mut one = open_source(&receiver, MqttTopic::new(topic).shared(group)).await;
    let mut two = open_source(&receiver, MqttTopic::new(topic).shared(group)).await;
    let sender = connect(live_broker(&url)).await;
    let publisher = publisher_of(&sender, MqttPublish::default().qos(Qos::AtLeastOnce)).await;
    for index in 0..20u32 {
        publish(&publisher, topic, &index.to_be_bytes()).await;
    }
    let received = drain_group(&mut [&mut one, &mut two]).await;
    assert_shared_out(&received, 20);
    drop((one, two));
    receiver.shutdown().await.expect("the receiver ends");
    sender.shutdown().await.expect("the sender ends");
}
