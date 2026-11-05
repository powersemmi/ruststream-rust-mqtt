//! [`MqttRunner`], the task that owns a broker's connection, and the [`Network`] it runs over.
//!
//! The runner is the only code that touches the client. It waits for the app to connect the
//! broker, opens the connection, and then serves one loop: packets from the server, commands from
//! the broker's handles, and the keep-alive timer. Each of them is a step that completes without
//! waiting on the others, so a handler's acknowledgement never waits behind a read.

use alloc::boxed::Box;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::fmt;
use core::future::{Future, poll_fn};
use core::mem;
use core::num::NonZero;
use core::pin::pin;
use core::time::Duration;

use embassy_futures::select::{Either3, select3};
use embedded_io_async::{Read, Write};
use rust_mqtt::buffer::AllocBuffer;
use rust_mqtt::client::event::{Event, Publish};
use rust_mqtt::client::options::{
    AckOptions, ConnectOptions, DisconnectOptions, PublicationOptions, SubscriptionOptions,
    TopicReference,
};
use rust_mqtt::client::{Client, MqttError as ClientError};
use rust_mqtt::config::{KeepAlive, SessionExpiryInterval};
use rust_mqtt::types::{
    IdentifiedQoS, MqttBinary, MqttString, MqttStringPair, PacketIdentifier, ReasonCode,
    TopicFilter, TopicName, VarByteInt,
};
use ruststream::HeaderMap;

use crate::error::MqttError;
use crate::link::{Command, Link, Phase, Shared, Staged, SubscribeRequest};
use crate::message::{Acker, Body, MqttMessage};
use crate::qos::Qos;
use crate::queue::Queue;
use crate::topic::is_mqtt_text;

/// How many subscribes wait for the server's answer at once.
const SUBSCRIBE_MAXIMUM: usize = 8;
/// How many `QoS` 1 and 2 deliveries the server may have in flight to this client: the receive
/// maximum the session announces.
const RECEIVE_MAXIMUM: usize = 32;
/// How many `QoS` 1 and 2 publishes of this client may wait for the server's acknowledgement.
const SEND_MAXIMUM: usize = 32;
/// How many subscription identifiers one packet may name.
const SUBSCRIPTION_IDENTIFIERS: usize = 4;
/// How many user properties a packet carries, in either direction.
const USER_PROPERTIES: usize = 16;

/// The client the runner drives over a network's stream.
type Session<Stream> = Client<
    'static,
    'static,
    Stream,
    AllocBuffer,
    SUBSCRIBE_MAXIMUM,
    RECEIVE_MAXIMUM,
    SEND_MAXIMUM,
    SUBSCRIPTION_IDENTIFIERS,
    USER_PROPERTIES,
    0,
    0,
>;

/// The connection a runner opens: a TCP stream to the server, and the clock the keep-alive
/// reads.
///
/// The crate implements it over embassy-net (`EmbassyNetwork`, feature `embassy`) and over compio
/// (feature `compio`). A board with another link (a TLS session, a modem's socket) implements it
/// for that link.
///
/// A read of the stream must be cancel-safe: the runner stops waiting for the server's next
/// packet when a handle sends it work, and picks the read up again after.
///
/// # Examples
///
/// ```
/// use core::time::Duration;
///
/// use ruststream_rust_mqtt::Network;
///
/// /// A board's network: a socket it already holds, and the board's own timer.
/// struct Board<Socket> {
///     socket: Option<Socket>,
/// }
///
/// impl<Socket> Network for Board<Socket>
/// where
///     Socket: embedded_io_async::Read + embedded_io_async::Write,
/// {
///     type Stream<'net>
///         = Socket
///     where
///         Self: 'net;
///     type Error = &'static str;
///
///     async fn connect(&mut self, _host: &str, _port: u16) -> Result<Socket, &'static str> {
///         self.socket.take().ok_or("the board has one socket")
///     }
///
///     async fn sleep(duration: Duration) {
///         let _ = duration;
///         core::future::pending::<()>().await;
///     }
/// }
/// ```
pub trait Network {
    /// The stream the client reads and writes, which may borrow the network's buffers.
    type Stream<'net>: Read + Write
    where
        Self: 'net;
    /// What a failed connection reports.
    type Error: fmt::Debug;

    /// Opens a stream to `host` at `port`.
    fn connect(
        &mut self,
        host: &str,
        port: u16,
    ) -> impl Future<Output = Result<Self::Stream<'_>, Self::Error>>;

    /// Completes after `duration`, on the clock of the runtime the runner runs on.
    fn sleep(duration: Duration) -> impl Future<Output = ()>;
}

/// The task that owns an [`MqttBroker`](crate::MqttBroker)'s connection.
///
/// [`MqttBroker::new`](crate::MqttBroker::new) returns it beside the broker, and it has to run
/// for the broker to connect: a device spawns it as a task of its own. It waits until the app
/// connects the broker, opens the connection, serves it until the app shuts the broker down or
/// the connection is lost, and then waits for the next connect.
///
/// # Examples
///
/// ```
/// use ruststream_rust_mqtt::{MqttRunner, Network};
///
/// /// The body of the board's MQTT task, over whatever network the board brings.
/// async fn mqtt_task(runner: MqttRunner, network: impl Network) -> ! {
///     runner.run(network).await
/// }
/// ```
#[must_use = "the broker connects only while its runner runs"]
pub struct MqttRunner {
    link: Shared<Link>,
}

impl fmt::Debug for MqttRunner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttRunner")
            .field("address", &self.link.config.address())
            .finish_non_exhaustive()
    }
}

impl MqttRunner {
    pub(crate) const fn new(link: Shared<Link>) -> Self {
        Self { link }
    }

    /// Serves the broker's connections over `network`, one session after another, for as long as
    /// the task runs.
    pub async fn run<N: Network>(self, mut network: N) -> ! {
        loop {
            self.link.connect_requested.wait().await;
            let ended = session(&self.link, &mut network).await;
            finish(&self.link, ended.err());
        }
    }
}

/// Ends a session: every handle learns the broker is closed, every waiting command is answered,
/// and the next connect starts from a clean state.
fn finish(link: &Link, error: Option<MqttError>) {
    // The phase is set first: a handle that sends a command after the drain below reads it and
    // stops waiting.
    link.set_phase(Phase::Closed(error.clone()));
    link.close_all(error.as_ref());
    while let Ok(command) = link.commands.try_receive() {
        if let Command::Subscribe(request) = command {
            request.done.signal(Err(MqttError::NotConnected));
        }
    }
    // A connect that never got an answer gets this one.
    link.connected
        .signal(Err(error.unwrap_or(MqttError::NotConnected)));
    link.closed.signal(());
}

/// One session: the connection, the CONNECT handshake, and the serving loop.
async fn session<N: Network>(link: &Shared<Link>, network: &mut N) -> Result<(), MqttError> {
    let config = &link.config;
    let connect_failed = |reason: String| MqttError::Connect {
        address: config.address().to_string(),
        reason,
    };
    let (host, port) = config.endpoint()?;
    let stream = network
        .connect(host, port)
        .await
        .map_err(|error| connect_failed(format!("{error:?}")))?;

    // The client keeps each field it receives in a buffer of its own on the heap, and the
    // provider is a unit type, so it lives for the program without taking any memory.
    let buffer: &'static mut AllocBuffer = Box::leak(Box::new(AllocBuffer));
    let mut client: Session<N::Stream<'_>> = Client::new(buffer);
    // Every handshake waits for the handler's settlement.
    client.ack_manually_when(&|_| true);

    let mut options = ConnectOptions::new()
        .keep_alive(keep_alive(config.keep_alive_interval()))
        .session_expiry_interval(session_expiry(config.session_expiry()));
    if config.starts_clean() {
        options = options.clean_start();
    }
    if let Some((user, password)) = config.credentials_pair() {
        let user = MqttString::from_str(user)
            .map_err(|error| connect_failed(format!("the user name: {error:?}")))?;
        let password = MqttBinary::from_slice(password.as_bytes())
            .map_err(|error| connect_failed(format!("the password: {error:?}")))?;
        options = options.user_name(user).password(password);
    }
    let client_id = config.client_identifier();
    let client_id = if client_id.is_empty() {
        None
    } else {
        Some(
            MqttString::from_str(client_id)
                .map_err(|error| connect_failed(format!("the client identifier: {error:?}")))?,
        )
    };
    client
        .connect(stream, &options, client_id)
        .await
        .map_err(|error| connect_failed(format!("{error:?}")))?;

    link.set_phase(Phase::Open);
    link.connected.signal(Ok(()));
    serve::<N, _>(link, &mut client).await
}

/// The keep-alive the session announces.
fn keep_alive(interval: Duration) -> KeepAlive {
    let seconds = u16::try_from(interval.as_secs()).unwrap_or(u16::MAX);
    NonZero::new(seconds).map_or(KeepAlive::Infinite, KeepAlive::Seconds)
}

/// How long the server keeps the session after the connection ends.
fn session_expiry(expiry: Duration) -> SessionExpiryInterval {
    let seconds = u32::try_from(expiry.as_secs()).unwrap_or(u32::MAX);
    if seconds == u32::MAX {
        return SessionExpiryInterval::NeverEnd;
    }
    NonZero::new(seconds).map_or(
        SessionExpiryInterval::EndOnDisconnect,
        SessionExpiryInterval::Seconds,
    )
}

/// Whether the session goes on after a step.
enum Step {
    Continue,
    Stop,
}

/// What the runner keeps between the steps of one session.
struct State {
    /// Subscribes the server has not answered yet, by packet identifier.
    pending: Vec<(PacketIdentifier, Shared<SubscribeRequest>)>,
    /// Deliveries routed to a full queue: the runner reads nothing more until they are in.
    parked: Vec<(MqttMessage, Shared<Queue>)>,
    /// `QoS` 1 and 2 deliveries no subscription takes yet: a persistent session's server sends
    /// what it kept right after the CONNACK, before the app opens its subscriptions again. They
    /// wait, unacknowledged, for the subscription that takes them; the server's receive maximum
    /// bounds how many there are.
    held: Vec<MqttMessage>,
}

/// The serving loop of an open session.
async fn serve<N: Network, Stream: Read + Write>(
    link: &Shared<Link>,
    client: &mut Session<Stream>,
) -> Result<(), MqttError> {
    let keep_alive = link.config.keep_alive_interval();
    let mut state = State {
        pending: Vec::with_capacity(SUBSCRIBE_MAXIMUM),
        parked: Vec::with_capacity(SUBSCRIPTION_IDENTIFIERS),
        held: Vec::new(),
    };
    let mut ping = pin!(ping_after::<N>(keep_alive));
    loop {
        let step = if state.parked.is_empty() {
            match select3(client.poll_header(), link.commands.receive(), ping.as_mut()).await {
                Either3::First(header) => {
                    let header = header.map_err(lost)?;
                    let event = client.poll_body(header).await.map_err(lost)?;
                    on_event(link, client, &mut state, event).await?
                }
                Either3::Second(command) => {
                    ping.set(ping_after::<N>(keep_alive));
                    on_command(link, client, &mut state, command).await?
                }
                Either3::Third(()) => {
                    client.ping().await.map_err(lost)?;
                    ping.set(ping_after::<N>(keep_alive));
                    Step::Continue
                }
            }
        } else {
            match select3(room(&state.parked), link.commands.receive(), ping.as_mut()).await {
                Either3::First(()) => {
                    unpark(&mut state.parked);
                    Step::Continue
                }
                Either3::Second(command) => {
                    ping.set(ping_after::<N>(keep_alive));
                    on_command(link, client, &mut state, command).await?
                }
                Either3::Third(()) => {
                    client.ping().await.map_err(lost)?;
                    ping.set(ping_after::<N>(keep_alive));
                    Step::Continue
                }
            }
        };
        if matches!(step, Step::Stop) {
            return Ok(());
        }
    }
}

/// The keep-alive timer: due once nothing went out for the interval, never when pings are off.
async fn ping_after<N: Network>(keep_alive: Duration) {
    if keep_alive.is_zero() {
        core::future::pending::<()>().await;
    }
    N::sleep(keep_alive).await;
}

/// Ready once the first parked delivery's queue has room.
async fn room(parked: &[(MqttMessage, Shared<Queue>)]) {
    let queue = &parked[0].1;
    poll_fn(|cx| queue.poll_room(cx)).await;
}

/// Moves the parked deliveries into their queues while they have room.
fn unpark(parked: &mut Vec<(MqttMessage, Shared<Queue>)>) {
    while !parked.is_empty() {
        let (message, queue) = parked.remove(0);
        if let Err(message) = queue.push(message) {
            parked.insert(0, (message, queue));
            return;
        }
    }
}

/// The error a client failure ends the session with.
#[allow(clippy::needless_pass_by_value)] // `map_err` hands the error over by value.
fn lost<const N: usize>(error: ClientError<'_, N>) -> MqttError {
    MqttError::Connection(format!("{error:?}"))
}

/// Answers one packet from the server.
async fn on_event<Stream: Read + Write>(
    link: &Shared<Link>,
    client: &mut Session<Stream>,
    state: &mut State,
    event: Event<'static, SUBSCRIPTION_IDENTIFIERS, USER_PROPERTIES>,
) -> Result<Step, MqttError> {
    match event {
        Event::Publish(publish) => deliver(link, client, state, publish).await?,
        Event::Suback(suback) => {
            if let Some(at) = state
                .pending
                .iter()
                .position(|(packet, _)| *packet == suback.packet_identifier)
            {
                let (_, request) = state.pending.swap_remove(at);
                let answer = if suback.reason_code.is_success() {
                    Ok(())
                } else {
                    Err(MqttError::Subscribe {
                        filter: request.wire_filter.clone(),
                        reason: format!("the server answered {:?}", suback.reason_code),
                    })
                };
                request.done.signal(answer);
            }
        }
        // The server released a `QoS` 2 delivery the handler has received: the runner completes
        // the handshake.
        Event::PublishReleased(release) => client
            .manual_complete(release.packet_identifier, &AckOptions::new())
            .await
            .map_err(lost)?,
        // A `QoS` 2 delivery the server sends again after the handler received it: the
        // handshake already moved on, so there is nothing new to hand to the handler.
        Event::Duplicate(_)
        | Event::Pingresp
        | Event::Unsuback(_)
        | Event::PublishRejected(_)
        | Event::PublishAborted(_)
        | Event::PublishAcknowledged(_)
        | Event::PublishReceived(_)
        | Event::PublishComplete(_)
        | Event::Ignored
        | Event::Auth(_) => {}
    }
    Ok(Step::Continue)
}

/// Routes a PUBLISH to the subscriptions it was sent for.
async fn deliver<Stream: Read + Write>(
    link: &Shared<Link>,
    client: &mut Session<Stream>,
    state: &mut State,
    publish: Publish<'static, SUBSCRIPTION_IDENTIFIERS, USER_PROPERTIES>,
) -> Result<(), MqttError> {
    let Publish {
        identified_qos,
        topic,
        content_type,
        response_topic,
        correlation_data,
        user_properties,
        subscription_identifiers,
        message,
        ..
    } = publish;
    let (qos, packet) = match identified_qos {
        IdentifiedQoS::AtMostOnce => (Qos::AtMostOnce, None),
        IdentifiedQoS::AtLeastOnce(packet) => (Qos::AtLeastOnce, Some(packet)),
        IdentifiedQoS::ExactlyOnce(packet) => (Qos::ExactlyOnce, Some(packet)),
    };
    // The session announces no incoming topic alias, so every PUBLISH names its topic.
    let topic = match topic {
        TopicReference::Name(topic) | TopicReference::Mapping(topic, _) => topic,
        TopicReference::Alias(_) => {
            return acknowledge_unrouted(client, qos, packet).await;
        }
    };
    let mut identifiers = heapless::Vec::<u32, SUBSCRIPTION_IDENTIFIERS>::new();
    for identifier in &subscription_identifiers {
        let _ = identifiers.push(identifier.value());
    }
    let headers = headers_of(
        &user_properties,
        content_type.as_ref(),
        response_topic.as_ref(),
        correlation_data.as_ref(),
    );

    let mut targets = heapless::Vec::<Shared<Queue>, SUBSCRIPTION_IDENTIFIERS>::new();
    link.routes.lock(|routes| {
        routes
            .borrow_mut()
            .reach(&identifiers, topic.as_ref().as_str(), |_, queue| {
                let _ = targets.push(Shared::clone(queue));
            });
    });
    let Some((owner, others)) = targets.split_first() else {
        // A `QoS` 0 delivery nobody takes is gone, as at-most-once allows; an acknowledged one
        // waits for its subscription.
        if let Some(packet) = packet {
            state.held.push(MqttMessage::new(
                Body::Wire {
                    topic,
                    payload: message,
                },
                headers,
                qos,
                Some(Acker::Wire {
                    link: Shared::clone(link),
                    packet,
                }),
            ));
        }
        return Ok(());
    };
    // A packet that names several subscriptions was sent once for all of them: every other one
    // receives a copy that settles nothing, and the first owns the acknowledgement.
    for queue in others {
        let copy = MqttMessage::new(
            Body::Wire {
                topic: TopicName::new(
                    MqttString::try_from(String::from(topic.as_ref().as_str()))
                        .expect("a topic the client received is a valid string"),
                )
                .expect("a topic the client received is a topic"),
                payload: rust_mqtt::Bytes::Owned(Box::from(&*message)),
            },
            headers.clone(),
            qos,
            None,
        );
        place(state, queue, copy);
    }
    let delivery = MqttMessage::new(
        Body::Wire {
            topic,
            payload: message,
        },
        headers,
        qos,
        packet.map(|packet| Acker::Wire {
            link: Shared::clone(link),
            packet,
        }),
    );
    place(state, owner, delivery);
    Ok(())
}

/// Hands the held deliveries a new subscription's filter matches to that subscription.
fn release_held(link: &Shared<Link>, state: &mut State, request: &SubscribeRequest) {
    if state.held.is_empty() {
        return;
    }
    let filter = crate::routes::plain_filter(&request.wire_filter);
    let held = mem::take(&mut state.held);
    for message in held {
        if !crate::topic::matches(message.topic(), filter) {
            state.held.push(message);
            continue;
        }
        let mut target = None;
        link.routes.lock(|routes| {
            routes
                .borrow_mut()
                .reach(&[request.identifier], message.topic(), |_, queue| {
                    target = Some(Shared::clone(queue));
                });
        });
        match target {
            Some(queue) => place(state, &queue, message),
            None => state.held.push(message),
        }
    }
}

/// Pushes a delivery into its queue, or parks it until the queue has room.
fn place(state: &mut State, queue: &Shared<Queue>, message: MqttMessage) {
    if let Err(message) = queue.push(message) {
        state.parked.push((message, Shared::clone(queue)));
    }
}

/// Acknowledges a delivery no subscription takes, so the server's window does not shrink by it.
async fn acknowledge_unrouted<Stream: Read + Write>(
    client: &mut Session<Stream>,
    qos: Qos,
    packet: Option<PacketIdentifier>,
) -> Result<(), MqttError> {
    match packet {
        Some(packet) => acknowledge(client, qos, packet).await,
        None => Ok(()),
    }
}

/// Sends the acknowledgement a handler's settlement asks for.
async fn acknowledge<Stream: Read + Write>(
    client: &mut Session<Stream>,
    qos: Qos,
    packet: PacketIdentifier,
) -> Result<(), MqttError> {
    let options = AckOptions::new();
    let sent = match qos {
        Qos::AtMostOnce => Ok(()),
        Qos::AtLeastOnce => {
            client
                .manual_acknowledge(packet, ReasonCode::Success, &options)
                .await
        }
        Qos::ExactlyOnce => {
            client
                .manual_receive(packet, ReasonCode::Success, &options)
                .await
        }
    };
    sent.map_err(lost)
}

/// Carries out one command of a handle.
async fn on_command<Stream: Read + Write>(
    link: &Shared<Link>,
    client: &mut Session<Stream>,
    state: &mut State,
    command: Command,
) -> Result<Step, MqttError> {
    match command {
        Command::Ack { packet, qos } => acknowledge(client, qos, packet).await?,
        Command::Publish(index) => publish_staged(link, client, state, index).await?,
        Command::Subscribe(request) => {
            let filter = MqttString::from_str(&request.wire_filter)
                .ok()
                .and_then(TopicFilter::new);
            let Some(filter) = filter else {
                request.done.signal(Err(MqttError::Subscribe {
                    filter: request.wire_filter.clone(),
                    reason: "not a filter the client sends".to_string(),
                }));
                return Ok(Step::Continue);
            };
            let identifier = VarByteInt::new(request.identifier).ok_or_else(|| {
                MqttError::Invalid("the subscription identifiers ran out".to_string())
            })?;
            let options = SubscriptionOptions::new()
                .qos(request.qos.to_client())
                .subscription_identifier(identifier);
            match client.subscribe(filter, &options).await {
                Ok(packet) => {
                    release_held(link, state, &request);
                    state.pending.push((packet, request));
                }
                Err(error) => request.done.signal(Err(MqttError::Subscribe {
                    filter: request.wire_filter.clone(),
                    reason: format!("{error:?}"),
                })),
            }
        }
        Command::Shutdown => {
            // The session is over whichever way the DISCONNECT goes.
            let _ = client.disconnect(&DisconnectOptions::new()).await;
            return Ok(Step::Stop);
        }
    }
    Ok(Step::Continue)
}

/// Sends the publish staged in slot `index` and answers it.
async fn publish_staged<Stream: Read + Write>(
    link: &Shared<Link>,
    client: &mut Session<Stream>,
    state: &mut State,
    index: usize,
) -> Result<(), MqttError> {
    let slot = &link.slots[index];
    // The staged buffers move out for the send and back after it, so they keep their capacity.
    let mut staged = slot
        .staged
        .lock(|staged| mem::take(&mut *staged.borrow_mut()));
    let answer = loop {
        match send(client, &staged).await {
            // The server's receive maximum is full: the runner reads acknowledgements until it
            // frees a place, then sends.
            Err(Refusal::Quota) => {
                let header = client.poll_header().await.map_err(lost)?;
                let event = client.poll_body(header).await.map_err(lost)?;
                on_event(link, client, state, event).await?;
            }
            Err(Refusal::Message(error)) => break Err(error),
            Err(Refusal::Session(error)) => {
                let headers = mem::take(&mut staged.headers);
                slot.staged.lock(|slot| {
                    let mut slot = slot.borrow_mut();
                    *slot = mem::take(&mut staged);
                    slot.headers = headers;
                });
                slot.done.signal(Err(error.clone()));
                return Err(error);
            }
            Ok(()) => break Ok(()),
        }
    };
    slot.staged.lock(|slot| *slot.borrow_mut() = staged);
    slot.done.signal(answer);
    Ok(())
}

/// Why a send did not go out.
enum Refusal {
    /// The server's receive maximum is full for now.
    Quota,
    /// This message cannot be sent; the session goes on.
    Message(MqttError),
    /// The connection failed.
    Session(MqttError),
}

/// Sends one staged publish.
async fn send<Stream: Read + Write>(
    client: &mut Session<Stream>,
    staged: &Staged,
) -> Result<(), Refusal> {
    let refused = |reason: String| {
        Refusal::Message(MqttError::Publish {
            topic: staged.topic.clone(),
            reason,
        })
    };
    let topic = MqttString::from_str(&staged.topic)
        .ok()
        .and_then(TopicName::new)
        .ok_or_else(|| refused("not a topic the client sends".to_string()))?;
    let mut options =
        PublicationOptions::new(TopicReference::Name(topic)).qos(staged.qos.to_client());
    if staged.retain {
        options = options.retain();
    }
    let mut properties = heapless::Vec::<MqttStringPair<'_>, USER_PROPERTIES>::new();
    for (name, value) in staged.headers.iter() {
        if name == "correlation-id" {
            let data = MqttBinary::from_slice(value)
                .map_err(|_| refused("the correlation-id header is too long".to_string()))?;
            options = options.correlation_data(data);
            continue;
        }
        let text = core::str::from_utf8(value)
            .ok()
            .filter(|text| is_mqtt_text(name) && is_mqtt_text(text))
            .ok_or_else(|| {
                refused(format!(
                    "header {name:?} is not text an MQTT property carries: its name and value \
                     must be UTF-8 with no control character"
                ))
            })?;
        let text = MqttString::from_str(text)
            .map_err(|_| refused(format!("header {name:?} is too long")))?;
        match name {
            "content-type" => {
                options = options
                    .payload_format_indicator(is_text_media_type(text.as_str()))
                    .content_type(text);
            }
            "reply-to" => {
                let topic = TopicName::new(text)
                    .ok_or_else(|| refused("the reply-to header is not a topic".to_string()))?;
                options = options.response_topic(topic);
            }
            other => {
                let name = MqttString::from_str(other)
                    .map_err(|_| refused(format!("header {other:?} is too long")))?;
                properties
                    .push(MqttStringPair::new(name, text))
                    .map_err(|_| {
                        refused(format!(
                            "a publish carries at most {USER_PROPERTIES} user properties"
                        ))
                    })?;
            }
        }
    }
    options = options.user_properties(&properties);
    match client
        .publish(&options, rust_mqtt::Bytes::Borrowed(&staged.payload))
        .await
    {
        Ok(_) => Ok(()),
        Err(ClientError::SendQuotaExceeded) => Err(Refusal::Quota),
        Err(
            error @ (ClientError::ServerMaximumPacketSizeExceeded
            | ClientError::PacketMaximumLengthExceeded),
        ) => Err(refused(format!("{error:?}"))),
        Err(error) => Err(Refusal::Session(lost(error))),
    }
}

/// Whether a content type names text, which sets the payload format indicator.
fn is_text_media_type(content_type: &str) -> bool {
    let media = content_type.split(';').next().unwrap_or_default().trim();
    let lower = |prefix: &str| {
        media.len() >= prefix.len() && media[..prefix.len()].eq_ignore_ascii_case(prefix)
    };
    lower("text/")
        || media.eq_ignore_ascii_case("application/json")
        || media.to_ascii_lowercase().ends_with("+json")
}

/// The headers a delivery carries: its user properties, and the properties that have a header
/// of their own.
fn headers_of(
    user_properties: &[MqttStringPair<'_>],
    content_type: Option<&MqttString<'_>>,
    response_topic: Option<&TopicName<'_>>,
    correlation_data: Option<&MqttBinary<'_>>,
) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for pair in user_properties {
        headers.insert(
            pair.name.as_str().to_string(),
            ruststream::Bytes::copy_from_slice(pair.value.as_str().as_bytes()),
        );
    }
    if let Some(content_type) = content_type {
        headers.insert(
            "content-type",
            ruststream::Bytes::copy_from_slice(content_type.as_str().as_bytes()),
        );
    }
    if let Some(response_topic) = response_topic {
        headers.insert(
            "reply-to",
            ruststream::Bytes::copy_from_slice(response_topic.as_ref().as_str().as_bytes()),
        );
    }
    if let Some(correlation) = correlation_data {
        headers.insert(
            "correlation-id",
            ruststream::Bytes::copy_from_slice(correlation.as_bytes()),
        );
    }
    headers
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_textual_content_type_sets_the_payload_format_indicator() {
        assert!(is_text_media_type("application/json"));
        assert!(is_text_media_type("text/plain; charset=utf-8"));
        assert!(is_text_media_type("application/vnd.api+json"));
        assert!(!is_text_media_type("application/octet-stream"));
    }

    #[test]
    fn a_keep_alive_of_zero_turns_pings_off() {
        assert_eq!(keep_alive(Duration::ZERO), KeepAlive::Infinite);
        assert_eq!(
            keep_alive(Duration::from_secs(30)),
            KeepAlive::Seconds(NonZero::new(30).expect("thirty is not zero"))
        );
    }
}
