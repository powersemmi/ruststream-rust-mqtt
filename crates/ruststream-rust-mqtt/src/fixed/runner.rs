//! [`MqttRunner`] of the device variant: the task that owns a broker's connection and speaks the
//! protocol from the broker's own buffers.
//!
//! The runner waits for the app to connect the broker, opens the connection, and then serves one
//! loop: packets from the server, commands from the broker's handles, and the keep-alive timer.
//! A packet is read straight into a receive buffer, exactly as long as its fixed header says, so
//! a PUBLISH stays where it arrived: the buffer moves into the delivery, which borrows its topic
//! and payload from it.

use core::fmt;
use core::pin::pin;
use core::time::Duration;

use embassy_futures::select::{Either3, select3};
use embedded_io_async::{Read, Write};
use heapless::{Deque, Vec as FixedVec};
use ruststream_core::diag::warn;
use zzq_proto::packet::decode_body;
use zzq_proto::property::{MAXIMUM_PACKET_SIZE, RECEIVE_MAXIMUM, SERVER_KEEP_ALIVE};
use zzq_proto::{Packet, PacketType, Version, fixed_header};

use crate::fixed::config::MqttConfig;
use crate::fixed::error::MqttError;
use crate::fixed::state::{
    Command, Held, IN_FLIGHT, InboundLease, Link, Mode, OutboundLease, Sent,
};
use crate::fixed::wire::{self, Unreadable};
use crate::network::Network;
use crate::qos::Qos;
use crate::topic::matches;

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
pub struct MqttRunner<
    const MESSAGES: usize = 4,
    const PACKET: usize = 1024,
    const SUBSCRIPTIONS: usize = 8,
> {
    link: &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
    config: MqttConfig,
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> fmt::Debug
    for MqttRunner<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttRunner")
            .field("address", &self.config.address())
            .finish_non_exhaustive()
    }
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize>
    MqttRunner<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    pub(crate) const fn new(
        link: &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
        config: MqttConfig,
    ) -> Self {
        Self { link, config }
    }

    /// Serves the broker's connections over `network`, one session after another, for as long as
    /// the task runs.
    pub async fn run<N: Network>(self, mut network: N) -> ! {
        loop {
            self.link.connect_requested.wait().await;
            let ended = session(self.link, &self.config, &mut network).await;
            let error = ended.err();
            self.link.end_session(error);
            // A connect that never got an answer gets this one.
            self.link
                .connected
                .signal(Err(error.unwrap_or(MqttError::NotConnected)));
            self.link.closed.signal(());
        }
    }
}

/// The receive buffer the runner reads the next packet into, and how much of it holds bytes.
struct Reader<const MESSAGES: usize, const PACKET: usize> {
    buffer: Option<InboundLease<MESSAGES, PACKET>>,
    filled: usize,
}

/// Where a whole packet sits in the reader's buffer.
#[derive(Debug, Clone, Copy)]
struct Framed {
    header_len: usize,
    len: usize,
}

impl<const MESSAGES: usize, const PACKET: usize> Reader<MESSAGES, PACKET> {
    const fn new() -> Self {
        Self {
            buffer: None,
            filled: 0,
        }
    }

    /// Reads until a whole packet sits in the buffer, taking a free receive buffer first if the
    /// reader holds none. It reads no byte past the packet, so the next one starts at the front
    /// of the next buffer.
    ///
    /// # Cancel safety
    ///
    /// Cancel-safe, given a cancel-safe read of the stream: what was read stays in the buffer
    /// and the next call goes on from there.
    async fn fill<Stream: Read, const SUBSCRIPTIONS: usize>(
        &mut self,
        link: &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
        stream: &mut Stream,
    ) -> Result<Framed, MqttError> {
        loop {
            let buffer = match &mut self.buffer {
                Some(buffer) => buffer,
                held @ None => held.insert(link.inbound.take().await),
            };
            let bytes = &mut buffer.bytes;
            let want = match fixed_header(&bytes[..self.filled]) {
                Ok(Some(header)) => {
                    let len = header.packet_len();
                    if len > PACKET {
                        return Err(MqttError::PacketTooLarge {
                            size: len,
                            capacity: PACKET,
                        });
                    }
                    if self.filled == len {
                        return Ok(Framed {
                            header_len: header.header_len,
                            len,
                        });
                    }
                    len
                }
                Ok(None) => (self.filled + 1).max(2),
                Err(_) => {
                    return Err(MqttError::Connection(
                        "the server sent a malformed fixed header",
                    ));
                }
            };
            let read = stream
                .read(&mut bytes[self.filled..want])
                .await
                .map_err(|error| {
                    warn!(target: "ruststream::rust_mqtt", error = ?error, "a read of the MQTT connection failed");
                    MqttError::Connection("a read failed")
                })?;
            if read == 0 {
                return Err(MqttError::Connection("the server closed the connection"));
            }
            self.filled += read;
        }
    }

    /// The buffer holding the packet just read, for a delivery to keep.
    fn take(&mut self) -> Option<InboundLease<MESSAGES, PACKET>> {
        self.filled = 0;
        self.buffer.take()
    }

    /// The packet just read, done with: the buffer reads the next one.
    const fn consume(&mut self) {
        self.filled = 0;
    }

    fn bytes(&self) -> &[u8] {
        self.buffer.as_ref().map_or(&[], |buffer| &buffer.bytes[..])
    }
}

/// Writes one packet and pushes it out.
async fn write<Stream: Write>(stream: &mut Stream, bytes: &[u8]) -> Result<(), MqttError> {
    let written = match stream.write_all(bytes).await {
        Ok(()) => stream.flush().await,
        Err(error) => Err(error),
    };
    written.map_err(|error| {
        warn!(target: "ruststream::rust_mqtt", error = ?error, "a write of the MQTT connection failed");
        MqttError::Connection("a write failed")
    })
}

/// One session: the connection, the CONNECT handshake, and the serving loop.
async fn session<
    N: Network,
    const MESSAGES: usize,
    const PACKET: usize,
    const SUBSCRIPTIONS: usize,
>(
    link: &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
    config: &MqttConfig,
    network: &mut N,
) -> Result<(), MqttError> {
    let address = config.address();
    let (host, port) = config.endpoint()?;
    let mut stream = network.connect(host, port).await.map_err(|error| {
        warn!(target: "ruststream::rust_mqtt", address = %address, error = ?error, "the MQTT connection could not be opened");
        MqttError::Connect {
            address,
            reason: "the network could not open the connection",
        }
    })?;

    // One buffer stays free for the server's answers while the others wait for their handlers.
    let receive_maximum = MESSAGES - 1;
    {
        let mut buffer = link.outbound.take().await;
        let len = wire::connect(&mut buffer.bytes, config, receive_maximum, PACKET)?;
        write(&mut stream, &buffer.bytes[..len]).await?;
    }
    let mut reader = Reader::<MESSAGES, PACKET>::new();
    let framed = reader.fill(link, &mut stream).await?;
    let (keep_alive, server_receive, server_maximum) = {
        let bytes = reader.bytes();
        match decode_body(bytes[0], &bytes[framed.header_len..framed.len], Version::V5) {
            Ok(Packet::ConnAck(connack)) if connack.code < 0x80 => {
                let properties = connack.properties;
                let keep_alive = properties.get_int(SERVER_KEEP_ALIVE).map_or_else(
                    || config.keep_alive_interval(),
                    |seconds| Duration::from_secs(u64::from(seconds)),
                );
                let receive = properties
                    .get_int(RECEIVE_MAXIMUM)
                    .map_or(IN_FLIGHT, |most| most as usize);
                let maximum = properties
                    .get_int(MAXIMUM_PACKET_SIZE)
                    .map_or(PACKET, |most| most as usize);
                (keep_alive, receive, maximum)
            }
            Ok(Packet::ConnAck(connack)) => {
                return Err(MqttError::Refused {
                    address,
                    code: connack.code,
                });
            }
            _ => {
                return Err(MqttError::Connection(
                    "the server answered the CONNECT with another packet",
                ));
            }
        }
    };
    reader.consume();

    link.begin_session(Mode::Wire, server_receive, server_maximum);
    link.connected.signal(Ok(()));
    let mut state = State::new();
    let served = serve::<N, _, MESSAGES, PACKET, SUBSCRIPTIONS>(
        link,
        &mut stream,
        &mut reader,
        &mut state,
        keep_alive,
    )
    .await;
    if served.is_ok() {
        // The session is over whichever way the DISCONNECT goes.
        let mut buf = [0; 4];
        let len = wire::disconnect(&mut buf);
        if write(&mut stream, &buf[..len]).await.is_ok() {
            N::close(stream).await;
        }
    }
    served
}

/// Whether the session goes on after a step.
enum Step {
    Continue,
    Stop,
}

/// Where a `QoS` 2 delivery stands between its PUBLISH and the server's PUBREL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    /// Handed to its subscription; the handler has not settled it.
    Delivered,
    /// The handler settled it and the PUBREC went out.
    Received,
}

/// A PUBLISH that reached several subscriptions: each gets a copy in a receive buffer of its own,
/// and the first one keeps the buffer the packet arrived in.
struct Copying<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> {
    source: InboundLease<MESSAGES, PACKET>,
    targets: FixedVec<usize, SUBSCRIPTIONS>,
    next: usize,
}

/// What the runner keeps between the steps of one session.
struct State<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> {
    /// Subscribes the server has not answered yet, by packet identifier.
    subscribing: FixedVec<(u16, Held), IN_FLIGHT>,
    /// The `QoS` 2 deliveries the server has not released yet.
    exactly_once: FixedVec<(u16, Stage), MESSAGES>,
    /// `QoS` 1 and 2 deliveries no subscription takes yet: a persistent session's server sends
    /// what it kept right after the CONNACK, before the app opens its subscriptions again. They
    /// wait, unacknowledged, for the subscription that takes them; the session's receive maximum
    /// bounds how many there are.
    held: Deque<InboundLease<MESSAGES, PACKET>, MESSAGES>,
    copying: Option<Copying<MESSAGES, PACKET, SUBSCRIPTIONS>>,
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize>
    State<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    const fn new() -> Self {
        Self {
            subscribing: FixedVec::new(),
            exactly_once: FixedVec::new(),
            held: Deque::new(),
            copying: None,
        }
    }

    fn stage(&self, packet: u16) -> Option<Stage> {
        self.exactly_once
            .iter()
            .find(|(held, _)| *held == packet)
            .map(|(_, stage)| *stage)
    }

    fn set_stage(&mut self, packet: u16, stage: Stage) {
        match self
            .exactly_once
            .iter_mut()
            .find(|(held, _)| *held == packet)
        {
            Some((_, held)) => *held = stage,
            // The session's receive maximum bounds the `QoS` 2 deliveries in flight below the
            // count of receive buffers.
            None => {
                let _ = self.exactly_once.push((packet, stage));
            }
        }
    }

    fn release(&mut self, packet: u16) {
        self.exactly_once.retain(|(held, _)| *held != packet);
    }
}

/// The keep-alive timer: due once nothing went out for the interval, never when pings are off.
async fn ping_after<N: Network>(keep_alive: Duration) {
    if keep_alive.is_zero() {
        core::future::pending::<()>().await;
    }
    N::sleep(keep_alive).await;
}

/// What woke the serving loop.
enum Woken<const MESSAGES: usize, const PACKET: usize> {
    Packet(Result<Framed, MqttError>),
    Buffer(InboundLease<MESSAGES, PACKET>),
    Command(Command<PACKET>),
    Ping,
}

/// The serving loop of an open session.
async fn serve<
    N: Network,
    Stream: Read + Write,
    const MESSAGES: usize,
    const PACKET: usize,
    const SUBSCRIPTIONS: usize,
>(
    link: &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
    stream: &mut Stream,
    reader: &mut Reader<MESSAGES, PACKET>,
    state: &mut State<MESSAGES, PACKET, SUBSCRIPTIONS>,
    keep_alive: Duration,
) -> Result<(), MqttError> {
    let mut ping = pin!(ping_after::<N>(keep_alive));
    loop {
        let woken = if state.copying.is_some() {
            match select3(link.inbound.take(), link.commands.receive(), ping.as_mut()).await {
                Either3::First(buffer) => Woken::Buffer(buffer),
                Either3::Second(command) => Woken::Command(command),
                Either3::Third(()) => Woken::Ping,
            }
        } else {
            match select3(
                reader.fill(link, stream),
                link.commands.receive(),
                ping.as_mut(),
            )
            .await
            {
                Either3::First(framed) => Woken::Packet(framed),
                Either3::Second(command) => Woken::Command(command),
                Either3::Third(()) => Woken::Ping,
            }
        };
        let step = match woken {
            Woken::Packet(framed) => on_packet(link, stream, reader, state, framed?).await?,
            Woken::Buffer(buffer) => {
                copy_next(link, state, buffer);
                Step::Continue
            }
            Woken::Command(command) => {
                ping.set(ping_after::<N>(keep_alive));
                on_command(link, stream, state, command).await?
            }
            Woken::Ping => {
                let mut buf = [0; 4];
                let len = wire::ping(&mut buf);
                write(stream, &buf[..len]).await?;
                ping.set(ping_after::<N>(keep_alive));
                Step::Continue
            }
        };
        if matches!(step, Step::Stop) {
            return Ok(());
        }
    }
}

/// Sends a PUBACK, PUBREC, PUBREL or PUBCOMP.
async fn send_ack<Stream: Write>(
    stream: &mut Stream,
    kind: PacketType,
    packet: u16,
) -> Result<(), MqttError> {
    let mut buf = [0; 4];
    let len = wire::ack(&mut buf, kind, packet);
    write(stream, &buf[..len]).await
}

/// Acknowledges a delivery no subscription takes or none can read, so the server's window does
/// not shrink by it.
async fn acknowledge_unrouted<Stream: Write>(
    stream: &mut Stream,
    qos: Qos,
    packet: u16,
) -> Result<(), MqttError> {
    match qos {
        Qos::AtMostOnce => Ok(()),
        Qos::AtLeastOnce => send_ack(stream, PacketType::PubAck, packet).await,
        Qos::ExactlyOnce => send_ack(stream, PacketType::PubRec, packet).await,
    }
}

/// Answers one packet from the server.
async fn on_packet<
    Stream: Read + Write,
    const MESSAGES: usize,
    const PACKET: usize,
    const SUBSCRIPTIONS: usize,
>(
    link: &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
    stream: &mut Stream,
    reader: &mut Reader<MESSAGES, PACKET>,
    state: &mut State<MESSAGES, PACKET, SUBSCRIPTIONS>,
    framed: Framed,
) -> Result<Step, MqttError> {
    let first = reader.bytes()[0];
    if first >> 4 == PacketType::Publish as u8 {
        let Some(buffer) = reader.take() else {
            return Ok(Step::Continue);
        };
        return deliver(link, stream, state, buffer, framed)
            .await
            .map(|()| Step::Continue);
    }
    let decoded = {
        let bytes = reader.bytes();
        decode_body(first, &bytes[framed.header_len..framed.len], Version::V5)
    };
    let packet = match decoded {
        Ok(packet) => packet,
        Err(error) => {
            warn!(target: "ruststream::rust_mqtt", error = %error, "the MQTT server sent a malformed packet");
            return Err(MqttError::Connection("the server sent a malformed packet"));
        }
    };
    // What the packet says is read before the buffer is reused.
    let step = match packet {
        Packet::PubRec(ack) if ack.reason < 0x80 => Reaction::Send(PacketType::PubRel, ack.pkid),
        // A refused PUBREC ends the exchange as a PUBACK or a PUBCOMP does.
        Packet::PubAck(ack) | Packet::PubComp(ack) | Packet::PubRec(ack) => {
            link.release_packet(ack.pkid);
            Reaction::None
        }
        Packet::PubRel(ack) => {
            state.release(ack.pkid);
            Reaction::Send(PacketType::PubComp, ack.pkid)
        }
        Packet::SubAck(suback) => Reaction::Subscribed {
            packet: suback.pkid,
            code: suback.codes.first().copied().unwrap_or(0x80),
        },
        Packet::Disconnect(disconnect) => {
            return Err(MqttError::Disconnected {
                code: disconnect.reason,
            });
        }
        Packet::PingResp | Packet::UnsubAck(_) | Packet::Auth(_) => Reaction::None,
        Packet::ConnAck(_)
        | Packet::Connect(_)
        | Packet::Publish(_)
        | Packet::Subscribe(_)
        | Packet::Unsubscribe(_)
        | Packet::PingReq => {
            return Err(MqttError::Connection(
                "the server sent a packet a client never receives",
            ));
        }
    };
    reader.consume();
    match step {
        Reaction::None => {}
        Reaction::Send(kind, packet) => send_ack(stream, kind, packet).await?,
        Reaction::Subscribed { packet, code } => subscribed(link, state, packet, code),
    }
    Ok(Step::Continue)
}

/// What the runner does about a control packet once its buffer is free again.
enum Reaction {
    None,
    Send(PacketType, u16),
    Subscribed { packet: u16, code: u8 },
}

/// Answers the subscription whose SUBSCRIBE the server answered, and hands it the held
/// deliveries its filter matches.
fn subscribed<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize>(
    link: &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
    state: &mut State<MESSAGES, PACKET, SUBSCRIPTIONS>,
    packet: u16,
    code: u8,
) {
    link.release_packet(packet);
    let Some(at) = state
        .subscribing
        .iter()
        .position(|(waiting, _)| *waiting == packet)
    else {
        return;
    };
    let (_, held) = state.subscribing.swap_remove(at);
    let source = link.table.lock(|table| table.borrow().source(held));
    let result = if code < 0x80 {
        Ok(())
    } else {
        if let Some(source) = &source {
            warn!(
                target: "ruststream::rust_mqtt",
                filter = %source.filter.as_str(),
                code = code,
                "the MQTT server refused a subscription"
            );
        }
        Err(MqttError::Subscribe { code })
    };
    if result.is_ok()
        && let Some(source) = source
    {
        release_held(link, state, source.filter.as_str(), source.identifier);
    }
    link.subscribed[held.index].signal((held.generation, result));
}

/// Hands the held deliveries `filter` matches to the subscriptions of its wire filter.
fn release_held<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize>(
    link: &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
    state: &mut State<MESSAGES, PACKET, SUBSCRIPTIONS>,
    filter: &str,
    identifier: u32,
) {
    for _ in 0..state.held.len() {
        let Some(delivery) = state.held.pop_front() else {
            break;
        };
        if !matches(delivery.topic(), filter) {
            // The deque had room for this one before it was taken out.
            let _ = state.held.push_back(delivery);
            continue;
        }
        let packet = delivery.packet;
        let qos = delivery.qos;
        let mut delivery = Some(delivery);
        link.table.lock(|table| {
            let mut table = table.borrow_mut();
            let mut target = None;
            table.reach(&[identifier], "", |member, _| target = Some(member));
            if let (Some(member), Some(delivery)) = (target, delivery.take()) {
                table.push(member, delivery);
            }
        });
        match delivery {
            Some(delivery) => {
                let _ = state.held.push_back(delivery);
            }
            None if qos == Qos::ExactlyOnce => state.set_stage(packet, Stage::Delivered),
            None => {}
        }
    }
}

/// Routes the PUBLISH in `buffer` to the subscriptions it was sent for.
async fn deliver<
    Stream: Write,
    const MESSAGES: usize,
    const PACKET: usize,
    const SUBSCRIPTIONS: usize,
>(
    link: &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
    stream: &mut Stream,
    state: &mut State<MESSAGES, PACKET, SUBSCRIPTIONS>,
    mut buffer: InboundLease<MESSAGES, PACKET>,
    framed: Framed,
) -> Result<(), MqttError> {
    let read = wire::read_publish(&mut buffer, framed.header_len, framed.len, link.session());
    let (qos, packet) = (buffer.qos, buffer.packet);
    match read {
        Ok(()) => {}
        Err(Unreadable::Malformed) => {
            return Err(MqttError::Connection("the server sent a malformed PUBLISH"));
        }
        Err(Unreadable::Alias) => {
            // The session announces no topic alias, so a server that sends one breaks the
            // protocol; the delivery has no topic to route by and is acknowledged away.
            drop(buffer);
            return acknowledge_unrouted(stream, qos, packet).await;
        }
        Err(Unreadable::Headers) => {
            warn!(
                target: "ruststream::rust_mqtt",
                topic = %buffer.topic(),
                entries = ruststream_core::HEADER_ENTRIES,
                bytes = ruststream_core::HEADER_BYTES,
                "an MQTT delivery carries more properties than the fixed headers of a message hold; it is acknowledged and dropped"
            );
            drop(buffer);
            return acknowledge_unrouted(stream, qos, packet).await;
        }
    }
    if qos == Qos::ExactlyOnce {
        match state.stage(packet) {
            // A PUBLISH the server sends again after the handler received it: the handshake
            // moved on, so the PUBREC goes out again and nothing new reaches the handler.
            Some(Stage::Received) => return send_ack(stream, PacketType::PubRec, packet).await,
            Some(Stage::Delivered) => return Ok(()),
            None => {}
        }
    }
    let mut targets = FixedVec::<usize, SUBSCRIPTIONS>::new();
    link.table.lock(|table| {
        table
            .borrow_mut()
            .reach(&buffer.identifiers, buffer.topic(), |member, _| {
                let _ = targets.push(member);
            });
    });
    if targets.is_empty() {
        // A `QoS` 0 delivery nobody takes is gone, as at-most-once allows; an acknowledged one
        // waits for its subscription.
        if qos != Qos::AtMostOnce {
            // The receive maximum bounds the held deliveries below the count of buffers.
            let _ = state.held.push_back(buffer);
        }
        return Ok(());
    }
    if qos == Qos::ExactlyOnce {
        state.set_stage(packet, Stage::Delivered);
    }
    if targets.len() == 1 {
        link.table
            .lock(|table| table.borrow_mut().push(targets[0], buffer));
        return Ok(());
    }
    state.copying = Some(Copying {
        source: buffer,
        targets,
        next: 1,
    });
    // The copies take free buffers as they come; the loop waits for each one while it serves the
    // handles.
    while let Some(copy) = link.inbound.try_take() {
        copy_next(link, state, copy);
        if state.copying.is_none() {
            break;
        }
    }
    Ok(())
}

/// Fills `copy` with the next copy of the packet being copied, and hands the packet itself to its
/// first subscription once every other has its copy.
fn copy_next<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize>(
    link: &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
    state: &mut State<MESSAGES, PACKET, SUBSCRIPTIONS>,
    mut copy: InboundLease<MESSAGES, PACKET>,
) {
    let Some(copying) = state.copying.as_mut() else {
        return;
    };
    copy.copy_from(&copying.source);
    // A copy settles nothing: the packet was sent once for every subscription, and the first
    // owns the acknowledgement.
    copy.packet = 0;
    let target = copying.targets[copying.next];
    copying.next += 1;
    let done = copying.next == copying.targets.len();
    link.table
        .lock(|table| table.borrow_mut().push(target, copy));
    if done && let Some(copying) = state.copying.take() {
        let owner = copying.targets[0];
        link.table
            .lock(|table| table.borrow_mut().push(owner, copying.source));
    }
}

/// Carries out one command of a handle.
async fn on_command<
    Stream: Write,
    const MESSAGES: usize,
    const PACKET: usize,
    const SUBSCRIPTIONS: usize,
>(
    link: &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
    stream: &mut Stream,
    state: &mut State<MESSAGES, PACKET, SUBSCRIPTIONS>,
    command: Command<PACKET>,
) -> Result<Step, MqttError> {
    match command {
        Command::Send(buffer) => send(link, stream, state, buffer).await?,
        Command::Ack {
            packet,
            qos,
            session,
        } => {
            // An acknowledgement of a session that is gone names a packet the server no longer
            // waits for.
            if session == link.session() {
                match qos {
                    Qos::AtMostOnce => {}
                    Qos::AtLeastOnce => send_ack(stream, PacketType::PubAck, packet).await?,
                    Qos::ExactlyOnce => {
                        send_ack(stream, PacketType::PubRec, packet).await?;
                        state.set_stage(packet, Stage::Received);
                    }
                }
            }
        }
        Command::Shutdown => return Ok(Step::Stop),
    }
    Ok(Step::Continue)
}

/// Writes the packet a handle encoded, and answers it.
async fn send<
    Stream: Write,
    const MESSAGES: usize,
    const PACKET: usize,
    const SUBSCRIPTIONS: usize,
>(
    link: &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
    stream: &mut Stream,
    state: &mut State<MESSAGES, PACKET, SUBSCRIPTIONS>,
    buffer: OutboundLease<PACKET>,
) -> Result<(), MqttError> {
    if let Err(error) = write(stream, &buffer.bytes[..buffer.len]).await {
        match buffer.sent {
            Sent::Publish => link.answer(buffer, Err(error)),
            Sent::Subscribe {
                subscription,
                generation,
                ..
            } => link.subscribed[subscription].signal((generation, Err(error))),
        }
        return Err(error);
    }
    match buffer.sent {
        Sent::Publish => link.answer(buffer, Ok(())),
        Sent::Subscribe {
            packet,
            subscription,
            generation,
        } => {
            // The window holds as many identifiers as this list, so it has room.
            let _ = state.subscribing.push((
                packet,
                Held {
                    index: subscription,
                    generation,
                },
            ));
        }
    }
    Ok(())
}
