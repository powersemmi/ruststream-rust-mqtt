//! [`MqttState`], the memory a broker of the device variant owns, and the state its handles share
//! with its runner.
//!
//! Every buffer is a part of one value of known size that the board declares as a `static`: the
//! receive buffers deliveries borrow from, the send buffers publishes are encoded into, the
//! subscriptions with their queues, and the channels between the handles and the runner. Nothing
//! is allocated, at startup or per message.

use core::cell::{Cell, RefCell};
use core::fmt;
use core::task::{Context, Poll};

use embassy_sync::blocking_mutex::Mutex;
use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use embassy_sync::channel::Channel;
use embassy_sync::signal::Signal;
use embassy_sync::waitqueue::{MultiWakerRegistration, WakerRegistration};
use heapless::{Deque, Vec as FixedVec};
use ruststream::Headers;

use crate::fixed::error::MqttError;
use crate::fixed::lease::{Lease, Pool};
#[cfg(feature = "testing")]
use crate::fixed::testing::Harness;
use crate::qos::Qos;
use crate::topic::matches;

/// The lock every shared piece takes: a critical section, which a single-core device takes by
/// masking interrupts and a host test takes through its `std` implementation.
pub(crate) type Lock = CriticalSectionRawMutex;

/// How many publishes are encoded at once: one goes out while the next is written.
pub(crate) const OUTBOUND: usize = 2;

/// How many commands wait for the runner at once before a sender waits for room.
const COMMANDS: usize = 16;

/// How many `QoS` 1 and 2 publishes and subscribes wait for the server's answer at once.
pub(crate) const IN_FLIGHT: usize = 16;

/// How many subscription identifiers one delivery names.
pub(crate) const IDENTIFIERS: usize = 4;

/// One receive buffer: the bytes of a PUBLISH as they arrived, and what the runner read out of
/// them. A delivery holds it and borrows its topic and payload from it.
pub(crate) struct Inbound<const PACKET: usize> {
    pub(crate) bytes: [u8; PACKET],
    pub(crate) topic: (usize, usize),
    pub(crate) payload: (usize, usize),
    pub(crate) headers: Headers,
    pub(crate) qos: Qos,
    /// The packet identifier the acknowledgement names, zero for `QoS` 0.
    pub(crate) packet: u16,
    /// The session the delivery arrived in: an acknowledgement after a reconnect names a packet
    /// of a session that is gone.
    pub(crate) session: u32,
    /// The subscription identifiers the server named for it.
    pub(crate) identifiers: FixedVec<u32, IDENTIFIERS>,
    /// Whether the test harness counts the delivery in flight.
    #[cfg(feature = "testing")]
    pub(crate) counted: bool,
}

impl<const PACKET: usize> Inbound<PACKET> {
    pub(crate) const fn new() -> Self {
        Self {
            bytes: [0; PACKET],
            topic: (0, 0),
            payload: (0, 0),
            headers: Headers::new(),
            qos: Qos::AtMostOnce,
            packet: 0,
            session: 0,
            identifiers: FixedVec::new(),
            #[cfg(feature = "testing")]
            counted: false,
        }
    }

    pub(crate) fn topic(&self) -> &str {
        // The runner checked the bytes are a topic before it handed the buffer on.
        core::str::from_utf8(&self.bytes[self.topic.0..self.topic.1]).unwrap_or_default()
    }

    pub(crate) fn payload(&self) -> &[u8] {
        &self.bytes[self.payload.0..self.payload.1]
    }

    /// Copies `other` into this buffer, for a second subscription the same packet reaches.
    pub(crate) fn copy_from(&mut self, other: &Self) {
        let end = other.payload.1.max(other.topic.1);
        self.bytes[..end].copy_from_slice(&other.bytes[..end]);
        self.topic = other.topic;
        self.payload = other.payload;
        self.headers.clone_from(&other.headers);
        self.qos = other.qos;
        self.packet = other.packet;
        self.session = other.session;
        self.identifiers.clone_from(&other.identifiers);
        #[cfg(feature = "testing")]
        {
            self.counted = other.counted;
        }
    }
}

/// What a send buffer carries, which says what the runner does once it is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Sent {
    /// A PUBLISH: its publisher waits for the answer of the write.
    Publish,
    /// A SUBSCRIBE: its subscription waits for the server's SUBACK.
    Subscribe {
        packet: u16,
        subscription: usize,
        generation: u32,
    },
}

/// One send buffer: a whole packet, encoded by the handle that sends it.
pub(crate) struct Outbound<const PACKET: usize> {
    pub(crate) bytes: [u8; PACKET],
    pub(crate) len: usize,
    pub(crate) index: usize,
    pub(crate) sent: Sent,
}

impl<const PACKET: usize> Outbound<PACKET> {
    const fn new() -> Self {
        Self {
            bytes: [0; PACKET],
            len: 0,
            index: 0,
            sent: Sent::Publish,
        }
    }
}

/// A receive buffer out of the broker's pool.
pub(crate) type InboundLease<const MESSAGES: usize, const PACKET: usize> =
    Lease<Inbound<PACKET>, MESSAGES>;

/// A send buffer out of the broker's pool.
pub(crate) type OutboundLease<const PACKET: usize> = Lease<Outbound<PACKET>, OUTBOUND>;

/// What a handle asks the runner to do.
pub(crate) enum Command<const PACKET: usize> {
    /// Writes the packet in this buffer.
    Send(OutboundLease<PACKET>),
    /// Acknowledges the delivery with this packet identifier.
    Ack { packet: u16, qos: Qos, session: u32 },
    /// Ends the session: a DISCONNECT, then every handle is told the broker is closed.
    Shutdown,
}

/// How a connected broker reaches its subscriptions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    /// Through the runner and the server.
    Wire,
    /// Through the in-process transport of the test harness.
    #[cfg(feature = "testing")]
    InProcess,
}

/// Where the connection is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    /// Built, not connected: the runner waits for `connect`.
    Idle,
    /// `connect` asked the runner to connect, and it has not answered yet.
    Connecting,
    /// The session is open.
    Open(Mode),
    /// The session ended, by a shutdown or by the connection's loss.
    Closed(Option<MqttError>),
}

/// The answer a publisher waits for: the write of its packet. The send buffer comes back with the
/// answer, so the next publisher takes it only after this one has read its answer.
enum Reply<const PACKET: usize> {
    Idle,
    Waiting,
    /// The publisher stopped waiting: the runner frees the buffer once it is written.
    Abandoned,
    Done(Result<(), MqttError>, OutboundLease<PACKET>),
}

struct Answer<const PACKET: usize> {
    reply: Reply<PACKET>,
    waker: WakerRegistration,
}

/// The packet identifiers of the `QoS` 1 and 2 publishes and the subscribes the server has not
/// answered yet. The server's receive maximum bounds how many publishes are in flight.
struct Window {
    open: bool,
    next: u16,
    used: FixedVec<u16, IN_FLIGHT>,
    limit: usize,
    waiters: MultiWakerRegistration<4>,
}

impl Window {
    const fn new() -> Self {
        Self {
            open: false,
            next: 0,
            used: FixedVec::new(),
            limit: IN_FLIGHT,
            waiters: MultiWakerRegistration::new(),
        }
    }

    fn poll_take(&mut self, cx: &Context<'_>) -> Poll<Option<u16>> {
        if !self.open {
            return Poll::Ready(None);
        }
        if self.used.len() >= self.limit {
            self.waiters.register(cx.waker());
            return Poll::Pending;
        }
        loop {
            self.next = self.next.wrapping_add(1).max(1);
            if !self.used.contains(&self.next) {
                break;
            }
        }
        // The length was checked against a limit no larger than the capacity.
        let _ = self.used.push(self.next);
        Poll::Ready(Some(self.next))
    }

    fn release(&mut self, packet: u16) {
        if let Some(at) = self.used.iter().position(|used| *used == packet) {
            self.used.swap_remove(at);
            self.waiters.wake();
        }
    }

    fn reset(&mut self, open: bool, limit: usize) {
        self.open = open;
        self.used.clear();
        self.limit = limit.clamp(1, IN_FLIGHT);
        self.waiters.wake();
    }
}

/// The longest filter a subscription named at run time holds, in bytes. A filter that is a
/// literal of the firmware is held whole and has no limit.
pub(crate) const NAME_BYTES: usize = 64;

/// The filter a subscription reads: a literal of the firmware, held whole, or a filter named at
/// run time, copied into the broker's memory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Name {
    Static(&'static str),
    Built(heapless::String<NAME_BYTES>),
}

impl Name {
    /// A filter named at run time; `None` past [`NAME_BYTES`].
    pub(crate) fn built(name: &str) -> Option<Self> {
        heapless::String::try_from(name).ok().map(Self::Built)
    }

    pub(crate) fn as_str(&self) -> &str {
        match self {
            Self::Static(name) => name,
            Self::Built(name) => name,
        }
    }
}

/// What a subscription was opened with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Source {
    pub(crate) filter: Name,
    pub(crate) share: Option<&'static str>,
    pub(crate) qos: Qos,
    pub(crate) identifier: u32,
}

/// Where a subscription stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Standing {
    /// No subscriber holds the entry.
    Free,
    /// Its subscriber reads it.
    Open,
    /// The session ended: the stream ends once its queue is drained. The entry stays held until
    /// its subscriber is dropped.
    Ended(Option<MqttError>),
}

/// One subscription: the filter it was opened with and the deliveries waiting for its handler.
struct Subscription<const MESSAGES: usize, const PACKET: usize> {
    standing: Standing,
    /// Counts the subscribers the entry has had, so a late answer for an earlier one is told
    /// apart.
    generation: u32,
    filter: Name,
    share: Option<&'static str>,
    qos: Qos,
    /// The identifier its SUBSCRIBE carried; the subscriptions of one wire filter share it.
    identifier: u32,
    /// The order the subscriptions of one wire filter take their turns in.
    order: u32,
    /// Every receive buffer may wait here, so a push never finds it full.
    queue: Deque<InboundLease<MESSAGES, PACKET>, MESSAGES>,
    reader: WakerRegistration,
}

impl<const MESSAGES: usize, const PACKET: usize> Subscription<MESSAGES, PACKET> {
    const fn new() -> Self {
        Self {
            standing: Standing::Free,
            generation: 0,
            filter: Name::Static(""),
            share: None,
            qos: Qos::AtMostOnce,
            identifier: 0,
            order: 0,
            queue: Deque::new(),
            reader: WakerRegistration::new(),
        }
    }

    fn is_open(&self) -> bool {
        self.standing == Standing::Open
    }
}

/// A subscription's entry as its subscriber holds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Held {
    pub(crate) index: usize,
    pub(crate) generation: u32,
}

/// What joining the table gave a new subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Joined {
    pub(crate) held: Held,
    pub(crate) identifier: u32,
    /// Whether the subscription opened a wire filter the session did not hold yet, which the
    /// server is asked for.
    pub(crate) opened: bool,
}

/// The subscriptions of one broker.
pub(crate) struct Table<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> {
    subscriptions: [Subscription<MESSAGES, PACKET>; SUBSCRIPTIONS],
    next_identifier: u32,
    next_order: u32,
    /// The turn of each shared wire filter, by its identifier.
    turns: FixedVec<(u32, u32), SUBSCRIPTIONS>,
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize>
    Table<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    const fn new() -> Self {
        Self {
            subscriptions: [const { Subscription::new() }; SUBSCRIPTIONS],
            next_identifier: 0,
            next_order: 0,
            turns: FixedVec::new(),
        }
    }

    /// Opens a subscription on `filter`, in the share group `share`.
    pub(crate) fn join(
        &mut self,
        filter: Name,
        share: Option<&'static str>,
        qos: Qos,
    ) -> Result<Joined, MqttError> {
        let Some(index) = self
            .subscriptions
            .iter()
            .position(|entry| entry.standing == Standing::Free)
        else {
            return Err(MqttError::NoFreeSubscription {
                capacity: SUBSCRIPTIONS,
            });
        };
        let existing = self
            .subscriptions
            .iter()
            .find(|entry| {
                entry.is_open() && entry.filter.as_str() == filter.as_str() && entry.share == share
            })
            .map(|entry| entry.identifier);
        let (identifier, opened) = if let Some(identifier) = existing {
            (identifier, false)
        } else {
            // Identifiers start at one: zero is a protocol error.
            self.next_identifier += 1;
            (self.next_identifier, true)
        };
        self.next_order = self.next_order.wrapping_add(1);
        let entry = &mut self.subscriptions[index];
        entry.standing = Standing::Open;
        entry.generation = entry.generation.wrapping_add(1);
        entry.filter = filter;
        entry.share = share;
        entry.qos = qos;
        entry.identifier = identifier;
        entry.order = self.next_order;
        Ok(Joined {
            held: Held {
                index,
                generation: entry.generation,
            },
            identifier,
            opened,
        })
    }

    /// Frees the entry `held`, with every delivery still waiting in it: their buffers go back to
    /// the pool, and the server redelivers what was not acknowledged when a persistent session
    /// resumes.
    pub(crate) fn leave(&mut self, held: Held) {
        let entry = &mut self.subscriptions[held.index];
        if entry.generation != held.generation || entry.standing == Standing::Free {
            return;
        }
        entry.standing = Standing::Free;
        entry.queue.clear();
        let identifier = entry.identifier;
        if !self
            .subscriptions
            .iter()
            .any(|entry| entry.is_open() && entry.identifier == identifier)
        {
            self.turns.retain(|(turn, _)| *turn != identifier);
        }
    }

    /// What the open entry `held` was opened with.
    pub(crate) fn source(&self, held: Held) -> Option<Source> {
        let entry = &self.subscriptions[held.index];
        (entry.generation == held.generation && entry.is_open()).then(|| Source {
            filter: entry.filter.clone(),
            share: entry.share,
            qos: entry.qos,
            identifier: entry.identifier,
        })
    }

    /// The subscriptions a delivery reaches, one per wire filter: the server names the
    /// subscriptions it sent it for by their identifiers, and a delivery that names none reaches
    /// every filter its topic matches. The subscriptions of one wire filter take it in turn.
    pub(crate) fn reach(
        &mut self,
        identifiers: &[u32],
        topic: &str,
        mut visit: impl FnMut(usize, Qos),
    ) {
        let mut seen = FixedVec::<u32, SUBSCRIPTIONS>::new();
        for at in 0..SUBSCRIPTIONS {
            let entry = &self.subscriptions[at];
            if !entry.is_open() || seen.contains(&entry.identifier) {
                continue;
            }
            let reached = if identifiers.is_empty() {
                matches(topic, entry.filter.as_str())
            } else {
                identifiers.contains(&entry.identifier)
            };
            if !reached {
                continue;
            }
            let identifier = entry.identifier;
            // At most one identifier per open entry, so it fits.
            let _ = seen.push(identifier);
            if let Some(member) = self.take_turn(identifier) {
                visit(member, self.subscriptions[member].qos);
            }
        }
    }

    /// The member of the wire filter `identifier` whose turn it is.
    fn take_turn(&mut self, identifier: u32) -> Option<usize> {
        let mut members = FixedVec::<(u32, usize), SUBSCRIPTIONS>::new();
        for (at, entry) in self.subscriptions.iter().enumerate() {
            if entry.is_open() && entry.identifier == identifier {
                let _ = members.push((entry.order, at));
            }
        }
        if members.is_empty() {
            return None;
        }
        members.sort_unstable();
        if !self.turns.iter().any(|(held, _)| *held == identifier) {
            // One turn per open wire filter, and the table holds at most that many filters.
            let _ = self.turns.push((identifier, 0));
        }
        let (_, turn) = self
            .turns
            .iter_mut()
            .find(|(held, _)| *held == identifier)?;
        let at = (*turn as usize) % members.len();
        *turn = turn.wrapping_add(1);
        Some(members[at].1)
    }

    /// The member of the wire filter `filter` that takes the next delivery, by its position among
    /// the members.
    #[cfg(feature = "testing")]
    pub(crate) fn next_taker(&self, filter: &str) -> Option<usize> {
        let entry = self
            .subscriptions
            .iter()
            .find(|entry| entry.is_open() && entry.filter.as_str() == filter)?;
        let members = self
            .subscriptions
            .iter()
            .filter(|other| other.is_open() && other.identifier == entry.identifier)
            .count();
        let turn = self
            .turns
            .iter()
            .find(|(held, _)| *held == entry.identifier)
            .map_or(0, |(_, turn)| *turn);
        Some((turn as usize) % members)
    }

    /// Hands `delivery` to the subscription at `index`.
    pub(crate) fn push(&mut self, index: usize, delivery: InboundLease<MESSAGES, PACKET>) {
        let entry = &mut self.subscriptions[index];
        // Every receive buffer fits in one queue.
        let _ = entry.queue.push_back(delivery);
        entry.reader.wake();
    }

    /// The next delivery of the entry `held`, or its end.
    #[allow(clippy::type_complexity)] // The stream's own item type.
    pub(crate) fn poll_next(
        &mut self,
        held: Held,
        cx: &Context<'_>,
    ) -> Poll<Option<Result<InboundLease<MESSAGES, PACKET>, MqttError>>> {
        let entry = &mut self.subscriptions[held.index];
        if entry.generation != held.generation {
            return Poll::Ready(None);
        }
        if let Some(delivery) = entry.queue.pop_front() {
            return Poll::Ready(Some(Ok(delivery)));
        }
        match &mut entry.standing {
            Standing::Open => {
                entry.reader.register(cx.waker());
                Poll::Pending
            }
            Standing::Ended(error) => Poll::Ready(error.take().map(Err)),
            Standing::Free => Poll::Ready(None),
        }
    }

    /// The next delivery of the entry `held` that waits already.
    pub(crate) fn try_next(&mut self, held: Held) -> Option<InboundLease<MESSAGES, PACKET>> {
        let entry = &mut self.subscriptions[held.index];
        if entry.generation != held.generation {
            return None;
        }
        entry.queue.pop_front()
    }

    /// Whether the next poll of the entry `held` could yield.
    pub(crate) fn may_be_ready(&self, held: Held) -> bool {
        let entry = &self.subscriptions[held.index];
        entry.generation != held.generation || !entry.is_open() || !entry.queue.is_empty()
    }

    /// Ends every open subscription: its stream yields what waits and then ends, with `error`
    /// where the session was lost.
    pub(crate) fn end_all(&mut self, error: Option<MqttError>) {
        for entry in &mut self.subscriptions {
            if entry.is_open() {
                entry.standing = Standing::Ended(error);
                entry.reader.wake();
            }
        }
        self.turns.clear();
    }
}

/// The server's answer to a subscription's SUBSCRIBE, with the generation of the subscription it
/// answers.
pub(crate) type Subscribed = (u32, Result<(), MqttError>);

/// The state of one broker, shared by its handles and its runner.
pub(crate) struct Link<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> {
    phase: Mutex<Lock, Cell<Phase>>,
    session: Mutex<Lock, Cell<u32>>,
    /// The largest packet the server takes, which a publish is checked against.
    server_maximum: Mutex<Lock, Cell<usize>>,
    /// Raised by `connect`; the runner waits on it between sessions.
    pub(crate) connect_requested: Signal<Lock, ()>,
    /// The runner's answer to a `connect`.
    pub(crate) connected: Signal<Lock, Result<(), MqttError>>,
    /// The runner's answer to a `shutdown`.
    pub(crate) closed: Signal<Lock, ()>,
    pub(crate) commands: Channel<Lock, Command<PACKET>, COMMANDS>,
    pub(crate) inbound: Pool<Inbound<PACKET>, MESSAGES>,
    pub(crate) outbound: Pool<Outbound<PACKET>, OUTBOUND>,
    answers: Mutex<Lock, RefCell<[Answer<PACKET>; OUTBOUND]>>,
    /// The server's answer to each subscription's SUBSCRIBE, with the generation it was for.
    pub(crate) subscribed: [Signal<Lock, Subscribed>; SUBSCRIPTIONS],
    pub(crate) table: Mutex<Lock, RefCell<Table<MESSAGES, PACKET, SUBSCRIPTIONS>>>,
    window: Mutex<Lock, RefCell<Window>>,
    #[cfg(feature = "testing")]
    pub(crate) harness: Harness,
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize>
    Link<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    const fn new() -> Self {
        Self {
            phase: Mutex::new(Cell::new(Phase::Idle)),
            session: Mutex::new(Cell::new(0)),
            server_maximum: Mutex::new(Cell::new(PACKET)),
            connect_requested: Signal::new(),
            connected: Signal::new(),
            closed: Signal::new(),
            commands: Channel::new(),
            inbound: Pool::new(),
            outbound: Pool::new(),
            answers: Mutex::new(RefCell::new(
                [const {
                    Answer {
                        reply: Reply::Idle,
                        waker: WakerRegistration::new(),
                    }
                }; OUTBOUND],
            )),
            subscribed: [const { Signal::new() }; SUBSCRIPTIONS],
            table: Mutex::new(RefCell::new(Table::new())),
            window: Mutex::new(RefCell::new(Window::new())),
            #[cfg(feature = "testing")]
            harness: Harness::new(),
        }
    }

    pub(crate) fn phase(&self) -> Phase {
        self.phase.lock(Cell::get)
    }

    pub(crate) fn set_phase(&self, next: Phase) {
        self.phase.lock(|phase| phase.set(next));
    }

    /// The transport of the open session, or `None` while it is not open.
    pub(crate) fn mode(&self) -> Option<Mode> {
        match self.phase() {
            Phase::Open(mode) => Some(mode),
            _ => None,
        }
    }

    pub(crate) fn is_open(&self) -> bool {
        self.mode().is_some()
    }

    pub(crate) fn session(&self) -> u32 {
        self.session.lock(Cell::get)
    }

    /// Starts a session: its number tells its deliveries apart from an earlier one's, and the
    /// window opens with the server's receive maximum.
    pub(crate) fn begin_session(&self, mode: Mode, receive_maximum: usize, server_maximum: usize) {
        self.session
            .lock(|session| session.set(session.get().wrapping_add(1)));
        self.server_maximum
            .lock(|held| held.set(server_maximum.min(PACKET)));
        self.window
            .lock(|window| window.borrow_mut().reset(true, receive_maximum));
        self.set_phase(Phase::Open(mode));
    }

    pub(crate) fn server_maximum(&self) -> usize {
        self.server_maximum.lock(Cell::get)
    }

    /// Waits for a packet identifier, or `None` once the session is over.
    pub(crate) async fn take_packet(&self) -> Option<u16> {
        core::future::poll_fn(|cx| self.window.lock(|window| window.borrow_mut().poll_take(cx)))
            .await
    }

    pub(crate) fn release_packet(&self, packet: u16) {
        self.window
            .lock(|window| window.borrow_mut().release(packet));
    }

    /// Readies the answer of the send buffer `index` for the publisher that sends it.
    pub(crate) fn expect_answer(&self, index: usize) {
        self.answers.lock(|answers| {
            answers.borrow_mut()[index].reply = Reply::Waiting;
        });
    }

    /// Answers the publisher of `buffer`; the buffer goes back to the pool once it has read the
    /// answer, or at once if it stopped waiting.
    pub(crate) fn answer(&self, buffer: OutboundLease<PACKET>, result: Result<(), MqttError>) {
        let index = buffer.index;
        let abandoned = self.answers.lock(|answers| {
            let mut answers = answers.borrow_mut();
            let answer = &mut answers[index];
            if matches!(answer.reply, Reply::Abandoned) {
                answer.reply = Reply::Idle;
                return Some(buffer);
            }
            answer.reply = Reply::Done(result, buffer);
            answer.waker.wake();
            None
        });
        drop(abandoned);
    }

    /// The answer of the send buffer `index`, once the runner wrote it.
    pub(crate) fn poll_answer(
        &self,
        index: usize,
        cx: &Context<'_>,
    ) -> Poll<Result<(), MqttError>> {
        let done = self.answers.lock(|answers| {
            let mut answers = answers.borrow_mut();
            let answer = &mut answers[index];
            match core::mem::replace(&mut answer.reply, Reply::Idle) {
                Reply::Done(result, buffer) => Some((result, buffer)),
                other => {
                    answer.reply = other;
                    answer.waker.register(cx.waker());
                    None
                }
            }
        });
        match done {
            // The buffer goes back to the pool outside the lock.
            Some((result, buffer)) => {
                drop(buffer);
                Poll::Ready(result)
            }
            None => Poll::Pending,
        }
    }

    /// The publisher of the send buffer `index` stopped waiting.
    pub(crate) fn abandon_answer(&self, index: usize) {
        let buffer = self.answers.lock(|answers| {
            let mut answers = answers.borrow_mut();
            let answer = &mut answers[index];
            match core::mem::replace(&mut answer.reply, Reply::Idle) {
                Reply::Done(_, buffer) => Some(buffer),
                Reply::Waiting => {
                    answer.reply = Reply::Abandoned;
                    None
                }
                other => {
                    answer.reply = other;
                    None
                }
            }
        });
        drop(buffer);
    }

    /// Ends the session's state: every subscription ends, the window closes and every waiting
    /// handle is answered.
    pub(crate) fn end_session(&self, error: Option<MqttError>) {
        // The phase is set first: a handle that sends a command after the drain below reads it
        // and stops waiting.
        self.set_phase(Phase::Closed(error));
        self.window
            .lock(|window| window.borrow_mut().reset(false, IN_FLIGHT));
        self.table.lock(|table| table.borrow_mut().end_all(error));
        while let Ok(command) = self.commands.try_receive() {
            if let Command::Send(buffer) = command {
                self.refuse(buffer);
            }
        }
        for answer in &self.subscribed {
            if !answer.signaled() {
                answer.signal((u32::MAX, Err(MqttError::NotConnected)));
            }
        }
    }

    /// Answers the handle that sent `buffer` that the session is over.
    pub(crate) fn refuse(&self, buffer: OutboundLease<PACKET>) {
        match buffer.sent {
            Sent::Publish => self.answer(buffer, Err(MqttError::NotConnected)),
            Sent::Subscribe {
                subscription,
                generation,
                ..
            } => {
                self.subscribed[subscription].signal((generation, Err(MqttError::NotConnected)));
            }
        }
    }
}

/// The memory of one broker of the device variant: the receive and send buffers, the
/// subscriptions and the channels between the broker's handles and its runner.
///
/// The board declares it once, as a `static` that is taken mutably once, and hands it to
/// [`MqttBroker::new`](crate::MqttBroker::new). Its size is fixed by three numbers:
///
/// - `MESSAGES`: the receive buffers. Each delivery holds one from its arrival until the handler
///   settles it, so this is how many deliveries wait or are in hand at once. The session asks the
///   server to keep at most one fewer `QoS` 1 and 2 deliveries in flight, so a buffer stays free
///   for the server's answers; the runner stops reading the connection while every buffer is
///   held. At least two.
/// - `PACKET`: the bytes of each receive and send buffer, the largest packet in either direction.
///   The session announces it to the server as its maximum packet size, so the server never
///   sends a larger one, and a larger publish is refused before it is sent.
/// - `SUBSCRIPTIONS`: how many subscriptions are open at once.
///
/// The memory it takes is about `(MESSAGES + 2) * PACKET` bytes plus a small part per
/// subscription and per message.
///
/// # Examples
///
/// ```
/// use ruststream_rust_mqtt::{MqttBroker, MqttConfig, MqttRunner, MqttState};
/// use static_cell::ConstStaticCell;
///
/// /// Eight deliveries of up to 512 bytes, and four subscriptions.
/// static MQTT: ConstStaticCell<MqttState<8, 512, 4>> = ConstStaticCell::new(MqttState::new());
///
/// /// What `main` does before it spawns the runner's task and the app's.
/// fn board() -> (MqttBroker<8, 512, 4>, MqttRunner<8, 512, 4>) {
///     MqttBroker::new(MQTT.take(), MqttConfig::new("192.168.1.10:1883").client_id("sensor-7"))
/// }
/// # let _ = board;
/// ```
pub struct MqttState<
    const MESSAGES: usize = 4,
    const PACKET: usize = 1024,
    const SUBSCRIPTIONS: usize = 8,
> {
    link: Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
    inbound: [Inbound<PACKET>; MESSAGES],
    outbound: [Outbound<PACKET>; OUTBOUND],
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize>
    MqttState<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    /// The memory of a broker, in a form a `static` holds.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_rust_mqtt::MqttState;
    /// use static_cell::ConstStaticCell;
    ///
    /// /// Four deliveries of up to 1 KiB, and eight subscriptions.
    /// static MQTT: ConstStaticCell<MqttState> = ConstStaticCell::new(MqttState::new());
    /// # let _ = &MQTT;
    /// ```
    #[must_use]
    pub const fn new() -> Self {
        const {
            assert!(
                MESSAGES >= 2,
                "an MqttState holds at least two receive buffers: one stays free for the packets \
                 the session reads while the others wait for their handlers"
            );
            assert!(
                PACKET >= 64,
                "an MqttState's packets hold at least 64 bytes"
            );
            assert!(
                SUBSCRIPTIONS >= 1,
                "an MqttState holds at least one subscription"
            );
        }
        Self {
            link: Link::new(),
            inbound: [const { Inbound::new() }; MESSAGES],
            outbound: [const { Outbound::new() }; OUTBOUND],
        }
    }

    /// Hands every buffer to its pool and returns the state the handles share.
    pub(crate) fn split(&'static mut self) -> &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS> {
        let Self {
            link,
            inbound,
            outbound,
        } = self;
        for buffer in inbound.iter_mut() {
            link.inbound.put(buffer);
        }
        for (index, buffer) in outbound.iter_mut().enumerate() {
            buffer.index = index;
            link.outbound.put(buffer);
        }
        link
    }
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> Default
    for MqttState<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> fmt::Debug
    for MqttState<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttState")
            .field("messages", &MESSAGES)
            .field("packet", &PACKET)
            .field("subscriptions", &SUBSCRIPTIONS)
            .finish_non_exhaustive()
    }
}
