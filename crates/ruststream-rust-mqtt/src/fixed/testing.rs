//! The device variant's in-process mode, behind the `testing` feature: the transport the test
//! harness connects the broker through instead of the network.
//!
//! The connected broker, its publishers, its subscribers and its deliveries are the production
//! types over the production state, so a service's descriptors and publish policies run against
//! it unchanged. A publish is encoded into a receive buffer and read back the way the runner reads
//! a PUBLISH from the server, so a delivery holds a buffer of the broker's memory and a publish
//! too large for it is refused.
//!
//! The transport is a model of the server. A publish reaches every subscription whose filter
//! matches its topic, at the lesser of the two qualities of service; a share group takes one
//! copy, which its members take in turn. A subscription that is not shared receives the retained
//! messages its filter matches when it opens.
//!
//! The bookkeeping (the publish log, the retained messages and the coordinator that counts
//! deliveries in flight) is the harness's own and lives on the heap of the test binary, inside a
//! section the allocation check of a device test lets through.

use alloc::string::String;
use alloc::vec::Vec;
use core::cell::RefCell;
use core::future::{Future, ready};

use assert_no_alloc::permit_alloc;
use embassy_sync::blocking_mutex::Mutex;
use ruststream::testing::{Coordinator, InProcess, TestableBroker};
use ruststream::{Headers, OutgoingMessage, RawMessage};
use ruststream_core::__alloc::bytes::Bytes;
use zzq_proto::fixed_header;

use crate::fixed::broker::{ConnectedMqttBroker, MqttBroker};
use crate::fixed::error::MqttError;
use crate::fixed::source::validate_topic;
use crate::fixed::state::{IN_FLIGHT, Joined, Link, Lock, Mode};
use crate::fixed::wire::{self, Outgoing};
use crate::qos::Qos;
use crate::topic::matches;

/// A retained message, as the model of the server keeps it.
struct Retained {
    topic: String,
    payload: Vec<u8>,
    headers: Headers,
    qos: Qos,
}

/// What the harness keeps of one broker.
#[derive(Default)]
struct Book {
    log: Vec<RawMessage>,
    retained: Vec<Retained>,
    coordinator: Option<Coordinator>,
}

/// What the test harness reads off a broker: every accepted publish, the retained messages, and
/// the coordinator that counts the deliveries in flight.
pub(crate) struct Harness {
    book: Mutex<Lock, RefCell<Option<Book>>>,
}

impl Harness {
    pub(crate) const fn new() -> Self {
        Self {
            book: Mutex::new(RefCell::new(None)),
        }
    }

    /// Starts a run: the state is a `static`, so a test that connects it again must not read the
    /// log of the run before.
    fn reset(&self) {
        permit_alloc(|| {
            self.book
                .lock(|book| *book.borrow_mut() = Some(Book::default()))
        });
    }

    fn with<Answer>(&self, read: impl FnOnce(&mut Book) -> Answer) -> Answer {
        permit_alloc(|| {
            self.book
                .lock(|book| read(book.borrow_mut().get_or_insert_with(Book::default)))
        })
    }

    fn coordinator(&self) -> Option<Coordinator> {
        self.with(|book| book.coordinator.clone())
    }

    pub(crate) fn consumed(&self) {
        if let Some(coordinator) = self.coordinator() {
            permit_alloc(|| coordinator.consumed());
        }
    }

    /// A publish of the broker's own publisher.
    pub(crate) fn publish<
        const MESSAGES: usize,
        const PACKET: usize,
        const SUBSCRIPTIONS: usize,
    >(
        &self,
        link: &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
        msg: &OutgoingMessage<'_, &[u8]>,
        qos: Qos,
        retain: bool,
    ) -> Result<(), MqttError> {
        // The server refuses what a live publish could not send, before it records anything.
        let mut scratch = link.inbound.try_take().ok_or(full())?;
        wire::publish(
            &mut scratch.bytes,
            PACKET,
            &Outgoing {
                topic: msg.name(),
                payload: msg.payload(),
                headers: msg.headers(),
                qos,
                retain,
                packet: 1,
            },
        )?;
        drop(scratch);
        self.receive(link, msg.name(), msg.payload(), msg.headers(), qos, retain);
        Ok(())
    }

    fn receive<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize>(
        &self,
        link: &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
        topic: &str,
        payload: &[u8],
        headers: &Headers,
        qos: Qos,
        retain: bool,
    ) {
        self.with(|book| {
            book.log.push(
                RawMessage::new(String::from(topic), Bytes::copy_from_slice(payload))
                    .with_headers(headers.clone()),
            );
            if retain {
                book.retained.retain(|held| held.topic != topic);
                // An empty retained payload is how a publisher clears the topic.
                if !payload.is_empty() {
                    book.retained.push(Retained {
                        topic: String::from(topic),
                        payload: payload.to_vec(),
                        headers: headers.clone(),
                        qos,
                    });
                }
            }
        });
        let mut targets = heapless::Vec::<(usize, Qos), SUBSCRIPTIONS>::new();
        link.table.lock(|table| {
            table.borrow_mut().reach(&[], topic, |member, wanted| {
                let _ = targets.push((member, wanted.min(qos)));
            });
        });
        for (member, qos) in targets {
            self.hand_over(link, member, topic, payload, headers, qos);
        }
    }

    /// Encodes the PUBLISH the server would send to the subscription at `member` into a receive
    /// buffer and reads it back, as the runner does.
    fn hand_over<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize>(
        &self,
        link: &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
        member: usize,
        topic: &str,
        payload: &[u8],
        headers: &Headers,
        qos: Qos,
    ) {
        // A server stops sending to a client whose window is full; the test outran the capacity
        // its handlers drain, which is a test to fix rather than a message to lose.
        let Some(mut buffer) = link.inbound.try_take() else {
            panic!(
                "the in-process MQTT subscription to {topic:?} found every receive buffer held: {}",
                full()
            );
        };
        let packet = u16::from(qos != Qos::AtMostOnce);
        let encoded = wire::publish(
            &mut buffer.bytes,
            PACKET,
            &Outgoing {
                topic,
                payload,
                headers,
                qos,
                retain: false,
                packet,
            },
        );
        let Ok(len) = encoded else {
            panic!("the in-process MQTT server re-encodes the publish to {topic:?} it accepted");
        };
        let header_len = match fixed_header(&buffer.bytes[..len]) {
            Ok(Some(header)) => header.header_len,
            _ => unreachable!("the crate's own encoder writes a fixed header"),
        };
        let session = link.session();
        if wire::read_publish(&mut buffer, header_len, len, session).is_err() {
            unreachable!("the crate's own encoder writes a PUBLISH its reader reads");
        }
        if let Some(coordinator) = self.coordinator() {
            permit_alloc(|| coordinator.enqueued());
            buffer.counted = true;
        } else {
            buffer.counted = false;
        }
        link.table
            .lock(|table| table.borrow_mut().push(member, buffer));
    }

    /// A subscription opened: one that is not shared and opened its wire filter receives the
    /// retained messages its filter matches.
    pub(crate) fn opened<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize>(
        &self,
        link: &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
        joined: Joined,
        filter: &str,
        share: Option<&str>,
    ) {
        if !joined.opened || share.is_some() {
            return;
        }
        let wanted = link
            .table
            .lock(|table| table.borrow().source(joined.held))
            .map_or(Qos::AtMostOnce, |source| source.qos);
        let retained: Vec<(String, Vec<u8>, Headers, Qos)> = self.with(|book| {
            book.retained
                .iter()
                .filter(|held| matches(&held.topic, filter))
                .map(|held| {
                    (
                        held.topic.clone(),
                        held.payload.clone(),
                        held.headers.clone(),
                        held.qos.min(wanted),
                    )
                })
                .collect()
        });
        for (topic, payload, headers, qos) in &retained {
            self.hand_over(link, joined.held.index, topic, payload, headers, *qos);
        }
        permit_alloc(|| drop(retained));
    }
}

/// The refusal of a publish while every receive buffer is held.
const fn full() -> MqttError {
    MqttError::Publish(
        "the in-process MQTT server found every receive buffer held: settle the deliveries \
         before publishing more",
    )
}

/// Counts a delivery out of the harness's coordinator when it is settled or dropped.
pub(crate) struct Counted<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> {
    link: &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize>
    Counted<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    pub(crate) const fn new(link: &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS>) -> Self {
        Self { link }
    }
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> Drop
    for Counted<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    fn drop(&mut self) {
        self.link.harness.consumed();
    }
}

// The options are checked as `connect` checks them, so a configuration a service could not
// connect with is not one a test connects with either.
impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> InProcess
    for MqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    fn connect_in_process(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        let link = self.link;
        let connected = self.config.endpoint().map(|_| {
            link.harness.reset();
            link.begin_session(Mode::InProcess, IN_FLIGHT, PACKET);
            ConnectedMqttBroker { link }
        });
        ready(connected)
    }
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> TestableBroker
    for ConnectedMqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    fn install_coordinator(&self, coordinator: Coordinator) {
        self.link.harness.with(|book| {
            if book.coordinator.is_none() {
                book.coordinator = Some(coordinator);
            }
        });
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        let link = self.in_process("inject");
        let accepted = validate_topic(message.name())
            .and_then(|()| wire::check_headers(message.headers()))
            .and_then(|()| link.harness.publish(link, &message, Qos::default(), false));
        if let Err(error) = accepted {
            panic!(
                "the injected message to {:?} is not one a server takes: {error}",
                message.name()
            );
        }
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.in_process("published").harness.with(|book| {
            book.log
                .iter()
                .filter(|message| message.name() == name)
                .cloned()
                .collect()
        })
    }

    fn routes(&self, destination: &str, subscriptions: &[&str]) -> Vec<usize> {
        let link = self.in_process("routes");
        permit_alloc(|| {
            let mut routed = Vec::new();
            let mut answered: Vec<&str> = Vec::new();
            for filter in subscriptions {
                if !matches(destination, filter) || answered.contains(filter) {
                    continue;
                }
                answered.push(filter);
                // The subscriptions the app opened under this name, in the order it opened them.
                let positions: Vec<usize> = subscriptions
                    .iter()
                    .enumerate()
                    .filter(|(_, name)| *name == filter)
                    .map(|(position, _)| position)
                    .collect();
                let turn = link.table.lock(|table| table.borrow().next_taker(filter));
                match turn {
                    Some(turn) => routed.extend(positions.get(turn)),
                    // A filter this connection has not opened answers by its name alone.
                    None => routed.extend(positions),
                }
            }
            routed
        })
    }
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize>
    ConnectedMqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    fn in_process(&self, operation: &str) -> &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS> {
        assert!(
            self.link.mode() != Some(Mode::Wire),
            "`{operation}` drives the in-process transport, and this broker was connected with \
             `connect`: the harness connects it with `connect_in_process`"
        );
        self.link
    }
}
