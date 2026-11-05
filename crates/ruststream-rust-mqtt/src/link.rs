//! The state a broker's handles share with its runner: the phase of the connection, the
//! commands the handles send the runner, the publishes staged for it, and the subscriptions it
//! routes deliveries to.
//!
//! Every piece is fixed at startup. A publish, an acknowledgement and a delivery move through
//! storage that already exists, so none of them allocates.

use alloc::rc::Rc;
use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::cell::RefCell;

use embassy_sync::blocking_mutex::Mutex;
use embassy_sync::blocking_mutex::raw::{CriticalSectionRawMutex, NoopRawMutex};
use embassy_sync::channel::Channel;
use embassy_sync::signal::Signal;
use rust_mqtt::types::PacketIdentifier;
use ruststream::HeaderMap;
use ruststream_core::IfSend;

use crate::config::MqttConfig;
use crate::error::MqttError;
use crate::qos::Qos;
use crate::routes::Routes;

/// The lock every shared piece takes: a critical section next to a multi-thread runtime, nothing
/// on a single-threaded one.
pub(crate) type RawLock = IfSend<CriticalSectionRawMutex, NoopRawMutex>;

/// A pointer several handles of one broker share: `Arc` next to a multi-thread runtime, `Rc`
/// without one.
pub(crate) type Shared<Value> = IfSend<Arc<Value>, Rc<Value>>;

/// How many commands wait for the runner at once before a sender waits for room.
const COMMANDS: usize = 32;

/// How many publishes are staged for the runner at once.
pub(crate) const STAGED: usize = 8;

/// What a handle asks the runner to do.
pub(crate) enum Command {
    /// Sends the publish staged in this slot.
    Publish(usize),
    /// Settles the delivery with this packet identifier.
    Ack { packet: PacketIdentifier, qos: Qos },
    /// Opens a subscription.
    Subscribe(Shared<SubscribeRequest>),
    /// Ends the session: a DISCONNECT, then every handle is told the broker is closed.
    Shutdown,
}

/// Where the connection is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Phase {
    /// Built, not connected: the runner waits for `connect`.
    Idle,
    /// `connect` asked the runner to connect, and it has not answered yet.
    Connecting,
    /// The session is open.
    Open,
    /// The session ended, by a shutdown or by the connection's loss.
    Closed(Option<MqttError>),
}

/// One publish waiting for the runner: the bytes are copied in, so the publisher lends them.
///
/// The buffers keep their capacity from one publish to the next, so a slot allocates only until
/// it has held the largest message the service sends.
#[derive(Default)]
pub(crate) struct Staged {
    pub(crate) topic: String,
    pub(crate) payload: Vec<u8>,
    pub(crate) headers: HeaderMap,
    pub(crate) qos: Qos,
    pub(crate) retain: bool,
}

/// One staging slot and the answer the runner gives for it.
pub(crate) struct Slot {
    pub(crate) staged: Mutex<RawLock, RefCell<Staged>>,
    pub(crate) done: Signal<RawLock, Result<(), MqttError>>,
}

/// A subscription the runner opens on the server.
pub(crate) struct SubscribeRequest {
    /// The filter as sent on the wire, `$share/<group>/<filter>` for a share group.
    pub(crate) wire_filter: String,
    pub(crate) qos: Qos,
    /// The subscription identifier the server names on every packet it sends for it.
    pub(crate) identifier: u32,
    pub(crate) done: Signal<RawLock, Result<(), MqttError>>,
}

/// The state of one broker, shared by its handles and its runner.
pub(crate) struct Link {
    pub(crate) config: MqttConfig,
    phase: Mutex<RawLock, RefCell<Phase>>,
    /// Raised by `connect`; the runner waits on it between sessions.
    pub(crate) connect_requested: Signal<RawLock, ()>,
    /// The runner's answer to a `connect`.
    pub(crate) connected: Signal<RawLock, Result<(), MqttError>>,
    /// The runner's answer to a `shutdown`.
    pub(crate) closed: Signal<RawLock, ()>,
    pub(crate) commands: Channel<RawLock, Command, COMMANDS>,
    pub(crate) slots: [Slot; STAGED],
    /// The indexes of the slots no publish holds.
    pub(crate) free: Channel<RawLock, usize, STAGED>,
    pub(crate) routes: Mutex<RawLock, RefCell<Routes>>,
}

impl Link {
    pub(crate) fn new(config: MqttConfig) -> Self {
        let link = Self {
            config,
            phase: Mutex::new(RefCell::new(Phase::Idle)),
            connect_requested: Signal::new(),
            connected: Signal::new(),
            closed: Signal::new(),
            commands: Channel::new(),
            slots: core::array::from_fn(|_| Slot {
                staged: Mutex::new(RefCell::new(Staged::default())),
                done: Signal::new(),
            }),
            free: Channel::new(),
            routes: Mutex::new(RefCell::new(Routes::default())),
        };
        for index in 0..STAGED {
            // The channel holds exactly the slots, so every index fits.
            let _ = link.free.try_send(index);
        }
        link
    }

    pub(crate) fn phase(&self) -> Phase {
        self.phase.lock(|phase| phase.borrow().clone())
    }

    pub(crate) fn set_phase(&self, next: Phase) {
        self.phase.lock(|phase| *phase.borrow_mut() = next);
    }

    /// Whether handles may still send commands.
    pub(crate) fn is_open(&self) -> bool {
        self.phase
            .lock(|phase| matches!(*phase.borrow(), Phase::Open))
    }

    /// Closes every queue and answers every waiting handle: the session is over.
    pub(crate) fn close_all(&self, error: Option<&MqttError>) {
        let queues = self
            .routes
            .lock(|routes| routes.borrow_mut().drain_queues());
        for queue in queues {
            queue.close(error.cloned());
        }
        for slot in &self.slots {
            slot.done.signal(Err(MqttError::NotConnected));
        }
    }
}
