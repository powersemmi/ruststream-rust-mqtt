//! The queue of one subscription: the runner pushes the deliveries routed to it, and the
//! subscription's stream takes them.

use alloc::collections::VecDeque;
use core::cell::RefCell;
use core::task::{Context, Poll};

use embassy_sync::blocking_mutex::Mutex;
use embassy_sync::waitqueue::WakerRegistration;

use crate::error::MqttError;
use crate::link::RawLock;
use crate::message::MqttMessage;

/// The deliveries of one subscription, waiting for its stream.
pub(crate) struct Queue {
    state: Mutex<RawLock, RefCell<State>>,
}

struct State {
    items: VecDeque<MqttMessage>,
    capacity: usize,
    reader: WakerRegistration,
    writer: WakerRegistration,
    /// Where the queue is: open, or closed with the error its stream still has to report.
    closed: Closed,
}

/// Whether a queue still takes deliveries.
enum Closed {
    Open,
    /// The session ended: the stream drains what waits, reports the error once if there is
    /// one, and ends.
    Ended(Option<MqttError>),
}

impl Closed {
    const fn is_open(&self) -> bool {
        matches!(self, Self::Open)
    }
}

impl Queue {
    /// A queue that holds `capacity` deliveries. The storage is taken here, once.
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(RefCell::new(State {
                items: VecDeque::with_capacity(capacity),
                capacity,
                reader: WakerRegistration::new(),
                writer: WakerRegistration::new(),
                closed: Closed::Open,
            })),
        }
    }

    /// Adds a delivery, or hands it back when the queue is full. A closed queue drops it.
    pub(crate) fn push(&self, message: MqttMessage) -> Result<(), MqttMessage> {
        self.state.lock(|state| {
            let mut state = state.borrow_mut();
            if !state.closed.is_open() {
                return Ok(());
            }
            if state.items.len() == state.capacity {
                return Err(message);
            }
            state.items.push_back(message);
            state.reader.wake();
            Ok(())
        })
    }

    /// Ready once the queue has room for one more delivery, or once it closed.
    pub(crate) fn poll_room(&self, cx: &Context<'_>) -> Poll<()> {
        self.state.lock(|state| {
            let mut state = state.borrow_mut();
            if !state.closed.is_open() || state.items.len() < state.capacity {
                Poll::Ready(())
            } else {
                state.writer.register(cx.waker());
                Poll::Pending
            }
        })
    }

    /// The next delivery; `None` once the queue closed and drained.
    pub(crate) fn poll_next(
        &self,
        cx: &Context<'_>,
    ) -> Poll<Option<Result<MqttMessage, MqttError>>> {
        self.state.lock(|state| {
            let mut state = state.borrow_mut();
            if let Some(message) = state.items.pop_front() {
                state.writer.wake();
                return Poll::Ready(Some(Ok(message)));
            }
            if let Closed::Ended(error) = &mut state.closed {
                return Poll::Ready(error.take().map(Err));
            }
            state.reader.register(cx.waker());
            Poll::Pending
        })
    }

    /// The next delivery if one waits.
    pub(crate) fn try_next(&self) -> Option<MqttMessage> {
        self.state.lock(|state| {
            let mut state = state.borrow_mut();
            let next = state.items.pop_front();
            if next.is_some() {
                state.writer.wake();
            }
            next
        })
    }

    /// Whether a poll of the stream would answer now: a delivery waits, or the queue closed and
    /// the stream ends. It is what the subscription's readiness probe reads.
    pub(crate) fn may_be_ready(&self) -> bool {
        self.state.lock(|state| {
            let state = state.borrow();
            !state.items.is_empty() || !state.closed.is_open()
        })
    }

    /// Ends the queue: what waits stays readable, and the stream ends after it.
    pub(crate) fn close(&self, error: Option<MqttError>) {
        self.state.lock(|state| {
            let mut state = state.borrow_mut();
            if state.closed.is_open() {
                state.closed = Closed::Ended(error);
            }
            state.reader.wake();
            state.writer.wake();
        });
    }
}
