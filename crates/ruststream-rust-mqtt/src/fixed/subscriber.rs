//! [`MqttSubscriber`], the stream of one subscription's deliveries in the device variant.

use core::fmt;
use core::num::NonZeroUsize;
use core::pin::Pin;
use core::task::{Context, Poll};

use futures_core::Stream;
use heapless::Vec as FixedVec;
use ruststream::{BatchSlot, BatchStream, BatchSubscriber, ReadyHint, Subscriber};

use crate::fixed::error::MqttError;
use crate::fixed::message::MqttMessage;
use crate::fixed::state::{Held, Link};

/// The deliveries of one subscription, in the order the server sent them.
///
/// Dropping it ends the subscription for this process: the deliveries still waiting in it go
/// back to the broker unacknowledged, for the server to redeliver when a persistent session
/// resumes. The stream ends after a shutdown once what waits is drained, and yields the error
/// first when the connection was lost.
///
/// # Examples
///
/// ```
/// use futures::StreamExt;
/// use ruststream::{IncomingMessage, Settle, Subscriber};
/// use ruststream_rust_mqtt::MqttSubscriber;
///
/// /// Reads one command and acknowledges it.
/// async fn next_command(subscriber: &mut MqttSubscriber) -> Option<usize> {
///     let mut deliveries = core::pin::pin!(subscriber.stream());
///     let command = deliveries.next().await?.ok()?;
///     let len = command.payload().len();
///     command.ack().await.ok()?;
///     Some(len)
/// }
/// # let _ = next_command;
/// ```
pub struct MqttSubscriber<
    const MESSAGES: usize = 4,
    const PACKET: usize = 1024,
    const SUBSCRIPTIONS: usize = 8,
> {
    link: &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
    held: Held,
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize>
    MqttSubscriber<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    pub(crate) const fn new(
        link: &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
        held: Held,
    ) -> Self {
        Self { link, held }
    }

    fn poll_next(
        &self,
        cx: &Context<'_>,
    ) -> Poll<Option<Result<MqttMessage<MESSAGES, PACKET, SUBSCRIPTIONS>, MqttError>>> {
        self.link
            .table
            .lock(|table| table.borrow_mut().poll_next(self.held, cx))
            .map(|next| next.map(|next| next.map(|inbound| MqttMessage::new(inbound, self.link))))
    }

    fn try_next(&self) -> Option<MqttMessage<MESSAGES, PACKET, SUBSCRIPTIONS>> {
        self.link
            .table
            .lock(|table| table.borrow_mut().try_next(self.held))
            .map(|inbound| MqttMessage::new(inbound, self.link))
    }
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> Drop
    for MqttSubscriber<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    fn drop(&mut self) {
        self.link
            .table
            .lock(|table| table.borrow_mut().leave(self.held));
    }
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> fmt::Debug
    for MqttSubscriber<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttSubscriber")
            .field("subscription", &self.held.index)
            .finish_non_exhaustive()
    }
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> Subscriber
    for MqttSubscriber<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    type Message = MqttMessage<MESSAGES, PACKET, SUBSCRIPTIONS>;
    type Error = MqttError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, MqttError>> + '_ {
        Deliveries { subscriber: self }
    }

    fn ready_hint(&self) -> impl ReadyHint + 'static + use<MESSAGES, PACKET, SUBSCRIPTIONS> {
        Waiting {
            link: self.link,
            held: self.held,
        }
    }
}

/// The probe an embedded app asks before it polls an idle subscription.
struct Waiting<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> {
    link: &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
    held: Held,
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> ReadyHint
    for Waiting<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    fn may_be_ready(&self) -> bool {
        self.link
            .table
            .lock(|table| table.borrow().may_be_ready(self.held))
    }
}

struct Deliveries<'a, const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> {
    subscriber: &'a MqttSubscriber<MESSAGES, PACKET, SUBSCRIPTIONS>,
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> Stream
    for Deliveries<'_, MESSAGES, PACKET, SUBSCRIPTIONS>
{
    type Item = Result<MqttMessage<MESSAGES, PACKET, SUBSCRIPTIONS>, MqttError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.subscriber.poll_next(cx)
    }
}

/// A batch of deliveries of the device variant: at most `MESSAGES` of them, the receive buffers
/// the broker holds.
pub struct MqttBatch<Message, const MESSAGES: usize>(FixedVec<Message, MESSAGES>);

impl<Message, const MESSAGES: usize> fmt::Debug for MqttBatch<Message, MESSAGES> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttBatch")
            .field("messages", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl<Message, const MESSAGES: usize> BatchSlot for MqttBatch<Message, MESSAGES> {
    type Message = Message;

    fn with_capacity(_size: NonZeroUsize) -> Self {
        Self(FixedVec::new())
    }

    fn drain(&mut self) -> impl Iterator<Item = Message> + '_ {
        self.0.drain(..)
    }
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> BatchSubscriber
    for MqttSubscriber<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    type Batch = MqttBatch<MqttMessage<MESSAGES, PACKET, SUBSCRIPTIONS>, MESSAGES>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl BatchStream<Self::Batch, Error = MqttError> + '_ {
        Batches {
            subscriber: self,
            size,
        }
    }
}

struct Batches<'a, const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> {
    subscriber: &'a MqttSubscriber<MESSAGES, PACKET, SUBSCRIPTIONS>,
    size: NonZeroUsize,
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize>
    BatchStream<MqttBatch<MqttMessage<MESSAGES, PACKET, SUBSCRIPTIONS>, MESSAGES>>
    for Batches<'_, MESSAGES, PACKET, SUBSCRIPTIONS>
{
    type Error = MqttError;

    fn poll_fill(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        slot: &mut MqttBatch<MqttMessage<MESSAGES, PACKET, SUBSCRIPTIONS>, MESSAGES>,
    ) -> Poll<Option<Result<(), MqttError>>> {
        let first = match self.subscriber.poll_next(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
            Poll::Ready(Some(Ok(first))) => first,
        };
        // The slot is empty and holds every buffer, so the first push cannot be refused.
        let _ = slot.0.push(first);
        let limit = self.size.get().min(MESSAGES);
        while slot.0.len() < limit {
            let Some(next) = self.subscriber.try_next() else {
                break;
            };
            if slot.0.push(next).is_err() {
                break;
            }
        }
        Poll::Ready(Some(Ok(())))
    }
}
