//! [`MqttSubscriber`], the stream of one subscription's deliveries.

use alloc::vec::Vec;
use core::fmt;
use core::num::NonZeroUsize;
use core::pin::Pin;
use core::task::{Context, Poll};

use futures_core::Stream;
use ruststream::{BatchStream, BatchSubscriber, ReadyHint, Subscriber};

use crate::heap::error::MqttError;
use crate::heap::link::{Link, Shared};
use crate::heap::message::MqttMessage;
use crate::heap::queue::Queue;
use crate::heap::routes::Member;

/// One subscription of an [`MqttBroker`](crate::MqttBroker): the stream of the deliveries the
/// server sends for it.
///
/// Dropping it stops routing deliveries to it. The subscription itself stays in the server's
/// session, as a persistent session keeps it across connections.
///
/// The stream ends when the session ends: after a shutdown it drains what waits and ends, and
/// after the connection was lost it yields the error once, then ends.
///
/// # Examples
///
/// ```
/// use ruststream::{IncomingMessage, Settle, Subscriber};
/// use ruststream_rust_mqtt::MqttSubscriber;
///
/// /// Acknowledges every delivery until the session ends.
/// async fn drain(mut subscriber: MqttSubscriber) -> usize {
///     use futures::StreamExt;
///     let mut handled = 0;
///     let mut deliveries = core::pin::pin!(subscriber.stream());
///     while let Some(Ok(delivery)) = deliveries.next().await {
///         let _ = delivery.payload();
///         let _ = delivery.ack().await;
///         handled += 1;
///     }
///     handled
/// }
/// # let _ = drain;
/// ```
pub struct MqttSubscriber {
    queue: Shared<Queue>,
    /// The route this subscription is a member of, which a drop leaves.
    detach: Detach,
}

/// Where a subscription leaves its route when it is dropped.
pub(crate) enum Detach {
    Wire(Shared<Link>, Member),
    #[cfg(feature = "testing")]
    Local(Shared<crate::heap::in_process::Bus>, Member),
}

impl fmt::Debug for MqttSubscriber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttSubscriber").finish_non_exhaustive()
    }
}

impl MqttSubscriber {
    pub(crate) fn new(queue: Shared<Queue>, detach: Detach) -> Self {
        Self { queue, detach }
    }
}

impl Drop for MqttSubscriber {
    fn drop(&mut self) {
        match &self.detach {
            Detach::Wire(link, member) => {
                link.routes
                    .lock(|routes| routes.borrow_mut().leave(*member));
            }
            #[cfg(feature = "testing")]
            Detach::Local(bus, member) => bus.leave(*member),
        }
    }
}

impl Subscriber for MqttSubscriber {
    type Message = MqttMessage;
    type Error = MqttError;

    fn stream(&mut self) -> impl Stream<Item = Result<MqttMessage, MqttError>> + '_ {
        Deliveries { queue: &self.queue }
    }

    fn ready_hint(&self) -> impl ReadyHint + 'static + use<> {
        Waiting(Shared::clone(&self.queue))
    }
}

/// The readiness probe of one subscription: whether its stream would answer a poll now.
struct Waiting(Shared<Queue>);

impl ReadyHint for Waiting {
    fn may_be_ready(&self) -> bool {
        self.0.may_be_ready()
    }
}

/// The stream of one subscription. It reads the queue in place, so a stream dropped between
/// polls loses nothing.
struct Deliveries<'a> {
    queue: &'a Shared<Queue>,
}

impl Stream for Deliveries<'_> {
    type Item = Result<MqttMessage, MqttError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.queue.poll_next(cx)
    }
}

impl BatchSubscriber for MqttSubscriber {
    type Batch = Vec<MqttMessage>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl BatchStream<Vec<MqttMessage>, Error = MqttError> + '_ {
        Batches {
            queue: &self.queue,
            size,
        }
    }
}

/// The batch stream of one subscription: it waits for the first delivery and takes what else
/// already waits, up to the batch size, in the same poll.
struct Batches<'a> {
    queue: &'a Shared<Queue>,
    size: NonZeroUsize,
}

impl BatchStream<Vec<MqttMessage>> for Batches<'_> {
    type Error = MqttError;

    fn poll_fill(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        slot: &mut Vec<MqttMessage>,
    ) -> Poll<Option<Result<(), MqttError>>> {
        let first = match self.queue.poll_next(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
            Poll::Ready(Some(Ok(first))) => first,
        };
        slot.push(first);
        while slot.len() < self.size.get() {
            let Some(next) = self.queue.try_next() else {
                break;
            };
            slot.push(next);
        }
        Poll::Ready(Some(Ok(())))
    }
}
