//! [`Pool`] and [`Lease`]: buffers the broker owns, handed out one at a time and returned when the
//! holder drops them.
//!
//! The buffers are parts of a `static` the board declares, so a buffer moves between the runner,
//! the queues and the deliveries as a `&'static mut` reference, never as a copy. A lease returns
//! its buffer on every path that drops it (a settlement, a dropped future, a closed queue), so a
//! pool never loses one.

use core::fmt;
use core::ops::{Deref, DerefMut};

use embassy_sync::channel::Channel;

use crate::fixed::state::Lock;

/// The free buffers of one kind.
pub(crate) struct Pool<Item: 'static, const N: usize> {
    free: Channel<Lock, &'static mut Item, N>,
}

impl<Item: 'static, const N: usize> Pool<Item, N> {
    pub(crate) const fn new() -> Self {
        Self {
            free: Channel::new(),
        }
    }

    /// Hands `item` to the pool. The pool holds as many buffers as there are, so it always has
    /// room for one coming back.
    pub(crate) fn put(&self, item: &'static mut Item) {
        if self.free.try_send(item).is_err() {
            unreachable!("a pool holds every buffer of its kind, so one coming back finds room");
        }
    }

    /// Waits for a free buffer. Cancel-safe: a dropped wait takes nothing.
    pub(crate) async fn take(&'static self) -> Lease<Item, N> {
        Lease::new(self.free.receive().await, self)
    }

    /// A free buffer, or `None` while every one is held.
    pub(crate) fn try_take(&'static self) -> Option<Lease<Item, N>> {
        self.free
            .try_receive()
            .ok()
            .map(|item| Lease::new(item, self))
    }
}

/// One buffer out of a [`Pool`], returned to it when the lease drops.
pub(crate) struct Lease<Item: 'static, const N: usize> {
    item: Option<&'static mut Item>,
    pool: &'static Pool<Item, N>,
}

impl<Item: 'static, const N: usize> Lease<Item, N> {
    const fn new(item: &'static mut Item, pool: &'static Pool<Item, N>) -> Self {
        Self {
            item: Some(item),
            pool,
        }
    }
}

impl<Item: 'static, const N: usize> Deref for Lease<Item, N> {
    type Target = Item;

    fn deref(&self) -> &Item {
        self.item
            .as_deref()
            .unwrap_or_else(|| unreachable!("a lease holds its buffer until it drops"))
    }
}

impl<Item: 'static, const N: usize> DerefMut for Lease<Item, N> {
    fn deref_mut(&mut self) -> &mut Item {
        self.item
            .as_deref_mut()
            .unwrap_or_else(|| unreachable!("a lease holds its buffer until it drops"))
    }
}

impl<Item: 'static, const N: usize> Drop for Lease<Item, N> {
    fn drop(&mut self) {
        if let Some(item) = self.item.take() {
            self.pool.put(item);
        }
    }
}

impl<Item: 'static, const N: usize> fmt::Debug for Lease<Item, N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Lease").finish_non_exhaustive()
    }
}
