//! The table that routes deliveries to subscriptions.
//!
//! One wire filter is one subscription on the server: subscribing the same filter again replaces
//! the server's subscription rather than adding one. So the subscriptions the app opens on one
//! wire filter (the members of a share group, or one filter mounted twice) are members of one
//! route, which carries one subscription identifier, and they take its deliveries in turn.

use alloc::string::String;
use alloc::vec::Vec;

use crate::heap::link::Shared;
use crate::heap::queue::Queue;
use crate::qos::Qos;
use crate::topic::matches;

/// A member of a route: one subscription of the app.
pub(crate) type Member = u64;

/// One subscription on the server and the app's subscriptions that read it.
pub(crate) struct Route {
    pub(crate) identifier: u32,
    pub(crate) wire_filter: String,
    /// The quality of service the route subscribed at, which the in-process server delivers at.
    #[cfg_attr(not(feature = "testing"), allow(dead_code))]
    pub(crate) qos: Qos,
    members: Vec<(Member, Shared<Queue>)>,
    /// The member whose turn the next delivery is.
    turn: usize,
}

impl Route {
    /// The filter topics are matched against: the part after `$share/<group>/` for a share
    /// group.
    pub(crate) fn plain_filter(&self) -> &str {
        plain_filter(&self.wire_filter)
    }

    /// The position of the member whose turn it is, which then passes to the next one.
    fn take_turn(&mut self) -> Option<usize> {
        if self.members.is_empty() {
            return None;
        }
        let at = self.turn % self.members.len();
        self.turn = at + 1;
        Some(at)
    }
}

/// What joining a route answered.
pub(crate) struct Joined {
    pub(crate) identifier: u32,
    pub(crate) member: Member,
    /// Whether this member opened the route, so the server is asked for the subscription.
    pub(crate) opened: bool,
}

/// The routes of one session.
#[derive(Default)]
pub(crate) struct Routes {
    table: Vec<Route>,
    next_identifier: u32,
    next_member: Member,
}

impl Routes {
    /// Adds a subscription of the app on `wire_filter`.
    pub(crate) fn join(&mut self, wire_filter: &str, qos: Qos, queue: Shared<Queue>) -> Joined {
        self.next_member += 1;
        let member = self.next_member;
        if let Some(route) = self
            .table
            .iter_mut()
            .find(|route| route.wire_filter == wire_filter)
        {
            route.members.push((member, queue));
            return Joined {
                identifier: route.identifier,
                member,
                opened: false,
            };
        }
        // Identifiers start at one: zero is a protocol error.
        self.next_identifier += 1;
        let identifier = self.next_identifier;
        self.table.push(Route {
            identifier,
            wire_filter: wire_filter.into(),
            qos,
            members: alloc::vec![(member, queue)],
            turn: 0,
        });
        Joined {
            identifier,
            member,
            opened: true,
        }
    }

    /// Takes a subscription of the app out; the route goes with its last member.
    pub(crate) fn leave(&mut self, member: Member) {
        for route in &mut self.table {
            route.members.retain(|(held, _)| *held != member);
        }
        self.table.retain(|route| !route.members.is_empty());
    }

    /// Calls `visit` once per route the delivery reaches, with the queue of the member whose turn
    /// it is: the routes named by `identifiers`, or, when the packet names none, every route whose
    /// filter matches `topic`.
    pub(crate) fn reach(
        &mut self,
        identifiers: &[u32],
        topic: &str,
        mut visit: impl FnMut(&Route, &Shared<Queue>),
    ) {
        for route in &mut self.table {
            let reached = if identifiers.is_empty() {
                matches(topic, route.plain_filter())
            } else {
                identifiers.contains(&route.identifier)
            };
            if reached && let Some(at) = route.take_turn() {
                let route = &*route;
                visit(route, &route.members[at].1);
            }
        }
    }

    /// Which of the members of the route on `wire_filter` takes the next delivery, by the order
    /// they joined; `None` where no route holds that filter.
    #[cfg(feature = "testing")]
    pub(crate) fn next_taker(&self, wire_filter: &str) -> Option<usize> {
        self.table
            .iter()
            .find(|route| route.wire_filter == wire_filter)
            .map(|route| route.turn % route.members.len())
    }

    /// Empties the table and hands back every queue, to be closed.
    pub(crate) fn drain_queues(&mut self) -> Vec<Shared<Queue>> {
        self.table
            .drain(..)
            .flat_map(|route| route.members.into_iter().map(|(_, queue)| queue))
            .collect()
    }
}

/// The filter a shared subscription matches topics with: the part after `$share/<group>/`.
pub(crate) fn plain_filter(wire_filter: &str) -> &str {
    wire_filter
        .strip_prefix("$share/")
        .and_then(|rest| rest.split_once('/'))
        .map_or(wire_filter, |(_, filter)| filter)
}
