//! The integration tests of the crate, one module per file.

// The embassy apps the harness runs are not `Send` by design, as the library's are.
#![allow(clippy::future_not_send)]

mod support;

mod compio_live;
mod conformance;
mod harness;
mod live;
