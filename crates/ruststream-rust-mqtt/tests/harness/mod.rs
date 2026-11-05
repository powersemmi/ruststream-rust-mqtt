//! A device service's app on the embedded harness, the broker connected in process.

mod embedded;
mod headers;
#[cfg(feature = "compio")]
mod on_compio;
mod publishing;
mod retry;
mod routing;
