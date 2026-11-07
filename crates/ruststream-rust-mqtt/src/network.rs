//! [`Network`], the link a broker's runner connects over: a TCP stream to the server and the clock
//! of the runtime the runner runs on.

use core::fmt;
use core::future::Future;
use core::time::Duration;

use embedded_io_async::{Read, Write};

/// The connection a runner opens: a TCP stream to the server, and the clock the keep-alive
/// reads.
///
/// The crate implements it over embassy-net (`EmbassyNetwork`, feature `embassy`) and over compio
/// (feature `compio`). A board with another link (a TLS session, a modem's socket) implements it
/// for that link.
///
/// A read of the stream must be cancel-safe: the runner stops waiting for the server's next
/// packet when a handle sends it work, and picks the read up again after.
///
/// # Examples
///
/// ```
/// use core::time::Duration;
///
/// use ruststream_rust_mqtt::Network;
///
/// /// A board's network: a socket it already holds, and the board's own timer.
/// struct Board<Socket> {
///     socket: Option<Socket>,
/// }
///
/// impl<Socket> Network for Board<Socket>
/// where
///     Socket: embedded_io_async::Read + embedded_io_async::Write,
/// {
///     type Stream<'net>
///         = Socket
///     where
///         Self: 'net;
///     type Error = &'static str;
///
///     async fn connect(&mut self, _host: &str, _port: u16) -> Result<Socket, &'static str> {
///         self.socket.take().ok_or("the board has one socket")
///     }
///
///     async fn sleep(duration: Duration) {
///         let _ = duration;
///         core::future::pending::<()>().await;
///     }
/// }
/// ```
pub trait Network {
    /// The stream the client reads and writes, which may borrow the network's buffers.
    type Stream<'net>: Read + Write
    where
        Self: 'net;
    /// What a failed connection reports.
    type Error: fmt::Debug;

    /// Opens a stream to `host` at `port`.
    fn connect(
        &mut self,
        host: &str,
        port: u16,
    ) -> impl Future<Output = Result<Self::Stream<'_>, Self::Error>>;

    /// Completes after `duration`, on the clock of the runtime the runner runs on.
    fn sleep(duration: Duration) -> impl Future<Output = ()>;

    /// Completes once what was written to `stream` has left, so the DISCONNECT the runner wrote
    /// last reaches the server. The default completes at once: a stream whose flush already
    /// waits for its bytes needs nothing more.
    fn close(stream: Self::Stream<'_>) -> impl Future<Output = ()> {
        drop(stream);
        core::future::ready(())
    }
}
