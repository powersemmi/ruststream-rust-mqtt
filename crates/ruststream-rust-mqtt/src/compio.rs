//! [`CompioNetwork`], the runner's network on a compio host: compio's TCP stream and clock.
//!
//! `io_uring` reads into a buffer it owns until the read completes, while the client reads into
//! the buffer it is given and may stop waiting at any point. So a task of the connection reads
//! the stream into owned buffers, and the stream the client sees copies out of them: one copy per
//! read, and a read the client stops waiting for loses nothing.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::poll_fn;
use std::rc::Rc;
use std::task::{Poll, Waker};
use std::time::Duration;

use compio::BufResult;
use compio::io::{AsyncRead, AsyncWriteExt};
use compio::net::TcpStream;
use compio::runtime::JoinHandle;
use embedded_io_async::{ErrorKind, ErrorType, Read, Write};

use crate::runner::{MqttRunner, Network};

/// How many bytes one read of the stream takes at most.
const READ_BUFFER: usize = 16 * 1024;

/// Starts a broker's runner on the core that connects it.
pub(crate) fn start(runner: MqttRunner) {
    compio::runtime::spawn(runner.run(CompioNetwork::new())).detach();
}

/// The runner's network on a compio host: a TCP stream on the core the runner runs on.
///
/// [`MqttBroker::compio`](crate::MqttBroker::compio) starts its runner over it; a service builds
/// one only to run a runner itself.
///
/// # Examples
///
/// ```
/// use ruststream_rust_mqtt::{CompioNetwork, MqttRunner};
///
/// /// A runner a service starts by hand, on the core it runs on.
/// async fn serve(runner: MqttRunner) -> ! {
///     runner.run(CompioNetwork::new()).await
/// }
/// # let _ = serve;
/// ```
#[derive(Debug, Default)]
pub struct CompioNetwork {
    _private: (),
}

impl CompioNetwork {
    /// A network on the current compio runtime.
    #[must_use]
    pub const fn new() -> Self {
        Self { _private: () }
    }
}

impl Network for CompioNetwork {
    type Stream<'net> = CompioStream;
    type Error = std::io::Error;

    async fn connect(&mut self, host: &str, port: u16) -> Result<CompioStream, std::io::Error> {
        let stream = Rc::new(TcpStream::connect((host, port)).await?);
        stream.set_nodelay(true)?;
        let pipe = Rc::new(RefCell::new(Pipe::default()));
        let reader = compio::runtime::spawn(read_into(Rc::clone(&stream), Rc::clone(&pipe)));
        Ok(CompioStream {
            stream,
            pipe,
            outgoing: Vec::new(),
            _reader: reader,
        })
    }

    async fn sleep(duration: Duration) {
        compio::time::sleep(duration).await;
    }
}

/// What the reading task has read and the client has not taken yet.
#[derive(Default)]
struct Pipe {
    /// Filled buffers, oldest first, and how far the client has read the first.
    filled: VecDeque<Vec<u8>>,
    at: usize,
    /// Buffers the client has emptied, for the task's next reads.
    spare: Vec<Vec<u8>>,
    /// Set when the stream ended or failed: the client reads what is left, then this.
    end: Option<ErrorKind>,
    /// The client, waiting for bytes.
    client: Option<Waker>,
    /// The reading task, waiting for a buffer back.
    reader: Option<Waker>,
}

/// The reading task of one connection.
async fn read_into(stream: Rc<TcpStream>, pipe: Rc<RefCell<Pipe>>) {
    loop {
        let buffer = pipe
            .borrow_mut()
            .spare
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(READ_BUFFER));
        let BufResult(read, buffer) = (&*stream).read(buffer).await;
        let ended = {
            let mut held = pipe.borrow_mut();
            match read {
                Ok(0) => held.end = Some(ErrorKind::ConnectionReset),
                Ok(_) => held.filled.push_back(buffer),
                Err(_) => held.end = Some(ErrorKind::Other),
            }
            if let Some(client) = held.client.take() {
                client.wake();
            }
            held.end.is_some()
        };
        if ended {
            return;
        }
        // The pipe keeps at most two buffers ahead of the client: the task waits for one back.
        poll_fn(|cx| {
            let mut held = pipe.borrow_mut();
            if held.filled.len() < 2 {
                Poll::Ready(())
            } else {
                held.reader = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await;
    }
}

/// The stream the client reads and writes on a compio host.
pub struct CompioStream {
    stream: Rc<TcpStream>,
    pipe: Rc<RefCell<Pipe>>,
    /// The packet being written, handed to `io_uring` whole on the flush and kept for the next one.
    outgoing: Vec<u8>,
    /// The reading task, which ends with the stream.
    _reader: JoinHandle<()>,
}

impl std::fmt::Debug for CompioStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompioStream")
            .field("waiting", &self.pipe.borrow().filled.len())
            .finish_non_exhaustive()
    }
}

impl ErrorType for CompioStream {
    type Error = ErrorKind;
}

impl Read for CompioStream {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, ErrorKind> {
        poll_fn(|cx| {
            let mut held = self.pipe.borrow_mut();
            let held = &mut *held;
            let Some(first) = held.filled.front() else {
                if let Some(end) = held.end {
                    return Poll::Ready(Err(end));
                }
                held.client = Some(cx.waker().clone());
                return Poll::Pending;
            };
            let available = &first[held.at..];
            let taken = available.len().min(buf.len());
            buf[..taken].copy_from_slice(&available[..taken]);
            held.at += taken;
            if held.at == first.len() {
                let mut emptied = held.filled.pop_front().expect("the first buffer is there");
                emptied.clear();
                held.spare.push(emptied);
                held.at = 0;
                if let Some(reader) = held.reader.take() {
                    reader.wake();
                }
            }
            Poll::Ready(Ok(taken))
        })
        .await
    }
}

impl Write for CompioStream {
    // The client writes a packet in pieces and flushes after each packet, so the pieces gather in
    // the owned buffer and the flush hands them to the kernel in one write.
    fn write(&mut self, buf: &[u8]) -> impl Future<Output = Result<usize, ErrorKind>> {
        self.outgoing.extend_from_slice(buf);
        std::future::ready(Ok(buf.len()))
    }

    async fn flush(&mut self) -> Result<(), ErrorKind> {
        if self.outgoing.is_empty() {
            return Ok(());
        }
        let outgoing = std::mem::take(&mut self.outgoing);
        let BufResult(written, mut outgoing) = (&*self.stream).write_all(outgoing).await;
        outgoing.clear();
        self.outgoing = outgoing;
        written.map_err(|_| ErrorKind::Other)
    }
}
