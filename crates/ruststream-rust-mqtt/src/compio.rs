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
        let outbox = Rc::new(RefCell::new(Outbox::default()));
        let reader = compio::runtime::spawn(read_into(Rc::clone(&stream), Rc::clone(&pipe)));
        let writer = compio::runtime::spawn(write_from(stream, Rc::clone(&outbox)));
        Ok(CompioStream {
            pipe,
            outbox,
            _reader: reader,
            _writer: writer,
        })
    }

    async fn sleep(duration: Duration) {
        compio::time::sleep(duration).await;
    }

    // The writing task hands the kernel what the client gathered after the client's flush, so
    // the stream waits for its outbox to drain before it goes.
    async fn close(stream: Self::Stream<'_>) {
        stream.release();
        poll_fn(|cx| {
            let mut held = stream.outbox.borrow_mut();
            if (held.gathered.is_empty() && !held.writing) || held.failed.is_some() {
                return Poll::Ready(());
            }
            held.client = Some(cx.waker().clone());
            Poll::Pending
        })
        .await;
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

/// What the client has written and the writing task has not handed to the kernel yet.
///
/// Two buffers take turns: the client writes into one while the task's write owns the other. The
/// bytes leave when the runner has nothing more to write in its pass and waits for the server, so
/// the packets of one pass (a reply and the acknowledgement of the delivery it answers, the
/// acknowledgements of a batch) leave in one write. Each write is one operation of the driver.
#[derive(Default)]
struct Outbox {
    gathered: Vec<u8>,
    /// The buffer the last write gave back, for the next turn.
    spare: Vec<u8>,
    /// Set while a write is with the kernel.
    writing: bool,
    /// Set when the gathered bytes are to leave: the runner has nothing more to write in this
    /// pass (it waits for the server), or the outbox runs full, or the stream closes.
    released: bool,
    /// Set when a write failed: the client's next write or flush reports it.
    failed: Option<ErrorKind>,
    /// The writing task, waiting for bytes.
    writer: Option<Waker>,
    /// The client, waiting for the gathered bytes to drain below the limit.
    client: Option<Waker>,
}

/// How many bytes the client may gather ahead of the writing task before a flush waits.
const GATHER_LIMIT: usize = 64 * 1024;

/// The writing task of one connection.
async fn write_from(stream: Rc<TcpStream>, outbox: Rc<RefCell<Outbox>>) {
    loop {
        let gathered = poll_fn(|cx| {
            let mut held = outbox.borrow_mut();
            if held.gathered.is_empty() || !held.released {
                held.writer = Some(cx.waker().clone());
                return Poll::Pending;
            }
            let spare = std::mem::take(&mut held.spare);
            let gathered = std::mem::replace(&mut held.gathered, spare);
            held.writing = true;
            held.released = false;
            if let Some(client) = held.client.take() {
                client.wake();
            }
            Poll::Ready(gathered)
        })
        .await;
        let BufResult(written, mut gathered) = (&*stream).write_all(gathered).await;
        gathered.clear();
        let mut held = outbox.borrow_mut();
        held.spare = gathered;
        held.writing = false;
        if let Some(client) = held.client.take() {
            client.wake();
        }
        if written.is_err() {
            held.failed = Some(ErrorKind::Other);
            if let Some(client) = held.client.take() {
                client.wake();
            }
            return;
        }
    }
}

/// The stream the client reads and writes on a compio host.
pub struct CompioStream {
    pipe: Rc<RefCell<Pipe>>,
    outbox: Rc<RefCell<Outbox>>,
    /// The reading and the writing task, which end with the stream.
    _reader: JoinHandle<()>,
    _writer: JoinHandle<()>,
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

impl CompioStream {
    /// Lets the gathered bytes leave.
    fn release(&self) {
        let mut held = self.outbox.borrow_mut();
        if held.gathered.is_empty() {
            return;
        }
        held.released = true;
        if let Some(writer) = held.writer.take() {
            writer.wake();
        }
    }

    /// Copies what the pipe holds into `buf`; `None` when it holds nothing yet.
    fn take_waiting(&self, buf: &mut [u8]) -> Option<Result<usize, ErrorKind>> {
        let mut held = self.pipe.borrow_mut();
        let held = &mut *held;
        let Some(first) = held.filled.front() else {
            return held.end.map(Err);
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
        Some(Ok(taken))
    }
}

impl Read for CompioStream {
    // The client reads a packet's header a byte at a time: each read is a copy out of the
    // buffer the reading task filled, and waits only when the buffer is empty.
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, ErrorKind> {
        if let Some(read) = self.take_waiting(buf) {
            return read;
        }
        poll_fn(|cx| {
            if let Some(read) = self.take_waiting(buf) {
                return Poll::Ready(read);
            }
            self.pipe.borrow_mut().client = Some(cx.waker().clone());
            // The runner waits for the server: what it wrote in this pass leaves now.
            self.release();
            Poll::Pending
        })
        .await
    }
}

impl Write for CompioStream {
    // The client writes a packet in pieces and flushes after each packet: the pieces gather in
    // the outbox, and the flush hands them to the writing task, which takes all that gathered
    // in one write.
    fn write(&mut self, buf: &[u8]) -> impl Future<Output = Result<usize, ErrorKind>> {
        let mut held = self.outbox.borrow_mut();
        let written = held.failed.map_or_else(
            || {
                held.gathered.extend_from_slice(buf);
                Ok(buf.len())
            },
            Err,
        );
        std::future::ready(written)
    }

    async fn flush(&mut self) -> Result<(), ErrorKind> {
        poll_fn(|cx| {
            let mut held = self.outbox.borrow_mut();
            if let Some(failed) = held.failed {
                return Poll::Ready(Err(failed));
            }
            // The client goes on while the bytes gather; they leave once it waits for the
            // server, and it waits for them only when it runs this far ahead of the kernel.
            if held.gathered.len() > GATHER_LIMIT / 2 {
                held.released = true;
                if let Some(writer) = held.writer.take() {
                    writer.wake();
                }
            }
            if held.gathered.len() > GATHER_LIMIT {
                held.client = Some(cx.waker().clone());
                return Poll::Pending;
            }
            Poll::Ready(Ok(()))
        })
        .await
    }
}
