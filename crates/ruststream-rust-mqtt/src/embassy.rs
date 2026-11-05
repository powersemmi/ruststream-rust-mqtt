//! [`EmbassyNetwork`], the runner's network on a device: embassy-net's TCP socket and
//! embassy-time's clock.

use core::fmt;
use core::net::Ipv4Addr;
use core::time::Duration;

use embassy_net::dns::DnsQueryType;
use embassy_net::tcp::{ConnectError, TcpSocket};
use embassy_net::{IpAddress, IpEndpoint, Stack};

use crate::runner::Network;

/// The runner's network on a device: a TCP socket on an embassy-net stack, over buffers the
/// board owns.
///
/// The socket's buffers are the board's: their size is the most the socket holds of the
/// server's packets and of the client's before the radio takes them. A host name is resolved
/// through the stack's DNS client.
///
/// # Examples
///
/// ```
/// use embassy_net::Stack;
/// use ruststream_rust_mqtt::{EmbassyNetwork, MqttRunner};
///
/// /// The board's MQTT task: the socket's buffers live in the task.
/// async fn mqtt(runner: MqttRunner, stack: Stack<'static>) -> ! {
///     let mut rx = [0; 1024];
///     let mut tx = [0; 1024];
///     runner.run(EmbassyNetwork::new(stack, &mut rx, &mut tx)).await
/// }
/// # let _ = mqtt;
/// ```
pub struct EmbassyNetwork<'a> {
    stack: Stack<'a>,
    rx: &'a mut [u8],
    tx: &'a mut [u8],
}

impl fmt::Debug for EmbassyNetwork<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EmbassyNetwork")
            .field("rx", &self.rx.len())
            .field("tx", &self.tx.len())
            .finish_non_exhaustive()
    }
}

impl<'a> EmbassyNetwork<'a> {
    /// A network on `stack`, whose socket reads into `rx` and writes from `tx`.
    pub const fn new(stack: Stack<'a>, rx: &'a mut [u8], tx: &'a mut [u8]) -> Self {
        Self { stack, rx, tx }
    }
}

/// Why a device could not connect.
#[derive(Debug)]
pub enum EmbassyConnectError {
    /// The host is neither an IPv4 address nor a name the stack's DNS client resolves.
    Resolve,
    /// The TCP connection failed.
    Connect(ConnectError),
}

impl Network for EmbassyNetwork<'_> {
    type Stream<'net>
        = TcpSocket<'net>
    where
        Self: 'net;
    type Error = EmbassyConnectError;

    async fn connect(&mut self, host: &str, port: u16) -> Result<TcpSocket<'_>, Self::Error> {
        let address = match host.parse::<Ipv4Addr>() {
            Ok(address) => IpAddress::from(address),
            Err(_) => *self
                .stack
                .dns_query(host, DnsQueryType::A)
                .await
                .map_err(|_| EmbassyConnectError::Resolve)?
                .first()
                .ok_or(EmbassyConnectError::Resolve)?,
        };
        let mut socket = TcpSocket::new(self.stack, self.rx, self.tx);
        socket
            .connect(IpEndpoint::new(address, port))
            .await
            .map_err(EmbassyConnectError::Connect)?;
        Ok(socket)
    }

    async fn sleep(duration: Duration) {
        let micros = u64::try_from(duration.as_micros()).unwrap_or(u64::MAX);
        embassy_time::Timer::after(embassy_time::Duration::from_micros(micros)).await;
    }
}
