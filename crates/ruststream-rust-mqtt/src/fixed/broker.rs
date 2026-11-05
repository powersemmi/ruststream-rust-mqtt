//! [`MqttBroker`] of the device variant, its connected and closed forms.

use core::fmt;
use core::future::Future;

use ruststream::{AddressedCopies, Broker, ConnectedBroker, Subscribe};

use crate::fixed::config::MqttConfig;
use crate::fixed::error::MqttError;
use crate::fixed::runner::MqttRunner;
use crate::fixed::source::validate_topic;
use crate::fixed::state::{Command, Link, Mode, MqttState, Name, Phase, Sent};
use crate::fixed::subscriber::MqttSubscriber;
use crate::fixed::wire;
use crate::qos::Qos;

/// An MQTT 5 broker for a device: no heap, every buffer in an [`MqttState`] the board declares.
///
/// [`new`](Self::new) builds it with the [`MqttRunner`] that owns its connection: the runner
/// performs every read and write, and the broker's handles talk to it through the state. A device
/// spawns the runner as a task of its own over embassy-net. Building one performs no I/O: the
/// connection opens when the app connects the broker.
///
/// The three numbers are the state's: how many deliveries wait or are in hand at once, the
/// largest packet in bytes, and how many subscriptions are open at once. Their defaults are 4,
/// 1024 and 8, so `MqttBroker` alone names a broker over `MqttState::new()`.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts};
/// use ruststream::{Deserialized, subscriber};
/// use ruststream_rust_mqtt::{MqttBroker, MqttConfig, MqttRunner, MqttState, MqttTopic};
/// use static_cell::ConstStaticCell;
///
/// #[derive(Deserialized)]
/// struct Command<'a>(&'a [u8]);
///
/// #[subscriber(MqttTopic::new("valves/v1/set"))]
/// async fn set(command: &Command<'_>) -> HandlerOutcome {
///     let _ = command.0;
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app(runtime = embassy)]
/// pub fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
///     EmbeddedApp::new().with_broker(broker, |b| b.include(set))
/// }
///
/// /// The broker's memory: four deliveries of up to 1 KiB, and eight subscriptions.
/// static MQTT: ConstStaticCell<MqttState> = ConstStaticCell::new(MqttState::new());
///
/// /// The board builds the broker and its runner; `main` spawns the runner's task and the app's.
/// pub fn board() -> (MqttBroker, MqttRunner) {
///     MqttBroker::new(MQTT.take(), MqttConfig::new("192.168.1.10:1883").client_id("valve-1"))
/// }
/// # }
/// # fn main() {}
/// ```
pub struct MqttBroker<
    const MESSAGES: usize = 4,
    const PACKET: usize = 1024,
    const SUBSCRIPTIONS: usize = 8,
> {
    pub(crate) link: &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
    pub(crate) config: MqttConfig,
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> fmt::Debug
    for MqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttBroker")
            .field("address", &self.config.address())
            .finish_non_exhaustive()
    }
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize>
    MqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    /// Builds the broker over `state` and the runner that owns its connection. It performs no
    /// I/O.
    ///
    /// The state is taken mutably for the life of the program, so one state serves one broker.
    /// The runner has to run for the broker to connect: a device spawns it as a task of its own
    /// with [`MqttRunner::run`] over embassy-net.
    pub fn new(
        state: &'static mut MqttState<MESSAGES, PACKET, SUBSCRIPTIONS>,
        config: MqttConfig,
    ) -> (Self, MqttRunner<MESSAGES, PACKET, SUBSCRIPTIONS>) {
        let link = state.split();
        (Self { link, config }, MqttRunner::new(link, config))
    }

    /// The settings this broker connects with.
    #[must_use]
    pub const fn config(&self) -> &MqttConfig {
        &self.config
    }
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> Broker
    for MqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    type Error = MqttError;
    type Connected = ConnectedMqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS>;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        let link = self.link;
        if link.is_open() {
            return Err(MqttError::Invalid("the broker is connected already"));
        }
        link.connected.reset();
        link.set_phase(Phase::Connecting);
        link.connect_requested.signal(());
        link.connected.wait().await?;
        Ok(ConnectedMqttBroker { link })
    }
}

/// The connected [`MqttBroker`]: subscriptions open and publishers pair against it.
///
/// # Examples
///
/// ```
/// use ruststream::SubscriptionSource;
/// use ruststream_rust_mqtt::{ConnectedMqttBroker, MqttError, MqttSubscriber, MqttTopic};
///
/// /// Opens the board's command subscription once the broker is connected.
/// async fn commands(connected: &ConnectedMqttBroker) -> Result<MqttSubscriber, MqttError> {
///     MqttTopic::new("valves/v1/set").subscribe(connected).await
/// }
/// # let _ = commands;
/// ```
pub struct ConnectedMqttBroker<
    const MESSAGES: usize = 4,
    const PACKET: usize = 1024,
    const SUBSCRIPTIONS: usize = 8,
> {
    pub(crate) link: &'static Link<MESSAGES, PACKET, SUBSCRIPTIONS>,
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> Clone
    for ConnectedMqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    fn clone(&self) -> Self {
        Self { link: self.link }
    }
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> fmt::Debug
    for ConnectedMqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectedMqttBroker")
            .finish_non_exhaustive()
    }
}

/// Frees a subscription's entry unless the subscriber took it over.
struct Joining<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> {
    subscriber: Option<MqttSubscriber<MESSAGES, PACKET, SUBSCRIPTIONS>>,
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize>
    ConnectedMqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    /// Opens a subscription to `filter`: the server is asked for it, and its deliveries are
    /// routed to the subscription by the identifier the subscribe carried.
    pub(crate) async fn open(
        &self,
        filter: Name,
        share: Option<&'static str>,
        qos: Qos,
    ) -> Result<MqttSubscriber<MESSAGES, PACKET, SUBSCRIPTIONS>, MqttError> {
        let link = self.link;
        let Some(mode) = link.mode() else {
            return Err(MqttError::NotConnected);
        };
        // The route is in place before the server is asked, so a retained message it sends
        // right after its answer finds the queue.
        let joined = link
            .table
            .lock(|table| table.borrow_mut().join(filter.clone(), share, qos))?;
        let subscriber = MqttSubscriber::new(link, joined.held);
        match mode {
            Mode::Wire => {}
            #[cfg(feature = "testing")]
            Mode::InProcess => {
                link.harness.opened(link, joined, filter.as_str(), share);
                return Ok(subscriber);
            }
        }
        if !joined.opened {
            // The server holds this filter for the session already: the subscription takes its
            // turn among the filter's members.
            return Ok(subscriber);
        }
        let mut joining = Joining {
            subscriber: Some(subscriber),
        };
        let session = link.session();
        let packet = link.take_packet().await.ok_or(MqttError::NotConnected)?;
        let mut buffer = link.outbound.take().await;
        let Some(len) = wire::subscribe(
            &mut buffer.bytes,
            packet,
            joined.identifier,
            filter.as_str(),
            share,
            qos,
        ) else {
            link.release_packet(packet);
            return Err(MqttError::Invalid(
                "the SUBSCRIBE packet does not fit a send buffer of the broker's memory",
            ));
        };
        buffer.len = len;
        buffer.session = session;
        buffer.sent = Sent::Subscribe {
            packet,
            subscription: joined.held.index,
            generation: joined.held.generation,
        };
        let answer = &link.subscribed[joined.held.index];
        answer.reset();
        link.commands.send(Command::Send(buffer)).await;
        loop {
            if !link.is_open() {
                return Err(MqttError::NotConnected);
            }
            let (generation, result) = answer.wait().await;
            if generation == joined.held.generation {
                result?;
                break;
            }
        }
        joining.subscriber.take().ok_or(MqttError::NotConnected)
    }
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> ConnectedBroker
    for ConnectedMqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    type Error = MqttError;
    type Closed = ClosedMqttBroker;

    async fn shutdown(self) -> Result<Self::Closed, Self::Error> {
        let link = self.link;
        match link.mode() {
            Some(Mode::Wire) => {
                link.closed.reset();
                link.commands.send(Command::Shutdown).await;
                link.closed.wait().await;
            }
            #[cfg(feature = "testing")]
            Some(Mode::InProcess) => link.end_session(None),
            None => {}
        }
        match link.phase() {
            Phase::Closed(Some(error)) => Err(error),
            _ => Ok(ClosedMqttBroker { _closed: () }),
        }
    }
}

impl<const MESSAGES: usize, const PACKET: usize, const SUBSCRIPTIONS: usize> Subscribe
    for ConnectedMqttBroker<MESSAGES, PACKET, SUBSCRIPTIONS>
{
    type Subscriber = MqttSubscriber<MESSAGES, PACKET, SUBSCRIPTIONS>;
    // A string-literal subscription names one topic, which is where its copies go back to.
    type Copies = AddressedCopies;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        let checked = validate_topic(name).and_then(|()| {
            Name::built(name).ok_or(MqttError::Invalid(
                "a topic named at run time holds at most 64 bytes",
            ))
        });
        async move { self.open(checked?, None, Qos::default()).await }
    }
}

/// What a [`ConnectedMqttBroker`] shutdown leaves: the session ended with a DISCONNECT.
///
/// # Examples
///
/// ```
/// use ruststream::ConnectedBroker;
/// use ruststream_rust_mqtt::{ClosedMqttBroker, ConnectedMqttBroker, MqttError};
///
/// /// Ends the session when the board powers down.
/// async fn power_down(connected: ConnectedMqttBroker) -> Result<ClosedMqttBroker, MqttError> {
///     connected.shutdown().await
/// }
/// # let _ = power_down;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClosedMqttBroker {
    _closed: (),
}
