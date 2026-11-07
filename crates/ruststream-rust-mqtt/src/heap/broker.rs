//! [`MqttBroker`], its connected and closed forms, and the runner that owns its connection.

use alloc::string::ToString;
use core::fmt;
use core::future::Future;

use ruststream::{
    AddressedCopies, Broker, ConnectedBroker, DefaultPublish, DescribeServer, ServerSpec, Subscribe,
};

use crate::heap::config::MqttConfig;
use crate::heap::error::MqttError;
#[cfg(feature = "testing")]
use crate::heap::in_process::Bus;
use embassy_sync::signal::Signal;

use crate::heap::link::{Command, Link, Phase, Shared, SubscribeRequest};
use crate::heap::publisher::MqttPublish;
use crate::heap::queue::Queue;
use crate::heap::runner::MqttRunner;
use crate::heap::source::{MqttFilter, MqttTopic};
use crate::heap::subscriber::{Detach, MqttSubscriber};

/// How many deliveries one subscription holds, waiting for its handler, before the runner stops
/// reading the connection until the handler takes one.
pub(crate) const DELIVERIES_PER_SUBSCRIPTION: usize = 16;

/// An MQTT 5 broker over the rust-mqtt client.
///
/// [`new`](Self::new) builds it with the [`MqttRunner`] that owns its connection: the runner
/// performs every read and write, and the broker's handles talk to it. A device spawns the runner
/// as a task of its own over embassy-net; a host on compio builds the broker with
/// [`compio`](Self::compio), which starts the runner itself. Building one performs no I/O: the
/// connection opens when the app connects the broker.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts};
/// use ruststream::{Deserialized, subscriber};
/// use ruststream_rust_mqtt::{MqttBroker, MqttConfig, MqttTopic};
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
/// /// The board builds the broker and its runner; `main` spawns the runner's task and the app's.
/// pub fn board() {
///     let (broker, runner) = MqttBroker::new(MqttConfig::new("192.168.1.10:1883").client_id("valve-1"));
///     # drop((broker, runner));
/// }
/// # }
/// # fn main() {}
/// ```
pub struct MqttBroker {
    link: Shared<Link>,
    /// The runner the broker starts itself when it connects, where the runtime lets it.
    #[cfg(feature = "compio")]
    starts: Option<MqttRunner>,
}

impl fmt::Debug for MqttBroker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttBroker")
            .field("address", &self.link.config.address())
            .finish_non_exhaustive()
    }
}

impl MqttBroker {
    /// Builds the broker and the runner that owns its connection. It performs no I/O.
    ///
    /// The runner has to run for the broker to connect: a device spawns it as a task of its own
    /// with [`MqttRunner::run`] over embassy-net.
    pub fn new(config: MqttConfig) -> (Self, MqttRunner) {
        let link = Shared::new(Link::new(config));
        (
            Self {
                link: Shared::clone(&link),
                #[cfg(feature = "compio")]
                starts: None,
            },
            MqttRunner::new(link),
        )
    }

    /// Builds a broker for the compio runtime. It performs no I/O: when the app connects it, the
    /// broker starts its runner on the core that connects it, over compio's TCP stream.
    #[cfg(feature = "compio")]
    #[must_use]
    pub fn compio(config: MqttConfig) -> Self {
        let (mut broker, runner) = Self::new(config);
        broker.starts = Some(runner);
        broker
    }

    /// The settings this broker connects with.
    #[must_use]
    pub fn config(&self) -> &MqttConfig {
        &self.link.config
    }
}

impl Broker for MqttBroker {
    type Error = MqttError;
    type Connected = ConnectedMqttBroker;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        #[cfg(feature = "compio")]
        let mut this = self;
        #[cfg(feature = "compio")]
        if let Some(runner) = this.starts.take() {
            crate::heap::compio::start(runner);
        }
        #[cfg(feature = "compio")]
        let link = this.link;
        #[cfg(not(feature = "compio"))]
        let link = self.link;
        if link.phase() == Phase::Open {
            return Err(MqttError::Invalid(
                "the broker is connected already".to_string(),
            ));
        }
        link.connected.reset();
        link.set_phase(Phase::Connecting);
        link.connect_requested.signal(());
        link.connected.wait().await?;
        Ok(ConnectedMqttBroker {
            backend: Backend::Wire(link),
        })
    }
}

// The document names where clients connect, never who: the credentials stay out of it.
impl DescribeServer for MqttBroker {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::from_url(self.link.config.address(), "mqtt").protocol_version("5.0")
    }
}

/// Where a connected broker's work goes.
#[derive(Clone)]
pub(crate) enum Backend {
    /// The runner and the server it is connected to.
    Wire(Shared<Link>),
    /// The in-process transport the test harness connects through.
    #[cfg(feature = "testing")]
    InProcess(Shared<Bus>),
}

/// The connected [`MqttBroker`]: subscriptions open and publishers pair against it.
///
/// # Examples
///
/// ```
/// use ruststream::{ConnectedBroker, SubscriptionSource};
/// use ruststream_rust_mqtt::{ConnectedMqttBroker, MqttError, MqttTopic, MqttSubscriber};
///
/// /// Opens the board's command subscription once the broker is connected.
/// async fn commands(connected: &ConnectedMqttBroker) -> Result<MqttSubscriber, MqttError> {
///     MqttTopic::new("valves/v1/set").subscribe(connected).await
/// }
/// # let _ = commands;
/// ```
#[derive(Clone)]
pub struct ConnectedMqttBroker {
    pub(crate) backend: Backend,
}

impl fmt::Debug for ConnectedMqttBroker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectedMqttBroker")
            .finish_non_exhaustive()
    }
}

impl ConnectedMqttBroker {
    /// Opens a subscription to `filter`: the server is asked for it, and its deliveries are
    /// routed to the subscription by the identifier the subscribe carried.
    pub(crate) async fn open(&self, filter: &MqttFilter) -> Result<MqttSubscriber, MqttError> {
        match &self.backend {
            Backend::Wire(link) => open_on_wire(link, filter).await,
            #[cfg(feature = "testing")]
            Backend::InProcess(bus) => Ok(bus.subscribe(filter)),
        }
    }
}

async fn open_on_wire(
    link: &Shared<Link>,
    filter: &MqttFilter,
) -> Result<MqttSubscriber, MqttError> {
    if !link.is_open() {
        return Err(MqttError::NotConnected);
    }
    let wire_filter = filter.wire_filter();
    let queue = Shared::new(Queue::new(DELIVERIES_PER_SUBSCRIPTION));
    // The route is in place before the server is asked, so a retained message it sends right
    // after its answer finds the queue.
    let joined = link.routes.lock(|routes| {
        routes
            .borrow_mut()
            .join(&wire_filter, filter.qos_value(), Shared::clone(&queue))
    });
    let subscriber = MqttSubscriber::new(queue, Detach::Wire(Shared::clone(link), joined.member));
    if !joined.opened {
        // The server holds this filter for the session already: the subscription takes its
        // turn among the route's members.
        return Ok(subscriber);
    }
    let request = Shared::new(SubscribeRequest {
        wire_filter,
        qos: filter.qos_value(),
        identifier: joined.identifier,
        done: Signal::new(),
    });
    link.commands
        .send(Command::Subscribe(Shared::clone(&request)))
        .await;
    if link.is_open() {
        request.done.wait().await?;
        Ok(subscriber)
    } else {
        Err(MqttError::NotConnected)
    }
}

impl ConnectedBroker for ConnectedMqttBroker {
    type Error = MqttError;
    type Closed = ClosedMqttBroker;

    async fn shutdown(self) -> Result<Self::Closed, Self::Error> {
        {
            match self.backend {
                Backend::Wire(link) => {
                    if link.is_open() {
                        link.closed.reset();
                        link.commands.send(Command::Shutdown).await;
                        link.closed.wait().await;
                    }
                    match link.phase() {
                        Phase::Closed(Some(error)) => Err(error),
                        _ => Ok(ClosedMqttBroker { _closed: () }),
                    }
                }
                #[cfg(feature = "testing")]
                Backend::InProcess(bus) => {
                    bus.close();
                    Ok(ClosedMqttBroker { _closed: () })
                }
            }
        }
    }
}

impl Subscribe for ConnectedMqttBroker {
    type Subscriber = MqttSubscriber;
    // A string-literal subscription names one topic, which is where its copies go back to.
    type Copies = AddressedCopies;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        let topic = MqttTopic::new(name);
        async move {
            topic.validate()?;
            self.open(&topic.into_filter()).await
        }
    }
}

impl DefaultPublish for ConnectedMqttBroker {
    type Policy = MqttPublish;
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
