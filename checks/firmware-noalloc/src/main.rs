//! A device service as a board links it: the MQTT broker's runner and an app with one subscriber
//! on the embassy executor, over an embassy-net stack whose driver is the channel driver a radio
//! task feeds, and no global allocator.
//!
//! The broker's buffers are the `static` below, the subscriber borrows its payload from them, and
//! the binary links only while nothing in the dependency graph needs a heap.

#![no_std]
#![no_main]

use cortex_m_rt::exception;
use embassy_executor::Spawner;
use embassy_net::{Ipv4Address, Ipv4Cidr, Stack, StackResources, StaticConfigV4};
use embassy_net_driver_channel::driver::{HardwareAddress, LinkState};
use embassy_net_driver_channel::{self as channel, Runner as RadioRunner};
use panic_halt as _;
use ruststream::runtime::{EmbeddedApp, Mounts};
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};
use ruststream_rust_mqtt::{
    EmbassyNetwork, MqttBroker, MqttConfig, MqttRunner, MqttState, MqttTopic,
};
use static_cell::{ConstStaticCell, StaticCell};
use systick_timer::SystickDriver;

// The core clock of the board and the SysTick reload for a 1 ms wrap.
embassy_time_driver::time_driver_impl!(static DRIVER: SystickDriver<4>
    = SystickDriver::new(64_000_000, 63_999));

#[exception]
fn SysTick() {
    DRIVER.systick_interrupt();
}

/// The broker's receive and send buffers, subscription table and packet bookkeeping.
static MQTT: ConstStaticCell<MqttState<4, 1024, 8>> = ConstStaticCell::new(MqttState::new());

/// A command as the controller sends its bytes.
#[derive(Deserialized)]
struct Command<'a>(&'a [u8]);

/// The answer to a command, as the bytes that leave.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "valves/v1/state")]
struct Done(&'static [u8]);

/// Applies every command and reports the valve's state.
#[subscriber(MqttTopic::new("valves/v1/set"), reply)]
async fn set(command: &Command<'_>) -> Done {
    let _ = command.0;
    Done(b"done")
}

#[ruststream::app(runtime = embassy)]
fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| b.include(set))
}

/// The largest frame the radio carries.
const MTU: usize = 1500;
type ChannelState = channel::State<MTU, 4, 4>;

/// The radio's task. A board reads its radio into `rx_buf` and writes `tx_buf` to it; this one
/// has no radio, so it takes each outgoing frame and reports it sent.
#[embassy_executor::task]
async fn radio(mut runner: RadioRunner<'static, MTU>) -> ! {
    runner.set_link_state(LinkState::Up);
    let (_state, _rx, mut tx) = runner.borrow_split();
    loop {
        let _frame = tx.tx_buf().await;
        tx.tx_done();
    }
}

#[embassy_executor::task]
async fn network(mut runner: embassy_net::Runner<'static, channel::Device<'static, MTU>>) -> ! {
    runner.run().await
}

#[embassy_executor::task]
async fn mqtt(
    runner: MqttRunner,
    stack: Stack<'static>,
    rx: &'static mut [u8],
    tx: &'static mut [u8],
) -> ! {
    runner.run(EmbassyNetwork::new(stack, rx, tx)).await
}

#[embassy_executor::main]
async fn main(spawner: Spawner) {
    let mut core = cortex_m::Peripherals::take().expect("the core peripherals are taken once");
    DRIVER.start(&mut core.SYST);

    static CHANNEL: StaticCell<ChannelState> = StaticCell::new();
    let (radio_runner, device) =
        channel::new(CHANNEL.init(ChannelState::new()), HardwareAddress::Ip);
    spawner.spawn(radio(radio_runner).expect("the task pool holds the radio"));

    static RESOURCES: StaticCell<StackResources<4>> = StaticCell::new();
    let config = embassy_net::Config::ipv4_static(StaticConfigV4 {
        address: Ipv4Cidr::new(Ipv4Address::new(192, 168, 1, 20), 24),
        gateway: Some(Ipv4Address::new(192, 168, 1, 1)),
        dns_servers: heapless::Vec::new(),
    });
    let (stack, net_runner) = embassy_net::new(
        device,
        config,
        RESOURCES.init(StackResources::new()),
        0x5eed,
    );
    spawner.spawn(network(net_runner).expect("the task pool holds the network"));

    static RX: StaticCell<[u8; 1024]> = StaticCell::new();
    static TX: StaticCell<[u8; 1024]> = StaticCell::new();
    let (broker, mqtt_runner) = MqttBroker::new(
        MQTT.take(),
        MqttConfig::new("192.168.1.10:1883").client_id("valve-1"),
    );
    spawner.spawn(
        mqtt(mqtt_runner, stack, RX.init([0; 1024]), TX.init([0; 1024]))
            .expect("the task pool holds the MQTT task"),
    );
    spawner.spawn(app::task(broker).expect("the task pool holds the app"));
}
