//! The connection settings of a device: a `const` the firmware keeps, with the address, the client
//! identifier, the keep-alive, the credentials and the session the server keeps.
//!
//! ```text
//! cd checks/noalloc && cargo test --example device_config
//! ```

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use core::time::Duration;

use ruststream::Deserialized;
use ruststream_rust_mqtt::prelude::*;

// --8<-- [start:config]
/// The server on the plant network, as this valve. The texts are literals of the firmware, so
/// the settings are a constant. The server keeps the session for ten minutes after the
/// connection ends and redelivers what the valve had not acknowledged.
const VALVE: MqttConfig = MqttConfig::new("mqtt://192.168.1.10:1883")
    .client_id("valve-1")
    .keep_alive(Duration::from_secs(30))
    .credentials("valve-1", "secret")
    .persistent_session(Duration::from_secs(600));
// --8<-- [end:config]

/// A command as the controller sends its bytes.
#[derive(Deserialized)]
struct Command<'a>(&'a [u8]);

/// Every command is delivered exactly once, also across a reconnect.
#[subscriber(MqttTopic::new("valves/v1/set").qos(Qos::ExactlyOnce))]
async fn set(command: &Command<'_>) -> HandlerOutcome {
    let _ = command.0;
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn app(broker: MqttBroker) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(broker, |b| b.include(set))
}

fn main() {
    let _ = VALVE;
}

#[cfg(test)]
mod tests {
    use embassy_futures::block_on;
    use ruststream::testing::embedded::TestApp;
    use ruststream::{Outgoing, Serialized};
    use ruststream_rust_mqtt::MqttState;
    use static_cell::ConstStaticCell;

    use super::*;

    /// What the controller publishes, to the topic it names.
    #[derive(Outgoing, Serialized)]
    struct Sent(&'static [u8]);

    #[test]
    fn the_app_starts_with_the_settings_of_the_valve() {
        static MEMORY: ConstStaticCell<MqttState> = ConstStaticCell::new(MqttState::new());
        block_on(async {
            let (broker, _runner) = MqttBroker::new(MEMORY.take(), VALVE);
            let tb = TestApp::start(app(broker)).await.expect("the app starts");
            let valve = tb.broker::<MqttBroker>();
            valve
                .message(&Sent(b"open"))
                .to("valves/v1/set")
                .publish()
                .await
                .expect("the harness publishes");

            valve
                .subscriber("valves/v1/set")
                .assert_called_once()
                .settled(HandlerOutcome::ack());
            tb.shutdown().await.expect("the app stops");
        });
    }

    #[test]
    fn an_address_with_another_scheme_fails_at_startup() {
        static MEMORY: ConstStaticCell<MqttState> = ConstStaticCell::new(MqttState::new());
        block_on(async {
            let (broker, _runner) =
                MqttBroker::new(MEMORY.take(), MqttConfig::new("mqtts://192.168.1.10:8883"));
            let started = TestApp::start(app(broker)).await;
            assert!(started.is_err(), "the broker connects over plain TCP");
        });
    }

    #[test]
    fn the_password_stays_out_of_the_debug_form() {
        let shown = format!("{VALVE:?}");
        assert!(shown.contains("valve-1"));
        assert!(!shown.contains("secret"));
    }
}
