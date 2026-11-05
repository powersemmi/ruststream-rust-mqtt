---
sidebar_position: 0
title: MqttBroker
description: "MqttBroker connects a service to an MQTT 5 server, and its runner owns the connection; a device spawns the runner as a task of its own."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# MqttBroker

[`MqttBroker`](api:ruststream_rust_mqtt::MqttBroker) connects a service to an MQTT 5 server, and the [`MqttRunner`](api:ruststream_rust_mqtt::MqttRunner) that comes with it owns the connection.

[`MqttBroker::new`](api:ruststream_rust_mqtt::MqttBroker::new) takes a [`MqttConfig`](./configuration.md) and returns the broker with its runner.
It performs no I/O, so the whole service fits the synchronous app builder.
The runner performs every read and write on the connection, and the handles of the service (subscriptions, publishers, acknowledgements) talk to it.
It waits until the app connects the broker, opens the connection over a [network](./network.md), serves it until the app shuts the broker down or the connection is lost, and then waits for the next connect.

A device spawns the runner as a task of its own, next to the task of the app.
The app starts only while the runner runs, because connecting the broker waits for the runner to open the connection.
On a compio host, [`MqttBroker::compio`](api:ruststream_rust_mqtt::MqttBroker::compio) starts the runner on the core that connects the broker, so the host spawns nothing.

The broker moves through three types.
`connect` consumes the broker and returns a [`ConnectedMqttBroker`](api:ruststream_rust_mqtt::ConnectedMqttBroker), and `shutdown` consumes that and returns a [`ClosedMqttBroker`](api:ruststream_rust_mqtt::ClosedMqttBroker).
A publisher the service kept across the shutdown answers [`MqttError::NotConnected`](api:ruststream_rust_mqtt::MqttError::NotConnected).

The crate builds the framework's `alloc` variant: the client keeps every field it receives in a heap buffer of its own.
The crate has three features, all off by default: `embassy` adds the network of a device, `compio` adds the network of a compio host and `MqttBroker::compio`, and `testing` adds the in-process transport a [`TestApp`](../testing/index.md) connects.

## Example

A device service: the handler and the app, then the runner's task and the `start` function the board's `main` calls once its network stack is up.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_device.rs#device"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_device.rs#device"
```

</TabItem>
</Tabs>

### The runner's task

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_device.rs#runner"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_device.rs#runner"
```

</TabItem>
</Tabs>
