---
sidebar_position: 0
title: MqttBroker
description: "MqttBroker connects a service to an MQTT 5 server over the memory the board declares, and its runner owns the connection; a device spawns the runner as a task of its own."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# MqttBroker

[`MqttBroker`](api:ruststream_rust_mqtt::MqttBroker) connects a service to an MQTT 5 server, and the [`MqttRunner`](api:ruststream_rust_mqtt::MqttRunner) that comes with it owns the connection.

On a device, [`MqttBroker::new`](api:ruststream_rust_mqtt::MqttBroker::new) takes the [`MqttState`](./memory.md) the board declared as a `static` and a [`MqttConfig`](./configuration.md), and returns the broker with its runner.
The state is taken for the life of the program, so one state serves one broker.
Building the broker performs no I/O, so the whole service fits the synchronous app builder.
The runner performs every read and write on the connection, and the handles of the service (subscriptions, publishers, acknowledgements) talk to it.
It waits until the app connects the broker, opens the connection over a [network](./network.md), serves it until the app shuts the broker down or the connection is lost, and then waits for the next connect.

A device spawns the runner as a task of its own, next to the task of the app.
The app starts only while the runner runs, because connecting the broker waits for the runner to open the connection.

The broker moves through three types.
`connect` consumes the broker and returns a [`ConnectedMqttBroker`](api:ruststream_rust_mqtt::ConnectedMqttBroker), and `shutdown` consumes that and returns a [`ClosedMqttBroker`](api:ruststream_rust_mqtt::ClosedMqttBroker).
A publisher the service kept across the shutdown answers [`MqttError::NotConnected`](api:ruststream_rust_mqtt::MqttError::NotConnected).

## The two variants

The crate has two variants, chosen by its `alloc` feature, and each fits one kind of build.

The device variant is the default and needs no heap.
The crate speaks the protocol itself: a PUBLISH is read into a receive buffer of the state, and the delivery borrows its topic and payload from that buffer.
Its settings and descriptors hold `&'static str`, so they are constants of the firmware.
A handler reads the payload as bytes it borrows, with `#[derive(Deserialized)]`, and a reply leaves as the bytes it returns, with `#[derive(Serialized)]`.

The heap variant is the `alloc` feature, which `compio` turns on.
The broker runs over the [`rust-mqtt`](https://docs.rs/rust-mqtt) client, which keeps every field it receives in a heap buffer of its own, and the settings hold owned strings.
`MqttBroker::new(config)` takes no state, and on a compio host `MqttBroker::compio` starts the runner on the core that connects the broker, so the host spawns nothing.
A device that has a heap takes this variant too, with `alloc` and `embassy`.
The [compio guide](../../guides/getting-started-compio.md) shows it.

The features, all off by default: `embassy` adds the network of a device, `alloc` selects the heap variant, `compio` adds the network of a compio host and `MqttBroker::compio` (and `alloc`), and `testing` adds the in-process transport a [`TestApp`](../testing/index.md) connects.

## Example

A device service: the handler and the app, then the memory, the runner's task and the `start` function the board's `main` calls once its network stack is up.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/device/device_service.rs#device"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/device_service.rs#device"
```

</TabItem>
</Tabs>

### The runner's task

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/device/device_service.rs#board"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/device_service.rs#board"
```

</TabItem>
</Tabs>
