---
sidebar_position: 1
title: Getting started on a device
description: "A device service has no heap: the handler borrows its payload from the receive buffer, the broker's memory is a static, and main spawns the runner and the app."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Getting started on a device

A device service has no heap.
The broker's memory is a `static` the board declares, a delivery borrows its payload from a receive buffer of that memory, and a reply is encoded into a send buffer of it.
This guide builds a valve board that applies the commands published to `valves/v1/set` and reports the state it reached.

A service has three parts: the handler and the app, the task of the runner that owns the MQTT connection, and the `main` of the board that spawns both.

## The crates

```bash
cargo add ruststream --no-default-features --features embassy,macros
cargo add ruststream-rust-mqtt --features embassy
cargo add embassy-executor embassy-net static_cell
```

The `embassy` feature of the broker crate adds `EmbassyNetwork`, the runner's network over an embassy-net stack.
The broker crate is the device variant until its `alloc` feature is on, and the framework follows: a feature of `ruststream` that needs a heap, such as `json`, turns the framework's `alloc` on, and the broker crate then asks for its own `alloc` feature as well.
A device that has a heap takes the [heap variant](./getting-started-compio.md) the same way.

## The handler and the app

The handler takes a `Command` and returns the `State` it reaches.
`Command` is the payload as the bytes that arrived: it borrows them from the receive buffer of the broker, so reading a command copies nothing.
`State` is the bytes that leave, and the reply is encoded straight into a send buffer.
`#[ruststream::app(runtime = embassy)]` turns the app function into an embassy task, `app::task`.

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

## The memory, the runner and the board

[`MqttState`](../components/broker/memory.md) is all the memory the broker owns: the receive buffers deliveries borrow from, the send buffers, and the table of subscriptions.
The board declares it as a `static` and hands it to `MqttBroker::new` once, which returns the broker with its runner.
The runner owns the connection: it opens it when the app connects the broker, and every read, write and acknowledgement goes through it.
So the board spawns two tasks, the runner's over the network stack and the app's with the broker.
The socket buffers live in the runner's task, and their size is the board's choice.

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

The defaults of `MqttState` are four deliveries of up to 1 KiB and eight subscriptions.
The address, the client id, the credentials and the session are the settings of [`MqttConfig`](../components/broker/configuration.md).
Before the firmware reaches a board, [test the service on the host](./testing.md): the test declares its own memory and needs no runner.
