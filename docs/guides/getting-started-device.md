---
sidebar_position: 1
title: Getting started on a device
description: "A device service in three parts: the handler and the app, the runner task over the board's network stack, and the board's main that spawns both."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Getting started on a device

A device service has three parts: the handler and the app, the task of the runner that owns the MQTT connection, and the `main` of the board that spawns both.
This guide builds a sensor board that stores the readings published to `sensors/temp`.

## The crates

```bash
cargo add ruststream --no-default-features --features embassy,macros,json
cargo add ruststream-rust-mqtt --features embassy
cargo add embassy-executor embassy-net
```

The `embassy` feature of the broker crate adds `EmbassyNetwork`, the runner's network over an embassy-net stack.
The `json` feature selects the default codec of the handlers.

## The handler and the app

The handler takes a `Reading` and returns an outcome; `ack` sends the acknowledgement of the delivery.
The app function receives the broker from the board, and `#[ruststream::app(runtime = embassy)]` turns it into an embassy task, `app::task`.

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

## The runner and the board

`MqttBroker::new` returns the broker with its runner.
The runner owns the connection: it opens it when the app connects the broker, and every read, write and acknowledgement goes through it.
So the board spawns two tasks, the runner's over the network stack and the app's with the broker.
The socket buffers live in the runner's task, and their size is the board's choice.

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

The address, the client id, the credentials and the session are the settings of [`MqttConfig`](../components/broker/configuration.md).
Before the firmware reaches a board, [test the service on the host](./testing.md).
