---
sidebar_position: 2
title: Getting started on a compio host
description: "On a compio host the broker is the heap variant, and MqttBroker::compio starts the runner on the core that connects it, so the service is a handler and an app."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Getting started on a compio host

On a compio host the service is the handler and the app.
`MqttBroker::compio` starts the runner on the core that connects the broker, over the TCP stream of compio, so nothing is spawned by hand.

## The heap variant

A host has a heap, and the crate's heap variant uses it: the `alloc` feature, which `compio` turns on.
The broker runs over the [`rust-mqtt`](https://docs.rs/rust-mqtt) client, the settings hold owned strings, and handlers decode their payload with the codec of the framework, `json` among them.
The broker needs no `MqttState`, because the client allocates what it receives.

A device that has a heap takes the same variant with `alloc` and `embassy`.
It builds the broker with `MqttBroker::new(config)` and spawns the runner as a task, as in [getting started on a device](./getting-started-device.md), and the handlers use serde types.
A device without a heap takes the [device variant](./getting-started-device.md).

## The crates

```bash
cargo add ruststream --no-default-features --features compio,macros,json
cargo add ruststream-rust-mqtt --features compio
```

## The service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_network_compio.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_network_compio.rs#app"
```

</TabItem>
</Tabs>

`#[ruststream::app(runtime = compio, cores = 1)]` writes the `main` of the binary, which understands the command `run`.
`cargo run -- run` starts the service against the server named in the config.
The subscription, the acknowledgement and the publish work as on a device: only the network, the runtime and the memory differ.
[Testing a compio service](../components/testing/compio.md) shows the test of this app.

## A device with a heap

The runner is a task of the board, and the broker takes the config alone.

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
