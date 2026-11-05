---
sidebar_position: 2
title: Getting started on a compio host
description: "On a compio host MqttBroker::compio starts the runner on the core that connects the broker, so the service is the handler and the app."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Getting started on a compio host

On a compio host the service is the handler and the app.
`MqttBroker::compio` starts the runner on the core that connects the broker, over the TCP stream of compio, so nothing is spawned by hand.

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
The subscription, the acknowledgement and the publish work as on a device: only the network and the runtime differ.
[Testing a compio service](../components/testing/compio.md) shows the test of this app.
