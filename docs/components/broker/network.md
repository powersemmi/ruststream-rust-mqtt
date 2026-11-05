---
sidebar_position: 2
title: Network
description: "Network is the link the runner connects over: embassy-net on a device, compio's TCP stream on a host, or a link of the board's own."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Network

[`Network`](api:ruststream_rust_mqtt::Network) is the link the runner connects over: it opens a stream to a host and a port, and it sleeps on the clock of the runtime the runner runs on.

The stream is anything that reads and writes through `embedded-io-async`, and a read of it must be cancel-safe: the runner stops waiting for the next packet of the server when a handle sends it work, and picks the read up again afterwards.
A device uses [`EmbassyNetwork`](api:ruststream_rust_mqtt::EmbassyNetwork) (feature `embassy`): a TCP socket on an embassy-net stack, over a receive buffer and a send buffer that the task of the runner owns.
A host name in the address is resolved by the DNS client of the stack, and an IPv4 literal is used as it is.
A failure is an [`EmbassyConnectError`](api:ruststream_rust_mqtt::EmbassyConnectError): the name did not resolve, or the TCP connection failed.

A compio host uses [`CompioNetwork`](api:ruststream_rust_mqtt::CompioNetwork) (feature `compio`), and [`MqttBroker::compio`](api:ruststream_rust_mqtt::MqttBroker::compio) starts the runner over it.
`io_uring` reads into a buffer it owns until the read completes, so a task of the connection reads the stream into owned buffers and the client reads out of them.
That costs one copy per read, and a read the client stops waiting for loses nothing.

A board with another link, such as a TLS session or the socket of a modem, implements `Network` for that link; the rustdoc of the trait shows a board's network.
Whatever the network, the runner takes it as an argument of [`MqttRunner::run`](api:ruststream_rust_mqtt::MqttRunner::run).

## Example

The runner of a device, over embassy-net.

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

### A compio host

The broker starts its runner on the core that connects it.

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
