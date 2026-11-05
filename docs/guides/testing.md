---
sidebar_position: 3
title: Testing a device service
description: "A device service is tested on the host with the embedded TestApp over the memory the test declares, a compio service with the compio TestApp; both connect the broker in process."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing a device service

A test runs the production app unchanged on the host.
The embedded `TestApp` runs a device service and the compio `TestApp` runs a compio service; both connect the broker in process, so no server and no runner are needed.

## The crate

```bash
cargo add --dev ruststream-rust-mqtt --features testing
```

The tests need `testing` next to the runtime feature of the service: `embassy` for a device, `compio` for a host.

## A test

The test declares the broker's memory as a `static`, as the board does, and starts the app with a broker built by `MqttBroker::new` over it.
It publishes a message to it as another client of the server would, and asserts what the handler received and how it settled.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/device/device_testing.rs#test"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/device_testing.rs#test"
```

</TabItem>
</Tabs>

## Time

The clock of the test is its own.
A `retry_after` copy comes back once `advance` has moved the clock past its delay.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/device/device_testing.rs#advance"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/device_testing.rs#advance"
```

</TabItem>
</Tabs>

## On compio

A compio service is the heap variant, and its test needs no memory of its own.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_testing_compio.rs#test"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_testing_compio.rs#test"
```

</TabItem>
</Tabs>

What the in-process transport leaves to the server, a persistent session and the handshakes of QoS 1 and 2 among them, is tested on the [live stand](../components/testing/live.md).
