---
sidebar_position: 0
title: The embedded TestApp
description: "The embedded TestApp runs a device service on the host with the broker connected in process, and the test drives it with publish, advance and assertions."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# The embedded TestApp

The embedded [`TestApp`](api:ruststream::testing::embedded::TestApp) runs a device service on the host with the broker connected in process, and the test drives it with publish, advance and assertions.

The test hands the production app its broker, as the device's task does, and addresses the broker by its type: `tb.broker::<MqttBroker>()`.
The runner is not needed, because the in-process transport replaces the connection.
It is a model of the server: it keeps the subscriptions, the retained message of each topic and the share groups, matches filters with wildcards, and delivers at the lower of the two levels.
`message(..).publish()` is a publish of another client of the server, at QoS 1 and not retained.
The clock is the test's: [`advance`](api:ruststream::testing::embedded::TestApp::advance) moves it, and a `retry_after` copy comes back once its delay has passed.

A subscription holds 16 deliveries, and a test that publishes more without letting the handlers settle them fails with a message that says so.
A persistent session and its redelivery, the receive window and the handshakes behind QoS 1 and 2 belong to the server, and the [live stand](./live.md) tests them.
The assertions on the options of a publish, such as `with_options`, read the steps and the retain flag of an `Out` slot.

## Example

A job is answered, and the answer is read from the topic it was published to.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_testing_embedded.rs#test"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_testing_embedded.rs#test"
```

</TabItem>
</Tabs>

### A retry on the mock clock

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_testing_embedded.rs#advance"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_testing_embedded.rs#advance"
```

</TabItem>
</Tabs>
