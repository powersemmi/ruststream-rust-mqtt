---
sidebar_position: 1
title: Testing a compio service
description: "The compio TestApp runs a service on the test's own compio runtime, with the broker connected in process."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing a compio service

The compio [`TestApp`](api:ruststream::testing::compio::TestApp) runs a service on the test's own compio runtime, with the broker connected in process.

The test is a plain `#[test]` that blocks on one future of a `compio::runtime::Runtime`, and the calls are those of the [embedded harness](./index.md).
The harness runs the app as the one core of the test's runtime, and `MqttBroker::compio` needs no runner of its own there.
The clock is the runner's virtual clock, which `advance` moves.

## Example

The same job, answered on compio.

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
