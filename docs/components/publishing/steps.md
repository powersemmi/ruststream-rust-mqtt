---
sidebar_position: 1
title: MqttPublishSteps
description: "The steps qos and retain on the publish builder reopen, for one message, what the policy of the mount fixed."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# MqttPublishSteps

The steps [`qos`](api:ruststream_rust_mqtt::MqttPublishSteps::qos) and [`retain`](api:ruststream_rust_mqtt::MqttPublishSteps::retain) on the publish builder reopen, for one message, what the policy of the mount fixed.

[`MqttPublishSteps`](api:ruststream_rust_mqtt::MqttPublishSteps) is in the [prelude](api:ruststream_rust_mqtt::prelude), and the steps appear on a builder over an MQTT publisher and on no other broker's.
A setting that a call leaves unset keeps the value of the policy.
A handler body that takes a step bounds its slot with `Out<impl Publisher<Options = MqttPublishOptions>, Slot>`, and that is the one place a body names a type of this crate.
The settings of one call travel as an [`MqttPublishOptions`](api:ruststream_rust_mqtt::MqttPublishOptions), and a test reads them back with `with_options`.

## Example

A state change is sent exactly once and retained.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_publishing_steps.rs#steps"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_publishing_steps.rs#steps"
```

</TabItem>
</Tabs>
