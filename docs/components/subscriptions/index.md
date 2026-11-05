---
sidebar_position: 0
title: MqttTopic
description: "MqttTopic subscribes to one topic a publisher can name, and answers for its own retry copies."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# MqttTopic

[`MqttTopic`](api:ruststream_rust_mqtt::MqttTopic) subscribes to one topic a publisher can name, and answers for its own retry copies.

A topic carries no wildcard, so it is a name a publisher can use.
A subscription on a topic is reached again by publishing to that topic, which is why the descriptor can say where the copy of a [retried](../settlement/retry-after.md) delivery goes.
A value with a `+` or a `#` is refused when the app starts, and the message names [`MqttFilter`](./mqtt-filter.md) as the descriptor that takes it.
The topic must not be empty and must be text an MQTT string carries: no control character and no Unicode non-character.

[`qos`](api:ruststream_rust_mqtt::MqttTopic::qos) sets the [quality of service](./qos.md) the subscription asks for, and [`shared`](api:ruststream_rust_mqtt::MqttTopic::shared) makes it a [shared subscription](./shared.md).
A definition written `#[subscriber(MqttTopic)]` takes its topic from the name given at the mount site.

## Example

A valve listens on its own topic.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_subscriptions_topic.rs#descriptor"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_subscriptions_topic.rs#descriptor"
```

</TabItem>
</Tabs>
