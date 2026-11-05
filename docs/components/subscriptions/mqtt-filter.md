---
sidebar_position: 1
title: MqttFilter
description: "MqttFilter subscribes to every topic its wildcards match: + matches one level and # matches the rest."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# MqttFilter

[`MqttFilter`](api:ruststream_rust_mqtt::MqttFilter) subscribes to every topic its wildcards match: `+` matches one level and `#` matches the rest.

One filter reads many topics, so it addresses none of them.
A [retry copy](../settlement/retry-after.md) cannot go to a filter, and the mount site names where it goes.
The delivery keeps the topic it was published to, and [`MqttMessage::topic`](api:ruststream_rust_mqtt::MqttMessage::topic) returns it.
A filter that is not a valid topic filter is refused when the app starts.

[`qos`](api:ruststream_rust_mqtt::MqttFilter::qos) and [`shared`](api:ruststream_rust_mqtt::MqttFilter::shared) work as on [`MqttTopic`](./index.md).
A definition written `#[subscriber(MqttFilter)]` takes its filter from the name given at the mount site.

## Example

Every sensor of the first floor reports on a topic of its own.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_subscriptions_filter.rs#descriptor"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_subscriptions_filter.rs#descriptor"
```

</TabItem>
</Tabs>
