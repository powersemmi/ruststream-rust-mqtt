---
sidebar_position: 3
title: Shared subscription
description: "A shared subscription hands each message to one member of a group, so boards that run the same service share the work."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Shared subscription

A shared subscription hands each message to one member of a group, so boards that run the same service share the work.

[`shared(group)`](api:ruststream_rust_mqtt::MqttTopic::shared) on [`MqttTopic`](./index.md) or [`MqttFilter`](./mqtt-filter.md) subscribes through `$share/<group>/<filter>`, an MQTT 5 feature.
The server gives each matching message to one subscription of the group instead of to every one.
The group name must not be empty and must not contain `/`, `+` or `#`; the descriptor refuses it when the app starts.

The server sends no retained message to a shared subscription when it joins.
A retry copy of a delivery goes to the plain topic: the `$share` prefix is a name for subscribing, and a publish to it would reach a topic of that literal spelling.

## Example

Every board runs this service, and the server gives each job to one of them.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_subscriptions_shared.rs#descriptor"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_subscriptions_shared.rs#descriptor"
```

</TabItem>
</Tabs>
