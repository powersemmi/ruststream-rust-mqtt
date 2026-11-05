---
sidebar_position: 2
title: Retained messages
description: "A retained message is kept by the server, which hands the last one of each topic to every new subscription that is not shared."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Retained messages

A retained message is kept by the server, which hands the last one of each topic to every new subscription that is not shared.

[`retain(true)`](api:ruststream_rust_mqtt::MqttPublish::retain) on the policy retains every message of the mount, and the [step](./steps.md) retains one.
A dashboard that starts later reads the state without waiting for the next change.
Publishing an empty retained payload clears the topic.
The subscription receives the retained message at the lower of the two levels, as any other delivery.

## Example

The state of the valve is retained, so a subscriber that joins later reads the latest one.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_publishing_retained.rs#retained"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_publishing_retained.rs#retained"
```

</TabItem>
</Tabs>
