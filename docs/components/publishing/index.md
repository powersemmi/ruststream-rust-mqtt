---
sidebar_position: 0
title: MqttPublish
description: "MqttPublish is the publish policy of a mount: the quality of service and the retain flag of every message it publishes."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# MqttPublish

[`MqttPublish`](api:ruststream_rust_mqtt::MqttPublish) is the publish policy of a mount: the quality of service and the retain flag of every message it publishes.

A policy is a plain declaration that holds no connection, and the runtime pairs it with the connected broker into an [`MqttPublisher`](api:ruststream_rust_mqtt::MqttPublisher).
[`qos`](api:ruststream_rust_mqtt::MqttPublish::qos) defaults to [`Qos::AtLeastOnce`](api:ruststream_rust_mqtt::Qos::AtLeastOnce) and [`retain`](api:ruststream_rust_mqtt::MqttPublish::retain) to `false`.
The policy is also the default of the broker, so a reply and a retry copy leave through it unless the mount names another.

On a device the publisher encodes the message straight into a send buffer of the broker's [memory](../broker/memory.md), which the runner writes, and a message that does not fit the packet is refused with the message handed back.
On the heap variant the publisher copies the bytes of a message into a staging slot of the broker, whose buffers keep their capacity, so a publish allocates nothing once the slots have held the largest message the service sends.
`Ok` means the PUBLISH packet went out into the session; at QoS 1 and 2 the session keeps the message until the server acknowledges it.
A publish to a topic no server takes, such as one with a wildcard, is refused with [`MqttError::Publish`](api:ruststream_rust_mqtt::MqttError::Publish), and a publish after the shutdown answers `MqttError::NotConnected`.
A refused publish hands its message back.

## Example

A device reports every reading in degrees, and the reports are sent exactly once.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_publishing_policy.rs#policy"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_publishing_policy.rs#policy"
```

</TabItem>
</Tabs>
