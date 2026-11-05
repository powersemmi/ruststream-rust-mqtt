---
sidebar_position: 0
title: Acknowledgement
description: "An ack sends PUBACK at QoS 1 and PUBREC at QoS 2; QoS 0 has no acknowledgement."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Acknowledgement

An [`ack`](api:ruststream::runtime::HandlerOutcome::ack) sends PUBACK at QoS 1 and PUBREC at QoS 2; QoS 0 has no acknowledgement.

A message settles when the handler returns, and the [quality of service](../subscriptions/qos.md) of the delivery decides what that sends.
At QoS 1 and 2 the server keeps a delivery until it is acknowledged, and the client finishes the QoS 2 handshake on its own once the server releases the message.
A QoS 0 delivery has no acknowledgement packet, so settling it answers [`AckError::Unsupported`](api:ruststream::AckError::Unsupported), which the runtime logs as a warning.
[`drop`](api:ruststream::runtime::HandlerOutcome::drop) acknowledges the delivery too, because an acknowledgement is the only final answer MQTT has, and the server never sends the message again.
[`retry`](./nack.md) asks for a negative acknowledgement, which MQTT does not have, and [`retry_after`](./retry-after.md) is the outcome that tries again later.

The acknowledgement goes to the runner, which sends it; a delivery settled after the broker shut down answers `AckError::Broker`, carrying [`MqttError::NotConnected`](api:ruststream_rust_mqtt::MqttError::NotConnected).

## Example

A device agent applies the commands it knows and drops the rest; both answers settle the delivery.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_settlement_ack.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_settlement_ack.rs#handler"
```

</TabItem>
</Tabs>

### QoS 0

The subscription reads telemetry at QoS 0.
The handler settles each delivery as usual, and nothing is sent to the server.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_settlement_qos0.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_settlement_qos0.rs#handler"
```

</TabItem>
</Tabs>
