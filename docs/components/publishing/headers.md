---
sidebar_position: 3
title: Headers and properties
description: "Headers travel as MQTT 5 user properties; content-type, reply-to and correlation-id take the matching properties."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Headers and properties

Headers travel as MQTT 5 user properties, and `content-type`, `reply-to` and `correlation-id` take the matching properties of the protocol.

A serde struct is the contract of a message's headers, and each field is one property whose value is a string.
The handler reads them with the `Headers` extractor, and a publish writes them with `with_headers`.
A textual content type also sets the payload format indicator, and `correlation-id` is binary on the wire.
A header whose name or value is not UTF-8 text without control characters is refused with [`MqttError::Publish`](api:ruststream_rust_mqtt::MqttError::Publish), except the correlation id.
The header `partition-key` ([`PARTITION_KEY_HEADER`](api:ruststream_rust_mqtt::PARTITION_KEY_HEADER)) is the key that keyed workers order a subscription's deliveries by.

## Example

The state leaves with the trace id the command arrived with.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_publishing_headers.rs#headers"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_publishing_headers.rs#headers"
```

</TabItem>
</Tabs>
