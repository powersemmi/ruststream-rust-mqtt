---
sidebar_position: 1
title: Negative acknowledgement
description: "MQTT has no negative acknowledgement: retry answers Unsupported and leaves the delivery unacknowledged."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Negative acknowledgement

MQTT has no negative acknowledgement, so `retry` answers [`AckError::Unsupported`](api:ruststream::AckError::Unsupported) and leaves the delivery unacknowledged.

A [`nack`](api:ruststream::Settle::nack) with `requeue` set is what `retry` asks for.
The transport has no packet for it, so the delivery stays unacknowledged on the server, which redelivers it when a [persistent session](../broker/configuration.md) resumes.
The runtime logs the unsupported answer as a warning and does not send the message again by itself.
A handler that wants the delivery back within the session answers [`retry_after`](./retry-after.md).
A `nack` without `requeue` is a [drop](./index.md), and it acknowledges.

## Example

A job that is not ready to run is answered with `retry`, and the runtime sees one call.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_settlement_nack.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_settlement_nack.rs#handler"
```

</TabItem>
</Tabs>
