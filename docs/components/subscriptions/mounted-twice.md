---
sidebar_position: 4
title: One filter mounted twice
description: "Two handlers on one filter are one subscription on the server, so they take the messages in turn."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# One filter mounted twice

Two handlers on one filter are one subscription on the server, so they take the messages in turn.

A server holds one subscription per filter, and subscribing the same filter again replaces it.
So the subscriptions an app opens on one filter are members of one route: they share its deliveries, and each message reaches one of them, the next in order.
The members of a [share group](./shared.md) work the same way.
A handler that must see every message reads a filter of its own.

## Example

Two handlers on `jobs/+`: the first takes the first message, the second takes the next, and so on.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_subscriptions_twice.rs#descriptor"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_subscriptions_twice.rs#descriptor"
```

</TabItem>
</Tabs>
