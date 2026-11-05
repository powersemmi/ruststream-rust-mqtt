---
sidebar_position: 1
title: The runner and the handles
description: "One task, the runner, owns the connection; the handles of the service reach it through storage that exists from the start."
---

# The runner and the handles

One task, the runner, owns the connection to the server.
The handles of the service (the subscriptions, the publishers and the acknowledgements of the handlers) reach it through storage that exists from the start.

```mermaid
flowchart LR
    subgraph service["the service"]
        handler["handler"]
        publisher["publisher"]
        subscription["subscription"]
    end
    subgraph link["shared link"]
        commands["commands (32)"]
        slots["staging slots (8)"]
        queues["delivery queues (16 each)"]
    end
    runner["MqttRunner"]
    server[("MQTT server")]
    handler -- "ack" --> commands
    publisher -- "bytes" --> slots
    publisher -- "publish" --> commands
    subscription -- "subscribe" --> commands
    commands --> runner
    slots --> runner
    runner -- "deliveries" --> queues
    queues --> subscription
    runner <--> server
```

The runner is the only code that touches the client.
It serves one loop: packets from the server, commands from the handles and the keep-alive timer.
Each of the three is a step that completes without waiting for the others, so the acknowledgement of a handler never waits behind a read.

A subscription has a queue of 16 deliveries.
When a queue is full, the runner stops reading the connection until the handler takes one.
That is the flow control of the crate.

A publish copies its bytes into one of 8 staging slots and sends the runner a command.
The slot comes back to the pool when the runner has written the packet.
The commands wait in a queue of 32, and a sender waits for room when it is full.

When the connection ends, by a shutdown or by a loss, the runner answers every command that waits, tells every handle the broker is closed, and waits for the next connect.
A handle used after that answers an error and never reports a success the connection did not make.

On a device the runner is a task next to the app's.
On a compio host the broker starts it on the core that connects the broker.
