---
sidebar_position: 3
title: MqttState
description: "MqttState is the memory of a device broker: three numbers fix how many deliveries are held at once, the largest packet and how many subscriptions are open."
---

# MqttState

[`MqttState`](api:ruststream_rust_mqtt::MqttState) is the memory a broker of the device variant owns: the receive buffers a delivery borrows from, the send buffers a publish is encoded into, the table of subscriptions and the channels between the handles and the runner.
It is one value of known size that the board declares as a `static`, so nothing is allocated, at startup or per message.

Three numbers fix its size, and they are the type's parameters: `MqttState<MESSAGES, PACKET, SUBSCRIPTIONS>` with the defaults 4, 1024 and 8.
The broker, its runner and its deliveries carry the same three numbers, and a broker over the defaults is named `MqttBroker` alone.

`MESSAGES` is the number of receive buffers.
A delivery holds one from its arrival until its handler settles it or drops it, so this is how many deliveries wait or are in hand at once.
The session asks the server to keep at most one fewer `QoS` 1 and 2 deliveries in flight, so a buffer stays free for the server's answers.
When every buffer is held, the runner stops reading the connection until a handler settles one: that is the back-pressure of the device, and it reaches the server through the TCP window.
A device whose handlers are slow, or that acknowledge late, takes more buffers; the minimum is two.

`PACKET` is the size in bytes of each receive buffer and each send buffer, so it is the largest packet in either direction.
The session announces it to the server as its maximum packet size, and the server never sends a larger one.
A publish that does not fit is refused before it is sent, and the message is handed back to the caller.
The packet holds the fixed header, the topic and the properties besides the payload, so the largest payload is smaller than `PACKET`.
The minimum is 64.

`SUBSCRIPTIONS` is how many subscriptions are open at once, one for each mounted descriptor.
The minimum is one.

A PUBLISH that reaches several subscriptions is copied into a receive buffer for each one beyond the first, so it holds as many buffers as there are subscriptions it matches.
The memory it takes is about `(MESSAGES + 2) * PACKET` bytes, the two being the send buffers, plus a small part for each subscription and each message.
The numbers are checked when the state is built, in a `const` context, so a value below a minimum does not compile.

## Example

Six deliveries of up to 512 bytes and four subscriptions, about 4 KiB of the board's RAM.
More frames than buffers pass through it, because a settled buffer returns to the broker.

{/* single-form: the memory is a type and a static, which have no macro form */}
```rust reference="examples/device/device_memory.rs#state"
```
