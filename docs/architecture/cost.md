---
sidebar_position: 2
title: What a delivery and a publish cost
description: "On a device a delivery borrows the receive buffer its packet arrived in and a publish is encoded into a send buffer, with no allocation; the heap variant takes a heap buffer for each field."
---

# What a delivery and a publish cost

## The device variant

A PUBLISH is read from the socket into a receive buffer of the broker's [memory](../components/broker/memory.md), exactly as long as its fixed header says.
The delivery holds that buffer and borrows its topic and payload from it, so the payload is not copied between the socket and the handler.
The headers of the packet are parsed into the delivery, and the buffer goes back to the broker when the handler settles the delivery or drops it.
A packet that reaches several subscriptions is copied once into a receive buffer for each subscription beyond the first.

A publish is encoded into one of two send buffers, and the runner writes it.
The publisher lends the bytes, and `Ok` means the packet went out into the session.
An acknowledgement is a command in a queue of fixed size, and the runner sends the packet.

Nothing is allocated, at startup or per message: every buffer is a part of the `MqttState` the board declares, and its size is `(MESSAGES + 2) * PACKET` bytes plus a small part for each subscription.

## The heap variant

The client, rust-mqtt 0.6, gives each field it receives a heap buffer of its own, so a delivery takes one buffer for each field: the topic, the payload and each string property.
The buffers move into the message without a copy, and they are freed when the message is dropped.

A publish copies the bytes of the message into a staging slot.
The buffers of a slot keep their capacity, so a publish allocates nothing once the slots have held the largest message the service sends.
The publisher lends the bytes, and `Ok` means the packet went out into the session.

An acknowledgement is a command in a queue of fixed size.
It allocates nothing, and the runner sends the packet.

Every queue is fixed at startup: 32 commands, 8 staging slots and 16 deliveries for each subscription.

On compio the network adds one copy for each read: `io_uring` reads into a buffer it owns, and the client reads out of it.
