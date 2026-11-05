---
sidebar_position: 2
title: What a delivery and a publish cost
description: "A delivery takes one heap buffer for each field the client received; a publish takes a copy of its bytes into a staging slot."
---

# What a delivery and a publish cost

The client, rust-mqtt 0.6, gives each field it receives a heap buffer of its own, so a delivery takes one buffer for each field: the topic, the payload and each string property.
The crate builds the `alloc` variant of the framework for that reason, and the heap is the device's.
The buffers move into the message without a copy, and they are freed when the message is dropped.

A publish copies the bytes of the message into a staging slot.
The buffers of a slot keep their capacity, so a publish allocates nothing once the slots have held the largest message the service sends.
The publisher lends the bytes, and `Ok` means the packet went out into the session.

An acknowledgement is a command in a queue of fixed size.
It allocates nothing, and the runner sends the packet.

Every queue is fixed at startup: 32 commands, 8 staging slots and 16 deliveries for each subscription.

On compio the network adds one copy for each read: `io_uring` reads into a buffer it owns, and the client reads out of it.
