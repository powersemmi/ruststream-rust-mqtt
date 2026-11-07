---
slug: /
sidebar_position: 0
description: "Guides to building MQTT 5 services with ruststream-rust-mqtt, the MQTT broker of RustStream, for a device without a heap and for a compio host."
---

# Guides

`ruststream-rust-mqtt` is the MQTT 5 broker of [RustStream](https://powersemmi.github.io/ruststream/latest/), for the embassy and compio runtimes.

The crate has two variants.
The device variant needs no heap: the board declares the broker's memory as a `static`, and a delivery borrows its payload from a receive buffer of it.
The heap variant (feature `alloc`, implied by `compio`) runs over the [rust-mqtt](https://docs.rs/rust-mqtt) client and fits a compio host, or a device that has a heap.

Start with the variant you build for: [getting started on a device](./getting-started-device.md) or [getting started on a compio host](./getting-started-compio.md).
[Testing a device service](./testing.md) shows how to run either on the host before it reaches a board.
