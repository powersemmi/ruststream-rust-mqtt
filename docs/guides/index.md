---
slug: /
sidebar_position: 0
description: "Guides to building MQTT 5 services with ruststream-rust-mqtt, the MQTT broker of RustStream over the rust-mqtt client."
---

# Guides

`ruststream-rust-mqtt` is the MQTT 5 broker of [RustStream](https://powersemmi.github.io/ruststream/latest/)
over the [rust-mqtt](https://docs.rs/rust-mqtt) client, for the embassy and compio runtimes.

A device on embassy and a host on compio run the same handlers.
Start with the runtime you build for: [getting started on a device](./getting-started-device.md) or [getting started on a compio host](./getting-started-compio.md).
[Testing a device service](./testing.md) shows how to run either on the host before it reaches a board.
