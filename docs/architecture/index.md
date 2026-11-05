---
sidebar_position: 0
description: "Where ruststream-rust-mqtt sits between the MQTT broker and the RustStream runtime."
---

# Architecture

This section draws where `ruststream-rust-mqtt` sits between the MQTT broker and the RustStream
runtime.

[The runner and the handles](./runner.md) shows how a connection is owned by one task while the
handlers, the publishers and the acknowledgements talk to it.
[What a delivery and a publish cost](./cost.md) states what each of them takes from the device.
