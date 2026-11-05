---
sidebar_position: 0
description: "ruststream-rust-mqtt 在 MQTT Broker 与 RustStream 运行时之间的位置。"
---

# 架构

本节说明 `ruststream-rust-mqtt` 位于 MQTT Broker 与 RustStream 运行时之间的哪个位置。

[Runner 与句柄](./runner.md)展示连接如何由一个任务持有，而处理器、发布者和确认与它通信。
[一次投递和一次发布的开销](./cost.md)说明它们各自占用设备多少资源。
