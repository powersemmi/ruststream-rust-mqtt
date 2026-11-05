---
slug: /
sidebar_position: 0
description: "使用 MQTT 5 broker ruststream-rust-mqtt 构建服务的指南：RustStream 在 rust-mqtt 客户端之上的 MQTT broker。"
---

# 指南

`ruststream-rust-mqtt` 是 [RustStream](https://powersemmi.github.io/ruststream/latest/zh/) 的 MQTT 5 Broker，
构建在 [rust-mqtt](https://docs.rs/rust-mqtt) 客户端之上，面向 embassy 和 compio 运行时。

embassy 上的设备和 compio 上的主机运行同样的处理器。
请从你构建所用的运行时开始：[在设备上入门](./getting-started-device.md)或[在 compio 主机上入门](./getting-started-compio.md)。
[测试设备服务](./testing.md)展示如何在服务到达开发板之前，在主机上运行二者。
