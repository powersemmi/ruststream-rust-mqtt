---
slug: /
sidebar_position: 0
description: "使用 RustStream 的 MQTT 5 Broker ruststream-rust-mqtt 构建服务的指南：面向没有堆的设备，以及 compio 主机。"
---

# 指南

`ruststream-rust-mqtt` 是 [RustStream](https://powersemmi.github.io/ruststream/latest/zh/) 的 MQTT 5 Broker，面向 embassy 和 compio 运行时。

本 crate 有两个变体。
设备变体不需要堆：开发板把 Broker 的内存声明为 `static`，投递从其中的接收缓冲区借用载荷。
堆变体（`alloc` 特性，`compio` 会隐含开启）运行在 [rust-mqtt](https://docs.rs/rust-mqtt) 客户端之上，适合 compio 主机，或者带有堆的设备。

请从你构建所用的变体开始：[在设备上入门](./getting-started-device.md)或[在 compio 主机上入门](./getting-started-compio.md)。
[测试设备服务](./testing.md)展示如何在服务到达开发板之前，在主机上运行二者。
