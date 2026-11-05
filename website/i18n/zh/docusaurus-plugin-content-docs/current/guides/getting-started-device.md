---
sidebar_position: 1
title: 在设备上入门
description: "设备服务没有堆：处理器从 Broker 的接收缓冲区借用载荷，Broker 的内存是 static，开发板的 main 启动 runner 和应用。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 在设备上入门

设备服务没有堆。
Broker 的内存是开发板声明的 `static`，投递从这块内存的接收缓冲区借用载荷，回复则编码进其中的发送缓冲区。
本指南构建一块阀门开发板，它执行发布到 `valves/v1/set` 的命令，并报告所达到的状态。

服务由三部分组成：处理器和应用，持有 MQTT 连接的 runner 任务，以及启动二者的开发板 `main`。

## 所需的 crate

```bash
cargo add ruststream --no-default-features --features embassy,macros
cargo add ruststream-rust-mqtt --features embassy
cargo add embassy-executor embassy-net static_cell
```

Broker crate 的 `embassy` 特性提供 `EmbassyNetwork`，即基于 embassy-net 栈的 runner 网络。
Broker crate 在 `alloc` 特性开启之前是设备变体，框架随之而定：`ruststream` 中需要堆的特性（例如 `json`）会开启框架的 `alloc`，此时 Broker crate 也要开启自己的 `alloc` 特性。
带有堆的设备以同样的方式使用[堆变体](./getting-started-compio.md)。

## 处理器和应用

处理器接收 `Command` 并返回所达到的 `State`。
`Command` 是以到达时的字节表示的载荷：它从 Broker 的接收缓冲区借用这些字节，所以读取命令不复制任何内容。
`State` 是发出的字节，回复被直接编码进发送缓冲区。
`#[ruststream::app(runtime = embassy)]` 把应用函数变成一个 embassy 任务 `app::task`。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust reference="examples/device/device_service.rs#device"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust reference="examples/manual/device_service.rs#device"
```

</TabItem>
</Tabs>

## 内存、runner 和开发板

[`MqttState`](../components/broker/memory.md) 是 Broker 拥有的全部内存：投递借用的接收缓冲区、发送缓冲区和订阅表。
开发板把它声明为 `static`，并一次性交给 `MqttBroker::new`，后者返回 Broker 及其 runner。
Runner 持有连接：应用连接 Broker 时由它打开连接，所有读、写和确认都经过它。
因此开发板启动两个任务：基于网络栈的 runner，以及带着 Broker 的应用。
套接字缓冲区位于 runner 的任务中，大小由开发板决定。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust reference="examples/device/device_service.rs#board"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust reference="examples/manual/device_service.rs#board"
```

</TabItem>
</Tabs>

`MqttState` 的默认值是四条投递，每条最多 1 KiB，以及八个订阅。
地址、客户端 ID、凭据和会话是 [`MqttConfig`](../components/broker/configuration.md) 的设置，它是固件的 `const`。
在固件到达开发板之前，请先[在主机上测试服务](./testing.md)：测试声明自己的内存，不需要 runner。
