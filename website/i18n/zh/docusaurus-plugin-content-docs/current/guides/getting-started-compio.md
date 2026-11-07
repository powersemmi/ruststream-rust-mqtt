---
sidebar_position: 2
title: 在 compio 主机上入门
description: "在 compio 主机上，Broker 是堆变体，MqttBroker::compio 在连接它的核心上启动 runner，因此服务由处理器和应用组成。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 在 compio 主机上入门

在 compio 主机上，服务只需要处理器和应用。
`MqttBroker::compio` 在连接 Broker 的核心上启动 runner，使用 compio 的 TCP 流，因此不需要手动启动任何任务。

## 堆变体

主机有堆，本 crate 的堆变体会使用它：即 `alloc` 特性，`compio` 会开启它。
Broker 运行在 [`rust-mqtt`](https://docs.rs/rust-mqtt) 客户端之上，设置持有自有字符串，处理器用框架的编解码器解码载荷，`json` 是其中之一。
Broker 不需要 `MqttState`，因为客户端自己分配收到的内容。

带有堆的设备同样使用这个变体，开启 `alloc` 和 `embassy` 即可。
它用 `MqttBroker::new(config)` 构建 Broker，并把 runner 作为任务启动，方式与[在设备上入门](./getting-started-device.md)相同，处理器使用 serde 类型。
没有堆的设备使用[设备变体](./getting-started-device.md)。

## 所需的 crate

```bash
cargo add ruststream --no-default-features --features compio,macros,json
cargo add ruststream-rust-mqtt --features compio
```

## 服务

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust reference="examples/mqtt_network_compio.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust reference="examples/manual/mqtt_network_compio.rs#app"
```

</TabItem>
</Tabs>

`#[ruststream::app(runtime = compio, cores = 1)]` 生成二进制程序的 `main`，它理解命令 `run`。
`cargo run -- run` 针对配置中指定的服务器启动服务。
订阅、确认和发布的工作方式与设备上相同，只有网络、运行时和内存不同。
[测试 compio 服务](../components/testing/compio.md)展示这个应用的测试。

## 带有堆的设备

Runner 是开发板的一个任务，Broker 只接收配置。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust reference="examples/mqtt_device.rs#runner"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust reference="examples/manual/mqtt_device.rs#runner"
```

</TabItem>
</Tabs>
