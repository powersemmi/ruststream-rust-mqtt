---
sidebar_position: 1
title: 在设备上入门
description: "设备服务由三部分组成：处理器和应用，基于开发板网络栈的 runner 任务，以及启动二者的开发板 main。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 在设备上入门

设备服务由三部分组成：处理器和应用，持有 MQTT 连接的 runner 任务，以及启动二者的开发板 `main`。
本指南构建一块传感器开发板，它保存发布到主题 `sensors/temp` 的读数。

## 所需的 crate

```bash
cargo add ruststream --no-default-features --features embassy,macros,json
cargo add ruststream-rust-mqtt --features embassy
cargo add embassy-executor embassy-net
```

Broker crate 的 `embassy` 特性提供 `EmbassyNetwork`，即基于 embassy-net 栈的 runner 网络。
`json` 特性选择处理器默认的编解码器。

## 处理器和应用

处理器接收 `Reading` 并返回结果；`ack` 发送这次投递的确认。
应用函数从开发板接收 Broker，`#[ruststream::app(runtime = embassy)]` 把它变成一个 embassy 任务 `app::task`。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust reference="examples/mqtt_device.rs#device"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust reference="examples/manual/mqtt_device.rs#device"
```

</TabItem>
</Tabs>

## Runner 和开发板

`MqttBroker::new` 返回 Broker 以及它的 runner。
Runner 持有连接：应用连接 Broker 时由它打开连接，所有读、写和确认都经过它。
因此开发板启动两个任务：基于网络栈的 runner，以及带着 Broker 的应用。
套接字缓冲区位于 runner 的任务中，大小由开发板决定。

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

地址、客户端 ID、凭据和持久会话是 [`MqttConfig`](../components/broker/configuration.md) 的设置。
在固件到达开发板之前，请先[在主机上测试服务](./testing.md)。
