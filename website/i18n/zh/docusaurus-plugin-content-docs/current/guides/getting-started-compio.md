---
sidebar_position: 2
title: 在 compio 主机上入门
description: "在 compio 主机上，MqttBroker::compio 在连接 Broker 的核心上启动 runner，因此服务只需要处理器和应用。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 在 compio 主机上入门

在 compio 主机上，服务只需要处理器和应用。
`MqttBroker::compio` 在连接 Broker 的核心上启动 runner，使用 compio 的 TCP 流，因此不需要手动启动任何任务。

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
订阅、确认和发布的工作方式与设备上相同，只有网络和运行时不同。
[测试 compio 服务](../components/testing/compio.md)展示这个应用的测试。
