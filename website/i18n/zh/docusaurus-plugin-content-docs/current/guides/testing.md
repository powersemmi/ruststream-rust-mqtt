---
sidebar_position: 3
title: 测试设备服务
description: "设备服务在主机上用 embedded TestApp 测试，compio 服务用 compio TestApp 测试；二者都在进程内连接 Broker。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 测试设备服务

测试在主机上原样运行生产环境的应用。
Embedded `TestApp` 运行设备服务，compio `TestApp` 运行 compio 服务；二者都在进程内连接 Broker，因此不需要服务器，也不需要 runner。

## 所需的 crate

```bash
cargo add --dev ruststream-rust-mqtt --features testing
```

测试需要 `testing` 特性，以及服务的运行时特性：设备用 `embassy`，主机用 `compio`。

## 测试

测试像开发板一样，把 Broker 的内存声明为 `static`，并用 `MqttBroker::new` 在其上构建 Broker，再启动应用。
测试像服务器的另一个客户端那样向它发布消息，然后断言处理器收到了什么，以及这次投递如何结算。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust reference="examples/device/device_testing.rs#test"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust reference="examples/manual/device_testing.rs#test"
```

</TabItem>
</Tabs>

## 时间

测试拥有自己的时钟。
`advance` 把时钟推过延迟之后，`retry_after` 的副本就会回来。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust reference="examples/device/device_testing.rs#advance"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust reference="examples/manual/device_testing.rs#advance"
```

</TabItem>
</Tabs>

## 在 compio 上

compio 服务是堆变体，它的测试不需要自己的内存。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust reference="examples/mqtt_testing_compio.rs#test"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust reference="examples/manual/mqtt_testing_compio.rs#test"
```

</TabItem>
</Tabs>

进程内传输把持久会话以及 QoS 1 和 2 的握手留给服务器，这些在[真实测试环境](../components/testing/live.md)中测试。
