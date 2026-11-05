---
sidebar_position: 2
title: Первый сервис на хосте compio
description: "На хосте compio MqttBroker::compio запускает runner на ядре, которое подключает брокер, поэтому сервис состоит из обработчика и приложения."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Первый сервис на хосте compio

На хосте compio сервис состоит из обработчика и приложения.
`MqttBroker::compio` запускает runner на ядре, которое подключает брокер, поверх TCP-потока compio, так что вручную ничего запускать не нужно.

## Крейты

```bash
cargo add ruststream --no-default-features --features compio,macros,json
cargo add ruststream-rust-mqtt --features compio
```

## Сервис

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust reference="examples/mqtt_network_compio.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust reference="examples/manual/mqtt_network_compio.rs#app"
```

</TabItem>
</Tabs>

`#[ruststream::app(runtime = compio, cores = 1)]` пишет `main` бинарника, который понимает команду `run`.
`cargo run -- run` запускает сервис против сервера из конфигурации.
Подписка, подтверждение и публикация работают так же, как на устройстве: различаются только сеть и рантайм.
[Тестирование сервиса на compio](../components/testing/compio.md) показывает тест этого приложения.
