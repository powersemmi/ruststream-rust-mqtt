---
sidebar_position: 2
title: Первый сервис на хосте compio
description: "На хосте compio брокер это вариант с кучей, а MqttBroker::compio запускает runner на ядре, которое его подключает, так что сервис - обработчик и приложение."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Первый сервис на хосте compio

На хосте compio сервис состоит из обработчика и приложения.
`MqttBroker::compio` запускает runner на ядре, которое подключает брокер, поверх TCP-потока compio, так что вручную ничего запускать не нужно.

## Вариант с кучей

У хоста есть куча, и вариант крейта с кучей её использует: это фича `alloc`, которую включает `compio`.
Брокер работает поверх клиента [`rust-mqtt`](https://docs.rs/rust-mqtt), настройки хранят строки во владении, а обработчики декодируют полезную нагрузку кодеком фреймворка, например `json`.
Брокеру не нужен `MqttState`, потому что клиент сам выделяет то, что принимает.

Устройство с кучей берёт тот же вариант с фичами `alloc` и `embassy`.
Оно создаёт брокер через `MqttBroker::new(config)` и запускает runner задачей, как в [первом сервисе на устройстве](./getting-started-device.md), а обработчики используют типы serde.
Устройство без кучи берёт [вариант для устройства](./getting-started-device.md).

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
Подписка, подтверждение и публикация работают так же, как на устройстве: различаются только сеть, рантайм и память.
[Тестирование сервиса на compio](../components/testing/compio.md) показывает тест этого приложения.

## Устройство с кучей

Runner это задача платы, а брокер принимает одну только конфигурацию.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust reference="examples/mqtt_device.rs#runner"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust reference="examples/manual/mqtt_device.rs#runner"
```

</TabItem>
</Tabs>
