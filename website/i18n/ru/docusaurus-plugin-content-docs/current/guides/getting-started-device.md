---
sidebar_position: 1
title: Первый сервис на устройстве
description: "Сервис для устройства состоит из трёх частей: обработчик и приложение, задача runner поверх сетевого стека платы и main платы, который запускает обе."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Первый сервис на устройстве

Сервис для устройства состоит из трёх частей: обработчика с приложением, задачи runner, которая владеет соединением MQTT, и `main` платы, который запускает обе.
В этом руководстве мы соберём плату с датчиком, которая сохраняет показания из темы `sensors/temp`.

## Крейты

```bash
cargo add ruststream --no-default-features --features embassy,macros,json
cargo add ruststream-rust-mqtt --features embassy
cargo add embassy-executor embassy-net
```

Фича `embassy` крейта брокера добавляет `EmbassyNetwork`, сеть runner поверх стека embassy-net.
Фича `json` выбирает кодек обработчиков по умолчанию.

## Обработчик и приложение

Обработчик принимает `Reading` и возвращает результат; `ack` отправляет подтверждение доставки.
Функция приложения получает брокер от платы, а `#[ruststream::app(runtime = embassy)]` превращает её в задачу embassy, `app::task`.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust reference="examples/mqtt_device.rs#device"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust reference="examples/manual/mqtt_device.rs#device"
```

</TabItem>
</Tabs>

## Runner и плата

`MqttBroker::new` возвращает брокер вместе с его runner.
Runner владеет соединением: он открывает его, когда приложение подключает брокер, и через него проходят все чтения, записи и подтверждения.
Поэтому плата запускает две задачи: runner поверх сетевого стека и приложение с брокером.
Буферы сокета живут в задаче runner, а их размер выбирает плата.

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

Адрес, идентификатор клиента, учётные данные и постоянная сессия задаются в [`MqttConfig`](../components/broker/configuration.md).
Прежде чем прошивка попадёт на плату, [проверьте сервис на хосте](./testing.md).
