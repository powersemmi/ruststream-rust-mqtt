---
sidebar_position: 1
title: Первый сервис на устройстве
description: "У сервиса для устройства нет кучи: обработчик заимствует полезную нагрузку из буфера приёма брокера, память брокера это static, а main платы запускает runner и приложение."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Первый сервис на устройстве

У сервиса для устройства нет кучи.
Память брокера это `static`, которое объявляет плата; доставка заимствует полезную нагрузку из буфера приёма этой памяти, а ответ кодируется в её буфер отправки.
В этом руководстве мы соберём плату с клапанами, которая применяет команды из темы `valves/v1/set` и сообщает достигнутое состояние.

Сервис состоит из трёх частей: обработчика с приложением, задачи runner, которая владеет соединением MQTT, и `main` платы, который запускает обе.

## Крейты

```bash
cargo add ruststream --no-default-features --features embassy,macros
cargo add ruststream-rust-mqtt --features embassy
cargo add embassy-executor embassy-net static_cell
```

Фича `embassy` крейта брокера добавляет `EmbassyNetwork`, сеть runner поверх стека embassy-net.
Пока фича `alloc` крейта брокера выключена, он остаётся вариантом для устройства, и фреймворк следует за ним: фича `ruststream`, которой нужна куча, например `json`, включает `alloc` фреймворка, и тогда крейту брокера нужна и собственная фича `alloc`.
Устройство с кучей так же берёт [вариант с кучей](./getting-started-compio.md).

## Обработчик и приложение

Обработчик принимает `Command` и возвращает достигнутое `State`.
`Command` это полезная нагрузка в виде пришедших байтов: она заимствует их из буфера приёма брокера, поэтому чтение команды ничего не копирует.
`State` это уходящие байты, и ответ кодируется прямо в буфер отправки.
`#[ruststream::app(runtime = embassy)]` превращает функцию приложения в задачу embassy, `app::task`.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust reference="examples/device/device_service.rs#device"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust reference="examples/manual/device_service.rs#device"
```

</TabItem>
</Tabs>

## Память, runner и плата

[`MqttState`](../components/broker/memory.md) это вся память, которой владеет брокер: буферы приёма, из которых заимствуют доставки, буферы отправки и таблица, где хранятся подписки.
Плата объявляет её как `static` и один раз передаёт в `MqttBroker::new`, который возвращает брокер вместе с его runner.
Runner владеет соединением: он открывает его, когда приложение подключает брокер, и через него проходят все чтения, записи и подтверждения.
Поэтому плата запускает две задачи: runner поверх сетевого стека и приложение с брокером.
Буферы сокета живут в задаче runner, а их размер выбирает плата.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust reference="examples/device/device_service.rs#board"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust reference="examples/manual/device_service.rs#board"
```

</TabItem>
</Tabs>

По умолчанию `MqttState` рассчитан на четыре доставки до 1 КиБ; подписки в нём хранятся в таблице на восемь записей.
Адрес, идентификатор клиента, учётные данные и сессия задаются в [`MqttConfig`](../components/broker/configuration.md), `const` прошивки.
Прежде чем прошивка попадёт на плату, [проверьте сервис на хосте](./testing.md): тест объявляет собственную память, и runner ему не нужен.
