---
sidebar_position: 3
title: Тестирование сервиса для устройства
description: "Сервис для устройства тестируют на хосте через embedded TestApp, сервис на compio через compio TestApp; оба подключают брокер внутри процесса."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Тестирование сервиса для устройства

Тест запускает боевое приложение без изменений на хосте.
Embedded `TestApp` запускает сервис для устройства, а compio `TestApp` запускает сервис на compio; оба подключают брокер внутри процесса, поэтому сервер и runner не нужны.

## Крейт

```bash
cargo add --dev ruststream-rust-mqtt --features testing
```

Тестам нужна фича `testing` рядом с фичей рантайма сервиса: `embassy` для устройства, `compio` для хоста.

## Тест

Тест объявляет память брокера как `static`, как это делает плата, и запускает приложение с брокером, созданным через `MqttBroker::new` над ней.
Он публикует в брокер сообщение так, как это сделал бы другой клиент сервера, и проверяет, что получил обработчик и как завершилась доставка.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust reference="examples/device/device_testing.rs#test"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust reference="examples/manual/device_testing.rs#test"
```

</TabItem>
</Tabs>

## Время

У теста собственные часы.
Копия `retry_after` возвращается, когда `advance` переводит часы за её задержку.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust reference="examples/device/device_testing.rs#advance"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust reference="examples/manual/device_testing.rs#advance"
```

</TabItem>
</Tabs>

## На compio

Сервис на compio это вариант с кучей, и его тесту собственная память не нужна.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust reference="examples/mqtt_testing_compio.rs#test"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust reference="examples/manual/mqtt_testing_compio.rs#test"
```

</TabItem>
</Tabs>

Внутрипроцессный транспорт оставляет серверу постоянную сессию и рукопожатия QoS 1 и 2; их проверяют на [живом стенде](../components/testing/live.md).
