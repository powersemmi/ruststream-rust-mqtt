---
slug: /
sidebar_position: 0
description: "Руководства по сервисам на MQTT 5 с ruststream-rust-mqtt, брокером MQTT для RustStream поверх клиента rust-mqtt."
---

# Руководства

Крейт `ruststream-rust-mqtt` реализует брокер MQTT 5 для [RustStream](https://powersemmi.github.io/ruststream/latest/ru/)
поверх клиента [rust-mqtt](https://docs.rs/rust-mqtt) для рантаймов embassy и compio.

Устройство на embassy и хост на compio запускают одни и те же обработчики.
Начните с рантайма, под который вы собираете сервис: [первый сервис на устройстве](./getting-started-device.md) или [первый сервис на хосте compio](./getting-started-compio.md).
[Тестирование сервиса для устройства](./testing.md) показывает, как запустить любой из них на хосте до того, как он попадёт на плату.
