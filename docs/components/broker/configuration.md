---
sidebar_position: 1
title: MqttConfig
description: "MqttConfig holds where the broker connects and as whom: the address, the client id, the keep-alive, the credentials and the session."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# MqttConfig

[`MqttConfig`](api:ruststream_rust_mqtt::MqttConfig) holds where the broker connects and as whom: the address, the client id, the keep-alive, the credentials and the session.

On a device every text is a `&'static str`, so the settings hold no buffer of their own and the firmware keeps them in a `const`.
The password is the device's own: the firmware reads it when it boots and adds it to the constant.
On the heap variant the texts are owned, and the service takes the password the deployment hands it when it starts.

Building a config performs no I/O.
The address is read when the broker connects, and a value the broker cannot connect to is reported there as [`MqttError::Connect`](api:ruststream_rust_mqtt::MqttError::Connect).
It is `host:port` or `mqtt://host[:port]`, with the port defaulting to 1883, and an IPv6 literal keeps its brackets.
Another scheme, a port that is not a number and an address with no host are refused.

The client id is the identity of the session on the server.
It starts empty, which asks the server to assign one, and a session that outlives a connection needs an id of its own.
The keep-alive is the interval, in whole seconds, after which the runner pings an idle connection; it defaults to 60 seconds and zero turns pings off.
[`credentials`](api:ruststream_rust_mqtt::MqttConfig::credentials) sets the user name and the password, and the `Debug` form of the config shows the user and never the password.

[`persistent_session`](api:ruststream_rust_mqtt::MqttConfig::persistent_session) resumes the session the server keeps for the client id instead of starting a clean one, and asks the server to keep it for the given time after the connection ends.
The server then redelivers the QoS 1 and QoS 2 messages that a previous connection left unacknowledged.

## Example

The server on the plant network, as the valve `valve-1`, with a session the server keeps for ten minutes.
A device keeps the address, the client id, the keep-alive and the session in a `const`, and adds the password it was provisioned with when it boots.

{/* single-form: the settings are a constant built by methods, which have no macro form */}
```rust reference="examples/device/device_config.rs#config"
```

### On the heap variant

The same settings on a host with a heap, with the password the deployment hands the service.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_config.rs#config"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_config.rs#config"
```

</TabItem>
</Tabs>
