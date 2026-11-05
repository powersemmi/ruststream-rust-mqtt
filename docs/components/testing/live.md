---
sidebar_position: 2
title: The live stand
description: "A live test runs the service against a real server; the stand is mosquitto, started by just test-brokers."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# The live stand

A live test runs the service against a real server, for what the in-process transport leaves to the server: the connection, a persistent session and its redelivery, the handshakes behind QoS 1 and 2.

The stand is a mosquitto container with an anonymous listener and one that needs a password.
`just test-brokers` starts it, runs the live suites with `RUSTSTREAM_REQUIRE_LIVE` set, and stops it.
A live test reads the address of the stand from `MQTT_TEST_URL`; the variable `RUSTSTREAM_REQUIRE_LIVE` turns a missing stand into a failure, so a test that skipped itself is noticed.
The test below runs the service on compio and talks to it as a second client of the same server.

## Example

A job published to the server is answered by the service, and the answer comes back through a subscription of the test.

{/* single-form: a live test drives the broker's own connection, which has no macro form */}
```rust reference="examples/mqtt_testing_live.rs#test"
```
