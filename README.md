<h1 align="center">ruststream-rust-mqtt</h1>

<p align="center">
  <i>The MQTT 5 broker of the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework over the <code>rust-mqtt</code> client, for the embassy and compio runtimes.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-rust-mqtt/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-rust-mqtt/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/ruststream-rust-mqtt"><img src="https://img.shields.io/crates/v/ruststream-rust-mqtt.svg" alt="crates.io"></a>
  <a href="https://docs.rs/ruststream-rust-mqtt"><img src="https://img.shields.io/docsrs/ruststream-rust-mqtt" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/MSRV-1.95-blue.svg" alt="MSRV 1.95">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
</p>

---

`ruststream-rust-mqtt` connects a RustStream service to an MQTT 5 broker over
[`rust-mqtt`](https://crates.io/crates/rust-mqtt). Handlers, routing, codecs and middleware come
from the framework; this crate is the transport.

The crate is `#![no_std]` and forbids `unsafe` code.

## License

Apache-2.0. See [LICENSE](LICENSE).
