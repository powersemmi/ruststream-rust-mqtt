//! Reads the variant of the build from the contract crate: `ruststream_alloc` is on when
//! `ruststream-core` was built with `alloc`, whichever crate asked for it, so the crate can refuse a
//! build whose framework is the heap variant while the crate itself is the device one.

use std::env;

fn main() {
    println!("cargo::rustc-check-cfg=cfg(ruststream_alloc)");
    println!("cargo::rerun-if-env-changed=DEP_RUSTSTREAM_CORE_ALLOC");
    if env::var_os("DEP_RUSTSTREAM_CORE_ALLOC").is_some() {
        println!("cargo::rustc-cfg=ruststream_alloc");
    }
}
