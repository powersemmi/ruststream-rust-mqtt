#!/usr/bin/env bash
# Builds the rustdoc the site's API links resolve against, into target/doc: this crate with the
# features docs.rs renders it with, and the core release the lock file pins with every feature,
# as docs.rs renders the core. A page may link items of either crate.
set -euo pipefail
cd "$(dirname "$0")/.."
metadata="$(cargo metadata --locked --format-version 1)"
# docs.rs renders this crate with the features its metadata names: the device variant, without
# `alloc`.
crate_features="$(python3 -c '
import json, sys
packages = {p["name"]: p for p in json.load(sys.stdin)["packages"]}
mqtt = packages["ruststream-rust-mqtt"]
docs_rs = mqtt["metadata"]["docs"]["rs"]
print(",".join(
    [name for name in mqtt["features"] if name != "default"]
    if docs_rs.get("all-features")
    else docs_rs.get("features", [])
))
' <<< "$metadata")"
# docs.rs renders the core with every feature, which includes `alloc`: that build runs from a
# manifest of its own under target/, at the version the lock file pins, so the core's heap variant
# never reaches this crate's device build, and its pages are copied beside this crate's.
core_version="$(python3 -c '
import json, sys
packages = {p["name"]: p for p in json.load(sys.stdin)["packages"]}
core = packages.get("ruststream")
print(core["version"] if core else "")
' <<< "$metadata")"
cargo doc --locked --no-deps -p ruststream-rust-mqtt --features "$crate_features"
if [ -n "$core_version" ]; then
  core_dir="target/core-rustdoc"
  mkdir -p "$core_dir/src"
  cat > "$core_dir/Cargo.toml" <<MANIFEST
[package]
name = "core-rustdoc"
version = "0.0.0"
edition = "2024"
publish = false

[workspace]

[dependencies]
ruststream = { version = "=$core_version", features = [$(python3 -c '
import json, sys
packages = {p["name"]: p for p in json.load(sys.stdin)["packages"]}
print(", ".join(f"\"{name}\"" for name in packages["ruststream"]["features"] if name != "default"))
' <<< "$metadata")] }
MANIFEST
  : > "$core_dir/src/lib.rs"
  cargo doc --no-deps -p ruststream --manifest-path "$core_dir/Cargo.toml"
  core_doc="$(cargo metadata --format-version 1 --no-deps --manifest-path "$core_dir/Cargo.toml" \
    | python3 -c 'import json, sys; print(json.load(sys.stdin)["target_directory"])')/doc"
  rm -rf target/doc/ruststream
  cp -r "$core_doc/ruststream" target/doc/ruststream
fi
