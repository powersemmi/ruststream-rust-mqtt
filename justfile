set shell := ["bash", "-eu", "-o", "pipefail", "-c"]
set dotenv-load := false

export PATH := env("HOME") + "/.cargo/bin:" + env("HOME") + "/.local/bin:" + env("PATH")

default: check

check:
    cargo fmt --all -- --check
    # The benchmark package is left out of the all-features legs on purpose: it is built with the
    # feature set a service ships, and the framework's harness feature is a compile error in it.
    # Its own leg follows each of them.
    cargo clippy --workspace --exclude ruststream-rust-mqtt-bench --all-targets --all-features -- -D warnings
    cargo clippy -p ruststream-rust-mqtt-bench --all-targets -- -D warnings
    # The device variant builds without `alloc`, which the crate's own dev-dependencies turn on, so
    # its sources are linted and tested from a workspace of their own.
    cargo fmt --manifest-path checks/noalloc/Cargo.toml -- --check
    cargo clippy --manifest-path checks/noalloc/Cargo.toml --all-targets -- -D warnings
    cargo check --workspace --exclude ruststream-rust-mqtt-bench --all-targets --all-features
    cargo check -p ruststream-rust-mqtt-bench --all-targets
    cargo check --workspace --no-default-features
    # CI denies rustdoc warnings, so a broken intra-doc link fails the build. Running it here is
    # what keeps that a local finding rather than a red pull request.
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
    RUSTDOCFLAGS="-D warnings" cargo doc -p ruststream-rust-mqtt --features embassy,testing --no-deps

test:
    cargo test --workspace --all-features --tests
    cargo test --workspace --all-features --doc
    cargo test --manifest-path checks/noalloc/Cargo.toml

# The live stand: mosquitto with an anonymous and an authenticated listener.
brokers-up:
    docker compose -p ruststream-rust-mqtt -f docker-compose.test.yml up -d --wait

brokers-down:
    docker compose -p ruststream-rust-mqtt -f docker-compose.test.yml down -v

# The live suites against the stand. This recipe starts the stand, so a gated test that skips
# itself here is a fault, not a developer without a broker.
test-brokers: brokers-up
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just brokers-down' EXIT
    MQTT_TEST_URL=mqtt://127.0.0.1:1893 \
    MQTT_TEST_AUTH_URL=mqtt://127.0.0.1:1894 \
    MQTT_TEST_AUTH_PASSWORD=s3cret \
    RUSTSTREAM_REQUIRE_LIVE=1 \
        cargo test --workspace --all-features --tests -- --test-threads=1
    MQTT_TEST_URL=mqtt://127.0.0.1:1893 \
    MQTT_TEST_AUTH_URL=mqtt://127.0.0.1:1894 \
    MQTT_TEST_AUTH_PASSWORD=s3cret \
    RUSTSTREAM_REQUIRE_LIVE=1 \
        cargo test --manifest-path checks/noalloc/Cargo.toml --tests -- --test-threads=1

fmt:
    cargo fmt --all

security: deny zizmor

# Dependency-graph checks (advisories, licenses, duplicates, sources).
# Needs cargo-deny: cargo install cargo-deny --locked
deny:
    cargo deny check

zizmor:
    uvx zizmor .github/workflows

typo:
    uvx codespell

ci: check test typo security

# The firmware binaries of the two variants, with their sizes. The heap variant is linked for a
# Cortex-M4F with a global allocator and 256 KiB of RAM. The device variant has no allocator and
# no `alloc` anywhere in its graph, so it fails to link while a dependency needs a heap; it is
# linked for a Cortex-M4F and for a Cortex-M0+, which has no compare-and-swap. The locks are
# committed and the builds take them as they are.
check-firmware:
    #!/usr/bin/env bash
    set -euo pipefail
    RUSTFLAGS="" cargo build --locked --release --manifest-path checks/firmware/Cargo.toml \
        --target thumbv7em-none-eabihf
    size checks/firmware/target/thumbv7em-none-eabihf/release/ruststream-rust-mqtt-firmware
    for target in thumbv7em-none-eabihf thumbv6m-none-eabi; do
        RUSTFLAGS="" cargo build --locked --release \
            --manifest-path checks/firmware-noalloc/Cargo.toml --target "$target"
        size "checks/firmware-noalloc/target/$target/release/ruststream-rust-mqtt-firmware-noalloc"
    done

# What a message costs on the service's core, counted under valgrind: instructions through
# callgrind and allocations through DHAT, for a consuming service and a replying one on the stand's
# mosquitto, built the way the broker ships (fat LTO, never the `testing` feature). The count of
# deliveries per run is a leading number (default 500, the sibling broker crate's count) and is
# read at build time. RUSTFLAGS is cleared because valgrind aborts on the instructions a recent
# CPU advertises. Needs valgrind. The runner of the benchmarks has to be the release of the
# library the lock file pins; the recipe installs it into `target/gungraun-runner` on the first
# run and after the library moves. Take the machine lock of the benchmarks before running it.
[positional-arguments]
bench-code *ARGS: brokers-up
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just brokers-down' EXIT
    messages=500
    if [[ "${1:-}" =~ ^[0-9]+$ ]]; then
        messages="$1"
        shift
    fi
    mkdir -p target
    version="$(cargo pkgid gungraun)"
    version="${version##*@}"
    runner="$PWD/target/gungraun-runner"
    installed="$("$runner/bin/gungraun-runner" --version 2> /dev/null || true)"
    if [ "$installed" != "gungraun-runner $version" ]; then
        cargo install --locked --root "$runner" gungraun-runner --version "=$version"
    fi
    unset GUNGRAUN_RUNNER
    export PATH="$runner/bin:$PATH" RUSTFLAGS="" RUSTSTREAM_BENCH_MESSAGES="$messages" \
        GUNGRAUN_HOME="$PWD/target/gungraun/$messages" MQTT_TEST_URL=mqtt://127.0.0.1:1893
    cargo bench -p ruststream-rust-mqtt-bench --no-run
    cargo bench -p ruststream-rust-mqtt-bench --no-fail-fast \
        -- --output-format=json "$@" > target/bench-code.json
    python3 scripts/bench_code_table.py --messages "$messages" target/bench-code.json

# What a message costs on the device variant, counted as `bench-code` counts the heap variant's:
# the same scenarios (consume at QoS 1, reply), the same payload and message count, callgrind and
# DHAT, built without `alloc` in the workspace of `checks/bench-noalloc` with fat LTO and never the
# `testing` feature. The stand has to be up already (`just brokers-up`, mosquitto on 1893). Take the
# machine lock of the benchmarks before running it.
[positional-arguments]
bench-code-noalloc *ARGS:
    #!/usr/bin/env bash
    set -euo pipefail
    messages=500
    if [[ "${1:-}" =~ ^[0-9]+$ ]]; then
        messages="$1"
        shift
    fi
    manifest="$PWD/checks/bench-noalloc/Cargo.toml"
    version="$(cargo pkgid --manifest-path "$manifest" gungraun)"
    version="${version##*@}"
    runner="$PWD/target/gungraun-runner"
    mkdir -p target
    installed="$("$runner/bin/gungraun-runner" --version 2> /dev/null || true)"
    if [ "$installed" != "gungraun-runner $version" ]; then
        cargo install --locked --root "$runner" gungraun-runner --version "=$version"
    fi
    unset GUNGRAUN_RUNNER
    export PATH="$runner/bin:$PATH" RUSTFLAGS="" RUSTSTREAM_BENCH_MESSAGES="$messages" \
        GUNGRAUN_HOME="$PWD/target/gungraun-noalloc/$messages" \
        MQTT_TEST_URL="${MQTT_TEST_URL:-mqtt://127.0.0.1:1893}"
    cargo bench --manifest-path "$manifest" --no-run
    cargo bench --manifest-path "$manifest" --no-fail-fast \
        -- --output-format=json "$@" > target/bench-code-noalloc.json
    python3 scripts/bench_code_table.py --messages "$messages" target/bench-code-noalloc.json
