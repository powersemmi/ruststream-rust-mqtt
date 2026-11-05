set shell := ["bash", "-eu", "-o", "pipefail", "-c"]
set dotenv-load := false

export PATH := env("HOME") + "/.cargo/bin:" + env("HOME") + "/.local/bin:" + env("PATH")

default: check

check:
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets --all-features -- -D warnings
    cargo check --workspace --all-targets --all-features
    cargo check --workspace --no-default-features
    # CI denies rustdoc warnings, so a broken intra-doc link fails the build. Running it here is
    # what keeps that a local finding rather than a red pull request.
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps

test:
    cargo test --workspace --all-features --tests
    cargo test --workspace --all-features --doc

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
