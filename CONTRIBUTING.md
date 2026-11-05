# Contributing to ruststream-rust-mqtt

`ruststream-rust-mqtt` is the MQTT 5 broker crate of RustStream over the `rust-mqtt` client. The
framework's core crate lives in [`powersemmi/ruststream`](https://github.com/powersemmi/ruststream),
and each of the other broker crates in a repository of its own. This page covers the environment
and the checks a change passes before review.

## Environment

- **Rust** through rustup. `rust-toolchain.toml` selects stable with rustfmt and clippy. The
  minimum supported version is 1.95, the `rust-version` in `Cargo.toml`.
- **just**, which runs every recipe below.
- Per task:

| Task | Tool | Install |
| --- | --- | --- |
| `just deny` | cargo-deny | `cargo install cargo-deny --locked` |
| `just typo`, `just zizmor` | uv | the uv documentation |
| the documentation site | Node.js 22 and Python 3 | `scripts/site_rustdoc.sh` (the rustdoc the API links resolve against), then `npm ci` in `website/` and `npm start` (one language) or `npm run build` (all three, strict); `npm run test` and `npm run typecheck` check the site's own code |

## Checking a change

```bash
just check          # rustfmt, clippy, cargo check with all features and with none, rustdoc
just test           # the tests and the doc tests
just ci             # check and test, plus codespell, cargo deny and zizmor
```

## Pull requests

- One logical change per pull request.
- Open it as a draft. CI runs when it is marked ready for review.
- A pull request merges as one squashed commit, after the `CI result` check and one approving
  review. Commits are signed.
- Documentation changes with the code. An item's rustdoc says what it is and does. The site in
  `docs/` holds the guides, one page per component and the architecture, in English, Russian and
  Chinese (`website/i18n/`), and an edit reaches all three. `scripts/lint_doc_fences.py`,
  `scripts/check_doc_parity.py` and `scripts/check_doc_i18n.py` check the fences and the
  translations.
