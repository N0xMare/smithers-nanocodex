# smithers-nanocodex

[![CI](https://github.com/N0xMare/smithers-nanocodex/actions/workflows/ci.yml/badge.svg)](https://github.com/N0xMare/smithers-nanocodex/actions/workflows/ci.yml)

`smithers-nanocodex` is a one-shot native bridge between Smithers and stock
[Nanocodex](https://crates.io/crates/nanocodex). Smithers starts a fresh bridge
process for each `generate()` call; the bridge runs one normal Nanocodex model
and tool loop, returns the final answer plus a resumable snapshot, shuts down,
and exits.

The bridge is intentionally small in scope. It is not a daemon, workflow
planner, process pool, session database, or second multi-agent system.

## What one turn looks like

```text
Smithers task
  -> external NanocodexAgent adapter
  -> Bubblewrap PID containment
  -> smithers-nanocodex serve
  -> stock Nanocodex 0.3.0 + native tools
  -> events + final message + session snapshot
  -> durable Smithers checkpoint
  -> shutdown and process exit
```

Each server process:

1. emits a capability-bearing `hello` record;
2. accepts one strict `turn.start` command over stdin;
3. optionally resumes an exact snapshot in the same canonical workspace;
4. emits bounded, sanitized progress records over protocol-only stdout;
5. accepts one correlated cancellation path;
6. emits one authoritative terminal record;
7. shuts down and exits.

The complete, normative contract is [docs/spec.md](docs/spec.md). Its JSON
Schemas and shape fixtures live under [docs/schema](docs/schema) and
[docs/fixtures](docs/fixtures); policy fingerprints have explicit golden
vectors.

## Baseline

- Bridge: `0.0.1`
- Wire protocol: `smithers.nanocodex/1`
- Nanocodex: exactly `0.3.0`
- Rust: `1.97`
- Checkpoints: `nanocodex.session-snapshot/1`, resume only
- Release target: x86_64 GNU/Linux, glibc 2.35 or newer (Ubuntu 22.04 baseline)

The immutable consumer pin for the qualified release artifact is
[`docs/releases/v0.0.1.json`](docs/releases/v0.0.1.json). Future Smithers
integration work should select the matching target, download that exact archive,
verify its recorded SHA-256 digest, and then validate the runtime capabilities
before launching it.

A conforming Smithers deployment additionally requires an executable Bubblewrap
and usable private PID namespaces. Releasing this bridge is an input to the
separate Smithers adapter work; it does not by itself claim that adapter has
shipped or completed integration qualification.

Protocol v1 keeps stock Code Mode and stock tool families enabled. It does not
offer MCP, subagents, steering, arbitrary JavaScript tools, custom provider
endpoints, Code Mode disablement, checkpoint relocation, or checkpoint fork.
Custom `instructions` replace Nanocodex's defaults completely; there is no
append mode.

## Build and inspect

```bash
cargo build --locked --release
./target/release/smithers-nanocodex --version
./target/release/smithers-nanocodex capabilities --json
```

Capability inspection is side-effect free: it performs no authentication,
network request, workspace mutation, or agent construction.

Start the JSONL worker with:

```bash
./target/release/smithers-nanocodex serve --protocol-version 1
```

Prompts and snapshots travel through JSONL stdin/stdout, never process
arguments. Stdout is reserved for protocol records.

## Authentication

Protocol supports two explicit modes:

- `api-key-env` names a bridge-process environment variable. The credential
  value is not sent in JSON. After loading it, the bridge blanks that exact
  variable for stock native tool subprocesses. This does not erase the bridge
  parent's original environment from same-UID host inspection such as Linux
  `/proc`.
- `chatgpt` uses an explicit or standard Nanocodex/Codex auth file. The bridge
  reads it through an enforced 1 MiB streaming cap and gives Nanocodex an
  owner-private staged copy, synchronizing refreshes back with bounded reads
  and atomic writes.

The transport is Nanocodex's WebSocket-preferred Responses transport with its
built-in sticky HTTPS fallback. Custom endpoints are rejected.

## Security boundary: important

The required conforming Smithers integration launches the bridge with Bubblewrap in a
private PID namespace. That makes descendant membership and cleanup
authoritative, including detached native tool processes.

It is not a filesystem, network, or general credential sandbox. The current
profile binds `/` read-write, preserves host networking, and exposes `/dev` per
host permissions. Managed ChatGPT auth files, unrelated environment secrets,
SSH/cloud credentials, and other readable host files can still be reached by
Code Mode or stock tools. A secret a tool reads can enter provider input, final
text, or the opaque session snapshot.

Event filtering and stderr redaction do not sanitize final messages or opaque
snapshots. Treat checkpoints as secrets. Deployments that require tool
credential isolation need a stronger external mount/environment/network policy
or credential broker; this bridge does not claim to provide one.

Bubblewrap is Linux-only. A conforming Smithers adapter must fail closed on
macOS, Windows, Linux arm64, or a Linux x86_64 host where Bubblewrap/PID
namespaces are unavailable. A process-group-only macOS fallback is not part of
protocol support. CI still runs the provider-independent suite on macOS 15 to
preserve source portability, but v0.0.1 publishes no macOS artifact and makes no
Smithers-on-macOS support claim.

## Checkpoints

A completed turn contains Nanocodex's exact opaque snapshot and its canonical
workspace. Smithers wraps it in a strict envelope containing the bridge,
Nanocodex, snapshot, and policy versions. Resume requires all of them, the
policy fingerprint, and the canonical workspace to match.

Before `generate()` succeeds, the adapter validates and size-checks the full
checkpoint, awaits `onCheckpoint`, then returns the identical checkpoint object.
The absolute Smithers checkpoint ceiling is 16 MiB. The bridge limits its opaque
snapshot to 15 MiB, leaving at least 1 MiB for Smithers' envelope. Cross-worktree
and cross-machine relocation are intentionally unsupported because Nanocodex
0.3.0 binds snapshots to a canonical absolute workspace.

The policy fingerprint algorithm is language-independent and versioned. Golden
UTF-8 bytes and SHA-256 results are in
[policy-fingerprint-v1.json](docs/fixtures/policy-fingerprint-v1.json).

## Development

Provider-independent verification needs no credentials:

```bash
cargo fmt --all -- --check
cargo check --locked --all-targets
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets
```

Live provider tests are ignored unless explicitly selected:

```bash
SMITHERS_NANOCODEX_LIVE_API_KEY=1 OPENAI_API_KEY=... \
  cargo test --locked --test live \
  live_api_key_turn_and_fresh_process_resume -- --ignored --exact

SMITHERS_NANOCODEX_LIVE_CHATGPT=1 \
  cargo test --locked --test live \
  live_managed_chatgpt_turn_and_fresh_process_resume -- --ignored --exact
```

The bridge and the Smithers adapter are separate deliverables. This repository
does not contain or publish Smithers' JavaScript integration.

## Release installation

Releases publish an `x86_64-unknown-linux-gnu` archive and SHA-256 checksum. A
tag must exactly match the package version; tagging and pushing remain explicit
maintainer actions. The v0.0.1 archive qualified for the Smithers bridge
baseline is pinned by tag commit, byte length, and digest in
[`docs/releases/v0.0.1.json`](docs/releases/v0.0.1.json). A published baseline
manifest is immutable; a newly qualified release gets a new manifest rather
than changing an existing pin.

```bash
sha256sum --check smithers-nanocodex-v0.0.1-x86_64-unknown-linux-gnu.tar.gz.sha256
tar -xzf smithers-nanocodex-v0.0.1-x86_64-unknown-linux-gnu.tar.gz
sudo install -m 0755 \
  smithers-nanocodex-v0.0.1-x86_64-unknown-linux-gnu/smithers-nanocodex \
  /usr/local/bin/smithers-nanocodex
```

Smithers may discover the executable on `PATH` or receive an explicit absolute
path.

## License

Licensed under the [MIT License](LICENSE).
