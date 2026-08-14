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
  -> smithers-nanocodex serve
  -> stock Nanocodex 0.5.0 + native tools
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

- Bridge: `0.0.2`
- Wire protocol: `smithers.nanocodex/1`
- Nanocodex: exactly `0.5.0`
- Rust: `1.97`
- Checkpoints: `nanocodex.session-snapshot/1`, resume only
- Tool profile: `nanocodex-stock-0.5.0`
- Release targets: `x86_64-unknown-linux-gnu` (glibc 2.35+, Ubuntu 22.04) and
  `aarch64-apple-darwin` (macOS 15)
- Models: `gpt-5.6-sol` (default), `gpt-5.6-terra`, `gpt-5.6-luna`
- Thinking/effort: `none`/`low`/`medium`/`high` (default if omitted)/`xhigh`/`max`
- Reasoning mode: `standard` (default) or `pro`
- Fast mode: optional priority processing

The current consumer contract is
[`docs/releases/v0.0.2.json`](docs/releases/v0.0.2.json). GitHub release
sidecar checksums are the published pin; this file does not duplicate those
digests. The historical qualified pin
[`docs/releases/v0.0.1.json`](docs/releases/v0.0.1.json) is immutable and is
not updated. In that pin, `policyFingerprint` is the algorithm identifier; in
a checkpoint envelope the same field name is the SHA-256 digest.

A conforming Smithers adapter spawns this binary directly. Bubblewrap is not
required and must not wrap the worker. Releasing this bridge is an input to the
separate Smithers adapter work; it does not by itself claim that adapter has
shipped or completed integration qualification.

## Migrating from 0.0.1

0.0.1 required the Smithers adapter to wrap this binary in a specific
Bubblewrap PID-containment profile. 0.0.2 removes that launch gate.

- Spawn `smithers-nanocodex serve --protocol-version 1` directly, the same
  way other Smithers CLI agents are spawned.
- Do not wrap this worker in `bwrap` or `sandbox-exec`.
- Isolation, if required, is a separate host policy — typically Smithers
  `<Sandbox>` around the whole agent process. That outer layer is not this
  binary and is not a substitute for the direct-spawn argv.
- 0.0.1 / Nanocodex 0.3.0 / `nanocodex-stock-0.3.0` checkpoints cannot resume
  on this baseline. Start a new conversation.

Two containment layers:

1. **Worker argv (required):** this binary, no wrapper.
2. **Optional host isolation:** Smithers `<Sandbox>` (Linux Bubblewrap, macOS
   `sandbox-exec`, Docker, or a provider sandbox). Independent of this crate.

Protocol v1 keeps stock Code Mode and stock tool families enabled. It does not
offer MCP, subagents, steering, arbitrary JavaScript tools, custom provider
endpoints, Code Mode disablement, checkpoint relocation, or checkpoint fork.
Custom `instructions` replace Nanocodex's defaults completely; there is no
append mode.

## Build and inspect

```bash
cargo build --locked --release
./target/release/smithers-nanocodex --version
./target/release/smithers-nanocodex capabilities
./target/release/smithers-nanocodex capabilities --json
```

`capabilities` without flags prints pretty-printed JSON.
`capabilities --json` prints the same object as compact JSON. Both are
side-effect free: they perform no authentication, network request, workspace
mutation, or agent construction.

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
  and atomic writes. That staging is not a credential-isolation boundary:
  native tools can still read the original file when host permissions allow.

The transport is Nanocodex's WebSocket-preferred Responses transport with its
built-in sticky HTTPS fallback. Custom endpoints are rejected.

## Security boundary: important

This worker is not a sandbox. The adapter starts it as an ordinary process
(layer 1). Managed ChatGPT auth files, unrelated environment secrets, SSH/cloud
credentials, and other readable host files can still be reached by Code Mode
or stock tools. A secret a tool reads can enter provider input, final text, or
the opaque session snapshot.

Event filtering and stderr redaction do not sanitize final messages or opaque
snapshots. Treat checkpoints as secrets. Deployments that require tool
credential isolation must apply a stronger policy *outside* this binary —
typically Smithers `<Sandbox>` (layer 2) — or a credential broker. An outer
Bubblewrap/`sandbox-exec` sandbox is host policy, not a wrapper of this
worker's argv. This bridge does not provide filesystem, network, or
credential isolation.

Windows, musl, Linux arm64, and Intel macOS are not shipped in 0.0.2. Cancel
uses process-group best-effort cleanup, the same class as other Smithers CLI
agents; a double-forked native tool may outlive the bridge.

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
0.5.0 binds snapshots to a canonical absolute workspace.

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

A `v0.0.2` tag publishes one archive per shipped target, each with a SHA-256
checksum. The tag must exactly match the package version; tagging and pushing
remain explicit maintainer actions. After publication, record each archive's
digest and size in [`docs/releases/v0.0.2.json`](docs/releases/v0.0.2.json)
and set `status` to `qualified`. Until that happens, **do not treat the
install snippets below as live URLs** — build from source. A published
baseline manifest is then immutable; a later release gets a new file rather
than rewriting this one.

### Linux

```bash
target=x86_64-unknown-linux-gnu
archive=smithers-nanocodex-v0.0.2-${target}.tar.gz
sha256sum --check "${archive}.sha256"
tar -xzf "$archive"
sudo install -m 0755 \
  "smithers-nanocodex-v0.0.2-${target}/smithers-nanocodex" \
  /usr/local/bin/smithers-nanocodex
```

### macOS (arm64)

```bash
target=aarch64-apple-darwin
archive=smithers-nanocodex-v0.0.2-${target}.tar.gz
shasum -a 256 -c "${archive}.sha256"
tar -xzf "$archive"
# Browser-downloaded GitHub assets are quarantined. This first unsigned cut
# needs the attribute cleared before Gatekeeper will execute it:
xattr -d com.apple.quarantine \
  "smithers-nanocodex-v0.0.2-${target}/smithers-nanocodex" || true
sudo install -m 0755 \
  "smithers-nanocodex-v0.0.2-${target}/smithers-nanocodex" \
  /usr/local/bin/smithers-nanocodex
```

Smithers may discover the executable on `PATH` or receive an explicit absolute
path.

## License

Licensed under the [MIT License](LICENSE). Third-party crate notices for the
shipped binaries are in
[third-party/THIRD-PARTY-LICENSES.html](third-party/THIRD-PARTY-LICENSES.html).
