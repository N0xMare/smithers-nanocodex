# Smithers–Nanocodex Contract

- Contract revision: `1.0.0`
- Bridge release baseline: `smithers-nanocodex 0.0.1`
- Wire protocol: `smithers.nanocodex/1`
- Checkpoint codec: `nanocodex.session-snapshot/1`
- Policy fingerprint: `smithers.nanocodex.policy-fingerprint/1`
- Status: normative

## 1. Scope and conformance

This document is the authoritative contract between the native bridge and an
external Smithers `NanocodexAgent`. It covers one bridge process, one accepted
Nanocodex turn, and one optional same-workspace continuation.

The words MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY have their RFC 2119
meanings. A conforming bridge implements sections 3 through 9. A conforming
Smithers adapter implements sections 10 through 13 as well.

Machine-readable companions are:

- [`schema/client-v1.schema.json`](schema/client-v1.schema.json)
- [`schema/server-v1.schema.json`](schema/server-v1.schema.json)
- [`schema/capabilities-v1.schema.json`](schema/capabilities-v1.schema.json)
- [`schema/checkpoint-v1.schema.json`](schema/checkpoint-v1.schema.json)
- [`fixtures/client-success-v1.jsonl`](fixtures/client-success-v1.jsonl)
- [`fixtures/client-cancel-v1.jsonl`](fixtures/client-cancel-v1.jsonl)
- [`fixtures/server-success-v1.jsonl`](fixtures/server-success-v1.jsonl)
- [`fixtures/checkpoint-v1.json`](fixtures/checkpoint-v1.json)
- [`fixtures/policy-fingerprint-v1.json`](fixtures/policy-fingerprint-v1.json)

The JSONL and checkpoint fixtures are schema/shape examples. Their synthetic
opaque snapshot objects are deliberately not Nanocodex resume sessions and
MUST NOT be used as continuation input. Deterministic tests create genuine
Nanocodex snapshots and prove typed, fresh-process resume; the policy
fingerprint fixture is the only byte-for-byte golden-vector set.

The schemas describe JSON shapes. Requirements stated here for UTF-8 byte
length, canonical paths, parser resources, ordering, durability, process
containment, and cross-record state remain normative when JSON Schema cannot
express them.

## 2. Fixed baseline and deliberate exclusions

Protocol v1 has this fixed baseline:

| Item | Value |
| --- | --- |
| Bridge package | `smithers-nanocodex 0.0.1` |
| Nanocodex crate | exactly `0.3.0` |
| Rust toolchain | `1.97` |
| Model | Nanocodex 0.3.0 stock model (`gpt-5.6-sol`) |
| Native Code Mode | enabled; not disableable |
| Stock tool families | enabled |
| Released target | `x86_64-unknown-linux-gnu` |
| Continuation | same-canonical-workspace `resume` only |

Protocol v1 does not support a daemon, process pool, more than one accepted
turn, cross-worktree relocation, cross-machine resume, checkpoint `fork`,
steering, MCP configuration, subagents, JavaScript tools, custom endpoints,
explicit HTTPS selection, disabling Code Mode, or appending to Nanocodex's
private default instructions.

`options.instructions` is a complete replacement. Omitting it or sending
`null` selects the stock instructions.

## 3. Executable and capabilities

The executable surface is:

```text
smithers-nanocodex --version
smithers-nanocodex capabilities --json
smithers-nanocodex serve --protocol-version 1
```

`capabilities --json` MUST perform no authentication, provider request,
workspace mutation, or Nanocodex construction. It emits one JSON value matching
`capabilities-v1.schema.json` and exits.

`serve --protocol-version 1` emits `hello` before consuming a command. A CLI
protocol version other than `1` is rejected before the JSONL server starts.

Compatibility is decided from `hello.data`, not from bridge semantic version
alone. For this contract, protocol name/version, Nanocodex version, target,
checkpoint versions, ordered mode arrays, feature booleans, and limits MUST
match the capability schema. `bridgeVersion` is informational.

The exact v1 limits are:

| Capability | Bytes/count |
| --- | ---: |
| `maxInputRecordBytes` | 25,165,824 (24 MiB) |
| `maxOutputRecordBytes` | 41,943,040 (40 MiB) |
| `maxPromptBytes` | 4,194,304 (4 MiB) |
| `maxSnapshotBytes` | 15,728,640 (15 MiB) |
| `maxEventBytes` | 1,048,576 (1 MiB) |
| `maxEventTotalBytes` | 16,777,216 (16 MiB) |
| `maxStderrBytes` | 65,536 (64 KiB) |
| `maxCommandRecords` | 256 nonblank physical records |
| `maxJsonDepth` | 64; root is level 1 |
| `maxJsonNodes` | 262,144 value nodes |
| `maxJsonObjectMembers` | 16,384 per object |
| `maxJsonArrayElements` | 131,072 per array |
| `maxJsonStringBytes` | 18,874,368 (18 MiB) decoded UTF-8 |
| `maxJsonKeyBytes` | 1,024 UTF-8 bytes |
| `maxManagedAuthFileBytes` | 1,048,576 (1 MiB) |

## 4. JSONL transport and resource accounting

### 4.1 Framing

Stdin and stdout are UTF-8 JSON Lines. The normal delimiter is one LF byte
(`0x0a`). CRLF is accepted on stdin by stripping one CR immediately before an
LF. Empty LF, CRLF, and CR-only lines are ignored. A nonblank final record may
end at EOF without LF. A CR at EOF is not stripped and therefore makes the JSON
record invalid.

`maxInputRecordBytes` counts bytes before LF. It excludes LF and includes a
preceding CR during the limit check, even though that CR is subsequently
stripped. `maxOutputRecordBytes` counts the serialized server JSON bytes before
the writer appends LF. Consequently, a body of exactly the advertised limit is
permitted and occupies `limit + 1` bytes on the LF-terminated wire.

Smithers MUST compare `byteLength(UTF8(JSON.stringify(command)))`, without the
LF, to `maxInputRecordBytes`, then write that body plus one LF. Its stdout line
limit MUST likewise apply before stripping CR and without counting LF. Its
aggregate stdout limit MUST count every physical byte, including CR, LF, blank
lines, and partial lines.

The bridge flushes after every output record. Stdout is protocol-only. Prompts,
snapshots, and credentials MUST NOT be placed in argv or stderr.

### 4.2 Strict JSON

Client records MUST contain one JSON value and no trailing data. Duplicate keys
at any nesting depth, unknown typed fields, non-finite numbers, malformed UTF-8,
or a shape not admitted by the client schema are rejected. Protocol v1 has no
extension fields.

The strict parser additionally enforces:

| Resource | Maximum |
| --- | ---: |
| Value nesting depth | 64; root is level 1 |
| Total JSON value nodes | 262,144 |
| Members in one object | 16,384 |
| Elements in one array | 131,072 |
| One decoded string | 18,874,368 bytes |
| One object key | 1,024 UTF-8 bytes |

These structural limits apply before typed command conversion and include the
opaque resume snapshot.

Every nonblank physical input record counts toward `maxCommandRecords`,
including `turn.start`, duplicates, unsupported commands, and records that fail
parsing. Blank lines do not count. Reaching a 257th nonblank record is fatal.

## 5. Client records

### 5.1 Common rules and identifiers

Every command has `protocol: "smithers.nanocodex"`, `version: 1`, `type`,
`commandId`, and `data`. `turn.start` and `turn.cancel` also require
`requestId`. No client command carries `seq`.

`commandId`, `requestId`, and `sessionId` are 1 through 128 ASCII bytes from
exactly:

```text
A-Z a-z 0-9 . _ : -
```

The regular expression is `^[A-Za-z0-9._:-]{1,128}$`. A `commandId` is
single-use for the life of the process. Reuse produces `command.rejected` with
`duplicate_command_id` while the physical command budget remains.

### 5.2 `turn.start`

The first nonblank input record MUST be `turn.start`. A conforming client MUST
omit `sessionId`; the bridge decoder also accepts JSON `null` as the absent
value. Its exact envelope is:

```json
{
  "protocol": "smithers.nanocodex",
  "version": 1,
  "type": "turn.start",
  "commandId": "command-1",
  "requestId": "request-1",
  "data": {
    "prompt": "Implement the requested change.",
    "workspace": "/absolute/canonical/worktree",
    "auth": {
      "mode": "api-key-env",
      "environmentVariable": "OPENAI_API_KEY"
    },
    "transport": { "kind": "websocket" },
    "options": {},
    "continuation": null
  }
}
```

The `data` fields are:

| Field | Required | Contract |
| --- | --- | --- |
| `prompt` | yes | String; after Unicode trimming it MUST be nonempty; UTF-8 encoding MUST be at most `maxPromptBytes`. |
| `workspace` | yes | Absolute path. It MUST exist, be a directory, canonicalize successfully, and have a UTF-8 canonical form. |
| `auth` | yes | One exact tagged object from section 5.3. |
| `transport` | yes | Exactly `{ "kind": "websocket" }`. |
| `options` | no | Exact object from section 5.4; omission means `{}`. |
| `continuation` | no | `null` or the exact resume object in section 5.5; omission means `null`. |

The Smithers adapter MUST resolve `rootDir`, then constructor `cwd`, then
`process.cwd()`, in that order. It MUST canonicalize the chosen directory before
building `turn.start`. Constructor state MUST NOT override a per-call Worktree
root.

### 5.3 Authentication and environment names

API-key authentication is:

```json
{
  "mode": "api-key-env",
  "environmentVariable": "OPENAI_API_KEY"
}
```

The environment name MUST match `^[A-Za-z_][A-Za-z0-9_]{0,127}$`. The bridge
reads the value from its process environment after request validation and
rejects a missing, non-Unicode, or whitespace-only value. The credential value
never appears in the command.

After loading it, the bridge configures stock native tool subprocesses with an
empty value for that exact environment name. This is a narrow defense for the
selected API-key variable, not a general environment-secret boundary. The name
remains visible, every unrelated inherited environment value remains subject to
Nanocodex's own behavior, and a same-UID child may still inspect an ancestor's
original environment through host facilities such as Linux `/proc` when the
external sandbox permits it.

Managed ChatGPT authentication is:

```json
{ "mode": "chatgpt", "authFile": "/absolute/path/auth.json" }
```

`authFile` is optional, and JSON `null` is equivalent to omission. The bridge
accepts a relative path, resolves it from its process cwd, and applies no
path-specific length limit beyond the generic decoded-string limit. The current
Smithers adapter requires an absolute path and rejects one longer than 4,096
UTF-16 code units before constructing the command.
Resolution order is:

1. explicit `authFile`;
2. `NANOCODEX_AUTH_FILE` when present, including an empty value;
3. nonempty `CODEX_HOME` plus `/auth.json`;
4. `HOME` plus `/.codex/auth.json`;
5. `USERPROFILE` plus `/.codex/auth.json`.

An empty `CODEX_HOME` falls through to `HOME`/`USERPROFILE`. The bridge
canonicalizes and opens the resolved target nonblocking where the platform
supports it, requires the opened target to be a regular file, and reads at most
1 MiB plus one detection byte. It therefore enforces the byte cap even for a
virtual regular file whose metadata reports length zero. A final symlink is
allowed only when its current target resolves to such a file; broken links and
links to directories, FIFOs, or devices are rejected.

Nanocodex consumes an owner-private staged regular-file copy, never the
caller-supplied path. Before and after each managed-auth operation the bridge
synchronizes through the same bounded reader and atomic owner-only writes. An
adopted external generation replaces Nanocodex's in-memory managed-auth handle
before a bearer is returned. A different account or malformed external
generation remains a pending failure on every later operation until the caller
replaces it with a valid same-account document. Repeated valid external changes
across three consecutive snapshot attempts fail as temporarily unavailable
instead of returning a generation known to be stale.
Before copying back a Nanocodex refresh, the bridge fully prepares its atomic
replacement and then re-reads the original; an external generation observed by
that final check wins and is adopted by the staged copy. POSIX paths do not
provide a compare-and-swap against uncoordinated writers: an external atomic
replacement in the final check-to-rename interval and the bridge replacement
are last-writer-wins. Operators SHOULD NOT run an interactive login or another
credential-refreshing process against the same auth file concurrently with a
bridge turn. These controls bound bridge reads and narrow persistence races;
they do not create a security boundary against a same-UID host process racing
path components or reading either credential file.

### 5.4 Turn options

`options` admits only:

| Field | Values | Meaning |
| --- | --- | --- |
| `instructions` | string or `null` | `null`/absent uses stock instructions. A string is a complete replacement, not an append. A replacement MUST trim nonempty and be at most 4 MiB UTF-8. |
| `thinking` | `none`, `low`, `medium`, `high`, `xhigh`, `max` | Direct Nanocodex thinking setting. |
| `reasoningMode` | `standard`, `pro` | Direct Nanocodex reasoning mode. |
| `fastMode` | boolean | Direct Nanocodex fast-mode setting. |

Each individual option is optional. JSON `null` is equivalent to omission for
all four fields; the `options` object itself, when present, MUST NOT be `null`.

There is no model, tool, endpoint, MCP, subagent, Code Mode, or append-prompt
option in protocol v1.

### 5.5 Continuation

A resumed start uses:

```json
{
  "mode": "resume",
  "snapshot": {}
}
```

`snapshot` is the exact `nanocodexSnapshot` object from a validated checkpoint;
it MUST NOT be edited, redacted, normalized, or relocated. The bridge requires:

- compact JSON encoding at most `maxSnapshotBytes`;
- all advertised JSON depth/node/member/array/key/string structural limits;
- `request_prefix` as an array and non-null `context_snapshot`;
- exact typed deserialize/serialize semantic equality;
- Nanocodex snapshot version `1`;
- the stored workspace string to equal the requested canonical UTF-8 path;
- a second filesystem canonicalization of the stored path to equal the
  requested directory.

Before publishing `turn.completed`, the bridge also embeds the produced
snapshot in a complete minimal `turn.start` resume envelope: the same canonical
workspace, authentication mode/path, and option semantics, all option keys
present, and a one-byte prompt. That complete envelope MUST satisfy
`maxInputRecordBytes` and every structural parser limit. This reserves the
outer record's depth and nodes and guarantees the published snapshot has at
least one protocol-valid fresh-process resume framing. A later caller remains
responsible for ensuring its chosen prompt and serialized command fit the same
record limit.

Nanocodex then verifies the reconstructed instructions/tool request prefix.
Changing the canonical workspace, replacement instructions, or stock tool
profile fails resume. A symlink works only when canonicalization resolves to the
original stored path.

### 5.6 `turn.cancel`

The cancellation shape is:

```json
{
  "protocol": "smithers.nanocodex",
  "version": 1,
  "type": "turn.cancel",
  "commandId": "command-2",
  "requestId": "request-1",
  "sessionId": "session-1",
  "data": { "reason": "idle_timeout" }
}
```

Before `turn.accepted`, a conforming client MUST omit `sessionId`; JSON `null`
is decoded as omission. Cancellation is latched by the exact `requestId`.
After `turn.accepted`, `sessionId` MUST be present and equal the accepted
session. A mismatch or post-acceptance JSON `null` produces
`command.rejected`.

`data.reason` is optional. If present, it MUST contain 1 through 128 UTF-8 bytes
and no Unicode control character (General Category `Cc`). Omission is equivalent
to `"cancelled"`; JSON `null` is also accepted as omission. The reason is untrusted status text and MUST NOT be placed in
durable errors, logs, metrics, or checkpoint metadata.

An accepted pre-session cancellation is acknowledged before the eventual
`process.failed`. An accepted exact-session cancellation is acknowledged before
`turn.cancelled`. `command.accepted` means routing/cleanup acknowledgement, not
that a terminal has already been written. Exact cancellation rejection produces
`command.rejected`; the turn may still complete.

## 6. Server records and state

### 6.1 Envelope and sequence

Every server record contains exactly:

```json
{
  "protocol": "smithers.nanocodex",
  "version": 1,
  "type": "agent.event",
  "seq": 3,
  "requestId": "request-1",
  "sessionId": "session-1",
  "data": {}
}
```

plus only the correlation fields defined for that record type. `seq` starts at
1 for `hello` and increases by exactly 1 process-wide. One writer assigns it.
There are no records after a terminal.

The exact correlation and data fields are:

| Type | Correlation fields | Exact `data` |
| --- | --- | --- |
| `hello` | none | capability object |
| `turn.accepted` | `requestId`, start `commandId`, `sessionId` | `{}` |
| `agent.event` | `requestId`, `sessionId` | `{ "event": projectedEvent }` |
| `agent.event-truncated` | `requestId`, `sessionId` | `{ "upstreamType": string|null, "upstreamSeq": integer|null, "reason": truncationReason }` |
| `command.accepted` | `requestId`, cancel `commandId`; `sessionId` iff already accepted | `{ "command": "turn.cancel" }` |
| `command.rejected` | `requestId`, command `commandId`; `sessionId` iff already accepted | `{ "error": publicError }` |
| `turn.completed` | `requestId`, `sessionId` | completion in section 6.3 |
| `turn.failed` | `requestId`, `sessionId` | `{ "error": publicError }` plus optional cleanup recovery |
| `turn.cancelled` | `requestId`, `sessionId` | `{ "reason": boundedReason }` |
| `process.failed` | optional `requestId`; never `commandId`/`sessionId` | `{ "error": publicError }` |

`turn.accepted` is emitted only after Nanocodex accepts the prompt and the
bridge owns that turn's exact cancellation handle. `sessionId` is the ID from
the constructed Nanocodex handle.

### 6.2 State and command outcomes

The observable state order is:

```text
Boot -> Hello -> AwaitStart -> Opening -> Running -> Finalizing -> Terminal
```

Malformed framing/JSON, duplicate JSON keys, envelope mismatch, parser resource
overflow, physical record overflow, and an invalid first command are fatal.
Before acceptance the terminal is `process.failed`; after acceptance it is
`turn.failed` when stdout remains available.

A well-formed but unsupported or illegal-state post-start command receives
`command.rejected`. Commands already available when finalization begins are
rejected with `turn_not_cancellable`; finalization never reopens the turn.

The bridge supervises exact cancellation separately from the backend. A hung or
panicked exact-cancellation future MUST NOT prevent EOF, SIGINT, SIGTERM, stdout
closure, or backend completion from taking the fallback cancellation path. A
blocked stdout writer MUST NOT stop input, signal, cancellation, or backend
state progression. Enqueue order remains output order.

The bridge gives an accepted exact-cancellation task 1,000 ms to acknowledge.
A timeout or panic emits `command.rejected` with `cancellation_timed_out` or
`cancellation_task_failed`, activates fallback cancellation, and settles an
accepted turn as `turn.failed`/`internal`. If backend completion or external
EOF/signal/EPIPE wins first, the bridge aborts the still-pending exact task and
emits `command.rejected` with `turn_not_cancellable` before the terminal. An
external initiating cause still owns the resulting cancellation reason.

Once a turn is accepted and while output remains available, exactly one of
`turn.completed`, `turn.failed`, or `turn.cancelled` is emitted. Before
acceptance, an unrecoverable outcome emits exactly one `process.failed`.

### 6.3 Terminal data

`turn.completed.data` contains exactly:

```json
{
  "finalMessage": "Done.",
  "usage": {
    "inputTokens": 0,
    "cachedInputTokens": 0,
    "cacheWriteInputTokens": 0,
    "outputTokens": 0,
    "reasoningOutputTokens": 0,
    "totalTokens": 0,
    "estimatedUsd": null,
    "costStatus": "usage_not_reported",
    "serviceTier": null
  },
  "snapshotVersion": 1,
  "snapshot": {},
  "canonicalWorkspace": "/absolute/canonical/worktree"
}
```

Token counts are nonnegative integers. `estimatedUsd` is a nonnegative decimal
string or `null`. This baseline emits `costStatus` as
`estimated_from_usage`, `usage_not_reported`, or `other`, and `serviceTier` as
`standard`, `priority`, or `null`. Consumers MUST tolerate an unknown bounded
status/tier string without changing token accounting.

`canonicalWorkspace` is taken from the completed `SessionSnapshot`, not copied
blindly from `turn.start`. It MUST equal the requested canonical workspace
before Smithers publishes a checkpoint.

Successful backend ordering is:

1. drain through the upstream terminal event;
2. await authoritative `TurnResult`;
3. capture final message, explicit usage fields, snapshot version, snapshot,
   session identity, and snapshot workspace;
4. enforce snapshot/output limits;
5. await `Nanocodex::shutdown()`;
6. enqueue `turn.completed`;
7. flush the record and exit zero.

If steps 1 through 4 succeed but shutdown fails, the bridge emits
`turn.failed` with error code `cleanup_failed`, category `cleanup`, and:

```json
{
  "completed": {
    "snapshotVersion": 1,
    "snapshot": {},
    "canonicalWorkspace": "/absolute/canonical/worktree"
  }
}
```

Recovery omits `finalMessage` and `usage`. No other error may carry
`completed`. If any terminal would exceed `maxOutputRecordBytes`, it is replaced
by a bounded `turn.failed`/`process.failed` with `result_too_large`; a partial
snapshot is never emitted.

## 7. Agent events

### 7.1 Bridge projection

`agent.event.data.event` has exactly `type`, `upstreamSeq`, and `payload`.
`upstreamSeq` is Nanocodex's original sequence and is independent of bridge
`seq`.

The only payload fields projected by this bridge baseline are:

| Upstream `type` | Projected `payload` |
| --- | --- |
| `assistant.delta` | `modelCallIndex`, `itemId` (string/null), `phase` (`commentary`/`final_answer`/null), `text` |
| `assistant.message` | same fields as `assistant.delta` |
| `tool.call` | `callId`, `tool`, `modelCallIndex` |
| `tool.result` | `callId`, `tool`, `status`, `durationNs`, `startedAfterNs` (integer/null) |
| every run/model/transport lifecycle type | `{}` |

`tool.result.status` is `completed`, `failed`, `cancelled`, or `unknown`.
Tool arguments, tool result bodies, provider frames, raw error strings,
workspace paths, and lifecycle configuration are never present in an ordinary
projected event.

`api.event` and `reasoning.summary.delta` are never ordinary events. They become
`event_policy` truncation records when their raw payload is within the per-event
limit; an oversized raw payload is classified earlier as `event_limit`.

### 7.2 Truncation and backpressure

Truncation reasons are exactly:

| Reason | Meaning |
| --- | --- |
| `event_policy` | Raw provider or reasoning content is deliberately omitted. |
| `event_limit` | Raw upstream payload or safe projected event plus a 1,024-byte envelope reserve exceeds 1 MiB. |
| `aggregate_event_limit` | The next charged event plus a final 1,024-byte marker reserve would exceed 16 MiB. Emitted at most once; later events are suppressed. |
| `bridge_event_backpressure` | A bounded notice/output queue was full. It is coalesced and has null upstream type/sequence when no exact event can be named. |

The bridge continues draining the upstream stream after any truncation or drop.
Event limits never truncate terminal data. Congestion can omit multiple events
behind one backpressure marker.

### 7.3 Smithers `AgentCliEvent` mapping

The external adapter projects only:

- `turn.accepted` -> one `started` event;
- `assistant.delta` -> `onStdout(text)` only;
- `assistant.message` -> completed message action;
- `tool.call` -> started tool action with tool name and optional model-call
  index, never arguments;
- `tool.result` -> completed tool action with status/duration, never result;
- `run.error` or an event type ending `.failed` -> generic lifecycle warning;
- `agent.event-truncated` -> generic omitted-event warning;
- terminal success/failure/cancellation -> the corresponding completed event.

An unknown `agent.event` type that ends in `.failed` becomes only the generic
warning. Every other unknown type is ignored. Its payload MUST NOT be logged,
persisted, spread into metadata, or passed through. Unknown top-level server
record types are fatal protocol errors.

`onEvent` is observational: it is not awaited and observer failures do not own
the generation lifecycle. `onCheckpoint` has the opposite rule in section 10.

## 8. Public errors and exits

A public error contains exactly `code`, `category`, `message`, `retry`, and an
optional `retryAfterMs`. Codes match `^[a-z][a-z0-9_]{0,127}$`. Categories are:

```text
protocol config auth checkpoint workspace provider tool cleanup internal
```

Retry is `never`, `safe`, or `after`. `retryAfterMs` is present if and only if
retry is `after`. Messages are sanitized and bounded to 512 characters plus an
optional ellipsis. Consumers MUST branch on category/code/retry, never message
text. Unknown valid error codes are forward-compatible.

Notable stable backend codes include:

- auth/configuration: `invalid_auth_environment`, `auth_unavailable`,
  `auth_file_unavailable`, `auth_file_unreadable`, `invalid_auth_file_type`,
  `auth_file_too_large`, `invalid_auth_file`, `auth_login_required`,
  `auth_login_failed`, `auth_temporarily_unavailable`,
  `auth_account_changed`, `auth_refresh_failed`;
- checkpoint/workspace: `invalid_snapshot`, `snapshot_too_large`,
  `snapshot_structure_too_large`, `snapshot_resume_record_too_large`,
  `snapshot_version_unsupported`, `checkpoint_missing`,
  `checkpoint_lineage_mismatch`, `checkpoint_unavailable`, `workspace_changed`;
- provider: `provider_` followed by Nanocodex's stable response-error class;
- cleanup/internal: `cleanup_failed`, `result_too_large`.
- cancellation supervision: `cancellation_timed_out`,
  `cancellation_task_failed`, `turn_not_cancellable`.

The JSON terminal is authoritative even if the process later exits nonzero.
Without a terminal, Smithers synthesizes a bounded bridge error from exit/signal
and redacted stderr; it MUST NOT include argv, prompts, snapshots, raw protocol
records, or unbounded child errors.

Exit codes are:

| Code | Meaning |
| ---: | --- |
| 0 | completed |
| 2 | protocol/config/checkpoint/workspace failure |
| 3 | authentication failure |
| 4 | provider/tool turn failure |
| 5 | internal/cleanup/output failure |
| 130 | cancellation |

## 9. Bridge cancellation and shutdown

Protocol cancellation, stdin EOF, SIGINT, SIGTERM, and stdout failure feed one
coordinator. The first accepted initiating cause owns public cancellation
classification. The bridge invokes the exact Nanocodex `TurnControl` at most
once for a client cancellation. Fallback cancellation remains available when
that exact acknowledgement hangs, panics, or races completion.

Cancellation does not create a checkpoint. A cancelled or failed turn has no
snapshot unless a completed boundary is explicitly carried by the
`cleanup_failed` recovery shape.

The one-shot Tokio runtime uses two async worker threads. Its separate blocking
pool remains Tokio-managed for portable stdin and native operations. After
`serve` returns, runtime shutdown is bounded to 100 ms so an outstanding
portable stdin read cannot keep the process alive.

## 10. Smithers checkpoint contract

### 10.1 Envelope and validation

The adapter declares only:

```js
checkpointFormats = [{ codec: "nanocodex.session-snapshot", versions: [1] }];
checkpointCapabilities = [
  { codec: "nanocodex.session-snapshot", versions: [1], modes: ["resume"] },
];
```

The exact durable envelope is:

```json
{
  "codec": "nanocodex.session-snapshot",
  "version": 1,
  "payload": {
    "bridgeProtocolVersion": 1,
    "nanocodexVersion": "0.3.0",
    "snapshotVersion": 1,
    "canonicalWorkspace": "/absolute/canonical/worktree",
    "policyFingerprint": "sha256:e4580c36cd5e0b89d1bdc7aeda2c3664ca61d239fdd9807e6b62019b81dbde86",
    "nanocodexSnapshot": {}
  }
}
```

All objects have exactly the fields shown. The adapter MUST reject a codec,
version, bridge protocol, Nanocodex version, snapshot version, canonical
workspace, or policy fingerprint mismatch before spawning the bridge.

The checkpoint must be stable JSON: plain objects/arrays, enumerable data
properties, no cycles, no holes or extra array properties, finite numbers, no
negative zero, and no integer outside the language's exact/safe integer range.
Validation clones the value through compact JSON.

Smithers measures UTF-8 bytes of the complete compact envelope, not just the
opaque snapshot. The configured per-call and per-agent limits are intersected;
neither may exceed the absolute 16,777,216-byte Smithers ceiling. The bridge's
15 MiB snapshot ceiling reserves at least 1 MiB for the durable envelope and
does not relax a smaller configured ceiling.

Snapshots contain complete unredacted model history, reasoning, tool inputs,
and tool outputs. The checkpoint MUST be encrypted/access-controlled as a
secret and MUST NOT enter ordinary events, logs, traces, metrics, stderr, error
serialization, or URLs.

### 10.2 Language-independent `policyFingerprint`

The algorithm input is one `instructions` value: `null` for stock instructions
or a sequence of Unicode scalar values for a complete replacement. Unicode is
not normalized.

Define `J(s)` as a JSON string encoder with these exact rules:

1. Begin/end with ASCII `"`.
2. Encode `"` as `\"` and `\` as `\\`.
3. Encode U+0008, U+0009, U+000A, U+000C, and U+000D as `\b`, `\t`, `\n`,
   `\f`, and `\r` respectively.
4. Encode every other U+0000 through U+001F scalar as lowercase `\u00xx`.
5. Encode every other scalar directly as UTF-8. Do not escape `/`, non-ASCII,
   U+2028, or U+2029. Reject an unpaired UTF-16 surrogate rather than hashing
   it.

Construct exactly, with no spaces, BOM, or newline:

```text
{"fingerprintVersion":1,"instructions":I,"tools":{"profile":"nanocodex-stock-0.3.0","codeMode":true,"mcp":false,"subagents":false}}
```

where `I` is ASCII `null` or `J(instructions)`. Member order is fixed as shown;
implementations MUST NOT hash a caller-supplied object, sort keys, or use a
general-purpose serializer whose escaping differs from `J`.

Compute SHA-256 over those exact UTF-8 bytes, encode the 32-byte digest as 64
lowercase hexadecimal digits, and prefix `sha256:`. The golden vectors,
including exact byte hex, JSON escaping, Unicode separators, and distinct
NFC/NFD strings, are in `policy-fingerprint-v1.json`.

For a conformance harness that accepts a raw policy object, member order is
ignored, the field set/fixed values must be exact, and any JSON number whose
mathematical value is exactly integer one is re-emitted as ASCII `1`. Thus the
fixture's reordered object containing source token `1e0` normalizes to the
stock vector. Production adapters accept only `instructions` and construct the
record themselves; they do not accept raw policy JSON.

Only continuation-sensitive instructions and the fixed stock tool profile are
fingerprinted. Thinking, reasoning mode, fast mode, authentication, transport,
workspace, timeouts, IDs, and credentials are intentionally excluded.

### 10.3 Publication durability and callback ordering

For a normal completion the adapter MUST process serially:

1. validate terminal correlation and `canonicalWorkspace`;
2. construct, stable-JSON validate, size-check, and clone the envelope;
3. call `onCheckpoint(checkpoint)` when supplied;
4. await that callback to durable success;
5. build the result with the identical checkpoint object;
6. emit the Smithers completed event;
7. resolve `generate()`.

“Durable success” means the callback's own transaction/atomic replace and any
required fsync/remote acknowledgement have completed. The adapter must not
assume callback invocation alone is durable.

If `onCheckpoint` rejects or throws, its exact failure rejects `generate()`;
the adapter MUST NOT report success, emit a success-completed event, or silently
retry publication. The checkpoint value is content-identical across callback
and returned result so Smithers can deduplicate it.

For `cleanup_failed` recovery, the adapter validates and durably publishes the
recovery checkpoint first, then rejects the generation with the cleanup error.
The recovery checkpoint may be attached non-enumerably for in-process recovery,
but ordinary error serialization MUST NOT include it. Callback failure still
wins and is returned directly.

## 11. Smithers timers and cancellation escalation

Total timeout, idle timeout, and abort are external Smithers concerns. Timer
values MUST be nonnegative safe integers no larger than `2^31 - 1` ms and MUST
be validated before spawn.

The total timer starts when the contained child is spawned and never resets.
The idle timer also starts at spawn. It resets on every nonempty raw stdout or
stderr byte chunk, before UTF-8/JSON validation and even when the chunk is only
part of a line. Blank stdout bytes and stderr that is later redacted still count
as activity.

The idle timer does not reset for stdin writes, successful callbacks, process
census, CPU use, filesystem changes, network use, or silent native-tool/model
work. The bridge emits no synthetic heartbeat. Hosts must choose an idle timeout
large enough for legitimate quiet provider and tool periods. Marking an
authoritative terminal clears total/idle timers and begins a separate bounded
terminal-exit grace period.

The first abort/timeout cause is latched. Smithers sends exactly one correlated
`turn.cancel` when start has been written, waits within the configured cancel
grace, sends SIGTERM, then SIGKILL, and verifies closure. If cancellation occurs
while `turn.start` is backpressured, it waits only within that same cancel grace,
queues cancellation behind the completed start write, and then escalates.

## 12. Mandatory Linux process containment

### 12.1 Support distinction

The bridge crate contains portable code paths, but its artifact contract is only
x86_64 GNU/Linux with glibc 2.35 or newer, built and smoked on Ubuntu 22.04. A
conforming Smithers adapter additionally requires an executable Bubblewrap and
usable private PID namespaces. It MUST reject Linux arm64, macOS x86_64, macOS
arm64, Windows, an older glibc baseline, and every other unsupported host before
bridge spawn.

macOS is not “supported without Bubblewrap.” It has no Bubblewrap PID namespace
and protocol v1 defines no equivalent containment primitive. Future macOS
support requires a separate released target plus clean-host capability,
cancellation, detached-daemon, pipe/socket/heartbeat, and forced-cleanup tests.
A process-group-only fallback MUST NOT be advertised as equivalent.

CI runs the complete provider-independent source suite on macOS 15 arm64 to
preserve portability and detect accidental platform coupling. That source-level
signal is not a macOS artifact, containment qualification, or support claim.

### 12.2 Exact Bubblewrap launch

Both capability preflight and `serve` MUST fail closed unless an executable
`bwrap` is found at `/usr/bin/bwrap`, `/bin/bwrap`, or an explicit `PATH`
candidate. Each payload is launched without a shell using this exact common
prefix:

```text
bwrap
  --unshare-pid
  --die-with-parent
  --new-session
  --bind / /
  --proc /proc
  --dev-bind /dev /dev
  --
  /absolute/or-PATH-resolved/smithers-nanocodex
  <mode arguments>
```

`<mode arguments>` is exactly `capabilities --json` for preflight and exactly
`serve --protocol-version 1` for the turn process. Preflight MUST validate the
capability object and contained-process closure before the turn process starts.

The adapter sets the canonical workspace as cwd, uses explicit stdin/stdout/
stderr pipes, and starts the wrapper as a detached process group. Prompts,
snapshots, commands, and credentials are absent from argv.

`--unshare-pid` plus `--die-with-parent` is the authoritative descendant
membership boundary: when the namespace init exits, detached/reparented members
cannot remain in that namespace. `--new-session`, process-group signaling, and
PID/start-time-guarded `/proc` enumeration are supplemental. The adapter MUST:

- census descendants during execution and immediately before/after signaling;
- guard PIDs with Linux start time to avoid PID-reuse kills;
- send SIGTERM to known descendants and the root process group;
- after bounded grace, repeat with SIGKILL;
- re-enumerate to close fork-during-signal races;
- reject success if root closure or surviving-descendant cleanup cannot be
  verified;
- keep an authoritative terminal non-enumerably available to recovery logic if
  process cleanup later fails.

Tests MUST include a rapidly double-forked/new-session daemon and prove no PID,
heartbeat growth, listening Unix socket, or held pipe survives. A negative
control that bypasses the PID namespace must prove the fixture would escape.

### 12.3 What Bubblewrap does not isolate

This launch is process containment, not a tool sandbox. `--bind / /` exposes the
host root read-write. Omitting `--unshare-net` preserves host networking.
`--dev-bind /dev /dev` exposes host devices according to host permissions.

Therefore protocol v1 makes no claim that Code Mode or stock tools are
filesystem-, network-, or credential-isolated. In particular:

- managed ChatGPT auth files remain readable by native tools when host
  permissions allow;
- `HOME`, `CODEX_HOME`, unrelated environment secrets, SSH/cloud credentials,
  repository secrets, and other mounted host files may be reachable;
- the API-key mitigation blanks only the exact selected variable in native tool
  subprocesses; it does not erase the bridge parent's original environment, and
  same-UID descendants may be able to inspect that parent through `/proc`;
- a prompt injection/tool can place any secret it can read into provider input,
  final text, tool history, or the opaque snapshot.

Event projection and stderr redaction do not sanitize final messages or opaque
snapshots. Operators needing credential isolation MUST supply a stronger
external filesystem/environment/network sandbox or a credential broker and
validate it independently. The current Bubblewrap profile does not provide it.

## 13. Forward compatibility

Protocol v1 is fail-closed:

- unknown client fields, command types, transport fields, and tagged variants
  are rejected;
- unknown top-level server types, envelope fields, truncation reasons, and
  capability fields are rejected by Smithers;
- a sequence gap, duplicate `hello`, record after terminal, correlation change,
  or turn record before acceptance is fatal;
- unknown valid public error codes are accepted and classified by category and
  retry metadata;
- unknown `agent.event` types are the sole open record-level projection point
  and follow section 7.3 without exposing their payload.

A producer MUST NOT add a field to an existing v1 object and call it backward
compatible. New commands, terminal fields, modes, transports, containment
profiles, or checkpoint semantics require a new negotiated protocol/codec/
fingerprint version as applicable. A consumer may support multiple versions,
but it MUST select one exact schema after `hello`; it MUST NOT guess from bridge
semver or silently downgrade.

## 14. Required verification

Provider-independent bridge artifact verification MUST cover:

- schemas/fixtures and capability output;
- fragmented LF/CRLF/EOF framing and all byte boundaries;
- duplicate keys, unknown fields, parser depth/node/member/array/key/string
  limits, and physical command counting including duplicates;
- monotonic sequence, correlation, acceptance, terminal, and no-post-terminal
  rules;
- cancellation before/after acceptance, rejection, hung/panicked exact cancel,
  completion races, EOF, SIGTERM, EPIPE, and blocked stdout;
- event policy/size/aggregate/backpressure truncation while upstream draining
  continues;
- fresh completion, same-path fresh-process resume, changed/missing workspace,
  instruction/tool mismatch, and checkpoint size limits;
- adversarial credential tests for bounded virtual-file reads, managed-auth
  staging/refresh synchronization, and the distinction between exact API-key
  blanking and the managed-auth/host-secret non-isolation stated here.

This repository's CI owns those bridge checks. Releasing the bridge makes an
artifact available to the separate Smithers adapter; it is not by itself a
claim that Smithers integration has shipped or completed qualification.

Before a Smithers adapter can claim protocol v1 release support, combined
integration verification MUST additionally cover:

- policy fingerprint golden vectors in at least two implementation languages;
- awaited checkpoint publication, callback failure, cleanup recovery, and
  non-enumerable error recovery;
- Bubblewrap fail-closed startup and the detached-daemon positive/negative
  containment tests in section 12;
- clean-host execution of the released archive through capability preflight,
  fresh generation, cancellation, cleanup, and same-workspace resume.

Live API-key and managed-ChatGPT tests remain explicit opt-in smoke tests. They
do not replace deterministic protocol, security-boundary, or containment tests.
