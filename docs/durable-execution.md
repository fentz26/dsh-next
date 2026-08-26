# Durable execution architecture (Track F)

Status gate: **DESCRIPTOR LAYER VALIDATED AT 10K SCALE; RUNTIME CYCLE NOT YET** —
this document fixes vocabulary, ownership tables, commit boundaries and crash
semantics. The storage half now exists as `@dsh-next/descriptor-store` with
correctness tests and a measured scale benchmark; the wake/admit/commit runtime
cycle is not implemented yet.

## Measured prototype status

* `packages/descriptor-store`: JSON-only records validated against the model
  below (format version, mandatory source identity), WAL+FULL durability,
  pull-based wake lookup by kind/key-prefix (no scheduler).
* Correctness tests 4/4: round trip, invalid-descriptor refusal (incl. missing
  source identity), scan respects sleeping/waking state, durability across
  reopen.
* Scale (`benches/results/descriptor-scale.txt`, darwin-arm64): 10k dormant
  descriptors → RSS ~95 MiB total (~1 KB/descriptor incl. SQLite page
  overhead); upsert p50 47 µs; get-by-id p99 2 µs; wake-scan p99 0.58 ms;
  full sleep→wake→sleep cycle p50 74 µs / p99 148 µs.
* Claim wording per program rules: "10k dormant descriptors tested" — not a
  million-agent claim. It reuses DSH primitives only (Session log authority,
`ctx.waits` orchestration, Jobs registry); dsh-next adds no second event bus
and never persists live JS objects.

## State classification

| State | Class | Owner / location |
|---|---|---|
| Session event log (turns, messages, chunks, tool calls/results) | **durable** | persistence provider (stock or Track B worker) |
| Wait descriptors & folds (`wait/change` history) | **durable** | `ctx.waits` session-log design |
| Job registry records, output journals, waiter bookkeeping | **ephemeral/reconstructable** | `LocalJobRegistry` (stays TS) |
| Agent instance, Cordis context, plugin effects, controllers | **ephemeral** | process-local by design |
| Subprocess handles / PTYs | **external-ephemeral** today | stock provider lifecycle |
| Execution world (cwd/env/secrets) | **external** | OS; referenced by descriptor |
| Durable agent descriptor (below) | **durable (new, minimal)** | provider-owned metadata, cache-rules like Track C checkpoints |

## Durable descriptor model

A sleeping logical agent is a *record*, not an object:

```
{
  sessionId            // canonical anchor
  formatVersion        // SESSION_FORMAT_VERSION seen at sleep
  agentPresetId        // composition identity
  executionWorld       // cwd + validated env class (never secret values)
  wakeConditions[]     // wait ids/timers/event kinds via ctx.waits providers
  budgetState          // e.g. maxConsecutiveWakes-compatible budgets
  lastCommitted        // { sourceRevision, seq watermark, turnId }
}
```

Constraints: JSON-safe plain data; regenerable where possible; validated on
restore against source revision (mismatch ⇒ rebuild from log). AbortControllers,
closures, Cordis contexts are categorically excluded.

## Bounded execution cycle

DSH's own turn is the natural unit (one or more model/tool steps closed by
`turn/end`):

```
wake condition settles (ctx.waits dispatch — event driven, NO polling loop)
  → admit: prepare/load session (Track C worker path keeps this responsive)
  → run one bounded continuation through ordinary Agent APIs
  → flush Session durability (session/flush semantics)
  → persist descriptor update atomically AFTER the flush ack
  → release runtime resources (jobs/subprocess disposal already drains)
```

Commit boundary ordering (explicit): session durability ⇒ wait-state commits ⇒
descriptor write. A crash between any two stages leaves a deterministic
state: replay resumes from the last fully completed boundary; partially
written descriptor updates must be keyed/versioned so they can be discarded.

## Crash scenario classes

| Scenario | Recovery behavior target | Risk note |
|---|---|---|
| crash before model call | wake again from unchanged descriptor | trivial |
| crash mid-stream | turn was never durably closed; interrupted-turn repair closes it on next load (existing semantics) | none new |
| tool side effect happened, crash BEFORE `tool/result` persisted | replay may redo effect — **at-least-once for external tools unless idempotency keys exist** | do NOT claim exactly-once |
| crash after result persisted but before turn/end | existing interrupted-turn repair closes turn | none new |
| crash after session flush, before descriptor update | descriptor stale ⇒ wake replays validation via revisions; event log authoritative, extra dispatch is idempotent-class | low |

External-side-effect contract for tools remains whatever DSH defines today;
dsh-next documents three explicit classes (at-most-once / at-least-once /
idempotency-capable) instead of inventing guarantees.

## Wake conditions & admission

Reuse `ctx.waits` provider model (timer/wait-job/providers register via
`registerProvider`; readiness dispatches one ordinary agent continuation).
No scheduler scans sessions; pending wake indexes are ordinary durable wait
records. Sleeping agents therefore cost storage rows, not timers — target:
10k+ dormant descriptors as pure metadata (bench planned, not yet run).

## Measurement plan (before any implementation claim)

* descriptor scale bench: create/persist/query 10 / 1k / 10k synthetic
  descriptors (keyless, no model calls)
* wake latency: time-to-agent-ready for small/medium/giant sessions via Track C
* efficiency model: always-live process vs sleep/wake cycle CPU + RSS over
  wait-heavy synthetic workflows.
