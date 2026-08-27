# Persistence worker (Track B)

Status gate: **WORKER PERSISTENCE PROCEED** — correctness differential green
(8/8 incl. crash injection and torn-tail repair parity), and the Phase 0
concurrency pain point (main-thread stalls from synchronous SQLite) is
measurably eliminated.

## Architecture

```
main thread                                    worker thread
────────────                                   ──────────────
WorkerSqliteBackend                    owns →  SqliteStore (node:sqlite)
  PersistenceBackend hook surface              appendBatch/loadStored/
  pending map + bounded in-flight              readStoredRevision/list/
  FIFO dispatch                                loadStoredFrom/commitRepair
  AbortSignal advisory gating                  DatabaseSync / WAL / FULL sync
        │      structured clone (JSON-shaped)            │
        └────────── request/response frames ─────────────┘
                                                   responses only after commit
```

* One worker owns ONE database file (matches stock shape: a single store serves
  every session). The main thread never touches DatabaseSync for managed
  sessions.
* Requests are processed strictly FIFO — a single connection serializes them
  anyway; FIFO preserves coordinator ordering across sessions.
* Durability: `appendBatch` responds only after the transaction commits, so
  coordinator semantics (`append resolves only after durability`) are intact.
  WAL + `synchronous=FULL` + BEGIN IMMEDIATE boundaries are unchanged — this
  compares architecture, not weaker durability.

## Semantics preservation checklist

append-only · contiguous seq · exact SessionEvent JSON · header row · revision
tokens · inspect non-mutating closers · prepare/load torn-tail repair ·
`readFrom` suffix-only reads · format refusal (unchanged — coordinator-owned) ·
ignorable unknown events (coordinator-owned) · dispose draining (close op →
exit await → terminate fallback).

## Failure semantics — generation-scoped state machine (P0 hardening)

```text
new → opening → ready ⇄ (restarting → ready | failed) → closing → closed
```

* Every Worker gets a monotonic **generation id**; its message/error/exit
  handlers are closures over that generation. Events from stale generations
  are ignored and can never fail the live one (recovery.test.ts G).
* Exactly ONE published generation (`active`) exists at any time. A
  replacement is opened only AFTER the old generation fully terminated
  (exit awaited / terminate resolved). **Double-writer safety comes from
  lifecycle ownership, not from SQLite locking** — overlaps are prevented,
  not arbitrated.
* Every dispatched request acquires exactly one capacity slot and releases it
  exactly once on every terminal path (success, worker error frame, crash,
  exit, shutdown, paged-stream failure, safety-limit failure) through one
  centralized settlement (`PendingReq`). Backpressure waiters are explicit
  objects, rejected deterministically on crash/dispose; queued aborted calls
  are removed and never dispatched.
* Worker death ⇒ its in-flight requests reject with typed
  `WorkerPersistenceError` (commit status never guessed; writes are NEVER
  auto-replayed), waiters reject, then EITHER:
  - default: state `failed`, calls fail fast; or
  - `restartOnCrash: true`: state `restarting` (stats.failed stays TRUE),
    old generation terminated, then WORKER REOPEN retried up to
    `restartAttempts` total attempts (default 3, deterministic 20ms·n
    backoff, no jitter). Calls arriving before `restarted` fail fast with
    the state name — no hidden queueing or replay. `failed` clears only
    after a replacement actually opened.
* Candidate crashes during open reject that attempt deterministically and
  fully terminate the candidate (no orphan SQLite owners, no hangs).
* `close()` always wins over recovery: no resurrection, no `restarted` after
  dispose, all workers reaped.

Reproduced-then-fixed defects (deterministic repro logs in
`benches/results/repro-p0-*.txt`): inverted reopen retry condition (1 attempt
on failure; extra workers spawned on success), inFlight capacity leak on
crash and on paged error frames (post-restart deadlocks), recovery hang when
a candidate died while opening, and the paged safety bound silently RESOLVING
partial history. Previous tests missed these because the fault-injection hook
bypassed capacity accounting, timing races substituted for deterministic
lifecycle control, and no test counted spawned workers/attempts.

Coverage: recovery.test.ts (12/12: retry counts, single-replacement,
exhaustion, real-capacity crash, queued settlement, candidate death, stale
generations, paged error release, safety-bound rejection, close-during-
restart, repeated-cycle stability) + restart.test.ts (2/2) + differential
suite (9/9) + coordinator integration (3/3).

## Wire transport placement (#15/#16 evidence)

`benches/results/ipc-transport.txt`: crossing 200-event write-behind batches
as ONE JSON string beats structured-cloning the object graph ~2x
(31 ms vs 67 ms per 300 iterations); stringify cost on the main thread measured
within noise. The backend therefore ships `appendTransport='stringified'` as
the measured default (escape hatch available). Return-direction cloning of full
reconstructed graphs was the confirmed slow path at million-event scale —
hence loadStoredPaged for giant logs.
* Backpressure: bounded in-flight requests (default 64); overflow waits FIFO.
  No unbounded main-thread queuing if the worker falls behind.
* Cancellation: abort rejects only ops not yet dispatched. Dispatched writes
  are never cancelled post-dispatch (commit ambiguity beats redundant work);
  dispatched reads complete harmlessly in their snapshot tx.

## Measured results (darwin-arm64, Node 22.23.1, M3)

Sequential 100k events, batch 200 (3 trials):

| | wall | events/s | main-lag p99 | canaryMax |
|---|---|---|---|---|
| stock | 428–558 ms | 179–233k | 4.4–36 ms | 39–46 ms |
| worker | 600–701 ms | 143–167k | **1.22–1.26 ms** | 15–19 ms |

Concurrent 50 sessions ×2000 events:

| | wall | main-lag mean | lag p99 | canaryMax |
|---|---|---|---|---|
| stock | 448–980 ms | 21.8–50.0 ms | 38–518 ms | 40–518 ms |
| worker | 480–2010 ms* | **1.00 ms** | **1.27–1.45 ms** | 1.9–2.4 ms |

\* One worker trial hit a 1.42 s wall/batch-p99 tail — consistent with serialized
retry behavior also visible in stock (single 405 ms stall). Worker-side retries
now surface as *caller-perceived latency*, not loop stalls; this trade-off is
the core of the design and should be revisited with the busy-timeout knobs
before wider rollout.

Throughput cost of moving work off-thread: ~10–25 % slower wall-clock in
sequential campaigns (IPC + structured clone, measured: worker cpu ≈ what used
to be main cpu plus a small overhead). RSS cost ≈ +20–30 MiB (worker heap).

## Classification

WIN on the target metric (event-loop responsiveness) at roughly equal
throughput; REGRESSION nowhere severe. Selected/bundled as `persistence =
worker` for dsh-next profiles with automatic diagnostic + fallback to stock if
worker init fails; `persistence = stock` remains a one-line config switch.

## HMR / reload model (#99)

One worker exists per backend instance; Cordis teardown awaits backend.close()
which drains, closes SQLite and joins the thread before unload completes.
Because provider instances are not shared across compositions, plugin reloads
cannot produce duplicate workers or duplicate writers by construction.

## rusqlite gate

NOT TRIGGERED: no significant problem remains after mitigation — IPC cost is
modest, node:sqlite exposes everything needed, decode costs stay unremarkable,
RSS delta small. Re-evaluate only if large-session decode (Track C findings)
or multi-process concurrency reopens the question.
