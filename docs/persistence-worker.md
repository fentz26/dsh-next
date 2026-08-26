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

## Failure semantics

* Worker `error`/premature exit ⇒ every pending request rejects with typed
  `WorkerPersistenceError`; backend enters failed state; further calls fail
  fast. **No automatic restart** (deterministic failure first; restart is a
  later, separately-gated feature).
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

## rusqlite gate

NOT TRIGGERED: no significant problem remains after mitigation — IPC cost is
modest, node:sqlite exposes everything needed, decode costs stay unremarkable,
RSS delta small. Re-evaluate only if large-session decode (Track C findings)
or multi-process concurrency reopens the question.
