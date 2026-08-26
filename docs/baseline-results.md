# Phase 0 baseline results and decision

Environment: Apple M3 (8 cores), macOS darwin-arm64, Node v22.23.1,
Rust 1.97.1 (release, lto). DSH at 0.1.1-rc.2. Raw logs in `benches/results/`.

## 1. Session persistence append (real SqliteStore, WAL + synchronous=FULL)

10k events:

| batch | trials wall | events/s | batch p50 | p99 | max |
|---|---|---|---|---|---|
| 50  | 61–81 ms   | 123–163k | 0.29–0.39 ms | 0.7–1.2 ms | ≤3 ms |
| 200 | 22–24 ms   | 419–454k | 0.40–0.41 ms | 0.9–1.9 ms | ≤2 ms |

100k events:

| batch | trials wall | events/s | batch p50 | p99 | max |
|---|---|---|---|---|---|
| 50  | 554–595 ms | 168–180k | 0.24–0.25 ms | 0.71–0.86 ms | ≤3.6 ms |
| 200 | 202–294 ms | 340–497k | 0.32–0.39 ms | 0.68–5.28 ms | ≤12 ms |

Concurrent sessions (2000 events each, 100-event batches):

| n | batch p50 | batch p99 | loop lag mean | lag p99 / max |
|---|---|---|---|---|
| 1  | 0.30 ms | ≤0.85 ms | 0.9–1.0 ms | ~1.2 / ~1.4 ms |
| 10 | 0.32–0.36 ms | 5.7–6.3 ms | ~3.6 ms | 5.7–6.3 ms |
| 50 | 0.30 ms | 22–25 ms | ~17.3 ms | 23–25 ms |

Reading: the write path is fast and mostly loop-friendly for realistic
single-session load (stalls ≈ per-batch commit cost). The measurable problem
is **serialized write contention**: ≥10 concurrent sessions on one DB turn
into multi-ms to tens-of-ms main-thread stalls because every transaction is a
synchronous BEGIN IMMEDIATE on the loop thread.

## 2. Cold load (full reconstruction) and suffix read

| scenario | events | wall | cpu | RSS peak |
|---|---|---|---|---|
| mixed-10k | 10 k | 2.2–5.1 ms | 4–11 ms | ~487 MiB proc total |
| mixed-100k | 100 k | 34–48 ms | 51–69 ms | +~100 MiB across trials |
| chunk-stream-50MB | 1.09 M logical | 249–755 ms | 474–1644 ms | ~650–966 MiB |

Suffix read (`loadStoredFrom` at mid-log): 10k ≈ 1.2–2.6 ms; 100k ≈ 41–96 ms;
50 MB stream ≈ 132–544 ms first-read effects tapering to ~150 ms.

Reading: cold load of very large packed histories is one long synchronous
main-thread stall (hundreds of ms) with heavy transient RSS — the same
"everything sync on main thread" story. It is real but bounded; users hit it
on resume of giant sessions, not continuously.

## 3. Compression (node:zlib zstd, as DSH calls it)

Realistic compressible payloads, 2000 ops each:

| payload | encodeData p50 | max | decode p50 | decode max |
|---|---|---|---|---|
| 8 KB (~43× ratio)  | 0.014 ms | 5.8 ms  | 0.006 ms | 0.44 ms |
| 64 KB (~370× ratio) | 0.028 ms | 0.97 ms | 0.015 ms | 2.7 ms |
| 1 MB (~3960× ratio) | 0.176 ms | 15.2 ms | 0.272 ms | 97.5 ms |

Batch simulation (one ~900 KB packed row + 60 scalar rows): 0.28–0.84 ms wall,
no observable drift.

Verdict: compression is **not** the bottleneck under typical operation; rare
decoding spikes up to ~97 ms at 1 MB exist but are infrequent cold-load events.
No Rust rewrite of zstd is justified — Node's zstd is already native, matching
the brief's warning.

## 4. OutputCollector vs optimized TS vs native pilot

Push throughput is never the problem (DSH's zero-copy push retains caller
buffers): all implementations push 200 MiB in seconds-fraction time. The hot
path is observer polling via `readFrom` against a filled 4 MiB window:

| impl | readFrom p50 | readFrom p99 | 10 observers × 3000 reads wall |
|---|---|---|---|
| DSH OutputCollector | 252–303 µs | 575–862 µs | 8.3 s |
| TS ReferenceByteJournal (same strategy) | 225–257 µs | 409–548 µs | 7.9 s |
| TS SegmentedByteJournal | **0.3–0.4 µs** | 0.5–0.6 µs | **13 ms** |
| Native Rust NativeByteJournal | 0.8–0.9 µs | 1.8–2.1 µs | 30 ms |

(3000 reads × ~280 µs ≈ 0.84 s of pure main-thread stall per observer per
window — multiplied by open readers this is where harness UI/jobs feedback
would visibly stutter.)

Journal microbenchmarks (16 MiB window):

| case | append ops/s ref/seg/native | tail readFrom ops/s ref/seg/native |
|---|---|---|
| 64 B chunks  | 31M / 19M / 6.8M    | 961 / 3.9M / 26k |
| 1 KB chunks  | 645k / 5.5M / 5.1M  | 2k / 4.1M / 119k |
| 64 KB chunks | 11.9M / 478k / 815k | 3.5k / 94k / 1.5k |

FFI batching (`appendBatch` of 512 buffers vs single crossings):
1.44× (64 B), 1.32× (1 KB), 2.85× (64 KB).

## 5. Analysis

* The dominant measured inefficiency is **read-path materialization**
  (concat whole window + UTF-8 decode per read). A structural fix exists that
  does not need Rust: offset-indexed segments returning zero-copy views. It
  beats both current DSH by ~500–700× and the native copy-out design by ~2×
  on pure reads, while native wins on write-heavy small-chunk mixes
  (5.1M vs 645k appends/s at 1 KB).
* Where does that leave the native journal? Its honest value proposition:
  predictable worst-cases, no GC pressure from view churn, and the option of
  async work off the JS thread later. But Phase 0 evidence says the simpler
  segmented TS journal already removes the user-visible stalls.
* SQLite persistence is healthy enough that a full native provider rewrite is
  unjustified by these numbers alone. The genuine pain point is concurrent-
  session write serialization + large-sync cold loads; an incremental,
  evidence-driven next step would be moving store transactions off the main
  thread (worker-owned connection or async backend), which can be attempted in
  TS first (`node:worker_threads` + `DatabaseSync` transfer or Atomics handoff)
  before any Rust.

## 6. Decision

**LIMIT RUST TO SPECIFIC COMPONENTS.**

* PROCEED (small, already proven by pilot + differential tests): keep the
  native bounded-journal pilot as a maintained component behind a feature
  probe; ship the segmented TS journal as the default fast path, native module
  optional where write-throughput-heavy collection matters.
* DEFER persistence provider rewrite until off-main-thread SQLite is prototyped
  in TS/workers and still shows measured deficits (loop stalls at N>~10
  sessions or huge-session resume stalls are the metrics to beat).
* DEFER process supervision pending its design note; nothing measured points
  at FFI here.
* REJECT rewriting: compression, jobs registry, wait runtime, agent loop,
  Cordis (no measurable value; high integration risk).

This satisfies the acceptance bar of §48/§56: if Rust loses or barely wins
while increasing complexity substantially, we stop — and today that is exactly
what the data says for everything except the narrowly scoped journal pilot.
