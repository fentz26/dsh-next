# Benchmark methodology

## Principles

* Compare against the **real DSH implementations**, imported directly from
  the checkout (sources are exposed via the packages' `./src/*` exports), not
  re-implementations — except `encodeData/decodeData`, which are module-private
  in DSH and replicated byte-for-byte from the audited code (threshold 4096 B,
  zstd level 3, smaller-of logic).
* No debug-vs-release apples/oranges: Rust is built with `--release` (lto),
  benchmarks run on stock Node.
* Warm up before measuring (2k warmup iterations on journal microbenches);
  steady-state only. Startup is never measured here.
* Multiple trials per scenario (`BENCH_TRIALS`, default 3); report best and
  spread where relevant.
* Prefer percentiles over means; p50/p95/p99/max for operation latency,
  plus event-loop lag percentiles.

## Metrics collected for every scenario

| Metric | How |
|---|---|
| wall clock | `process.hrtime.bigint()` deltas |
| CPU time | `process.cpuUsage()` delta vs pre-workload baseline |
| RSS / heap | `process.memoryUsage()` at measurement end |
| per-op latency | `hrtime` around each call → p50/p95/p99/max |
| event-loop lag | three complementary probes (below) |
| throughput | derived ops/s or MiB/s |

## Event-loop lag probes

A synchronous-stretch workload can complete entirely inside microtasks, so a
lag probe that itself needs a macrotask turn would never fire (this was
observed and is exactly why naive setups report "0 lag"). The harness therefore
uses:

1. `perf_hooks.monitorEventLoopDelay` histogram (records loop turnaround).
2. A 1 ms interval timer computing scheduling drift (`maxDriftMs`).
3. Two `setImmediate` canaries recording inter-tick delay > 1 ms
   (`canaryP95/P99/Max`) — they observe stalls whenever any macrotask boundary
   exists around the workload.

Important interpretation rule: **for fully-synchronous single-call workloads
(cold load, one SQLite transaction) the honest stall measure is the wall time
of the call itself** — the canaries can only fire if the workload yields. The
persistence benches drive each batch through one macrotask boundary
(`setImmediate`) because real DSH drains batches from timer callbacks; this
lets probes observe realistic per-batch stalls.

## Reproducing environment facts

The runner prints CPU model, Node version, platform, and DSH root. Machine
details of the recorded Phase 0 baseline: Apple M3 (8 cores), macOS
(darwin-arm64), Node v22.23.1 (node:sqlite available natively), Rust
1.97.1 release builds for the native pilot, tmpdir-backed SQLite files
(WAL + synchronous=FULL as configured by DSH itself).

## Workload sizes (pinned)

* Persistence append: mixed realistic logs at 10k and 100k events, batches of
  50 and 200 events (DSH's write-behind window produces batch sizes in this
  range in practice). Concurrency: 1/10/50 sessions appending to one database
  file, 2000 events each, 100-event batches.
* Cold load: 10k events, 100k events, chunk-heavy stream ~50 MB logical text
  (~1.09 M packed-eligible chunk events). Larger streams scale by row count;
  50 MB was chosen to keep trials fast enough for several repetitions.
* Compression: 1 KB / 8 KB / 64 KB / 1 MB payloads of realistic compressible
  log-like JSON text (ratio ~40–300×; random bytes reported separately since
  they defeat compression entirely), 2000 ops each, decode bounded by
  `maxOutputLength` like DSH does.
* Collector/journal: 64 MiB stream pushed in 8 KiB chunks against a 4 MiB
  window (collector bench); 64 B / 1 KB / 64 KB appends × 100k against a
  16 MiB window (journal bench); observer polling at tail offsets; 100
  concurrent collectors × 2 MiB.

## Known limitations

* The concurrent-sessions scenario exercises store-level contention but not
  the full Cordis write-behind coordinator; coordinator overhead is constant
  bookkeeping and identical across native/TS providers by design.
* Chunk-stream cold-load beyond 50 MB is extrapolated, not measured.
* Windows/Linux numbers will differ; Phase 0 records darwin-arm64 only.
