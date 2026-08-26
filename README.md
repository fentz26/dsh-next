# dsh-next

Performance-oriented backend/runtime modernization layer for DeepSeek Harness
(DSH). **Rust where benchmarks justify it, TypeScript everywhere else.**

dsh-next is an out-of-tree plugin repository. It does NOT fork or rewrite DSH.
It replaces selected execution/data-plane capability providers through normal
Cordis/profile/bundle composition, while DSH remains the control plane:

```text
DeepSeek Harness TypeScript control plane   ← unchanged
├─ Cordis plugin graph / session semantics / Agent Loop
├─ Jobs registry / Wait orchestration / model+tool contracts
│         │
│         ▼  existing capability seams (ctx.sessionPersistence, ctx.subprocess, …)
└─ dsh-next
   ├─ thin TypeScript adapters
   └─ Rust/native execution plane   ← only where measured wins exist
```

## Non-negotiable principles

1. **Benchmark-first policy.** No rewrite before evidence. A component that
   benchmarks the same or worse than its TS counterpart stays in TypeScript.
2. **No control-plane rewrites.** Cordis, agent-loop, jobs registry, wait
   runtime, session event semantics, browser UI are out of scope permanently
   unless profiling produces extraordinary evidence.
3. **Semantics preservation.** Any native provider must preserve DSH contracts
   exactly (persistence durability/recovery rules, subprocess termination
   trees, credential scrubbing, `DSH_*` env namespace, …).
4. **Fail-safe fallback.** Native acceleration that is unavailable must
   degrade explicitly to stock providers — never silently claim acceleration.
5. **No new event bus.** Native components stay behind service boundaries;
   JS-facing coordination remains on Cordis/Session seams.

## Repository layout

```text
docs/
  architecture.md             audit + boundary design (Phase 0)
  benchmark-methodology.md    how benchmarks are built and run
  baseline-results.md         Phase 0 measurement tables + analysis
  journal-pilot.md            native journal pilot: API, tests, verdicts
  persistence-provider-design.md  future ctx.sessionPersistence provider notes
  supervisor-design-note.md   persistent process supervisor feasibility (design only)
packages/journal/             BoundedByteJournal: TS reference + optimized candidate
crates/native-journal/        Rust napi-rs pilot (NativeByteJournal)
benches/                      reproducible benchmark harness (run against real DSH)
```

## Running

Benchmarks need a DeepSeek Harness checkout (built workspace):

```bash
DSH_ROOT=~/deepseek-harness pnpm bench              # everything
DSH_ROOT=~/deepseek-harness pnpm bench:persistence  # sqlite append/concurrency
DSH_ROOT=~/deepseek-harness pnpm bench:cold-load    # session reconstruction
DSH_ROOT=~/deepseek-harness pnpm bench:compression  # zstd encode/decode stalls
DSH_ROOT=~/deepseek-harness pnpm bench:collector    # OutputCollector stress
DSH_ROOT=~/deepseek-harness pnpm bench:journal      # journal strategies + FFI batching
pnpm test                                           # TS + native differential tests
```

The native module is built separately today (Phase 0 pilot):

```bash
cd crates/native-journal && cargo build --release \
  && cp target/release/libdsh_next_native_journal.dylib pilot.node
```

## Phase 0 status

See [docs/baseline-results.md](docs/baseline-results.md) for measured results
and the final decision record. Short version:

* SQLite persistence: healthy throughput (350–500k events/s at realistic
  batch sizes); loop stalls ≤ ~5 ms single-session; ~17–25 ms loop lag under
  50 concurrent sessions. Compression is not the bottleneck.
* The measured structural hotspot is **read-path materialization**
  (`OutputCollector.readFrom` concatenates the whole window per read:
  ~250–300 µs/call vs ~0.3–1 µs for segment-based designs ≈ 300–700×,
  seconds of main-thread time under observer polling).
* An optimized **TypeScript** segmented journal captures most of that win; the
  Rust pilot matches semantics and is competitive on mixed workloads but does
  not dominate the best TS on pure reads (zero-copy subarrays beat the FFI
  copy-out).

Decision: **LIMIT RUST TO SPECIFIC COMPONENTS** — see the results document.
