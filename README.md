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

## Program status

Evidence-gated tracks (gates in each doc; ADRs in [docs/adr.md](docs/adr.md)):

| Track | Status | Doc |
|---|---|---|
| A — Segmented journal | **JOURNAL READY** — `segmented-ts` default, native experimental | [journal.md](docs/journal.md) |
| B — Worker persistence | **PROCEED** — main-loop lag ~1 ms vs 17–50 ms; differential suite green | [persistence-worker.md](docs/persistence-worker.md) |
| C — Giant-session resume | **ARCHITECTURAL SEAM REQUIRED** — expansion dominates; designs ranked | [resume-acceleration.md](docs/resume-acceleration.md) |
| D — Projection caches | absorbed into Track C design (revision-keyed cache rules) | resume-acceleration.md |
| E — Native primitives | narrowly scoped (journal pilot only); per-ADR-001 rules | adr.md |
| F — Durable execution | state model/commit boundary/crash classes specified; no code yet | [durable-execution.md](docs/durable-execution.md) |
| G — Supervisor | design note only (sidecar rationale) | [supervisor-design-note.md](docs/supervisor-design-note.md) |
| H — Diagnostics/benchmarks | `pnpm diagnostics`, `pnpm bench [--json]` | benchmark-methodology.md |
| I — Compat/distribution | version floor + degradation matrix documented | [compatibility.md](docs/compatibility.md) |

Phase 0 history and baseline tables: [docs/baseline-results.md](docs/baseline-results.md).
