# Architecture Decision Records (condensed)

Format: problem → evidence → alternatives → decision → consequences → revisit trigger.

## ADR-001: Default journal is segmented TypeScript, not native

* **Problem**: OutputCollector's concat-on-read measured 252–303 µs/read; need a bounded journal primitive for dsh-next providers.
* **Evidence** (Phase 0, darwin-arm64): segmented TS 0.3–0.4 µs/read; Rust napi 0.8–0.9 µs; 30k observer polls: DSH 8.3 s / TS 13 ms / native 30 ms.
* **Alternatives**: ship native as default; patch upstream collector; do nothing.
* **Decision**: `segmented-ts` default via `selectJournal()`; native stays experimental behind probe.
* **Consequences**: zero-copy JS reads win; no FFI overhead/GC-free guarantee but measurably better everywhere it matters.
* **Revisit if**: write-heavy small-chunk mixes dominate real workloads (native append was ~8× at 1 KB), or upstream adopts the utility extraction.

## ADR-002: Worker threads before rusqlite

* **Problem**: synchronous main-thread SQLite stalls (p99 38–518 ms loop lag at 50 concurrent sessions).
* **Evidence** (Track B): worker backend flattens main-loop lag to ~1.0 ms mean / ≤1.5 ms p99 across all scenarios at 10–25 % throughput cost and ~+25 MiB RSS; differential suite green incl. crash injection and torn-tail parity.
* **Alternatives**: rusqlite provider now; accept stalls; async sqlite crate.
* **Decision**: worker-owned `node:sqlite`; rusqlite gate NOT triggered (no significant residual problem: IPC modest, API sufficient, RSS delta small).
* **Consequences**: fail-fast failure semantics instead of auto-restart (later, separately gated); one more thread to dispose.
* **Revisit if**: Track C giant-session decode proves node:sqlite decode path is the residual bottleneck, or packaged IPC cost regresses.

## ADR-003: Checkpoints are derived cache only; big resume win needs an upstream seam

* **Problem**: ~1.09 M logical events reconstruct in 250–755 ms with ~GB-class RSS; log must remain sole authority.
* **Evidence** (Track C profiler): storage read/decode negligible; eager expansion into >1M JS objects dominates. Public prepare/load contracts force full materialization.
* **Alternatives**: opaque snapshot replacing replay; lazy hydration purely in dsh-next.
* **Decision**: checkpoint tables = rebuildable cache with revision/format keys and silent-discard-on-mismatch; pursue a generic paged-hydration seam upstream first; meanwhile deliver worker-side reconstruction so resumes stop blocking the loop.
* **Consequences**: no headline wall-time/RSS win claimable today; no risk of second-authority divergence or silently skipping unknown required plugin events.
* **Revisit if**: upstream accepts/exposes pagination seam.

## ADR-004: Jobs registry and Waits stay TypeScript

* **Problem**: none measured — both are orchestration/bookkeeping, never computational hotspots.
* **Evidence**: Phase 0 audit — Map/Set state, listener tables, Cordis scopes; moving them across FFI buys nothing and breaks owner-relative delivery semantics.
* **Decision**: keep TS; native may sit *under* them (supervisor) later.
* **Revisit if**: profiling ever shows otherwise (none expected).

## ADR-005: Supervisor is a sidecar, not N-API

* **Problem**: restart-resilient subprocess ownership requires surviving Node crashes; addon crashes would take the harness down with it.
* **Evidence**: blast-radius asymmetry (docs/supervisor-design-note.md); contract complexity table shows high regression surface.
* **Decision**: deferred sidecar design; authenticated local IPC, opaque process ids, versioned framed protocol, PTY last.
* **Revisit if**: Tracks A–C stabilize AND a validated use case demands survive-restart jobs.
