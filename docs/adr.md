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

## ADR-006: Decode optimization alone is insufficient; paged source precedes snapshots

* **Problem**: giant-session resume stalls allocate >1M JS objects; earlier phase split decode into 41/23/36% stages with no single hotspot.
* **Evidence**: expansion-attribution bench (8-pass); consumer audit shows agent-ready state derives from surface nodes only — raw chunks unnecessary for continuation (core/session index.ts:729,704–750).
* **Decision**: build backend-generic PagedLogicalSource (packages/paged-history) instead of snapshotting or faster decoders. Snapshot-only designs still materialize the whole events[] graph — the actual bottleneck.
* **Consequences**: acceptance demo 5,002 objects vs 1,092,267 and 1.9 ms vs 285 ms for suffix reads on the 1M fixture; full materialization remains available explicitly.
* **Revisit if**: upstream adopts/declines the seam proposal.

## ADR-007: Consumer-audit gates migration order

* **Evidence**: docs/session-scale.md classification table — identity-sensitive consumers (telemetry-otel seq check), seq-indexed increments (token meter), per-call FULL folds (plan-mode/title/schedule), query double-clones.
* **Decision**: do not move consumers onto async paging without first-class sync guarantees they rely on today; Session Query flagged as first independent beneficiary; plan-mode folds are checkpoint candidates v2.
* **Consequences**: safe incremental migration per tranche list; compatibility materializer stays for identity/exact-array users (#50/#52).

## ADR-008: Checkpoints stay derived; conservative prefix invalidation v1

* **Problem**: full-log revision changes every append ⇒ naive revision keys invalidate constantly.
* **Decision**: generate at turn/end with invalidation-on-any-mutation policy initially; stronger prefix tokens (hashes/commit markers) deferred to upstream semantic review (docs/checkpoints.md).
* **Revisit if**: measured wake costs at realistic cadences make stricter tokens necessary.

## ADR-009: Windowed restore seam shipped upstream-side FIRST, minimal width

* **Problem**: prepareCore→ctx.sessions.prepare demands contiguous seed[] from 0.
* **Evidence**: all four resume-mode measurements; 881-test green regression sweep on patched core.
* **Decision**: land the NARROWEST seam on branch dsh-next/lazy-session-seam — RestoredHistoryWindow/fromRestoreWindow preserving canonical seqs — WITHOUT yet inventing checkpoint-injection plumbing inside core.
* **Consequences**: lazy-window gives canonical-id Sessions cheaply but partial derived state; complete agent-ready semantics continue via the validated dense-rebase checkpoint until fold-state injection tranche lands.
* **Revisit trigger**: upstream PR review outcomes + telemetry/token-meter E-class migration patches.
