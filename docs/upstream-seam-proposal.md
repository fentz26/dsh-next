# Upstream seam proposal: paged historical hydration for giant sessions

Status: DRAFT for DSH maintainer review. Not implemented anywhere; no dsh-next
code depends on it existing. Backed by measurements in
[resume-acceleration.md](resume-acceleration.md) and
`benches/results/resume-*.txt`.

## Problem (measured)

A session log of ~1.09 M logical events (~50 MB text) reconstructs in 202–498 ms
as a single synchronous main-thread stall with ~+400–600 MiB peak RSS. >99% of
those events are packed `assistant/chunk` deltas required for replay/UI fidelity
but not for an agent's next model request. Off-main-thread reconstruction was
prototyped two ways in dsh-next: single-frame transfer regressed to 2.4–3.1 s,
paged frames still cost 1.7–2.2 s — transferring any fully-expanded graph is
strictly more expensive than building it locally.

## The missing seam

Public `SessionPersistence.prepare/load/inspect` require fully materialized,
frozen event graphs (`session.events` synchronously complete). Nothing allows a
backend to hand the runtime *pages* of immutable history while the active tail
materializes eagerly.

## Proposal sketch (generic, non-breaking)

An OPTIONAL capability on persistence backends:

```
interface PagedHistoryBackend {
  // Eager suffix = everything from seq >= cut; history served on demand.
  readonly supportsPagedHistory?: true
}
interface SessionHistoryPage {
  events: readonly SessionEvent[]      // frozen, unaliased
  nextPageOffset?: number              // undefined = end reached
}
session.history.pages(pageSize, signal?): AsyncIterable<SessionHistoryPage>
session.history.hydrateThrough(seq): Promise<void>   // awaits materialization
```

Invariants preserved:

* Session event log remains the SOLE authority; pages are projections with
  backend-guaranteed contiguity (existing coordinator rules).
* Synchronous-completeness of current APIs unchanged for backends that do not
  declare the capability.
* Format refusal / ignorable unknown events validated per page exactly as for
  whole logs — unknown REQUIRED plugin events reject identically whether lazy
  or eager.
* Replay/UI consumers simply iterate pages; time-to-agent-ready becomes bounded
  by eager-suffix size instead of total history length.

Estimated effect (from measured stage breakdown): agent-ready resume bounded by
~tens-of-ms decode + page fetch instead of hundreds of ms; RSS delta drops from
sub-GB to active-tail-sized.

## Why upstream, why minimal

The bridge (`sessions.prepare` publishing frozen Sessions) lives entirely inside
DSH core; no out-of-tree provider can substitute it without violating Session
authority. The above adds one optional interface + one iteration helper — it does
not move orchestration logic or mandate Rust/native anything.
