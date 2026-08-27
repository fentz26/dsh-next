# Checkpoints — design status (research/prototype stage)

Checkpoints accelerate *derived current state*; they NEVER replace the canonical
log (sole authority) and remain derived/rebuildable/discardable artifacts.

## What would be checkpointed first (and why it is safe)

`deriveMessages()` folds surface nodes ONLY (`user/message`, non-empty
`assistant/message`, `tool/result`) incrementally with cursor state
(core/session index.ts:729,704–750) — raw chunks never participate. A
model-facing checkpoint therefore stores the materialized message list + fold
cursor + requestHeader/requestContext fold states (same incremental pattern,
index.ts:673–702). Byte equivalence target: messages_full_replay ===
checkpoint+suffix replay (#116/#57).

Precedent inside DSH: session-projection already persists its own checkpoints
with FULL fallback (session-projection index.ts:444–446), validating this shape.

## Identity & validity

* Identity keys: sessionId · source backend identity · source revision
  (`SessionPersistenceRevision`) · prefix end seq K · checkpoint format version.
* **Prefix identity problem:** full-log revision changes on every append;
  conservative v1 policy = invalidate on ANY source mutation. Generation at
  `turn/end` boundaries makes this cheap in practice (a stale-by-one-turn
  rebuild equals one turn's suffix).
* Stronger prefix tokens (prefix hash / durable prefix commit markers) are noted
  for upstream discussion (#20) — not invented locally without core review.

## Storage plan

SQLite: separate `dsh_next_checkpoints` table (never mixed into event rows);
JSONL: provider-owned sidecar file `<session>.checkpoint`. Bounded payload with
size metric `checkpointBytes / logicalBytes` recorded at write time (#61–64);
checksum for fail-open-to-rebuild corruption handling (#109).

## Crash & failure semantics (#59/#105)

Crash before/during/after checkpoint write: canonical log unaffected either
way; partial writes discarded by checksum/version checks; interrupted-turn
repair mutates the tail AFTER K ⇒ any mutation of events ≤ K invalidates.
Checkpoint failures never fail appends (#22).

## Current implementation status

NOT implemented. Blocking prerequisite: runtime-facing checkpoint consumption
requires the sessions bridge (`sessions.prepare`) that only exists in-process
in DSH core; a cache without a consumer cannot demonstrate acceptance. Design,
safety rules and measurable size/cost gates are fixed here so implementation
follows immediately once the paged-seam lands (tranche order docs/paged-hydration.md).
