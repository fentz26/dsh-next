# Session continuation ("Continue-As-New") — research note

Status: RESEARCH ONLY — no implementation. Feasibility: compatible with DSH
semantics via composition; several semantic decisions belong to DSH core.

## Concept

A long-lived *logical agent* maps to many bounded physical sessions:

```
logical execution id E
  ├─ session A (bounded history)
  ├─ session B …
```

Each physical log stays resumable-paged (docs/paged-hydration.md); archival
history remains canonical and queryable forever — continuation bounds only the
ACTIVE log.

## Distinct from fork

Fork/seed carries parent ancestry into a child for purposes like teaming or
retry (parentSession/seedLength semantics). Continuation instead moves EXECUTION
forward to a fresh empty log while carrying explicit forward state. New metadata
vocabulary (do NOT reuse parentSession unless proven equivalent):

```
continuationOf?: SessionId
continuedBy?: SessionId
logicalExecutionId: string
```

## Safe boundaries

Only closed units roll over: `turn/end` (incl. reason classification), goal
round completion, post-compaction boundaries, explicit model/user continuation,
or a history-size threshold evaluated between turns. Never mid-turn/mid-tool.

## Carry-forward set (candidate)

Model-visible compacted context · cwd/execution-world reference · agent preset ·
objective/goal ids · durable schedules/waits ownership transfer notes · lineage
links. Never process-local handles (Jobs handles, PTYs, controllers).

## Interactions

* Telemetry adoption replay uses `firstLiveSeq` (core index.ts:450–472) — a new
  session is just another root from telemetry's view; logical linking is extra
  metadata, preserving telemetry semantics.
* Wait histories live in each physical log already (fold per session); cross-
  session wait continuity must re-register providers against the new session.
* Compaction interacts BEFORE rollover: prefer compaction when total-inbound
  still fits; continue-as-new when growth is structural.

## Blockers / next steps

1. Upstream agreement on metadata vocabulary + whether `continue` belongs near
   compaction policy.
2. UI/Trajectory lineage rendering expectations.
3. Prototype only after paged preparation lands (waking the NEW session must be
   cheap for the feature to pay off).
