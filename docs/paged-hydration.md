# Paged logical hydration — design + prototype results

Supersedes the upstream proposal's speculative framing with measured evidence.
(Proposal doc for maintainers: `docs/upstream-seam-proposal.md`; consumer audit:
`docs/session-scale.md`.)

## Target architecture

```text
            CANONICAL SESSION LOG  (sole authority)
                    │
        ┌───────────┴────────────┐
        ▼                        ▼
 PagedLogicalSource      Derived checkpoints      (future tranche)
 backend-generic         revision/prefix-keyed
 packed/lazy pages       projection/current-state
        │                        │
        └───────────┬────────────┘
                    ▼
             Session preparation
        hot suffix ∥ current derived state
                    ▼
                Agent ready
```

Full-history consumers migrate to explicit paged/streaming access instead of
forcing recovery to hydrate everything.

## Optimizer hierarchy (governing rule)

```
don't read it > don't decode it > don't materialize it >
cache exact derived state > move off-thread > optimize decoder
```

## Prototype: @dsh-next/paged-history (implemented)

Backend-generic interface (`PagedLogicalSource`), first implementation over DSH
SQLite schema v17 READ-ONLY with fail-closed guards (application id `DSHP`,
`user_version=17`). JSONL/future backends implement the same interface; core
never sees physical rows (#10).

Semantics locked by 8 differential tests against canonical loads:

* LOGICAL seq coordinates only (#78)
* page = events in `[startSeq, startSeq+limit)` intersected with byte budget;
  ≥1 event emitted when any exist; malformed in-range data refuses loudly (#13);
  predecessor-overlap decode-and-skip handles packed runs up to
  `MAX_PACKED_ROW_MEMBERS`
* end-of-log → short page + `endOfLogAt`
* fresh, top-level-frozen, unaliased pages per call; source retains nothing
  (#72/#73) — compatible with current fresh/unaliased restoration rules (#81)
* cancellation between physical rows

## Measured acceptance (1.09 M-event / 50 MB fixture)

| path | objects allocated | wall | CPU | RSS delta |
|---|---:|---:|---:|---:|
| full canonical load | **1,092,267** | 285 ms | 690 ms | +149 MiB |
| paged `readSuffix(4000)` | **5,002** | **1.9 ms** | **2 ms** | **+0.4 MiB** |

Architecture result: *not reading 996k events* beats every decode optimization
by two to three orders of magnitude on the axes that matter.

## Migration tranches (spec #114/#115/#87)

1. ✅ logical range-read source (this prototype)
2. first consumer migration — best out-of-tree candidate remains Session Query
   (whole-history clones per observation); Trajectory UI is already virtualized
   and its hydration path needs client-runtime verification first
3. model-facing checkpoint prototype (see docs/checkpoints.md design status)
4. lazy Session preparation via `PagedLogicalSource` — requires upstream seam
   (`prepareCore` publishes through `ctx.sessions.prepare`, coordinator.ts:905)
5. stop eager whole-materialization on ordinary cold resume

Stage-4/5 remain blocked by the public bridge contract exactly as documented in
resume-acceleration.md; acceptance numbers above quantify what the seam unlocks.
