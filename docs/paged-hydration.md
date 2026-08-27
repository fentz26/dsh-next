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

## Resume-mode measurements (milestone task #5)

| mode | objects materialized | wall→agent-ready | main CPU | RSS Δ |
|---|---:|---:|---:|---|
| legacy fromRestore(all) | 1,000,001 | 524.7 ms (stall) | 1112 ms | +199.7 MiB |
| checkpoint-prefix(excl chunks)+suffix | **4,117** | **22.9 ms** | 38 ms | +1.7 MiB |
| hot cache (pre-deserialized) | 4,117 | **14.2 ms** | 19 ms | +1.5 MiB |

Fresh-run repetition (this milestone's final acceptance, Node 22 / M3):

```
resume-legacy-full-restore | events=1,000,001 wall=580.94ms cpu=1158ms rss=+216.7MiB
resume-checkpoint-suffix   | events=    4,117 wall= 28.33ms cpu= 47.9ms rss=+3.0MiB rows=117
resume-hot-cache           | events=    4,117 wall= 15.39ms cpu= 20.6ms rss=+1.5MiB
```

## Paging vs full hydration — final metric set (`paged-acceptance`)

Acceptance gate for task #1 — a ~4k logical page against the 1.09 M-event /
50 MB canonical log:

| metric | full load | paged `readSuffix(4000)` |
|---|---:|---:|
| materialized events | **1,092,267** | **4,000** |
| inspected logical (incl. skip window) | 1,092,267 | **4,001** |
| decoded logical incl. skipped predecessors | 1,092,267 | 5,002 |
| physical rows touched | 2,188+ | **11** |
| compressed bytes read from SQLite | ≈50 MB | **11,544 B** |
| decoded payload bytes | ≈30 MB | 403,512 B |
| wall | 455 ms | **5.8 ms** |
| CPU | 998 ms | **21 ms** |
| end-of-op RSS Δ | +151.7 MiB | **+1.5 MiB** |
| loop block | full call | 5.8 ms |

Both paths are synchronous here; "loop block" equals wall time by
construction. The gate holds with three orders of magnitude of headroom:
a 4k page never inspects or materializes the ~1M-event log.

Storage-plane lesson encoded in code: packed rows carry TAG types
(text-chunks/…), so storage-level type exclusion must map logical types to their
packed tags — otherwise 'don't even read it' silently degrades into reading
everything.

## Migration tranches (spec #114/#115/#87)

1. ✅ logical range-read source (this prototype)
2. ✅ first real consumer migrated: the raw-artifact export producer
   (`export-consumer.ts`) streams byte-identical JSONL through paged ranges
   with bounded chunks and preserves `readRaw`'s absent-session fallback
   (differential test 3/3). Remaining full-history consumers (Session Query
   snapshots, Trajectory hydration) migrate on the same pattern.
3. ✅ model-facing checkpoint prototype — `@dsh-next/checkpoint-replay`
   chunk-filtered `distill()` prefix validated against REAL Session machinery
   (`equivalence.test.ts` 6/6: messages + requestHeader identical at multiple
   boundaries; corrupt/future-version checksum fail-open to replay;
   unknown-required events pass verbatim rather than being skipped).
4. ⛔ lazy Session preparation via `PagedLogicalSource` — requires upstream seam
   (`prepareCore` publishes through `ctx.sessions.prepare`, coordinator.ts:905)
5. ⛔ stop eager whole-materialization on ordinary cold resume

Tranche-3 design constraint worth reusing upstream: checkpoint rows cannot live
in the canonical SQLite file (an auxiliary-table write from a second connection
trips `validateSchemaForMutation`); storage must be a provider-owned sidecar.

Stage-4/5 remain blocked by the public bridge contract exactly as documented in
resume-acceleration.md; acceptance numbers above quantify what the seam unlocks.
