# Resume acceleration for giant sessions (Track C)

Status gate: **ARCHITECTURAL SEAM REQUIRED** — see final section. This doc
records the measured breakdown, why each shortcut is currently blocked by
Session/public contracts, and the acceleration designs ready once seams land
(or viable provider-side with a different risk profile).

## Measured breakdown (~1.09 M logical events / 50 MB stream, Node 22, M3)

| stage | wall | share of story |
|---|---|---|
| S1 raw `SELECT … FROM events` (2,188 packed rows) | 2–19 ms | negligible |
| S2 sqlite-value → EventRow decode | ≤1 ms | negligible |
| S3+S4 decode+expand+committed-prefix validation | ~250–400 ms | **dominant** |
| peak RSS delta during reconstruction | ~+400–600 MiB | dominant |

Phase 0 whole-op cold loads bracket these numbers (249–755 ms wall,
~650–966 MiB RSS), so attribution is consistent across both harnesses:
the cost is NOT storage access, compression, or validation bookkeeping — it is
**materializing >1M live JS event objects eagerly**, of which >99 % here are
packed `assistant/chunk` deltas needed for UI/replay fidelity but not for the
agent's next model request.

## Why this is not fixable out-of-tree today

Audited contract chain (`docs/architecture.md` §1.1):

* `load(id)` must return a complete `SessionInspection`; prepared Sessions are
  frozen and published in place (coordinator lines 144–171, 720–787).
* Consumers assume `session.events`/inspection graphs are synchronously
  materialized. There is no paging/lazy-event seam on the public surface.
* `PersistenceBackend.loadStored` shapes must remain fresh unaliased graphs —
  backend-level laziness would leak representation choices upward.

Therefore every dsh-next-side trick still pays eager expansion; a checkpoint
cache can only shave the minority slice (zstd decompress + varint + JSON
parse ≈ small fraction vs allocation/graph construction) unless consumers can
defer hydration.

## Acceleration designs (ranked, evidence-driven)

### 1. Worker-owned reconstruction (measured: stall gone, wall regresses)

Measured on the 1.09 M-event fixture after implementing both variants:

| variant | loadStored wall | main-thread lag | verdict |
|---|---|---|---|
| stock synchronous | 202–498 ms | entire call blocks | baseline |
| worker, single clone frame | 2.4–3.1 s (~10×) | ~1 ms flat | regression |
| worker, paged frames (20k/frame) | 1.7–2.2 s | ~1 ms flat | better, still >2× |

`loadStoredPaged` streams fixed-size event pages (differential-tested 9/9);
structured-clone cost scales with object count regardless of framing, so
*transferring* the expanded graph costs strictly more than building it
in place. Conclusion: graph transfer is a dead end at this scale; only the
upstream seam below (consumer-side lazy hydration, no bulk transfer) or
byte-level transports paired with consumer contracts that avoid expansion
can deliver the structural win.

### 2. Upstream seam proposal: paged immutable suffix access

If DSH exposed an optional capability like "prepared sessions may source
historical ranges lazily", then:

```
resume:
  metadata/index eagerly          (seq boundaries, types, physical offsets)
  active-suffix materialized      (events ≥ last turn/start or seed length)
  historical pages hydrated on demand / in background
```

Requirement would be genuinely generic (any large-log consumer benefits),
non-breaking behind an interface extension, versioned via format refusal.
This is where the big time-to-agent-ready win lives (>99 % of chunk events can
defer); raw chunks retained — never dropped.

### 3. Derived checkpoint cache (provider-side, cache-only)

Persisted under the SAME provider database in clearly-marked rebuildable
tables (`dsh_next_checkpoints`, separate from canonical `events`), storing
folded projections keyed by {sessionId, SESSION_FORMAT_VERSION, source
revision, prefix end seq, checkpoint format version}. Any mismatch/corruption ⇒
drop silently, full replay wins (fail-safe). Registered-fold serializers only
for core folds today (model messages/header/context); unknown required plugin
events disable checkpointing for that session rather than ever skipping them.
Not built yet — it accelerates *projections*, not `session.events`
materialization, so its value depends on seam #2 landing first. Ordering chosen
accordingly.

## Consistency/failure rules (apply whenever implemented)

Checkpoint refers to sessionId + format version + source revision + prefix seq
+ own format version (+ optional checksum). Generation asynchronous/off-thread
after idle/durable milestones (never per append). On mismatch: discard +
rebuild from canonical log. Canonical log remains sole authority; cache entries
are always regenerable.

## Gate rationale

`RESUME ACCELERATION PROCEED` is not claimable because the structural win
requires consumer-side hydration semantics outside dsh-next's control.
`NOT JUSTIFIED` is false too: the breakdown justifies pursuing seam #2 with
measured stakes (>100× time reduction potential for agent-ready resume, GBs
saved at larger scales). Hence: **ARCHITECTURAL SEAM REQUIRED**, with
worker-reconstruction (design 1) delivered meanwhile.
