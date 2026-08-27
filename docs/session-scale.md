# Session-scale dependency map (mandatory audit, spec #2/#3)

Source: full consumer audit of DSH HEAD (`0.1.1-rc.2`, `b150a551b8`).
Key: FULL = needs whole array · SUFFIX = range/slice · RA = random access by seq
· PROJ = fold/projection · IDENTITY = exact array/object identity matters ·
SYNC = breaks if made async.

## The decisive finding

**`deriveMessages()` never reads raw deltas.**
`packages/core/session/src/index.ts:729` walks only *surface nodes* incrementally
(`surface.nodes`, cursor-based, per-call cost = new nodes only; rebuild on
rewrite). Projection (core/session/src/surface.ts:83–114) consumes exactly:
`user/message`, non-empty `assistant/message`, `tool/result`.
Chunk/boundary/log-only events derive to `null`.

Consequence for the giant fixture: **~1.09 M packed chunk events are pure
materialization overhead for agent-ready resume.** They matter for replay/UI
fidelity, not for continuing execution.

Related incremental folds already exist inside Session: `requestHeader()`
(index.ts:673–683) and `requestContext()` (694–702) — same cursor pattern.

## Consumer classification

| Consumer | Path | Access | Notes |
|---|---|---|---|
| Session `_forkSeed` | core/session index.ts:1101–1140 | RA + SUFFIX(prefix) + findLast | seed boundary semantics |
| Agent Loop | core/agent-loop agent.ts:92 findLast('turn/start'); runtime-context.ts:36–44 reverse scan w/ surface.nodes | recent-event scans SYNC | dev-only invariant does FULL fold |
| agent inbox | core/agent/src/inbox.ts:32 | SUFFIX from `seedLength`, one-time iterate | |
| Token meter | llm/token-meter index.ts:174–179,297 | incremental ITER + RA indexed by seq | portable to indexed source |
| llm-retry | llm-retry index.ts:182; invariant.ts:150–164 | recent RA + FULL(dev-only) | |
| Goal | goal index.ts:425,438; tool-goal authority.ts:31 | PROJ incremental + suffix | full seed fold once |
| Schedule | schedule index.ts:51; tools.ts:224; runtime.ts:209 | FULL arg | caching unverified (unknown) |
| Wait folds | wait/wait index.ts:358 | FULL arg → single fold | durable wait history independent |
| Teams (experimental) | experimental/agent-team journal.ts:31; mailbox/roster slice(seedLength) | FULL fold + SUFFIX | |
| Plan mode | plan-mode index.ts:249…501 | FULL re-fold PER CALL sync | hot, folding candidate |
| Compaction | compaction-basic region.ts:163,306,503–507; tool-pairing.ts:59 | RA-by-seq + reverse scans | keying by shadowedSeqs |
| Coordinator itself | coordinator.ts:742,918,975,984,989,1174,1196 | length check / snapshot borrow / suffix clone | owns current guarantees |
| session-projection | session/session-projection index.ts:457–494 | PROJ with checkpoint restore + FULL fallback | ALREADY has checkpoint persistence! (444–446) |
| Session title | session-title index.ts:350…760 | FULL folds + findLast | |
| Telemetry | session-telemetry coordinator.ts:142 adoption replay; otel index.ts:247 | FULL iter + **IDENTITY**: compares event refs to `session.events[seq]` | identity requirement lives here specifically |
| Session Query | session-query corpus.ts:242,295; session-query-sqlite index.ts:856–870,790–835 | FULL clone per observation + FTS union | heaviest quadratic-ish consumer on giant sessions |
| ApiProxy/host | apiproxy api-proxy.ts:451,498–504,1384–1404,3494 | some() / list-metadata fold / recent pairing scan; export uses RAW artifacts not .events (:3580+) | |
| Subagent drivers | subagent*/lifecycle.ts:196 etc | boundary=length; own suffix slices | |

## Cross-cutting contract facts

* `session.events` is a cached frozen copy of the internal log until next append
  (core index.ts:551–562); events deep-frozen at acceptance (append :630,
  restore freezeRestoredObject :536).
* Publish path: coordinator `prepareCore` → `ctx.sessions.prepare(id,{seed,…})`
  (coordinator.ts:905); `Session.fromRestore` validates envelopes then freezes.
* Prepared-session cache: **count-based**, default size 5
  (coordinator.ts:27, preparations.ts LRU). No byte-weight budget exists (#36 gap).
* Everything listed is synchronous today; the request path calls `deriveMessages()`
  inline (agent-loop agent.ts:341).
* Unknowns flagged honestly: schedule fold caching; whether client-runtime
  hydrates old trajectory turns from persistence on attach.

## Implications for paged hydration

1. Agent resume needs NO historical chunks — a surface-node/descriptor-grade
   representation suffices (spec #27/#28 confirmed structurally, not just hoped).
2. Sequenced consumers (token meter, telemetry otel identity check) are the ones
   that pin array-indexed identity — they must either keep compatibility arrays
   or migrate to seq-coordinate access explicitly (#91 vs #90 advice honored).
3. session-projection ALREADY implements its own durable checkpoint + FULL fallback
   (index.ts:444–446) — precedent showing a checkpointable-fold pattern exists in
   core vocabulary (spec #67).
4. Session Query is the best first *independent* optimization target after any
   seam lands: it re-clones whole histories per observation today (#93/#48).
