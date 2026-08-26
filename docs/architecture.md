# dsh-next — Architecture Audit and Boundary Design (Phase 0)

Status: Phase 0 complete. Baseline: DSH `0.1.1-rc.2`
(commit `b150a551b8`, deepseek-harness). All file references below are
relative to that checkout.

## 1. What DSH provides today (audited seams)

Everything is a Plugin. Capability seams are ordinary Cordis services; a
provider is any Cordis plugin whose base-class constructor claims the service
key. Providers are replaced via declarative bundle/profile patches
(`packages/bundle/base/cordis.patch.yml`): one insert over an empty profile
root, later layers replace whole rows by id, last-write-wins.

### 1.1 Session persistence

* Contract: `@deepseek-ai/dsh-session-persistence` — abstract class
  `SessionPersistence` registered as `ctx.sessionPersistence`
  (`packages/session/session-persistence/src/index.ts:84-88`).
  Public surface: `locate`, `supportsRawArtifacts`/`readRaw`, `create`,
  `append`, `prepare`, `load`, `inspect`, `readFrom`, `list`,
  `listSnapshots`.
* Shared orchestration: `PersistenceCoordinator`
  (`session-persistence/src/coordinator.ts:588`) enforces contiguity
  (`appendCore`, 682–710), crash repair, interrupted-turn closure, format
  refusal (`SessionFormatUnsupportedError`, `SessionPersistenceCorruptionError`),
  ignorable-unknown-event policy, revision currency revalidation, and installs
  the write path on Cordis events (`installWritePath`, 1086–1137).
* Write-behind batching: fixed coalescing window, default **200 ms**
  (`DEFAULT_WRITE_BATCH_MAX_DELAY_MS`, coordinator.ts:30); per-session queue in
  `write-behind.ts`; `session/flush` forces a durability barrier.
* Backends ship as plugins: `JsonlSessionPersistence`
  (`session-persistence-jsonl/src/index.ts:121`) and
  `SqliteSessionPersistence` (`session-persistence-sqlite/src/index.ts:52`).
  Only the JSONL row ships in the default profile; the SQLite row id is
  `session-persistence-sqlite` with package
  `@deepseek-ai/dsh-session-persistence-sqlite`.

SQLite backend internals (`session-persistence-sqlite/src/`):

| Concern | Where |
|---|---|
| Chunk-row packing (delta timestamps, run continuation, ≤1024 members/≤1 MiB data) | `codec.ts` (deliberate fork of core's `chunk-rows.ts`) |
| Physical binding: JSON stringify → zstd≥4 KiB level 3 smaller-of → varint/zigzag `sourceEventSeqs` | `compression.ts:72-166` |
| Committed-prefix validation / torn-tail scan | `compression.ts:227-276` (`scanRows`) |
| Schema v17, app-id `DSHP`, canonical schema equality, pre-mutation recheck | `schema.ts:117-149, 214-251, 260-276` |
| Durability: WAL chosen + `synchronous=FULL` pinned, BEGIN IMMEDIATE per batch, header upsert + inserts + revision bump in one tx | `store.ts:173-199`, `schema.ts:177-184` |
| Seek-capable suffix read | `store.ts:324-344` (`physicalSpanFrom`) |

**All of it runs synchronously on the Node main thread** — Node's built-in
`node:sqlite` `DatabaseSync` plus `zstdCompressSync` from `node:zlib`. The
README of the package acknowledges this limitation.

### 1.2 Subprocess / shell

* Contract `ctx.subprocess`: `SubprocessRuntime extends Service`
  (`subprocess/subprocess/src/index.ts:102-140`), spawn spec/outcome types in
  `types.ts`. Handles expose **Node streams directly**:
  `stdin: Writable`, `stdout/stderr: Readable` (`types.ts:167-194`).
* Local implementation: `LocalSubprocessRuntime` +
  `spawnSubprocess` (`subprocess/subprocess-local/src/spawn.ts:326`).
  Semantics that must survive any replacement: executable resolution
  (`subprocess-local/src/index.ts:104-144`), credential scrubbing +
  `DSH_*` namespace ordering (`subprocess/src/index.ts:44-66`,
  `spawn.ts:37-47`), detached process groups, SIGTERM→grace→SIGKILL tree
  escalation (`spawn.ts:260-315, 439-453`), Windows `taskkill /T /F`
  (276–282), liveness observation incl. pid-reuse cancellation (381–425),
  PTY support via node-pty (`terminal.ts`), disposal quiescence +
  host-exit force kill (`index.ts:49-102`), abort escalation.
* `OutputCollector` (`spawn.ts:104-251`): Buffer[] window with byte-exact
  head trimming, whole-stream byte offsets, non-consuming `readFrom(offset)`
  returning UTF-8 text, lossy flag when the cursor slid out of the window,
  O_EXCL+0600 spill files under a private mkdtemp dir, spill discarded once
  the stream outgrows `maxSpillBytes`.

### 1.3 Jobs and Waits

* `LocalJobRegistry` (`jobs/jobs-local/src/index.ts`): one process-local
  instance; mutable state is `Map<JobId, TrackedTask>`, id counters,
  scope-layered listener tables (`ScopedLayers`), per-agent disposal hooks;
  bounded per-job output journal (256 KiB default). Pure bookkeeping — no
  computational hot path.
* `WaitService` (`wait/wait/src/index.ts:269`) is durable session-log-backed
  event orchestration (`wait/change` folds, providers, wake budgets). It is
  correctness-critical coordination logic deeply tied to session semantics.

### 1.4 Native precedent

`native/landlock-run` is DSH's existing native distribution model:

* Entry package (`@deepseek-ai/node-addon-landlock-run`) + platform optional
  deps (`…-linux-x64`, `…-linux-arm64`) gated by npm `os`/`cpu` fields.
* Availability decided at runtime by `probe()` (real child enforcement test);
  consumers fail closed.
* **No install-time compilation ever**; verification gates forbid lifecycle
  scripts; prebuilt artifacts staged as `bin/<tool>` with manifest checks.

dsh-next adopts this shape for anything native (see §5).

## 2. Ownership boundaries worth recording

* Persistence events crossing the coordinator are cloned into the write-behind
  queue; backends must return fresh, unaliased graphs from reads.
* OutputCollector retains caller buffers without copying (push is zero-copy);
  it pays on read instead (`Buffer.concat(this.chunks)` per read,
  `readFrom`/`finalize`). Any replacement must pick and document which side
  pays the copy.
* The Rust pilot copies exactly once at each FFI boundary (see
  docs/journal-pilot.md for the ownership table).

## 3. Candidate ranking after audit + measurement

Verified against real benchmarks (docs/baseline-results.md):

| Subsystem | Hypothesis | Measured verdict |
|---|---|---|
| OutputCollector read path | high interest | **Confirmed hotspot** — 250–300 µs per `readFrom` (whole-window concat + UTF-8 decode); observer polling burns seconds of main-thread time |
| SQLite append path | high interest | Mostly healthy: p99 batch ≈ 0.7–1 ms single-session, loop stalls ≤ ~5 ms; ~17–25 ms loop lag only at 50 concurrent sessions on one DB |
| Compression | medium | Not the bottleneck: encode p50 0.03–0.36 ms for realistic text payloads; batch simulation shows no drift. Spikes exist (≤15 ms encode, ≤97 ms decode worst-case observed at 1 MB) but are rare |
| Process supervision | high interest (future) | Deferred — no measurement yet justifies FFI; contracts are intricate (§1.2). Design note only |
| Jobs/Waits/agent-loop/Cordis | low | Unchanged: keep TS. No evidence, heavy integration cost |

## 4. Boundary design rules adopted

1. **N-API for synchronous/bounded primitives** (journal, codec helpers):
   cheap calls, no runtime, direct memory handoff via Buffers. Avoid thousands
   of tiny crossings — measured batching benefit is 1.3–2.9× (journal bench).
2. **Sidecar for long-lived supervision** (future): blast-radius isolation and
   restartability require process separation; N-API would couple supervisor
   crashes to the harness process. Design note only in Phase 0.
3. **Batching**: prefer batch-shaped APIs at FFI boundaries matching DSH's
   existing write-behind batches rather than per-event crossings.
4. **Errors**: Rust panics must never cross FFI; results map to typed JS
   errors (pilot validates `maxBytes`/`offset` explicitly).
5. **Cancellation/disposal**: async native work must integrate AbortSignal +
   Cordis disposal; nothing detached survives plugin unload. (Pilot is fully
   synchronous so nothing to cancel.)
6. **Memory ownership** is documented per boundary in docs/journal-pilot.md.

## 5. Distribution plan (adopting landlock-run lessons)

Planned layout (not built in Phase 0 beyond the pilot crate):

```text
@dsh-next/native            entry: probe() + graceful absence handling
@dsh-next/native-darwin-arm64, -darwin-x64,
-dsh-next-native-linux-x64-gnu/-musl, -linux-arm64-gnu,
-win32-x64-msvc             platform optionalDependencies, os/cpu gated
```

Prebuilt-only (no build-on-install), CI matrix generated from checked-in
metadata like DSH does. If napi-rs is used at scale, its prebuilt target list
replaces hand-maintained platform packages but keeps the same gating/fallback
semantics.

## 6. Explicit non-work (Phase 0 contract)

Not rewritten, not redesigned, not moved across FFI: Cordis, agent-loop, jobs
registry, wait runtime, session event semantics, derived model history, web
UI, Agent Teams, GoodJob. dsh-goodjob remains a separate product consuming
public DSH APIs; it must never need Rust knowledge.
