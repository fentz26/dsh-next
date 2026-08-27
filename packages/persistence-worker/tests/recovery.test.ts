/**
 * Generation-scoped recovery suite (P0 hardening) — deterministic tests A–K.
 * Run: DSH_ROOT=... npx tsx packages/persistence-worker/tests/recovery.test.ts
 */
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createRequire } from 'node:module'
import { pathToFileURL } from 'node:url'
import { Worker } from 'node:worker_threads'
import { WorkerSqliteBackend } from '../src/backend.ts'
import { WorkerPersistenceError } from '../src/protocol.ts'

const require_ = createRequire(import.meta.url)
const dshRoot = process.env.DSH_ROOT
if (dshRoot === undefined) {
  console.log('SKIP: set DSH_ROOT to a DeepSeek Harness checkout')
  process.exit(0)
}
const { SqliteStore } = require_(pathToFileURL(`${dshRoot}/packages/session/session-persistence-sqlite/src/store.ts`).href) as {
  SqliteStore: new (o: unknown) => {
    open(): Promise<void>
    appendBatch(m: unknown, e: unknown[], f: boolean): Promise<void>
    close(): Promise<void>
  }
}

let passed = 0
let failed = 0
async function check(name: string, fn: () => Promise<void>): Promise<void> {
  try {
    await fn()
    passed++
    console.log(`ok - ${name}`)
  } catch (err) {
    failed++
    console.error(`FAIL - ${name}`)
    console.error(err)
  }
}
const wait = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))

function eventPromise(be: WorkerSqliteBackend, ev: string, timeoutMs: number): { promise: Promise<boolean>; count: () => number } {
  let n = 0
  return {
    count: () => n,
    promise: new Promise<boolean>((resolve) => {
      const t = setTimeout(() => resolve(false), timeoutMs)
      be.on(ev, () => {
        n++
        clearTimeout(t)
        resolve(true)
      })
    }),
  }
}

const dir = mkdtempSync(join(tmpdir(), 'recovery-'))
const mk = (over: Record<string, unknown> = {}): Promise<WorkerSqliteBackend> => {
  const be = new WorkerSqliteBackend({
    path: join(dir, `${crypto.randomUUID()}.sqlite`),
    journalMode: 'wal',
    busyTimeoutMs: 5000,
    restartOnCrash: true,
    ...over,
  } as never)
  return be.init().then(() => be)
}

// A. reopen retry semantics: attempt 1 fails, attempt 2 succeeds, exactly 2.
await check('A retry semantics: exactly 2 attempts, READY, one active worker', async () => {
  const be = await mk()
  const before = { opens: be.openAttemptsForTest, spawns: be.workersSpawnedForTest }
  be.testFailOpens = 1
  await be.killForTest()
  const restarted = await eventPromise(be, 'restarted', 5000).promise
  if (!restarted) throw new Error('no restarted event')
  await wait(80)
  if (be.openAttemptsForTest - before.opens !== 2) throw new Error(`attempts=${be.openAttemptsForTest - before.opens}, want 2`)
  if (be.lifecycleState !== 'ready') throw new Error(`state=${be.lifecycleState}`)
  if (be.liveWorkersForTest !== 1) throw new Error(`liveWorkers=${be.liveWorkersForTest}`)
  if (be.stats().failed) throw new Error('stats.failed must clear after successful restart')
  await be.close()
})

// B. immediate successful reopen: EXACTLY one replacement worker, one event.
await check('B successful reopen: exactly ONE replacement, one restarted event', async () => {
  const be = await mk()
  const ev = eventPromise(be, 'restarted', 5000)
  const before = { opens: be.openAttemptsForTest, spawns: be.workersSpawnedForTest }
  await be.killForTest()
  if (!(await ev.promise)) throw new Error('no restarted')
  await wait(120) // any phantom extra opens would land here
  if (be.workersSpawnedForTest - before.spawns !== 1) throw new Error(`spawned ${be.workersSpawnedForTest - before.spawns}, want 1`)
  if (be.openAttemptsForTest - before.opens !== 1) throw new Error(`opens ${be.openAttemptsForTest - before.opens}, want 1`)
  if (ev.count() !== 1) throw new Error(`restarted events=${ev.count()}`)
  if (be.liveWorkersForTest !== 1) throw new Error('exactly one live worker expected')
  await be.close()
})

// C. all attempts fail: exactly N attempts, FAILED, no leaked worker.
await check('C exhausted restarts: exactly 3 attempts, FAILED, zero live workers', async () => {
  const be = await mk({ restartAttempts: 3 })
  const ev = eventPromise(be, 'failed', 5000)
  be.testFailOpens = 99 // every attempt fails
  await be.killForTest()
  if (!(await ev.promise)) throw new Error('no failed event')
  await wait(80)
  if (be.openAttemptsForTest !== 1 + 3) throw new Error(`opens=${be.openAttemptsForTest}, want 4 (1 initial + 3 retries)`)
  if (be.lifecycleState !== 'failed') throw new Error(`state=${be.lifecycleState}`)
  if (!be.failedState) throw new Error('failedState must be true')
  if (be.liveWorkersForTest !== 0) throw new Error(`liveWorkers=${be.liveWorkersForTest} (leak)`)
  let rejected = false
  try {
    await be.list()
  } catch {
    rejected = true
  }
  if (!rejected) throw new Error('post-failure calls must fail fast')
  await be.close()
})

// D. crash with REAL in-flight capacity: reject + capacity zero + recovery + next op OK.
await check('D real in-flight capacity: rejects, capacity→0, restart, next op completes', async () => {
  const be = await mk({ maxInFlight: 1 })
  const meta = { version: 0, id: crypto.randomUUID(), createdAt: Date.now(), cwd: '/w' }
  await be.appendBatch(meta, [{ type: 'turn/start', seq: 0, time: 1, data: { turn: 1 } }], false)

  const slow = be.injectPendingForTest() // counts REAL capacity now
  let slowErr: unknown
  void slow.catch((e) => (slowErr = e))
  if (Number(be.inFlightForTest) !== 1) throw new Error(`inFlight=${be.inFlightForTest} before crash`)

  await be.killForTest()
  await eventPromise(be, 'restarted', 5000).promise
  if (!(slowErr instanceof WorkerPersistenceError)) throw new Error('in-flight must reject with WorkerPersistenceError')
  if (Number(be.inFlightForTest) !== 0) throw new Error(`inFlight leaked: ${be.inFlightForTest}`)
  await be.list() // would deadlock forever if capacity leaked
  await be.close()
})

// E. crash with queued callers: queued + in-flight settle deterministically.
await check('E queued callers settle; no orphan promises/stale queue entries', async () => {
  const be = await mk({ maxInFlight: 1 })
  const inflight = be.injectPendingForTest()
  let inflightErr: unknown
  void inflight.catch((e) => (inflightErr = e))
  const queued = be.list()
  let queuedErr: unknown
  void queued.catch((e) => (queuedErr = e))
  await wait(20)
  if (be.queuedForTest !== 1) throw new Error('expected one queued waiter')
  await be.killForTest()
  await eventPromise(be, 'restarted', 5000).promise
  if (!(inflightErr instanceof WorkerPersistenceError)) throw new Error('in-flight not settled')
  if (!(queuedErr instanceof WorkerPersistenceError)) throw new Error('queued caller not settled')
  if (Number(be.queuedForTest) !== 0) throw new Error(`stale queue entries: ${be.queuedForTest}`)
  await be.close()
})

// F. candidate crashes during open: attempt rejects (no hang); later attempt recovers.
await check('F candidate death during open rejects and a later attempt recovers', async () => {
  const be = await mk()
  be.testCrashCandidates = 1 // next spawn dies hard while opening
  await be.killForTest()
  const restarted = await eventPromise(be, 'restarted', 5000).promise
  if (!restarted) throw new Error('recovery hung or never emitted restarted')
  if (be.openAttemptsForTest !== 3) throw new Error(`opens=${be.openAttemptsForTest}, want 3 (crashed candidate + retry)`)
  if (be.lifecycleState !== 'ready') throw new Error(`state=${be.lifecycleState}`)
  await be.list()
  await be.close()
})

// F2. initial init candidate crash: init() rejects instead of hanging.
await check('F2 initial open candidate crash rejects init deterministically', async () => {
  const be = new WorkerSqliteBackend({
    path: join(dir, 'f2.sqlite'),
    journalMode: 'wal',
    busyTimeoutMs: 5000,
  })
  be.testCrashCandidates = 1
  let threw = false
  try {
    await be.init()
  } catch {
    threw = true
  }
  if (!threw) throw new Error('init must reject when candidate dies during open')
  if (be.lifecycleState !== 'failed') throw new Error(`state=${be.lifecycleState}`)
  await be.close()
})

// G. stale old-generation lifecycle events cannot fail the current generation.
await check('G stale generation events are ignored (direct dispatcher + double-event)', async () => {
  const be = await mk()
  await be.killForTest()
  await eventPromise(be, 'restarted', 5000).promise

  // Node strips a Worker's listeners once it terminates, so synthetic emits on
  // a dead worker cannot reach handlers at all. Exercise the stale branch
  // directly through the generation dispatcher with a fabricated stale gen.
  const internals = be as unknown as {
    onGenerationEvent(gen: { id: number; worker: unknown; exited: boolean }, err: Error): void
  }
  const staleGen = { id: 0, worker: be.activeWorkerForTest, exited: false }
  const failedEv = eventPromise(be, 'failed', 400)
  internals.onGenerationEvent(staleGen, new Error('stale generation error'))
  internals.onGenerationEvent({ ...staleGen, id: -1, exited: true }, new Error('stale exit'))
  if (await failedEv.promise) throw new Error('stale event must NOT fail current generation')
  if (be.lifecycleState !== 'ready') throw new Error(`state=${be.lifecycleState}`)

  // Real-world stale path: crash emits error+exit pairs; the second event of
  // each pair arrives after this.active cleared → stale branch (covered live
  // by every cycle in K; asserted again here for determinism).
  await be.list()
  await be.close()
})

// H. paged error frame releases capacity; next request proceeds.
await check('H paged error response releases capacity; next request runs', async () => {
  const be = await mk({ maxInFlight: 1 })
  const w = be.activeWorkerForTest as Worker & {
    postMessage: (m: unknown, ...r: unknown[]) => void
  }
  const origPost = w.postMessage.bind(w)
  let armed = true
  let pagedSeq = -1
  w.postMessage = ((msg: { req?: { op?: string; seq?: number; payload?: unknown[] } }) => {
    if (armed && msg?.req?.op === 'loadStoredPaged') {
      armed = false
      pagedSeq = msg.req.seq as number
      msg.req.payload = [{ unbindable: true }, msg.req.payload?.[1]]
    }
    return origPost({ req: msg?.req })
  }) as typeof w.postMessage

  let pagedErr: unknown
  await be.loadStoredPaged('any', 100).catch((e) => (pagedErr = e))
  w.postMessage = origPost
  if (!(pagedErr instanceof WorkerPersistenceError)) throw new Error('paged must reject on error frame')
  if (be.inFlightForTest !== 0) throw new Error(`capacity leaked: ${be.inFlightForTest}`)
  void pagedSeq
  await be.list() // proves capacity released
  await be.close()
})

// I. page safety bound: typed rejection, never partial success.
await check('I page safety bound rejects typed error with no partial resolve', async () => {
  const be = await mk()
  const dbPath = join(dir, 'i-fixture.sqlite')
  const builder = new SqliteStore({ path: dbPath, journalMode: 'wal', busyTimeoutMs: 5000 })
  await builder.open()
  const meta = { version: 0, id: crypto.randomUUID(), createdAt: Date.now(), cwd: '/w' }
  // 5 pages at pageSize=1000 with safety bound 2: frames 3+ exceed the bound
  // while more pages remain (no last flag yet) → must reject, never resolve.
  const events = Array.from({ length: 5000 }, (_, i) => ({
    type: 'assistant/chunk',
    seq: i,
    time: 1 + i,
    data: { turn: 0, step: 0, chunk: { type: 'text-delta', index: 0, text: 'x'.repeat(30) } },
  }))
  for (let i = 0; i < events.length; i += 500) await builder.appendBatch(meta, events.slice(i, i + 500), false)
  await builder.close()
  await be.close()

  const be2 = new WorkerSqliteBackend({ path: dbPath, journalMode: 'wal', busyTimeoutMs: 5000 })
  await be2.init()
  be2.maxPagesSafetyForTest = 2
  let err: unknown
  const result = await be2.loadStoredPaged(meta.id, 1000).then(
    (v) => v,
    (e) => {
      err = e
      return undefined
    },
  )
  if (result !== undefined) throw new Error(`safety bound RESOLVED partial data (${result.events.length} events)`)
  if (!(err instanceof WorkerPersistenceError) || !String(err.message).includes('safety bound')) {
    throw new Error(`expected typed safety-bound error, got ${String(err)}`)
  }
  if (be2.inFlightForTest !== 0) throw new Error('capacity leaked on safety failure')
  await be2.close()
})

// J. close during restart: CLOSED wins, no resurrection, no restarted after dispose.
await check('J close during restart: closed wins, worker reaped, no post-dispose restart', async () => {
  const be = await mk()
  const restarted = eventPromise(be, 'restarted', 600)
  be.testFailOpens = 1 // restart takes ≥2 attempts → close() lands mid-restart
  await be.killForTest()
  await wait(5) // let recovery start (old termination + attempt 1 failing)
  await be.close()
  await wait(250)
  if (restarted.count() !== 0) throw new Error('restarted fired after dispose')
  if (be.lifecycleState !== 'closed') throw new Error(`state=${be.lifecycleState}`)
  if (be.liveWorkersForTest !== 0) throw new Error(`workers leaked: ${be.liveWorkersForTest}`)
})

// K. repeated crash/recover cycles: no listener growth, no inFlight drift, one owner.
await check('K repeated cycles: stable listeners/capacity/ownership', async () => {
  const be = await mk()
  for (let cycle = 0; cycle < 5; cycle++) {
    await be.killForTest()
    if (!(await eventPromise(be, 'restarted', 5000).promise)) throw new Error(`cycle ${cycle}: no restart`)
    if (be.liveWorkersForTest !== 1) throw new Error(`cycle ${cycle}: liveWorkers=${be.liveWorkersForTest}`)
    if (be.inFlightForTest !== 0) throw new Error(`cycle ${cycle}: inFlight=${be.inFlightForTest}`)
    const w = be.activeWorkerForTest as Worker
    const msgListeners = w.listenerCount('message')
    const errListeners = w.listenerCount('error')
    if (msgListeners !== 1) throw new Error(`cycle ${cycle}: message listeners=${msgListeners}`)
    if (errListeners !== 1) throw new Error(`cycle ${cycle}: error listeners=${errListeners}`)
    await be.list()
  }
  if (be.workersSpawnedForTest !== 6) throw new Error(`spawned=${be.workersSpawnedForTest}, want 6`)
  await be.close()
})

console.log(`\n${passed} passed, ${failed} failed`)
rmSync(dir, { recursive: true, force: true })
process.exit(failed > 0 ? 1 : 0)
