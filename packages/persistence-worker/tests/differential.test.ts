/**
 * Track B differential tests: stock (synchronous, main-thread) SqliteStore vs
 * worker-owned SqliteStore fed IDENTICAL event sequences must reconstruct
 * identical logical state; failure modes must fail fast and deterministically.
 *
 * Run: DSH_ROOT=... npx tsx packages/persistence-worker/tests/differential.test.ts
 */
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createRequire } from 'node:module'
import { pathToFileURL } from 'node:url'
import { WorkerPersistenceError } from '../src/protocol.ts'
import { WorkerSqliteBackend } from '../src/backend.ts'

const require_ = createRequire(import.meta.url)
const dshRoot = process.env.DSH_ROOT
if (dshRoot === undefined) {
  console.error('SKIP: set DSH_ROOT to a DeepSeek Harness checkout')
  process.exit(0)
}
const storeModulePath = join(dshRoot, 'packages/session/session-persistence-sqlite/src/store.ts')
const { SqliteStore } = require_(pathToFileURL(storeModulePath).href) as {
  SqliteStore: new (o: { path: string; journalMode: string; busyTimeoutMs: number }) => StockStore
}

interface StockStore {
  open(): Promise<void>
  appendBatch(meta: unknown, events: unknown[], isMaterialized: boolean): Promise<void>
  loadStored(id: string): Promise<StockLoaded | undefined>
  loadStoredFrom(id: string, fromSeq: number): Promise<StockSuffix | undefined>
  readStoredRevision(id: string): Promise<string | undefined>
  commitRepair(meta: unknown, tornMarker: unknown, closers: unknown[]): Promise<void>
  list(): Promise<unknown[]>
  close(): Promise<void>
}
interface StockLoaded {
  meta: Record<string, unknown>
  events: Array<{ seq: number }>
  revision: string
  tornMarker?: unknown
}

interface StockSuffix {
  meta: Record<string, unknown>
  events: Array<{ seq: number }>
}

let passed = 0
let failed = 0
function check(name: string, fn: () => Promise<void>): Promise<void> {
  return (async () => {
    try {
      await fn()
      passed++
      console.log(`ok - ${name}`)
    } catch (err) {
      failed++
      console.error(`FAIL - ${name}`)
      console.error(err)
    }
  })()
}

// Synthetic mixed log: turns/chunks/tool calls — JSON-safe plain objects.
function generateMixedEvents(count: number): Array<Record<string, unknown>> {
  const events: Array<Record<string, unknown>> = []
  let seq = 0
  let time = Date.now() - 3_600_000
  let turn = 0
  while (events.length < count) {
    events.push({ type: 'turn/start', seq: seq++, time: (time += 2), data: { turn } })
    for (let c = 0; c < 40 && events.length + 3 < count; c++) {
      events.push({
        type: 'assistant/chunk',
        seq: seq++,
        time: (time += 1),
        data: { turn, step: 0, chunk: { type: 'text-delta', index: 0, text: `t${c} `.repeat(4) } },
      })
    }
    events.push({
      type: 'tool/call',
      seq: seq++,
      time: (time += 2),
      data: { callId: `call-${turn}`, name: 'bash', arguments: '{"command":"ls"}', turn, step: 0 },
    })
    events.push({
      type: 'tool/result',
      seq: seq++,
      time: (time += 800),
      data: { callId: `call-${turn}`, output: `out-${turn}\n`.repeat(20), isError: false },
    })
    events.push({ type: 'turn/end', seq: seq++, time: (time += 10), data: { turn, reason: 'end-turn' } })
    turn++
  }
  return events.slice(0, count)
}

const dir = mkdtempSync(join(tmpdir(), 'dsh-next-worker-'))
const events = generateMixedEvents(2000)
const header = { version: 0, id: crypto.randomUUID(), createdAt: Date.now(), cwd: '/tmp/bench' }

async function feed(
  appendBatch: (meta: unknown, batch: unknown[], materialized: boolean) => Promise<void>,
): Promise<void> {
  for (let i = 0; i < events.length; i += 100) {
    await appendBatch(header, events.slice(i, i + 100), false)
  }
}

const stockDb = join(dir, 'stock.sqlite')
const stock = new SqliteStore({ path: stockDb, journalMode: 'wal', busyTimeoutMs: 5000 })
await stock.open()

const workerDb = join(dir, 'worker.sqlite')
const workerBackend = new WorkerSqliteBackend({
  path: workerDb,
  journalMode: 'wal',
  busyTimeoutMs: 5000,
})
await workerBackend.init()

await check('identical appends resolve in both backends', async () => {
  await feed(async (meta, batch, materialized) => {
    await stock.appendBatch(meta, batch, materialized)
  })
  await feed(workerBackend.appendBatch.bind(workerBackend))
})

const workerLoad = (await workerBackend.loadStored(header.id)) as StockLoaded | undefined
const stockLoad = await stock.loadStored(header.id)

await check('loadStored reconstruction identical (meta + events)', async () => {
  if (!stockLoad || !workerLoad) throw new Error('both loads required')
  if (JSON.stringify(stockLoad.meta) !== JSON.stringify(workerLoad.meta)) throw new Error('meta mismatch')
  if (stockLoad.events.length !== workerLoad.events.length)
    throw new Error(`event count mismatch ${stockLoad.events.length} vs ${workerLoad.events.length}`)
  const a = JSON.stringify(stockLoad.events)
  const b = JSON.stringify(workerLoad.events)
  if (a !== b) throw new Error('event graphs diverge')
})

await check('revisions stable per backend and comparable format', async () => {
  const s1 = await stock.readStoredRevision(header.id)
  const s2 = await stock.readStoredRevision(header.id)
  const w1 = (await workerBackend.readStoredRevision(header.id)) as string
  const w2 = (await workerBackend.readStoredRevision(header.id)) as string
  if (s1 !== s2 || w1 !== w2) throw new Error('revision not stable within backend')
  // Revisions embed store identity (dev/ino/uuid) so cross-backend values differ
  // by design; they must both be non-empty opaque strings.
  if (typeof s1 !== 'string' || s1.length === 0 || typeof w1 !== 'string' || w1.length === 0)
    throw new Error('revision shape invalid')
})

await check('readFrom suffix reads identical', async () => {
  const mid = Math.floor(events.length / 2)
  const s = await stock.loadStoredFrom(header.id, mid)
  const w = (await workerBackend.loadStoredFrom(header.id, mid)) as StockSuffix
  if (JSON.stringify(s?.events) !== JSON.stringify(w.events)) throw new Error('suffix divergence')
  if ((s as { tornMarker?: unknown }).tornMarker !== undefined) throw new Error('unexpected tear')
})

await check('paged load equals single-shot load', async () => {
  const single = (await workerBackend.loadStored(header.id)) as StockLoaded
  const paged = await workerBackend.loadStoredPaged(header.id, 333)
  if (JSON.stringify(single.events) !== JSON.stringify(paged.events)) throw new Error('paged divergence')
  if (JSON.stringify(single.meta) !== JSON.stringify(paged.meta)) throw new Error('paged meta mismatch')
  if (single.tornMarker !== undefined && paged.tornMarker === undefined)
    throw new Error('paged lost tornMarker')
})

await check('list identical', async () => {
  const s = await stock.list()
  const w = (await workerBackend.list()) as unknown[]
  if (w.length !== s.length) throw new Error('list length mismatch')
  if (JSON.stringify((s[0] as { id: string }).id) !== JSON.stringify((w[0] as { id: string }).id))
    throw new Error('list id mismatch')
})

await check('torn-tail corruption: identical detection + repair', async () => {
  await stock.close()
  await workerBackend.close()
  rmSync(`${stockDb}-wal`, { force: true })
  rmSync(`${stockDb}-shm`, { force: true })
  rmSync(`${workerDb}-wal`, { force: true })
  rmSync(`${workerDb}-shm`, { force: true })

  // Corrupt the last physical row identically in both databases via raw sqlite.
  const { DatabaseSync } = require_('node:sqlite') as typeof import('node:sqlite')
  for (const db of [stockDb, workerDb]) {
    const raw = new DatabaseSync(db)
    const row = raw
      .prepare('SELECT seq FROM events ORDER BY rowid DESC LIMIT 1')
      .get() as { seq: number } | undefined
    if (row === undefined) throw new Error('no rows to corrupt')
    raw.prepare("UPDATE events SET data = x'00deadbeef' WHERE seq = ?").run(row.seq)
    raw.close()
  }

  const stock2 = new SqliteStore({ path: stockDb, journalMode: 'wal', busyTimeoutMs: 5000 })
  await stock2.open()
  const worker2 = new WorkerSqliteBackend({ path: workerDb, journalMode: 'wal', busyTimeoutMs: 5000 })
  await worker2.init()

  const s = await stock2.loadStored(header.id)
  const w = (await worker2.loadStored(header.id)) as StockLoaded
  if (s === undefined) throw new Error('stock load undefined')
  if (s.tornMarker === undefined) throw new Error('stock did not detect torn tail')
  if (w.tornMarker === undefined) throw new Error('worker did not detect torn tail')
  if (JSON.stringify(s.events) !== JSON.stringify(w.events)) throw new Error('preserved prefix differs')

  // Repair both: truncate torn tail only (closers=[] keeps histories balanced —
  // the corrupted row is the trailing turn/end region; append fresh closers).
  const closers: unknown[] = []
  await stock2.commitRepair((s as StockLoaded).meta, s.tornMarker, closers)
  await worker2.commitRepair((w as StockLoaded).meta, w.tornMarker, closers)

  const sAfter = (await stock2.loadStored(header.id)) as StockLoaded | undefined
  const wAfter = (await worker2.loadStored(header.id)) as StockLoaded
  if (sAfter === undefined) throw new Error('stock post-repair load undefined')
  if (JSON.stringify(sAfter.events) !== JSON.stringify(wAfter.events))
    throw new Error('post-repair event graphs differ')

  await stock2.close()
  await worker2.close()
})

await check('worker crash fails pending ops deterministically + subsequent calls fail fast', async () => {
  const crashDb = join(dir, 'crash.sqlite')
  const be = new WorkerSqliteBackend({ path: crashDb, journalMode: 'wal', busyTimeoutMs: 5000 })
  await be.init()
  const slow = be.list() // keep one request pending while we kill the thread

  let pendingRejected: unknown
  void slow.catch((e) => (pendingRejected = e))

  await be.killForTest()
  // Force a macrotask boundary so rejection handlers run.
  await new Promise((r) => setTimeout(r, 10))
  if (!(pendingRejected instanceof WorkerPersistenceError)) {
    throw new Error(`pending rejected with ${(pendingRejected as Error)?.name ?? typeof pendingRejected}`)
  }

  let subsequentFailed = false
  try {
    await be.list()
  } catch (err) {
    subsequentFailed = err instanceof WorkerPersistenceError
  }
  if (!subsequentFailed) throw new Error('calls after failure must fail fast')
})

await check('disposed backend rejects further ops', async () => {
  await workerBackend.close().catch(() => {})
  let rejected = false
  try {
    await workerBackend.list()
  } catch (err) {
    rejected = err instanceof WorkerPersistenceError
  }
  if (!rejected) throw new Error('expected deterministic rejection after dispose')
})

await stock.close().catch(() => {})

console.log(`\n${passed} passed, ${failed} failed`)
rmSync(dir, { recursive: true, force: true })
process.exit(failed > 0 ? 1 : 0)
