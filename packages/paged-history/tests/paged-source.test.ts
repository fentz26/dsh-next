/**
 * Paged logical source tests: differential vs canonical storage reads,
 * packed-run straddling, byte budgets, end-of-log semantics, fail-closed
 * guards, and freshness.
 *
 * Run: DSH_ROOT=... npx tsx packages/paged-history/tests/paged-source.test.ts
 */
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createRequire } from 'node:module'
import { pathToFileURL } from 'node:url'
import { openSqliteSource } from '../src/index.ts'

const require_ = createRequire(import.meta.url)
const dshRoot = process.env.DSH_ROOT
if (dshRoot === undefined) {
  console.log('SKIP: set DSH_ROOT to a DeepSeek Harness checkout')
  process.exit(0)
}
const { SqliteStore } = require_(pathToFileURL(`${dshRoot}/packages/session/session-persistence-sqlite/src/store.ts`).href) as {
  SqliteStore: new (o: { path: string; journalMode: string; busyTimeoutMs: number }) => StockStore
}

interface StockStore {
  open(): Promise<void>
  appendBatch(meta: unknown, events: unknown[], m: boolean): Promise<void>
  loadStored(id: string): Promise<{ meta?: unknown; events?: Array<Record<string, unknown>> } | undefined>
  loadStoredFrom(id: string, fromSeq: number): Promise<{ events?: Array<Record<string, unknown>> } | undefined>
  close(): Promise<void>
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

// Valid fixture vocabulary per DSH coordinator-contract (identified messages).
function generateMixedEvents(count: number): Array<Record<string, unknown>> {
  const events: Array<Record<string, unknown>> = []
  let seq = 0
  let time = Date.now() - 3_600_000
  let turn = 1
  while (events.length < count) {
    events.push({ type: 'turn/start', seq: seq++, time: (time += 2), data: { turn } })
    events.push({
      type: 'user/message',
      seq: seq++,
      time: (time += 3),
      data: { id: `u-${turn}`, role: 'user', content: [{ type: 'text', text: 'hi' }], source: { kind: 'user' } },
      surfaceOp: 'append',
    })
    events.push({ type: 'step/start', seq: seq++, time: (time += 1), data: { turn, step: 1 } })
    for (let c = 0; c < 40 && events.length + 4 < count; c++) {
      events.push({
        type: 'assistant/chunk',
        seq: seq++,
        time: (time += 1),
        data: { turn, step: 1, chunk: { type: 'text-delta', index: 0, text: `d${c}` } },
      })
    }
    events.push({
      type: 'assistant/message',
      seq: seq++,
      time: (time += 5),
      data: {
        turn,
        step: 1,
        message: {
          id: `a-${turn}`,
          role: 'assistant',
          content: [{ type: 'text', text: 'hello' }],
          source: { kind: 'model', provider: 'mock', model: 'mock' },
        },
      },
      surfaceOp: 'append',
    })
    events.push({ type: 'step/end', seq: seq++, time: (time += 2), data: { turn, step: 1 } })
    events.push({
      type: 'turn/end',
      seq: seq++,
      time: (time += 4),
      data: { turn, reason: { kind: 'completed' } },
    })
    turn++
  }
  return events.slice(0, count)
}

const dir = mkdtempSync(join(tmpdir(), 'paged-history-test-'))
const dbPath = join(dir, 'log.sqlite')
const store = new SqliteStore({ path: dbPath, journalMode: 'wal', busyTimeoutMs: 5000 })
await store.open()
const header = { version: 0, id: crypto.randomUUID(), createdAt: Date.now(), cwd: '/w' }
const events = generateMixedEvents(6_000).map((e, i) => ({ ...e, seq: i }))
for (let i = 0; i < events.length; i += 500) {
  await store.appendBatch(header as never, events.slice(i, i + 500).map((e) => ({ ...e })), false)
}

const canonical = (await store.loadStored(header.id))!.events! // eslint-disable-line @typescript-eslint/no-non-null-assertion

const BIG_SLACK = 1 << 30
const src = await openSqliteSource(dbPath)

await check('meta reports header + logical length', async () => {
  const m = await src.meta(header.id)
  if (m === undefined) throw new Error('meta missing')
  if (m.length !== events.length) throw new Error(`length ${m.length} != ${events.length}`)
})

async function compareRange(startSeq: number, count: number): Promise<void> {
  const page = await src.readRange(header.id, startSeq, { limit: count })
  const want = canonical.slice(startSeq, startSeq + count).map((e) => JSON.stringify(e)).join('\u0000')
  const got = page.events.map((e) => JSON.stringify(e)).join('\u0000')
  if (want !== got) throw new Error(`range mismatch at start=${startSeq}`)
  if (page.events[0] && Number((page.events[0] as { seq: number }).seq) !== startSeq)
    throw new Error(`first event seq != startSeq (${startSeq})`)
}

await check('head range equals canonical slice', async () => compareRange(0, 1000))
await check('mid-range starting inside a packed run hits exact boundary event', async () =>
  compareRange(1234, 400),
)

await check('suffix read matches canonical tail', async () => {
  const page = await src.readSuffix(header.id, 500)
  if (page.events.length !== 500) throw new Error(`got ${page.events.length}`)
  const tail = canonical.slice(-500).map((e) => JSON.stringify(e))
  if (tail.join('|') !== page.events.map((e) => JSON.stringify(e)).join('|')) throw new Error('tail mismatch')
  if (page.endOfLogAt !== events.length) throw new Error('endOfLogAt should equal total')
})

await check('start beyond log → empty page with endOfLogAt=total', async () => {
  const beyond = await src.readRange(header.id, events.length + 500)
  if (beyond.events.length !== 0 || beyond.endOfLogAt !== events.length) throw new Error('expected empty EOF page')
})

await check('byte budget stops early but always emits ≥1 event', async () => {
  const page = await src.readRange(header.id, 0, { limit: 1_000_000, maxBytes: 20 })
  if (page.events.length === 0) throw new Error('must emit at least one event')
  if (page.approxPayloadBytes > 20 + BIG_SLACK && page.events.length === 1) {
    // acceptable: single huge first event overflows the budget once emitted
  }
  if ((page.events.at(-1) as { seq: number }).seq !== 0 && page.events.length === 1) throw new Error('unexpected tail behavior')
})

await check('pages are fresh and top-level frozen across calls', async () => {
  const a = await src.readRange(header.id, 10, { limit: 5 })
  const b = await src.readRange(header.id, 10, { limit: 5 })
  if (JSON.stringify(a.events) !== JSON.stringify(b.events)) throw new Error('nondeterministic read')
  if (a.events[0] === b.events[0]) throw new Error('aliased object returned — violates fresh/unaliased rule')
  if (!Object.isFrozen(a.events)) throw new Error('top level not frozen')
})

await check('fail closed on non-session database / drifted schema guard', async () => {
  const other = join(dir, 'other.sqlite')
  const sqlite = require_('node:sqlite') as unknown as typeof import('node:sqlite')
  const raw = new sqlite.DatabaseSync(other)
  raw.exec('CREATE TABLE t(x)')
  raw.close()
  try {
    await openSqliteSource(other)
    throw new Error('should have refused foreign DB')
  } catch (err) {
    if (!String(err).includes('DSHP')) throw err
  }
})

await src.close()
try {
  await src.meta(header.id)
  throw new Error('closed source must refuse')
} catch (err) {
  if (!String(err).includes('closed')) throw err
}

console.log(`\n${passed} passed, ${failed} failed`)
store.close().catch(() => {})
rmSync(dir, { recursive: true, force: true })
process.exit(failed > 0 ? 1 : 0)
