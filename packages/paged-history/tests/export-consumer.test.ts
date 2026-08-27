/**
 * Export-consumer migration proof: the paged raw-artifact producer must be
 * byte-identical to the full-load JSONL reconstruction while never holding
 * more than one bounded page of events alive.
 *
 * Run: DSH_ROOT=... npx tsx packages/paged-history/tests/export-consumer.test.ts
 */
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createRequire } from 'node:module'
import { pathToFileURL } from 'node:url'
import { readRawArtifactPaged, sessionLogJsonlFilename, streamRawArtifact } from '../src/export-consumer.ts'

const require_ = createRequire(import.meta.url)
const dshRoot = process.env.DSH_ROOT
if (dshRoot === undefined) {
  console.log('SKIP: set DSH_ROOT to a DeepSeek Harness checkout')
  process.exit(0)
}
const { SqliteStore } = require_(pathToFileURL(`${dshRoot}/packages/session/session-persistence-sqlite/src/store.ts`).href) as {
  SqliteStore: new (o: { path: string; journalMode: string; busyTimeoutMs: number }) => {
    open(): Promise<void>
    appendBatch(meta: unknown, events: unknown[], m: boolean): Promise<void>
    loadStored(id: string): Promise<{ events?: Array<Record<string, unknown>> } | undefined>
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

function assert(cond: boolean, msg: string): void {
  if (!cond) throw new Error(`assertion failed: ${msg}`)
}

// Mixed turns so packing (assistant/chunk runs) interleaves with message nodes.
function generateMixed(count: number): Array<Record<string, unknown>> {
  const evts: Array<Record<string, unknown>> = []
  let seq = 0
  let turn = 1
  const t = () => 1_700_000_000_000 + seq
  while (evts.length < count) {
    evts.push({ type: 'turn/start', seq: seq++, time: t(), data: { turn } })
    evts.push({
      type: 'user/message', seq: seq++, time: t(),
      data: { id: `u${turn}`, role: 'user', content: [{ type: 'text', text: `q ${turn}` }], source: { kind: 'user' } },
      surfaceOp: 'append',
    })
    evts.push({ type: 'step/start', seq: seq++, time: t(), data: { turn, step: 1 } })
    for (let c = 0; c < 300 && evts.length < count; c++) {
      evts.push({ type: 'assistant/chunk', seq: seq++, time: t(), data: { turn, step: 1, chunk: { type: 'text-delta', index: 0, text: `z${c}` } } })
    }
    evts.push({
      type: 'assistant/message', seq: seq++, time: t(),
      data: { turn, step: 1, message: { id: `a${turn}`, role: 'assistant', content: [{ type: 'text', text: `done ${turn}` }], source: { kind: 'model', provider: 'mock', model: 'mock' } } },
      surfaceOp: 'append',
    })
    evts.push({ type: 'step/end', seq: seq++, time: t(), data: { turn, step: 1 } })
    evts.push({ type: 'turn/end', seq: seq++, time: t(), data: { turn, reason: { kind: 'completed' } } })
    turn++
  }
  return evts.slice(0, count)
}

const dir = mkdtempSync(join(tmpdir(), 'paged-export-test-'))
const dbPath = join(dir, 'log.sqlite')
const store = new SqliteStore({ path: dbPath, journalMode: 'wal', busyTimeoutMs: 5000 })
await store.open()
const header = { version: 0, id: crypto.randomUUID(), createdAt: Date.now(), cwd: '/w' }
const events = generateMixed(120_000)
for (let i = 0; i < events.length; i += 500) {
  await store.appendBatch(header as never, events.slice(i, i + 500).map((e) => ({ ...e })), false)
}
const srcMod = require_(pathToFileURL(join(import.meta.dirname, '../src/index.ts')).href) as typeof import('../src/index.ts')
const src = await srcMod.openSqliteSource(dbPath)

await check('paged artifact is byte-identical to full-load reconstruction', async () => {
  const artifact = await readRawArtifactPaged(src as never, header.id, { pageSize: 2048 })
  const canonical = (await store.loadStored(header.id))!.events! // eslint-disable-line @typescript-eslint/no-non-null-assertion
  const want = canonical.map((e) => JSON.stringify(e)).join('\n') + '\n'
  if (artifact === undefined) throw new Error('artifact missing')
  if (artifact.content !== want) throw new Error('content divergence')
  if (artifact.filename !== sessionLogJsonlFilename(header.id)) throw new Error('filename convention drift')
})

await check('streaming yields bounded chunks without whole-log strings', async () => {
  const pagesRead: number[] = []
  const instrumentedSource = {
    kind: 'instrumented',
    meta: (id: string) => src.meta(id),
    readRange: async (id: string, startSeq: number, o?: { limit?: number }) => {
      o?.limit !== undefined && pagesRead.push(o.limit)
      return await src.readRange(id, startSeq, o)
    },
    readSuffix: () => Promise.resolve({ events: [], inspectedCount: 0 }) as never,
    close: () => Promise.resolve(),
  }
  let chunks = 0
  let maxChunkLen = 0
  let totalLen = 0
  for await (const chunk of streamRawArtifact(instrumentedSource as never, header.id, { pageSize: 4096 })) {
    chunks++
    maxChunkLen = Math.max(maxChunkLen, chunk.length)
    totalLen += chunk.length
  }
  // 120k events / 4096 per range ⇒ many underlying reads; every yield must be
  // a bounded slice (~1 MiB), never the whole log as one string.
  const want = ((await store.loadStored(header.id))!.events ?? []) // eslint-disable-line @typescript-eslint/no-non-null-assertion
    .map((e) => JSON.stringify(e)).join('\n').length + 1
  assert(pagesRead.length > 20, `expected many range reads, got ${pagesRead.length}`)
  assert(totalLen === want, `streamed ${totalLen} != full artifact ${want}`)
  assert(maxChunkLen <= (1 << 20) + 65_536, `chunk exceeded bound: ${maxChunkLen}`)
})

await check('absent session resolves undefined like readRaw contract', async () => {
  const missing = await readRawArtifactPaged(src as never, crypto.randomUUID())
  if (missing !== undefined) throw new Error('missing session must resolve undefined')
})

src.close().catch(() => {})
store.close().catch(() => {})
rmSync(dir, { recursive: true, force: true })

console.log(`\nexport-consumer: ${passed} passed, ${failed} failed`)
process.exit(failed === 0 ? 0 : 1)
