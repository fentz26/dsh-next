/**
 * Expansion-cost attribution (Track C): splits decodeRow's work into
 * zstd-decompress / JSON.parse / envelope-construction fractions over the
 * REAL pipeline on a chunk-heavy log, so the upstream seam proposal carries
 * precise numbers instead of "expansion dominates".
 */
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createRequire } from 'node:module'
import { pathToFileURL } from 'node:url'
import { zstdDecompressSync } from 'node:zlib'

const require_ = createRequire(import.meta.url)
const dshRoot = process.env.DSH_ROOT
if (dshRoot === undefined) {
  console.error('SKIP: set DSH_ROOT')
  process.exit(0)
}
const pkg = `${dshRoot}/packages/session/session-persistence-sqlite`
const { SqliteStore } = require_(pathToFileURL(`${pkg}/src/store.ts`).href) as never as {
  SqliteStore: new (o: unknown) => StoreApi
}
const codec = await import(pathToFileURL(`${pkg}/src/codec.ts`).href) as unknown as {
  packChunkRuns(e: unknown[]): unknown[]
  decodeSerializedChunkRow(type: string, seq: number, time: number, data: string): unknown[]
}

interface StoreApi {
  open(): Promise<void>
  appendBatch(meta: unknown, events: unknown[], m: boolean): Promise<void>
  close(): Promise<void>
}

function generateChunkStream(totalBytes: number): Array<Record<string, unknown>> {
  const events: Array<Record<string, unknown>> = []
  let seq = 0
  let time = Date.now() - 60_000
  events.push({ type: 'turn/start', seq: seq++, time: (time += 2), data: { turn: 0 } })
  const nEvents = Math.floor(totalBytes / 48)
  for (let i = 0; i < nEvents; i++) {
    events.push({
      type: 'assistant/chunk',
      seq: seq++,
      time: (time += 1),
      data: { turn: 0, step: 0, chunk: { type: 'text-delta', index: 0, text: `tok-${i % 997}:`.repeat(3) } },
    })
  }
  return events
}

function rssMiB(): number {
  return process.memoryUsage().rss / (1024 * 1024)
}

export async function expansionAttributionBench(): Promise<void> {
  const streamBytes = Number(process.env.BENCH_RESUME_BYTES ?? 50 * 1024 * 1024)
  const dir = mkdtempSync(join(tmpdir(), 'dsh-next-attrib-'))
  try {
    const store = new SqliteStore({ path: join(dir, 'a.sqlite'), journalMode: 'wal', busyTimeoutMs: 5000 }) as StoreApi
    await store.open()
    const meta = { version: 0, id: crypto.randomUUID(), createdAt: Date.now(), cwd: '/w' }
    const events = generateChunkStream(streamBytes).map((e, i) => ({ ...e, seq: i }))
    for (let i = 0; i < events.length; i += 500) {
      await store.appendBatch(meta, events.slice(i, i + 500), false)
    }
    await store.close()

    // Pull packed rows and their raw data payloads.
    const db = DatabaseSyncRead(join(dir, 'a.sqlite'))
    const rowsRaw = db.all('SELECT seq, time, type, data FROM events ORDER BY seq') as Array<{
      seq: number; time: number; type: string; data: Buffer | string
    }>
    // Rows are physical StorageRecords: chunk rows encode via blob/compressed JSON.
    // Split phases across many iterations for stable percentages.
    let zstdMs = 0
    let parseMs = 0
    let expandMs = 0
    let decompressedBytes = 0
    const ITER = Number(process.env.BENCH_ATTRIB_ITERS ?? 8)

    for (let iter = 0; iter < ITER; iter++) {
      for (const r of rowsRaw) {
        const isPacked =
          r.type === 'text-chunks' || r.type === 'reasoning-chunks' || r.type === 'tool-call-chunks'
        if (isPacked) {
          // node:sqlite surfaces BLOBs as plain Uint8Array (not Buffer)
          const bytes = r.data instanceof Uint8Array && !(r.data instanceof Buffer)
            ? Buffer.from(r.data.buffer, r.data.byteOffset, r.data.byteLength)
            : Buffer.isBuffer(r.data) ? r.data : null
          if (bytes === null) continue // TEXT rows skip zstd entirely
          const tA = process.hrtime.bigint()
          const jsonText = zstdDecompressSync(bytes).toString('utf8')
          zstdMs += Number(process.hrtime.bigint() - tA) / 1e6
          decompressedBytes += jsonText.length

          // phase B: JSON.parse
          const tB = process.hrtime.bigint()
          const parsed = JSON.parse(jsonText) as Record<string, unknown>
          parseMs += Number(process.hrtime.bigint() - tB) / 1e6

          // phase C: envelope construction (real decode path on real data)
          const tC = process.hrtime.bigint()
          void codec.decodeSerializedChunkRow(r.type, r.seq, r.time, jsonText)
          expandMs += Number(process.hrtime.bigint() - tC) / 1e6
        }
      }
    }
    const total = zstdMs + parseMs + expandMs
    const logicalCount = rowsRaw.reduce((n, r) => {
      if (r.type !== 'text-chunks') return n
      const raw = r.data
      if (!(raw instanceof Uint8Array)) return n
      const b = Buffer.from(raw.buffer, raw.byteOffset, raw.byteLength)
      const parsed = JSON.parse(zstdDecompressSync(b).toString('utf8')) as { texts?: string[] }
      return n + (parsed.texts?.length ?? 1)
    }, 0)

    console.log(
      [
        `expansion-attribution rows=${rowsRaw.length} logicalEvents≈${logicalCount}`,
        `iters=${ITER}`,
        `zstd=${((zstdMs / ITER)).toFixed(0)}ms (${((100 * zstdMs) / total).toFixed(0)}%)`,
        `jsonParse=${((parseMs / ITER)).toFixed(0)}ms (${((100 * parseMs) / total).toFixed(0)}%)`,
        `envelopes(incl.2nd stub parse)=${((expandMs / ITER)).toFixed(0)}ms (${((100 * expandMs) / total).toFixed(0)}%)`,
        `decompressedMBperPass=${(decompressedBytes / ITER / 1024 / 1024).toFixed(1)}`,
        `rssNow=${rssMiB().toFixed(0)}MiB`,
      ].join(' | '),
    )
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
}

function DatabaseSyncRead(path: string): { all(q: string): unknown[] } & import('node:sqlite').DatabaseSync {
  const mod = require_('node:sqlite') as unknown as typeof import('node:sqlite')
  const Ctor = (mod as unknown as { DatabaseSync: new (p: string, o?: object) => import('node:sqlite').DatabaseSync }).DatabaseSync
  const db = new Ctor(path, { readOnly: true })
  return Object.assign(db, {
    all(q: string): unknown[] {
      return db.prepare(q).all()
    },
  })
}
void zstdDecompressSync
