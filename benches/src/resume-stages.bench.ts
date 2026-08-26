/**
 * Resume-stage profiler (Track C).
 *
 * Breaks giant-session cold reconstruction into stages against the REAL DSH
 * pipeline so optimization targets structural costs instead of one black box:
 *
 *   S1 sql-read      : raw SELECT of physical rows
 *   S2 row-decode    : decodeEventRow (sqlite value -> EventRow)
 *   S3 expand        : decodeRow (packed rows -> logical SessionEvents)
 *   S4 validate      : scanRows committed-prefix validation
 *   full loadStored  : real SqliteStore.loadStored for cross-check
 */
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createRequire } from 'node:module'
import { pathToFileURL } from 'node:url'

const require_ = createRequire(import.meta.url)

const dshRoot = process.env.DSH_ROOT
if (dshRoot === undefined) {
  console.error('SKIP: set DSH_ROOT')
  process.exit(0)
}
const sqlitePkgDir = `${dshRoot}/packages/session/session-persistence-sqlite`
const { SqliteStore } = require_(pathToFileURL(`${sqlitePkgDir}/src/store.ts`).href) as {
  SqliteStore: new (o: unknown) => StockStore
}
const compression = require_(pathToFileURL(`${sqlitePkgDir}/src/compression.ts`).href) as {
  scanRows(rows: Row[]): { preserved: unknown[]; tornFrom?: number }
}
const schemaMod = require_(pathToFileURL(`${sqlitePkgDir}/src/schema.ts`).href) as {
  decodeEventRow(r: Record<string, unknown>): Row
}

interface StockStore {
  open(): Promise<void>
  appendBatch(meta: unknown, events: unknown[], isMaterialized: boolean): Promise<void>
  loadStored(id: string): Promise<{ events?: unknown[] } | undefined>
  close(): Promise<void>
}

interface Row {
  [k: string]: unknown
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
      data: { turn: 0, step: 0, chunk: { type: 'text-delta', index: 0, text: 'q'.repeat(40) } },
    })
  }
  events.push({
    type: 'assistant/message',
    seq: seq++,
    time: (time += 4),
    data: { text: '[stream complete]', turn: 0, step: 0 },
    surfaceOp: 'append',
  })
  events.push({ type: 'turn/end', seq: seq++, time: (time += 1), data: { turn: 0, reason: 'end-turn' } })
  return events
}

function rssMiB(): number {
  return process.memoryUsage().rss / (1024 * 1024)
}

export async function resumeStagesBench(): Promise<void> {
  const streamBytes = Number(process.env.BENCH_RESUME_BYTES ?? 50 * 1024 * 1024)
  const dir = mkdtempSync(join(tmpdir(), 'dsh-next-resume-'))
  const dbPath = join(dir, 'resume.sqlite')

  try {
    // Build once with the stock store (batched).
    console.log(`# building ${fmt2(streamBytes / (1024 * 1024))} MB logical chunk-stream log…`)
    const store = new SqliteStore({ path: dbPath, journalMode: 'wal', busyTimeoutMs: 5000 }) as StockStore
    await store.open()
    const meta = { version: 0, id: crypto.randomUUID(), createdAt: Date.now(), cwd: '/tmp/bench' }
    const events = generateChunkStream(streamBytes).map((e, i) => ({ ...e, seq: i }))
    for (let i = 0; i < events.length; i += 500) {
      await store.appendBatch(meta as never, events.slice(i, i + 500), false)
    }

    const { DatabaseSync } = require_('node:sqlite') as typeof import('node:sqlite')
    await store.close()

    const trials = Number(process.env.BENCH_TRIALS ?? 3)
    for (let trial = 0; trial < trials; trial++) {
      global.gc?.()
      const baseRss = rssMiB()

      const db = new DatabaseSync(dbPath, { readOnly: true })
      // S1: raw row read
      let t1 = performanceNow()
      const rowsRaw = db.prepare('SELECT * FROM events WHERE session_id = ? ORDER BY seq').all(meta.id) as Array<Record<string, unknown>>
      t1 = performanceNow() - t1

      // S2: sqlite-value -> EventRow decode
      let rssBeforeExpand = rssMiB()
      const rowsDecoded: Row[] = []
      let t2 = performanceNow()
      for (const r of rowsRaw) rowsDecoded.push(schemaMod.decodeEventRow(r))
      t2 = performanceNow() - t2
      void rssBeforeExpand

      // S3: logical expansion (packed -> individual events)
      const { decodeRow } = require_(pathToFileURL(`${sqlitePkgDir}/src/compression.ts`).href) as {
        decodeRow(r: Row): unknown[]
      }
      let t3 = performanceNow()
      const expanded: number = rowsDecoded.reduce((n: number, r) => n + decodeRow(r).length, 0)
      t3 = performanceNow() - t3

      // S4: committed-prefix validation over decoded rows
      let t4 = performanceNow()
      const scanned = compression.scanRows(rowsDecoded)
      t4 = performanceNow() - t4
      const logicalCount = scanned.preserved.length
      void expanded

      // full loadStored comparison using a fresh store instance
      const storeFull = new SqliteStore({ path: dbPath, journalMode: 'wal', busyTimeoutMs: 5000 }) as StockStore
      await storeFull.open()
      const tfullStart = performanceNow()
      const loaded = await storeFull.loadStored(meta.id)
      const tFull = performanceNow() - tfullStart
      const peakRss = rssMiB()
      await storeFull.close()

      console.log(
        [
          `resume-stages trial=${trial}`,
          `physicalRows=${rowsDecoded.length}`,
          `logicalEvents=${logicalCount}`,
          `S1_sqlRead=${fmt2(t1)}ms`,
          `S2_rowDecode=${fmt2(t2)}ms`,
          `S3_expand=${fmt2(t3)}ms`,
          `S4_validate=${fmt2(t4)}ms`,
          `sum=${fmt2(t1 + t2 + t3 + t4)}ms`,
          `fullLoadStored=${fmt2(tFull)}ms`,
          `rssBase=${fmt2(baseRss)}MiB rssPeak=${fmt2(peakRss)}MiB`,
          `fullLoadCount=${loaded?.events?.length ?? 0}`,
        ].join(' | '),
      )
      db.close()
    }
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
}

function performanceNow(): number {
  return Number(process.hrtime.bigint()) / 1e6
}

function fmt2(n: number): string {
  return n.toLocaleString('en-US', { maximumFractionDigits: 2 })
}
