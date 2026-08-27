/**
 * Paged-hydration acceptance demo (#121):
 *   Baseline : full cold load of a ~1M-event session (eager whole graph)
 *   Paged    : readSuffix(4000) — target reads+materializes ~4k events only
 *
 * Reported per side: wall / cpu / RSS / logical events decoded / objects allocated.
 */
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createRequire } from 'node:module'
import { pathToFileURL } from 'node:url'
import { cpuBaseline, fmt, snapshotResources } from './harness/metrics.ts'
import { openSqliteSource } from '../../packages/paged-history/src/index.ts'

const require_ = createRequire(import.meta.url)
const dshRoot = process.env.DSH_ROOT
if (dshRoot === undefined) {
  console.error('SKIP: set DSH_ROOT')
  process.exit(0)
}
const { SqliteStore } = require_(pathToFileURL(`${dshRoot}/packages/session/session-persistence-sqlite/src/store.ts`).href) as {
  SqliteStore: new (o: unknown) => {
    open(): Promise<void>
    appendBatch(m: unknown, e: unknown[], f: boolean): Promise<void>
    loadStored(id: string): Promise<{ events?: unknown[] } | undefined>
    close(): Promise<void>
  }
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
      data: { turn: 0, step: 0, chunk: { type: 'text-delta', index: 0, text: `q${i % 997}`.repeat(3) } },
    })
  }
  return events
}

const rssMiB = (): number => process.memoryUsage().rss / (1024 * 1024)

export async function pagedAcceptanceBench(): Promise<void> {
  const streamBytes = Number(process.env.BENCH_RESUME_BYTES ?? 50 * 1024 * 1024)
  const dir = mkdtempSync(join(tmpdir(), 'paged-acceptance-'))
  try {
    console.log(`# building ${fmt(streamBytes / (1024 * 1024), 0)} MB logical log (~${fmt(Math.floor(streamBytes / 48), 0)} events)…`)
    const dbPath = join(dir, 'giant.sqlite')
    const builder = new SqliteStore({ path: dbPath, journalMode: 'wal', busyTimeoutMs: 5000 })
    await builder.open()
    const meta = { version: 0, id: crypto.randomUUID(), createdAt: Date.now(), cwd: '/w' }
    const events = generateChunkStream(streamBytes).map((e, i) => ({ ...e, seq: i }))
    for (let i = 0; i < events.length; i += 500) await builder.appendBatch(meta, events.slice(i, i + 500), false)

    // Baseline: full canonical load.
    const t0 = process.hrtime.bigint()
    const c0 = cpuBaseline()
    const r0 = rssMiB()
    const full = await builder.loadStored(meta.id)
    const snapFull = snapshotResources(t0, c0)
    const fullRssPeak = rssMiB() - r0

    // Paged path: metadata then last 4k events only.
    const src = await openSqliteSource(dbPath)
    const mt = process.hrtime.bigint()
    const mc = cpuBaseline()
    const mr0 = rssMiB()
    const info = await src.meta(meta.id)
    const page = await src.readSuffix(meta.id, 4000)
    const snapPaged = snapshotResources(mt, mc)
    void src.close()

    console.log(
      [
        `acceptance-baseline(full-load)`,
        `events_materialized=${fmt(full?.events?.length ?? 0, 0)}`,
        `wall=${fmt(snapFull.wallMs)}ms`,
        `cpu=${fmt(snapFull.cpuUserMs + snapFull.cpuSystemMs)}ms`,
        `rssDelta=+${fmt(fullRssPeak, 1)}MiB`,
      ].join(' | '),
    )
    console.log(
      [
        `acceptance-paged(suffix-4000)`,
        `logical_events_in_log=${info ? fmt(info.length, 0) : '?'}`,
        `objects_allocated=${fmt(page.inspectedCount, 0)}`,
        `wall=${fmt(snapPaged.wallMs)}ms`,
        `cpu=${fmt(snapPaged.cpuUserMs + snapPaged.cpuSystemMs)}ms`,
        `rssDelta=+${fmt(rssMiB() - mr0, 1)}MiB`,
        `payloadKB=${fmt(page.approxPayloadBytes / 1024, 1)}`,
      ].join(' | '),
    )
    await builder.close().catch(() => {})
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
}
