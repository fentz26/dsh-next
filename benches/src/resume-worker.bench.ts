/**
 * Cold-load responsiveness benchmark (Track C, design 1 evidence):
 * reconstructs the giant chunk-stream log via (a) synchronous main-thread
 * SqliteStore.loadStored and (b) the Track B worker backend, showing that
 * worker reconstruction removes the multi-hundred-ms main-thread stall
 * even though total wall time is comparable.
 */
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createRequire } from 'node:module'
import { pathToFileURL } from 'node:url'
import { LagMonitor, cpuBaseline, fmt, snapshotResources } from './harness/metrics.ts'
import { WorkerSqliteBackend } from '../../packages/persistence-worker/src/backend.ts'

const require_ = createRequire(import.meta.url)
const dshRoot = process.env.DSH_ROOT
if (dshRoot === undefined) {
  console.error('SKIP: set DSH_ROOT')
  process.exit(0)
}

interface StockStore {
  open(): Promise<void>
  appendBatch(meta: unknown, events: unknown[], isMaterialized: boolean): Promise<void>
  loadStored(id: string): Promise<{ events?: unknown[] } | undefined>
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

export async function resumeWorkerBench(): Promise<void> {
  const streamBytes = Number(process.env.BENCH_RESUME_BYTES ?? 50 * 1024 * 1024)
  const dir = mkdtempSync(join(tmpdir(), 'dsh-next-resume-worker-'))
  const dbPath = join(dir, 'resume.sqlite')
  try {
    console.log(`# building ${fmt2(streamBytes / (1024 * 1024))} MB logical log…`)
    const builder = await makeStore(dbPath)
    const meta = { version: 0, id: crypto.randomUUID(), createdAt: Date.now(), cwd: '/tmp/bench' }
    const events = generateChunkStream(streamBytes).map((e, i) => ({ ...e, seq: i }))
    for (let i = 0; i < events.length; i += 500) {
      await builder.appendBatch(meta, events.slice(i, i + 500), false)
    }
    await builder.close()

    // Warm OS page cache so we compare CPU/architecture, not disk.
    for (let trial = 0; trial < 1; trial++) {
      const w = await makeWorker(dbPath)
      await w.loadStored(meta.id)
      await w.close()
    }

    const trials = Number(process.env.BENCH_TRIALS ?? 3)

    for (const mode of ['stock', 'worker', 'worker-paged'] as const) {
      let lastLoad = 0
      for (let trial = 0; trial < trials; trial++) {
        const lag = new LagMonitor()
        lag.start()
        const t0 = process.hrtime.bigint()
        const cpu0 = cpuBaseline()

        let count = 0
        if (mode === 'stock') {
          const store = await makeStore(dbPath)
          const t = process.hrtime.bigint()
          const loaded = await store.loadStored(meta.id)
          lastLoad = Number(process.hrtime.bigint() - t) / 1e6
          count = loaded?.events?.length ?? 0
          const snap = snapshotResources(t0, cpu0)
          const lags = lag.stop()
          const s = lags
          await store.close().catch(() => {})
          report(mode, trial, snap, lags, count, lastLoad, s.canaryMaxMs)
        } else {
          const be = await makeWorker(dbPath)
          const t = process.hrtime.bigint()
          const loaded =
            mode === 'worker-paged'
              ? ((await be.loadStoredPaged(meta.id, 20_000)) as { events?: unknown[] } | undefined)
              : ((await be.loadStored(meta.id)) as { events?: unknown[] } | undefined)
          lastLoad = Number(process.hrtime.bigint() - t) / 1e6
          count = loaded?.events?.length ?? 0
          const snap = snapshotResources(t0, cpu0)
          const stats = be.stats()
          const lags = lag.stop()
          await be.close().catch(() => {})
          report(
            mode,
            trial,
            snap,
            lags,
            count,
            lastLoad,
            lags.canaryMaxMs,
            stats.workerCpuUserMs + stats.workerCpuSystemMs,
          )
        }
      }
    }
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
}

type LagSummary = ReturnType<LagMonitor['stop']>

function report(
  mode: string,
  trial: number,
  snap: { wallMs: number; rssMiB: number },
  lags: LagSummary,
  count: number,
  loadMs: number,
  canaryMax: number,
  workerCpu?: number,
): void {
  console.log(
    [
      `resume-${mode} trial=${trial}`,
      `events=${count}`,
      `wall=${fmt(snap.wallMs)}ms`,
      `loadStored=${fmt(loadMs)}ms`,
      ...(workerCpu !== undefined ? [`cpuWorker=${fmt(workerCpu)}ms`] : []),
      `rss=${fmt(snap.rssMiB, 1)}MiB`,
      `lag mean=${fmt(lags.meanMs, 3)} p99=${fmt(lags.p99ms, 3)}`,
      `canaryMax=${fmt(canaryMax, 3)}`,
    ].join(' | '),
  )
}

async function makeStore(path: string): Promise<StockStore> {
  const { SqliteStore } = require_(pathToFileURL(`${dshRoot}/packages/session/session-persistence-sqlite/src/store.ts`).href) as {
    SqliteStore: new (o: unknown) => StockStore
  }
  const store = new SqliteStore({ path, journalMode: 'wal', busyTimeoutMs: 5000 })
  await store.open()
  return store
}

async function makeWorker(path: string): Promise<WorkerSqliteBackend> {
  const be = new WorkerSqliteBackend({ path, journalMode: 'wal', busyTimeoutMs: 5000 })
  await be.init()
  return be
}

function performanceNowFallback(): number {
  return Number(process.hrtime.bigint()) / 1e6
}
void performanceNowFallback

function fmt2(n: number): string {
  return n.toLocaleString('en-US', { maximumFractionDigits: 2 })
}
