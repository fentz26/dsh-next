/**
 * Worker persistence benchmark: stock synchronous main-thread SqliteStore vs
 * worker-thread-owned SqliteStore across the exact Phase 0 workloads.
 */
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createRequire } from 'node:module'
import { pathToFileURL } from 'node:url'
import { LagMonitor, cpuBaseline, fmt, snapshotResources } from './harness/metrics.ts'
import { WorkerSqliteBackend } from '../../packages/persistence-worker/src/backend.ts'

interface StockStore {
  open(): Promise<void>
  appendBatch(meta: unknown, events: unknown[], isMaterialized: boolean): Promise<void>
  close(): Promise<void>
}

const require_ = createRequire(import.meta.url)

async function makeStock(path: string): Promise<StockStore> {
  const dshRoot = process.env.DSH_ROOT
  if (dshRoot === undefined) throw new Error('set DSH_ROOT')
  const { SqliteStore } = require_(pathToFileURL(`${dshRoot}/packages/session/session-persistence-sqlite/src/store.ts`).href) as {
    SqliteStore: new (o: { path: string; journalMode: string; busyTimeoutMs: number }) => StockStore
  }
  const store = new SqliteStore({ path, journalMode: 'wal', busyTimeoutMs: 5000 })
  await store.open()
  return store
}

const immediate = () => new Promise<void>((resolve) => setImmediate(resolve))

function generateMixedEvents(count: number): Array<Record<string, unknown>> {
  const events: Array<Record<string, unknown>> = []
  let seq = 0
  let time = Date.now() - 3_600_000
  let turn = 0
  while (events.length < count) {
    events.push({ type: 'turn/start', seq: seq++, time: (time += 2), data: { turn } })
    events.push({
      type: 'user/message',
      seq: seq++,
      time: (time += 3),
      data: { text: `Investigate subsystem ${turn} and report findings.` },
    })
    for (let c = 0; c < 40 && events.length + 4 < count; c++) {
      events.push({
        type: 'assistant/chunk',
        seq: seq++,
        time: (time += 1),
        data: { turn, step: 0, chunk: { type: 'text-delta', index: 0, text: 'x'.repeat(36) } },
      })
    }
    events.push({
      type: 'assistant/message',
      seq: seq++,
      time: (time += 5),
      data: { text: `Analysis ${turn}`, turn: 0, step: 0 },
    })
    events.push({ type: 'turn/end', seq: seq++, time: (time += 10), data: { turn, reason: 'end-turn' } })
    turn++
  }
  return events.slice(0, count)
}

export async function persistenceWorkerBench(): Promise<void> {
  const dir = mkdtempSync(join(tmpdir(), 'dsh-next-workerbench-'))
  const trials = Number(process.env.BENCH_TRIALS ?? 3)

  try {
    // ---- Sequential single-session campaigns ----
    for (const total of [10_000, 100_000]) {
      for (const batchSize of [50, 200]) {
        for (let trial = 0; trial < trials; trial++) {
          for (const mode of ['stock', 'worker'] as const) {
            const dbPath = join(dir, `${total}-${batchSize}-${trial}-${mode}.sqlite`)
            const store =
              mode === 'stock'
                ? await makeStock(dbPath)
                : await (async () => {
                    const be = new WorkerSqliteBackend({ path: dbPath, journalMode: 'wal', busyTimeoutMs: 5000 })
                    await be.init()
                    return be
                  })()
            const meta = { version: 0, id: crypto.randomUUID(), createdAt: Date.now(), cwd: '/tmp/bench' }

            const track = new LatencyTrackerLite()
            const lag = new LagMonitor()
            lag.start()
            const t0 = process.hrtime.bigint()
            const cpu0 = cpuBaseline()

            const allEvents = generateMixedEvents(total).map((e, i) => ({ ...e, seq: i }))
            for (let i = 0; i < allEvents.length; i += batchSize) {
              const batch = allEvents.slice(i, i + batchSize)
              await immediate()
              const bt = process.hrtime.bigint()
              await store.appendBatch(meta, batch, false)
              track.add(Number(process.hrtime.bigint() - bt) / 1e6)
            }

            const snap = snapshotResources(t0, cpu0)
            const lags = lag.stop()
            await store.close().catch(() => {})
            rmSync(dbPath, { force: true })
            rmSync(`${dbPath}-wal`, { force: true })
            rmSync(`${dbPath}-shm`, { force: true })

            const s = track.summary()
            const stats = mode === 'worker' ? (store as WorkerSqliteBackend).stats() : undefined
            console.log(
              [
                `persist-${mode} total=${total} batch=${batchSize} trial=${trial}`,
                `wall=${fmt(snap.wallMs)}ms`,
                `cpuMain=${fmt(snap.cpuUserMs + snap.cpuSystemMs)}ms`,
                ...(stats ? [`cpuWorker=${fmt(stats.workerCpuUserMs + stats.workerCpuSystemMs)}ms`] : []),
                `rss=${fmt(snap.rssMiB, 1)}MiB`,
                `events/s=${fmt((total / snap.wallMs) * 1000)}`,
                `batch_ms p50=${fmt(s.p50, 3)} p95=${fmt(s.p95, 3)} p99=${fmt(s.p99, 3)} max=${fmt(s.max, 3)}`,
                `lag mean=${fmt(lags.meanMs, 3)} p95=${fmt(lags.p95ms, 3)} p99=${fmt(lags.p99ms, 3)} canaryMax=${fmt(lags.canaryMaxMs, 3)}`,
              ].join(' | '),
            )
          }
        }
      }
    }

    // ---- Concurrent sessions ----
    for (const concurrency of [1, 10, 50]) {
      for (let trial = 0; trial < trials; trial++) {
        for (const mode of ['stock', 'worker'] as const) {
          const dbPath = join(dir, `conc-${concurrency}-${trial}-${mode}.sqlite`)
          const store =
            mode === 'stock'
              ? await makeStock(dbPath)
              : await (async () => {
                  const be = new WorkerSqliteBackend({ path: dbPath, journalMode: 'wal', busyTimeoutMs: 5000 })
                  await be.init()
                  return be
                })()

          const metas = Array.from({ length: concurrency }, () => ({
            version: 0,
            id: crypto.randomUUID(),
            createdAt: Date.now(),
            cwd: '/tmp/bench',
          }))
          const perSession = generateMixedEvents(2_000).map((e, i) => ({ ...e, seq: i }))

          const track = new LatencyTrackerLite()
          const lag = new LagMonitor()
          lag.start()
          const t0 = process.hrtime.bigint()
          const cpu0 = cpuBaseline()

          await Promise.all(
            metas.map(async (meta) => {
              for (let i = 0; i < perSession.length; i += 100) {
                const batch = perSession.slice(i, i + 100).map((e) => ({ ...e }))
                await immediate()
                const bt = process.hrtime.bigint()
                await store.appendBatch(meta, batch, false)
                track.add(Number(process.hrtime.bigint() - bt) / 1e6)
              }
            }),
          )

          const snap = snapshotResources(t0, cpu0)
          const lags = lag.stop()
          const stats = mode === 'worker' ? (store as WorkerSqliteBackend).stats() : undefined
          await store.close().catch(() => {})
          rmSync(dbPath, { force: true })
          rmSync(`${dbPath}-wal`, { force: true })
          rmSync(`${dbPath}-shm`, { force: true })

          const s = track.summary()
          console.log(
            [
              `persist-conc-${mode} n=${concurrency} trial=${trial}`,
              `wall=${fmt(snap.wallMs)}ms`,
              `cpuMain=${fmt(snap.cpuUserMs + snap.cpuSystemMs)}ms`,
              ...(stats ? [`cpuWorker=${fmt(stats.workerCpuUserMs + stats.workerCpuSystemMs)}ms`] : []),
              `rss=${fmt(snap.rssMiB, 1)}MiB`,
              `batch_ms p50=${fmt(s.p50, 3)} p95=${fmt(s.p95, 3)} p99=${fmt(s.p99, 3)} max=${fmt(s.max, 3)}`,
              `lag mean=${fmt(lags.meanMs, 3)} p95=${fmt(lags.p95ms, 3)} p99=${fmt(lags.p99ms, 3)} canaryMax=${fmt(lags.canaryMaxMs, 3)}`,
            ].join(' | '),
          )
        }
      }
    }
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
}

class LatencyTrackerLite {
  private samples: number[] = []
  add(ms: number): void {
    this.samples.push(ms)
  }
  summary(): { p50: number; p95: number; p99: number; max: number } {
    const sorted = [...this.samples].sort((a, b) => a - b)
    const pct = (p: number) => sorted[Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1)] ?? Number.NaN
    return { p50: pct(50), p95: pct(95), p99: pct(99), max: sorted[sorted.length - 1] ?? Number.NaN }
  }
}
