/**
 * Session persistence append benchmark — real DSH SQLite backend.
 *
 * Drives SqliteStore exactly as the coordinator would (appendBatch with
 * contiguous seqs), measuring wall/CPU/RSS/event-loop lag and per-batch
 * latency percentiles.
 */
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
  LagMonitor,
  cpuBaseline,
  createLatencyTracker,
  fmt,
  snapshotResources,
} from './harness/metrics.ts'
import { sqliteStoreModule } from './harness/dsh.ts'
import { generateMixedEvents } from './harness/events.ts'
import type { DshSqliteStore } from './harness/dsh.ts'

const BATCH_SIZES = [50, 200]

/** One macrotask turn: mimics real harness flow where each durable batch is
 * scheduled from timer/network callbacks, letting loop probes observe stalls. */
const immediate = () => new Promise<void>((resolve) => setImmediate(resolve))

async function runAppend(
  store: DshSqliteStore,
  meta: Record<string, unknown>,
  events: Record<string, unknown>[],
  batchSize: number,
  track: ReturnType<typeof createLatencyTracker>,
): Promise<void> {
  for (let i = 0; i < events.length; i += batchSize) {
    const batch = events.slice(i, i + batchSize)
    await immediate()
    const t0 = process.hrtime.bigint()
 const cpu0 = cpuBaseline()
    await store.appendBatch(meta as never, batch, false)
    track.add(Number(process.hrtime.bigint() - t0) / 1e6)
  }
}

export async function persistenceBench(): Promise<void> {
  const { SqliteStore } = await sqliteStoreModule()
  const dir = mkdtempSync(join(tmpdir(), 'dsh-next-bench-'))
  const trials = Number(process.env.BENCH_TRIALS ?? 3)

  try {
    for (const total of [10_000, 100_000]) {
      const events = generateMixedEvents(total)
      const baseSeq = events[0].seq as number
      // Re-seq from 0 per store run.
      let seq = 0
      for (const e of events) e.seq = seq++
      void baseSeq

      for (const batchSize of BATCH_SIZES) {
        for (let trial = 0; trial < trials; trial++) {
          const dbPath = join(dir, `append-${total}-${batchSize}-${trial}.sqlite`)
          const store = new SqliteStore({
            path: dbPath,
            journalMode: 'wal',
            busyTimeoutMs: 5000,
          })
          await store.open()
          const meta = {
            version: 0,
            id: crypto.randomUUID(),
            createdAt: Date.now(),
            cwd: '/tmp/bench',
          }
          const track = createLatencyTracker()
          const lag = new LagMonitor()
          const t0 = process.hrtime.bigint()
  const cpu0 = cpuBaseline()
          lag.start()

          await runAppend(store, meta, events, batchSize, track)

          const snap = snapshotResources(t0, cpu0)
          const lags = lag.stop()
          await store.close()
          rmSync(dbPath, { force: true })
          rmSync(`${dbPath}-wal`, { force: true })
          rmSync(`${dbPath}-shm`, { force: true })

          const s = track.summary()
          console.log(
            [
              `persistence-append total=${total} batch=${batchSize} trial=${trial}`,
              `wall=${fmt(snap.wallMs)}ms`,
              `cpu=${fmt(snap.cpuUserMs + snap.cpuSystemMs)}ms`,
              `rss=${fmt(snap.rssMiB, 1)}MiB`,
              `events/s=${fmt((total / snap.wallMs) * 1000)}`,
              `batch_ms p50=${fmt(s.p50)} p95=${fmt(s.p95)} p99=${fmt(s.p99)} max=${fmt(s.max)}`,
              `lag mean=${fmt(lags.meanMs)} p95=${fmt(lags.p95ms)} p99=${fmt(lags.p99ms)} canaryP99=${fmt(lags.canaryP99Ms, 3)} canaryMax=${fmt(lags.canaryMaxMs, 3)} drift=${fmt(lags.maxDriftMs)}`,
            ].join(' | '),
          )
        }
      }
    }

    // Concurrent sessions: N stores appending to the same database file.
    for (const concurrency of [1, 10, 50]) {
      const eventsPerSession = generateMixedEvents(2_000).map((e, i) => ({ ...e, seq: i }))
      for (let trial = 0; trial < trials; trial++) {
        const dbPath = join(dir, `concurrent-${concurrency}-${trial}.sqlite`)
        const stores: DshSqliteStore[] = []
        const metas: Record<string, unknown>[] = []
        for (let i = 0; i < concurrency; i++) {
          const store = new SqliteStore({ path: dbPath, journalMode: 'wal', busyTimeoutMs: 5000 })
          await store.open()
          stores.push(store)
          metas.push({ version: 0, id: crypto.randomUUID(), createdAt: Date.now(), cwd: '/tmp/bench' })
        }
        const track = createLatencyTracker()
        const lag = new LagMonitor()
        const t0 = process.hrtime.bigint()
  const cpu0 = cpuBaseline()
        lag.start()

        await Promise.all(
          stores.map(async (store, idx) => {
            for (let i = 0; i < eventsPerSession.length; i += 100) {
              const batch = eventsPerSession
                .slice(i, i + 100)
                .map((e) => ({ ...e }))
              await immediate()
              const bt = process.hrtime.bigint()
              await store.appendBatch(metas[idx] as never, batch, false)
              track.add(Number(process.hrtime.bigint() - bt) / 1e6)
            }
          }),
        )

        const snap = snapshotResources(t0, cpu0)
        const lags = lag.stop()
        for (const store of stores) await store.close()
        rmSync(dbPath, { force: true })
        rmSync(`${dbPath}-wal`, { force: true })
        rmSync(`${dbPath}-shm`, { force: true })

        const s = track.summary()
        console.log(
          [
            `persistence-concurrent n=${concurrency} trial=${trial}`,
            `wall=${fmt(snap.wallMs)}ms`,
            `cpu=${fmt(snap.cpuUserMs + snap.cpuSystemMs)}ms`,
            `rss=${fmt(snap.rssMiB, 1)}MiB`,
            `batch_ms p50=${fmt(s.p50)} p95=${fmt(s.p95)} p99=${fmt(s.p99)} max=${fmt(s.max)}`,
            `lag mean=${fmt(lags.meanMs)} p95=${fmt(lags.p95ms)} p99=${fmt(lags.p99ms)} canaryP99=${fmt(lags.canaryP99Ms, 3)} canaryMax=${fmt(lags.canaryMaxMs, 3)} drift=${fmt(lags.maxDriftMs)}`,
          ].join(' | '),
        )
      }
    }
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
}
