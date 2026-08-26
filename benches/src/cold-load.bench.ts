/**
 * Cold-load benchmark: reconstruct full session logs from SQLite.
 * Builds each log once, then measures repeated loadStored / loadStoredFrom.
 */
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { LagMonitor, cpuBaseline,
  createLatencyTracker, fmt, snapshotResources } from './harness/metrics.ts'
import { sqliteStoreModule } from './harness/dsh.ts'
import { generateMixedEvents, generateChunkStream } from './harness/events.ts'
import type { DshSqliteStore } from './harness/dsh.ts'

async function buildDb(
  SqliteStoreCtor: new (o: {
    path: string
    journalMode: 'wal' | 'delete' | 'truncate' | 'persist'
    busyTimeoutMs: number
  }) => DshSqliteStore,
  path: string,
  events: Record<string, unknown>[],
): Promise<{ store: DshSqliteStore; id: string }> {
  const store = new SqliteStoreCtor({ path, journalMode: 'wal', busyTimeoutMs: 5000 })
  await store.open()
  const meta = { version: 0, id: crypto.randomUUID(), createdAt: Date.now(), cwd: '/tmp/bench' }
  for (let i = 0; i < events.length; i += 500) {
    await store.appendBatch(meta as never, events.slice(i, i + 500), false)
  }
  return { store, id: meta.id }
}

export async function coldLoadBench(): Promise<void> {
  const { SqliteStore } = await sqliteStoreModule()
  const dir = mkdtempSync(join(tmpdir(), 'dsh-next-coldload-'))
  const trials = Number(process.env.BENCH_TRIALS ?? 5)

  const scenarios: Array<{ name: string; events: Record<string, unknown>[] }> = [
    { name: 'mixed-10k', events: generateMixedEvents(10_000).map((e, i) => ({ ...e, seq: i })) },
    { name: 'mixed-100k', events: generateMixedEvents(100_000).map((e, i) => ({ ...e, seq: i })) },
    {
      name: 'chunk-stream-50MB',
      events: generateChunkStream(50 * 1024 * 1024).map((e, i) => ({ ...e, seq: i })),
    },
  ]

  try {
    for (const scenario of scenarios) {
      const dbPath = join(dir, `${scenario.name}.sqlite`)
      const { store, id } = await buildDb(SqliteStore, dbPath, scenario.events)

      for (let trial = 0; trial < trials; trial++) {
        const track = createLatencyTracker()
        const lag = new LagMonitor()
        const t0 = process.hrtime.bigint()
 const cpu0 = cpuBaseline()
        lag.start()

        const loaded = await store.loadStored(id)
        track.add(0)

        const snap = snapshotResources(t0, cpu0)
        const lags = lag.stop()
        const count = loaded?.events?.length ?? 0

        console.log(
          [
            `cold-load ${scenario.name} trial=${trial}`,
            `events=${count}`,
            `wall=${fmt(snap.wallMs)}ms`,
            `cpu=${fmt(snap.cpuUserMs + snap.cpuSystemMs)}ms`,
            `rss=${fmt(snap.rssMiB, 1)}MiB`,
            `lag mean=${fmt(lags.meanMs)} p95=${fmt(lags.p95ms)} p99=${fmt(lags.p99ms)} canaryP99=${fmt(lags.canaryP99Ms, 3)} canaryMax=${fmt(lags.canaryMaxMs, 3)} drift=${fmt(lags.maxDriftMs)}`,
          ].join(' | '),
        )
      }

      // Suffix read from halfway through the log.
      const halfSeq = Math.floor(scenario.events.length / 2)
      for (let trial = 0; trial < trials; trial++) {
        const t0 = process.hrtime.bigint()
  const cpu0 = cpuBaseline()
        await store.loadStoredFrom(id, halfSeq)
        const snap = snapshotResources(t0, cpu0)
        console.log(
          [
            `suffix-read ${scenario.name} trial=${trial}`,
            `fromSeq=${halfSeq}`,
            `wall=${fmt(snap.wallMs)}ms`,
            `cpu=${fmt(snap.cpuUserMs + snap.cpuSystemMs)}ms`,
          ].join(' | '),
        )
      }

      await store.close()
      rmSync(dbPath, { force: true })
      rmSync(`${dbPath}-wal`, { force: true })
      rmSync(`${dbPath}-shm`, { force: true })
    }
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
}
