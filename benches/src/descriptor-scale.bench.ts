/**
 * Descriptor-store scale benchmark (Track F measurement plan):
 * 10 / 1,000 / 10,000 synthetic dormant descriptors — storage metadata only,
 * keyless, no model calls, no polling scheduler.
 */
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createLatencyTracker, cpuBaseline, fmt, snapshotResources } from './harness/metrics.ts'
import {
  DESCRIPTOR_FORMAT_VERSION,
  DescriptorStore,
} from '../../packages/descriptor-store/src/index.ts'
import type { AgentDescriptor } from '../../packages/descriptor-store/src/index.ts'

function syntheticDescriptor(i: number): AgentDescriptor {
  return {
    sessionId: `sess-${crypto.randomUUID()}`,
    formatVersion: DESCRIPTOR_FORMAT_VERSION,
    agentPresetId: 'dsh-bench-default',
    executionWorld: { cwd: '/workspace/project' },
    wake: {
      kind: i % 10 === 0 ? 'wait-provider' : 'timer',
      key: String(Date.now() + ((i * 7919) % 8_640_000)),
    },
    lastCommitted: {
      revision: `${i}:incarnation:uuid${i}:revision:${(i % 50) + 1}`,
      seqWatermark: i,
    },
  }
}

export async function descriptorScaleBench(): Promise<void> {
  const dir = mkdtempSync(join(tmpdir(), 'dsh-next-descriptors-'))
  try {
    for (const scale of [10, 1_000, 10_000]) {
      const dbPath = join(dir, `desc-${scale}.sqlite`)
      const store = new DescriptorStore(dbPath)
      await store.open()
      const track = createLatencyTracker()
      const t0 = process.hrtime.bigint()
      const cpu0 = cpuBaseline()

      // Upsert all (simulates sleep transitions).
      const made: AgentDescriptor[] = []
      for (let i = 0; i < scale; i++) {
        const d = syntheticDescriptor(i)
        const bt = process.hrtime.bigint()
        await store.upsert(d)
        track.add(Number(process.hrtime.bigint() - bt) / 1e6)
        made.push(d)
      }

      // Get-by-id latency sample.
      const idTrack = createLatencyTracker()
      for (let probe = 0; probe < Math.min(scale, 500); probe++) {
        const bt = process.hrtime.bigint()
        await store.get('sess-missing-probe') as unknown
        idTrack.add(Number(process.hrtime.bigint() - bt) / 1e6)
      }

      // Wake-query scan (pull utility; timer keys "due now").
      const scanTrack = createLatencyTracker()
      for (let probe = 0; probe < 100; probe++) {
        const bt = process.hrtime.bigint()
        await store.findSleepingByWake('timer', String(Date.now()))
        scanTrack.add(Number(process.hrtime.bigint() - bt) / 1e6)
      }

      const n = await store.count()

      // Sleep→wake→sleep full transition cost (real records).
      const cycTrack = createLatencyTracker()
      for (let i = 0; i < Math.min(scale, 1_000); i++) {
        const bt = process.hrtime.bigint()
        await store.setWaking(made[i].sessionId)
        await store.upsert(made[i], 'sleeping')
        cycTrack.add(Number(process.hrtime.bigint() - bt) / 1e6)
      }

      const snap = snapshotResources(t0, cpu0)
      const ups = track.summary()
      const gets = idTrack.summary()
      const scans = scanTrack.summary()

      console.log(
        [
          `descriptor-scale n=${scale}`,
          `count=${n}`,
          `wall=${fmt(snap.wallMs)}ms`,
          `cpuMain=${fmt(snap.cpuUserMs + snap.cpuSystemMs)}ms`,
          `rss=${fmt(snap.rssMiB, 1)}MiB`,
          `upsert_ms p50=${fmt(ups.p50, 3)} p99=${fmt(ups.p99, 3)}`,
          `get_ms p50=${fmt(gets.p50, 3)} p99=${fmt(gets.p99, 3)}`,
          `wakeScan_ms p50=${fmt(scans.p50, 3)} p99=${fmt(scans.p99, 3)}`,
          `sleepWakeCycle_ms p50=${fmt(cycTrack.summary().p50, 3)} p99=${fmt(cycTrack.summary().p99, 3)}`,
        ].join(' | '),
      )
      await store.close()
    }
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
}
