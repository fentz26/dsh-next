/**
 * OutputCollector benchmark — DSH's current collector vs optimized TS
 * segmented journal vs native Rust journal (when built).
 *
 * Scenarios:
 *  A. producer push throughput
 *  B. N observers polling readFrom
 *  C. 100 concurrent collectors
 */
import { randomBytes } from 'node:crypto'
import { createRequire } from 'node:module'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { LagMonitor, cpuBaseline,
  createLatencyTracker, fmt, snapshotResources } from './harness/metrics.ts'
import { outputCollectorModule } from './harness/dsh.ts'
import { ReferenceByteJournal, SegmentedByteJournal } from '@dsh-next/journal'

type ReadResult = {
  text?: string
  data?: Uint8Array
  nextOffset: number
  lossy: boolean
}

interface ByteSink {
  readonly kind: string
  push(chunk: Buffer): void
  readFrom(offset: number): ReadResult
  next(): number
}

async function loadNativeMod(): Promise<Record<string, unknown> | undefined> {
  try {
    const require = createRequire(import.meta.url)
    return require('../../crates/native-journal/pilot.node') as Record<string, unknown>
  } catch {
    return undefined
  }
}

const STREAM_BYTES = Number(process.env.BENCH_STREAM_BYTES ?? 64 * 1024 * 1024)
const FILL_BYTES = Math.min(STREAM_BYTES, 32 * 1024 * 1024)
const PUSH_CHUNK = 8192
const MAX_BYTES = 4 * 1024 * 1024

function makeSinks(dshCtor: unknown, spillDir: string, nativeMod: Record<string, unknown> | undefined): Array<() => ByteSink> {
  const factories: Array<() => ByteSink> = []

  factories.push(() => {
    // DSH collector keeps its own total offset; readFrom(0) returns nextOffset.
    const c = new (dshCtor as new (
      maxBytes: number,
      maxSpillBytes: number | undefined,
      label: string,
      spillDir: string,
    ) => { push(chunk: Buffer): void; readFrom(fromByte: number): { text: string; nextOffset: number; lossy: boolean } })(
      MAX_BYTES,
      undefined,
      'bench',
      spillDir,
    )
    return {
      kind: 'dsh-collector',
      push: (b) => c.push(b),
      readFrom: (o) => c.readFrom(o),
      next: () => c.readFrom(0).nextOffset,
    }
  })

  factories.push(() => {
    const j = new ReferenceByteJournal(MAX_BYTES)
    return {
      kind: 'ts-reference',
      push: (b) => void j.append(b),
      readFrom: (o) => j.readFrom(o),
      next: () => j.nextOffset,
    }
  })

  factories.push(() => {
    const j = new SegmentedByteJournal(MAX_BYTES)
    return {
      kind: 'ts-segmented',
      push: (b) => void j.append(b),
      readFrom: (o) => j.readFrom(o),
      next: () => j.nextOffset,
    }
  })

  if (nativeMod && typeof nativeMod.NativeByteJournal === 'function') {
    const NativeJ = nativeMod.NativeByteJournal as new (m: number) => {
      append(b: Buffer): number
      readFrom(o: number): { data: Buffer; nextOffset: number; lossy: boolean }
      nextOffset: number
    }
    factories.push(() => {
      const j = new NativeJ(MAX_BYTES)
      return {
        kind: 'native-rust',
        push: (b) => void j.append(b),
        readFrom: (o) => j.readFrom(o),
        next: () => j.nextOffset,
      }
    })
  }

  return factories
}

export async function collectorBench(): Promise<void> {
  const { OutputCollector } = await outputCollectorModule()
  const nativeMod = await loadNativeMod()
  if (!nativeMod) console.log('(native module not built — skipping native-rust rows)')
  const spillDir = mkdtempSync(join(tmpdir(), 'dsh-next-collector-'))
  const chunk = randomBytes(PUSH_CHUNK)

  try {
    for (const make of makeSinks(OutputCollector, spillDir, nativeMod)) {
      const sink = make()

      // Scenario A: pure push throughput.
      const lagA = new LagMonitor()
      const t0 = process.hrtime.bigint()
 const cpu0 = cpuBaseline()
      lagA.start()
      let pushed = 0
      while (pushed < STREAM_BYTES) {
        sink.push(chunk)
        pushed += chunk.length
      }
      const snapA = snapshotResources(t0, cpu0)
      const lagsA = lagA.stop()
      console.log(
        [
          `collector-push impl=${sink.kind}`,
          `stream=${fmt(STREAM_BYTES / (1024 * 1024), 0)}MiB`,
          `wall=${fmt(snapA.wallMs)}ms`,
          `throughput=${fmt(STREAM_BYTES / 1024 / 1024 / (snapA.wallMs / 1000), 1)}MiB/s`,
          `cpu=${fmt(snapA.cpuUserMs + snapA.cpuSystemMs)}ms`,
          `rss=${fmt(snapA.rssMiB, 1)}MiB`,
          `lag p99=${fmt(lagsA.p99ms, 3)} maxDrift=${fmt(lagsA.maxDriftMs, 3)}`,
        ].join(' | '),
      )

      // Scenario B: observers polling readFrom against a filled window.
      let filled = 0
      while (filled < FILL_BYTES) {
        sink.push(chunk)
        filled += chunk.length
      }
      for (const observers of [1, 10]) {
        const readsPerObserver = 3000
        const track = createLatencyTracker()
        const t1 = process.hrtime.bigint()
 const cpu1 = cpuBaseline()
        await Promise.all(
          Array.from({ length: observers }, (_, o) =>
            setImmediatePromise(() => {
              let offset = o === 0 ? Math.max(0, sink.next() - 1024) : sink.next() - 2048
              for (let r = 0; r < readsPerObserver; r++) {
                const bt = process.hrtime.bigint()
                const res = sink.readFrom(offset)
                track.add(Number(process.hrtime.bigint() - bt) / 1e6)
                if (res.nextOffset > offset) offset = res.nextOffset - (res.nextOffset > offset + 1024 ? 1024 : 0)
              }
            }),
          ),
        )
        const snapB = snapshotResources(t1, cpu1)
        const s = track.summary()
        console.log(
          [
            `collector-observers impl=${sink.kind} n=${observers}`,
            `reads=${fmt(observers * readsPerObserver, 0)}`,
            `wall=${fmt(snapB.wallMs)}ms`,
            `read_us p50=${fmt(s.p50 * 1000, 1)} p95=${fmt(s.p95 * 1000, 1)} p99=${fmt(s.p99 * 1000, 1)} max=${fmt(s.max * 1000, 1)}`,
            `cpu=${fmt(snapB.cpuUserMs + snapB.cpuSystemMs)}ms`,
          ].join(' | '),
        )
      }
    }

    // Scenario C: 100 concurrent collectors.
    for (const make of makeSinks(OutputCollector, spillDir, nativeMod)) {
      const probe = make()
      const collectors = Array.from({ length: 100 }, () => make())
      const lagC = new LagMonitor()
      const t2 = process.hrtime.bigint()
 const cpu2 = cpuBaseline()
      lagC.start()
      const bytesEach = 2 * 1024 * 1024
      for (let i = 0; i < bytesEach / PUSH_CHUNK; i++) {
        for (const c of collectors) c.push(chunk)
      }
      const snapC = snapshotResources(t2, cpu2)
      const lagsC = lagC.stop()
      console.log(
        [
          `collector-x100 impl=${probe.kind}`,
          `total=${fmt((bytesEach * 100) / (1024 * 1024), 0)}MiB`,
          `wall=${fmt(snapC.wallMs)}ms`,
          `throughput=${fmt((bytesEach * 100) / 1024 / 1024 / (snapC.wallMs / 1000), 1)}MiB/s`,
          `rss=${fmt(snapC.rssMiB, 1)}MiB`,
          `lag p99=${fmt(lagsC.p99ms, 3)} maxDrift=${fmt(lagsC.maxDriftMs, 3)}`,
        ].join(' | '),
      )
    }
  } finally {
    rmSync(spillDir, { recursive: true, force: true })
  }
}

function setImmediatePromise(fn: () => void): Promise<void> {
  return new Promise((resolve) => setImmediate(() => { fn(); resolve() }))
}
