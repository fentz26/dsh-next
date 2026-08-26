/**
 * Journal microbenchmark: append/readFrom across implementations, including
 * FFI batching overhead (single-buffer vs batched append across N-API).
 */
import { randomBytes } from 'node:crypto'
import { createRequire } from 'node:module'
import { fmt, snapshotResources } from './harness/metrics.ts'
import { ReferenceByteJournal, SegmentedByteJournal } from '@dsh-next/journal'

async function loadNativeMod(): Promise<Record<string, unknown> | undefined> {
  try {
    const require = createRequire(import.meta.url)
    return require('../../crates/native-journal/pilot.node') as Record<string, unknown>
  } catch {
    return undefined
  }
}

export async function journalBench(): Promise<void> {
  const nativeMod = await loadNativeMod()
  if (!nativeMod) console.log('(native module not built — skipping native-rust rows)')

  const CASES = [
    { name: 'tiny-64B', chunk: 64 },
    { name: 'small-1KB', chunk: 1024 },
    { name: 'large-64KB', chunk: 64 * 1024 },
  ]
  const APPENDS = Number(process.env.BENCH_JOURNAL_APPENDS ?? 100_000)

  interface Target {
    kind: string
    append(b: Buffer): void
    appendBatch?(bs: Buffer[]): void
    readFrom(o: number): unknown
    next(): number
  }

  const targets: Array<() => Target> = [
    () => {
      const j = new ReferenceByteJournal(16 * 1024 * 1024)
      return { kind: 'ts-reference', append: (b) => void j.append(b), readFrom: (o) => j.readFrom(o), next: () => j.nextOffset }
    },
    () => {
      const j = new SegmentedByteJournal(16 * 1024 * 1024)
      return { kind: 'ts-segmented', append: (b) => void j.append(b), readFrom: (o) => j.readFrom(o), next: () => j.nextOffset }
    },
  ]

  if (nativeMod && typeof nativeMod.NativeByteJournal === 'function') {
    const NativeJ = nativeMod.NativeByteJournal as new (m: number) => {
      append(b: Buffer): number
      appendBatch(bs: Buffer[]): number
      readFrom(o: number): unknown
      nextOffset: number
    }
    targets.push(() => {
      const j = new NativeJ(16 * 1024 * 1024)
      return {
        kind: 'native-rust',
        append: (b) => void j.append(b),
        appendBatch: (bs) => void j.appendBatch(bs),
        readFrom: (o) => j.readFrom(o),
        next: () => j.nextOffset,
      }
    })
  }

  for (const c of CASES) {
    const payload = randomBytes(c.chunk)
    for (const make of targets) {
      // warmup
      let t = make()
      for (let i = 0; i < 2000; i++) t.append(payload)

      t = make()
      const bt = process.hrtime.bigint()
      for (let i = 0; i < APPENDS; i++) t.append(payload)
      const wallAppend = Number(process.hrtime.bigint() - bt) / 1e6

      // readFrom polling (recent-tail reader like a log observer)
      const READS = 20_000
      const br = process.hrtime.bigint()
      for (let i = 0; i < READS; i++) {
        t.readFrom(t.next() - c.chunk * 8)
      }
      const wallRead = Number(process.hrtime.bigint() - br) / 1e6

      console.log(
        [
          `journal ${c.name} impl=${t.kind}`,
          `appends=${APPENDS}`,
          `append_ops/s=${fmt((APPENDS / wallAppend) * 1000, 0)}`,
          `read_ops/s=${fmt((READS / wallRead) * 1000, 0)}`,
        ].join(' | '),
      )
    }

    // Batching comparison on the native module only.
    if (nativeMod && typeof nativeMod.NativeByteJournal === 'function') {
      const NativeJ = nativeMod.NativeByteJournal as new (m: number) => {
        append(b: Buffer): number
        appendBatch(bs: Buffer[]): number
        nextOffset: number
      }
      const payloads = Array.from({ length: 512 }, () => randomBytes(c.chunk))

      let j = new NativeJ(256 * 1024 * 1024)
      const bSingle = process.hrtime.bigint()
      for (let round = 0; round < APPENDS / 512; round++) {
        for (const p of payloads) j.append(p)
      }
      const wallSingle = Number(process.hrtime.bigint() - bSingle) / 1e6

      j = new NativeJ(256 * 1024 * 1024)
      const bBatch = process.hrtime.bigint()
      for (let round = 0; round < APPENDS / 512; round++) {
        j.appendBatch(payloads)
      }
      const wallBatch = Number(process.hrtime.bigint() - bBatch) / 1e6

      console.log(
        [
          `journal-ffi-batching ${c.name} n=${APPENDS}`,
          `single_crossings ops/s=${fmt((APPENDS / wallSingle) * 1000, 0)}`,
          `batched_512 ops/s=${fmt((APPENDS / wallBatch) * 1000, 0)}`,
          `speedup=${fmt(wallSingle / wallBatch)}x`,
        ].join(' | '),
      )
    }
  }
}
