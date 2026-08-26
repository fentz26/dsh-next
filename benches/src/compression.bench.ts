/**
 * Compression benchmark: DSH's encodeData/decodeData path (node:zlib
 * zstdCompressSync on the main thread) vs raw zlib calls, across payload
 * sizes. Measures throughput and — critically — event-loop blocking per call.
 */
import { randomBytes } from 'node:crypto'
import { zstdCompressSync, zstdDecompressSync, constants } from 'node:zlib'
import { LagMonitor, cpuBaseline,
  createLatencyTracker, fmt, snapshotResources } from './harness/metrics.ts'
import { compressionModule } from './harness/dsh.ts'

const SIZES = [
  { name: '1KB', bytes: 1024 },
  { name: '8KB', bytes: 8 * 1024 },
  { name: '64KB', bytes: 64 * 1024 },
  { name: '1MB', bytes: 1024 * 1024 },
]

const OPS_PER_SCENARIO = Number(process.env.BENCH_COMPRESS_OPS ?? 2000)

/** Realistic compressible log-like payload (session JSON is highly repetitive). */
function payloadOf(bytes: number): Buffer {
  const unit = Buffer.from(
    JSON.stringify({
      level: 'info',
      msg: 'the quick brown fox jumps over the lazy dog; retrying step with backoff',
      turn: 3,
      step: 1,
      chunk: { type: 'text-delta', index: 0, text: 'partial assistant response tokens continue here' },
    }) + '\n',
  )
  const parts = Array.from({ length: Math.ceil(bytes / unit.length) }, () => unit)
  return Buffer.concat(parts).subarray(0, bytes)
}

/** Faithful replica of DSH compression.ts encodeData (module-private there). */
function dshEncodeDataLike(value: string | Uint8Array): string | Uint8Array {
  const bytes = Buffer.isBuffer(value) ? value : Buffer.from(value)
  if (bytes.length < 4096) return bytes.toString('utf8')
  const compressed = zstdCompressSync(bytes, {
    params: { [constants.ZSTD_c_compressionLevel]: 3 },
  })
  return compressed.length < bytes.length ? compressed : bytes
}

function dshDecodeDataLike(value: string | Uint8Array, maxOutputLength?: number): string {
  if (typeof value === 'string') return value
  const decoded =
    maxOutputLength === undefined ? zstdDecompressSync(value) : zstdDecompressSync(value, { maxOutputLength })
  return decoded.toString('utf8')
}

const encodeData = dshEncodeDataLike
const decodeData = dshDecodeDataLike

const SCALAR_JSON = JSON.stringify({ callId: 'c1', output: 'line\n'.repeat(80), isError: false })

export async function compressionBench(): Promise<void> {
  const mod = await compressionModule()

  for (const size of SIZES) {
    const payload = payloadOf(size.bytes)

    // -- DSH encodeData path (includes threshold check + smaller-of logic) --
    {
      const track = createLatencyTracker()
      const lag = new LagMonitor()
      const t0 = process.hrtime.bigint()
 const cpu0 = cpuBaseline()
      lag.start()
      let outLen = 0
      for (let i = 0; i < OPS_PER_SCENARIO; i++) {
        const ct = performanceNowWrap(() => {
          const out = encodeData(payload) as Buffer | string
          outLen += typeof out === 'string' ? out.length : out.length
        })
        track.add(ct)
      }
      const lags = lag.stop()
      const snap = snapshotResources(t0, cpu0)
      const s = track.summary()
      const ratio = typeof encodeData(payload) === 'string'
        ? 1
        : (encodeData(payload) as Buffer).length / size.bytes
      console.log(
        [
          `compression dsh-encodeData size=${size.name} ops=${OPS_PER_SCENARIO}`,
          `wall=${fmt(snap.wallMs)}ms ops/s=${fmt((OPS_PER_SCENARIO / snap.wallMs) * 1000)}`,
          `call_ms p50=${fmt(s.p50, 3)} p95=${fmt(s.p95, 3)} p99=${fmt(s.p99, 3)} max=${fmt(s.max, 3)}`,
          `lag p99=${fmt(lags.p99ms, 3)} canaryP99=${fmt(lags.canaryP99Ms, 3)} canaryMax=${fmt(lags.canaryMaxMs, 3)} drift=${fmt(lags.maxDriftMs, 3)}`,
          `outBytes=${fmt(outLen / OPS_PER_SCENARIO)} ratio=${fmt(ratio, 3)}`,
        ].join(' | '),
      )
    }

    // -- Raw zlib zstdCompressSync for comparison --
    {
      const track = createLatencyTracker()
      const t0 = process.hrtime.bigint()
  const cpu0 = cpuBaseline()
      for (let i = 0; i < OPS_PER_SCENARIO; i++) {
        const bt = process.hrtime.bigint()
        zstdCompressSync(payload)
        track.add(Number(process.hrtime.bigint() - bt) / 1e6)
      }
      const snap = snapshotResources(t0, cpu0)
      const s = track.summary()
      console.log(
        [
          `compression zlib-zstdSync size=${size.name} ops=${OPS_PER_SCENARIO}`,
          `wall=${fmt(snap.wallMs)}ms ops/s=${fmt((OPS_PER_SCENARIO / snap.wallMs) * 1000)}`,
          `call_ms p50=${fmt(s.p50, 3)} p95=${fmt(s.p95, 3)} p99=${fmt(s.p99, 3)} max=${fmt(s.max, 3)}`,
        ].join(' | '),
      )
    }

    // -- Decode path --
    if (size.bytes >= 4096) {
      const compressed = zstdCompressSync(payload)
      const track = createLatencyTracker()
      const lag = new LagMonitor()
      const t0 = process.hrtime.bigint()
  const cpu0 = cpuBaseline()
      lag.start()
      for (let i = 0; i < OPS_PER_SCENARIO; i++) {
        const bt = process.hrtime.bigint()
        decodeData(compressed, size.bytes * 2)
        track.add(Number(process.hrtime.bigint() - bt) / 1e6)
      }
      const lags = lag.stop()
      const snap = snapshotResources(t0, cpu0)
      const s = track.summary()
      console.log(
        [
          `compression dsh-decodeData size=${size.name} ops=${OPS_PER_SCENARIO}`,
          `wall=${fmt(snap.wallMs)}ms ops/s=${fmt((OPS_PER_SCENARIO / snap.wallMs) * 1000)}`,
          `call_ms p50=${fmt(s.p50, 3)} p99=${fmt(s.p99, 3)} max=${fmt(s.max, 3)}`,
          `lag p99=${fmt(lags.p99ms, 3)} canaryP99=${fmt(lags.canaryP99Ms, 3)} canaryMax=${fmt(lags.canaryMaxMs, 3)} drift=${fmt(lags.maxDriftMs, 3)}`,
        ].join(' | '),
      )
    }
  }

  // End-to-end record binding (JSON stringify + varint + zstd decision).
  {
    const mod2 = await compressionModule()
    const bindRecord = (mod2 as { bindRecord: (r: unknown) => unknown }).bindRecord
    const event = { type: 'tool/result', seq: 42, time: Date.now(), data: JSON.parse(SCALAR_JSON) }
    const track = createLatencyTracker()
    const t0 = process.hrtime.bigint()
    const cpu0 = cpuBaseline()
    for (let i = 0; i < OPS_PER_SCENARIO; i++) {
      const bt = process.hrtime.bigint()
      bindRecord(event)
      track.add(Number(process.hrtime.bigint() - bt) / 1e6)
    }
    const snap = snapshotResources(t0, cpu0)
    const st = track.summary()
    console.log(
      [
        `compression dsh-bindRecord ops=${OPS_PER_SCENARIO}`,
        `wall=${fmt(snap.wallMs)}ms ops/s=${fmt((OPS_PER_SCENARIO / snap.wallMs) * 1000)}`,
        `call_ms p50=${fmt(st.p50, 3)} p99=${fmt(st.p99, 3)} max=${fmt(st.max, 3)}`,
      ].join(' | '),
    )
  }

  // Event-loop stall while compressing one large batch of rows (worst case in
  // an appendBatch transaction): simulate a 1MB packed row + several scalars.
  {
    const bigRow = payloadOf(900 * 1024)
    const scalars = Array.from({ length: 60 }, () => payloadOf(2 * 1024))
    const lag = new LagMonitor()
    lag.start()
    const t0 = process.hrtime.bigint()
  const cpu0 = cpuBaseline()
    encodeData(bigRow)
    for (const s of scalars) encodeData(s)
    const snap = snapshotResources(t0, cpu0)
    const lags = lag.stop()
    console.log(
      [
        `compression batch-simulation (~1MB row + 60 scalar rows)`,
        `wall=${fmt(snap.wallMs)}ms`,
        `canaryP99=${fmt(lags.canaryP99Ms, 3)} canaryMax=${fmt(lags.canaryMaxMs, 3)} canaryP99=${fmt(lags.canaryP99Ms, 3)} canaryMax=${fmt(lags.canaryMaxMs, 3)} drift=${fmt(lags.maxDriftMs, 3)}ms lagP99=${fmt(lags.p99ms, 3)}ms`,
      ].join(' | '),
    )
  }
}

function performanceNowWrap(fn: () => void): number {
  const start = performance.now()
  fn()
  return performance.now() - start
}
