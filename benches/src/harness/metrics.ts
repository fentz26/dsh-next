/**
 * Metrics harness: event-loop lag, CPU time, RSS, per-operation latency
 * percentiles. Shared by every dsh-next benchmark.
 */
import { monitorEventLoopDelay, performance } from 'node:perf_hooks'

export function pct(sorted: number[], p: number): number {
  if (sorted.length === 0) return Number.NaN
  const idx = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1)
  return sorted[Math.max(0, idx)]
}

export interface LatencyTracker {
  add(ms: number): void
  /** p50/p95/p99/max in ms over the tracked operations. */
  summary(): { p50: number; p95: number; p99: number; max: number; count: number }
}

export function createLatencyTracker(): LatencyTracker {
  const samples: number[] = []
  return {
    add(ms) {
      samples.push(ms)
    },
    summary() {
      const sorted = [...samples].sort((a, b) => a - b)
      return {
        p50: pct(sorted, 50),
        p95: pct(sorted, 95),
        p99: pct(sorted, 99),
        max: sorted[sorted.length - 1] ?? Number.NaN,
        count: samples.length,
      }
    },
  }
}

/**
 * Event-loop lag: monitorEventLoopDelay histogram PLUS setImmediate canaries.
 * Canaries matter because a workload built purely on microtasks/resolved
 * promises never returns control to the event loop, yet its synchronous
 * stretches would still starve timers/streams in a real harness. Canary
 * callbacks measure inter-tick delay caused by such stretches.
 */
export class LagMonitor {
  private hist = monitorEventLoopDelay({ resolution: 1 })
  private driftTimer?: NodeJS.Timeout
  private lastTick = performance.now()
  private maxDriftMs = 0
  private canaries: Array<{ last: number; alive: boolean }> = []
  private canarySamples: number[] = []
  private canaryTimer?: NodeJS.Timeout

  start(): void {
    this.hist.enable()
    this.lastTick = performance.now()
    // 1 ms probing cadence; the timer callback measures scheduling delay.
    this.driftTimer = setInterval(() => {
      const now = performance.now()
      const drift = now - this.lastTick - 1
      if (drift > this.maxDriftMs) this.maxDriftMs = drift
      this.lastTick = now
    }, 1)
    this.driftTimer.unref?.()

    const CANARIES = 2
    for (let i = 0; i < CANARIES; i++) {
      const c = { last: performance.now(), alive: true }
      this.canaries.push(c)
      const tick = () => {
        if (!c.alive) return
        const now = performance.now()
        const delta = now - c.last
        // Ignore our own scheduling (~0ms); record only stalls > 1ms.
        if (delta > 1) this.canarySamples.push(delta)
        c.last = now
        setImmediate(tick)
      }
      setImmediate(tick)
    }
    // Periodically let macrotask queue breathe so canaries reschedule cheaply.
    this.canaryTimer = setInterval(() => {}, 50)
    this.canaryTimer.unref?.()
  }

  stop(): {
    meanMs: number
    p50ms: number
    p95ms: number
    p99ms: number
    maxDriftMs: number
    canaryP95Ms: number
    canaryP99Ms: number
    canaryMaxMs: number
  } {
    clearInterval(this.driftTimer)
    clearInterval(this.canaryTimer)
    for (const c of this.canaries) c.alive = false
    this.hist.disable()
    const nsToMs = (v: number) => v / 1e6
    const sorted = [...this.canarySamples].sort((a, b) => a - b)
    return {
      meanMs: nsToMs(this.hist.mean),
      p50ms: nsToMs(this.hist.percentile(50)),
      p95ms: nsToMs(this.hist.percentile(95)),
      p99ms: nsToMs(this.hist.percentile(99)),
      maxDriftMs: Math.max(this.maxDriftMs, sorted[sorted.length - 1] ?? 0),
      canaryP95Ms: pct(sorted, 95),
      canaryP99Ms: pct(sorted, 99),
      canaryMaxMs: sorted[sorted.length - 1] ?? 0,
    }
  }
}

export interface ResourceSnapshot {
  wallMs: number
  cpuUserMs: number
  cpuSystemMs: number
  rssMiB: number
  heapUsedMiB: number
}

export function cpuBaseline(): NodeJS.CpuUsage {
  return process.cpuUsage()
}

export function snapshotResources(startWall: bigint, startCpu?: NodeJS.CpuUsage): ResourceSnapshot {
  const mem = process.memoryUsage()
  const wallMs = Number(process.hrtime.bigint() - startWall) / 1e6
  if (startCpu === undefined) {
    return {
      wallMs,
      cpuUserMs: Number.NaN,
      cpuSystemMs: Number.NaN,
      rssMiB: mem.rss / (1024 * 1024),
      heapUsedMiB: mem.heapUsed / (1024 * 1024),
    }
  }
  const cpu = process.cpuUsage(startCpu)
  return {
    wallMs,
    cpuUserMs: cpu.user / 1000,
    cpuSystemMs: cpu.system / 1000,
    rssMiB: mem.rss / (1024 * 1024),
    heapUsedMiB: mem.heapUsed / (1024 * 1024),
  }
}

export function fmt(n: number, digits = 2): string {
  return n.toLocaleString('en-US', { maximumFractionDigits: digits })
}
