/**
 * WorkerSqliteBackend — main-thread proxy implementing the coordinator's
 * PersistenceBackend hook surface on top of a worker-owned SQLite connection.
 *
 * Failure semantics (fail-fast, no silent restart):
 * - worker 'error' or premature 'exit' rejects every pending request with a
 *   typed WorkerPersistenceError and marks the backend failed; subsequent
 *   calls fail fast. Restart/reopen is deliberately NOT automatic.
 * - close(): close op -> exit await -> terminate fallback, so Cordis disposal
 *   reaches quiescence even if the worker wedges.
 *
 * Backpressure: in-flight requests are bounded (`maxInFlight`, default 64);
 * over the bound requests wait FIFO before dispatch (no unbounded queuing).
 *
 * Cancellation: AbortSignal support is advisory for reads — an aborted signal
 * rejects only ops not yet dispatched; dispatched reads finish inside their
 * snapshot transaction and their result is still returned. Writes are never
 * cancelled after dispatch (commit ambiguity beats redundant work).
 */
import pathToFileURL_ from 'node:url'
import { Worker } from 'node:worker_threads'
import {
  PROTOCOL_VERSION,
  WorkerPersistenceError,
  type WorkerRequest,
  type WorkerResponse,
  type WorkerWireRequest,
} from './protocol.ts'

const pathToFileURL = pathToFileURL_.pathToFileURL

export interface WorkerBackendOptions {
  readonly path: string
  readonly journalMode: 'wal' | 'delete' | 'truncate' | 'persist'
  readonly busyTimeoutMs: number
  /** Module exporting DSH's SqliteStore (.ts under tsx; compiled .js when packaged). */
  readonly storeModulePath?: string
  readonly maxInFlight?: number
  /**
   * Append-path wire encoding. Measured (benches/results/ipc-transport.txt):
   * a single JSON string crosses ~2x faster than structured-cloning nested
   * event graphs (worker re-parses natively). Default: stringified.
   */
  readonly appendTransport?: 'stringified' | 'structured'
}

export interface BackendStats {
  workerCpuUserMs: number
  workerCpuSystemMs: number
  requestsDispatched: number
  failed: boolean
}

export interface PagedLoaded {
  meta: unknown
  revision: unknown
  tornMarker?: unknown
  events: unknown[]
}

/** Payload marker distinguishing the string-encoded event transport. */
const ENCODED_EVENTS = 1

/** Safety valve so a malformed/unflagged stream can never hang the caller. */
const MAX_PAGES_SAFETY = 1_000_000

export class WorkerSqliteBackend {
  private worker!: Worker
  private seq = 0
  private pending = new Map<number, { reject: (e: Error) => void }>()
  private disposed = false
  private failed = false
  private inFlight = 0
  private readonly maxInFlight: number
  private waitQueue: Array<() => void> = []
  private stats_: BackendStats = { workerCpuUserMs: 0, workerCpuSystemMs: 0, requestsDispatched: 0, failed: false }
  private readonly appendTransport: NonNullable<WorkerBackendOptions['appendTransport']>
  readonly name = 'session-persistence-sqlite-worker'

  constructor(private readonly options: WorkerBackendOptions) {
    this.maxInFlight = options.maxInFlight ?? 64
    this.appendTransport = options.appendTransport ?? 'stringified'
  }

  async init(): Promise<void> {
    const storeModulePath =
      this.options.storeModulePath ?? resolveDefaultStoreModulePath()
    this.worker = new Worker(new URL('./worker.ts', import.meta.url), {
      workerData: { protocolVersion: PROTOCOL_VERSION },
      execArgv: ['--import', 'tsx'],
      env: {
        ...process.env,
        DSH_NEXT_STORE_MODULE: pathToFileURL(storeModulePath).href,
      },
    })
    // Route framed responses to their per-request listeners. Bound to the
    // local instance so late frames during/after termination can never
    // dereference a cleared handle.
    {
      const w = this.worker
      w.on('message', ({ res }: { res: WorkerResponse }) => {
        w.emit(`res:${res.seq}`, res)
      })
    }
    const ready = new Promise<void>((resolve) => {
      // Failures during init propagate through the pending slot below.
      resolve(undefined)
    })
    await ready
    try {
      await this.waitFor(
        'open',
        {
          path: this.options.path,
          journalMode: this.options.journalMode,
          busyTimeoutMs: this.options.busyTimeoutMs,
        },
      )
    } catch (err) {
      await this.terminateNow(500)
      throw err
    }
    {
      const w = this.worker
      w.on('error', (err) => {
        if (!this.disposed) this.markFailed(err)
      })
      w.on('exit', () => {
        if (!this.disposed) this.markFailed(new Error('worker exited unexpectedly'))
      })
    }
  }

  // ---- PersistenceBackend hook surface ----

  loadStored(id: string, signal?: AbortSignal): Promise<unknown> {
    return this.call('loadStored', id, signal)
  }

  loadStoredFrom(id: string, fromSeq: number, signal?: AbortSignal): Promise<unknown> {
    return this.call('loadStoredFrom', [id, fromSeq], signal)
  }

  readStoredRevision(id: string, signal?: AbortSignal): Promise<unknown> {
    return this.call('readStoredRevision', id, signal)
  }

  appendBatch(meta: unknown, events: readonly unknown[], isMaterialized: boolean): Promise<void> {
    // Coordinator events arrive as lossless-JSON snapshots (it guarantees that
    // itself), so a single stringify crosses cheaper than cloning and parses
    // back to equivalent plain objects inside the worker.
    const payload =
      this.appendTransport === 'stringified'
        ? [meta, JSON.stringify(events), isMaterialized, ENCODED_EVENTS]
        : [meta, events, isMaterialized, 0]
    return this.call('appendBatch', payload) as Promise<void>
  }

  commitRepair(meta: unknown, tornMarker: unknown, closers: readonly unknown[]): Promise<void> {
    return this.call('commitRepair', [meta, tornMarker, closers]) as Promise<void>
  }

  list(signal?: AbortSignal): Promise<unknown> {
    return this.call('list', undefined, signal)
  }

  listSnapshots(signal?: AbortSignal): Promise<unknown> {
    return this.call('listSnapshots', undefined, signal)
  }

  stats(): BackendStats {
    return { ...this.stats_, failed: this.failed }
  }

  get failedState(): boolean {
    return this.failed
  }

  async close(): Promise<void> {
    if (this.disposed || this.failed) {
      this.disposed = true
      if (!this.worker) return
      await this.terminateNow(1000)
      return
    }
    this.disposed = true
    let closedGracefully = false
    try {
      await this.waitFor('close', undefined)
      closedGracefully = true
    } catch {
      // fallthrough to terminate
    }
    await this.terminateNow(closedGracefully ? 500 : 2000)
  }

  dispose(): Promise<void> {
    return this.close()
  }

  /**
   * Fault injection: registers a request-shaped pending entry that never
   * reaches the wire — the exact state a caller would hold during a crash
   * mid-dispatch. Combined with killForTest this makes "pending operations
   * reject deterministically" assertable without timing races.
   */
  injectPendingForTest(): Promise<never> {
    const seq = ++this.seq
    return new Promise<never>((_resolve, reject) => {
      this.pending.set(seq, { reject })
    })
  }

  /** Test/fault-injection hook: hard-kills the worker thread immediately.
   * Failure observation is synchronous and idempotent — callers need not race
   * asynchronous worker-exit events to see pending rejections. */
  async killForTest(): Promise<void> {
    if (!this.worker) throw new Error('worker not initialized')
    await this.terminateNow(0)
    this.markFailed(new Error('persistence worker terminated by killForTest'))
  }

  /**
   * Giant-log friendly reconstruction: identical logical result to loadStored,
   * streamed as fixed-size pages so neither a single gigantic clone nor the
   * full graph must exist on both sides simultaneously. `pageSize` is events
   * per frame (default 20k).
   */
  loadStoredPaged(id: string, pageSize = 20_000, signal?: AbortSignal): Promise<PagedLoaded> {
    if (signal?.aborted) return Promise.reject(signal.reason ?? new Error('aborted before loadStoredPaged'))
    if (this.failed) return Promise.reject(new WorkerPersistenceError('persistence worker previously failed'))
    return this.waitForPages(id, pageSize, signal)
  }

  // ---- internals: paged accumulation ----

  private waitForPages(
    id: string,
    pageSize: number,
    signal?: AbortSignal,
  ): Promise<PagedLoaded> {
    const seq = ++this.seq
    return new Promise<PagedLoaded>((resolve, reject) => {
      const acc: PagedLoaded = { meta: undefined, revision: undefined, events: [] }
      let tornMarker: unknown
      let anyMarker = false
      this.pending.set(seq, { reject })
      this.inFlight++
      this.stats_.requestsDispatched++
      let received = 0
      const listener = (res: WorkerResponse): void => {
        received++
        if (res.workerCpu) {
          this.stats_.workerCpuUserMs += res.workerCpu.userMs
          this.stats_.workerCpuSystemMs += res.workerCpu.systemMs
        }
        if (!res.ok) {
          cleanup()
          reject(new WorkerPersistenceError(res.error.message))
          return
        }
        const page = res.result as {
          first?: boolean
          last?: boolean
          meta?: unknown
          revision?: unknown
          tornMarker?: unknown
          events?: unknown[]
        }
        if (page.first && !anyMarker) {
          anyMarker = true
          acc.meta = page.meta
          acc.revision = page.revision
          if (page.tornMarker !== undefined) tornMarker = page.tornMarker
        }
        acc.events.push(...(page.events ?? []))
        if (page.last || received > MAX_PAGES_SAFETY) {
          cleanup()
          this.inFlight--
          const wake = this.waitQueue.shift()
          if (wake !== undefined) wake()
          resolve(anyMarker ? (tornMarker !== undefined ? { ...acc, tornMarker } : acc) : { ...acc })
          return
        }
      }
      const cleanup = (): void => {
        ;(this.worker as Worker).off(`res:${seq}`, listener)
        this.pending.delete(seq)
      }
      void signal
      ;(this.worker as Worker).on(`res:${seq}`, listener)
      const wire: WorkerWireRequest = { seq, op: 'loadStoredPaged', payload: [id, pageSize] }
      this.worker.postMessage({ req: wire })
    })
  }

  // ---- internals ----

  private call(
    op: WorkerRequest['op'],
    payload: unknown,
    signal?: AbortSignal,
  ): Promise<unknown> {
    if (signal?.aborted) return Promise.reject(signal.reason ?? new Error(`aborted before ${op}`))
    if (this.failed) return Promise.reject(new WorkerPersistenceError('persistence worker previously failed'))
    if (this.disposed && !this.workerAlive()) return Promise.reject(new WorkerPersistenceError('persistence worker disposed'))

    const dispatchOrWait = async (): Promise<unknown> => {
      while (this.inFlight >= this.maxInFlight) {
        await new Promise<void>((r) => this.waitQueue.push(r))
        if (this.failed) throw new WorkerPersistenceError('persistence worker previously failed')
      }
      return this.waitFor(op, payload)
    }

    return dispatchOrWait()
  }

  private waitFor<T>(op: WorkerWireRequest['op'], payload: unknown): Promise<T> {
    const seq = ++this.seq
    return new Promise<T>((resolve, reject) => {
      this.pending.set(seq, { reject })
      this.inFlight++
      this.stats_.requestsDispatched++
      this.worker.once(`res:${seq}`, (res: WorkerResponse) => {
        this.inFlight--
        const wake = this.waitQueue.shift()
        if (wake !== undefined) wake()
        this.pending.delete(res.seq)
        if (res.workerCpu) {
          this.stats_.workerCpuUserMs += res.workerCpu.userMs
          this.stats_.workerCpuSystemMs += res.workerCpu.systemMs
        }
        if (res.ok) resolve(res.result as T)
        else {
          const err = new WorkerPersistenceError(res.error.message)
          ;(err as Error & { workerErrorName?: string }).workerErrorName = res.error.name
          reject(err)
        }
      })
      const wire: WorkerWireRequest = { seq, op, payload }
      this.worker.postMessage({ req: wire })
    })
  }

  private markFailed(err: Error): void {
    if (this.failed) return
    this.failed = true
    this.stats_.failed = true
    const failure = err instanceof WorkerPersistenceError
      ? err
      : new WorkerPersistenceError('persistence worker died', {
          phase: 'lifecycle',
          cause: err.message,
        })
    for (const [, p] of this.pending) p.reject(failure)
    this.pending.clear()
    for (const wake of this.waitQueue.splice(0)) wake()
  }

  private workerAlive(): boolean {
    return Boolean(this.worker) && !this.failed
  }

  private terminateNow(timeoutMs: number): Promise<void> {
    const w = this.worker as Worker | undefined
    if (w === undefined) return Promise.resolve()
    this.worker = undefined as unknown as Worker
    return new Promise<void>((resolve) => {
      const timer = setTimeout(() => {
        w.terminate().finally(() => resolve())
      }, timeoutMs)
      w.once('exit', () => {
        clearTimeout(timer)
        resolve()
      })
    })
  }
}

/** Dev/bench default: DSH checkout TS source; packaged builds pin compiled JS instead. */
export function resolveDefaultStoreModulePath(dshRootEnv?: string): string {
  const root = dshRootEnv ?? process.env.DSH_ROOT
  if (root === undefined || root.length === 0) {
    throw new Error(
      'storeModulePath missing: set DSH_ROOT (dev/bench mode) or pass storeModulePath pointing at compiled @deepseek-ai/dsh-session-persistence-sqlite sources',
    )
  }
  return `${root}/packages/session/session-persistence-sqlite/src/store.ts`
}

// The worker emits responses tagged per-request; wire those here so waitFor's
// once-listener pattern works without a central message pump.
export function attachResponseRouting(worker: Worker): void {
  worker.on('message', ({ res }: { res: WorkerResponse }) => {
    worker.emit(`res:${res.seq}`, res)
  })
}
