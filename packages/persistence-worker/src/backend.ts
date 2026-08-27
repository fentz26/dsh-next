/**
 * WorkerSqliteBackend — generation-scoped worker lifecycle (P0 hardening).
 *
 * STATE MACHINE
 *   new → opening → ready ⇄ (restarting → ready | failed) → closing → closed
 *
 * INVARIANTS (test-enforced):
 * - Exactly ONE published/usable worker generation (`active`) at any time.
 * - Every Worker gets a monotonic generation id; its message/error/exit
 *   handlers are closures over THAT generation, so stale-generation events
 *   are ignored and can never fail a live generation.
 * - A replacement generation is opened only AFTER the previous generation
 *   fully terminated (its `exit` observed or terminate() resolved). Double-
 *   writer safety comes from lifecycle ownership, NOT from SQLite locking.
 * - Every dispatched request acquires exactly one capacity slot (when
 *   capacity-counted) and releases it exactly once on ALL terminal paths:
 *   success, worker error response, worker crash/exit, backend shutdown,
 *   paged-stream failure, safety-limit failure (centralized in PendingReq).
 * - Capacity waiters are explicit objects; crash/shutdown/abort reject them
 *   deterministically. Queued aborted calls are removed, never dispatched.
 * - Dispatched writes are NEVER cancelled or auto-replayed: an interrupted
 *   request rejects (commit status may be ambiguous) and retrying is the
 *   caller's decision. `appendBatch` keeps resolving only after the worker's
 *   transaction committed (durability semantics unchanged: WAL/FULL).
 *
 * RESTART POLICY (opt-in `restartOnCrash`, default fail-fast):
 * - On active-generation death: settle its requests/waiters, transition to
 *   `restarting` (stats.failed stays true), terminate the old generation
 *   completely, then retry WORKER REOPEN up to `restartAttempts` total
 *   attempts with deterministic backoff (20ms·attempt, no jitter).
 * - Calls arriving while not `ready` fail fast with WorkerPersistenceError
 *   (state included) — no hidden queueing/replay/reordering.
 * - `failed` clears only after a replacement opened successfully.
 */
import pathToFileURL_ from 'node:url'
import { Worker } from 'node:worker_threads'
import { EventEmitter } from 'node:events'
import {
  PROTOCOL_VERSION,
  WorkerPersistenceError,
  type WorkerRequest,
  type WorkerResponse,
  type WorkerWireRequest,
} from './protocol.ts'

const pathToFileURL = pathToFileURL_.pathToFileURL

export type BackendState =
  | 'new'
  | 'opening'
  | 'ready'
  | 'restarting'
  | 'failed'
  | 'closing'
  | 'closed'

export interface WorkerBackendOptions {
  readonly path: string
  readonly journalMode: 'wal' | 'delete' | 'truncate' | 'persist'
  readonly busyTimeoutMs: number
  /** Module exporting DSH's SqliteStore (.ts under tsx; compiled .js when packaged). */
  readonly storeModulePath?: string
  readonly maxInFlight?: number
  /**
   * Opt-in automatic reopen after an unexpected worker death. Default false
   * (deterministic fail-fast). Reopen retries the WORKER only — interrupted
   * user operations always reject and are never auto-replayed.
   */
  readonly restartOnCrash?: boolean
  /** Total reopen attempts per crash episode. Default 3. */
  readonly restartAttempts?: number
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
  state: BackendState
  generation: number
  restartAttempts: number
}

export interface PagedLoaded {
  meta: unknown
  revision: unknown
  tornMarker?: unknown
  events: unknown[]
}

/** Default bound so a malformed/unflagged page stream can never hang or
 * silently return partial history: exceeding it is a typed hard failure. */
const MAX_PAGES_SAFETY = 1_000_000

/** Marker distinguishing the string-encoded event transport. */
const ENCODED_EVENTS = 1

interface Generation {
  readonly id: number
  readonly worker: Worker
  /** Set by this generation's exit handler (once). */
  exited: boolean
}

interface CapacityWaiter {
  resolve(): void
  reject(err: Error): void
  signal?: AbortSignal
  onAbort(): void
}

type PageOutcome =
  | { first: boolean; last?: boolean; meta?: unknown; revision?: unknown; tornMarker?: unknown; events?: unknown[] }

/** One dispatched in-flight request. Centralizes exactly-once settlement. */
class PendingReq {
  private settled = false
  private readonly route = (res: WorkerResponse): void => {
    if (this.settled) return
    this.handlers.onMessage(res)
  }

  constructor(
    private readonly backend: WorkerSqliteBackend,
    readonly gen: Generation,
    readonly seq: number,
    private readonly countsCapacity: boolean,
    private readonly handlers: {
      onMessage(res: WorkerResponse): void
      onTerminal(err: Error): void
    },
  ) {
    backend.pending.set(seq, this)
    if (countsCapacity) backend.inFlight++
    gen.worker.on(`res:${seq}`, this.route)
  }

  /** Successful completion (caller already resolved its outer promise). */
  complete(): void {
    if (this.settled) return
    this.settled = true
    this.detach()
  }

  /** Terminal failure: settle + reject through the terminal handler. */
  fail(err: Error): void {
    if (this.settled) return
    this.settled = true
    this.detach()
    this.handlers.onTerminal(err)
  }

  private detach(): void {
    this.gen.worker.off(`res:${this.seq}`, this.route)
    this.backend.pending.delete(this.seq)
    if (this.countsCapacity) this.backend.releaseSlotInternal()
  }
}

export class WorkerSqliteBackend extends EventEmitter {
  private state: BackendState = 'new'
  private generationCounter = 0
  private active: Generation | undefined
  private candidate: Generation | undefined
  private recovery: Promise<void> | undefined
  private closingPromise: Promise<void> | undefined

  private seqCounter = 0
  /** @internal collaborator access for PendingReq settlement */
  readonly pending = new Map<number, PendingReq>()
  /** @internal collaborator access for PendingReq settlement */
  inFlight = 0
  private readonly waiters: CapacityWaiter[] = []

  private readonly maxInFlight: number
  private readonly restartOnCrash: boolean
  private readonly restartAttempts: number
  private readonly appendTransport: NonNullable<WorkerBackendOptions['appendTransport']>
  readonly name = 'session-persistence-sqlite-worker'

  private stats_: BackendStats = {
    workerCpuUserMs: 0,
    workerCpuSystemMs: 0,
    requestsDispatched: 0,
    failed: false,
    state: 'new',
    generation: 0,
    restartAttempts: 0,
  }

  /** @internal fault-injection hooks (deterministic tests only) */
  testFailOpens = 0
  testCrashCandidates = 0
  /** @internal observability counters for tests */
  openAttemptsForTest = 0
  workersSpawnedForTest = 0
  liveWorkersForTest = 0
  maxPagesSafetyForTest = MAX_PAGES_SAFETY

  constructor(private readonly options: WorkerBackendOptions) {
    super()
    this.maxInFlight = options.maxInFlight ?? 64
    this.restartOnCrash = options.restartOnCrash ?? false
    this.restartAttempts = Math.max(1, options.restartAttempts ?? 3)
    this.appendTransport = options.appendTransport ?? 'stringified'
  }

  // ------------------------------------------------------------------
  // lifecycle
  // ------------------------------------------------------------------

  async init(): Promise<void> {
    if (this.state !== 'new') {
      throw new Error(`init() requires a fresh backend (state=${this.state})`)
    }
    this.setState('opening')
    try {
      const gen = await this.spawnAndOpenGeneration()
      if ((this.state as BackendState) !== 'opening') {
        // closed/disposed during open
        await this.terminateGeneration(gen, 250)
        return
      }
      this.active = gen
      this.setState('ready')
    } catch (err) {
      const e = err instanceof Error ? err : new Error(String(err))
      if ((this.state as BackendState) === 'opening') {
        this.stats_.failed = true
        this.setState('failed')
        const failure = this.wrapLifecycleError(e, 'initial open failed')
        this.emit('failed', failure)
        throw failure
      }
      // close() won mid-open; surface the rejection without state clobbering.
      throw this.wrapLifecycleError(e, 'open aborted by close')
    }
  }

  /**
   * Spawn a CANDIDATE generation and open its store. Publishes nothing:
   * the caller atomically assigns `active` only after a successful open.
   * Candidate failures reject deterministically and the candidate is fully
   * terminated before the rejection propagates (no orphan SQLite owners).
   */
  private async spawnAndOpenGeneration(): Promise<Generation> {
    const id = ++this.generationCounter
    this.stats_.generation = id
    this.openAttemptsForTest++

    const storeModulePath = this.options.storeModulePath ?? resolveDefaultStoreModulePath()
    const worker = new Worker(new URL('./worker.ts', import.meta.url), {
      workerData: { protocolVersion: PROTOCOL_VERSION },
      execArgv: ['--import', 'tsx'],
      env: {
        ...process.env,
        DSH_NEXT_STORE_MODULE: pathToFileURL(storeModulePath).href,
      },
    })
    this.workersSpawnedForTest++
    this.liveWorkersForTest++
    this.allWorkersForTest.push(worker)

    const gen: Generation = { id, worker, exited: false }
    this.candidate = gen

    // Generation-scoped routing: responses land on THIS worker's emitter, so
    // stale generations can never answer current-generation requests.
    worker.on('message', ({ res }: { res: WorkerResponse }) => {
      worker.emit(`res:${res.seq}`, res)
    })
    worker.on('error', (err: Error) => this.onGenerationEvent(gen, err))
    worker.on('exit', () => {
      gen.exited = true
      this.liveWorkersForTest--
      this.onGenerationEvent(gen, new Error('worker exited'))
    })

    // @internal deterministic candidate hard-crash during open.
    if (this.testCrashCandidates > 0) {
      this.testCrashCandidates--
      const w = worker
      setImmediate(() => void w.terminate())
    }

    // @internal deterministic open failure: an invalid journal mode makes the
    // worker-side store open throw (PRAGMA journal_mode rejects unknown modes).
    const openPayload: Record<string, unknown> = {
      path: this.options.path,
      journalMode: this.options.journalMode,
      busyTimeoutMs: this.options.busyTimeoutMs,
    }
    if (this.testFailOpens > 0) {
      this.testFailOpens--
      openPayload.journalMode = '__dsh_next_test_open_failure__'
    }

    try {
      await this.dispatchOn(
        gen,
        'open',
        openPayload,
        { countsCapacity: false, requireActive: false },
      )
    } catch (err) {
      await this.terminateGeneration(gen, 300)
      if (this.candidate === gen) this.candidate = undefined
      throw err
    }
    if (this.candidate === gen) this.candidate = undefined
    return gen
  }

  /** Generation-lifecycle dispatcher: only the ACTIVE generation or the
   * current CANDIDATE can cause state transitions; stale events are ignored. */
  private onGenerationEvent(gen: Generation, err: Error): void {
    if (this.active === gen) {
      this.handleActiveDeath(gen, err)
      return
    }
    if (this.candidate === gen) {
      // Opening candidate died: settle its (open) requests so the attempt
      // rejects deterministically instead of hanging (bug-3 fix).
      this.settleAllForGeneration(gen, this.wrapLifecycleError(err, 'candidate worker died while opening'))
      return
    }
    // Stale generation event: ignored by design (test G).
  }

  private handleActiveDeath(gen: Generation, err: Error): void {
    const failure = this.wrapLifecycleError(err, 'persistence worker died')
    this.settleAllForGeneration(gen, failure)
    this.rejectAllWaiters(failure)
    if (this.active === gen) this.active = undefined

    if (this.state === 'closing' || this.state === 'closed') return
    if (this.state !== 'ready') return // defensive: double events settle idempotently

    if (this.restartOnCrash) {
      // stats.failed stays TRUE through restarting; clears only after a
      // replacement generation actually opened (requirement).
      this.stats_.failed = true
      this.setState('restarting')
      this.recovery = this.recover(gen)
      return
    }
    this.stats_.failed = true
    this.setState('failed')
    this.emit('failed', failure)
  }

  /**
   * Bounded reopen policy. Order is strict:
   *   1. fully terminate the dead generation (await exit/terminate)
   *   2. attempt spawn+open up to restartAttempts total
   *   3. publish the first success as READY atomically
   * Close always wins: any state change to closing/closed aborts recovery
   * and terminates any pending candidate (no resurrection after dispose).
   */
  private async recover(dead: Generation): Promise<void> {
    try {
      await this.terminateGeneration(dead, 1000)
      if (this.recoveryAborted()) return

      for (let attempt = 1; attempt <= this.restartAttempts; attempt++) {
        this.stats_.restartAttempts = attempt
        if (this.recoveryAborted()) return
        try {
          const gen = await this.spawnAndOpenGeneration()
          if (this.recoveryAborted()) {
            await this.terminateGeneration(gen, 500)
            return
          }
          this.active = gen
          this.stats_.failed = false
          this.setState('ready')
          this.emit('restarted')
          return
        } catch {
          if (this.recoveryAborted()) return
          if (attempt < this.restartAttempts) {
            await new Promise((r) => setTimeout(r, 20 * attempt)) // deterministic backoff
          }
        }
      }
      if (!this.recoveryAborted()) {
        this.stats_.failed = true
        this.setState('failed')
        this.emit(
          'failed',
          new WorkerPersistenceError('persistence worker failed to reopen after crash', {
            phase: 'restart',
          }),
        )
      }
    } finally {
      this.recovery = undefined
    }
  }

  private recoveryAborted(): boolean {
    return this.state === 'closing' || this.state === 'closed'
  }

  /** Fully terminate a generation: await natural exit briefly, then force. */
  private async terminateGeneration(gen: Generation, timeoutMs: number): Promise<void> {
    const w = gen.worker
    if (gen.exited) return
    await new Promise<void>((resolve) => {
      const timer = setTimeout(() => {
        void w.terminate().finally(() => resolve())
      }, Math.max(0, timeoutMs))
      w.once('exit', () => {
        clearTimeout(timer)
        resolve()
      })
    })
  }

  async close(): Promise<void> {
    if (this.state === 'closed') return
    if (this.state === 'closing' && this.closingPromise) {
      await this.closingPromise
      return
    }
    this.setState('closing')
    this.closingPromise = (async () => {
      // Reject everything in-flight & queued deterministically.
      const shutdownErr = new WorkerPersistenceError('persistence worker disposed')
      this.settleAll(shutdownErr)
      this.rejectAllWaiters(shutdownErr)

      // Abort any recovery in progress and reap its candidate.
      if (this.recovery) await this.recovery.catch(() => undefined)
      if (this.candidate) {
        await this.terminateGeneration(this.candidate, 500)
        this.candidate = undefined
      }

      const gen = this.active
      this.active = undefined
      if (gen && !gen.exited) {
        try {
          await this.dispatchOn(gen, 'close', undefined, { countsCapacity: false, requireActive: false })
        } catch {
          // worker unresponsive — force-terminate below
        }
        await this.terminateGeneration(gen, 1500)
      }
      this.setState('closed')
    })()
    await this.closingPromise
  }

  dispose(): Promise<void> {
    return this.close()
  }

  // ------------------------------------------------------------------
  // request plumbing
  // ------------------------------------------------------------------

  private setState(s: BackendState): void {
    this.state = s
    this.stats_.state = s
  }

  private wrapLifecycleError(err: Error, phase: string): WorkerPersistenceError {
    return err instanceof WorkerPersistenceError
      ? err
      : new WorkerPersistenceError('persistence worker died', { phase, cause: err.message })
  }

  private nextSeq(): number {
    return ++this.seqCounter
  }

  private requireReady(op: string): Generation {
    if (this.state !== 'ready' || this.active === undefined) {
      throw new WorkerPersistenceError(
        `persistence worker not ready for ${op} (state=${this.state})`,
      )
    }
    return this.active
  }

  private async acquireSlot(signal?: AbortSignal): Promise<void> {
    for (;;) {
      if (signal?.aborted) throw abortError(signal)
      if (this.inFlight < this.maxInFlight) return
      await new Promise<void>((resolve, reject) => {
        const waiter: CapacityWaiter = {
          resolve,
          reject,
          signal,
          onAbort: () => {
            const i = this.waiters.indexOf(waiter)
            if (i !== -1) this.waiters.splice(i, 1)
            reject(abortError(signal!))
          },
        }
        this.waiters.push(waiter)
        signal?.addEventListener('abort', waiter.onAbort, { once: true })
      })
      // Woke: loop re-validates (abort already threw; crash rejected).
      if (signal?.aborted) throw abortError(signal)
    }
  }

  /** @internal collaborator access for PendingReq settlement */
  releaseSlotInternal(): void {
    this.inFlight = Math.max(0, this.inFlight - 1)
    while (this.waiters.length > 0 && this.inFlight < this.maxInFlight) {
      const w = this.waiters.shift()!
      w.signal?.removeEventListener('abort', w.onAbort)
      if (w.signal?.aborted) continue
      w.resolve()
    }
  }

  private rejectAllWaiters(err: Error): void {
    while (this.waiters.length > 0) {
      const w = this.waiters.shift()!
      w.signal?.removeEventListener('abort', w.onAbort)
      w.reject(err)
    }
  }

  /** Terminal failure for every request bound to a generation (crash path). */
  private settleAllForGeneration(gen: Generation, err: Error): void {
    for (const p of [...this.pending.values()]) {
      if (p.gen === gen) p.fail(err)
    }
  }

  private settleAll(err: Error): void {
    for (const p of [...this.pending.values()]) p.fail(err)
  }

  /**
   * Dispatch a request on a specific generation. `requireActive:false` is
   * reserved for lifecycle ops (open/close on candidates); user ops require
   * the generation to still be the published active one.
   */
  private dispatchOn<T>(
    gen: Generation,
    op: WorkerWireRequest['op'],
    payload: unknown,
    opts: { countsCapacity: boolean; requireActive: boolean },
  ): Promise<T> {
    if (gen.exited) {
      return Promise.reject(new WorkerPersistenceError('worker generation already exited'))
    }
    if (opts.requireActive && this.active !== gen) {
      return Promise.reject(new WorkerPersistenceError('worker generation is no longer active'))
    }
    const seq = this.nextSeq()
    this.stats_.requestsDispatched++

    return new Promise<T>((resolve, reject) => {
      const req = new PendingReq(this, gen, seq, opts.countsCapacity, {
        onMessage: (res) => {
          if (res.workerCpu) {
            this.stats_.workerCpuUserMs += res.workerCpu.userMs
            this.stats_.workerCpuSystemMs += res.workerCpu.systemMs
          }
          if (res.ok) {
            req.complete()
            resolve(res.result as T)
          } else {
            req.fail(new WorkerPersistenceError(res.error.message))
          }
        },
        onTerminal: (err) => reject(err),
      })
      const wire: WorkerWireRequest = { seq, op, payload }
      gen.worker.postMessage({ req: wire })
    })
  }

  // ------------------------------------------------------------------
  // public API (surface unchanged + stats extensions)
  // ------------------------------------------------------------------

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
    // Coordinator events arrive as lossless-JSON snapshots; a single JSON
    // string crosses cheaper than cloning (measured) and parses back to
    // equivalent plain objects inside the worker.
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

  /**
   * Giant-log friendly reconstruction streamed as fixed-size frames. Safety
   * bound: a stream exceeding maxPagesSafety REJECTS with a typed error —
   * it must never resolve with silently partial history.
   */
  loadStoredPaged(id: string, pageSize = 20_000, signal?: AbortSignal): Promise<PagedLoaded> {
    if (signal?.aborted) return Promise.reject(abortError(signal))
    let gen: Generation
    try {
      gen = this.requireReady('loadStoredPaged')
    } catch (err) {
      return Promise.reject(err)
    }
    const seq = this.nextSeq()
    this.stats_.requestsDispatched++

    return new Promise<PagedLoaded>((resolve, reject) => {
      const acc: PagedLoaded = { meta: undefined, revision: undefined, events: [] }
      let tornMarker: unknown
      let anyMarker = false
      let frames = 0
      const req = new PendingReq(this, gen, seq, true, {
        onMessage: (res) => {
          if (res.workerCpu) {
            this.stats_.workerCpuUserMs += res.workerCpu.userMs
            this.stats_.workerCpuSystemMs += res.workerCpu.systemMs
          }
          if (!res.ok) {
            req.fail(new WorkerPersistenceError(res.error.message))
            return
          }
          const page = res.result as PageOutcome
          frames++
          if (page.first && !anyMarker) {
            anyMarker = true
            acc.meta = page.meta
            acc.revision = page.revision
            if (page.tornMarker !== undefined) tornMarker = page.tornMarker
          }
          acc.events.push(...(page.events ?? []))
          if (page.last) {
            req.complete()
            resolve(anyMarker ? (tornMarker !== undefined ? { ...acc, tornMarker } : acc) : { ...acc })
            return
          }
          if (frames > this.maxPagesSafetyForTest) {
            req.fail(
              new WorkerPersistenceError('paged reconstruction exceeded safety bound', {
                phase: 'paged',
              }),
            )
          }
        },
        onTerminal: (err) => reject(err),
      })
      const wire: WorkerWireRequest = { seq, op: 'loadStoredPaged', payload: [id, pageSize] }
      gen.worker.postMessage({ req: wire })
    })
  }

  stats(): BackendStats {
    return { ...this.stats_ }
  }

  get failedState(): boolean {
    return this.state === 'failed' || this.state === 'restarting'
  }

  get lifecycleState(): BackendState {
    return this.state
  }

  // ------------------------------------------------------------------
  // test/fault-injection hooks
  // ------------------------------------------------------------------

  /** All spawned workers, oldest first (stale-event injection, leak checks). */
  readonly allWorkersForTest: Worker[] = []

  get activeWorkerForTest(): Worker | undefined {
    return this.active?.worker
  }

  get inFlightForTest(): number {
    return this.inFlight
  }

  get queuedForTest(): number {
    return this.waiters.length
  }

  /** Hard-kill the ACTIVE worker (crash simulation). Awaits its exit. */
  async killForTest(): Promise<void> {
    const gen = this.active
    if (gen === undefined) throw new Error('no active worker')
    await gen.worker.terminate()
  }

  /**
   * Register a capacity-counted pending request that never reaches the wire —
   * exactly the state a real caller holds when the worker dies mid-flight.
   */
  injectPendingForTest(): Promise<never> {
    if (this.state !== 'ready' || this.active === undefined) {
      throw new Error('backend not ready')
    }
    const gen = this.active
    const seq = this.nextSeq()
    return new Promise<never>((_resolve, reject) => {
      new PendingReq(this, gen, seq, true, {
        onMessage: () => {
          // This seq was never dispatched; any response is a protocol bug.
          reject(new WorkerPersistenceError('unexpected response on injected pending'))
        },
        onTerminal: (err) => reject(err),
      })
    })
  }

  /** @internal shared call path: gate → slot → dispatch. */
  private async call(
    op: WorkerWireRequest['op'],
    payload: unknown,
    signal?: AbortSignal,
  ): Promise<unknown> {
    if (signal?.aborted) throw abortError(signal)
    const gen = this.requireReady(op)
    await this.acquireSlot(signal) // aborts/crash rejections propagate
    // Re-validate after possibly waiting for capacity.
    if (this.state !== 'ready' || this.active !== gen) {
      throw new WorkerPersistenceError(
        `worker generation changed while waiting for capacity (${op})`,
      )
    }
    return this.dispatchOn(gen, op, payload, { countsCapacity: true, requireActive: true })
  }
}

function abortError(signal: AbortSignal): Error {
  return signal.reason instanceof Error
    ? signal.reason
    : new Error('operation aborted before dispatch')
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
