/**
 * Persistence worker entry (Track B).
 *
 * Owns the ONLY handle to the SQLite database. The main thread never touches
 * DatabaseSync for sessions managed through this backend. Requests are framed
 * ops from protocol.ts; results are structured-cloned back, which is safe
 * because persistence payloads are JSON-shaped plain objects.
 *
 * Durability semantics: an appendBatch response is sent only AFTER the store's
 * transaction committed — `append` resolving on the main thread keeps its
 * "resolves only after durability" meaning. Reads run inside deferred snapshot
 * transactions in the worker exactly as stock does.
 *
 * Requests are processed strictly FIFO: one SQLite connection serializes
 * execution anyway; FIFO preserves coordinator ordering across sessions.
 */
import { parentPort, workerData } from 'node:worker_threads'

import { PROTOCOL_VERSION, type WorkerRequest, type WorkerResponse } from './protocol.ts'

interface DshStore {
  open(): Promise<void>
  appendBatch(meta: unknown, events: readonly unknown[], isMaterialized: boolean): Promise<void>
  loadStored(id: string): Promise<unknown>
  loadStoredFrom(id: string, fromSeq: number): Promise<unknown>
  readStoredRevision(id: string): Promise<unknown>
  commitRepair(meta: unknown, tornMarker: unknown, closers: readonly unknown[]): Promise<void>
  list(): Promise<unknown>
  listSnapshots(): Promise<unknown>
  close(): Promise<void>
}

const cfg = workerData as { protocolVersion?: number }
if (cfg?.protocolVersion !== PROTOCOL_VERSION) {
  throw new Error(`protocol version mismatch: expected ${PROTOCOL_VERSION}`)
}

function cpuNow(): { userMs: number; systemMs: number } {
  const cpu = process.cpuUsage()
  return { userMs: cpu.user / 1000, systemMs: cpu.system / 1000 }
}

type PendingItem = {
  req: WorkerRequest
  resolve: (r: WorkerResponse) => void
}

interface DispatchOutcome {
  result?: unknown
  paged?: Generator<unknown>
  store?: DshStore
}

function* streamPages(
  full: { meta?: unknown; events?: unknown[]; revision?: unknown; tornMarker?: unknown },
  size: number,
): Generator<unknown> {
  const events = full.events ?? []
  if (events.length === 0) {
    yield {
      first: true,
      last: true,
      meta: full.meta,
      revision: full.revision,
      tornMarker: full.tornMarker,
      events: [],
    }
    return
  }
  let idx = 0
  while (idx < events.length) {
    const slice = events.slice(idx, idx + size)
    idx += slice.length
    yield {
      first: idx === slice.length,
      last: idx >= events.length,
      meta: idx === slice.length ? full.meta : undefined,
      revision: idx === slice.length ? full.revision : undefined,
      tornMarker: idx === slice.length ? full.tornMarker : undefined,
      events: slice,
    }
  }
}

async function dispatch(store: DshStore | undefined, req: WorkerRequest): Promise<DispatchOutcome> {
  switch (req.op) {
    case 'open': {
      if (store !== undefined) throw new Error('store already opened')
      const mod = (await import(process.env.DSH_NEXT_STORE_MODULE as string)) as {
        SqliteStore: new (o: { path: string; journalMode: string; busyTimeoutMs: number }) => DshStore
      }
      const next = new mod.SqliteStore(req.payload)
      await next.open()
      return { result: undefined, store: next }
    }
    case 'appendBatch': {
      const s = requireStore(store)
      const [meta, rawEvents, isMaterialized, encoded] = req.payload as unknown as [
        unknown,
        unknown,
        boolean,
        number,
      ]
      const events =
        encoded === 1 ? (JSON.parse(rawEvents as string) as unknown[]) : (rawEvents as unknown[])
      await s.appendBatch(meta as never, events as never, isMaterialized)
      return { result: undefined, store }
    }
    case 'loadStored':
      return { result: await requireStore(store).loadStored(req.payload), store }
    case 'loadStoredPaged': {
      // Decode once inside the worker, then stream fixed-size pages as separate
      // frames sharing the request seq. Each frame carries a `last` flag; the
      // first carries meta/revision/tornMarker. Page-by-page transfer avoids
      // one gigantic structured-clone message (measured ~10x worse for 1M+
      // events) and bounds peak main-thread heap per page.
      const s = requireStore(store)
      const [id, pageSize] = req.payload
      const full = (await s.loadStored(id)) as {
        meta?: unknown
        events?: unknown[]
        revision?: unknown
        tornMarker?: unknown
      }
      return {
        paged: streamPages(full, Math.max(1, Math.floor(pageSize))),
        store,
      } as never
    }
    case 'loadStoredFrom': {
      const [id, fromSeq] = req.payload
      return { result: await requireStore(store).loadStoredFrom(id, fromSeq), store }
    }
    case 'readStoredRevision':
      return { result: await requireStore(store).readStoredRevision(req.payload), store }
    case 'commitRepair': {
      const [meta, tornMarker, closers] = req.payload
      await requireStore(store).commitRepair(meta as never, tornMarker, closers as never)
      return { result: undefined, store }
    }
    case 'list':
      return { result: await requireStore(store).list(), store }
    case 'listSnapshots':
      return { result: await requireStore(store).listSnapshots(), store }
    case 'close': {
      if (store !== undefined) await store.close()
      return { result: undefined, store: undefined }
    }
  }
}

function requireStore(store: DshStore | undefined): DshStore {
  if (store === undefined) throw new Error('store not opened')
  return store
}

async function main(): Promise<void> {
  const port = parentPort!
  let store: DshStore | undefined

  const queue: PendingItem[] = []
  let draining = false

  port.on('message', ({ req }: { req: WorkerRequest }) => {
    queue.push({ req, resolve: (r) => port.postMessage({ res: r }) })
    void drain()
  })

  async function drain(): Promise<void> {
    if (draining) return
    draining = true
    try {
      while (queue.length > 0) {
        const item = queue.shift()!
        const startCpu = cpuNow()
        let response: WorkerResponse
        try {
          const out = await dispatch(store, item.req)
          store = out.store
          if (out.paged !== undefined) {
            // Paged op: stream one frame per page; the page carrying
            // `last:true` is the final frame for this seq. No single-frame
            // response is emitted.
            for (const page of out.paged) {
              port.postMessage({
                res: { seq: item.req.seq, ok: true, result: page } satisfies WorkerResponse,
              })
            }
            continue
          }
          const now = cpuNow()
          response = {
            seq: item.req.seq,
            ok: true,
            result: out.result,
            workerCpu: {
              userMs: now.userMs - startCpu.userMs,
              systemMs: now.systemMs - startCpu.systemMs,
            },
          }
        } catch (err) {
          const e = err as Error
          const now = cpuNow()
          response = {
            seq: item.req.seq,
            ok: false,
            error: { name: e.name ?? 'Error', message: e.message ?? String(err) },
            workerCpu: {
              userMs: now.userMs - startCpu.userMs,
              systemMs: now.systemMs - startCpu.systemMs,
            },
          }
        }
        item.resolve(response)
      }
    } finally {
      draining = false
    }
  }
}

main().catch((err) => {
  // Early failures (bad module path, protocol mismatch) must surface as an
  // uncaught exception -> 'error' event on the main thread -> fail fast.
  throw err
})
