/**
 * Wire protocol between the harness main thread and the persistence worker.
 * Versioned so mismatches disable the provider instead of corrupting state.
 */
export const PROTOCOL_VERSION = 1

export interface WorkerOpenOptions {
  readonly path: string
  readonly journalMode: 'wal' | 'delete' | 'truncate' | 'persist'
  readonly busyTimeoutMs: number
}

export type WorkerRequest =
  | { seq: number; op: 'open'; payload: WorkerOpenOptions }
  | { seq: number; op: 'appendBatch'; payload: [meta: unknown, events: unknown[], isMaterialized: boolean] }
  | { seq: number; op: 'loadStored'; payload: string }
  | { seq: number; op: 'loadStoredPaged'; payload: [id: string, pageSize: number] }
  | { seq: number; op: 'loadStoredFrom'; payload: [id: string, fromSeq: number] }
  | { seq: number; op: 'readStoredRevision'; payload: string }
  | { seq: number; op: 'commitRepair'; payload: [meta: unknown, tornMarker: unknown, closers: unknown[]] }
  | { seq: number; op: 'listSnapshots'; payload: undefined }
  | { seq: number; op: 'list'; payload: undefined }
  | { seq: number; op: 'close'; payload: undefined }

/** Transport form: the typed union above documents intent; the wire stays loose
 * so a single generic send path works without contorting TypeScript inference. */
export interface WorkerWireRequest {
  seq: number
  op: WorkerRequest['op']
  payload: unknown
}

export type WorkerResponse =
  | { seq: number; ok: true; result: unknown; workerCpu?: WorkerCpuSample }
  | { seq: number; ok: false; error: WorkerErrorShape; workerCpu?: WorkerCpuSample }

export interface WorkerErrorShape {
  name: string
  message: string
}

export interface WorkerCpuSample {
  userMs: number
  systemMs: number
}

/** Typed error surfaced on the main thread for every failure mode. */
export class WorkerPersistenceError extends Error {
  override readonly name = 'WorkerPersistenceError'
  constructor(
    message: string,
    readonly detail?: { phase: string; cause?: string },
  ) {
    super(detail ? `${message} (${detail.phase})` : message)
  }
}
