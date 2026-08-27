/**
 * Paged logical Session history source (dsh-next prototype, Track C tranche C).
 *
 * ROLE: canonical persistence ──▶ PagedLogicalSource ──▶ range consumers
 * (no eager whole-log materialization).
 *
 * Contract (docs/paged-hydration.md):
 * - LOGICAL seq coordinates (#78)
 * - pages bounded by event count AND payload bytes (#75/#76)
 * - contiguity guaranteed inside returned pages
 * - end-of-log returns short page + endOfLogAt (#12)
 * - cancellation between physical rows
 * - malformed physical data in the requested range refuses loudly (#13);
 *   unknown-TYPE policy stays with canonical load()/inspect() whole-log checks
 * - fresh unaliased frozen pages per call; source retains nothing (#72/#73/#81)
 */

import { createRequire } from 'node:module'
import { pathToFileURL } from 'node:url'

const require_ = createRequire(import.meta.url)

export interface ReadRangeOptions {
  /** Maximum logical events returned. Default 4096, hard cap ~1M. */
  limit?: number
  /** Payload byte budget (page may stop early); ≥1 event still returned when any exist. */
  maxBytes?: number
  signal?: AbortSignal
}

export interface LogicalPage {
  readonly events: readonly Readonly<Record<string, unknown>>[]
  /** First seq NOT included due to end-of-log (undefined = more remain). */
  readonly endOfLogAt?: number
  readonly inspectedCount: number
  readonly approxPayloadBytes: number
}

export interface SessionSourceMeta {
  readonly header: Readonly<Record<string, unknown>>
  readonly length: number
}

export interface PagedLogicalSource {
  readonly kind: string
  meta(id: string): Promise<SessionSourceMeta | undefined>
  readRange(id: string, startSeq: number, options?: ReadRangeOptions): Promise<LogicalPage>
  readSuffix(id: string, lastN: number, options?: Omit<ReadRangeOptions, 'limit'>): Promise<LogicalPage>
  close(): Promise<void>
}

/** Mirrors DSH MAX_PACKED_ROW_MEMBERS so overlapping predecessors are fetched. */
const MAX_PACKED_ROW_MEMBERS = 1024
const SQLITE_APPLICATION_ID_DSHP = 0x44534850
const SCHEMA_VERSION_17 = 17

export async function openSqliteSource(path: string): Promise<PagedLogicalSource> {
  const dshRoot = process.env.DSH_ROOT
  const compression = require_(
    pathToFileURL(`${dshRoot}/packages/session/session-persistence-sqlite/src/compression.ts`).href,
  ) as unknown as { decodeRow(r: Record<string, unknown>): Array<{ seq: number }> }

  const sqlite = require_('node:sqlite') as unknown as typeof import('node:sqlite')
  const db = new sqlite.DatabaseSync(path, { readOnly: true })

  const appId = db.prepare('PRAGMA application_id').get() as { application_id?: number } | undefined
  if ((appId?.application_id ?? 0) !== SQLITE_APPLICATION_ID_DSHP) {
    db.close()
    throw new Error('paged-history: not a DSHP session database')
  }
  const ver = db.prepare('PRAGMA user_version').get() as { user_version?: number } | undefined
  if ((ver?.user_version ?? 0) !== SCHEMA_VERSION_17) {
    db.close()
    throw new Error(`paged-history: unsupported schema version ${ver?.user_version}`)
  }

  let closed = false
  const requireOpen = (): void => {
    if (closed) throw new Error('paged-history: source closed')
  }
  const lengthOf = (id: string): number => {
    const r = db.prepare('SELECT MAX(seq)+1 AS n FROM events WHERE session_id=?').get(id) as
      | { n: number | null }
      | undefined
    return r?.n ?? 0
  }

  async function meta(id: string): Promise<SessionSourceMeta | undefined> {
    requireOpen()
    const sess = db.prepare('SELECT version, created_at, cwd FROM sessions WHERE id=?').get(id) as
      | { version: number; created_at: number; cwd: string | null }
      | undefined
    if (sess === undefined) return undefined
    return {
      header: Object.freeze({ version: sess.version, createdAt: sess.created_at, cwd: sess.cwd ?? undefined }),
      length: lengthOf(id),
    }
  }

  async function readRange(
    id: string,
    startSeq: number,
    options?: ReadRangeOptions,
  ): Promise<LogicalPage> {
    requireOpen()
    if (!Number.isSafeInteger(startSeq) || startSeq < 0) {
      throw new RangeError('startSeq must be a non-negative integer')
    }
    const limit = Math.max(1, Math.min(options?.limit ?? 4096, 1 << 20))
    const maxBytes = options?.maxBytes ?? Number.POSITIVE_INFINITY

    const total = lengthOf(id)
    if (startSeq >= total) {
      return { events: [], endOfLogAt: total, inspectedCount: 0, approxPayloadBytes: 0 }
    }

    // Overlapping packed predecessors can cover up to FLOOR logical events
    // below startSeq; decode-and-skip precisely by logical seq afterwards.
    const floor = Math.max(0, startSeq - (MAX_PACKED_ROW_MEMBERS - 1))
    const rawRows = db
      .prepare(
        'SELECT seq, time, type, data, ignorable, surface_op, source_event_seqs FROM events WHERE session_id=? AND seq>=? ORDER BY seq',
      )
      .all(id, floor) as unknown as Array<Record<string, unknown>>

    const mutableEvents: Record<string, unknown>[] = []
    let inspected = 0
    let bytes = 0
    let expected = startSeq
    let sawTailRow = false

    for (let ri = 0; ri < rawRows.length; ri++) {
      if (options?.signal?.aborted) throw options.signal.reason ?? new Error('aborted')
      const row = rawRows[ri]!
      const isTail = ri === rawRows.length - 1

      let expanded: Array<{ seq: number }> = []
      try {
        expanded = compression.decodeRow(row)
      } catch (err) {
        throw new Error(`paged-history: malformed physical row at seq=${Number(row.seq)}`, { cause: err })
      }

      for (const ev of expanded) {
        inspected++
        if (ev.seq < startSeq) continue // predecessor coverage below window

        if (
          mutableEvents.length > 0 &&
          (mutableEvents.length >= limit || bytes >= maxBytes)
        ) {
          // Page filled with more remaining → NOT end-of-log.
          const events = Object.freeze(mutableEvents)
          return { events, inspectedCount: inspected, approxPayloadBytes: bytes }
        }

        if (ev.seq !== expected) {
          // First emitted event beyond requested start without continuity
          // means a gap/corruption in the log — refuse loudly.
          throw new Error(
            mutableEvents.length === 0 && ev.seq > startSeq
              ? `paged-history: gap before seq=${ev.seq}`
              : `paged-history: non-contiguous sequence at seq=${ev.seq}, expected ${expected}`,
          )
        }
        expected = ev.seq + 1

        mutableEvents.push(Object.freeze(ev as Readonly<Record<string, unknown>>))
        bytes += approxPayload(ev)
      }
      sawTailRow ||= isTail
    }

    const cursorNow = expected
    const endOfLogAt =
      cursorNow >= total && sawTailRow ? total : undefined
    const events = Object.freeze(mutableEvents)
    return {
      events,
      inspectedCount: inspected,
      approxPayloadBytes: bytes,
      ...(endOfLogAt !== undefined ? { endOfLogAt } : {}),
    }
  }

  async function readSuffix(
    id: string,
    lastN: number,
    options?: Omit<ReadRangeOptions, 'limit'>,
  ): Promise<LogicalPage> {
    requireOpen()
    if (!Number.isSafeInteger(lastN) || lastN < 1) throw new RangeError('lastN must be positive')
    const from = Math.max(0, lengthOf(id) - lastN)
    return readRange(id, from, { ...options, limit: lastN })
  }

  async function close(): Promise<void> {
    if (!closed) {
      closed = true
      db.close()
    }
  }

  void requireOpen
  return { kind: 'sqlite-v17-paged', meta, readRange, readSuffix, close }
}

function approxPayload(ev: Record<string, unknown>): number {
  try {
    return JSON.stringify((ev as { data?: unknown }).data ?? '').length
  } catch {
    return 0
  }
}
