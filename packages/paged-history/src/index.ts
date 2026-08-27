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
  /** Logical events emitted past the predecessor-skip window. */
  readonly inspectedCount: number
  /** Logical events inside touched physical rows, INCLUDING skipped predecessors. */
  readonly decodedLogicalCount: number
  readonly approxPayloadBytes: number
  /** Payload bytes of decoded logical events including skipped predecessors. */
  readonly decodedPayloadBytes: number
  /** Compressed data-column bytes read from SQLite for this page. */
  readonly compressedBytesRead: number
  /** Physical rows fetched and decoded (or skipped) for this page. */
  readonly physicalRowsTouched: number
}

function emptyPage(endOfLogAt?: number): LogicalPage {
  return {
    events: [],
    inspectedCount: 0,
    decodedLogicalCount: 0,
    approxPayloadBytes: 0,
    decodedPayloadBytes: 0,
    compressedBytesRead: 0,
    physicalRowsTouched: 0,
    ...(endOfLogAt !== undefined ? { endOfLogAt } : {}),
  }
}

export interface SessionSourceMeta {
  readonly header: Readonly<Record<string, unknown>>
  readonly length: number
}

export interface CheckpointReadOptions {
  /** Logical event types to EXCLUDE at the storage layer (never fetched/decoded). */
  excludeTypes?: readonly string[]
  /** Exclusive upper bound. */
  upToSeq: number
  maxBytes?: number
  signal?: AbortSignal
}

export interface PagedLogicalSource {
  readonly kind: string
  meta(id: string): Promise<SessionSourceMeta | undefined>
  readRange(id: string, startSeq: number, options?: ReadRangeOptions): Promise<LogicalPage>
  readSuffix(id: string, lastN: number, options?: Omit<ReadRangeOptions, 'limit'>): Promise<LogicalPage>
  /**
   * Structural-win path (#126): excluded event types never enter the SQL
   * result — their packed physical frames are not fetched or decompressed.
   * Used by derived-checkpoint generation/deserialization.
   */
  readPrefixFiltered(id: string, options: CheckpointReadOptions): Promise<LogicalPage>
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
      return emptyPage(total)
    }

    // Overlapping packed predecessors can cover up to FLOOR logical events
    // below startSeq; decode-and-skip precisely by logical seq afterwards.
    const floor = Math.max(0, startSeq - (MAX_PACKED_ROW_MEMBERS - 1))
    const rowStatement = db.prepare(
      'SELECT seq, time, type, data, ignorable, surface_op, source_event_seqs FROM events WHERE session_id=? AND seq>=? ORDER BY seq',
    )
    const rawRows = rowStatement.iterate(id, floor) as IterableIterator<Record<string, unknown>>

    const mutableEvents: Record<string, unknown>[] = []
    let inspected = 0
    let decodedLogical = 0
    let bytes = 0
    let decodedBytes = 0
    let compressedBytes = 0
    let rowsTouched = 0
    let expected = startSeq
    let sawTailRow = false

    for (const row of rawRows) {
      if (options?.signal?.aborted) throw options.signal.reason ?? new Error('aborted')
      rowsTouched++
      const dataLen =
        typeof row.data === 'string' ? Buffer.byteLength(row.data) : (row.data as Buffer | null)?.length ?? 0
      compressedBytes += dataLen

      let expanded: Array<{ seq: number }> = []
      try {
        expanded = compression.decodeRow(row)
      } catch (err) {
        throw new Error(`paged-history: malformed physical row at seq=${Number(row.seq)}`, { cause: err })
      }

      for (const ev of expanded) {
        decodedLogical++
        const payload = approxPayload(ev as Record<string, unknown>)
        decodedBytes += payload
        inspected++
        if ((ev.seq as number) < startSeq) {
          inspected--
          continue // predecessor coverage below window
        }
        const event = ev as Readonly<Record<string, unknown>>

        if (
          mutableEvents.length > 0 &&
          (mutableEvents.length >= limit || bytes >= maxBytes)
        ) {
          // Page filled with more remaining → NOT end-of-log.
          const events = Object.freeze(mutableEvents)
          return {
            events,
            inspectedCount: inspected,
            decodedLogicalCount: decodedLogical,
            approxPayloadBytes: bytes,
            decodedPayloadBytes: decodedBytes,
            compressedBytesRead: compressedBytes,
            physicalRowsTouched: rowsTouched,
          }
        }

        if ((ev.seq as number) !== expected) {
          // First emitted event beyond requested start without continuity
          // means a gap/corruption in the log — refuse loudly.
          throw new Error(
            mutableEvents.length === 0 && (ev.seq as number) > startSeq
              ? `paged-history: gap before seq=${ev.seq}`
              : `paged-history: non-contiguous sequence at seq=${ev.seq}, expected ${expected}`,
          )
        }
        expected = (ev.seq as number) + 1

        mutableEvents.push(Object.freeze(event))
        bytes += payload
      }
    }

    void sawTailRow
    const cursorNow = expected
    const endOfLogAt =
      cursorNow >= total ? total : undefined
    const events = Object.freeze(mutableEvents)
    return {
      events,
      inspectedCount: inspected,
      decodedLogicalCount: decodedLogical,
      approxPayloadBytes: bytes,
      decodedPayloadBytes: decodedBytes,
      compressedBytesRead: compressedBytes,
      physicalRowsTouched: rowsTouched,
      ...(endOfLogAt !== undefined ? { endOfLogAt } : {}),
    }
  }

  async function readPrefixFiltered(
    id: string,
    options: CheckpointReadOptions,
  ): Promise<LogicalPage> {
    requireOpen()
    const excludes = new Set(options.excludeTypes ?? [])
    if (!Number.isSafeInteger(options.upToSeq) || options.upToSeq < 0) {
      throw new RangeError('upToSeq must be a non-negative integer')
    }
    const maxBytes = options.maxBytes ?? Number.POSITIVE_INFINITY

    // Fully exclude dropped types AT SQL level: their physical frames are
    // never fetched nor decompressed ("don't even read it", governing rule).
    // NOTE: codec.ts packs eligible deltas under physical TAGS (text-chunks /
    // reasoning-chunks / tool-call-chunks); by construction a packed frame's
    // expansion contains ONLY its originating logical event kind, so excluding
    // a logical type must also exclude its packed tags.
    const PACKED_TAGS_BY_LOGICAL: Record<string, readonly string[]> = {
      'assistant/chunk': ['text-chunks', 'reasoning-chunks', 'tool-call-chunks'],
    }
    const sqlExcluded = new Set<string>(excludes)
    for (const t of excludes) {
      for (const tag of PACKED_TAGS_BY_LOGICAL[t] ?? []) sqlExcluded.add(tag)
    }
    void PACKED_TAGS_BY_LOGICAL

    const conds: string[] = ['session_id=?', 'seq<?']
    const params: Array<string | number> = [id, options.upToSeq]
    for (const t of sqlExcluded) {
      conds.push('type<>?')
      params.push(t)
    }
    const sql = `SELECT seq, time, type, data, ignorable, surface_op, source_event_seqs FROM events WHERE ${conds.join(' AND ')} ORDER BY seq`
    const rows = db.prepare(sql).all(...(params as never[])) as unknown as Array<
      Record<string, unknown>
    >

    const mutableEvents: Record<string, unknown>[] = []
    let inspected = 0
    let bytes = 0
    let expected = -1
    let gapDetected = false

    for (const row of rows) {
      if (options.signal?.aborted) throw options.signal.reason ?? new Error('aborted')
      let evs: Array<{ seq: number }> = []
      try {
        evs = compression.decodeRow(row)
      } catch (err) {
        throw new Error(`paged-history: malformed physical row at seq=${Number(row.seq)}`, { cause: err })
      }
      for (const ev of evs) {
        inspected++
        if (expected === -1) expected = ev.seq
        if (ev.seq !== expected && !gapDetected) {
          // Gaps belong to EXCLUDED types' runs — expected and harmless here;
          // dense rebasing happens in the checkpoint layer above us.
          gapDetected = true
        }
        expected = ev.seq + 1
        if (bytes >= maxBytes && mutableEvents.length > 0) break
        mutableEvents.push(Object.freeze(ev as Readonly<Record<string, unknown>>))
        bytes += approxPayload(ev)
      }
      if (options.maxBytes !== undefined && bytes >= options.maxBytes && mutableEvents.length > 0) break
    }

    return {
      events: Object.freeze(mutableEvents),
      inspectedCount: inspected,
      approxPayloadBytes: bytes,
      endOfLogAt: undefined,
      decodedLogicalCount: inspected,
      decodedPayloadBytes: bytes,
      compressedBytesRead: bytes,
      physicalRowsTouched: rows.length,
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
  return { kind: 'sqlite-v17-paged', meta, readRange, readSuffix, readPrefixFiltered, close }
}

function approxPayload(ev: Record<string, unknown>): number {
  try {
    return JSON.stringify((ev as { data?: unknown }).data ?? '').length
  } catch {
    return 0
  }
}
