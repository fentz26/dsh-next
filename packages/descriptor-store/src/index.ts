/**
 * Durable logical-agent descriptor store (Track F prototype).
 *
 * Stores ONLY the minimal resumable record defined in
 * docs/durable-execution.md — never live JS objects or closures. The Session
 * event log stays the sole authority: descriptors carry source identity
 * metadata so stale records can be detected and rebuilt.
 *
 * The `dsh_next_*` tables are clearly namespaced as REBUILDABLE state separate
 * from canonical session storage; losing them loses nothing that cannot be
 * recomputed from durable sources once higher layers exist.
 *
 * Wake semantics are event-driven by design: timers/providers register rows;
 * there is no polling scheduler here (queries are pull-based utilities).
 */
import { mkdirSync } from 'node:fs'
import { dirname } from 'node:path'
import { DatabaseSync } from 'node:sqlite'

export const DESCRIPTOR_FORMAT_VERSION = 1

export type WakeKind = 'timer' | 'wait-provider' | 'human' | 'explicit'

export interface AgentDescriptor {
  /** Canonical anchor: the durable session's id. */
  readonly sessionId: string
  readonly formatVersion: number
  /** Composition identity (agent preset id chosen by higher layers). */
  readonly agentPresetId?: string
  /** Execution world reference — cwd/env class, never secret values. */
  readonly executionWorld?: { readonly cwd?: string }
  /** Current pending wake condition (single primary condition v1). */
  readonly wake: {
    readonly kind: WakeKind
    /** Provider-specific addressing, e.g. epoch-ms timer or wait id. */
    readonly key: string
  }
  /** Source identity to validate against on restore. */
  readonly lastCommitted: {
    readonly revision: string
    readonly seqWatermark: number
  }
}

const SCHEMA = `
CREATE TABLE IF NOT EXISTS dsh_next_agent_descriptors (
  session_id       TEXT PRIMARY KEY,
  format_version   INTEGER NOT NULL,
  status           TEXT NOT NULL CHECK (status IN ('sleeping','waking')),
  wake_kind        TEXT NOT NULL,
  wake_key         TEXT NOT NULL,
  updated_at       INTEGER NOT NULL,
  revision         TEXT NOT NULL,
  seq_watermark    INTEGER NOT NULL,
  descriptor_json  TEXT NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS idx_dsh_next_wake
  ON dsh_next_agent_descriptors(wake_kind, wake_key)
  WHERE status = 'sleeping';
`

export class DescriptorStoreError extends Error {
  override name = 'DescriptorStoreError'
}

export class DescriptorStore {
  private db!: DatabaseSync
  private stmtCache = new Map<string, import('node:sqlite').StatementSync>()

  constructor(private readonly path: string) {}

  async open(): Promise<void> {
    if (this.path !== ':memory:') mkdirSync(dirname(this.path), { recursive: true })
    this.db = new DatabaseSync(this.path)
    this.db.exec('PRAGMA journal_mode = WAL')
    this.db.exec('PRAGMA synchronous = FULL')
    this.db.exec(SCHEMA)
  }

  async close(): Promise<void> {
    this.db.close()
  }

  async upsert(desc: AgentDescriptor, status: 'sleeping' | 'waking' = 'sleeping'): Promise<void> {
    assertValid(desc)
    this.stmt(
      `INSERT INTO dsh_next_agent_descriptors
         (session_id, format_version, status, wake_kind, wake_key, updated_at, revision, seq_watermark, descriptor_json)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(session_id) DO UPDATE SET
         format_version=excluded.format_version,
         status=excluded.status,
         wake_kind=excluded.wake_kind,
         wake_key=excluded.wake_key,
         updated_at=excluded.updated_at,
         revision=excluded.revision,
         seq_watermark=excluded.seq_watermark,
         descriptor_json=excluded.descriptor_json`,
    ).run(
      desc.sessionId,
      desc.formatVersion,
      status,
      desc.wake.kind,
      desc.wake.key,
      Date.now(),
      desc.lastCommitted.revision,
      desc.lastCommitted.seqWatermark,
      JSON.stringify(desc),
    )
  }

  async get(sessionId: string): Promise<{ descriptor: AgentDescriptor; status: 'sleeping' | 'waking' } | undefined> {
    const row = this.stmt(
      'SELECT descriptor_json, status FROM dsh_next_agent_descriptors WHERE session_id = ?',
    ).get(sessionId) as { descriptor_json: string; status: string } | undefined
    if (row === undefined) return undefined
    return { descriptor: JSON.parse(row.descriptor_json) as AgentDescriptor, status: row.status as 'sleeping' | 'waking' }
  }

  async delete(sessionId: string): Promise<boolean> {
    return this.stmt('DELETE FROM dsh_next_agent_descriptors WHERE session_id = ?').run(sessionId).changes > 0
  }

  /**
   * Pull-based wake lookup (utilities for an event-driven future; no polling
   * scheduler lives in dsh-next).
   */
  async findSleepingByWake(kind: WakeKind, keyPrefix: string): Promise<AgentDescriptor[]> {
    // Key-prefix match lets range-style keys (e.g. zero-padded epoch buckets)
    // serve event providers without a second index family.
    const rows = this.stmt(
      `SELECT descriptor_json FROM dsh_next_agent_descriptors
       WHERE status='sleeping' AND wake_kind=? AND substr(wake_key,1,length(?))=?`,
    ).all(kind, keyPrefix, keyPrefix) as Array<{ descriptor_json: string }>
    return rows.map((r) => JSON.parse(r.descriptor_json) as AgentDescriptor)
  }

  async setWaking(sessionId: string): Promise<boolean> {
    return (
      this.stmt("UPDATE dsh_next_agent_descriptors SET status='waking', updated_at=? WHERE session_id=?").run(
        Date.now(),
        sessionId,
      ).changes > 0
    )
  }

  async count(): Promise<number> {
    const row = this.stmt('SELECT COUNT(*) AS n FROM dsh_next_agent_descriptors').get() as { n: number }
    return row.n
  }

  private stmt(sql: string): import('node:sqlite').StatementSync {
    let st = this.stmtCache.get(sql)
    if (st === undefined) {
      st = this.db.prepare(sql)
      this.stmtCache.set(sql, st)
    }
    return st!
  }
}

function assertValid(d: AgentDescriptor): void {
  if (typeof d.sessionId !== 'string' || d.sessionId.length === 0) {
    throw new DescriptorStoreError('sessionId required')
  }
  if (d.formatVersion !== DESCRIPTOR_FORMAT_VERSION) {
    throw new DescriptorStoreError(`format version mismatch: ${d.formatVersion}`)
  }
  if (d.wake === undefined || typeof d.wake.kind !== 'string' || typeof d.wake.key !== 'string') {
    throw new DescriptorStoreError('wake condition required')
  }
  if (
    d.lastCommitted === undefined ||
    typeof d.lastCommitted.revision !== 'string' ||
    !Number.isSafeInteger(d.lastCommitted.seqWatermark)
  ) {
    throw new DescriptorStoreError('lastCommitted source identity required')
  }
}
