/** Locates the DeepSeek Harness checkout used as the benchmark baseline. */
import { existsSync } from 'node:fs'
import { homedir } from 'node:os'
import { join } from 'node:path'
import { pathToFileURL } from 'node:url'

export function dshRoot(): string {
  const candidates = [
    process.env.DSH_ROOT,
    join(homedir(), 'deepseek-harness'),
  ].filter(Boolean) as string[]
  for (const c of candidates) {
    if (existsSync(join(c, 'package.json'))) return c
  }
  throw new Error('DeepSeek Harness checkout not found. Set DSH_ROOT.')
}

export async function importDsh(relPathFromRoot: string): Promise<unknown> {
  return import(pathToFileURL(join(dshRoot(), relPathFromRoot)).href)
}

export async function sqliteStoreModule(): Promise<{
  SqliteStore: new (options: {
    path: string
    journalMode: 'wal' | 'delete' | 'truncate' | 'persist'
    busyTimeoutMs: number
  }) => DshSqliteStore
}> {
  return importDsh('packages/session/session-persistence-sqlite/src/store.ts') as never
}

export async function compressionModule(): Promise<DshCompression> {
  return importDsh('packages/session/session-persistence-sqlite/src/compression.ts') as never
}

export async function outputCollectorModule(): Promise<{ OutputCollector: new (opts: unknown) => DshOutputCollector }> {
  return importDsh('packages/subprocess/subprocess-local/src/spawn.ts') as never
}

// Structural types mirroring the audited DSH APIs (not imported, to keep the
// out-of-tree repo free of workspace dependencies).

export interface DshSqliteStore {
  open(): Promise<void>
  appendBatch(
    meta: { version: number; id: string; createdAt: number; cwd: string },
    events: readonly Record<string, unknown>[],
    isMaterialized: boolean,
  ): Promise<void>
  loadStored(id: string): Promise<{ meta: unknown; events: unknown[] } | undefined>
  loadStoredFrom(id: string, fromSeq: number): Promise<{ meta: unknown; events: unknown[] } | undefined>
  list(): Promise<unknown[]>
  close(): Promise<void>
}

export interface DshCompression {
  bindRecord(record: unknown): unknown
  encodeData(bytes: Buffer): Buffer | string
  decodeData(data: Buffer | string, maxOutputLength?: number): Buffer
  ZSTD_DATA_THRESHOLD_BYTES: number
  ZSTD_COMPRESSION_LEVEL: number
}

export interface CollectorRead {
  text: string
  nextOffset: number
  lossy: boolean
  spillPath?: string
}

export interface DshOutputCollector {
  push(chunk: Buffer): void
  readFrom(fromByte: number): CollectorRead
  finalize(): { text: string; truncated: boolean; spillPath?: string }
}
