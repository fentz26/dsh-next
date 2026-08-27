/**
 * Paged read-only consumer migration target: the Session raw-artifact
 * producer behind host log export (ApiProxy `/export` root artifact, backed by
 * `SessionPersistence.readRaw`).
 *
 * On backends without per-session raw artifacts (SQLite v17), DSH today has no
 * verbatim artifact to hand out; reconstructing one via `loadStored` forces
 * full eager hydration of every event. This module keeps the EXACT same
 * artifact contract — `{filename, content}` where content is the verbatim,
 * losslessly serialized logical event stream in seq order — while sourcing the
 * bytes through {@link PagedLogicalSource} ranges so no consumer ever forces
 * ~1M-event materialization.
 *
 * Exactness rule: each yielded chunk is byte-identical to concatenating
 * `JSON.stringify(event)` lines over the canonical ordering; the differential
 * test compares against the real `SqliteStore.loadStored()` oracle.
 *
 * Fallback preserved: session absent → `undefined` (matches `readRaw`
 * contract); malformed/gapped log → loud refusal (never silent truncation).
 * @module dsh-next/paged-history/export-consumer
 */
import type { PagedLogicalSource } from './index.ts'

/** Mirrors `SessionPersistence.SessionRawArtifact`. */
export interface RawArtifact {
  readonly filename: string
  readonly content: string
}

function safeSessionIdSegment(sessionId: string): string {
  const safe = sessionId.replace(/[^A-Za-z0-9_-]/g, '')
  return safe.length > 0 ? safe : 'session'
}

/** Same filename convention owned by the host export endpoint. */
export function sessionLogJsonlFilename(sessionId: string): string {
  return `dsh-session-${safeSessionIdSegment(sessionId)}.jsonl`
}

/**
 * Stream the session's raw artifact content as bounded text chunks.
 *
 * @param source - any paged logical source.
 * @param sessionId - canonical session id.
 * @param pageSize - logical events per underlying range read (bounded memory).
 * @yields text chunks whose concatenation equals the full artifact content.
 * @returns nothing; throws on malformed/gapped logs or missing session.
 */
export async function* streamRawArtifact(
  source: PagedLogicalSource,
  sessionId: string,
  options?: { pageSize?: number; signal?: AbortSignal },
): AsyncGenerator<string> {
  const meta = await source.meta(sessionId)
  if (meta === undefined) throw new Error(`paged-export: session ${sessionId} not found`)
  let emitted = ''
  let nextSeq = 0
  while (nextSeq < meta.length) {
    const page = await source.readRange(sessionId, nextSeq, {
      limit: options?.pageSize ?? 4096,
      signal: options?.signal,
    })
    for (const ev of page.events) {
      // Verbatim single-line serialization, matching full-load reconstruction
      // key order because pages are freshly decoded store rows.
      emitted += `${JSON.stringify(ev)}\n`
      if (emitted.length >= 1 << 20) {
        yield emitted
        emitted = ''
      }
    }
    if (page.endOfLogAt !== undefined) {
      nextSeq = page.endOfLogAt
    } else {
      nextSeq += page.events.length
    }
  }
  if (emitted.length > 0) yield emitted
}

/**
 * Artifact-shaped convenience wrapper: resolves `undefined` exactly like
 * `SessionPersistence.readRaw` does for an absent session so existing callers
 * keep their fallback paths unchanged.
 */
export async function readRawArtifactPaged(
  source: PagedLogicalSource,
  sessionId: string,
  options?: { pageSize?: number; signal?: AbortSignal },
): Promise<RawArtifact | undefined> {
  const meta = await source.meta(sessionId)
  if (meta === undefined) return undefined
  const parts: string[] = []
  for await (const chunk of streamRawArtifact(source, sessionId, options)) parts.push(chunk)
  return { filename: sessionLogJsonlFilename(sessionId), content: parts.join('') }
}
