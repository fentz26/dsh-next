/**
 * Derived checkpoint (v1): CHUNK-FILTERED CANONICAL PREFIX
 *
 * Design rationale (docs/checkpoints.md):
 * - The consumer audit proved the model-facing folds consume ONLY surface
 *   events (user/message, assistant/message, tool/result …). `assistant/chunk`
 *   events always derive to `null` in surface projection and never touch any
 *   fold cursor state in core/session.
 * - Therefore dropping ONLY `assistant/chunk` events from a canonical prefix,
 *   while keeping EVERY OTHER EVENT VERBATIM (including unknown-required and
 *   ignorable plugin events), yields a payload whose replay through the REAL
 *   `Session.fromRestore` machinery produces model-visible state equivalent to
 *   full replay — verified empirically, not assumed (test suite here).
 * - Canonical log stays the sole authority: checkpoints are rebuildable caches;
 *   any mismatch/corruption falls back to full replay (#59 fail-open rules).
 *
 * Format identity (spec #18/#119):
 *   version 1 · sessionId · sourceRevision(opaque passthrough) · prefixEndSeq K
 *   · droppedCount · checksum(CRC32-ish over event JSON lines)
 *
 * Seq handling: prefix events are stored DENSELY REBASED to satisfy the
 * `seq === index` restoration invariant; the original→dense mapping is implied
 * by order plus recorded boundary metadata, so audit tooling can map back (#42).
 */

export const CHECKPOINT_FORMAT_VERSION = 1

/** Event types EXCLUDED from checkpoint payloads in v1. */
const DROPPED_TYPES = new Set(['assistant/chunk'])

export interface CheckpointEventLine {
  /** Dense restored-log index. */
  i: number
  /** Original canonical seq (audit trail / mapping back, #42). */
  o: number
  t: string
  m: number // ms timestamp
  d: unknown
  /** surfaceOp — required for replacement semantics (#29). */
  s?: unknown
  /** sourceEventSeqs — provenance for surface replacements. */
  q?: number[]
}

export interface CheckpointArtifact {
  formatVersion: number
  sessionId: string
  sourceRevision?: string
  /** Exclusive upper bound: canonical prefix [0,K) was distilled. */
  prefixEndSeq: number
  prefixLogicalCount: number
  droppedCount: number
  events: CheckpointEventLine[]
  createdAt: number
  checksum: string
}

interface FilterInput {
  sessionId: string
  events: ReadonlyArray<
    { seq: number } & Partial<{
      type: string
      time: number
      data: unknown
      surfaceOp: unknown
      sourceEventSeqs: number[]
    }>
  >
  sourceRevision?: string
  /** Inclusive end bound of the distilled prefix. */
  prefixEndSeq: number
}

function lineChecksum(lines: string[]): string {
  // FNV-1a 32-bit, hex. Tamper-evident enough for a discardable cache (#109):
  // corruption ⇒ mismatch ⇒ fall back to canonical replay.
  let h = 0x811c9dc5
  for (const l of lines) {
    for (let i = 0; i < l.length; i++) {
      h ^= l.charCodeAt(i)
      h = Math.imul(h, 0x01000193)
    }
    h ^= 0xff
    h = Math.imul(h, 0x01000193)
  }
  return (h >>> 0).toString(16).padStart(8, '0')
}

/** Distill a canonical prefix into a checkpoint payload. */
export function distill(input: FilterInput): CheckpointArtifact {
  const lines: string[] = []
  const events: CheckpointEventLine[] = []
  let dense = 0
  let dropped = 0
  let logicalCount = 0

  for (const rawEv of input.events) {
    const e = rawEv as {
      seq: number
      type: string
      time: number
      data: unknown
      surfaceOp?: unknown
      sourceEventSeqs?: number[]
    }
    if (e.seq >= input.prefixEndSeq) break
    logicalCount++
    if (DROPPED_TYPES.has(e.type)) {
      dropped++
      continue
    }
    const line: CheckpointEventLine = {
      i: dense++,
      o: e.seq,
      t: e.type,
      m: e.time,
      d: e.data as never,
      ...(e.surfaceOp !== undefined ? { s: e.surfaceOp } : {}),
      ...(e.sourceEventSeqs !== undefined ? { q: e.sourceEventSeqs } : {}),
    }
    lines.push(JSON.stringify(line))
    events.push(line)
  }

  return {
    formatVersion: CHECKPOINT_FORMAT_VERSION,
    sessionId: input.sessionId,
    ...(input.sourceRevision !== undefined ? { sourceRevision: input.sourceRevision } : {}),
    prefixEndSeq: input.prefixEndSeq,
    prefixLogicalCount: logicalCount,
    droppedCount: dropped,
    events,
    createdAt: Date.now(),
    checksum: lineChecksum(lines),
  }
}

export function verifyCheckpoint(a: CheckpointArtifact): boolean {
  if (a.formatVersion !== CHECKPOINT_FORMAT_VERSION) return false
  return (
    lineChecksum(
      a.events.map((l) => JSON.stringify(l)),
    ) === a.checksum
  )
}
