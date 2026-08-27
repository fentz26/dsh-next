/**
 * BoundedByteJournal — TypeScript implementations.
 *
 * Canonical semantics (must match DSH OutputCollector / any native pilot):
 * - retains a bounded byte tail of an append-only stream
 * - absolute whole-stream byte offsets (`nextOffset`)
 * - `readFrom(offset)` -> retained suffix since `offset`; non-consuming;
 *   independent readers; `lossy` when `offset` fell below the window start
 * - memory stays bounded: at most `maxBytes` plus incoming-chunk transient
 *   headroom, exactly like DSH
 *
 * Byte-vs-text policy: bytes are canonical. Text conversion uses
 * Buffer.toString('utf8') on byte slices, matching DSH exactly — including
 * replacement characters when a read boundary splits a multi-byte sequence.
 */

export interface JournalRead {
  /** Retained bytes since `fromOffset` (whole window when lossy). */
  readonly data: Uint8Array
  /** Absolute offset for the next read (= total bytes appended so far). */
  readonly nextOffset: number
  /** True when `fromOffset` slid below the retained window start. */
  readonly lossy: boolean
}

/**
 * Production journal contract (bytes authoritative; see docs/journal.md).
 * - `append` accepts any chunk size; bytes are copied once into journal-owned
 *   segments; returns the new absolute stream length (`nextOffset`).
 * - `readFrom` is non-consuming with independent readers; lossy reads return
 *   the whole retained tail.
 */
export interface ByteJournal {
  readonly maxBytes: number
  readonly nextOffset: number
  readonly windowStart: number
  append(data: Uint8Array): number
  /**
   * Batched append for high-rate producers (mirrors native appendBatch):
   * chunk order preserved exactly; identical observable state to sequential
   * append calls (both implementations derive this by construction).
   */
  appendBatch?(chunks: readonly Uint8Array[]): number
  readFrom(offset: number): JournalRead
}

export type JournalImplementationName = 'segmented-ts' | 'reference-ts' | 'native-rust'

export interface JournalSelection {
  implementation: JournalImplementationName
  create(maxBytes: number): ByteJournal
  reason: string
}

/**
 * Selects the journal implementation for a consumer.
 *
 * Default is the segmented TS journal: Phase 0 measured it fastest on the hot
 * path (readFrom ~0.3–0.4 µs vs DSH ~252–303 µs and native ~0.8–0.9 µs) while
 * the native pilot does not dominate any measured workload end-to-end. The
 * native module remains available experimentally via @dsh-next/native.
 */
export function selectJournal(
  options: {
    native?: { available: boolean }
    preferred?: JournalImplementationName
  } = {},
): JournalSelection {
  if (options.preferred === 'reference-ts') {
    return {
      implementation: 'reference-ts',
      create: (maxBytes) => new ReferenceByteJournal(maxBytes),
      reason: 'explicitly requested reference strategy',
    }
  }
  if (options.preferred === 'native-rust' && options.native?.available) {
    return {
      implementation: 'native-rust',
      // Assigned lazily by @dsh-next/native (avoids a hard dependency here).
      create: (maxBytes) => {
        throw new Error('native-rust journal requires @dsh-next/native loadNative() factory wiring')
      },
      reason: 'experimental native selection',
    }
  }
  return {
    implementation: 'segmented-ts',
    create: (maxBytes) => new SegmentedByteJournal(maxBytes),
    reason:
      'measured best on read-heavy hot path; native available=' +
      String(options.native?.available ?? false) +
      ' (experimental only)',
  }
}

interface Segment {
  /** Stream offset of buffer[0]. */
  readonly start: number
  readonly buffer: Uint8Array
}

const SEGMENT_TARGET_BYTES = 64 * 1024

/**
 * Faithful re-implementation of the current DSH OutputCollector strategy:
 * Buffer[] chunks + bookkeeping, concatenating the WHOLE window on every read.
 * Exists as the differential-testing baseline and as the guaranteed-available
 * fallback when no native module is present.
 */
export class ReferenceByteJournal implements ByteJournal {
  private chunks: Buffer[] = []
  private windowBytes = 0
  private total = 0

  constructor(readonly maxBytes: number) {}

  get nextOffset(): number {
    return this.total
  }

  get windowStart(): number {
    return this.total - this.windowBytes
  }

  append(bytes: Uint8Array): number {
    const b = Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes)
    this.total += b.length
    this.chunks.push(b)
    this.windowBytes += b.length
    let excess = this.windowBytes - this.maxBytes
    while (excess > 0 && this.chunks.length > 0) {
      const head = this.chunks[0]
      if (head.length <= excess) {
        excess -= head.length
        this.windowBytes -= head.length
        this.chunks.shift()
      } else {
        this.chunks[0] = head.subarray(excess)
        this.windowBytes -= excess
        excess = 0
      }
    }
    return this.total
  }

  readFrom(fromOffset: number): JournalRead {
    const windowStart = this.total - this.windowBytes
    const buffer = Buffer.concat(this.chunks)
    const lossy = fromOffset < windowStart
    const slice = lossy ? buffer : buffer.subarray(fromOffset - windowStart)
    return { data: slice, nextOffset: this.total, lossy }
  }
}

/**
 * Optimized TS candidate: offset-indexed segmented log.
 *
 * Committed segments are ordered by stream offset; one partially-filled tail
 * scratch amortizes allocation across small appends. An explicit `windowStart`
 * makes bounds byte-exact even mid-segment. `readFrom` touches only segments
 * intersecting the requested range, so N observers polling cost O(bytes-read),
 * not O(window) each time.
 */
export class SegmentedByteJournal implements ByteJournal {
  private segments: Segment[] = []
  /** Absolute stream offset of the first retained byte. */
  private winStart = 0
  private total = 0

  /** Shared partially-filled tail scratch. */
  private scratch: Buffer | undefined
  /** Absolute stream offset of scratch[skip]. */
  private scratchAbs = 0
  private skip = 0
  private used = 0

  constructor(readonly maxBytes: number) {}

  get nextOffset(): number {
    return this.total
  }

  get windowStart(): number {
    return this.winStart
  }

  append(bytes: Uint8Array): number {
    const len = bytes.length
    const src = Buffer.isBuffer(bytes)
      ? bytes
      : Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength)

    // Amortize small writes into the shared tail scratch.
    if (len <= SEGMENT_TARGET_BYTES) {
      const scratchFullOrMissing =
        this.scratch === undefined ||
        this.used === SEGMENT_TARGET_BYTES ||
        this.used + len > SEGMENT_TARGET_BYTES
      if (scratchFullOrMissing) {
        this.flushScratch()
        this.scratch = Buffer.allocUnsafe(SEGMENT_TARGET_BYTES)
        this.skip = 0
        this.used = 0
        this.scratchAbs = this.total
      }
      const target = this.scratch as Buffer
      src.copy(target, this.used)
      this.used += len
      this.total += len
      this.evict()
      return this.total
    }

    // Large writes commit the scratch and become their own segment...
    // unless the previous scratch is exactly full, which already happened.
    this.flushScratch()
    this.segments.push({ start: this.total, buffer: src.subarray() })
    this.total += len
    this.evict()
    return this.total
  }

  appendBatch(chunks: readonly Uint8Array[]): number {
    for (const c of chunks) this.append(c)
    return this.total
  }

  private flushScratch(): void {
    if (this.scratch === undefined) return
    const view =
      this.skip === 0 && this.used === SEGMENT_TARGET_BYTES
        ? this.scratch
        : this.scratch.subarray(this.skip, this.used)
    // Bytes before `skip` were already evicted from the window.
    this.segments.push({
      start: this.scratchAbs + this.skip,
      buffer: view,
    })
    this.scratch = undefined
    this.skip = 0
    this.used = 0
  }

  private evict(): void {
    let excess = this.total - this.winStart - this.maxBytes
    while (excess > 0 && this.segments.length > 0) {
      const head = this.segments[0]
      if (head.buffer.length <= excess) {
        excess -= head.buffer.length
        this.winStart += head.buffer.length
        this.segments.shift()
      } else {
        this.segments[0] = {
          start: head.start + excess,
          buffer: head.buffer.subarray(excess),
        }
        this.winStart += excess
        excess = 0
      }
    }
    // Only the scratch remains: slide its logical start forward (subarray),
    // mirroring DSH's `subarray(excess)` head trim.
    if (excess > 0 && this.scratch !== undefined) {
      const avail = this.used - this.skip
      const cut = Math.min(avail, excess)
      this.skip += cut
      this.winStart += cut
    }
  }

  readFrom(fromOffset: number): JournalRead {
    const lossy = fromOffset < this.winStart
    const readStart = Math.max(fromOffset, this.winStart)

    const parts: Uint8Array[] = []

    // Binary search first committed segment ending beyond readStart.
    let lo = 0
    let hi = this.segments.length
    while (lo < hi) {
      const mid = (lo + hi) >>> 1
      const seg = this.segments[mid]
      if (seg.start + seg.buffer.length <= readStart) lo = mid + 1
      else hi = mid
    }
    for (let i = lo; i < this.segments.length; i++) {
      const seg = this.segments[i]
      parts.push(
        Buffer.isBuffer(seg.buffer)
          ? seg.buffer.subarray(Math.max(readStart, seg.start) - seg.start)
          : Uint8Array.prototype.slice.call(
              seg.buffer,
              Math.max(readStart, seg.start) - seg.start,
            ),
      )
    }

    // Pending scratch tail (always the highest-offset region).
    if (this.scratch !== undefined && this.used > this.skip) {
      const scratchStart = this.scratchAbs + this.skip
      if (readStart < this.scratchAbs + this.used) {
        const loInScratch = Math.max(readStart, scratchStart) - this.scratchAbs
        parts.push(this.scratch.subarray(loInScratch, this.used))
      }
    }

    const joined =
      parts.length === 1
        ? Buffer.from(
            parts[0].buffer,
            parts[0].byteOffset,
            parts[0].byteLength,
          )
        : Buffer.concat(
            parts.map((p) =>
              Buffer.isBuffer(p) ? p : Buffer.from(p.buffer, p.byteOffset, p.byteLength),
            ),
          )
    return { data: joined, nextOffset: this.total, lossy }
  }
}
