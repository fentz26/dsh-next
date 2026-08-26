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
export class ReferenceByteJournal {
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
export class SegmentedByteJournal {
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
