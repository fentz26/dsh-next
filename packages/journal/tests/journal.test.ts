/**
 * Differential + edge-case tests for BoundedByteJournal implementations.
 * Run: npx tsx packages/journal/tests/journal.test.ts
 */
import { randomBytes } from 'node:crypto'
import {
  ReferenceByteJournal,
  SegmentedByteJournal,
} from '../src/index.ts'
import type { JournalRead } from '../src/index.ts'

type Journaled = {
  maxBytes: number
  nextOffset: number
  windowStart: number
  append(b: Uint8Array): number
  readFrom(o: number): JournalRead
}

const IMPLS = [ReferenceByteJournal, SegmentedByteJournal] as const

let passed = 0
let failed = 0

function check(name: string, fn: () => void): void {
  try {
    fn()
    passed++
    console.log(`ok - ${name}`)
  } catch (err) {
    failed++
    console.error(`FAIL - ${name}`)
    console.error(err)
  }
}

function asBuffer(v: Uint8Array | { data: Uint8Array }): Buffer {
  const u = 'data' in v && !(v instanceof Uint8Array) ? v.data : (v as Uint8Array)
  return Buffer.isBuffer(u) ? u : Buffer.from(u.buffer, u.byteOffset, u.byteLength)
}

function assertSameReads(a: Uint8Array | { data: Uint8Array }, b: Uint8Array | { data: Uint8Array }): void {
  if (Buffer.compare(asBuffer(a), asBuffer(b)) !== 0) throw new Error('read data mismatch')
}

check('append + readFrom round trip below cap', () => {
  for (const J of IMPLS) {
    const j: Journaled = new J(1024 * 1024)
    const payload = randomBytes(5000)
    j.append(payload)
    const r = j.readFrom(0)
    if (r.lossy !== false) throw new Error('unexpected lossy')
    if (j.nextOffset !== 5000) throw new Error('nextOffset')
    assertSameReads(r.data, payload)
  }
})

check('bounded tail keeps exactly maxBytes', () => {
  for (const J of IMPLS) {
    const j: Journaled = new J(10_000)
    const stream = randomBytes(50_000)
    for (let i = 0; i < stream.length; i += 1000) j.append(stream.subarray(i, i + 1000))
    const r = j.readFrom(0)
    if (r.lossy !== true) throw new Error('expected lossy')
    if (r.data.byteLength > 10_000) throw new Error('over cap')
    const expected = stream.subarray(stream.length - r.data.byteLength)
    assertSameReads(r.data, expected)
    const mid = j.nextOffset - 4000
    const r2 = j.readFrom(mid)
    if (r2.lossy !== false) throw new Error('mid should be lossless')
    assertSameReads(r2.data, stream.subarray(mid))
  }
})

check('oversized single chunk trims to cap', () => {
  for (const J of IMPLS) {
    const j: Journaled = new J(1024)
    j.append(randomBytes(4096))
    const r = j.readFrom(0)
    if (r.data.byteLength !== 1024) throw new Error(`got ${r.data.byteLength}`)
  }
})

check('exact cap boundary', () => {
  for (const J of IMPLS) {
    const j: Journaled = new J(8192)
    const s = randomBytes(8192)
    j.append(s)
    const r = j.readFrom(0)
    if (r.lossy !== false) throw new Error('lossy at exact cap')
    assertSameReads(r.data, s)
  }
})

check('split multi-byte UTF-8 stays byte-exact', () => {
  for (const J of IMPLS) {
    const text = Buffer.from('α'.repeat(300), 'utf8')
    const j: Journaled = new J(text.length - 1)
    j.append(text)
    const r = j.readFrom(0)
    if (r.data.byteLength !== text.length - 1) throw new Error('byte count')
  }
})

check('independent readers do not consume', () => {
  for (const J of IMPLS) {
    const j: Journaled = new J(1 << 20)
    let off = 0
    for (let i = 0; i < 10; i++) {
      const chunk = randomBytes(1234 + i)
      j.append(chunk)
      off += chunk.length
    }
    if (j.nextOffset !== off) throw new Error('nextOffset drift')
    const a = j.readFrom(off - 2000)
    const b = j.readFrom(off - 2000)
    assertSameReads(a, b)
    if (j.readFrom(off).data.byteLength !== 0) throw new Error('eof read')
  }
})

check('randomized differential: segmented == reference', () => {
  let seed = 0x2545f491
  const rand = () => {
    seed ^= seed << 13
    seed ^= seed >>> 17
    seed ^= seed << 5
    return ((seed >>> 0) % 100000) / 100000
  }
  for (let trial = 0; trial < 30; trial++) {
    const maxBytes = 64 + Math.floor(rand() * 8192)
    const ref: Journaled = new ReferenceByteJournal(maxBytes)
    const opt: Journaled = new SegmentedByteJournal(maxBytes)
    let total = 0
    for (let step = 0; step < 200; step++) {
      const n = 1 + Math.floor(rand() * Math.max(1, maxBytes / 3))
      const chunk = randomBytes(n)
      ref.append(chunk)
      opt.append(chunk)
      total += n
      if (ref.windowStart !== opt.windowStart) throw new Error('windowStart mismatch')
      if (ref.nextOffset !== opt.nextOffset) throw new Error('nextOffset mismatch')
      const from =
        rand() < 0.3 ? Math.floor(rand() * total) : Math.max(0, total - Math.floor(rand() * maxBytes))
      const rr = ref.readFrom(from)
      const oo = opt.readFrom(from)
      if (rr.lossy !== oo.lossy) throw new Error('lossy mismatch')
      assertSameReads(rr, oo)
    }
  }
})

console.log(`\n${passed} passed, ${failed} failed`)
if (failed > 0) process.exit(1)
