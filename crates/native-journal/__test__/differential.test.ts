/**
 * Differential tests: native Rust journal vs TS reference (DSH strategy).
 * Run: npx tsx crates/native-journal/__test__/differential.test.ts
 * Requires a built pilot: crates/native-journal/pilot.node
 */
import { randomBytes } from 'node:crypto'
import { existsSync } from 'node:fs'
import { createRequire } from 'node:module'
import { ReferenceByteJournal } from '../../../packages/journal/src/index.ts'

interface NativeJournal {
  append(chunk: Buffer): number
  appendBatch(chunks: Buffer[]): number
  readFrom(offset: number): { data: Buffer; nextOffset: number; lossy: boolean }
  readonly nextOffset: number
  readonly windowStart: number
}

const nativePath = new URL('../pilot.node', import.meta.url).pathname
if (!existsSync(nativePath)) {
  console.error('SKIP: build the native pilot first (cargo build --release; cp ...)')
  process.exit(0)
}
const require_ = createRequire(import.meta.url)
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const native: any = require_(nativePath)

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

check('native probe', () => {
  if (!/ok/.test(native.probe())) throw new Error('probe failed')
})

check('basic round trip', () => {
  const j: NativeJournal = new native.NativeByteJournal(1 << 20)
  const payload = randomBytes(5000)
  j.append(payload)
  const r = j.readFrom(0)
  if (r.lossy !== false || r.nextOffset !== 5000) throw new Error('bad result')
  if (Buffer.compare(r.data, payload) !== 0) throw new Error('data mismatch')
})

check('randomized differential vs TS reference', () => {
  let seed = (0x2545f491 ^ 0x9e3779b9) >>> 0
  const rand = (): number => {
    seed ^= seed << 13
    seed ^= seed >>> 17
    seed ^= seed << 5
    return ((seed >>> 0) % 100000) / 100000
  }
  for (let trial = 0; trial < 30; trial++) {
    const maxBytes = 64 + Math.floor(rand() * 8192)
    const ref = new ReferenceByteJournal(maxBytes)
    const nat: NativeJournal = new native.NativeByteJournal(maxBytes)
    let total = 0
    for (let step = 0; step < 200; step++) {
      const n = 1 + Math.floor(rand() * Math.max(1, maxBytes / 3))
      const chunk = randomBytes(n)
      ref.append(chunk)
      nat.append(chunk)
      total += n

      if (ref.nextOffset !== nat.nextOffset) throw new Error('nextOffset mismatch')
      if (ref.windowStart !== nat.windowStart) throw new Error(`windowStart mismatch (${ref.windowStart} vs ${nat.windowStart})`)

      const from =
        rand() < 0.3 ? Math.floor(rand() * total) : Math.max(0, total - Math.floor(rand() * maxBytes))
      const rr = ref.readFrom(from)
      const nr = nat.readFrom(from)
      if (rr.lossy !== nr.lossy) throw new Error('lossy mismatch')
      if (nr.data.length !== rr.data.length) throw new Error(`len mismatch at from=${from}`)
      if (Buffer.compare(rr.data, nr.data) !== 0) throw new Error('content mismatch')
    }
  }
})

check('oversized chunk trims to cap + batched append', () => {
  const j: NativeJournal = new native.NativeByteJournal(1024)
  j.append(randomBytes(4096))
  if (j.readFrom(0).data.length !== 1024) throw new Error('cap')
  const j2: NativeJournal = new native.NativeByteJournal(4096)
  j2.appendBatch([randomBytes(1000), randomBytes(1000), randomBytes(500)])
  if (j2.nextOffset !== 2500) throw new Error('batch offsets')
})

check('split UTF-8 stays byte-exact', () => {
  const text = Buffer.from('\u03b1'.repeat(300), 'utf8')
  const j: NativeJournal = new native.NativeByteJournal(text.length - 1)
  j.append(text)
  const r = j.readFrom(0)
  if (r.data.length !== text.length - 1) throw new Error('byte count')
})

console.log(`\n${passed} passed, ${failed} failed`)
process.exit(failed > 0 ? 1 : 0)
