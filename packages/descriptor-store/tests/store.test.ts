/**
 * Descriptor-store tests (Track F prototype).
 * Run: npx tsx packages/descriptor-store/tests/store.test.ts
 */
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
  DESCRIPTOR_FORMAT_VERSION,
  DescriptorStore,
} from '../src/index.ts'
import type { AgentDescriptor } from '../src/index.ts'

let passed = 0
let failed = 0
async function check(name: string, fn: () => Promise<void>): Promise<void> {
  try {
    await fn()
    passed++
    console.log(`ok - ${name}`)
  } catch (err) {
    failed++
    console.error(`FAIL - ${name}`)
    console.error(err)
  }
}

function desc(sessionId: string, overrides: Partial<AgentDescriptor> = {}): AgentDescriptor {
  return {
    sessionId,
    formatVersion: DESCRIPTOR_FORMAT_VERSION,
    wake: { kind: 'timer', key: String(Date.now() + 60_000) },
    lastCommitted: { revision: `rev-${sessionId}`, seqWatermark: 42 },
    ...overrides,
  }
}

const dir = mkdtempSync(join(tmpdir(), 'dsh-next-desctest-'))
await check('round trip upsert/get/delete', async () => {
  const s = new DescriptorStore(join(dir, 'a.sqlite'))
  await s.open()
  const d = desc('sess-1')
  await s.upsert(d)
  const got = await s.get('sess-1')
  if (JSON.stringify(got?.descriptor) !== JSON.stringify(d)) throw new Error('round trip mismatch')
  if (got?.status !== 'sleeping') throw new Error('status')
  if ((await s.delete('sess-1')) !== true) throw new Error('delete')
  if ((await s.get('sess-1')) !== undefined) throw new Error('deleted row remains')
  await s.close()
})

await check('invalid descriptors rejected', async () => {
  const s = new DescriptorStore(':memory:')
  await s.open()
  try {
    await s.upsert(desc('x', { formatVersion: 999 } as unknown as AgentDescriptor))
    throw new Error('expected rejection')
  } catch (err) {
    if (!String(err).includes('format version')) throw err
  }
  try {
    await s.upsert(desc('', {}) as AgentDescriptor)
    throw new Error('expected rejection')
  } catch (err) {
    if (!String(err).includes('sessionId')) throw err
  }
  // no lastCommitted -> must refuse (source identity is mandatory)
  const bad = desc('y') as unknown as Record<string, unknown>
  delete bad.lastCommitted
  try {
    await s.upsert(bad as unknown as AgentDescriptor)
    throw new Error('expected rejection')
  } catch (err) {
    if (!String(err).includes('lastCommitted')) throw err
  }
  await s.close()
})

await check('wake scans respect kind and sleeping state', async () => {
  const s = new DescriptorStore(join(dir, 'b.sqlite'))
  await s.open()
  for (let i = 0; i < 5; i++) {
    await s.upsert(desc(`timer-${i}`, { wake: { kind: 'timer', key: `${1000 + i}` } }))
  }
  await s.upsert(desc('wait-1', { wake: { kind: 'wait-provider', key: 'w1' } }))
  if ((await s.findSleepingByWake('timer', '10')).length !== 5) throw new Error('timer scan')
  if ((await s.findSleepingByWake('wait-provider', 'w')).length !== 1) throw new Error('provider scan')
  await s.setWaking('timer-3')
  if ((await s.findSleepingByWake('timer', '10')).length !== 4) throw new Error('waking excluded')
  if ((await s.count()) !== 6) throw new Error('count')
  await s.close()
})

await check('durability across reopen', async () => {
  const path = join(dir, 'c.sqlite')
  const s1 = new DescriptorStore(path)
  await s1.open()
  await s1.upsert(desc('persist-me'))
  await s1.close()
  const s2 = new DescriptorStore(path)
  await s2.open()
  const got = await s2.get('persist-me')
  if (got === undefined || got.descriptor.lastCommitted.seqWatermark !== 42) throw new Error('not durable')
  await s2.close()
})

console.log(`\n${passed} passed, ${failed} failed`)
rmSync(dir, { recursive: true, force: true })
process.exit(failed > 0 ? 1 : 0)
