import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { WorkerSqliteBackend } from '../src/backend.ts'

let passed = 0, failed = 0
async function check(name: string, fn: () => Promise<void>): Promise<void> {
  try { await fn(); passed++; console.log(`ok - ${name}`) }
  catch (e) { failed++; console.error(`FAIL - ${name}`); console.error(e) }
}

const dir = mkdtempSync(join(tmpdir(), 'restart-test-'))
const dbPath = join(dir, 'r.sqlite')
const meta = { version: 0, id: crypto.randomUUID(), createdAt: Date.now(), cwd: '/w' }

await check('crash with restartOnCrash: pendings rejected, provider recovers, prior durable data intact', async () => {
  const be = new WorkerSqliteBackend({
    path: dbPath, journalMode: 'wal', busyTimeoutMs: 5000,
    maxInFlight: 1,
    restartOnCrash: true,
  })
  await be.init()

  // Durable write before the crash.
  const eventsA = [{ type: 'turn/start', seq: 0, time: 1, data: { turn: 1 } }]
  await be.appendBatch(meta, eventsA, false)

  let pendingRejected: unknown
  const slow = (be as unknown as { injectPendingForTest(): Promise<never> }).injectPendingForTest()
  void slow.catch((e) => (pendingRejected = e))

  const restarted = new Promise<void>((r) => be.once('restarted', () => r()))
  await be.killForTest()
  for (let i = 0; i < 100 && pendingRejected === undefined; i++) {
    await new Promise((r) => setTimeout(r, 5))
  }
  if (!(pendingRejected instanceof Error)) throw new Error('pending must reject deterministically')
  await Promise.race([restarted, new Promise((r) => setTimeout(r, 8000))])

  if (be.failedState) throw new Error('backend should have recovered')

  // Post-restart: previous durability visible + new writes work.
  const reloaded = (await be.loadStored(meta.id)) as { events?: Array<{ seq: number }> }
  if (reloaded?.events?.length !== 1) throw new Error('pre-crash durable data lost')
  await be.appendBatch(meta, [{ type: 'turn/end', seq: 1, time: 2, data: { turn: 1, reason: { kind: 'completed' } } }], true)
  const again = (await be.loadStored(meta.id)) as { events?: unknown[] }
  if ((again.events?.length ?? 0) !== 2) throw new Error('post-restart append lost')
  const stats = be.stats()
  if (stats.failed) throw new Error('stats should show recovery')
  await be.close()
})

await check('restart disabled by default stays fail-fast', async () => {
  const be = new WorkerSqliteBackend({ path: join(dir, 'nocrash.sqlite'), journalMode: 'wal', busyTimeoutMs: 5000 })
  await be.init()
  await be.killForTest()
  if (!be.failedState) throw new Error('expected failed state')
  let rejected = false
  try { await be.list() } catch { rejected = true }
  if (!rejected) throw new Error('fail-fast required')
})

console.log(`\n${passed} passed, ${failed} failed`)
rmSync(dir, { recursive: true, force: true })
process.exit(failed > 0 ? 1 : 0)
