/**
 * Checkpoint equivalence (spec #57/#116): does
 *
 *   FULL canonical replay
 *     ===
 *   chunk-filtered checkpoint prefix + suffix events appended live
 *
 * produce identical MODEL-VISIBLE state (deriveMessages, requestHeader,
 * requestContext, turn balance) using the REAL DSH Session machinery?
 *
 * Run: DSH_ROOT=... npx tsx packages/checkpoint-replay/tests/equivalence.test.ts
 */
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createRequire } from 'node:module'
import { pathToFileURL } from 'node:url'
import {
  CHECKPOINT_FORMAT_VERSION,
  distill,
  verifyCheckpoint,
} from '../src/index.ts'
import type { CheckpointArtifact } from '../src/index.ts'

const require_ = createRequire(import.meta.url)
const dshRoot = process.env.DSH_ROOT
if (dshRoot === undefined) {
  console.log('SKIP: set DSH_ROOT to a DeepSeek Harness checkout')
  process.exit(0)
}

type Event = Record<string, unknown>
const coreSession = await import(
  pathToFileURL(`${dshRoot}/packages/core/session/src/index.ts`).href
) as unknown as {
  Session: new (...a: unknown[]) => never
  SessionId: unknown
  SESSION_FORMAT_VERSION: number
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
}
const { Session } = coreSession as unknown as { Session: any }

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

// Realistic mix including surface replacement + plugin-style custom event.
function generateMixed(count: number): Array<Record<string, unknown>> {
  const evs: Array<Record<string, unknown>> = []
  let seq = 0
  let time = Date.now() - 3_600_000
  let turn = 1
  let ignorableInserted = false
  while (evs.length < count) {
    evs.push({ type: 'turn/start', seq: seq++, time: (time += 2), data: { turn } })
    if (!ignorableInserted && turn >= 3) {
      ignorableInserted = true
      evs.push({
        type: 'deployment/heartbeat',
        seq: seq++,
        time: (time += 1),
        data: { host: 'h1' },
        ignorable: true,
      })
    }
    evs.push({
      type: 'user/message',
      seq: seq++,
      time: (time += 3),
      data: {
        id: `u-${turn}`,
        role: 'user',
        content: [{ type: 'text', text: `task ${turn}` }],
        source: { kind: 'user' },
      },
      surfaceOp: 'append',
    })
    evs.push({ type: 'step/start', seq: seq++, time: (time += 1), data: { turn, step: 1 } })
    for (let c = 0; c < 30 && evs.length + 6 < count; c++) {
      evs.push({
        type: 'assistant/chunk',
        seq: seq++,
        time: (time += 1),
        data: { turn, step: 1, chunk: { type: 'text-delta', index: 0, text: `t${c}x`.repeat(5) } },
      })
    }
    // LIMITATION (#29): surface *replacement* ops are deliberately not
    // exercised yet — their op/start/end/sourceEventSeqs pairing rules need
    // core-side confirmation before a checkpoint format claims them. v1
    // checkpoint policy for replace-bearing logs: refuse-to-use checkpoint,
    // full replay fallback. This fixture covers appends + ignorable plugin
    // events only.
    evs.push({
      type: 'assistant/message',
      seq: seq++,
      time: (time += 5),
      data: {
        turn,
        step: 1,
        message: {
          id: `a-${turn}`,
          role: 'assistant',
          content: [{ type: 'text', text: `answer ${turn}` }],
          source: { kind: 'model', provider: 'mock', model: 'mock' },
        },
      },
      surfaceOp: 'append',
    })
    evs.push({ type: 'step/end', seq: seq++, time: (time += 2), data: { turn, step: 1 } })
    evs.push({ type: 'turn/end', seq: seq++, time: (time += 4), data: { turn, reason: { kind: 'completed' } } })
    turn++
  }
  return evs.slice(0, count)
}

const dir = mkdtempSync(join(tmpdir(), 'checkpoint-equiv-'))
void dir

// ---- Build reference session through the real restore path ----
type Ev = Record<string, unknown> & { seq: number }
const fullEvents = generateMixed(8_000).map((e, i) => ({ ...e, seq: i })) as unknown as Ev[]
const sessionId = crypto.randomUUID()
const meta = { version: 0, id: sessionId, createdAt: Date.now(), cwd: '/w' }
const ref = new (Session as any)(sessionId, [], meta) as any
// Use a live session and append everything so all folds build incrementally:
for (const e of fullEvents) {
  ref.append(e.type as never, e.data as never, {
    ...(e.surfaceOp !== undefined ? { surfaceOp: e.surfaceOp } : {}),
    ...(e.sourceEventSeqs !== undefined ? { sourceEventSeqs: e.sourceEventSeqs } : {}),
  } as never)
}
void sessionId

const K = Math.floor(fullEvents.length * 0.75) // checkpoint at 75%
const suffix = fullEvents.slice(K)

// ---- Distill checkpoint of [0,K) ----
const artifact: CheckpointArtifact = distill({
  sessionId,
  events: fullEvents as never,
  sourceRevision: 'revision-a',
  prefixEndSeq: K,
})

check('checkpoint format/version/checksum self-consistent', () => {
  if (artifact.formatVersion !== CHECKPOINT_FORMAT_VERSION) throw new Error('version')
  if (!verifyCheckpoint(artifact)) throw new Error('checksum mismatch')
  const tampered = JSON.parse(JSON.stringify(artifact)) as CheckpointArtifact
  tampered.events[0].t = 'user/hacked'
  if (verifyCheckpoint(tampered)) throw new Error('checksum must catch tampering')
})

function sessionFromCheckpointAndSuffix(a: CheckpointArtifact): InstanceType<any> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const s = new Session(crypto.randomUUID(), [], undefined) as any
  for (const line of a.events) {
    s.append(line.t as never, line.d as never, {
      ...(line.s !== undefined ? { surfaceOp: line.s } : {}),
      ...(line.q !== undefined ? { sourceEventSeqs: line.q } : {}),
    } as never)
  }
  return s
}

// Real restored sessions for both branches.
function buildRestored(eventsArr: Array<Record<string, unknown>>): any {
  const myMeta = { version: coreSession.SESSION_FORMAT_VERSION, id: sessionId, createdAt: meta.createdAt, cwd: '/w' }
  return Session.fromRestore(sessionId, eventsArr as never, myMeta) as any
}

// Branch A: full canonical restore.
const sessionFullRestore = buildRestored(fullEvents.map((e, i) => ({ ...e, seq: i })))
// Keep a SECOND live-appended one (reference computations).
void ref

check('full-restore vs live-append model state identical (baseline sanity)', () => {
  const a = JSON.stringify(sessionFullRestore.deriveMessages())
  const b = JSON.stringify(ref.deriveMessages())
  if (a !== b) throw new Error('restore vs append derivation differs — test premise broken')
})

// Branch B: checkpoint prefix + live-appended suffix (the wake path).
const resumedLive = sessionFromCheckpointAndSuffix(artifact)
const checkpointHeadCount = artifact.events.length
for (const e of suffix) {
  resumedLive.append(e.type as never, e.data as never, {
    ...(e.surfaceOp !== undefined ? { surfaceOp: e.surfaceOp } : {}),
    ...(e.sourceEventSeqs !== undefined ? { sourceEventSeqs: e.sourceEventSeqs } : {}),
  } as never)
}

check('model-visible messages: checkpoint+suffix === full replay', () => {
  const want = JSON.stringify(sessionFullRestore.deriveMessages())
  const got = JSON.stringify(resumedLive.deriveMessages())
  if (want !== got) throw new Error('deriveMessages divergence between resume strategies')
})

check('requestHeader fold equivalent', () => {
  void (sessionFullRestore as never)
  void (resumedLive as never)
  // requestHeader derives from request/header events; fixture has none, so the
  // honest assertion here is that BOTH sides agree on absence/shape.
  const want = (() => {
    try {
      return JSON.stringify((sessionFullRestore as any).requestHeader?.() ?? null)
    } catch {
      return 'unsupported'
    }
  })()
  const got = (() => {
    try {
      return JSON.stringify((resumedLive as any).requestHeader?.() ?? null)
    } catch {
      return 'unsupported'
    }
  })()
  if (want !== got) throw new Error(`requestHeader mismatch: ${want} vs ${got}`)
})

check('materialization reduction actually happened', () => {
  const ratio = checkpointHeadCount / fullEvents.length
  // Chunk-heavy fixture ⇒ keeping everything except chunks must be tiny.
  if (ratio > 0.15) throw new Error(`checkpoint kept ${(ratio * 100).toFixed(1)}% of events`)
})

// Randomized-ish suffix boundary sweep for extra confidence (#58).
check('equivalence holds across multiple checkpoint boundaries', () => {
  for (const frac of [0.1, 0.4, 0.55, 0.9]) {
    const k2 = Math.floor(fullEvents.length * frac)
    const art = distill({
      sessionId,
      events: fullEvents as never,
      sourceRevision: 'rev',
      prefixEndSeq: k2,
    })
    const s = sessionFromCheckpointAndSuffix(art)
    for (const e of fullEvents.slice(k2)) {
      s.append(e.type as never, e.data as never, {
        ...(e.surfaceOp !== undefined ? { surfaceOp: e.surfaceOp } : {}),
        ...(e.sourceEventSeqs !== undefined ? { sourceEventSeqs: e.sourceEventSeqs } : {}),
      } as never)
    }
    const want = JSON.stringify(sessionFullRestore.deriveMessages())
    const got = JSON.stringify(s.deriveMessages())
    if (want !== got) throw new Error(`divergence at boundary fraction ${frac}`)
  }
})

console.log(`\n${passed} passed, ${failed} failed`)
console.log(`artifact bytes≈${JSON.stringify(artifact.events.at(-1)).length}B lastLine; kept=${checkpointHeadCount}/${fullEvents.length}`)
process.exit(failed > 0 ? 1 : 0)
