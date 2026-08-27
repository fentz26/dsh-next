/**
 * Resume-mode measurement (milestone task #5):
 *
 *   legacy           full loadStored + Session.fromRestore(all events)
 *   checkpoint       readPrefixFiltered(exclude assistant/chunk) → dense
 *                    rebase → fromRestore(prefix) → live-append suffix pages
 *   hot              same as checkpoint with pre-deserialized prefix lines
 *
 * time-to-agent-ready := until a REAL Session exists whose header/messages/
 * requestHeader/turn-balance are available (deriveMessages called once).
 */
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createRequire } from 'node:module'
import { pathToFileURL } from 'node:url'
import { cpuBaseline, fmt, snapshotResources } from './harness/metrics.ts'
import { openSqliteSource } from '../../packages/paged-history/src/index.ts'

const require_ = createRequire(import.meta.url)
const dshRoot = process.env.DSH_ROOT
if (dshRoot === undefined) {
  console.error('SKIP: set DSH_ROOT')
  process.exit(0)
}
const { SqliteStore } = require_(pathToFileURL(`${dshRoot}/packages/session/session-persistence-sqlite/src/store.ts`).href) as {
  SqliteStore: new (o: unknown) => {
    open(): Promise<void>
    appendBatch(m: unknown, e: unknown[], f: boolean): Promise<void>
    loadStored(id: string): Promise<{ events?: Array<Record<string, unknown>> } | undefined>
    close(): Promise<void>
  }
}
// Uses the focused DSH branch (/tmp/dsh-lazy-seam, dsh-next/lazy-session-seam)
// when available via DSH_LAZY_CORE; falls back to DSH checkout otherwise.
const corePath =
  process.env.DSH_LAZY_CORE ??
  `${dshRoot}/packages/core/session/src/index.ts`
const coreSession = await import(pathToFileURL(corePath).href) as never as {
  Session: any
  SESSION_FORMAT_VERSION: number
}
const hasWindow = typeof coreSession.Session.fromRestoreWindow === 'function'
const { Session } = coreSession

// Fixture: mixed-with-sparse-messages — most volume is packed chunk runs,
// message pairs appear once per block so checkpoints carry real model state.
function generateGiant(totalTarget: number): Array<Record<string, unknown>> {
  const evs: Array<Record<string, unknown>> = []
  let seq = 0
  let time = Date.now() - 60_000
  let turn = 1
  while (evs.length < totalTarget) {
    evs.push({ type: 'turn/start', seq: seq++, time: (time += 2), data: { turn } })
    evs.push({
      type: 'user/message',
      seq: seq++,
      time: (time += 3),
      data: {
        id: `u-${turn}`,
        role: 'user',
        content: [{ type: 'text', text: `objective ${turn}` }],
        source: { kind: 'user' },
      },
      surfaceOp: 'append',
    })
    evs.push({ type: 'step/start', seq: seq++, time: (time += 1), data: { turn, step: 1 } })
    const remaining = totalTarget - evs.length - 6
    const chunkRun = Math.min(50_000, Math.max(40, remaining))
    for (let c = 0; c < chunkRun; c++) {
      evs.push({
        type: 'assistant/chunk',
        seq: seq++,
        time: (time += 1),
        data: { turn, step: 1, chunk: { type: 'text-delta', index: 0, text: `t${c % 1000}:` } },
      })
    }
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
  return evs.slice(0, totalTarget)
}

const rssMiB = (): number => process.memoryUsage().rss / (1024 * 1024)

export async function resumeModesBench(): Promise<void> {
  const target = Number(process.env.BENCH_RESUME_EVENTS ?? 1_000_000)
  const dir = mkdtempSync(join(tmpdir(), 'resume-modes-'))
  try {
    console.log(`# building fixture (~${fmt(target, 0)} logical events, sparse messages)…`)
    const dbPath = join(dir, 'giant.sqlite')
    const builder = new SqliteStore({ path: dbPath, journalMode: 'wal', busyTimeoutMs: 5000 })
    await builder.open()
    const meta = { version: coreSession.SESSION_FORMAT_VERSION, id: crypto.randomUUID(), createdAt: Date.now(), cwd: '/w' }
    const allEvents = generateGiant(target).map((e, i) => ({ ...e, seq: i }))
    for (let i = 0; i < allEvents.length; i += 2000) {
      await builder.appendBatch(meta, allEvents.slice(i, i + 2000), false)
    }

    const report = (
      mode: string,
      t0: bigint,
      cpu0: NodeJS.CpuUsage,
      rss0: number,
      materialized: number,
      extra = '',
    ): void => {
      const snap = snapshotResources(t0, cpu0)
      console.log(
        [
          `resume-${mode}`,
          `events_materialized=${fmt(materialized, 0)}`,
          `wall_to_agent_ready=${fmt(snap.wallMs)}ms`,
          `cpu_main=${fmt(snap.cpuUserMs + snap.cpuSystemMs)}ms`,
          `rssDelta=+${fmt(rssMiB() - rss0, 1)}MiB`,
          extra,
        ].join(' | '),
      )
    }

    // ---- Mode 1: legacy full resume ----
    {
      global.gc?.()
      const t0 = process.hrtime.bigint()
      const c0 = cpuBaseline()
      const r0 = rssMiB()
      const loaded = await builder.loadStored(meta.id)
      const events = loaded?.events ?? []
      const sess = new Session(crypto.randomUUID(), [], undefined) as any
      void sess
      // canonical restore path:
      const restored = Session.fromRestore(
        meta.id,
        events.map((e) => ({ ...e })),
        meta,
      ) as any
      void restored.deriveMessages()
      report('legacy-full-restore', t0, c0, r0, restored.events.length ?? events.length)
    }

    const src = await openSqliteSource(dbPath)

    // ---- Mode 2: checkpoint (chunk-excluded prefix) + suffix ----
    {
      global.gc?.()
      const info = await src.meta(meta.id)
      const tailFrom = Math.max(0, (info?.length ?? 0) - 4000)
      const t0 = process.hrtime.bigint()
      const c0 = cpuBaseline()
      const r0 = rssMiB()

      const prefixPage = await src.readPrefixFiltered(meta.id, {
        upToSeq: tailFrom,
        excludeTypes: ['assistant/chunk'],
      })
      const suffixPage = await src.readSuffix(meta.id, 4000)

      // Dense rebase naturally handled by Session.append (log.length contract).
      const sess = new (Session as any)(crypto.randomUUID(), [], undefined) as any
      let prevDropped = 0
      for (const ev of prefixPage.events) {
        if ((ev as { type?: string }).type === 'turn/start') prevDropped++
        void prevDropped
        sess.append((ev.type as string) as never, (ev.data as unknown) as never, {
          ...(ev.surfaceOp !== undefined ? { surfaceOp: ev.surfaceOp } : {}),
          ...(ev.sourceEventSeqs !== undefined ? { sourceEventSeqs: ev.sourceEventSeqs } : {}),
        } as never)
      }
      for (const ev of suffixPage.events) {
        sess.append((ev.type as string) as never, (ev.data as unknown) as never, {
          ...(ev.surfaceOp !== undefined ? { surfaceOp: ev.surfaceOp } : {}),
          ...(ev.sourceEventSeqs !== undefined ? { sourceEventSeqs: ev.sourceEventSeqs } : {}),
        } as never)
      }
      void sess.deriveMessages()
      report(
        'checkpoint-suffix',
        t0,
        c0,
        r0,
        prefixPage.events.length + suffixPage.events.length,
        `logical_events_in_log=${fmt(info?.length ?? 0, 0)} payloadKB=${fmt(
          (prefixPage.approxPayloadBytes + suffixPage.approxPayloadBytes) / 1024,
          1,
        )} physicalRowsTouched=${prefixPage.inspectedCount > 0 ? prefixPage.events.length : 0}`,
      )
    }

    // ---- Mode 2b: LAZY WINDOW (canonical seqs; recent window only) ----
    if (hasWindow && process.env.DSH_LAZY_CORE !== undefined) {
      global.gc?.()
      const t0 = process.hrtime.bigint()
      const c0 = cpuBaseline()
      const r0 = rssMiB()

      const info = await src.meta(meta.id)
      const page = await src.readSuffix(meta.id, 4000)
      // Keep original canonical seqs; baseSeq anchors the boundary.
      const baseSeq = Number((page.events[0] as { seq: number }).seq)
      const sess = Session.fromRestoreWindow(
        meta.id,
        { events: page.events.map((e) => ({ ...e })), baseSeq, totalLength: info?.length },
        meta,
      ) as any
      void sess.deriveMessages()
      report(
        'lazy-window',
        t0,
        c0,
        r0,
        page.events.length,
        `canonicalSeqStart=${baseSeq} endOfLog=${page.endOfLogAt !== undefined}`,
      )
    }

    // ---- Mode 3: hot cache (pre-deserialized prefix + suffix held in memory) ----
    {
      const warmPrefix = await src.readPrefixFiltered(meta.id, {
        upToSeq: Math.max(0, (await src.meta(meta.id))!.length - 4000),
        excludeTypes: ['assistant/chunk'],
      })
      const warmSuffix = await src.readSuffix(meta.id, 4000)
      global.gc?.()
      const t0 = process.hrtime.bigint()
      const c0 = cpuBaseline()
      const r0 = rssMiB()

      const sess = new (Session as any)(crypto.randomUUID(), [], undefined) as any
      for (const ev of [...warmPrefix.events, ...warmSuffix.events]) {
        sess.append((ev.type as string) as never, (ev.data as unknown) as never, {
          ...(ev.surfaceOp !== undefined ? { surfaceOp: ev.surfaceOp } : {}),
          ...(ev.sourceEventSeqs !== undefined ? { sourceEventSeqs: ev.sourceEventSeqs } : {}),
        } as never)
      }
      void sess.deriveMessages()
      report('hot-cache', t0, c0, r0, warmPrefix.events.length + warmSuffix.events.length)
    }

    void src.close()
    await builder.close().catch(() => {})
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
}
