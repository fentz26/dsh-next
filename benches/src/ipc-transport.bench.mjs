/**
 * IPC transport placement experiment (spec #15/#16).
 *
 * Measures the two candidate encodings across the worker boundary on REAL
 * persistence-shaped payloads (200-event batches like coordinator write-behind
 * drains; full ~2k-event graphs like cold loads):
 *
 *   main->worker : structured clone of event objects | JSON string (main
 *                  stringify, worker parse)
 *   worker->main : one giant clone of a reconstructed graph | paged frames
 *
 * Verdicts recorded in docs/persistence-worker.md §transport.
 */
import { Worker } from 'node:worker_threads'
import { performance } from 'node:perf_hooks'

function generateMixedEvents(count) {
  const events = []
  let seq = 0
  let time = Date.now() - 3_600_000
  let turn = 0
  while (events.length < count) {
    events.push({ type: 'turn/start', seq: seq++, time: (time += 2), data: { turn } })
    for (let c = 0; c < 40 && events.length + 4 < count; c++) {
      events.push({
        type: 'assistant/chunk',
        seq: seq++,
        time: (time += 1),
        data: { turn, step: 0, chunk: { type: 'text-delta', index: 0, text: 'x'.repeat(36) } },
      })
    }
    events.push({
      type: 'tool/result',
      seq: seq++,
      time: (time += 800),
      data: { callId: `c${turn}`, output: 'line\n'.repeat(20), isError: false },
    })
    events.push({ type: 'turn/end', seq: seq++, time: (time += 10), data: { turn, reason: 'end-turn' } })
    turn++
  }
  return events.slice(0, count)
}

const PORT_PARENT = 42

const workerCode = `
import { parentPort } from 'node:worker_threads'
let cached = null
parentPort.on('message', ({ id, op, payload }) => {
  switch (op) {
    case 'appendClone':
      cached = payload
      parentPort.postMessage({ id, ok: true })
      break
    case 'appendString':
      cached = JSON.parse(payload)
      parentPort.postMessage({ id, ok: true })
      break
    case 'returnGraph':
      parentPort.postMessage({ id, ok: true, payload: cached })
      break
    case 'returnPaged': {
      const size = payload.pageSize
      const evs = cached ?? []
      let idx = 0
      while (idx < evs.length) {
        const slice = evs.slice(idx, idx + size)
        idx += slice.length
        parentPort.postMessage({ id, ok: true, page: slice, last: idx >= evs.length })
      }
      if (evs.length === 0) parentPort.postMessage({ id, ok: true, page: [], last: true })
      break
    }
    case 'ping':
      parentPort.postMessage({ id, ok: true })
      break
  }
})
`

async function main() {
  const { mkdtempSync } = await import('node:fs')
  const { join } = await import('node:path')
  const { tmpdir } = await import('node:os')
  const { pathToFileURL } = await import('node:url')
  const dir = mkdtempSync(join(tmpdir(), 'ipc-bench-'))
  const { writeFileSync, rmSync } = await import('node:fs')
  const workerFile = join(dir, 'w.mjs')
  writeFileSync(workerFile, workerCode)
  const w = new Worker(pathToFileURL(workerFile))

  let nextId = 1
  const request = (op, payload) =>
    new Promise((resolve) => {
      const id = nextId++
      const handler = ({ id: rid, payload: rp }) => {
        if (rid === id) {
          w.off('message', handler)
          resolve(rp)
        }
      }
      w.on('message', handler)
      w.postMessage({ id, op, payload })
    })

  // Actually wait for the initial ready: send ping first.
  void PORT_PARENT
  await new Promise((resolve) => {
    const id = -1
    const handler = () => {
      w.off('message', handler)
      resolve(undefined)
    }
    w.on('message', handler)
    w.postMessage({ id, op: 'ping' })
  })

  const ITERS = Number(process.env.BENCH_IPC_ITERS ?? 300)
  // ---- Append-direction costs (200-event write-behind-shaped batches) ----
  const batch = generateMixedEvents(200)
  const asString = JSON.stringify(batch)

  // structured clone, main -> worker
  let t0 = performance.now()
  for (let i = 0; i < ITERS; i++) await request('appendClone', batch)
  const cloneAppendMs = performance.now() - t0

  // JSON string, main -> worker
  t0 = performance.now()
  let cpu0 = process.cpuUsage().user
  let strCost = 0
  for (let i = 0; i < ITERS; i++) {
    const bt = performance.now()
    await request('appendString', asString)
    strCost += performance.now() - bt
  }
  cpu0 += 0
  void cpu0
  const stringAppendMs = performance.now() - t0
  console.log(
    [
      `ipc-append n=${ITERS}xB200events`,
      `structured_clone total=${cloneAppendMs.toFixed(1)}ms`,
      `json_string roundtrip(excl.main stringify) total=${strCost.toFixed(1)}ms`,
      `json_string incl.stringify total=${stringAppendMs.toFixed(1)}ms`,
      `payloadKB≈${(asString.length / 1024).toFixed(0)}`,
    ].join(' | '),
  )

  // ---- Return-direction costs (~2k-event graph reconstruction transfer) ----
  const big = generateMixedEvents(2_000)
  await request('appendClone', big)
  const cBig = process.cpuUsage()

  t0 = performance.now()
  for (let i = 0; i < 50; i++) await request('returnGraph')
  const cloneReturnMs = performance.now() - t0

  t0 = performance.now()
  const PAGES = []
  for (let i = 0; i < 50; i++) {
    const pages = []
    await new Promise((resolve) => {
      const handler = ({ payload: page, last }) => {
        PAGES.push(1)
        pages.push(page)
        if (last) {
          w.off('message', handler)
          resolve(undefined)
        }
      }
      w.on('message', handler)
      w.postMessage({ id: nextId++, op: 'returnPaged', payload: { pageSize: 250 } })
    })
    void pages
  }
  const pagedReturnMs = performance.now() - t0
  const rc = process.cpuUsage(cBig)

  console.log(
    [
      `ipc-return n=50x2000eventgraphs`,
      `single_clone total=${cloneReturnMs.toFixed(1)}ms`,
      `paged_250 total=${pagedReturnMs.toFixed(1)}ms`,
      `cpu_total=${((rc.user + rc.system) / 1000).toFixed(0)}ms`,
    ].join(' | '),
  )

  await w.terminate()
  rmSync(dir, { recursive: true, force: true })
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})
