/**
 * Synthetic SessionEvent generators matching audited DSH shapes closely
 * enough to exercise the real store/codec/packing paths.
 */

export type Event = Record<string, unknown>

const CHUNK_BYTES = 48

export interface EventSpec {
  turnCount: number
  chunksPerStep: number
}

/**
 * A realistic mixed log: request header, turns with steps, user messages,
 * assistant chunk streams (packing-eligible), assistant messages,
 * tool calls/results, todos, and turn ends.
 */
export function generateMixedEvents(count: number): Event[] {
  const events: Event[] = []
  let seq = 0
  let time = Date.now() - 3_600_000
  let turn = 0
  while (events.length < count) {
    events.push({
      type: 'turn/start',
      seq: seq++,
      time: (time += 2),
      data: { turn },
    })
    events.push({
      type: 'user/message',
      seq: seq++,
      time: (time += 3),
      data: { text: `Investigate subsystem ${turn} and report findings.` },
    })
    for (let step = 0; step < 2 && events.length < count; step++) {
      events.push({
        type: 'step/start',
        seq: seq++,
        time: (time += 1),
        data: { turn, step },
      })
      // assistant/chunk stream — the packing target
      const chunkText = 'x'.repeat(CHUNK_BYTES - 12)
      for (
        let c = 0;
        c < 40 && events.length + 3 < count;
        c++
      ) {
        events.push({
          type: 'assistant/chunk',
          seq: seq++,
          time: (time += 1),
          data: { turn, step, chunk: { type: 'text-delta', index: 0, text: chunkText } },
        })
      }
      events.push({
        type: 'assistant/message',
        seq: seq++,
        time: (time += 5),
        data: { text: `Analysis of subsystem ${turn}...`, turn, step },
        sourceEventSeqs: undefined,
        surfaceOp: 'append',
      })
      events.push({
        type: 'tool/call',
        seq: seq++,
        time: (time += 2),
        data: {
          callId: `call-${turn}-${step}`,
          name: 'bash',
          arguments: JSON.stringify({ command: 'rg TODO .', timeoutMs: 30000 }),
          turn,
          step,
        },
      })
      events.push({
        type: 'tool/result',
        seq: seq++,
        time: (time += 800),
        data: {
          callId: `call-${turn}-${step}`,
          output: `done ${turn}/${step}: fixed.\n`.repeat(6),
          isError: false,
        },
      })
      events.push({
        type: 'step/end',
        seq: seq++,
        time: (time += 2),
        data: { turn, step },
      })
    }
    if (events.length >= count) break
    events.push({
      type: 'turn/end',
      seq: seq++,
      time: (time += 10),
      data: { turn, reason: 'end-turn' },
    })
    turn++
  }
  return events.slice(0, count)
}

/**
 * Chunk-heavy model stream: N bytes of logical streaming followed by a final
 * assistant/message. Each event is ~64 B of delta payload.
 */
export function generateChunkStream(totalBytes: number): Event[] {
  const events: Event[] = []
  let seq = 0
  let time = Date.now() - 60_000
  events.push({ type: 'turn/start', seq: seq++, time: (time += 2), data: { turn: 0 } })
  const bytesPerEvent = CHUNK_BYTES
  const nEvents = Math.floor(totalBytes / bytesPerEvent)
  for (let i = 0; i < nEvents; i++) {
    events.push({
      type: 'assistant/chunk',
      seq: seq++,
      time: (time += 1),
      data: {
        turn: 0,
        step: 0,
        chunk: {
          type: 'text-delta',
          index: 0,
          text: 'q'.repeat(bytesPerEvent - 8),
        },
      },
    })
  }
  events.push({
    type: 'assistant/message',
    seq: seq++,
    time: (time += 4),
    data: { text: '[stream complete]', turn: 0, step: 0 },
    surfaceOp: 'append',
  })
  events.push({ type: 'turn/end', seq: seq++, time: (time += 1), data: { turn: 0, reason: 'end-turn' } })
  return events
}
