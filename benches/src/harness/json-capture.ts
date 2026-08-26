/**
 * JSON capture for benchmark runs (Track H): intercepts console.log rows from
 * the runner and converts `key=value`-delimited lines into structured records
 * so results are machine-readable alongside human text.
 */
import { writeFileSync } from 'node:fs'

interface CaptureState {
  meta: Record<string, unknown>
  currentBenchmark: string
  records: Array<Record<string, unknown>>
  originalLog: typeof console.log
  installed: boolean
}

const state: CaptureState = {
  meta: {},
  currentBenchmark: '',
  records: [],
  originalLog: console.log.bind(console),
  installed: false,
}

export function startJsonCapture(meta: Record<string, unknown>): void {
  if (state.installed) return
  state.installed = true
  state.meta = meta
  state.records = []
  console.log = (...args: unknown[]): void => {
    const line = args.map(String).join(' ')
    if (line.startsWith('## ')) {
      state.currentBenchmark = line.slice(3).trim()
    } else if (line.includes('|')) {
      const record: Record<string, unknown> = {
        benchmark: state.currentBenchmark,
        timestamp: new Date().toISOString(),
        raw: line,
      }
      for (const pair of line.split(' | ')) {
        const idx = pair.indexOf('=')
        const key = idx === -1 ? 'message' : pair.slice(0, idx).trim()
        const rawValue = idx === -1 ? pair : pair.slice(idx + 1).trim()
        const numeric = Number(rawValue)
        record[key] = rawValue !== '' && !Number.isNaN(numeric) && /^[0-9.-]/.test(rawValue) ? numeric : rawValue
      }
      state.records.push(record)
    }
    state.originalLog(line)
  }
}

export function writeJsonCapture(outPath?: string): string | undefined {
  if (!state.installed) return undefined
  console.log = state.originalLog
  state.installed = false
  const path =
    outPath ?? `benches/results/latest-${new Date().toISOString().replace(/[:.]/g, '-')}.json`
  try {
    writeFileSync(path, JSON.stringify({ meta: state.meta, records: state.records }, null, 2))
    return path
  } catch {
    state.originalLog(`# warn: could not write JSON capture to ${path}`)
    return undefined
  }
}
