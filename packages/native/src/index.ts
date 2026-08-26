/**
 * Native module loader for dsh-next.
 *
 * Fallback policy (Phase 0): native acceleration is optional and explicitly
 * reported. When the module is absent we say so and callers use the stock /
 * TS implementations — we never silently claim native acceleration.
 */
import { createRequire } from 'node:module'
import { existsSync } from 'node:fs'
import { join } from 'node:path'

export interface NativeModuleInfo {
  available: boolean
  reason?: string
  /** Loaded napi module (journal pilot) when available. */
  journal?: {
    NativeByteJournal: new (maxBytesBytes: number) => {
      append(chunk: Uint8Array): number
      appendBatch(chunks: Uint8Array[]): number
      readFrom(offset: number): { data: Uint8Array; nextOffset: number; lossy: boolean }
      readonly nextOffset: number
      readonly windowStart: number
    }
    probe(): string
  }
}

const require_ = createRequire(import.meta.url)

const CANDIDATE_PATHS = [
  // Built pilot artifact during development / CI:
  '../../../crates/native-journal/pilot.node',
  // Future published layout (platform packages resolved here):
  '@dsh-next/native-journal',
]

export function loadNative(): NativeModuleInfo {
  const failures: string[] = []
  for (const candidate of CANDIDATE_PATHS.map((p) => resolveCandidate(p))) {
    if ('exists' in candidate && !candidate.exists) continue
    try {
      const mod = require_(candidate.path) as NativeModuleInfo['journal']
      const marker = typeof mod?.probe === 'function' ? mod.probe() : ''
      if (!marker.includes('ok')) {
        failures.push(`probe failed at ${candidate.path}`)
        continue
      }
      return { available: true, journal: mod }
    } catch (err) {
      failures.push(`${candidate.path}: ${(err as Error).message}`)
    }
  }
  return { available: false, reason: failures.join('; ') || 'no native candidates present' }
}

function resolveCandidate(
  p: string,
): { path: string; exists?: boolean } {
  if (p.includes('/')) {
    const abs = new URL(p, import.meta.url).pathname
    return { path: abs, exists: existsSync(abs) }
  }
  return { path: p }
}

/** Keyless diagnostics (CLI/log friendly). */
export function diagnosticReport(extra?: Record<string, string>): string {
  const nat = loadNative()
  const lines = [
    'dsh-next',
    '',
    `native module: ${nat.available ? 'available' : 'unavailable'}`,
    ...(nat.reason ? [`  reason: ${nat.reason}`] : []),
    `journal: ${nat.available ? 'native-capable (TS segmented default)' : 'typescript only'}`,
    `persistence: stock`,
    `subprocess: stock`,
  ]
  for (const [k, v] of Object.entries(extra ?? {})) lines.push(`${k}: ${v}`)
  return lines.join('\n')
}
