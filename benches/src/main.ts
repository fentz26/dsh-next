#!/usr/bin/env node
/**
 * dsh-next benchmark runner.
 *
 * Usage:
 *   DSH_ROOT=~/deepseek-harness pnpm bench [name...]
 *
 * Names: persistence-append cold-load compression collector journal
 */
import os from 'node:os'
import { dshRoot } from './harness/dsh.ts'

const RUNNERS: Record<string, () => Promise<void>> = {
  'persistence-append': async () => (await import('./persistence-append.bench.ts')).persistenceBench(),
  'cold-load': async () => (await import('./cold-load.bench.ts')).coldLoadBench(),
  compression: async () => (await import('./compression.bench.ts')).compressionBench(),
  collector: async () => (await import('./output-collector.bench.ts')).collectorBench(),
  journal: async () => (await import('./journal.bench.ts')).journalBench(),
}

function machineInfo(): string {
  const cpus = os.cpus()
  return [
    `cpu=${cpus[0]?.model ?? '?'} x${cpus.length}`,
    `node=${process.version}`,
    `platform=${os.platform()}${os.arch()}`,
    `dsh_root=${dshRoot()}`,
  ].join(', ')
}

async function main(): Promise<void> {
  const requested = process.argv.slice(2)
  const names = requested.length > 0 ? requested : Object.keys(RUNNERS)

  console.log(`# dsh-next benchmarks`)
  console.log(`# ${machineInfo()}`)
  console.log(`# trials=${process.env.BENCH_TRIALS ?? 3} (env: BENCH_TRIALS, BENCH_STREAM_BYTES, BENCH_JOURNAL_APPENDS, BENCH_COMPRESS_OPS)`)
  console.log()

  for (const name of names) {
    const runner = RUNNERS[name]
    if (!runner) {
      console.error(`unknown benchmark '${name}'. Available: ${Object.keys(RUNNERS).join(', ')}`)
      process.exitCode = 1
      continue
    }
    console.log(`## ${name}`)
    await runner()
    console.log()
  }
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
