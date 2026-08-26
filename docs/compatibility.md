# Compatibility

## DeepSeek Harness versions

* Audited baseline: **0.1.1-rc.2** (commit `b150a551b8`). All seam references
  (service keys, PersistenceBackend hook shape, bundle row ids
  `session-persistence-jsonl` / `session-persistence-sqlite`) are pinned to
  that tree.
* Feature matrix: `ctx.sessionPersistence` + coordinator write-behind +
  revision tokens (0.1.1-rc.2 ✓). dsh-next refuses unknown backend-hook drift
  via its own protocol/shape checks rather than assuming semver compatibility;
  the version floor is verified empirically per release, not inferred from
  package ranges.

## Node

* Node >= 22 required (`node:sqlite` DatabaseSync; Phase 0 baseline ran v22.23.1).
  Workers with tsx execArgv are dev/bench-only concerns; packaged builds must
  run without source transpilation.

## Platforms

| platform | journal TS | persistence worker | native pilot |
|---|---|---|---|
| darwin-arm64 | ✓ (verified) | ✓ (verified) | ✓ (built, experimental) |
| darwin-x64 | expected ✓ | untested | buildable |
| linux x64/arm64 | expected ✓ | untested | not built yet |
| win32-x64 | expected ✓ | untested | not built yet |

Only what is listed as verified is claimed.

## Graceful degradation matrix

| capability | if unavailable |
|---|---|
| native journal module | TS segmented journal (default anyway); diagnostics say so |
| worker support / init failure | provider enters failed state for NEW work + explicit diagnostic; callers may configure `persistence = stock` |
| DSH_ROOT missing (dev bench mode) | benches/tests SKIP with clear message; no crash |
| DSH seam drift detected | capability check disables affected provider path only; unrelated dsh-next features keep working |

## Packaging rules honored

No runtime dependency on developer paths: `DSH_ROOT` is used exclusively by
benchmarks/tests; shipped code receives compiled module paths explicitly
(clean-package gate pending until first packaged build exists — tracked).
