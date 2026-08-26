# Future persistence provider design (design note — not implemented)

This records how a `ctx.sessionPersistence` replacement would work behind the
existing seam, so a later phase can proceed without re-auditing. Nothing here
changes DSH storage identity or Session semantics.

## Replacement mechanics

* Package registers as a Cordis plugin claiming `sessionPersistence`, exactly
  like `SqliteSessionPersistence` (`sqlite/src/index.ts:52`).
* Replaced in userland via profile patch row addressing: swap the
  `session-persistence-sqlite` row for the dsh-next provider row. Row ids come
  from the audited bundle files, never invented.
* Backend implements the coordinator's `PersistenceBackend<TornMarker>` hooks:
  `loadStored`, `readStoredRevision`, `appendBatch(meta, events, isMaterialized)`
  (materialize + first batch ATOMIC), optional `loadStoredFrom`,
  `commitRepair`, `list`, `locate`, `close`. The coordinator already provides
  contiguity, repair orchestration, revision revalidation, format refusal, and
  write-behind batching — none of that moves into native code.

## Format compatibility decision

**Option A (preferred): read/write the exact existing SQLite schema v17**
including `DSHP` application id, packed chunk rows, varint/zigzag
`sourceEventSeqs`, and zstd framing rules. Rationale:

* interoperable with stock DSH (sessions survive switching providers)
* reuse of the differential spec/tests that already exist upstream
* no risk of silently creating a log that future DSH refuses.

A new format is only acceptable as an ADDITIONAL provider under a distinct
store identity; it must never masquerade as current SQLite.

## Contracts to preserve verbatim

Append-only, contiguous seq, exact `SessionEvent` JSON semantics, header row,
revision tokens per store identity, inspect (non-mutating synthetic closers),
prepare/load crash repair with torn-tail truncation only below the last
decodable turn/end, `readFrom` suffix-only reads, format refusal direction-aware
messages, ignorable unknown events, durability resolution semantics ("resolves
only after durable"), pre-mutation schema recheck.

## Where native could help — only if measured again

The measured deficits of the current backend are concurrency serialization and
large synchronous cold loads, not raw append throughput. The candidate
architecture is therefore not "Rust rewrite of SQLite" but **worker-owned
connection**: move `DatabaseSync` into a worker thread owned by the provider;
TS first. rusqlite in Rust becomes interesting only if worker-TS fails on
packaging/latency grounds; then napi async workers with a bundled SQLite
(licensing check required) sit behind the same backend interface. Decision
gates mirror baseline-results.md §6.
