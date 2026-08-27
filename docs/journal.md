# Journal (Track A)

Status gate: **JOURNAL READY** — production-quality segmented TS primitive,
differential-tested, benchmarks recorded; native pilot retained as optional
experiment.

## Contract

```ts
interface ByteJournal {
  readonly maxBytes: number
  readonly nextOffset: number      // absolute whole-stream bytes appended
  readonly windowStart: number     // absolute offset of first retained byte
  append(data: Uint8Array): number // returns new nextOffset
  readFrom(offset: number): {
    data: Uint8Array               // retained suffix since offset (tail when lossy)
    nextOffset: number             // absolute stream end at read time
    lossy: boolean                 // true when offset < windowStart
  }
}
```

Implementations in `packages/journal`:

| name | strategy | measured (Phase 0, darwin-arm64) |
|---|---|---|
| `ReferenceByteJournal` | DSH OutputCollector strategy mirror (Buffer[] + concat-on-read) | ~252–303 µs/read |
| `SegmentedByteJournal` | offset-indexed segments + amortizing tail scratch | **~0.3–0.4 µs/read** |
| `NativeByteJournal` (@dsh-next/native pilot) | Rust napi-rs segment deque | ~0.8–0.9 µs/read |

Selection is via `selectJournal()`; default = `segmented-ts`. The native
module never defaults — it does not dominate any measured workload end-to-end
and diagnostics report it as experimental (`nativeBenchmarkRecommended:
false`). This honors the evidence rather than a "native = faster" assumption.

## Semantics rules

* **Bytes are authoritative.** Nothing stores strings internally.
* Bounded memory: window ≤ `maxBytes`; an incoming chunk may transiently
  exceed before head-trimming back exactly (mirrors DSH).
* Reads are non-consuming; any number of independent readers may hold cursors.
* `readFrom(offset)` with `offset < windowStart` returns the whole retained
  tail and sets `lossy` — the gap is unrecoverable by the journal itself.
* Out-of-range offset policy (PINNED, all implementations incl. native):
  offsets below the window start are lossy tail reads; future offsets
  (`> nextOffset`) return empty data with `lossy=false`; a fresh journal is
  always readable. (Upstream DSH leaves negatives effectively undefined via
  subarray semantics; dsh-next defines them explicitly and tests it.)
* Absolute whole-stream coordinates are stable across eviction.

## UTF-8 correctness policy

Text conversion happens at the presentation layer on byte slices using
`Buffer.toString('utf8')`. Consequences:

* A read boundary that splits a multi-byte sequence yields replacement
  characters at slice edges — identical to current DSH behavior because the
  reference implementation replicates its exact call pattern.
* The journal guarantees only byte fidelity: concatenating consecutive read
  slices reproduces the original stream byte-for-byte even where each
  individual text rendering would not reassemble cleanly.
* Consumers needing lossless text across boundaries must decode incrementally
  with their own stateful decoder (e.g. `StringDecoder`) fed from journal
  bytes.

## Memory architecture

Segments carry `{start: number; buffer: Uint8Array}` ordered by stream offset;
one partially-filled tail scratch (~64 KiB target) amortizes allocation for
small appends. Eviction trims/drops head segments only; tail scratch slides
its logical start via subarray (no copy). Window accounting uses an explicit
`windowStart`, making bounds byte-exact mid-segment. No path reconstructs the
full retained window except genuinely lossy reads returning the entire tail.

## Read optimization

Reads binary-search the first segment ending beyond the requested offset, then
touch only intersecting segments (zero-copy `subarray` views). For tail-follow
readers this is O(bytes-read). Phase 0 considered a rolling per-reader cursor
and judged the binary search sufficient (30k polls ≈ 13 ms); more complexity
was not added without further measurement.

## Integration audit

Audited consumers of "bounded retained output" inside DSH:

1. **Subprocess OutputCollector** (`subprocess-local/src/spawn.ts:104-251`)
   — byte-offset journal + text reads + optional spill files. Contract
   *aligns* with ByteJournal modulo spill support (not implemented in the
   journal; dsh-next providers can add spill separately).
2. **Jobs observer journal** (`jobs-local`: `RetainedOutputChunk[]`,
   `nextOutputSequence`, `observe(id, afterSequence, maxChunks)`) — record-
   sequence-addressed chunks with a separate model-facing cursor and per-job
   byte bounds. Contract does **not** align (chunk sequences ≠ byte offsets;
   ownership sits inside LocalJobRegistry, which stays TS). Not unified.

### Chosen route

**Option C**: the journal ships inside dsh-next and will back future dsh-next
providers (worker persistence tracing streams, supervisor output journals,
diagnostic capture). No upstream DSH change is required or currently made.
Option B (a tiny upstream utility extraction of OutputCollector's storage
strategy) remains documented as a possible later contribution if DSH maintainers
want the same fix stock; it was deliberately not pursued first because
provider-side integration delivers the same user-visible benefit out-of-tree.

## Tests

* `packages/journal/tests/journal.test.ts` — edge cases + randomized
  differential against the reference strategy (7 checks, all green).
* `crates/native-journal/__test__/differential.test.ts` — native-vs-reference
  differential (5 checks, all green when built).
