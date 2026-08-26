# Native journal pilot — design, tests, verdict

## Scope

The Phase 0 pilot is deliberately minimal: a bounded byte journal primitive,
not a subprocess provider. It mirrors the audited semantics of DSH's
`OutputCollector` (packages/subprocess/subprocess-local/src/spawn.ts:104-251)
at byte level:

* append-only stream, absolute whole-stream offsets
* bounded tail (`maxBytes`), head-trimmed byte-exactly
* `readFrom(offset)` → retained suffix; non-consuming; independent readers
* `lossy` flag when the offset slid below the window start

## Implementations under test

1. **ReferenceByteJournal** (`packages/journal/src/index.ts`) — faithful TS
   re-implementation of the current DSH strategy (Buffer[] + concat-on-read).
   Also serves as the guaranteed-available fallback.
2. **SegmentedByteJournal** (same file) — optimized TS: offset-indexed
   segments plus one amortizing tail scratch; reads touch only intersecting
   segments and return zero-copy views.
3. **NativeByteJournal** (`crates/native-journal/src/lib.rs`) — Rust napi-rs,
   segment deque of owned Vec<u8>, same trimming/offset semantics.

## Binding strategy

napi-rs 2.x, synchronous class methods only. No async runtime is pulled in —
there is no workload here that justifies Tokio or a thread pool. The crate
builds as a cdylib with macOS `-undefined dynamic_lookup` linking, loaded via
`require('pilot.node')`. Future packaging follows the landlock-run model:
platform optionalDependencies, prebuilt artifacts only, runtime probe.

## Memory ownership table

| Boundary | Owner | Copy count |
|---|---|---|
| `append(Buffer)` input | JS → copied into Rust-owned `Vec<u8>` at crossing; JS Buffer untouched afterwards | 1 |
| `appendBatch(Vec<Buffer>)` input | same, per buffer, one FFI crossing total | 1 per buffer |
| `read_from().data` output | fresh Rust `Vec<u8>` assembled from segments → moved into a JS-backed Buffer; journal retains its own copy (reads are non-consuming) | 1 out |
| Evicted bytes | freed by Rust when the owning segment drains | — |

TS segmented journal avoids the read copy entirely by returning subarray views;
that asymmetry is why it wins pure-read benchmarks despite FFI being cheap.

## Error semantics

Rust maps invalid constructor arguments and offsets to typed `napi::Error`
(`InvalidArg`); no panic path exists on ordinary bad input — bad ranges are
lossy reads by definition. Panics across FFI are treated as unacceptable for
harness stability and none of the pilot's code can panic on user input.

## Correctness tests

`packages/journal/tests/journal.test.ts` and
`crates/native-journal/__test__/differential.test.ts` cover:

* round trip below cap; bounded tail equals the true stream suffix byte-for-byte
* oversized single chunk trims exactly to cap; exact-cap boundary stays lossless
* split multi-byte UTF-8 remains byte-exact (text conversion policy documented)
* independent readers do not consume; EOF reads return empty
* **randomized differential**: identical PRNG chunk sequences pushed into all
  three implementations with randomized `maxBytes` per trial; at every step,
  windowStart / nextOffset / lossy / retained bytes must match the reference

Result: all green for {reference ↔ segmented} and {reference ↔ native}.
A real-input conformance run against DSH's actual OutputCollector class was
done through the bench harness feeding both the same pushes (bench asserts
equal `nextOffset`; full differential text comparisons are covered by the
strategy-mirror above because ReferenceByteJournal replicates its code paths).

## Verdict (evidence in baseline-results.md)

* Optimized TS segmented journal: ~500–700× faster reads than current DSH
  collector; beats native on pure reads (zero-copy views vs FFI copy-out).
* Native wins write-heavy small-chunk mixes (~8× append throughput at 1 KB)
  and provides predictable worst-case latency without GC churn.

Decision: keep both; default to TS segmented, expose native behind probe()
for write-dominated workloads. Do not expand native scope further without new
measurements.
