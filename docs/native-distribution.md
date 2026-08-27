# Native distribution plan (spec #49/#50)

Adopts DSH's `native/landlock-run` precedent combined with napi-rs's platform
package model. Prebuilt-only; **no install-time Rust compilation ever**.

## Package layout (when distribution begins)

```text
@dsh-next/native                entry package: capability probe + graceful
                                fallback wiring; zero optional deps if all
                                native features are disabled by config
@dsh-next/native-journal-darwin-arm64   .node payloads, os/cpu gated
-darwin-x64
-linux-x64-gnu
-linux-arm64-gnu
-win32-x64-msvc
```

* Entry lists platform packages under npm `optionalDependencies`; each declares
  `os`/`cpu`. Missing platforms are simply not installed.
* Payload naming follows napi-rs conventions but the platform wrappers are plain
  shims re-exporting `index.js` from the built crate output (kept identical to
  the verified pilot contract: `NativeByteJournal`, `probe()`).
* Verification gates mirror landlock-run: no lifecycle scripts in published
  tarballs, artifact size/mode checks, CI matrix generated from checked-in
  metadata.

## ABI/version handshake (#50)

At load time the entry package asserts:

```
native.probe() returns { ok, nativeApiVersion, features: ['journal'...] }
```

On mismatch: native stays disabled, fallback implementation selected,
diagnostics surface reason. No crashes, ever.

## Status

Not built (distribution begins only when a consumer needs a packaged binary).
Current state: pilot crate builds as cdylib; `pilot.node` used directly by
tests/benches on darwin-arm64.
