# Security Policy

## Supported versions

dsh-next is a pre-1.0 prototype project. Only the latest commit of `main` is
supported at any time.

| branch / ref | status |
| --- | --- |
| `main` HEAD | supported |
| older commits / tags | not supported |

`main` currently carries **experimental provider code** (worker-thread
persistence, paged-history reads, derived checkpoints). None of it is wired
into a production DSH composition by default; stock DeepSeek Harness providers
remain the defaults until that integration work lands.

## Reporting a vulnerability

Please report vulnerabilities **privately** — do not open public GitHub issues
for security problems.

* Email: **contact@fentz.dev**
* If needed for coordination, a maintainer can arrange an encrypted channel
  after first contact.
* Please include:
  * affected component (`packages/*`, `crates/*`, `benches/*`, docs claims)
  * reproduction steps or a proof-of-concept fixture
  * assessed impact and any known mitigations

**Target response times:**

* Acknowledgement: within **72 hours**
* Triage + severity assessment: within **7 days**
* Fix or mitigation for high-severity issues: within **30 days**

We will credit reporters by handle on request, or keep reports anonymous.

## Scope

In scope:

* memory/lifecycle safety of native components (`crates/native-journal`,
  future `.node` payloads): panic paths, FFI boundary ownership, input bounds
* path handling / temp-file ownership for spill & cache artifacts
  (`OutputCollector` parity surfaces, `dsh_next_*` tables, sidecar caches)
* decompression bombs / unchecked allocation limits in paged-history &
  checkpoint decode paths
* IPC security: worker message validation, protocol version mismatch handling
* secrets hygiene: credential scrubbing semantics inherited from DSH must not
  regress through dsh-next providers

Out of scope:

* upstream DeepSeek Harness core itself (report via its own channels)
* benchmark-only code under `benches/`
* the experimental `/tmp/dsh-lazy-seam` worktree copies

## Hard rules inherited from the design docs

No component may weaken DSH guarantees for speed: canonical session logs stay
authoritative, derived checkpoints are always discardable fallbacks to canonical
replay, and native modules must never crash the harness process on ordinary bad
input.
