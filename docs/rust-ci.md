# Rust CI policy

dsh-next currently has one Rust crate: `crates/native-journal`, the napi-rs
bounded-journal pilot. GitHub Actions addresses that manifest explicitly; the
repository is not a Cargo workspace and CI does not pretend the TypeScript
packages are Rust members.

The blocking stable job uses the pinned Rust 1.97.1 toolchain and is the
authoritative Rust gate. It runs formatting, clippy with warnings denied,
nextest, `cargo check`, a release build, Cargo package validation, and the
existing native-vs-TypeScript differential test. nextest executes the crate's
Rust test binary. `cargo test --doc` is currently inapplicable because the
N-API crate exposes only a `cdylib` target; CI verifies that fact explicitly so
adding a doc-testable library target requires a corresponding doctest command.

`cargo-tog` coordinates Cargo registry/git downloads and compiler-object
caching. CI uses `mode: github`, which needs no R2/S3 bucket, credentials, or
other external object store. The public action installs cargo-nextest and owns
its sccache/rust-cache implementation details, so workflows do not layer a
second independent Rust cache on top.

Cache keys include the toolchain lane, runner OS and architecture, target
triple, build configuration, and `Cargo.lock` hash. Cargo-tog/sccache further
keys compiler objects by compilation identity, including the rustc identity
and target. Stable and nightly use distinct target directories, cargo-tog cache
directories, and key prefixes. A cache miss is only a performance event: every
command remains reproducible from `Cargo.lock` with an empty cache.

The current-nightly job is advisory and explicitly enables the next-generation
trait solver with `-Znext-solver=globally`. It runs nextest, the same doctest
applicability guard, check, and the release build, but omits nightly clippy to
avoid unrelated lint churn. A
failed nightly step remains visible in GitHub Actions while its job-level
advisory status does not block a normal pull request.

Both PR jobs report a single warm `cargo check` timing with compiler identity,
target, profile, and cache semantics. A weekly/manual cold-compile workflow
uses cargo-tog `registry-only` mode plus a fresh lane-specific target directory,
so stable and nightly are compared under the same cold compiler-object
conditions without clearing caches in blocking CI. Timing has no pass/fail
threshold.

`act-on` is intentionally not part of the authoritative path. GitHub Actions
remains the source of truth for action composition, cache service behavior, and
the advisory nightly canary.
