# Persistent process supervisor — feasibility design note (Phase 0)

Status: design only. No code beyond the journal pilot exists. This note exists
so the idea is captured honestly instead of being implemented prematurely.

## Idea

A native sidecar daemon (`dsh-next-supervisor`) owns spawned process trees so
they survive harness restarts; Node reconnects and re-adopts jobs.

```text
Node DSH ──framed IPC over private socket──▶ dsh-next-supervisor
                                             ├─ process tree A
                                             ├─ PTY C
                                             └─ output journals (bounded, spilled)
```

## Questions the design must answer before any implementation

| Question | Current position |
|---|---|
| Supervisor ownership | One supervisor per DSH home dir; lifecycle pinned to the composition's data dir, not to a session |
| Socket/pipe location | Under the same 0700 tmpdir/home path used by spill files today (`mkdtempSync(tmpdir/dsh-subprocess-*)`) |
| Authentication/ownership | Peer credential check (SO_PEERCRED / LOCAL_PEERCRED) + a capability token file created 0600 at startup |
| Process id namespace | Supervisor assigns stable ids (`jobId` from TS registry stays authoritative); pid mapping is supervisor-internal state |
| Reconnect | Node sends HELLO with incarnation id; supervisor replays adopted-handle summaries; missing incarnations treated as orphaned |
| Orphan detection | Supervisor watches its own children; Node crashes are detected by IPC death, triggering configurable policy (keep vs SIGTERM trees) |
| DSH restart | Adoption protocol above; `SubprocessHandle.done` reconstructed from supervisor state |
| Supervisor restart | Restarting supervisor must kill or re-adopt its trees — simplest contract: supervisor death = tree termination (documented blast radius) unless a re-adoption path is justified by evidence |
| Job identity mapping | Registry remains TS (see architecture.md); supervisor maps jobId → native handle only |
| Output retention | Journals keep bounded tails exactly like today's collectors, optionally spilling; readers reconnect via offset-resumed pulls |
| Terminal lifecycle | PTY owned by supervisor; foreground process inspection/signals become framed requests — highest-risk area for behavioral drift vs node-pty contracts |
| Cleanup guarantees | Whole-tree termination (SIGTERM→grace→SIGKILL, Windows taskkill) stays inside the supervisor where waitpid/prctl live naturally |
| Cross-platform strategy | POSIX: groups; Windows: job objects equivalent via taskkill patterns — parity must be proven per platform before shipping |
| Sandbox interaction | Landlock/sandbox runtimes launched BY DSH stay under DSH's own supervision chain until a measured need proves otherwise |

## Why not now

* No Phase 0 measurement implicates current supervision performance or
  correctness; the intricacy table above shows high regression surface.
* Blast radius: N-API supervision would tie supervisor correctness to the
  harness process; sidecar isolation changes crash semantics deliberately —
  that is a product decision requiring its own validation story.
* Nothing blocks incremental value: the journal pilot and (future) off-main-
  thread persistence deliver measurable wins first.
