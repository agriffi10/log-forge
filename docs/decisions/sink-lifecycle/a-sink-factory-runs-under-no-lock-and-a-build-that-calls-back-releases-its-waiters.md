---
type: Decision
title: "A sink factory runs under no lock, and a build that calls back releases its waiters"
description: "`_FactorySink` holds its build lock only to claim a build and to record its result, never across the factory call; a concurrent first emit waits on the build with no lock held, and for the length of a `flush()`, `shutdown()` or sink-changing `configure()` the factory makes, every emit waiting on that build or arriving at it refuses as a delivery failure instead. Never hold a lock across the factory, never refuse a waiter on a build whose factory is not inside one of those three calls, never let a yield outlast its call. (fix after SPEC-079)"
---

**The factory is the caller's code, and the library ran it under its build lock.** A factory that
ran a `@trace`d function built a worker whose drain thread emitted to the wrapper and blocked on
that lock; the factory then called something that waits on the drain thread. Measured at
`9f4d4dfd`, with the factory on the caller's thread: `flush(None)` and `shutdown(None)` hung,
`configure(sink=B)` stalled for its whole 5 s swap budget, and `flush(2)` and `shutdown(2)` for
their 2 s (`tests/test_factory_reentrant.py`). Every other entry point called from the factory —
`info`, `health`, `get_config`, `configure(sink_factory=…)`, `flush`/`shutdown` with no worker
pending — returned at once, from either thread a build runs on.

**Holding no lock is not enough by itself; the wait is the cycle.** A thread waiting for the
build, on an event rather than a lock, is still waited on by a factory waiting on the drain, if
the drain thread is that waiter. So the build lock is held only to claim a build (a per-build
token naming the claiming thread), to read a claim, and to record the result, and the waiters are
told when to stop waiting: `flush()`, `shutdown()` and a `configure()` that changes the sink wrap
the part of themselves that can wait on the drain thread in `_lifecycle.yielding()`, which, on a
thread running a factory, makes every thread waiting on that build refuse its emit with
`SinkDeliveryError`, and any thread arriving at it do the same, until the call returns. A waiter
parked when a yield started refuses even if the yield has already ended when it wakes, since the
yield's call may be waiting on that very thread. The worker retries a refused batch under
`max_retries` and delivers it if the build has landed by then; the orphan path counts it in
`orphan_lost`. Those three calls are the library's waits on the drain thread; `info()`,
`@trace` and the rest submit or emit and never wait on it.

**The claim is cleared in one `finally`**, with the result recorded in the same section, then the
build's wake event set, so a `BaseException` out of the factory or the checks leaves no claim to
refuse the next emit on that thread as re-entrant, and no waiter spinning on a set event. The
failure count `_announce` keeps runs before that section, while the claim still makes it single-
writer. A claim made by another process — a wrapper inherited mid-build through a caller's own
wrapper — is claimed over, since nothing here will settle it.

**Rejected: refusing every emit that meets a build in progress.** A drain thread meeting a slow
factory would refuse its span batch, and the worker's whole backoff (0.01 + 0.02 + 0.04 s) is
shorter than a broker connect, so a batch delivered before would be abandoned.

**Rejected: each concurrent first emit calling the factory, the first to publish winning.** It
calls the factory more than once at a time, building two connections, and a factory whose client
starts a thread that logs would cascade one build per log.

**Rejected: a bounded wait.** A timing constant with no measurement behind it, and still a stall
on every call back.

**Rejected: the drain's wait interrupted by the worker's stop signal only.** It answers
`shutdown(None)` and leaves `flush(None)` hanging.

**What is left.**

- **A factory that joins a thread of its own that logs.** The thread's first emit waits on the
  build, and the join waits on the thread: with no timeout on the join, the process hangs. The
  library cannot see a wait in the caller's code, and refusing every waiter is the rejected design
  above. Recorded under invariant 3.
- **A factory running on the drain thread** that calls `flush()` waits on its own drain thread,
  the drain-thread self-wait `docs/decisions/pipeline/` already records.
- **A yield that waits on nothing still refuses.** The yield is keyed to the call, not to whether
  this particular call can wait on the waiter: a factory on the drain thread that calls
  `configure(sink=B)`, which there is re-entrant and waits on nothing, still refuses an orphan log
  parked on the build, counted in `orphan_lost`, where `9f4d4dfd` delivered it (measured by the
  second diff review). Telling the calls apart by thread would split the rule per call site, and
  the same refusal is what releases those orphan logs when that drain-thread factory calls
  `flush(None)` instead: at `af5a4b2a` an orphan `info()` parked on that build was still blocked
  after 3 s, and now it returns, counted in `orphan_lost`.
- **A close that lands while the factory runs** is answered as before, by the wrapper's release
  count and `_lifecycle.rearm_if_released` (*A stop-signal setter runs under no lock…*): the
  build writes to the built sink and re-arms the wrapper, so the next `shutdown()` or the exit
  closes it once. Measured at `9f4d4dfd` by the plan review's probe, seventeen scenarios with a real
  interpreter exit, the built sink ended closed once in every one.

**Do NOT** hold a lock across the factory call. **Do NOT** refuse a waiter on a build whose
factory is not inside `flush()`, `shutdown()` or a sink-changing `configure()`. **Do NOT** let a yield outlast the
call that started it.

## History

### 2026-11-12

- **Settled**, as a fix after SPEC-079, handed on by the stop-signal setter fix (PR 341).
