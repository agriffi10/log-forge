# Spec: Fork Lifecycle

**ID:** SPEC-039  
**Status:** Draft  
**Last Updated:** 2026-08-09  
**Depends On:** SPEC-027, SPEC-028, SPEC-030, SPEC-033, SPEC-035

## Overview

`os.fork()` is unhandled anywhere in the tree — nothing in `src/`, `docs/` or `tests/` mentions
it. Two measured failures, from the concurrency surface of the 2026-08-07 audit (C4 / L2 / L2b):

**The child inherits a worker whose thread does not exist.** `submit` still enqueues and nothing
drains. Measured: the child's 6 events were never delivered, `atexit` closed the sink without
draining, and `health()` read `queued=2, dropped=0, failed_batches=0, stopped_reason=None,
retired=False` — the documented alert idiom is blind. `flush()` returning `False` was the only
honest surface, and the library wrote nothing to stderr across the whole run.

**The child inherits a sink lock held by a thread that no longer exists.** Fork while the drain
thread is inside `FileSink.emit` and the child's first `log_foundry.info()` blocks forever, on the
application's own thread. Measured: **19 of 60 forked children hung permanently**, `faulthandler`
showing `file.py:78 in emit ← api.py:93 in _log`. This reaches every sink SPEC-028 locked —
`FileSink`, `SQLiteSink`, `PostgresSink`, `ClickHouseSink`, the socket sinks.

The second is the one that matters: losing a child's logs is bad, hanging the application is
worse, and a logging call is the last place an application expects to deadlock. Prefork servers
(gunicorn, uWSGI, Celery) are a mainstream deployment for a logging library.

**This was SPEC-035 FR-005**, moved to its own spec after that spec's other five FRs shipped. It
is the largest single piece of work in that spec, it is the only one needing a new mechanism and a
new module, and holding a completed spec open for it helped nobody. Everything below — including
the amendments its ACs took during SPEC-035's Phase 1 and the measurements taken while preparing
it — is carried across unchanged, so nothing is re-derived. Structural note: this spec is one FR
per hazard rather than one FR with eleven criteria, which is what it was; that is a presentation
change and no criterion is dropped.

## Scope

### In Scope

- The inherited worker: a child that logs after forking delivers its events.
- The inherited locks: a child's first log call does not block.
- The inherited buffered writes: the child does not re-emit the parent's pending bytes.
- What the parent keeps, and what the child must not duplicate.
- Documenting the boundary — what a fork does *not* reach, and whose problem that is.

### Out of Scope

- **A `before` or `after_in_parent` handler.** Settled in FR-001 AC-1 on evidence, not
  preference.
- **Reaching into a third-party client's state.** A boto3 session's locks, `librdkafka`'s
  threads and a `psycopg` connection's file descriptor are not the library's to swap. FR-005
  records the boundary; it does not move it.
- **Restarting a worker after a *terminal failure*.** SPEC-019 settled that a thread which
  resurrects itself fights a process trying to exit. Rebuilding after a **fork** is a different
  event: the process is new, and nothing is trying to exit.
- **Undoing a `shutdown()` across a fork.** A retired parent forks a retired child (FR-002
  AC-4).
- **`os.forkpty`, `posix_spawn`, and `multiprocessing`'s spawn/forkserver start methods.**
  Only `fork` inherits the hazards here; spawn starts a fresh interpreter and needs nothing.

---

## Prior work, carried across — do not re-derive

Four measurements were run on CPython 3.13.1 while preparing this work. Their probes lived in a
session scratchpad that no longer exists, so they are recorded here rather than referenced.

1. **CPython repairs its own `Thread` objects.** In the child an inherited `Thread` reports
   `is_alive()` → `False`, and `join(2.0)` returns in 0.000 s. So `_lifecycle`'s existing
   `is_alive()` filters self-prune and **no closer-registry reset is needed** — one piece of this
   work that does not have to be built. Stated rather than assumed.
2. **An inherited `Lock` stays locked with no owner** — `acquire(timeout=1)` → `False`. That is
   the 19-of-60 hang, and re-initialising it is the library's own job.
3. **The child-side buffer discard works, and the unfixed case really does duplicate.** Forking
   with bytes pending in a `FileSink`-shaped buffered stream: without a discard the pending line
   appears **twice** on disk; with `os.dup2(devnull_fd, stream.fileno())` followed by reopening
   the path in append mode it appears **once**, the child's own write lands, and the inherited
   object's eventual GC flush goes to `/dev/null` with no exception. Both directions are measured,
   so the test for it will not be vacuous.
4. **`os.register_at_fork` child handlers run in registration order**, after `threading`'s own.

**Any new fork measurement must be constructed to land in the window it claims to clear** —
interpose on the stream's `write`; do not hope the timing lands there. An earlier draft claimed
the file sinks were immune on a measurement that forked *after* `emit` returned, when the buffer
is empty by construction: it exercised the non-hazard. That claim is struck in FR-003 rather than
deleted, because "it writes and flushes inside `emit`" is true, reads like immunity, and will be
re-derived otherwise.

---

## Functional Requirements

### FR-001: The child is the only side that is repaired

#### Description:

`os.register_at_fork` offers three hooks and this spec registers exactly one: `after_in_child`.

The question was left open by an earlier draft and is settled on two grounds, of which cost is
the weaker. **Cost:** acquiring a sink's SPEC-028 transport lock while the drain thread is
mid-`emit` blocked the fork for a measured 1.20 s, and with `HTTPSink`'s documented 90 s worst
case it would block gunicorn's master thread for that long — with no shutdown in progress, so
SPEC-027's stop signal cannot cut it.

**The load-bearing ground:** `before` does not run for a C-level fork at all. uWSGI calls
`PyOS_AfterFork_Child` only, so on one of the three deployments this spec names, a `before`
handler would not run and every hazard it was supposed to close would happen anyway. A
parent-side fix is therefore not available *in general*, which means the child handler has to be
sufficient regardless — and a parent-side handler would be a partial fix bought at that price.

#### Acceptance Criteria:

- [ ] AC-1: Only `after_in_child` is registered. A test asserts no `before` or `after_in_parent`
      handler is installed, so a later change has to argue with this FR rather than slip past it.
- [ ] AC-2: The order of work in the child is the contract and is stated in the module: re-init
      locks → discard buffers → registered handlers. A lock re-initialised after a handler that
      takes it is a handler that hangs.
- [ ] AC-3: The parent is unaffected — its worker, queue and counters are unchanged across the
      fork, and a test asserts the parent's delivery continues.

### FR-002: The child's worker is rebuilt, not retired

#### Description:

The child inherits a `Worker` object whose thread does not exist. Two candidate behaviours, and
they are not equivalent:

- *Rebuild the worker in the child.* Delivery continues and each child gets its own drain thread —
  which for a prefork server is what the user wants. But the child inherits the parent's sink
  object, and two processes writing one socket or one SQLite handle is its own defect (FR-005).
- *Retire the worker in the child and record why.* Nothing is delivered, but nothing is lost
  silently either: `stopped_reason` says the process forked.

**Rebuild is the design.** The sink is the caller's object and the caller's choice, `Sink` already
documents concurrent use, and a child that silently stops logging is the failure this whole arc
exists to remove. A `stopped_reason` of `"Forked"` is *not* right for a child that then works.

The rebuild is **in place** (`Worker._reinit_after_fork`), not a new object, so ownership guards
keyed on `_worker.sink is X` and `_lifecycle`'s registry stay valid across it.

#### Acceptance Criteria:

- [ ] AC-1: A child that logs after forking delivers those events.
- [ ] AC-2: The child's queue starts **empty** and the parent keeps what was in flight, so events
      queued in the parent but undelivered at fork time are not delivered twice. A test asserts
      the total across both processes.
- [ ] AC-3: Counters are zeroed in the child — they describe a drain thread that no longer
      exists — and `health()` in the child describes the child.
- [ ] AC-4: **A retired parent forks a retired child.** A fork does not undo a `shutdown()`, and
      a test pins it: the alternative silently revives a worker the caller terminated.
- [ ] AC-5: The rebuilt worker is the same object, so a guard keyed on `_worker.sink is X` holds
      across the fork. A test asserts identity, since rebuilding as a new object is the obvious
      implementation and breaks SPEC-033's ownership guards.
- [ ] AC-6: `Worker.stopped_reason` is **not** set to `"Forked"` on the rebuild path — the child
      works, and SPEC-019's field means the drain thread died. Stated because the alternative
      reads as the more honest one and is not.

### FR-003: Every lock the library owns is re-initialised, and the roster is derived

#### Description:

An inherited `Lock` stays locked with no owner (measurement 2). The child's first `info()` then
blocks forever on the application's own thread — 19 of 60 children in the audit's run.

**The roster must be derived, and the derivation rule stated**, rather than the illustrative list
(`decorator._worker_lock`, `_lifecycle._closers_lock`, `Worker._lock`, the counter locks, each
locking sink's pair) *being* the roster. A hand-list is what SPEC-035 FR-002 exists to stop, and
SPEC-036 FR-003 adds a counter lock after this spec ships that must be picked up with no edit
here.

**An identity memo is load-bearing, not tidiness.** A sink's `log_foundry_stop_signal` *is* the
worker's
`_stop` (SPEC-027); replacing them with two fresh events would leave the worker setting one and
the sink waiting on the other. The memo is also what makes re-initialising `threading.Event`s safe
at all, and they must carry their set state across.

#### Acceptance Criteria:

- [ ] AC-1: After a fork, the child's first log call does not block. A test forks repeatedly
      (≥50 iterations) with the drain thread actively emitting into a locking sink, and every
      child completes within a timeout. **The pre-fix version of this test hangs, and that is
      demonstrated** rather than asserted.
- [ ] AC-2: The traversal walks every `log_foundry.*` module in `sys.modules`, then descends only
      into log_foundry-owned instances and classes and plain containers. Containers are traversed
      because `MultiSink._sinks` is one; third-party client state is deliberately not reached
      (FR-005).
- [ ] AC-3: **Completeness is proved, not asserted:** an AST test that every
      `threading.Lock()`/`RLock()`/`Event()` construction in `src/` is assigned either to a module
      global or to a `self.<attr>` — the two shapes the traversal reaches. A lock held inside a
      container would be unreachable, so the test forbids that shape. This is what picks up
      SPEC-036 FR-003's lock with no edit here.
- [ ] AC-4: Two objects sharing one lock or one `Event` still share it afterwards, asserted by
      identity. The `log_foundry_stop_signal`/`_stop` pair is the case that matters and is named
      in the test.
- [ ] AC-5: A re-initialised `Event` carries its set state across the fork.
- [ ] AC-6: A lock that was **not** held at fork time is still replaced. Re-initialising only the
      held ones needs a way to ask whether a lock is held, and `threading.Lock` has none that is
      not itself a race.

### FR-004: The child discards inherited buffered writes

#### Description:

Without a `before` handler (FR-001), a fork landing *inside* `emit` — after the write loop, before
the flush — leaves the child holding the parent's unflushed bytes, which both processes then
write. `FileSink` opens a **buffered** stream (`file.py:55`) and flushes once at the end of the
batch (`:80-84`), so the window is the whole batch.

Measured against `5ad6699`, forking mid-batch with the child re-initialising the lock per FR-003
and continuing to log per FR-002 — identical for `RotatingFileSink`:

```
{"seq": 0, "msg": "a"}   {"seq": 1, "msg": "b"}   {"seq": 0, "msg": "a"}   {"seq": 99, …}
seq-0 appears 2x
```

~~The file sinks are immune, because they write and flush inside `emit`.~~ — struck in place
(SPEC-021), not deleted: the measurement behind it forked *after* `emit` returned, when the buffer
is empty by construction, so it exercised the non-hazard. The claim is true as a sentence and
reads like immunity.

This is also why the hazard is **not** settled by FR-005: that criterion names a shared handle,
which is the caller's sink to share, where this is the library's own sink duplicating its own
bytes.

The mechanism is measurement 3: `os.dup2(devnull_fd, stream.fileno())` then reopen the path in
append mode. The hook is probed by name (`discard_buffered_after_fork`) and documented next to
`read_losses` in `sinks/base.py` — the same optional-protocol shape as `losses()`, so no existing
sink stops satisfying `Sink`.

#### Acceptance Criteria:

- [ ] AC-1: A test forks **mid-`emit`** — interposing on the stream's `write`, per the method note
      above — and asserts the child re-emits none of the parent's buffered bytes.
- [ ] AC-2: The parent's own pending bytes still reach disk exactly once.
- [ ] AC-3: `FileSink` and `RotatingFileSink` implement the hook; a sink without it is
      unaffected, asserted with a bare `emit`/`close` class.
- [ ] AC-4: `sinks/base.py` documents the hook beside `losses()`, including that it runs in a
      forked child with a single thread and must not block.
- [ ] AC-5: A lint asserts every sink that writes to a buffered stream it owns implements the
      hook, derived from the sink roster rather than a hand-written list — SPEC-032's scope gate.

### FR-005: The boundary is documented, and what is beyond it is recorded

#### Description:

Three things a child cannot repair, each with a different owner. SPEC-021's rule is that an
accepted item is *recorded*, and these are not interchangeable — a shared handle is the caller's,
a third-party client's buffer is nobody's, and `sys.stdout` is the application's.

#### Acceptance Criteria:

- [ ] AC-1: **A sink shared across a fork is the caller's responsibility**, with the concrete
      hazard named: one socket or one SQLite handle, two processes. `README.md` and
      `architecture.md` §9 say so — a user running gunicorn preload needs to be able to find it.
- [ ] AC-2: **A third-party client that buffers across `emit`** — `KafkaSink`'s producer,
      `GooglePubSubSink`'s futures (SPEC-036 FR-002's roster) — holds state the library cannot
      reach, so a fork mid-batch there can still duplicate or strand it. Recorded in
      `architecture.md` §13. FR-005 AC-1's shared-handle sentence is a different hazard and does
      not cover this one.
- [ ] AC-3: **`StdoutSink` carries the same duplication hazard as the file sinks and is
      deliberately not fixed**: `sys.stdout` is a *process*-owned buffer, and the library must not
      discard the application's pending output to protect its own. Recorded in §13. This occupant
      was not expected when FR-004's hook was designed and is the reason the hook is per-sink
      rather than global.
- [ ] AC-4: `architecture.md` §9 states the fork behaviour as a whole — what the child rebuilds,
      what it discards, and what it leaves alone.

### FR-006: The mechanism lives where it cannot create an import cycle

#### Description:

The handler needs `decorator` (which owns `_worker` and the orphan records), `_lifecycle` and the
sinks — and all three would then import it. A new `src/log_foundry/_fork.py` importing only
`_diag` and `sinks.base`, exposing an **inverted registry** (`register_child_handler`), keeps the
dependency arrow pointing one way: `decorator.py` registers the worker rebuild rather than
`_fork.py` reaching for it.

`decorator.py`'s registered handler asks worker questions, so its guards land in SPEC-035
FR-002's roster and must be classified. That is the roster working as intended, not friction.

#### Acceptance Criteria:

- [ ] AC-1: `_fork.py` imports nothing from the package but `_diag` and `sinks.base`, asserted by
      the same kind of import test the package already uses for `_diag` and `sanitize`.
- [ ] AC-2: Registration happens at import of the package, once, and a double import does not
      register twice.
- [ ] AC-3: The new guards in `decorator.py` are classified in the FR-002 roster with reasons, and
      the roster is green.
- [ ] AC-4: A platform without `os.register_at_fork` (Windows) imports the package cleanly and
      the rest of the library is unaffected. CI runs Linux and macOS only, so this is asserted by
      construction — the registration is guarded — and stated in the docstring.

---

## Data Model

No new state and no `Health` field.

```python
# src/log_foundry/_fork.py — new module; imports only _diag and sinks.base
os.register_at_fork(after_in_child=_reinit_after_fork)
# No `before` / `after_in_parent`: FR-001. `before` does not run for a C-level fork, so the
# child has to close these hazards regardless; pre-acquiring the library's locks would buy a
# partial fix for a measured 1.20 s on the parent's fork path, ~90 s worst case with HTTPSink.

def register_child_handler(fn: Callable[[], None]) -> None: ...
    # Inverted registry: decorator.py registers the worker rebuild, so _fork imports nothing
    # that imports it.

# src/log_foundry/sinks/base.py — one more optional member, probed by name
#   optional: losses(), log_foundry_stop_signal, flush() (SPEC-036), discard_buffered_after_fork() (FR-004)
```

## Implementation Phases

### Phase 1: The lock roster (FR-003)

First, because its derivation decides how much of the rest is mechanical, and because the hang is
the worst of the three failures.

### Phase 2: `_fork.py` and the worker rebuild (FR-006, FR-002, FR-001)

### Phase 3: The buffer discard (FR-004)

### Phase 4: The boundary (FR-005)

Documentation and the §13 records. Last, because AC-2 and AC-3 name what the first three phases
turned out not to reach.
