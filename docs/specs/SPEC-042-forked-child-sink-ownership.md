# Spec: Forked-Child Sink Ownership

**ID:** SPEC-042  
**Status:** Draft  
**Last Updated:** 2026-08-11  
**Depends On:** SPEC-027, SPEC-030, SPEC-033, SPEC-039

## Overview

A forked child closes transports it never opened. SPEC-039 taught the child to repair itself —
its locks, its worker, its buffered writes — but left it believing it *owns* the sink object it
inherited, and every path that releases a sink acts on that belief. Measured: a
`configure(sink=…)` in a child sends a connection sink's protocol goodbye and the **parent's**
next write fails with `ECONNRESET`; a `shutdown()` in a child closes the inherited sink object;
and at exit both processes close their own copy. For a prefork server — gunicorn, uWSGI, Celery,
the deployment SPEC-039 exists for — that means one worker's routine startup or shutdown can take
down the transport every other worker is logging through.

SPEC-039 documented its way around this: "do not build a connection-holding sink before the fork
at all." That advice is correct and stays, but it is a constraint on the *user's* startup code to
work around a thing the library gets wrong, and it fails silently when ignored. This spec makes
the child behave: **it releases only a transport it re-acquired, and drains to everything else
without ever closing it.**

The asymmetry is already measurable and already half-built. After SPEC-039 FR-004 a child's
`FileSink` holds its **own** file descriptor — measured, fd 4 in the child against the parent's
fd 3 — because the fork hook reopens the path. Closing that is harmless. A socket, a database
connection or a driver's client has no such step, so the child holds the parent's and closing it
is destructive. The library can tell these apart today: the sink either re-acquired its transport
in the child or it did not.

## Scope

### In Scope

- Recording, per process, which sink objects it inherited across a fork rather than created.
- One release path for every sink close in the library, which refuses an inherited sink.
- Reporting that refusal through `health()` rather than leaving it silent.
- Keeping every *drain* unchanged — the child still gets its own events out.
- Re-stating the FR-004 hook as what it actually is: the sink's claim that the child's transport
  is now the child's own.

### Out of Scope

- **Sink-level `flush()`** — the residual this spec accepts is that a shared sink whose `close()`
  performs delivery loses whatever the child had buffered in it. That is SPEC-036's subject
  (`KafkaSink`'s producer, `GooglePubSubSink`'s futures) and this spec records the loss rather
  than inventing a second flush mechanism next to it.
- **Guessing whether the parent is still alive.** `os.getppid() == 1` answers a different
  question — a parent that is alive but has stopped using the sink is indistinguishable from one
  that is using it, and a parent that exited may already have closed it. A liveness guess would
  make the destructive close *rare* rather than absent, which is worse: rare is what does not
  show up in testing.
- **A `fork_policy=` configuration knob.** The correct behaviour does not vary by deployment, and
  a knob would make the wrong one reachable by configuration.
- **Changing what the parent does.** SPEC-039 FR-001 settled that only the child is repaired.
- **`multiprocessing`'s spawn/forkserver, `posix_spawn`, `os.forkpty`.** A fresh interpreter
  inherits no sink object.
- **Reworking `decorator.py`'s lifecycle state.** SPEC-040 owns that and forbids behaviour
  change; this spec is a behaviour change and does not wait on it. If SPEC-040 lands first the
  release path is one method on its owner instead of a module helper — strictly easier, and
  either order works.

---

## Prior work, carried across — do not re-derive

Measured while SPEC-039 shipped and while preparing this. Probes are recorded here rather than
referenced, since their scratchpads do not survive.

1. **A child's `configure(sink=B)` closes the inherited sink A**, and it fires whenever an emit
   has reached A in *this process's* record beforehand — armed by the parent before the fork, or
   by the child before it reconfigures. Measured in all four combinations. With a socket sink
   whose `close()` writes a goodbye the server acts on, the parent's next write then failed with
   `ECONNRESET`.
2. **Both swap paths close it**, not just the worker's: `Worker.swap_sink` drains, installs,
   fences with a second drain and closes; `decorator._swap_sink`'s no-worker branch calls
   `_lifecycle.close_detached` with neither drain. A process that only ever logs outside a span
   takes the second.
3. **A child's `shutdown()` closes the inherited sink object**, and at exit each process closes
   its own copy. Measured.
4. **A child's `FileSink` holds its own descriptor after the FR-004 hook** — fd 4 against the
   parent's fd 3 — so its close is harmless, and skipping it would lose nothing anyway, since
   `emit` flushes at the end of every batch.
5. **There are exactly three `close()` sites in `src/`** — `_lifecycle.py`'s detached closer
   (which both swap paths already funnel through), `decorator.py`'s orphan-path exit close, and
   `worker.py`'s own close at shutdown.

---

## Functional Requirements

### FR-001: The child records what it inherited, by identity

#### Description:

A sink is inherited when this process did not create it. The child cannot ask that question of an
object, so the fork handler answers it at the one instant it is knowable: everything the library
is holding at fork time was created before the fork.

Identity is the key, not a flag on the config, because a child may `configure()` its way back to
the same object and must not thereby claim it. Ids alone are unsound — an id is reusable once the
object dies — so the record holds a strong reference alongside, which is also what stops a
GC-triggered close of the very object being protected. `_fork.py`'s `_fresh_primitive` already
uses that pairing for the same reason.

The mark is sticky across a second fork and survives `configure()`, and it is cleared for one
object only by FR-005's re-acquisition.

#### Acceptance Criteria:

- [ ] AC-1: After a fork, every sink object the child holds — the configured sink, the worker's,
      the orphan record's, and any sink reachable inside a `MultiSink` — is recorded as
      inherited. The reachability rule is FR-003's walk, not a new one.
- [ ] AC-2: A sink the child constructs itself and installs with `configure()` is **not**
      inherited, and is closed normally at `shutdown()`.
- [ ] AC-3: `configure(sink=A)` in the child, naming the object it inherited, leaves A inherited.
      A test pins this, since a flag keyed on "the config changed" would clear it.
- [ ] AC-4: A grandchild inherits the mark: fork, fork again, and the second child still refuses.
- [ ] AC-5: The record holds a strong reference, and a test proves the inherited sink is not
      garbage-collected in the child while the record stands — a GC close is the same destructive
      close arriving by another route.
- [ ] AC-6: The parent's records are untouched, asserted by identity, per SPEC-039 FR-001 AC-3.

### FR-002: One release path, and it refuses an inherited sink

#### Description:

Three sites call `close()` on a sink today and each would need the same guard, which is how a
rule becomes true at two of three sites. They become one helper — the *release* — and the guard
lives in it once.

Refusing is a **skip, not a failure**: nothing is counted as lost, nothing is retried, and the
caller's own control flow is unchanged. `shutdown()` still returns, the swap still installs the
new sink, `atexit` still runs.

#### Acceptance Criteria:

- [ ] AC-1: Every sink close in `src/` goes through one release helper. A lint asserts no other
      module calls `.close()` on a sink, derived rather than hand-listed, with the three known
      sites named in the test so a fourth added later is a decision somebody takes.
- [ ] AC-2: The release refuses an inherited sink at all three sites — the swap (both paths), the
      worker's shutdown close, and the orphan-path exit close. Each has its own test; a fix that
      covers two of three is the defect this FR exists to remove.
- [ ] AC-3: A refused release does not raise, does not move `incomplete_swaps`, and does not move
      any loss counter. The sink is left **open**, which is the choice SPEC-027 FR-004 and
      SPEC-030 already made twice: a leaked resource in an exiting process beats a corrupt write.
- [ ] AC-4: **The end-to-end case is measured, not asserted:** a child that `configure()`s a new
      sink, and a child that calls `shutdown()`, both leave the parent's connection usable and
      the parent still delivering. The pre-fix version of each test fails, and that is
      demonstrated rather than claimed.
- [ ] AC-5: A sink that is *not* inherited is closed exactly as it is today, including the
      detached close, its capped grace and `closing_sinks`. A test asserts the counts, so
      "refuses everything" cannot pass.

### FR-003: The drain is untouched; only the release is refused

#### Description:

The child must still get its own events out. Refusing the *release* while keeping every *drain*
is what separates this from "the child stops delivering", and the swap is where the two are
easiest to confuse: `Worker.swap_sink` drains, installs, fences with a second drain, and only
then closes. The first three steps stay exactly as they are.

#### Acceptance Criteria:

- [ ] AC-1: A swap in the child still performs both drains, and events submitted before it land
      in the inherited sink. Asserted on the sink's own record, not on `flush()`'s return.
- [ ] AC-2: `shutdown()` in the child still performs its final drain and still stops the worker;
      only the close is skipped. `retired` still reads `True`.
- [ ] AC-3: `flush()` in a child is unchanged in every outcome, including its `reason`.
- [ ] AC-4: The events a child delivered through an inherited sink and the events the parent
      delivered through it are both present exactly once, across the two processes.

### FR-004: The refusal is reported, not silent

#### Description:

The library's standing rule is that a state the caller may need to know about is reported rather
than prevented (SPEC-019, SPEC-030). A process delivering through a transport it does not own is
such a state: it explains an unclosed handle at exit, and it is the signal that a deployment is
sharing a sink across a fork at all.

It is a **state, not a fault**, so it is deliberately not a term in the documented alert idiom —
the same call `closing_sinks` got in SPEC-030. `Health` is a frozen dataclass since SPEC-034
precisely so a field can be appended without proving indices.

#### Acceptance Criteria:

- [ ] AC-1: `Health` gains `inherited_sink: bool` — whether the sink this process is delivering
      through was created before a fork in this process's ancestry.
- [ ] AC-2: It reads `False` in a process that never forked, `True` in a child that inherited its
      sink, and `False` again in a child that installed one of its own.
- [ ] AC-3: No new stderr line and no new `_diag` verb. `_diag` states three (SPEC-029) and a
      refused release is neither a loss, an absorbed failure, nor a rejection.
- [ ] AC-4: The field is documented in `README.md`'s health table as **not** an alert term, with
      the one thing it does explain: a handle still open after `shutdown()`.

### FR-005: The fork hook says what it actually claims

#### Description:

SPEC-039 shipped `discard_buffered_after_fork()` as "throw away the bytes you inherited". What
`FileSink` does in it is larger and is the fact this spec turns on: it *re-acquires* the
transport, so the child's descriptor is its own. The name describes one consequence of the step
rather than the step, and a sink that only dropped a buffer without reopening would satisfy the
name while making a destructive close look safe.

It is renamed and its contract restated: the hook makes the sink usable in this child, and a sink
that returns from it is claiming the transport is now this process's own. This is free to do now
and will not be later — **the hook has never appeared in a release** (latest is `v0.10.1`;
SPEC-039 is unreleased), so no compatibility story is owed.

#### Acceptance Criteria:

- [ ] AC-1: The member is renamed to `reacquire_after_fork()` and `sinks/base.py` states both
      halves: strand what you inherited, and by returning you claim the transport as this
      process's own. The old name appears nowhere in `src/`, `tests/` or `docs/`.
- [ ] AC-2: A sink that implements it and returns normally is **releasable** by the child; one
      that does not implement it is not. Both directions have a test.
- [ ] AC-3: A hook that **raises** leaves the sink inherited, so a failed re-acquisition cannot
      make a destructive close look safe. SPEC-039 already absorbs the exception; this pins what
      the absorption means for ownership.
- [ ] AC-4: FR-004's AC-5 lint is unchanged in scope — every sink that opens a buffered stream of
      its own still implements the hook, under the new name and with the floor intact.
- [ ] AC-5: `FileSink` and `RotatingFileSink` need no behavioural change, only the rename; a test
      asserts the child's descriptor differs from the parent's, which is the claim the rename
      makes explicit.

### FR-006: The boundary docs follow the behaviour

#### Description:

SPEC-039 documented a workaround for this defect in three places. Those become descriptions of a
library that behaves, with the user-facing advice kept — it is still the better deployment — but
demoted from "or else" to "preferred".

#### Acceptance Criteria:

- [ ] AC-1: `architecture.md` §13's record of the post-fork `configure()` close is **struck in
      place and marked fixed by this spec**, per SPEC-021's rule that an open item is never
      deleted. The same for the exit-close half of the shared-sink record.
- [ ] AC-2: §9's fork section states the ownership rule as part of the child's contract: repair,
      deliver, release only what you re-acquired.
- [ ] AC-3: `README.md`'s Forking bullet keeps "build a connection-holding sink in the worker
      process" as the recommendation and drops the claim that reconfiguring in the child is
      harmful, since it no longer is.
- [ ] AC-4: SPEC-040's FR-005 AC-3 — which carries this as a named finding — is updated to point
      at this spec, so the handoff is closed on both ends.
- [ ] AC-5: The residual is recorded in §13: a shared sink whose `close()` performs delivery
      loses whatever the child had buffered in it, with the shipped roster named and the fix
      pointed at SPEC-036.

---

## Data Model

```python
# src/log_foundry/_lifecycle.py — process-global, as the closer registry already is
def mark_inherited(sink: object) -> None: ...   # every sink held at fork time
def claim(sink: object) -> None: ...            # FR-005: the hook returned, so it is ours
def is_inherited(sink: object) -> bool: ...
def release(sink: object, *, detached: bool) -> bool: ...
    # The one close path (FR-002). Returns whether the sink was actually closed.

# src/log_foundry/worker.py
@dataclass(frozen=True)
class Health:
    ...                       # unchanged
    inherited_sink: bool      # FR-004 — a state, never an alert term

# src/log_foundry/sinks/base.py — the optional member, renamed (FR-005)
#   optional: losses(), log_foundry_stop_signal, reacquire_after_fork()
```

`_fork.py` still imports only `_diag`. It publishes what it reached and what re-acquired; the
handler `_lifecycle` registers with it does the marking, so the dependency arrow is unchanged
(SPEC-039 FR-006).

## Implementation Phases

### Phase 1: One release path (FR-002 AC-1)

Fold the three close sites into one helper with no behaviour change and no guard yet, so the
guard has somewhere to live and the move is reviewable on its own.

### Phase 2: The record and the refusal (FR-001, FR-002, FR-003)

### Phase 3: The hook's contract and the report (FR-005, FR-004)

### Phase 4: The docs and the struck records (FR-006)
