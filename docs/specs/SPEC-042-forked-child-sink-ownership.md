# Spec: Forked-Child Sink Ownership

**ID:** SPEC-042  
**Status:** Completed  
**Last Updated:** 2026-08-11  
**Depends On:** SPEC-027, SPEC-030, SPEC-032, SPEC-033, SPEC-034, SPEC-039

## Overview

A forked child closes transports it never opened. SPEC-039 taught the child to repair itself — its
locks, its worker, its buffered writes — but left it believing it *owns* the sink object it
inherited, and every path that releases a sink acts on that belief. Measured: a `configure(sink=…)`
in a child sends a connection sink's protocol goodbye and the **parent's** next write fails with
`ECONNRESET`; a `shutdown()` in a child closes the inherited object; at exit both processes close
their own copy. For a prefork server — gunicorn, uWSGI, Celery, the deployment SPEC-039 exists for
— one worker's routine startup or shutdown can take down the transport every other worker is
logging through.

SPEC-039 documented its way around this: "do not build a connection-holding sink before the fork at
all." That advice is correct and stays, but it is a constraint on the *user's* startup code to work
around something the library gets wrong, and it fails silently when ignored. This spec makes the
child behave: **it releases only a transport it acquired in this process, and drains to everything
else without ever closing it.**

The asymmetry that makes this decidable is already measurable and already half-built. After
SPEC-039 FR-004 a child's `FileSink` holds its **own** file descriptor — measured, a different
number in each process — because the fork hook reopens the path, so closing it is harmless and
skipping it would lose nothing anyway (`emit` flushes at the end of every batch). A
socket, a database connection or a driver's client has no such step, so the child holds the
parent's and closing it is destructive.

## Scope

### In Scope

- Recording, per process, which sink objects it acquired — the configured sink and every sink
  reachable from it — so that anything else is refused rather than assumed releasable.
- Making **every** sink close in the library ask that question — including the five shipped
  wrapper sinks that close their children directly.
- Reporting the refusal through `health()` rather than leaving it silent.
- Keeping every *drain* unchanged — the child still gets its own events out.
- Re-stating the SPEC-039 FR-004 hook as what it actually is: the sink's claim that the child's
  transport is now the child's own.

### Out of Scope

- **Sink-level flush.** A shared sink whose `close()` performs *delivery* loses whatever the child
  had buffered in it. That needs a flush-without-release hook, which is SPEC-036's subject; FR-006
  AC-5 hands that spec the widened roster this one measured rather than inventing a second
  mechanism beside it.
- **A third-party wrapper's own `close()`.** A user's wrapper closes its children directly and the
  library never sees the call. The same ownership boundary SPEC-039 FR-005 already draws; FR-006
  AC-6 records it.
- **Guessing whether the parent is still alive.** `os.getppid() == 1` answers a different question
  — a parent that is alive but has stopped using the sink is indistinguishable from one that is
  using it, and a parent that exited may already have closed it. A liveness guess would make the
  destructive close *rare* rather than absent, which is worse: rare is what does not show up in
  testing.
- **A `fork_policy=` configuration knob.** The correct behaviour does not vary by deployment, and a
  knob would make the wrong one reachable by configuration.
- **Changing what the parent does.** SPEC-039 FR-001 settled that only the child is repaired.
- **`multiprocessing`'s spawn/forkserver, `posix_spawn`, `os.forkpty`.** A fresh interpreter
  inherits no sink object.
- **Reworking `decorator.py`'s lifecycle state.** SPEC-040 owns that and forbids behaviour change;
  this spec is a behaviour change and does not wait on it. If SPEC-040 lands first the release
  helper is one method on its owner instead of a module function — strictly easier, either order.

---

## Prior work, carried across — do not re-derive

Measured while SPEC-039 shipped and while preparing this spec. Probes are recorded here rather than
referenced, since their scratchpads do not survive.

1. **A child's `configure(sink=B)` closes the inherited sink A**, and it fires whenever an emit has
   reached A in *this process's* record beforehand — armed by the parent before the fork, or by the
   child before it reconfigures. Measured in all four combinations. With a socket sink whose
   `close()` writes a goodbye the server acts on, the parent's next write then failed with
   `ECONNRESET`.
2. **Both swap paths close it**: `Worker.swap_sink` drains, installs, fences with a second drain and
   closes; `decorator._swap_sink`'s no-worker branch calls `_lifecycle.close_detached` with neither
   drain. A process that only ever logs outside a span takes the second.
3. **A child's `shutdown()` closes the inherited sink object**, and at exit each process closes its
   own copy.
4. **A child's `FileSink` holds its own descriptor after the fork hook**, while a socket-holding
   sink holds the same one in both processes. The exact numbers are environment-dependent and the
   asymmetry is not — it exists because the hook reopens, which is the whole design.
5. **There are eight sink-close sites in `src/`, not three.** Three are the lifecycle's —
   `_lifecycle.close_detached`'s thread body, `decorator`'s orphan-path exit close, and `Worker`'s
   own close at shutdown. **Five are shipped wrapper sinks closing their children directly**:
   `MultiSink`, `FilteringSink`, `TransformSink`, and `LogstashSink`/`SentrySink` closing the
   `HTTPSink` they built. A first draft of this spec counted three, from a grep that did not
   include `sinks/`.
6. **The wrapper route is not theoretical and is worse than the direct one.** A child that builds
   its *own* `MultiSink` around an **inherited** inner sink closed the parent's inner sink
   **twice** — once through the old wrapper on the swap, once through its own wrapper at exit — and
   the inner was a *structural* sink, the case `README.md` addresses. A refusal keyed only on the
   object handed to the lifecycle's release path stops neither — and neither does a record that
   reaches only what `configure()` was handed and what SPEC-039's walk enters, which is measured:
   the container yields that inner and `_is_traversable` then declines it.

---

## Functional Requirements

### FR-001: The acquisition record — one mechanism, taken when the library is handed the sink

#### Description:

**The library may release only what it acquired here, and it records that at the one moment it is
knowable: when a sink is handed to it.** `configure(sink=…)` and `config._ensure_sink()`'s lazy
default stamp the current pid against the object — and against **every sink reachable from it**,
because a wrapper is handed over with its children and those children are acquired by the same act.
After a fork every stamp names another process, and a stamp is what a release consults.

A first draft used two sources — this stamp plus a mark laid down by SPEC-039's fork walk — and
review found the union still had a hole exactly where the fix was aimed: a **structural** sink
inside a `MultiSink` is neither stamped (`configure()` was handed the wrapper) nor marked
(`_is_traversable` is `_is_container(v) or _is_owned(v)`, and a foreign object is neither, so the
walk yields it from the container and then declines to enter it — measured). That object is the one
in prior work 6. Two mechanisms with a shared blind spot are worse than one whose coverage a reader
can state, so the mark is gone and the stamp reaches the whole graph.

`_lifecycle` owns the record and therefore the walk, and it reuses `_fork`'s descent predicates
rather than duplicating them — so this spec takes a **second** stated import arrow,
`_lifecycle → _fork`. That is the permitted direction and it is precedented: `decorator` already
imports `_fork`, and the inverted registry exists to stop `_fork` importing *them*, not the
reverse. The sink-shape test stays on the `_lifecycle` side, where `Sink` is already imported and
where `_fork`'s rule that it imports nothing but `_diag` is therefore untouched.

Reaching the graph is a **descent** question, not an ownership one, and the distinction is what
keeps this inside SPEC-039's boundary: the walk enters library objects and plain containers as it
always has, and *records* any sink-shaped member it encounters even when it will not descend into
it. Recording reads nothing from the object — an `id()`, a reference, and the two attribute lookups
`isinstance(x, Sink)` performs on a runtime-checkable Protocol. "Do not reach into third-party
state" (SPEC-039 FR-003 AC-2) forbids mutating and traversing a foreign object, not noticing it.

Three properties, each load-bearing:

- **Taken before the sink is published.** `configure()` assigns `_config.sink` and only then
  installs it, so a stamp written after the assignment leaves a window in which a concurrent
  orphan `info()` reaches an unrecorded sink — and a process exiting inside that window leaks it
  at `atexit`. Narrow, on the safe side, and free to remove by ordering.
- **Write-once per object.** `configure()` never overwrites a stamp naming another process; only
  FR-005's re-acquisition may re-stamp. That stops a child claiming an inherited sink by
  configuring its way back to it, and makes the answer survive a second fork.
- **No record means refused.** The default is the whole point of the spec: every gap must fail
  toward the leak, never toward the destructive close. A sink the library was never handed was
  never its to release.
  **Amended in delivery, escalated and approved:** every *lifecycle* path stamps, so this flat
  default only ever fired where a **user** closed a wrapper the library had never been handed —
  silently turning `FilteringSink(inner).close()` into a no-op, which is the failure mode this
  arc exists to remove. What shipped: an unrecorded sink inherits the answer from the wrapper
  releasing it, so neither-recorded is the caller's own graph and closes, while a *recorded*
  wrapper may not release an unrecorded member (which keeps AC-6). What makes the relaxation
  safe is that a forked child marks everything it inherited **before any handler runs**, so
  "unrecorded" is unclaimable there rather than merely unreleasable. See the delivery doc.
- **A strong reference beside the id**, since an id is reusable once its object dies, and a
  garbage-collected sink closes itself — the same destructive close by another route.
  `_fork._fresh_primitive` already uses that pairing for the same reason.

#### Acceptance Criteria:

- [ ] AC-1: A sink `configure()`d in this process is releasable here; the same object after a fork
      is not. Asserted in a real child.
- [ ] AC-2: A **structural** third-party sink — no library base, matching `README.md`'s documented
      shape — is refused in the child, both when it is the configured sink and when it is held
      **inside a `MultiSink`**. The second is the case both of a first draft's mechanisms missed
      and is not satisfiable by an owned inner.
- [ ] AC-3: A sink the child constructs itself and installs with `configure()` **is** releasable
      there, and is closed normally at `shutdown()` — including a `MultiSink`'s children, which
      the same act acquired.
- [ ] AC-4: `configure(sink=A)` in the child, naming the object it inherited, leaves A refused. A
      stamp overwritten on every `configure()` passes every other criterion here and fails this one.
- [ ] AC-5: A grandchild refuses too: fork, fork again, and the second child still refuses a sink
      the first inherited.
- [ ] AC-6: A sink with **no record at all** is refused, and a test names the case — one added to a
      wrapper after `configure()` ran. The consequence is a leak and is recorded in §13 (FR-006);
      a default of releasable is what this criterion exists to forbid.
      **Amended in delivery** (see the FR-001 note above): the criterion's *named case* holds
      exactly as written — a wrapper the library holds may not release an unrecorded member — but
      the general sentence does not. A sink no wrapper of the library's is releasing is the
      caller's own object and closes; refusing there was the silent no-op the amendment removes.
- [ ] AC-7: The record holds a strong reference: a test proves an inherited sink is not
      garbage-collected in the child while the record stands.
- [ ] AC-8: The parent's records are untouched, asserted by identity (SPEC-039 FR-001 AC-3).
- [ ] AC-9: The one blind spot is stated in the module docstring rather than left to be discovered:
      a sink the library was never handed carries no stamp, so it is refused — safe here, and the
      reason a wrapper mutated after `configure()` leaks.
- [ ] AC-10: **The stamp is taken only where a sink is newly installed** — `configure(sink=…)` and
      `_ensure_sink()`'s *construction* branch — and **never on `_ensure_sink()`'s fast-path
      return**, which `api._log` calls once per orphan event and which SPEC-034 FR-003 AC-6 built
      as a single unlocked read. Stamping there would put a graph walk and a lock acquisition on a
      per-event path. Asserted by counting: N orphan `info()` calls perform **zero** stamp walks.
- [ ] AC-11: The stamp walk's cost is **measured and stated**, on the shape that makes it worst —
      a sink holding caller data, as SPEC-039 measured 202 ms for a `MemorySink` with 100k events,
      whose list and dicts this descent enters for the same reason. If the number argues for
      bounding the descent, that bound is chosen with the measurement in hand rather than guessed.
- [ ] AC-12: The record takes its own lock, and the order is stated and pinned — `_worker_lock` →
      record lock, never the reverse — because `_get_worker` calls `_ensure_sink()` **while holding
      `_worker_lock`** (`decorator.py`). The orphan path is the opposite constraint and is covered
      by AC-10: it calls `_ensure_sink()` under **no** lock, once per event, so nothing there may
      acquire the record lock at all. The repo's lock-ordering history is why this is a criterion
      rather than a note.

### FR-002: One release path, and every closer in the library uses it

#### Description:

Eight sites close a sink today (prior work 5). Guarding the three lifecycle ones is what a first
draft of this spec specified, and it is measurably insufficient: the wrapper route closed a
parent's structural sink twice with all three guarded, because a wrapper the *child* built is
itself releasable and forwards the close to a child that is not.

So the release becomes one helper and **every** library closer calls it — the three lifecycle sites
and the five shipped wrappers. Refusing is a **skip, not a failure**: nothing is counted as lost,
nothing is retried, and the caller's control flow and error handling are unchanged.
`MultiSink.close` still isolates and continues; `shutdown()` still returns; the swap still installs
the new sink.

**The guard moves; the error handling stays.** The eight sites do not agree today and must not be
made to: three absorb with three different `_diag` texts (SPEC-029 wants the site named),
`MultiSink` absorbs *and* increments its `failed` counter, and `FilteringSink`/`TransformSink`
**propagate** — a documented `Raises:`. So the helper propagates whatever `close()` raises and each
caller keeps its own handler. Folding the `try/except` into the helper is the obvious reading of
"one release path" and it silently costs two things: `MultiSink.failed` stops moving, which drops
absorbed close failures out of `Health.sink.failed` (a SPEC-026 regression), and two documented
`Raises:` clauses become false.

**It also creates the first `sinks/ → ` core import arrow.** Today `sinks/*.py` imports `_diag` and
its own siblings, nothing else — the probe-by-name idiom (`losses()`,
`log_foundry_stop_signal`, `_fork`'s inverted registry) exists to keep it that way. That idiom
exists so the core does not reach into *third-party* sinks; five sinks the library ships calling one
of its own helpers is a different question, and pushing a predicate onto five wrappers instead would
be five attributes to keep in sync for no gain. So the arrow is **taken deliberately** — it is not a
cycle (`_lifecycle` imports `_diag`, and `Sink` only under `TYPE_CHECKING`) and AC-9 pins that.

#### Acceptance Criteria:

- [ ] AC-1: Every sink close in `src/` goes through the release helper, enforced by a lint whose
      **discriminator is named here rather than delegated to its docstring**, because a rule left
      to the builder is a hand-list wearing a derivation's clothes: resolve each `.close()`
      receiver to an annotation — through a local alias to a module global, through an attribute to
      its `__init__` parameter, and through an iteration to the container's — and the site is in
      scope iff that annotation names `Sink` or a class in the sink roster `test_sink_concurrency`
      already derives. All eight resolve; every driver close resolves to a non-sink type or to
      nothing, which is why the call expression alone cannot decide it (three of the eight carry no
      annotation at the call site, and so do several driver closes). The lint carries a floor and
      names the eight so a ninth is a decision somebody takes.
- [ ] AC-2: The refusal holds at each of the three lifecycle sites, with a test per site: the swap
      (both paths), the worker's shutdown close, and the orphan-path exit close. A fix covering two
      of three is the shape this defect already took once.
- [ ] AC-3: **The wrapper route is closed, and the pre-fix failure is demonstrated.** A child that
      builds its own `MultiSink` around an inherited inner sink closes it zero times; the test is
      shown failing with the wrapper sites unguarded, where it measures two closes. **The inner is
      a structural sink**, named in the criterion because an owned one passes against a record that
      still has FR-001's old hole — the test would go 2 → 0 and prove nothing about the case it was
      written from.
- [ ] AC-4: A refused release does not raise, does not move `incomplete_swaps`, and does not move
      any loss counter. The sink is left **open**, which is the trade SPEC-027 FR-004 and SPEC-030
      already made twice: a leaked resource in an exiting process beats a corrupt write.
- [ ] AC-5: `_orphan_closed_sink` — SPEC-033's double-close guard, assigned *before* the close
      today — still records what the release path was asked to close, so a refused release cannot
      turn into a second attempt later.
- [ ] AC-6: **The end-to-end case is measured, not asserted:** a child that `configure()`s a new
      sink and a child that calls `shutdown()` both leave the parent's connection usable and the
      parent still delivering. The pre-fix version of each test fails, demonstrated rather than
      claimed.
- [ ] AC-7: A sink that *is* releasable is closed exactly as today, including the detached close,
      its capped grace and `closing_sinks`. A test asserts the counts, so "refuses everything"
      cannot pass.
- [ ] AC-8: The helper returns what its callers need — two sites hand `close_detached`'s thread on
      to a `join` after releasing a lock — so no caller loses its bounded wait (SPEC-030 FR-003).
- [ ] AC-9: The five wrappers' error handling is unchanged, asserted rather than assumed: a child
      whose `close()` raises still increments `MultiSink.failed` and still reaches
      `Health.sink.failed`, and `FilteringSink`/`TransformSink` still propagate as their `Raises:`
      documents. The new `sinks/ → _lifecycle` import is pinned by the existing import test as
      **not** a cycle, and `_lifecycle` gains no import of a concrete sink.

### FR-003: The drain is untouched; only the release is refused

#### Description:

The child must still get its own events out. Refusing the *release* while keeping every *drain* is
what separates this from "the child stops delivering", and the swap is where the two are easiest to
confuse: `Worker.swap_sink` drains, installs, fences with a second drain, and only then closes. The
first three steps stay exactly as they are.

#### Acceptance Criteria:

- [ ] AC-1: A swap in the child still performs both drains, and events submitted before it land in
      the inherited sink. Asserted on the sink's own record, not on `flush()`'s return.
- [ ] AC-2: `shutdown()` in the child still performs its final drain and still stops the worker;
      only the close is skipped. `retired` still reads `True`.
- [ ] AC-3: `flush()` in a child is unchanged in every outcome, including its `reason`.
- [ ] AC-4: The events a child delivered through an inherited sink and those the parent delivered
      through it are both present exactly once, across the two processes.

### FR-004: The refusal is reported, not silent

#### Description:

A state the caller may need to know about is reported rather than prevented (SPEC-019, SPEC-030). A
process delivering through a transport it may not release is such a state: it explains a handle
still open after `shutdown()`, and it is the signal that a deployment is sharing a sink across a
fork at all.

It is a **state, not a fault**, so it is deliberately not a term in the documented alert idiom — the
call `closing_sinks` got in SPEC-030. `Health` is a frozen dataclass since SPEC-034 precisely so a
field can be appended without proving indices.

#### Acceptance Criteria:

- [ ] AC-1: `Health` gains `inherited_sink: bool`, and its **referent is named in the docstring**:
      the sink this process would deliver to now — the worker's if a worker exists, else the orphan
      record's, else the configured one. SPEC-033 measured those three disagreeing, so a field that
      says "the sink" without saying which is a field two readers read differently. It describes
      that **one** object, not the graph beneath it: in FR-002 AC-3's case the child built its own
      wrapper, so this reads `False` while the wrapper's child is refused — stated here because the
      opposite reading is the natural one.
- [ ] AC-2: It reads `False` in a process that never forked, `True` in a child that inherited its
      sink, and `False` again in a child that installed one of its own.
- [ ] AC-3: It is answerable with no worker — synthesized as `retired` already is (SPEC-031 FR-006)
      — and creating a worker to answer `health()` remains forbidden.
- [ ] AC-4: No new stderr line and no new `_diag` verb. `_diag` states three (SPEC-029) and a
      refused release is neither a loss, an absorbed failure, nor a rejection.
- [ ] AC-5: `README.md`'s health table documents it as **not** an alert term, and says what it does
      explain — including that it reads `True` for a shared `StdoutSink`, whose `close()` only
      flushes, so a `True` is not by itself evidence of anything held.

### FR-005: The fork hook says what it actually claims

#### Description:

SPEC-039 shipped `discard_buffered_after_fork()` as "throw away the bytes you inherited". What
`FileSink` does in it is larger and is the fact this spec turns on: it *re-acquires* the transport,
so the child's descriptor is its own. The name describes one consequence of the step rather than the
step, and a sink that only dropped a buffer without reopening would satisfy the name while making a
destructive close look safe.

It is renamed and its contract restated: the hook makes the sink usable in this child, and a sink
that returns from it claims the transport is now this process's own. This is free now and will not
be later — **the hook has never been in a stable release** (latest tag `v0.10.1`; `git tag
--contains` on the commit is empty). Merges to `main` do publish a `X.Y.Z.devN` pre-release, so it
has been *published*; a pre-release is not a compatibility promise, which is the whole point of the
`devN` channel, so no story is owed.

#### Acceptance Criteria:

- [ ] AC-1: The member is renamed to `reacquire_after_fork()` and `sinks/base.py` states both
      halves: strand what you inherited, and by returning you claim the transport as this process's
      own. The old name appears nowhere in `src/` or `tests/`.
- [ ] AC-2: **SPEC-039's completed spec and delivery doc keep the old name**, struck or annotated
      in place rather than rewritten — they record what shipped, and editing a completed spec to
      erase a name it shipped is the deletion SPEC-021 forbids. `architecture.md`'s live mentions
      move to the new name.
- [ ] AC-3: **Of a sink the child inherited**, one that implements the hook and returns normally is
      **releasable** there and one that does not is not. Both directions have a test, and both are
      scoped to an inherited sink: a sink the child constructed itself is releasable with no hook
      at all (FR-001 AC-3), so an unscoped reading of this criterion asserts the opposite.
- [ ] AC-4: A hook that **raises** leaves the sink unreleasable, so a failed re-acquisition cannot
      make a destructive close look safe. SPEC-039 already absorbs the exception; this pins what
      the absorption means for ownership.
- [ ] AC-5: SPEC-039 FR-004 AC-5's lint — every sink that opens a buffered stream of its own
      implements the hook — is unchanged in scope under the new name, floor intact.
- [ ] AC-6: `FileSink` and `RotatingFileSink` need no behavioural change, only the rename; a test
      asserts the child's descriptor differs from the parent's, which is the claim the rename makes
      explicit.
- [ ] AC-7: The ordering is stated where SPEC-039 states its own: the stamps are in place before
      any registered handler runs, since a handler may reach a release path.
- [ ] AC-8: **Re-acquisition re-stamps the sink that re-acquired, and nothing above it.** A child
      inheriting `MultiSink(FileSink, FileSink)` re-stamps the two children — only they implement
      the hook — while the wrapper keeps the parent's stamp and stays refused, which leaves the
      re-acquired children reachable only through a wrapper nothing will release. That is a leak
      and nothing is lost (`emit` flushes per batch), but it is stated rather than discovered:
      FR-005 AC-6's descriptor test uses a bare `FileSink` and passes while this stands.

### FR-006: The boundary docs follow the behaviour, and the residual is handed on accurately

#### Description:

SPEC-039 documented a workaround for this defect in three places. Those become descriptions of a
library that behaves, with the user-facing advice kept — it is still the better deployment — but
demoted from "or else" to "preferred".

The residual this spec accepts is **larger than a first draft claimed** and cannot all be pointed at
one place. Measured from the shipped `close()` bodies: `KafkaSink` and `GooglePubSubSink` deliver a
buffer, `NATSSink` drains its loop, and `SQLiteSink` and `PostgresSink` **commit**. For the last two
a refused close does not merely lose a buffer — it leaves the child's inserts uncommitted on a
connection the parent also holds, which is nonetheless the safer of the two outcomes, since
committing there writes into a transaction the parent may be mid-way through.

#### Acceptance Criteria:

- [ ] AC-1: `architecture.md` §13's record of the post-fork `configure()` close is **struck in place
      and marked fixed by this spec**, per SPEC-021. The same for the exit-close half of the
      shared-sink record.
- [ ] AC-2: §9's fork section states the ownership rule as part of the child's contract: repair,
      deliver, release only what you acquired here.
- [ ] AC-3: `README.md`'s Forking bullet keeps "build a connection-holding sink in the worker
      process" as the recommendation and drops the claim that reconfiguring in the child is harmful,
      since it no longer is.
- [ ] AC-4: SPEC-040's FR-005 AC-3 — which carries this as a named finding — points at this spec,
      so the handoff is closed on both ends.
- [ ] AC-5: The residual is recorded in §13 with the **measured** roster (Kafka, Pub/Sub, NATS,
      SQLite, Postgres) and what each loses, and **SPEC-036 is handed the widened roster on its own
      end** — its flush-hook roster names two of the five today.
- [ ] AC-6: The two leaks default-refuse accepts are recorded in §13 beside the residual roster: a
      sink the library was never handed (a wrapper mutated after `configure()` — no shipped sink
      does this, an AST scan of `sinks/` finds no sink-typed assignment to `self.<attr>` outside
      `__init__`), and a re-acquired child under a refused wrapper (FR-005 AC-8). Both cost a
      handle in an exiting process and neither loses an event.
- [ ] AC-7: The third-party-wrapper hole is recorded in §13: a user's wrapper closes its children
      directly and the library never sees the call, so the refusal cannot reach it. The remedy is
      the same one §9 already gives.

---

## Data Model

```python
# src/log_foundry/_lifecycle.py — process-global, as the closer registry already is
# As shipped. `owner=` and `_mark_inherited` are the FR-001 amendment recorded above; the
# original block said `releasable(sink) -> bool  # no record means False`, which is kept in that
# note rather than left here, since this is the most copyable statement of the contract.
def stamp(sink: object) -> None: ...          # configure()/_ensure_sink(), over the reachable
                                              # sink graph; never overwrites another pid's stamp
def reclaim(sink: object) -> None: ...        # FR-005: the hook returned, so it is ours now
def _mark_inherited() -> None: ...            # a fork handler; marks everything inherited
                                              # _FOREIGN so "no record" is unclaimable in a child
def releasable(sink: object, *, owner: object = None) -> bool: ...
    # Stamped for this pid. An *unrecorded* sink inherits the answer from the wrapper releasing
    # it — no wrapper, or an unrecorded one, means the caller's own object — and only while the
    # child's marking walk completed: the shipped test is
    # `owner_record is None and not _marking_failed`, and that second term is what makes the
    # relaxation safe when the walk could not finish.
def release(sink: Sink, *, detached: bool = False, owner: object = None) -> threading.Thread | None: ...
    # The one close path (FR-002). Returns the closer thread for a detached release, and
    # None both when the release was refused and when an inline close completed —
    # the two are distinguished by `releasable`, which callers already have to consult.

# src/log_foundry/worker.py
@dataclass(frozen=True)
class Health:
    ...                       # unchanged
    inherited_sink: bool      # FR-004 — a state, never an alert term

# src/log_foundry/sinks/base.py — the optional member, renamed (FR-005)
#   optional: losses(), log_foundry_stop_signal, reacquire_after_fork()
```

`_fork.py` still imports only `_diag` (SPEC-039 FR-006): it publishes which hooks succeeded, and
the handler `_lifecycle` registers with it re-stamps those. Nothing else happens at fork time —
the record was laid down at `configure()`, which is what removes the fork walk from this
mechanism entirely. The new arrow this spec does add is `sinks/ → _lifecycle`, taken deliberately
(FR-002).

## Implementation Phases

### Phase 1: One release path (FR-002 AC-1, AC-8)

All eight sites become the helper, with **no guard yet and no behaviour change**, so the move
reviews on its own and the guard has one home. The lint lands with it.

### Phase 2: The record and the refusal (FR-001, FR-002, FR-003)

### Phase 3: The hook's contract and the report (FR-005, FR-004)

### Phase 4: The docs, the struck records, and the handoffs (FR-006)
