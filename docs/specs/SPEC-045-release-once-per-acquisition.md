# Spec: Release Once Per Acquisition

**ID:** SPEC-045
**Status:** In Progress
**Last Updated:** 2026-09-01
**Depends On:** SPEC-030, SPEC-032, SPEC-033, SPEC-042, SPEC-044

## Overview

The library closes a sink twice, and leaves the live one unclosed in the same run. An ordinary
`log_foundry.info()` on an application thread that resolves the sink and is preempted before it
records what it reached can re-arm a sink the library has already closed; a later `shutdown()`
then closes that sink a second time, while the sink events are actually going to is closed by
nobody. Measured deterministically: `A.closes == 2`, `C.closes == 0`. A randomised-preemption
harness finds the same outcome 5 times in 120 trials from ordinary concurrency, against 0 in 120
with a single `configure` thread. The library has a standing decision that it performs exactly one
`close()` of a sink it owns and does not rely on `Sink.close()` being idempotent, because it
cannot enforce what a third-party sink does; this spec gives the process a record of what it has
already released and makes the one function that ever closes a sink consult it, so a sink is
closed once per time the library was handed it — no more, and no fewer.

## Scope

### In Scope

- A process-wide record of which sinks this process has already released, written and consulted by
  `_lifecycle.release()` — the single path by which the library ever closes a sink (SPEC-042
  FR-002).
- Restoring releasability when the library is handed a sink again, so a sink handed over twice is
  closed twice and one handed over once is closed once.
- Refusing to re-arm a released sink as the orphan path's owed close, which is what leaves the live
  sink unclosed and is recorded at `architecture.md` §13 as "orphans the live sink".
- The fork boundary: what a child inherits of the new record, and what `reacquire_after_fork()`
  restores.
- Correcting `architecture.md` §13, which records this as a `configure()` thread-safety wart, gives
  a reason for not fixing it that the code has already paid for, and calls a **correct** sequence a
  double close.

**This spec answers a framing question first, and the answer decides its shape.** `configure()`
documents itself as **not thread-safe**, so "concurrent `configure()` calls misbehave" could be a
contract to restate rather than a defect to fix. It is a defect, on three grounds, each measured
rather than argued:

1. **A lock around `configure()` does not reach it.** The double close reproduces with every
   `configure()` call made **sequentially on one thread**; the only concurrent party is a
   `log_foundry.info()` on an application thread. Serializing `configure()` end to end changes
   nothing about that interleaving.
2. **The racing caller is doing nothing the docs forbid.** Logging from many threads is the
   library's central use case. `configure()`'s disclaimer says a span finishing during a swap "may
   land on either sink" — a routing caveat — not that an ordinary `info()` may cause a sink to be
   closed twice.
3. The library **already decided** it performs one close and does not rely on `Sink.close()`
   idempotency (SPEC-032), and `_swap_sink` and `_adopt_declined_swap` both cite that decision as
   the reason for the latches they carry. A double close is that decision failing where it is
   stated. This spec **refines** that rule rather than superseding it: one close per *acquisition*,
   which is what makes a sink handed over twice closeable twice.

What this spec explicitly does **not** claim is that it makes `configure()` thread-safe. It makes
the *close* single. Everything else the disclaimer covers is untouched and stays documented.

### Out of Scope

- **Making `configure()` thread-safe.** Events reaching a sink the config no longer names, a span
  finishing on another thread landing on either sink, and two racing `configure()` calls leaving
  the config naming one sink while delivery goes to another are all unchanged. The docstring keeps
  saying so.
- **Retiring `_Lifecycle._orphan_closed_sink`.** It is not a "closed" record and must not be
  merged with the new one: SPEC-044 FR-004 sets it for a sink the swap deliberately leaves
  **open** (`_lifecycle.py`, `_swap_sink`), so it answers "do not re-arm this", not "this was
  closed". The two questions stay separate and each site says which it asks.
- **Policing a caller's own closes.** A sink the library was never handed has no record, so
  `FilteringSink(inner).close()` called twice still forwards twice. Recording those releases would
  mean pinning objects the library does not own, which is a cost `architecture.md` §13 declines and
  this spec agrees with it about.
- Changing `Sink.close()`'s documented obligation to make its release idempotent. It stays; the
  library still does not rely on it.
- The **misrouting** the same traces show — a `configure(sink=B)` that returns while every event
  goes to C. Real, visible in the reproduction, and a different defect.

---

## Functional Requirements

### FR-001: A release the library performed is not performed again

#### Description:

`_lifecycle.release()` records the sinks this process has released and refuses a second release of
one, so a sink the library owns receives exactly one `close()` per acquisition. The record lives
beside `_owned` and under `_owned_lock`: `_owned` already holds a strong reference to every sink
the library was ever handed and never shrinks, so an id kept alongside it pins nothing new and
cannot collide with a later object. The check and the mark are **one critical section**, so two
threads calling `release()` on the same sink concurrently perform one close between them; that
means `releasable()`'s body is factored into a lock-free inner that both it and the claim call
under a single acquire, because `_owned_lock` is a plain `Lock` and a claim that called
`releasable()` under it would deadlock.

A refused second release is a **skip**, matching the refusal `release()` already performs for a
sink this process did not acquire: nothing is counted lost, nothing is announced, no counter
moves, and every caller's control flow is unchanged.

The claim is taken where the close happens, not where it is requested. A detached release spawns
its closer without claiming, and the thread body claims through `release()` like any other caller
— so whichever of a detached close and a racing inline close claims first performs it, and the
other is skipped.

#### Acceptance Criteria:

The first three discriminate — each fails on today's tree. The rest are regression guards that
pass today and must keep passing.

- [ ] Two `release()` calls against the same library-owned sink call `close()` once.
- [ ] `run_concurrently` driving `release()` on one sink from many threads yields exactly one
      `close()`, and no call raises.
- [ ] A detached `release()` whose close is refused because an inline release claimed first still
      returns its thread, and the sink is closed once in total.
- [ ] A sink with no `_owned` record is released every time it is asked for, so
      `FilteringSink(inner).close()` twice still calls `inner.close()` twice.
- [ ] A sink whose record names another process is still refused, and the refusal is unchanged by
      the new record (SPEC-042 FR-001).
- [ ] `_EXPECTED_CLOSERS` is unchanged in size: the guard is added inside `release()`, not at a
      ninth close site.
- [ ] In the wrapper-graph residual below — `configure(MultiSink(A, B))` → `configure(A)`, where A
      is closed by the wrapper while it is the live sink — A's refused exit close discards nothing,
      because A refuses work after its own close (SPEC-032). Asserted with a sink that implements
      that refusal, not assumed.

### FR-002: Being handed a sink again restores its release

#### Description:

`stamp()` is the one point at which the library is handed a sink (SPEC-042 FR-001), and a sink
handed over again is one this process owes a close for again. `stamp()` therefore clears the
released mark for every reachable sink whose record names **this** process, and `reclaim()` clears
it for the sink it re-stamps.

Without this, FR-001 breaks a sequence that is **correct today**. `configure(A)` → `info()` →
`configure(B)` → `configure(A)` → `info()` → `shutdown()` calls `A.close()` twice, and both are
right: the first closes A as the sink being swapped out, the second closes A as the live sink,
after it has received a further event. Measured on both delivery paths, with `A.events` rising
between the two closes. Only per-acquisition scoping keeps that true while FR-001 holds.

The write-once rule stands: a record naming another process is never overwritten and its released
mark is not cleared, so a forked child cannot restore its own right to close an inherited sink by
configuring its way back to it.

#### Acceptance Criteria:

- [ ] `configure(A)` → `info()` → `configure(B)` → `configure(A)` → `info()` → `shutdown()` calls
      `A.close()` exactly **twice**, on the orphan path and on the worker path.
- [ ] In that sequence the event logged after the hand-back reaches A, and A's second close happens
      after it — so the flush is not lost.
- [ ] `configure(A)` → `info()` → `configure(B)` → `shutdown()` calls `A.close()` exactly **once**:
      one acquisition, one close.
- [ ] `stamp()` does not clear the released mark of a sink whose record names another process.
- [ ] `reclaim()` clears the released mark, so a re-acquired sink in a forked child is closeable.

### FR-003: A released sink is not re-armed as the owed close

#### Description:

The orphan path's owed-close record is a single slot, so a sink the library released can be armed
into it again once the slot has moved on. This is not merely adjacent to FR-001 — it is what makes
FR-001 insufficient alone. In the deterministic reproduction the re-armed sink A takes the record
from the live sink C, so with FR-001 in place the second `release(A)` is refused and **C is closed
by nobody**: a double close would become a lost close. `_note_orphan_emit` and
`_adopt_declined_swap` already refuse to arm the sink named by `_orphan_closed_sink`; they now also
refuse one this process has released. `_orphan_closed_sink` stays exactly as SPEC-044 left it,
because it also names a sink the swap left **open**, which is not a released sink and must still
block a re-arm.

The check sits inside the critical section those functions already take, so the lock order
`_state._lock` → `_config_lock` → `_owned_lock` is unchanged and `_note_orphan_emit`'s unlocked
fast path is untouched.

#### Acceptance Criteria:

- [ ] An orphan emit that resolved sink A, was preempted, and resumes after A has been released
      leaves `_state._orphan_sink` naming the live sink rather than A.
- [ ] In that scenario `A.close()` is called once in total **and the live sink is closed at exit** —
      both halves asserted in one test, since either alone passes a broken implementation.
- [ ] `_adopt_declined_swap` refuses to arm a sink this process has released.
- [ ] A sink named by `_orphan_closed_sink` because a swap left it **open** is still refused a
      re-arm, and is still closed by whoever owns it — the two records are not conflated.
- [ ] `_note_orphan_emit`'s unlocked fast path acquires no additional lock.

### FR-004: The record crosses a fork without repair

#### Description:

The finding here is that **no fork work is required**, and this FR is the proof rather than the
change. The released record holds `int` ids and no sink references, so it pins nothing, needs no
`_FORK_SKIP` entry, and gives `_fork`'s repair walk nothing to write back — the same reasoning
`_closing_now` carries. A child inherits it harmlessly: `_mark_inherited()` marks every inherited
sink `_FOREIGN` before any handler runs, so `release()` refuses those on ownership before the
released record is ever consulted. A child that re-acquires a transport through
`reacquire_after_fork()` has its mark cleared by `reclaim()` (FR-002) and may close what it now
holds.

#### Acceptance Criteria:

- [ ] The record is read **after** a release has populated it, and is asserted to be non-empty and
      to contain no sink references and no `_FORK_SKIP` entry — an empty record satisfies the
      structural half vacuously.
- [ ] A parent that has **released** a sink and then forks: the child refuses to close it, and the
      parent's later close of a sink it has *not* released still happens.
- [ ] A child whose sink implements `reacquire_after_fork()` closes it exactly once.
- [ ] The `_fork` shape lint still passes, and no new primitive is built outside a holder the walk
      can write back to.

### FR-005: The docs stop stating a wart the code does not have

#### Description:

`architecture.md` §13 is wrong in three ways about this and each is corrected in place, per
SPEC-021's rule that an open item is closed by being fixed, settled or recorded, never deleted.
It calls the hand-back sequence a double close when its two closes are one per acquisition and the
second flushes a live sink — measured. It gives "tracking every sink ever closed would pin them
all against garbage collection" as the reason not to fix this, when `_owned` already holds a strong
reference to every sink handed to the library and, by its own docstring, "grows by one entry per
sink ever handed to the library and never shrinks". And it says `sinks/base.py` **requires**
`close()` to be idempotent "which is what makes it tolerable", where `base.py` *asks* an
implementation to make its release idempotent and the library's own docstrings say it does not rely
on that.

#### Acceptance Criteria:

- [ ] §13's double-close entry is struck in place and marked closed by this spec, with all three
      corrections made rather than the paragraph deleted.
- [ ] `configure()`'s docstring no longer says handing the previous sink back "closes it twice",
      still says the call is not thread-safe, and says which guarantee is now unconditional.
- [ ] `sinks/base.py`'s statement that an implementation should make its release idempotent is
      unchanged, and the docstrings saying the library does not rely on it are still true.
- [ ] Every surviving statement that the library may close an owned sink more than once is removed.
      The population is enumerated by `grep -rniE 'clos(e|es|ed|ing)[^.]{0,80}(twice|two|second
      time)|double.?clos'` over `src/`, `docs/` and `README.md`; the expected survivors are the
      historical spec and delivery docs, which record what was true when written.

---

## Data Model

```python
# src/log_foundry/_lifecycle.py

_released: set[int] = set()
"""Ids of the sinks this process has already released, guarded by ``_owned_lock``.

Only ids that ``_owned`` holds a strong reference for are ever added, which is what makes an id
sufficient: the reference stops the object dying, so the id cannot be reused while the mark
stands. Holds no sink, so it pins nothing and needs no ``_FORK_SKIP`` entry.
"""


def _releasable_locked(sink: object, owner: object, pid: int) -> bool:
    """The ownership question, with ``_owned_lock`` already held by the caller."""


def _claim_release(sink: object, *, owner: object = None) -> bool:
    """Whether this process may close the sink *now*, marking it released if so.

    One acquire of ``_owned_lock`` covering the ownership test, the released test and the mark,
    so two concurrent releases of one sink produce one close.
    """


def _was_released(sink: object) -> bool:
    """Whether this process has already released the sink."""
```

---

## API / Interface Contract

No public signature changes. `release()`, `releasable()`, `stamp()` and `reclaim()` keep their
current signatures; `release()` gains one refusal case, already covered by its documented
`Returns: … or None — for a refused release`.

```python
release(sink: Sink, *, detached: bool = False, owner: object = None) -> threading.Thread | None
# unchanged signature; returns None for a release refused because this process already
# performed it, exactly as it already does for a sink this process did not acquire.
```

## Configuration / Environment

None.

## File & Folder Structure

```
src/log_foundry/
├── _lifecycle.py      # _released, _releasable_locked, _claim_release, _was_released;
│                      # release/releasable/stamp/reclaim, _note_orphan_emit,
│                      # _adopt_declined_swap
└── config.py          # configure()'s docstring only

tests/
├── test_release_once.py            # new — FR-001..FR-004 behaviour
├── test_sink_release_roster.py     # _LATCH_DISPOSITIONS, closer/requester tables
├── test_lifecycle_races.py         # the orphan-record disposition roster
├── test_worker_predicate_roster.py # per-module floors, if a new guard is a worker question
└── test_fork_lifecycle.py          # the shape lint and the no-_FORK_SKIP claim

docs/
├── architecture.md    # §13 struck in place
└── specs/INDEX.md     # status row + arc entry
```

## Implementation Phases

### Phase 1: The record and the chokepoint (FR-001, FR-002)

- Factor `releasable()`'s body into a lock-free inner; add `_released`, `_claim_release` and
  `_was_released` beside `_owned`, under `_owned_lock`.
- Route `release()`'s inline branch through `_claim_release`; leave the detached branch claiming
  in its thread body.
- Clear the mark in `stamp()` for reachable sinks whose record names this process, and in
  `reclaim()`.
- Pin the hand-back's two closes **before** changing anything, so FR-002's criteria are measured
  against the tree rather than assumed; then mutation-test the new guard.

### Phase 2: The re-arm guards (FR-003)

- Commit the deterministic reproduction (sequential `configure()` calls, one preempted emit) and
  observe it fail on the pre-fix tree, asserting both halves: A closed once, C closed.
- Add the released test to `_note_orphan_emit` and `_adopt_declined_swap`, inside the critical
  section each already takes.
- Confirm `_orphan_closed_sink`'s open-sink case still blocks a re-arm.

### Phase 3: The fork boundary and the rosters (FR-004)

- Assert the populated record holds no sinks and needs no `_FORK_SKIP` entry.
- Re-derive every roster's population and check the floors: the worker-predicate roster's
  per-module floors, `_EXPECTED_CLOSERS` / `_EXPECTED_REQUESTERS` /
  `_EXPECTED_UNJOINED_REQUESTERS`, `_LATCH_DISPOSITIONS`, and the orphan-record disposition roster.

### Phase 4: The docs (FR-005)

- Strike `architecture.md` §13's double-close entry in place, making all three corrections.
- Rewrite `configure()`'s hand-back sentence.
- Run FR-005's enumeration and reconcile every hit.

---

## Revision history

- **2026-09-01** — authored. Measurements taken on `main` at `4dbb28f` before any design. A
  randomised-preemption harness (jitter at `_ensure_sink`, `emit` and `close`; two log threads, two
  `configure` threads, two `shutdown` threads) measured **5 of 120** trials with a doubled close,
  **0 of 120** with a single `configure` thread, **1 of 120** on the worker path. Two interleavings
  were traced to completion; both end with a sink armed as the orphan path's owed close *after* it
  was released.
- **2026-09-01, after the spec review** — two claims withdrawn, one measurement added.
  ~~The single-threaded hand-back `configure(A)` → `info()` → `configure(B)` → `configure(A)` is a
  double close, and is ground 1 for rejecting a lock.~~ It is **not** a defect: its two closes are
  one per acquisition and the second flushes A after a further event reached it (`A.events` 1 → 2
  between them). §13 is wrong about it, which FR-005 now corrects, and FR-002's criteria were
  inverted accordingly — they had demanded "exactly once" for a sequence in which twice is right,
  which would have required leaving the live sink unclosed at exit.
  Ground 1 is replaced by a **deterministic** reproduction: every `configure()` sequential on one
  thread, one ordinary preempted `info()`, giving `A.closes == 2` and `C.closes == 0`. A
  configure-serializing lock was also built and measured at 0 of 120 on the randomised harness —
  a rate change, not a fix, which the deterministic case settles.
- A residual is recorded rather than fixed, and is the same family in both directions: a sink that
  is simultaneously reachable from another graph the library is releasing —
  `configure(MultiSink(A, B))` → `configure(A)`, or `configure(A)` → `configure(MultiSink(A, B))`,
  where `stamp()`'s graph walk clears A's mark and the swap then closes A while A is live. Measured
  today as `A.closes == 2` with A already closed while live, so the case is broken before this
  change and is not repaired by it. FR-001 has a criterion asserting the refused close discards
  nothing, resting on SPEC-032's post-close refusal rather than on the assumption.
