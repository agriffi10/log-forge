# Spec: Every Owed Close Is Performed

**ID:** SPEC-045
**Status:** In Progress
**Last Updated:** 2026-09-01
**Depends On:** SPEC-030, SPEC-032, SPEC-033, SPEC-042, SPEC-044

## Overview

The sink every event is going to can be closed by nobody. The orphan path records which sink it
owes a close for in a **single slot**, so arming a second sink discards the first — and an
ordinary `log_foundry.info()` that resolved a sink and was preempted before recording it will
arm a superseded sink over the live one when it resumes. Measured deterministically, with every
`configure()` call made sequentially on one thread: the live sink `C.closes == 0`, its buffer
never delivered, while a superseded sink was closed twice. This spec makes the record hold every
sink owed a close rather than one, so each is closed exactly once per period it was written to,
and makes every reader of the record consume all of it.

## Scope

### In Scope

- `_Lifecycle._orphan_sink` becomes `_orphan_owed`, a record of every sink the orphan path owes a
  close for, mutated in place and emptied by one transition.
- The three sites that consume it — the exit close, `configure()`'s swap, and the first
  `@trace`'s worker build — each closing **every** sink it names rather than one.
- The three sites that merely read it — `flush()`'s sink-buffer drain, the fork walk's
  inheritance roots, and `health().inherited_sink` — following it to more than one sink.
- Correcting `architecture.md` §13, which records this as a `configure()` thread-safety wart and
  gives a reason for not fixing it that the code has already paid for.

**This spec answers a framing question first, and the answer decides its shape.** `configure()`
documents itself as **not thread-safe**, so "concurrent `configure()` calls misbehave" could be a
contract to restate rather than a defect to fix. It is a defect, on three grounds, each measured:

1. **A lock around `configure()` does not reach it.** The loss reproduces with every `configure()`
   call made sequentially on one thread; the only concurrent party is a `log_foundry.info()` on an
   application thread. Serializing `configure()` end to end changes nothing about it.
2. **The racing caller is doing nothing the docs forbid.** Logging from many threads is the
   library's central use case. `configure()`'s disclaimer says a span finishing during a swap "may
   land on either sink" — a routing caveat — not that an ordinary `info()` may leave the live sink
   unclosed.
3. **What is lost is data, not tidiness.** A sink whose `close()` *is* its delivery loses
   everything it buffered. `sinks/base.py` asks an implementation to refuse work once it has
   released something, but a sink that only flushes has released nothing and correctly keeps
   accepting; nineteen shipped sink modules add no post-close guard at all.

### Out of Scope

- **Making `configure()` thread-safe.** Events reaching a sink the config no longer names, and two
  racing `configure()` calls leaving the config naming one sink while delivery goes to another,
  are unchanged. The docstring keeps saying so.
- **Refusing a repeat close.** A sink written to after its close has something new to flush, so a
  second close is owed rather than spurious. A draft of this spec had `release()` veto the repeat;
  it stranded 2 of 3 events on a wrapper-graph shape and lost events on 31 of 80 fuzz seeds
  against 0 before it. The library still performs one close per *period the sink was written to*,
  which is what "closed once" has always meant here.
- **`_Lifecycle._orphan_closed_sink`.** Unchanged. SPEC-044 FR-004 sets it for a sink a swap
  deliberately leaves **open**, which is a different claim from one that was closed, and it still
  blocks a re-arm.
- The pre-existing shape where a sink is released while still live inside the wrapper that
  replaced it (`configure(MultiSink(A, B))` after `configure(A)`), recorded in §13. This spec must
  not make it worse, and a criterion pins that; it does not repair it.

---

## Functional Requirements

### FR-001: Every sink the orphan path owes a close for is closed

#### Description:

`_Lifecycle._orphan_owed` replaces `_orphan_sink` and holds every sink owed a close, keyed by
`id` so identity decides membership and a sink that defines `__eq__` cannot collide with another.
The three transitions that consume it — `_close_orphan_sink`, `_swap_sink` and `_get_worker` —
each act on **every** sink it names. That last part is the requirement, not a consequence: all
three had always handled exactly one sink because there had only ever been one, and two of them
survive a mutation that truncates them to the most recently armed sink.

#### Acceptance Criteria:

- [ ] The deterministic reproduction — one preempted `info()`, `configure()` calls sequential on
      one thread — leaves the live sink closed once and nothing it buffered undelivered.
- [ ] In that same reproduction the sink the resumed emit reached is also closed, so the fix
      moves the loss rather than removing it if either half is dropped. Both are asserted in one
      test.
- [ ] Three sinks written to in turn are each closed exactly once, on the orphan path and the
      worker path.
- [ ] The record holds two sinks at once, asserted directly — a slot cannot, and a return to one
      is the regression this spec exists to prevent.
- [ ] A swap closes **every** superseded sink, not only the last armed.
- [ ] Building the worker releases **every** owed sink it did not adopt, not only the last armed.

### FR-002: A sink written to after its close is owed another

#### Description:

A close discharges what the sink held at that moment, not the sink forever. A sink that takes an
event afterwards has something new to flush, so it is armed again and closed again. This is
current behaviour and is pinned rather than changed, because it is what makes the record safe to
be a set: a design that instead refused the repeat close was built and measured, and stranded
buffers on every shape where a sink outlives the configuration that installed it.

#### Acceptance Criteria:

- [ ] `configure(A)` → log → `configure(B)` → `configure(A)` → log → `shutdown()` closes A twice,
      the second after the later event reached it, on both delivery paths.
- [ ] In that sequence A delivers every event that reached it — nothing is left buffered.
- [ ] `configure(A)` → log → `configure(MultiSink(A, B))` → log → exit leaves nothing A took
      undelivered. The sink is closed while live inside the wrapper, which this spec does not
      repair, but it must not become a loss.

### FR-003: The record is emptied by one transition, and mutated in place

#### Description:

`_Lifecycle.take_orphan_owed()` reads and clears under the lifecycle lock in one step, so two
transitions cannot both decide the same sink's close is theirs. No site rebinds the record to a
fresh object: a rebind silently drops whatever another thread armed between the read and the
write, which is the single-slot defect with a wider window.

#### Acceptance Criteria:

- [ ] `take_orphan_owed()` returns what the record held, in arming order, and leaves it empty.
- [ ] Many threads taking concurrently split the record — no sink reaches two takers, and every
      armed sink reaches one.
- [ ] An AST roster fails a site that rebinds the record or empties it anywhere but in
      `take_orphan_owed`, and the existing disposition roster covers every site that arms or drops
      a sink.

### FR-004: Every reader of the record follows it to more than one sink

#### Description:

Three sites read the record without consuming it, and each was written against a single slot:
`_flush_live_sink` drains the sink's own client buffer (SPEC-036 FR-002), `_inheritance_roots`
feeds the forked child's marking walk (SPEC-042 FR-001), and `_delivering_to_an_inherited_sink`
answers `health().inherited_sink`. The first two must reach every owed sink; the third asks about
one sink and takes the most recently armed, which is the one an emit reached last.

#### Acceptance Criteria:

- [ ] `flush()` empties the client buffer of every owed sink, not only one, and reports success
      only if it did.
- [ ] A forked child refuses to close **every** sink the parent had owed, not only the last
      armed — an unmarked one is claimable and its transport destroyable (SPEC-042 FR-001).
- [ ] `health().inherited_sink` still answers from the most recently armed sink where there is no
      worker.
- [ ] The record needs no `_FORK_SKIP` entry, asserted while it is populated: it drops a sink the
      moment its close is decided, so unlike `_owned` and `_orphan_closed_sink` it never holds a
      superseded one.

### FR-005: The docs stop stating a wart the code does not have

#### Description:

`architecture.md` §13 records this as a `configure()` thread-safety issue whose fix would cost
pinning every sink ever closed. Both halves are wrong: the loss reproduces with no concurrent
`configure()` at all, and the fix needs no new record of closed sinks — it needs the record of
*owed* ones to stop being a slot. The paragraphs are struck in place and marked with the spec
that closed them, per SPEC-021's rule.

#### Acceptance Criteria:

- [ ] §13's double-close entry is struck in place and corrected rather than deleted, and says
      what the defect actually was: the live sink closed by nobody.
- [ ] `configure()`'s docstring no longer says handing the previous sink back "closes it twice"
      as a warning, still says the call is not thread-safe, and says which guarantee is now
      unconditional.
- [ ] `README.md` states the guarantee where it already states the thread-safety caveat.
- [ ] No doc left in the tree claims the library may leave a sink it owns unclosed, or that
      tracking this would pin every sink ever closed.

---

## Data Model

```python
# src/log_foundry/_lifecycle.py, on _Lifecycle

self._orphan_owed: dict[int, Sink] = {}
"""Every sink the orphan path owes a close for, keyed by id and in arming order."""


def take_orphan_owed(self) -> list[Sink]:
    """Empties the record and returns what it held, oldest first. Callers hold ``_lock``."""
```

---

## API / Interface Contract

No public signature changes. The record is private; `configure()`, `flush()`, `shutdown()` and
`health()` keep their signatures and their documented meanings.

## Configuration / Environment

None.

## File & Folder Structure

```
src/log_foundry/
├── _lifecycle.py      # _orphan_owed, take_orphan_owed, and the six sites that use them
└── config.py          # configure()'s docstring only

tests/
├── test_owed_closes.py             # new — FR-001..FR-004
├── test_lifecycle_races.py         # the disposition roster + the in-place-mutation lint
├── test_worker_predicate_roster.py # re-keyed rows, per-module floor 45 -> 46
├── test_sink_release_roster.py     # the release-receiver resolver
└── test_orphan_sink_handoff.py, test_shutdown_lifecycle.py, test_api.py, conftest.py

docs/
├── architecture.md    # §13 struck in place
└── specs/INDEX.md     # status row + arc entry
```

## Implementation Phases

### Phase 1: The record (FR-001, FR-003)

- `_orphan_owed` and `take_orphan_owed` on `_Lifecycle`.
- Convert the three consuming transitions to act on every sink they take.
- Convert the two arming sites to add rather than replace.

### Phase 2: The readers (FR-004)

- `_flush_live_sink`, `_inheritance_roots`, `_delivering_to_an_inherited_sink`.

### Phase 3: The rosters (FR-001, FR-003)

- Re-derive every roster's population after the rename and check the floors — a rename kills an
  AST roster as silently as a deletion.
- Add the in-place-mutation lint; update the disposition table.

### Phase 4: The docs (FR-005)

- `architecture.md` §13, `configure()`'s docstring, `README.md`.

---

## Revision history

- **2026-09-01** — authored as *Release Once Per Acquisition*, on the reading that the defect was
  a sink closed twice. ~~`release()` would keep a record of what it had already closed and refuse
  a second close.~~ **Withdrawn on measurement**, in two steps:
  - The **spec review** established that the single-threaded hand-back
    (`configure(A)` → `configure(B)` → `configure(A)`) is *not* a double close: its two closes are
    one per acquisition, and the second flushes A after a further event reached it.
  - The **second diff review** established that the refusal itself loses data — 2 of 3 events on a
    wrapper-graph shape, 31 of 80 fuzz seeds against 0 before it — because a sink written to after
    its close still needs one. Two narrower variants were built and measured and each still lost
    on an adversarial seed (3 of 40, then 2 of 40).

  What survived every measurement is that the real defect is the **live sink closed by nobody**
  (`C.closes == 0`), caused by the owed-close record being a single slot. Making it a set removes
  the trade rather than choosing a side: nothing is stranded and nothing is closed that had
  nothing to flush. Verified against a lifecycle fuzz that finds 0 undelivered events over 4×80
  seeded runs plus 80 runs of the two seeds that had caught the earlier designs, matching `main`.
- Two of the three consuming transitions were found by **mutation**, not by review: truncating
  `_swap_sink`'s and `_get_worker`'s handling to the most recently armed sink passed the entire
  suite. One test per reader, not one test for the record.
