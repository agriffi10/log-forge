# Spec: Lifecycle Ownership — One Owner for the Worker and the Sink

**ID:** SPEC-040  
**Status:** Draft  
**Last Updated:** 2026-08-09  
**Depends On:** SPEC-030, SPEC-031, SPEC-033, SPEC-035, SPEC-039

## Overview

This spec fixes no bug. It is here because of what the bug list looks like.

`decorator.py` is 999 lines and, besides the decorator, it owns the process's entire delivery
lifecycle: `_worker`, `_worker_lock`, `_atexit_registered`, `_orphan_sink`, `_orphan_closed_sink`,
`_orphan_stop`, `_orphan_retired`, and the twelve functions that read them — `_get_worker`,
`_live_worker`, `_process_worker`, `_note_orphan_emit`, `_offer_orphan_signal`,
`_close_orphan_sink`, `_swap_sink`, `_adopt_declined_swap`, `_shutdown_worker`, `_flush_worker`,
`_worker_health`, `_register_exit_handler`. A module named for a decorator is the process's
lifecycle manager, and the state it manages is seven loose globals with no state machine over
them.

Every one of these came out of that, and nothing else:

| | |
|---|---|
| SPEC-030 | a late `configure(sink=…)` retargeted the config while events kept going to the captured sink |
| SPEC-031 FR-006 | an orphan-only process registered no `atexit`, so its sink was never closed |
| SPEC-033 | `_swap_sink` returned early on a null worker, leaving the previous sink open |
| audit C1 | an orphan log stole the worker's stop signal — a SPEC-033 regression |
| audit C2 | a swap racing `shutdown()` left the new sink owned by nobody — a SPEC-033 regression |
| SPEC-035 FR-001/003 | those two regressions |
| SPEC-035 FR-002 | a test that walks the AST and forces each of sixteen worker questions to declare which of four categories it asks |

Seven pieces of work, one shape: **"who owns the worker or the sink at this instant, and what may
this path therefore do?"** — asked ad hoc at sixteen sites in one module, answered by four
categories that exist only in a test's data table and `architecture.md` §9.2.

FR-002's roster is the right response to a defect that recurs at *sites*, and it works: it caught
a real regression during its own review. But it polices the symptom. Sixteen sites asking four
questions about seven globals is a state machine that was never written down as one, and eleven
review rounds went into a test that makes the absence survivable rather than into removing it.

**Nothing here changes behaviour.** That is the whole discipline: the arc above is what happens
when this state is edited, so a change to it must be provably behaviour-preserving or it is
another entry in that table.

## Scope

### In Scope

- Moving the worker/sink lifecycle state and its guards out of `decorator.py` into one owner.
- Making the four questions of `architecture.md` §9.2 methods on that owner rather than
  expressions at call sites.
- Keeping every published signal — `Health`, `_diag` lines, `flush()`'s verdict — bit-identical.

### Out of Scope

- **Any behaviour change at all**, including ones that look like improvements. If the current
  behaviour is wrong, that is a finding for its own spec, recorded here and not fixed in passing.
  A refactor that also fixes things cannot be verified by the test suite that has to gate it.
- **`Worker` itself.** The questions inside the object — `_close_if_owed`, `swap_sink`'s
  retirement re-checks — are already one object's own state. SPEC-035 FR-002 records why the
  module boundary sits where it does.
- **Splitting `worker.py`** (1,220 lines). Real, and a different subject.
- **Retiring SPEC-035 FR-002's roster.** It is how this spec is *verified* (FR-004). Whether it
  is still worth its weight afterwards is FR-005's recorded question, answered with evidence a
  year of maintenance provides and this spec does not.

---

## Functional Requirements

### FR-001: One object owns the lifecycle state

#### Description:

The seven globals become the state of one object with one lock, living in `_lifecycle.py` — which
already exists, already holds the closer registry and the stop-signal offer, and was created by
SPEC-033 for exactly this reason before the rest of the state followed it.

`decorator.py` keeps the decorator, the span machinery and `continue_trace`. The public façade in
`__init__.py` is unchanged.

#### Acceptance Criteria:

- [ ] AC-1: `decorator.py` holds no worker or orphan-sink global, and its line count falls by at
      least a third. The count is stated in the PR, not asserted by a test.
- [ ] AC-2: The public API is untouched — `__all__`, every signature, and every module a caller
      may import are identical. A test compares the exported surface before and after.
- [ ] AC-3: The internal call sites move with the state; no module gains an import cycle, checked
      by the existing import test.
- [ ] AC-4: `_lifecycle.py` states the state machine in its module docstring: the states, the
      transitions, and which of the four §9.2 questions each guard asks.

### FR-002: The four questions become methods, asked once each

#### Description:

`architecture.md` §9.2 names four questions — existence, liveness, ownership, ownership ∧ moment.
They are currently expressions typed out at sixteen sites, which is why three reviewers could each
name a different site, each be fixed, and a fourth ship broken.

Four methods on the owner, each with the reasoning that is currently spread across sixteen roster
rows in its docstring. A call site then *selects* a question instead of *composing* one.

#### Acceptance Criteria:

- [ ] AC-1: Each of the four is one method, and every guard calls one of them. A guard that needs
      something else is a fifth question and forces a decision — which is what the roster achieves
      today by failing.
- [ ] AC-2: The conjunction SPEC-035 FR-001 settled (`_worker.sink is sink and _worker.draining`)
      is inside the ownership ∧ moment method, so the two measured-wrong predicates it rejected
      cannot be reintroduced by composing them at a call site.
- [ ] AC-3: Each method's docstring carries the reason its roster row carries today, including the
      struck ones. The reasons are the deliverable of SPEC-035 FR-002 and must not be lost in a
      move — deleting them is how this spec becomes the eighth row in the table above.
- [ ] AC-4: The `_live_worker` / ownership distinction that four defects turned on is stated once,
      in one place, rather than in each caller.

### FR-003: The move is proved behaviour-preserving

#### Description:

The suite is 1,199 tests and is the gate. It is not sufficient on its own: the two SPEC-033
regressions were both green.

#### Acceptance Criteria:

- [ ] AC-1: The whole suite passes unchanged — **no test is edited**, except for imports of
      moved private names. Any test that needs its *assertions* changed is a behaviour change and
      fails this FR.
- [ ] AC-2: The number of edited test files and the reason for each is stated in the PR.
- [ ] AC-3: The lifecycle tests that exist because of the table above —
      `test_shutdown_lifecycle.py`, `test_orphan_sink_handoff.py`, `test_worker.py` — pass
      untouched. They are the regression suite for this exact state.
- [ ] AC-4: An execution harness, not a review: the concurrency cases run under
      `conftest.run_concurrently` and the injected preemption points that already exist, per
      `docs/process.md` on lifecycle work.
- [ ] AC-5: Mutation-tested in the direction that matters — each of the four questions is
      swapped for its neighbour, and each swap fails a *behavioural* test, not only the roster.
      A question nothing behavioural distinguishes is recorded (SPEC-035 FR-002 round 7b found
      exactly one such site) rather than silently accepted.

### FR-004: The roster survives the move and still binds

#### Description:

SPEC-035 FR-002's roster walks `decorator.py`. Move the state and it walks a module with almost
no sites left, and would pass vacuously — the exact failure its own module docstring warns about
in a different form.

#### Acceptance Criteria:

- [ ] AC-1: The roster's scope follows the state to `_lifecycle.py`, and covers both modules if
      any guard remains in `decorator.py`.
- [ ] AC-2: A test asserts the roster is **non-empty** and covers at least as many sites as it did
      before the move. A roster that walks the wrong module is the vacuous case, and it passes.
- [ ] AC-3: The accessor derivation (`_accessor_names`) moves with it and is re-verified against
      the round-11 attack in its new home.

### FR-005: What this does not settle is recorded

#### Description:

Two questions this spec deliberately leaves open, recorded per SPEC-021 so they are decisions
rather than omissions.

#### Acceptance Criteria:

- [ ] AC-1: Whether the roster is still worth its weight once the questions are methods — it is
      ~1,000 lines of test against four methods — is recorded in `architecture.md` §9.2 as a
      question to revisit with evidence, not answered here. Removing it in the same change that
      removes its subject leaves nothing watching either.
- [ ] AC-2: `worker.py` at 1,220 lines is recorded in §13 as the same shape one level down,
      unsplit and not scheduled.

---

## Data Model

```python
# src/log_foundry/_lifecycle.py — the seven globals become one owner
class _Lifecycle:
    _worker: Worker | None
    _orphan_sink: Sink | None          # the sink an orphan emit actually reached (SPEC-031/033)
    _orphan_closed_sink: Sink | None
    _orphan_stop: threading.Event
    _orphan_retired: bool
    _atexit_registered: bool
    _lock: threading.Lock              # the process-wide lock, unchanged in scope and ordering

    # architecture.md §9.2, one method each — never composed at a call site
    def exists(self) -> Worker | None: ...
    def live(self) -> Worker | None: ...
    def owns(self, sink: Sink) -> bool: ...
    def owns_now(self, sink: Sink) -> bool: ...
```

No new state, no field added or removed, no lock added and no lock ordering changed —
`_worker_lock` → `_closers_lock` stays the only two-lock path and stays in that order.

## Implementation Phases

### Phase 1: The four questions become methods, in place

`decorator.py` keeps its globals; the guards start calling methods. Reviewable on its own, and
FR-003's mutation sweep runs here where the diff is smallest.

### Phase 2: The state moves to `_lifecycle.py`

Mechanical once Phase 1 has removed the composed expressions.

### Phase 3: The roster follows (FR-004), and the records are written (FR-005)
