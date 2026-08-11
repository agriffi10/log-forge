# Handoff — SPEC-039, Phases 3 and 4

Written at a context boundary, mid-spec. Everything below is verifiable from the repo; where a
number is quoted it was measured, not estimated.

This does **not** supersede `HANDOFF-2026-08-10.md`, which covers the arc. It is narrower: how to
finish SPEC-039 without re-deriving what its first two phases already settled.

## Where things stand in one line

**SPEC-039 Phases 1 and 2 are merged and `main` is green; Phase 3 is FR-004 and Phase 4 is FR-005
plus the completion ritual.**

## State

| | |
|---|---|
| `main` | `71af180` — green, every workflow including Release |
| Suite | 1612 passed, 2 skipped, 2 xfailed |
| Merged | #147 (FR-003, FR-001 AC-1/AC-3, FR-006 AC-1/AC-2/AC-4) · #148 (FR-002, FR-006 AC-2/AC-3, FR-001 AC-2) |
| Left | **FR-004** (Phase 3), **FR-005** (Phase 4), then the completion ritual |
| Spec file | `docs/specs/SPEC-039-fork-lifecycle.md`, still `Status: Draft` |

The spec was amended once during Phase 1 — FR-002 AC-2 now records that the queue **object** must
be replaced rather than drained, and why the retired path needs it too. That amendment is on
`main` and is already implemented.

## Start here

1. **Read `_fork.py`'s `_reinit_after_fork` (line 413) before planning.** It is a two-step
   contract today — `_reinit_primitives()` at line 440, then the handler loop at line 443 — and
   FR-004's discard is the **middle** step. It must be **inline between the two**, not a
   registered handler. `decorator` registers first and its rebuild starts a live drain thread, so
   anything registered after it runs with a thread already emitting. This is recorded in that
   function's docstring and in `register_child_handler`'s; do not rediscover it by deadlock.
2. FR-004 AC-5's lint derives from the sink roster in `tests/test_sink_concurrency.py`
   (`_sink_classes_with_an_emit`, ~line 633). Reuse it — it is already defines-or-inherits scoped
   and floored. `sinks/file.py` is the only module in `src/` that calls `open()` into a `self`
   attribute (lines 55, 185, 350); `StdoutSink` is deliberately out of scope and FR-005 AC-3 says
   why.
3. FR-005 is documentation and `architecture.md` §13 records. Take it last, as the spec says — its
   AC-2 and AC-3 name what the first three phases turned out not to reach, and Phases 1 and 2
   added three more items to record (below).

## What Phases 1 and 2 shipped that Phase 3 must know

- **`_fork.py` imports only `_diag`.** A test enforces it, and it checks *every* imported name —
  an earlier version asked whether the names *intersected* the allowed set, which would have let
  `from log_foundry import _diag, decorator` through. FR-004's hook is probed by name off objects
  the traversal already reaches; it needs no new import.
- **The traversal reaches owned instances, owned classes, and plain containers, keyed on the whole
  MRO.** A user's `class MySink(FileSink)` is repaired; a separately held third-party client is
  not. `FileSink`/`RotatingFileSink` are reached, so the discard hook can be called from the walk.
- **An AST lint forbids building a lock anywhere the walk cannot write it back** (module/class
  namespace or `self.<attr>`), and a second lint refuses `Condition`/`Semaphore`/
  `BoundedSemaphore`/`Barrier` outright, because `_fresh_primitive` cannot replace them. If
  Phase 3 adds state, it must satisfy both.
- **The worker rebuild is in place and the queue object is replaced.** Guards keyed on
  `_worker.sink is X` still hold across a fork.
- **`decorator.py`'s new guards are on the SPEC-035 roster.** Any worker question Phase 3 adds
  there must be classified with a reason, or `test_worker_predicate_roster.py` fails. It flagged
  all three of Phase 2's before a line of test was written, and one classification changed the
  code: `resume` is a hoisted binding rather than an inline keyword argument, because **a keyword
  argument is not a position the roster files**.

## Measurements already taken — do not re-derive

Phase 1 and 2 measured these. The spec's own "Prior work, carried across" section has four more.

| Claim | Measured |
|---|---|
| Inherited `Lock` is locked with no owner | `acquire(timeout=1)` → `False` |
| Child's first log call blocks on it | 60 of 60 with the drain thread parked inside `emit` |
| A user's subclass of a shipped sink hung where a plain `FileSink` returned | child killed by `SIGALRM`; plain sink returned |
| Inherited started `Thread` in a child | reports dead; bounded **and** unbounded `join` return in 0.0000 s |
| Traversal cost | 0.45 ms idle; 202 ms with a `MemorySink` holding 100k events |
| Retired child forked mid-`shutdown()` | paid the full 30.00 s budget at exit before the fix |
| A quiet worker does **not** hold submissions in its queue | `qsize` 5 → 0 in 10 ms — `_drain`'s `get` dequeues immediately |

That last one matters for FR-004: **a long `flush_interval` does not park a drain thread.** Use the
`_Gate`/`_GatingStream` machinery in `tests/test_fork_lifecycle.py` to construct the window.

## Method lessons from seven review rounds

Two PRs, seven adversarial review rounds, six blocking findings. **Only one was a behavioural
defect the diff introduced** (the retired child's 30 s exit, found by a reviewer probing, not
reading). Every other was the *evidence* being weaker than it claimed. Budget for that.

1. **Mutate scoped to the function under test.** Five counter lines in `Worker._reinit_after_fork`
   have twins in `Worker.__init__`; a first sweep removed the `__init__` occurrence and reported
   kills for the wrong site. Partition the source on the `def` line before replacing.
2. **A statement with no killing test is the default, not the exception.** Rounds 1, 2 and 3 each
   found one. Sweep every statement in a new function individually and expect survivors.
3. **Do not claim a kill in a commit message without having run it.** Three rounds blocked on
   exactly this. It is cheaper to write "unkilled, and here is why" than to be caught.
4. **Construct the window; never hope the timing lands there.** A test that forks after `emit`
   returns exercises the non-hazard — the lock is free and the buffer empty by construction. That
   is the trap the spec warns about, and it is *precisely* FR-004's subject.
5. **A bounded negative needs a gap, not a barrier.** There is no event for "nothing will ever
   happen", so a wait is the only expression — make the gap large (ms of real work against a
   second of waiting) and assert the precondition.
6. **`fork` clears a pending alarm in the child, and the library's handler runs before any test
   code.** A repair that hangs *in the handler* produces a child no alarm can kill. `run_in_child`
   holds a parent-side deadline and `SIGKILL` for that reason — keep it.

## To record in Phase 4 (FR-005 / architecture.md §13)

Found during Phases 1 and 2 and not yet written down anywhere but here and the code:

1. **A third-party *sink*'s own locks are not repaired** — it is outside the traversal's ownership
   boundary. The worker rebuild re-offers the stop signal to cover the one consequence that
   would otherwise break SPEC-027; the sink's own locks remain the caller's problem.
2. **A class mixing a third-party base in alongside a library one has its foreign attributes
   replaced too.** Measured on `class MySink(FileSink, ThirdPartyBase)`. A separately *held*
   client is still untouched, which is the boundary FR-005 AC-1 states — the two cannot be told
   apart from the instance.
3. **Both parent and child hold an orphan-path close record for a shared sink**, so each closes
   its own copy at exit. This is FR-005 AC-1's hazard (one handle, two processes) and was left
   deliberately rather than fixed.
4. **`Worker._reinit_after_fork` installs `self._thread` only after `start()` succeeds**, so a
   live drain thread briefly coexists with the inherited dead one in that attribute. Safe only
   because the drain thread never reads it — an invariant stated in the docstring and enforced by
   nothing.

## Commands

```bash
poetry run pytest -q && poetry run ruff check . && poetry run mypy
```

```bash
poetry run pytest tests/test_fork_lifecycle.py -q -p no:randomly
```
