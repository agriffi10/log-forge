# Spec: Orphan-Path Sink Handoff

**ID:** SPEC-033  
**Status:** Draft  
**Last Updated:** 2026-08-07  
**Depends On:** SPEC-026, SPEC-028, SPEC-030, SPEC-031

## Overview

`configure(sink=...)` promises that the previous sink is drained, closed, and must not be handed
back to a later call. That promise holds only when a background worker exists. A process that logs
exclusively through the level calls — `info()`, `warning()` and friends, with no `@trace` anywhere —
never builds one, so the promise silently does not apply to it: the previous sink is left open, its
locally-buffered events are never delivered, and `health()` reports nothing at all, because the
field that would report it (`incomplete_swaps`) describes a worker that does not exist.

This is the last item from the 2026-08-05 audit arc. SPEC-031 FR-006 fixed the *shutdown* half of
the same root cause — a process with no worker now closes its sink exactly once at exit — and
explicitly scoped this half out, recording it in `architecture.md` §13 so that striking through the
paragraph it replaced would not be read as closing this variant too. This spec is the home §13 says
it needs.

The fix is small in behaviour and specific in mechanism. The library already knows which sink an
orphan log reached — it resolves one immediately before emitting — but records only *that* one was
reached, not *which*. Recording the identity is what lets a late `configure(sink=...)` close it. The
close itself reuses the bounded, abandonable closer SPEC-030 built for the worker path rather than
inventing a second one, and that machinery moves to a module both paths can reach, because a
facility used by a path with no worker does not belong to the worker.

**Measured, on `f17edd4`**, `configure(sink=A)` → `info()` → `configure(sink=B)` → `info()` →
`shutdown()`:

```
A.closed = False   A.held = 1     <- one event, never delivered, sink never released
B.closed = True    B.held = 1
incomplete_swaps = 0   closing_sinks = 0   retired = True   failed_batches = 0
```

The same sequence with one `@trace` call ahead of it — which builds the worker — closes A correctly
and is the control that isolates the defect to the no-worker path.

## Scope

### In Scope

- Recording the **identity** of the sink an orphan emit reached, not merely that one was reached.
- Closing that sink when a late `configure(sink=...)` retargets a process that has no worker.
- Bounding that close on the same terms as the worker path's, and granting it the same exit grace.
- Making `health().closing_sinks` reachable on the no-worker path.
- Extracting the daemon-closer machinery into one module both paths use.
- Recording the resolution in `architecture.md` §13 and correcting `configure()`'s docstring, whose
  swap paragraph currently describes behaviour this path does not have.

### Out of Scope

- **Any new `Health` field.** SPEC-030 settled that vocabulary and SPEC-031 declined to extend it
  for the same root cause. `closing_sinks` becomes *reachable* on this path; it is not new, and
  nothing else is added.
- **Widening `incomplete_swaps`.** FR-005 settles it in the other direction — it keeps its
  worker-only meaning, and this path never moves it. That is a decision, not an omission.
- **Creating a worker to perform the swap.** The refusal SPEC-030 FR-003 made, SPEC-031 FR-006
  repeated, and `_flush_worker` and `_worker_health` also make: standing up a thread to prove there
  is nothing to drain is pure cost.
- **Draining or fencing the orphan path.** There is nothing buffered to drain — an orphan emit is
  synchronous and has returned before `configure()` is entered on the same thread — and the one
  concurrent writer a fence could not exclude is the same one the *worker* path cannot exclude
  either. FR-002 states the contract that already covers it.
- **Making `configure()` thread-safe.** It remains a startup call, as its docstring says and as
  `Worker.swap_sink` restates. This spec does not change that and does not make a concurrent
  orphan emit during a swap deterministic.
- **Bounding the *live* sink's close at shutdown.** SPEC-031 FR-006's `_close_orphan_sink` closes
  inline and unbounded, matching `Worker._close_if_owed`, and arch §13 records why running that one
  on a daemon was built and reverted. Only a **swapped-out** sink's close is bounded here.
- **Closing a sink configured after `shutdown()`.** `configure(sink=B)` → `info()` on a retired
  orphan-only process leaves B open, because the close is once-only across the process (SPEC-031
  FR-006) and logging after `shutdown()` is a documented user error that SPEC-030 settled as
  reported rather than prevented. FR-001 AC-7 pins this as intended, not incidental.
- **Reviving the worker path's swap semantics on this path.** No `_offer_stop_signal`, no double
  drain, no `_record_incomplete_swap`. What is shared is the *close*, not the swap protocol.

---

## Functional Requirements

### FR-001: The orphan path records which sink it wrote to

#### Description:

`decorator._orphan_close_owed` is a boolean: it records that *a* sink was written to, which is
sufficient for a close at exit (where `_ensure_sink()` still returns that same sink) and
insufficient for a close at swap time (where `configure()` has already reassigned `_config.sink`
before the swap runs — see `config.py:128,143`, the assignment precedes `_swap_live_sink`). By the
time anything could close the old sink, the config no longer names it.

Replace the flag with the sink object. `api._log` resolves `_ensure_sink()` immediately before
emitting and is the only place in the library that holds that identity; it passes it to
`_note_orphan_emit`, which stores it.

The two properties SPEC-031 FR-006 established are preserved verbatim, and both are load-bearing:
arming is keyed on an event **reaching** a sink rather than on a sink being **configured** (because
`configure()` runs `_ensure_sink()` unconditionally and would arm a close over a `StdoutSink`
nothing was written to), and arming happens **before** the emit rather than after it (because
SPEC-026 FR-001 makes a total failure raise, and a sink that raised is still a sink whose socket
must be released).

#### Acceptance Criteria:

- [ ] AC-1: After an orphan emit, the library holds a reference to the exact sink object that emit
      was made against — identity, not a lookup deferred to close time.
- [ ] AC-2: Reassigning `_config.sink` between the emit and the close does not change which sink is
      closed. A test asserts this directly, since it is the property the boolean lacked.
- [ ] AC-3: A process that calls `configure()` and never makes an orphan emit records nothing, and
      closes nothing at swap or at exit (SPEC-031 FR-006 AC-8, unchanged).
- [ ] AC-4: A sink whose orphan `emit` **raised** is still recorded and still closed.
- [ ] AC-5: The hot path stays one unlocked read plus an identity comparison; the lock is taken only
      when the recorded sink is not the one being emitted to.
- [ ] AC-6: The reference is cleared when the sink is closed or when a worker takes ownership, so a
      swapped-out sink becomes collectable rather than pinned for the process lifetime.
- [ ] AC-7: An orphan emit made **after** the once-only close has been performed does not re-arm it.
      A refused post-`shutdown()` emit against the closed sink therefore cannot cause a second
      `close()` on it — the outcome SPEC-031 FR-006 ranks worse than an unclosed sink.
- [ ] AC-8: `tests/conftest.py`'s reset fixture covers the new state; no test leaks a recorded sink
      into the next test.

### FR-002: A late `configure(sink=...)` with no worker closes the sink the orphan path was using

#### Description:

`decorator._swap_sink` returns early when `_worker is None`. That early return is correct on its own
terms — a process with no worker has captured no sink to *swap* — but it is currently the whole
function for this path, so nothing performs the handoff at all. Give it a second branch: with no
worker and a recorded orphan sink that is not the incoming one, close the recorded sink and clear
the record.

No drain and no fence. The worker path drains twice because events sit in a queue and because the
drain thread may be inside the old sink's `emit`; neither is true here. Orphan events are emitted
synchronously on the caller's thread and have returned before `configure()` is entered. The one
writer that could still be inside the old sink's `emit` is an orphan emitter on *another*
application thread — and that is precisely the writer `Worker._close_swapped_out` documents itself
as **not** covering either, which is why `sinks/base.py` requires `close()` to tolerate a concurrent
`emit` (SPEC-028 FR-001) and why the sinks holding transport state take their lock in both. This
path inherits that contract unchanged; it does not weaken it.

#### Acceptance Criteria:

- [ ] AC-1: `configure(sink=A)` → `info()` → `configure(sink=B)` calls `A.close()` exactly once,
      with no `@trace` anywhere in the process.
- [ ] AC-2: After that swap, `info()` reaches B and not A.
- [ ] AC-3: A subsequent `shutdown()` closes B, and does not close A a second time.
- [ ] AC-4: `configure(sink=A)` → `configure(sink=B)` with no orphan emit between them closes
      nothing (AC-3 of FR-001, asserted end to end).
- [ ] AC-5: `configure(sink=A)` → `info()` → `configure(sink=A)` — the same object — closes nothing
      and leaves the record armed, so A is still closed at exit. This mirrors
      `Worker.swap_sink`'s `self.sink is new_sink` no-op.
- [ ] AC-6: With a worker present, this branch closes nothing and `Worker.swap_sink` still owns the
      close. A test covers the mixed process — orphan log, then `@trace`, then the swap — and
      asserts exactly one `close()` on the old sink. This is the case an independent review of
      SPEC-031 FR-006 found uncovered, and it is the one most likely to double-close.
- [ ] AC-7: A `close()` that raises is absorbed and announced through `_diag.absorbed`; `configure()`
      returns normally and the swap still stands (SPEC-025 FR-004).
- [ ] AC-8: After the swap, the next orphan emit re-arms the record against the new sink, so B is
      closed at exit by `_close_orphan_sink` on the path SPEC-031 FR-006 built.
- [ ] AC-9: Each assertion above is mutation-tested — the branch is stashed and the test re-run — so
      no criterion is ticked by a test that passes against the defect it claims to catch.

### FR-003: The swapped-out sink's close is bounded, abandonable, and granted the exit grace

#### Description:

Close it on the shared daemon closer of FR-004 rather than inline. The reason is SPEC-030 FR-003's,
measured again here: a sink that hangs in `close()` blocks the caller for as long as it hangs, and
`configure()` is on the application's startup path. **Measured on `f17edd4`** against a sink whose
`close()` sleeps 8 s, the worker path returns in 5.00 s — its `DEFAULT_SWAP_TIMEOUT` — and reports
`closing_sinks = 1`. An inline close here would take the full 8 s and report nothing, reintroducing
on this path the exact gap arch §13 records as closed for the other one.

Everything SPEC-030 settled about that closer carries over unchanged and is not re-litigated: the
thread is a daemon; the join is capped; **an expired join derives no signal** — no counter moves and
no line is written, which is what dissolved SPEC-028's wrong-signal objection — and the live fact is
published as `health().closing_sinks` instead. `shutdown()` on this path must grant an outstanding
closer the same `DEFAULT_CLOSER_GRACE`, carved from its own budget, after the live sink's inline
close; without it a daemon closer that is slow but *succeeding* is killed at interpreter exit,
losing the buffer of a sink whose `close()` is its delivery — the loss `Worker._join_closers` exists
to prevent, and one this path would otherwise take.

#### Acceptance Criteria:

- [ ] AC-1: A swapped-out orphan sink whose `close()` sleeps well past the budget does not hold
      `configure()` beyond `DEFAULT_SWAP_TIMEOUT`. The test keeps the gap between the budget and the
      sleep wide, not the budget tight.
- [ ] AC-2: An expired join increments no counter and writes no `_diag` line.
- [ ] AC-3: `health().closing_sinks` reads 1 while such a close is running **in a process with no
      worker**, and returns to 0 once it finishes.
- [ ] AC-4: `shutdown()` on an orphan-only process joins an outstanding closer for at most
      `DEFAULT_CLOSER_GRACE`, and for no more than what remains of its own timeout.
- [ ] AC-5: That grace runs **after** the live orphan sink's inline close, matching
      `Worker.shutdown`'s order, and a test pins the ordering rather than only the outcome.
- [ ] AC-6: The live orphan sink's own close at shutdown stays inline and unbounded — unchanged from
      SPEC-031 FR-006, and asserted so a future refactor cannot quietly move it onto the closer.
- [ ] AC-7: A `Thread.start` that fails leaves the sink open and announces it; there is no inline
      fallback, for the reason `Worker._close_swapped_out` gives — the fallback reintroduces the
      unbounded wait in the one situation where the process is already under resource pressure.

### FR-004: One process-wide closer registry, used by both paths

#### Description:

`DEFAULT_CLOSER_GRACE`, the live-closer list, the daemon spawn, the guarded close body and the
capped grace join currently live on `Worker` (`worker.py:29,229,561-646,746-793`) and are reachable
only through a worker instance. Move them to a leaf module — `src/log_foundry/_closing.py`, on the
precedent of `_diag.py`, `sinks/_retry.py` and `sinks/_batch.py` — holding process-global state, and
have `Worker` and `decorator` both call it.

Duplicating the machinery instead was considered and rejected on two grounds. First it is the
mistake SPEC-029 diagnosed: rules applied in one place stay applied, and rules remembered in two
drift — twelve of twenty-eight diagnostic sites drifted exactly that way. Second, a per-path
registry is measurably blind in a mixed process: a closer started before a worker existed would be
invisible to `health().closing_sinks` and would be denied the exit grace once the worker owned
`shutdown()` — the "every field describes a worker that does not exist" shape this arc has now
fixed three times. `closing_sinks` is inherently process-scoped; there is only ever one worker.

Keeping it at module scope inside `worker.py` was also considered and rejected: a facility whose
whole purpose here is to serve a path with **no worker** does not belong in the worker module, and
CLAUDE.md's single-concept rule applies to a file already at 1176 lines.

#### Acceptance Criteria:

- [ ] AC-1: `health().closing_sinks` reports the same number whether or not a worker exists, and a
      closer started before the worker was built is still counted after one is built.
- [ ] AC-2: `shutdown()` joins closers started on either path, under one shared grace.
- [ ] AC-3: `_closing.py` imports nothing from the package but `_diag`, and `Sink` only under
      `TYPE_CHECKING`; `mypy --strict` and the import-cycle expectations are unchanged.
- [ ] AC-4: Every SPEC-030 behaviour currently asserted through `Worker._close_swapped_out`,
      `Worker._join_closers`, `Worker._closers` and `worker.DEFAULT_CLOSER_GRACE` still holds; those
      tests are re-pointed at the new seams rather than deleted.
- [ ] AC-5: The re-point is verified by diffing `pytest --collect-only` test **names** before and
      after, not pass counts — a scripted rewrite of a test file has silently removed tests in this
      repo before.
- [ ] AC-6: `Worker.health()` no longer prunes the closer list under the worker's own lock; the
      registry's lock is its own, and `health()` remains safe to call during an emit (SPEC-026).

### FR-005: `incomplete_swaps` keeps its worker-only meaning

#### Description:

SPEC-030 defines `incomplete_swaps` as a **drain** that could not be confirmed, and pairs it with a
specific consequence: queued items may have reached the new sink instead of the old one, and the old
sink was left open. Neither half exists on this path. There is no queue and no drain, so there is
nothing to confirm; and an expired *close* join is explicitly not a signal, by the decision that
made SPEC-030's bounded close available at all.

Moving the counter here would give one field two meanings and make the alert idiom ambiguous — a
non-zero `incomplete_swaps` would no longer tell an operator whether events were misrouted or merely
whether a close was slow. It stays where it is, and this is recorded rather than left to inference,
because the obvious reading of "the swap didn't fully complete" points the wrong way.

#### Acceptance Criteria:

- [ ] AC-1: No orphan-path swap increments `incomplete_swaps`, on any path — including a close that
      raises, a close whose join expires, and a `Thread.start` that fails.
- [ ] AC-2: A test asserts `incomplete_swaps == 0` after an orphan-path swap whose close hangs past
      the budget, so the field cannot be widened later without a failing test.
- [ ] AC-3: `Health`'s docstring states that `incomplete_swaps` describes the worker's drain and
      does not cover the orphan path.
- [ ] AC-4: No field is added to `Health`.

### FR-006: Record the resolution

#### Description:

Per SPEC-021's rule, an open item is closed by being fixed, settled, or recorded — never deleted.
The §13 paragraph recording this variant is struck through in place and marked with the spec that
closed it, as SPEC-031 did to the paragraph above it. `configure()`'s docstring currently describes
a swap contract that this path does not honour; that becomes true rather than aspirational.

#### Acceptance Criteria:

- [ ] AC-1: The "One variant is not fixed and needs its own home" paragraph in `architecture.md` §13
      is struck through in place, marked **closed by SPEC-033**, with its reasoning left readable.
- [ ] AC-2: `architecture.md` §7 and §9 describe the swap as covering both delivery paths, and state
      that the close is shared while the swap protocol is not.
- [ ] AC-3: `configure()`'s docstring no longer implies a worker; its swap paragraph holds for a
      process that has never opened a span.
- [ ] AC-4: `architecture.md` §13 carries no open item introduced by this spec.
- [ ] AC-5: `sh scripts/spec-lint.sh` passes.
- [ ] AC-6: On completion: spec `Status: Completed`, the `INDEX.md` row updated, a delivery doc from
      the template, a `component-inventory.md` row for `_closing.py`, and one Key Decisions line in
      `CLAUDE.md`.

---

## Data Model

```python
# src/log_foundry/decorator.py — module state
_worker: Worker | None                # unchanged
_worker_lock: threading.Lock          # unchanged; guards all of the below
_atexit_registered: bool              # unchanged
_orphan_sink: Sink | None             # REPLACES _orphan_close_owed: bool.
                                      #   the sink an orphan emit reached and which nothing else
                                      #   has closed. None == nothing owed.
_orphan_sink_closed: bool             # unchanged: the once-only latch for the terminal close.
                                      #   also gates re-arming (FR-001 AC-7).
_orphan_retired: bool                 # unchanged: synthesizes health().retired

# src/log_foundry/_closing.py — new leaf module, process-global state
DEFAULT_CLOSER_GRACE: float = 2.0     # moved from worker.py
_closers: list[threading.Thread]
_closers_lock: threading.Lock
```

## API / Interface Contract

```python
# src/log_foundry/_closing.py
def close_detached(sink: Sink, timeout: float | None) -> None:
    """Closes a sink no longer being delivered to, on a daemon thread joined for `timeout`."""

def join_closers(timeout: float | None) -> None:
    """Gives outstanding closes their last chance, capped at DEFAULT_CLOSER_GRACE."""

def closing_count() -> int:
    """Closes running at this instant — the gauge behind Health.closing_sinks."""

# src/log_foundry/decorator.py
def _note_orphan_emit(sink: Sink) -> None: ...     # was _note_orphan_emit() -> None
def _swap_sink(new_sink: Sink, timeout: float | None = DEFAULT_SWAP_TIMEOUT) -> None: ...
                                                   # signature unchanged; gains the no-worker branch

# src/log_foundry/api.py — the one call site that changes
sink = _ensure_sink()
_note_orphan_emit(sink)
sink.emit([event])
```

No public API changes. `configure`, `shutdown`, `flush` and `health` keep their signatures, and
`Health` keeps its fields.

## Configuration / Environment

None. No new config keys, env vars or settings; `DEFAULT_CLOSER_GRACE` and `DEFAULT_SWAP_TIMEOUT`
keep their values and move or stay as FR-004 describes.

## File & Folder Structure

```
src/log_foundry/
├── _closing.py          # NEW — the shared daemon closer + grace join + gauge
├── _diag.py             # unchanged
├── api.py               # one call site: pass the sink to _note_orphan_emit
├── config.py            # docstring only
├── decorator.py         # _orphan_sink identity; the no-worker branch of _swap_sink
└── worker.py            # closer machinery delegates to _closing

tests/
├── conftest.py                  # reset fixture covers _orphan_sink
├── test_config.py               # SPEC-030 closer tests re-pointed at _closing
└── test_orphan_sink_handoff.py  # NEW — FR-001..FR-005
docs/
├── architecture.md                                    # §7, §9, §13
├── specs/{INDEX.md,SPEC-033-orphan-path-sink-handoff.md}
├── spec-delivery/SPEC-033-orphan-path-sink-handoff.md  # on completion
└── component-inventory.md                              # a row for _closing.py
```

## Implementation Phases

### Phase 1: Extract the closer

- Create `_closing.py` with the process-global registry, `close_detached`, `join_closers` and
  `closing_count`, moving `DEFAULT_CLOSER_GRACE` and the guarded close body out of `worker.py`.
- Point `Worker._close_swapped_out`, `Worker._join_closers` and `Worker.health()` at it.
- Re-point the SPEC-030 closer tests in `test_config.py`; diff `--collect-only` names to prove
  nothing was dropped (FR-004 AC-5).
- Green on `pytest`, `ruff`, `mypy` before touching behaviour.

### Phase 2: The recorded identity

- `_orphan_close_owed` → `_orphan_sink`; thread the sink through `_note_orphan_emit` from
  `api._log`; gate re-arming on the terminal latch.
- Update `_close_orphan_sink` to close the recorded sink and clear the record.
- Update `tests/conftest.py`'s reset fixture.
- Tests for FR-001, including the reassignment case (AC-2) and the post-shutdown re-arm (AC-7).

### Phase 3: The swap branch

- Give `_swap_sink` its no-worker branch: no-op checks, close via `_closing.close_detached`, clear
  the record, defer to the worker when one exists.
- Grant the grace join on the orphan `shutdown()` path, after the inline close.
- Tests for FR-002, FR-003 and FR-005 — the mixed process (FR-002 AC-6) and the hanging close
  (FR-003 AC-1) first, since they are the two most likely to be wrong.
- Mutation-test every new assertion (FR-002 AC-9).

### Phase 4: Documentation

- `architecture.md` §13 strike-through, §7 and §9 updates; `configure()`'s docstring.
- Completion ritual per FR-006 AC-6.
