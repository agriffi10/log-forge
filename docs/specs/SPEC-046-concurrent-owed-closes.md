# Spec: Concurrent Owed Closes

**ID:** SPEC-046
**Status:** In Progress
**Last Updated:** 2026-09-01
**Depends On:** SPEC-027, SPEC-030, SPEC-031, SPEC-033, SPEC-045

## Overview

`shutdown()` now costs one slow sink's `close()` **multiplied by the number of sinks owed one**.
SPEC-045 made the orphan path's owed-close record hold every sink rather than one, which is what
stopped the live sink going unclosed — but it left `_close_orphan_sink` draining that record
inline and in sequence. Measured against a 1.0 s budget with 2-second closes: one owed sink takes
2.00 s, two take 4.01 s, four take 8.02 s. The single-sink cost is the pre-existing constraint
that `shutdown(timeout=…)` cannot bound a `Sink.close` that takes no timeout; the **multiplication
is new**, and it is this spec's whole subject. The owed closes run concurrently and are all waited
for, so the cost becomes the slowest close rather than their sum, and nothing is abandoned.

## Scope

### In Scope

- `_close_orphan_sink` running the owed closes **concurrently** and joining every one of them, so
  the caller's cost is `max` rather than `sum`.
- Keeping SPEC-045's guarantee exactly: every owed sink is closed once, and nothing it buffered
  is stranded.
- Keeping the one-owed-sink path unchanged — no thread, no new cost, the same code today's tests
  measure.
- Closing the `architecture.md` §12 entry this spec exists for, and correcting the sites that
  describe the exit close as running the owed sinks in sequence.

**Why not the detached-closer machinery.** SPEC-030 built `_start_closer` / `join_closers` /
`DEFAULT_CLOSER_GRACE` for a swapped-out sink, and reusing it here was the obvious first design.
It was built and measured, and it loses data: `join_closers` caps at `DEFAULT_CLOSER_GRACE`
unconditionally, so a superseded sink whose `close()` exceeds 2 s is abandoned and killed at
interpreter exit — measured, `shutdown(timeout=1.0)` with four 2-second closes completed **1 of
4**, and a 3-second close delivered nothing where today it delivers. It also reintroduced
SPEC-044's double grace (4.02 s against a 2 s grace) and ran into the §13 entry recording that a
daemon close of *this* sink was built and reverted, because a daemon killed at exit can be inside
`SQLiteSink.commit()`. Joining every close avoids all three: nothing is a daemon anyone abandons,
so nothing is killed mid-write, no grace is charged, and no close is lost.

### Out of Scope

- **Bounding an individual `Sink.close`.** It takes no timeout, and giving it one changes a
  published protocol. `shutdown(timeout=…)` still does not bound the exit close, exactly as §13
  records — one stuck sink still hangs the exit, as it does today. This spec removes the
  multiplication and narrows nothing else.
- **`_delivering_to_an_inherited_sink` reading the record's last entry**, which stays §12's open
  item. This spec settles which sink closes inline, which is a different question.
- `Worker._close_sink` and the worker's own shutdown ordering, which close one sink and are
  unchanged.
- The other five §12 open items.

---

## Functional Requirements

### FR-001: The owed closes run concurrently, not in sequence

#### Description:

`_close_orphan_sink` closes one owed sink on the calling thread and the rest on threads of their
own, then joins every one of them before returning. The caller's cost becomes the slowest close
rather than their sum.

The inline sink is the **configured** sink where it is among those owed, and otherwise the most
recently armed. The config is the authority for which sink is being delivered to — that is §12's
own reasoning for its fourth open item — and keeping that one on the caller's thread is what
preserves SPEC-030's recorded decision that `shutdown()`'s own close stays inline. The fallback
exists so that the single-owed-sink case never spawns a thread (FR-003). This deliberately does
**not** copy `_delivering_to_an_inherited_sink`, which takes the record's last entry and whose own
docstring says neither end of the record is authoritative for "installed".

#### Acceptance Criteria:

- [ ] Four owed sinks whose `close()` each take 2 s: `shutdown()` returns in **under 4 s**, where
      today it takes 8.02 s.
- [ ] In that same scenario **all four closes complete**, asserted on each sink having finished —
      not on elapsed time, since a shorter total is also what dropping a close produces.
- [ ] The closes **overlap**: each starts within one close-duration of the first.
- [ ] The configured sink's close has completed by the time `shutdown()` returns, and ran on the
      calling thread.
- [ ] Where the configured sink is not among those owed, the most recently armed one runs inline.

### FR-002: Every owed sink is still closed exactly once, and nothing is stranded

#### Description:

SPEC-045's guarantee is what this must not trade away, and the first design did trade it. Every
close still goes through `release()`, so the ownership refusal and the `_closing_now` bracket are
unchanged; and every close is **joined**, so none is abandoned to a daemon that interpreter exit
kills. A sink whose `close()` *is* its delivery still delivers, however long it takes.

#### Acceptance Criteria:

- [ ] With a buffering double whose `close()` is its delivery and takes **longer than
      `DEFAULT_CLOSER_GRACE`**, every owed sink's buffer is delivered — the case the rejected
      design lost.
- [ ] Every owed sink is closed exactly once across the whole shutdown, on the orphan path and the
      worker path.
- [ ] A close that raises is absorbed and announced as it is today, and does not stop the other
      owed sinks being closed or joined — asserted **on both the calling thread and a fan-out
      thread**, and on stderr rather than only on counters. The thread half is the one this
      change introduces, and a `_close_owed` that re-raises only there passes the whole suite
      without it (measured); a close that raises still increments its own counters, so counters
      alone cannot tell guarded from unguarded.
- [ ] A forked child still refuses to close what it inherited. The refusal lives in
      `releasable()`, which every close reaches through `release()` whichever thread it is on, and
      `tests/test_owed_closes.py` already pins it; this criterion is satisfied by that test
      continuing to pass rather than by a new one on the fan-out, and says so rather than
      implying coverage it does not add.
- [ ] `tests/test_owed_closes.py` and `tests/test_orphan_sink_handoff.py` pass **unedited**, in
      particular the two that pin the live sink's close as inline and unbounded.
      `tests/test_lifecycle_races.py` is edited in one direction only: its under-lock lint matches
      call names and cannot see through a rename, so the new helper is added to the offender set.
      A widening, never an accommodation — no assertion is relaxed.

### FR-003: One owed sink costs exactly what it costs today

#### Description:

The common case is one owed sink, and it must not acquire a thread, a join, or a measurable cost
for a concurrency it does not need. That is also what keeps this spec's blast radius honest: the
change is invisible to every process that has only ever had one sink.

#### Acceptance Criteria:

- [ ] With one owed sink, no thread is created for the close — asserted on the thread count or on
      the close running on the calling thread, not inferred from timing.
- [ ] With one owed sink the code path is the same one as today — the close is performed by the
      same call on the same thread, which AC-1 asserts. A wall-clock "unchanged within noise"
      criterion is deliberately **not** used: it is satisfied by any implementation fast enough,
      including a wrong one, and it is the flaky-bound shape this repo has reverted before.
- [ ] `health().closing_sinks` is unchanged by this path: these closes are joined before
      `shutdown()` returns, so none is ever outstanding when it does.

### FR-004: The docs stop describing an exit close that runs in sequence

#### Description:

`architecture.md` §12 carries this as an open item and it is closed by being fixed, so it moves to
*Resolved* with the spec that closed it (SPEC-021's rule). §13's `shutdown()`-timeout entry says
the cost is the sum; `_close_orphan_sink`'s docstring describes a single inline close. Both stop
being true. `_lifecycle.py` also still points at §13 for the `inherited_sink` limit, which
`dcb07c3` moved to §12.

#### Acceptance Criteria:

- [ ] §12's exit-close entry moves to *Resolved*, naming this spec.
- [ ] §13's `shutdown()`-timeout entry says the exit cost is now the slowest owed close rather
      than their sum, and that a single close is still unbounded.
- [ ] `_close_orphan_sink`'s docstring states the split, why the configured sink is the inline
      one, and why every close is joined rather than detached.
- [ ] The stale `§13` pointer for the `inherited_sink` limit in `_lifecycle.py` names §12.
- [ ] No **live** doc says the exit close runs the owed sinks in sequence, verified by grepping
      the phrases SPEC-045 introduced. SPEC-045's own spec and delivery doc keep the phrase: they
      are records of what was true when written, which is the carve-out SPEC-021's rule implies
      and this criterion states rather than leaves to judgement.

---

## Data Model

No new types, no new module state. `_close_orphan_sink` gains a local split of the owed list into
one inline sink and the rest, and a list of joined threads.

---

## API / Interface Contract

No public signature changes. `shutdown(timeout=…)` keeps its meaning and its documented limit.

## Configuration / Environment

None. `DEFAULT_CLOSER_GRACE` is untouched and unused by this path.

## File & Folder Structure

```
src/log_foundry/
└── _lifecycle.py      # _close_orphan_sink only

tests/
├── test_concurrent_owed_closes.py  # new — FR-001..FR-003
├── test_owed_closes.py             # must pass unedited (FR-002 AC-5)
├── test_orphan_sink_handoff.py     # pins the live sink's close inline and unbounded
└── test_lifecycle_races.py         # pins the daemon-close revert

docs/
├── architecture.md    # §12 entry to Resolved; §13 timeout entry
└── specs/INDEX.md     # status row + arc entry
```

## Implementation Phases

### Phase 1: The fan-out (FR-001, FR-002, FR-003)

- Commit the reproduction first and observe today's linear cost.
- Split the owed list; run the rest on threads; join all of them.
- Mutation-test the join: a fan-out that does not join passes an elapsed-time assertion and loses
  closes, which is the confound FR-001 AC-2 exists to catch.

### Phase 2: The docs (FR-004)

- §12 entry to *Resolved*; §13's timeout entry; `_close_orphan_sink`'s docstring; the stale
  `inherited_sink` pointer; the grep.

---

## Revision history

- **2026-09-01** — authored, measured on `main` at `dcb07c3` before any design: against
  `shutdown(timeout=1.0)` with 2-second closes, 1 owed sink 2.00 s, 2 owed 4.01 s, 4 owed 8.02 s,
  and with 5-second closes 2 owed took 10.01 s. Linear in the number owed, which is what makes the
  multiplication rather than the single close this spec's subject.
- **2026-09-01, after the spec review** — the design was replaced. ~~The superseded owed sinks are
  released **detached**, through the `_start_closer` / `join_closers` / `DEFAULT_CLOSER_GRACE`
  machinery SPEC-030 built.~~ The reviewer built that mechanism and measured it: with four owed
  2-second closes against a 1.0 s budget it completed **1 of 4**, because the grace is what remains
  of the shutdown budget and a 2 s inline close leaves none; and independently, `join_closers` caps
  at `DEFAULT_CLOSER_GRACE`, so any owed sink whose close exceeds 2 s loses its buffer where today
  it delivers. It also measured 4.02 s against a 2 s grace — SPEC-044's double charge, verbatim —
  and it walked into the §13 entry recording that a daemon close of this same sink was built and
  reverted because interpreter exit can kill it inside `SQLiteSink.commit()`. Joining every close
  rather than detaching any avoids all three, and is strictly better than today on both axes: the
  cost falls from `sum` to `max` and the loss stays zero.
