---
type: Spec
id: SPEC-101
title: "A Kept Sink Behind a Caller's Wrapper Is Closed After That Wrapper, Never Beside It"
description: "Has the library close a sink it wrote to, wherever a caller's wrapper holds it, after each close of that wrapper the library makes, so a wrapper that leaves its member alone no longer leaves the member closed by nobody, a wrapper that closes its member gives it a second close in sequence rather than two at once, and the model test learns to tell a wrapper's close from the library's."
spec_status: In Progress
depends_on: [SPEC-042, SPEC-054, SPEC-076, SPEC-081, SPEC-086, SPEC-087, SPEC-097, SPEC-098]
arc: audit-2026-11-14
arc_order: 17
---

## Overview

Since SPEC-087, a sink that two sink graphs share is kept open across a `configure()` and closed
later by the graph that holds it. For the wrappers the library ships, the library makes that close
itself, through the wrapper's forward. For a wrapper a caller wrote, the close is whatever the
wrapper's own `close()` does.

A wrapper that leaves its member alone therefore leaves the member closed by nobody. In
`configure(A)` then `configure(Leaves(A))`, every event reaches A and A is never closed, which
breaks invariant 5's first promise. A wrapper that closes its member works, but where the record
also owes that member, the library closes it at the same moment, and the two closes overlap. The
library cannot tell the two kinds of wrapper apart.

This spec has the library close such a member **after** each close of its holder that the
library makes, on every path that closes a holder. It does this for as long as the holder lives,
just as a shipped wrapper's forward closes its member every time. As a result:

- The wrapper that leaves its member alone gets the member closed by the library.
- The wrapper that closes its member gets a second `close()` in sequence. The sink protocol
  already requires a sink to tolerate that, and every shipped sink that was run tolerated it.
- No close overlaps another.
- The model test gains both kinds of wrapper. Its oracle learns to tell the wrapper's close from
  the library's, so it still counts the library's own closes exactly.

## Scope

### In Scope

- Each sink the library wrote to that lies beneath a caller's wrapper is attached to the closest
  such wrapper for as long as that wrapper lives. The library closes the member after every close
  of the wrapper it makes (FR-001).
- When one decision takes both a caller's wrapper and a sink beneath it, the sink is closed after
  the wrapper rather than beside it, in `_close_owed` and in the swap's stale take. A shipped
  holder is not ordered (FR-002).
- The same rule holds on the swap, `shutdown()`, the exit drain and the orphan path, through an
  interrupt, and in a forked child. It runs inside SPEC-097's command path and SPEC-098's stable
  signal without adding a wait or a hand (FR-003).
- SPEC-086's sequential driver installs a caller's wrapper of either kind over the configured
  recorder, once the library has written to that recorder. Its repeat rule tells a wrapper's
  close from the library's (FR-004).
- The record: invariant 5, `architecture.md` §12 and §13, the decision, the README's *Writing your
  own sink*, the pending release notes and the inventory (FR-005).

### Out of Scope

- **A sink the library first writes through a caller's wrapper.** In `configure(Leaves(X))`, or
  `configure(X)` with nothing written and then `configure(Leaves(X))`, the library has written
  only to the wrapper, so X is not attached. Its close stays the wrapper's, as the README's second
  route says today. Invariant 5 promises a close to a sink the library wrote to. FR-004's draw
  installs a wrapper only over a recorder the library wrote to for this reason. Measured: drawing
  over an unwritten recorder fails seed 5 with "every sink written to is closed: [0]".
- **A displaced caller's wrapper that closes its member.** In `configure(Mine(A))` then
  `configure(A)`, `Mine.close()` closes the live sink, and every event A then refuses is counted
  and announced (`architecture.md` §13). That close is the caller's own code, made before anything
  the library could order after it. The `reverse` rows of the *Evidence* read the same on every
  tree.
- **A `sink_factory` that has not built, and a sink past the read's bound.** The read never
  reaches such a sink, so nothing is attached on its account. Both stay as §13 records them.
- **Ordering behind a holder whose `close` is the package's.** That holder's forward already
  skips a member the same decision took (SPEC-087's *closed* part), so ordering buys it nothing.
  It would also tie the member's close to the holder's slowest member: 3.0 s instead of 0.0 s in
  the `multi` row of `spec101_slow_holder.py` before this was excluded.
- **Skipping the second close of a member that reports itself closed**, and **a declaration by
  which a wrapper says it closes its members**: alternatives (b) and (c), measured in the
  *Evidence* and not chosen (*Revision history*).
- **A caller closing a wrapper themselves.** Outside a release the library started there is no
  decision and no keep, so nothing is closed after the holder. This matches SPEC-087's treatment
  of a caller closing a shipped wrapper.
- **The concurrent half of the model test, its soak (SPEC-099), and SPEC-085's cells.** None of
  them draws a wrapper. The 637 cells read the same with and without this change.
- **The stop signal of a kept sink a straddling log put back in the record.** That is SPEC-098's.
- **Changing what SPEC-087's keep spares.** This spec adds what happens to a member after the keep
  spares it.

---

## Functional Requirements

**Terms.**

- A **holder** is a sink whose installed graph (`_installed_graph`) reaches another sink, its
  **member**.
- A **caller's wrapper** is a holder whose `close` is not defined in the package. A subclass of a
  shipped wrapper that overrides `close` is one; a subclass that inherits `close` is not.
  - `Leaves` is a caller's wrapper whose `close()` leaves its member alone.
  - `Mine` is a caller's wrapper whose `close()` calls its member's.
- **Written** means the library has armed the sink as a root in its owed record in this process.
  That is the moment it delivers to the sink directly.
- A member is **attached** to a caller's wrapper when the library will close it after that
  wrapper.
- **After** means the member is closed on the thread that closed the holder, once the holder's
  `close()` has returned or raised, and before the holder's in-flight entry is discharged.

### FR-001: A written sink beneath a caller's wrapper is closed after each library close of it

#### Description:

`_CloseRegistry` records two things, guarded by its `_lock`, and each entry is dropped once its
sink is collected:

- each sink it arms (*written*);
- for each caller's wrapper, the members attached to it.

**Attaching.** At every `configure(sink=…)`, before its first critical section, every written sink
in the graph the change installs is attached. It goes to the closest caller's wrapper in that graph
that reaches it, meaning the one with the smallest graph. A sink with no caller's wrapper above it
is attached to nothing, because a shipped holder's forward closes it.

A forwarded release that SPEC-087's keep spares, because the member lies beneath a sink still in
the record, attaches the member the same way, to the closest caller's wrapper among those
remaining sinks.

**Closing.** `release()` closes the wrapper. Then, with the same decision's keep, it releases each
attached member in sequence, except:

- a member the record owes, since its own take closes it;
- a member with a close already in flight;
- a member the keep spares. If the keep spares a member because the graph a swap installs keeps
  it, the member moves to its closest caller's wrapper in that graph. A member that a shipped
  forward of the same decision already closed stays claimed, and is not closed again.

**Persistence.** The attachment outlives the close. A wrapper written to again after its close is
armed and closed again, and its member is closed again after it. A shipped `MultiSink(A)`
behaves the same way through its forward, as SPEC-045 requires of a sink written to after its
close.

The library does not ask whether the wrapper closed the member, because it cannot see that.
`Leaves(A)` gets one library close of A per close of `Leaves`. `Mine(A)` gets its own close and
then the library's, in sequence.

#### Acceptance Criteria:

- [ ] For `Leaves`, in `configure(A)` then `configure(Leaves(A))` and in
      `configure(MultiSink(A, B))` then `configure(Leaves(A))`, each with one write before and two
      after, then `shutdown()`, on both delivery paths: A receives every event, no loss term
      moves, and A is closed exactly once (invariants 2 and 5).
- [ ] `configure(A)`, a log, `configure(Leaves(A))`, a log, `shutdown()`, a log, `shutdown()`:
      A is closed twice, at write counts 2 and 3, as `MultiSink(A)` is. `Mine(A)` reads
      `[2, 2, 3, 3]` with no two closes overlapping (invariant 5).
- [ ] `lifecycle/leaves_remaining_holder.py` prints `A.closes=1 A.closed_while_inside=0` and
      `A.refused=0` (invariants 3 and 5).
- [ ] For `Mine` in the first criterion's sequences, A is closed twice with no overlap: once by
      `Mine.close()`, then once by the library after it returns. No event is refused
      (invariants 2 and 5).
- [ ] Every shipped-wrapper row of `tests/test_shared_sink_swap.py` still closes each shared sink
      exactly once (invariant 5).
- [ ] A member whose close raises an `Exception` is absorbed with one `_diag` line naming a close
      after the wrapper that holds it. The wrapper's other members are still closed (invariants 1
      and 11).
- [ ] Over 200 rounds of `configure(A)`, a log, `configure(W(A))`, a log, the two records hold
      only entries for sinks still alive. After `shutdown()` that is one attachment, for the
      configured wrapper, and two written sinks (invariant 5).

### FR-002: One decision closes a member after a caller's wrapper it takes with it

#### Description:

`_close_owed`'s take, or the swap's stale take, can take both a caller's wrapper and a sink
beneath it. That happens after an unconfirmed `configure(A)` then `configure(Mine(A))`, or after
SPEC-087's straddle. Today the take closes both at once, and `Mine.close()` closes A beside the
take's own close of A, so the two closes overlap.

So once the take has passed over what lies beneath a remaining sink (SPEC-087 FR-003), a taken
sink that lies in the graph of a taken caller's wrapper is attached to that wrapper with its
in-flight entry. Where several wrappers qualify, it goes to the closest one: the wrapper whose
graph is smallest, among those that reach the sink and that the sink's own graph does not reach.
The sink is not handed to a closer of its own. It is closed once after the wrapper, by FR-001's
path, and then dropped. Its entry stays in flight until then, so a bystander `shutdown()` waits for
it within the call's one grace.

A holder whose `close` is the package's is never ordered. Its forward already skips a member that
the decision took. Ordering behind it would only delay the member.

This costs a caller's wrapper something. A member ordered behind a slow `close()` is closed only
when that close returns. A member behind a `close()` that never returns is never closed.

#### Acceptance Criteria:

- [ ] After an unconfirmed `configure(A)` then `configure(Mine(A))`, two span writes and
      `shutdown()`, A is closed twice and the two closes never overlap. On `a38bd80c` they overlap
      (invariant 5).
- [ ] The same sequence with `Leaves(A)` closes A exactly once (invariant 5).
- [ ] `spec101_slow_holder.py`'s `multi` row, a shipped `MultiSink(A, Z)` whose Z takes 3 s to
      close, closes A at 0.0 s from the `shutdown()` call, as on SPEC-097's prototype. Its `slow`
      row, a caller's wrapper whose `close()` takes 3 s, closes A at 3.0 s (invariants 3 and 5).
- [ ] With `MultiSink(Y, Z)` and Y owed together, a swap's stale take and a closer's take each
      close Y and Z exactly once, with no overlap (invariant 5).

### FR-003: The order holds on every twin path, and inside the command path and the stable signal

#### Description:

The after-holder close lives in `release()`, the one function through which the library closes a
sink (SPEC-042). Every path that closes a holder therefore carries it:

- the swap's detached closer;
- `_close_owed`'s inline close and its fan-out threads, which serve `shutdown()`, the exit drain
  and a sink the orphan path armed;
- a shipped wrapper's forward, when the caller's wrapper is nested inside it.

**An interrupt** between a take and the hand-off of a holder puts the holder back
(`_CloseRegistry.put_back`). The holder keeps its persistent attachments. A member ordered behind
it by FR-002 is put back with its taken entry, which `put_back` discharges.

A `BaseException` raised by a member's close reaches the caller, as an inline close's does
(SPEC-025). Every member that FR-002 ordered behind the same holder and that has not been closed
yet is put back with its entry discharged.

**A forked child** never runs the after-holder close for an inherited holder, because `releasable`
refuses that holder first. The child clears both records in `_clear_after_fork`, under the lock it
already takes to clear the in-flight half, so the fork roster gains no lock.

**SPEC-097.** The member's close is part of its holder's close, inside whatever command made it. A
stop that merges into a running stop, or that a sink change runs at its end, waits for it as it
waits for the holder.

**SPEC-098.** The member has held its stable event since its first delivery, and every stop sets
it, so the after-holder close hands nothing.

#### Acceptance Criteria:

- [ ] FR-001's first criterion, ended by the exit drain instead of `shutdown()`, closes A exactly
      once behind `Leaves` and twice in sequence behind `Mine`, on both paths (invariants 5 and 6).
- [ ] A child forked after `configure(A)`, a log and `configure(W(A))` writes once and calls
      `shutdown()`. The child closes A zero times for either wrapper, and the parent then closes A
      as FR-001 says (invariant 12).
- [ ] A `KeyboardInterrupt` injected into `_inline_close_choice` after the take of `Leaves(A)` over
      a kept A leaves `Leaves(A)` owed with A still attached to it, and the next `shutdown()`
      closes A once (`lifecycle/spec101_interrupt.py`) (invariants 1 and 5).
- [ ] A member FR-002 ordered behind a holder whose member close raises `KeyboardInterrupt` is
      owed when the call returns, with no close of it in flight (invariants 1 and 5).
- [ ] On the tree carrying SPEC-097 and SPEC-098, `tests/test_model_cells.py`'s 637 cells give the
      same verdict, cell for cell, as the same tree without this change (invariant 6).

### FR-004: The model test installs a caller's wrapper, and its oracle tells the wrapper's close from the library's

#### Description:

The sequential driver of `tests/test_invariants_model.py` installs a fresh recorder at every
`configure` step. The step now draws one of three installs:

- a fresh recorder, as today;
- `Leaves(current)`;
- `Mine(current)`.

`current` is the configured sink. A wrapper is installed only when that sink is a recorder the
library has already written to. Otherwise the step installs a fresh recorder. The draw comes from
a second random stream keyed on the seed, so every seed's list of steps is unchanged. `Leaves` and
`Mine` live beside the recorder, in `tests/test_invariants_model.py`.

`Mine.close()` sets a thread-local mark around its close of its member. The recorder records each
close's mark in a new `Closes.by_wrapper` field, which defaults to empty so SPEC-085's cells,
which share the type, are unchanged.

`check_repeat` then reads two closes at one write count:

| First close | Second close | Verdict |
|---|---|---|
| the wrapper's | the library's | not a repeat: the library owed it |
| the library's | the wrapper's | a repeat |
| the library's | the library's | a repeat |
| the wrapper's | the wrapper's | a repeat |

The second row is the order FR-002 forbids. The fourth means the library closed the wrapper twice
with nothing written between.

The overlap check and `check_late_write` still read every close. The driver's own "every sink
written to is closed" assertion is unchanged.

#### Acceptance Criteria:

- [ ] Over the 24 default sequential seeds, the driver passes. `Leaves` and `Mine` are each
      installed over a written recorder in at least 12 seeds (invariant 5).
- [ ] The list of steps each seed draws is the same as on the tree before this change, so the
      gate's seeds keep their meaning (invariant 5).
- [ ] With the after-holder close removed, at least one default seed fails the driver's "every
      sink written to is closed" assertion (invariant 5).
- [ ] With attachments dropped after their first close, at least one default seed fails
      `check_late_write` (invariant 5).
- [ ] With `Mine`'s mark removed, at least one default seed fails `check_repeat` (invariant 5).
- [ ] With FR-002's order removed, a test that runs the unconfirmed `configure(A)` then
      `configure(Mine(A))` through the recorder fails the overlap check. No default seed failed
      without that order when it was measured (invariant 5).
- [ ] SPEC-085's 637 cells give the same verdicts with the new `Closes` field (invariant 6).

### FR-005: The record says what a caller's wrapper now gets

#### Description:

These are the completion ritual's edits.

- **`docs/invariants.md`, invariant 5.**
  - **Narrative.** "Closed once when its holder is released" gains: "and, where that holder is a
    caller's wrapper, closed by the library after each close of the holder it makes, if the
    library wrote to it (SPEC-101)".
  - **Exceptions removed.** "A caller's wrapper that leaves its member alone leaves a kept sink
    closed by nobody" goes, and so does "closes it beside the record's own close where the record
    also owes it, two closes overlapping".
  - **Exceptions added.** Three replace them:
    - a caller's wrapper that closes its member closes it once itself, and the library closes it
      again after, in sequence and never overlapping;
    - a member ordered behind a caller's wrapper is closed only when that wrapper's `close()`
      returns, so a slow close delays the member and one that never returns leaves it unclosed;
    - a sink the library first writes through a caller's wrapper is that wrapper's to close.
  - **Kept.** The displaced-wrapper exception stays.
  - ***Guarded*.** It names the model test's wrapper installs.
- **§12.** The item *A sink the library wrote to and never closes, behind a wrapper that leaves its
  member alone* is struck, dated, and names SPEC-101. Its table gains a column for this spec: 1
  close for `Leaves`, and 2 in sequence for the wrapper that closes its member.
- **§13.** *A sink a swap keeps is closed by the graph that holds it* changes:
  - Its fourth shape is rewritten with FR-001's counts.
  - Its third shape's "two closes overlapping" becomes two in sequence.
  - It gains FR-002's cost for a slow or hung caller's wrapper, and the shape of a sink first
    written through a caller's wrapper.
  - Its displaced half and its rejection of a deferred displaced-wrapper release stay.
- **The decision** *A swap keeps the sinks the graph it installs holds, and one decision closes a
  sink once* (`docs/decisions/pipeline/`) is refined in place, with a dated History entry:
  - It gains Part 4: a written sink beneath a caller's wrapper is attached to it and closed after
    each library close of it; one decision closes a member it takes after a caller's wrapper it
    takes; a shipped holder is not ordered.
  - Its fence adds "a written member beneath a caller's wrapper is closed after it, never beside
    it".
  - Its "never" list adds "never order a member behind a holder whose close is the package's" and
    "never skip the after-holder close on the strength of the member's type or a wrapper's
    say-so".
- **README, *Writing your own sink*.**
  - The second route becomes: "leave the member's close out of your wrapper's `close()`. The
    library closes a member it has delivered to directly after each close of your wrapper, and a
    member it never delivered to directly is yours to close."
  - New sentence: a wrapper that closes its member itself gets a second `close()` of that member
    from the library after its own, which the protocol's idempotent close absorbs.
  - New sentence: a slow `close()` delays that member's close.
- **The release notes**, in the pending `docs/release-notes/v1.2.1.md` if it is on `main`, and
  otherwise under a "Release notes owed" heading in the delivery doc:
  - The compatibility entry about a wrapper of your own that leaves its member alone is rewritten:
    the library now closes that sink after the wrapper.
  - A new compatibility entry: a wrapper of your own that closes its member now has that member
    receive a second `close()` after the wrapper's.
  - *Fixed* gains: such a member is no longer closed by two threads at once.
- **`docs/component-inventory.md`.** The `_lifecycle` row names the two records and the
  after-holder close.
- **The tests SPEC-087 wrote that pin the old counts** are rewritten to FR-001's counts:
  `test_a_wrapper_that_leaves_its_member_installed_over_a_kept_sink`,
  `test_a_callers_wrapper_forward_on_a_fenced_swap_and_with_no_worker` and
  `test_a_hoarding_wrapper_with_every_close_held_open`.

#### Acceptance Criteria:

- [ ] The fifth entry of `docs/invariants.md` no longer says a kept sink is closed by nobody or
      that two closes overlap behind a caller's wrapper. It carries the three exceptions above and
      names SPEC-101 in its narrative, its exceptions and its *Guarded* line.
- [ ] §12's item is struck and dated with SPEC-101. §13 reads 1 close for `Leaves`, two in
      sequence for the third shape, and the slow-wrapper cost.
- [ ] The decision carries Part 4, the extended fence and "never" list, and a dated History entry.
- [ ] The README's *Writing your own sink* tells no caller to close a member the library delivered
      to directly. It says a wrapper that closes its member gets a second close after its own.
- [ ] The release notes carry the two compatibility entries and the *Fixed* entry.
- [ ] This FR serves no invariant: it is the record of FR-001 to FR-004.

---

## Data Model

```python
# src/log_foundry/_lifecycle.py — private
class _CloseRegistry:
    _written: dict[int, weakref.ref[Sink] | Sink]
    """By id, each sink this process armed as a root; strong only for a type that cannot be weakly
    referenced. Guarded by ``_lock``; an entry whose sink is gone is dropped."""
    _after: dict[int, tuple[weakref.ref[Sink] | Sink, list[list[object]]]]
    """By holder id, the holder and its attached members, each ``[member, closing]``: ``closing``
    is ``None`` for a persistent attachment and the member's taken ``_Close`` for one FR-002
    ordered, which is closed once and dropped. Guarded by ``_lock``; cleared with ``_written`` and
    the in-flight half in ``_clear_after_fork``."""

class _Keep:
    root: Sink | None
    """The sink the deciding swap installs; ``None`` for the closer's take."""
```

```python
# tests/model_oracles.py
@dataclass(frozen=True)
class Closes:
    writes: int
    at_entry: tuple[int, ...]
    at_return: tuple[int, ...]
    by_wrapper: tuple[bool, ...] = ()
    """Per close, whether a caller's wrapper made it; empty reads as all False."""
```

## API / Interface Contract

No public surface changes. Private, in `src/log_foundry/_lifecycle.py`:

```python
class _CloseRegistry:
    def attach(self, holder: Sink, member: Sink, closing: _Close | None = None) -> None: ...
    def take_ordered(self, holder: Sink) -> list[tuple[Sink, _Close]]: ...
    def was_written(self, sink: Sink) -> bool: ...

def _ships_close(holder: object) -> bool:
    """Whether the holder's ``close``, read statically, is defined in the package."""

def _closest_caller_holder(sink: Sink, roots: Sequence[Sink]) -> Sink | None: ...
def _attach_written(root: Sink, graph: Mapping[int, object]) -> None: ...
def _attach_skipped(keep: _Keep, sink: Sink, previous: Sink | None = None) -> None: ...
def _release_after(holder: Sink, keep: _Keep) -> None: ...
def _order_after(taken: Sequence[Sink]) -> dict[int, Sink]: ...

# release(): after sink.close() returns or raises, and before its entry is discharged:
#     if active is not None: _release_after(sink, active)
```

```python
# tests/test_invariants_model.py
class Leaves: ...   # emit forwards; close() leaves the member alone
class Mine: ...     # emit forwards; close() marks its thread, closes the member, unmarks
```

## File & Folder Structure

```
src/log_foundry/_lifecycle.py           # the two records, the after-holder close, the take order
tests/test_after_holder_close.py        # new: FR-001 to FR-003
tests/model_oracles.py                  # Closes.by_wrapper; check_repeat reads it
tests/test_invariants_model.py          # Leaves, Mine and the configure step's install draw
tests/test_shared_sink_swap.py          # three tests rewritten to FR-001's counts
```

## Implementation Phases

### Phase 1: The records and the after-holder close (FR-001, FR-003)

- `_CloseRegistry._written`, `_after` and their methods under `_lock`; `_Keep.root`.
- The attach at each `configure()` and at a spared forward.
- `release()` runs `_release_after` before the holder's discharge.
- The put-back and the clear in `_clear_after_fork`.
- `tests/test_after_holder_close.py`, and SPEC-087's three tests rewritten.

### Phase 2: The take order (FR-002)

- `_order_after` over caller's wrappers, in `_close_owed` and the swap's stale take, after the
  pass-over.

### Phase 3: The model test (FR-004)

- `Leaves`, `Mine` and the mark; the install draw over a written recorder; `Closes.by_wrapper` and
  `check_repeat`.
- The four mutants, each run once.

### Phase 4: The record (FR-005)

---

## Evidence

Measured on 2026-11-17, Python 3.13.1 on macOS, on three trees:

- **base**: `a38bd80c`.
- **SPEC-097's prototype**: `core-pipeline/spec097_prototype.patch` applied to `git archive
  ead40798 src tests docs/audits/2026-11-14-probes`. It carries SPEC-098's part too; SPEC-098 has
  no patch of its own.
- **the prototype**: that tree with `lifecycle/spec101_prototype.patch` applied after it. It
  changes `_lifecycle.py` and builds FR-004 in `tests/model_oracles.py` and
  `tests/test_invariants_model.py`.

The prototype differs from this spec in three ways:

- It keeps its two records in module dicts with no lock, where FR-001 places them on
  `_CloseRegistry`.
- It reads `SPEC101_OPTION` to build alternatives (b) and (c).
- Its driver writes each install into the seed's history and prints a per-seed draw count.

**The sequences** (`lifecycle/spec101_sequences.py`): A's close count, and **ov** where two closes
overlapped. Base and SPEC-097's prototype read the same in every row. Every row delivered every
event with no loss term moved, except `reverse` with `Mine`, which reads the same on every tree.

| Sequence | Path | `Leaves`: base, 097 | `Mine`: base, 097 | `Leaves` | `Mine` | (b) `Mine` | (c) `Mine` declared |
|---|---|---|---|---|---|---|---|
| installed, multi, retired | both | 0 | 1 | 1 | 2 | 2 | 1 |
| exit drain | both | 0 | 1 | 1 | 2 | 2 | 1 |
| rearmed: `at_entry` | orphan | `[]` | `[2, 3]` | `[2, 3]` | `[2, 2, 3, 3]` | `[2, 2, 3, 3]` | `[2, 3]` |
| unconfirmed | spans | 1 | 2 **ov** | 1 | 2 | 2 | 1 |
| fork (child / parent) | orphan | 0 / 0 | 0 / 1 | 0 / 1 | 0 / 2 | 0 / 2 | 0 / 1 |
| A a `FileSink` subclass, installed and multi | orphan | 0 | 1 | 1 | 2 | 1 | 1 |
| reverse (displaced wrapper) | both | 1 | 2, refused | 1 | 2, refused | 2, refused | 2, refused |

SPEC-086's oracles give these verdicts on A's record:

- `Leaves` on base: `never-closed@<count>`.
- `Mine` on the prototype: `repeat@<count>` without the mark, hence FR-004.
- `Leaves` on the prototype: green in every row.

`leaves_remaining_holder.py` prints `A.closes=0` on base and `A.closes=1 A.closed_while_inside=0
... A.refused=0` on the prototype.

**The rearmed sequence** is the spec review's finding. The first prototype attached a member only
at a cede or a skip and dropped the attachment after one close. Behind `Leaves` it read
`at_entry=[2]` with 3 writes, where `MultiSink(A)` reads `[2, 3]`, so A was written after its last
close. Built into the model test, that first prototype failed `check_late_write` in 8 of the 24
sequential seeds.

**What each alternative costs**, measured above:

- **(a)** `Leaves` gets one library close per close of the wrapper. `Mine` gets its own close and
  then the library's, in sequence.
- **(b)** Same as (a), except a member whose shipped guard reads closed is skipped. That helps
  only the sinks holding a `ClosedGuard` at `_guard`. At `a38bd80c` those are `FileSink`,
  `RotatingFileSink`, `KafkaSink`, `ClickHouseSink`, `AzureEventHubsSink`, `MongoDBSink`,
  `RedisStreamsSink`, `RedisListSink`, `RabbitMQSink`, `PostgresSink` and `SQLiteSink`. On those
  sinks a second close already returns at once.
- **(c)** A declared `Mine` gets one close. It costs new public surface, and an undeclared `Mine`
  reads as (a).
- **(d)** Leave the base column as it is: `Leaves` stays at 0.

**FR-004 built** (the prototype's tests). Over the 24 default sequential seeds and the seed-range
test, 25 of 25 pass.

- **Draws.** `Leaves` was installed 23 times in 21 seeds and `Mine` 18 times in 16. A wrapper drawn
  over a configured sink that was not a written recorder installed a fresh recorder: 37 times for
  `Leaves` and 38 for `Mine`.
- **Each mutant**, run once:

  | Mutant | Seeds failing | Assertion that fails |
  |---|---|---|
  | No after-holder close (SPEC-097's prototype with the same tests) | 18 of 24 | "every sink written to is closed" |
  | Attachments dropped after one close | 5 | `check_late_write` |
  | `Mine`'s mark removed | 14 | `check_repeat` |
  | FR-002's order removed | none | — |

  With FR-002's order removed, no default seed failed, while `spec101_sequences.py`'s unconfirmed
  `Mine` row read `overlaps=1`. So FR-004 holds that order with a test of its own.

**The cells.** All 637 of SPEC-085's cells were run with pins and withholding ignored, by a scratch
module calling `test_model_cells.run_cell`. SPEC-097's prototype, this prototype's `src` alone,
and this prototype with its tests gave the same verdict cell for cell, 11 red on each.

**A slow or hung holder** (`lifecycle/spec101_slow_holder.py`). The setup is an unconfirmed
`configure(A)`, then a holder over A, then `shutdown(timeout=0.5)`. Times are seconds from the
`shutdown()` call to A's close.

| Holder | SPEC-097's prototype | the prototype |
|---|---|---|
| `slow`: a caller's wrapper whose `close()` takes 3 s | 0.0 | 3.0 |
| `hung`: a caller's wrapper whose `close()` waits 20 s | 0.0 | 20.01 |
| `multi`: a shipped `MultiSink(A, Z)` whose Z takes 3 s | 0.0 | 0.0 |

On both trees the `shutdown()` call returned only when the holder's inline close did. A holder
whose close never returns therefore leaves its member unclosed. Before FR-002 excluded shipped
holders, the `multi` row read 3.0 s, as the spec review measured.

**An interrupt** (`lifecycle/spec101_interrupt.py`). After the injected interrupt, the next
`shutdown()` closes A once on the prototype (`A.at_entry=[2]`). On base and on SPEC-097's
prototype it closes A never, since nothing attaches it. The first prototype also lost A, because
its put-back dropped every attachment.

FR-003's rule for a member's `BaseException` is in the prototype but was not exercised: no probe
raises one from a member's close.

**A second close in sequence** (`lifecycle/spec101_second_close.py`) was run on each shipped sink
that needs no server: `StdoutSink`, `MemorySink`, `NullSink`, `CallbackSink`, `LoggingSink`,
`FileSink`, `RotatingFileSink`, `SQLiteSink` and `HTTPSink`. Each was closed twice without raising.

**Bounded** (`lifecycle/spec101_rounds.py`). Over 200 rounds, the prototype ended with one
attachment, for the configured wrapper, and two written sinks: A and that wrapper. That held at
the last round and after `shutdown()`. `Leaves` read `A.closes=1` and `Mine` 201, against 0 and
200 on base. `Mine`'s count is the displaced half, which closes the live A every round on every
tree.

**The suite.** The seven files are `test_shared_sink_swap.py`, `test_owed_closes.py`,
`test_shutdown_mid_swap.py`, `test_sink_ownership.py`, `test_invariants_model.py`,
`test_fork_lifecycle.py` and `test_lock_held_discharges.py`.

- **SPEC-097's prototype alone.** Run together under `-n 8`, it fails 12. Run serially it fails 16,
  because four more fail in that order and not under `-n 8`: `test_the_soak_is_wired[*]` and
  `test_the_soak_records_the_worker_path`. They pass in the same serial order on the unpatched
  tree. So they come from SPEC-097's prototype leaving state behind between tests, not from this
  change and not from chance.
- **This prototype.** It adds the same eight failures in both modes, all in
  `test_shared_sink_swap.py`, and each pins a count FR-001 changes:
  - the two `Leaves` rows read 1 close where they assert 0;
  - the four caller's-wrapper forward rows and the two hoarding rows read `closes=2 overlaps=0
    refused=0` where they assert 1.

Re-run on 2026-11-24 on `5d0e7f68` (`origin/main` carrying SPEC-097 and SPEC-098):
`leaves_remaining_holder.py` printed `A.closes=0 A.closed_while_inside=0 A.closes_while_parked=0
A.refused=0 A.events=5 B.closes=1`, `spec101_interrupt.py` closed A never (`A.at_entry=[]`), and
`spec101_sequences.py` read the base column in every row it ran, the unconfirmed `Mine` row
included at `A.closes=2 overlaps=1`. So the base column holds on the tree this spec is built on.

## Revision history

- **Authored on 2026-11-17 against `a38bd80c`**, from `architecture.md` §12's item and its
  **Closed by**, which SPEC-097 records as placed in a spec of its own (Andrew, 2026-11-16).
- **Andrew's call, 2026-11-17: "(a) Close member after holder (Recommended)".** He was given four
  options with the measurements above:
  - (a), as specified;
  - (b), as (a), but skipping a member whose shipped closed guard already reads closed;
  - (c), a public declaration by the wrapper;
  - (d), no fix, with the caller's duty documented.

  Under (a), a sink behind a caller's wrapper that closes its member gets two closes in sequence,
  recorded as an exception to invariant 5.
- **Revised after the spec review, 2026-11-17.**
  - **A member is attached for as long as its wrapper lives** (FR-001). The first draft attached a
    member only at a cede or a skip, and dropped it after one close. A member written again after
    its after-holder close was then never closed again, against SPEC-045. Attaching now follows
    the library's writes: every written sink beneath a caller's wrapper is attached at each
    `configure()`. That also covers a sink whose owed close was already paid before the wrapper
    was installed. A sink first written through the wrapper is placed out of scope.
  - **The order is restricted to caller's wrappers** (FR-002). This is the coordinator's decision
    on the review's finding that a shipped holder delayed its member to the holder's slowest
    close. The remaining cost for a caller's wrapper is recorded in FR-005's edits.
  - **A put-back holder keeps its members** (FR-003). The first prototype's put-back dropped them.
    The rule for a member's `BaseException` is stated.
  - **FR-004 is built and run.** `current` is the configured sink, and a wrapper is drawn only over
    a written recorder. The repeat rule's four cases are stated, `Closes.by_wrapper` is in the
    Data Model, and the criterion that named `check_closed`, which the driver never calls, now
    names the driver's own assertion.
  - **The suite figures are corrected** as stated under *Evidence*.
- **Checked against `5d0e7f68` before the build, 2026-11-24.** `Leaves` and `Mine` move to
  `tests/test_invariants_model.py`, beside the recorder the sequential driver installs; the base
  column was re-measured and holds.
