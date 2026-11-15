---
type: Spec
id: SPEC-089
title: "flush() Counts What Its Window Lost, and Stays Truthy Over It"
description: "Gives FlushResult event counts of what the process lost while a flush() call was outstanding, read from the event ledger, without changing what bool(flush()) reports, so a handler can see a loss a truthy flush covers; and rewrites in place every statement that said a truthy flush meant delivery."
spec_status: Draft
depends_on: [SPEC-021, SPEC-034, SPEC-036, SPEC-050, SPEC-051, SPEC-072, SPEC-088]
arc: audit-2026-11-14
arc_order: 5
---

## Overview

A Lambda handler calls `flush()` in a `finally` and branches on the result. Today a truthy
result can cover a total loss. A sink on the verdict contract that takes 1 of 30 events, drops
all 30 or cannot confirm any of them settles its run, and `flush()` returns `ok=True` with the
loss visible only in `health().sink` and on stderr (2026-11-14 audit, finding 2). The README's
Lambda idiom does not read `health().sink`, so a handler written from it sees none of these
losses. On 2026-11-15 Andrew was asked "Should flush() stay truthy over a counted loss and gain
loss counts, or turn falsy?" and answered "Keep truthy, add counts". This spec builds that
answer. What `bool(flush())` reports does not change. `FlushResult` gains event counts of what
the process lost while the call was outstanding, read from the ledger SPEC-088 specifies, so a
handler that reads them sees the loss. Every statement that says a truthy `flush()` means the
sink took the events is rewritten in place and dated, and the README's handler idioms read the
new counts.

## Scope

### In Scope

- Five count fields appended to `FlushResult`, with their total, and nothing else about the
  type changing (FR-001).
- The call's window, the loss terms it counts, and what the counts do and do not cover, on a
  marker the drain answers, on both of the drain's answerers (FR-002).
- Every other outcome: a window read on the calling thread, a drain that is dying when the call
  reads, a forked child, and a fault in a read (FR-003).
- The record rewritten in place: the `flush()` decision reversed in part, the result-type
  decision and invariant 10 refined, five docstrings, the superseded markers, the release notes
  and the two §12 items (FR-004).
- The README's `flush()` wording and its Lambda handler idiom (FR-005).

### Out of Scope

- **Any change to what `bool(flush())` reports, or to its `reason` values.** Andrew decided this
  on 2026-11-15 ("Keep truthy, add counts"). A result that carries a loss count can be truthy.
- **`KafkaSink`'s and `GooglePubSubSink`'s verdict on enqueue, and the other acknowledgement
  fixes** (finding 12 and its neighbours). They are a later spec of this arc. Until then, a loss
  a sink counts after its `emit` returned is in none of the counts. That covers a failed Kafka
  delivery report, a `MultiSink` child that raised beside a sibling that accepted, and any
  `emit`-only sink's own internal loss. SPEC-088 ends the ledger where such an `emit` returns,
  and that loss stays in `health().sink`.
- **Shipping an open span's events before the span closes** (finding 8), which waits on Andrew.
  A `flush()` hands over the buffers its own context's open spans hold, as SPEC-036 built, and
  those events are counted like any others. The events buffered on spans the call does not sweep
  are another matter. They are the spans of other threads and tasks, and what the calling span
  buffers after the sweep. They are pending in `health().events.open_spans`, in none of the
  counts, and a result whose counts are zero says nothing about them (FR-002).
- **Larger batches under a backlog.** That is a later spec of this arc. The queue and the batch
  sizes are unchanged here.
- **Counts attributed to one caller's events.** The counts are process-wide over the window.
  Telling one caller's lost events from another's would need every event tagged with the flush
  that carried it, which is not built. The over-count this leaves is stated and held by a
  criterion (FR-002).
- **A loss debited before the call starts.** That is the scope the verdict already has
  (`Worker._nothing_lost_since`), and `health()` holds that loss.
- **`in_span_lost` and `orphan_lost` in the counts.** FR-002 gives the reason. Both stay in
  `health()`.
- **The general `health()` idiom at `README.md:1252-1260`.** It reads `health()`, not
  `FlushResult`, and SPEC-088 left it as written.
- **`shutdown()`'s return and `configure(sink=...)`'s internal flush.** Neither gains counts,
  and the swap's use of `Worker.flush`'s verdict is unchanged.
- **A `flush()` made from a signal handler.** This spec makes no new promise about a handler.
  The read it adds takes the ledger's registration lock, which the §12 item for finding 3 already
  lists (SPEC-088 FR-003).

---

## Functional Requirements

### FR-001: `FlushResult` gains five counts and their total, and its truthiness does not change

#### Description:

**The shape.** `FlushResult` gains five `int` fields, each defaulting to zero, named for the
ledger terms they count: `overflow_lost`, `batch_lost`, `sink_lost`, `drain_lost` and
`stranded`. A read-only property, `lost`, returns their sum. The same word names the same term
in `health().events` (SPEC-088), so a reader who finds a non-zero `sink_lost` on a result knows
which field of the ledger to watch. Three other shapes were weighed:

- **One `lost` total and no breakdown.** That is the smallest shape. A handler could see that
  something was lost and could not tell a destination refusing events (`sink_lost`) from a
  destination not keeping up (`overflow_lost`). Those need different fixes, which is the reason
  `reason` exists.
- **The window's difference as an `EventLedger`.** It would carry gauges and `delivered`, and a
  gauge's difference can be negative where `EventLedger` documents every field as a count.
- **A new public type holding the five.** It adds a name to the surface for no reader the flat
  fields do not already serve.

`FlushResult` becomes `@dataclass(frozen=True, kw_only=True)`, so the new fields are
keyword-only and `__match_args__` stays empty (SPEC-051). `_Result` and `ContinueResult` are
unchanged, so `continue_trace()` gains nothing. `results.py` stays a leaf that imports nothing
from the package, since the counts are plain `int`s. `__bool__` still returns `ok` and nothing
else.

**What a caller can see.** `repr()` shows the five fields. `==` compares them, so a result with
a non-zero count is no longer equal to `FlushResult(ok=True)`; a result whose window lost nothing
still is. `dataclasses.asdict` and `replace` carry them. The release notes say all of this
(FR-004).

#### Acceptance Criteria:

- [ ] `FlushResult(ok=True)` and `FlushResult(ok=False, reason="timed-out")` still construct, and
      on each every count and `lost` read 0 (invariant 10).
- [ ] `bool(FlushResult(ok=True, sink_lost=3)) is True` and
      `bool(FlushResult(ok=False, reason="abandoned", batch_lost=3)) is False` (invariant 10).
- [ ] `FlushResult` is frozen, its five new fields are `int`s defaulting to 0, `lost` is their
      sum and cannot be assigned, `FlushResult.__match_args__ == ()`, and a positional build is
      refused with the keyword-only message (invariant 10).
- [ ] `ContinueResult`'s fields are still exactly `ok` and `reason`, and
      `ContinueResult(ok=True, sink_lost=1)` raises `TypeError` (invariant 10).
- [ ] `tests/typed_consumer/accepts.py` reads `flush().lost` and `flush().sink_lost` and builds
      `FlushResult(ok=True, stranded=2)` under `mypy --strict`, and `rejects.py` shows an
      assignment to `lost` and a `ContinueResult` built with `sink_lost` refused (invariant 10).
- [ ] `log_foundry.__all__` gains no name, and every existing test that asserts a `flush()`'s
      truthiness or `reason` passes without an edit (invariant 10).

### FR-002: The counts are the worker's loss terms, from the call's start to the drain's answer

#### Description:

**Where the window starts.** On the calling thread, in `_flush_worker`, after `timeout` has been
validated and before `_sweep_open_spans()` runs, through SPEC-088's
`_lifecycle._ledger_snapshot()`. Starting before the sweep matters. The sweep hands the calling
context's open-span buffers to `Worker.submit`, and a full queue drops them there into
`overflow_lost`. That loss happens before `Worker.flush` builds its marker, so a window opened
there could not count it.

**Where it ends.** The drain thread takes a second snapshot when it answers the marker: after it
has emitted everything ahead of the marker and computed `delivered`, and before it sets the
marker's event. The marker carries the snapshot back, as it already carries `delivered`. By then
every event ahead of the marker has left the queue and settled, so each of its losses has been
debited. The window does not stay open while the caller wakes, so a batch the drain loses after
answering is not counted. The verdict's window ends at the same point, because
`_nothing_lost_since` reads `failed_batches` there, so a truthy result and a zero count are judged
at the same moment. Both answerers of a marker take the snapshot: `_drain`'s marker branch and
`_final_drain`, which answers markers on `shutdown()`'s exit drain (invariant 6). The counts are
the fieldwise difference of the two snapshots for the five terms. SPEC-088 FR-004 makes each
loss term non-decreasing in a process, and lets `_ledger_snapshot()` be called from any thread,
the drain thread included. Two snapshots cost a few microseconds each by SPEC-088's measurement,
on a call that waits for a drain.

**Which terms.** The five terms an event handed to the worker can leave by. `skipped` is not a
loss. `delivered` and the gauges (`open_spans`, `queued`, `in_flight`) are not losses either: an
event in a gauge is still on its way. Two loss terms are left out. `in_span_lost` and
`orphan_lost` are debited where an event is built or logged on the caller's own path, before
anything is handed over. A flush carries no event that can leave through them, and the sweep
debits neither: it resolves the worker before it detaches a buffer and stops if that fails, and
`Worker.submit` raises nothing (`decorator.py:450-462`). Counting them would add only other
threads' losses. They stay in `health()`, which the README's idioms read (FR-005).

**What the counts are.** They include every loss of an event the call carried: the events
submitted before the call that were still queued or in hand when it started, and the buffers its
sweep handed over. They also include **other events' losses debited in the window**. The counts
are process-wide differences and over-count in three ways:

- events of other threads that were ahead of the marker;
- other threads' submissions that overflow while the call waits;
- `sink_lost` from an orphan log on any thread, because SPEC-088 debits a settled run's losses to
  `sink_lost` on both delivery paths.

A non-zero count therefore says that the process lost events in the window, not that this
caller's events were among them. A zero count says that none of the call's events was lost
between its start and its answer.

**What the counts are not.** A loss debited before the call starts is outside the window. That
includes a batch the worker's own trigger abandoned, which is the scope the verdict already
has. Measured at `9b54b370`: under a switch interval of 1e-6, the count trigger emitted p09's
batch before `flush()` was entered in 8 to 51 of 200 trials per case, and today's verdict then
read `ok=True` over the all-rejected case in some of them (Evidence). So the criteria below put
the worker's triggers out of reach, and p09's last case pins the boundary. An event still
pending when the window ends is in no count, nor is an event buffered on an open span the call
does not sweep (Out of Scope).

#### Acceptance Criteria:

- [ ] On a worker whose batch size and interval no submission of the case reaches, so the flush's
      marker emits the batch, p09's four lossy verdict plans each return a truthy `flush()`
      whose counts are all zero except `sink_lost`. `sink_lost` is 29 for "1 accepted, 29 never
      land", 29 for "1 accepted, 29 rejected", 30 for "all dropped" and 30 for "all unknown"
      (invariants 2 and 7).
- [ ] The same worker with p09's "all 30 rejected" plan returns `reason == "abandoned"` with
      `batch_lost == 30` and `sink_lost == 0` (invariants 2 and 7).
- [ ] The same worker with a sink that accepts every event returns a result equal to
      `FlushResult(ok=True)` (invariant 2).
- [ ] p09's last case, a batch the worker's own trigger abandoned before `flush()` was called,
      returns a truthy result with `lost == 0`, while `health().events.batch_lost` has moved by
      30 (invariant 2).
- [ ] A second thread closes 3 spans whose 9 events the sink drops and joins, then the first
      closes 2 spans the sink accepts and flushes. The flush is truthy with `sink_lost == 9`
      in 200 of 200 runs, and the sink holds the first thread's 6 events (invariant 2).
- [ ] While the drain is held inside the sink with the flush's batch, another thread makes an
      orphan log whose event cannot be built and an in-span log whose event cannot be built.
      `health().orphan_lost` and `health().in_span_lost` each move by 1, and the flush's
      counts stay all zero (invariant 2).
- [ ] A flush from inside a span holding 2 buffered events, with a sink that drops every event,
      returns `sink_lost == 2`. A span left open on another thread with 2 buffered events is in
      no count, and `health().events.open_spans == 2` after the call (invariant 2).
- [ ] A flush whose marker is answered by `shutdown()`'s exit drain, with a sink that drops every
      event, returns `sink_lost` equal to the events ahead of its marker (invariants 2 and 6).
- [ ] With the calling thread held by an injected hook after the drain answered its marker, a
      batch the drain then loses is in none of the counts, and the next `flush()` counts it
      (invariant 2).

### FR-003: Every other outcome carries the counts read on the caller, and no read closes on a dying drain

#### Description:

**A window the drain did not close is closed on the calling thread.** Sometimes no drain answers
the marker with a snapshot. The worker may not exist, `Worker.flush` may return before it queues
a marker (`"retired"`, `"thread-died"`, `"queue-full"`, or an `"abandoned"` given up before the
put), the wait may time out, or the marker may be answered pessimistically by an expired
`shutdown()` or the terminal sweep. In each case the window ends on the calling thread when
`Worker.flush` returns, and before `_flush_live_sink()` runs. Every `FlushResult` `flush()`
returns carries its window's counts, whatever its reason. That includes `"abandoned"` from a
failed sweep, `"thread-died"` from `_flush_worker`, and `"sink-flush"`, whose counts are the
drain's window, since a sink's own client-buffer flush is outside the ledger. A process with no
worker returns zero counts: SPEC-088 reports the worker's terms as zero there.

**A dying drain is waited for before the read.** A drain thread dying on a `BaseException` from
the sink answers the marker it holds in `_drain`'s `finally`, before `_terminal_failure` debits
`drain_lost`. SPEC-088 counts the queue in `stranded` only once `Worker._thread.is_alive()`
returns `False`. A caller that reads at once can therefore miss both. Measured at `9b54b370`,
with a switch interval of 1e-6, the caller's read came before `_terminal_failure` had recorded in
1 of 96 runs and before the thread had ended in 10 of 96. At the default interval it came after
both in 200 of 200 (Evidence). So a marker answered while the drain unwinds an exception is
marked as such. A call whose marker is so marked, or whose read finds the drain finished
(`_drain_finished` set) while its thread is still alive, waits for the thread to end, then
reads. The wait is idle and lasts at most what is left of the call's timeout, or indefinitely
for `timeout=None`, as the rest of the call does. A call whose budget runs out first reads what
has been debited by then. An expired `shutdown()` leaves `_drain_finished` clear on a drain that
is still working, so its pessimistic answer causes no wait.

**A window a fork crosses is the child's.** SPEC-088 FR-005 restarts a forked child's ledger
with every loss term at zero. The start snapshot is stamped with the pid it was read in. If the
end is read in another process, the counts are the end snapshot's five terms, which are the
child's losses since the fork, all inside the window. Each count is also published as zero
rather than negative. In one process a count can fall only when a test replaces the process
worker by hand.

**A fault in a read is absorbed.** The drain thread's snapshot runs inside its marker branch,
and an exception escaping there would end the only thread that delivers. A fault in it is
announced by type and the marker is answered without a snapshot, so the caller reads instead. A
fault in the caller's own read is announced by type and leaves the counts at zero with the
verdict unchanged, because a flush is the call most likely to be made in a `finally`.

#### Acceptance Criteria:

- [ ] With the sink held inside `deliver` on the flush's batch, `flush(0.2)` returns
      `"timed-out"` with `lost == 0` and `health().events.in_flight` equal to the held batch
      (invariants 2 and 3).
- [ ] After a `shutdown()` that completed, a `flush()` from inside a span holding 2 buffered
      events returns `"retired"` with `stranded == 2` (invariant 2).
- [ ] With the queue full behind a held drain, a `flush()` from inside a span holding 2 buffered
      events returns a falsy result with `overflow_lost == 2` (invariant 2).
- [ ] The sink holds the flush's 9-event batch until another thread has queued 6 more events,
      then raises `SystemExit`. The flush returns falsy with `drain_lost == 9` and
      `stranded == 6`, in 200 of 200 runs under a switch interval of 1e-6 (invariants 2 and 3).
- [ ] With the dying drain held by an injected hook inside `_terminal_failure`, `flush(0.5)`
      returns within 0.5 s plus 50 ms of wall time and with near-zero CPU, while the thread is
      still held (invariant 3).
- [ ] A drain whose batch settled with 3 events dropped, under a sink whose own `flush()` raises,
      returns `"sink-flush"` with `sink_lost == 3` (invariants 2 and 6).
- [ ] No `FlushResult` is built in `worker.py` or `_lifecycle.py` except through the one function
      that attaches the window's counts, and a test that derives the constructions from the tree
      fails on a new one built any other way (invariant 6).
- [ ] The parent's worker has counted 5 events in `overflow_lost`. A child forked from a hook
      inside a flush's window returns counts equal to the child's own losses since the fork, none
      negative, and the parent's flush is unaffected (invariant 12).
- [ ] A fault injected into the drain thread's snapshot is announced by type once, the drain
      thread stays alive, and the flush's counts match the same case uninjected
      (invariants 1 and 11).
- [ ] A fault injected into the caller's snapshot is announced by type once, and `flush()`
      returns its uninjected verdict with every count 0 (invariants 1 and 11).

### FR-004: The record says what a truthy `flush()` means, rewritten in place and dated

#### Description:

Andrew's answer reverses part of a settled decision and refines another. Under
`docs/process/completion-ritual.md` step 5, each is rewritten in place, with title, fence and
entry together, and gains a dated History entry at the top. Every other site still stating the
old claim gets a superseded marker, a short blockquote naming the change, this spec and the
decision's path. Nothing is deleted.

**Reversed in part.**
`docs/decisions/pipeline/flush-reports-delivery-and-answers-from-the-drain-that-carried-the-events.md`
says "`True` means the sink took them, not merely that a drain ran" and "`delivered` starts
`False` and is written only by the owning drain, so no added path can produce `ok=True` over lost
events". Both stop being true under closure (b). A truthy result means the drain that carried
the events finished, and abandoned no batch while the call was outstanding. Events a settled run
lost are counted on the result, not folded into its truth. The marker mechanism, the three places
a flush reports over, and SPEC-050's narrowing all stand. The title "reports delivery" is
rewritten to say what is reported. The file name is kept (`knowledge-layout.md`).

**Refined.**
`docs/decisions/public-api/a-result-that-can-grow-a-reason-must-stop-being-a-bool-before-10-not-after.md`
fences "`FlushResult`/`ContinueResult` grow by new reason values only", and invariant 10 says
"results grow by new reason values" (`docs/invariants.md:607`). FR-001 grows `FlushResult` by
appended keyword fields. The rule the decision exists for, never a changed `__bool__`, is
untouched, and both texts say so.

**The docstrings**, at `9b54b370`:

- `results.py:20-21`, "It grows by new ``reason`` values";
- `results.py:61`, "``ok=True`` carries the guarantee instead: the sink took them";
- `__init__.py:64-65`, "so a truthy result means they were delivered";
- `worker.py:895-897`, "``ok=True`` is unaffected and still means the sink took them";
- `_lifecycle.py:5734`, "truthy when everything outstanding was delivered".

**The other sites.** `docs/release-notes/v1.0.0.md:288`, "**`ok=True` still means the sink took
them**", gets a superseded marker, because a reader who lands only there must see the reversal.
Completed specs, delivery docs and `docs/audits/` are records of their time and are left alone,
as the ritual's list of sites leaves them. `docs/specs/ARCS.md` already records the answer in
SPEC-088's revision of the arc's narrative and needs no marker. §13's first constraint is what
closure (b) keeps, and it stays as written.

The release notes follow SPEC-088's form. No pending notes file exists on `main` at `9b54b370`,
so the entry goes in the pending notes for the next tag, whichever spec of the arc creates them.

#### Acceptance Criteria:

- [ ] The `flush()` decision's file keeps its name, and its title, fence and entry are rewritten
      in place. They no longer say that `True` means the sink took the events, or that no path
      can produce `ok=True` over lost events. They say what a truthy result does mean and that
      the counts carry the loss (invariants 2 and 10).
- [ ] The `flush()` decision's History gains, at the top, an entry dated the day this spec
      lands. It is marked as a reversal in part, names this spec, and quotes Andrew's question
      and answer of 2026-11-15 (invariants 2 and 10).
- [ ] The result-type decision's fence and entry, and invariant 10's text, say that a result
      grows by new reason values and by appended keyword fields and never by a changed
      `__bool__`, and the decision gains a dated History entry (invariant 10).
- [ ] None of the five docstrings this FR lists still states the claim quoted for it.
      `FlushResult`'s docstring names the five counts, the window and the over-count, and
      `docstring-lint.py` passes (invariant 10).
- [ ] A superseded marker naming this spec and the decision's path stands at
      `docs/release-notes/v1.0.0.md:288` (invariant 10).
- [ ] `git grep` for "sink took them", "evidence of delivery", "means they were delivered" and
      "everything landed" finds no hit outside a superseded marker, `docs/audits/`,
      `docs/specs/` and `docs/spec-delivery/` (invariant 10).
- [ ] The pending release notes carry one entry. It names the five fields and `lost`, says that
      truthiness is unchanged, describes the window and the over-count, and says that `repr()`
      and `==` now include the counts (invariant 10).
- [ ] The `docs/architecture.md` §12 item for finding 2 is struck as closed by this spec. The
      item for finding 12 records that a loss a sink counts after its `emit` returned is in none
      of the counts, and stays open for its acknowledgement fix (invariant 2).

### FR-005: The README's handler idioms read the counts

#### Description:

The Lambda idiom (`README.md:1500-1503` at `9b54b370`) branches on `not drained` and on
cumulative `health()` counters. It does not read the `h.sink` term the general idiom carries
(`:1259`), so a handler written from it misses every loss p09 shows. The condition gains
`drained.lost` and the `h.sink` term. Its comment then says three things: `drained` is falsy when
the drain did not finish, `drained.lost` counts what the process lost while the call ran, and a
truthy result does not rule that out. The idiom's existing terms stay, and so does its print
branch.

The drain snippet's comment (`:1375`, "truthy when everything landed") and the `FlushResult`
paragraph (`:1415-1418`, "A truthy result is evidence of delivery") are rewritten to say what a
truthy result means. They name `lost`, the five fields, the window and the over-count.
`:767-769` already says that a truthy `flush()` means the drain completed and not that every
chunk landed, and it stays.

#### Acceptance Criteria:

- [ ] The Lambda idiom's condition, read from `README.md` by a test and evaluated against the
      `flush()` result and `health()` of FR-002's "all dropped" case, is true. At `9b54b370` the
      same evaluation is false (invariant 2).
- [ ] The same evaluation against FR-002's all-accepting case, in a process that has lost
      nothing, is false (invariant 2).
- [ ] The Lambda idiom's condition reads `drained.lost` and the `h.sink` term, and its comment
      says that a truthy `drained` can carry a non-zero `lost` (invariant 2).
- [ ] `README.md` no longer says "truthy when everything landed" or "evidence of delivery". Its
      `FlushResult` section names `lost` and the five fields, with the window and the over-count
      (invariants 2 and 10).

---

## Data Model

```python
# src/log_foundry/results.py — still imports nothing from the package
@dataclass(frozen=True, kw_only=True)
class FlushResult(_Result):
    overflow_lost: int = 0
    batch_lost: int = 0
    sink_lost: int = 0
    drain_lost: int = 0
    stranded: int = 0

    @property
    def lost(self) -> int: ...  # the sum of the five


# src/log_foundry/worker.py, private
class _FlushMarker:
    ...  # delivered, event, seen_failures, unchanged
    ledger: EventLedger | None  # the drain's end snapshot, written before event.set()
    unwinding: bool  # answered by a drain unwinding an exception; the caller waits, then reads
```

---

## API / Interface Contract

```python
r = log_foundry.flush(timeout=5.0)
if not r:
    ...  # the drain did not finish, or a batch was abandoned: r.reason says which
if r.lost:
    ...  # the process lost events while the call ran; r.sink_lost and the rest say where

# private
start = _lifecycle._ledger_snapshot()  # calling thread, before the sweep
end = marker.ledger or _lifecycle._ledger_snapshot()  # drain's answer, else the caller's read
```

## File & Folder Structure

```
src/log_foundry/
├── results.py          # FlushResult's five fields and lost
├── worker.py           # _FlushMarker.ledger and .unwinding; both answerers snapshot; the wait
├── _lifecycle.py       # _flush_worker: the start read, the end read, the one constructor
└── __init__.py         # flush()'s docstring
tests/
├── test_flush_loss_counts.py   # FR-002, FR-003
├── test_public_surface.py      # FR-001's shape criteria
├── typed_consumer/             # accepts.py and rejects.py grow
└── (a README idiom test)       # FR-005
docs/
├── decisions/pipeline/flush-reports-delivery-and-answers-from-the-drain-that-carried-the-events.md
├── decisions/public-api/a-result-that-can-grow-a-reason-must-stop-being-a-bool-before-10-not-after.md
├── invariants.md, architecture.md (§12), release-notes/ (v1.0.0.md's marker; the pending notes)
└── README.md
```

## Implementation Phases

### Phase 1: The shape (FR-001)

- `FlushResult`'s fields and `lost`, the typed consumer, and the public-surface tests. Every
  result still carries zero counts, so nothing a caller sees changes yet.

### Phase 2: The window (FR-002, FR-003)

- The start read in `_flush_worker`, the snapshot in both of the drain's answerers, the one
  constructor, the end read on the caller, the wait on a dying drain, the fork rule and the
  absorbed faults.
- One test per criterion, with p09's plans on a worker whose triggers are out of reach.

### Phase 3: The record and the README (FR-004, FR-005)

- The two decisions, invariant 10, the docstrings, the superseded marker, the release notes and
  the two §12 items.
- The README's wording and the Lambda idiom, with the test that evaluates its condition.

---

## Evidence

Everything here was measured at `9b54b370` on macOS, on CPython 3.13.1 unless a row says
3.12.12, while other sessions loaded the machine (load average near 180). The scripts are scratch
prototypes and are not committed. None of them can read the ledger, which SPEC-088 specifies and
has not built. They approximate `sink_lost` with the change in
`health().sink.dropped + health().sink.failed`, and `batch_lost` with the change in
`failed_batches`, between the entry to `_lifecycle._flush_worker` and its return.

**p09 today.** `PYTHONPATH=src python3.13 -B
docs/audits/2026-11-14-probes/core-pipeline/p09_flush_barrier.py` printed the core frame's
recorded output line for line. Four plans return `FlushResult(ok=True, reason=None)` over 29,
29, 30 and 30 lost events, "all 30 REJECTED" returns `abandoned` with `failed_batches +1`, and
the batch abandoned before the call leaves a later `flush()` at `ok=True`.

**Where p09's loss falls against a window opened on entry.** p09's plans, 10 spans per trial,
with a fresh sink per trial.

| Run | Deliver called before `flush()` was entered | Loss outside the window |
|---|---|---|
| 3.13.1, default switch interval, 300 trials per case | 0 in every case | 0 in every case |
| 3.12.12, default switch interval, 300 trials per case | 0 in every case | 0 in every case |
| 3.13.1, switch interval 1e-6, 200 trials per case | 8, 16, 16, 51, 18 and 14 | 0, 18, 17, 57 and 15, and 0 for the control |

At the default interval the change read in the window equalled the sink's loss in every trial:
29, 29, 30 and 30, with `failed_batches +1` for "all 30 REJECTED" and 0 for a sink accepting
everything. At 1e-6 the count trigger sometimes emitted the batch before the call. "1 accepted,
29 never land" stayed inside the window anyway, because its four rounds outlast the call's
start. "All 30 REJECTED" then read `ok=True` in some trials, which is today's verdict scope
working as written. So FR-002's criteria put the triggers out of reach.

**The over-count.** A second thread closed 3 spans whose 9 events the sink answered `DROPPED`
and joined, then the main thread closed 2 spans the sink accepted and flushed. In 200 of 200
runs, `flush()` was truthy, the window's change was 9 and the sink held the 6 accepted events.

**A dying drain at the caller's read.** One process per trial: 3 spans, then `flush(5)`, with an
`emit`-only sink that raises `SystemExit`. Each trial read three things at the moment
`Worker.flush` returned: its reason, whether `stopped_reason` was set (`_terminal_failure` has
recorded, which is where SPEC-088 debits `drain_lost`), and whether the drain thread was alive.

| Run | `abandoned`, recorded, thread ended | recorded, thread alive | not recorded, thread alive |
|---|---|---|---|
| default switch interval, 200 processes | 200 | 0 | 0 |
| switch interval 1e-6, 100 processes | 86 | 9 | 1 |

At 1e-6, 4 of the 100 processes printed nothing. A rerun of 60 such processes printed 60 lines,
so the four were not reproduced and are not explained.

---

## Revision history

- **2026-11-15, drafted** from Andrew's answer of the same day to the question the §12 item for
  finding 2 put to him. The item weighed two closures, and this spec is closure (b). Closure (a)
  would have made `flush()` falsy over any loss in its window. It was not chosen, and nothing
  here builds it.
