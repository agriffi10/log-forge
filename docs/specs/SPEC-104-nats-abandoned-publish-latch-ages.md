---
type: Spec
id: SPEC-104
title: "NATSSink's Abandoned-Publish Latch Ages, as Its PING Latch Does"
description: "Makes a NATSSink call that meets a publish abandoned less than one publish_timeout ago wait for it on the loop until that window ends, without cancelling it and without publishing past it, so a stall a little longer than one budget no longer loses a batch, and with SPEC-102's resend no event but the abandoned one, while a longer stall still costs one window in all."
spec_status: Completed
depends_on: [SPEC-047, SPEC-061, SPEC-062, SPEC-102]
---

## Overview

`NATSSink` bounds every call by one `publish_timeout`. A publish the driver does not return from
inside that budget is abandoned, left running and never cancelled, and it arms the
abandoned-publish latch. Today, while any abandoned publish is still pending, every later round of
the worker's and every `flush()` is refused at once. The worker gives a `NATSSink` batch four
rounds separated by 0.01, 0.02 and 0.04 s, so a stall only a little longer than one budget
exhausts each batch in turn and loses it. At the stop those waits are zero, and the whole exit
backlog goes through the latch in microseconds, although the stalled publish would have finished
seconds before the `shutdown()` deadline. Every such loss is counted and announced; none is silent.
`docs/architecture.md` §12 files the defect (the item beginning "A `NATSSink` whose
abandoned-publish latch is armed refuses every round of the worker's at once").

This spec gives the abandoned-publish latch the aging the PING latch already has. A call that
meets an abandoned publish younger than one `publish_timeout` waits for it on the loop until that
window ends, and proceeds if it finishes; only a publish that has been pending a whole window is
refused at once. A transient stall then loses no batch, and with SPEC-102's resend no event but the
abandoned publish's own; a long stall costs the callers one window in all rather than one budget
each. Nothing a caller configures changes, and no line or counter is added. The design was measured
against four alternatives and chosen; the numbers are under *Evidence* and the rejected designs
under *Revision history*.

**Sequencing.** SPEC-102 (Completed, `e7b437f9`) makes `NATSSink` resend the unattempted tail of a
round and relies on this latch so that a resend never overtakes the abandoned publish. It edited
`_verdict_of`, `_register` and the docstrings of `_publish_all`, `_round` and the class, so this
spec is built on top of it, and FR-003's event-level criteria assume its resend.

## Scope

### In Scope

- Dating each abandoned publish when it is abandoned, in the record the loop thread already
  replaces whole (FR-001).
- Refusing at once only once the abandoned publish has been pending one `publish_timeout`, read
  before the lock and again under it, where the latch is read today (FR-001).
- A round, and `flush()` as its twin, waiting for a younger abandoned publish on the loop until its
  window ends, never cancelling it and never publishing while it is pending (FR-002).
- The end-to-end outcome through the library: a transient stall loses no batch at the stop or in
  the steady drain, a long stall costs one window in all, and the exit bound stands (FR-003).
- The record: the decision, the invariants page, the `NATSSink` docstrings, the deletion of the
  uncalled `_refuse_while_stuck`, the §12 item and the pending release notes (FR-004).

**The twin paths** (invariant 6). Every path into the sink meets the same latch, because each runs
`_round` or `flush()`: the worker's steady drain and its exit drain (`_drain` and `_final_drain`,
which differ only in the engine's waits between rounds, and this wait is the sink's own), the orphan
path on the caller's thread, `emit` through `emit_via_deliver` and `deliver` directly, a
`FilteringSink` or `TransformSink` forwarding `deliver`, and `MultiSink` calling `emit`. `flush()`
is the twin of a round and keeps the same rule. `close()` keeps its own rule (FR-001).

### Out of Scope

- **The PING latch** (`_latched`, `_await_probe`), which stays as it is, read before the lock only.
  It already ages: a caller waits on the outstanding probe inside its own deadline and is refused at
  once only once the probe has gone unanswered for `publish_timeout`. One consequence is kept: for a
  core client the sink connected itself, a caller arriving after the abandoning round returned is
  refused at once by the PING latch in the common shape, while one queued behind the abandoning
  round does wait (FR-001). The common shape is a pending buffer above 2 MiB behind an earlier
  blocked drain: it cannot drain by the deadline, so `_confirm` leaves a probe behind dated from the
  start of that round. When the driver's flusher is itself blocked writing the forced batch, nats-py
  has already zeroed `_pending_data_size` before `transport.drain()`, so `_drained` can read zero
  and no such probe is left. `docs/architecture.md` §12 carries this as its own item ("An owned core
  `NATSSink` whose abandoned publish leaves its buffer undrained…").
- **A destination that refuses at once during the stop**, such as a client that reports itself
  disconnected. Nothing is in flight for the sink to wait on; `docs/architecture.md` §12 carries it
  as its own item ("A destination that refuses at once during the stop…").
- **SPEC-102's resend of the unattempted tail.** That spec owns it; this one only keeps the order it
  relies on.
- **`Nats-Msg-Id` deduplication** (its own §12 item).
- **Any change to the engine or the worker**: no new `RunContext` flag, no change to the backoff
  between rounds, to SPEC-062's prediction, or to SPEC-027's rule that a shutdown cuts every
  backoff short.
- **Any new setting, diagnostic line, counter or `Health` field.**
- **`close()`'s guard.** It still skips its confirming round trip while an abandoned publish is
  pending, young or old, and still cancels pending tasks at the close.

---

## Functional Requirements

### FR-001: An abandoned publish is dated, and the latch refuses at once only once it is one window old

#### Description:

When `_publish_bounded` abandons a publish, it records the instant of abandonment beside the task,
in `self._stuck`, which the loop thread replaces whole so a caller thread reading it without a lock
sees a consistent pair. The latch's refusal (`_stuck_refusal`) fires only while the pending
abandoned publish was abandoned at least `publish_timeout` ago; that window is the one
`publish_timeout` after the abandoning round's deadline. It is read where the latch is read today:
before the lock (`_refusal_outside_the_lock`) and again under it (`_refusal_under_the_lock`), with
the same `RETRYABLE` outcome and the same error text.

The window is dated from the abandonment, not from the start of the call whose budget the stall
spent. That is a deliberate exception to the register's rule that a latch is dated from the start
of that call: an abandonment happens at that call's deadline, so a latch dated from its start would
already be one window old the instant it is armed, which is the behaviour this spec removes.

**A caller queued behind the abandoning round now waits too.** The check under the lock was there so
that callers queued behind an abandoning round would not each spend a budget. Under the aged rule
such a caller finds the publish young, passes that check, and waits on the loop until the window
ends, so its call is bounded by about `2 * publish_timeout` plus `CALL_SLACK` from its own start: the
abandoning round, then the rest of the window. Every caller queued behind it takes the lock after
the window has aged and is refused under the lock, putting nothing on the loop. The PING latch is
still read before the lock only, and `_round_on_loop` does not read `_latched()` before its wait.

`_publish_stuck()` keeps its meaning, an abandoned publish still pending, and `close()` keeps
reading it.

#### Acceptance Criteria:

- [ ] While another thread holds the sink's lock, an `emit` made once the abandoned publish has been
      pending `publish_timeout` since its abandonment raises `SinkDeliveryError` matching
      `abandoned at an earlier` within the matrix's `NEXT_BOUND`; `deliver` returns every outcome
      `RETRYABLE`, and `flush()` raises the same error, in the same bound.
- [ ] `_refusal_outside_the_lock` returns `None` while the pending abandoned publish is younger than
      `publish_timeout`, and the latch's `RETRYABLE` refusal once it is that old.
- [ ] A caller that takes the lock after the window has aged puts nothing on the loop: `_submit` is
      not called and the double logs no call. Mutating `_refusal_under_the_lock` to return `None`
      makes this fail; it is mutation-tested.
- [ ] In `tests/test_sinks_nats_matrix.py`, the queued caller's bound for the four abandoned-publish
      cells `owned-core-emit1-first-publish`, `owned-js-emit1-first-publish`,
      `injected-core-emit1-first-publish` and `injected-js-emit1-first-publish` becomes
      `2.5 * P` (the matrix's `P`), half a budget clear of the `2 * P` it now takes; the PING-latch
      cells keep `QUEUED_BOUND`.
- [ ] Once the abandoned publish finishes, its entry leaves `self._stuck`, and the next call is
      neither refused nor kept waiting by it.
- [ ] With a young abandoned publish pending, `close()` on a client the sink connected itself sends
      no confirming PING, as today.
- [ ] Serves invariants 2, 3 and 6.

### FR-002: A round and `flush()` wait for a young abandoned publish until its window ends, never cancel it, and never publish past it

#### Description:

A call that passes the latch with an abandoned publish still pending first waits for it on the
loop, before any other step of its coroutine (`_round_on_loop`, `_flush_on_loop`). At most one
abandoned publish is ever pending, because nothing publishes while one is. The wait is
`asyncio.wait` on that task, bounded by the window's end, `abandoned_at + publish_timeout`, and it
never cancels it (SPEC-061's rule: a cancelled driver wait the server may still answer is worse
than a late one). The window's end always falls inside the call's own deadline: the lock serialises
the coroutines, so the publish was abandoned before this coroutine started. If the publish finishes,
the call proceeds with what remains of its one `publish_timeout`, which may be nothing. If not, it
refuses with today's error: a round's report carries the latch's `SinkDeliveryError` and `_round`
returns it `RETRYABLE`; `flush()` raises it.

After the wait the call re-reads whether the abandoned publish is still pending and refuses if it
is. **This re-check is the guard whose failure would be silent**: without it a call would publish
while the abandoned publish is still in the driver, and the latch's purpose, SPEC-102's ordering,
would be broken with no refusal or line to show it; only the order of the driver's calls would.

The wait is work, as the ack wait it continues is, so it reads no stop signal: on the exit drain,
where the engine's own waits are zero, a round still waits for the young abandoned publish.

#### Acceptance Criteria:

- [ ] With an injected JetStream double whose abandoned publish finishes inside its window, the next
      `emit` returns after that publish finished, publishes its own batch with every outcome
      `ACCEPTED`, and its wall time is within `publish_timeout + CALL_SLACK`.
- [ ] With a stall of three `publish_timeout`s, an `emit` made in the young window raises the
      latch's `SinkDeliveryError` no later than the window's end plus `CALL_SLACK`, and `deliver`
      returns every outcome `RETRYABLE` in the same bound.
- [ ] With that stall, a call entering at three quarters of the window is refused within half a
      `publish_timeout`; waiting to the call's own deadline instead of the window's end makes it
      take about one `publish_timeout` and fails this.
- [ ] `flush()` keeps both rules: it returns after a publish that finishes inside the window, and
      raises the latch's `SinkDeliveryError` no later than the window's end plus `CALL_SLACK`
      otherwise.
- [ ] With an injected JetStream double, a stall of three `publish_timeout`s, and a worker round, an
      orphan-path `emit` and a `flush()` each made in the young window, the double's log shows no
      `publish` and no `flush` started between the abandoned publish's start and its end.
- [ ] Replacing the re-check after the wait with "nothing pending" makes the preceding criterion's
      test fail on the order it asserts; this guard is mutation-tested.
- [ ] After a call that waited and refused, the abandoned task is not cancelled, and the double
      later records it finishing.
- [ ] A caller thread's ~~`time.thread_time()`~~ CPU time, read by `conftest.cpu_window("thread")`,
      across a wait of one window is below 0.05 s (corrected at completion, 2026-11-18).
- [ ] With a stall of three `publish_timeout`s, a level call with no active span made in the young
      window returns no later than the window's end plus `CALL_SLACK`, and one made after the
      window's end is refused within `NEXT_BOUND`.
- [ ] A stop signal set on the sink does not shorten the wait: the existing test that a set signal
      does not shorten a batch passes with the batch meeting a young abandoned publish.
- [ ] `tests/test_sinks_nats_matrix.py` stays gated by mutation: its abandoned-publish cells assert
      the next caller's refusal once the window has aged, and each new guard (the age comparison,
      the window bound and the re-check) turns at least one cell red on its stated reason.
- [ ] Serves invariants 2, 3, 4 and 6.

### FR-003: Through the library, a transient stall loses no batch, a long one costs one window in all, and the exit bound stands

#### Description:

The end-to-end outcome, driven through `log_foundry` with a JetStream `NATSSink` over an injected
client, in the shape of the in-repo probe: one publish after a trigger ignores the driver timeout
it is handed and takes 1.5 `publish_timeout`s while every other publish acks in a millisecond. The
engine and the worker are untouched, so what changes is only what the sink's rounds do. The
event-level criteria hold with SPEC-102's resend in place, which this spec is built on: without it,
the stalled round's unattempted tail is counted by the sink rather than resent.

The exit drain's bound stays the one ~~the module docstring states~~ the `DEFAULT_PUBLISH_TIMEOUT`
docstring states and the class docstring restates (corrected at completion, 2026-11-18), the
`shutdown()` budget plus up to two `publish_timeout`s. Each call's wait ends at the window's end, inside the call's own
deadline, so every call still spends at most that one deadline plus the backstop's `CALL_SLACK`;
that sentence is not edited.

#### Acceptance Criteria:

- [ ] With the stall triggered by the first round after `shutdown(timeout=S)` begins and a backlog
      queued, `shutdown()` returns within `S`, the stalled publish has acked, and
      `health().events.batch_lost` and `health().failed_batches` both read 0.
- [ ] In that case, with SPEC-102's resend in place, every event but the abandoned publish's reaches
      the double, and `health().events.sink_lost` is at most 1.
- [ ] Reverting FR-001's age check to "any pending" makes that test, with an exit backlog of more
      than one exit batch, fail on `batch_lost` (corrected at completion, 2026-11-18).
- [ ] With the stall triggered in the steady drain, once the backlog has drained and
      `log_foundry.flush()` has returned, `batch_lost` and `failed_batches` both read 0, every event
      but the abandoned publish's has reached the double, and `sink_lost` is at most 1.
- [ ] With every publish after the stop taking 1.5 `publish_timeout`s, `shutdown(timeout=S)` returns
      within `S + 2 * publish_timeout + CALL_SLACK`.
- [ ] With one publish stalled for fifteen `publish_timeout`s and events logged during it, every
      call that began inside the window returns no later than the window's end plus `CALL_SLACK`,
      every round and call that began after it is refused within the matrix's `NEXT_BOUND`, and a
      `shutdown(timeout=10)` made after the window returns within 1 s.
- [ ] In every case above the ledger balances, with `health().events.unaccounted` 0, and every
      event refused or lost is counted once and announced.
- [ ] The diff adds no `_diag` call, no `SinkCounters` field, no `Health` field, no constructor
      parameter, and no edit to the ~~module docstring's~~ sentence stating the exit bound, in the
      `DEFAULT_PUBLISH_TIMEOUT` docstring and the class docstring (corrected at completion,
      2026-11-18).
- [ ] Serves invariants 2, 3, 6 and 11.

### FR-004: The record states the latch's new rule

#### Description:

The completion ritual (`docs/process/completion-ritual.md`, step 5) rewrites in place, each with a
dated History entry where the document keeps one, every site that states the old rule. SPEC-102's
docstrings, rebased in, are among the `NATSSink` docstrings this covers. `_refuse_while_stuck` has
no caller and is deleted rather than reworded; the two docstrings that name it point at
`_stuck_refusal` instead.

#### Acceptance Criteria:

- [ ] `docs/decisions/sink-lifecycle/a-drivers-liveness-flag-is-not-evidence-a-sink-that-cannot-see-the-wire-asks-the-server-and-never-cancels-a-wait-the-server-may-still-answer.md`
      is refined in place: its second latch refuses at once only once one window old and is waited
      for until the window ends before that, its "do NOT date a latch" fence names the
      abandoned-publish exception and why, the rejected designs are recorded with their numbers,
      and its History gains a dated SPEC-104 entry at the top.
- [ ] In `docs/invariants.md`, the second entry's sentence "with every later call refused while it
      is still pending" states the new rule, and the third entry's parenthetical names `NATSSink`'s
      wait on its own abandoned publish beside `pubsub`'s wait on its own futures.
- [ ] In `docs/invariants.md`, the fourth entry, on whose thread carries sink I/O, gains a recorded
      exception: an orphan-path level call in the young window can block the application's thread
      for up to one `publish_timeout` (10 s at the default), where it was refused at once.
- [ ] `tests/test_sink_retry.py::test_only_the_engine_waits_or_backs_off` passes with
      `WAIT_EXCLUSIONS` unchanged, since an `asyncio.wait` is not the engine's shared wait.
- [ ] The `DRIVER_DEADLINE_MARGIN` docstring, the timing rule's one stated home, names the
      abandonment-dating exception and why.
- [ ] The `NATSSink` class docstring and the docstrings of `_stuck_refusal`,
      `_refusal_outside_the_lock`, `_refusal_under_the_lock`, `_round_on_loop`, `_flush_on_loop`,
      `_latched`, `_publish_all` and `_publish_bounded`, and every `Raises:` clause in `nats.py`
      naming the abandoned-publish latch, state the aged rule and the wait, and none still says a
      later call is refused at once while the publish is pending.
- [ ] `_refuse_while_stuck` no longer exists in `src/`, and no docstring names it.
- [ ] The §12 item moves to *Resolved*, closed by SPEC-104, and every other doc site still stating
      the old rule carries a superseded marker.
- [ ] The pending release notes (completion ritual, step 6) say that a `NATSSink` stall a little
      longer than one `publish_timeout` no longer loses batches, and that a call during the window
      may now wait up to one `publish_timeout` where it was refused at once.
- [ ] `sh scripts/docs-lint.sh`, `sh scripts/spec-lint.sh` and
      `poetry run python scripts/docstring-lint.py` exit 0.
- [ ] This FR serves no invariant: it is the record of a change FR-001 to FR-003 make, judged by
      the completion ritual's own rules.

---

## Data Model

```python
# src/log_foundry/sinks/nats.py — NATSSink
self._stuck: tuple[tuple[asyncio.Task[Any], float], ...]
#   (abandoned publish, time.monotonic() at abandonment); replaced whole on the loop thread,
#   read without a lock by caller threads. At most one entry is pending at a time, because
#   nothing publishes while one is.
```

## API / Interface Contract

No public surface changes. The private shape the build adds:

```python
def _stuck_aged(self) -> bool: ...
#   True while the pending abandoned publish was abandoned >= publish_timeout ago;
#   what _stuck_refusal reads in place of _publish_stuck

async def _await_stuck(self) -> bool: ...
#   waits on the pending abandoned publish until abandoned_at + publish_timeout, never
#   cancelling; True only when none is pending afterwards
```

## File & Folder Structure

```
src/log_foundry/sinks/nats.py              # FR-001, FR-002; docstrings and deletion for FR-004
tests/test_sinks_nats_liveness.py          # FR-001, FR-002 (latch and wait, order, CPU)
tests/test_sinks_nats_matrix.py            # FR-001, FR-002 (cells, queued bound, mutation gate)
tests/test_sinks_nats.py                   # FR-003 (through the library)
docs/decisions/sink-lifecycle/a-drivers-liveness-flag-is-not-evidence-…-still-answer.md   # FR-004
docs/invariants.md, docs/architecture.md, docs/release-notes/<next tag>.md                   # FR-004
```

## Implementation Phases

### Phase 1: The aged latch and the wait (FR-001, FR-002)

- Date each abandonment in `self._stuck`; read the age in `_stuck_refusal`.
- Add the window-bounded, never-cancelling wait and its re-check to `_round_on_loop` and
  `_flush_on_loop`.
- Write the latch, wait, order and CPU tests; update the matrix cells and the queued bound, and
  mutation-test the re-check, the under-lock read and every new guard.

### Phase 2: The library-level outcome and the record (FR-003, FR-004)

- Write the exit, steady, slow-exit and long-stall tests through `log_foundry`.
- Rewrite the docstrings, delete `_refuse_while_stuck`, update the decision, the invariants page and
  the §12 item, and top up the pending release notes.

## Evidence

Every figure is an observed range on an unloaded machine, with its run count; the loss figures move
with load. The probe is `docs/audits/2026-11-14-probes/core-pipeline/spec104_nats_exit_latch.py`;
`sink_lost` is the sink-counted loss it prints. `transient`: the first publish after the trigger
takes 3 s against a 2 s `publish_timeout` while every other publish acks in 1 ms; 2,000 events;
`shutdown(timeout=10)`; arguments `transient <exit|steady> 1000 2 10`. Measured at `61b942c0`'s
`src/`, which differs from `03dbf502` only in `_ledger.py`, two runs each on 3.13.1 and 3.12.12
(four runs per cell):

| Design | `transient exit` | `transient steady` |
|---|---|---|
| `61b942c0` | `batch_lost` 1,632 to 1,636, `sink_lost` 344 to 348, returns in 2.02 to 2.03 s | `batch_lost` 182 to 202 over 10 or 11 batches, `sink_lost` 342 to 352 |
| This spec's design (prototype) | `batch_lost` 0, `sink_lost` 342 to 350, returns in 5.21 to 5.26 s | `batch_lost` 0, `sink_lost` 336 to 348 |

The prototype predates SPEC-102, so its `sink_lost` is the stalled round's unattempted tail, which
SPEC-102 resends, plus the abandoned publish itself; FR-003 asserts the outcome with the resend in
place. Earlier observed points: at `03dbf502`, four runs read `transient exit` `batch_lost` 1,516 to
1,720 in 2.00 to 2.13 s and `transient steady` `batch_lost` 242 over 13 batches; an earlier
prototype run exited in 5.06 s; the spec reviewer saw `03dbf502`'s `transient exit` at `batch_lost`
1,266.

`slow exit`, every publish after the stop taking 3 s, one run on 3.13.1: the prototype exits in
8.02 s of the 10 s budget with `batch_lost` 1,522 against `61b942c0`'s 1,630, losses of the same
order, inside the budget. The spec reviewer observed 6.39 s with `batch_lost` 1,228 against 1,468.

The cost of a stall to the calls that wait on the drain thread, from the same probe's `cost`
trigger (`<mode> cost 10 2 10`: 10 spans whose round meets the stall, 50 more logged 2.5 s into it,
then `flush(timeout=5)` and `shutdown(timeout=10)` each timed), at `61b942c0`, two runs each on
3.13.1 and 3.12.12:

| Mode | `03dbf502`'s design | This spec's design (prototype) |
|---|---|---|
| `long`, a 30 s stall | `flush` 0.36 to 0.43 s, `shutdown` 0.00 s, `batch_lost` 100 to 118 | `flush` 1.93 to 2.08 s, `shutdown` 0.00 s, `batch_lost` 100 to 118 |
| `transient`, a 3 s stall | `flush` 0.36 to 0.43 s and not truthy, `batch_lost` 100 to 118 | `flush` 0.63 to 0.66 s and truthy, `batch_lost` 0 |

So a stall that outlives the window costs `flush()` the rest of one window, once, and
`shutdown()` nothing; FR-003's long-stall criterion is its test.

## Revision history

- **2026-11-17, drafted.** The design was chosen over four alternatives, each prototyped and
  measured on the same probe:
  - **Keeping the engine's backoff at exit and ending it at the deadline.** It recovered only 1,339
    of 2,000 events, changes every sink, and reverses SPEC-027's rule that a shutdown cuts every
    backoff short.
  - **Waiting in every round with no age bound.** It closes both transient cases, but a long stall
    then cost `flush(timeout=5)` its whole 5.01 s and `shutdown(timeout=10)` 9.04 s, because every
    round of the drain spends a budget and `flush()`, `shutdown()` and `configure(sink=…)` wait on
    the drain thread.
  - **An exit-only wait keyed on a new `RunContext.stopped` engine flag.** It leaves the steady
    drain's loss in place and adds engine surface.
  - **A worker-only engine flag with the age bound.** It measured the same as this design, but this
    one needs no engine change and treats every caller alike, as the PING latch does.
- **Spec review, 2026-11-17.**
  - A caller queued behind the abandoning round now waits out the window (about `2 * P` from its
    start, measured 0.79 s against the old 0.60 s bound); FR-001 states it and widens the four
    cells' queued bound, and the PING latch stays read before the lock only.
  - FR-003 asserts the event-level outcome with SPEC-102's resend in place, the Evidence gains a
    `sink_lost` column, and Phase 1 adds SPEC-102 to `depends_on`.
  - FR-001's under-lock criterion now asserts that nothing reaches the loop, since the refusal text
    alone could not tell the mutant apart.
  - FR-004 adds the `DRIVER_DEADLINE_MARGIN`, `_round_on_loop`, `_flush_on_loop` and `_latched`
    docstrings, deletes the uncalled `_refuse_while_stuck`, and adds the orphan-path exception to
    the invariants page's fourth entry.
  - The wait is bounded by the window's end alone, since the call's deadline never binds; the
    detectable mutant is named.
  - The PING-latch bullet and the §12 item are narrowed to the common shape, where the buffer
    cannot have drained.
  - The orphan criterion names its stall, and the ordering criterion its injected JetStream double.
  - The re-check's rationale says what breaks, SPEC-102's ordering, rather than that every counter
    reads the same.
  - The Evidence gives observed ranges with run counts, the reviewer's runs as further points, and
    "losses of the same order" for the slow exit.
- **Rebased onto `e7b437f9`, 2026-11-17.** SPEC-102 landed, so it joins `depends_on` now rather than in Phase 1, and the Sequencing paragraph says it landed.
- **2026-11-18, completed** at `6c4d5956`, with four corrections found by the build:
  - FR-003 AC3 cannot hold for an exit backlog that leaves as one exit batch. With SPEC-102's
    resend, a batch whose abandoned publish counts as landed settles, so under "any pending" its
    refused tail is `sink_lost`, not `batch_lost`: 200 events gave `batch_lost` 0. The test uses a
    backlog that leaves as two exit batches, and the defect then loses the second whole
    (`batch_lost` 100). AC3 now says so.
  - FR-001 AC1 and FR-002 AC9 cite the matrix's `NEXT_BOUND`, so their tests live in
    `tests/test_sinks_nats_matrix.py`: the liveness file cannot import from the matrix, which
    already imports from it.
  - FR-002 AC8 reads the caller thread's CPU through `conftest.cpu_window("thread")`, which reads
    `thread_time` with garbage collection paused, because the timing gate refuses a raw
    `time.thread_time()`.
  - FR-003's exit-bound sentence is not in the module docstring: it lives in the
    `DEFAULT_PUBLISH_TIMEOUT` docstring and is restated in the class docstring. The diff edits
    neither; the description and AC8 now name both.
