---
type: Spec
id: SPEC-102
title: "NATSSink Resends the Tail Its Deadline Left Unattempted"
description: "Makes an event a NATSSink round never handed to the driver resendable when something else in the round was published, so the worker's existing retry rounds deliver the tail of a backlog batch that a short publish_timeout cuts, rather than counting it lost; closes the NATSSink residual SPEC-090 filed in architecture.md section 12, and the 1.2.1 tag waits for it."
spec_status: Completed
depends_on: [SPEC-047, SPEC-061, SPEC-062, SPEC-072, SPEC-074, SPEC-088, SPEC-090]
arc: audit-2026-11-14
arc_order: 18
---

## Overview

`NATSSink` bounds a whole round by one `publish_timeout`. When the budget runs out partway through
a batch, the events it never reached are left unattempted. If something else in that round was
published, today those events are counted lost and announced, and the worker never offers them
again. Since SPEC-090 a batch under a backlog grows to what the sink delivers in half a flush
interval, so a JetStream `NATSSink` whose budget is shorter than that loses the tail of a backlog
batch: with a 2 ms ack, 200 spans and a 0.2 s budget, 193 of 400 events were acked at `be686953`
on 3.13.1 and 3.12.12, where all 400 were before SPEC-090 (`docs/architecture.md` §12). Those events never reached the driver, so nothing about them can have landed,
and sending them again cannot duplicate them. This spec makes them resendable, so the worker's
existing retry rounds deliver them. An event that is still undelivered when those rounds end is
counted and announced once, as before. Andrew decided that the 1.2.1 release waits for this fix, so
its pending release notes state it as fixed rather than as a known gap.

## Scope

### In Scope

- `_verdict_of`: in a round where something was published, an `unattempted` event is
  `Outcome.RETRYABLE` and not sink-counted (FR-001).
- `_register`: in that branch the `NATSSink publish_timeout <t>s` line is registered `When.LAST`,
  so it names only events still unresolved when the run ends on that round (FR-002).
- The bound: the resends are the engine's existing rounds, so no new wait is added (FR-003).
- `tests/test_sinks_nats_engine_matrix.py` keeps comparing every cell with the frozen 1.x record
  except eight named departures, each pinned to its new value (FR-004).
- The probe measurement, recorded in the delivery doc (FR-005).
- The record: the `nats.py` docstrings, `docs/architecture.md` §12, `docs/release-notes/v1.2.1.md`,
  and one decision file (FR-006).

**The twin paths** (invariant 6). The worker path runs up to `max_retries + 1` rounds and is where
the behaviour changes. `NATSSink.emit` runs one round (`emit_via_deliver`), so its counts, lines and
return are unchanged, and so is `MultiSink`, which never offers `deliver` and calls a child's
`emit`. The orphan path delivers a batch of one event, which cannot hold an unattempted event beside
a published one. A `FilteringSink` or `TransformSink` around a `NATSSink` offers `deliver` exactly
when the sink does (SPEC-075) and maps the verdict to its own indices, so it resends the tail as the
worker does. The exit drain's resend rounds are subject to SPEC-062 and SPEC-090's prediction, as
any retry is. A `deliver` called directly, outside any run, now returns `ACCEPTED` for the published
event and `RETRYABLE` for the unattempted ones, and still writes `lost <n> event(s); NATSSink
publish_timeout <t>s` at once, because a detached round commits a `LAST` line at once, as every
engine sink's `LAST` line does.

**The accepted JetStream asymmetry reaches further.** A resend of the tail is a fresh round, and a
round that publishes nothing is answered whole by the unchanged nothing-published branch: every
event `RETRYABLE` and sink-counted, each raised publish counted and announced `ALWAYS`. So when a
resend round's only publish raises, that raised publish is resent by the next round and is counted
once per round it raised in, although a JetStream ack wait that expired may have persisted it. That
is the asymmetry invariant 2 already records for a batch in which nothing else landed, closed only
by the `Nats-Msg-Id` item; before this spec it could happen only in a batch's first round, and now
it can also happen in a round that resends a tail. This spec keeps the nothing-published rule as it
is, adds no run-context signal to tell a resend round from a first round, and records the wider
reach (FR-002, FR-006).

### Out of Scope

- **A publish that raised.** A raised publish stays `REJECTED` in a round where something was
  published. A round that publishes nothing, a resend round included, is answered whole as before,
  so its raised publish is resent and counted once per call it raised in (*The accepted JetStream
  asymmetry reaches further*, above). A JetStream ack wait that expired may still have persisted
  the event, so such a resend can duplicate it. Making that safe needs the `Nats-Msg-Id` header,
  which is `docs/architecture.md` §12's own item and which SPEC-095 also keeps out of its scope.
- **A publish abandoned at the deadline.** It stays `UNKNOWN`, or `ACCEPTED` where a core
  confirmation judges it, exactly as today, and is never resent. Its line is unchanged.
- **A round that published nothing.** Every event of it is already `RETRYABLE` and sink-counted,
  with its `SETTLED_LAST` line (SPEC-088). Nothing in that branch changes, including when the round
  is a resend of a tail.
- **SPEC-095's resend count.** SPEC-095 counts a resend of an event a send *may have landed*, and
  lists `NATSSink` among the sinks that never mark one. An unattempted event was never sent, so
  its resend is not one SPEC-095 counts, whichever of the two specs is built first. This spec adds
  no mark and no counter.
- **SPEC-093 and SPEC-094.** Neither touches `NATSSink`'s round, its deadline or the unattempted
  tail. SPEC-094 names `NATSSink` only as the sink that already confirms core batches.
- **A larger default budget, a per-event budget, or a cap on the batch `NATSSink` is handed.** The
  batch rule is SPEC-090's and stays as it is, and `publish_timeout` keeps its default of 10 s and
  its meaning: one bound for one round.
- **Re-recording the 1.x fixture.** `tests/data/nats_engine_matrix_93113e1b.json` is a frozen record
  of 1.x at `93113e1b` and is not regenerated; `tests/nats_engine_matrix_harness.py`'s recorded
  shape is not changed, because the harness must still run unchanged against `93113e1b`.
- **The README**, unless the build finds a sentence in it stating the old behaviour. At `03dbf502`
  none does.

---

## Functional Requirements

### FR-001: An event the round never handed to the driver is resent

#### Description:

`_verdict_of` maps each event's fate in a round where something was published (`ok` or
`abandoned`). Today an `unattempted` event there is `REJECTED`. It becomes `RETRYABLE`, and it is
not marked sink-counted.

**Why `RETRYABLE` is safe.** An `unattempted` event is a *definitive non-delivery*: `_publish_all`
never handed it to the driver, so no attempt wrote it. That is the distinction SPEC-094 draws
between a report no attempt could have escaped and one that *may have landed*, and it is the
footing of the settled decision *A client exception costs its chunk, and is provable non-delivery
for it*, which already makes such a chunk `RETRYABLE`. SPEC-094 answers its definitive reports
`REJECTED` because each is a refusal that every attempt would meet again. This one is not: the
cause is a spent budget, and the next round brings a fresh one. So the event is resendable, and a
resend cannot duplicate it, which is what *No shipped sink declares itself idempotent* requires of
any resend.

**Both kinds of unattempted event.** It covers the events whose turn came after the budget was
spent and the events after a publish abandoned at the deadline (`_publish_all`'s `break`). For the
second kind, the abandoned-publish latch (`_stuck_refusal`) refuses every later round `RETRYABLE`
before it publishes while that publish is still pending, so a resend cannot overtake it. Once it
clears, the next resend round delivers, but only if it clears before the run's rounds run out. On
the worker path that means within the backoff waits between the remaining rounds. On the exit
drain the stop event is set, so those waits are zero, ~~the tail behind an abandoned publish is
refused every round and lost~~ which leaves the publish little room to finish between two rounds:
the tail behind it is lost unless it does, counted once and named by `lost <n> event(s); refused
by NATSSink after others landed, SinkDeliveryError` (corrected at completion, 2026-11-17).

> **Superseded in part by SPEC-104.** The latch no longer refuses every later round while the
> publish is pending. A round made within one `publish_timeout` of the abandonment waits for it on
> the loop until that window ends, and is refused `RETRYABLE` only if it is still pending then, or
> once the window has aged; it still never publishes while the publish is pending, so the order
> above holds. On the exit drain too, a tail behind a publish that finishes inside its window is
> now delivered. The decision: *A driver's liveness flag is not evidence*
> (`docs/decisions/sink-lifecycle/`).

#### Acceptance Criteria:

- [ ] In a round where at least one event is `ok` or `abandoned`, `_verdict_of` returns
      `Outcome.RETRYABLE` for every `unattempted` event and leaves it out of the sink-counted set;
      every other fate maps exactly as it does at `03dbf502`.
- [ ] A deterministic test drives a `NATSSink` through `Worker(sink, max_retries=2)._emit`, with the
      round deadline moved by `tests/nats_engine_matrix_harness.py`'s `Clock` and never by wall
      time, and shows that every event the first round left unattempted is in the second round's
      batch and that each is published exactly once across the run.
- [ ] A deterministic test with a published, a raised and an unattempted event in one round shows
      that the raised event is in no later round's batch and that the unattempted one is.
- [ ] A round in which nothing was published still answers every event `RETRYABLE` and
      sink-counted, and a round refused before its first publish is unchanged.
- [ ] Reverting the new branch so that `unattempted` maps to `REJECTED` again makes both tests
      above fail: the test that every unattempted event is in the second round's batch, and the
      test that the unattempted event beside a raised one is in a later round's batch. That is the
      mutant they exist to kill, and it is run once at the build.
- [ ] Serves invariants 2 and 7.

### FR-002: An unattempted event is counted and named once, and only if the run ends without it

#### Description:

`_register` announces the unattempted remainder of a round where something was published with
`_diag.lost("event", n, "NATSSink publish_timeout {t}s")`. Today it is registered `When.ALWAYS`.
It becomes `When.LAST`, so it is written only when the run ends on that round with those events
still unresolved. The engine then counts them once in `failed`, through `unresolved`, because a run
in which anything was accepted or unknown never fails, so it settles and counts its unresolved
events (`_engine.py` `_tally` and `run`). The line's text does not change (invariant 11).

When the run ends on a later round, that round names what it left. If it was refused by the
abandoned-publish latch, the engine's own line names the refused events, `lost <n> event(s);
refused by NATSSink after others landed, SinkDeliveryError`, in place of the `publish_timeout` line.
This change of line is intended: the events are still counted once and announced once.

#### Acceptance Criteria:

- [ ] Through the worker, when every resend round publishes something, an event a later round
      delivered is in no counter and is named by no `log-foundry:` line.
- [ ] A deterministic test pins the wider asymmetry. Through `Worker(sink, max_retries=2)._emit`
      with a JetStream `NATSSink` and a 5-event batch, where round 1's first publish spends the
      budget, round 2's first publish raises after the budget is spent, and round 3's publishes
      all succeed: the rounds are offered events `[0..4]`, `[1..4]` and `[1..4]`; the double sees
      6 publishes, event 1 twice; and the run ends with `losses().failed` 1 and the one line
      `lost 1 event(s); NATSSink publish, RuntimeError`.
- [ ] Through the worker, an event still unresolved when the rounds run out is counted once in
      `losses().failed` and named once, by the last round's `publish_timeout` line when that round
      registered one, or by the engine's `refused by NATSSink after others landed` line when the
      last round was refused.
- [ ] A run whose next round SPEC-062's prediction declines ends on the round that registered the
      line, so the unattempted events are counted once and the `publish_timeout` line is written
      once; a test drives this through `run_delivery`'s `decline`.
- [ ] Through `emit`, every case of the engine matrix records exactly what 1.x recorded: one call,
      the same counts, the same lines in the same order, the same raise.
- [ ] Registering the line `When.ALWAYS` again makes a test fail on the lines it pins: at
      `03dbf502` with FR-001 applied, `landed-beside-the-unattempted-js-injected` through the
      worker then writes four lines where FR-004 pins one.
- [ ] Marking the unattempted events sink-counted makes a test fail on `failed`, because the engine
      would then count none of the events left unresolved.
- [ ] Serves invariants 2, 6 and 11.

### FR-003: The resend adds no wait

#### Description:

Each resend is one of the engine's existing rounds: within `run_delivery`'s `rounds_allowed` (the
worker's `max_retries + 1`), separated by the existing backoff through `sinks._retry.wait` on the
stop signal, asked SPEC-062's prediction before it starts, and itself bounded by one
`publish_timeout`. So the worst case per batch is what a stalled server already costs, and the
exit drain's bound is SPEC-090's as it stands. A round refused by the abandoned-publish latch
publishes nothing and spends no budget.

> **Superseded in part by SPEC-104.** A round refused once the latch has aged still spends no
> budget, but a round made in the abandoned publish's first `publish_timeout` waits for it until
> that window ends, inside the round's own deadline, before it publishes or is refused; the exit
> drain's bound is unchanged. The decision: *A driver's liveness flag is not evidence*
> (`docs/decisions/sink-lifecycle/`).

#### Acceptance Criteria:

- [ ] Through the worker, a batch makes at most `max_retries + 1` `deliver` calls, whatever its
      unattempted events.
- [ ] In each of the four `an-abandoned-publish-*` cases through the worker, the double sees two
      publishes, as in 1.x: the rounds that resend the tail while the abandoned publish is pending
      publish nothing.
- [ ] No sleep, wait, timer or deadline is added to `sinks/nats.py`, `_engine.py` or `worker.py`.
- [ ] Serves invariant 3.

### FR-004: The 1.x matrix names its eight departures, and still catches a ninth

#### Description:

`tests/test_sinks_nats_engine_matrix.py::test_the_engine_matches_1x` compares each case's record
with the one recorded at 1.x. After FR-001 and FR-002, exactly eight worker cells depart from it.
The test keeps comparing every other cell for equality with the frozen fixture, and pins each of
the eight to its new value on the fields this spec moves and to the 1.x record on every other
field. Measured with FR-001 and FR-002 applied at `03dbf502`, twice, with the same result:

| Cell (`worker` path) | 1.x | Now |
|---|---|---|
| `landed-beside-the-unattempted-{core,js}-{injected,owned}` | `calls` 1, `publishes` 1, `failed` 4, `lost 4 event(s); NATSSink publish_timeout 0.2s` | `calls` 3, `publishes` 3, `failed` 2, `lost 2 event(s); NATSSink publish_timeout 0.2s` |
| `an-abandoned-publish-{core,js}-{injected,owned}` | `calls` 1; first line `lost 1 event(s); NATSSink publish_timeout 0.2s` | `calls` 3; first line `lost 1 event(s); refused by NATSSink after others landed, SinkDeliveryError` |

In the `an-abandoned-publish` cells every other line, `failed`, `failed_batches`, `publishes` and
`raised` stay as 1.x recorded them.

#### Acceptance Criteria:

- [ ] `test_the_engine_matches_1x` asserts equality with the fixture for every cell outside the
      eight, so a change to any other cell still fails it.
- [ ] Each of the eight cells asserts that it differs from the fixture on each field the table
      moves, equals the table's value there, and equals the fixture on every other field.
- [ ] The departures are one named set of exactly eight cells in the test; a ninth cell that
      differs from the fixture fails, and a departure cell whose record goes back to 1.x fails.
- [ ] `tests/data/nats_engine_matrix_93113e1b.json` is byte for byte what it is at `03dbf502`, and
      `test_the_fixture_names_its_commit_and_command` and both `test_a_planted_verdict_is_caught`
      cases still pass.
- [ ] Serves invariants 2, 6 and 7.

### FR-005: The backlog probe delivers every event

#### Description:

`docs/audits/2026-11-14-probes/core-pipeline/spec090_nats_ack_backlog.py` drives a JetStream
`NATSSink` behind a fake ack through the real worker. Its fake JetStream ignores the timeout it is
handed, so a publish in flight at the deadline is abandoned rather than raised, and each cut round
writes one `NATSSink unconfirmed, JetStream publish abandoned` line. That abandoned, unconfirmed
publish is the accepted asymmetry invariant 2 records, closed only by the `Nats-Msg-Id` item. The
probe depends on wall-clock timing, so it is a measurement recorded in the delivery doc and not a
CI test. With FR-001 and FR-002 applied at `03dbf502` on 3.13.1 it acked 400 of 400 with
`sink.failed` 4, four abandoned lines and seven `deliver` calls, and at a 10 s budget 400 of 400
with `sink.failed` 0.

**The fix has a bound, measured at the build** (revised at completion, 2026-11-17). A backlog
batch is delivered when its delivery fits in the worker's `max_retries + 1` rounds, four at the
default, of one `publish_timeout` each. SPEC-090 sizes the batch to about half a flush interval at
the measured rate, 0.5 s at the default, so at a 0.2 s budget the rounds give 0.8 s against 0.5 s.
A run whose delivery slows by more than that margin between measuring and sending still loses the
tail past its last round: counted in `failed` and named by the last round's `publish_timeout`
line, never silent. A publish abandoned at the deadline whose ack is still pending when the
process exits is not acked either, and is counted and named as unconfirmed. Measured rates, none
of them a threshold: this session's runs acked all 400 in 19 of 20 on 3.13.1 and 3 of 3 on
3.12.12, and the diff review's instrumented copy in 58 of 60 on 3.13.1; every miss was counted and
named (the delivery doc gives each).

#### Acceptance Criteria:

- [ ] ~~`spec090_nats_ack_backlog.py 0.002 200 0.2`, run three times on each of 3.12 and 3.13,
      acks 400 of 400 in every run~~ ~~(first completion revision) acks 400 of 400 in at least 19
      of 20 runs on 3.13 and in all three on 3.12~~ (revised again at completion after the diff
      review, since a pass count fitted to its own runs is no criterion; the measured rates are in
      the description above): `spec090_nats_ack_backlog.py 0.002 200 0.2`, run at least three times
      on each of 3.12 and 3.13, leaves nothing silent in any recorded run: every event not acked is
      counted in `sink.failed` and named by a `NATSSink publish_timeout` or `JetStream publish
      abandoned` line, so `sink.failed` equals the sum of those lines' counts and that sum is at
      least 400 minus the acked count.
- [ ] `spec090_nats_ack_backlog.py 0.002 200 10`, run once on each of 3.12 and 3.13, acks 400 of 400
      with `sink.failed` 0.
- [ ] The delivery doc records each run's command, interpreter, acked count, `sink.failed`, the
      `deliver` calls, the longest `deliver` and the commit it ran at.
- [ ] Serves invariant 2.

### FR-006: The record states the new rule

#### Description:

The docstrings that state the old rule are rewritten in place, with the superseded sentence struck
and the new one marked with this spec. §12's item is closed. The release notes stop listing the
loss as a known gap and state the fix, because the 1.2.1 tag waits for this spec. The decision
joins the `sink-delivery` area as one file, fence and entry together.

#### Acceptance Criteria:

- [ ] `_verdict_of`, `_register` and `_publish_all` in `sinks/nats.py` state that an unattempted
      event in a round where something was published is `RETRYABLE`, and why, and that its line is
      `LAST`; no docstring in `nats.py`, the class docstring included, says without a strike that
      such an event is `REJECTED` or lost.
- [ ] `docs/architecture.md` §12's item *A JetStream `NATSSink` whose `publish_timeout` is shorter
      than half the flush interval loses the tail of a backlog batch* is struck where it stands
      with a pointer, and moved under *Resolved* marked closed by SPEC-102 with FR-005's
      measurement, saying that a tail behind an abandoned publish that is still pending when the
      rounds run out, ~~always the case on the exit drain~~ (on the exit drain, unless that
      publish finishes between two rounds), is still lost, counted and named.
- [ ] The JetStream asymmetry `docs/invariants.md` records under *Every accepted event is
      delivered, or counted and announced* and §12's item *A
      timed-out JetStream publish is not retried, and cannot safely be* each say that the
      asymmetry also occurs in a round that resends a tail and publishes nothing, citing SPEC-102.
- [ ] `docs/release-notes/v1.2.1.md` no longer has the known-gaps entry *A JetStream `NATSSink` with
      a `publish_timeout` under half a second can lose the tail of a backlog batch*; its *Fixed*
      section states the fix under a heading ending `(SPEC-102)`, including the changed worker
      lines, the tail behind a still-pending abandoned publish that is still lost (~~always on the
      exit drain~~ on the exit drain unless that publish finishes between two rounds), and that a resend round in which only a raised publish ran sends that publish
      again; and the opening paragraph that lists the specs the release carries names SPEC-102.
- [ ] The release notes' list of exceptions to a code-free upgrade, which at `03dbf502` names nine
      and none for `NATSSink`, gains code that counts or matches `NATSSink`'s `publish_timeout`
      lines on the worker path; the count and the sentence saying which section lists each are
      updated to match.
- [ ] `docs/decisions/sink-delivery/` gains one decision file, *An event a NATS round never handed
      to the driver is resendable*, whose fence states the rule and the definitive-non-delivery
      reason, that an abandoned publish is never resent, and that a raised publish stays `REJECTED`
      in a round where something was published while a round that publishes nothing, a resend
      round included, is answered whole as before, so its raised publish is resent and counted
      once per call it raised in; the file ends in a dated `## History`;
      `sh scripts/gen-index.sh` regenerates `docs/decisions/index.yaml` with it.
- [ ] `grep -n "unattempted\|tail of a backlog" README.md` finds nothing, or what it finds states
      the new rule.
- [ ] Serves no invariant: these are the prose that records the rule FR-001 to FR-003 change, held
      by the reviewer rather than by a test.

---

## Data Model

No new type, field or counter. The change is to one mapping and one registration rule:

```python
# src/log_foundry/sinks/nats.py — _verdict_of, a round where something was published
for state in states:
    if state == "ok":
        outcomes.append(Outcome.UNKNOWN if unconfirmed else Outcome.ACCEPTED)
    elif state == "abandoned":
        judged = published.abandoned_counted or unconfirmed
        outcomes.append(Outcome.UNKNOWN if judged else Outcome.ACCEPTED)
    elif state == "unattempted":
        outcomes.append(Outcome.RETRYABLE)        # was REJECTED (FR-001)
    else:                                          # "raised"
        outcomes.append(Outcome.REJECTED)
return tuple(outcomes), frozenset()               # unattempted is not sink-counted

# src/log_foundry/sinks/nats.py — _register, the same branch
if unattempted:
    context.announce(
        lambda: _diag.lost("event", unattempted, f"NATSSink publish_timeout {timeout}s"),
        when=When.LAST,                            # was When.ALWAYS (FR-002)
    )
```

| Fate in a round where something was published | Before | After |
|---|---|---|
| `ok` | `ACCEPTED`, or `UNKNOWN` when unconfirmed | unchanged |
| `abandoned` | `ACCEPTED`, or `UNKNOWN` when counted or unconfirmed | unchanged |
| `raised` | `REJECTED` | unchanged |
| `unattempted` | `REJECTED`, line `ALWAYS` | `RETRYABLE`, line `LAST` |

## API / Interface Contract

No public signature changes. What a caller can see moves only on the worker path and on a wrapper
offering `deliver`:

```
# Worker path, a 5-event batch whose first publish spends the 0.2 s budget, max_retries=2
1.x and today:  1 deliver call, 1 published, failed 4
                log-foundry: lost 4 event(s); NATSSink publish_timeout 0.2s
After:          3 deliver calls, 3 published, failed 2
                log-foundry: lost 2 event(s); NATSSink publish_timeout 0.2s
```

## Configuration / Environment

None.

## File & Folder Structure

```
src/log_foundry/sinks/nats.py                      # _verdict_of, _register, docstrings
tests/test_sinks_nats_engine_matrix.py             # the eight named departures (FR-004)
tests/test_sinks_nats_unattempted.py               # FR-001 to FR-003, new, on the harness's doubles
docs/architecture.md                               # §12 item struck and moved under Resolved
docs/release-notes/v1.2.1.md                       # known gap removed, Fixed entry added
docs/decisions/sink-delivery/an-event-a-nats-round-never-handed-to-the-driver-is-resendable.md
docs/decisions/index.yaml                          # generated
docs/spec-delivery/SPEC-102-nats-resends-the-unattempted-tail.md
```

## Implementation Phases

### Phase 1: The verdict, the line and their tests (FR-001 to FR-004)

- Change `_verdict_of` and `_register` as the Data Model shows.
- Write `tests/test_sinks_nats_unattempted.py` on the harness's `Clock`, `Script` and `Client`,
  observing each `deliver` call's batch through a wrapper of its own so that `run_case`'s recorded
  shape stays as 1.x recorded it.
- Name the eight departures in `tests/test_sinks_nats_engine_matrix.py`.
- Run the three named mutants (FR-001's reverted branch, FR-002's `ALWAYS` and its sink-counted
  marking) once each, and record the result in the delivery doc.

### Phase 2: The measurement and the record (FR-005, FR-006)

- Run the probe as FR-005 says, on both interpreters.
- Rewrite the docstrings, close §12's item, edit the release notes and write the decision file in
  the completion ritual.

---

## Revision history

- **2026-11-17, authored.** The fix and its measurements came from the orchestrating session's
  prototype at `03dbf502`, which the author re-ran: with the two changes applied, the matrix's cells
  differed from 1.x in exactly the eight worker cells FR-004's table gives, identically on two runs,
  and no `emit` cell differed; registering the line `ALWAYS` again made
  `landed-beside-the-unattempted-js-injected` write four lines, the engine's settled line among
  them; and the probe gave the figures in FR-005's description. The prototype was reverted. The
  orchestrating session's own run of the full suite with the prototype applied failed exactly the
  eight cells and passed the rest.
- **2026-11-17, spec review.** A resend round that publishes nothing is answered whole by the
  unchanged nothing-published branch, so its raised publish is resent and counted per call. The
  reviewer reproduced it, and the author re-ran it with the prototype: offered `[0..4]`, `[1..4]`,
  `[1..4]`, 6 publishes, `failed` 1. The first draft said a raised publish is never resent, and
  that criterion and the planned fence were false. The orchestrating session kept the
  nothing-published rule and declined a run-context signal, so the spec now states the wider
  asymmetry, pins it with a test (FR-002) and records it in invariant 2, §12 and the release notes
  (FR-006). Also: the Overview's acked figure is now §12's anchored 193 at `be686953`, since the
  first draft's unanchored 200 did not reproduce (the reviewer measured 185 to 190 at `03dbf502`);
  FR-001 says a tail behind an abandoned publish is delivered only if that publish clears before
  the rounds run out, never on the exit drain; the exit drain's rounds are "subject to" the
  prediction rather than "declined by" it; FR-001's mutant criterion names its tests; and the twin
  paths gain a direct `deliver` outside any run, whose outcomes the author also re-ran.
- **2026-11-17, completion.** FR-005 AC1 revised to the measured outcome. Twenty runs on 3.13.1 at
  `b35ad63a`, `00ec9029` and `06b7ff41`, whose `src/` differ only in docstring and line layout,
  acked 400 of 400 in nineteen; the twentieth acked 361, with `sink.failed` 42 and one `lost 39 event(s);
  NATSSink publish_timeout 0.2s` line. The criterion as drafted, all 400 in every run, holds only
  while a batch's delivery fits the worker's four rounds of 0.2 s; FR-005's description now states
  that bound, and `docs/architecture.md` §12 files the residual. Also: FR-001's exit-drain sentence
  said the tail behind an abandoned publish is always lost there; it is lost unless that publish
  finishes between two rounds, which the zero waits leave little room for. FR-006's §12 and release
  notes criteria carried the same word, now struck there.
- **2026-11-17, completion, after diff review frame 1.** FR-005 AC1 was fitted to its own data: its
  pass count of 19 of 20 was the measured rate. The review ran an instrumented copy 60 times at
  `1823bbe1` and saw two misses, one with two `publish_timeout` lines and one with no such line,
  where an abandoned publish was still pending at exit; the first revision's line-sum clause would
  have failed that run although its loss was counted and named. AC1 now asks only that nothing is
  silent, and the rates live in the description. FR-006's "nearly always" for the exit drain, which
  nothing measured, now reads as FR-001 does.
