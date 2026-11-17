---
type: Spec
id: SPEC-103
title: "Repeated Sink Lines at Exit Are Folded"
description: "Has the exit drain, from its first abandoned batch on, fold the stderr lines a sink or the delivery engine writes inside each later exit batch's sink call into one line per distinct text, with its summed count and how many lines it stands for, written once at the end of the exit or before an expiry line, so a dead sink at exit writes a handful of lines rather than one per ten spans, while its batches, retries, waits and deadline stay exactly as they are."
spec_status: Draft
depends_on: [SPEC-062, SPEC-090]
arc: audit-2026-11-14
arc_order: 19
---

## Overview

Take a sink that dies while the process runs. It is still dead at `shutdown()`, and the exit drain
hands it the backlog ten spans at a time. Every one of those calls fails, and every failure writes
the sink's own line on stderr. A refusing `HTTPSink` with 10,000 spans queued at exit makes 16,016
requests and writes 1,004 `log-foundry:` lines. Nothing is lost silently. The worker's own lines
for those batches are already gathered into one tally (SPEC-090 FR-003). The sink's lines and the
delivery engine's lines are not, and they bury everything else the process wrote at exit. This is
the second residual SPEC-090 filed in `docs/architecture.md` §12.

The obvious remedy, sending a dead sink fewer and larger batches, was built and measured in six
forms. Every form broke the `shutdown()` budget for some sink: any growth makes the first attempt
after a slowdown proportionally longer, and no cost bound survives that. Andrew decided on
2026-11-17 to keep today's exit batches and fold the repeated lines ("Fold lines, keep batches").
From the first abandoned exit batch on, the lines a sink or the engine writes inside a later exit
batch are folded. Each distinct line is written once more at the end of the exit, with its summed
count and how many lines it stands for. That refusing `HTTPSink` then writes 6 lines. Its 16,016
requests are unchanged, and that call volume is accepted. Nothing about batching, retries, waits
or the deadline changes, and no setting is added.

## Scope

### In Scope

- The fold. After the first abandoned exit batch, `_diag`'s per-batch writers fold rather than
  write inside each later exit batch's sink call. The folded table is held on the worker and
  written once, by the same writers and in the same order that write SPEC-090's exit tallies.
  The worker's own lines are never folded. A forked child resets the table (FR-001).
- Proof that timing and batching are today's: the slowdown probes, the exit budget, and a suite
  guard that keeps growth out (FR-002).
- The record: §12, the pipeline and diagnostics decisions, SPEC-090's marker, the README and the
  pending release notes, and the §12 item for a sink that recovers by exit (FR-003).

### Out of Scope

- **Any change to how exit batches are sized.** No growth behind a zero target, no reading a zero
  target as the cap, and no declining of later batches after an abandoned one. Each was built and
  rejected (Evidence). SPEC-090 FR-001 rule 4 stands as written: an abandoned exit batch leaves
  the target as it was.
- **The calls a dead sink receives at exit.** A refusing `HTTPSink` with 10,000 spans at exit
  still makes 16,016 requests. Andrew accepted that call volume in place of growth.
- **The worker's own lines and tallies.** Their text, order and counts are unchanged, including
  SPEC-062's `retry declined` line, which SPEC-090 FR-003 already writes at most once at exit.
- **Lines outside an exit batch's sink call**: the steady drain, the exit batches up to and
  including the first abandoned one, the orphan path, `flush()`, a sink's `close()`, its build
  and its hand-off. These are written as today.
- **A line a sink writes on a thread of its own.** The fold is per thread, so such a line is
  written as today.
- **`_diag.rejected`.** No shipped sink and not the engine reaches it. Its only callers are
  `continue_trace`'s parsers in `decorator.py`.
- **A sink that recovers by exit and is handed a second batch the exit budget did not predict.**
  FR-003 files it in §12, and SPEC-105 closes it.
- **The `NATSSink` abandoned-publish latch at exit** (SPEC-104), and any wait inside a refused
  exit round.
- **Any public setting, argument or environment variable** (invariant 10).

---

## Functional Requirements

### FR-001: From the first abandoned exit batch on, the lines inside each exit batch's sink call are folded into one per distinct text

#### Description:

**Today.** `Worker._emit_pending` (`worker.py:2704`) marks a batch whose emit begins after the
stop as an exit batch (`exit_batch = self._stopped`, `:2759`) and calls `_emit` (`:2775`). Inside
`_emit` the sink is reached in one of two places: the engine run, `_engine.run_delivery(...)`
(`:3365`), or a 1.x `sink.emit(batch)` (`:3411`). Whatever the sink or the engine writes there
goes straight to stderr through `_diag.lost`, `_diag.absorbed` or `_diag.pending`. The worker's
own abandon line for every exit batch after the first goes into a tally instead
(`_tally_exit_abandon`, `:3216`). That tally sets `_exit_abandoned_seen` when the first abandoned
exit batch is counted (`:3238`), and `_write_exit_tallies` (`:2965`) writes it once. It is called
from `_final_drain`'s `finally` (`:3190`), from `_run`'s outer `finally` (`:2384`), and from
`stop()` when its join expires, before its expiry line (`:2266`).

**The fold window.** An exit batch whose sink call begins while `_exit_abandoned_seen` is set has
its sink call folded. The window covers the engine run, or each 1.x `sink.emit`, and nothing
else in `_emit`. So the worker's own `retry declined` lines (`:3387` on the engine path, `:3441`
on the 1.x path) and its abandon lines are outside it and are written as today. The first
abandoned exit batch's own sink lines are written as today, because the flag is set only once
that batch is counted, after its sink call. The window is set on the drain thread alone, through
a thread-local in `_diag`, so a line a sink writes from a thread of its own is written at once.

**What folds.** Inside the window, three writers record a row instead of writing a line. Each
row's key is the text the line would have carried, without its count:

- `lost(what, count, detail)` is keyed on the unit and the rendered detail. It sums `count`.
- `absorbed(where, exc, detail)` is keyed on the phrase, the exception type's stored name, and
  the rendered detail. It counts failures.
- `pending(detail)` is keyed on the rendered detail. It counts occurrences.

"Rendered" means escaped and bounded exactly as the written line would be, so two rows are one
key exactly when their lines would have read the same. `rejected` does not fold. No shipped sink
and not the engine reaches it.

**How a row is recorded.** A row is added to `Worker._exit_fold` under `Worker._lock`, the lock
the exit tallies are written under. The worker hands `_diag` the table and the lock when the
window opens and withdraws them when it closes, in a `finally`, so `_diag` stays a leaf that
imports nothing from the package. Every counter a folded line names has already moved, because
the callers follow `_diag`'s record-first rule. Folding changes only whether the line is written
then. The worker never holds `_lock` across a sink call (`:3365`, `:3411`), so recording under it
cannot deadlock on the drain thread.

**How the table is written.** `_write_exit_tallies` takes the table under the lock, leaves it
empty, and writes one line per key, in the order the keys were first recorded. These folded lines
come before the abandon tally's line and the decline tally's line. So the table is written, like
the tallies, by whichever takes it first: the exit drain when it ends, or `stop()` when its join
expires, before the expiry line. A drain thread that goes on after an expired join writes what it
added afterwards at its own end. Nothing is written twice. The lines read:

- `log-foundry: lost <total> <what>(s); <detail>; <k> line(s) folded at exit`
- `log-foundry: absorbed <k> failure(s) while <where> (<Type>); <detail>; <k> line(s) folded at exit`
- `log-foundry: <detail>; <k> line(s) folded at exit`

The `; <detail>` part is present only when the folded line had one. The tail is appended after
the detail is bounded, so the bound never cuts it. Every folded line ends in the same tail,
`line(s) folded at exit`, so a reader can find them all with one search. The folded lines go
through one new `_diag` function, the module's fifth stderr write, so `_diag` stays the one
writer (invariant 11). They are never written through a `_diag.lost` call in `worker.py`,
because `tests/test_invariants_model.py` derives `_WORKER_LOST` from every such call and requires
each one's noun to be a literal.

**What a reader sees.** Every distinct line still appears at least once. If it was written before
the fold began, it appears as written. If it recurs inside the window, it appears once more as a
folded total. The prototype, on the zero-target probe below, wrote
`log-foundry: lost 999 request(s); HTTPSink, 16 attempt(s), connection error, ConnectionRefusedError errno=61; 999 line(s) folded at exit`.

**Why summing a count is truthful.** `lost` normally takes the increment its caller has just
counted. A throttled site passes a running total instead. Summing a running total would overstate
a loss. No `lost` call under `src/log_foundry/sinks/` is throttled: the throttled sites are the
worker's queue-full and post-shutdown lines and the console's echo line, none of them inside a
sink call. The sink factory's throttled line (`sinks/_factory.py:452`) is an `absorbed` whose
detail carries the running total, so each of its lines is its own key. A roster keeps it that way
(criteria).

**A forked child.** `_reinit_after_fork` (`worker.py:935`) empties `_exit_fold` on both branches,
beside the tallies (`:1104-1105`), because a child does not announce an exit the parent was
making. It also withdraws the fork target on the forking thread, which is the child's only
thread. So a sink that forks from inside a folded call leaves no window open in the child. The
field gets a row in `tests/test_lifecycle_inventory.py`'s Worker table.

**Threads of a sink's own.** The implementer's inventory of the `_diag` calls a shipped sink or
the engine can make per batch on the drain thread found none written from a sink's own thread.
`NATSSink` publishes on its own loop thread (`sinks/nats.py:536`), but announces its lines through
the run context on the drain thread (`_register`, `:853`). `KafkaSink`'s `_on_delivery`
(`sinks/kafka.py:499`) runs inside `poll` and `flush` on the calling thread. Those lines fold. A
line from a custom sink's own thread does not.

**Twin paths.** The engine run and the 1.x `emit` are folded by one window around each, so an
engine sink and a 1.x sink failing the same way write the same folded lines (invariant 6). The
steady drain is not a twin of this rule: it folds nothing.

#### Acceptance Criteria:

- [ ] `spec090_dead_http_exit.py zero 10000 3` on 3.12 and 3.13 writes at most 8 `log-foundry:`
      lines, where today it writes 1,004. Its 16,016 opener calls and `failed_batches` 1,001 are
      unchanged (invariants 2 and 11).
- [ ] `spec090_dead_http_exit.py cap 10000 3` on 3.12 and 3.13 writes at most 6 lines, where
      today it writes 22. Its 321 calls and `failed_batches` 20 are unchanged (invariants 2 and
      11).
- [ ] A suite test runs one exit twice: once as built, and once with the fold window held shut.
      The exit has a dead sink, a backlog of several exit batches, and sink lines of all three
      kinds. Both runs write the same set of distinct line texts, once counts and the fold tail
      are set aside. For each key, the folded line's count plus the counts of that key's lines
      written before the fold equals the sum the unfolded run's lines of that key name. For
      `absorbed` and `pending`, `<k>` equals the number of lines folded (invariants 2 and 11).
- [ ] A dead 1.x sink and a dead engine sink at exit, whose calls write the same `lost` and
      `absorbed` lines, write the same folded lines with the same totals (invariant 6).
- [ ] Two exits, one on the engine path and one on the 1.x path, in each of which a batch after
      the first abandoned one declines a retry, each write SPEC-062's `retry declined` line once
      and unfolded. The abandon and decline tallies' lines read as they do today (invariants 6
      and 11).
- [ ] Lines written before the first abandoned exit batch is counted are written at once and
      unfolded. That covers the steady drain's, the earlier exit batches', and the first
      abandoned exit batch's own sink lines (invariant 11).
- [ ] When the join expires during folded exit batches, `stop()` writes the folded lines once,
      before the expiry line. The drain thread then writes only the rows it recorded
      afterwards, at its own end, and no folded row is counted in both (invariants 2 and 11).
- [ ] In the zero probe's output, the folded `request(s)` line comes before
      `more exit batch(es) abandoned` (invariant 11).
- [ ] A line a sink writes with `_diag.lost`, from a thread it starts inside a folded `emit`, is
      written at once and unfolded (invariant 11).
- [ ] `_reinit_after_fork` empties `_exit_fold` with `resume=True` and with `resume=False`. A
      child forked from inside a folded sink call writes its own `_diag` lines at once.
      `tests/test_lifecycle_inventory.py` lists `_exit_fold` with its child column (invariant 12).
- [ ] `_diag`'s module docstring names its five stderr writes, and `tests/test_diag.py`'s
      tripwire (`test_diag_still_has_exactly_the_four_writers_it_documents`) is rewritten in
      place to expect five. `tests/test_diag.py`'s other rosters pass with the new writer
      (invariant 11).
- [ ] A test derived from `src/log_foundry/sinks/` fails on a `_diag.lost` call in a function that
      reads `WARN_EVERY`, because summing a running total overstates a loss (invariant 2).
- [ ] `tests/test_invariants_model.py` passes unchanged. `_WORKER_LOST`, derived from the
      `_diag.lost` calls in `worker.py`, finds the same shapes as today (invariants 2 and 11).

### FR-002: Batches, retries, waits and the deadline are today's, and growth stays out

#### Description:

The fold reads nothing that sizes a batch, schedules a retry, waits, or predicts the deadline:
`_take`, `_complete`, `_set_target`, `_retry_declined`, `_exit_longest`, and the engine's and the
worker's waits on `_stop`. Its only effect on timing is inside a folded attempt, which the worker
times: a dictionary update under `Worker._lock` takes the place of a stderr write. Exit batches
behind a zero target stay `batch_size` submissions, as SPEC-090 has them.

The guard against growth earns its place, because the growth designs broke the budget in a way no
suite test on today's tree could see. A sink that fails quickly and then turns slow mid-exit was
handed a grown batch whose first slow attempt ran far past the prediction:

- **`spec103_sqs_slowdown_exit.py`** (a black-holed `SQSSink`, on a clock only its client moves).
  On `7f400f51` the drain ended 1.60, 2.81 and 6.42 s past a 0.5 s deadline for `slow_at` 0.004,
  0.01 and 0.02 s. Today and on the fold it ends 0.18 to 0.40 s before the deadline.
- **`spec103_http_slowdown_exit.py`** (a refusing `HTTPSink` whose requests turn into 0.3 s
  timeouts after a quick window). On `7f400f51` at a 1.0 s budget, `shutdown()` returned
  `ShutdownTimeout` with `close()` run 0 times, and the drain went on 18.3 to 21.2 s after the
  return. Today and on the fold the join completes with one close.

That shape is guarded in the suite, on the SQS form, where the fake clock makes it exact. It
cannot be shown failing against `7f400f51`'s mechanism by planting it on today's modules, because
that worker predates SPEC-091 and SPEC-102. So the guard is run once in a worktree at `7f400f51`,
and the run is recorded in the delivery doc.

#### Acceptance Criteria:

- [ ] `spec103_sqs_slowdown_exit.py <slow_at> 0.5` with `slow_at` of 0.004, 0.01 and 0.02 s, for
      `sqs` and `sns`, on 3.12 and 3.13, ends the drain before the deadline, completes the join
      and runs `close()` once, as today (invariants 3 and 5).
- [ ] `spec103_http_slowdown_exit.py 1.0` and `spec103_http_slowdown_exit.py 1.0 0.002` on 3.12
      and 3.13 complete the join, with `close()` run once and the drain ended by the time
      `shutdown()` returns, as today (invariants 3 and 5).
- [ ] `spec090_exit_budget.py tree` on 3.12 and 3.13 completes the join after 2 attempts, runs
      `close()` once and reads `failed_batches == 3`, as today (invariants 3 and 5).
- [ ] A suite test on the SQS slowdown shape, through the shipped `shutdown()` on a clock only the
      client moves, ends the drain before the deadline at `slow_at` 0.004, 0.01 and 0.02 s. The
      same test, run in a worktree at `7f400f51`, ends the drain past the deadline, and the
      delivery doc records that run (invariant 3).
- [ ] `test_a_dead_sink_at_exit_writes_at_most_three_lines` passes unchanged, still reading
      `198 more exit batch(es) abandoned`. Exit batches behind a zero target are `batch_size`
      submissions (invariant 2).
- [ ] `spec103_poison_exit.py 2000` delivers 3,002 of 4,002, and
      `spec103_recovered_slow_exit.py 2 2000 1.2` completes its join, on 3.12 and 3.13, as today
      (invariants 2 and 3).

### FR-003: The record says what changed, why growth was rejected, and what is still open

#### Description:

The completion ritual writes the record. Every site that states the old claim was found by
`grep -rnE "cannot aggregate|tallies do not reach|not aggregated|follow the number of exit|per exit batch"
src tests docs README.md` at `fec8c8e8`, and by reading the README's *When the worker flushes*.
Each site and what the build does with it:

| Site | Says | The build |
|---|---|---|
| `docs/architecture.md:1584-1606`, §12 | the dead-sink residual and its three candidate closures | strikes it to one line, in SPEC-088's form (`:1536-1542`), and moves the full text under *Resolved*. It gains `→ **closed by SPEC-103 FR-001**`: the lines are folded, the calls accepted by Andrew's decision, the zero and cap rows before (`6b9d50f6`) and after on 3.12 and 3.13, and one clause for each rejected design |
| `docs/architecture.md` §12, open | nothing; the recovered-sink item is not on `main` | files it: "A sink that recovers by exit can be handed a second exit batch whose attempt the exit budget did not predict", with `spec103_recovered_slow_exit.py 2 2000 <timeout>`, its expiries at 0.45, 0.5, 0.6 and 0.65 s, invariants 3 and 5, and "Closed by SPEC-105" |
| `docs/decisions/pipeline/a-backlog-drains-in-larger-batches-and-capacity-is-not-a-setting.md` | an abandoned exit batch leaves the target as it was, and the trade for a sink refusing large batches | keeps the rule. It adds that growth behind a zero target at exit was built six ways and rejected, with the reasoning (any growth makes the first attempt after a slowdown proportionally longer, and no cost bound survives that) and the two slowdown probes' numbers. **Rejected** gains A, B, D, D2, E, E′, E3 with `1fa9ea15`, and E6, one line each with its defect. A dated History entry names this spec and Andrew's answer |
| `docs/decisions/failure-paths/one-module-writes-every-diagnostic-so-the-rules-are-applied-once-rather-than-remembered-twenty-eight-times.md` | `_diag` owns its writers | the body gains the fold: at exit `_diag` folds per-batch sink lines into one per distinct text, through its fifth write. Its fence is unchanged, and a dated History entry is added |
| `src/log_foundry/worker.py:2722-2723`, `_emit_pending` | the sink's and engine's lines are ones "the exit tallies cannot aggregate" | rewrites: they are folded after the first abandoned exit batch |
| `src/log_foundry/worker.py:3325-3330`, `_emit` | "The tallies do not reach a line the engine or the sink itself writes per batch … those follow the number of exit batches" | rewrites to state the fold window and that the worker's own lines stay outside it |
| `src/log_foundry/_diag.py` module docstring | "Four writers, one per kind of line" | names the fifth write and the fold |
| `tests/test_exit_drain_batches.py:705-727`, `:730-766` | the engine's and `HTTPSink`'s lines "grow with the number of exit batches" | the docstrings are rewritten: the lines are folded, and the call bound is unchanged. The assertions stay, since they bound lines from above |
| `docs/specs/SPEC-090-backlog-batching.md:392-400`, FR-003 *What the tallies do not reach* | the sink's and engine's lines are written once per exit batch | adds a superseded marker: closed by SPEC-103, which folds them, and where the decisions live |
| `README.md:245-254` | the exit drain's batches and its one decline line | adds a sentence: once a batch of the exit has been abandoned, the stderr lines a sink writes for each later batch are folded into one per distinct line, written at the end of the exit with its count |
| `README.md:1423` | "Each such batch writes one `log-foundry: lost N event(s)` line" | adds that at exit, after an abandoned batch, such lines are folded |
| `docs/release-notes/v1.2.1.md:210-219` (pending; no `v1.2.1` tag exists) | a sink's own line "still writes one per exit batch … those lines are one per batch" | rewrites: a sink's own lines and the engine's at exit are folded after the first abandoned batch, so a dead sink at exit writes a few folded lines, 6 for the refusing `HTTPSink` with 10,000 spans. It names the new tail `line(s) folded at exit` as a changed diagnostic string |

`docs/component-inventory.md`, `docs/implementation-guide.md` and `docs/invariants.md` state no
part of the old claim, and the fold adds no module, so the build adds nothing to them.

#### Acceptance Criteria:

- [ ] `docs/architecture.md` §12 carries the dead-sink item struck to one line naming SPEC-103.
      *Resolved* carries the full item, with the zero and cap rows before and after on 3.12 and
      3.13 and the rejected designs (serves no invariant: a record of a closed item).
- [ ] §12 carries the recovered-sink item as an open item, with its reproduction and numbers, the
      invariants the Description names, and "Closed by SPEC-105". If `main` already carries it
      when the build lands, it is left as found (serves no invariant: it files a defect and fixes
      none).
- [ ] The pipeline decision states that growth at exit was rejected, gives the reasoning and the
      slowdown numbers, lists A, B, D, D2, E, E′, E3 with `1fa9ea15`, and E6 under **Rejected**,
      and carries a History entry dated the build's day (serves no invariant: the register's
      record).
- [ ] The diagnostics decision describes the fold and carries a dated History entry naming this
      spec (serves no invariant: the register's record).
- [ ] Every site in the table above is rewritten, marked or kept as the table says. A rerun of the
      grep finds the old claim only at a struck, marked or kept site (serves no invariant: prose
      hygiene).
- [ ] `docs/release-notes/v1.2.1.md` says a dead sink at exit writes a few folded lines, and names
      the `line(s) folded at exit` tail (serves no invariant: the release artifact no gate holds
      to the code).

---

## Data Model

```python
# src/log_foundry/_diag.py (a leaf: imports nothing from the package)
_FOLD: threading.local   # .table and .lock, set only on a drain thread inside an exit sink call

def fold_into(table: dict[tuple[str, ...], list[int]] | None,
              lock: threading.Lock | None) -> None: ...   # opens or closes the window
def write_folded(rows: list[tuple[tuple[str, ...], list[int]]]) -> None: ...  # the fifth write

# src/log_foundry/worker.py, private
class Worker:
    # SPEC-103 FR-001; written under _lock; emptied by _reinit_after_fork on both branches
    _exit_fold: dict[tuple[str, ...], list[int]]   # key -> [summed count, lines folded]
```

The key's first element names the kind: `"lost"`, `"absorbed"` or `"pending"`.

---

## API / Interface Contract

None public. No signature, argument, setting, environment variable, `Health` field or ledger term
changes. A caller sees one difference: a new diagnostic line shape at exit, which the release
notes name.

```
# spec090_dead_http_exit.py zero 10000 3, on the fold prototype, 3.13.1 and 3.12.12
opener calls=16016 lines=6 failed_batches=1001          # today: 16016 / 1004 / 1001
log-foundry: lost 999 request(s); HTTPSink, 16 attempt(s), connection error, ConnectionRefusedError errno=61; 999 line(s) folded at exit
log-foundry: lost 19980 event(s); 999 more exit batch(es) abandoned
```

## File & Folder Structure

```
src/log_foundry/
├── _diag.py                       # the fold window, the three folding writers, write_folded
└── worker.py                      # the window around each exit sink call; _exit_fold; the
                                   # write in _write_exit_tallies; the fork reset; docstrings
tests/
├── test_exit_drain_batches.py     # FR-001's comparison, twins, worker-lines, expiry and thread
│                                  # tests; FR-002's SQS slowdown guard; two docstrings
├── test_diag.py                   # the five-write tripwire; the WARN_EVERY roster
├── test_backlog_batching.py       # the fork reset of _exit_fold
└── test_lifecycle_inventory.py    # the _exit_fold row
docs/
├── architecture.md                # §12 struck, Resolved; the recovered-sink item
├── decisions/pipeline/a-backlog-drains-in-larger-batches-and-capacity-is-not-a-setting.md
├── decisions/failure-paths/one-module-writes-every-diagnostic-….md
├── release-notes/v1.2.1.md
├── specs/SPEC-090-backlog-batching.md   # one marker
└── spec-delivery/SPEC-103-*.md    # the 7f400f51 guard run
README.md
```

## Implementation Phases

### Phase 1: The fold and its guards (FR-001, FR-002)

- `_diag`'s window, the folding writers and `write_folded`. The worker's window around the engine
  run and each 1.x `emit` of an exit batch, `_exit_fold`, its write in `_write_exit_tallies`, the
  fork reset, and the inventory row.
- FR-001's tests, the `test_diag` tripwire and roster, and FR-002's SQS slowdown guard. The guard
  is run once in a worktree at `7f400f51`.
- The zero, cap, exit-budget, poison, recovered and two slowdown probes on 3.12 and 3.13, for the
  record.

### Phase 2: The record (FR-003)

- §12, both decisions, the docstrings FR-003 names, the README, the pending release notes and
  SPEC-090's marker.

---

## Evidence

Measured on macOS (Darwin, 10 cores), with CPython 3.13.1 and 3.12.12, while other sessions loaded
the machine. Counts were identical on both interpreters unless a range is given. The growth
designs were measured at `03dbf502` and on the branch `spec-103-exit-zero-target`. The fold was
measured at `fec8c8e8` with its prototype applied to `6b9d50f6`'s `src/` (`main`, after SPEC-091).
`e7b437f9` (SPEC-102) read as today on every slowdown probe. The probes are under
`docs/audits/2026-11-14-probes/core-pipeline/`, each headed by what it shows.

**The designs, in the order they were tried.**

| Design | What it did | Zero probe (calls / lines / `failed_batches`) | The defect a review or probe found |
|---|---|---|---|
| today | exit batches of `batch_size` behind a zero target | 16,016 / 1,004 / 1,001 | the residual |
| A | read a zero target as the cap at exit | 336 / 24 / 21 | a sink that recovers by exit was handed 1,000 events first, and the 1.2 s join expired with the sink left open |
| B | decline every exit batch after the first abandoned one | 32 / 5 / 1,001 | one poisoned batch declined every deliverable batch behind it: 68 of 4,002 delivered, against 3,002, under a line blaming the deadline |
| C | hush every sink line after the first abandoned exit batch, and write one count | 16,016 / 6 / 1,001 | it folded lines whose detail differs (an errno, a type) into one count, and was new sink-contract surface |
| D, D2 | set the target from an abandoned batch's own rate | 352 / 25 / 22 | one quick failure lifted the target past a sink's 400-event limit: 290 and 0 of 4,002 delivered, against 3,754 and 3,962 |
| E | double behind a zero target, with no rate bound | 416 / 29 / 26 | a slow-failing sink's batches grew to 320 events, longer than any attempt the budget had seen |
| E′ | double, bounded by the rate | 416 / 29 / 26 | `max_retries=0`: the join expired in 4 to 5 of 10 runs at 1.28 to 1.32 s, where today expired none |
| E3, and its predictor fix at `1fa9ea15` | add a bound by the longest exit attempt | 416 / 29 / 26 | a step cost (a black-holed `SQSSink` at 0.3 s a request) grew 10 to 13 messages, 0.6 s against a predicted 0.4 s, and the join expired with `close()` never run in 3 of 3 runs; a quadratic cost broke it too |
| E6, with its reset at `7f400f51` | grow only after quick failures, back to zero after a slow one | 416 / 29 / 26 | a sink that turns slow mid-exit: FR-002's two slowdown probes ran the drain 1.60 to 6.42 s past the deadline, or left it running 18.3 to 21.2 s after `shutdown()` returned with `close()` never run |
| **the fold** (refined C) | keep today's batches; fold repeated sink and engine lines into one per distinct text | **16,016 / 6 / 1,001** | — |

**The fold, measured** (prototype `variant_Cfold_on_a38bd80c.diff` on `6b9d50f6`):

- **Zero, cap and lines.** `spec090_dead_http_exit.py cap 10000 3`: 321 / 4 / 20, against 321 /
  22 / 20 today. `spec090_dead_sink_exit_lines.py`: unchanged at 3 lines.
- **Budget and outcomes.** `spec090_exit_budget.py tree`: unchanged. `spec103_poison_exit.py
  2000`: 3,002 of 4,002, as today. `spec103_recovered_slow_exit.py 2 2000 1.2`: the join
  completes, as today.
- **`spec103_sqs_slowdown_exit.py`**: the drain ends 0.18 to 0.40 s before the deadline, as today,
  for `SQSSink` and `SNSSink` alike. On `7f400f51` it ended 1.60, 2.81 and 6.42 s past a 0.5 s
  deadline for `slow_at` 0.004, 0.01 and 0.02 s, and 0.10 to 5.92 s past at larger budgets.
- **`spec103_http_slowdown_exit.py`**: one close and nothing after the return, as today. On
  `7f400f51` at a 1.0 s budget it returned `ShutdownTimeout` with `close()` run 0 times, and the
  drain ran 18.3 to 21.2 s after the return. With a 2 ms quick window it ran 3.8 to 6.7 s after.
  The quick window's effect depends on the machine's speed, as the probe's header says. A re-run
  while drafting, on 3.13.1, behaved the same way:
  - At `quick` 0.002 on `7f400f51`: `ShutdownTimeout`, 0 closes, the drain 8.66 s after the
    return. Today: the join completed with one close.
  - At the default 10 ms, both trees completed with one close, because the whole backlog went
    out inside the window on that machine.
- **The suite against the prototype.** A run of the whole suite at `fec8c8e8` with the prototype
  applied in place, then restored, while drafting: 11,912 passed. Two failed, both structural
  tripwires this spec names: `test_diag_still_has_exactly_the_four_writers_it_documents` (a new
  stderr write) and `test_every_lifecycle_entry_has_a_home` (a new `Worker` attribute). No test
  that reads a sink's or the engine's lines at exit failed. `tests/test_invariants_model.py`
  passed, because its oracle reads only the worker's lines.
- **Where the prototype differs from FR-001.** The prototype folded across the whole of `_emit`,
  so it also folded the worker's `retry declined` lines. FR-001 narrows the window to the sink
  call, which excludes them. It recorded without a lock, and only `lost` and `absorbed`. None of
  these differences moves a number above, because no retry was declined in those runs and no
  `pending` line was written.

**The reasoning Andrew asked to be recorded.** Every growth design sized the next exit batch from
what the last one cost. A sink whose cost changes between batches defeats that. A batch grown
while the sink was quick is handed to it just as it turns slow, and that first slow attempt is
longer by the factor the batch grew. E′, E3, `1fa9ea15` and E6 each closed the previous
instance of this, and each was then broken by a cost shape the bound had not assumed: linear,
step, quadratic, then a slowdown. The fold changes no batch, so no cost shape can reach the
budget through it.

---

## Revision history

- **2026-11-17, the mechanism replaced.** This spec was first written to grow a dead sink's exit
  batches behind a zero target. That spec is
  `docs/specs/SPEC-103-dead-sink-at-exit-is-probed-in-growing-batches.md` on branch
  `spec-103-exit-zero-target`, last at `7f400f51`, never merged. It went through E′, E3, the
  predictor fix at `1fa9ea15`, and E6 with its reset, each answering a review. Then the slowdown
  probes broke E6. Asked whether to keep growing or keep today's batches and fold the lines,
  Andrew answered "Fold lines, keep batches (Recommended)" on 2026-11-17. This spec replaces the
  growth spec, so it returns to spec review. Nothing of the growth design lands, except as this
  history.
- **2026-11-17, a trade withdrawn.** For the growth design, Andrew accepted on 2026-11-17 that a
  sink refusing batches over 400 events would be handed batches past its limit after five quick
  exit failures in a row. ~~A size-limited sink failing five quick exit batches in a row delivers
  0 of 4,002 (`spec103_size_limited_exit.py 2 400 1-5 2000 zero`, against 3,882 on the tree
  before it)~~: superseded on 2026-11-17, because nothing grows now, so no batch can pass a sink's
  limit. It never reached `main`, so this entry is its one record.
