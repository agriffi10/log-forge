# Completed Spec — SPEC-089: flush() Counts What Its Window Lost, and Stays Truthy Over It

## What was completed?

- **The shape** (FR-001). `FlushResult` in `results.py` is `kw_only` itself and gains
  `overflow_lost`, `batch_lost`, `sink_lost`, `drain_lost` and `stranded`, each an `int`
  defaulting to zero, with the read-only property `lost` as their sum. `__bool__` still returns
  `ok` alone, `__match_args__` stays empty, `ContinueResult` is unchanged and `__all__` gains no
  name. The typed consumer reads and builds the new fields and refuses an assignment to `lost`.
- **The window** (FR-002). It opens on the calling thread in `_lifecycle._open_window`, before
  the sweep, stamped with the pid. It closes where the drain answers the marker. The new
  `_FlushMarker` fields are `counted` (only `log_foundry.flush()` asks for a snapshot), `ledger`
  (the drain's end snapshot, written before `event.set()`), `ledger_pid` (the pid the drain took
  that snapshot in, stamped beside it so the fork rule compares the processes the two readings
  were actually taken in) and `unwinding`.
  `Worker._seal_marker` is called immediately before a marker's `event.set()`. SPEC-090 landed
  first with `_answer_marker`, the one answerer `_drain` and `_final_drain` share, so the rebase
  folded the seal into it: a `settled` flag there, and `_seal_marker` in its `finally`. With the
  call removed, 13 tests of `tests/test_flush_loss_counts.py` fail; with `unwinding` always
  false, 9 do. Its reading goes through `_lifecycle._ledger_snapshot_if_free`, which only tries the
  ledger's registration lock and returns `None` when another thread holds it; the marker is then
  answered with no snapshot and the caller reads the end.
- **Every other outcome** (FR-003). `Worker.flush` keeps its signature and verdict and is now a
  wrapper over `Worker._flush_marked(timeout, *, counted)`, which returns the verdict and the
  marker it queued, or `None` in its place. `retarget` asks for no snapshot and never waits.
  `_lifecycle._close_window` takes the marker's ledger, when the drain stamped it in this
  process, or reads the end itself, first waiting idle, within the call's deadline, for a drain
  that is dying. The wait is on `Worker._drain_accounted`, a third drain event set by
  `_terminal_failure` right after its debit and before its stderr line, and by `_run`'s outermost
  `finally` on every exit; `_reinit_after_fork` clears it where it starts a thread and sets it
  where it does not. It never waits on the drain thread itself. The wait is
  `_lifecycle._await_accounting`, taken in slices of `_ACCOUNTING_SLICE` (50 ms) and only while
  the caller's thread is in the process the window's start was read in, so a child forked from a
  signal handler before or during the wait reads its own end rather than waiting on its own live
  drain. A start that could not be read
  makes the call uncounted: no snapshot is asked for and the end is not read. `_lifecycle._with_counts` is the one place a result gains its counts, every
  `return` in `_flush_worker` goes through it, and a test derives those returns from the tree. A
  window a fork crosses counts the child's terms. No count reads negative, and a fault in either
  read is absorbed by type.
- **The record** (FR-004) and **the README** (FR-005) are rewritten in place. The pipeline
  flush decision is reversed in part and gains the window's paragraph, and the result-type
  decision is refined, both with History dated 2026-11-16. Invariant 10 and the six docstrings
  are rewritten, `v1.0.0.md` carries a superseded marker, `docs/release-notes/v1.2.1.md` carries
  the entry, and the two §12 items are updated. The Lambda idiom reads
  `drained.lost` and the `h.sink` term, and `tests/test_readme_flush_idiom.py` evaluates it from
  the README.
- **Tests:** `tests/test_flush_loss_counts.py` (FR-002 and FR-003, plus FR-004's phrase guard),
  `tests/test_readme_flush_idiom.py` (FR-005) and `tests/test_public_surface.py` (FR-001).
- **FR-002 AC-7 is two tests.** The first counts the calling span's swept buffer. The second,
  `test_a_span_open_on_another_thread_is_in_no_count`, is the one SPEC-091 rewrites. SPEC-091's
  pointer grep was narrowed to `'events.open_spans == 2'`, because the bare form also matched
  `tests/test_event_ledger.py:517`.

## What changed from earlier specs?

- SPEC-021, SPEC-036 and SPEC-050: a truthy `flush()` no longer means every event reached the
  sink. It means the drain that carried them finished and abandoned no batch in the window. The
  marker mechanism, the three places a flush reports over and SPEC-050's narrowing stand.
- SPEC-034: a result grows by appended keyword fields as well as by reason values. `==` and
  `repr()` on a `FlushResult` now include the counts.
- A `flush(timeout=None)` whose drain dies under it now waits until that drain has debited what
  it held, which is before its stderr line, where it returned `"abandoned"` at once.
- SPEC-088: **the ledger's `stranded` moment moved.** The queue of a dying drain counted as
  `stranded` only once its thread had ended; it now counts so once `Worker._drain_accounted` is
  set or the thread has ended (`Worker._drain_terms`). The event follows the drain's last take
  and last debit, so the reading's soundness argument holds, and a `flush()` waiting out a dying
  drain no longer has to wait out its stderr line to see `stranded`, which a `join` did and which
  deadlocked against a caller holding a lock that line needs.
- SPEC-088: **`flush()` reads the ledger under the registration lock**, as `health()` does, so a
  call started while another thread is inside that section (a first credit, a fold) waits for
  it. Measured with the second diff review's `p_reglock_shutdown.py`: a `flush(5)` started while
  another thread held the lock and then called `shutdown(1)` inside it waited out the section and
  returned `"retired"`, where at the base it was answered `ok=True` before the shutdown; the
  shutdown itself was prompt and closed the sink. The drain's own reading only tries the lock.
- Rosters: `_SITE_FLOOR["log_foundry._lifecycle"]` in `test_worker_predicate_roster.py` rises
  from 96 to 100, the derived count, with four new rows and the `_flush_worker` row re-keyed.
  After the second diff review two rows are re-keyed again, to `counted=counted` and to the
  conditional end read, and the count measured on the branch is still 100. After the
  execution-frame review the floor rises to 101, with a row for `_await_accounting`'s slice test,
  and the `_close_window` binding is re-keyed to pass the window's start. The lifecycle
  inventory gains `_drain_accounted`'s construction and field rows, beside `_drain_settled`'s. Two
  pins are re-keyed to `_flush_marked`: the `Worker.flush` lock site in `test_runtime_holder.py`
  and the `drain-flush` pin's signature in `tests/model_pins.py`.

## Premises corrected

- FR-004's "no pending notes file exists on `main` at `9b54b370`" no longer held when the
  build started: `docs/release-notes/v1.2.1.md` existed, so the entry went there.
- The spec's `file:line` cites predate #378 to #383 and are anchored to `9b54b370`. Read them at
  that commit, not on `main`.

## Verification

- The two 200-run loops under a switch interval of 1e-6 (FR-003 AC-4 and AC-5) took 0.27 s and
  0.41 s under `-n 12` at a load average near 150, on `src/` as at `ce8e6904`. Phase 3 changed
  only `flush()`'s docstring in `src/`, so `595fa44f` runs the same code.
- FR-005's "false at `9b54b370`" was checked once and is not a committed test. The Lambda idiom
  as it stood on `origin/main`, byte-identical to `9b54b370`'s, evaluates False on the
  all-dropped case.
- **FR-003 AC-6 and AC-7 were revised after the first diff review.** Their bounds (0.5 s plus
  50 ms, and 50 ms) sat under the repo's `STALL_ALLOWANCE` of 0.5 s, and AC-6's scenario spent
  none of the call's budget before the drain died, so a wait that restarted the deadline passed
  it. AC-6's sink now spends 1.0 s of a `flush(2.0)` before raising, a spy pins the `join`
  argument at no more than the 1.0 s left, and the ceiling is 2.5 s; AC-7's ceiling is 2.5 s,
  half the `flush(5)` a self-wait would take, with its one-line assertion as the discriminator.
- **The second diff review** (the system frame) found a blocker, two majors and two minors, all
  answered in code, and its probes are regression tests in `tests/test_flush_loss_counts.py`,
  each bounded, with the unbounded cases in a watchdog child: a flush and a shutdown made inside
  the registration lock, a flush from a collector callback inside it, a flush started while
  another thread holds it, a flush holding a lock that a replaced `sys.stderr` writes under while
  the drain dies (with a 2.0 s budget and with none), a closing-sweep answer read while the
  thread lives, a flush meeting a drain stopped before its debit, a flush meeting a clean exit
  before its sweep, a fork after the seal, and a persistent ledger fault.
- **The drain-thread guard is defence in depth now.** The dying drain's stderr line is written
  after `_drain_accounted` is set, so a `flush()` made from that line finds the event set, and
  removing the guard was run as a mutant: it survives, an equivalent mutant. FR-003 AC-7's test
  asserts the outcome, a prompt return and one stderr line, not the guard.
- **Mutants run after the second diff review**, each in place on the branch, each against the
  whole of `tests/test_flush_loss_counts.py` (and, for the ledger and fork ones, with
  `test_event_ledger.py`, `test_event_ledger_defects.py`, `test_fork_lifecycle.py`,
  `test_worker.py` and `test_ledger_check.py`). Killed: no wait at all; no unwinding mark in
  `_drain`; no unwinding mark in `_final_drain`; the second dying arm removed; a busy-poll wait;
  a `wait()` that ignores the deadline; a deadline that restarts; the fork pid check removed; a
  drain snapshot that blocks on the lock; a ledger that ignores `_drain_accounted`;
  `_terminal_failure` not setting it; `_run`'s `finally` not setting it; a `join` in place of the
  event; an uncounted start that still counts; a resumed child keeping it set (killed only once
  `test_a_child_of_a_dead_drain_counts_its_own_queue_as_queued` was added, having first
  survived). Survived, both equivalent: the drain-thread guard removed, as above, and a retired
  child leaving it clear, since a retired child's inherited thread reads dead and `is_alive()`
  then decides alone; it is set there for the same reason `_drain_finished` is.
- **The execution-frame review** ran the build and found one minor defect, answered in code,
  and one older one, filed. The minor: a child forked on the caller's thread during the wait for
  a dying drain, or after the marker's unwinding answer and before the wait, waited on its own
  resumed drain's `_drain_accounted`, which that live drain sets only when it ends, so the
  child's `flush(None)` never returned and a bounded one waited out its budget. The reviewer's
  probes at `96cd0480`, re-run on this branch: `q_fork_wait.py none` and
  `q_fork_before_wait.py none` reported the child still waiting after 6 s, and with `3` the child
  returned after its whole 3.0 s; after the fix, with the second probe's wrapper forwarding
  `_close_window`'s new fourth argument, each child returns in 0.2 s to 0.6 s with every count
  0, the parent with `drain_lost=3`, and the retired-worker variant's child in 0.65 s. Two
  regression tests fork on the caller's thread, one inside the wait and one before it. AC-6's
  spy now asserts the slices: none unbounded, their timeouts summing to at most the 1.0 s left,
  and at most one per 50 ms of it and one more. Mutants, each in place against the whole of
  `tests/test_flush_loss_counts.py`, all killed: the pid check removed from the sliced wait (both
  fork tests); the process taken at the wait's entry rather than the window's start (the
  before-the-wait test); a busy-poll slice; a slice of `None`; a deadline that restarts; and no
  wait at all.
- **Filed, not fixed:** the item *A `flush()` made inside the event ledger's registration lock
  waits on a drain that waits on that lock, when the sink logs through the library* in
  `architecture.md` §12 Open. It reproduces at `251a9321`, before any of this spec's code.
- **Settled by the execution-frame review:** waiting on `_drain_accounted` inside `yielding()`
  does not deadlock, for the shape run: `q_yielding.py dying`, a `flush(None)` inside a
  `sink_factory` on an application thread while the drain dies under it, returns in 0.3 s with
  `drain_lost=3` and `stranded=3`.
- **Not settled.** One plan premise is not tested: a late `marker.ledger` written after a
  timeout is harmless.
