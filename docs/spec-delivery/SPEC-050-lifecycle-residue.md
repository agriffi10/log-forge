# Completed Spec — SPEC-050: Lifecycle Residue

## What was completed?

R3, R4, R6, R11 and R13 of `docs/audits/2026-09-02-pre-1.0-audit.md`, each reproduced by running
it against `eb80099` before the spec was written and re-run against the fix.

- **FR-001** — `Worker.shutdown`'s expiry branch now calls `_release_waiters()`; the drain thread
  registers markers it has taken out of the queue so that sweep can reach them, releases them
  after answering, drops them for a forked child, and self-answers one taken after the sweep has
  already run. The first attempt shipped only the first of those — the audit's prescribed remedy,
  which does not cover the audit's own probe — and a peer session reproduced R3 verbatim against
  it; a third ordering — a marker enqueued *after* the sweep — was then found by a reviewer and
  closed by having `flush()`'s post-put re-check consult `_drain_settled`, the only flag the
  expiry branch sets. A review of *that* then found `flush(timeout=None)` could still wait
  forever one line earlier, blocked in `Queue.put` on a full queue where no marker exists for the
  sweep to answer — pre-existing rather than introduced, reproduced against `main` as a control,
  and closed by taking that wait in slices. New members: `_given_up`, `_settled`, `_take_marker`, `_release_marker`, `_put_marker` and `_PUT_POLL_SECONDS`. The pessimistic-verdict population is correspondingly
  wider, recorded in `docs/decisions.md`
  as a narrowing of "`flush()` answers from the drain that carried the events"; the converse is
  untouched, since `delivered` is only ever written by the owning drain. A
  `flush(timeout=None)` parked behind a stuck sink used to wait forever on a drain that call had
  just given up on; it now returns `FlushResult(ok=False, reason="abandoned")`.
- **FR-002** — an in-flight inline `close()` is recorded in a slot holding an `Event`, on the
  worker path (`Worker._closing`, drained in `_close_if_owed`) and the orphan path
  (`_lifecycle._orphan_closing`, in `_close_orphan_sink`). A caller that did not claim the close
  waits for it, capped at `DEFAULT_CLOSER_GRACE`. Before: `atexit` returned through a running
  close and process exit killed it — twelve buffered events lost at 0.31 s, nothing on stderr.
- **FR-003** — `decorator._flush` resolves the worker *before* detaching the span's buffer, so
  `_end` can count `len(span.events)` into `in_span_lost` when the close is absorbed. A process
  that cannot start a drain thread reported `in_span_lost=0` over total loss.
- **FR-004** — a sink stranded by an unconfirmed `configure(sink=...)` is recorded in
  `Worker._unclosed_swaps` and closed, detached, by the first `shutdown()` that finds the drain
  thread ended. New: `Worker._FORK_SKIP`, `Worker._discard_owed_swap`, `_lifecycle.discharge_owed`.
- **FR-005** — `Worker.submit` re-reads `_shutdown_done` after its put, so a submission that
  raced the final drain reaches `submitted_after_shutdown` and its stderr line.

**Deviations.** The spec listed three prune sites for the owed-swap record; the confirmed-swap one
survived every mutant because a recorded sink is never the live sink, so it was removed and the
invariant stated where it stood. The spec said the orphan re-arm route was guarded but not
asserted because no reachable sequence was found; building it showed it *is* reachable
(`A.closes == 2`), so the discharge is atomic with taking the record and the route is tested.

**Corrected by the second diff review, which built rather than read.** The orphan path's
in-flight-close record was a single slot holding one `Event`; a second orphan close overwrote it
and its own completion then cleared it, so a later bystander waited for nothing and the process
exited through the first close — the very defect FR-002 exists to remove, one level down. It is
now a count plus an idle gate, which is the correction SPEC-045 made to the owed-close record for
the same reason. Measured: the bystander waited 0.000 s and lost five events, and now waits
0.955 s and loses none.

**A fifth reviewer was spent, out of budget and said so.** The orphan record was *replaced*
rather than revised after both diff reviews, so those reviews examined a mechanism that no longer
existed — which `process.md` prices at one more pass in the frame that catches a replacement's own
class of defect. It found three: an async `KeyboardInterrupt` landing between taking the in-flight
count and the `try` leaked it permanently (measured, once every few hundred iterations under a real
`SIGINT` storm); a `fork()` from *inside* the inline close left the child at `-1`, which
`if not _orphan_closing` never satisfies again; and the suite was green only because three tests
that leave a close running happen to sit *after* the one that measures the no-wait path — reversing
that pair fails it at 4.01 s. The first two share one edit (take inside the `try`, guarded by a flag
and floored at zero); the third is a `conftest` reset.

**Rejected, with the reasoning in `decisions.md`:** `health().retired` reading `True` for a worker
built after an orphan-only `shutdown()`. The field documents an action the caller took, the
alert idiom is the *pair*, and the fresh worker's events are not lost silently.

## What changed from earlier specs?

- **SPEC-030's "the previous sink is left open"** now means *for now*, not forever. Superseded in
  place at `architecture.md` §7, `docs/decisions.md`, and four `worker.py` docstrings including the
  public `Health.incomplete_swaps`. **Owed elsewhere:** `README.md:253` and `README.md:1009` carry
  the same claim and belong to the release-surface session.
- **SPEC-031's `_release_waiters`** gains a third caller.
  `test_an_expired_shutdown_leaves_the_sentinel_for_the_live_thread` asserted the opposite and is
  superseded in place: the events still reach the sink, so what the sweep trades is a verdict.
- **`Health.in_span_lost`** has two causes now, not one. Its "always means the data" clause is
  struck in place, here and in `decorator._note_in_span_loss`.
- `_close_if_owed` takes a `deadline` parameter; one test monkeypatched it with a zero-arg lambda.

## Verification

All five gates green locally at exit 0 — `ruff`, `mypy --strict`, `pytest`, `spec-lint`,
`docs-lint` — plus `docs-lint-test` (the linter was not touched, so this was a check, not a
requirement). Twenty-six new tests. Every guard whose failure would be silent was mutation-tested
one at a time, restored by copying from the scratchpad rather than `git checkout --`, with
`log_foundry.__file__` confirmed to resolve to this worktree: sixteen mutants planted, fifteen
killed, one documented as equivalent (reading `_orphan_closing` under the lock versus the value
captured under the same lock). Five were killed only after the test that named them was rewritten
to reach the guard — including two the first diff review found by mutating what the author had
not: the orphan path's arming condition, which only a *third* successive `shutdown()` can see,
and its wait, which took a flat cap and made the two paths disagree by two seconds on
`shutdown(timeout=0)`. The second diff review ran a 200-run concurrency fuzz per side: under the
documented single-threaded `configure()` contract, sinks that took events and were never closed
fall from 124/200 to 0/200, with the double-close rate unchanged; at a real interpreter exit, a
daemon-thread `shutdown()` goes from losing twelve events silently in 0.203 s to delivering all
twelve in 1.506 s. Every bound held at 0.000 s CPU.
