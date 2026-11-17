# Completed Spec — SPEC-102: NATSSink Resends the Tail Its Deadline Left Unattempted

## What was completed?

- **The verdict and the line** (FR-001, FR-002), in `sinks/nats.py`: `_verdict_of` answers an
  `unattempted` event `RETRYABLE`, not sink-counted, in a round where something was published, and
  `_register` registers that round's `NATSSink publish_timeout <t>s` line `When.LAST`. The
  docstrings of `_verdict_of`, `_register`, `_publish_all`, `_round` and the class state the new
  rule, the old sentences struck.
- **The tests** (FR-001 to FR-004): `tests/test_sinks_nats_unattempted.py`, 25 tests on the
  harness's `Clock` and doubles, with a per-call `RoundScript` and a `RecordingClient` of its own so
  the harness's recorded shape is untouched; `tests/test_sinks_nats_engine_matrix.py` names its
  eight departures in `DEPARTURES`, pins each to FR-004's table through `_moved`, and gains
  `test_the_departures_are_eight_cells_the_harness_names`. The 1.x fixture is unchanged.
- **The record** (FR-006): the decision *An event a NATS round never handed to the driver is
  resendable* (`docs/decisions/sink-delivery/`); §12's item struck and moved under *Resolved*, and
  the narrower residual filed as a new open item; the JetStream asymmetry's wider reach in
  `docs/invariants.md` and §12's `Nats-Msg-Id` item; `docs/release-notes/v1.2.1.md`, its known gap
  replaced by a *Fixed* entry, a narrower known gap for the residual and a tenth upgrade
  exception. The README states neither rule.
- **Two corrections to the spec at completion**, in its revision history: FR-005 AC1 now asks that
  no run leaves a loss silent, with the bound and the measured rates in FR-005's description rather
  than as a pass count, and FR-001's exit-drain sentence no longer says the tail behind an abandoned
  publish is always lost there, since it is delivered if that publish finishes between two rounds.

## What changed from earlier specs?

- SPEC-074's NATS mapping: an unattempted event beside a published one was `REJECTED` and its line
  `ALWAYS`. Through the worker, eight cells of SPEC-074's 1.x matrix now depart from 1.x by name;
  every `emit` cell still matches it.
- SPEC-090's `NATSSink` residual is closed, down to a bound: a batch is delivered while its delivery
  fits the worker's `max_retries + 1` rounds of one `publish_timeout` each.

## The probe (FR-005)

`docs/audits/2026-11-14-probes/core-pipeline/spec090_nats_ack_backlog.py 0.002 200 <budget>`.
3.13.1 is the worktree's `.venv`; 3.12.12 is pyenv's with `PYTHONPATH=src`. `src/` is
behaviourally the same at every commit named: `00ec9029` changes only docstring and line layout,
and `06b7ff41` and `1823bbe1` change no `src/`. Each run: acked of 400, `sink.failed`, `deliver`
calls, longest `deliver`. The orchestrating session's twelve replace an earlier set of twelve,
whose output was not kept.

| Budget | Interpreter, runs | Commit | Runs |
|---|---|---|---|
| 0.2 s | 3.13.1, implementer's first three | `b35ad63a`, tree clean | 361, 42, 10, 0.209 s, with three abandoned lines and one `lost 39 event(s); NATSSink publish_timeout 0.2s` line; 400, 3, 8, 0.201 s; 400, 4, 8, 0.201 s |
| 0.2 s | 3.13.1, implementer's next five | `b35ad63a` by the worktree's reflog, which held it from 09:19 to 09:35, and the runs were at 09:32; whether the tree was clean then was not recorded | 400, 3, 6, 0.200 s; 400, 3, 7, 0.201 s; 400, 4, 8, 0.201 s; 400, 3, 8, 0.201 s; 400, 3, 8, 0.201 s |
| 0.2 s | 3.13.1, orchestrating session's twelve, load averages 16.26, 54.58, 61.66 before and 15.68, 51.96, 60.55 after | `06b7ff41` | 400, 3, 6, 0.200 s; 400, 3, 6, 0.200 s; 400, 3, 6, 0.200 s; 400, 2, 6, 0.200 s; 400, 3, 6, 0.200 s; 400, 2, 6, 0.200 s; 400, 2, 6, 0.200 s; 400, 3, 6, 0.200 s; 400, 3, 6, 0.200 s; 400, 3, 6, 0.200 s; 400, 3, 6, 0.200 s; 400, 3, 6, 0.200 s |
| 0.2 s | 3.12.12, implementer's three | `b35ad63a`, tree clean | 400, 3, 8, 0.201 s; 400, 3, 8, 0.201 s; 400, 2, 8, 0.202 s |
| 10 s | 3.13.1 | `b35ad63a`, tree clean | 400, 0, 3, 0.492 s |
| 10 s | 3.12.12 | `b35ad63a`, tree clean | 400, 0, 4, 0.511 s |

In every run that acked 400, `sink.failed` equalled its `JetStream publish abandoned` lines: the
unconfirmed publish invariant 2 records, whose ack the probe's fake still delivers. The one miss
here is a bound of the fix, not a defect in it: at 0.2 s the four default rounds give 0.8 s against
SPEC-090's 0.5 s batch, and that run's delivery slowed past the margin. It is §12's open item *A
JetStream `NATSSink` backlog batch whose delivery outlasts the worker's rounds still loses its
tail*.

**The diff review's runs.** Diff review frame 1 ran an instrumented copy of the probe 60 times at
0.2 s on 3.13.1 at `1823bbe1`, at load averages of 24 to 38; its per-run figures are the
reviewer's and are not tabled here. 58 acked 400. Run A acked 399 with `sink.failed` 3, three
abandoned lines and no `publish_timeout` line: the last batch's abandoned publish was still pending
when the process exited, counted and named as unconfirmed. Run B acked 382 with `sink.failed` 19,
one abandoned line and `lost 13` and `lost 5` `publish_timeout` lines, its rounds taking 0.237 to
0.394 s against the 0.2 s budget: CPU starvation, the bound above. Both satisfy FR-005 AC1 as
revised, since nothing in either was silent. Measured rates, none a threshold: 19 of 20 on
3.13.1 and 3 of 3 on 3.12.12 here, and 58 of 60 in the review.

**Diff review frame 2's reproduction.** The wider reach is not JetStream-only: a resend round in
which every publish raises is answered whole and counts each raise, in either mode. With rounds
`[["late-ok"], ["raise"], ["raise"]]` and `max_retries=2` the rounds were offered `[[0,1],[1],[1]]`,
event 1 was never delivered, `failed` read 2 and `lost 1 event(s); NATSSink publish,
RuntimeError` was written twice, where at `03dbf502` the event was `REJECTED` once with one
`publish_timeout` line. The ledger still balances (`sink_lost` 1): this is 1.x's own per-call
count, which the 1.x fixture's `every-publish-raising-core-injected` and
`every-event-unencodable-core-injected` worker cells record as `failed` 9 for 3 events, and which
invariant 2 records as `NATSSink`'s exception. For a deterministic raise it is an over-count of
`failed` only; for an expired JetStream ack wait it is also the duplicating asymmetry. Invariant 2,
§12's `Nats-Msg-Id` item, the release notes and the decision now say both.

## Verification

- At `00ec9029`, every gate exited 0: ruff, mypy, spec-lint, spec-lint-test, docs-lint,
  docstring-lint, and pytest on 3.13.1 (11685 passed, 39 skipped, 21 xfailed) and on 3.12.12 in a
  separate venv (11689 passed, 35 skipped, 21 xfailed). The collect-only diff against `dd358b0e` is
  additions only, with one id re-keyed by line number,
  `test_numeric_door_roster.py::test_removing_a_helper_call_is_reported[sinks/nats.py:NATSSink.max_reconnect_attempts@473]`
  to `@484`, not a removal.
- Three mutants, each planted inside its function and reverted: M1 (unattempted back to
  `REJECTED`) failed 28, both FR-001 tests among them; M2 (the line back to `ALWAYS`) failed 29,
  including `landed-beside-the-unattempted-js-injected-worker` (four lines where one is pinned) and,
  which the spec did not predict, all eight matching `emit` cells; M3 (the tail marked sink-counted)
  failed 24: on `failed` in the four landed worker cells, the run-out test (`(3,0,0) == (3,2,0)`),
  the decline test and the three `max_retries` cases, and also the seven `_verdict_of` table rows,
  on the counted set, and the eight `emit` cells of both departure families.
- Owed to CI: the integration suite, whose conftest needs all nine services, was not run locally;
  CI's integration job is its first run.
