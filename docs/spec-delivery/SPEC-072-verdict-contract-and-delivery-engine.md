# Completed Spec — SPEC-072: Verdict Contract and Delivery Engine

## What was completed?

- `src/log_foundry/sinks/verdict.py` (public): `Outcome` (six members, closed for 1.x), `Verdict`
  (frozen, keyword-only, refused on the real type), `SinkCounters` (`@final`) and the static probe
  `read_deliver`. All four are in `log_foundry.__all__`, which goes from 30 names to 34, and
  `log_foundry.sinks.verdict` joins the frozen documented layout.
- New optional sink members, read statically and never declared on `Sink`: `deliver(batch) ->
  Verdict`, `log_foundry_counters` (exactly a `SinkCounters`, required for `deliver` to be offered)
  and `log_foundry_idempotent` (only `True` counts). The engine also reads the existing
  `max_retries`, `max_retry_after`, `log_foundry_stop_signal` and SPEC-070's `_retryable_error`.
- `src/log_foundry/sinks/_engine.py` (private): `run_delivery`, `_run`, `emit_via_deliver`,
  `outcomes_of`, `cycle_backoff`, `stop_signal_of`, `Delivery` and the backoff constants. Rounds
  resend only `RETRYABLE` events within the 1.x total of attempts and wait schedule, and commit
  once through a `Deferral`.
- `Worker._emit` and the orphan call in `api.py` call `run_delivery` first and fall back to `emit`
  unchanged; `Worker._retry_declined` holds SPEC-062's comparison once, for both loops. `_fork.py`
  reads `log_foundry_counters` on a value the walk would not push and repairs its lock, one level
  deep. `Sink`'s docstring states the contract.
- **Inert for shipped sinks**: none defines `deliver`. The engine is proven against
  `tests/verdict_sinks.py` in `tests/test_verdict.py`, `tests/test_delivery_engine.py` and
  `tests/test_delivery_paths.py`, plus new cases in `tests/test_fork_lifecycle.py`,
  `tests/test_invariants_model.py` (a `deliver` recorder over every seed), `tests/test_deferral.py`,
  `tests/test_diag.py`, `tests/test_type_door_roster.py` and the typed consumer.
- Deviations, each recorded in the spec's Revision history (2026-11-07): the two exact `__all__`
  pins were restated rather than passing unedited; an always-raising `deliver` reads `batch abandoned
  after 1 emit attempts` beside an absorbed line, not the 12-call run's line; the fork reach is at
  the value the walk would not push, not at a refused holder.

## What changed from earlier specs?

- The decision *A raising `emit` counts nothing* is rewritten to cover `deliver` (the engine is a
  fourth catcher); *After a status line the status decides* and *A client exception costs its
  chunk* gain History lines for how the engine applies them; *The frozen surface is the import
  path* now names `sinks/verdict` beside `sinks/base`; a new decision, *A sink may report per
  event*, records the contract.
- `Worker._emit`'s 1.x loop calls `cycle_backoff`, whose bounded exponent ends the `OverflowError`
  a `Worker(max_retries>=1025)` raised on the drain thread (pinned by
  `test_the_workers_emit_loop_survives_a_huge_max_retries`). It has no release note: `Worker` is
  not exported and `configure()` always builds it with `max_retries=3`, so the fix is internal.
- `docs/invariants.md` entries 2, 3, 6, 7 and 12 and `architecture.md` §8 and §9 state the second
  form; invariant 7's recorded exception extends to `UNKNOWN` and an unreadable verdict.

## Verification

- Ruff, mypy, pytest, spec-lint, docs-lint and docstring-lint locally. Integration CI is owed on
  the PR.
- Fork-walk cost of the reach, median of 7 `_fork._reinit_primitives()` calls with a configured
  `MemorySink` holding 100k four-field events, `bd8006b4` against `d11e9e90`: the builder measured
  idle 0.92 ms against 1.55 ms and loaded 447 ms against 483 ms on Python 3.12.12; a re-run on
  3.13.1 gave idle 0.83 against 1.45 ms and loaded about 385 against 419 ms. Noisy run to run.
  At `06c323b8`, whose static read replaced `inspect.getattr_static` (first diff review), the
  same measure on 3.12.12 gave idle 1.3 ms and loaded 470 to 477 ms.
- The model test's `deliver` form adds about 7 s serial across its 16 seeds (the builder's 7.0 s;
  7.3 s summed from `--durations` in a 3.13.1 re-run at `d11e9e90`). The whole file took 1.9 s at
  `-n 12` in the builder's run and 2.7 s to 2.9 s in the re-run.
- Known weak mutant: the one in which a failed run counts its drops reddens the model test on 1 of
  16 seeds only; the engine unit case `[REJECTED, DROPPED]` in `tests/test_delivery_engine.py` is
  what kills it directly.

## Follow-ups

- `emit_via_deliver` is private (`sinks/_engine.py`), yet the public `Sink` docstring points
  third-party authors at it as their engine sink's `emit`; whether to make it public is raised as a
  follow-up for the `stability-redesign` arc.
- **A registry of live `SinkCounters`** (SPEC-079). The fork reach finds counters one level from
  a value the walk declines, so an engine sink inside a third-party wrapper, or a third-party
  container-subclass sink, is not repaired, and a child whose parent held that lock at the fork
  hangs on its first commit (invariant 12's recorded exception). A registry the walk reads instead
  of searching would remove the boundary.
- **Two drafts still prescribe `inspect.getattr_static`** where this build uses
  `sinks.base._static_value`, which runs no metaclass `__eq__`/`__hash__`: SPEC-075 (about line
  143, the wrapper's read of the inner sink's counters) and SPEC-079 (about lines 242-243, the
  walk's counters read). Each should name the shared reader when it is next revised.
- **A wrapper's stop signal is read statically.** `stop_signal_of` reads
  `log_foundry_stop_signal` with the static read, so a wrapper that keeps it behind a property —
  `FilteringSink` and `MultiSink` do — reads as having none, and its engine run would wait with
  `time.sleep`. SPEC-075 must say how a wrapper's signal reaches the engine.
