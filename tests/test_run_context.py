"""SPEC-073 FR-002: the engine's run context, against scripted sinks.

The context is family-neutral, so it is proven here against ``tests/verdict_sinks.py``'s scripted
sinks and a bare counter owner, apart from the HTTP family that uses it
(``tests/test_http_rounds.py``).
"""

from __future__ import annotations

import gc
import threading
import weakref
from typing import Any

import pytest

from log_foundry import _diag
from log_foundry.sinks import _engine
from log_foundry.sinks._engine import RunContext, When, detached, one_round, run_context
from log_foundry.sinks.verdict import Outcome, Verdict
from verdict_sinks import Scripted, WaitRecorder, events

A, R, J = Outcome.ACCEPTED, Outcome.RETRYABLE, Outcome.REJECTED


class Owner:
    """A counter owner as a shipped sink is one: named counters under one ``_counter_lock``."""

    def __init__(self) -> None:
        self._counter_lock = threading.Lock()
        self.always = 0
        self.settled = 0
        self.last = 0


def _stop() -> WaitRecorder:
    stop = WaitRecorder()
    stop.set()
    return stop


def _deliver(sink: Any, batch: list[dict[str, object]], **kwargs: Any) -> _engine.Delivery:
    kwargs.setdefault("worker_retries", None)
    kwargs.setdefault("stop", _stop())
    delivery = _engine.run_delivery(sink, batch, **kwargs)
    assert delivery is not None
    return delivery


def _lost(err: str) -> list[str]:
    return [line for line in err.splitlines() if "log-foundry: lost" in line]


# --- 2.1: rounds, notes, threads, nesting ----------------------------------------------------


def test_round_and_notes_across_three_rounds() -> None:
    """``round`` reads 1, 2 and 3 for one run's three rounds, and ``notes`` is one ``dict``."""
    seen: list[tuple[int, int]] = []
    owner = Owner()

    def step(batch: list[dict[str, object]]) -> Verdict:
        context = run_context()
        notes = context.notes(owner)
        notes["rounds"] = int(notes.get("rounds", 0)) + 1  # type: ignore[call-overload]
        seen.append((context.round, id(notes)))
        outcome = A if context.round == 3 else R
        return Verdict.uniform(outcome, len(batch))

    sink = Scripted([step], max_retries=2)
    _deliver(sink, events(1))

    assert [round_number for round_number, _ in seen] == [1, 2, 3]
    assert len({notes for _, notes in seen}) == 1, "the same dict in every round"


def test_two_threads_never_share_a_context() -> None:
    """Two runs on two threads against one sink see their own notes and commit their own counts."""
    owner = Owner()
    barrier = threading.Barrier(2, timeout=5.0)
    contexts: dict[str, RunContext] = {}
    leaked: list[object] = []

    def step(batch: list[dict[str, object]]) -> Verdict:
        context = run_context()
        name = threading.current_thread().name
        contexts[name] = context
        context.notes(owner)[name] = True
        barrier.wait()
        leaked.extend(key for key in context.notes(owner) if key != name)
        context.count(owner, "always", 1, when=When.ALWAYS)
        return Verdict.uniform(A, len(batch))

    sink = Scripted([step])
    threads = [
        threading.Thread(target=sink.emit, args=(events(1),), name=f"run-{i}") for i in range(2)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10.0)

    assert len(contexts) == 2
    assert contexts["run-0"] is not contexts["run-1"]
    assert leaked == [], "neither run saw the other's notes"
    assert owner.always == 2, "each run committed its own ledger, once"


def test_a_nested_run_sees_a_fresh_context_and_restores_the_outer() -> None:
    """A run inside a round has its own context; the outer one is current again afterwards."""
    owner = Owner()
    inner_rounds: list[int] = []
    outer: list[tuple[RunContext, int, object]] = []

    def inner_step(batch: list[dict[str, object]]) -> Verdict:
        context = run_context()
        inner_rounds.append(context.round)
        assert "outer" not in context.notes(owner)
        return Verdict.uniform(A if context.round == 2 else R, len(batch))

    inner = Scripted([inner_step], max_retries=1)
    inner.log_foundry_stop_signal = _stop()

    def outer_step(batch: list[dict[str, object]]) -> Verdict:
        context = run_context()
        context.notes(owner)["outer"] = "kept"
        inner.emit(batch)
        after = run_context()
        outer.append((after, after.round, after.notes(owner).get("outer")))
        assert after is context
        return Verdict.uniform(A, len(batch))

    _deliver(Scripted([outer_step]), events(1))

    assert inner_rounds == [1, 2]
    assert [(round_number, note) for _, round_number, note in outer] == [(1, "kept")]


# --- 2.2: the commit rules -------------------------------------------------------------------


def _counting(script: list[Outcome], owner: Owner) -> Scripted:
    """A sink whose every round counts one of each rule into ``owner`` and answers its script."""
    calls = [0]

    def step(batch: list[dict[str, object]]) -> Verdict:
        context = run_context()
        context.count(owner, "always", 1, when=When.ALWAYS)
        context.count(owner, "settled", 1, when=When.SETTLED)
        context.count(owner, "last", 1, when=When.LAST)
        outcome = script[min(calls[0], len(script) - 1)]
        calls[0] += 1
        return Verdict.uniform(outcome, len(batch))

    return Scripted([step], max_retries=len(script) - 1)


def test_settled_commits_only_for_a_settled_run() -> None:
    settled, failed = Owner(), Owner()
    _deliver(_counting([A], settled), events(1))
    _deliver(_counting([J], failed), events(1))
    assert settled.settled == 1
    assert failed.settled == 0


def test_always_commits_for_both() -> None:
    settled, failed = Owner(), Owner()
    _deliver(_counting([A], settled), events(1))
    _deliver(_counting([J], failed), events(1))
    assert (settled.always, failed.always) == (1, 1)


@pytest.mark.parametrize("ending", ["exhausted", "accepted", "declined"])
def test_last_commits_only_the_final_rounds_entries(ending: str) -> None:
    """Three rounds register three ``LAST`` counts; one commits, a declined run's included."""
    owner = Owner()
    if ending == "declined":
        sink = _counting([R], owner)
        sink.max_retries = 0
        asked: list[float] = []

        def decline(longest: float) -> bool:
            asked.append(longest)
            return True

        delivery = _deliver(sink, events(1), worker_retries=3, decline=decline)
        assert delivery.declined and sink.calls == 1
        assert owner.last == 1, "the decline ended the run after round one, its last"
        return
    sink = _counting([R, R, R] if ending == "exhausted" else [R, R, A], owner)
    _deliver(sink, events(1))
    assert sink.calls == 3
    assert owner.always == 3
    assert owner.last == 1, "only the final round's entry"


def test_a_base_exception_from_round_two_commits_nothing(
    capsys: pytest.CaptureFixture[str],
) -> None:
    owner = Owner()
    calls = [0]

    def step(batch: list[dict[str, object]]) -> Verdict:
        calls[0] += 1
        if calls[0] == 2:
            raise KeyboardInterrupt
        context = run_context()
        context.count(owner, "always", 1, when=When.ALWAYS)
        context.announce(lambda: _diag.lost("event", 1, "Owner, registered"), when=When.ALWAYS)
        return Verdict.uniform(R, len(batch))

    with pytest.raises(KeyboardInterrupt):
        _deliver(Scripted([step], max_retries=2), events(1))

    assert owner.always == 0
    assert capsys.readouterr().err == ""
    assert _engine._RUNS.get() == (), "the stack is popped on the way out"


def test_counts_commit_before_lines_in_registration_order(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Each line runs after every count committed, outside the owner's lock, in order."""
    owner = Owner()
    seen: list[tuple[str, int, bool]] = []

    def line(tag: str) -> None:
        free = owner._counter_lock.acquire(blocking=False)
        if free:
            owner._counter_lock.release()
        seen.append((tag, owner.always, free))

    def step(batch: list[dict[str, object]]) -> Verdict:
        context = run_context()
        context.announce(lambda: line("first"), when=When.ALWAYS)
        context.count(owner, "always", 2, when=When.ALWAYS)
        context.announce(lambda: line("second"), when=When.ALWAYS)
        return Verdict.uniform(A, len(batch))

    _deliver(Scripted([step]), events(1))
    assert seen == [("first", 2, True), ("second", 2, True)]


# --- the engine's own line, owners, detached and immediate contexts ---------------------------


@pytest.mark.parametrize("registers", [True, False], ids=["sink-line", "no-sink-line"])
def test_a_sink_line_suppresses_the_engines_settled_line(
    registers: bool, capsys: pytest.CaptureFixture[str]
) -> None:
    """SPEC-072 FR-003's refinement: a settled loss is announced by the sink's lines alone."""

    def step(batch: list[dict[str, object]]) -> Verdict:
        if registers:
            run_context().announce(
                lambda: _diag.lost("event", 1, "Scripted, its own line"), when=When.ALWAYS
            )
        return Verdict(outcomes=(A, J))

    _deliver(Scripted([step]), events(2))
    lines = _lost(capsys.readouterr().err)
    if registers:
        assert lines == ["log-foundry: lost 1 event(s); Scripted, its own line"]
    else:
        assert len(lines) == 1 and "delivery round(s) to Scripted" in lines[0], lines


def test_a_line_that_will_not_commit_does_not_suppress_the_engines_line(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Keyed on lines that commit: a ``LAST`` line from an earlier round leaves no silence."""
    calls = [0]

    def step(batch: list[dict[str, object]]) -> Verdict:
        calls[0] += 1
        if calls[0] == 1:
            run_context().announce(lambda: _diag.lost("event", 1, "never"), when=When.LAST)
            return Verdict.uniform(R, len(batch))
        return Verdict(outcomes=(A, J))

    _deliver(Scripted([step], max_retries=1), events(2))
    lines = _lost(capsys.readouterr().err)
    assert len(lines) == 1 and "delivery round(s) to Scripted" in lines[0], lines


def test_a_count_commits_into_its_named_owner() -> None:
    """A delegate counting for another object commits into that object, never into itself."""
    named = Owner()

    def step(batch: list[dict[str, object]]) -> Verdict:
        run_context().count(named, "always", 3, when=When.ALWAYS)
        return Verdict.uniform(A, len(batch))

    sink = Scripted([step])
    _deliver(sink, events(1))
    assert named.always == 3
    assert sink.log_foundry_counters.losses().failed == 0


def test_a_context_holds_no_owner_after_its_run() -> None:
    """Invariant 12: once its run ends a context references no owner and no notes."""
    box: list[Any] = [Owner()]
    alive = weakref.ref(box[0])
    kept: list[RunContext] = []

    def step(batch: list[dict[str, object]]) -> Verdict:
        context = run_context()
        kept.append(context)
        context.notes(box[0])["x"] = 1
        context.count(box[0], "always", 1, when=When.LAST)
        return Verdict.uniform(R, len(batch))

    _deliver(Scripted([step], max_retries=1), events(1))
    box.clear()
    gc.collect()
    assert alive() is None, "the context released the owner"
    assert kept[0]._entries == [] and kept[0]._notes == {} and kept[0]._lines == []


@pytest.mark.parametrize(
    ("outcome", "settled"), [(A, 1), (J, 0)], ids=["settled-verdict", "failed-verdict"]
)
def test_a_round_outside_any_run_commits_on_return(outcome: Outcome, settled: int) -> None:
    """A direct ``deliver`` is a one-round run: its own last, settled unless its verdict fails."""
    owner = Owner()

    def round_fn(batch: list[dict[str, object]]) -> Verdict:
        context = run_context()
        assert context.round == 1
        context.count(owner, "always", 1, when=When.ALWAYS)
        context.count(owner, "settled", 1, when=When.SETTLED)
        context.count(owner, "last", 1, when=When.LAST)
        assert owner.always == 0, "registered, not yet committed"
        return Verdict.uniform(outcome, len(batch))

    verdict = one_round(round_fn, events(2))

    assert verdict.outcomes == (outcome, outcome)
    assert (owner.always, owner.settled, owner.last) == (1, settled, 1)
    assert _engine._RUNS.get() == ()


def test_a_round_inside_a_run_counts_into_that_run() -> None:
    """Inside a run ``one_round`` adds no context: the enclosing run commits the count."""
    owner = Owner()
    observed: list[int] = []

    def inner_round(batch: list[dict[str, object]]) -> Verdict:
        run_context().count(owner, "settled", 1, when=When.SETTLED)
        return Verdict.uniform(A, len(batch))

    def step(batch: list[dict[str, object]]) -> Verdict:
        one_round(inner_round, batch)
        observed.append(owner.settled)
        return Verdict.uniform(J, len(batch))

    _deliver(Scripted([step]), events(1))
    assert observed == [0]
    assert owner.settled == 0, "the enclosing run failed, so its settled count never committed"


def test_detached_commits_a_raising_call_as_failed_and_reraises() -> None:
    owner = Owner()

    def call() -> int:
        context = run_context()
        context.count(owner, "always", 1, when=When.ALWAYS)
        context.count(owner, "settled", 1, when=When.SETTLED)
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        detached(call, lambda _: False)
    assert (owner.always, owner.settled) == (1, 0)


def test_detached_commits_nothing_on_a_base_exception() -> None:
    owner = Owner()

    def call() -> int:
        run_context().count(owner, "always", 1, when=When.ALWAYS)
        raise SystemExit(3)

    with pytest.raises(SystemExit):
        detached(call, lambda _: False)
    assert owner.always == 0


def test_outside_any_run_a_count_and_a_line_commit_at_once(
    capsys: pytest.CaptureFixture[str],
) -> None:
    owner = Owner()
    context = run_context()
    context.count(owner, "always", 2, when=When.LAST)
    assert owner.always == 2
    context.announce(lambda: _diag.lost("event", 2, "Owner, at once"), when=When.SETTLED)
    assert _lost(capsys.readouterr().err) == ["log-foundry: lost 2 event(s); Owner, at once"]


def test_the_drops_view_counts_only_in_a_settled_run() -> None:
    """``drops(owner)`` keeps the ``Deferral`` shape and registers everything ``SETTLED``."""
    settled, failed = Owner(), Owner()

    def step_for(owner: Owner, outcome: Outcome) -> Any:
        def step(batch: list[dict[str, object]]) -> Verdict:
            view = run_context().drops(owner)
            view.count("settled")
            view.count("settled", 2)
            return Verdict.uniform(outcome, len(batch))

        return step

    _deliver(Scripted([step_for(settled, A)]), events(1))
    _deliver(Scripted([step_for(failed, J)]), events(1))
    assert (settled.settled, failed.settled) == (3, 0)


def test_a_misspelt_owner_counter_is_absorbed_and_its_line_still_written(
    capsys: pytest.CaptureFixture[str],
) -> None:
    owner = Owner()

    def step(batch: list[dict[str, object]]) -> Verdict:
        context = run_context()
        context.count(owner, "no_such_counter", 1, when=When.ALWAYS)
        context.announce(lambda: _diag.lost("event", 1, "Owner, still said"), when=When.ALWAYS)
        return Verdict.uniform(A, len(batch))

    _deliver(Scripted([step]), events(1))
    err = capsys.readouterr().err
    assert "absorbed a failure while counting a delivery (AttributeError)" in err
    assert "1 count(s) for Owner not counted" in err
    assert "log-foundry: lost 1 event(s); Owner, still said" in err
