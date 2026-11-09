"""The HTTP round's hook guard and its announcement of an unplaced event (SPEC-073 FR-001).

A raising hook costs only its own chunk: ``DROPPED`` on ``_unrenderable`` when the chunk's request
was not yet sent, ``UNKNOWN`` through ``_unadjudicated`` once it was, every other chunk keeping
its outcome. The hooks are derived from ``HTTPSink._round``'s own calls, so a hook added later
fails :func:`test_every_hook_the_round_reaches_has_a_fault_scenario` by name until it has one.
No shipped class raises from these hooks, so every faulting class is built here.

An event an ``_items`` override leaves out without counting is ``REJECTED`` and announced by the
round itself, once, so it is never left to the engine's settled line, which a sink line beside it
suppresses (invariant 2).
"""

from __future__ import annotations

import ast
import inspect
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any

import pytest

from log_foundry.sinks import http
from log_foundry.sinks.base import SinkDeliveryError
from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.verdict import Outcome, Verdict
from test_http_rounds import Scripted, _ok, _status, _through_the_worker
from test_sinks_http import FakeResponse

A, J, D, U = Outcome.ACCEPTED, Outcome.REJECTED, Outcome.DROPPED, Outcome.UNKNOWN


class _Planted(Exception):
    """The fault every scenario plants, so a line naming it names the planted hook's raise."""


def _lost(err: str) -> list[str]:
    return [line for line in err.splitlines() if "log-foundry: lost" in line]


def _built(name: str, lost: int) -> str:
    return f"log-foundry: lost {lost} event(s); Faulting, rendering raised {name}"


def _handled(lost: int = 1) -> str:
    return (
        f"log-foundry: lost {lost} event(s); Faulting handling its response raised _Planted; "
        "they may have landed; not retried"
    )


def _read_raised() -> str:
    return (
        "log-foundry: lost 1 event(s); Faulting reading a 2xx response raised _Planted; "
        "they may have landed; not retried"
    )


def _no_outcome() -> str:
    return (
        "log-foundry: lost 1 event(s); Faulting reading a 2xx response gave no outcome per "
        "event; they may have landed; not retried"
    )


def _request_lost(reason: str) -> str:
    return f"log-foundry: lost 1 request(s); Faulting, 1 attempt(s), {reason}"


# --- the population: every method of HTTPSink the round reaches ---------------------------------


def reached_hooks(source: str) -> set[str]:
    """Every ``HTTPSink`` method ``_round`` reaches through ``self``, followed transitively.

    A ``self.<name>`` reference counts whether it is called or passed (``_retryable_error`` is
    handed to ``is_retryable``), and only names the class defines as methods, so an attribute
    such as ``self.url`` is not a hook. An explicit ``HTTPSink.<name>(self, ...)`` call runs the
    base and is not one either.

    Args:
      source: ``http.py``'s text.

    Returns:
      The method names.
    """
    tree = ast.parse(source)
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "HTTPSink")
    methods = {n.name: n for n in cls.body if isinstance(n, ast.FunctionDef)}
    seen: set[str] = set()
    todo = ["_round"]
    while todo:
        for node in ast.walk(methods[todo.pop()]):
            if (
                isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Name)
                and node.value.id == "self"
                and node.attr in methods
                and node.attr not in seen
            ):
                seen.add(node.attr)
                todo.append(node.attr)
    return seen


def test_the_roster_follows_a_planted_hook_through_a_helper() -> None:
    """A hook reached only through another method is found, and an attribute is not."""
    source = (
        "class HTTPSink:\n"
        "    def _round(self, batch):\n"
        "        self._helper()\n"
        "        return self.url\n"
        "    def _helper(self):\n"
        "        return is_retryable(self._planted, None)\n"
        "    def _planted(self, err):\n"
        "        return HTTPSink._unseen(self)\n"
        "    def _unseen(self):\n"
        "        return 0\n"
    )
    assert reached_hooks(source) == {"_helper", "_planted"}


# --- the scenarios: one fault per hook, and what it must cost -----------------------------------


@dataclass(frozen=True)
class Scenario:
    """One planted fault: which hook raises on which call, and what the round must report.

    Attributes:
      hook: The method that raises.
      nth: Which of its calls raises; every other call runs the base.
      outcomes: The verdict's outcomes.
      lines: Every ``lost`` line, in order.
      script: The opener's answers.
      requests: How many requests are made.
      events: The batch.
      count: ``max_batch_count``.
      extra: Further overrides the class needs to reach the hook.
    """

    hook: str
    nth: int
    outcomes: tuple[Outcome, ...]
    lines: tuple[str, ...]
    script: tuple[Any, ...] = (_ok(),)
    requests: int = 3
    events: tuple[dict[str, object], ...] = ({"i": 0}, {"i": 1}, {"i": 2})
    count: int = 1
    extra: dict[str, Any] = field(default_factory=dict)


def _garbage(self: Any, payload: bytes, items: list[Any]) -> object:
    return "not an outcome per event"


def _free(self: Any, rendered: str, event: dict[str, object]) -> int:
    return 0


def _by_size(index: int, request: urllib.request.Request) -> Any:
    return _status(413) if len(request.data or b"") > 20 else _ok()


def _after_one(index: int, request: urllib.request.Request) -> Any:
    return _ok() if index == 0 else _status(413)


_REFUSED = (
    "log-foundry: lost 1 event(s); Faulting, refused at the smallest request this sink can build"
)
_SMALL, _MEDIUM, _LARGE = {"i": 0}, {"i": 1, "pad": "x" * 30}, {"i": 2, "pad": "x" * 60}

SCENARIOS: dict[str, Scenario] = {
    # Before any chunk: the round has nothing to send.
    "_chunk_budget": Scenario("_chunk_budget", 1, (D, D, D), (_built("_Planted", 3),), requests=0),
    "_body_reserve": Scenario("_body_reserve", 1, (D, D, D), (_built("_Planted", 3),), requests=0),
    "_items": Scenario("_items", 1, (D, D, D), (_built("_Planted", 3),), requests=0),
    "_event_ceiling": Scenario(
        "_event_ceiling", 1, (D, D, D), (_built("_Planted", 3),), requests=0
    ),
    # Inside ``_items``' own per-event guard (SPEC-058 FR-005): the event alone.
    "_render": Scenario("_render", 2, (A, D, A), (_built("_Planted", 1),), requests=2),
    "_item_size": Scenario("_item_size", 2, (A, D, A), (_built("_Planted", 1),), requests=2),
    # Before the chunk's request is sent: DROPPED, on _unrenderable.
    "_take": Scenario("_take", 2, (A, D, A), (_built("_Planted", 1),), requests=2),
    "_known_refused": Scenario(
        "_known_refused", 2, (A, D, A), (_built("_Planted", 1),), requests=2
    ),
    "_compresses": Scenario("_compresses", 3, (A, D, A), (_built("_Planted", 1),), requests=2),
    "_body": Scenario("_body", 2, (A, D, A), (_built("_Planted", 1),), requests=2),
    "_prepare": Scenario("_prepare", 2, (A, D, A), (_built("_Planted", 1),), requests=2),
    "_apply_auth": Scenario("_apply_auth", 2, (A, D, A), (_built("_Planted", 1),), requests=2),
    "_drop_refused:known": Scenario(
        "_drop_refused",
        2,
        (A, D, D),
        (
            _REFUSED,
            _built("_Planted", 1),
        ),
        script=(_by_size,),
        requests=2,
        events=(_SMALL, _MEDIUM, _LARGE),
    ),
    # After the send: UNKNOWN, through the base _unadjudicated. The harness raises in place of
    # calling the base, so an ``_exchange`` fault sends nothing, and still reads as sent: the
    # guard cannot tell, and UNKNOWN is the outcome that claims nothing.
    "_exchange": Scenario("_exchange", 2, (A, U, A), (_handled(),), requests=2),
    "_retryable_status": Scenario(
        "_retryable_status", 1, (A, U, A), (_handled(),), script=(_ok(), _status(503), _ok())
    ),
    "_give_up": Scenario(
        "_give_up", 1, (A, U, A), (_handled(),), script=(_ok(), _status(400), _ok())
    ),
    "_read": Scenario("_read", 2, (A, U, A), (_handled(),)),
    "_outcomes": Scenario("_outcomes", 2, (A, U, A), (_read_raised(),)),
    "_handle_response": Scenario("_handle_response", 2, (A, U, A), (_read_raised(),)),
    "_unadjudicated": Scenario(
        "_unadjudicated",
        1,
        (U, U, U),
        (_handled(), _no_outcome(), _no_outcome()),
        extra={"_outcomes": _garbage},
    ),
    "_drop_refused:413": Scenario(
        "_drop_refused",
        1,
        (A, U, A),
        (_handled(),),
        script=(_ok(), _status(413), _ok()),
        events=({"i": 0}, {"i": 1, "pad": "x"}, {"i": 2}),
    ),
    "_abandon_unreduced": Scenario(
        "_abandon_unreduced",
        1,
        (A, A, U, U),
        (_handled(2),),
        script=(_after_one,),
        requests=1 + http.MAX_BUDGET_REDUCTIONS + 1,
        events=({"i": 0}, {"i": 1}, {"i": 2}, {"i": 3}),
        count=2,
        extra={"_item_size": _free},
    ),
    # Inside the attempt _exchange classifies: its own table, REJECTED and counted in failed.
    # ``_attempt`` reads ``_response_limit`` after the opener answers, so that request was made:
    # SPEC-073's recorded edge, kept.
    "_attempt": Scenario(
        "_attempt", 2, (A, J, A), (_request_lost("connection error, _Planted"),), requests=2
    ),
    "_response_limit": Scenario(
        "_response_limit",
        2,
        (A, J, A),
        (_request_lost("connection error, _Planted"),),
    ),
    "_retryable_error": Scenario(
        "_retryable_error",
        1,
        (A, J, A),
        (_request_lost("connection error, URLError"),),
        script=(_ok(), urllib.error.URLError("refused"), _ok()),
    ),
}


def _faulting(scenario: Scenario, opener: Scripted) -> HTTPSink:
    """A sink whose ``scenario.hook`` raises ``_Planted`` on its ``nth`` call, else runs the base."""
    calls = [0]

    def planted(self: Any, *args: Any, **kwargs: Any) -> Any:
        calls[0] += 1
        if calls[0] == scenario.nth:
            raise _Planted
        return getattr(super(cls, self), scenario.hook)(*args, **kwargs)

    cls = type("Faulting", (HTTPSink,), {scenario.hook: planted, **scenario.extra})
    return cls("http://h", max_batch_count=scenario.count, max_retries=0, opener=opener)


def test_every_hook_the_round_reaches_has_a_fault_scenario() -> None:
    reached = reached_hooks(inspect.getsource(http))
    planted = {scenario.hook for scenario in SCENARIOS.values()}
    assert sorted(reached - planted) == [], "a hook the round reaches has no fault scenario"
    assert sorted(planted - reached) == [], "a scenario plants a hook the round never reaches"


@pytest.mark.parametrize("name", sorted(SCENARIOS))
def test_a_raising_hook_costs_only_its_chunk(name: str, capsys: pytest.CaptureFixture[str]) -> None:
    scenario = SCENARIOS[name]
    opener = Scripted(*scenario.script)
    sink = _faulting(scenario, opener)
    verdict = HTTPSink.deliver(sink, list(scenario.events))
    assert verdict == Verdict(outcomes=scenario.outcomes)
    assert len(opener.calls) == scenario.requests
    assert _lost(capsys.readouterr().err) == list(scenario.lines)


@pytest.mark.parametrize("name", sorted(SCENARIOS))
def test_emit_and_the_worker_settle_a_raising_hook(
    name: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """The twin paths: ``emit`` returns, and the worker delivers, with the same lines."""
    scenario = SCENARIOS[name]
    _faulting(scenario, Scripted(*scenario.script)).emit(list(scenario.events))
    assert _lost(capsys.readouterr().err) == list(scenario.lines)
    sink = _faulting(scenario, Scripted(*scenario.script))
    delivered, failed_batches = _through_the_worker(sink, list(scenario.events), max_retries=0)
    assert delivered and failed_batches == 0
    assert _lost(capsys.readouterr().err) == list(scenario.lines)


# --- what a faulting hook registered before it raised is discarded ------------------------------


def test_a_drop_refused_that_counts_then_raises_is_counted_once(
    capsys: pytest.CaptureFixture[str],
) -> None:
    class Faulting(HTTPSink):
        def _drop_refused(self, chunk: list[Any], pending: Any) -> None:
            super()._drop_refused(chunk, pending)
            raise _Planted

    sink = Faulting("http://h", max_batch_count=1, opener=Scripted(_ok(), _status(413), _ok()))
    batch: list[dict[str, object]] = [{"i": 0}, {"i": 1, "pad": "x"}, {"i": 2}]
    assert sink.deliver(batch) == Verdict(outcomes=(A, U, A))
    assert sink.dropped_oversized == 0, "the drop the hook registered is discarded"
    assert sink.dropped_unadjudicated == 1
    assert _lost(capsys.readouterr().err) == [_handled()]


def test_an_items_that_counts_then_raises_is_counted_once(
    capsys: pytest.CaptureFixture[str],
) -> None:
    class Faulting(HTTPSink):
        def _items(self, batch: list[dict[str, object]], pending: Any) -> list[Any]:
            super()._items(batch, pending)
            raise _Planted

    opener = Scripted(_ok())
    sink = Faulting("http://h", max_batch_bytes=200, opener=opener)
    batch: list[dict[str, object]] = [{"i": 0, "big": "x" * 500}, {"i": 1}]
    assert sink.deliver(batch) == Verdict(outcomes=(D, D))
    assert opener.calls == []
    assert sink.dropped_oversized == 0, "the oversized drop the hook registered is discarded"
    assert sink.losses().dropped == 2
    assert _lost(capsys.readouterr().err) == [_built("_Planted", 2)]


def test_an_abandon_unreduced_that_counts_then_raises_is_counted_once(
    capsys: pytest.CaptureFixture[str],
) -> None:
    class Faulting(HTTPSink):
        def _item_size(self, rendered: str, event: dict[str, object]) -> int:
            return 0

        def _abandon_unreduced(self, chunk: list[Any]) -> None:
            super()._abandon_unreduced(chunk)
            raise _Planted

    sink = Faulting("http://h", max_batch_count=2, opener=Scripted(_after_one))
    assert sink.deliver([{"i": n} for n in range(4)]) == Verdict(outcomes=(A, A, U, U))
    assert sink.failed == 0, "the abandoned request the hook registered is discarded"
    assert _lost(capsys.readouterr().err) == [_handled(2)]


@pytest.mark.parametrize("returned", ["empty", "too long"])
def test_an_unusable_take_costs_one_item_and_ends(
    returned: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """An empty ``_take`` would loop forever; a long one would misplace its chunk's events."""

    class Faulting(HTTPSink):
        def _take(self, items: list[Any], start: int, budget: int) -> list[Any]:
            if start == 1:
                return [] if returned == "empty" else items + items
            return super()._take(items, start, budget)

    opener = Scripted(_ok())
    sink = Faulting("http://h", max_batch_count=1, opener=opener)
    assert sink.deliver([{"i": 0}, {"i": 1}, {"i": 2}]) == Verdict(outcomes=(A, D, A))
    assert len(opener.calls) == 2
    assert _lost(capsys.readouterr().err) == [_built("ValueError", 1)]


def test_a_base_exception_from_a_hook_still_propagates() -> None:
    class Interrupting(HTTPSink):
        def _retryable_status(self, status: int) -> bool:
            raise KeyboardInterrupt

    sink = Interrupting("http://h", opener=Scripted(_status(503)))
    with pytest.raises(KeyboardInterrupt):
        sink.deliver([{"i": 0}])
    assert sink.failed == 0 and sink.dropped_unadjudicated == 0


# --- an event no request carries is announced by the round, once -------------------------------


class _Omitting(HTTPSink):
    """An ``_items`` override that leaves out its batch's second item without counting it."""

    def _items(self, batch: list[dict[str, object]], pending: Any) -> list[Any]:
        items = super()._items(batch, pending)
        return items[:1] + items[2:]


_UNPLACED = "log-foundry: lost 1 event(s); _Omitting, _items returned no item for them; not retried"


def test_an_unplaced_event_beside_a_sink_line_is_announced(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The oversized drop's line suppresses the engine's; the omitted event has its own."""
    batch: list[dict[str, object]] = [{"i": 0, "big": "x" * 500}, {"i": 1}, {"i": 2}, {"i": 3}]
    lines = [
        (
            "log-foundry: lost 1 event(s); _Omitting, up to 520 bytes exceeds the 200-byte "
            "per-event ceiling"
        ),
        _UNPLACED,
    ]
    sink = _Omitting("http://h", max_batch_bytes=200, opener=Scripted(_ok()))
    assert sink.deliver(batch) == Verdict(outcomes=(J, A, J, A))
    assert _lost(capsys.readouterr().err) == lines
    sink = _Omitting("http://h", max_batch_bytes=200, opener=Scripted(_ok()))
    sink.emit(batch)
    assert _lost(capsys.readouterr().err) == lines
    assert sink.log_foundry_counters.failed_events == 2, "both unplaced events are REJECTED"
    assert sink.dropped_oversized == 1, "the counted drop stays a drop"


def test_an_unplaced_event_alone_is_announced_once(capsys: pytest.CaptureFixture[str]) -> None:
    """Its own line replaces the engine's settled line, never sits beside it."""
    sink = _Omitting("http://h", opener=Scripted(_ok()))
    assert sink.deliver([{"i": 0}, {"i": 1}, {"i": 2}]) == Verdict(outcomes=(A, J, A))
    assert _lost(capsys.readouterr().err) == [_UNPLACED]


def test_unplaced_events_of_a_failed_run_are_left_to_its_catcher(
    capsys: pytest.CaptureFixture[str],
) -> None:
    class Empty(HTTPSink):
        def _items(self, batch: list[dict[str, object]], pending: Any) -> list[Any]:
            return []

    opener = Scripted(_ok())
    with pytest.raises(SinkDeliveryError):
        Empty("http://h", opener=opener).emit([{"i": 0}, {"i": 1}])
    assert opener.calls == []
    assert _lost(capsys.readouterr().err) == [], "the raise is the run's one announcement"


# --- a loss whose hook registered no line is announced by the round ------------------------------


class _Refusing(_Omitting):
    """An ``_Omitting`` whose 1.x bool reader refuses the chunk carrying event 2."""

    def _handle_response(self, payload: bytes, items: list[Any]) -> bool:
        return items[0].event.get("i") != 2


def test_a_silent_reader_beside_an_unplaced_event_is_still_announced(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Frame-1 review: the unplaced line no longer silences the only line a bool reader had."""
    sink = _Refusing("http://h", max_batch_count=1, opener=Scripted(_ok()))
    sink.emit([{"i": 0}, {"i": 1}, {"i": 2}])
    assert _lost(capsys.readouterr().err) == [
        _UNPLACED.replace("_Omitting", "_Refusing"),
        "log-foundry: lost 1 event(s); _Refusing, refused for good; not retried",
    ]
    assert sink.log_foundry_counters.failed_events == 2


@pytest.mark.parametrize("outcome", ["rejected", "dropped", "unknown"])
def test_a_reader_returning_a_loss_with_no_line_is_announced_once(
    outcome: str, capsys: pytest.CaptureFixture[str]
) -> None:
    lost = {"rejected": J, "dropped": D, "unknown": U}[outcome]

    class Silent(HTTPSink):
        def _outcomes(self, payload: bytes, items: list[Any]) -> tuple[Outcome, ...]:
            return tuple(lost if item.event.get("i") == 1 else A for item in items)

    sink = Silent("http://h", max_batch_bytes=200, max_batch_count=1, opener=Scripted(_ok()))
    batch: list[dict[str, object]] = [{"i": 0, "big": "x" * 500}, {"i": 1}, {"i": 2}]
    assert sink.deliver(batch) == Verdict(outcomes=(D, lost, A))
    line = (
        "log-foundry: lost 1 event(s); Silent, outcome unknown; they may have landed"
        if outcome == "unknown"
        else "log-foundry: lost 1 event(s); Silent, refused for good; not retried"
    )
    assert _lost(capsys.readouterr().err) == [
        (
            "log-foundry: lost 1 event(s); Silent, up to 520 bytes exceeds the 200-byte "
            "per-event ceiling"
        ),
        line,
    ]


def test_an_abandon_unreduced_with_no_line_is_announced_by_the_round(
    capsys: pytest.CaptureFixture[str],
) -> None:
    class Silent(HTTPSink):
        def _item_size(self, rendered: str, event: dict[str, object]) -> int:
            return 0

        def _abandon_unreduced(self, chunk: list[Any]) -> None:
            return None

    sink = Silent("http://h", max_batch_count=2, opener=Scripted(_after_one))
    assert sink.deliver([{"i": n} for n in range(4)]) == Verdict(outcomes=(A, A, J, J))
    assert _lost(capsys.readouterr().err) == [
        "log-foundry: lost 2 event(s); Silent, refused for good; not retried"
    ]


def test_an_idempotent_unknown_with_no_line_is_announced_only_by_the_last_round(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """An idempotent sink's ``UNKNOWN`` is resent; one a later round lands is never announced."""
    answers = iter([U, A])

    class Silent(HTTPSink):
        log_foundry_idempotent = True

        def _outcomes(self, payload: bytes, items: list[Any]) -> tuple[Outcome, ...]:
            return (next(answers),) * len(items)

    opener = Scripted(_ok())
    Silent("http://h", max_retries=1, opener=opener).emit([{"i": 0}])
    assert len(opener.calls) == 2
    assert _lost(capsys.readouterr().err) == []


def test_a_reader_that_counts_then_raises_is_counted_once(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Frame-1 review: ``_read`` discards what a faulting reader registered before it raised."""
    from log_foundry.sinks.elasticsearch import ElasticsearchSink

    class Faulting(ElasticsearchSink):
        def _outcomes(self, payload: bytes, items: list[Any]) -> tuple[Outcome, ...]:
            super()._outcomes(payload, items)
            raise _Planted

    body = b'{"errors": true, "items": [{"index": {"status": 400, "error": {"type": "x"}}}]}'
    sink = Faulting("http://h", index="i", opener=Scripted(FakeResponse(200, body)))
    assert sink.deliver([{"i": 0}]) == Verdict(outcomes=(U,))
    assert sink.item_errors == 0, "the item error the reader registered is discarded"
    assert sink.dropped_unadjudicated == 1
    assert _lost(capsys.readouterr().err) == [_read_raised()]
