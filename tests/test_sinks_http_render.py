"""SPEC-058 FR-005 — ``HTTPSink`` renders per event, and counts what it dropped only on return.

``HTTPSink._items`` rendered and sized every event before any request, so one event whose
``_render`` or ``_item_size`` raised abandoned the batch. Each event now renders under a per-event
guard, and every drop ``_items`` decides — oversize and unrenderable — is counted and announced only
when ``emit`` returns normally: a raising ``emit`` is retried whole, and counting first counted one
oversized event four times (Probe C). A batch of nothing but unrenderable or oversized events
settles without raising, because re-rendering identical input in the sink's own code fails
identically.
"""

from __future__ import annotations

import importlib
import json
import pkgutil

import pytest

import log_foundry
import log_foundry.sinks
from log_foundry.sinks.base import SinkLosses
from log_foundry.sinks.elasticsearch import ElasticsearchSink, OpenSearchSink
from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.loki import LokiSink
from log_foundry.sinks.memory import MemorySink
from log_foundry.sinks.multi import MultiSink
from log_foundry.worker import Worker
from test_sinks_http import FakeOpener, FakeResponse

SENTINEL = "s3cr3t-sentinel"
STAMP = "2026-08-10T00:00:00.000Z"


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Neutralize the HTTP backoff so retry tests run instantly."""
    monkeypatch.setattr("log_foundry.sinks.http.wait", lambda _delay, _stop=None: None)


class _Opaque:
    """A value ``json.dumps`` cannot encode, so any sink's ``_render`` raises ``TypeError``."""


class RaisesOnMarked(HTTPSink):
    """An ``HTTPSink`` whose ``_render`` raises the configured error for an event marked ``bad``."""

    error: BaseException = ValueError("render failed")

    def _render(self, event: dict[str, object]) -> str:
        if event.get("bad") or event.get("message") == "bad":
            raise self.error
        return super()._render(event)


def _ev(i: int, **extra: object) -> dict[str, object]:
    return {"i": i, "timestamp": STAMP, "message": "m", **extra}


def _bodies(opener: FakeOpener) -> list[list[dict[str, object]]]:
    return [[json.loads(line) for line in call["body"].splitlines()] for call in opener.calls]


def _render_lines(err: str) -> list[str]:
    return [line for line in err.splitlines() if "rendering raised" in line]


def _ceiling_lines(err: str) -> list[str]:
    return [line for line in err.splitlines() if "per-event ceiling" in line]


def test_one_unrenderable_event_costs_only_itself(capsys) -> None:
    opener = FakeOpener()
    sink = HTTPSink("http://h", opener=opener)
    worker = Worker(sink, batch_size=1)
    try:
        worker.submit([_ev(0), _ev(1, raw=_Opaque()), _ev(2), _ev(3)])
        assert worker.flush(timeout=5.0)
        assert worker.health().failed_batches == 0
    finally:
        worker.stop()

    assert [[event["i"] for event in body] for body in _bodies(opener)] == [[0, 2, 3]]
    assert sink.losses() == SinkLosses(dropped=1, failed=0)
    assert _render_lines(capsys.readouterr().err) == [
        "log-foundry: lost 1 event(s); HTTPSink, rendering raised TypeError"
    ]


class _RaisingStr(str):
    """A ``str`` whose own ``__str__`` raises: ``json.dumps`` reads its data, ``str()`` does not."""

    def __str__(self) -> str:
        raise RuntimeError("no label for you")


def test_a_raising_item_size_costs_only_its_event(capsys) -> None:
    """``LokiSink._item_size`` reads labels through ``str()``, after ``_render`` succeeded."""
    opener = FakeOpener()
    sink = LokiSink("http://loki", labels=("service",), opener=opener)
    bad = _ev(1, service=_RaisingStr("svc"))
    sink._render(bad)
    with pytest.raises(RuntimeError):
        sink._item_size(sink._render(bad), bad)

    sink.emit([_ev(0, service="svc"), bad, _ev(2, service="svc")])

    sent = [
        json.loads(line)["i"]
        for call in opener.calls
        for stream in json.loads(call["body"])["streams"]
        for _ts, line in stream["values"]
    ]
    assert sent == [0, 2]
    assert sink.losses() == SinkLosses(dropped=1, failed=0)
    assert _render_lines(capsys.readouterr().err) == [
        "log-foundry: lost 1 event(s); LokiSink, rendering raised RuntimeError"
    ]


def test_an_all_unrenderable_batch_settles_without_a_request(capsys) -> None:
    opener = FakeOpener()
    sink = HTTPSink("http://h", opener=opener)
    worker = Worker(sink, batch_size=1, max_retries=0)
    try:
        worker.submit([_ev(i, raw=_Opaque()) for i in range(3)])
        assert worker.flush(timeout=5.0)
        assert worker.health().failed_batches == 0
    finally:
        worker.stop()

    assert opener.calls == []
    assert sink.losses() == SinkLosses(dropped=3, failed=0)
    assert _render_lines(capsys.readouterr().err) == [
        "log-foundry: lost 3 event(s); HTTPSink, rendering raised TypeError"
    ]


def test_a_mixed_batch_keeps_the_two_counts_apart() -> None:
    opener = FakeOpener()
    sink = HTTPSink("http://h", max_batch_bytes=2000, opener=opener)

    sink.emit([_ev(0), _ev(1, raw=_Opaque()), _ev(2, big="x" * 5000), _ev(3)])

    assert sink.dropped_oversized == 1, "oversize only"
    assert sink.losses() == SinkLosses(dropped=2, failed=0), "oversize plus unrenderable"
    assert [[event["i"] for event in body] for body in _bodies(opener)] == [[0, 3]]


_RECIPES = {
    "DatadogSink": lambda **kw: log_foundry.sinks.datadog.DatadogSink("key", **kw),
    "ElasticsearchSink": lambda **kw: ElasticsearchSink("http://es", index="i", **kw),
    "HoneycombSink": lambda **kw: log_foundry.sinks.honeycomb.HoneycombSink("key", "dataset", **kw),
    "LokiSink": lambda **kw: LokiSink("http://loki", **kw),
    "NewRelicSink": lambda **kw: log_foundry.sinks.newrelic.NewRelicSink("key", **kw),
    "OpenSearchSink": lambda **kw: OpenSearchSink("http://os", index="i", **kw),
    "SplunkHECSink": lambda **kw: log_foundry.sinks.splunk.SplunkHECSink(
        "http://splunk", "tok", **kw
    ),
}
"""How to build each shipped ``HTTPSink`` subclass. A class with no recipe fails the sweep.

Each recipe forwards keyword arguments, so a sweep can hand every subclass the same ``opener=``
and request shape (SPEC-059 FR-004); every subclass accepts them through ``HTTPForwardKwargs``.
"""


def _shipped_http_subclasses() -> list[type[HTTPSink]]:
    """Every ``HTTPSink`` subclass the package defines, found transitively, not hand-listed."""
    for module in pkgutil.iter_modules(log_foundry.sinks.__path__):
        importlib.import_module(f"log_foundry.sinks.{module.name}")
    found: list[type[HTTPSink]] = []
    stack = list(HTTPSink.__subclasses__())
    while stack:
        cls = stack.pop()
        stack.extend(cls.__subclasses__())
        if cls.__module__.startswith("log_foundry.") and cls not in found:
            found.append(cls)
    return sorted(found, key=lambda cls: cls.__name__)


def test_the_subclass_sweep_found_the_family() -> None:
    """A floor, so the parametrized sweep below cannot pass by finding nothing."""
    names = {cls.__name__ for cls in _shipped_http_subclasses()}
    assert {"ElasticsearchSink", "OpenSearchSink", "LokiSink"} <= names
    assert len(names) >= 7, names


@pytest.mark.parametrize("cls", _shipped_http_subclasses(), ids=lambda cls: cls.__name__)
def test_every_http_subclass_folds_unrenderable_into_losses(cls: type[HTTPSink]) -> None:
    assert cls.__name__ in _RECIPES, f"{cls.__name__} has no recipe; add one to _RECIPES"
    sink = _RECIPES[cls.__name__]()
    assert type(sink) is cls

    sink.emit([_ev(0, raw=_Opaque())])

    assert sink.losses().dropped == 1


def _oversized_span() -> None:
    @log_foundry.trace
    def work() -> None:
        log_foundry.info("hello", big="x" * 5000)

    work()


@pytest.mark.parametrize("opener", ["down", "down-once"])
def test_probe_c_rerun_the_oversized_event_is_counted_once_or_never(opener: str, capsys) -> None:
    """Probe C through the worker: abandoned, nothing of ``_items`` is counted; retried, once."""
    responses = (
        [FakeResponse(500, b"")]
        if opener == "down"
        else [FakeResponse(500, b""), FakeResponse(200, b"")]
    )
    sink = HTTPSink("http://h", max_batch_bytes=2000, max_retries=0, opener=FakeOpener(responses))
    log_foundry.configure(service="t", sink=sink)

    _oversized_span()
    result = log_foundry.flush()

    err = capsys.readouterr().err
    if opener == "down":
        assert not result.ok and log_foundry.health().failed_batches == 1
        assert sink.dropped_oversized == 0
        assert sink.losses().dropped == 0
        assert _ceiling_lines(err) == []
        assert "lost 3 event(s); batch abandoned" in err, "the abandon line is the only count"
    else:
        assert result.ok and log_foundry.health().failed_batches == 0
        assert sink.dropped_oversized == 1
        assert sink.losses().dropped == 1
        assert len(_ceiling_lines(err)) == 1


@pytest.mark.parametrize("opener", ["down", "down-once"])
def test_a_retried_unrenderable_event_is_counted_once_or_never(opener: str, capsys) -> None:
    responses = (
        [FakeResponse(500, b"")]
        if opener == "down"
        else [FakeResponse(500, b""), FakeResponse(200, b"")]
    )
    sink = HTTPSink("http://h", max_retries=0, opener=FakeOpener(responses))
    worker = Worker(sink, batch_size=1, max_retries=3)
    try:
        worker.submit([_ev(0), _ev(1, raw=_Opaque()), _ev(2)])
        delivered = worker.flush(timeout=5.0)
    finally:
        worker.stop()

    err = capsys.readouterr().err
    if opener == "down":
        assert not delivered
        assert sink.losses().dropped == 0
        assert _render_lines(err) == []
    else:
        assert delivered
        assert sink.losses().dropped == 1
        assert len(_render_lines(err)) == 1


def test_a_keyboard_interrupt_from_render_propagates_uncounted() -> None:
    sink = RaisesOnMarked("http://h", opener=FakeOpener())
    sink.error = KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        sink.emit([_ev(0, bad=True), _ev(1)])

    assert sink.losses() == SinkLosses(dropped=0, failed=0)


def test_no_sentinel_reaches_stderr(capsys) -> None:
    sink = RaisesOnMarked("http://h", opener=FakeOpener())
    sink.error = ValueError(f"bad {SENTINEL}")

    sink.emit([_ev(0, bad=True, secret=SENTINEL), _ev(1, secret=SENTINEL)])

    err = capsys.readouterr().err
    assert SENTINEL not in err
    assert _render_lines(err) == [
        "log-foundry: lost 1 event(s); RaisesOnMarked, rendering raised ValueError"
    ]


def test_an_orphan_unrenderable_event_settles_inside_the_sink() -> None:
    """Outside a span the batch is one event; an unrenderable one settles as an oversized one does."""
    sink = RaisesOnMarked("http://h", opener=FakeOpener())
    log_foundry.configure(service="t", sink=sink)

    log_foundry.info("bad")

    assert sink.losses() == SinkLosses(dropped=1, failed=0)
    assert log_foundry.health().orphan_lost == 0


def test_model_a_the_oversized_event_is_absorbed_once() -> None:
    """FR-005 and FR-007's model (a): the oversized event lands in the fan-out's absorbed term.

    The HTTP child's request fails, so its ``emit`` raises and counts nothing from ``_items``;
    the fan-out swallows the raise because its sibling delivered, and absorbs the batch's three
    events. Under FR-005 alone, with SPEC-036's silent-only rule, the event was in no term.
    """
    http = HTTPSink(
        "http://h", max_batch_bytes=2000, max_retries=0, opener=FakeOpener([FakeResponse(500, b"")])
    )
    log_foundry.configure(service="t", sink=MultiSink(http, MemorySink()))

    _oversized_span()
    assert log_foundry.flush().ok

    health = log_foundry.health()
    assert http.failed == 1, "the premise: the child counted its own abandoned request"
    assert health.sink == SinkLosses(dropped=0, failed=http.failed + 3)
