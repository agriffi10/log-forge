"""SPEC-059 FR-004 — ``HTTPSink._drop_refused`` counts through the calling emit's Deferral.

A lone item the destination answers ``413``, and a lone item ``_known_refused`` discards without
offering it, were counted at once. Once a drop has settled a chunk, ``emit`` raises only through a
later chunk's exception ``_deliver`` does not catch — Probe D's ``http.client.IncompleteRead``, an
``HTTPException`` and not an ``OSError`` — and the worker's retry then counted the drop once per
attempt. ``_drop_refused`` now takes ``emit``'s ledger, so that raise discards it.

Probe D: ``max_batch_count=1, max_retries=0``, an opener answering ``413`` to the 2,000-byte
event's lone request (sent first) and, for the ordinary event's request, ``200`` with a ``read()``
that raises ``IncompleteRead``. Down repeats that on every attempt; once serves the second.
"""

from __future__ import annotations

import http.client
from typing import Any

import pytest

import log_foundry
from log_foundry.sinks.base import SinkLosses
from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.memory import MemorySink
from log_foundry.sinks.multi import MultiSink
from log_foundry.worker import Worker
from test_sinks_http import FakeOpener, FakeResponse
from test_sinks_http_render import _RECIPES, _shipped_http_subclasses

_STAMP = "2026-08-10T00:00:00.000Z"
_MARKER = "refused at the smallest request this sink can build"


class _IncompleteResponse(FakeResponse):
    """A ``200`` whose body read fails part-way, as a dropped connection does."""

    def read(self) -> bytes:
        raise http.client.IncompleteRead(b"")


def _attempt() -> list[FakeResponse]:
    """One Probe D attempt: ``413`` for the oversized event, then a read that raises."""
    return [FakeResponse(413, b""), _IncompleteResponse(200, b"")]


def _down() -> list[FakeResponse]:
    return _attempt() * 4


def _once() -> list[FakeResponse]:
    return [*_attempt(), FakeResponse(413, b""), FakeResponse(200, b"")]


def _event(**fields: object) -> dict[str, object]:
    """An event every platform subclass can render: it carries a message and a timestamp."""
    return {"message": "m", "timestamp": _STAMP, **fields}


def _probe_d() -> list[dict[str, object]]:
    return [_event(big="x" * 2_000), _event(i=1)]


def _http(opener: FakeOpener) -> HTTPSink:
    return HTTPSink("http://h", max_batch_count=1, max_retries=0, opener=opener)


def _drop_lines(err: str) -> list[str]:
    return [line for line in err.splitlines() if _MARKER in line]


def _through_the_worker(sink: Any, batch: list[dict[str, object]]) -> tuple[bool, int]:
    """Hands ``batch`` to a real worker as one batch; whether it was delivered, and abandonments."""
    worker = Worker(sink, batch_size=len(batch), max_retries=3)
    try:
        worker.submit(batch)
        delivered = bool(worker.flush(timeout=30.0))
        failed_batches = worker.health().failed_batches
    finally:
        worker.stop()
    return delivered, failed_batches


def test_probe_d_down_counts_no_413_drop(capsys) -> None:
    """Four attempts each refused the event and then raised; none returned, so none counts."""
    opener = FakeOpener(_down())
    sink = _http(opener)

    delivered, failed_batches = _through_the_worker(sink, _probe_d())

    err = capsys.readouterr().err
    assert not delivered and failed_batches == 1
    assert len(opener.calls) == 8, "the premise: the 413 and the failed read on every attempt"
    assert sink.dropped_oversized == 0
    assert sink.losses().dropped == 0
    assert _drop_lines(err) == [], "no 413 line from an emit that raised"
    assert "lost 2 event(s); batch abandoned after 4 emit attempts" in err, (
        "the abandon line is the only count"
    )


def test_probe_d_once_counts_the_413_drop_once(capsys) -> None:
    """Attempt 1 raised and counted nothing; attempt 2 returned and counted the drop once."""
    opener = FakeOpener(_once())
    sink = _http(opener)

    delivered, failed_batches = _through_the_worker(sink, _probe_d())

    err = capsys.readouterr().err
    assert delivered and failed_batches == 0
    assert len(opener.calls) == 4
    assert sink.dropped_oversized == 1
    assert sink.losses().dropped == 1
    assert _drop_lines(err) == [f"log-foundry: lost 1 event(s); HTTPSink, {_MARKER}"]


@pytest.mark.parametrize("mode", ["down", "once"])
def test_the_never_offered_route_defers_too(mode: str, capsys) -> None:
    """Two equal oversized events: the first answered ``413``, the second never offered.

    ``_known_refused`` discards the second because its body is no smaller than one this ``emit``
    already had refused, so each attempt makes two requests for three events. Both routes into
    ``_drop_refused`` are held to the same ledger.
    """
    opener = FakeOpener(_down() if mode == "down" else _once())
    sink = _http(opener)
    batch = [_event(big="x" * 2_000, n=0), _event(big="x" * 2_000, n=1), _event(i=1)]

    delivered, _ = _through_the_worker(sink, batch)

    lines = _drop_lines(capsys.readouterr().err)
    attempts = 4 if mode == "down" else 2
    assert len(opener.calls) == 2 * attempts, "the premise: the second event was never offered"
    if mode == "down":
        assert not delivered
        assert sink.dropped_oversized == 0
        assert lines == []
    else:
        assert delivered
        assert sink.dropped_oversized == 2, "each of the two events, once"
        assert lines == [f"log-foundry: lost 1 event(s); HTTPSink, {_MARKER}"] * 2


def test_an_all_413_batch_settles_and_counts_all(capsys) -> None:
    """Unchanged: every chunk settled by a drop, so the call returns and commits all N."""
    opener = FakeOpener([FakeResponse(413, b"")])
    sink = _http(opener)

    sink.emit([_event(i=i) for i in range(3)])

    assert len(opener.calls) == 1, "the two no smaller than the refused one are never offered"
    assert sink.dropped_oversized == 3
    assert sink.losses() == SinkLosses(dropped=3, failed=0)
    assert len(_drop_lines(capsys.readouterr().err)) == 3


@pytest.mark.parametrize("cls", _shipped_http_subclasses(), ids=lambda cls: cls.__name__)
def test_every_http_subclass_counts_no_413_drop_when_down(cls: type[HTTPSink]) -> None:
    """Probe D down over every shipped subclass, derived transitively, ES and OpenSearch among them.

    ``ElasticsearchSink`` and ``OpenSearchSink`` fold ``dropped_oversized`` into their own
    ``losses()``, so the assertion is on ``losses().dropped`` as each subclass reports it.
    """
    assert cls.__name__ in _RECIPES, f"{cls.__name__} has no recipe; add one to _RECIPES"
    opener = FakeOpener(_down())
    sink = _RECIPES[cls.__name__](opener=opener, max_batch_count=1, max_retries=0)
    assert type(sink) is cls

    delivered, failed_batches = _through_the_worker(sink, _probe_d())

    assert not delivered and failed_batches == 1
    assert len(opener.calls) == 8, "the premise: the 413 and the failed read on every attempt"
    assert sink.losses().dropped == 0


def test_an_orphan_413_settles_inside_the_sink() -> None:
    """Unchanged: outside a span the refused event is the batch; it settles and counts once."""
    opener = FakeOpener([FakeResponse(413, b"")])
    sink = HTTPSink("http://h", opener=opener)
    log_foundry.configure(service="t", sink=sink)

    log_foundry.info("outside a span")

    assert len(opener.calls) == 1
    assert sink.dropped_oversized == 1
    assert sink.losses() == SinkLosses(dropped=1, failed=0)
    assert log_foundry.health().orphan_lost == 0


def test_a_fanout_absorbs_the_probe_d_batch() -> None:
    """The child raised after its drop, its sibling delivered: the fan-out owns both events."""
    child = _http(FakeOpener(_attempt()))
    multi = MultiSink(child, MemorySink())

    multi.emit(_probe_d())

    own = child.losses()
    assert own.dropped == 0, "the child's raise discarded its 413 drop"
    assert multi.losses() == SinkLosses(dropped=0, failed=own.failed + 2)
