"""SPEC-059 FR-004 — ``HTTPSink._drop_refused`` counts through the run's ``SETTLED`` ledger.

A lone item the destination answers ``413``, and a lone item ``_known_refused`` discards without
offering it, were counted at once, and a later raise had the worker retry the batch and count the
drop once per attempt. ``_drop_refused`` takes a ledger that commits only when ``emit`` returns —
on the delivery engine (SPEC-073), only when the run settles — so a run that fails counts none.

Probe D: ``max_batch_count=1, max_retries=0``, an opener answering ``413`` to the 2,000-byte
event's lone request (sent first) and a second answer for the ordinary event's. The "down" tests
pin invariant 2's rule that a failed run counts no drop, so their second answer is a ``400``,
``REJECTED`` for every family class: the run fails, and the worker sends it once, since no round
resends an outcome rejected for good. The "once" tests pin that a resend never recounts a drop,
so their second answer is a ``URLError``, ``RETRYABLE``, then a ``200`` in round two.

Probe D used to be a ``200`` whose ``read()`` raised ``IncompleteRead``, then an opener raising
``RuntimeError``, then an instance-level ``_handle_response`` that raised on the second chunk's
``200``. None keeps its meaning on the engine: a failed body read after a status line settles the
``2xx``, an opener's ``RuntimeError`` beside the drop fails the run (pinned below as SPEC-073's
move 2), and a raising reader makes its chunk ``UNKNOWN``, which settles the run.
"""

from __future__ import annotations

import urllib.error
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


def _down() -> list[FakeResponse | Exception]:
    """Probe D down: ``413`` for the oversized event, then ``400`` for the other, refused for good."""
    return [FakeResponse(413, b""), FakeResponse(400, b"")]


def _once() -> list[FakeResponse | Exception]:
    """Probe D once: ``413``, then a ``URLError`` the engine resends, then ``200`` in round two."""
    return [
        FakeResponse(413, b""),
        urllib.error.URLError("connection refused"),
        FakeResponse(200, b""),
    ]


def _event(**fields: object) -> dict[str, object]:
    """An event every platform subclass can render: it carries a message and a timestamp.

    The size-bearing keys sit under ``fields``, the shape assembly produces, rather than at the
    top level: a mapping that carries only the keys it knows — ``OTLPSink``'s — would otherwise
    render Probe D's two events to the same length, and ``_known_refused`` would discard the
    second without offering it (SPEC-056 FR-005).
    """
    return {"message": "m", "timestamp": _STAMP, "fields": dict(fields)}


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
    """One round refused the event and then the batch for good; the run failed, so no drop counts.

    2 calls where ``fda134a`` and ``c4e51fbd`` made 8: the worker resent the raising batch whole
    four times, and on the engine a round resends nothing rejected for good.
    """
    opener = FakeOpener(_down())
    sink = _http(opener)

    delivered, failed_batches = _through_the_worker(sink, _probe_d())

    err = capsys.readouterr().err
    assert not delivered and failed_batches == 1
    assert len(opener.calls) == 2, "one round: the 413 and the 400"
    assert sink.dropped_oversized == 0
    assert sink.losses() == SinkLosses(dropped=0, failed=1)
    assert _drop_lines(err) == [], "no 413 line from a run that failed"
    assert "lost 1 request(s); HTTPSink, 1 attempt(s), HTTP 400" in err
    assert "lost 2 event(s); batch abandoned after 1 emit attempts" in err, (
        "the worker's abandoned line counts the batch"
    )


def test_probe_d_once_counts_the_413_drop_once(capsys) -> None:
    """Round one dropped the event and found the other retryable; round two landed it, once."""
    opener = FakeOpener(_once())
    sink = _http(opener)

    delivered, failed_batches = _through_the_worker(sink, _probe_d())

    err = capsys.readouterr().err
    assert delivered and failed_batches == 0
    assert len(opener.calls) == 3, "the 413, the URLError, and round two's single request"
    assert b"big" not in opener.calls[2]["body"], "round two resends only the retryable event"
    assert sink.failed == 0, "the retryable request landed in the run's last round"
    assert sink.dropped_oversized == 1
    assert sink.losses() == SinkLosses(dropped=1, failed=0)
    assert _drop_lines(err) == [f"log-foundry: lost 1 event(s); HTTPSink, {_MARKER}"]


@pytest.mark.parametrize("mode", ["down", "once"])
def test_the_never_offered_route_defers_too(mode: str, capsys) -> None:
    """Two equal oversized events: the first answered ``413``, the second never offered.

    ``_known_refused`` discards the second because its body is no smaller than one this run
    already had refused, so round one makes two requests for three events. Both routes into
    ``_drop_refused`` are held to the same ledger.
    """
    opener = FakeOpener(_down() if mode == "down" else _once())
    sink = _http(opener)
    batch = [_event(big="x" * 2_000, n=0), _event(big="x" * 2_000, n=1), _event(i=1)]

    delivered, _ = _through_the_worker(sink, batch)

    lines = _drop_lines(capsys.readouterr().err)
    if mode == "down":
        assert len(opener.calls) == 2, "the premise: the second event was never offered"
        assert not delivered
        assert sink.dropped_oversized == 0
        assert lines == []
    else:
        assert len(opener.calls) == 3, "round one's two requests and round two's one"
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
    assert len(opener.calls) == 2, "one round: the 413 and the 400"
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
    """The child's run failed after its drop, its sibling delivered: the fan-out owns both events."""
    child = _http(FakeOpener(_down()))
    multi = MultiSink(child, MemorySink())

    multi.emit(_probe_d())

    own = child.losses()
    assert own.dropped == 0, "the child's failed run discarded its 413 drop"
    assert multi.losses() == SinkLosses(dropped=0, failed=own.failed + 2)


def test_an_opener_exception_after_a_drop_fails_the_run_and_counts_no_drop(capsys) -> None:
    """SPEC-073 FR-002 move 2: the drop beside a run that otherwise failed is not counted.

    The opener's ``RuntimeError`` is refused by the predicate, so its request is ``REJECTED`` at
    its one attempt and counted in ``failed``. With nothing else accepted the run fails, so the
    ``413`` drop is not counted and the event is counted with its batch, by the worker's line.
    Under SPEC-070 the drop settled the batch and was counted; with the ``400`` tests above this
    pins the same rule on the other ``REJECTED`` route of the classification.
    """
    opener = FakeOpener([FakeResponse(413, b""), RuntimeError("injected opener")])
    sink = HTTPSink("http://h", max_batch_count=1, max_retries=2, opener=opener)

    delivered, failed_batches = _through_the_worker(sink, _probe_d())

    err = capsys.readouterr().err
    assert not delivered and failed_batches == 1
    assert len(opener.calls) == 2, "one request per chunk; the RuntimeError is not retried"
    assert sink.losses() == SinkLosses(dropped=0, failed=1)
    assert _drop_lines(err) == []
    assert "lost 1 request(s); HTTPSink, 1 attempt(s), connection error, RuntimeError" in err
    assert "lost 2 event(s); batch abandoned after 1 emit attempts" in err
