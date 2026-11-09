"""SPEC-074 on #315's run context: an engine sink's ``UNKNOWN`` is announced once per run.

#315 gave the run context ``resends_unknown``, answered with the run sink's idempotence, because a
delegate forwarding ``deliver`` decides whether an ``UNKNOWN`` is resent, not the inner sink. The
converted sinks' ``UNKNOWN`` registrations — an unread SQS or SNS reply, an unadjudicable Kinesis
or Firehose response, MongoDB's unpairable ``writeErrors`` and NATS's unconfirmed publishes —
read it, so under an idempotent delegate, which resends them, each is counted and announced by
the last round only, not once per round. #315 also gave the context ``_mark``/``_rewind`` for the
HTTP round's hook guard; a rewind discards the SPEC-074 marks a faulting hook set as it discards
the hook's counts. Serves invariant 2.
"""

from __future__ import annotations

from typing import Any

import pytest

import nats_engine_matrix_harness as harness
from log_foundry import SinkCounters, emit_via_deliver
from log_foundry.sinks import _engine
from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.nats import NATSSink, _Published
from log_foundry.sinks.verdict import Outcome, Verdict
from test_http_rounds import Scripted, _ok
from test_sinks_db_engine import Script, bulk_error
from test_sinks_db_engine import build as build_db
from test_sinks_queue_engine import QUEUES, STREAMS, build, events, lost

U = Outcome.UNKNOWN


@pytest.fixture(autouse=True)
def _no_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("log_foundry.sinks._retry.wait", lambda _delay, _stop=None: None)


class Delegate:
    """A third-party sink forwarding ``deliver`` to an inner engine sink, as ``one_round`` allows."""

    max_retries = 1

    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.log_foundry_counters = SinkCounters()

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        return self.inner.deliver(batch)

    def emit(self, batch: list[dict[str, object]]) -> None:
        emit_via_deliver(self, batch)


class IdempotentDelegate(Delegate):
    log_foundry_idempotent = True


def _run(outer: type[Delegate], inner: Any, n: int) -> _engine.Delivery:
    return _engine.run_delivery(outer(inner), events(n), worker_retries=0, stop=None)


@pytest.mark.parametrize("outer", [Delegate, IdempotentDelegate], ids=["plain", "idempotent"])
@pytest.mark.parametrize("kind", QUEUES)
def test_an_unread_queue_reply_is_announced_once_per_run(
    kind: str, outer: type[Delegate], capsys: pytest.CaptureFixture[str]
) -> None:
    inner, client = build(kind, lambda entries, call: {"Failed": "garbage"}, max_retries=1)
    delivery = _run(outer, inner, 2)
    assert delivery.unknown == 2
    assert len(client.calls) == (2 if outer is IdempotentDelegate else 1), "the run's resend"
    lines = lost(capsys.readouterr().err)
    assert len(lines) == 1 and "could not adjudicate" in lines[0], lines


@pytest.mark.parametrize("outer", [Delegate, IdempotentDelegate], ids=["plain", "idempotent"])
@pytest.mark.parametrize("kind", STREAMS)
def test_an_unadjudicable_stream_response_is_counted_once_per_run(
    kind: str, outer: type[Delegate], capsys: pytest.CaptureFixture[str]
) -> None:
    key = (
        ("FailedRecordCount", "Records")
        if kind == "KinesisSink"
        else ("FailedPutCount", "RequestResponses")
    )
    inner, client = build(kind, lambda entries, call: {key[0]: 1, key[1]: [{}]}, max_retries=1)
    delivery = _run(outer, inner, 2)
    assert delivery.unknown == 2
    assert len(client.calls) == (2 if outer is IdempotentDelegate else 1), "the run's resend"
    assert inner.dropped_unadjudicated == 2, "each record counted once, by the last round"
    lines = lost(capsys.readouterr().err)
    assert len(lines) == 1 and "could not adjudicate" in lines[0], lines


@pytest.mark.parametrize("outer", [Delegate, IdempotentDelegate], ids=["plain", "idempotent"])
def test_unpairable_write_errors_are_announced_once_per_run(
    outer: type[Delegate], capsys: pytest.CaptureFixture[str]
) -> None:
    inner, collection = build_db("MongoDBSink", Script(then=bulk_error([9])), max_retries=1)
    delivery = _run(outer, inner, 2)
    assert delivery.unknown == 2
    assert len(collection.documents) == (2 if outer is IdempotentDelegate else 1)
    lines = lost(capsys.readouterr().err)
    assert len(lines) == 1 and "could not pair" in lines[0], lines


class _Unconfirming:
    """Forwards a round to ``NATSSink._register`` as an unanswered confirmation of every event."""

    max_retries = 1

    def __init__(self, inner: NATSSink) -> None:
        self.inner = inner
        self.log_foundry_counters = SinkCounters()

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        published = _Published([("ok", "")] * len(batch), False, "publish")
        self.inner._register(published, True)
        return Verdict(outcomes=(U,) * len(batch))

    def emit(self, batch: list[dict[str, object]]) -> None:
        emit_via_deliver(self, batch)


class _IdempotentUnconfirming(_Unconfirming):
    log_foundry_idempotent = True


@pytest.mark.parametrize(
    "outer", [_Unconfirming, _IdempotentUnconfirming], ids=["plain", "idempotent"]
)
def test_a_nats_unconfirmed_line_is_written_once_per_run(
    outer: type[_Unconfirming], capsys: pytest.CaptureFixture[str]
) -> None:
    inner = NATSSink("logs", client=harness.Client(harness.Script([])))
    try:
        delivery = _engine.run_delivery(outer(inner), events(2), worker_retries=0, stop=None)
    finally:
        inner.close()
    assert delivery.unknown == 2 and delivery.attempts == (2 if outer is not _Unconfirming else 1)
    lines = [line for line in lost(capsys.readouterr().err) if "no PONG" in line]
    assert lines == ["log-foundry: lost 2 event(s); NATSSink unconfirmed, no PONG after the batch"]


def test_a_rewind_discards_the_marks_set_since_its_mark() -> None:
    context = _engine.RunContext()
    context.final_rejections([0])
    context.sink_counted([1])
    first = ValueError("first")
    context.refuse(first)
    mark = context._mark()
    context.final_rejections([2])
    context.sink_counted([3])
    context.refuse(KeyError("second"))
    context._rewind(mark)
    assert context._marks() == _engine._Marks((0,), first, (1,))


def test_a_reader_that_marks_then_raises_costs_only_its_chunk(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """#315's guard settles a raising reader's chunk ``UNKNOWN``; its stale mark must go with it.

    Left in place, the final-rejection mark names an index whose outcome is now ``UNKNOWN``,
    and the engine fails the whole round as one whose marks do not fit, landed chunk included.
    """
    class Marking(HTTPSink):
        def _outcomes(self, payload: bytes, items: list[Any]) -> tuple[Outcome, ...]:
            if any(item.event.get("t") for item in items):
                _engine.run_context().final_rejections([1])
                raise RuntimeError("planted")
            return (Outcome.ACCEPTED,) * len(items)

    sink = Marking("http://h", max_batch_count=1, max_retries=0, opener=Scripted(_ok(), _ok()))
    delivery = _engine.run_delivery(sink, [{"i": 0}, {"i": 1, "t": 1}], worker_retries=0, stop=None)
    assert (delivery.accepted, delivery.unknown, delivery.raised) == (1, 1, 0)
    assert "marked an index" not in capsys.readouterr().err
