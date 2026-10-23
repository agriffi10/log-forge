"""SPEC-059 FR-001 — an oversize drop in the record-building sinks counts only when emit returns.

``KinesisSink``, ``FirehoseSink``, ``SNSSink``, ``MongoDBSink`` and ``AzureEventHubsSink`` decide an
oversize drop before any request. Counting it at once meant a raise later in the same ``emit`` left
it counted once per worker attempt (Probe A: four for one event when the destination is down). Each
now records the drop in the calling ``emit``'s ``Deferral``, so a raising ``emit`` counts nothing
and the catcher — the worker's abandon line, the orphan path, ``MultiSink`` — owns the batch.

Every probe runs through the real ``Worker(sink, batch_size=len(batch), max_retries=3)``, with the
sink's own ``max_retries=0`` and its module's backoff ``wait`` replaced by a no-op. *Down* fails
every send; *once* fails the first and serves the second.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry
from log_foundry import _diag
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.eventhubs import AzureEventHubsSink
from log_foundry.sinks.firehose import FirehoseSink
from log_foundry.sinks.kinesis import KinesisSink
from log_foundry.sinks.memory import MemorySink
from log_foundry.sinks.mongodb import MongoDBSink
from log_foundry.sinks.multi import MultiSink
from log_foundry.sinks.sns import SNSSink
from log_foundry.worker import Worker
from test_sinks_eventhubs import FakeProducer
from test_sinks_firehose import _Raising as FirehoseClient
from test_sinks_kinesis import _Raising as KinesisClient
from test_sinks_mongodb import FakeBulkWriteError, FakeCollection, make_sink
from test_sinks_sns import _Raising as SNSClient

if TYPE_CHECKING:
    from collections.abc import Callable

DOWN = set(range(1, 100))
ONCE = {1}
_MODULES = ("kinesis", "firehose", "sns", "mongodb", "eventhubs")


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch) -> None:
    """Every sink's retry wait is a no-op, and Event Hubs packs raw bytes (no azure dependency)."""
    for module in _MODULES:
        monkeypatch.setattr(f"log_foundry.sinks.{module}.wait", lambda _delay, _stop=None: None)
    monkeypatch.setattr("log_foundry.sinks.eventhubs._event_data_cls", lambda: lambda body: body)


class _InterruptingKinesis(KinesisClient):
    """A Kinesis client whose every request raises ``KeyboardInterrupt``."""

    def _go(self, items: list[str]) -> None:
        self.calls += 1
        raise KeyboardInterrupt


class _InterruptingFirehose(FirehoseClient):
    """A Firehose client whose every request raises ``KeyboardInterrupt``."""

    def _go(self, items: list[str]) -> None:
        self.calls += 1
        raise KeyboardInterrupt


class _InterruptingSNS(SNSClient):
    """An SNS client whose every request raises ``KeyboardInterrupt``."""

    def _go(self, items: list[str]) -> None:
        self.calls += 1
        raise KeyboardInterrupt


class _NumberedCollection(FakeCollection):
    """A collection whose ``insert_many`` raises on chosen call numbers, or always raises ``error``."""

    def __init__(self, fail_on: set[int], error: BaseException | None) -> None:
        super().__init__()
        self.calls = 0
        self.fail_on = fail_on
        self.error = error

    def insert_many(self, documents: Any, ordered: bool = True) -> None:
        self.calls += 1
        self.inserted.append((list(documents), ordered))
        if self.error is not None:
            raise self.error
        if self.calls in self.fail_on:
            raise ConnectionError("the server could not be reached")


class _NumberedProducer(FakeProducer):
    """A producer holding 600 bytes a batch whose ``send_batch`` raises on chosen call numbers."""

    def __init__(self, fail_on: set[int], error: BaseException | None) -> None:
        super().__init__(max_bytes=600)
        self.calls = 0
        self.fail_on = fail_on
        self.error = error

    def send_batch(self, batch: Any) -> None:
        self.calls += 1
        if self.error is not None:
            raise self.error
        if self.calls in self.fail_on:
            raise RuntimeError("the hub could not be reached")
        self.sent.append(list(batch.items))


@dataclass(frozen=True)
class _Case:
    """One sink: how to build it over a numbered failing client, and what its drop looks like."""

    build: Callable[[set[int], BaseException | None], Any]
    oversized: int
    marker: str


def _kinesis(fail_on: set[int], error: BaseException | None) -> KinesisSink:
    client = _InterruptingKinesis(set()) if error else KinesisClient(fail_on)
    return KinesisSink("stream", client=client, max_retries=0)


def _firehose(fail_on: set[int], error: BaseException | None) -> FirehoseSink:
    client = _InterruptingFirehose(set()) if error else FirehoseClient(fail_on)
    return FirehoseSink("ds", client=client, max_retries=0)


def _sns(fail_on: set[int], error: BaseException | None) -> SNSSink:
    client = _InterruptingSNS(set()) if error else SNSClient(fail_on)
    return SNSSink("arn:topic", client=client, max_retries=0)


def _mongodb(fail_on: set[int], error: BaseException | None) -> MongoDBSink:
    return make_sink(_NumberedCollection(fail_on, error), max_retries=0)


def _eventhubs(fail_on: set[int], error: BaseException | None) -> AzureEventHubsSink:
    return AzureEventHubsSink(producer=_NumberedProducer(fail_on, error), max_retries=0)


_SINKS = {
    "eventhubs": _Case(_eventhubs, 2_000, "AzureEventHubsSink, too large for an empty"),
    "firehose": _Case(_firehose, 1_100_000, "-byte per-record limit"),
    "kinesis": _Case(_kinesis, 1_100_000, "-byte per-record limit"),
    "mongodb": _Case(_mongodb, 17_000_000, "MongoDBSink, exceeds the 16 MB limit"),
    "sns": _Case(_sns, 300_000, "-byte message limit"),
}


def _requests(sink: Any) -> int:
    """How many requests the sink's client was asked to make."""
    if isinstance(sink, AzureEventHubsSink):
        return int(sink.producer.calls)
    if isinstance(sink, MongoDBSink):
        return int(sink._collection.calls)
    return int(sink.client.calls)


def _pair(case: _Case) -> list[dict[str, object]]:
    """One oversized event, then one ordinary event."""
    return [{"big": "x" * case.oversized}, {"i": 1}]


def _drop_lines(err: str, case: _Case) -> list[str]:
    return [line for line in err.splitlines() if case.marker in line]


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


@pytest.mark.parametrize("name", sorted(_SINKS))
def test_an_abandoned_batch_counts_no_oversize_drop(name: str, capsys) -> None:
    """Probe A, down: four attempts each decided the drop; none of them returned, so none counts."""
    case = _SINKS[name]
    sink = case.build(DOWN, None)

    delivered, failed_batches = _through_the_worker(sink, _pair(case))

    err = capsys.readouterr().err
    assert not delivered and failed_batches == 1, "every attempt raised; the worker abandoned"
    assert _requests(sink) == 4, "the premise: the ordinary event was offered on every attempt"
    assert sink.dropped_oversized == 0
    assert sink.losses().dropped == 0
    assert _drop_lines(err, case) == [], "no oversize line from an emit that raised"
    assert "lost 2 event(s); batch abandoned after 4 emit attempts" in err, (
        "the abandon line is the only count of the oversized event"
    )


@pytest.mark.parametrize("name", sorted(_SINKS))
def test_a_batch_delivered_on_retry_counts_its_oversize_drop_once(name: str, capsys) -> None:
    """Probe A, once: attempt 1 raises and counts nothing, attempt 2 returns and counts one."""
    case = _SINKS[name]
    sink = case.build(ONCE, None)

    delivered, failed_batches = _through_the_worker(sink, _pair(case))

    err = capsys.readouterr().err
    assert delivered and failed_batches == 0
    assert _requests(sink) == 2, "the premise: one failed attempt, then one that delivered"
    assert sink.dropped_oversized == 1
    assert sink.losses().dropped == 1
    assert len(_drop_lines(err, case)) == 1, _drop_lines(err, case)


@pytest.mark.parametrize("name", sorted(_SINKS))
def test_an_all_oversized_batch_makes_no_request_and_counts_all(name: str, capsys) -> None:
    """Unchanged: nothing to send is not a send failure, so it returns and commits every drop."""
    case = _SINKS[name]
    sink = case.build(DOWN, None)

    sink.emit([{"big": "x" * case.oversized, "n": n} for n in range(3)])

    assert _requests(sink) == 0
    assert sink.dropped_oversized == 3
    assert sink.losses().dropped == 3
    assert len(_drop_lines(capsys.readouterr().err, case)) == 3


@pytest.mark.parametrize("name", sorted(_SINKS))
def test_a_keyboard_interrupt_on_the_send_counts_nothing(name: str, capsys) -> None:
    """A ``BaseException`` from the client leaves ``emit`` and discards the drop decided before it."""
    case = _SINKS[name]
    sink = case.build(set(), KeyboardInterrupt())

    with pytest.raises(KeyboardInterrupt):
        sink.emit(_pair(case))

    assert _requests(sink) == 1, "the premise: the interrupt came from the send"
    assert sink.dropped_oversized == 0
    assert _drop_lines(capsys.readouterr().err, case) == []


def test_a_partial_bulk_write_commits_the_oversize_drop_and_the_rejects(capsys) -> None:
    """A bulk write that stored some returns, so the oversize drop beside it commits."""
    sink = make_sink(FakeCollection(error=FakeBulkWriteError(1), error_times=1), max_retries=0)

    sink.emit([{"big": "x" * 17_000_000}, {"i": 1}, {"i": 2}])

    assert sink.losses() == SinkLosses(dropped=1, failed=1)
    err = capsys.readouterr().err
    assert len(_drop_lines(err, _SINKS["mongodb"])) == 1
    assert "MongoDBSink bulk write; the rest were inserted" in err


@pytest.mark.parametrize("name", ["eventhubs", "mongodb"])
def test_a_closed_sink_raises_and_counts_nothing(name: str, capsys) -> None:
    """Unchanged: a closed sink refuses the batch before deciding anything."""
    case = _SINKS[name]
    sink = case.build(set(), None)
    sink.close()

    with pytest.raises(SinkDeliveryError, match="closed"):
        sink.emit(_pair(case))

    assert sink.dropped_oversized == 0
    assert sink.losses() == SinkLosses(dropped=0, failed=0)
    assert _drop_lines(capsys.readouterr().err, case) == []


def test_the_eventhubs_oversize_line_is_written_outside_the_lock(monkeypatch) -> None:
    """The ledger is entered outside the emit lock, so its line runs after the lock is released."""
    sink = _eventhubs(set(), None)
    real = _diag.lost
    held: list[bool] = []

    def spy(unit: str, n: int, detail: str | None = None) -> None:
        if detail is not None and "too large for an empty" in detail:
            held.append(sink._lock.locked())
        real(unit, n, detail)

    monkeypatch.setattr(_diag, "lost", spy)

    sink.emit(_pair(_SINKS["eventhubs"]))

    assert held == [False], "the oversize line was written while the emit lock was held"


@pytest.mark.parametrize("name", ["firehose", "kinesis"])
def test_two_oversized_events_each_name_their_own_size(name: str, capsys) -> None:
    """Each line names its own record's size, bound when the drop was decided, in event order."""
    case = _SINKS[name]
    sink = case.build(set(), None)

    sink.emit([{"big": "x" * 1_100_000}, {"big": "x" * 1_200_000}, {"i": 1}])

    lines = _drop_lines(capsys.readouterr().err, case)
    sizes = [int(match.group(1)) for line in lines if (match := re.search(r"(\d+) bytes", line))]
    assert len(lines) == 2 and len(sizes) == 2, lines
    assert sizes[1] - sizes[0] == 100_000, f"each line names its own record: {sizes}"


@pytest.mark.parametrize("name", sorted(_SINKS))
def test_an_orphan_oversized_event_is_counted_once(name: str) -> None:
    """Unchanged: outside a span the batch is the one event, which never raises and counts once."""
    case = _SINKS[name]
    sink = case.build(set(), None)
    log_foundry.configure(service="t", sink=sink, max_value_bytes=case.oversized * 2)

    log_foundry.info("outside a span", big="x" * case.oversized)

    assert sink.dropped_oversized == 1
    assert sink.losses().dropped == 1
    assert log_foundry.health().orphan_lost == 0


@pytest.mark.parametrize("name", sorted(_SINKS))
def test_a_fanout_absorbs_the_oversized_event_once(name: str) -> None:
    """The child raised, its sibling delivered: the fan-out owns both events, the child neither."""
    case = _SINKS[name]
    child = case.build(DOWN, None)
    multi = MultiSink(child, MemorySink())

    multi.emit(_pair(case))

    own = child.losses()
    assert own.dropped == 0, "the child's raise discarded its oversize drop"
    assert multi.losses() == SinkLosses(dropped=0, failed=own.failed + 2)
