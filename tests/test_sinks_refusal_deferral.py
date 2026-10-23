"""SPEC-059 FR-003 — a refused hand-off counts only when emit returns, in Kafka and Pub/Sub.

``KafkaSink.emit`` and ``GooglePubSubSink.emit`` counted ``rejected`` the moment ``produce`` or
``publish`` raised. When every event was refused the sink raised, the worker retried, and each
attempt counted the same refusals again (Probe B: twelve for three events when down) — and when a
later attempt accepted them, the count recorded a loss that never happened. Both now count through
a ``Deferral`` whose ``with`` encloses the hand-off loop, the reap and the total-failure raise.

Every case runs for both sinks; the worker-driven ones use the real
``Worker(sink, batch_size=len(batch), max_retries=3)``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

import log_foundry
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.kafka import KafkaSink
from log_foundry.sinks.memory import MemorySink
from log_foundry.sinks.multi import MultiSink
from log_foundry.sinks.pubsub import GooglePubSubSink
from log_foundry.worker import Worker
from test_sinks_kafka import FakeProducer
from test_sinks_pubsub import PollableFuture, PollablePublisher

Refusal = Callable[[int], BaseException | None]
"""Given a hand-off's 1-based call number, the exception that refuses it, or ``None`` to accept."""

_SINKS = ("kafka", "pubsub")
_MARKERS = {"kafka": "KafkaSink produce, ", "pubsub": "GooglePubSubSink refused the publish, "}


def _never(_call: int) -> BaseException | None:
    return None


class _RefusingProducer(FakeProducer):
    """A Kafka producer that refuses chosen ``produce`` calls and counts every hand-off."""

    def __init__(self, refuse: Refusal, deliver_error: object = None) -> None:
        super().__init__(deliver_error)
        self.calls = 0
        self.refuse = refuse

    def produce(self, topic, value=None, key=None, callback=None) -> None:
        self.calls += 1
        error = self.refuse(self.calls)
        if error is not None:
            raise error
        super().produce(topic, value, key, callback)

    @property
    def accepted(self) -> int:
        return len(self.produced)


class _RefusingPublisher(PollablePublisher):
    """A Pub/Sub client that refuses chosen ``publish`` calls and counts every hand-off."""

    def __init__(self, refuse: Refusal, factory: Callable[[int], PollableFuture]) -> None:
        super().__init__(factory)
        self.calls = 0
        self.refuse = refuse

    def publish(self, topic, data=None, **attrs) -> PollableFuture:
        self.calls += 1
        error = self.refuse(self.calls)
        if error is not None:
            raise error
        return super().publish(topic, data, **attrs)

    @property
    def accepted(self) -> int:
        return len(self.futures)


def _build(name: str, refuse: Refusal) -> tuple[Any, Any]:
    """The sink over a client refusing as ``refuse`` says; settled, healthy futures for Pub/Sub."""
    if name == "kafka":
        producer = _RefusingProducer(refuse)
        return KafkaSink("logs", producer=producer), producer
    client = _RefusingPublisher(refuse, lambda _i: PollableFuture(settled=True))
    return GooglePubSubSink("projects/p/topics/t", client=client), client


def _refusal(name: str) -> BaseException:
    """Each client's own refusal: a full local queue for Kafka, a client fault for Pub/Sub."""
    return BufferError("local queue full") if name == "kafka" else RuntimeError("no credentials")


def _refusal_lines(err: str, name: str) -> list[str]:
    return [line for line in err.splitlines() if _MARKERS[name] in line]


def _three() -> list[dict[str, object]]:
    return [{"i": i} for i in range(3)]


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


@pytest.mark.parametrize("name", _SINKS)
def test_a_batch_refused_on_every_attempt_counts_no_refusal(name: str, capsys) -> None:
    """Probe B, down: every attempt refused all three and raised, so none of it counts."""
    error = _refusal(name)
    sink, client = _build(name, lambda _call: error)

    delivered, failed_batches = _through_the_worker(sink, _three())

    err = capsys.readouterr().err
    assert not delivered and failed_batches == 1
    assert client.calls == 12, "the premise: three hand-offs refused on each of four attempts"
    assert sink.rejected == 0
    assert sink.losses().dropped == 0
    assert _refusal_lines(err, name) == [], "no refusal line from an emit that raised"
    assert "lost 3 event(s); batch abandoned after 4 emit attempts" in err, (
        "the abandon line is the only count"
    )


@pytest.mark.parametrize("name", _SINKS)
def test_a_batch_accepted_on_retry_counts_no_refusal(name: str, capsys) -> None:
    """Probe B, once: every hand-off refused on attempt 1, all accepted on attempt 2 — no loss."""
    error = _refusal(name)
    sink, client = _build(name, lambda call: error if call <= 3 else None)

    delivered, failed_batches = _through_the_worker(sink, _three())

    assert delivered and failed_batches == 0
    assert client.calls == 6 and client.accepted == 3, "all three reached the client on retry"
    assert sink.rejected == 0, "the refusals of the raising attempt recorded a loss that never was"
    assert sink.losses().dropped == 0
    assert _refusal_lines(capsys.readouterr().err, name) == []


@pytest.mark.parametrize("name", _SINKS)
def test_a_partial_refusal_commits_one_count_and_one_line(name: str, capsys) -> None:
    """One refusal of three: the others were handed off, so the call returns and commits."""
    error = _refusal(name)
    sink, client = _build(name, lambda call: error if call == 2 else None)

    sink.emit(_three())

    assert client.accepted == 2
    assert sink.rejected == 1
    assert sink.losses().dropped == 1
    unit = "message" if name == "kafka" else "event"
    lines = _refusal_lines(capsys.readouterr().err, name)
    assert lines == [f"log-foundry: lost 1 {unit}(s); {_MARKERS[name]}{type(error).__name__}"]


@pytest.mark.parametrize("name", _SINKS)
def test_two_refusals_each_name_their_own_exception_type(name: str, capsys) -> None:
    """Each line names its own event's exception type, bound when the refusal was decided."""
    errors: dict[int, BaseException] = {1: BufferError("full"), 2: RuntimeError("fault")}
    sink, _ = _build(name, errors.get)

    sink.emit(_three())

    lines = _refusal_lines(capsys.readouterr().err, name)
    assert [line.rsplit(", ", 1)[1] for line in lines] == ["BufferError", "RuntimeError"], lines
    assert sink.rejected == 2


@pytest.mark.parametrize("name", _SINKS)
def test_a_refusal_followed_by_a_raise_counts_nothing(name: str, capsys) -> None:
    """A raise elsewhere in the same emit — Kafka's ``poll``, Pub/Sub's reap — discards the refusal.

    Pub/Sub's raise is the real ``_reap`` → ``_resolve`` path: the accepted publish's future has
    settled to a ``KeyboardInterrupt``, which ``_resolve`` does not catch.
    """
    error = _refusal(name)
    if name == "kafka":

        class PollRaises(_RefusingProducer):
            def poll(self, timeout) -> None:
                raise RuntimeError("poll failed")

        client: Any = PollRaises(lambda call: error if call == 1 else None)
        sink: Any = KafkaSink("logs", producer=client)
        raised: type[BaseException] = RuntimeError
    else:
        client = _RefusingPublisher(
            lambda call: error if call == 1 else None,
            lambda _i: PollableFuture(KeyboardInterrupt(), settled=True),  # type: ignore[arg-type]
        )
        sink = GooglePubSubSink("projects/p/topics/t", client=client)
        raised = KeyboardInterrupt

    with pytest.raises(raised):
        sink.emit([{"i": 0}, {"i": 1}])

    assert client.calls == 2, "the premise: the refusal was decided before the raise"
    assert sink.rejected == 0
    assert _refusal_lines(capsys.readouterr().err, name) == []


@pytest.mark.parametrize("name", _SINKS)
def test_a_closed_sink_raises_and_counts_nothing(name: str, capsys) -> None:
    """Unchanged: a closed sink refuses the batch before touching its client."""
    sink, client = _build(name, lambda _call: _refusal(name))
    sink.close()

    with pytest.raises(SinkDeliveryError, match="closed"):
        sink.emit(_three())

    assert client.calls == 0
    assert sink.losses() == SinkLosses(dropped=0, failed=0)
    assert _refusal_lines(capsys.readouterr().err, name) == []


def test_the_kafka_delivery_callback_counts_failed_before_emit_returns() -> None:
    """``failed`` stays immediate: observed from inside ``poll``, where a deferred count reads 0."""
    seen: list[int] = []

    class Observing(_RefusingProducer):
        def poll(self, timeout) -> None:
            seen.append(sink.failed)

    sink = KafkaSink("logs", producer=Observing(_never, deliver_error=RuntimeError("nack")))

    sink.emit([{"i": 0}])

    assert seen == [1], "the callback's failed moved inside emit, before it returned"
    assert sink.losses() == SinkLosses(dropped=0, failed=1)


def test_the_pubsub_orphaned_future_counts_failed_before_emit_returns() -> None:
    """``failed`` for a future a mid-batch close orphaned moves inside emit, as it happens."""
    seen: list[int] = []

    class Closing(_RefusingPublisher):
        def publish(self, topic, data=None, **attrs) -> PollableFuture:
            if self.calls == 0:
                future = super().publish(topic, data, **attrs)
                sink.close()
                return future
            seen.append(sink.failed)
            return super().publish(topic, data, **attrs)

    sink = GooglePubSubSink(
        "projects/p/topics/t", client=Closing(_never, lambda _i: PollableFuture(settled=False))
    )

    sink.emit([{"i": 0}, {"i": 1}])

    assert seen == [1], "the first orphan was counted before the second publish, inside emit"
    assert sink.losses() == SinkLosses(dropped=0, failed=2)


def test_the_pubsub_reap_counts_failed_before_emit_returns() -> None:
    """``failed`` from the reap moves inside emit: observed after the real ``_reap`` returns."""
    sink = GooglePubSubSink(
        "projects/p/topics/t",
        client=_RefusingPublisher(_never, lambda _i: PollableFuture(RuntimeError("nack"))),
    )
    real = sink._reap
    seen: list[int] = []

    def observed() -> None:
        real()
        seen.append(sink.failed)

    sink._reap = observed  # type: ignore[method-assign]

    sink.emit([{"i": 0}])

    assert seen == [1], "the reap's failed moved inside emit, before it returned"
    assert sink.losses() == SinkLosses(dropped=0, failed=1)


@pytest.mark.parametrize("name", _SINKS)
def test_probe_c_an_orphan_refusal_is_in_orphan_lost_only(name: str) -> None:
    """Outside a span the refused event is the whole batch, so only ``orphan_lost`` counts it."""
    error = _refusal(name)
    sink, client = _build(name, lambda _call: error)
    log_foundry.configure(service="t", sink=sink)

    log_foundry.info("outside a span")

    assert client.calls == 1, "the premise: the one event was offered and refused"
    assert log_foundry.health().orphan_lost == 1
    assert sink.losses().dropped == 0, "one event, one term: not also in the sink's dropped"


@pytest.mark.parametrize("name", _SINKS)
def test_a_fanout_absorbs_the_refused_batch(name: str) -> None:
    """The child refused everything and raised, its sibling delivered: the fan-out owns all 3."""
    error = _refusal(name)
    child, _ = _build(name, lambda _call: error)
    multi = MultiSink(child, MemorySink())

    multi.emit(_three())

    assert child.losses() == SinkLosses(dropped=0, failed=0)
    assert multi.losses() == SinkLosses(dropped=0, failed=3)
