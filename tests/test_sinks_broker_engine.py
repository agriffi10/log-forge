"""SPEC-074 FR-004 — the broker sinks, NATSSink among them, report per message on the engine.

The two Redis sinks make one pipeline per round, ``RabbitMQSink`` one publish per message per
round and ``AzureEventHubsSink`` one send per pack per round, the last two under their lock for
that round only. ``NATSSink`` keeps SPEC-061's coroutine and maps what it learnt; its full ledger
against 1.x is ``tests/test_sinks_nats_engine_matrix.py``'s. Every double here is this file's own
and records what reached it. Serves invariants 2, 3, 6 and 7.
"""

from __future__ import annotations

import asyncio
import json
import sys
import threading
import types
from typing import Any

import pytest

import log_foundry
from conftest import install_worker
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.eventhubs import AzureEventHubsSink
from log_foundry.sinks.nats import NATSSink
from log_foundry.sinks.rabbitmq import RabbitMQSink
from log_foundry.sinks.redis import RedisListSink, RedisStreamsSink
from log_foundry.worker import Worker

REDIS = (RedisStreamsSink, RedisListSink)


@pytest.fixture(autouse=True)
def _no_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("log_foundry.sinks._retry.wait", lambda _delay, _stop=None: None)
    monkeypatch.setattr("log_foundry.sinks.eventhubs._event_data_cls", lambda: lambda body: body)


def events(n: int) -> list[dict[str, object]]:
    return [{"i": i} for i in range(n)]


def lost(err: str) -> list[str]:
    return [line for line in err.splitlines() if line.startswith("log-foundry: lost")]


def _worker(sink: object, retries: int = 2) -> Worker:
    worker = Worker(sink, batch_size=1000, flush_interval=60.0, max_retries=retries)  # type: ignore[arg-type]
    install_worker(worker)
    return worker


def through_worker(sink: object, batch: list[dict[str, object]], retries: int = 2) -> Worker:
    worker = _worker(sink, retries)
    worker.submit(batch)
    worker.flush(timeout=30.0)
    return worker


# --- Redis ----------------------------------------------------------------------------------------


class Pipeline:
    def __init__(self, owner: Redis) -> None:
        self.owner = owner

    def xadd(self, name: str, fields: Any, **options: Any) -> None:
        pass

    def rpush(self, name: str, value: Any) -> None:
        pass

    def ltrim(self, name: str, start: int, end: int) -> None:
        pass

    def execute(self) -> list[object]:
        self.owner.executes += 1
        if self.owner.error is not None:
            raise self.owner.error
        return []


class Redis:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.executes = 0

    def pipeline(self) -> Pipeline:
        return Pipeline(self)

    def close(self) -> None:
        pass


def _redis(cls: type, error: Exception | None = None) -> tuple[Any, Redis]:
    client = Redis(error)
    return cls("k", client=client, max_retries=3), client


@pytest.mark.parametrize("cls", REDIS, ids=lambda c: c.__name__)
def test_redis_spends_the_budget(cls: type, capsys: pytest.CaptureFixture[str]) -> None:
    sink, client = _redis(cls, ConnectionError("down"))
    with pytest.raises(SinkDeliveryError):
        sink.emit(events(5))
    assert client.executes == 4
    assert sink.failed == 5
    assert lost(capsys.readouterr().err) == [
        f"log-foundry: lost 5 event(s); {cls.__name__}, 4 attempt(s), ConnectionError"
    ]
    worked, worked_client = _redis(cls, ConnectionError("down"))
    worker = through_worker(worked, events(5))
    assert worked_client.executes == 12, "the Overview's probe: (3 + 1) x (2 + 1)"
    assert worker.failed_batches == 1
    assert worked.failed == 5


@pytest.mark.parametrize("cls", REDIS, ids=lambda c: c.__name__)
def test_redis_type_error_is_one_call(cls: type, capsys: pytest.CaptureFixture[str]) -> None:
    sink, client = _redis(cls, TypeError("caller"))
    with pytest.raises(SinkDeliveryError):
        sink.emit(events(2))
    assert client.executes == 1
    assert sink.failed == 2
    assert lost(capsys.readouterr().err) == [
        f"log-foundry: lost 2 event(s); {cls.__name__}, 1 attempt(s), TypeError"
    ]
    worked, worked_client = _redis(cls, TypeError("caller"))
    through_worker(worked, events(2))
    assert worked_client.executes == 1, "SPEC-070 FR-001's three, superseded on the engine"
    assert worked.failed == 2


# --- RabbitMQ -------------------------------------------------------------------------------------


class Channel:
    def __init__(self, owner: AMQP) -> None:
        self.owner = owner

    def basic_publish(
        self, *, exchange: str, routing_key: str, body: bytes, properties: Any
    ) -> None:
        i = json.loads(body).get("i")
        error = self.owner.plan.pop(len(self.owner.attempts), None)
        self.owner.attempts.append(i)
        if error is not None:
            raise error
        self.owner.published.append(i)

    def close(self) -> None:
        pass


class AMQP:
    """A connection whose ``plan`` maps a publish's 0-based number to the error it raises."""

    def __init__(self, plan: dict[int, Exception]) -> None:
        self.plan = plan
        self.attempts: list[int] = []
        self.published: list[int] = []
        self.release: threading.Event | None = None

    def channel(self) -> Channel:
        return Channel(self)

    def close(self) -> None:
        pass


def _rabbit(plan: dict[int, Exception], **kwargs: Any) -> tuple[RabbitMQSink, AMQP]:
    connection = AMQP(plan)
    return RabbitMQSink(exchange="e", routing_key="r", connection=connection, **kwargs), connection


def test_rabbitmq_resends_only_the_failed_message(monkeypatch: pytest.MonkeyPatch) -> None:
    sink, connection = _rabbit({1: ConnectionError("dropped")}, max_retries=3)
    resets: list[int] = []
    real = sink._reset
    monkeypatch.setattr(sink, "_reset", lambda: (resets.append(1), real())[1])
    sink.emit(events(3))
    assert connection.attempts == [0, 1, 2, 1]
    assert len(resets) == 1
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


def test_rabbitmq_refused_message_costs_only_itself() -> None:
    sink, connection = _rabbit({1: TypeError("bad")}, max_retries=3)
    sink.emit(events(3))
    assert connection.published == [0, 2]
    assert sink.failed == 1


# --- Event Hubs -----------------------------------------------------------------------------------


class Pack:
    def __init__(self, limit: int) -> None:
        self.items: list[bytes] = []
        self.limit = limit

    def add(self, data: bytes) -> None:
        if sum(len(item) for item in self.items) + len(data) > self.limit:
            raise ValueError("full")
        self.items.append(data)

    def __len__(self) -> int:
        return len(self.items)


class Producer:
    def __init__(self, errors: list[Exception | None], limit: int = 30) -> None:
        self.errors = errors
        self.limit = limit
        self.sends: list[list[int]] = []

    def create_batch(self) -> Pack:
        return Pack(self.limit)

    def send_batch(self, batch: Pack) -> None:
        self.sends.append([json.loads(item).get("i") for item in batch.items])
        error = self.errors.pop(0) if self.errors else None
        if error is not None:
            raise error

    def close(self) -> None:
        pass


def test_eventhubs_oversize_counts_on_a_settled_run() -> None:
    producer = Producer([], limit=30)
    sink = AzureEventHubsSink(producer=producer, max_retries=3)
    sink.emit([{"i": 0}, {"i": 1, "pad": "x" * 40}, {"i": 2}, {"i": 3}])
    assert sink.dropped_oversized == 1
    assert sink.losses().dropped == 1


def test_eventhubs_resends_only_the_failed_pack() -> None:
    producer = Producer([ConnectionError("down")], limit=20)
    sink = AzureEventHubsSink(producer=producer, max_retries=3)
    sink.emit(events(4))
    assert producer.sends == [[0, 1], [2, 3], [0, 1]]
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


# --- a close during a backoff ---------------------------------------------------------------------


@pytest.mark.parametrize("kind", ["RabbitMQSink", "AzureEventHubsSink"])
def test_a_close_during_a_backoff_returns_before_the_next_round(
    kind: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    waiting = threading.Event()
    release = threading.Event()

    def wait(delay: float, stop: object = None) -> None:
        waiting.set()
        assert release.wait(10.0)

    monkeypatch.setattr("log_foundry.sinks._retry.wait", wait)
    if kind == "RabbitMQSink":
        sink, connection = _rabbit({0: ConnectionError("down")}, max_retries=3)
        attempts = lambda: len(connection.attempts)  # noqa: E731
    else:
        producer = Producer([ConnectionError("down")] * 10, limit=1000)
        sink = AzureEventHubsSink(producer=producer, max_retries=3)  # type: ignore[assignment]
        attempts = lambda: len(producer.sends)  # noqa: E731
    outcome: list[BaseException | None] = []

    def emit() -> None:
        try:
            sink.emit(events(1))
            outcome.append(None)
        except BaseException as err:
            outcome.append(err)

    thread = threading.Thread(target=emit)
    thread.start()
    assert waiting.wait(10.0)
    closer = threading.Thread(target=sink.close)
    closer.start()
    closer.join(2.0)
    assert not closer.is_alive(), "close() waited on a lock held across the backoff"
    release.set()
    thread.join(10.0)
    assert attempts() == 1, "the round after the close published nothing"
    assert isinstance(outcome[0], SinkDeliveryError) and "closed" in str(outcome[0])
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


# --- NATS -----------------------------------------------------------------------------------------


class NATS:
    """A ``nats-py``-shaped client whose PINGs are answered as ``answers`` says, in turn."""

    def __init__(self, answers: list[bool] | None = None, steps: list[str] | None = None) -> None:
        self.answers = list(answers or [])
        self.steps = list(steps or [])
        self.is_connected = True
        self.pending_data_size = 0
        self.stats = {"reconnects": 0}
        self.flushes = 0
        self.published: list[bytes] = []
        self.gate: asyncio.Event | None = None

    async def flush(self, timeout: float | None = None) -> None:  # noqa: ASYNC109
        self.flushes += 1
        answered = self.answers.pop(0) if self.answers else True
        if not answered:
            raise TimeoutError("no PONG")

    async def publish(self, subject: str, payload: bytes, *, timeout: float | None = None) -> None:  # noqa: ASYNC109
        step = self.steps.pop(0) if self.steps else "ok"
        if step == "raise":
            raise RuntimeError("no responders")
        if step == "ack":
            raise TimeoutError("ack wait expired")
        if step == "hold":
            self.gate = asyncio.Event()
            await self.gate.wait()
        self.published.append(payload)

    def jetstream(self) -> NATS:
        return self

    async def close(self) -> None:
        if self.gate is not None:
            self.gate.set()

    async def drain(self) -> None:
        pass


def _owned(monkeypatch: pytest.MonkeyPatch, client: NATS, **kwargs: Any) -> NATSSink:
    module = types.ModuleType("nats")

    async def connect(*args: Any, **kw: Any) -> NATS:
        return client

    module.connect = connect  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "nats", module)
    return NATSSink(
        "logs", servers="nats://x:4222", publish_timeout=0.5, drain_timeout=0.2, **kwargs
    )


def _delivers(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    calls: list[int] = []
    real = NATSSink.deliver
    monkeypatch.setattr(
        NATSSink, "deliver", lambda self, batch: (calls.append(1), real(self, batch))[1]
    )
    return calls


def test_nats_an_unanswered_probe_is_retried_by_the_worker(monkeypatch: pytest.MonkeyPatch) -> None:
    client = NATS(answers=[False, False, False])
    sink = _owned(monkeypatch, client)
    calls = _delivers(monkeypatch)
    try:
        worker = through_worker(sink, events(2))
        assert len(calls) == 3
        assert client.published == []
        assert worker.failed_batches == 1
        assert sink.losses() == SinkLosses(dropped=0, failed=0)
    finally:
        sink.close()


def test_nats_a_pong_on_the_second_round_delivers_once(monkeypatch: pytest.MonkeyPatch) -> None:
    client = NATS(answers=[False, True, True])
    sink = _owned(monkeypatch, client)
    calls = _delivers(monkeypatch)
    try:
        worker = through_worker(sink, events(2))
        assert len(calls) == 2
        assert len(client.published) == 2
        assert worker.failed_batches == 0
        assert sink.losses() == SinkLosses(dropped=0, failed=0)
    finally:
        sink.close()


def test_nats_a_raise_beside_unconfirmed_publishes(monkeypatch: pytest.MonkeyPatch) -> None:
    client = NATS(answers=[True, False], steps=["raise", "ok", "ok"])
    sink = _owned(monkeypatch, client)
    try:
        sink.emit(events(3))
        assert sink.failed == 3
    finally:
        sink.close()


def test_nats_every_publish_raising_raises_including_an_ack_wait() -> None:
    client = NATS(steps=["raise", "ack", "raise"])
    sink = NATSSink("logs", client=client, jetstream=True, publish_timeout=0.5)
    try:
        with pytest.raises(SinkDeliveryError):
            sink.emit(events(3))
        assert sink.failed == 3
    finally:
        sink.close()


def test_nats_a_blocked_publish_still_returns() -> None:
    client = NATS(steps=["hold"])
    sink = NATSSink("logs", client=client, publish_timeout=0.2)
    try:
        sink.emit(events(4))
        assert sink.failed == 4
    finally:
        sink.close()


def test_nats_one_unencodable_event_is_failed_not_dropped() -> None:
    sink = NATSSink("logs", client=NATS(), publish_timeout=0.5)
    try:
        sink.emit([{"i": 0}, {"i": 1, "blob": b"raw"}, {"i": 2}])
        assert sink.losses() == SinkLosses(dropped=0, failed=1)
    finally:
        sink.close()


def test_nats_all_unencodable_raises() -> None:
    sink = NATSSink("logs", client=NATS(), publish_timeout=0.5)
    try:
        with pytest.raises(SinkDeliveryError):
            sink.emit([{"blob": b"a"}, {"blob": b"b"}])
        assert sink.failed == 2
    finally:
        sink.close()


# --- the worker and orphan twins ------------------------------------------------------------------


def _orphan(sink: Any) -> SinkLosses:
    log_foundry.configure(service="t", sink=sink)
    log_foundry.info("one")
    log_foundry.info("two")
    return sink.losses()


def _worked(sink: Any) -> SinkLosses:
    worker = _worker(sink, retries=0)
    for _ in range(2):
        worker.submit(events(1))
        worker.flush(timeout=30.0)
    return sink.losses()


def _make(kind: str, case: str) -> Any:
    if kind in ("RedisStreamsSink", "RedisListSink"):
        cls = RedisStreamsSink if kind == "RedisStreamsSink" else RedisListSink
        return _redis(cls, ConnectionError("down") if case == "total" else None)[0]
    if kind == "RabbitMQSink":
        plan: dict[int, Exception] = (
            {0: TypeError("x")}
            if case == "partial"
            else {i: ConnectionError("x") for i in range(9)}
        )
        return _rabbit(plan, max_retries=1)[0]
    errors: list[Exception | None] = (
        [ConnectionError("down")] * 9 if case == "total" else [TypeError("x")]
    )
    return AzureEventHubsSink(producer=Producer(errors, limit=1000), max_retries=1)


@pytest.mark.parametrize("case", ["total", "partial"])
@pytest.mark.parametrize(
    "kind", ["RedisStreamsSink", "RedisListSink", "RabbitMQSink", "AzureEventHubsSink"]
)
def test_the_twins_keep_one_ledger(kind: str, case: str) -> None:
    assert _orphan(_make(kind, case)) == _worked(_make(kind, case))


@pytest.mark.parametrize("case", ["total", "partial"])
def test_both_redis_sinks_keep_one_ledger(case: str) -> None:
    error = ConnectionError("down") if case == "total" else None
    ledgers = []
    for cls in REDIS:
        sink, client = _redis(cls, error)
        worker = through_worker(sink, events(3))
        ledgers.append((client.executes, worker.failed_batches, sink.losses()))
    assert ledgers[0] == ledgers[1]
