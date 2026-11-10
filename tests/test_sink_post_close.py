"""SPEC-076 FR-004 — a closed or closing sink answers without reaching its transport.

An engine sink's refused round calls ``RunContext.refuse`` with the ``SinkDeliveryError`` its 1.x
``emit`` raised and answers ``REJECTED`` for every event, so a run that fails moves no sink
counter and the adapter raises that error by identity; an emit-only adopter raises its 1.x
refusal. Every case also asserts the driver double saw no call. Each sink is built over the
recording rig ``tests/sink_rigs.py`` already has for it, or ``test_engine_sinks_roster._build``
for ``NATSSink``. Serves invariants 2, 6 and 7.
"""

from __future__ import annotations

import ast
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry
import sink_rigs
from conftest import install_worker
from log_foundry import SinkLosses
from log_foundry.sinks import _engine
from log_foundry.sinks._engine import emit_via_deliver
from log_foundry.sinks.base import SinkDeliveryError
from log_foundry.sinks.nats import NATSSink
from log_foundry.sinks.verdict import Outcome, Verdict
from log_foundry.worker import Worker

if TYPE_CHECKING:
    from collections.abc import Callable

LIMIT = 5.0

ENGINE_CLOSED: dict[str, Callable[[int], str]] = {
    "ClickHouseSink": lambda n: f"ClickHouseSink inserted none of {n} event(s): the sink is closed",
    "PostgresSink": lambda n: f"PostgresSink inserted none of {n} event(s): the sink is closed",
    "AzureEventHubsSink": lambda n: f"AzureEventHubsSink sent none of {n} event(s): the sink is closed",
    "RabbitMQSink": lambda n: f"RabbitMQSink published none of {n} message(s): the sink is closed",
    "SocketTransport": lambda n: (
        f"SocketTransport delivered none of {n} message(s): the transport is closed"
    ),
    "NATSSink": lambda n: f"NATSSink published none of {n} event(s): the sink is closed",
    "MongoDBSink": lambda n: f"MongoDBSink inserted none of {n} document(s): the sink is closed",
    "RedisListSink": lambda n: f"RedisListSink delivered none of {n} event(s): the sink is closed",
    "RedisStreamsSink": lambda n: (
        f"RedisStreamsSink delivered none of {n} event(s): the sink is closed"
    ),
}
"""Each engine adopter's closed message, fda134a's text (``SocketTransport`` counts the batch, as
SPEC-074 made it). ``NATSSink`` keeps its own closed state (SPEC-061) and answers the same way."""

ENGINE_CLOSING: dict[str, Callable[[int], str]] = {
    **ENGINE_CLOSED,
    "NATSSink": lambda n: f"NATSSink published none of {n} event(s): the sink is closing",
}
"""While a close is in flight: the closed message, except ``NATSSink``'s own closing one."""

EMIT_ONLY_CLOSED: dict[str, Callable[[int], str]] = {
    "FileSink": lambda n: f"FileSink wrote none of {n} event(s): the sink is closed",
    "RotatingFileSink": lambda n: f"RotatingFileSink wrote none of {n} event(s): the sink is closed",
    "SQLiteSink": lambda n: f"SQLiteSink inserted none of {n} event(s): the sink is closed",
    "KafkaSink": lambda n: f"KafkaSink produced none of {n} message(s): the sink is closed",
}
"""Each emit-only adopter's 1.x refusal, raised from ``emit``."""


class Built:
    """One engine sink under test: the object whose ``deliver`` refuses, and what to wrap."""

    def __init__(self, name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        self.name = name
        if name == "NATSSink":
            published: list[bytes] = []

            class Client:
                async def publish(self, subject: str, payload: bytes) -> None:
                    published.append(payload)

                async def close(self) -> None:
                    pass

            self.target: Any = NATSSink("logs", client=Client(), publish_timeout=1.0)
            self.outer: Any = self.target
            self.calls: Callable[[], int] = lambda: len(published)
            return
        rig = sink_rigs.RIGS["SyslogSink" if name == "SocketTransport" else name](
            monkeypatch, tmp_path
        )
        self.rig = rig
        self.outer = rig.sink
        self.target = rig.sink._socket if name == "SocketTransport" else rig.sink
        self.calls = rig.calls

    def counters(self) -> tuple[int, int, SinkLosses]:
        """``failed_events``, ``.failed`` and ``losses()`` of the sink that keeps the counters."""
        counters = self.target.log_foundry_counters
        return counters.failed_events, int(getattr(self.target, "failed", 0)), self.target.losses()


class RefuseSpy:
    """Records every error a round hands to ``RunContext.refuse``, then delegates."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.errors: list[Exception] = []
        original = _engine.RunContext.refuse
        spy = self

        def refuse(context: _engine.RunContext, error: Exception) -> None:
            spy.errors.append(error)
            original(context, error)

        monkeypatch.setattr(_engine.RunContext, "refuse", refuse)


class DeliverCount:
    """Counts the calls a class's ``deliver`` receives."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, cls: type) -> None:
        self.calls = 0
        original = cls.deliver  # type: ignore[attr-defined]
        counter = self

        def deliver(sink: Any, batch: list[dict[str, object]]) -> Any:
            counter.calls += 1
            return original(sink, batch)

        monkeypatch.setattr(cls, "deliver", deliver)


def _batch(n: int = 2) -> list[dict[str, object]]:
    return [sink_rigs.ok(f"e{i}") for i in range(n)]


def _hold_a_section(built: Built) -> tuple[threading.Thread, threading.Event]:
    """Puts a close in flight behind a section that holds the transport; returns it and its gate.

    A ``SERIAL`` guard's section is the holder of its lock, so the test takes that lock, through
    the 1.x name, on another thread. A ``CLOSE_ONLY`` guard's section is entered for real, and its
    close then waits at most ``CLOSE_OVERLAP_WAIT``, inside which the test acts. ``NATSSink``'s
    close sets its own closing flag before it takes its own lock.
    """
    target = built.target
    holding, leave = threading.Event(), threading.Event()
    guard = getattr(target, "_guard", None)

    def hold() -> None:
        if guard is not None and guard._mode.name == "CLOSE_ONLY":
            with guard.enter():
                holding.set()
                leave.wait(LIMIT)
            return
        with target._lock:
            holding.set()
            leave.wait(LIMIT)

    threading.Thread(target=hold, daemon=True).start()
    assert holding.wait(LIMIT)
    closer = threading.Thread(target=target.close, daemon=True)
    closer.start()

    def closing() -> bool:
        return bool(target._closing) if guard is None else bool(guard.closed)

    began = time.monotonic()
    while not closing():
        assert time.monotonic() - began < LIMIT, "the close never began"
        time.sleep(0.001)
    return closer, leave


@pytest.mark.parametrize("state", ["closed", "closing"])
@pytest.mark.parametrize("name", sorted(ENGINE_CLOSED))
def test_a_closed_or_closing_engine_sink_refuses_through_its_run_context(
    name: str, state: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """AC-1: ``deliver`` refuses with the sink's own message, ``REJECTED``, and no driver call."""
    built = Built(name, monkeypatch, tmp_path)
    spy = RefuseSpy(monkeypatch)
    closer: threading.Thread | None = None
    leave: threading.Event | None = None
    if state == "closed":
        built.target.close()
    else:
        closer, leave = _hold_a_section(built)
    before = built.calls()
    try:
        verdict = built.target.deliver(_batch(3))
    finally:
        if leave is not None and closer is not None:
            leave.set()
            closer.join(LIMIT)
    expected = (ENGINE_CLOSED if state == "closed" else ENGINE_CLOSING)[name](3)
    assert isinstance(verdict, Verdict)
    assert verdict.outcomes == (Outcome.REJECTED,) * 3
    assert len(spy.errors) == 1
    assert type(spy.errors[0]) is SinkDeliveryError and str(spy.errors[0]) == expected
    assert built.calls() == before, f"{name} reached its driver while {state}"


@pytest.mark.parametrize("path", ["worker", "orphan", "emit"])
@pytest.mark.parametrize("name", sorted(ENGINE_CLOSED))
def test_a_closed_engine_sink_costs_one_deliver_and_moves_no_counter(
    name: str,
    path: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """AC-2: one ``deliver``; the catcher counts the batch; no sink counter moves; no line."""
    built = Built(name, monkeypatch, tmp_path)
    spy = RefuseSpy(monkeypatch)
    count = DeliverCount(monkeypatch, type(built.target))
    built.target.close()
    before_counters = built.counters()
    before_calls = built.calls()
    if path == "worker":
        worker = Worker(built.outer, batch_size=1000, flush_interval=60.0)
        install_worker(worker)
        worker.submit(_batch(1))
        assert not worker.flush(timeout=LIMIT), "a refused batch reported a flush success"
        assert log_foundry.health().failed_batches == 1
    elif path == "orphan":
        log_foundry.configure(service="t", version="0", env="test", sink=built.outer)
        log_foundry.info("after close")
        assert log_foundry.health().orphan_lost == 1
    else:
        outer = built.outer
        with pytest.raises(SinkDeliveryError) as raised:
            if name == "SocketTransport":
                outer.emit(_batch(1))
            else:
                emit_via_deliver(outer, _batch(1))
        assert spy.errors and raised.value is spy.errors[-1], "the 1.x error was not raised by identity"
        assert str(raised.value) == ENGINE_CLOSED[name](1)
    assert count.calls == 1, f"{name}: {count.calls} deliver call(s) for one refused batch"
    assert built.counters() == before_counters, "a refusal moved a sink counter"
    assert built.calls() == before_calls
    assert "SinkDeliveryError" not in capsys.readouterr().err


def test_a_closed_socket_mode_logstash_raises_the_transports_error_by_identity(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """AC-2's twin: socket-mode ``LogstashSink`` refuses through the ``SocketTransport`` it holds."""
    rig = sink_rigs.RIGS["LogstashSink"](monkeypatch, tmp_path)
    spy = RefuseSpy(monkeypatch)
    rig.sink.close()
    before = rig.calls()
    with pytest.raises(SinkDeliveryError) as raised:
        rig.sink.emit(_batch(1))
    assert spy.errors and raised.value is spy.errors[-1]
    assert str(raised.value) == ENGINE_CLOSED["SocketTransport"](1)
    assert rig.calls() == before
    assert rig.sink._socket.losses() == SinkLosses(dropped=0, failed=0)


@pytest.mark.parametrize("name", sorted(EMIT_ONLY_CLOSED))
def test_an_emit_only_adopter_raises_its_1x_refusal(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """AC-3: exactly ``SinkDeliveryError`` with today's message; no count, no driver call."""
    rig = sink_rigs.RIGS[name](monkeypatch, tmp_path)
    rig.sink.close()
    before_calls = rig.calls()
    before_losses = rig.sink.losses()
    with pytest.raises(SinkDeliveryError) as raised:
        rig.sink.emit(_batch(2))
    assert type(raised.value) is SinkDeliveryError
    assert str(raised.value) == EMIT_ONLY_CLOSED[name](2)
    assert rig.calls() == before_calls
    assert rig.sink.losses() == before_losses


BETWEEN_ROUNDS = ("ClickHouseSink", "RabbitMQSink", "SyslogSink")


def _two_round_sink(name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Any:
    """Builds a per-event sink with one resend allowed, whose second driver call fails once."""
    rig = sink_rigs.RIGS[name](monkeypatch, tmp_path)
    calls = {"n": 0}
    if name == "ClickHouseSink":
        from log_foundry.sinks.clickhouse import ClickHouseSink

        double: Any = sink_rigs._ClickHouse(rig.state)
        sink: Any = ClickHouseSink("t", client=double, max_retries=1, chunk_size=1)
        cls, method = sink_rigs._ClickHouse, "insert"
    elif name == "RabbitMQSink":
        from log_foundry.sinks.rabbitmq import RabbitMQSink

        sink = RabbitMQSink(
            exchange="x", routing_key="k", connection=sink_rigs._Connection(rig.state),
            max_retries=1,
        )
        cls, method = sink_rigs._Channel, "basic_publish"
    else:
        from log_foundry.sinks.syslog import SyslogSink

        sink = SyslogSink("loghost", 514, transport="udp", max_retries=1)
        cls, method = sink_rigs._Socket, "sendto"
    original = getattr(cls, method)

    def second_fails(self: Any, *args: Any, **kwargs: Any) -> Any:
        calls["n"] += 1
        if calls["n"] == 2:
            rig.state.log.append(("refused", None))
            raise ConnectionRefusedError(111, "Connection refused")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(cls, method, second_fails)
    return sink, rig, calls


@pytest.mark.parametrize("name", BETWEEN_ROUNDS)
def test_a_sink_closed_between_two_rounds_settles_and_counts_the_refused_round_once(
    name: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """AC-4: round one lands one event; the close lands in the backoff; round two calls nothing.

    The run settles, so no catcher sees it: the refused round's event is counted once in
    ``failed_events`` with one line, and ``flush()`` is truthy.
    """
    sink, rig, calls = _two_round_sink(name, monkeypatch, tmp_path)

    def close_in_the_backoff(delay: float, stop: Any) -> Any:
        sink.close()
        return stop

    monkeypatch.setattr(_engine, "_wait", close_in_the_backoff)
    counters = sink.log_foundry_counters
    worker = Worker(sink, batch_size=1000, flush_interval=60.0, max_retries=0)
    install_worker(worker)
    worker.submit(_batch(2))
    assert worker.flush(timeout=LIMIT), "a run that landed an event did not settle"
    assert calls["n"] == 2, "the round after the close made a driver call"
    assert rig.delivered() == ["e0"], "an event was offered twice or the wrong one landed"
    assert counters.failed_events == 1
    lines = [line for line in capsys.readouterr().err.splitlines() if "lost" in line]
    assert len(lines) == 1, lines


@pytest.mark.parametrize("name", sorted(ENGINE_CLOSED))
def test_the_worker_and_orphan_paths_leave_one_ledger(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """AC-7: one event to a closed engine sink leaves the same ``SinkLosses`` on either path."""
    ledgers: list[SinkLosses] = []
    for path in ("worker", "orphan"):
        (tmp_path / path).mkdir()
        built = Built(name, monkeypatch, tmp_path / path)
        built.target.close()
        if path == "worker":
            worker = Worker(built.outer, batch_size=1000, flush_interval=60.0)
            install_worker(worker)
            worker.submit(_batch(1))
            worker.flush(timeout=LIMIT)
        else:
            log_foundry.configure(service="t", version="0", env="test", sink=built.outer)
            log_foundry.info("after close")
        ledgers.append(built.target.losses())
        log_foundry.shutdown(timeout=LIMIT)
    assert ledgers[0] == ledgers[1] == SinkLosses(dropped=0, failed=0)


def test_a_closed_kafka_flush_raises_its_message(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """AC-3: ``KafkaSink.flush()`` after ``close()`` raises its own 1.x refusal."""
    rig = sink_rigs.RIGS["KafkaSink"](monkeypatch, tmp_path)
    rig.sink.close()
    with pytest.raises(SinkDeliveryError) as raised:
        rig.sink.flush()
    assert type(raised.value) is SinkDeliveryError
    assert str(raised.value) == "KafkaSink cannot flush: the sink is closed"


ADOPTER_RIGS = (
    "AzureEventHubsSink",
    "ClickHouseSink",
    "FileSink",
    "KafkaSink",
    "LogstashSink",
    "MongoDBSink",
    "PostgresSink",
    "RabbitMQSink",
    "RedisListSink",
    "RedisStreamsSink",
    "RotatingFileSink",
    "SQLiteSink",
    "SyslogSink",
)
"""Every adopter by the sink a caller holds: ``SocketTransport`` through its two holders."""


@pytest.mark.parametrize("name", ADOPTER_RIGS)
def test_an_empty_batch_returns_before_the_guard(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """AC-5: ``emit([])`` returns ``None``, open and closed, and reaches no driver."""
    rig = sink_rigs.RIGS[name](monkeypatch, tmp_path)
    assert rig.sink.emit([]) is None
    rig.sink.close()
    before = rig.calls()
    assert rig.sink.emit([]) is None
    assert rig.calls() == before


PAST_THE_BOUND = {
    "MongoDBSink": ("pymongo.errors", "InvalidOperation", "_Collection", "insert_many"),
    "RedisListSink": ("redis.exceptions", "ConnectionError", "_Pipeline", "execute"),
}


@pytest.mark.parametrize("name", sorted(PAST_THE_BOUND))
def test_a_round_past_the_bound_fails_on_the_released_client(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """SPEC-076 FR-003 AC-5: a round still inside after the close's bound meets a closed client.

    The drain thread's round parks in the client; a close on the test's thread waits its bound,
    then releases. The parked call then raises what that driver raises on a closed client. Nothing
    raises into the closing thread, no later round calls the client, and the two drivers differ
    only in what is counted: ``pymongo``'s ``InvalidOperation`` is ``REJECTED`` and counted in
    ``.failed``; ``redis-py``'s ``ConnectionError`` is ``RETRYABLE``, the next round is refused,
    and a failed run commits nothing for it.
    """
    import driver_standins

    module_key, error_name, double_name, method = PAST_THE_BOUND[name]
    errors = driver_standins.install(monkeypatch, module_key)
    double = getattr(sink_rigs, double_name)
    inside, let_go = threading.Event(), threading.Event()
    calls = {"n": 0}

    def parked(self: Any, *args: Any, **kwargs: Any) -> Any:
        calls["n"] += 1
        inside.set()
        assert let_go.wait(LIMIT)
        raise getattr(errors, error_name)("the client is closed")

    monkeypatch.setattr(double, method, parked)
    rig = sink_rigs.RIGS[name](monkeypatch, tmp_path)
    if name == "MongoDBSink":
        from log_foundry.sinks.mongodb import MongoDBSink

        sink: Any = MongoDBSink(
            client=sink_rigs._MongoClient(rig.state), database="d", collection="c", max_retries=1
        )
    else:
        from log_foundry.sinks.redis import RedisListSink

        sink = RedisListSink("k", client=sink_rigs._Redis(rig.state), max_retries=1)
    worker = Worker(sink, batch_size=1000, flush_interval=60.0, max_retries=0)
    install_worker(worker)
    worker.submit(_batch(3))
    flushed: list[bool] = []
    flusher = threading.Thread(target=lambda: flushed.append(worker.flush(timeout=LIMIT)))
    flusher.start()
    assert inside.wait(LIMIT), "the drain thread never reached the client"
    sink.close()
    let_go.set()
    flusher.join(LIMIT * 2)
    assert len(flushed) == 1 and not flushed[0], "a run that failed reported a flush success"
    assert calls["n"] == 1, "a round after the release reached the client"
    assert log_foundry.health().failed_batches == 1
    assert sink.failed == (3 if name == "MongoDBSink" else 0)


def _opener_built(make: Callable[[Any], Any]) -> Callable[[pytest.MonkeyPatch, Path], Any]:
    """A builder for an HTTP-family sink over ``test_sinks_http.FakeOpener``."""

    def build(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[Any, Callable[[], int]]:
        from test_sinks_http import FakeOpener

        opener = FakeOpener()
        return make(opener), lambda: len(opener.calls)

    return build


def _rig_built(name: str) -> Callable[[pytest.MonkeyPatch, Path], Any]:
    def build(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[Any, Callable[[], int]]:
        rig = sink_rigs.RIGS[name](monkeypatch, tmp_path)
        return rig.sink, rig.calls

    return build


def _inner_built(wrap: Callable[[Any], Any]) -> Callable[[pytest.MonkeyPatch, Path], Any]:
    def build(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[Any, Callable[[], int]]:
        from log_foundry.sinks.memory import MemorySink

        inner = MemorySink()
        return wrap(inner), lambda: len(inner.events)

    return build


def _http(cls_name: str, *args: Any, **kwargs: Any) -> Callable[[pytest.MonkeyPatch, Path], Any]:
    def make(opener: Any) -> Any:
        import importlib

        module = {
            "HTTPSink": "http", "DatadogSink": "datadog", "SplunkHECSink": "splunk",
            "NewRelicSink": "newrelic", "HoneycombSink": "honeycomb", "LokiSink": "loki",
            "ElasticsearchSink": "elasticsearch", "OpenSearchSink": "elasticsearch",
            "OTLPSink": "otlp", "SentrySink": "sentry",
        }[cls_name]
        cls = getattr(importlib.import_module(f"log_foundry.sinks.{module}"), cls_name)
        return cls(*args, opener=opener, **kwargs)

    return _opener_built(make)


def _callback(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[Any, Callable[[], int]]:
    from log_foundry.sinks.callback import CallbackSink

    batches: list[object] = []
    return CallbackSink(batches.append), lambda: len(batches)


def _logging(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[Any, Callable[[], int]]:
    import logging

    from log_foundry.sinks.logging_sink import LoggingSink

    records: list[logging.LogRecord] = []

    class Keep(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    logger = logging.getLogger(f"spec076.{tmp_path.name}")
    logger.propagate = False
    logger.addHandler(Keep())
    logger.setLevel(logging.DEBUG)
    return LoggingSink(logger), lambda: len(records)


def _null(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[Any, Callable[[], int]]:
    from log_foundry.sinks.null import NullSink

    return NullSink(), lambda: 0


def _memory(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[Any, Callable[[], int]]:
    from log_foundry.sinks.memory import MemorySink

    sink = MemorySink()
    return sink, lambda: len(sink.events)


def _multi(inner: Any) -> Any:
    from log_foundry.sinks.multi import MultiSink

    return MultiSink(inner)


def _filtering(inner: Any) -> Any:
    from log_foundry.sinks.filtering import FilteringSink

    return FilteringSink(inner)


def _factory(inner: Any) -> Any:
    """``sink_factory``'s private wrapper, its sink built by one empty emit (SPEC-079 FR-006)."""
    from log_foundry.sinks._factory import _FactorySink

    wrapper = _FactorySink(lambda: inner)
    wrapper.emit([])
    return wrapper


def _transform(inner: Any) -> Any:
    from log_foundry.sinks.transform import TransformSink

    return TransformSink(inner, lambda event: event)


def _sentry(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[Any, Callable[[], int]]:
    """``SentrySink`` over an SDK that cannot deliver, so it sends through its HTTP fallback."""
    from log_foundry.sinks.sentry import SentrySink
    from test_sentry_backend import DSN, FakeModule, InactiveClient
    from test_sinks_http import FakeOpener

    opener = FakeOpener()
    sink = SentrySink(DSN, opener=opener, client=FakeModule(InactiveClient()))
    return sink, lambda: len(opener.calls)

ACCEPTING_BUILDERS: dict[str, Callable[[pytest.MonkeyPatch, Path], Any]] = {
    "callback.CallbackSink": _callback,
    "datadog.DatadogSink": _http("DatadogSink", "k", max_retries=0),
    "elasticsearch.ElasticsearchSink": _http("ElasticsearchSink", "http://x", index="i", max_retries=0),
    "elasticsearch.OpenSearchSink": _http("OpenSearchSink", "http://x", index="i", max_retries=0),
    "filtering.FilteringSink": _inner_built(_filtering),
    "firehose.FirehoseSink": _rig_built("FirehoseSink"),
    "honeycomb.HoneycombSink": _http("HoneycombSink", "k", "d", max_retries=0),
    "http.HTTPSink": _http("HTTPSink", "http://x", max_retries=0),
    "kinesis.KinesisSink": _rig_built("KinesisSink"),
    "logging_sink.LoggingSink": _logging,
    "loki.LokiSink": _http("LokiSink", "http://x", max_retries=0),
    "memory.MemorySink": _memory,
    "multi.MultiSink": _inner_built(_multi),
    "_factory._FactorySink": _inner_built(_factory),
    "newrelic.NewRelicSink": _http("NewRelicSink", "k", max_retries=0),
    "null.NullSink": _null,
    "otlp.OTLPSink": _http("OTLPSink", max_retries=0),
    "sentry.SentrySink": _sentry,
    "sns.SNSSink": _rig_built("SNSSink"),
    "splunk.SplunkHECSink": _http("SplunkHECSink", "http://x", "t", max_retries=0),
    "sqs.SQSSink": _rig_built("SQSSink"),
    "stdout.StdoutSink": _rig_built("StdoutSink"),
    "stdout.StderrSink": _rig_built("StderrSink"),
    "transform.TransformSink": _inner_built(_transform),
}
"""A builder per sink that records it accepts after close, each over a double it already has.

Which sinks belong here is derived (``_accepting``), never read from this map: a key the
derivation does not produce, or a derived sink with no key, fails the test below."""


def _accepting() -> list[tuple[str, Any]]:
    """Every sink class whose recorded decision is that it accepts after ``close()``."""
    from test_sink_concurrency import (
        ACCEPTS_AFTER_CLOSE,
        _decision_doc,
        _sink_classes_with_an_emit,
    )

    return [
        (f"{stem}.{cls.name}", cls)
        for stem, cls in _sink_classes_with_an_emit()
        if ACCEPTS_AFTER_CLOSE in _decision_doc(cls)
    ]


def test_the_accepting_builders_cover_exactly_the_derived_population() -> None:
    """FR-005 AC-8: the builder map neither misses a derived sink nor names a stale one."""
    derived = {name for name, _ in _accepting()}
    assert derived == set(ACCEPTING_BUILDERS), (
        f"missing: {sorted(derived - set(ACCEPTING_BUILDERS))}; "
        f"stale: {sorted(set(ACCEPTING_BUILDERS) - derived)}"
    )
    assert len(derived) >= 20, f"the accepting roster collapsed to {len(derived)}"


@pytest.mark.parametrize("qualified", sorted(ACCEPTING_BUILDERS))
def test_a_sink_that_releases_nothing_still_delivers_after_close(
    qualified: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """FR-005 AC-8: it builds no guard, and a batch after ``close()`` still reaches its double."""
    from test_sink_concurrency import _guard_mode

    cls = dict(_accepting())[qualified]
    helpers = {m.name: m for m in cls.body if isinstance(m, ast.FunctionDef | ast.AsyncFunctionDef)}
    assert _guard_mode(helpers) is None and "ClosedGuard" not in ast.unparse(cls)
    sink, delivered = ACCEPTING_BUILDERS[qualified](monkeypatch, tmp_path)
    sink.close()
    before = delivered()
    sink.emit([{**sink_rigs.ok("after"), "level": "ERROR"}])
    if qualified != "null.NullSink":
        assert delivered() > before, f"{qualified} did not deliver after close()"


ALIAS_RIGS = {
    "AzureEventHubsSink": ("AzureEventHubsSink", "_lock"),
    "ClickHouseSink": ("ClickHouseSink", "_lock"),
    "FileSink": ("FileSink", "_lock"),
    "PostgresSink": ("PostgresSink", "_lock"),
    "RabbitMQSink": ("RabbitMQSink", "_lock"),
    "RotatingFileSink": ("RotatingFileSink", "_lock"),
    "SQLiteSink": ("SQLiteSink", "_lock"),
    "SocketTransport": ("SyslogSink", "_lock"),
    "KafkaSink": ("KafkaSink", "_close_lock"),
    "MongoDBSink": ("MongoDBSink", "_close_lock"),
    "_RedisSink": ("RedisListSink", "_close_lock"),
}
"""Each adopter, the rig that builds it, and its 1.x lock name at fda134a."""


def _adopter(name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[Any, Any]:
    rig = sink_rigs.RIGS[ALIAS_RIGS[name][0]](monkeypatch, tmp_path)
    return (rig.sink._socket if name == "SocketTransport" else rig.sink), rig


@pytest.mark.parametrize("name", sorted(ALIAS_RIGS))
def test_each_adopter_keeps_its_1x_names_read_only(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """FR-005 AC-7: ``_closed`` and the 1.x lock name read through, and refuse assignment."""
    sink, _ = _adopter(name, monkeypatch, tmp_path)
    lock_name = ALIAS_RIGS[name][1]

    class Legacy(type(sink)):  # type: ignore[misc]
        def closed_as_1x_read_it(self) -> bool:
            return bool(self._closed)

    sink.__class__ = Legacy
    assert sink.closed_as_1x_read_it() is False
    assert getattr(sink, lock_name) is sink._guard._lock
    for alias in ("_closed", lock_name):
        with pytest.raises(AttributeError):
            setattr(sink, alias, None)
    sink.close()
    assert sink.closed_as_1x_read_it() is True


SERIAL_ALIAS = sorted(n for n, (_, lock) in ALIAS_RIGS.items() if lock == "_lock")


@pytest.mark.parametrize("name", SERIAL_ALIAS)
def test_a_subclass_holding_the_1x_lock_holds_off_a_section(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """FR-005 AC-7: ``with self._lock:`` in a ``SERIAL`` subclass serialises with a section."""
    sink, rig = _adopter(name, monkeypatch, tmp_path)
    holding, leave = threading.Event(), threading.Event()

    class Legacy(type(sink)):  # type: ignore[misc]
        def hold(self) -> None:
            with self._lock:
                holding.set()
                leave.wait(LIMIT)

    sink.__class__ = Legacy
    holder = threading.Thread(target=sink.hold, daemon=True)
    holder.start()
    assert holding.wait(LIMIT)
    before = rig.calls()
    done = threading.Event()

    def deliver() -> None:
        if name == "SocketTransport":
            rig.sink.emit(_batch(1))
        else:
            sink.emit(_batch(1))
        done.set()

    threading.Thread(target=deliver, daemon=True).start()
    try:
        assert not done.wait(0.2), "a section ran while a subclass held the 1x lock"
        assert rig.calls() == before
    finally:
        leave.set()
    assert done.wait(LIMIT), "the section never ran once the subclass let go"
    assert rig.calls() > before


def _derived_adopters() -> tuple[dict[str, tuple[str, str, bool]], dict[str, ast.ClassDef]]:
    """Every class building a ``ClosedGuard``, from the tree: mode, module, defines ``deliver``.

    Returns the parsed classes too, so a caller can find who holds or subclasses one.
    """
    from test_sink_concurrency import _sink_modules, guard_builder_findings

    trees = {path.name: ast.parse(path.read_text(encoding="utf-8")) for path in _sink_modules()}
    modes, _ = guard_builder_findings(trees)
    classes: dict[str, ast.ClassDef] = {}
    adopters: dict[str, tuple[str, str, bool]] = {}
    for module, tree in trees.items():
        for node in tree.body:
            if not isinstance(node, ast.ClassDef):
                continue
            classes[node.name] = node
            if node.name in modes:
                defines = {m.name for m in node.body if isinstance(m, ast.FunctionDef)}
                adopters[node.name] = (modes[node.name], module, "deliver" in defines)
    return adopters, classes


def _held_as(name: str, classes: dict[str, ast.ClassDef]) -> set[str]:
    """The sinks a caller holds for an adopter: itself, its subclasses if private, or its holders.

    A private base (``_RedisSink``) is held through the classes that subclass it; a transport
    with no ``emit`` (``SocketTransport``) through the classes that build it.
    """
    node = classes[name]
    if name.startswith("_"):
        return {
            other.name
            for other in classes.values()
            if any(isinstance(b, ast.Name) and b.id == name for b in other.bases)
        }
    if not any(isinstance(m, ast.FunctionDef) and m.name == "emit" for m in node.body):
        return {
            other.name
            for other in classes.values()
            if any(
                isinstance(call, ast.Call)
                and isinstance(call.func, ast.Name)
                and call.func.id == name
                for call in ast.walk(other)
            )
        }
    return {name}


def test_every_hand_listed_adopter_population_is_the_derived_one() -> None:
    """Frame-1 review SF-4: no per-adopter list in these tests can shrink, or grow, silently.

    Each list is held equal to what the tree says: the classes building a ``ClosedGuard`` (and
    ``NATSSink`` where a list covers engine sinks with closed state of their own), each mapped
    to the sink a caller holds for it — ``_RedisSink`` through both Redis sinks,
    ``SocketTransport`` through ``SyslogSink`` and socket-mode ``LogstashSink``.
    """
    from test_sink_concurrency import CLOSED_MESSAGES, SERIAL_DRIVERS, guarded_section_findings

    adopters, classes = _derived_adopters()
    assert len(adopters) >= 11, f"the adopter scan collapsed to {sorted(adopters)}"
    engine = {name for name, (_, _, deliver) in adopters.items() if deliver}
    emit_only = set(adopters) - engine

    def subclassed(names: set[str]) -> set[str]:
        return {held for n in names for held in (_held_as(n, classes) if n.startswith("_") else {n})}

    def held(names: set[str]) -> set[str]:
        return {h for n in names for h in _held_as(n, classes)}

    assert set(ENGINE_CLOSED) == subclassed(engine) | {"NATSSink"}
    assert set(EMIT_ONLY_CLOSED) == subclassed(emit_only)
    assert set(ADOPTER_RIGS) == held(set(adopters))
    assert set(ALIAS_RIGS) == set(adopters)
    for name, (_, lock) in ALIAS_RIGS.items():
        expected = "_lock" if adopters[name][0] == "SERIAL" else "_close_lock"
        assert lock == expected, (name, lock)
        assert ALIAS_RIGS[name][0] in held({name}), (name, ALIAS_RIGS[name][0])
    serial = {name for name, (mode, _, _) in adopters.items() if mode == "SERIAL"}
    transports = {n for n in serial if _held_as(n, classes) != {n}}
    assert set(SERIAL_DRIVERS) - held(transports) == serial - transports
    assert all(set(SERIAL_DRIVERS) & held({t}) for t in transports)
    modules = {module for _, module, _ in adopters.values()}
    assert set(CLOSED_MESSAGES) == modules
    import log_foundry

    root = Path(log_foundry.__file__).parent / "sinks"
    for module, openings in CLOSED_MESSAGES.items():
        sections, _ = guarded_section_findings((root / module).read_text(encoding="utf-8"))
        assert len(openings) == sections, (module, openings, sections)
