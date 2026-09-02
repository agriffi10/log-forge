"""SPEC-027 — bounded, interruptible retry: the shared wait and the `Retry-After` clamp.

Every test here is in-process. Timing assertions use generous bounds and assert the *shape*
(returned early / did not return early), never a precise duration.
"""

from __future__ import annotations

import json
import math
import threading
import time

import pytest

import log_foundry.sinks._socket as socket_mod
from log_foundry import _lifecycle
from log_foundry.sinks._retry import clamp_server_delay, usable_timeout, wait
from log_foundry.sinks.http import DEFAULT_MAX_RETRY_AFTER, HTTPSink
from log_foundry.worker import Worker
from test_sinks_http import FakeOpener, FakeResponse

# --- FR-001: Retry-After is bounded and sign-checked ---------------------------------------


def test_a_delay_above_the_ceiling_is_clamped() -> None:
    assert clamp_server_delay(86400.0, 30.0) == 30.0


def test_a_delay_at_or_below_the_ceiling_is_honoured_exactly() -> None:
    assert clamp_server_delay(30.0, 30.0) == 30.0
    assert clamp_server_delay(2.5, 30.0) == 2.5


def test_absent_means_fall_back() -> None:
    assert clamp_server_delay(None, 30.0) is None


@pytest.mark.parametrize("value", [-1.0, -0.0, 0.0, math.nan, math.inf, -math.inf])
def test_an_unusable_delay_falls_back_to_exponential_backoff(value: float) -> None:
    """``NaN`` is why the test is ``not (value > 0)`` and not ``value <= 0``."""
    assert clamp_server_delay(value, 30.0) is None


def test_the_ceiling_is_the_callers() -> None:
    assert clamp_server_delay(120.0, 300.0) == 120.0
    assert clamp_server_delay(120.0, 5.0) == 5.0


def test_a_hostile_retry_after_never_reaches_sleep(monkeypatch) -> None:
    """Unbounded, ``Retry-After: 86400`` stalled all logging for a day."""
    slept: list[float] = []
    monkeypatch.setattr("log_foundry.sinks._retry.time.sleep", slept.append)

    sink = HTTPSink(
        "http://x",
        max_retries=1,
        max_retry_after=30.0,
        opener=FakeOpener([FakeResponse(429, b"", {"Retry-After": "86400"})]),
    )
    with pytest.raises(Exception):  # noqa: B017 — SinkDeliveryError; the raise is SPEC-026's
        sink.emit([{"a": 1}])

    assert slept == [30.0]


def test_a_negative_retry_after_does_not_raise_into_the_caller(monkeypatch) -> None:
    """``time.sleep(-1)`` raises ``ValueError``; on the orphan path that reached the caller."""
    slept: list[float] = []
    monkeypatch.setattr("log_foundry.sinks._retry.time.sleep", slept.append)

    sink = HTTPSink(
        "http://x",
        max_retries=1,
        opener=FakeOpener([FakeResponse(503, b"", {"Retry-After": "-5"})]),
    )
    with pytest.raises(Exception):  # noqa: B017 — SinkDeliveryError, never ValueError
        sink.emit([{"a": 1}])

    assert slept == [0.1], "fell back to the sink's own backoff for attempt 0"


def test_an_http_date_retry_after_still_falls_back(monkeypatch) -> None:
    slept: list[float] = []
    monkeypatch.setattr("log_foundry.sinks._retry.time.sleep", slept.append)

    sink = HTTPSink(
        "http://x",
        max_retries=1,
        opener=FakeOpener([FakeResponse(503, b"", {"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"})]),
    )
    with pytest.raises(Exception):  # noqa: B017
        sink.emit([{"a": 1}])

    assert slept == [0.1]


def test_max_retry_after_is_a_keyword_with_a_documented_default() -> None:
    assert HTTPSink("http://x").max_retry_after == DEFAULT_MAX_RETRY_AFTER
    assert HTTPSink("http://x", max_retry_after=5.0).max_retry_after == 5.0


def test_every_platform_subclass_inherits_the_ceiling() -> None:
    from log_foundry.sinks.datadog import DatadogSink
    from log_foundry.sinks.elasticsearch import ElasticsearchSink
    from log_foundry.sinks.honeycomb import HoneycombSink
    from log_foundry.sinks.loki import LokiSink
    from log_foundry.sinks.newrelic import NewRelicSink
    from log_foundry.sinks.splunk import SplunkHECSink

    built = [
        DatadogSink("k", max_retry_after=7.0),
        SplunkHECSink("http://x", "t", max_retry_after=7.0),
        NewRelicSink("k", max_retry_after=7.0),
        HoneycombSink("k", "d", max_retry_after=7.0),
        LokiSink("http://x", max_retry_after=7.0),
        ElasticsearchSink("http://x", index="i", max_retry_after=7.0),
    ]
    for sink in built:
        assert sink.max_retry_after == 7.0, type(sink).__name__


# --- FR-002: a wait is interruptible --------------------------------------------------------


def _run_bounded(fn, bound: float = 2.0) -> bool:
    """Run ``fn`` on a thread; ``True`` if it finished inside ``bound``.

    Deliberately not a bare call with an elapsed-time assertion: against an *uninterruptible*
    wait that would block the test for the full delay, which reads in CI as a hung run rather
    than a regression. A join with a timeout fails in ``bound`` seconds either way.
    """
    thread = threading.Thread(target=fn, daemon=True)
    thread.start()
    thread.join(bound)
    return not thread.is_alive()


def test_wait_returns_early_when_the_signal_is_set() -> None:
    stop = threading.Event()
    stop.set()
    assert _run_bounded(lambda: wait(30.0, stop)), "a set signal must not be waited out"


def test_wait_waits_when_the_signal_is_clear() -> None:
    stop = threading.Event()
    started = time.monotonic()
    wait(0.05, stop)
    assert time.monotonic() - started >= 0.04


def test_wait_with_no_signal_sleeps(monkeypatch) -> None:
    """A sink used standalone backs off exactly as it did before this spec."""
    slept: list[float] = []
    monkeypatch.setattr("log_foundry.sinks._retry.time.sleep", slept.append)
    wait(2.5, None)
    assert slept == [2.5]


@pytest.mark.parametrize("delay", [0.0, -1.0, math.nan, math.inf])
def test_wait_ignores_an_unusable_delay(delay: float, monkeypatch) -> None:
    """Total: a backoff computed from arithmetic must not become an exception on the drain thread."""
    monkeypatch.setattr(
        "log_foundry.sinks._retry.time.sleep",
        lambda _s: pytest.fail("an unusable delay must not reach sleep"),
    )
    wait(delay, None)
    wait(delay, threading.Event())


def test_the_worker_hands_the_sink_its_stop_signal() -> None:
    sink = HTTPSink("http://x", opener=FakeOpener())
    worker = Worker(sink, batch_size=1)
    try:
        assert sink.log_foundry_stop_signal is worker._stop
    finally:
        worker.shutdown()


def test_a_sink_with_no_stop_signal_attribute_is_left_alone() -> None:
    class Plain:
        def emit(self, batch: list[dict[str, object]]) -> None: ...
        def close(self) -> None: ...

    sink = Plain()
    worker = Worker(sink, batch_size=1)
    try:
        assert not hasattr(sink, "log_foundry_stop_signal")
    finally:
        worker.shutdown()


def test_a_sink_that_refuses_the_signal_does_not_stop_the_worker(capsys) -> None:
    """Losing interruptibility is a degradation; failing to start the worker is an outage."""

    class Stubborn:
        log_foundry_stop_signal = None

        def __setattr__(self, name: str, value: object) -> None:
            raise AttributeError("read-only")

        def emit(self, batch: list[dict[str, object]]) -> None: ...
        def close(self) -> None: ...

    worker = Worker(Stubborn(), batch_size=1)
    try:
        assert worker._thread.is_alive()
    finally:
        worker.shutdown()
    assert "handing the sink its stop signal" in capsys.readouterr().err


def test_the_socket_transport_backoff_is_interruptible(monkeypatch) -> None:
    from test_sinks_syslog import RefusingSocket

    monkeypatch.setattr(socket_mod, "_make_udp", lambda host: RefusingSocket())
    transport = socket_mod.SocketTransport("h", 1, transport="udp", max_retries=3)
    transport.log_foundry_stop_signal = threading.Event()
    transport.log_foundry_stop_signal.set()

    outcome: list[str] = []

    def send() -> None:
        try:
            transport.send_all([b"x"])
        except Exception as err:
            outcome.append(type(err).__name__)

    assert _run_bounded(send), "4 attempts of uninterruptible backoff would take ~1.5s"
    assert outcome == ["SinkDeliveryError"]
    assert transport.failed == 1, "the message is still abandoned and counted"


def test_shutdown_is_not_held_by_a_sink_mid_backoff() -> None:
    """The measured symptom: shutdown() blocked 22s because a log endpoint asked it to."""
    sink = HTTPSink(
        "http://x",
        max_retries=3,
        max_retry_after=30.0,
        opener=FakeOpener([FakeResponse(503, b"", {"Retry-After": "30"})]),
    )
    worker = Worker(sink, batch_size=1, max_retries=0)
    worker.submit([{"a": 1}])
    time.sleep(0.2)  # let the drain thread reach the backoff

    assert _run_bounded(worker.shutdown, bound=5.0), "held by the sink's backoff"


# --- FR-002 (Phase 2): every retrying sink waits through the shared helper ------------------


def _modules_with_a_retry_loop() -> list[str]:
    """Every sinks module containing a ``for attempt in range(...)`` re-send loop.

    Derived, not listed: a hand-written roster is exactly how ``KinesisSink``, ``FirehoseSink``
    and ``SNSSink`` shipped with a re-send loop, no wait and no stop signal while two tests
    claimed to cover "every retrying sink". A new sink with a retry loop joins this set the day
    it is written.
    """
    import ast
    import pathlib as _pathlib

    import log_foundry

    found: list[str] = []
    for path in sorted((_pathlib.Path(log_foundry.__file__).parent / "sinks").glob("*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.For)
                and isinstance(node.target, ast.Name)
                and node.target.id == "attempt"
            ):
                found.append(path.stem)
                break
    return found


def test_every_module_with_a_retry_loop_waits_between_attempts() -> None:
    """A re-send loop with no wait is the SQSSink defect, in a sink nobody listed."""
    import ast
    import pathlib as _pathlib

    import log_foundry

    root = _pathlib.Path(log_foundry.__file__).parent / "sinks"
    missing: list[str] = []
    for name in _modules_with_a_retry_loop():
        source = (root / f"{name}.py").read_text()
        missing.extend(
            f"{name}.py:{node.lineno}"
            for node in ast.walk(ast.parse(source))
            if (
                isinstance(node, ast.For)
                and isinstance(node.target, ast.Name)
                and node.target.id == "attempt"
                and not any(
                    isinstance(inner, ast.Call)
                    and (
                        (isinstance(inner.func, ast.Name) and inner.func.id == "wait")
                        # ``HTTPSink`` reaches ``wait`` through its own helper, which is where
                        # the ``Retry-After`` clamp lives; the helper is checked by its own tests.
                        or (
                            isinstance(inner.func, ast.Attribute)
                            and inner.func.attr == "_sleep_backoff"
                        )
                    )
                    for inner in ast.walk(node)
                )
            )
        )
    assert missing == [], f"retry loops with no interruptible wait: {missing}"


def test_every_module_with_a_retry_loop_declares_a_stop_signal() -> None:
    import pathlib as _pathlib

    import log_foundry

    root = _pathlib.Path(log_foundry.__file__).parent / "sinks"
    missing = [
        name
        for name in _modules_with_a_retry_loop()
        if "self.log_foundry_stop_signal" not in (root / f"{name}.py").read_text()
    ]
    assert missing == [], f"no log_foundry_stop_signal: {missing}"


def test_no_sink_calls_time_sleep_directly() -> None:
    """A lint on the idiom: a new `time.sleep` in a retry loop is uninterruptible again."""
    import ast
    import pathlib

    import log_foundry

    root = pathlib.Path(log_foundry.__file__).parent / "sinks"
    offenders: list[str] = []
    for path in sorted(root.glob("*.py")):
        if path.name == "_retry.py":  # the one module allowed to sleep
            continue
        offenders.extend(
            f"{path.name}:{node.lineno}"
            for node in ast.walk(ast.parse(path.read_text()))
            if isinstance(node, ast.Call)
            and (
                # ``time.sleep(...)`` and ``from time import sleep; sleep(...)`` alike — the
                # second form would otherwise slip past a lint that only matches attributes.
                (isinstance(node.func, ast.Attribute) and node.func.attr == "sleep")
                or (isinstance(node.func, ast.Name) and node.func.id == "sleep")
            )
        )
    assert offenders == [], f"uninterruptible sleeps: {offenders}"


def _one_of_each_retrying_sink(monkeypatch) -> list[object]:
    """A constructed instance of every sink with a retry loop, with fake transports."""
    from log_foundry.sinks.clickhouse import ClickHouseSink
    from log_foundry.sinks.eventhubs import AzureEventHubsSink
    from log_foundry.sinks.firehose import FirehoseSink
    from log_foundry.sinks.kinesis import KinesisSink
    from log_foundry.sinks.mongodb import MongoDBSink
    from log_foundry.sinks.postgres import PostgresSink
    from log_foundry.sinks.rabbitmq import RabbitMQSink
    from log_foundry.sinks.redis import RedisStreamsSink
    from log_foundry.sinks.sns import SNSSink
    from log_foundry.sinks.sqs import SQSSink
    from log_foundry.sinks.syslog import SyslogSink
    from test_sinks_clickhouse import FakeClickHouse
    from test_sinks_eventhubs import FakeProducer
    from test_sinks_firehose import FakeFirehose
    from test_sinks_kinesis import FakeKinesis
    from test_sinks_mongodb import FakeCollection, FakeMongoClient
    from test_sinks_postgres import FakeConnection as FakePG
    from test_sinks_rabbitmq import FakeConnection as FakeAMQP
    from test_sinks_redis import FakeRedis
    from test_sinks_sns import FakeSNS
    from test_sinks_sqs import FakeSQSClient
    from test_sinks_syslog import FakeSocket

    monkeypatch.setattr(socket_mod, "_make_udp", lambda host: FakeSocket())
    return [
        HTTPSink("http://x", opener=FakeOpener()),
        SyslogSink("h", transport="udp"),
        RedisStreamsSink("s", client=FakeRedis()),
        PostgresSink("logs", connection=FakePG()),
        ClickHouseSink("logs", client=FakeClickHouse()),
        MongoDBSink(
            client=FakeMongoClient(FakeCollection()), database="d", collection="c"
        ),
        RabbitMQSink(exchange="e", routing_key="r", connection=FakeAMQP()),
        AzureEventHubsSink(producer=FakeProducer()),
        SQSSink("https://q/x", client=FakeSQSClient()),
        KinesisSink("s", client=FakeKinesis()),
        FirehoseSink("s", client=FakeFirehose()),
        SNSSink("arn", client=FakeSNS()),
    ]


def test_every_retrying_sink_accepts_a_stop_signal(monkeypatch) -> None:
    """The worker probes by name; a sink without the attribute silently keeps sleeping."""
    for sink in _one_of_each_retrying_sink(monkeypatch):
        assert hasattr(sink, "log_foundry_stop_signal"), type(sink).__name__
        assert sink.log_foundry_stop_signal is None, f"{type(sink).__name__} starts uninterruptible"


def test_the_worker_reaches_every_retrying_sink(monkeypatch) -> None:
    """The wiring is what makes the attribute worth having."""
    for sink in _one_of_each_retrying_sink(monkeypatch):
        worker = Worker(sink, batch_size=1)
        try:
            assert sink.log_foundry_stop_signal is worker._stop, type(sink).__name__
        finally:
            worker.shutdown()


def test_the_socket_backed_sinks_pass_the_signal_to_their_transport(monkeypatch) -> None:
    """``SyslogSink`` and ``LogstashSink`` hold the retry loop one level down."""
    from log_foundry.sinks.logstash import LogstashSink
    from log_foundry.sinks.syslog import SyslogSink
    from test_sinks_syslog import FakeSocket

    monkeypatch.setattr(socket_mod, "_make_udp", lambda host: FakeSocket())
    monkeypatch.setattr(socket_mod, "_make_tcp", lambda host, port, timeout: FakeSocket())

    for sink, backend in (
        (SyslogSink("h", transport="udp"), lambda s: s._socket),
        (LogstashSink(host="h", port=1), lambda s: s._socket),
        (LogstashSink(url="http://x", opener=FakeOpener()), lambda s: s._http),
    ):
        worker = Worker(sink, batch_size=1)
        try:
            assert backend(sink).log_foundry_stop_signal is worker._stop, type(sink).__name__
        finally:
            worker.shutdown()


def test_sentry_forwards_to_its_http_fallback() -> None:
    from log_foundry.sinks.sentry import SentrySink

    sink = SentrySink("http://k@sentry.local/1", backend="http", opener=FakeOpener())
    worker = Worker(sink, batch_size=1)
    try:
        assert sink._http is not None and sink._http.log_foundry_stop_signal is worker._stop
    finally:
        worker.shutdown()


def _drain_one(sink) -> float:
    """Submit one span through a real worker and time how long shutdown takes."""
    worker = Worker(sink, batch_size=1, max_retries=0)
    worker.submit([{"a": 1}])
    time.sleep(0.2)  # let the drain thread reach the sink's backoff
    started = time.monotonic()
    finished = _run_bounded(worker.shutdown, bound=6.0)
    return time.monotonic() - started if finished else float("inf")


def test_a_long_sink_backoff_does_not_hold_shutdown() -> None:
    """One drain thread, so a sink's backoff is a global pause — and it spans shutdown()."""
    import log_foundry.sinks.redis as redis_mod
    from log_foundry.sinks.redis import RedisStreamsSink
    from test_sinks_redis import FakeRedis

    original = redis_mod._BACKOFF_BASE
    redis_mod._BACKOFF_BASE = 20.0  # one attempt's backoff far exceeds the bound
    try:
        sink = RedisStreamsSink("s", client=FakeRedis(fail=True), max_retries=3)
        assert _drain_one(sink) < 6.0, "shutdown waited out the sink's backoff"
    finally:
        redis_mod._BACKOFF_BASE = original


def test_a_standalone_sink_backs_off_exactly_as_before(monkeypatch) -> None:
    """No worker, no signal — the pre-SPEC-027 behaviour, unchanged."""
    from log_foundry.sinks.base import SinkDeliveryError
    from log_foundry.sinks.redis import RedisStreamsSink
    from test_sinks_redis import FakeRedis

    slept: list[float] = []
    monkeypatch.setattr("log_foundry.sinks._retry.time.sleep", slept.append)

    sink = RedisStreamsSink("s", client=FakeRedis(fail=True), max_retries=2)
    assert sink.log_foundry_stop_signal is None
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}])
    assert slept == [0.1, 0.2], "1 + 2 retries, doubling"


# --- FR-003: SQSSink backs off between attempts ---------------------------------------------


def _sqs_slept(monkeypatch, client, **kwargs) -> list[float]:
    from log_foundry.sinks.sqs import SQSSink

    slept: list[float] = []
    monkeypatch.setattr("log_foundry.sinks._retry.time.sleep", slept.append)
    sink = SQSSink("https://q/x", client=client, **kwargs)
    try:
        sink.emit([{"log_id": "a"}])
    except Exception:  # SPEC-026's raise; this test is about the waits
        pass
    return slept


def test_sqs_separates_attempts_by_a_growing_delay(monkeypatch) -> None:
    """It was alone in re-sending immediately, while naming throttling as the retryable case."""
    from test_sinks_sqs_fifo import FaultingClient

    slept = _sqs_slept(
        monkeypatch, FaultingClient(sender_fault=False, code="Throttled"), max_retries=3
    )
    assert slept == [0.1, 0.2, 0.4], "one wait before each retry, none before giving up"


def test_a_first_attempt_that_succeeds_never_waits(monkeypatch) -> None:
    from test_sinks_sqs import FakeSQSClient

    assert _sqs_slept(monkeypatch, FakeSQSClient()) == []


def test_a_sender_fault_is_abandoned_with_no_backoff_first(monkeypatch) -> None:
    """SPEC-016 FR-006 abandons on the attempt that observed it; a wait would be pure delay."""
    from test_sinks_sqs_fifo import FaultingClient

    assert _sqs_slept(monkeypatch, FaultingClient(sender_fault=True), max_retries=3) == []


def test_the_sqs_backoff_is_interruptible() -> None:
    import log_foundry.sinks.sqs as sqs_mod
    from log_foundry.sinks.sqs import SQSSink
    from test_sinks_sqs_fifo import FaultingClient

    original = sqs_mod._BACKOFF_BASE
    sqs_mod._BACKOFF_BASE = 20.0
    try:
        sink = SQSSink(
            "https://q/x", client=FaultingClient(sender_fault=False), max_retries=3
        )
        assert _drain_one(sink) < 6.0
    finally:
        sqs_mod._BACKOFF_BASE = original


def test_a_wrapper_forwards_the_signal_to_what_actually_waits(monkeypatch) -> None:
    """Set on a wrapper the signal reaches nothing — the defect moved, not fixed."""
    from log_foundry.sinks.filtering import FilteringSink
    from log_foundry.sinks.multi import MultiSink
    from log_foundry.sinks.syslog import SyslogSink
    from log_foundry.sinks.transform import TransformSink
    from test_sinks_syslog import FakeSocket

    monkeypatch.setattr(socket_mod, "_make_udp", lambda host: FakeSocket())
    inner = SyslogSink("h", transport="udp")
    http = HTTPSink("http://x", opener=FakeOpener())

    for wrapper, leaves in (
        (MultiSink(inner, http), (inner, http)),
        (FilteringSink(http), (http,)),
        (TransformSink(http, lambda e: e), (http,)),
    ):
        http.log_foundry_stop_signal = None
        inner.log_foundry_stop_signal = None
        worker = Worker(wrapper, batch_size=1)
        try:
            for leaf in leaves:
                assert leaf.log_foundry_stop_signal is worker._stop, type(wrapper).__name__
            if inner in leaves:
                assert inner._socket.log_foundry_stop_signal is worker._stop, (
                    "and on through SyslogSink to the transport that actually waits"
                )
        finally:
            worker.shutdown()


def test_a_child_that_refuses_the_signal_does_not_stop_its_siblings(capsys) -> None:
    from log_foundry.sinks.multi import MultiSink

    class Stubborn:
        log_foundry_stop_signal = None

        def __setattr__(self, name: str, value: object) -> None:
            raise AttributeError("read-only")

        def emit(self, batch: list[dict[str, object]]) -> None: ...
        def close(self) -> None: ...

    willing = HTTPSink("http://x", opener=FakeOpener())
    worker = Worker(MultiSink(Stubborn(), willing), batch_size=1)
    try:
        assert willing.log_foundry_stop_signal is worker._stop, "the sibling still got it"
    finally:
        worker.shutdown()
    assert "handing a MultiSink child its stop signal" in capsys.readouterr().err


# --- FR-004: shutdown() cannot block forever ------------------------------------------------


class _WedgedSink:
    """A sink whose ``emit`` blocks until released — a network call with a generous timeout."""

    def __init__(self) -> None:
        self.release = threading.Event()
        self.entered = threading.Event()
        self.closed = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        self.entered.set()
        self.release.wait(30.0)

    def close(self) -> None:
        self.closed += 1


def test_shutdown_returns_within_its_bound_on_a_blocked_drain() -> None:
    sink = _WedgedSink()
    worker = Worker(sink, batch_size=1)
    worker.submit([{"a": 1}])
    assert sink.entered.wait(2.0), "the drain thread is inside emit"
    try:
        started = time.monotonic()
        assert _run_bounded(lambda: worker.shutdown(timeout=0.5), bound=5.0)
        assert time.monotonic() - started < 5.0
    finally:
        sink.release.set()


def test_an_expired_shutdown_is_recorded_and_announced(capsys) -> None:
    sink = _WedgedSink()
    worker = Worker(sink, batch_size=1)
    worker.submit([{"a": 1}])
    assert sink.entered.wait(2.0)
    try:
        worker.shutdown(timeout=0.2)
        health = worker.health()
        err = capsys.readouterr().err
    finally:
        sink.release.set()

    assert health.stopped_reason == "ShutdownTimeout"
    assert "shutdown timed out" in err
    assert err.count("\n") == 1, "one line"


def test_an_expired_shutdown_leaves_the_sink_open() -> None:
    """The drain thread may still be inside emit; closing under it corrupts rather than delays."""
    sink = _WedgedSink()
    worker = Worker(sink, batch_size=1)
    worker.submit([{"a": 1}])
    assert sink.entered.wait(2.0)
    try:
        worker.shutdown(timeout=0.2)
        assert sink.closed == 0
    finally:
        sink.release.set()


def test_an_expired_shutdown_does_not_overwrite_a_terminal_reason() -> None:
    """A thread that died on SystemExit is worse news than the timeout that followed it."""
    sink = _WedgedSink()
    worker = Worker(sink, batch_size=1)
    worker.submit([{"a": 1}])
    assert sink.entered.wait(2.0)
    with worker._lock:
        worker.stopped_reason = "SystemExit"
    try:
        worker.shutdown(timeout=0.2)
        assert worker.health().stopped_reason == "SystemExit"
    finally:
        sink.release.set()


def test_a_normal_shutdown_is_unaffected() -> None:
    from conftest import FakeSink

    sink = FakeSink()
    worker = Worker(sink, batch_size=1)
    worker.submit([{"a": 1}])
    worker.shutdown()
    health = worker.health()

    assert health.stopped_reason is None
    assert [e["a"] for e in sink.events] == [1]
    worker.shutdown()  # still idempotent


def test_the_public_shutdown_takes_a_timeout() -> None:
    import inspect

    import log_foundry
    from log_foundry.worker import DEFAULT_SHUTDOWN_TIMEOUT

    signature = inspect.signature(log_foundry.shutdown)
    assert signature.parameters["timeout"].default == DEFAULT_SHUTDOWN_TIMEOUT


def test_the_atexit_path_forwards_a_bounded_timeout(monkeypatch) -> None:
    """An unbounded join in an atexit handler is a process that will not exit."""
    import log_foundry
    from log_foundry.worker import DEFAULT_SHUTDOWN_TIMEOUT

    seen: list[float | None] = []

    class SpyWorker:
        def shutdown(self, timeout: float | None = None) -> None:
            seen.append(timeout)

    monkeypatch.setattr(_lifecycle._state, "_worker", SpyWorker())

    _lifecycle._shutdown_worker()  # what atexit calls: no argument
    log_foundry.shutdown()  # and what a caller gets by default
    log_foundry.shutdown(timeout=None)  # None is still available on request

    assert seen == [DEFAULT_SHUTDOWN_TIMEOUT, DEFAULT_SHUTDOWN_TIMEOUT, None]


# --- review follow-ups ----------------------------------------------------------------------


@pytest.mark.parametrize("ceiling", [0.0, -1.0, math.nan])
def test_an_unusable_ceiling_falls_back_rather_than_defeating_the_clamp(ceiling: float) -> None:
    """A zero ceiling returned 0.0 (a hot retry loop); a NaN one made ``min`` return the value."""
    assert clamp_server_delay(86400.0, ceiling) is None


def test_a_zero_max_retry_after_does_not_produce_a_hot_retry_loop(monkeypatch) -> None:
    """The obvious way to lower the ceiling for a tight deadline must not remove the backoff."""
    slept: list[float] = []
    monkeypatch.setattr("log_foundry.sinks._retry.time.sleep", slept.append)

    sink = HTTPSink(
        "http://x",
        max_retries=2,
        max_retry_after=0.0,
        opener=FakeOpener([FakeResponse(429, b"", {"Retry-After": "60"})]),
    )
    with pytest.raises(Exception):  # noqa: B017
        sink.emit([{"a": 1}])

    assert slept == [0.1, 0.2], "fell back to exponential backoff, not to no backoff"


def test_a_huge_finite_delay_does_not_raise_on_the_drain_thread(monkeypatch) -> None:
    """``time.sleep(1e18)`` raises OverflowError; "total" has to mean total."""
    from log_foundry.sinks._retry import MAX_WAIT

    slept: list[float] = []
    monkeypatch.setattr("log_foundry.sinks._retry.time.sleep", slept.append)
    wait(1e18, None)
    assert slept == [MAX_WAIT]

    # The Event branch overflows too, and must be exercised *unset* — a set flag returns
    # without ever looking at the timeout, so setting it first tests nothing.
    errors: list[str] = []

    stop = threading.Event()

    def waiting_on(event: threading.Event) -> None:
        try:
            wait(1e18, event)
        except BaseException as err:
            errors.append(type(err).__name__)

    thread = threading.Thread(target=waiting_on, args=(stop,), daemon=True)
    thread.start()
    thread.join(0.3)
    assert errors == [], "Event.wait overflows past the platform's time_t too"
    assert thread.is_alive(), "and is still waiting out the capped delay, not returning early"
    stop.set()  # released rather than left parked on an 86,400 s wait for the session
    thread.join(2.0)


def test_a_later_shutdown_closes_a_sink_an_expired_one_left_open() -> None:
    """One expiry used to mean the sink was never closed for the life of the process."""
    sink = _WedgedSink()
    worker = Worker(sink, batch_size=1)
    worker.submit([{"a": 1}])
    assert sink.entered.wait(2.0)

    worker.shutdown(timeout=0.2)
    assert sink.closed == 0, "not while the thread is still inside emit"

    sink.release.set()
    worker._thread.join(5.0)
    worker.shutdown(timeout=1.0)
    assert sink.closed == 1, "the close was deferred, not abandoned"

    worker.shutdown(timeout=1.0)
    assert sink.closed == 1, "and still idempotent"


def test_a_deferred_close_does_not_re_drain() -> None:
    """The once-only flag still holds: a second call closes, it does not emit again."""
    sink = _WedgedSink()
    worker = Worker(sink, batch_size=1)
    worker.submit([{"a": 1}])
    assert sink.entered.wait(2.0)
    worker.shutdown(timeout=0.2)
    sink.release.set()
    worker._thread.join(5.0)

    calls_before = sink.entered.is_set()
    worker.submit([{"b": 2}])  # nothing consumes this now
    worker.shutdown(timeout=1.0)

    assert calls_before and sink.closed == 1


def test_the_expired_shutdown_line_counts_what_is_still_queued(capsys) -> None:
    """"item(s)", not "event(s)": the queue holds one entry per span, plus internal markers."""
    sink = _WedgedSink()
    worker = Worker(sink, batch_size=1)
    worker.submit([{"a": 1}, {"a": 2}])  # one item, two events
    assert sink.entered.wait(2.0)
    worker.submit([{"b": 1}])
    worker.submit([{"c": 1}])
    try:
        worker.shutdown(timeout=0.2)
        err = capsys.readouterr().err
    finally:
        sink.release.set()

    assert "lost 1 drain(s)" not in err, "nothing counted a 'drain'"
    # 2 submissions still queued + the _SHUTDOWN sentinel. Asserted exactly, because a count
    # nobody checks is how "lost 1 drain(s)" survived the first round of tests.
    assert "lost 3 item(s)" in err
    assert "event(s)" not in err, "the queue counts spans, not the events inside them"
    assert "shutdown timed out after 0.2s" in err


def test_the_timeout_in_the_line_is_not_taken_on_the_callers_word(capsys) -> None:
    """``timeout`` is caller data, so its ``__str__`` is not the library's to trust (arch §6)."""
    from log_foundry.worker import _bounded_seconds

    class Hostile:
        def __str__(self) -> str:
            return "0\nlog-foundry: forged line"

    assert _bounded_seconds(Hostile()) == "?"  # type: ignore[arg-type]
    assert _bounded_seconds(None) == "no timeout"
    assert _bounded_seconds(0.25) == "0.25s"


def _delays_for(sink, batch, monkeypatch) -> list[float]:
    """Record the delays a sink waits between attempts, without waiting them."""
    module = type(sink).__module__
    slept: list[float] = []
    monkeypatch.setattr(f"{module}.wait", lambda delay, _stop=None: slept.append(delay))
    try:
        sink.emit(batch)
    except Exception:  # SPEC-026's raise; this test is about the waits
        pass
    return slept


def test_the_three_aws_stream_sinks_back_off_between_attempts(monkeypatch) -> None:
    """The AST lint proves a ``wait`` call exists; only this proves it is in the right place."""
    from log_foundry.sinks.firehose import FirehoseSink
    from log_foundry.sinks.kinesis import KinesisSink
    from log_foundry.sinks.sns import SNSSink
    from test_sinks_firehose import FakeFirehose
    from test_sinks_kinesis import FakeKinesis
    from test_sinks_sns import FakeSNS

    kinesis_body = json.dumps({"trace_id": "t", "a": 1}).encode("utf-8")
    firehose_body = json.dumps({"a": 1}).encode("utf-8") + b"\n"  # FR-005 delimiter
    cases = (
        (KinesisSink("s", client=FakeKinesis(always_fail={kinesis_body}), max_retries=3),
         [{"trace_id": "t", "a": 1}]),
        (FirehoseSink("s", client=FakeFirehose(always_fail={firehose_body}), max_retries=3),
         [{"a": 1}]),
        (SNSSink("arn", client=FakeSNS(always_fail={"0"}), max_retries=3), [{"a": 1}]),
    )
    for sink, batch in cases:
        assert _delays_for(sink, batch, monkeypatch) == [0.1, 0.2, 0.4], (
            f"{type(sink).__name__}: one wait before each retry, none before giving up"
        )


def test_a_stream_sink_does_not_wait_before_abandoning(monkeypatch) -> None:
    """FR-003: a wait before giving up is pure delay on the drain thread.

    Kinesis stands for the three: the exact ``[0.1, 0.2, 0.4]`` sequence asserted above already
    pins the property for all of them, since a wait before abandoning would make it four.
    """
    from log_foundry.sinks.kinesis import KinesisSink
    from test_sinks_kinesis import FakeKinesis

    body = json.dumps({"trace_id": "t", "a": 1}).encode("utf-8")
    sink = KinesisSink("s", client=FakeKinesis(always_fail={body}), max_retries=0)
    assert _delays_for(sink, [{"trace_id": "t", "a": 1}], monkeypatch) == []


def test_a_first_attempt_that_succeeds_never_waits_on_any_of_them(monkeypatch) -> None:
    from log_foundry.sinks.firehose import FirehoseSink
    from log_foundry.sinks.kinesis import KinesisSink
    from log_foundry.sinks.sns import SNSSink
    from test_sinks_firehose import FakeFirehose
    from test_sinks_kinesis import FakeKinesis
    from test_sinks_sns import FakeSNS

    for sink in (
        KinesisSink("s", client=FakeKinesis()),
        FirehoseSink("s", client=FakeFirehose()),
        SNSSink("arn", client=FakeSNS()),
    ):
        assert _delays_for(sink, [{"a": 1}], monkeypatch) == [], type(sink).__name__


def test_an_unadjudicable_chunk_is_abandoned_with_no_wait(monkeypatch) -> None:
    """SPEC-018 never re-sends it, so there is nothing to wait for."""
    from log_foundry.sinks.kinesis import KinesisSink
    from test_sinks_kinesis import MalformedKinesis

    sink = KinesisSink("s", client=MalformedKinesis(None), max_retries=3)
    assert _delays_for(sink, [{"a": 1}], monkeypatch) == []


def test_concurrent_shutdowns_close_the_sink_exactly_once() -> None:
    """atexit and user code both call it; a double close on a released sink is what we avoid.

    It *does* reproduce the race: measured against the pre-fix worker it fails ~60% of runs with
    ``assert 2 == 1``, no artificial delay needed — the barrier releases three callers while
    ``batch_size=1`` lets the drain thread exit almost immediately, which is exactly the window.
    Not a certainty per run, so the structural fix is what makes it right: one lock-guarded
    decision owns the close, and this pins the observable.
    """
    from conftest import FakeSink

    class CountingSink(FakeSink):
        def __init__(self) -> None:
            super().__init__()
            self.closed = 0

        def close(self) -> None:
            self.closed += 1

    sink = CountingSink()
    worker = Worker(sink, batch_size=1)
    worker.submit([{"a": 1}])

    ready = threading.Barrier(4)

    def shut() -> None:
        ready.wait(5.0)
        worker.shutdown(timeout=5.0)

    threads = [threading.Thread(target=shut, daemon=True) for _ in range(3)]
    for thread in threads:
        thread.start()
    ready.wait(5.0)
    for thread in threads:
        thread.join(10.0)

    assert sink.closed == 1


# --- SPEC-047: usable_timeout, the rule KafkaSink's flush and NATSSink's publish budget share ---


@pytest.mark.parametrize("bad", [0, -1.0, float("inf"), float("nan")])
def test_a_timeout_that_bounds_nothing_falls_back_to_the_caller_s_default(bad: float) -> None:
    # `nan` is the reason the test is `not (0 < value < inf)` rather than a pair of comparisons:
    # it compares False to everything, so `value <= 0` would let it through to become the bound.
    assert usable_timeout(bad, 7.5) == 7.5


def test_a_timeout_that_bounds_something_is_honoured_exactly() -> None:
    assert usable_timeout(2.5, 7.5) == 2.5


def test_the_default_is_the_callers_not_a_constant_of_this_module() -> None:
    # The whole reason for extracting this with a parameter: KafkaSink's flush timeout and
    # NATSSink's publish budget are unrelated numbers, and a baked-in constant would silently
    # hand one sink the other's fallback.
    assert usable_timeout(0, 10.0) == 10.0
    assert usable_timeout(0, 30.0) == 30.0
