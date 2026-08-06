"""SPEC-027 — bounded, interruptible retry: the shared wait and the `Retry-After` clamp.

Every test here is in-process. Timing assertions use generous bounds and assert the *shape*
(returned early / did not return early), never a precise duration.
"""

from __future__ import annotations

import math
import threading
import time

import pytest

import log_foundry.sinks._socket as socket_mod
from log_foundry.sinks._retry import clamp_server_delay, wait
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
        assert sink.stop_signal is worker._stop
    finally:
        worker.shutdown()


def test_a_sink_with_no_stop_signal_attribute_is_left_alone() -> None:
    class Plain:
        def emit(self, batch: list[dict[str, object]]) -> None: ...
        def close(self) -> None: ...

    sink = Plain()
    worker = Worker(sink, batch_size=1)
    try:
        assert not hasattr(sink, "stop_signal")
    finally:
        worker.shutdown()


def test_a_sink_that_refuses_the_signal_does_not_stop_the_worker(capsys) -> None:
    """Losing interruptibility is a degradation; failing to start the worker is an outage."""

    class Stubborn:
        stop_signal = None

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

    monkeypatch.setattr(socket_mod, "_make_udp", RefusingSocket)
    transport = socket_mod.SocketTransport("h", 1, transport="udp", max_retries=3)
    transport.stop_signal = threading.Event()
    transport.stop_signal.set()

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
