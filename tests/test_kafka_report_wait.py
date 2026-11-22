"""SPEC-094 FR-001 — ``KafkaSink`` waits for this round's delivery reports, on the engine.

Each round hands every remaining message to the producer, then waits, within ``flush_timeout``
and until the stop signal, for the reports on its own messages, asking ``flush(slice)`` in slices.
A success is ``ACCEPTED``; a code in ``DEFINITIVE_CODES`` is ``REJECTED``; any other error, or no
report in time, is ``UNKNOWN``, counted once with a may-have-landed line and never resent. Nothing
is counted after the round's verdict. Serves invariants 2, 3, 7 and 10.
"""

from __future__ import annotations

import inspect
import threading
import time
from typing import Any

import pytest

import log_foundry
from conftest import cpu_window, install_worker
from log_foundry.sinks import _engine, kafka
from log_foundry.sinks.base import SinkLosses
from log_foundry.sinks.kafka import KafkaSink
from log_foundry.sinks.verdict import Outcome
from log_foundry.worker import Worker
from test_fork_lifecycle import _held_by_a_parent_thread, run_in_child


class _Code:
    """A ``KafkaError``-shaped report: a numeric ``code()``."""

    def __init__(self, code: int) -> None:
        """Keeps the code.

        Args:
          code: librdkafka's code.

        Returns:
          None.

        Raises:
          None.
        """
        self._code = code

    def code(self) -> int:
        """The code.

        Args:
          None.

        Returns:
          The code.

        Raises:
          None.
        """
        return self._code


class _Reporting:
    """A producer whose ``flush`` serves every held report with one error and reads empty."""

    def __init__(self, error: object = None) -> None:
        """Keeps the error every report carries.

        Args:
          error: The report's error, or ``None`` for success.

        Returns:
          None.

        Raises:
          None.
        """
        self.error = error
        self.held: list[Any] = []
        self.produced = 0

    def produce(self, topic: str, value: Any = None, key: Any = None, callback: Any = None) -> None:
        """Holds the message's callback.

        Args:
          topic: Ignored.
          value: Ignored.
          key: Ignored.
          callback: The report callback.

        Returns:
          None.

        Raises:
          None.
        """
        self.produced += 1
        self.held.append(callback)

    def poll(self, timeout: float) -> int:
        """Serves nothing.

        Args:
          timeout: Ignored.

        Returns:
          0.

        Raises:
          None.
        """
        return 0

    def flush(self, timeout: float | None = None) -> int:
        """Serves every held report.

        Args:
          timeout: Ignored.

        Returns:
          0, the queue empty.

        Raises:
          None.
        """
        served, self.held = self.held, []
        for callback in served:
            callback(self.error, None)
        return 0


class _Silent(_Reporting):
    """A producer that never reports and whose ``flush`` returns ``remaining`` at once."""

    def __init__(self, remaining: object = 1) -> None:
        """Keeps what ``flush`` returns.

        Args:
          remaining: ``flush``'s answer.

        Returns:
          None.

        Raises:
          None.
        """
        super().__init__()
        self.remaining = remaining
        self.flushes = 0

    def flush(self, timeout: float | None = None) -> Any:
        """Serves nothing and returns at once.

        Args:
          timeout: Ignored.

        Returns:
          ``remaining``.

        Raises:
          None.
        """
        self.flushes += 1
        return self.remaining


class _OneStuck(_Reporting):
    """``spec094_stuck_partition.py``'s producer: the first message never reports.

    Every later one is reported at the next ``poll`` or ``flush``, and ``flush`` holds its whole
    timeout while the stuck message is queued, as ``rd_kafka_flush`` does.
    """

    def __init__(self) -> None:
        """Starts with nothing stuck.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        super().__init__()
        self.stuck = False

    def produce(self, topic: str, value: Any = None, key: Any = None, callback: Any = None) -> None:
        """Strands the first message; holds the rest.

        Args:
          topic: Ignored.
          value: Ignored.
          key: Ignored.
          callback: The report callback.

        Returns:
          None.

        Raises:
          None.
        """
        if not self.stuck:
            self.stuck = True
            return
        self.held.append(callback)

    def poll(self, timeout: float) -> int:
        """Serves every held report.

        Args:
          timeout: Ignored.

        Returns:
          How many it served.

        Raises:
          None.
        """
        served, self.held = self.held, []
        for callback in served:
            callback(None, None)
        return len(served)

    def flush(self, timeout: float | None = None) -> int:
        """Serves what it holds, then holds its timeout while the stuck message is queued.

        Args:
          timeout: Seconds.

        Returns:
          1, the stuck message.

        Raises:
          None.
        """
        self.poll(0)
        if timeout:
            time.sleep(timeout)
        return 1


def _batch(n: int) -> list[dict[str, object]]:
    """``n`` events.

    Args:
      n: How many.

    Returns:
      The events.

    Raises:
      None.
    """
    return [{"message": f"m{i}", "trace_id": "t"} for i in range(n)]


def _lost(err: str) -> list[str]:
    """The ``lost`` lines of a stderr capture.

    Args:
      err: The capture.

    Returns:
      The lines.

    Raises:
      None.
    """
    return [line for line in err.splitlines() if "lost" in line]


def _through_the_worker(sink: KafkaSink, n: int) -> tuple[bool, int]:
    """One batch of ``n`` through a real ``Worker`` with ``max_retries=3``.

    Args:
      sink: The sink.
      n: Events.

    Returns:
      The flush verdict and the worker's failed batches.

    Raises:
      None.
    """
    worker = Worker(sink, batch_size=1000, flush_interval=60.0, max_retries=3)
    install_worker(worker)
    worker.submit(_batch(n))
    ok = bool(worker.flush(timeout=10.0))
    return ok, log_foundry.health().failed_batches


def _stopped(sink: KafkaSink) -> threading.Event:
    """Hands the sink a stop signal already set.

    Args:
      sink: The sink.

    Returns:
      The signal.

    Raises:
      None.
    """
    stop = threading.Event()
    stop.set()
    sink.log_foundry_stop_signal = stop
    return stop


def test_a_definitive_code_fails_the_batch_and_each_message_is_produced_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Code 87, ``INVALID_RECORD``: ``flush()`` false, one failed batch, no resend."""
    monkeypatch.setattr(_engine, "cycle_backoff", lambda attempt: 0.0)
    producer = _Reporting(_Code(87))
    ok, failed_batches = _through_the_worker(KafkaSink("t", producer=producer), 3)
    assert (ok, failed_batches) == (False, 1)
    assert producer.produced == 3


@pytest.mark.parametrize("code", [10, 29], ids=["size", "authorization"])
def test_a_report_that_does_not_rule_out_a_kept_attempt_is_unknown_and_flush_stays_true(
    code: int, capsys: pytest.CaptureFixture[str]
) -> None:
    """Codes 10 and 29 judge what an attempt can change: ``UNKNOWN``, counted once, one line."""
    sink = KafkaSink("t", producer=_Reporting(_Code(code)))
    ok, failed_batches = _through_the_worker(sink, 3)
    assert (ok, failed_batches) == (True, 0)
    assert sink.losses() == SinkLosses(dropped=0, failed=3)
    lines = _lost(capsys.readouterr().err)
    assert (
        len(lines) == 1
        and "lost 3 message(s); KafkaSink, no definitive delivery report" in (lines[0])
    ), lines


def test_one_stuck_message_costs_later_rounds_one_slice_not_the_timeout() -> None:
    """``spec094_stuck_partition.py`` as a test: emit 0 waits the timeout, later ones one slice.

    ``flush_timeout`` is 1.0 s so the later emits' ceiling, 0.5 s, sits at the timing gate's stall
    allowance while one blocking flush, which costs every emit the whole 1.0 s, still fails it.
    """
    sink = KafkaSink("t", producer=_OneStuck(), flush_timeout=1.0)
    took: list[float] = []
    failed: list[int] = []
    for _ in range(4):
        began = time.monotonic()
        sink.emit(_batch(10))
        took.append(time.monotonic() - began)
        failed.append(sink.losses().failed)
    assert 1.0 <= took[0] <= 2.0, took
    assert all(t <= 0.5 for t in took[1:]), took
    assert failed == [1, 1, 1, 1], "the stuck message counted once, nothing new after"


def test_a_report_served_after_its_round_settled_changes_nothing(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A late report, from a later round's ``flush`` or from ``close()``, moves no count."""
    producer = _Silent(remaining=1)
    sink = KafkaSink("t", producer=producer, flush_timeout=0.1)
    sink.emit(_batch(1))
    late = producer.held.pop()
    assert sink.losses() == SinkLosses(dropped=0, failed=1)
    capsys.readouterr()
    late(_Code(87), None)
    late(None, None)
    sink.close()
    assert sink.losses() == SinkLosses(dropped=0, failed=1)
    assert _lost(capsys.readouterr().err) == []


def test_an_empty_queue_reads_unreported_messages_accepted_and_a_nonempty_one_unknown() -> None:
    """``flush`` returning 0 with no report: ``ACCEPTED``; returning 1: ``UNKNOWN`` at deadline."""
    empty = KafkaSink("t", producer=_Silent(remaining=0), flush_timeout=0.2)
    assert list(empty.deliver(_batch(3)).outcomes) == [Outcome.ACCEPTED] * 3
    queued = KafkaSink("t", producer=_Silent(remaining=1), flush_timeout=0.2)
    assert list(queued.deliver(_batch(3)).outcomes) == [Outcome.UNKNOWN] * 3


@pytest.mark.parametrize("remaining", [False, 0.0, "0"], ids=["false", "float-zero", "str-zero"])
def test_only_the_int_zero_is_an_empty_reading(remaining: object) -> None:
    """The empty reading is the ``int`` 0 and nothing else: not ``False``, ``0.0`` or ``"0"``.

    Each equals or resembles 0, so a guard that compared without judging the type would read an
    unreported message delivered; ``bool`` is an ``int`` subclass, so ``_diag.is_a(value, int)``
    alone admits ``False`` too, and the guard excludes it by name.
    """
    sink = KafkaSink("t", producer=_Silent(remaining=remaining), flush_timeout=0.1)
    assert sink.deliver(_batch(1)).outcomes == (Outcome.UNKNOWN,)


class _Clock:
    """A clock the stand-in advances, so the first-slice test reads no wall time."""

    def __init__(self) -> None:
        """Starts at zero.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.now = 0.0

    def monotonic(self) -> float:
        """The fake time.

        Args:
          None.

        Returns:
          Seconds.

        Raises:
          None.
        """
        return self.now


class _Partitioned(_Reporting):
    """Message ``i`` on partition ``i % 4``; partition ``p``'s reports are due at ``10 + 8p`` ms.

    ``flush`` advances the fake clock to the next partition due, if that is within its timeout,
    serves that one partition's reports and returns, as a ``flush`` woken by one served operation
    does; otherwise it advances the clock by its timeout and serves nothing.
    """

    def __init__(self, clock: _Clock) -> None:
        """Keeps the clock.

        Args:
          clock: The fake clock.

        Returns:
          None.

        Raises:
          None.
        """
        super().__init__()
        self.clock = clock
        self.start: float | None = None

    def produce(self, topic: str, value: Any = None, key: Any = None, callback: Any = None) -> None:
        """Holds the callback with its partition.

        Args:
          topic: Ignored.
          value: Ignored.
          key: Ignored.
          callback: The report callback.

        Returns:
          None.

        Raises:
          None.
        """
        if self.start is None:
            self.start = self.clock.now
        self.held.append((len(self.held) % 4, callback))

    def flush(self, timeout: float | None = None) -> int:
        """Serves the next partition due within ``timeout``, advancing the clock to it.

        Args:
          timeout: Seconds.

        Returns:
          The reports still held.

        Raises:
          None.
        """
        assert self.start is not None
        pending = sorted({p for p, _ in self.held})
        if pending:
            due = self.start + 0.010 + 0.008 * pending[0]
            if due <= self.clock.now + (timeout or 0):
                self.clock.now = max(self.clock.now, due)
                served = [cb for p, cb in self.held if p == pending[0]]
                self.held = [(p, cb) for p, cb in self.held if p != pending[0]]
                for callback in served:
                    callback(None, None)
                return len(self.held)
        self.clock.now += timeout or 0
        return len(self.held)


def test_under_a_set_stop_the_first_slice_still_takes_every_report_inside_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reports due at 10, 18, 26 and 34 ms all land inside the 50 ms first slice: all ``ACCEPTED``.

    The first slice is a deadline at the wait's start plus ``_REPORT_SLICE``, not one pass; with it
    set to the start, the re-plant reads 30 of the 40 ``UNKNOWN``.
    """
    clock = _Clock()
    monkeypatch.setattr(kafka, "time", clock)
    sink = KafkaSink("t", producer=_Partitioned(clock), flush_timeout=5)
    _stopped(sink)
    verdict = sink.deliver(_batch(40))
    assert sum(o is Outcome.UNKNOWN for o in verdict.outcomes) == 0
    assert clock.now == pytest.approx(0.034)


@pytest.mark.parametrize("stop", [False, True], ids=["stop-unset", "stop-set"])
def test_a_flush_that_returns_at_once_does_not_spin(stop: bool) -> None:
    """With ``flush`` returning 1 at once, a 0.5 s wait uses under 0.1 s of CPU, stop or no stop."""
    producer = _Silent(remaining=1)
    sink = KafkaSink("t", producer=producer, flush_timeout=0.5)
    if stop:
        _stopped(sink)
    with cpu_window() as window:
        sink.deliver(_batch(1))
    assert window.spent < 0.1, window.spent
    assert producer.flushes < 100, producer.flushes


def test_with_no_report_a_round_waits_its_timeout_and_counts_once(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """No report ever, ``flush_timeout=0.5``: one ``emit`` takes 0.5 s to 1.5 s, counted once."""
    sink = KafkaSink("t", producer=_Silent(remaining=1), flush_timeout=0.5)
    began = time.monotonic()
    sink.emit(_batch(2))
    took = time.monotonic() - began
    assert 0.5 <= took <= 1.5, took
    assert sink.losses() == SinkLosses(dropped=0, failed=2)
    assert len(_lost(capsys.readouterr().err)) == 1


def test_a_stop_set_during_the_wait_ends_it_with_its_messages_unknown() -> None:
    """The stop set 0.3 s in, ``flush_timeout=5``: the round returns in 0.3 s to 1.3 s."""
    sink = KafkaSink("t", producer=_Silent(remaining=1), flush_timeout=5)
    stop = threading.Event()
    sink.log_foundry_stop_signal = stop
    began = time.monotonic()
    timer = threading.Timer(0.3, stop.set)
    timer.start()
    verdict = sink.deliver(_batch(2))
    took = time.monotonic() - began
    timer.cancel()
    assert 0.3 <= took <= 1.3, took
    assert verdict.outcomes == (Outcome.UNKNOWN, Outcome.UNKNOWN)


def test_flush_reads_true_over_an_indefinite_report_and_the_hook_does_not_wait() -> None:
    """``log_foundry.flush(timeout=5)`` is truthy; the hook returns within 1 s and never raises."""
    sink = KafkaSink("t", producer=_Silent(remaining=1), flush_timeout=0.5)
    log_foundry.configure(service="t", sink=sink)

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("one")

    work()
    assert log_foundry.flush(timeout=5)
    began = time.monotonic()
    sink.flush()
    assert time.monotonic() - began <= 1.0


def test_a_round_whose_every_produce_is_refused_is_resent_by_the_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``BufferError`` on every produce: ``RETRYABLE``, so the worker's run resends the round."""
    monkeypatch.setattr(_engine, "cycle_backoff", lambda attempt: 0.0)

    class Full(_Reporting):
        def produce(self, *args: Any, **kwargs: Any) -> None:
            self.produced += 1
            raise BufferError("Local: Queue full")

    producer = Full()
    ok, failed_batches = _through_the_worker(KafkaSink("t", producer=producer), 2)
    assert (ok, failed_batches) == (False, 1)
    assert producer.produced == 8, "two messages on each of the run's four rounds"


def test_a_refusal_beside_a_hand_off_is_counted_in_rejected_once_the_run_settles(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """One of two refused: the other is produced and accepted; ``rejected`` moves by one."""

    class HalfFull(_Reporting):
        def produce(self, *args: Any, **kwargs: Any) -> None:
            if self.produced:
                raise BufferError("Local: Queue full")
            super().produce(*args, **kwargs)

    sink = KafkaSink("t", producer=HalfFull())
    sink.emit(_batch(2))
    assert sink.rejected == 1
    assert sink.losses() == SinkLosses(dropped=1, failed=0)
    assert _lost(capsys.readouterr().err) == [
        "log-foundry: lost 1 message(s); KafkaSink produce, BufferError"
    ]


def test_the_signature_and_the_names_are_kept() -> None:
    """Invariant 10: ``__init__`` unchanged; ``failed``, ``rejected``, ``losses``, ``flush``, ``close``."""
    params = list(inspect.signature(KafkaSink).parameters)
    assert params == [
        "topic",
        "producer",
        "bootstrap_servers",
        "key_field",
        "flush_timeout",
        "producer_config",
    ]
    sink = KafkaSink("t", producer=_Reporting())
    for name in ("failed", "rejected", "losses", "flush", "close"):
        assert hasattr(sink, name), name
    sink.failed = 4
    sink.rejected = 2
    assert sink.losses() == SinkLosses(dropped=2, failed=4)


def test_a_late_report_served_in_a_forked_child_never_takes_the_parents_lock() -> None:
    """Invariant 12: a settled table reads the counters lock afresh, so a child never deadlocks.

    The round settles with its report outstanding; the stand-in keeps the callback, as librdkafka
    keeps it, past the round. A parent thread holds the counters lock across the fork, and the
    child serves the late report, as its close flush or ``flush()`` hook would. The fork repair
    rebinds ``SinkCounters._counter_lock`` in the child; a table that had kept the parent's lock
    object would block on it there, and the bounded child would never finish.
    """
    producer = _Silent(remaining=1)
    sink = KafkaSink("t", producer=producer, flush_timeout=0.1)
    log_foundry.configure(service="fork", version="0", env="test", sink=sink)
    assert sink.deliver(_batch(1)).outcomes == (Outcome.UNKNOWN,)
    (late,) = producer.held
    released, holder = _held_by_a_parent_thread(sink.log_foundry_counters._counter_lock)

    def serve_the_late_report() -> str:
        late(None, None)
        return "served"

    try:
        child = run_in_child(serve_the_late_report, timeout=5)
    finally:
        released.set()
        holder.join(5.0)

    assert child.finished, f"the child {child.ending}, having written {child.output!r}"
    assert child.output == "served", child.output
