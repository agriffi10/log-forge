"""SPEC-093 FR-002 — an event the Sentry SDK discards at capture is counted as dropped, never sent.

``Client.capture_event`` returns ``None`` when the SDK discarded the event: an ignored error,
``before_send``, the error sample rate, or no transport. ``SentrySink`` counted every such event
in ``sent``. It now counts it in ``discarded``, reported as ``losses().dropped``, through the
``emit``'s ``Deferral``, with one line per batch, and a batch with any discard returns rather than
raising, so the worker never resends it. Serves invariants 2, 7 and 11.
"""

from __future__ import annotations

from typing import Any

import pytest

import log_foundry
from conftest import install_worker
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.sentry import SentrySink
from log_foundry.worker import Worker


class _Scripted:
    """An SDK stand-in whose ``capture_event`` answers or raises from a script, one per call."""

    def __init__(self, *script: object) -> None:
        """Keeps the script: a value to return, or an exception instance to raise.

        Args:
          *script: One entry per call, the last repeated.

        Returns:
          None.

        Raises:
          None.
        """
        self.script = list(script)
        self.calls = 0

    def capture_event(self, event: dict[str, Any]) -> object:
        """Answers the next script entry.

        Args:
          event: The event, unused.

        Returns:
          The scripted value.

        Raises:
          Exception: When the script says so.
        """
        entry = self.script[min(self.calls, len(self.script) - 1)]
        self.calls += 1
        if isinstance(entry, BaseException):
            raise entry
        return entry


class _Counting(SentrySink):
    """A ``SentrySink`` that counts the ``emit`` calls the worker makes."""

    emits = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Counts, then emits.

        Args:
          batch: The events.

        Returns:
          None.

        Raises:
          SinkDeliveryError: As ``SentrySink.emit``.
        """
        type(self).emits += 1
        super().emit(batch)


def _errors(n: int) -> list[dict[str, object]]:
    """``n`` events at ``ERROR``, which the default ``min_level`` sends.

    Args:
      n: How many.

    Returns:
      The events.

    Raises:
      None.
    """
    return [{"level": "ERROR", "message": f"m{i}"} for i in range(n)]


def _through_the_worker(sink: SentrySink, events: list[dict[str, object]]) -> bool:
    """Runs one batch through a real ``Worker`` that would retry a raise three times.

    Args:
      sink: The sink.
      events: The batch.

    Returns:
      The flush verdict.

    Raises:
      None.
    """
    worker = Worker(sink, batch_size=1000, flush_interval=60.0, max_retries=3)
    install_worker(worker)
    worker.submit(events)
    return bool(worker.flush(timeout=10.0))


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


@pytest.fixture(autouse=True)
def _reset_emits() -> None:
    """Starts each test's emit count at zero.

    Args:
      None.

    Returns:
      None.

    Raises:
      None.
    """
    _Counting.emits = 0


def test_a_batch_of_nothing_but_discards_returns_counted_and_is_emitted_once(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Criterion 3: it returns, ``discarded`` moves by its size, and the worker emits it once."""
    client = _Scripted(None)
    sink = _Counting(client=client)
    assert _through_the_worker(sink, _errors(3))
    assert (_Counting.emits, client.calls) == (1, 3)
    assert (sink.sent, sink.discarded, sink.transport_errors) == (0, 3, 0)
    assert sink.losses() == SinkLosses(dropped=3, failed=0)
    [line] = _lost(capsys.readouterr().err)
    assert "lost 3 event(s); SentrySink, discarded by the Sentry SDK at capture" in line


def test_two_discards_and_one_raise_return_and_count_each_where_it_belongs(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Criterion 4: none sent; ``discarded`` +2, ``transport_errors`` +1, emitted once."""
    client = _Scripted(None, RuntimeError("transport down"), None)
    sink = _Counting(client=client)
    assert _through_the_worker(sink, _errors(3))
    assert (_Counting.emits, client.calls) == (1, 3)
    assert (sink.sent, sink.discarded, sink.transport_errors) == (0, 2, 1)
    assert sink.losses() == SinkLosses(dropped=2, failed=1)
    lines = _lost(capsys.readouterr().err)
    assert any("SentrySink, discarded by the Sentry SDK at capture" in x for x in lines)


def test_a_batch_whose_every_event_raises_still_raises_and_discards_nothing() -> None:
    """Criterion 5: 1.x's total failure: it raises, ``transport_errors`` moves, ``discarded`` not."""
    sink = SentrySink(client=_Scripted(RuntimeError("transport down")))
    with pytest.raises(SinkDeliveryError):
        sink.emit(_errors(2))
    assert (sink.sent, sink.discarded, sink.transport_errors) == (0, 0, 2)


@pytest.mark.parametrize("value", ["", 0, object()], ids=["empty-str", "zero", "object"])
def test_any_return_but_none_reads_as_captured(value: object) -> None:
    """Criterion 7: ``""``, ``0`` or an object counts the event in ``sent``, as in 1.x."""
    sink = SentrySink(client=_Scripted(value))
    sink.emit(_errors(2))
    assert (sink.sent, sink.discarded) == (2, 0)
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


def test_the_count_is_visible_in_health() -> None:
    """``losses().dropped`` reaches ``health().sink`` like every other sink's count."""
    sink = SentrySink(client=_Scripted(None))
    log_foundry.configure(service="t", sink=sink)
    sink.emit(_errors(2))
    assert log_foundry.health().sink == SinkLosses(dropped=2, failed=0)
