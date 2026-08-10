"""SPEC-008/038 — utility sinks: StderrSink formatting, NullSink drop-count, MemorySink ring buffer.

StderrSink is exercised through an injected ``StringIO`` (and once through ``capsys`` for the
``sys.stderr`` default); the other two hold no external resources, so they are tested directly.
"""

from __future__ import annotations

import io
import sys

from log_foundry.sinks.base import Sink
from log_foundry.sinks.memory import MemorySink
from log_foundry.sinks.null import NullSink
from log_foundry.sinks.stdout import StderrSink


class FlushCounter(io.StringIO):
    """A StringIO that counts ``flush`` calls, to assert the sinks flush."""

    def __init__(self) -> None:
        super().__init__()
        self.flushes = 0

    def flush(self) -> None:
        self.flushes += 1
        super().flush()


# --- FR-004: StderrSink -----------------------------------------------------------------


def test_stderr_is_a_sink() -> None:
    assert isinstance(StderrSink(), Sink)


def test_stderr_writes_one_json_line_per_event() -> None:
    buffer = io.StringIO()
    StderrSink(buffer).emit([{"a": 1}, {"b": 2}])
    assert buffer.getvalue() == '{"a": 1}\n{"b": 2}\n'


def test_stderr_default_stream_is_sys_stderr() -> None:
    assert StderrSink()._stream is sys.stderr


def test_stderr_default_targets_stderr_not_stdout(capsys) -> None:
    StderrSink().emit([{"a": 1}])
    captured = capsys.readouterr()
    assert captured.err == '{"a": 1}\n'
    assert captured.out == ""


def test_stderr_emit_and_close_flush() -> None:
    buffer = FlushCounter()
    sink = StderrSink(buffer)
    sink.emit([{"a": 1}])
    sink.close()
    assert buffer.flushes >= 2  # one on emit, one on close


# --- FR-005: NullSink -------------------------------------------------------------------


def test_null_is_a_sink() -> None:
    assert isinstance(NullSink(), Sink)


def test_null_discards_and_counts_dropped() -> None:
    sink = NullSink()
    sink.emit([{"a": 1}, {"b": 2}])
    sink.emit([{"c": 3}])
    assert sink.dropped == 3


def test_null_close_is_noop() -> None:
    NullSink().close()  # must not raise


# --- FR-006: MemorySink -----------------------------------------------------------------


def test_memory_is_a_sink() -> None:
    assert isinstance(MemorySink(), Sink)


def test_memory_collects_events_in_order() -> None:
    sink = MemorySink()
    sink.emit([{"n": 1}, {"n": 2}])
    sink.emit([{"n": 3}])
    assert sink.events == [{"n": 1}, {"n": 2}, {"n": 3}]


def test_memory_ring_keeps_only_most_recent() -> None:
    sink = MemorySink(maxlen=2)
    sink.emit([{"n": 1}, {"n": 2}, {"n": 3}])  # trims within a single over-long batch
    assert sink.events == [{"n": 2}, {"n": 3}]
    sink.emit([{"n": 4}])  # trims across batches too
    assert sink.events == [{"n": 3}, {"n": 4}]


def test_memory_events_readable_after_close() -> None:
    sink = MemorySink()
    sink.emit([{"n": 1}])
    sink.close()  # no-op
    assert sink.events == [{"n": 1}]
