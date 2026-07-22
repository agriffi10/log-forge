"""SPEC-001 FR-005 — StdoutSink line formatting, flush, and injectable stream."""

import io
import json

import pytest

stdout_sink = pytest.importorskip("log_foundry.sinks.stdout")


class _RecordingStream(io.StringIO):
    """A StringIO that counts flush() calls so we can assert the sink flushes."""

    def __init__(self) -> None:
        super().__init__()
        self.flushes = 0

    def flush(self) -> None:  # noqa: D102
        self.flushes += 1
        super().flush()


def test_emit_writes_one_json_line_per_event_then_flushes() -> None:
    stream = _RecordingStream()
    sink = stdout_sink.StdoutSink(stream=stream)
    batch = [{"level": "INFO", "message": "a"}, {"level": "ERROR", "message": "b"}]

    sink.emit(batch)

    lines = stream.getvalue().splitlines()
    assert len(lines) == 2
    assert [json.loads(line) for line in lines] == batch
    assert stream.flushes >= 1  # emit flushes after writing the batch


def test_emit_output_is_valid_json_round_trip() -> None:
    stream = io.StringIO()
    sink = stdout_sink.StdoutSink(stream=stream)
    event = {"trace_id": "a" * 32, "fields": {"user_id": 7}, "parent_span_id": None}

    sink.emit([event])

    assert json.loads(stream.getvalue().strip()) == event


def test_empty_batch_writes_nothing() -> None:
    stream = io.StringIO()
    stdout_sink.StdoutSink(stream=stream).emit([])
    assert stream.getvalue() == ""


def test_close_flushes_the_stream() -> None:
    stream = _RecordingStream()
    stdout_sink.StdoutSink(stream=stream).close()
    assert stream.flushes >= 1
