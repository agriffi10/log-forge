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

    def flush(self) -> None:
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


# -- SPEC-017 FR-001: a poisonous field survives the real sink + worker -------------------


def test_stdout_emits_a_previously_poisonous_batch_in_full(capsys) -> None:
    """The end-to-end criterion: a real StdoutSink and a real Worker, not a FakeSink.

    `datetime` in a field used to raise TypeError inside StdoutSink.emit, which the worker
    retried and then abandoned — losing the whole batch and every co-batched event with it.
    """
    import json
    from datetime import datetime

    from log_foundry.model import Span, build_event
    from log_foundry.worker import Worker

    span = Span(trace_id="a" * 32, span_id="b" * 16, parent_span_id=None, name="fn", start_ts=0.0)
    events = [
        build_event(span, "INFO", "clean", fields={"n": 1}, baggage={}),
        build_event(span, "INFO", "poison", fields={"at": datetime(2026, 1, 1)}, baggage={}),
        build_event(span, "INFO", "also-clean", fields={"n": 3}, baggage={}),
    ]

    worker = Worker(stdout_sink.StdoutSink(), batch_size=10)
    try:
        worker.submit(events)
        assert worker.flush(timeout=5.0) is True
    finally:
        worker.shutdown()

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert [e["message"] for e in lines] == ["clean", "poison", "also-clean"]
    assert worker.failed_batches == 0, "the batch must not be abandoned"
