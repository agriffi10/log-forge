"""SPEC-002 — console echo (FR-002): additivity, format, default-off.

The echoed line is human-readable and goes to the ConsoleWriter's stream; the same event
still reaches the sink. Tests inject a StringIO console stream and use the FakeSink from
conftest to prove the event rode both paths.
"""

import contextvars
import io

from log_foundry import console as console_mod


def test_console_writer_renders_level_and_message() -> None:
    stream = io.StringIO()
    writer = console_mod.ConsoleWriter(stream=stream)

    writer.write({"level": "INFO", "message": "hello"})

    line = stream.getvalue()
    assert line == "INFO    hello\n"  # level left-justified to width 7


def test_echo_true_writes_console_and_still_reaches_sink(lf, fake_sink, monkeypatch) -> None:
    stream = io.StringIO()
    monkeypatch.setattr("log_foundry.api._console", console_mod.ConsoleWriter(stream=stream))

    @lf.trace(name="work")
    def work() -> None:
        lf.info("echoed", echo=True)

    contextvars.copy_context().run(work)

    # console got a human-readable line...
    assert "echoed" in stream.getvalue()
    assert "INFO" in stream.getvalue()
    # ...and the event still rode the normal pipeline to the sink
    assert any(e["message"] == "echoed" for e in fake_sink.events)


def test_echo_false_writes_nothing_to_console(lf, fake_sink, monkeypatch) -> None:
    stream = io.StringIO()
    monkeypatch.setattr("log_foundry.api._console", console_mod.ConsoleWriter(stream=stream))

    @lf.trace(name="work")
    def work() -> None:
        lf.info("silent")

    contextvars.copy_context().run(work)

    assert stream.getvalue() == ""
    assert any(e["message"] == "silent" for e in fake_sink.events)


def test_echo_on_orphan_log_still_writes_console(lf, fake_sink, monkeypatch) -> None:
    stream = io.StringIO()
    monkeypatch.setattr("log_foundry.api._console", console_mod.ConsoleWriter(stream=stream))

    lf.error("orphan echo", echo=True)

    assert "orphan echo" in stream.getvalue()
    assert any(e["message"] == "orphan echo" for e in fake_sink.events)


# -- SPEC-031 FR-003: the documented stream and binding are the real ones --------------------


def test_the_console_default_stream_is_stderr_not_stdout() -> None:
    """Two documents said stdout; the code has always said stderr (SPEC-031 FR-003)."""
    import sys

    writer = console_mod.ConsoleWriter()
    assert writer._stream is sys.stderr


def test_the_stream_is_bound_at_construction_not_read_per_write(capsys) -> None:
    """The surprise FR-003 documents: a later redirect is not honoured, an explicit one is."""
    import contextlib
    import io
    import sys

    writer = console_mod.ConsoleWriter()
    captured = io.StringIO()
    with contextlib.redirect_stderr(captured):
        writer.write({"level": "INFO", "message": "not-redirected"})
    assert captured.getvalue() == "", "the writer kept the stream it resolved at construction"
    assert "not-redirected" in capsys.readouterr().err

    explicit = io.StringIO()
    console_mod.ConsoleWriter(stream=explicit).write({"level": "INFO", "message": "captured"})
    assert "captured" in explicit.getvalue(), "stream= is how a caller captures the output"
    assert sys.stderr is not explicit


def test_the_stdout_sink_binds_its_stream_at_construction_too() -> None:
    import contextlib
    import io

    from log_foundry.sinks.stdout import StdoutSink

    sink = StdoutSink()
    redirected = io.StringIO()
    with contextlib.redirect_stdout(redirected):
        sink.emit([{"message": "x"}])
    assert redirected.getvalue() == "", "documented on StdoutSink.__init__ (SPEC-031 FR-003)"

    explicit = io.StringIO()
    StdoutSink(stream=explicit).emit([{"message": "y"}])
    assert "y" in explicit.getvalue()


# -- SPEC-055 FR-005: the writer owns its stream's failures ------------------------------------


class _FaultingStream:
    """A stream that raises the given exception on every write, counting the attempts."""

    def __init__(self, exc: BaseException) -> None:
        self.exc = exc
        self.writes = 0

    def write(self, text: str) -> int:
        self.writes += 1
        raise self.exc

    def flush(self) -> None:
        pass


def _event(i: int) -> dict[str, object]:
    return {"level": "INFO", "message": f"line {i}"}


def test_a_broken_pipe_disables_echo_after_one_line(capsys) -> None:
    """The audit's measurement: 200,000 events into `head -1` wrote 199,970 diagnostic lines."""
    stream = _FaultingStream(BrokenPipeError(32, "Broken pipe"))
    writer = console_mod.ConsoleWriter(stream=stream)  # type: ignore[arg-type]
    for i in range(200_000):
        writer.write(_event(i))
    err = capsys.readouterr().err
    lines = [line for line in err.splitlines() if "echoing to the console" in line]
    assert len(lines) == 1
    assert "(BrokenPipeError)" in lines[0] and "echo is disabled" in lines[0]
    assert stream.writes == 1


def test_a_closed_stream_disables_echo_the_same_way(capsys) -> None:
    import io

    closed = io.StringIO()
    closed.close()
    writer = console_mod.ConsoleWriter(stream=closed)
    for i in range(1000):
        writer.write(_event(i))
    err = capsys.readouterr().err
    assert err.count("echoing to the console") == 1
    assert "(ValueError)" in err and "echo is disabled" in err


def test_an_unencodable_message_does_not_disable_echo(capsys) -> None:
    """`UnicodeEncodeError` is a `ValueError`, and it is one message, not a dead stream."""
    import io

    strict = io.TextIOWrapper(io.BytesIO(), encoding="ascii", errors="strict")
    writer = console_mod.ConsoleWriter(stream=strict)
    writer.write({"level": "INFO", "message": "café"})
    writer.write({"level": "INFO", "message": "plain"})
    strict.flush()
    assert b"plain" in strict.buffer.getvalue(), "the stream is still written after the fault"
    err = capsys.readouterr().err
    assert "(UnicodeEncodeError)" in err and "echo is disabled" not in err
    assert writer._failures == 1


def test_a_transient_fault_is_throttled(capsys) -> None:
    """2,500 `OSError`s produce lines at totals 1, 1000 and 2000, and every call is attempted."""
    stream = _FaultingStream(OSError(28, "No space left on device"))
    writer = console_mod.ConsoleWriter(stream=stream)  # type: ignore[arg-type]
    for i in range(2500):
        writer.write(_event(i))
    err = capsys.readouterr().err
    lines = [line for line in err.splitlines() if "echoing to the console" in line]
    totals = [line.split(";")[1].split()[0] for line in lines]
    assert totals == ["1", "1000", "2000"]
    assert all("(OSError)" in line for line in lines)
    assert stream.writes == 2500


def test_the_failure_count_is_exact_across_threads(capsys) -> None:
    """The counter is taken under a lock: 8 x 1,000 failures count to exactly 8,000, nine lines."""
    from conftest import run_concurrently

    stream = _FaultingStream(OSError(11, "Resource temporarily unavailable"))
    writer = console_mod.ConsoleWriter(stream=stream)  # type: ignore[arg-type]

    def work(index: int, iteration: int) -> None:
        writer.write(_event(index))

    assert run_concurrently(work, threads=8, per_thread=1000) == []
    assert writer._failures == 8000
    err = capsys.readouterr().err
    assert err.count("echoing to the console") == 9


def test_an_echoed_event_still_reaches_the_sink_when_the_stream_is_broken(
    lf, fake_sink, monkeypatch
) -> None:
    """Both delivery paths: the echo is a second audience, and its loss costs the event nothing."""
    stream = _FaultingStream(BrokenPipeError(32, "Broken pipe"))
    monkeypatch.setattr(
        "log_foundry.api._console",
        console_mod.ConsoleWriter(stream=stream),  # type: ignore[arg-type]
    )

    @lf.trace(name="work")
    def work() -> str:
        lf.info("in-span", echo=True)
        return "returned"

    assert contextvars.copy_context().run(work) == "returned"
    lf.info("orphan", echo=True)
    lf.shutdown()
    messages = [e["message"] for e in fake_sink.events]
    assert "in-span" in messages and "orphan" in messages
    before = lf.health()
    assert (before.in_span_lost, before.orphan_lost) == (0, 0)
