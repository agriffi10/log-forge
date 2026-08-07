"""SPEC-002 — console echo (FR-002): additivity, format, default-off.

The echoed line is human-readable and goes to the ConsoleWriter's stream; the same event
still reaches the sink. Tests inject a StringIO console stream and use the FakeSink from
conftest to prove the event rode both paths.
"""

import contextvars
import io

import pytest

console_mod = pytest.importorskip("log_foundry.console")


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
