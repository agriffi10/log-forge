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
