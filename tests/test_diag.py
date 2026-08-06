"""SPEC-029 — the diagnostic channel's own rules (FR-001, FR-002, FR-003, FR-004).

Pure unit tests over ``_diag``. Nothing here goes through a sink, a span or the worker: the point
is that the *writers themselves* name an exception by type, bound and escape any detail, and cannot
raise on a broken stream. The call sites that must use them are covered by their own suites, and by
the no-``stderr.write``-outside-``_diag`` guard below (FR-001).
"""

from __future__ import annotations

import ast
import sys
import urllib.error
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from log_foundry import _diag

if TYPE_CHECKING:
    from collections.abc import Callable


class _Boom(Exception):
    """An exception whose text carries what arch §6 keeps out of the library's own output."""

    def __init__(self) -> None:
        super().__init__("INSERT INTO logs VALUES ('user@example.com', 'card-4111')")


class _RaisingStream:
    """A ``sys.stderr`` whose ``write`` fails, as a closed fd or a broken pipe does."""

    def __init__(self, exc: BaseException) -> None:
        self._exc = exc
        self.calls = 0

    def write(self, text: str) -> int:
        self.calls += 1
        raise self._exc

    def flush(self) -> None:
        return None


class _Stderr:
    """Reader for what ``_diag`` wrote, over pytest's own capture.

    Deliberately not a monkeypatched ``sys.stderr``: pytest re-assigns ``sys.stderr`` when it
    resumes global capture between the setup and call phases, so a patch applied in a fixture is
    silently undone before the test body runs. Accumulating rather than draining, so a second
    ``getvalue()`` in one test does not read empty.
    """

    def __init__(self, capsys: pytest.CaptureFixture[str]) -> None:
        self._capsys = capsys
        self._text = ""

    def getvalue(self) -> str:
        self._text += self._capsys.readouterr().err
        return self._text


@pytest.fixture
def err(capsys: pytest.CaptureFixture[str]) -> _Stderr:
    """What the library wrote to stderr during this test."""
    return _Stderr(capsys)


# -- FR-002: an exception is reported by type, not by repr ----------------------------------


def test_absorbed_writes_the_type_name_only(err: _Stderr) -> None:
    _diag.absorbed("closing a span", _Boom())

    line = err.getvalue()
    assert "_Boom" in line
    assert "user@example.com" not in line, "the exception's message must never be written"
    assert "card-4111" not in line
    assert "INSERT" not in line


def test_absorbed_never_writes_the_repr_or_args(err: _Stderr) -> None:
    exc = _Boom()
    _diag.absorbed("emitting an orphan log", exc, "1 event(s) lost")

    line = err.getvalue()
    assert repr(exc) not in line
    assert str(exc) not in line
    assert str(exc.args[0]) not in line
    assert line == (
        "log-foundry: absorbed a failure while emitting an orphan log (_Boom); 1 event(s) lost\n"
    )


def test_lost_carries_the_count_and_the_detail(err: _Stderr) -> None:
    _diag.lost("event", 12, "PostgresSink, 3 attempts, OperationalError")

    assert err.getvalue() == (
        "log-foundry: lost 12 event(s); PostgresSink, 3 attempts, OperationalError\n"
    )


def test_lost_without_a_detail_omits_the_semicolon(err: _Stderr) -> None:
    _diag.lost("batch", 1)

    assert err.getvalue() == "log-foundry: lost 1 batch(s)\n"


@pytest.mark.parametrize(
    "write",
    [
        lambda: _diag.absorbed("closing a span", _Boom(), "a detail"),
        lambda: _diag.lost("event", 3, "a detail"),
        lambda: _diag.rejected("unparseable traceparent", "00-bad"),
    ],
)
def test_every_line_is_single_and_prefixed(err: _Stderr, write: Callable[[], None]) -> None:
    write()

    line = err.getvalue()
    assert line.startswith("log-foundry: ")
    assert line.endswith("\n")
    assert line.count("\n") == 1, "one diagnostic is one line"


# -- FR-002: a detail is bounded and escaped ------------------------------------------------


def test_detail_is_truncated_to_the_documented_bound(err: _Stderr) -> None:
    _diag.lost("event", 1, "x" * 5000)

    line = err.getvalue()
    assert "x" * _diag._MAX_DETAIL in line
    assert "x" * (_diag._MAX_DETAIL + 1) not in line
    assert line.endswith("…\n")


def test_detail_control_characters_cannot_forge_a_second_line(err: _Stderr) -> None:
    _diag.lost("event", 1, "SQSSink\nlog-foundry: everything is fine\r\x00")

    line = err.getvalue()
    assert len(line.splitlines()) == 1, "an embedded newline must not become a line break"
    assert "\\n" in line
    assert "\\r" in line
    assert "\\x00" in line
    assert "everything is fine" in line, "escaped, not dropped — the text is still diagnostic"


@pytest.mark.parametrize(
    ("name", "char"),
    [
        ("NEL", "\x85"),
        ("LINE SEPARATOR", "\u2028"),
        ("PARAGRAPH SEPARATOR", "\u2029"),
        ("CSI", "\x9b"),
        ("RIGHT-TO-LEFT OVERRIDE", "\u202e"),
    ],
)
def test_a_non_c0_separator_cannot_forge_a_second_line(err: _Stderr, name: str, char: str) -> None:
    """A ``range(0x20)`` table misses these; ``splitlines()`` and terminals do not.

    Counting ``"\\n"`` would report one line while a log shipper reading the same bytes saw two,
    which is the failure mode worth naming: the check that says it is safe disagrees with the
    reader that isn't.
    """
    _diag.lost("event", 1, f"SQSSink{char}log-foundry: everything is fine")

    line = err.getvalue()
    assert len(line.splitlines()) == 1, f"{name} must not survive as a break"
    assert char not in line, f"{name} must be escaped, not passed through"


def test_absorbed_detail_is_escaped_and_bounded_too(err: _Stderr) -> None:
    _diag.absorbed("draining the log queue", _Boom(), "held 2\nqueued " + "9" * 5000)

    line = err.getvalue()
    assert line.count("\n") == 1
    assert len(line) < _diag._MAX_DETAIL + 200


def test_the_bound_applies_to_the_escaped_text(err: _Stderr) -> None:
    """Escaping precedes truncation, so the bound governs what is *written*.

    Truncating first would also produce an escaped line, but a detail of ``_MAX_DETAIL``
    control characters would then expand to four times the bound on its way out.
    """
    _diag.lost("event", 1, "\n" * 5000)

    prefix = "log-foundry: lost 1 event(s); "
    assert len(err.getvalue()) == len(prefix) + _diag._MAX_DETAIL + len("…\n")


# -- FR-002: rejected keeps SPEC-014's bounded repr exactly ---------------------------------


def test_rejected_bounds_the_echoed_value(err: _Stderr) -> None:
    _diag.rejected("unparseable traceparent", "0" * 500)

    line = err.getvalue()
    assert line.startswith("log-foundry: ignoring inbound trace context (unparseable traceparent):")
    assert "0" * _diag._MAX_REJECTED_ECHO not in line, "the repr's opening quote counts toward it"
    assert line.endswith("…\n")


def test_rejected_repr_escapes_an_injected_line(err: _Stderr) -> None:
    _diag.rejected("unparseable traceparent", "00-bad\nlog-foundry: everything is fine-01")

    line = err.getvalue()
    assert line.count("\n") == 1
    assert "\\n" in line


def test_rejected_echoes_a_short_value_verbatim(err: _Stderr) -> None:
    _diag.rejected("invalid trace_id", "nope")

    assert (
        err.getvalue() == "log-foundry: ignoring inbound trace context (invalid trace_id): 'nope'\n"
    )


def test_rejected_escapes_a_repr_that_lies(err: _Stderr) -> None:
    """``repr`` escaping line breaks is a property of the built-ins, not of ``repr``.

    ``__repr__`` is user code and may *return* a raw newline. The call sites pass an inbound
    header, so this is the untrusted path by construction (SPEC-014) — and the values are only
    ``str`` by annotation, which a caller not running mypy is free to ignore.
    """

    class _Liar:
        def __repr__(self) -> str:
            return "00-bad\nlog-foundry: everything is fine"

    _diag.rejected("unparseable traceparent", _Liar())

    assert len(err.getvalue().splitlines()) == 1


def test_the_documented_bounds_are_the_spec_s(err: _Stderr) -> None:
    """Pinned to literals, not to themselves — the Data Model names these two numbers."""
    assert _diag._MAX_DETAIL == 200
    assert _diag._MAX_REJECTED_ECHO == 64


# -- FR-002: errno_of, the library-controlled OSError detail ---------------------------------


def test_errno_of_reads_an_oserror() -> None:
    assert _diag.errno_of(OSError(111, "Connection refused")) == "errno=111"


def test_errno_of_unwraps_a_urlerror_reason() -> None:
    assert _diag.errno_of(urllib.error.URLError(OSError(113, "No route to host"))) == "errno=113"


def test_errno_of_is_empty_without_one() -> None:
    assert _diag.errno_of(ValueError("nope")) == ""
    assert _diag.errno_of(urllib.error.URLError("nodename nor servname provided")) == ""


def test_errno_of_survives_an_exploding_attribute() -> None:
    class _Hostile(Exception):
        @property
        def errno(self) -> int:
            raise RuntimeError("boom")

    assert _diag.errno_of(_Hostile()) == ""


def test_errno_of_never_returns_the_exception_text() -> None:
    assert "Connection refused" not in _diag.errno_of(OSError(111, "Connection refused"))


def test_errno_of_renders_an_int_subclass_as_a_number() -> None:
    """``isinstance(code, int)`` admits a subclass, whose ``__str__`` is arbitrary user code.

    Drivers routinely carry an ``IntEnum`` or a bespoke code class, so interpolating the value as
    found would put whatever that class returns on stderr — through the one helper offered as the
    safe alternative to the exception's message.
    """

    class _Code(int):
        def __str__(self) -> str:
            return "INSERT INTO logs VALUES ('user@example.com')"

    class _Driver(Exception):
        def __init__(self) -> None:
            super().__init__("boom")
            self.errno = _Code(111)

    assert _diag.errno_of(_Driver()) == "errno=111"


# -- FR-003: a diagnostic can never be the failure ------------------------------------------


@pytest.mark.parametrize(
    "write",
    [
        lambda: _diag.absorbed("closing a span", _Boom(), "a detail"),
        lambda: _diag.lost("event", 3, "a detail"),
        lambda: _diag.rejected("unparseable traceparent", "00-bad"),
    ],
)
@pytest.mark.parametrize("fault", [ValueError("closed"), OSError(32, "Broken pipe")])
def test_a_broken_stream_is_absorbed(
    monkeypatch: pytest.MonkeyPatch, write: Callable[[], None], fault: Exception
) -> None:
    stream = _RaisingStream(fault)
    monkeypatch.setattr(sys, "stderr", stream)

    write()

    assert stream.calls == 1, "it tried, and swallowed the fault"


def test_a_none_stderr_is_absorbed(monkeypatch: pytest.MonkeyPatch) -> None:
    """``sys.stderr`` is ``None`` under a pythonw-style host and at interpreter shutdown."""
    monkeypatch.setattr(sys, "stderr", None)

    _diag.absorbed("closing a span", _Boom())
    _diag.lost("event", 1)
    _diag.rejected("invalid trace_id", "x")


@pytest.mark.parametrize(
    "write",
    [
        lambda: _diag.absorbed("closing a span", _Boom()),
        lambda: _diag.lost("event", 3),
        lambda: _diag.rejected("unparseable traceparent", "00-bad"),
    ],
)
@pytest.mark.parametrize("fault", [KeyboardInterrupt(), SystemExit(1)])
def test_a_baseexception_from_the_write_still_propagates(
    monkeypatch: pytest.MonkeyPatch, write: Callable[[], None], fault: BaseException
) -> None:
    """The operator's or the runtime's intent, not a stream fault to swallow (SPEC-025)."""
    monkeypatch.setattr(sys, "stderr", _RaisingStream(fault))

    with pytest.raises(type(fault)):
        write()


def test_an_unrenderable_detail_still_leaves_the_count(err: _Stderr) -> None:
    """The detail is rendered inside the caller's f-string, so it must fail alone.

    Losing the whole line would take the count with it — the one part of the message an operator
    cannot reconstruct from anywhere else.
    """

    class _Unrenderable(str):
        def isprintable(self) -> bool:
            raise RuntimeError("boom")

    _diag.lost("event", 7, _Unrenderable("whatever"))

    assert err.getvalue() == "log-foundry: lost 7 event(s)\n"


def test_a_hostile_value_cannot_break_rejected(err: _Stderr) -> None:
    """``repr`` runs user code; ``rejected`` is called from the caller's thread."""

    class _Hostile:
        def __repr__(self) -> str:
            raise RuntimeError("boom")

    _diag.rejected("unusable baggage header", _Hostile())

    assert err.getvalue() == "", "nothing written, nothing raised"


# -- FR-004: the rules are documented where the next writer will look -----------------------


def test_module_docstring_states_the_rules() -> None:
    doc = _diag.__doc__ or ""
    assert "arch §6" in doc, "the type-name rule needs its justification"
    assert "PostgresSink" in doc, "the sharpest leak example"
    assert "BaseException" in doc, "the guard's one exclusion"
    assert "Record first, announce second" in doc
    assert "logging" in doc, "why stderr rather than the logging module"


def test_diag_imports_nothing_from_its_own_package() -> None:
    """A leaf by necessity: it is imported at module scope while the package is half-built."""
    tree = ast.parse(Path(_diag.__file__).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            assert not any(a.name.startswith("log_foundry") for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            assert not (node.module or "").startswith("log_foundry")
            assert node.level == 0, "a relative import is an intra-package import"


# -- FR-002, enforced across the package ----------------------------------------------------

_SRC = Path(_diag.__file__).parent


def _diag_calls(tree: ast.AST) -> list[ast.Call]:
    """Every ``_diag.<writer>(...)`` call in a module."""
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "_diag"
    ]


@pytest.mark.parametrize("path", sorted(_SRC.rglob("*.py")), ids=lambda p: p.name)
def test_no_diagnostic_interpolates_a_repr(path: Path) -> None:
    """No ``_diag`` call site may re-introduce the twelve sinks' ``{err!r}`` (SPEC-029 FR-002).

    A ``repr`` prints attribute values, so a psycopg error carries the failing statement and its
    bound parameters, and a client object carries its credentials. Checked at the call sites rather
    than by grepping for ``!r`` outright, because a ``raise ValueError(f"invalid table name
    {name!r}")`` is correct — that echoes the caller's own bad argument back to the caller, which
    is the opposite direction.

    ``str(exc)`` is not mechanically detectable here (nothing in the AST says which name holds an
    exception); the per-sink tests carry that one, `test_an_abandoned_insert_never_reprints_the_event`
    above all.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for call in _diag_calls(tree):
        for node in ast.walk(call):
            if isinstance(node, ast.FormattedValue):
                assert node.conversion != ord("r"), f"{path.name}: !r in a diagnostic"
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                assert node.func.id != "repr", f"{path.name}: repr() in a diagnostic"
            if isinstance(node, ast.Attribute):
                assert node.attr != "args", f"{path.name}: exception args in a diagnostic"


def test_the_enforcement_actually_sees_the_call_sites() -> None:
    """Guards the guard: a walker that matched nothing would pass every file vacuously."""
    calls = sum(
        len(_diag_calls(ast.parse(p.read_text(encoding="utf-8")))) for p in _SRC.rglob("*.py")
    )
    assert calls >= 30, f"expected the converted call sites to be found, saw {calls}"
