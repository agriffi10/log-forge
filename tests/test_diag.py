"""SPEC-029 — the diagnostic channel's own rules (FR-001, FR-002, FR-003, FR-004).

Pure unit tests over ``_diag``. Nothing here goes through a sink, a span or the worker: the point
is that the *writers themselves* name an exception by type, bound and escape any detail, and cannot
raise on a broken stream. The call sites that must use them are covered by their own suites, and by
the no-``stderr.write``-outside-``_diag`` guard below (FR-001).
"""

from __future__ import annotations

import ast
import io
import sys
import threading
import time
import urllib.error
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from conftest import armed, object_probe, str_probe
from log_foundry import _diag

if TYPE_CHECKING:
    import types
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
        lambda: _diag.pending("a detail"),
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


# -- SPEC-067 FR-001: rejected echoes a str's first 64 characters, anything else its type ---


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
    """A ``__repr__`` free to return a raw newline is never called at all (SPEC-067 FR-001).

    ``__repr__`` is user code, and the call sites pass an inbound header that is only ``str`` by
    annotation, which a caller not running mypy is free to ignore. SPEC-029 escaped what it
    returned; SPEC-067 stops calling it, so a non-``str`` is echoed by its type's name.
    """
    called: list[str] = []

    class _Liar:
        def __repr__(self) -> str:
            called.append("__repr__")
            return "00-bad\nlog-foundry: everything is fine"

    _diag.rejected("unparseable traceparent", _Liar())

    out = err.getvalue()
    assert len(out.splitlines()) == 1
    assert out.endswith(": <_Liar>\n")
    assert called == [], "the value's own __repr__ ran"


def test_a_metaclass_and_a_class_property_cannot_divert_the_type_name(err: _Stderr) -> None:
    """The name is read through ``type``'s own descriptor, the route through ``str``'s MRO.

    ``__subclasscheck__`` is not a hook here: ``issubclass(type(v), str)`` consults ``str``'s
    metaclass, never the value's. The probe is armed only around the call, so a failure here is
    reported by pytest rather than ending the run in INTERNALERROR.
    """
    value = object_probe("C", class_property="raise", meta=True)

    with armed() as calls:
        _diag.rejected("invalid trace_id", value)

    assert err.getvalue() == "log-foundry: ignoring inbound trace context (invalid trace_id): <C>\n"
    assert calls == []


def test_a_type_name_that_is_a_hostile_str_is_escaped_and_bounded(err: _Stderr) -> None:
    """A class's ``__name__`` may be any ``str`` subclass instance, of any length, with a newline."""
    name = str_probe("a\nlog-foundry: forged" + "z" * 479, raising="all")

    class D:
        pass

    D.__name__ = name

    with armed() as calls:
        _diag.rejected("invalid trace_id", D())

    out = err.getvalue()
    assert out.count("\n") == 1
    assert out == (
        "log-foundry: ignoring inbound trace context (invalid trace_id): "
        "<a\\nlog-foundry: forgedzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz…>\n"
    )
    assert calls == []


@pytest.mark.parametrize(
    ("length", "echo"),
    [
        (64, "<NNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN>"),
        (65, "<NNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN…>"),
    ],
    ids=["64-whole", "65-cut-and-marked"],
)
def test_a_type_name_is_cut_and_marked_past_64(err: _Stderr, length: int, echo: str) -> None:
    """A printable name one past the bound is cut **and** marked, never cut silently.

    The name route slices 65 characters, not 64: a 65-character printable name sliced at 64
    escapes to exactly 64 and the cut would carry no ``…``.
    """
    _diag.rejected("invalid trace_id", type("N" * length, (), {})())

    assert (
        err.getvalue()
        == f"log-foundry: ignoring inbound trace context (invalid trace_id): {echo}\n"
    )


@pytest.mark.parametrize(
    ("value", "echo"),
    [(b"00-bad", "<bytes>"), (123, "<int>"), ([1, 2], "<list>")],
    ids=["bytes", "int", "list"],
)
def test_a_non_str_is_echoed_by_type_name(err: _Stderr, value: object, echo: str) -> None:
    """``<bytes>`` names the caller's actual mistake, where ``b'00-bad'`` looked like a header."""
    _diag.rejected("unparseable traceparent", value)

    assert err.getvalue() == (
        f"log-foundry: ignoring inbound trace context (unparseable traceparent): {echo}\n"
    )


def test_a_hostile_str_subclass_is_echoed_as_the_str_it_holds(err: _Stderr) -> None:
    """Quoted, escaped and bounded, by ``str``'s own slots, with none of the subclass's code."""
    value = str_probe(
        "a\nb" + "0" * 500,
        raising={"__repr__", "__getitem__", "__len__", "__str__", "__iter__", "__eq__"},
    )

    with armed() as calls:
        _diag.rejected("unparseable traceparent", value)

    assert err.getvalue() == (
        "log-foundry: ignoring inbound trace context (unparseable traceparent): "
        "'a\\nb00000000000000000000000000000000000000000000000000000000000…\n"
    )
    assert calls == []


@pytest.mark.parametrize(
    ("value", "echo"),
    [
        ("nope", "'nope'"),
        ("", "''"),
        ("it's", '"it\'s"'),
        ('say "hi"', "'say \"hi\"'"),
        ("a'b\"c", "'a\\'b\"c'"),
        ("a\nb", "'a\\nb'"),
        ("a\tb", "'a\\tb'"),
        ("\x85", "'\\x85'"),
        ("\u2028", "'\\u2028'"),
        ("0" * 64, "'000000000000000000000000000000000000000000000000000000000000000…"),
    ],
    ids=[
        "plain",
        "empty",
        "apostrophe",
        "double-quotes",
        "both-quotes",
        "newline",
        "tab",
        "nel",
        "line-separator",
        "sixty-four",
    ],
)
def test_a_short_exact_str_echoes_as_before(err: _Stderr, value: str, echo: str) -> None:
    """The unchanged half: at most 64 characters, the line is byte-identical to SPEC-029's."""
    _diag.rejected("invalid trace_id", value)

    assert (
        err.getvalue()
        == f"log-foundry: ignoring inbound trace context (invalid trace_id): {echo}\n"
    )


@pytest.mark.parametrize(
    ("value", "echo"),
    [
        ("0" * 500, "'000000000000000000000000000000000000000000000000000000000000000…"),
        ("x" * 100 + "'", "'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx…"),
        (
            "it's" + "x" * 100 + '"',
            "\"it'sxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx…",
        ),
    ],
    ids=["zeros", "apostrophe-past-the-cut", "both-quotes-past-the-cut"],
)
def test_a_long_str_is_cut_before_its_repr(err: _Stderr, value: str, echo: str) -> None:
    """The quote is chosen from the first 64 characters, so text past the cut cannot change it."""
    _diag.rejected("invalid trace_id", value)

    assert (
        err.getvalue()
        == f"log-foundry: ignoring inbound trace context (invalid trace_id): {echo}\n"
    )


def test_the_reason_is_escaped_and_never_cut(err: _Stderr) -> None:
    """``reason`` is a library literal: escaped like everything written, but not bounded."""
    reason = "r" * 150 + "\n" + "s" * 149

    _diag.rejected(reason, "x")

    out = err.getvalue()
    assert out.count("\n") == 1
    assert out == f"log-foundry: ignoring inbound trace context ({'r' * 150}\\n{'s' * 149}): 'x'\n"


def test_an_echo_that_cannot_be_computed_still_writes_the_line(
    err: _Stderr, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``"rejected"`` means the line was announced, so the echo failing must not lose it."""

    def _no_memory(value: object) -> str:
        raise MemoryError

    monkeypatch.setattr(_diag, "_echo", _no_memory)

    _diag.rejected("invalid trace_id", "x")

    assert err.getvalue() == "log-foundry: ignoring inbound trace context (invalid trace_id): <?>\n"


def test_a_baseexception_from_the_echo_propagates(
    err: _Stderr, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The guard catches ``Exception`` only; an interrupt mid-echo is the operator's intent."""

    def _interrupted(value: object) -> str:
        raise KeyboardInterrupt

    monkeypatch.setattr(_diag, "_echo", _interrupted)

    with pytest.raises(KeyboardInterrupt):
        _diag.rejected("invalid trace_id", "x")
    assert err.getvalue() == ""


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
        lambda: _diag.pending("a detail"),
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
    _diag.pending("a detail")


@pytest.mark.parametrize(
    "write",
    [
        lambda: _diag.absorbed("closing a span", _Boom()),
        lambda: _diag.lost("event", 3),
        lambda: _diag.rejected("unparseable traceparent", "00-bad"),
        lambda: _diag.pending("a detail"),
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
    """A raising ``__repr__`` is never called, so the line is written (SPEC-067 FR-001).

    Before SPEC-067 the echo was ``repr(value)``, inside the guard that covers the write, so the
    raise was swallowed with the line and ``continue_trace`` reported ``"rejected"`` — announced —
    with nothing on stderr.
    """

    class _Hostile:
        def __repr__(self) -> str:
            raise RuntimeError("boom")

    _diag.rejected("unusable baggage header", _Hostile())

    assert err.getvalue() == (
        "log-foundry: ignoring inbound trace context (unusable baggage header): <_Hostile>\n"
    )


# -- FR-004: the rules are documented where the next writer will look -----------------------


def test_module_docstring_states_the_rules() -> None:
    doc = _diag.__doc__ or ""
    assert "arch §6" in doc, "the type-name rule needs its justification"
    assert "PostgresSink" in doc, "the sharpest leak example"
    assert "BaseException" in doc, "the guard's one exclusion"
    assert "Record first, announce second" in doc
    assert "logging" in doc, "why stderr rather than the logging module"
    assert "An inbound value runs no code of its own" in doc, "SPEC-067's rule for rejected"


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


@pytest.mark.parametrize(
    "path", sorted(_SRC.rglob("*.py")), ids=lambda p: p.relative_to(_SRC).as_posix()
)
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
    where = path.relative_to(_SRC).as_posix()
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for call in _diag_calls(tree):
        for node in ast.walk(call):
            if isinstance(node, ast.FormattedValue):
                assert node.conversion != ord("r"), f"{where}: !r in a diagnostic"
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                assert node.func.id != "repr", f"{where}: repr() in a diagnostic"
            if isinstance(node, ast.Attribute):
                assert node.attr != "args", f"{where}: exception args in a diagnostic"


def test_the_enforcement_actually_sees_the_call_sites() -> None:
    """Guards the guard: a walker that matched nothing would pass every file vacuously."""
    calls = sum(
        len(_diag_calls(ast.parse(p.read_text(encoding="utf-8")))) for p in _SRC.rglob("*.py")
    )
    assert calls >= 30, f"expected the converted call sites to be found, saw {calls}"


def test_the_engines_diagnostics_are_on_the_roster() -> None:
    """SPEC-072 FR-003 AC9: the engine's lines are ``_diag.<writer>`` calls the scan above sees.

    A ``from log_foundry._diag import lost`` would be a bare-name call this walker never matches,
    so the repr check would pass the engine vacuously; pinning the exact count here stops that,
    and a writer added or removed is a visible edit here.
    """
    engine = _SRC / "sinks" / "_engine.py"
    writers = sorted(
        call.func.attr
        for call in _diag_calls(ast.parse(engine.read_text(encoding="utf-8")))
        if isinstance(call.func, ast.Attribute)
        and call.func.attr in {"lost", "absorbed", "pending"}
    )
    assert writers == ["absorbed", "absorbed", "absorbed", "lost", "lost"], (
        "the engine writes three absorbed lines and two lost lines — a settled run's loss and, "
        f"since SPEC-074, a settled run's refused events — as _diag calls: {writers}"
    )


_STDERR_NAMES = {"stderr", "__stderr__"}
_WRITE_METHODS = {"write", "writelines"}


def _stderr_writes(tree: ast.AST) -> list[ast.Call]:
    """Every ``sys.stderr.write(...)`` / ``stderr.write(...)`` call in a module.

    Matched on the AST, not by grepping the source: ``sinks/_socket.py`` names
    ``sys.stderr.write`` in a comment explaining what it replaced, and a text search would read
    that as a violation — the sort of false positive that gets an enforcement test deleted.

    Three shapes, because ``.write`` alone is not the one a person reaches for by accident:

    * ``sys.stderr.write(...)`` / ``.writelines(...)``, and the ``sys.__stderr__`` spelling.
    * ``print(..., file=sys.stderr)`` — the most common way a Python author writes to stderr at
      all, and invisible to ``ruff`` here (``T20`` is not in this repo's ``select``).
    * ``traceback.print_exc()`` and its siblings, which default to stderr. This is the one that
      matters most: it is the natural thing to reach for inside exactly the ``except Exception:``
      blocks this spec converted, it arrives unguarded, and it prints the exception's **message
      and stack** — so it reopens FR-002's leak as well as FR-003's.
    """
    found: list[ast.Call] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Attribute):
            target = func.value
            writes_a_stream = (
                func.attr in _WRITE_METHODS
                and isinstance(target, ast.Attribute)
                and target.attr in _STDERR_NAMES
            )
            prints_a_traceback = (
                func.attr.startswith("print_")
                and isinstance(target, ast.Name)
                and target.id == "traceback"
            )
            if writes_a_stream or prints_a_traceback:
                found.append(node)
        elif isinstance(func, ast.Name) and func.id == "print":
            found.extend(
                node
                for kw in node.keywords
                if kw.arg == "file"
                and isinstance(kw.value, ast.Attribute)
                and kw.value.attr in _STDERR_NAMES
            )
    return found


@pytest.mark.parametrize(
    "path",
    sorted(p for p in _SRC.rglob("*.py") if p != _SRC / "_diag.py"),
    ids=lambda p: p.relative_to(_SRC).as_posix(),
)
def test_only_diag_writes_to_stderr(path: Path) -> None:
    """One module owns every diagnostic line (SPEC-029 FR-001).

    The twenty-odd sites this spec converted disagreed with each other precisely because each one
    was written from scratch; a new bare write would start the drift again — and would arrive
    unguarded, which on the worker thread ends delivery for good.

    Referencing ``sys.stderr`` is still fine and deliberate: ``console.py`` and ``sinks/stdout.py``
    use it as a *destination for the user's events*, which is the opposite direction from a
    diagnostic about the library itself.
    """
    writes = _stderr_writes(ast.parse(path.read_text(encoding="utf-8")))
    assert not writes, (
        f"{path.relative_to(_SRC).as_posix()}:{writes[0].lineno} writes to stderr directly; "
        f"use _diag.absorbed / _diag.lost / _diag.rejected"
    )


@pytest.mark.parametrize(
    "source",
    [
        "import sys\nsys.stderr.write('x')\n",
        "import sys\nsys.stderr.writelines(['x'])\n",
        "import sys\nsys.__stderr__.write('x')\n",
        "import sys\nprint('x', file=sys.stderr)\n",
        "import traceback\ntraceback.print_exc()\n",
        "import traceback\ntraceback.print_exception(err)\n",
    ],
)
def test_the_stderr_walker_matches_every_shape_a_person_writes(source: str) -> None:
    """Guards the guard: a walker that matched nothing would pass every file vacuously."""
    assert _stderr_writes(ast.parse(source))


@pytest.mark.parametrize(
    "source",
    [
        "import sys\nfrom sys import stderr\nstderr.write('x')\n",
        "import sys\nout = sys.stderr\nout.write('x')\n",
        "import os\nos.write(2, b'x')\n",
    ],
)
def test_the_stderr_walker_states_what_it_does_not_catch(source: str) -> None:
    """A lint, not a sandbox.

    A stream rebound to a bare name, or a raw fd, is invisible to it. Recorded as a test so the
    limitation is a decision someone made rather than a gap they will discover — but none of
    these is a shape reached for by accident, which is the bar an enforcement lint has to clear.
    """
    assert _stderr_writes(ast.parse(source)) == []


def test_diag_still_has_exactly_the_five_writes_it_documents() -> None:
    """A tripwire, separate from the vacuity check above, and deliberately brittle.

    ``_diag``'s own docstring names four writers, one per kind of line: ``absorbed``, ``lost``,
    ``rejected`` and, since SPEC-068 FR-001, ``pending``, and since SPEC-103 FR-001 a fifth
    write, ``take_folded``'s, which writes the lines folded at exit. A sixth is fine — it just
    has to be a decision, taken with the docs in front of you, rather than a quiet sixth spelling
    of a line the others already write.
    """
    writes = _stderr_writes(ast.parse((_SRC / "_diag.py").read_text(encoding="utf-8")))
    assert len(writes) == 5, (
        f"_diag has {len(writes)} stderr writes, not 5 — if a write was added, update "
        f"_diag's docstring to match"
    )
    assert "Five stderr writes" in (_diag.__doc__ or ""), "the docstring names the five writes"


# -- SPEC-063 FR-004: the announcement names the type's stored name ---------------------------


def _stored_exception(**metaclass_attrs: object) -> type[BaseException]:
    """An exception class stored as ``Stored`` whose metaclass carries the given attributes."""
    meta = type("_Meta", (type,), dict(metaclass_attrs))
    return meta("Stored", (Exception,), {})  # type: ignore[no-any-return]


def _raising_name(cls: type) -> str:
    raise RuntimeError("name")


class _HijackName(str):
    """A ``str`` subclass for a stored ``__name__``: ``str()`` raises and formatting renames."""

    def __str__(self) -> str:
        raise RuntimeError("str")

    def __format__(self, spec: str) -> str:
        return "HIJACK"


def test_absorbed_names_the_stored_name_when_the_metaclass_name_raises(err: _Stderr) -> None:
    """FR-004 AC6: the line is written, naming ``Stored``, where it was swallowed whole."""
    _diag.absorbed("x", _stored_exception(__name__=property(_raising_name))())
    assert err.getvalue() == "log-foundry: absorbed a failure while x (Stored)\n"


def test_absorbed_is_not_hijacked_by_a_str_subclass_name(err: _Stderr) -> None:
    """FR-004 AC6: a ``str`` subclass stored as the name is written as its text, not ``HIJACK``."""
    exc_cls = type("Stored", (Exception,), {})
    exc_cls.__name__ = _HijackName("Stored")
    _diag.absorbed("x", exc_cls())
    assert err.getvalue() == "log-foundry: absorbed a failure while x (Stored)\n"


def test_type_name_reads_the_stored_name_past_a_raising_metaclass_name() -> None:
    """The metaclass property never runs, so the read cannot raise into a ``Raises: None`` path."""
    hostile = _stored_exception(__name__=property(_raising_name))()
    with pytest.raises(RuntimeError, match="name"):
        _ = type(hostile).__name__
    assert _diag.type_name(hostile) == "Stored"


def test_type_name_is_not_renamed_by_a_metaclass_name_property() -> None:
    """A property answering another string is not consulted: the type is named for what it stores."""
    renamed = _stored_exception(__name__=property(lambda cls: "Renamed"))()
    assert type(renamed).__name__ == "Renamed"
    assert _diag.type_name(renamed) == "Stored"


def test_type_name_returns_a_str_subclass_name_as_exact_text() -> None:
    """A ``str`` subclass stored as the name comes back as an exact ``str``, its methods unrun."""
    exc_cls = type("Stored", (Exception,), {})
    exc_cls.__name__ = _HijackName("Stored")
    name = _diag.type_name(exc_cls())
    assert type(name) is str
    assert f"{name}" == "Stored"


def test_an_orphan_loss_from_a_hostile_exception_class_is_counted_and_announced(
    err: _Stderr,
) -> None:
    """FR-004 AC6: the loss is counted and the line names ``Stored``; it was counted in silence."""
    import log_foundry as lf

    hostile = _stored_exception(__name__=property(_raising_name))

    class _RaisingSink:
        def emit(self, batch: list[dict[str, object]]) -> None:
            raise hostile()

        def close(self) -> None:
            return None

    lf.configure(service="diag", sink=_RaisingSink())  # type: ignore[arg-type]
    before = lf.health().orphan_lost
    try:
        lf.info("m")
    except BaseException:
        pytest.fail("the hostile class's exception escaped lf.info: the orphan guard regressed")
    assert lf.health().orphan_lost == before + 1
    assert "absorbed a failure while emitting an orphan log (Stored)" in err.getvalue()


# -- SPEC-103 FR-001: the fold ----------------------------------------------------------------

_TAIL = "line(s) folded at exit"


def _folding(table: _diag.FoldTable, write: Callable[[], None]) -> None:
    """Runs ``write`` with this thread's fold window open on ``table``, then withdraws it."""
    _diag.fold_into(table)
    try:
        write()
    finally:
        _diag.fold_into(None)


def test_each_writer_folds_inside_the_window_and_take_writes_one_line_per_key(
    err: _Stderr,
) -> None:
    """Inside the window nothing is written; the take writes one line per key, first seen first."""
    table = _diag.FoldTable()

    def write() -> None:
        for count in (2, 3):
            _diag.lost("event", count, "Sink, refused")
            _diag.absorbed("sending a batch", ConnectionError(), "Sink")
            _diag.pending("Sink, a request may still land")
        _diag.lost("event", 7)

    _folding(table, write)
    assert err.getvalue() == "", "a folded line was written inside the window"
    _diag.take_folded(table, close=False)
    assert err.getvalue().splitlines() == [
        f"log-foundry: lost 5 event(s); Sink, refused; 2 {_TAIL}",
        (
            "log-foundry: absorbed 2 failure(s) while sending a batch (ConnectionError); Sink; "
            f"2 {_TAIL}"
        ),
        f"log-foundry: Sink, a request may still land; 2 {_TAIL}",
        f"log-foundry: lost 7 event(s); 1 {_TAIL}",
    ]
    assert table.rows == {}, "the take clears the table"


def test_rejected_never_folds(err: _Stderr) -> None:
    """``rejected`` is written at once inside a window: no shipped sink and no engine reach it."""
    table = _diag.FoldTable()
    _folding(table, lambda: _diag.rejected("unparseable traceparent", "x"))
    assert "ignoring inbound trace context" in err.getvalue()
    assert table.rows == {}


def test_two_details_that_render_alike_are_one_key(err: _Stderr) -> None:
    """The key is the rendered detail: an escaped newline and a cut past the bound fold together."""
    table = _diag.FoldTable()
    long_a = "x" * _diag._MAX_DETAIL + "a"
    long_b = "x" * _diag._MAX_DETAIL + "b"

    def write() -> None:
        _diag.lost("event", 1, "a\nb")
        _diag.lost("event", 1, "a\\nb")
        _diag.pending(long_a)
        _diag.pending(long_b)

    _folding(table, write)
    _diag.take_folded(table, close=False)
    lines = err.getvalue().splitlines()
    assert lines[0] == f"log-foundry: lost 2 event(s); a\\nb; 2 {_TAIL}", lines
    assert lines[1].endswith(f"…; 2 {_TAIL}"), lines
    assert len(lines) == 2, lines


def test_the_tail_survives_a_detail_cut_at_the_bound(err: _Stderr) -> None:
    """The tail is appended after the detail is bounded, so a long detail never cuts it."""
    table = _diag.FoldTable()
    _folding(table, lambda: _diag.lost("event", 4, "y" * 500))
    _diag.take_folded(table, close=False)
    line = err.getvalue().rstrip("\n")
    assert line.endswith(f"y…; 1 {_TAIL}"), line


def test_a_running_total_inside_the_window_is_written_at_once(err: _Stderr) -> None:
    """``running_total=True`` writes at once: summing running totals overstates a loss."""
    table = _diag.FoldTable()
    _folding(table, lambda: _diag.lost("submission", 1000, "cumulative", running_total=True))
    assert err.getvalue() == "log-foundry: lost 1000 submission(s); cumulative\n"
    assert table.rows == {}


def test_a_count_that_is_not_an_exact_int_is_written_at_once(err: _Stderr) -> None:
    """Only ``int`` arithmetic runs under the fold lock, so any other count is written at once."""
    table = _diag.FoldTable()
    _folding(table, lambda: _diag.lost("event", True, "a bool"))
    assert err.getvalue() == "log-foundry: lost True event(s); a bool\n"
    assert table.rows == {}


def test_a_record_into_a_closed_table_is_written_at_once(err: _Stderr) -> None:
    """After ``stop()``'s closing take, a record is written at once, unfolded, and never held."""
    table = _diag.FoldTable()
    _folding(table, lambda: _diag.lost("event", 1, "before"))
    _diag.take_folded(table, close=True)
    assert table.closed
    _folding(table, lambda: _diag.lost("event", 1, "after"))
    assert err.getvalue().splitlines() == [
        f"log-foundry: lost 1 event(s); before; 1 {_TAIL}",
        "log-foundry: lost 1 event(s); after",
    ]
    assert table.rows == {}


def test_take_clears_the_table_in_place_and_keeps_its_object(err: _Stderr) -> None:
    """The open window holds the table object, so the take clears its rows rather than rebinding."""
    table = _diag.FoldTable()
    rows = table.rows
    _folding(table, lambda: _diag.lost("event", 1, "d"))
    _diag.take_folded(table, close=False)
    assert table.rows is rows
    assert rows == {}
    _diag.take_folded(table, close=False)
    assert err.getvalue().count(_TAIL) == 1, "a taken row is written once"


def _held_on_another_thread(lock: threading.Lock) -> tuple[threading.Event, threading.Thread]:
    """Holds ``lock`` on a daemon thread until the returned event is set."""
    held, release = threading.Event(), threading.Event()

    def hold() -> None:
        with lock:
            held.set()
            release.wait(10)

    thread = threading.Thread(target=hold, daemon=True)
    thread.start()
    assert held.wait(5)
    return release, thread


_EACH_WRITER: dict[str, tuple[Callable[[], None], str]] = {
    "lost": (lambda: _diag.lost("event", 2, "held"), "log-foundry: lost 2 event(s); held\n"),
    "absorbed": (
        lambda: _diag.absorbed("sending a batch", ValueError(), "held"),
        "log-foundry: absorbed a failure while sending a batch (ValueError); held\n",
    ),
    "pending": (lambda: _diag.pending("held"), "log-foundry: held\n"),
}
"""One call of each folding writer, and the line it writes when it does not fold."""


@pytest.mark.parametrize("writer", sorted(_EACH_WRITER))
def test_a_record_never_waits_on_the_fold_lock(writer: str, err: _Stderr) -> None:
    """A recorder that cannot take ``_FOLD_LOCK`` at once writes its line at once, unfolded.

    Each folding writer carries its own inline acquire, so each is checked: a blocking acquire
    in one of them waits out the other thread's hold, which here is 10 s.
    """
    write, line = _EACH_WRITER[writer]
    table = _diag.FoldTable()
    release, thread = _held_on_another_thread(_diag._FOLD_LOCK)
    try:
        started = time.monotonic()
        _folding(table, write)
        waited = time.monotonic() - started
    finally:
        release.set()
        thread.join(5)
    assert waited < 1.0, f"{writer} waited {waited:.2f}s on the fold lock"
    assert err.getvalue() == line
    assert table.rows == {}


class _RecordingLock:
    """A stand-in for ``_FOLD_LOCK`` that records how it was asked for and is never free.

    Attributes:
      calls: The ``(args, kwargs)`` of every ``acquire`` call, in order.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def acquire(self, *args: object, **kwargs: object) -> bool:
        """Records the call and refuses, as a lock held elsewhere would."""
        self.calls.append((args, kwargs))
        return False

    def release(self) -> None:
        """Never reached: nothing is acquired."""
        raise AssertionError("released a lock that was never acquired")


@pytest.mark.parametrize("writer", sorted(_EACH_WRITER))
def test_each_writer_asks_for_the_fold_lock_without_waiting(
    writer: str, err: _Stderr, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each writer asks for ``_FOLD_LOCK`` with ``blocking=False`` and nothing else.

    The timing test above passes a bounded wait shorter than its bound, such as
    ``acquire(timeout=0.5)``, which still waits on the thread that holds the lock: a finalizer
    re-entering there would stall for it on every line. So the call itself is asserted.
    """
    write, line = _EACH_WRITER[writer]
    lock = _RecordingLock()
    monkeypatch.setattr(_diag, "_FOLD_LOCK", lock)
    table = _diag.FoldTable()
    _folding(table, write)
    assert lock.calls == [((), {"blocking": False})], f"{writer} asked for the lock: {lock.calls}"
    assert err.getvalue() == line
    assert table.rows == {}


def test_the_drain_take_gives_way_once_the_table_is_closed(err: _Stderr) -> None:
    """The drain's take returns once a closer has the table, rather than waiting on its lock.

    The ``shutdown()`` caller holds the lock inside its closing take, a signal handler there can
    call ``shutdown()`` again and wait for the drain to settle, and a drain take that waited for
    the lock without bound then never returned (SPEC-103 diff review frame 2).
    """
    table = _diag.FoldTable()
    _folding(table, lambda: _diag.lost("event", 3, "held by the closer"))
    release, thread = _held_on_another_thread(_diag._FOLD_LOCK)
    try:
        table.closed = True
        started = time.monotonic()
        _diag.take_folded(table, close=False)
        waited = time.monotonic() - started
    finally:
        release.set()
        thread.join(5)
    assert waited < _diag._CLOSING_TAKE_WAIT + 2.0, waited
    assert list(table.rows) == [("lost", "event", "; held by the closer")], "the rows are kept"
    assert err.getvalue() == ""


def test_the_drain_take_keeps_waiting_while_the_table_is_open(err: _Stderr) -> None:
    """While no closer has the table, the drain's take waits for the lock and then writes."""
    table = _diag.FoldTable()
    _folding(table, lambda: _diag.lost("event", 3, "waited for"))
    held, release = threading.Event(), threading.Event()

    def hold() -> None:
        with _diag._FOLD_LOCK:
            held.set()
            release.wait(10)

    holder = threading.Thread(target=hold, daemon=True)
    holder.start()
    assert held.wait(5)
    taking = threading.Thread(target=_diag.take_folded, args=(table,), kwargs={"close": False})
    taking.start()
    taking.join(4 * _diag._CLOSING_TAKE_WAIT)
    still_waiting = taking.is_alive()
    release.set()
    holder.join(5)
    taking.join(5)
    assert still_waiting, "the drain take gave up while the table was open"
    assert err.getvalue() == f"log-foundry: lost 3 event(s); waited for; 1 {_TAIL}\n"


def test_the_closing_take_waits_for_the_fold_lock_a_bounded_time(err: _Stderr) -> None:
    """``stop()``'s take closes the table at once and leaves the rows when the lock stays held."""
    table = _diag.FoldTable()
    _folding(table, lambda: _diag.lost("event", 3, "kept"))
    release, thread = _held_on_another_thread(_diag._FOLD_LOCK)
    try:
        started = time.monotonic()
        _diag.take_folded(table, close=True)
        waited = time.monotonic() - started
    finally:
        release.set()
        thread.join(5)
    assert waited < _diag._CLOSING_TAKE_WAIT + 2.0, waited
    assert table.closed, "closing is a plain store, made before the lock is asked for"
    assert list(table.rows) == [("lost", "event", "; kept")], "the rows stay for the drain's take"
    assert err.getvalue() == ""
    _diag.take_folded(table, close=False)
    assert err.getvalue() == f"log-foundry: lost 3 event(s); kept; 1 {_TAIL}\n"


def test_withdraw_in_child_clears_the_window_while_another_thread_holds_the_fold_lock() -> None:
    """The child's first step clears this thread's window and takes no lock."""
    _diag.fold_into(_diag.FoldTable())
    release, thread = _held_on_another_thread(_diag._FOLD_LOCK)
    try:
        _diag.withdraw_fold_in_child()
        assert _diag._FOLD.table is None
    finally:
        release.set()
        thread.join(5)
        _diag.fold_into(None)


def test_the_window_is_this_threads_alone() -> None:
    """A thread that did not open a window reads none, so a sink's own thread writes at once."""
    seen: list[object] = []
    _diag.fold_into(_diag.FoldTable())
    try:
        other = threading.Thread(target=lambda: seen.append(_diag._FOLD.table))
        other.start()
        other.join(5)
    finally:
        _diag.fold_into(None)
    assert seen == [None]


_UNBUILT_CALLS = {
    "lost": [
        ("call", "lost"),
        ("call", "_escape"),
        ("c_call", "str.isprintable"),
        ("call", "_suffix"),
        ("call", "_bounded"),
        ("call", "_escape"),
        ("c_call", "str.isprintable"),
        ("c_call", "len"),
        ("c_call", "StringIO.write"),
    ],
    "lost bare": [
        ("call", "lost"),
        ("call", "_escape"),
        ("c_call", "str.isprintable"),
        ("call", "_suffix"),
        ("c_call", "StringIO.write"),
    ],
    "absorbed": [
        ("call", "absorbed"),
        ("call", "_escape"),
        ("c_call", "str.isprintable"),
        ("call", "_escape"),
        ("c_call", "str.isprintable"),
        ("call", "_suffix"),
        ("call", "_bounded"),
        ("call", "_escape"),
        ("c_call", "str.isprintable"),
        ("c_call", "len"),
        ("c_call", "StringIO.write"),
    ],
    "pending": [
        ("call", "pending"),
        ("call", "_bounded"),
        ("call", "_escape"),
        ("c_call", "str.isprintable"),
        ("c_call", "len"),
        ("c_call", "StringIO.write"),
    ],
}
"""Every call each writer made outside a window before SPEC-103, measured at ``1a86ad87``.

Recorded with ``sys.setprofile`` on CPython 3.13.1 and 3.12.12, identically. Python frames and C
calls both: ``tests/test_event_ledger_defects.py::test_p11_counts_every_assembly_fault`` runs at a
recursion limit of 400, and a fold check written as a helper adds a frame to every line the
library writes. This is the guard for that rather than p11 itself, which a helper-based check
passed in the plan review (SPEC-103 FR-001).
"""

_WRITER_CALLS: dict[str, Callable[[], None]] = {
    "lost": lambda: _diag.lost("event", 3, "detail"),
    "lost bare": lambda: _diag.lost("event", 3),
    "absorbed": lambda: _diag.absorbed("sending", ConnectionError(), "detail"),
    "pending": lambda: _diag.pending("detail"),
}


@pytest.mark.parametrize("writer", sorted(_WRITER_CALLS))
def test_a_writer_outside_the_window_calls_what_the_unbuilt_writer_called(
    writer: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FR-001: outside a window each writer calls exactly what it called before the fold."""
    monkeypatch.setattr(sys, "stderr", io.StringIO())
    call = _WRITER_CALLS[writer]
    seen: list[tuple[str, str]] = []

    def profile(frame: types.FrameType, event: str, arg: object) -> None:
        if event == "call" and frame.f_code is not call.__code__:
            seen.append(("call", frame.f_code.co_qualname))
        elif event == "c_call":
            seen.append(("c_call", getattr(arg, "__qualname__", repr(arg))))

    sys.setprofile(profile)
    try:
        call()
    finally:
        sys.setprofile(None)
    assert [s for s in seen if s != ("c_call", "setprofile")] == _UNBUILT_CALLS[writer]


# -- SPEC-103 FR-001: every throttled ``lost`` passes ``running_total`` -----------------------

_THROTTLED_WITHOUT = (
    "a lost call in a function that reads WARN_EVERY does not pass running_total=True"
)


def _reads_warn_every(function: ast.AST) -> bool:
    """Whether a function's body reads ``WARN_EVERY``, as a bare name or as an attribute."""
    return any(
        (isinstance(node, ast.Name) and node.id == "WARN_EVERY")
        or (isinstance(node, ast.Attribute) and node.attr == "WARN_EVERY")
        for node in ast.walk(function)
    )


def _is_lost_call(node: ast.AST) -> bool:
    """Whether a node is a call of ``_diag.lost`` or of a bare ``lost``."""
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Name):
        return func.id == "lost"
    return (
        isinstance(func, ast.Attribute)
        and func.attr == "lost"
        and isinstance(func.value, ast.Name)
        and func.value.id == "_diag"
    )


def _passes_running_total(call: ast.Call) -> bool:
    """Whether a call passes ``running_total=True`` as a literal."""
    return any(
        k.arg == "running_total" and isinstance(k.value, ast.Constant) and k.value.value is True
        for k in call.keywords
    )


def throttled_lost_calls(source: str) -> list[tuple[str, int, bool]]:
    """Each ``lost`` call in a function reading ``WARN_EVERY``: ``(function, line, passes it)``."""
    found: dict[int, tuple[str, int, bool]] = {}
    for function in ast.walk(ast.parse(source)):
        if not isinstance(function, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        if not _reads_warn_every(function):
            continue
        for node in ast.walk(function):
            if isinstance(node, ast.Call) and _is_lost_call(node):
                found.setdefault(
                    id(node), (function.name, node.lineno, _passes_running_total(node))
                )
    return sorted(found.values(), key=lambda site: site[1])


def running_total_findings(source: str, where: str) -> list[tuple[str, str]]:
    """The roster's refusals in one module, each ``(reason, site)``."""
    return [
        (_THROTTLED_WITHOUT, f"{where}:{line} {name}")
        for name, line, passes in throttled_lost_calls(source)
        if not passes
    ]


def test_every_throttled_lost_call_passes_running_total() -> None:
    """FR-001: a throttled site passes a running total, which a fold window must never sum."""
    findings = [
        finding
        for path in sorted(_SRC.rglob("*.py"))
        for finding in running_total_findings(
            path.read_text(encoding="utf-8"), path.relative_to(_SRC).as_posix()
        )
    ]
    assert findings == [], findings


def test_the_running_total_roster_finds_todays_three_sites() -> None:
    """The roster is derived from all of ``src/``, and it sees the three throttled sites."""
    sites = [
        (path.relative_to(_SRC).as_posix(), name)
        for path in sorted(_SRC.rglob("*.py"))
        for name, _line, _passes in throttled_lost_calls(path.read_text(encoding="utf-8"))
    ]
    assert sites == [
        ("context.py", "_announce_dropped"),
        ("worker.py", "_announce"),
        ("worker.py", "_announce"),
    ]


_THROTTLE = "from log_foundry import _diag\ndef f(total):\n    if total % _diag.WARN_EVERY == 0:\n"

_ROSTER_REFUSES = {
    "no keyword": _THROTTLE + "        _diag.lost('event', total, 'cumulative')\n",
    "running_total=False": _THROTTLE + "        _diag.lost('event', total, running_total=False)\n",
    "a variable": _THROTTLE + "        _diag.lost('event', total, running_total=flag)\n",
    "a bare WARN_EVERY and a bare lost": (
        "from log_foundry._diag import WARN_EVERY, lost\ndef f(total):\n"
        "    if total % WARN_EVERY == 0:\n        lost('event', total)\n"
    ),
}

_ROSTER_IS_SILENT = {
    "an unthrottled lost": "from log_foundry import _diag\ndef f():\n    _diag.lost('event', 1)\n",
    "a throttled absorbed": _THROTTLE + "        _diag.absorbed('x', ValueError(), 'total')\n",
    "a throttled lost with the keyword": (
        _THROTTLE + "        _diag.lost('event', total, running_total=True)\n"
    ),
}


@pytest.mark.parametrize("case", sorted(_ROSTER_REFUSES))
def test_the_running_total_roster_refuses_its_shapes(case: str) -> None:
    """Each shape is refused, for the reason the roster exists."""
    findings = running_total_findings(_ROSTER_REFUSES[case], "fixture.py")
    assert [reason for reason, _site in findings] == [_THROTTLED_WITHOUT], findings


@pytest.mark.parametrize("case", sorted(_ROSTER_IS_SILENT))
def test_the_running_total_roster_is_silent_on_its_shapes(case: str) -> None:
    """Half a gate's regressions are false positives: these must pass."""
    assert running_total_findings(_ROSTER_IS_SILENT[case], "fixture.py") == []


# -- SPEC-103 diff review frame 2: a snapshot count is not an increment either ----------------

_SNAPSHOT_SITES = frozenset({("_lifecycle.py", "_check_ledger")})
"""The ``lost`` sites whose count is a reading rather than an increment, by file and function.

A throttled running total is derived (``WARN_EVERY``); a snapshot cannot be, so it is listed.
``_check_ledger`` writes ``unaccounted`` from a ledger reading at the end of every
``shutdown()``, which a sink calling ``shutdown()`` from its ``emit`` reaches inside an exit fold
window: folded, three such lines read ``lost 123`` against a true 42. A sweep of every
``_diag.lost`` call in ``src/`` at SPEC-103's diff review found no other count that is not an
increment. **A new snapshot site has to be added here by hand**; what the checks below hold is
that each listed site passes ``running_total=True``, that no entry is stale, and that nothing
passes it without one of the two rosters saying why.
"""

_SNAPSHOT_MISSING = "a snapshot lost call does not pass running_total=True"
_SNAPSHOT_STALE = "a snapshot roster entry names no lost call"
_RUNNING_TOTAL_UNLISTED = "running_total=True where neither roster says the count is not one"


def snapshot_findings(
    source: str, where: str, roster: frozenset[tuple[str, str]]
) -> list[tuple[str, str]]:
    """The snapshot roster's refusals in one module, each ``(reason, site)``."""
    findings: list[tuple[str, str]] = []
    listed = {name for file, name in roster if file == where}
    seen: set[str] = set()
    throttled = {line for _name, line, _passes in throttled_lost_calls(source)}
    for function in ast.walk(ast.parse(source)):
        if not isinstance(function, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        for node in ast.walk(function):
            if not (isinstance(node, ast.Call) and _is_lost_call(node)):
                continue
            site = f"{where}:{node.lineno} {function.name}"
            if function.name in listed:
                seen.add(function.name)
                if not _passes_running_total(node):
                    findings.append((_SNAPSHOT_MISSING, site))
            elif _passes_running_total(node) and node.lineno not in throttled:
                findings.append((_RUNNING_TOTAL_UNLISTED, site))
    findings.extend((_SNAPSHOT_STALE, f"{where} {name}") for name in sorted(listed - seen))
    return sorted(set(findings))


def test_every_snapshot_lost_call_passes_running_total() -> None:
    """Every listed snapshot site passes it, no entry is stale, and no other site passes it."""
    findings = [
        finding
        for path in sorted(_SRC.rglob("*.py"))
        for finding in snapshot_findings(
            path.read_text(encoding="utf-8"), path.relative_to(_SRC).as_posix(), _SNAPSHOT_SITES
        )
    ]
    assert findings == [], findings


_FIXTURE_ROSTER = frozenset({("fixture.py", "reading")})

_SNAPSHOT_REFUSES = {
    "a listed site without the keyword": (
        "from log_foundry import _diag\ndef reading(n):\n    _diag.lost('event', n)\n",
        _SNAPSHOT_MISSING,
    ),
    "a listed function with no lost call": (
        "from log_foundry import _diag\ndef reading(n):\n    return n\n",
        _SNAPSHOT_STALE,
    ),
    "an unlisted site passing the keyword": (
        (
            "from log_foundry import _diag\ndef reading(n):\n"
            "    _diag.lost('event', n, running_total=True)\n"
            "def other(n):\n    _diag.lost('event', n, running_total=True)\n"
        ),
        _RUNNING_TOTAL_UNLISTED,
    ),
}

_SNAPSHOT_IS_SILENT = {
    "a listed site with the keyword": (
        "from log_foundry import _diag\ndef reading(n):\n"
        "    _diag.lost('event', n, running_total=True)\n"
    ),
    "an unlisted increment": (
        "from log_foundry import _diag\ndef reading(n):\n    _diag.lost('event', n, running_total=True)\n"
        "def other():\n    _diag.lost('event', 1)\n"
    ),
    "an unlisted throttled site with the keyword": (
        "from log_foundry import _diag\ndef reading(n):\n    _diag.lost('event', n, running_total=True)\n"
        "def other(n):\n    if n % _diag.WARN_EVERY == 0:\n"
        "        _diag.lost('event', n, running_total=True)\n"
    ),
}


@pytest.mark.parametrize("case", sorted(_SNAPSHOT_REFUSES))
def test_the_snapshot_roster_refuses_its_shapes(case: str) -> None:
    """Each shape is refused, for its own reason."""
    source, reason = _SNAPSHOT_REFUSES[case]
    findings = snapshot_findings(source, "fixture.py", _FIXTURE_ROSTER)
    assert [found for found, _site in findings] == [reason], findings


@pytest.mark.parametrize("case", sorted(_SNAPSHOT_IS_SILENT))
def test_the_snapshot_roster_is_silent_on_its_shapes(case: str) -> None:
    """Half a gate's regressions are false positives: these must pass."""
    assert snapshot_findings(_SNAPSHOT_IS_SILENT[case], "fixture.py", _FIXTURE_ROSTER) == []
