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

from conftest import armed, object_probe, str_probe
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


def test_diag_still_has_exactly_the_three_writers_it_documents() -> None:
    """A tripwire, separate from the vacuity check above, and deliberately brittle.

    ``architecture.md`` §6 and ``_diag``'s own docstring both describe three writers. A fourth is
    fine — it just has to be a decision, taken with the docs in front of you, rather than a
    quiet fourth spelling of a line the other three already write.
    """
    writes = _stderr_writes(ast.parse((_SRC / "_diag.py").read_text(encoding="utf-8")))
    assert len(writes) == 3, (
        f"_diag has {len(writes)} stderr writes, not 3 — if a writer was added, update "
        f"_diag's docstring and architecture.md §6 to match"
    )


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
