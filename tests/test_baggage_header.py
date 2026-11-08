"""SPEC-064 — the outbound baggage header is bounded, total, and capped by whole members.

`current_baggage_header()` used to render every value with `str()` and percent-encode the result,
so a shared-sublist DAG never returned, a raising `__str__`, a lone surrogate or a huge integer
raised into the caller, and nothing kept the header under the size the next process's parser
accepts. It now renders the value event assembly coerces, guards each member's render, and keeps
whole members up to `BAGGAGE_MAX_BYTES`.

Every expected header below is a literal, never an expression computed from `coerce`: a test that
derives its expectation from the code under test passes against whatever that code does.

Two routes reach the render guard. The spoofed-`__class__` objects reach `str()` through
coercion's `isinstance(value, bool)` branch, which SPEC-063 removes; each coercion-route test
asserts that premise first, so it fails loudly rather than passing vacuously once that branch is
gone. The route-independent variants hand the hostile object straight to the render by replacing
`sanitize.sanitize_fields`, so the guard stays tested whichever spec lands first.
"""

from __future__ import annotations

import contextlib
import itertools
import signal
import threading
import time
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import quote, unquote

import pytest

import log_foundry
from log_foundry import context, sanitize
from log_foundry.config import Config

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping

_CAP = 8192
_ROUTES = ("in_span", "orphan")


@pytest.fixture(autouse=True)
def _fresh_drop_count(monkeypatch: pytest.MonkeyPatch) -> None:
    """Gives each test its own running total, so a first drop is draw 1 whatever ran before.

    `raising=False` because the unfixed tree has no such attribute, and the run against it must
    fail on the behaviour each test names, not on this fixture.
    """
    monkeypatch.setattr(context, "_dropped_members", itertools.count(1), raising=False)


# -- the deadline (SPEC-057's alarm harness, copied: test modules here do not import each other) --


class _Deadline(BaseException):
    """What the alarm raises: a `BaseException`, because the render's guard catches `Exception`."""


@contextlib.contextmanager
def _deadline(seconds: float, what: str) -> Iterator[None]:
    """Fails the test, instead of hanging the run, when the body has not returned in time."""
    assert threading.current_thread() is threading.main_thread(), (
        "SIGALRM reaches only the main thread, so the deadline cannot fire here"
    )

    def _ring(signum: int, frame: object) -> None:
        raise _Deadline

    previous = signal.signal(signal.SIGALRM, _ring)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    except _Deadline:
        pytest.fail(f"{what} did not return within {seconds} s", pytrace=False)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def _fan_out() -> object:
    """`y = 0`, then `y = [y] * 16` seven times: 16^7 paths to one leaf."""
    shape: object = 0
    for _ in range(7):
        shape = [shape] * 16
    return shape


def _header(**kv: object) -> str:
    """Sets baggage with no span open and returns the header for it."""
    context.set_baggage(**kv)
    return context.current_baggage_header()


def _lines(err: str) -> list[str]:
    return [line for line in err.splitlines() if "baggage member" in line]


# -- hostile values -------------------------------------------------------------------------------


class _Halt(BaseException):
    """A `BaseException` from a value's own `__str__`, which must reach the caller."""


class _RaisingEncode(str):
    """A `str` subclass that gets past `str()` and then raises inside `quote`."""

    def encode(self, *args: object, **kwargs: object) -> bytes:  # type: ignore[override]
        raise RuntimeError("encode-secret")


class _NamelessMeta(type):
    """A metaclass whose `__name__` raises while armed, so even naming the type fails.

    Armed only around the render by `_nameless()`. Raising unconditionally breaks pytest itself:
    when a test holding such an object fails, `saferepr` reads `cls.__name__` and the run ends in
    INTERNALERROR instead of a failure that names the test.
    """

    armed = False

    @property
    def __name__(cls) -> str:  # type: ignore[override]
        if _NamelessMeta.armed:
            raise RuntimeError("name-secret")
        return type.__dict__["__name__"].__get__(cls)  # type: ignore[no-any-return]


@contextlib.contextmanager
def _nameless() -> Iterator[None]:
    """Arms `_NamelessMeta` for the body only, and always disarms it before any assertion."""
    _NamelessMeta.armed = True
    try:
        yield
    finally:
        _NamelessMeta.armed = False


class _ClaimsBoolRaises:
    """Claims `bool` through `__class__`, refused by coercion since SPEC-063; `__str__` raises."""

    @property
    def __class__(self) -> type:  # type: ignore[override]
        return bool

    def __str__(self) -> str:
        raise RuntimeError("secret")


class _ClaimsBoolSubStr:
    """Claims `bool`; its `__str__` returns a `str` subclass whose `encode` raises."""

    @property
    def __class__(self) -> type:  # type: ignore[override]
        return bool

    def __str__(self) -> str:
        return _RaisingEncode("payload-secret")


class _ClaimsBoolNameless(metaclass=_NamelessMeta):
    """Claims `bool`; its type's name cannot be read and its `__str__` raises."""

    @property
    def __class__(self) -> type:  # type: ignore[override]
        return bool

    def __str__(self) -> str:
        raise RuntimeError("secret")


class _ClaimsBoolHalts:
    """Claims `bool`; its `__str__` raises a `BaseException`."""

    @property
    def __class__(self) -> type:  # type: ignore[override]
        return bool

    def __str__(self) -> str:
        raise _Halt


class _ClaimsBoolSpins:
    """Claims `bool`; its `__str__` spins for up to 3 s of wall clock, for the alarm's control."""

    @property
    def __class__(self) -> type:  # type: ignore[override]
        return bool

    def __str__(self) -> str:
        stop = time.monotonic() + 3.0
        while time.monotonic() < stop:
            pass
        return "spun out"


class Boom:
    """An ordinary object whose `__str__` raises; coercion never calls it."""

    def __str__(self) -> str:
        raise RuntimeError("secret")


def _assert_refused_by_coercion(value: object) -> None:
    """The coercion-route premise since SPEC-063: the spoof becomes coercion's own placeholder."""
    coerced = sanitize.sanitize_fields({"v": value}, cfg=Config())[0]["v"]
    assert coerced == f"<unserializable: {type.__dict__['__name__'].__get__(type(value))}>", (
        "coercion returned this spoof as something other than its real-type placeholder"
    )


@pytest.fixture
def bypass_coercion(monkeypatch: pytest.MonkeyPatch) -> None:
    """Hands the render the baggage uncoerced, so a hostile value reaches it on any coercer."""

    def _identity(fields: Mapping[str, object], *, cfg: Config) -> tuple[dict[str, object], bool]:
        return dict(fields), False

    monkeypatch.setattr(sanitize, "sanitize_fields", _identity)


# -- FR-001: the header is rendered through assembly's bounded coercion --------------------------


def test_the_deadline_escapes_the_render_guard(bypass_coercion: None) -> None:
    """The harness's positive control: the alarm stops a render through `except Exception`.

    Were the guard to catch `BaseException`, it would turn the interrupted render into a
    placeholder and return, and `pytest.fail` would not be raised.
    """
    with (
        pytest.raises(pytest.fail.Exception, match=r"did not return within 0\.2 s"),
        _deadline(0.2, "the spinning render"),
    ):
        _header(v=_ClaimsBoolSpins())


@pytest.mark.parametrize("route", _ROUTES)
def test_a_shared_sublist_dag_header_returns_within_a_cpu_second(lf, route: str) -> None:
    """AC1: 16^7 visits under `str`; one budgeted pass here, then the cap makes it `""`."""

    def call() -> tuple[str, float]:
        lf.set_baggage(a=_fan_out())
        start = time.thread_time()
        header = lf.current_baggage_header()
        return header, time.thread_time() - start

    with _deadline(5, "current_baggage_header() on the DAG"):
        header, spent = lf.trace(call)() if route == "in_span" else call()
    assert spent < 1.0, f"{route}: {spent:.3f} s of CPU"
    assert header == "", "the coerced DAG renders to ~385 KB, past the cap"


@pytest.mark.parametrize("route", _ROUTES)
def test_a_node_limited_dag_header_fits_and_carries_the_placeholder(
    lf, route: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC1, second half: with a 64-node budget the header is under the cap and is cut there."""
    monkeypatch.setattr(sanitize, "_MAX_NODES", 64)

    def call() -> str:
        lf.set_baggage(a=_fan_out())
        return lf.current_baggage_header()

    with _deadline(5, "current_baggage_header() on the DAG"):
        header = lf.trace(call)() if route == "in_span" else call()
    assert 0 < len(unquote(header)) < _CAP
    assert "<node limit>" in unquote(header)


class _E(Enum):
    A = "a"


@pytest.mark.parametrize(
    ("key", "value", "expected"),
    [
        ("d", datetime(2026, 1, 1), "d=2026-01-01T00%3A00%3A00"),
        ("e", _E.A, "e=a"),
        ("p", Path("/srv/x"), "p=%3Cunserializable%3A%20PosixPath%3E"),
        ("tu", (1, 2), "tu=%5B1%2C%202%5D"),
        ("se", {1, 2}, "se=%5B1%2C%202%5D"),
        ("b", b"ab", "b=ab"),
        ("f", float("nan"), "f=%3Cfloat%3A%20nan%3E"),
        ("t", True, "t=True"),
        ("n", None, "n=None"),
        ("l", ["x"], "l=%5B%27x%27%5D"),
        ("m", {"a": 1}, "m=%7B%27a%27%3A%201%7D"),
    ],
    ids=[
        "datetime",
        "enum",
        "path",
        "tuple",
        "set",
        "bytes",
        "nan",
        "true",
        "none",
        "list",
        "dict",
    ],
)
def test_the_header_renders_the_coerced_value(key: str, value: object, expected: str) -> None:
    """AC2: each table row, as the literal header text."""
    assert _header(**{key: value}) == expected


def test_members_past_max_keys_are_dropped_as_assembly_drops_them(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """AC2: 300 members, 256 emitted; a `max_keys` drop is coercion's, so it is not announced."""
    header = _header(**{f"k{i}": i for i in range(300)})
    assert [member.split("=")[0] for member in header.split(",")] == [f"k{i}" for i in range(256)]
    assert _lines(capsys.readouterr().err) == []


def test_a_string_past_max_value_bytes_is_clipped() -> None:
    """AC3: the clipped value, marker included; the unfixed render sent all 40 characters."""
    log_foundry.configure(max_value_bytes=16)
    try:
        assert _header(s="x" * 40) == "s=xx%E2%80%A6%5Btruncated%5D"
    finally:
        log_foundry.configure(max_value_bytes=Config().max_value_bytes)


def test_an_int_too_large_to_render_becomes_its_placeholder() -> None:
    """AC4, case (d): `str` refuses an int of more than 4,300 digits; coercion names its size."""
    assert _header(i=10**4300) == "i=%3Cint%3A%20~4301%20digits%3E"


def test_the_render_is_one_pass_over_the_whole_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    """AC5: `a` spends the four-node budget, so `b` is cut; a per-member pass would render it."""
    monkeypatch.setattr(sanitize, "_MAX_NODES", 4)
    assert _header(a=[1, 2, 3], b=[4]) == "a=%5B1%2C%202%2C%203%5D,b=%3Cnode%20limit%3E"


def test_the_header_and_the_event_carry_the_same_node_limited_value(
    lf, fake_sink, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC6, twin: event assembly and the header coerce one baggage with one coercer."""
    monkeypatch.setattr(sanitize, "_MAX_NODES", 64)

    @lf.trace
    def work() -> str:
        lf.set_baggage(a=_fan_out())
        lf.info("twin")
        return lf.current_baggage_header()

    with _deadline(5, "info() and current_baggage_header() on the DAG"):
        header = work()
    [event] = [e for e in fake_sink.events if e["message"] == "twin"]
    assert "<node limit>" in str(event["fields"]["a"])
    assert unquote(header) == "a=" + str(event["fields"]["a"])


def test_plain_string_baggage_renders_as_before() -> None:
    """AC7: an exact `str` passes through, and a comma still round-trips."""
    assert _header(request_id="req-123", tenant="acme") == "request_id=req-123,tenant=acme"
    context.reset_context()
    header = _header(r="req,123", e="a=b")
    assert context.parse_baggage_header(header) == {"r": "req,123", "e": "a=b"}


def test_the_public_surface_and_signatures_are_unchanged() -> None:
    """AC8: no public name added, and neither function's signature moved."""
    import inspect

    assert context.__all__ == [
        "current_baggage_header",
        "current_trace_context",
        "current_traceparent",
        "get_baggage",
        "reset_context",
        "set_baggage",
    ]
    # SPEC-072 FR-001 grew the surface by exactly four names (Outcome, SinkCounters, Verdict and
    # read_deliver), and 1.2.0 by emit_via_deliver; this pin re-states the 30 it held plus those
    # five, in sorted position.
    assert log_foundry.__all__ == [
        "DEFAULT_SHUTDOWN_TIMEOUT",
        "DEFAULT_SWAP_TIMEOUT",
        "Config",
        "ContinueResult",
        "FlushResult",
        "Health",
        "Outcome",
        "Sink",
        "SinkCounters",
        "SinkDeliveryError",
        "SinkLosses",
        "Verdict",
        "__version__",
        "configure",
        "continue_trace",
        "critical",
        "current_baggage_header",
        "current_trace_context",
        "current_traceparent",
        "debug",
        "emit_via_deliver",
        "error",
        "flush",
        "flush_sink",
        "get_baggage",
        "get_config",
        "health",
        "info",
        "read_deliver",
        "read_losses",
        "reset_context",
        "set_baggage",
        "shutdown",
        "trace",
        "warning",
    ]
    assert str(inspect.signature(context.current_baggage_header)) == "() -> 'str'"
    assert (
        str(inspect.signature(context.format_baggage_header))
        == "(baggage: 'dict[str, object]') -> 'str'"
    )


# -- FR-002: the render is total -------------------------------------------------------------------


def test_a_raising_str_becomes_its_type_placeholder() -> None:
    """Case (b): coercion names the type before any `str()` runs; the message never leaves."""
    header = _header(b=Boom())
    assert header == "b=%3Cunserializable%3A%20Boom%3E"
    assert "secret" not in unquote(header)


def test_a_lone_surrogate_value_becomes_the_replacement_character() -> None:
    """Case (c): `quote` raised `UnicodeEncodeError` on a raw surrogate."""
    assert _header(k="\ud800") == "k=%EF%BF%BD"


def test_a_lone_surrogate_key_becomes_the_replacement_character() -> None:
    """Case (c), in a key: keys are coerced in the same pass."""
    assert _header(**{"\ud800": "v"}) == "%EF%BF%BD=v"


def test_an_inbound_lone_surrogate_renders_and_stays_stored() -> None:
    """Case (c), inbound: `parse_baggage_header` passes the surrogate through; intake is unchanged."""
    log_foundry.continue_trace(baggage="k=\ud800")
    assert context.current_baggage_header() == "k=%EF%BF%BD"
    assert context.get_baggage()["k"] == "\ud800"


def test_a_bool_claim_becomes_coercion_s_own_placeholder() -> None:
    """Coercion route: since SPEC-063 the spoof is refused by coercion, and only its member changes.

    Before SPEC-063 it reached the render as itself and the guard named it; now coercion names it
    by its real type before the render, so the header is the same text by the other route and the
    guard itself is held by the `bypass_coercion` twins.
    """
    _assert_refused_by_coercion(_ClaimsBoolRaises())
    assert _header(a="1", v=_ClaimsBoolRaises(), z="2") == (
        "a=1,v=%3Cunserializable%3A%20_ClaimsBoolRaises%3E,z=2"
    )


def test_the_render_guard_isolates_a_raising_value(bypass_coercion: None) -> None:
    """The guard, route-independent: a value whose `str()` raises, handed straight to the render."""
    assert _header(a="1", v=Boom(), z="2") == "a=1,v=%3Cunserializable%3A%20Boom%3E,z=2"


def test_a_bool_claim_with_a_raising_encode_becomes_coercion_s_placeholder() -> None:
    """Measured escape 1, coercion route: since SPEC-063 coercion refuses it before any `str()`."""
    _assert_refused_by_coercion(_ClaimsBoolSubStr())
    assert _header(a="1", v=_ClaimsBoolSubStr(), z="2") == (
        "a=1,v=%3Cunserializable%3A%20_ClaimsBoolSubStr%3E,z=2"
    )


def test_a_str_subclass_with_a_raising_encode_is_guarded_on_any_coercer(
    bypass_coercion: None,
) -> None:
    """Measured escape 1, route-independent; a guard around `str()` alone fails it."""
    assert _header(a="1", v=_ClaimsBoolSubStr(), z="2") == (
        "a=1,v=%3Cunserializable%3A%20_ClaimsBoolSubStr%3E,z=2"
    )


def test_a_raising_metaclass_name_is_read_as_the_stored_name() -> None:
    """Measured escape 2, coercion route: since SPEC-063 the stored name is read, so none raises."""
    _assert_refused_by_coercion(_ClaimsBoolNameless())
    with _nameless():
        header = _header(a="1", v=_ClaimsBoolNameless(), z="2")
    assert header == "a=1,v=%3Cunserializable%3A%20_ClaimsBoolNameless%3E,z=2"


def test_a_raising_metaclass_name_is_read_as_the_stored_name_on_any_coercer(
    bypass_coercion: None,
) -> None:
    """Measured escape 2, route-independent: `_type_name` reads the stored name (SPEC-063)."""
    with _nameless():
        header = _header(a="1", v=_ClaimsBoolNameless(), z="2")
    assert header == "a=1,v=%3Cunserializable%3A%20_ClaimsBoolNameless%3E,z=2"


def _quote_refusing(refused: Callable[[object], bool]) -> Callable[..., str]:
    """A `quote` that raises for the text `refused` picks and is the real one otherwise."""

    def _quote(text: str, safe: str = "/") -> str:
        if refused(text):
            raise ValueError("refused")
        return quote(text, safe=safe)

    return _quote


def test_an_unrenderable_key_skips_its_member_and_is_counted(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A key that cannot be encoded skips its member only; the header still parses."""
    monkeypatch.setattr(context, "quote", _quote_refusing(lambda text: text == "bad"))
    header = _header(a="1", bad="2", z="3")
    assert header == "a=1,z=3"
    assert context.parse_baggage_header(header) == {"a": "1", "z": "3"}
    assert _lines(capsys.readouterr().err) == [
        (
            "log-foundry: lost 1 baggage member(s); "
            "running total; members dropped from the outbound baggage header"
        )
    ]


def test_an_unencodable_placeholder_skips_its_member_and_is_counted(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-002: when even the placeholder cannot be encoded, the member is skipped and counted."""
    monkeypatch.setattr(
        context,
        "quote",
        _quote_refusing(lambda text: isinstance(text, str) and text.startswith("<unserializable")),
    )
    assert _header(a="1", b=Boom(), z="2") == "a=1,z=2"
    assert len(_lines(capsys.readouterr().err)) == 1
    assert next(context._dropped_members) == 2


def test_a_base_exception_from_str_propagates_on_any_coercer(bypass_coercion: None) -> None:
    """Route-independent: a `BaseException` reaches the caller; an `Exception` does not."""
    with pytest.raises(_Halt):
        _header(a="1", v=_ClaimsBoolHalts())
    context.reset_context()
    assert _header(a="1", v=Boom()) == "a=1,v=%3Cunserializable%3A%20Boom%3E"


@pytest.mark.parametrize(
    "make",
    [Boom, _ClaimsBoolRaises, _ClaimsBoolSubStr, _ClaimsBoolNameless],
    ids=["boom", "raises", "substr", "nameless"],
)
@pytest.mark.parametrize("bypass", [False, True], ids=["coerced", "uncoerced"])
def test_no_placeholder_carries_the_message_or_a_repr(
    make: Callable[[], object], bypass: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Invariant 11: a type name is all that leaves the library about a value."""
    if bypass:
        monkeypatch.setattr(
            sanitize, "sanitize_fields", lambda fields, *, cfg: (dict(fields), False)
        )
    with _nameless():
        header = _header(v=make())
    shown = unquote(header)
    assert shown.startswith("v=<unserializable: ")
    for leaked in ("secret", "object at 0x", "Error"):
        assert leaked not in shown, leaked


# -- FR-003: the cap, by whole members, an empty key skipped, and the announcement ----------------


def test_an_over_cap_baggage_keeps_whole_members_in_order() -> None:
    """Twenty 1,004-byte members: eight fit (8,039 bytes) and each is whole."""
    baggage = {f"k{i:02d}": chr(ord("a") + i) * 1000 for i in range(20)}
    header = _header(**baggage)
    assert len(header) == 8039
    parsed = context.parse_baggage_header(header)
    assert parsed is not None
    assert list(parsed) == [f"k{i:02d}" for i in range(8)]
    assert all(parsed[key] == baggage[key] for key in parsed)


def test_a_header_of_exactly_the_cap_is_whole_and_one_byte_more_drops_the_member(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The boundary: 8,192 bytes is kept whole and silently; 8,193 drops the last member."""
    header = _header(a="x" * 4000, b="y" * 4187)
    assert len(header) == _CAP
    assert header == "a=" + "x" * 4000 + ",b=" + "y" * 4187
    assert _lines(capsys.readouterr().err) == []
    context.reset_context()
    assert _header(a="x" * 4000, b="y" * 4188) == "a=" + "x" * 4000
    assert len(_lines(capsys.readouterr().err)) == 1


def test_the_cap_keeps_a_prefix_and_never_packs() -> None:
    """`c` would fit after `b` is dropped, but the first misfit ends the header."""
    assert _header(a="1", b="x" * 8190, c="2") == "a=1"


def test_a_single_member_over_the_cap_yields_empty_and_announces(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A value at the default 8 KiB ceiling already makes a header the parser refuses whole."""
    assert _header(k="x" * 8192) == ""
    assert len(_lines(capsys.readouterr().err)) == 1


def test_the_cap_is_on_the_encoded_length() -> None:
    """2,000 `€` is 6,000 UTF-8 bytes, under the ceiling, and 18,000 characters encoded."""
    assert len("€" * 2000) == 2000
    assert _header(k="€" * 2000) == ""


_CORPUS: dict[str, tuple[dict[str, object], list[str]]] = {
    "plain": ({"request_id": "req-123", "tenant": "acme"}, ["request_id", "tenant"]),
    "comma": ({"r": "req,123", "e": "a=b"}, ["r", "e"]),
    "over-cap": (
        {f"k{i:02d}": "v" * 1000 for i in range(20)},
        [f"k{i:02d}" for i in range(8)],
    ),
    "single-over-cap": ({"k": "x" * 8192}, []),
    "exact-cap": ({"a": "x" * 4000, "b": "y" * 4187}, ["a", "b"]),
    "prefix": ({"a": "1", "b": "x" * 8190, "c": "2"}, ["a"]),
    "empty-key": ({"a": "1", "": "x", "b": "2"}, ["a", "b"]),
    "surrogates": ({"\ud800": "v", "k": "\ud800"}, ["�", "k"]),
    "mixed": (
        {
            "n": 7,
            "d": datetime(2026, 1, 1),
            "m": {"a": 1},
            "s": "\ud800",
            "": "x",
            "big": "x" * 8190,
            "after": "1",
        },
        ["n", "d", "m", "s"],
    ),
}


@pytest.mark.parametrize("case", list(_CORPUS))
def test_every_emitted_header_parses_to_the_expected_prefix(case: str) -> None:
    """Formatter and parser are twins: what is emitted is what the parser accepts (invariant 6)."""
    baggage, expected = _CORPUS[case]
    header = _header(**baggage)
    if not expected:
        assert header == ""
        return
    parsed = context.parse_baggage_header(header)
    assert parsed is not None, f"{case}: the parser refused {len(header)} bytes"
    assert list(parsed) == expected


def test_an_empty_key_is_skipped_not_ended(capsys: pytest.CaptureFixture[str]) -> None:
    """`=x` made the parser refuse the whole header; now it is skipped and counted."""
    header = _header(a="1", **{"": "x"}, b="2")
    assert header == "a=1,b=2"
    assert context.parse_baggage_header(header) == {"a": "1", "b": "2"}
    assert [line.split(";")[0] for line in _lines(capsys.readouterr().err)] == [
        "log-foundry: lost 1 baggage member(s)"
    ]


def test_an_empty_key_ahead_of_an_over_cap_member_counts_both() -> None:
    """The members before the misfit get through, and both rules' drops are in the total."""
    assert _header(a="1", **{"": "x"}, b="x" * 8190, c="2") == "a=1"
    assert next(context._dropped_members) == 4, "the empty key, b, and c after it"


def test_the_first_drop_writes_one_line(capsys: pytest.CaptureFixture[str]) -> None:
    """The first drop is announced at once, carrying its total."""
    context.format_baggage_header({"": "x"})
    lines = _lines(capsys.readouterr().err)
    assert len(lines) == 1
    assert lines[0].startswith("log-foundry: lost 1 baggage member(s)")


def test_the_throttle_writes_at_one_and_every_thousandth(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """1,001 drops write two lines, at running totals 1 and 1,000."""
    for _ in range(1001):
        context.format_baggage_header({"": "x"})
    lines = _lines(capsys.readouterr().err)
    assert [line.split()[2] for line in lines] == ["1", "1000"]


def test_a_first_call_dropping_several_members_announces_once(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The first-ever drop is a prefix drop of two members: it draws 1 and 2, and 1 crosses.

    The rule is "any value this call drew crosses", not "the last value crosses": checked on the
    last value alone, this call ends on 2 and the first loss of the process is never announced.
    """
    assert _header(a="1", b="x" * 8190, c="2") == "a=1"
    lines = _lines(capsys.readouterr().err)
    assert len(lines) == 1
    assert lines[0].startswith("log-foundry: lost 2 baggage member(s)")


def test_a_crossing_inside_one_call_is_announced_with_the_call_s_last_total(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Three drops drawn as 999, 1000 and 1001: the crossing at 1000 is announced, as 1001."""
    monkeypatch.setattr(context, "_dropped_members", itertools.count(999), raising=False)
    assert context.format_baggage_header({"": "x", "b": "x" * 8192, "c": "1"}) == ""
    lines = _lines(capsys.readouterr().err)
    assert len(lines) == 1
    assert lines[0].startswith("log-foundry: lost 1001 baggage member(s)")


def test_the_drop_line_names_no_key_or_value(capsys: pytest.CaptureFixture[str]) -> None:
    """The announcement carries a count, never the dropped member's key or value."""
    assert _header(big="x" * 8180, **{"secret-key": "secret-value"}) == "big=" + "x" * 8180
    err = capsys.readouterr().err
    assert len(_lines(err)) == 1, "the positive control: a line was written"
    assert "secret-key" not in err
    assert "secret-value" not in err


def test_concurrent_drops_announce_each_crossing_once(capsys: pytest.CaptureFixture[str]) -> None:
    """Eight threads, 500 drops each: every crossing is announced by exactly one call."""
    barrier = threading.Barrier(8)

    def work() -> None:
        barrier.wait()
        for _ in range(500):
            context.format_baggage_header({"": "x"})

    threads = [threading.Thread(target=work) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert next(context._dropped_members) == 4001
    totals = [int(line.split()[2]) for line in _lines(capsys.readouterr().err)]
    assert sorted(totals) == [1, 1000, 2000, 3000, 4000]


def test_an_inbound_header_at_the_cap_is_re_capped_outbound() -> None:
    """Twin: 8,192 bytes inbound; `quote` makes each `(` three bytes, so `b` is dropped whole."""
    inbound = "a=1,b=" + "(" * 8186
    assert len(inbound.encode()) == _CAP
    assert context.parse_baggage_header(inbound) is not None
    log_foundry.continue_trace(baggage=inbound)
    assert context.current_baggage_header() == "a=1"
