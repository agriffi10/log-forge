"""SPEC-057 — every coercion pass is bounded in work: reads cost the ceiling, and two budgets.

FR-003 bounds what one *read* costs: ``truncate_str``, ``truncate_tail`` and the bytes-like branch
of the coercer read a prefix (or suffix) of the ceiling plus one, never the whole input. The
rendering must be byte-identical to the unbounded computation it replaced, so the tests below hold
it to a frozen reference copy of that computation rather than to the argument in the docstrings.

FR-001 and FR-002 bound the pass as a whole: at most 131,072 values visited and 16 MiB of text
charged, the value that crosses either becoming a marked ``<node limit>`` or ``<size limit>`` after
which nothing is visited. The hostile shapes run under a SIGALRM deadline so that a missing bound
fails the test instead of hanging the run, and every other shape sits just over a budget for the
same reason. Expected outputs and charge totals are written as literals, never computed by calling
``src``, so an expectation cannot mirror the code it checks.

This module is also run once against ``main``'s ``sanitize.py`` (FR-001's third criterion), so it
must collect there: it imports only names that exist on ``main``, reads the two budgets through
``getattr`` with a default, and writes the two new placeholders as literals. Failure messages
render values through ``ascii()`` and truncate them, because a lone surrogate in a plain ``repr``
crashes the xdist worker that tries to report it.
"""

from __future__ import annotations

import contextlib
import ctypes
import re
import signal
import sys
import threading
import tracemalloc
from datetime import UTC, datetime, time
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, NamedTuple
from uuid import UUID

import pytest

import log_foundry as lf
from log_foundry import sanitize
from log_foundry.config import Config
from log_foundry.model import Span, build_event
from log_foundry.sanitize import (
    TRUNCATION_MARKER,
    coerce,
    sanitize_fields,
    truncate_str,
    truncate_tail,
)
from log_foundry.sinks.memory import MemorySink

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

_NODES: int = getattr(sanitize, "_MAX_NODES", 131_072)
_BYTES: int = getattr(sanitize, "_MAX_RENDERED_BYTES", 16 * 1024 * 1024)
_MiB = 1024 * 1024


# -- the reference: today's unbounded algorithm, frozen ------------------------------------------
#
# Copied from `src/log_foundry/sanitize.py` at 324936e, the module before SPEC-057, and never
# edited when `src/` changes: it is what "byte-identical to today's unbounded computation" means.

_REF_MARKER_BYTES = len(TRUNCATION_MARKER.encode("utf-8"))
_REF_SURROGATES = re.compile("[\ud800-\udfff]")


def _ref_measured(value: str) -> tuple[str, bytes, bool]:
    plain = str.__str__(value)
    try:
        return plain, plain.encode("utf-8"), False
    except UnicodeEncodeError:
        clean = _REF_SURROGATES.sub("�", plain)
        return clean, clean.encode("utf-8"), True


def _ref_truncate_str(value: str, max_bytes: int) -> tuple[str, bool]:
    plain, raw, replaced = _ref_measured(value)
    if len(raw) <= max_bytes:
        return plain, replaced
    budget = max_bytes - _REF_MARKER_BYTES
    if budget <= 0:
        return TRUNCATION_MARKER, True
    return raw[:budget].decode("utf-8", errors="ignore") + TRUNCATION_MARKER, True


def _ref_truncate_tail(value: str, max_bytes: int) -> tuple[str, bool]:
    plain, raw, replaced = _ref_measured(value)
    if len(raw) <= max_bytes:
        return plain, replaced
    budget = max_bytes - _REF_MARKER_BYTES
    if budget <= 0:
        return TRUNCATION_MARKER, True
    return TRUNCATION_MARKER + raw[-budget:].decode("utf-8", errors="ignore"), True


def _ref_decoded(value: bytes) -> tuple[str, bool]:
    try:
        return value.decode("utf-8"), False
    except UnicodeDecodeError:
        return value.decode("utf-8", errors="replace"), True


def _ref_bytes_like(value: bytes | bytearray | memoryview, max_bytes: int) -> tuple[str, bool]:
    """Today's ``self.text(self._decoded(bytes(value)))``, with the flag the pass would set."""
    text, replaced = _ref_decoded(bytes(value))
    clipped, altered = _ref_truncate_str(text, max_bytes)
    return clipped, replaced or altered


# -- FR-003 AC1: the bounded reads render byte-identically -------------------------------------

_CEILINGS = range(1, 41)
_WIDE = ("é", "€", "😀")


def _str_corpus(m: int) -> list[str]:
    """Strings placed around a ceiling of ``m`` bytes, at both ends, for the head and the tail."""
    out = ["a" * n for n in (m - 1, m, m + 1, m + 2, 2 * m + 20) if n >= 0]
    for ch in _WIDE:
        out.append(ch * (m + 3))
        out.append(("a" + ch) * (m + 1))
        for off in range(max(0, m - 20), m + 6):
            out.append("a" * off + ch + "b" * 30)
            out.append("b" * 30 + ch + "a" * off)
            out.append("a" * off + ch * 3)
            out.append(ch * 3 + "a" * off)
    for off in [*range(max(1, m - 14)), m + 1, m + 3, m + 10]:
        out.append("a" * off + "\ud800" + "c" * (m + 5))
        out.append("c" * (m + 5) + "\ud800" + "a" * off)
    out.append("\ud800" * (m + 2))
    return out


def _bytes_corpus(m: int) -> list[bytes]:
    """The str corpus as bytes, plus invalid and truncated UTF-8 at the cut and after it."""
    out = [s.encode("utf-8", "surrogatepass") for s in _str_corpus(m)]
    for off in range(max(0, m - 6), m + 4):
        for bad in (b"\xff", b"\x80", b"\xe2\x82", b"\xf0\x9f\x98"):
            out.append(b"a" * off + bad + b"z" * (m + 5))
            out.append(b"a" * off + bad)
    return out


class _Wide(ctypes.Structure):
    """A 64-byte record: a zero-dimensional view larger than every ceiling in ``_CEILINGS``."""

    _fields_ = [("a", ctypes.c_char * 64)]


def _views(raw: bytes) -> Iterator[tuple[str, bytes | bytearray | memoryview]]:
    """Each bytes-like form FR-003 AC1 names, all holding ``raw`` in logical order."""
    yield "bytes", raw
    yield "bytearray", bytearray(raw)
    yield "memoryview", memoryview(raw)
    interleaved = bytes(b for pair in zip(raw, bytes(len(raw)), strict=True) for b in pair)
    yield "strided", memoryview(interleaved)[::2]
    if raw and len(raw) % 2 == 0:
        yield "itemsize-2", memoryview(raw).cast("H")
        units = [raw[i : i + 2] + b"\x00\x00" for i in range(0, len(raw), 2)]
        yield "itemsize-2-strided", memoryview(b"".join(units)).cast("H")[::2]
        yield "2-d", memoryview(raw).cast("B", shape=[2, len(raw) // 2])


@pytest.mark.parametrize("m", _CEILINGS)
def test_bounded_reads_match_the_unbounded_reference_for_str(m: int) -> None:
    """FR-003 AC1, the str half: head and tail, every ceiling from 1 to 40."""
    mismatches = []
    for s in _str_corpus(m):
        if truncate_str(s, m) != _ref_truncate_str(s, m):
            mismatches.append(("head", ascii(s)[:60], ascii(truncate_str(s, m))[:60]))
        if truncate_tail(s, m) != _ref_truncate_tail(s, m):
            mismatches.append(("tail", ascii(s)[:60], ascii(truncate_tail(s, m))[:60]))
    assert not mismatches, f"{len(mismatches)} mismatches at {m}: {mismatches[:5]}"
    assert truncate_str("a" * m, m) == ("a" * m, False)
    assert truncate_tail("a" * m, m) == ("a" * m, False)
    assert truncate_str("a" * (m + 1), m)[1] is True
    assert truncate_tail("a" * (m + 1), m)[1] is True


@pytest.mark.parametrize("m", _CEILINGS)
def test_bounded_reads_match_the_unbounded_reference_for_bytes_like(m: int) -> None:
    """FR-003 AC1, the bytes half: every form, every ceiling, through a whole pass."""
    cfg = Config(max_value_bytes=m)
    mismatches = []
    for raw in _bytes_corpus(m):
        for form, value in _views(raw):
            fields, flag = sanitize_fields({"k": value}, cfg=cfg)
            got, want = (fields["k"], flag), _ref_bytes_like(value, m)
            if got != want:
                mismatches.append((form, ascii(raw)[:50], ascii(got)[:50], ascii(want)[:50]))
        record = _Wide()
        ctypes.memmove(ctypes.addressof(record), (raw + b"z" * 64)[:64], 64)
        view = memoryview(record)
        assert view.ndim == 0
        fields, flag = sanitize_fields({"k": view}, cfg=cfg)
        if (fields["k"], flag) != _ref_bytes_like(view, m):
            mismatches.append(("0-d", ascii(raw)[:50], ascii(fields["k"])[:50], ""))
    assert not mismatches, f"{len(mismatches)} mismatches at {m}: {mismatches[:5]}"


# -- FR-003 AC2 and AC3: a subclass is read through its base type, never its own dunders -------

_CALLS: list[str] = []


def _record(name: str, mode: str, lie: object) -> object:
    _CALLS.append(name)
    if mode == "raise":
        raise RuntimeError(f"{name} consulted")
    return lie


class _LyingStr(str):
    mode = "lie"

    def __len__(self) -> int:
        return _record("__len__", self.mode, 3)

    def __getitem__(self, key: object) -> str:
        return _record("__getitem__", self.mode, "LIE")

    def encode(self, *args: object, **kwargs: object) -> bytes:
        return _record("encode", self.mode, b"LIE")

    def __str__(self) -> str:
        return _record("__str__", self.mode, "LIE")


@pytest.mark.parametrize("mode", ["lie", "raise"])
@pytest.mark.parametrize("length", [5, 20_000])
def test_a_str_subclass_is_read_through_str_itself(mode: str, length: int) -> None:
    """FR-003 AC2: the answer is the plain text's, and no override was consulted."""
    _LyingStr.mode = mode
    value = _LyingStr("é" * length)
    plain = str.__str__(value)
    cfg = Config()
    _CALLS.clear()
    head = truncate_str(value, cfg.max_value_bytes)
    tail = truncate_tail(value, cfg.max_value_bytes)
    coerced = coerce(value, cfg=cfg)
    calls = list(_CALLS)
    assert calls == [], f"the subclass's own dunders were consulted: {calls}"
    assert head == _ref_truncate_str(plain, cfg.max_value_bytes)
    assert tail == _ref_truncate_tail(plain, cfg.max_value_bytes)
    assert coerced == _ref_truncate_str(plain, cfg.max_value_bytes)[0]
    assert type(head[0]) is str
    assert type(tail[0]) is str
    assert type(coerced) is str


_OTHER = b"R" * 50_000


class _LyingBytes(bytes):
    mode = "lie"

    def __bytes__(self) -> bytes:
        return _record("__bytes__", self.mode, b"Q" * 50_000)

    def __buffer__(self, flags: int) -> memoryview:
        return _record("__buffer__", self.mode, memoryview(_OTHER))

    def __len__(self) -> int:
        return _record("__len__", self.mode, 3)

    def __getitem__(self, key: object) -> bytes:
        return _record("__getitem__", self.mode, b"LIE")


class _LyingBytearray(bytearray):
    mode = "lie"

    def __bytes__(self) -> bytes:
        return _record("__bytes__", self.mode, b"Q" * 50_000)

    def __buffer__(self, flags: int) -> memoryview:
        return _record("__buffer__", self.mode, memoryview(_OTHER))

    def __len__(self) -> int:
        return _record("__len__", self.mode, 3)

    def __getitem__(self, key: object) -> bytes:
        return _record("__getitem__", self.mode, b"LIE")


@pytest.mark.parametrize("mode", ["lie", "raise"])
@pytest.mark.parametrize("length", [5, 20_000])
@pytest.mark.parametrize("kind", [_LyingBytes, _LyingBytearray], ids=["bytes", "bytearray"])
def test_a_bytes_or_bytearray_subclass_is_rendered_from_the_bytes_it_stores(
    kind: type[_LyingBytes | _LyingBytearray], mode: str, length: int
) -> None:
    """FR-003 AC3, the recorded exception: the stored bytes, and no override consulted."""
    kind.mode = mode
    stored = ("é" * length).encode("utf-8")
    value = kind(stored)
    cfg = Config()
    _CALLS.clear()
    fields, flag = sanitize_fields({"k": value}, cfg=cfg)
    calls = list(_CALLS)
    assert calls == [], f"the subclass's own dunders were consulted: {calls}"
    assert (fields["k"], flag) == _ref_bytes_like(stored, cfg.max_value_bytes)


# -- FR-003 AC4: a non-contiguous view is read in whole rows -----------------------------------


def test_a_non_contiguous_view_renders_identically_to_the_reference() -> None:
    """FR-003 AC4: two stdlib views, one whose single row exceeds the ceiling, and an ASCII twin.

    The spec's first view is every byte value in turn, so most of it does not decode and each
    bad byte widens to three: a prefix a row short of the ceiling plus one still clips, and
    rounding the row count down instead of up would pass it. The ASCII twin decodes byte for
    byte, so 585 rows (8,190 bytes) would be written whole where the view is clipped.
    """
    buf = (bytes(range(256)) * 110)[:28_000]
    narrow_rows = memoryview(buf).cast("B", shape=[2000, 14])[::2]
    one_wide_row = memoryview(bytes(range(97, 123)) * 1154)[:30_000].cast("B", shape=[3, 10_000])
    ascii_rows = memoryview(b"x" * 28_000).cast("B", shape=[2000, 14])[::2]
    views = {"narrow rows": narrow_rows, "one wide row": one_wide_row[::2], "ascii": ascii_rows}
    for label, view in views.items():
        assert not view.c_contiguous
        fields, flag = sanitize_fields({"k": view}, cfg=Config())
        assert (fields["k"], flag) == _ref_bytes_like(view, 8192), f"{label} differs"
        assert flag is True


# -- FR-003 AC5: the work is bounded by allocation, not by time --------------------------------


class _PlainSubclass(str):
    pass


def _span(name: str) -> Span:
    return Span(trace_id="0" * 32, span_id="0" * 16, parent_span_id=None, name=name, start_ts=0.0)


def _allocation_case(case: str) -> Callable[[], object]:
    """Builds one case's input up front and returns the call whose allocation is measured."""
    n = 32 * _MiB
    cfg = Config()
    if case.endswith(("-str", "-subclass")):
        text = _PlainSubclass("x" * n) if case.endswith("-subclass") else "x" * n
        if case.startswith("truncate_str"):
            return lambda: truncate_str(text, 8192)
        if case.startswith("truncate_tail"):
            return lambda: truncate_tail(text, 8192)
        return lambda: coerce(text, cfg=cfg)
    if case == "coerce-bytes":
        raw = b"x" * n
        return lambda: coerce(raw, cfg=cfg)
    if case == "coerce-bytearray":
        mutable = bytearray(n)
        return lambda: coerce(mutable, cfg=cfg)
    if case == "coerce-memoryview":
        view = memoryview(bytes(n))
        return lambda: coerce(view, cfg=cfg)
    if case == "coerce-strided-memoryview":
        strided = memoryview(bytes(2 * n))[::2]
        return lambda: coerce(strided, cfg=cfg)
    big = "x" * n
    span = _span(big if case == "build_event-span-name" else "work")
    message = big if case == "build_event-message" else "m"
    build_event(span, "INFO", "warm", fields={}, baggage={})
    return lambda: build_event(span, "INFO", message, fields={}, baggage={})


_ALLOCATION_CASES = [
    "truncate_str-str",
    "truncate_str-subclass",
    "truncate_tail-str",
    "truncate_tail-subclass",
    "coerce-str",
    "coerce-subclass",
    "coerce-bytes",
    "coerce-bytearray",
    "coerce-memoryview",
    "coerce-strided-memoryview",
    "build_event-message",
    "build_event-span-name",
]


@pytest.mark.parametrize("case", _ALLOCATION_CASES)
def test_reads_are_bounded_by_the_ceiling_not_the_input(case: str) -> None:
    """FR-003 AC5: the traced peak, net of what was live before the call, is under 1 MiB.

    Measured as a delta on both branches: when tracemalloc is already running the input is among
    the traced blocks, so the raw peak would include it, and when it is not the baseline is the
    handful of blocks ``start()`` itself allocates. The strided view is an addition to the spec's
    list: without it, reading the row path whole would fail nothing.
    """
    call = _allocation_case(case)
    already = tracemalloc.is_tracing()
    if already:
        tracemalloc.reset_peak()
    else:
        tracemalloc.start()
    try:
        base = tracemalloc.get_traced_memory()[0]
        call()
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        if not already:
            tracemalloc.stop()
    assert peak - base < _MiB, f"{case}: the call allocated {peak - base:,} bytes at its peak"


# -- FR-001 and FR-002: the harness ------------------------------------------------------------

_NODE_LIMIT = "<node limit>"
_SIZE_LIMIT = "<size limit>"
_ROUTES = ("in_span", "orphan")
_S = "x" * 8192


class _Deadline(BaseException):
    """What the alarm raises: a ``BaseException``, because ``_Coercer.value`` catches ``Exception``.

    An ``Exception`` raised inside the walk would be turned into a placeholder by the coercer's
    own guard, and the walk would carry on; only a ``BaseException`` escapes it, as a
    ``KeyboardInterrupt`` does (invariant 1).
    """


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


def _configure(**ceilings: int) -> MemorySink:
    sink = MemorySink()
    lf.configure(sink=sink, **ceilings)  # type: ignore[arg-type]
    return sink


def _emit(
    route: str, fields: dict[str, object], sink: MemorySink, *, deadline: float | None = None
) -> dict[str, object]:
    """Runs one ``info`` on one delivery path and returns the event it produced."""

    def call() -> None:
        lf.info("m", fields=fields)

    guard = _deadline(deadline, "the level call") if deadline else contextlib.nullcontext()
    with guard:
        if route == "orphan":
            call()
        else:
            lf.trace(name="work")(call)()
    if route == "in_span":
        assert lf.flush(timeout=10)
    events = [e for e in sink.events if e["message"] == "m"]
    assert len(events) == 1, f"{route}: expected one event, got {len(events)}"
    sink.events.clear()
    return events[0]


def _leaves(obj: object) -> Iterator[object]:
    """Yields every leaf value in walk order: list members and mapping values, depth first."""
    if isinstance(obj, list):
        for item in obj:
            yield from _leaves(item)
    elif isinstance(obj, dict):
        for item in obj.values():
            yield from _leaves(item)
    else:
        yield obj


def _values(obj: object) -> int:
    """Counts what the pass counts: every container and every leaf, but no mapping key."""
    if isinstance(obj, list):
        return 1 + sum(_values(item) for item in obj)
    if isinstance(obj, dict):
        return 1 + sum(_values(item) for item in obj.values())
    return 1


def _fan_out(leaf: object) -> object:
    """``y = leaf``, then ``y = [y] * 16`` seven times: a DAG with 16^7 paths to its leaf."""
    shape = leaf
    for _ in range(7):
        shape = [shape] * 16
    return shape


class _Spin(list):  # type: ignore[type-arg]
    """A list whose iteration never yields, for the deadline's positive control."""

    def __iter__(self) -> Iterator[object]:
        while True:
            pass


def test_the_deadline_escapes_the_coercers_guard() -> None:
    """FR-001 AC2: the alarm stops a walk that never returns, through the coercer's own guard.

    The behaviour is checked before the class: were ``_Deadline`` an ``Exception``, the coercer
    would turn the interrupted walk into a placeholder and return, and this would not raise.
    """
    with (
        pytest.raises(pytest.fail.Exception, match=r"did not return within 0\.2 s"),
        _deadline(0.2, "the spinning walk"),
    ):
        coerce(_Spin(), cfg=Config())
    assert issubclass(_Deadline, BaseException)
    assert not issubclass(_Deadline, Exception)


# -- FR-001: the node budget -------------------------------------------------------------------


@pytest.mark.parametrize("route", _ROUTES)
def test_a_shared_sublist_dag_returns_within_the_node_budget(route: str) -> None:
    """FR-001 AC1: 16^7 leaf visits on main; the budget's count, then one placeholder, here."""
    sink = _configure()
    event = _emit(route, {"a": _fan_out(0)}, sink, deadline=5)
    fields = event["fields"]
    assert isinstance(fields, dict)
    leaves = list(_leaves(fields["a"]))
    assert _values(fields["a"]) == _NODES + 1
    assert leaves[-1] == _NODE_LIMIT
    assert leaves.count(_NODE_LIMIT) == 1
    assert event.get("truncated") is True


@pytest.mark.parametrize("route", _ROUTES)
def test_a_list_at_the_node_budget_is_whole_and_one_past_it_is_cut(route: str) -> None:
    """FR-001 AC4: ``_NODES`` values fit exactly, and the one after them is the placeholder."""
    sink = _configure(max_keys=_NODES)
    whole = _emit(route, {"a": [0] * (_NODES - 1)}, sink)
    assert whole["fields"] == {"a": [0] * (_NODES - 1)}
    assert "truncated" not in whole
    cut = _emit(route, {"a": [0] * _NODES}, sink)
    fields = cut["fields"]
    assert isinstance(fields, dict)
    assert len(fields["a"]) == _NODES
    assert fields["a"][:-1] == [0] * (_NODES - 1)
    assert fields["a"][-1] == _NODE_LIMIT
    assert cut.get("truncated") is True


def test_a_list_at_the_node_budget_is_whole_and_one_past_it_is_cut_through_coerce() -> None:
    """FR-001 AC4's last clause: ``coerce`` is one pass on the same terms."""
    cfg = Config(max_keys=_NODES)
    assert coerce([0] * (_NODES - 1), cfg=cfg) == [0] * (_NODES - 1)
    assert coerce([0] * _NODES, cfg=cfg) == [0] * (_NODES - 1) + [_NODE_LIMIT]


@pytest.mark.parametrize("route", _ROUTES)
def test_a_wide_ordinary_event_is_not_cut(route: str) -> None:
    """FR-001 AC5: 256 lists of 256 integers, 65,793 values, under the default ceilings."""
    sink = _configure()
    event = _emit(route, {"a": [[0] * 256] * 256}, sink)
    assert event["fields"] == {"a": [[0] * 256] * 256}
    assert "truncated" not in event


@pytest.mark.parametrize("route", _ROUTES)
def test_the_placeholder_replaces_a_container_unread_and_ends_every_open_container(
    route: str,
) -> None:
    """FR-001 AC6: the container past the budget is never entered, and ``b`` is never reached."""
    sink = _configure(max_keys=_NODES)
    event = _emit(route, {"a": [[0] * (_NODES - 2), [1, 2, 3]], "b": 1}, sink)
    fields = event["fields"]
    assert isinstance(fields, dict)
    assert list(fields) == ["a"], "an entry after the cut was written"
    assert fields == {"a": [[0] * (_NODES - 2), _NODE_LIMIT]}
    assert event.get("truncated") is True


@pytest.mark.parametrize("route", _ROUTES)
def test_the_node_check_precedes_the_depth_check(route: str) -> None:
    """FR-001 AC7: a ``<depth limit>`` still spends a slot, and the budget is checked first."""
    sink = _configure(max_keys=_NODES, max_depth=1)
    event = _emit(route, {"a": [0] * _NODES}, sink)
    fields = event["fields"]
    assert isinstance(fields, dict)
    assert list(fields) == ["a"]
    assert len(fields["a"]) == _NODES
    assert fields["a"][-1] == _NODE_LIMIT
    assert fields["a"][:-1] == ["<depth limit>"] * (_NODES - 1)
    assert event.get("truncated") is True


_BIG_DECIMAL_9M = Decimal("1" * 9_000_000)


@pytest.mark.parametrize("sweep", [False, True], ids=["at-close", "swept"])
@pytest.mark.parametrize("budget", ["node", "byte"])
def test_the_baggage_pass_honours_both_budgets(budget: str, sweep: bool) -> None:
    """FR-001 AC8, and its byte-budget twin: ``backfill_baggage`` is a pass on the same terms.

    ``span.start`` is built before the body sets the baggage and ``span.end`` is built with none,
    so whatever either carries came through ``backfill_baggage``'s own pass. The swept twin
    flushes inside the span, which backfills ``span.start`` on the sweep path
    (``_sweep_open_spans``) and ``span.end`` at close: invariant 6's two paths to one promise.
    The node shape is 262,657 values; the byte shape charges 9,000,001 and then crosses.
    """
    sink = _configure(max_keys=512)
    big: object = [[0] * 512] * 512 if budget == "node" else [_BIG_DECIMAL_9M, _BIG_DECIMAL_9M]

    @lf.trace(name="work")
    def body() -> None:
        lf.set_baggage(b=big)
        if sweep:
            lf.info("inside")
            assert lf.flush(timeout=10)

    body()
    assert lf.flush(timeout=10)
    for message in ("span.start", "span.end"):
        events = [e for e in sink.events if e["message"] == message]
        assert len(events) == 1, f"{message}: {len(events)} events"
        fields = events[0]["fields"]
        assert isinstance(fields, dict)
        if budget == "node":
            leaves = list(_leaves(fields["b"]))
            assert leaves[-1] == _NODE_LIMIT, message
            assert _values(fields["b"]) == _NODES + 1, message
        else:
            assert fields["b"] == ["1" * 8178 + TRUNCATION_MARKER, _SIZE_LIMIT], message
        assert events[0].get("truncated") is True, message


# -- FR-002: the byte budget -------------------------------------------------------------------


@pytest.mark.parametrize("route", _ROUTES)
def test_a_pass_charged_exactly_the_budget_is_whole_and_one_byte_more_is_cut(route: str) -> None:
    """FR-002 AC1: 1 + 2,047 × 8,192 + 8,191 = 16,777,216 fits; one byte more does not."""
    sink = _configure(max_keys=4096)
    exact = _emit(route, {"a": [_S] * 2047 + ["x" * 8191]}, sink)
    exact_fields = exact["fields"]
    assert isinstance(exact_fields, dict)
    assert exact_fields["a"][-1] == "x" * 8191
    assert exact_fields == {"a": [_S] * 2047 + ["x" * 8191]}
    assert "truncated" not in exact
    over = _emit(route, {"a": [_S] * 2047 + ["x" * 8192, "z"], "b": 1}, sink)
    over_fields = over["fields"]
    assert isinstance(over_fields, dict)
    assert over_fields["a"][2047] == _SIZE_LIMIT
    assert over_fields["a"][2048:] == [], "a sibling after the cut was written"
    assert over_fields == {"a": [_S] * 2047 + [_SIZE_LIMIT]}
    assert over.get("truncated") is True


class _Opaque:
    """A value no rule coerces, so it becomes a type-name placeholder."""


@pytest.mark.parametrize("over", [0, 1], ids=["exact", "over"])
@pytest.mark.parametrize("route", _ROUTES)
def test_produced_text_and_integers_are_charged_and_placeholders_are_not(
    route: str, over: int
) -> None:
    """FR-002 AC2: every probe ahead of the special string either charges exactly or nothing.

    Charges: ``a`` 1, the integer key ``12345`` 5, the tuple key 10,000 though written at 8,192,
    2,046 strings of 8,192, and the special string; the float, the bool, the ``None``, the
    over-long integer and the three ``<depth limit>`` values charge nothing. So 6,378 makes the
    total exactly 16,777,216. ``10**9000`` has 9,001 digits, past ``max_value_bytes`` under any
    interpreter limit, so it is a placeholder here whatever ``sys.get_int_max_str_digits()`` is.
    """
    sink = _configure(max_keys=4096, max_depth=2)
    special = "y" * (6378 + over)
    probes: list[object] = [{12345: None, ("x" * 9995,): None}, 1.5, True, None, 10**9000, [0]]
    event = _emit(route, {"a": [*probes, *([_S] * 2046), special]}, sink)
    fields = event["fields"]
    assert isinstance(fields, dict)
    out = fields["a"]
    tuple_key = "('" + "x" * 8176 + TRUNCATION_MARKER
    assert out[0] == {"12345": "<depth limit>", tuple_key: "<depth limit>"}
    assert out[1:4] == [1.5, True, None]
    assert isinstance(out[4], str)
    assert out[4].startswith("<int: ")
    assert out[5] == ["<depth limit>"]
    assert out[6:-1] == [_S] * 2046
    last = _SIZE_LIMIT if over else special
    assert out[-1] == last
    assert _SIZE_LIMIT not in list(_leaves(out))[:-1]


def test_a_callers_string_is_charged_after_clipping_and_produced_text_before() -> None:
    """FR-002 AC3: a caller's clipped string costs its clip; a ``Decimal`` costs its ``str()``."""
    text = "x" * 10_000_000
    fields, flag = sanitize_fields({"a": [text] * 256}, cfg=Config())
    assert _SIZE_LIMIT not in fields["a"]
    assert fields == {"a": ["x" * 8178 + TRUNCATION_MARKER] * 256}
    assert flag is True
    number = Decimal("1" * 10_000_000)
    fields, flag = sanitize_fields({"a": [number, number]}, cfg=Config())
    assert fields["a"][-1] == _SIZE_LIMIT
    assert fields == {"a": ["1" * 8178 + TRUNCATION_MARKER, _SIZE_LIMIT]}
    assert flag is True


@pytest.mark.parametrize("route", _ROUTES)
def test_a_key_that_crosses_leaves_its_value_unread(route: str) -> None:
    """FR-002 AC4: the key is charged before its value is read, so ``bb``'s value never is."""
    sink = _configure(max_keys=4096)
    head = [_S] * 2047
    crossed = _emit(route, {"a": [*head, "x" * 8190], "bb": None}, sink)
    crossed_fields = crossed["fields"]
    assert isinstance(crossed_fields, dict)
    assert crossed_fields.get(_SIZE_LIMIT) == _SIZE_LIMIT, "the crossing key's value was read"
    assert crossed_fields == {"a": [*head, "x" * 8190], _SIZE_LIMIT: _SIZE_LIMIT}
    assert crossed.get("truncated") is True
    fits = _emit(route, {"a": [*head, "x" * 8189], "bb": None}, sink)
    assert fits["fields"] == {"a": [*head, "x" * 8189], "bb": None}
    assert "truncated" not in fits


@pytest.mark.parametrize("sign", [1, -1], ids=["positive", "negative"])
@pytest.mark.parametrize("route", _ROUTES)
def test_an_integer_is_charged_its_rendered_length_sign_included(route: str, sign: int) -> None:
    """FR-002 AC5: ``10**3999`` charges 4,000, so 4,194 fit after ``a``; its negative, 4,193."""
    limit = sys.get_int_max_str_digits()
    assert limit == 0 or limit >= 4001, f"the premise needs a limit of 0 or >= 4,001, not {limit}"
    number = sign * 10**3999
    sink = _configure()
    event = _emit(route, {"a": [[number] * 256] * 17}, sink)
    fields = event["fields"]
    assert isinstance(fields, dict)
    leaves = list(_leaves(fields["a"]))
    written = 4194 if sign == 1 else 4193
    integers = sum(1 for leaf in leaves if type(leaf) is int)
    assert integers == written, f"{integers} integers were written before the cut, not {written}"
    assert leaves[:written] == [number] * written
    assert leaves[written:] == [_SIZE_LIMIT]
    assert event.get("truncated") is True


@pytest.mark.parametrize(
    ("fits", "crosses"), [(12345, 123456), (-1234, -12345)], ids=["positive", "negative"]
)
def test_an_integer_landing_exactly_on_the_budget_fits(fits: int, crosses: int) -> None:
    """The integer charge is exact at the budget: five characters fit the last five bytes.

    ``a`` costs 1, 2,047 strings of 8,192 and one of 8,186 cost 16,777,211, so an integer that
    renders in five characters (``12345``, or ``-1234`` with its sign) lands exactly on the
    budget and is written, and one that renders in six crosses it. This holds the integer's own
    copy of the byte check to the same strict comparison as the strings', which the
    budget-exactness test pins only for text.
    """
    head = [_S] * 2047 + ["x" * 8186]
    cfg = Config(max_keys=4096)
    whole, flag = sanitize_fields({"a": [*head, fits]}, cfg=cfg)
    assert whole["a"][-1] == fits
    assert flag is False
    cut, flag = sanitize_fields({"a": [*head, crosses]}, cfg=cfg)
    assert cut["a"][-1] == _SIZE_LIMIT
    assert cut["a"][:-1] == head
    assert flag is True


def _int_window(cfg: Config) -> list[int]:
    """Integers either side of the configured integer ceiling, both signs, for the parity test."""
    limit = sys.get_int_max_str_digits()
    ceiling = cfg.max_value_bytes if limit == 0 else min(cfg.max_value_bytes, limit)
    out = []
    for digits in range(max(1, ceiling - 3), ceiling + 3):
        out += [10 ** (digits - 1), 10**digits - 1, -(10 ** (digits - 1)), -(10**digits - 1)]
    return out


@pytest.mark.parametrize("ceiling", [8192, 5])
def test_an_integer_value_and_an_integer_key_are_bounded_alike(ceiling: int) -> None:
    """The value path and the key path carry one bound in two places, so hold them to one answer.

    ``integer`` writes the bound out inline for speed and ``_bounded_int`` keeps it for keys. On
    every integer in a window either side of the effective ceiling, both signs, the value is
    kept exactly when the key renders as its own digits, and a replaced value names the same
    placeholder the key does. The window's edge is computed here only to aim it; what is
    asserted is that the two paths agree, not where either draws the line.
    """
    cfg = Config(max_value_bytes=ceiling)
    disagreements = []
    for number in _int_window(cfg):
        value = coerce(number, cfg=cfg)
        fields, _ = sanitize_fields({"a": {number: 0}}, cfg=cfg)
        key = next(iter(fields["a"]))
        value_kept = type(value) is int and value == number
        key_kept = key.lstrip("-").isdigit()
        if value_kept != key_kept or (ceiling == 8192 and not value_kept and key != value):
            disagreements.append((number.bit_length(), number < 0, value_kept, key_kept))
    assert not disagreements, f"(bits, negative, value kept, key kept): {disagreements}"


def test_a_string_past_the_budget_under_a_raised_ceiling_is_replaced_not_clipped() -> None:
    """FR-002 AC6: under a 32 MiB ceiling, a string of the whole budget plus its key crosses."""
    fields, flag = sanitize_fields({"a": "x" * _BYTES}, cfg=Config(max_value_bytes=32 * _MiB))
    assert fields == {"a": _SIZE_LIMIT}
    assert flag is True


_BIG_DECIMAL_10M = Decimal("1" * 10_000_000)


@pytest.mark.parametrize("route", _ROUTES)
def test_a_shared_huge_decimal_returns_within_the_byte_budget(route: str) -> None:
    """FR-002 AC7: 256 references to one 10,000,000-digit ``Decimal``, under the deadline."""
    sink = _configure()
    event = _emit(route, {"a": [_BIG_DECIMAL_10M] * 256}, sink, deadline=5)
    assert event["fields"] == {"a": ["1" * 8178 + TRUNCATION_MARKER, _SIZE_LIMIT]}
    assert event.get("truncated") is True


@pytest.mark.parametrize("route", _ROUTES)
def test_a_dag_of_a_shared_long_string_returns_within_the_byte_budget(route: str) -> None:
    """FR-002 AC8: each leaf clips to 8,192 bytes of two-byte text, so the 2,048th crosses."""
    sink = _configure()
    event = _emit(route, {"a": _fan_out("é" * 1_048_576)}, sink, deadline=5)
    fields = event["fields"]
    assert isinstance(fields, dict)
    leaves = list(_leaves(fields["a"]))
    assert leaves[:2047] == ["é" * 4089 + TRUNCATION_MARKER] * 2047
    assert leaves[2047:] == [_SIZE_LIMIT]
    assert _NODE_LIMIT not in leaves
    assert event.get("truncated") is True


@pytest.mark.parametrize("route", _ROUTES)
def test_a_nest_of_long_produced_keys_returns_within_the_byte_budget(route: str) -> None:
    """FR-002 AC9: three levels of 256 keys whose ``str()`` is a mebibyte; the 14th inner crosses.

    ``a`` costs 1, a key ``(s, i)`` its ``str()`` length (1,048,583 for ``i`` below 10, 1,048,584
    for 10 to 99) and a ``0`` costs 1, so after the outer and middle keys the innermost mapping
    fits thirteen entries. Those thirteen keys clip to one string and collapse into one entry.
    """
    s = "x" * 1_048_576
    inner = {(s, i): 0 for i in range(256)}
    middle = {(s, i): inner for i in range(256)}
    outer = {(s, i): middle for i in range(256)}
    sink = _configure()
    event = _emit(route, {"a": outer}, sink, deadline=5)
    key = "('" + "x" * 8176 + TRUNCATION_MARKER
    assert event["fields"] == {"a": {key: {key: {key: 0, _SIZE_LIMIT: _SIZE_LIMIT}}}}
    assert event.get("truncated") is True


class _ProducedFallback(Enum):
    """An enum whose member value has no rule of its own, so it renders as ``str(member)``."""

    MEMBER_WITH_A_LONG_NAME = 2j


class _KeyBoom:
    """A key whose ``__str__`` raises, so it becomes the key placeholder."""

    def __str__(self) -> str:
        raise RuntimeError("no")


class _Site(NamedTuple):
    """One produced-text site: what it is fed, at which ceiling, what it costs and writes."""

    id: str
    slot: str
    probe: object
    ceiling: int
    charge: int
    written: str


_EMOJI_KEY = ("😀" * 20,)
_EMOJI_KEY_TEXT = "('" + "😀" * 20 + "',)"

_PRODUCED_SITES: list[_Site] = [
    _Site("decimal", "value", Decimal("1" * 40), 14, 40, TRUNCATION_MARKER),
    _Site("uuid", "value", UUID("12345678-1234-5678-1234-567812345678"), 14, 36, TRUNCATION_MARKER),
    _Site("datetime", "value", datetime(2026, 1, 1, 12, 30, 45, 123456), 14, 26, TRUNCATION_MARKER),
    _Site("time", "value", time(12, 30, 45, 123456, tzinfo=UTC), 14, 21, TRUNCATION_MARKER),
    _Site(
        "enum-fallback",
        "value",
        _ProducedFallback.MEMBER_WITH_A_LONG_NAME,
        14,
        41,
        TRUNCATION_MARKER,
    ),
    _Site("value-placeholder", "value", _Opaque(), 14, 25, TRUNCATION_MARKER),
    _Site("tuple-key", "key", ("x" * 20,), 14, 25, TRUNCATION_MARKER),
    _Site("float-key", "key", 1.2345678901234567, 14, 18, TRUNCATION_MARKER),
    _Site("int-key-past-the-ceiling", "key", 10**20, 14, 17, TRUNCATION_MARKER),
    _Site("key-placeholder", "key", _KeyBoom(), 14, 30, TRUNCATION_MARKER),
    _Site("non-ascii-key-unclipped", "key", _EMOJI_KEY, 8192, 85, _EMOJI_KEY_TEXT),
]


@pytest.mark.parametrize("site", _PRODUCED_SITES, ids=[site.id for site in _PRODUCED_SITES])
def test_each_produced_text_site_is_charged_before_clipping(
    monkeypatch: pytest.MonkeyPatch, site: _Site
) -> None:
    """Produced text is charged the larger of its characters before clipping and its bytes after.

    At ``max_value_bytes=14`` every clipped site's text becomes the marker alone, so a site
    charged after clipping (14) and one charged before it (its length in characters) differ.
    The non-ASCII key is the other half: unclipped at the default ceiling, its 25 characters are
    85 bytes, so a site charged its characters alone would undercharge it. The budget is patched
    to sit exactly on the charge, then one byte under it: the site fits, then crosses, which pins
    the charge to exactly its number in both directions. A ``date`` is not here: it is ten
    characters, never clipped where these are, so both charges are the same number.
    """
    keyed = site.slot == "key"
    fields: dict[str, object] = {"a": {site.probe: 0}} if keyed else {"a": site.probe}
    tail = 1 if keyed else 0
    cfg = Config(max_value_bytes=site.ceiling)
    monkeypatch.setattr(sanitize, "_MAX_RENDERED_BYTES", 1 + site.charge + tail)
    fits, flag = sanitize_fields(fields, cfg=cfg)
    expected: object = site.written if site.slot == "value" else {site.written: 0}
    assert fits == {"a": expected}
    assert flag is (site.written == TRUNCATION_MARKER)
    monkeypatch.setattr(sanitize, "_MAX_RENDERED_BYTES", site.charge)
    crossed, flag = sanitize_fields(fields, cfg=cfg)
    expected = _SIZE_LIMIT if site.slot == "value" else {_SIZE_LIMIT: _SIZE_LIMIT}
    assert crossed == {"a": expected}
    assert flag is True


def test_the_produced_sites_are_as_long_as_the_table_says() -> None:
    """The per-site test's premise, measured with the standard library rather than with ``src``."""
    texts = {
        "decimal": str(Decimal("1" * 40)),
        "uuid": str(UUID("12345678-1234-5678-1234-567812345678")),
        "datetime": datetime(2026, 1, 1, 12, 30, 45, 123456).isoformat(),
        "time": time(12, 30, 45, 123456, tzinfo=UTC).isoformat(),
        "enum-fallback": str(_ProducedFallback.MEMBER_WITH_A_LONG_NAME),
        "value-placeholder": "<unserializable: _Opaque>",
        "tuple-key": str(("x" * 20,)),
        "float-key": str(1.2345678901234567),
        "int-key-past-the-ceiling": "<int: ~21 digits>",
        "key-placeholder": "<unserializable key: _KeyBoom>",
        "non-ascii-key-unclipped": str(_EMOJI_KEY),
    }
    assert set(texts) == {site.id for site in _PRODUCED_SITES}
    for site in _PRODUCED_SITES:
        text = texts[site.id]
        if site.ceiling == 14:
            assert len(text) == site.charge > 16, site.id
        else:
            assert text == site.written, site.id
            assert len(text) == 25, site.id
            assert len(text.encode("utf-8")) == site.charge <= site.ceiling, site.id


class _CallerText(NamedTuple):
    """One caller-text case: the value, its ceiling, what it costs and what it is written as."""

    id: str
    value: object
    ceiling: int
    charge: int
    written: str


_CALLER_TEXTS: list[_CallerText] = [
    _CallerText("clip-mid-character", "😀" * 3000, 8192, 8190, "😀" * 2044 + TRUNCATION_MARKER),
    _CallerText("clipped-bytes", b"x" * 20_000, 8192, 8192, "x" * 8178 + TRUNCATION_MARKER),
    _CallerText("marker-alone", "x" * 20, 10, 14, TRUNCATION_MARKER),
    _CallerText("unclipped-non-ascii", "é" * 100, 8192, 200, "é" * 100),
]


@pytest.mark.parametrize("case", _CALLER_TEXTS, ids=[case.id for case in _CALLER_TEXTS])
def test_caller_text_is_charged_its_utf8_length_after_clipping(
    monkeypatch: pytest.MonkeyPatch, case: _CallerText
) -> None:
    """A caller's text costs exactly the UTF-8 bytes it is written as, wherever the clip lands.

    The budget-exactness test clips only where a cut lands on a whole character, so these pin
    the rest. Four-byte characters leave 8,176 of the 8,178 head bytes, so the clip is 8,190
    bytes, not the ceiling. Bytes are the caller's text once decoded, charged as such: the
    8,193-character prefix read from them would cost one more as produced text. A marker that
    stands alone under a ceiling below its own length costs its 14 bytes, not the ceiling. And
    unclipped text costs its bytes, not its characters. The budget is patched to sit exactly on
    the charge, then one byte under it, so the charge is pinned in both directions.
    """
    cfg = Config(max_value_bytes=case.ceiling)
    monkeypatch.setattr(sanitize, "_MAX_RENDERED_BYTES", 1 + case.charge)
    fits, flag = sanitize_fields({"a": case.value}, cfg=cfg)
    assert fits == {"a": case.written}
    assert flag is (case.written.endswith(TRUNCATION_MARKER))
    monkeypatch.setattr(sanitize, "_MAX_RENDERED_BYTES", case.charge)
    crossed, flag = sanitize_fields({"a": case.value}, cfg=cfg)
    assert crossed == {"a": _SIZE_LIMIT}
    assert flag is True


def test_the_caller_texts_are_as_long_as_the_table_says() -> None:
    """The caller-text test's premise, measured with the standard library rather than ``src``."""
    for case in _CALLER_TEXTS:
        assert len(case.written.encode("utf-8")) == case.charge, case.id
    assert len("😀".encode()) == 4
    assert len(TRUNCATION_MARKER.encode("utf-8")) == 14


def test_the_budgets_add_no_public_name() -> None:
    """FR-001 AC10: both budgets and both placeholders stay private."""
    assert sanitize.__all__ == [
        "TRUNCATION_MARKER",
        "coerce",
        "sanitize_fields",
        "truncate_str",
        "truncate_tail",
    ]
    for name in ("_MAX_NODES", "_MAX_RENDERED_BYTES", "_NODE_LIMIT", "_SIZE_LIMIT"):
        assert name not in sanitize.__all__
        assert name not in lf.__all__


def test_the_budgets_are_the_specified_numbers() -> None:
    """The Data Model's two numbers, read directly: the ``getattr`` defaults would hide a drift."""
    assert sanitize._MAX_NODES == 131_072  # type: ignore[attr-defined]
    assert sanitize._MAX_RENDERED_BYTES == 16 * 1024 * 1024  # type: ignore[attr-defined]
