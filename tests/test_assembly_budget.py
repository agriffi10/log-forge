"""SPEC-057 — every coercion pass is bounded in work: reads cost the ceiling, and two budgets.

FR-003 bounds what one *read* costs: ``truncate_str``, ``truncate_tail`` and the bytes-like branch
of the coercer read a prefix (or suffix) of the ceiling plus one, never the whole input. The
rendering must be byte-identical to the unbounded computation it replaced, so the tests below hold
it to a frozen reference copy of that computation rather than to the argument in the docstrings.

This module is also run once against ``main``'s ``sanitize.py`` (FR-001's third criterion), so it
must collect there: it imports only names that exist on ``main``, reads the two budgets through
``getattr`` with a default, and writes the two new placeholders as literals. Failure messages
render values through ``ascii()`` and truncate them, because a lone surrogate in a plain ``repr``
crashes the xdist worker that tries to report it.
"""

from __future__ import annotations

import ctypes
import re
import tracemalloc
from typing import TYPE_CHECKING

import pytest

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
    """FR-003 AC4: two stdlib views, one whose single row exceeds the ceiling."""
    buf = (bytes(range(256)) * 110)[:28_000]
    narrow_rows = memoryview(buf).cast("B", shape=[2000, 14])[::2]
    one_wide_row = memoryview(bytes(range(97, 123)) * 1154)[:30_000].cast("B", shape=[3, 10_000])
    for view in (narrow_rows, one_wide_row[::2]):
        assert not view.c_contiguous
        fields, flag = sanitize_fields({"k": view}, cfg=Config())
        assert (fields["k"], flag) == _ref_bytes_like(view, 8192), view.shape
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
