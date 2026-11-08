"""A header value that is not a ``str`` is copied once, and sends the bytes it always sent.

``_valid_headers`` copied every header name and ``str`` value as exact ``str``, but kept any other
value as the caller's object, so ``http.client.putheader`` ran that object's own code on every
request: an ``encode`` it found by ``hasattr`` (so ``__getattribute__`` too), or an ``int``
subclass's ``__str__``. Each value is now copied once, at construction, into what ``putheader``
would have sent — exact ``bytes`` for any value that is the caller's own object, the value itself
for an exact ``int``, ``bool`` or ``bytes`` — and every request sends the copy (invariants 11 and
13). The bytes on the wire are unchanged: the head pinned below was captured on a raw socket at
``c4e51fbd``, before the copy, with these very headers.
"""

from __future__ import annotations

import enum
import http.client
from typing import Any

import pytest

from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.logstash import LogstashSink
from test_sinks_http import FakeOpener
from test_sinks_http_header_collisions import _raw_request
from test_sinks_http_render import _RECIPES, _shipped_http_subclasses

URL = "http://h/x"


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Neutralize the HTTP backoff so a failing case cannot stall the suite."""
    monkeypatch.setattr("log_foundry.sinks._retry.wait", lambda _delay, _stop=None: None)


class _PutheaderOpener(FakeOpener):
    """A ``FakeOpener`` that also puts every header through ``http.client.putheader``.

    A bare ``FakeOpener`` never reaches ``putheader``, so a value's per-request code would not run
    under it whether or not the sink copied the value — a counting test on it is vacuous. This
    runs ``putheader`` on a started request exactly as ``urllib``'s ``do_open`` does, under the
    ``title``-d name, with no socket, and keeps the header lines it produced.
    """

    def __init__(self) -> None:
        super().__init__()
        self.lines: list[list[bytes]] = []

    def __call__(self, request, timeout=None):
        conn = http.client.HTTPConnection("localhost")
        conn.putrequest("POST", "/", skip_host=True, skip_accept_encoding=True)
        for name, value in request.header_items():
            conn.putheader(name.title(), value)
        self.lines.append(list(conn._buffer[1:]))
        return super().__call__(request, timeout)


class _IntSub(int):
    """An ``int`` subclass with no methods of its own: ``putheader`` sends its digits."""


class _OwnStr(int):
    """An ``int`` subclass whose own ``__str__`` ``putheader``'s ``int`` branch calls."""

    def __str__(self) -> str:
        return "own"


class _Color(enum.IntEnum):
    """An ``IntEnum``: ``str()`` is the digits on 3.11 and later."""

    RED = 1


class _Mixed(int, enum.Enum):
    """An ``int`` mix-in enum: ``str()`` is ``_Mixed.A``, and that is what was sent."""

    A = 3


class _Encodes:
    """Not an ``int`` and not bytes: ``putheader`` sends what its ``encode("latin-1")`` returns."""

    def encode(self, codec: str) -> bytes:
        return b"enc-" + codec.encode()


class _BytesSub(bytes):
    """A ``bytes`` subclass: no ``encode``, so ``putheader`` passes its buffer through."""


def _values() -> dict[str, Any]:
    """The header values whose wire bytes were captured at ``c4e51fbd``, built fresh per call."""
    return {
        "X-Int": 5,
        "X-Zero": 0,
        "X-Neg": -12,
        "X-True": True,
        "X-False": False,
        "X-Bytes": b"raw-bytes",
        "X-Bytearray": bytearray(b"ba"),
        "X-Memoryview": memoryview(b"mv"),
        "X-Intsub": _IntSub(7),
        "X-Ownstr": _OwnStr(9),
        "X-Intenum": _Color.RED,
        "X-Mixed": _Mixed.A,
        "X-Enc": _Encodes(),
        "X-Bytessub": _BytesSub(b"bs"),
        "X-Latin": b"\xe9",
    }


_HEAD_AT_C4E51FBD = (
    b"POST /x HTTP/1.1\r\nAccept-Encoding: identity\r\nContent-Length: 9\r\n"
    b"Host: 127.0.0.1:PORT\r\nUser-Agent: Python-urllib/V\r\n"
    b"Content-Type: application/x-ndjson\r\nX-Int: 5\r\nX-Zero: 0\r\nX-Neg: -12\r\n"
    b"X-True: True\r\nX-False: False\r\nX-Bytes: raw-bytes\r\nX-Bytearray: ba\r\n"
    b"X-Memoryview: mv\r\nX-Intsub: 7\r\nX-Ownstr: own\r\nX-Intenum: 1\r\nX-Mixed: _Mixed.A\r\n"
    b"X-Enc: enc-latin-1\r\nX-Bytessub: bs\r\nX-Latin: \xe9\r\nConnection: close"
)
"""Captured at ``c4e51fbd`` on a raw socket with :func:`_values`, the classes renamed only.

``True`` goes out as ``True``, not ``1``: ``putheader``'s ``int`` branch is ``str(value)``."""


def test_every_value_sends_the_bytes_it_sent_before_the_copy() -> None:
    head = _raw_request(lambda url: HTTPSink(url, headers=_values()).emit([{"i": 1}]))

    assert head == _HEAD_AT_C4E51FBD


def test_a_plain_int_bool_or_bytes_is_stored_as_given_and_the_callers_objects_as_bytes() -> None:
    """An exact ``int``, ``bool`` or ``bytes`` runs no caller code in ``putheader`` and cannot
    change, so it is kept; every other value is the caller's object and is stored as a copy."""
    opener = FakeOpener()
    sink = HTTPSink(URL, headers=_values(), opener=opener)
    sink.emit([{"i": 1}])
    sent = opener.calls[-1]["headers"]

    for name, value in (
        ("x-int", 5),
        ("x-true", True),
        ("x-false", False),
        ("x-bytes", b"raw-bytes"),
    ):
        assert sent[name] == value and type(sent[name]) is type(value), name
    for name, value in (
        ("x-bytearray", b"ba"),
        ("x-memoryview", b"mv"),
        ("x-intsub", b"7"),
        ("x-ownstr", b"own"),
        ("x-intenum", b"1"),
        ("x-mixed", b"_Mixed.A"),
        ("x-enc", b"enc-latin-1"),
        ("x-bytessub", b"bs"),
    ):
        assert sent[name] == value and type(sent[name]) is bytes, name


class _Counted:
    """Counts every attribute read and every ``encode``, the code ``putheader`` ran per request."""

    reads = 0
    encodes = 0

    def __getattribute__(self, name: str) -> Any:
        type(self).reads += 1
        return object.__getattribute__(self, name)

    def encode(self, codec: str) -> bytes:
        type(self).encodes += 1
        return b"counted"


class _CountedInt(int):
    """An ``int`` subclass counting its ``__str__``, which ``putheader``'s ``int`` branch calls."""

    calls = 0

    def __str__(self) -> str:
        type(self).calls += 1
        return "41"


class _ClaimsEverything(type):
    """A metaclass whose ``__eq__`` says its classes equal any type, ``str`` included."""

    def __eq__(cls, other: object) -> bool:
        return True

    __hash__ = type.__hash__


class _PosesAsStr(metaclass=_ClaimsEverything):
    """A value whose type compares equal to ``str``; its ``encode`` answers anew each call."""

    calls = 0

    def encode(self, codec: str) -> bytes:
        type(self).calls += 1
        return f"v{type(self).calls}".encode()


def test_a_callers_value_runs_none_of_its_code_on_any_emit() -> None:
    """At ``c4e51fbd``: each of these ran once more on every emit, inside ``putheader``.

    ``_PosesAsStr`` is the one a membership test lets through: ``type(value) in (str, ...)``
    compares with ``==``, and its metaclass answers ``True``, so the caller's object was stored
    and the wire sent ``v2``, ``v3``, ``v4``. The plain types are recognised by identity."""
    _Counted.reads = _Counted.encodes = _CountedInt.calls = _PosesAsStr.calls = 0
    opener = _PutheaderOpener()
    sink = HTTPSink(
        URL,
        headers={"X-A": _Counted(), "X-B": _CountedInt(41), "X-C": _PosesAsStr()},
        opener=opener,
    )
    built = (_Counted.reads, _Counted.encodes, _CountedInt.calls, _PosesAsStr.calls)

    for i in range(4):
        sink.emit([{"i": i}])

    assert (_Counted.reads, _Counted.encodes, _CountedInt.calls, _PosesAsStr.calls) == built
    for lines in opener.lines:
        assert b"X-A: counted" in lines and b"X-B: 41" in lines, lines
        assert b"X-C: v1" in lines, lines
    assert len(opener.lines) == 4


def test_a_callers_value_runs_its_code_a_bounded_number_of_times_at_construction() -> None:
    """Once for the copy, and ``__str__`` once more for the ``CR``/``LF`` check, as before."""
    _Counted.encodes = _CountedInt.calls = 0

    HTTPSink(URL, headers={"X-A": _Counted(), "X-B": _CountedInt(41)})

    assert _Counted.encodes == 1
    assert _CountedInt.calls == 2


class _Changing:
    """An ``encode`` answering a new value each time, as a value read live would send."""

    calls = 0

    def encode(self, codec: str) -> bytes:
        type(self).calls += 1
        return f"v{type(self).calls}".encode()


def test_a_value_that_answers_differently_is_frozen_at_what_it_answered_at_construction() -> None:
    _Changing.calls = 0
    opener = _PutheaderOpener()
    sink = HTTPSink(URL, headers={"X-Live": _Changing()}, opener=opener)

    sink.emit([{"i": 1}])
    sink.emit([{"i": 2}])

    assert [b"X-Live: v1" in lines for lines in opener.lines] == [True, True]


def test_a_mutable_bytes_value_changed_afterwards_is_not_sent() -> None:
    value = bytearray(b"before")
    opener = _PutheaderOpener()
    sink = HTTPSink(URL, headers={"X-Buf": value}, opener=opener)

    value[:] = b"after!"
    sink.emit([{"i": 1}])

    assert b"X-Buf: before" in opener.lines[-1]


class _InjectingEncode:
    """An ``encode`` returning bytes that would start a second header line."""

    def __str__(self) -> str:
        return "harmless"

    def encode(self, codec: str) -> bytes:
        return b"v\r\nX-Injected: 1"


class _InjectingInt(int):
    """An ``int`` subclass whose text carries ``CR LF``: the ``CR``/``LF`` check reads it."""

    def __str__(self) -> str:
        return "1\r\nX-Injected: 1"


@pytest.mark.parametrize(
    ("value", "message"),
    [
        (_InjectingEncode(), "HTTPSink header cannot be sent over HTTP (ValueError): 'X-V'"),
        (_InjectingInt(1), "HTTPSink header value must not contain CR or LF: 'X-V'"),
        (b"v\r\nX-Injected: 1", "HTTPSink header cannot be sent over HTTP (ValueError): 'X-V'"),
        (
            bytearray(b"v\nX-Injected: 1"),
            "HTTPSink header cannot be sent over HTTP (ValueError): 'X-V'",
        ),
    ],
    ids=["encode", "int-str", "bytes", "bytearray"],
)
def test_a_copy_that_would_inject_a_header_is_refused(value: object, message: str) -> None:
    """The copy meets the checks the value met before: ``putheader``'s on the bytes, ``CR``/``LF``
    on ``str(value)``. The message names the header, never the value."""
    with pytest.raises(ValueError) as caught:
        HTTPSink(URL, headers={"X-V": value})

    assert str(caught.value) == message
    assert "Injected" not in str(caught.value)
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


class _EncodesToStr:
    """An ``encode`` returning ``str``, which ``putheader``'s legality check cannot read."""

    def encode(self, codec: str) -> str:
        return "text"


class _EncodeRaises:
    """An ``encode`` raising ``UnicodeEncodeError``, as a value outside Latin-1 does."""

    def encode(self, codec: str) -> bytes:
        raise UnicodeEncodeError(codec, "SECRET", 0, 1, "outside latin-1")


class _LatinInt(int):
    """An ``int`` subclass whose text is not ASCII: ``putheader`` encodes it as ASCII."""

    def __str__(self) -> str:
        return "é"


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        (1.5, "TypeError"),
        (None, "TypeError"),
        (_EncodesToStr(), "TypeError"),
        (_EncodeRaises(), "UnicodeEncodeError"),
        (_LatinInt(1), "UnicodeEncodeError"),
        (memoryview(b"abcdef")[::2], "TypeError"),
    ],
    ids=["float", "none", "encode-to-str", "encode-raises", "non-ascii-int", "strided-view"],
)
def test_a_value_putheader_refused_keeps_its_refusal(value: object, reason: str) -> None:
    """Each was refused at ``c4e51fbd`` with this message, by ``putheader`` on the caller's value;
    the copy fails the same way, so nothing that constructed is refused and nothing refused
    constructs."""
    with pytest.raises(ValueError) as caught:
        HTTPSink(URL, headers={"X-V": value})

    assert str(caught.value) == f"HTTPSink header cannot be sent over HTTP ({reason}): 'X-V'"
    assert "SECRET" not in str(caught.value)
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


def _logstash(**kw: Any) -> LogstashSink:
    """``LogstashSink`` wraps an ``HTTPSink`` in HTTP mode rather than subclassing one."""
    return LogstashSink(url=URL, **kw)


_HEADER_TAKERS: dict[str, Any] = {
    **_RECIPES,
    "HTTPSink": lambda **kw: HTTPSink(URL, **kw),
    "LogstashSink": _logstash,
}
"""Every class taking ``headers``: the shipped subclasses through their recipes, ``HTTPSink`` and
``LogstashSink``'s HTTP mode. ``SentrySink`` takes no ``headers``."""


def test_the_header_roster_covers_every_shipped_subclass() -> None:
    assert {cls.__name__ for cls in _shipped_http_subclasses()} <= set(_HEADER_TAKERS)


@pytest.mark.parametrize("name", sorted(_HEADER_TAKERS))
def test_every_class_taking_headers_sends_the_copy_and_runs_no_caller_code_per_emit(
    name: str,
) -> None:
    _Counted.reads = _Counted.encodes = _CountedInt.calls = 0
    opener = _PutheaderOpener()
    sink = _HEADER_TAKERS[name](headers={"X-A": _Counted(), "X-B": _CountedInt(41)}, opener=opener)
    built = (_Counted.reads, _Counted.encodes, _CountedInt.calls)

    sink.emit([{"i": 1}])
    sink.emit([{"i": 2}])

    assert (_Counted.reads, _Counted.encodes, _CountedInt.calls) == built
    assert opener.lines, "no request reached putheader"
    for lines in opener.lines:
        assert b"X-A: counted" in lines and b"X-B: 41" in lines, lines


def test_a_contiguous_view_of_more_than_one_dimension_still_sends_its_bytes() -> None:
    """``putheader`` read a C-contiguous buffer of any shape, so this constructed and sent
    ``abcdef`` at ``c4e51fbd``; only a strided view, which it could not read, is refused."""
    opener = _PutheaderOpener()
    sink = HTTPSink(URL, headers={"X-V": memoryview(b"abcdef").cast("B", (2, 3))}, opener=opener)

    sink.emit([{"i": 1}])

    assert b"X-V: abcdef" in opener.lines[-1]
