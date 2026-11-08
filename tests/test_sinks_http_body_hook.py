"""A subclass's own ``_body`` may label its own coding, and compression is fixed at construction.

``_body`` is a documented 1.x extension hook. A subclass whose ``_body`` encodes the body itself —
``zlib.compress`` beside ``Content-Encoding: deflate``, or its own gzip beside ``gzip`` — built with
``gzip=False`` sends a true label and constructed at ``c4e51fbd``; the 1.2.0 refusal of
``gzip=False`` beside a coding holds only for the library's own ``_body``, read statically
(``_library_body``), so such a sink still constructs and sends the bytes it sent. Whether the body
is compressed is fixed at construction whenever a caller sets a ``Content-Encoding``, so that
subclass is never gzipped a second time when ``gzip`` is set later; and ``gzip`` itself is read
once, so a caller ``__bool__`` runs at construction and never per request (invariants 10, 11, 13).
"""

from __future__ import annotations

import gzip
import re
import socket
import threading
import zlib
from typing import TYPE_CHECKING, Any, ClassVar

import pytest

from log_foundry.sinks import http as http_sink
from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.logstash import LogstashSink
from test_sinks_http import FakeOpener
from test_sinks_http_render import _RECIPES, _shipped_http_subclasses

if TYPE_CHECKING:
    from collections.abc import Callable

URL = "http://h/x"
PLAIN = b'{"i": 1}\n'
"""What the library's own ``_body`` builds for ``[{"i": 1}]``, written out."""


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Neutralize the HTTP backoff so a failing case cannot stall the suite."""
    monkeypatch.setattr("log_foundry.sinks.http.wait", lambda _delay, _stop=None: None)


def _exchange(send: Callable[[str], None]) -> tuple[bytes, bytes]:
    """Serves one request on a raw socket; returns its head (port, version scrubbed) and body."""
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    port = server.getsockname()[1]
    got: list[tuple[bytes, bytes]] = []

    def serve() -> None:
        conn, _ = server.accept()
        with conn:
            data = b""
            while b"\r\n\r\n" not in data:
                data += conn.recv(65536)
            head, _, rest = data.partition(b"\r\n\r\n")
            found = re.search(rb"Content-Length: (\d+)", head)
            length = int(found.group(1)) if found else 0
            while len(rest) < length:
                rest += conn.recv(65536)
            got.append((head, rest))
            conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        send(f"http://127.0.0.1:{port}/x")
    finally:
        thread.join(5)
        server.close()
    head, body = got[0]
    head = re.sub(rb"Python-urllib/[0-9.]+", b"Python-urllib/V", head)
    return head.replace(str(port).encode(), b"PORT"), body


class _Deflating(HTTPSink):
    """A 1.x subclass that deflates its own body and labels it ``deflate``.

    Attributes:
      built: Every body this ``_body`` returned, so the wire can be held to it byte for byte.
    """

    built: ClassVar[list[bytes]] = []

    def _body(self, items: Any) -> tuple[bytes, str]:
        body, content_type = super()._body(items)
        out = zlib.compress(body)
        type(self).built.append(out)
        return out, content_type


class _SelfGzipping(HTTPSink):
    """A 1.x subclass that gzips its own body and labels it ``gzip``, with ``gzip=False``.

    Attributes:
      built: Every body this ``_body`` returned.
    """

    built: ClassVar[list[bytes]] = []

    def _body(self, items: Any) -> tuple[bytes, str]:
        body, content_type = super()._body(items)
        out = gzip.compress(body, mtime=0)
        type(self).built.append(out)
        return out, content_type


def _head(length: int, coding: str) -> bytes:
    """The request head both subclasses sent at ``c4e51fbd``, captured on a raw socket there."""
    return (
        b"POST /x HTTP/1.1\r\nAccept-Encoding: identity\r\nContent-Length: "
        + str(length).encode()
        + b"\r\nHost: 127.0.0.1:PORT\r\nUser-Agent: Python-urllib/V\r\n"
        b"Content-Type: application/x-ndjson\r\nContent-Encoding: "
        + coding.encode()
        + b"\r\nConnection: close"
    )


@pytest.mark.parametrize("given", [False, True], ids=["gzip-false", "gzip-true"])
def test_a_deflating_body_hook_constructs_and_sends_what_it_sent_in_1_1_0(given: bool) -> None:
    """The head and 17-byte body below were captured on a raw socket against ``v1.1.0`` with
    either ``gzip``: a caller ``Content-Encoding`` suppressed compression there. With
    ``gzip=False`` it was refused at ``d247ca31``; with ``gzip=True`` at ``c4e51fbd`` and every
    head since, by the ``gzip=True`` refusal, which now holds only for the library's ``_body``."""
    _Deflating.built.clear()

    head, body = _exchange(
        lambda url: _Deflating(url, headers={"Content-Encoding": "deflate"}, gzip=given).emit(
            [{"i": 1}]
        )
    )

    assert head == _head(17, "deflate")
    assert body == _Deflating.built[-1] == b"x\x9c\xabV\xcaT\xb2R0\xac\xe5\x02\x00\x0cP\x02;"
    assert zlib.decompress(body) == PLAIN


@pytest.mark.parametrize("given", [False, True], ids=["gzip-false", "gzip-true"])
def test_a_self_gzipping_body_hook_sends_what_it_built_once_as_in_1_1_0(given: bool) -> None:
    """Against ``v1.1.0`` either ``gzip`` sent the hook's 29-byte body once under its ``gzip``
    label. With ``gzip=False`` it was refused at ``d247ca31``; with ``gzip=True`` it was gzipped a
    second time (46 bytes) at ``c4e51fbd`` and every head since. Its gzip header's OS byte differs
    between interpreters, so the body is held to what its own ``_body`` returned."""
    _SelfGzipping.built.clear()

    head, body = _exchange(
        lambda url: _SelfGzipping(url, headers={"Content-Encoding": "gzip"}, gzip=given).emit(
            [{"i": 1}]
        )
    )

    assert head == _head(29, "gzip")
    assert body == _SelfGzipping.built[-1]
    assert gzip.decompress(body) == PLAIN


def test_a_self_gzipping_body_hook_is_never_gzipped_again_when_gzip_is_set_later() -> None:
    """At ``c4e51fbd``: setting ``gzip`` true gzipped the body a second time under one ``gzip``
    label (46 bytes on the wire), undecodable as the label says. The decision is fixed at
    construction from ``gzip=False``, so the body goes out as the hook built it."""
    _SelfGzipping.built.clear()

    def send(url: str) -> None:
        sink = _SelfGzipping(url, headers={"Content-Encoding": "gzip"})
        sink.gzip = True
        sink.emit([{"i": 1}])

    head, body = _exchange(send)

    assert head == _head(29, "gzip")
    assert body == _SelfGzipping.built[-1]
    assert gzip.decompress(body) == PLAIN


class _PassThrough(HTTPSink):
    """Overrides ``_body`` only to call the library's: a static read cannot see that."""

    def _body(self, items: Any) -> tuple[bytes, str]:
        return super()._body(items)


def test_a_pass_through_body_hook_is_read_as_the_subclasss_own_and_constructs() -> None:
    """The documented edge: where ``_body`` is defined is read, not what it does, so the sink
    constructs as it did in 1.1.0 and sends its unencoded body under the label it was given."""
    opener = FakeOpener()
    sink = _PassThrough(URL, headers={"Content-Encoding": "deflate"}, opener=opener)

    sink.emit([{"i": 1}])

    assert http_sink._library_body(sink) is False
    assert opener.calls[-1]["body"] == PLAIN
    assert opener.calls[-1]["headers"]["content-encoding"] == "deflate"


def test_a_body_set_on_the_instance_is_read_as_the_callers() -> None:
    sink = HTTPSink(URL)
    sink._body = lambda items: (b"x", "text/plain")  # type: ignore[method-assign]

    assert http_sink._library_body(sink) is False


def test_a_pass_through_body_hook_beside_gzip_true_sends_an_uncompressed_body_as_1_1_0_did() -> (
    None
):
    """The cost the static read accepts: it cannot tell this from an encoding hook, so the library
    does not compress it, and the body goes out uncompressed under a ``gzip`` label, which is what
    ``v1.1.0`` sent."""
    opener = FakeOpener()
    sink = _PassThrough(URL, headers={"Content-Encoding": "gzip"}, gzip=True, opener=opener)

    sink.emit([{"i": 1}])

    assert opener.calls[-1]["body"] == PLAIN
    assert opener.calls[-1]["headers"]["content-encoding"] == "gzip"


def _deflate(items: Any) -> tuple[bytes, str]:
    """A body hook assigned to an instance: the library's body, deflated."""
    body, content_type = HTTPSink._body(_PLAIN_SINK, items)
    return zlib.compress(body), content_type


_PLAIN_SINK = HTTPSink(URL)
"""A sink whose library ``_body`` the instance-assigned hook calls."""


class _AssignsAfter(HTTPSink):
    """Assigns its body hook to the instance after ``super().__init__`` has returned."""

    def __init__(self, url: str, **kw: Any) -> None:
        super().__init__(url, headers={"Content-Encoding": "deflate"}, **kw)
        self._body = _deflate  # type: ignore[method-assign]


class _AssignsBefore(HTTPSink):
    """Assigns the same hook before calling ``super().__init__``."""

    def __init__(self, url: str, **kw: Any) -> None:
        self._body = _deflate  # type: ignore[method-assign]
        super().__init__(url, headers={"Content-Encoding": "deflate"}, **kw)


def test_a_body_assigned_after_super_init_is_unseen_and_refused_the_accepted_edge() -> None:
    """Construction cannot see an assignment made after it returns, so the sink is judged as the
    library's and refused, where ``v1.1.0`` constructed it. Assigning before ``super().__init__``,
    or defining ``_body`` on the class (``_Deflating``), constructs."""
    with pytest.raises(ValueError) as caught:
        _AssignsAfter(URL)

    assert str(caught.value) == (
        "_AssignsAfter gzip=False, the default, cannot apply beside header 'Content-Encoding', "
        "which names an encoding the body is not in; drop the header, or pass gzip=True if it "
        "names gzip"
    )
    opener = FakeOpener()
    _AssignsBefore(URL, opener=opener).emit([{"i": 1}])
    assert zlib.decompress(opener.calls[-1]["body"]) == PLAIN


def _overriders() -> list[type[HTTPSink]]:
    """Every shipped ``HTTPSink`` subclass defining ``_body`` itself, derived from the roster."""
    return [cls for cls in _shipped_http_subclasses() if "_body" in vars(cls)]


def test_the_shipped_body_overriders_are_found() -> None:
    """A floor, so the sweep below cannot pass by finding nothing."""
    assert len(_overriders()) >= 3, [cls.__name__ for cls in _overriders()]


@pytest.mark.parametrize("cls", _overriders(), ids=lambda cls: cls.__name__)
def test_every_shipped_body_override_is_the_librarys_own_and_meets_the_refusal(cls) -> None:
    sink = _RECIPES[cls.__name__](opener=FakeOpener())
    assert http_sink._library_body(sink) is True

    with pytest.raises(ValueError) as caught:
        _RECIPES[cls.__name__](headers={"content-encoding": "br"})

    assert str(caught.value) == (
        f"{cls.__name__} gzip=False, the default, cannot apply beside header 'content-encoding', "
        "which names an encoding the body is not in; drop the header, or pass gzip=True if it "
        "names gzip"
    )


@pytest.mark.parametrize(
    "make",
    [lambda: HTTPSink(URL), lambda: LogstashSink(url=URL)._http],
    ids=["HTTPSink", "LogstashSink-http"],
)
def test_the_base_body_is_the_librarys_own(make) -> None:
    assert http_sink._library_body(make()) is True


class _Recording(type):
    """A metaclass recording every class attribute read, and claiming the package as its module."""

    reads: ClassVar[list[str]] = []

    def __getattribute__(cls, name: str) -> Any:
        _Recording.reads.append(name)
        return type.__getattribute__(cls, name)

    def __getattr__(cls, name: str) -> Any:
        _Recording.reads.append("getattr:" + name)
        raise AttributeError(name)

    @property
    def __module__(cls) -> str:  # type: ignore[override]
        return "log_foundry"


class _Hostile(HTTPSink, metaclass=_Recording):
    """A subclass whose metaclass would answer for it; its own ``_body`` is defined here."""

    def _body(self, items: Any) -> tuple[bytes, str]:
        return super()._body(items)


class _HostileInstance(HTTPSink):
    """A subclass whose instances count every attribute read."""

    reads = 0

    def __getattribute__(self, name: str) -> Any:
        type(self).reads += 1
        return object.__getattribute__(self, name)

    def _body(self, items: Any) -> tuple[bytes, str]:
        return super()._body(items)


def test_the_body_read_runs_no_metaclass_or_instance_hook() -> None:
    """``_Hostile.__module__`` answers ``log_foundry`` through its metaclass, which a
    ``getattr`` would believe; the static read takes the module from the class's own namespace,
    and neither hook runs while it reads."""
    hostile = _Hostile(URL)
    counted = _HostileInstance(URL)
    _Recording.reads.clear()
    _HostileInstance.reads = 0

    assert http_sink._library_body(hostile) is False
    assert http_sink._library_body(counted) is False

    assert _Recording.reads == []
    assert _HostileInstance.reads == 0


# --- gzip is read once ------------------------------------------------------------------------


class _CountingBool:
    """A ``gzip`` value counting every ``__bool__``, which ``_prepare`` used to ask per request."""

    calls = 0

    def __bool__(self) -> bool:
        type(self).calls += 1
        return True


def test_a_non_bool_gzip_is_read_once_and_stored_as_the_bool_it_gave() -> None:
    """At ``c4e51fbd``: its ``__bool__`` ran on every send, and ``_prepare`` and the refusal
    memory could each get a different answer from one that flips."""
    _CountingBool.calls = 0
    opener = FakeOpener()
    sink = HTTPSink(URL, gzip=_CountingBool(), opener=opener)  # type: ignore[arg-type]
    built = _CountingBool.calls

    for i in range(3):
        sink.emit([{"i": i}])

    assert built == 1
    assert _CountingBool.calls == built
    assert sink.gzip is True
    assert all(gzip.decompress(call["body"]).startswith(b"{") for call in opener.calls)


@pytest.mark.parametrize(("given", "stored"), [(1, True), (0, False), ("", False)])
def test_a_working_non_bool_gzip_still_constructs(given: object, stored: bool) -> None:
    """``gzip=1`` works today and is not refused; it is kept as the ``bool`` it reads as."""
    opener = FakeOpener()
    sink = HTTPSink(URL, gzip=given, opener=opener)  # type: ignore[arg-type]

    sink.emit([{"i": 1}])

    assert sink.gzip is stored
    assert (opener.calls[-1]["body"] != PLAIN) is stored


class _RaisingBool:
    """A ``gzip`` value whose ``__bool__`` raises."""

    def __bool__(self) -> bool:
        raise RuntimeError("from __bool__")


def test_a_gzip_whose_bool_raises_raises_at_construction() -> None:
    """At ``c4e51fbd`` it constructed and raised out of every send; read once, it raises here."""
    with pytest.raises(RuntimeError, match="from __bool__"):
        HTTPSink(URL, gzip=_RaisingBool())  # type: ignore[arg-type]
