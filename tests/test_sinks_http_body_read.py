"""After a status line the status decides: a body read that fails is never a retry.

``HTTPSink._attempt`` read the body with a bare ``response.read()`` once the status was known, and
``_send`` caught only ``(URLError, OSError)``. Measured against a real socket, with
``max_batch_count=2`` and four events: a ``2xx`` whose ``Content-Length`` body arrived short raised
``IncompleteRead`` out of ``emit``, so chunk 2 was never sent, or — after a delivered chunk — the
worker re-sent chunk 1; a stall or a reset mid-body was retried as a connection error, re-POSTing
each chunk three times at ``max_retries=2``; and a garbage status line raised ``BadStatusLine``
raw. Now a failed read is ``b""`` and the status decides — a ``2xx`` settles, a ``503`` retries,
a ``413`` splits, a ``400`` is abandoned once — and a pre-status protocol error is retried and
counted like a connection error.

The first half binds real loopback sockets, because what fails is ``http.client``'s framing and a
double that agrees with the code cannot show it. The second half sweeps every shipped subclass
with an injected opener, since that is where the per-class reading of the body lives.
"""

from __future__ import annotations

import http.client
import socket
import socketserver
import struct
import threading
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.elasticsearch import ElasticsearchSink, OpenSearchSink
from log_foundry.sinks.http import HTTPSink, _no_redirect_opener
from log_foundry.sinks.logstash import LogstashSink
from log_foundry.sinks.otlp import OTLPSink
from test_sinks_http import FakeOpener, FakeResponse
from test_sinks_http_refused import _through_the_worker
from test_sinks_http_render import _RECIPES, _shipped_http_subclasses

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

_STAMP = "2026-08-10T00:00:00.000Z"
_TIMEOUT = 5.0
"""Seconds per request everywhere but the stall test, generous so a loaded runner never times out."""


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Neutralize backoff waits, as ``test_sinks_http`` does for its own module."""
    monkeypatch.setattr("log_foundry.sinks.http.wait", lambda _delay, _stop=None: None)


def _event(i: int) -> dict[str, object]:
    """An event every platform subclass can render: it carries a message and a timestamp."""
    return {"message": "m", "timestamp": _STAMP, "i": i}


def _batch(n: int) -> list[dict[str, object]]:
    return [_event(i) for i in range(n)]


# --- the raw server ---------------------------------------------------------------------------


def _response(status: int, declared: int, body: bytes) -> bytes:
    """A raw response whose ``Content-Length`` says ``declared`` and whose body is ``body``."""
    return (
        f"HTTP/1.1 {status} X\r\nContent-Length: {declared}\r\nConnection: close\r\n\r\n".encode()
        + body
    )


class _RawServer(socketserver.ThreadingTCPServer):
    """A loopback server that answers each request with raw bytes chosen by ``plan``.

    ``plan[i]`` scripts the i-th request and the last entry repeats. The modes:

    - ``ok``: a ``200`` with a complete ``{}`` body.
    - ``short:<status>``: that status, ``Content-Length: 100``, two bytes, then a clean close.
    - ``stall``: a ``200`` with two of 100 bytes, then nothing until the server is torn down.
    - ``reset``: a ``200`` with two of 100 bytes, then — once the client has the status line —
      an RST.
    - ``badstatus``: a line that is not a status line.
    - ``manyheaders``: a ``200`` status line followed by 101 headers, one past ``http.client``'s
      limit.
    - ``longheader``: a ``200`` status line followed by one header line over 65,536 bytes.
    """

    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, plan: list[str]) -> None:
        super().__init__(("127.0.0.1", 0), _RawHandler)
        self.plan = plan
        self.posts: list[int] = []
        self.errors: list[str] = []
        self.status_seen = threading.Semaphore(0)
        self.teardown = threading.Event()
        self._lock = threading.Lock()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server_address[1]}"

    def take(self, body: bytes) -> str:
        """Records one request's body line count and returns the mode scripted for it."""
        with self._lock:
            index = len(self.posts)
            self.posts.append(body.count(b"\n"))
        return self.plan[min(index, len(self.plan) - 1)]


class _RawHandler(socketserver.BaseRequestHandler):
    """Reads one whole request, then writes whatever its server's plan says."""

    server: _RawServer

    def handle(self) -> None:
        sock: socket.socket = self.request
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = sock.recv(65536)
            if not chunk:
                return
            data += chunk
        head, body = data.split(b"\r\n\r\n", 1)
        length = next(
            int(line.split(b":", 1)[1])
            for line in head.split(b"\r\n")
            if line.lower().startswith(b"content-length:")
        )
        while len(body) < length:
            chunk = sock.recv(65536)
            if not chunk:
                return
            body += chunk
        mode = self.server.take(body)
        if mode == "ok":
            sock.sendall(_response(200, 2, b"{}"))
        elif mode.startswith("short:"):
            sock.sendall(_response(int(mode.split(":")[1]), 100, b"{}"))
        elif mode == "stall":
            sock.sendall(_response(200, 100, b"{}"))
            self.server.teardown.wait(30)
        elif mode == "reset":
            sock.sendall(_response(200, 100, b"{}"))
            if not self.server.status_seen.acquire(timeout=5):
                self.server.errors.append("the client never reported a status before the RST")
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            sock.close()
        elif mode == "badstatus":
            sock.sendall(b"garbage\r\n\r\n")
        elif mode == "manyheaders":
            headers = b"".join(b"X-%d: v\r\n" % i for i in range(101))
            sock.sendall(b"HTTP/1.1 200 OK\r\n" + headers + b"\r\n")
        elif mode == "longheader":
            try:
                sock.sendall(b"HTTP/1.1 200 OK\r\nX-Long: " + b"a" * 70_000 + b"\r\n\r\n")
            except OSError:
                pass
        else:
            self.server.errors.append(f"unknown mode {mode!r}")


@contextmanager
def _serving(plan: list[str]) -> Iterator[_RawServer]:
    """Serves ``plan`` on an ephemeral loopback port for the duration of the block."""
    server = _RawServer(plan)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.teardown.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _counting_opener(server: _RawServer) -> tuple[Callable[..., Any], list[int]]:
    """The real opener, releasing ``status_seen`` each time it returns a parsed response."""
    real = _no_redirect_opener()
    returned: list[int] = []

    def opener(request: Any, timeout: float | None = None) -> Any:
        response = real(request, timeout=timeout)
        returned.append(1)
        server.status_seen.release()
        return response

    return opener, returned


def _lost_lines(err: str) -> list[str]:
    return [line for line in err.splitlines() if "lost" in line]


# --- a 2xx settles, however its body ends -----------------------------------------------------


def test_a_short_2xx_on_the_first_chunk_settles_it(capsys: pytest.CaptureFixture[str]) -> None:
    """Before: ``IncompleteRead`` out of ``emit`` with posts ``[2]``, so chunk 2 was never sent."""
    with _serving(["short:200"]) as server:
        sink = HTTPSink(server.url, max_batch_count=2, max_retries=0, timeout=_TIMEOUT)
        sink.emit(_batch(4))

    assert server.posts == [2, 2], "each chunk POSTed exactly once"
    assert sink.losses() == SinkLosses(dropped=0, failed=0)
    assert _lost_lines(capsys.readouterr().err) == []


def test_a_good_chunk_then_a_short_one_is_not_resent() -> None:
    """Before: the raise after chunk 2 made the worker's retry re-send the delivered chunk 1."""
    with _serving(["ok", "short:200"]) as server:
        sink = HTTPSink(server.url, max_batch_count=2, max_retries=0, timeout=_TIMEOUT)
        sink.emit(_batch(4))

    assert server.posts == [2, 2]
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


def test_a_stalled_2xx_body_is_not_retried(capsys: pytest.CaptureFixture[str]) -> None:
    """Before: six POSTs at ``max_retries=2``, ``failed=2`` and a ``SinkDeliveryError``."""
    with _serving(["stall"]) as server:
        sink = HTTPSink(server.url, max_batch_count=2, max_retries=2, timeout=0.5)
        sink.emit(_batch(4))

    assert server.posts == [2, 2], "the read timeout came after a 2xx, so nothing is re-sent"
    assert sink.failed == 0
    assert "connection error" not in capsys.readouterr().err


def test_a_reset_during_a_2xx_body_is_not_retried(capsys: pytest.CaptureFixture[str]) -> None:
    """The RST waits until the opener has returned, so it always lands after the status line.

    Without that handshake the RST could race the status line, which would make this a pre-status
    failure and a legitimate retry — a test that sometimes measured the wrong window.
    """
    with _serving(["reset"]) as server:
        opener, returned = _counting_opener(server)
        sink = HTTPSink(
            server.url, max_batch_count=2, max_retries=2, timeout=_TIMEOUT, opener=opener
        )
        sink.emit(_batch(4))

    assert server.errors == [], server.errors
    assert len(returned) == 2, "the premise: the status line reached the client for each chunk"
    assert server.posts == [2, 2]
    assert sink.failed == 0
    assert "connection error" not in capsys.readouterr().err


@pytest.mark.parametrize("cls", [ElasticsearchSink, OpenSearchSink], ids=lambda c: c.__name__)
def test_a_short_2xx_to_bulk_is_counted_unadjudicated(
    cls: type[ElasticsearchSink], capsys: pytest.CaptureFixture[str]
) -> None:
    """A ``_bulk`` verdict lives in the body, so an unreadable one is counted, never retried."""
    with _serving(["short:200"]) as server:
        sink = cls(server.url, index="i", max_batch_count=2, max_retries=2, timeout=_TIMEOUT)
        sink.emit(_batch(4))

    assert len(server.posts) == 2, "one POST per chunk"
    assert sink.dropped_unadjudicated == 4
    assert sink.losses() == SinkLosses(dropped=0, failed=4)
    line = (
        f"log-foundry: lost 2 event(s); {cls.__name__} could not adjudicate a _bulk response "
        f"(2 event(s) sent, 0 readable item(s), 0 error(s)); they may have landed; not retried"
    )
    assert _lost_lines(capsys.readouterr().err) == [line, line]


# --- any other status still decides as it always did ------------------------------------------


def test_a_503_whose_body_fails_is_still_retried_by_status() -> None:
    """Before: ``IncompleteRead`` off the ``HTTPError``'s body escaped ``emit`` after one POST.

    A connection-error retry would also POST twice here; the exhausted case below is the one that
    tells the status's retry from that one, by the ``HTTP 503`` it reports.
    """
    with _serving(["short:503", "ok"]) as server:
        sink = HTTPSink(server.url, max_batch_count=2, max_retries=1, timeout=_TIMEOUT)
        sink.emit(_batch(2))

    assert server.posts == [2, 2], "the 503 was retried and the retry delivered"
    assert sink.failed == 0


def test_an_exhausted_503_whose_body_fails_is_reported_as_http_503(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with _serving(["short:503"]) as server:
        sink = HTTPSink(server.url, max_batch_count=2, max_retries=1, timeout=_TIMEOUT)
        with pytest.raises(SinkDeliveryError):
            sink.emit(_batch(2))

    assert server.posts == [2, 2]
    assert sink.failed == 1
    assert "HTTPSink, 2 attempt(s), HTTP 503" in capsys.readouterr().err


def test_a_400_whose_body_fails_is_abandoned_once(capsys: pytest.CaptureFixture[str]) -> None:
    """A ``400`` is not retryable whatever its body did; a short body used to escape ``emit``."""
    with _serving(["short:400"]) as server:
        sink = HTTPSink(server.url, max_batch_count=2, max_retries=2, timeout=_TIMEOUT)
        with pytest.raises(SinkDeliveryError):
            sink.emit(_batch(2))

    assert server.posts == [2], "abandoned on the first attempt"
    assert sink.failed == 1
    assert "HTTPSink, 1 attempt(s), HTTP 400" in capsys.readouterr().err


def test_a_413_whose_body_fails_still_splits() -> None:
    """The ``413`` reaches ``emit``'s budget search, which re-sends the events one per request."""
    with _serving(["short:413", "ok"]) as server:
        sink = HTTPSink(server.url, max_retries=2, timeout=_TIMEOUT)
        sink.emit(_batch(2))

    assert server.posts == [2, 1, 1]
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


# --- before any status line: a connection error, retried and counted --------------------------


def test_a_garbage_status_line_is_retried_then_counted(capsys: pytest.CaptureFixture[str]) -> None:
    """Before: ``BadStatusLine`` escaped ``emit`` raw. Now a total failure raises as designed."""
    with _serving(["badstatus"]) as server:
        sink = HTTPSink(server.url, max_batch_count=2, max_retries=2, timeout=_TIMEOUT)
        with pytest.raises(SinkDeliveryError):
            sink.emit(_batch(2))

    assert server.posts == [2, 2, 2]
    assert sink.failed == 1
    assert "HTTPSink, 3 attempt(s), connection error, BadStatusLine" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("mode", "kind"),
    [("manyheaders", "HTTPException"), ("longheader", "LineTooLong")],
)
def test_a_header_block_that_will_not_parse_is_retried_then_counted(
    mode: str, kind: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """The edge of the rule: the status line arrived, but ``urllib`` never hands it back.

    ``http.client`` raises while parsing the headers, inside the opener, so the status — here a
    ``2xx`` — is never seen and the failure is retried like a read timeout awaiting the headers.
    ``LineTooLong`` subclasses ``HTTPException``, and the too-many-headers error *is* one, so a
    guard naming ``BadStatusLine`` alone would let both escape ``emit``.
    """
    with _serving([mode]) as server:
        sink = HTTPSink(server.url, max_batch_count=2, max_retries=2, timeout=_TIMEOUT)
        with pytest.raises(SinkDeliveryError):
            sink.emit(_batch(2))

    assert server.posts == [2, 2, 2]
    assert sink.failed == 1
    assert f"HTTPSink, 3 attempt(s), connection error, {kind}" in capsys.readouterr().err


def test_a_garbage_status_beside_a_good_chunk_is_partial() -> None:
    with _serving(["ok", "badstatus"]) as server:
        sink = HTTPSink(server.url, max_batch_count=2, max_retries=0, timeout=_TIMEOUT)
        sink.emit(_batch(4))

    assert server.posts == [2, 2]
    assert sink.losses() == SinkLosses(dropped=0, failed=1)


# --- both delivery paths ----------------------------------------------------------------------


def test_through_a_worker_nothing_is_resent() -> None:
    """Before: posts ``[3, 3, 3, 3, 3]`` and ``failed_batches == 1``."""
    with _serving(["ok", "short:200"]) as server:
        sink = HTTPSink(server.url, max_batch_count=3, max_retries=0, timeout=_TIMEOUT)
        delivered, failed_batches = _through_the_worker(sink, _batch(6))

    assert delivered and failed_batches == 0
    assert server.posts == [3, 3]
    assert sink.losses() == SinkLosses(dropped=0, failed=0), "settled, not abandoned"


def test_on_the_orphan_path_nothing_is_lost() -> None:
    """Outside a span the emit runs on the caller's thread; a short 2xx must not reach it."""
    with _serving(["short:200"]) as server:
        sink = HTTPSink(server.url, timeout=_TIMEOUT)
        log_foundry.configure(service="t", sink=sink)
        log_foundry.info("outside a span")

    assert server.posts == [1]
    assert log_foundry.health().orphan_lost == 0
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


# --- every shipped subclass -------------------------------------------------------------------


class _UnreadableResponse(FakeResponse):
    """A response whose status is known and whose body read raises ``exc``."""

    def __init__(
        self, status: int, exc: BaseException, headers: dict[str, str] | None = None
    ) -> None:
        super().__init__(status, b"", headers)
        self._exc = exc

    def read(self, n: int = -1) -> bytes:
        raise self._exc


_READ_ERRORS = [
    http.client.IncompleteRead(b""),
    TimeoutError("timed out"),
    ValueError("neither an OSError nor an HTTPException"),
]
"""One ``HTTPException``, one ``OSError`` and one that is neither, so narrowing the guard fails."""

_BODY_READERS = {ElasticsearchSink, OpenSearchSink, OTLPSink}
"""The shipped subclasses whose ``_handle_response`` reads the body.

A new body reader must be added here, and must decide what ``b""`` means for it: an override
cannot tell an empty ``2xx`` body from one that could not be read. The decision is recorded in
``_COUNTS_UNREADABLE``: the ``_bulk`` sinks count it, since their only verdict is in the body;
``OTLPSink`` reads it as full success, since an OTLP success needs no ``partialSuccess`` at all
(SPEC-056 FR-004).
"""

_COUNTS_UNREADABLE = {ElasticsearchSink, OpenSearchSink}
"""The body readers for which ``b""`` is an unadjudicated chunk rather than a delivered one."""


def test_the_body_readers_are_exactly_the_listed_set() -> None:
    """Defines-or-inherits: attribute lookup sees ``OpenSearchSink`` through its parent."""
    readers = {
        cls
        for cls in _shipped_http_subclasses()
        if cls._handle_response is not HTTPSink._handle_response
    }
    assert readers == _BODY_READERS


@pytest.mark.parametrize("exc", _READ_ERRORS, ids=lambda e: type(e).__name__)
@pytest.mark.parametrize("cls", _shipped_http_subclasses(), ids=lambda cls: cls.__name__)
def test_every_http_subclass_settles_an_unreadable_2xx(cls: type[HTTPSink], exc: Exception) -> None:
    assert cls.__name__ in _RECIPES, f"{cls.__name__} has no recipe; add one to _RECIPES"
    opener = FakeOpener([_UnreadableResponse(200, exc)])
    sink = _RECIPES[cls.__name__](opener=opener, max_batch_count=2, max_retries=2)
    assert type(sink) is cls

    sink.emit(_batch(4))

    assert len(opener.calls) == 2, "one request per chunk, none re-sent"
    expected = SinkLosses(dropped=0, failed=4 if cls in _COUNTS_UNREADABLE else 0)
    assert sink.losses() == expected


@pytest.mark.parametrize("exc", _READ_ERRORS, ids=lambda e: type(e).__name__)
def test_logstash_http_mode_settles_an_unreadable_2xx(exc: Exception) -> None:
    """``LogstashSink`` holds an ``HTTPSink`` rather than subclassing one, so it is not swept."""
    opener = FakeOpener([_UnreadableResponse(200, exc)])
    sink = LogstashSink(url="http://ls", opener=opener, max_batch_count=2, max_retries=2)

    sink.emit(_batch(4))

    assert len(opener.calls) == 2
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


def test_a_base_exception_from_a_body_read_is_never_caught() -> None:
    """The guard catches ``Exception``: an interrupt mid-read must reach the caller unchanged."""
    opener = FakeOpener([_UnreadableResponse(200, KeyboardInterrupt())])
    sink = HTTPSink("http://h", opener=opener, max_retries=2)

    with pytest.raises(KeyboardInterrupt):
        sink.emit(_batch(1))

    assert len(opener.calls) == 1
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


def test_retry_after_survives_a_failed_body_read(monkeypatch: pytest.MonkeyPatch) -> None:
    """The headers are read off the response even when its body is not."""
    delays: list[float] = []
    monkeypatch.setattr(
        "log_foundry.sinks.http.wait", lambda delay, _stop=None: delays.append(delay)
    )
    opener = FakeOpener(
        [
            _UnreadableResponse(429, http.client.IncompleteRead(b""), {"Retry-After": "2"}),
            FakeResponse(200, b""),
        ]
    )
    sink = HTTPSink("http://h", opener=opener, max_retries=1)

    sink.emit(_batch(1))

    assert delays == [2.0]
    assert len(opener.calls) == 2
