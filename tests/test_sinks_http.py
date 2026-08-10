"""SPEC-009 — HTTPSink core: body format, headers/auth, gzip, bounded retry, and re-chunking.

Almost every test injects a fake ``urlopen``-shaped opener, so nothing touches the network, and
retry tests patch ``_retry``'s ``time.sleep`` to a no-op so bounded backoff does not actually
sleep. The one exception is SPEC-038 FR-001's AC-2 reproduction, which binds a real
``http.server`` on loopback because the defect it pins is about what goes on the wire, and a
double that agrees with the code cannot show that.
"""

from __future__ import annotations

import gzip
import json
import urllib.error

import pytest

from log_foundry.sinks.base import Sink, SinkDeliveryError
from log_foundry.sinks.http import HTTPSink


class FakeResponse:
    """A minimal ``urlopen`` response: status + body + headers."""

    def __init__(self, status: int = 200, body: bytes = b"", headers: dict | None = None) -> None:
        self.status = status
        self._body = body
        self.headers = headers or {}

    def read(self) -> bytes:
        return self._body

    def getcode(self) -> int:
        return self.status


class FakeOpener:
    """Record each request; return queued responses (or raise queued exceptions) in order."""

    def __init__(self, responses: list | None = None) -> None:
        self.calls: list[dict] = []
        self._responses = responses

    def __call__(self, request, timeout=None):
        self.calls.append(
            {
                "url": request.full_url,
                "method": request.get_method(),
                "headers": {k.lower(): v for k, v in request.header_items()},
                "body": request.data,
                "timeout": timeout,
            }
        )
        if not self._responses:
            return FakeResponse(200, b"{}")
        item = self._responses[min(len(self.calls) - 1, len(self._responses) - 1)]
        if isinstance(item, Exception):
            raise item
        return item


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Neutralize backoff sleeps so retry tests run instantly."""
    # ``wait`` is bound into each sink at import, and its Event branch never reaches
    # ``time.sleep`` — patching either centrally would leave this fixture inert.
    monkeypatch.setattr("log_foundry.sinks.http.wait", lambda _delay, _stop=None: None)


# --- FR-001: core POST ------------------------------------------------------------------


def test_is_a_sink() -> None:
    assert isinstance(HTTPSink("http://x/logs"), Sink)


def test_ndjson_body_is_one_json_line_per_event() -> None:
    opener = FakeOpener()
    HTTPSink("http://x/logs", opener=opener).emit([{"a": 1}, {"b": 2}])
    call = opener.calls[0]
    assert call["url"] == "http://x/logs"
    assert call["method"] == "POST"
    assert call["body"].decode("utf-8") == '{"a": 1}\n{"b": 2}\n'
    assert call["headers"]["content-type"] == "application/x-ndjson"


def test_json_array_body() -> None:
    opener = FakeOpener()
    HTTPSink("http://x/logs", body_format="json_array", opener=opener).emit([{"a": 1}, {"b": 2}])
    call = opener.calls[0]
    assert json.loads(call["body"]) == [{"a": 1}, {"b": 2}]
    assert call["headers"]["content-type"] == "application/json"


def test_custom_headers_and_bearer_auth_applied() -> None:
    opener = FakeOpener()
    HTTPSink("http://x", headers={"X-Env": "prod"}, auth="tok123", opener=opener).emit([{"a": 1}])
    headers = opener.calls[0]["headers"]
    assert headers["x-env"] == "prod"
    assert headers["authorization"] == "Bearer tok123"


def test_basic_auth_tuple_applied() -> None:
    opener = FakeOpener()
    HTTPSink("http://x", auth=("user", "pw"), opener=opener).emit([{"a": 1}])
    # base64("user:pw") == "dXNlcjpwdw=="
    assert opener.calls[0]["headers"]["authorization"] == "Basic dXNlcjpwdw=="


def test_timeout_is_passed_to_opener() -> None:
    opener = FakeOpener()
    HTTPSink("http://x", timeout=2.5, opener=opener).emit([{"a": 1}])
    assert opener.calls[0]["timeout"] == 2.5


def test_empty_batch_makes_no_request() -> None:
    opener = FakeOpener()
    HTTPSink("http://x", opener=opener).emit([])
    assert opener.calls == []


# --- FR-002: compression + content headers ---------------------------------------------


def test_gzip_compresses_body_and_sets_encoding() -> None:
    opener = FakeOpener()
    HTTPSink("http://x", gzip=True, opener=opener).emit([{"a": 1}])
    call = opener.calls[0]
    assert call["headers"]["content-encoding"] == "gzip"
    assert gzip.decompress(call["body"]).decode("utf-8") == '{"a": 1}\n'


def test_caller_can_override_content_type() -> None:
    opener = FakeOpener()
    HTTPSink("http://x", headers={"Content-Type": "application/custom"}, opener=opener).emit(
        [{"a": 1}]
    )
    assert opener.calls[0]["headers"]["content-type"] == "application/custom"


# --- FR-001 / FR-012: retry + abandon --------------------------------------------------


def test_retries_on_429_then_succeeds() -> None:
    opener = FakeOpener([FakeResponse(429, b"", {"Retry-After": "0"}), FakeResponse(200, b"ok")])
    sink = HTTPSink("http://x", opener=opener)
    sink.emit([{"a": 1}])
    assert len(opener.calls) == 2
    assert sink.failed == 0


def test_persistent_5xx_is_abandoned_and_counted(capsys) -> None:
    opener = FakeOpener([FakeResponse(500, b"err")])
    sink = HTTPSink("http://x", max_retries=2, opener=opener)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}])  # nothing landed, so the worker must see it (SPEC-026 FR-001)
    assert len(opener.calls) == 3  # initial + 2 retries
    assert sink.failed == 1
    err = capsys.readouterr().err
    assert "lost 1 request(s)" in err
    assert "HTTP 500" in err, "the status is the library-controlled detail"


def test_connection_error_is_retried_then_abandoned(capsys) -> None:
    opener = FakeOpener([urllib.error.URLError("boom")])
    sink = HTTPSink("http://x", max_retries=1, opener=opener)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}])
    assert len(opener.calls) == 2  # initial + 1 retry
    assert sink.failed == 1
    err = capsys.readouterr().err
    assert "lost 1 request(s)" in err
    assert "URLError" in err and "boom" not in err, "the type, never the message"


def test_httperror_is_treated_as_a_response() -> None:
    err = urllib.error.HTTPError("http://x", 503, "unavailable", {}, None)  # type: ignore[arg-type]
    opener = FakeOpener([err, FakeResponse(200, b"ok")])
    sink = HTTPSink("http://x", opener=opener)
    sink.emit([{"a": 1}])
    assert len(opener.calls) == 2
    assert sink.failed == 0


# --- SPEC-038 FR-001: re-chunking to the destination's limits ----------------------------


class _ResponseScript:
    """An opener whose answer is chosen per call by a function of the request body."""

    def __init__(self, decide) -> None:
        self.calls: list[bytes] = []
        self._decide = decide

    def __call__(self, request, timeout=None):
        self.calls.append(request.data)
        return self._decide(len(self.calls), request.data)


def test_a_batch_within_the_limits_is_still_one_request() -> None:
    """The chunk loop must not fragment a batch that already fits."""
    opener = FakeOpener()
    HTTPSink("http://x", opener=opener).emit([{"a": i} for i in range(10)])
    assert len(opener.calls) == 1


def test_a_batch_over_the_count_limit_becomes_several_bounded_requests() -> None:
    opener = FakeOpener()
    sink = HTTPSink("http://x", max_batch_count=100, opener=opener)
    sink.emit([{"a": i} for i in range(250)])
    lines = [call["body"].decode().rstrip("\n").split("\n") for call in opener.calls]
    assert [len(chunk) for chunk in lines] == [100, 100, 50]
    assert sum(len(chunk) for chunk in lines) == 250, "every event is sent exactly once"


def test_a_batch_over_the_byte_limit_becomes_several_bounded_requests() -> None:
    opener = FakeOpener()
    sink = HTTPSink("http://x", max_batch_bytes=500, opener=opener)
    sink.emit([{"pad": "x" * 100} for _ in range(20)])
    assert len(opener.calls) > 1
    assert all(len(call["body"]) <= 500 for call in opener.calls), (
        [len(c["body"]) for c in opener.calls]
    )


def test_an_event_too_large_for_the_budget_is_dropped_before_any_request(capsys) -> None:
    """AC-4. A single event no request can carry is permanent, so it is dropped and counted."""
    opener = FakeOpener()
    sink = HTTPSink("http://x", max_batch_bytes=200, opener=opener)
    sink.emit([{"a": 1}, {"pad": "x" * 500}, {"b": 2}])
    sent = b"".join(call["body"] for call in opener.calls)
    assert b'"a": 1' in sent and b'"b": 2' in sent
    assert b"xxxx" not in sent, "the oversized event never reached the wire"
    assert sink.dropped_oversized == 1
    assert sink.losses().dropped == 1
    assert "lost 1 event(s)" in capsys.readouterr().err


def test_a_failing_chunk_does_not_abandon_the_chunks_that_succeeded() -> None:
    """AC-3. Partial delivery is counted, not re-raised: the worker retries whole batches."""
    opener = _ResponseScript(
        lambda n, _body: FakeResponse(500, b"") if n == 2 else FakeResponse(200, b"")
    )
    sink = HTTPSink("http://x", max_batch_count=1, max_retries=0, opener=opener)
    sink.emit([{"a": 1}, {"a": 2}, {"a": 3}])
    assert len(opener.calls) == 3
    assert sink.failed == 1, "only the failing chunk is counted"


def test_every_chunk_failing_is_the_total_failure_the_worker_retries() -> None:
    opener = FakeOpener([FakeResponse(500, b"")])
    sink = HTTPSink("http://x", max_batch_count=1, max_retries=0, opener=opener)
    with pytest.raises(SinkDeliveryError, match="delivered none of 3 chunk"):
        sink.emit([{"a": 1}, {"a": 2}, {"a": 3}])
    assert sink.failed == 3


def test_a_413_is_split_rather_than_retried_and_the_halves_land() -> None:
    """AC-4. 413 is permanent for those bytes, so the answer is a smaller request."""
    opener = _ResponseScript(
        lambda _n, body: FakeResponse(413, b"") if body.count(b"\n") > 2 else FakeResponse(200, b"")
    )
    sink = HTTPSink("http://x", max_retries=3, opener=opener)
    sink.emit([{"a": i} for i in range(4)])
    accepted = [body for body in opener.calls if body.count(b"\n") <= 2]
    delivered = [line for body in accepted for line in body.decode().splitlines()]
    assert sorted(delivered) == [f'{{"a": {i}}}' for i in range(4)], (
        "every event reaches the wire exactly once, in smaller requests"
    )
    assert sink.failed == 0, "a split that delivers is not a failure"


def test_a_413_for_one_event_is_dropped_rather_than_split_forever(capsys) -> None:
    """AC-4. Halving terminates at one event, which is then permanently too large."""
    opener = FakeOpener([FakeResponse(413, b"")])
    sink = HTTPSink("http://x", max_retries=0, opener=opener)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}, {"a": 2}])
    assert sink.dropped_oversized == 2
    assert len(opener.calls) == 3, "the whole chunk, then each half once -- never a re-send loop"
    assert "HTTP 413" in capsys.readouterr().err


def test_a_413_is_not_split_for_a_caller_that_cannot_split(capsys) -> None:
    """`_send`'s default keeps SentrySink's counted, announced abandonment exactly as it was."""
    opener = FakeOpener([FakeResponse(413, b"")])
    sink = HTTPSink("http://x", max_retries=0, opener=opener)
    with pytest.raises(SinkDeliveryError):
        sink._send(b"body", content_type="application/json")
    assert sink.failed == 1, "an unsplittable 413 is counted where it always was"
    assert "HTTP 413" in capsys.readouterr().err


def test_max_batch_bounds_are_floored_so_a_zero_cannot_discard_the_batch() -> None:
    sink = HTTPSink("http://x", max_batch_count=0, max_batch_bytes=-5)
    assert (sink.max_batch_count, sink.max_batch_bytes) == (1, 1)


def test_six_thousand_events_in_one_emit_become_many_bounded_real_requests() -> None:
    """AC-2, the reproduction, against a real `http.server` rather than an injected opener.

    This is the measured defect: `Worker._final_drain` hands the sink its whole exit backlog --
    5,980 events in one `emit` when the queue has backed up -- and before FR-001 that went out as
    a single request the destination rejected whole, taking the backlog with it. Nothing here is
    faked: real sockets, real `urllib`, real request framing, so the chunking is exercised
    end to end rather than against a double that could agree with the code.
    """
    import http.server
    import threading

    received: list[int] = []
    bodies: list[bytes] = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            body = self.rfile.read(int(self.headers["Content-Length"]))
            bodies.append(body)
            received.append(body.count(b"\n"))
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args) -> None:  # keep the test output clean
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        port = server.server_address[1]
        sink = HTTPSink(
            f"http://127.0.0.1:{port}/logs", max_batch_count=1000, max_batch_bytes=100_000
        )
        sink.emit([{"n": i, "msg": "an event of unremarkable size"} for i in range(6000)])
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert len(bodies) > 1, "6,000 events must not go out as one request"
    assert sum(received) == 6000, f"every event delivered exactly once, got {sum(received)}"
    assert max(received) <= 1000, f"a request exceeded the count limit: {max(received)}"
    assert max(len(body) for body in bodies) <= 100_000, "a request exceeded the byte limit"
    assert sink.failed == 0 and sink.dropped_oversized == 0
