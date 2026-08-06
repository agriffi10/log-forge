"""SPEC-009 — HTTPSink core: body format, headers/auth, gzip, bounded retry (fake opener).

Every test injects a fake ``urlopen``-shaped opener, so nothing touches the network. Retry tests
patch ``_retry``'s ``time.sleep`` to a no-op so bounded backoff does not actually sleep.
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
