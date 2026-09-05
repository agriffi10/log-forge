"""SPEC-009 — HTTPSink core: body format, headers/auth, gzip, bounded retry, and re-chunking.

Almost every test injects a fake ``urlopen``-shaped opener, so nothing touches the network, and
retry tests patch ``_retry``'s ``time.sleep`` to a no-op so bounded backoff does not actually
sleep. The one exception is SPEC-038 FR-001's AC-2 reproduction, which binds a real
``http.server`` on loopback because the defect it pins is about what goes on the wire, and a
double that agrees with the code cannot show that.
"""

from __future__ import annotations

import gzip
import http.server
import json
import threading
import urllib.error
from typing import ClassVar

import pytest

from log_foundry.sinks.base import Sink, SinkDeliveryError, SinkLosses
from log_foundry.sinks.http import MAX_BUDGET_REDUCTIONS, HTTPSink


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
    assert len(opener.calls) == 3, (
        "the whole chunk once, then each half once. A 413 must NOT join the retryable set: "
        "re-sending identical bytes can only earn the same answer, and the wait before each "
        f"re-send holds the one drain thread. Got {len(opener.calls)} requests"
    )
    delivered = [line for body in opener.calls[1:] for line in body.decode().splitlines()]
    assert sorted(delivered) == [f'{{"a": {i}}}' for i in range(4)], (
        "every event reaches the wire exactly once, in smaller requests"
    )
    assert sink.failed == 0, "a split that delivers is not a failure"


def test_a_413_is_never_retried_even_when_retries_are_available() -> None:
    """AC-4's central clause, asserted on the request count with retries switched on.

    Without this the clause is unfalsifiable: adding 413 to the retryable set beside 429 left
    the entire suite green, because the other 413 tests all run at `max_retries=0` and so cannot
    distinguish "not retried" from "no retries configured". This is the SyslogSink pathology
    FR-007 objects to — futile re-sends with backoff on the single drain thread — reached from
    the other direction.
    """
    opener = FakeOpener([FakeResponse(413, b"")])
    sink = HTTPSink("http://x", max_retries=5, opener=opener)
    sink.emit([{"a": 1}])
    assert len(opener.calls) == 1, (
        f"one event, one 413, one request — got {len(opener.calls)}, so 413 is being retried"
    )
    assert sink.dropped_oversized == 1


def test_a_413_for_one_event_is_dropped_rather_than_asked_about_twice(capsys) -> None:
    """AC-4. The search terminates at one event, which is then permanently too large.

    It does **not** raise. A permanent drop is *settled*: no retry can improve on it, so sending
    the worker round again only re-runs the whole cascade and re-counts the same events. Before
    that distinction, a wholly-413 batch reported `losses().dropped` at four times the number of
    events actually lost — once per worker attempt — against a counter whose contract, unlike
    `failed`'s, is an exact count rather than an upper bound.
    """
    opener = FakeOpener([FakeResponse(413, b"")])
    sink = HTTPSink("http://x", max_retries=0, opener=opener)
    sink.emit([{"a": 1}, {"a": 2}])
    assert sink.dropped_oversized == 2, "each event counted once, not once per worker attempt"
    assert len(opener.calls) == 2, (
        "the pair, then one event alone. The second is dropped without a request: its body is no "
        "smaller than one the destination has already refused in this emit, so asking again "
        "could only get the same answer"
    )
    assert "refused at the smallest request" in capsys.readouterr().err


def test_a_permanently_dropped_batch_does_not_send_the_worker_round_again() -> None:
    """The counter consequence of AC-4, asserted where it is observable: no raise, one count."""
    opener = FakeOpener([FakeResponse(413, b"")])
    sink = HTTPSink("http://x", max_retries=0, opener=opener)
    for _ in range(4):  # what the worker's retry would have done
        sink.emit([{"a": 1}, {"a": 2}])
    assert sink.dropped_oversized == 8, "four deliberate emits, two events each"
    assert sink.losses().failed == 0, "a permanent drop is not an abandoned request"


def test_an_endpoint_that_refuses_everything_costs_one_pass_not_one_request_per_event() -> None:
    """AC-4. The search is bounded, and a size already refused is not asked about again.

    Recursive chunk-halving was 2N-1 requests — 11,954 measured for one 5,980-event backlog.
    Halving the *budget* converges in log2(ratio), and remembering the smallest refused body
    within the emit stops the tail becoming one request per event once the budget is below a
    single item.

    The bound asserted here holds for **uniformly-sized** events, which is what this test builds.
    It is not the general worst case: with sizes in strictly decreasing order every lone item is
    smaller than anything yet refused, so each asks a genuinely new question and the cost is O(N)
    — measured at 507 requests for 500 such events, against 9 uniform and 11 increasing. That is
    a property of the memory being a single low-water mark, not a defect, and a random backlog
    gives the ~ln N expectation.
    """
    opener = FakeOpener([FakeResponse(413, b"")])
    sink = HTTPSink("http://x", max_retries=0, opener=opener)
    sink.emit([{"a": i} for i in range(1000)])
    assert len(opener.calls) <= MAX_BUDGET_REDUCTIONS + 2, (
        f"the 413 search was not bounded: {len(opener.calls)} requests for 1,000 events"
    )
    assert sink.dropped_oversized == 1000, "every event accounted for exactly once"
    assert sink.losses().failed == 0, "a 413 is a size verdict, not an abandoned request"


def test_a_destination_smaller_than_the_budget_still_delivers_everything() -> None:
    """The common misconfiguration: a 5 MB default budget against a much smaller endpoint.

    A depth-capped chunk-halving delivered 2 events of 2,000 here, because a 250x ratio needs
    ~8 halvings and the cap allowed 4. Budget reduction has no such cliff.
    """
    limit = 2_000
    opener = _ResponseScript(
        lambda _n, body: FakeResponse(413, b"") if len(body) > limit else FakeResponse(200, b"")
    )
    sink = HTTPSink("http://x", max_batch_bytes=1_000_000, max_retries=0, opener=opener)
    sink.emit([{"a": i, "pad": "y" * 60} for i in range(500)])
    accepted = [body for body in opener.calls if len(body) <= limit]
    delivered = [line for body in accepted for line in body.decode().splitlines()]
    assert len(delivered) == 500, f"only {len(delivered)} of 500 events reached the wire"
    assert sink.dropped_oversized == 0 and sink.losses().failed == 0
    assert len(opener.calls) - len(accepted) <= MAX_BUDGET_REDUCTIONS, "the search stayed bounded"


def _stopping_sink(opener, **kwargs) -> HTTPSink:
    """A sink whose worker stop event is already set, as it is throughout the exit drain."""
    sink = HTTPSink("http://x", opener=opener, **kwargs)
    signal = threading.Event()
    signal.set()
    sink.log_foundry_stop_signal = signal
    return sink


def test_the_413_split_still_runs_during_a_shutdown() -> None:
    """`Worker.shutdown` sets the stop event *before* joining, so `_final_drain` emits with it set.

    A revision ended the split while stopping, reasoning that the halving runs on the thread
    `shutdown()` is joining. Measured, that disabled splitting for the whole exit drain: a
    2,000-event backlog that had been delivering in 30 requests delivered **nothing**, against
    `losses().dropped == 2000`. The fan-out is bounded by MAX_SPLIT_DEPTH and the total by
    `shutdown(timeout=)`, so the guard bought no bound and cost every deliverable event on the
    one path FR-001 exists to protect.
    """
    opener = _ResponseScript(
        lambda _n, body: FakeResponse(413, b"") if body.count(b"\n") > 2 else FakeResponse(200, b"")
    )
    sink = _stopping_sink(opener)
    sink.emit([{"a": i} for i in range(4)])
    delivered = [line for body in opener.calls[1:] for line in body.decode().splitlines()]
    assert sorted(delivered) == [f'{{"a": {i}}}' for i in range(4)], (
        "the split must still run while stopping, or the exit drain delivers nothing"
    )
    assert sink.dropped_oversized == 0


def test_a_transient_failure_is_still_retried_during_a_shutdown() -> None:
    """Likewise for `_send`'s retries: the exit drain is where a retry matters most.

    Suppressing them while stopping lost one event of six to a single transient 503 — invisible
    at the batch level, because partial failure correctly does not raise, so the removed retry
    was the only one that chunk would ever get. SPEC-027 already makes every backoff *wait*
    return instantly on a set stop event, so suppression bought request count, not time, and
    `shutdown(timeout=)` already bounds that.
    """
    opener = FakeOpener([FakeResponse(503, b""), FakeResponse(200, b"")])
    sink = _stopping_sink(opener, max_retries=3)
    sink.emit([{"a": 1}])
    assert len(opener.calls) == 2, "the transient 503 must still be retried while stopping"
    assert sink.failed == 0


def test_a_413_is_not_split_for_a_caller_that_cannot_split(capsys) -> None:
    """`_send`'s default keeps SentrySink's counted, announced abandonment exactly as it was."""
    opener = FakeOpener([FakeResponse(413, b"")])
    sink = HTTPSink("http://x", max_retries=0, opener=opener)
    with pytest.raises(SinkDeliveryError):
        sink._send(b"body", content_type="application/json")
    assert sink.failed == 1, "an unsplittable 413 is counted where it always was"
    assert "HTTP 413" in capsys.readouterr().err


def test_max_batch_count_is_floored_so_a_zero_cannot_discard_the_batch() -> None:
    """~~`max_batch_count=0, max_batch_bytes=-5` both floor to 1.~~

    **Superseded in part by SPEC-049** (system-frame diff review): the count half stands — a count
    of one still delivers one event per request, so it is on FR-001's floor side — but the bytes
    half is struck, because a one-byte body ceiling made every event oversized and delivered
    nothing. A floor that lands on a value that delivers nothing is a refusal in a floor's clothes,
    so `max_batch_bytes<=0` is refused now (`test_a_non_positive_byte_ceiling_is_refused_not_floored`).
    """
    sink = HTTPSink("http://x", max_batch_count=0)
    assert sink.max_batch_count == 1


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


def test_a_batch_of_wholly_oversized_events_reports_success_and_makes_no_request() -> None:
    """The oversize drop is not a delivery failure, so it must not send the worker round again.

    Consistent with `SQSSink`/`KinesisSink`: an event no request can carry is counted through
    `losses().dropped` and reported there, not raised. Stated here because `emit` returns before
    the chunk loop in this case, so no `chunks`/`settled` bookkeeping runs at all.
    """
    opener = FakeOpener()
    sink = HTTPSink("http://x", max_batch_bytes=50, opener=opener)
    sink.emit([{"pad": "x" * 200} for _ in range(3)])
    assert opener.calls == [], "nothing could be sent"
    assert sink.dropped_oversized == 3
    assert sink.losses() == SinkLosses(dropped=3, failed=0)


def test_the_per_item_separator_is_charged_so_an_ndjson_body_stays_within_budget() -> None:
    """`_Item.size` charges one byte per item for the separator; without it a body overruns.

    The overrun is bounded by `max_batch_count` bytes, which is why it survived every other
    assertion here — every one of them had slack larger than the item count.
    """
    opener = FakeOpener()
    events = [{"a": i} for i in range(20)]
    exact = sum(len(json.dumps(event).encode()) + 1 for event in events)
    sink = HTTPSink("http://x", max_batch_bytes=exact, opener=opener)
    sink.emit(events)
    assert len(opener.calls) == 1, "the budget is exactly one body's worth"
    assert len(opener.calls[0]["body"]) == exact, (
        "an NDJSON body is exactly the sum of the item sizes: each event plus its newline"
    )


def test_a_json_array_body_never_exceeds_the_budget_at_an_exact_fit() -> None:
    """The array's brackets are body framing no item is charged for, so they are reserved."""
    opener = FakeOpener()
    events = [{"a": i} for i in range(20)]
    exact = sum(len(json.dumps(event).encode()) + 1 for event in events)
    sink = HTTPSink("http://x", body_format="json_array", max_batch_bytes=exact, opener=opener)
    sink.emit(events)
    assert all(len(call["body"]) <= exact for call in opener.calls), (
        f"a json_array body ran past the budget: {[len(c['body']) for c in opener.calls]}"
    )


def test_a_compressible_event_is_still_offered_after_an_incompressible_one_was_refused() -> None:
    """The refusal memory is uncompressed; under gzip the destination judges the wire bytes.

    Compression ratio is per-event, so "larger uncompressed" stops implying "also refused". A
    6,021-byte event that gzips to 64 was being discarded with no request made, because a
    441-byte incompressible event had set the mark — against a limit of 200 on the wire. That is
    loss the library invented, and it inflated `dropped`, whose contract is an exact count.
    """
    import hashlib

    # Deterministic high-entropy text: gzip cannot shrink digest output meaningfully.
    noise = "".join(hashlib.sha256(str(i).encode()).hexdigest() for i in range(8))
    incompressible = {"i": 0, "pad": noise}
    compressible = {"i": 1, "pad": "a" * 6000}

    limit = 200
    sizes: list[int] = []

    def opener(request, timeout=None):
        sizes.append(len(request.data))
        return FakeResponse(413 if len(request.data) > limit else 200, b"")

    sink = HTTPSink("http://x", gzip=True, max_retries=0, opener=opener)
    sink.emit([incompressible, compressible])
    assert sink.dropped_oversized == 1, (
        f"only the incompressible event is undeliverable; dropped {sink.dropped_oversized}"
    )
    assert sizes[-1] <= limit, "the compressible event was offered alone, and fitted"


def test_a_lone_item_exactly_at_the_budget_is_dropped_rather_than_sent_over_it() -> None:
    """`_event_ceiling` subtracts the body reserve, which is the one case a body could overrun.

    `_take` yields a lone item whatever the budget, so an item admitted at exactly
    `max_batch_bytes` produced a body that much plus the framing no item is charged for — one
    byte for a JSON array, fourteen for Loki. Reverting this passed the entire suite.
    """
    opener = FakeOpener()
    event = {"a": "x" * 40}
    size = len(json.dumps(event).encode()) + 1
    sink = HTTPSink("http://x", body_format="json_array", max_batch_bytes=size, opener=opener)
    sink.emit([event])
    assert opener.calls == [], "an item that cannot fit once framed is dropped, not overrun"
    assert sink.dropped_oversized == 1

    roomy = HTTPSink(
        "http://x", body_format="json_array", max_batch_bytes=size + 1, opener=(ok := FakeOpener())
    )
    roomy.emit([event])
    assert len(ok.calls) == 1, "one more byte of budget and it ships"
    assert len(ok.calls[0]["body"]) <= size + 1


def test_a_multi_item_chunk_at_the_reduction_bound_is_reported_not_discarded() -> None:
    """The backstop path: those events were never individually refused, so they are not drops.

    Reaching it needs a `_item_size` that does not shrink with the budget, which no shipped sink
    has — but a backstop nothing exercises is a backstop that rots. Discarding here would count
    events as permanently undeliverable on the strength of a chunk-level refusal, inventing loss
    and inflating `dropped`, whose contract is an exact count.
    """

    class BadlySized(HTTPSink):
        def _item_size(self, rendered: str, event: dict[str, object]) -> int:
            """Reports every item as free, so no reduction ever shrinks a chunk.

            Args:
              rendered: Unused.
              event: Unused.

            Returns:
              Zero.

            Raises:
              None.
            """
            return 0

    opener = FakeOpener([FakeResponse(413, b"")])
    sink = BadlySized("http://x", max_retries=0, max_batch_count=8, opener=opener)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": i} for i in range(20)])
    assert sink.dropped_oversized == 0, (
        "events refused only as part of a chunk are not permanent per-event drops"
    )
    assert sink.losses().failed > 0, (
        "but they are counted and announced: leaving them silent hid 8 of 10 events whenever a "
        "sibling chunk delivered and emit therefore returned normally"
    )
    assert len(opener.calls) <= MAX_BUDGET_REDUCTIONS + 8, "and the search still terminated"


def test_the_diagnostic_reports_the_attempts_actually_made(capsys) -> None:
    """A non-retryable status abandons on the first request, and must say so.

    Deriving the count from `max_retries` reported "4 attempt(s)" for an HTTP 400 that was sent
    once. Nothing in the suite asserted an attempt count for this sink, so reverting the fix
    passed all 1,346 tests.
    """
    opener = FakeOpener([FakeResponse(400, b"")])
    sink = HTTPSink("http://x", max_retries=3, opener=opener)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}])
    assert len(opener.calls) == 1, "400 is not retryable"
    assert "1 attempt(s)" in capsys.readouterr().err

    retried = FakeOpener([FakeResponse(500, b"")])
    exhausted = HTTPSink("http://x", max_retries=2, opener=retried)
    with pytest.raises(SinkDeliveryError):
        exhausted.emit([{"a": 1}])
    assert len(retried.calls) == 3
    assert "3 attempt(s)" in capsys.readouterr().err, "and an exhausted retry reports all of them"


class _Recorder(http.server.BaseHTTPRequestHandler):
    """Records every request it receives, and answers whatever its class attributes say."""

    seen: ClassVar[list[tuple[str, str, str | None, int]]] = []
    redirect_to: str | None = None

    def _record(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        type(self).seen.append(
            (self.command, self.path, self.headers.get("Authorization"), len(body))
        )

    def do_POST(self) -> None:
        self._record()
        if self.redirect_to is not None:
            self.send_response(type(self).status)
            self.send_header("Location", self.redirect_to)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self) -> None:
        self._record()
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *args) -> None:
        pass


def _serve(handler: type) -> tuple[http.server.ThreadingHTTPServer, threading.Thread, int]:
    """Binds `handler` on an ephemeral loopback port and serves it on a daemon thread."""
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, server.server_address[1]


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
def test_a_redirect_is_abandoned_rather_than_followed(status: int, capsys) -> None:
    """SPEC-048 FR-001, against two real origins because the defect is in `urllib`'s own opener.

    Before this, `urlopen`'s default opener followed 301/302/303 on a POST by rewriting the
    method to GET and dropping the body, while keeping every header -- so an `http://` collector
    behind a load balancer redirecting to `https://` lost every batch and forwarded the bearer
    token to whatever host the redirect named, and the sink read the redirect target's 200 as
    delivery. Measured: `[('POST', '/post', 'Bearer secret-token', 800), ('GET', '/landing',
    'Bearer secret-token', 0)]`, emit returned, losses (0, 0).

    **307 and 308 are regression pins, not evidence.** CPython's `redirect_request` already
    raises `HTTPError` for a POST on those two, so those parameters pass against the unfixed
    sink; only 301/302/303 exercise the fix. Labelled so a green run is not read as proof for
    all five.

    The assertion that binds is the *target's* empty request list: a test asserting only that
    emit raised would pass against a sink that followed the redirect and then met a 4xx.
    """
    target = type("_Target", (_Recorder,), {"seen": [], "redirect_to": None, "status": 200})
    tgt_server, tgt_thread, tgt_port = _serve(target)
    collector = type(
        "_Collector",
        (_Recorder,),
        {"seen": [], "redirect_to": f"http://127.0.0.1:{tgt_port}/landing", "status": status},
    )
    col_server, col_thread, col_port = _serve(collector)
    try:
        sink = HTTPSink(f"http://127.0.0.1:{col_port}/post", auth="secret-token", max_retries=0)
        with pytest.raises(SinkDeliveryError):
            sink.emit([{"i": i, "pad": "x" * 60} for i in range(10)])
    finally:
        for server, thread in ((col_server, col_thread), (tgt_server, tgt_thread)):
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    assert target.seen == [], (
        f"the redirect target received {target.seen}; a batch and a bearer token reached a host "
        f"the caller never configured"
    )
    assert sink.losses() == SinkLosses(dropped=0, failed=1), "the batch is counted, not silent"
    assert f"HTTP {status}" in capsys.readouterr().err, "and named by status"


def test_an_injected_opener_is_used_exactly_as_given() -> None:
    """The no-redirect opener is the default only; an injected one is the caller's object."""
    fake = FakeOpener([FakeResponse(200)])
    assert HTTPSink("http://x", opener=fake)._opener is fake


def test_gzip_does_not_overwrite_a_content_encoding_the_caller_set() -> None:
    """SPEC-048 FR-007. `Content-Encoding` was the one caller header `gzip=True` overrode.

    And when the caller's value wins the body must not be compressed, or the header and the bytes
    disagree and the destination decodes garbage -- so the assertion is on the *body*, parsed,
    not on its length.
    """
    plain = FakeOpener([FakeResponse(200)])
    HTTPSink(
        "http://x", gzip=True, headers={"Content-Encoding": "identity"}, opener=plain
    ).emit([{"a": 1}])
    call = plain.calls[0]
    assert call["headers"]["content-encoding"] == "identity"
    assert json.loads(call["body"].decode().strip()) == {"a": 1}, "and the body is not gzipped"

    zipped = FakeOpener([FakeResponse(200)])
    HTTPSink("http://x", gzip=True, opener=zipped).emit([{"a": 1}])
    call = zipped.calls[0]
    assert call["headers"]["content-encoding"] == "gzip"
    assert json.loads(gzip.decompress(call["body"]).decode().strip()) == {"a": 1}


def test_a_per_request_content_encoding_does_not_suppress_gzip() -> None:
    """`extra_headers` sits *beneath* the caller's own, so it must not switch compression off.

    The test is against `self._headers` rather than the merged map, and this pins that choice.
    Keying on the merged map would let *any* per-request `Content-Encoding` switch the sink's own
    compression off — a subclass or a future caller passing one through `extra_headers` would
    silently stop gzipping, with the header and the intent disagreeing. No shipped caller does
    that today (`SentrySink` is the only one passing `extra_headers` at all, and it passes
    `X-Sentry-Auth`), which is exactly why a test is what keeps it true.
    """
    opener = FakeOpener([FakeResponse(200)])
    sink = HTTPSink("http://x", gzip=True, opener=opener)
    sink._send(b'{"a": 1}\n', content_type="application/x-ndjson",
               extra_headers={"Content-Encoding": "identity"})
    call = opener.calls[0]
    assert call["headers"]["content-encoding"] == "gzip", "the sink's own gzip still wins"
    assert gzip.decompress(call["body"])


# --- SPEC-049 FR-001: the HTTP family refuses what it cannot use ------------------------------


@pytest.mark.parametrize("bad", [-1, 0, float("nan"), float("inf")])
def test_an_unusable_timeout_is_refused_at_construction(bad: float) -> None:
    """It used to reach `urlopen` and raise a raw `ValueError` from inside `emit`, every batch.

    Uncounted and forever: nothing but `health().failed_batches` moved, and the caller was
    standing at `configure()` when the mistake was made and heard nothing about it.
    """
    with pytest.raises(ValueError, match="timeout") as info:
        HTTPSink("http://x", timeout=bad)
    assert repr(bad) in str(info.value), "the message names the value received"



@pytest.mark.parametrize(
    "headers",
    [
        {"X": "a\r\nInjected: 1"},
        {"X\r\nInjected": "1"},
        {"X": "a\nb"},
        {"X": "a\rb"},
        {"X\nY": "1"},
    ],
)
def test_a_crlf_header_is_refused_at_construction(headers: dict) -> None:
    """`http.client` raises on these from inside `emit`; refusing here also stops the injection."""
    with pytest.raises(ValueError, match="CR or LF"):
        HTTPSink("http://x", headers=headers)


def test_a_crlf_bearer_token_is_refused() -> None:
    """The token goes into `Authorization` verbatim, so it is the injection with the most at stake."""
    with pytest.raises(ValueError, match="auth token"):
        HTTPSink("http://x", auth="tok\r\nInjected: 1")


def test_a_basic_auth_pair_containing_crlf_is_accepted() -> None:
    """The asymmetry is deliberate, and pinned so it does not read as an oversight.

    The `(user, password)` form is base64-encoded before it reaches the header, so a CR in it
    cannot inject anything. Refusing it would be inventing a rule the wire does not need.
    """
    sink = HTTPSink("http://x", auth=("us\r\ner", "pa\nss"), opener=FakeOpener([FakeResponse(200)]))
    sink.emit([{"a": 1}])
    header = sink._opener.calls[0]["headers"]["authorization"]
    assert header.startswith("Basic ") and "\r" not in header and "\n" not in header


@pytest.mark.parametrize("url", ["file:///etc/passwd", "ftp://h/x", "gopher://h", "x", ""])
def test_a_non_http_url_is_refused_at_construction(url: str) -> None:
    """`file://` raised a raw `TypeError` per batch; `ftp://` burned the whole retry budget."""
    with pytest.raises(ValueError, match="http or https"):
        HTTPSink(url)


@pytest.mark.parametrize("url", ["http://h/x", "https://h/x", "HTTP://h/x"])
def test_an_http_url_constructs(url: str) -> None:
    assert HTTPSink(url).url == url


def test_every_shipped_http_subclass_inherits_the_refusal() -> None:
    """SPEC-049 FR-001, named rather than floored — the roster test's own rule.

    `test_public_surface.py` already derives this population transitively, because `OpenSearchSink`
    inherits through `ElasticsearchSink` and is invisible to a direct `class X(HTTPSink)` scan. Its
    docstring says why a floor is the wrong instrument: "A floor set below the real number lets
    subclasses leave silently, which is the failure this whole roster exists to prevent." So this
    names the seven and asserts the derivation still finds exactly them.
    """
    from test_public_surface import _http_sink_subclasses

    derived = {f"{module}.{node.name}" for module, node in _http_sink_subclasses()}
    assert derived == {
        "datadog.DatadogSink",
        "elasticsearch.ElasticsearchSink",
        "elasticsearch.OpenSearchSink",
        "honeycomb.HoneycombSink",
        "loki.LokiSink",
        "newrelic.NewRelicSink",
        "splunk.SplunkHECSink",
    }, f"the subclass population moved: {sorted(derived)}"

    from log_foundry.sinks.datadog import DatadogSink
    from log_foundry.sinks.elasticsearch import ElasticsearchSink, OpenSearchSink
    from log_foundry.sinks.honeycomb import HoneycombSink
    from log_foundry.sinks.loki import LokiSink
    from log_foundry.sinks.newrelic import NewRelicSink
    from log_foundry.sinks.splunk import SplunkHECSink

    builders = (
        lambda: DatadogSink("k", timeout=-1),
        lambda: ElasticsearchSink("http://h", index="i", timeout=-1),
        lambda: OpenSearchSink("http://h", index="i", timeout=-1),
        lambda: HoneycombSink("k", dataset="d", timeout=-1),
        lambda: LokiSink("http://h", timeout=-1),
        lambda: NewRelicSink("k", timeout=-1),
        lambda: SplunkHECSink("http://h", token="t", timeout=-1),
    )
    assert len(builders) == len(derived), "every named subclass is built, not a sample of them"
    for build in builders:
        with pytest.raises(ValueError, match="timeout"):
            build()


# --- SPEC-049, system-frame review: the family's other degenerate arguments ---------------------


@pytest.mark.parametrize("url", ["http:///x", "http:", "https://"])
def test_a_host_less_url_is_refused_at_construction(url: str) -> None:
    """It passed the scheme check and failed every request with a counted URLError, forever."""
    with pytest.raises(ValueError, match="url names no host"):
        HTTPSink(url)


def test_an_unknown_body_format_is_refused_rather_than_silently_rendered_as_ndjson() -> None:
    """`LogstashSink(url=…, body_format="xml")` already refused this; the class it wraps did not."""
    with pytest.raises(ValueError, match="HTTPSink body_format must be one of"):
        HTTPSink("http://x", body_format="xml")


@pytest.mark.parametrize("bad", [0, -5])
def test_a_non_positive_byte_ceiling_is_refused_not_floored(bad: int) -> None:
    """The floor at one was not the count floor's twin: a one-byte body delivers nothing.

    Measured before the fix, `max_batch_bytes` of 0, -5 and 1 each dropped every event as
    oversized and made zero requests, while `max_batch_count` floored to 1 still delivered one
    event per request — so only the count is on FR-001's floor side.
    """
    with pytest.raises(ValueError, match="HTTPSink max_batch_bytes must be a positive integer") as info:
        HTTPSink("http://x", max_batch_bytes=bad)
    assert repr(bad) in str(info.value)


def test_a_positive_byte_ceiling_and_none_still_construct() -> None:
    assert HTTPSink("http://x", max_batch_bytes=2048).max_batch_bytes == 2048
    assert HTTPSink("http://x").max_batch_bytes == HTTPSink.MAX_BATCH_BYTES
