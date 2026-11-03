"""SPEC-056 FR-003 and FR-004 — ``OTLPSink``: endpoint, retry set, response bound, budget,
partial success — every body shape driven through ``emit`` against a fake opener."""

from __future__ import annotations

import ast
import gzip
import json
import threading
import time
from pathlib import Path
from typing import Any

import pytest

import log_foundry
from log_foundry import _otel
from log_foundry._otel import request_body
from log_foundry.sinks import _retry
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.otlp import DEFAULT_ENDPOINT, OTLPSink
from log_foundry.sinks.transform import TransformSink
from log_foundry.worker import Worker
from test_otel_envelope import roundtrip
from test_sink_concurrency import PER_THREAD, THREADS, SlowCounter
from test_sinks_http import FakeOpener, FakeResponse
from test_sinks_http_render import _RECIPES, _shipped_http_subclasses

STAMP = "2026-10-05T12:34:56.789Z"
MIB = 1024 * 1024
LIMIT = 4 * MIB


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Neutralize the HTTP backoff so retry tests run instantly."""
    monkeypatch.setattr("log_foundry.sinks.http.wait", lambda _delay, _stop=None: None)


def _event(i: int = 0, **over: object) -> dict[str, object]:
    event: dict[str, object] = {
        "timestamp": STAMP,
        "level": "INFO",
        "message": f"m{i}",
        "trace_id": "0af7651916cd43dd8448eb211c80319c",
        "span_id": "b7ad6b7169203331",
        "parent_span_id": None,
        "log_id": f"log-{i}",
        "function": "work",
        "service": "svc",
        "version": "1",
        "env": "test",
        "fields": {"i": i},
    }
    event.update(over)
    return event


def _sink(*responses: FakeResponse | Exception, **kw: Any) -> tuple[OTLPSink, FakeOpener]:
    opener = FakeOpener(list(responses) if responses else None)
    return OTLPSink(opener=opener, **kw), opener


def _bodies(opener: FakeOpener) -> list[str]:
    return [call["body"].decode("utf-8") for call in opener.calls]


def _messages(document: str) -> list[str]:
    return [
        record["body"]["stringValue"]
        for resource_logs in json.loads(document)["resourceLogs"]
        for scope_logs in resource_logs["scopeLogs"]
        for record in scope_logs["logRecords"]
    ]


def _partial(count: object, **extra: object) -> FakeResponse:
    """A ``200`` partial success; ``bytes`` as the count are spliced in raw, past ``json.dumps``."""
    if isinstance(count, bytes):
        extras = "".join(f", {json.dumps(k)}: {json.dumps(v)}" for k, v in extra.items())
        raw = b'{"partialSuccess": {"rejectedLogRecords": ' + count + extras.encode() + b"}}"
        return FakeResponse(200, raw)
    body = {"partialSuccess": {"rejectedLogRecords": count, **extra}}
    return FakeResponse(200, json.dumps(body).encode("utf-8"))


def _lost_lines(err: str) -> list[str]:
    return [line for line in err.splitlines() if "log record(s)" in line]


# --- FR-003: construction and the wire ----------------------------------------------------


def test_one_post_to_the_default_endpoint_parses_as_otlp() -> None:
    """AC-1 (invariant 7)."""
    sink, opener = _sink()
    sink.emit([_event()])
    assert len(opener.calls) == 1
    call = opener.calls[0]
    assert call["url"] == "http://localhost:4318/v1/logs" == DEFAULT_ENDPOINT
    assert call["method"] == "POST"
    assert call["headers"]["content-type"] == "application/json"
    message = roundtrip(call["body"].decode("utf-8"))
    assert message.resource_logs[0].scope_logs[0].log_records[0].body.string_value == "m0"


def test_the_endpoint_is_used_exactly_as_written() -> None:
    """AC-2."""
    for endpoint in ("https://c.example:4318/otlp/v1/logs", "http://h:4318"):
        opener = FakeOpener()
        OTLPSink(endpoint, opener=opener).emit([_event()])
        assert opener.calls[0]["url"] == endpoint


def test_every_platform_keyword_reaches_the_request_or_the_attribute() -> None:
    """AC-3."""
    opener = FakeOpener()
    sink = OTLPSink(
        opener=opener,
        headers={"x-api-key": "k"},
        auth="tok",
        gzip=True,
        timeout=2.5,
        max_retries=1,
        max_retry_after=7.0,
        max_batch_count=50,
        max_batch_bytes=70_000,
    )
    sink.emit([_event()])
    call = opener.calls[0]
    assert call["headers"]["x-api-key"] == "k"
    assert call["headers"]["authorization"] == "Bearer tok"
    assert call["headers"]["content-encoding"] == "gzip"
    assert call["timeout"] == 2.5
    assert roundtrip(gzip.decompress(call["body"]).decode("utf-8"))
    assert (sink.max_retries, sink.max_retry_after) == (1, 7.0)
    assert (sink.max_batch_count, sink.max_batch_bytes) == (50, 70_000)
    assert sink._opener is opener
    opener = FakeOpener()
    OTLPSink(opener=opener, auth=("u", "p")).emit([_event()])
    assert opener.calls[0]["headers"]["authorization"].startswith("Basic ")


def test_body_format_is_pinned_and_refused_as_a_keyword() -> None:
    """AC-4; the typed half is ``tests/typed_consumer/rejects.py``."""
    with pytest.raises(TypeError, match="body_format"):
        OTLPSink(body_format="ndjson")  # type: ignore[call-arg]


def test_method_must_be_exactly_post() -> None:
    """AC-5 (invariant 13)."""
    for method in ("PUT", "post", "GET"):
        with pytest.raises(ValueError, match="method"):
            OTLPSink(method=method)
    assert OTLPSink(method="POST").method == "POST"


def test_construction_refusals_name_the_argument() -> None:
    """AC-15 (invariant 13)."""
    for endpoint in ("ftp://h/v1/logs", "http:///v1/logs", ""):
        with pytest.raises(ValueError, match="endpoint"):
            OTLPSink(endpoint)
    with pytest.raises(TypeError, match="endpoint"):
        OTLPSink(5)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="timeout"):
        OTLPSink(timeout=-1)
    with pytest.raises(ValueError, match="header"):
        OTLPSink(headers={"x": "a\nb"})


def test_scope_version_is_the_library_version() -> None:
    """Plan review MINOR-4a: the posted ``scope.version`` is ``log_foundry.__version__``."""
    sink, opener = _sink()
    sink.emit([_event()])
    scope = json.loads(_bodies(opener)[0])["resourceLogs"][0]["scopeLogs"][0]["scope"]
    assert scope == {"name": "log_foundry", "version": log_foundry.__version__}


# --- FR-003: retry set, response bound ----------------------------------------------------


@pytest.mark.parametrize("status", [429, 502, 503, 504])
def test_otlps_retryable_statuses_are_retried(status: int) -> None:
    """AC-6, the retried half (invariant 7)."""
    sink, opener = _sink(FakeResponse(status, b""), FakeResponse(200, b""), max_retries=2)
    sink.emit([_event()])
    assert len(opener.calls) == 2
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


@pytest.mark.parametrize("status", [500, 501, 400, 401, 404])
def test_every_other_status_is_abandoned_on_the_first_request(
    status: int, capsys: pytest.CaptureFixture[str]
) -> None:
    """AC-6, the abandoned half: a ``500`` is not retried here (invariant 7)."""
    sink, opener = _sink(FakeResponse(status, b""), FakeResponse(200, b""), max_retries=2)
    with pytest.raises(SinkDeliveryError):
        sink.emit([_event()])
    assert len(opener.calls) == 1
    assert sink.losses().failed == 1
    assert f"OTLPSink, 1 attempt(s), HTTP {status}" in capsys.readouterr().err


def test_a_persistent_503_makes_three_requests_and_waits_on_the_stop_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AC-7 (invariant 3). The backoff goes through ``_retry.wait`` with the sink's signal."""
    seen: list[tuple[float, threading.Event | None]] = []

    def spy(delay: float, stop: threading.Event | None = None) -> None:
        seen.append((delay, stop))
        _retry.wait(delay, stop)

    monkeypatch.setattr("log_foundry.sinks.http.wait", spy)
    sink, opener = _sink(FakeResponse(503, b""), max_retries=2)
    sink.log_foundry_stop_signal = threading.Event()
    sink.log_foundry_stop_signal.set()
    started = time.perf_counter()
    with pytest.raises(SinkDeliveryError):
        sink.emit([_event()])
    assert time.perf_counter() - started < 0.25, "a set signal cuts every backoff short"
    assert len(opener.calls) == 3
    assert [stop for _, stop in seen] == [sink.log_foundry_stop_signal] * 2


def test_a_413_halves_the_budget_and_resends_the_same_records() -> None:
    """AC-8. The inherited exception to OTLP's no-retry rule, asserted as a documented one."""
    events = [_event(i) for i in range(10)]
    responses = [FakeResponse(413, b"")] + [FakeResponse(200, b"")] * 20
    sink, opener = _sink(*responses)
    sink.emit(events)
    assert opener.calls[0]["url"] and len(opener.calls) > 2
    delivered = [m for body in _bodies(opener)[1:] for m in _messages(body)]
    assert sorted(delivered) == sorted(f"m{i}" for i in range(10))
    for body in _bodies(opener)[1:]:
        roundtrip(body)
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


def test_the_siblings_keep_the_family_retry_set_and_the_unbounded_read() -> None:
    """AC-9, regression guard: every other HTTP sink is unchanged by construction."""
    from test_public_surface import _http_sink_subclasses

    class _BareRead:
        status = 200

        def read(self) -> bytes:
            return b"{}"

    big = b" " * (5 * MIB)
    siblings = [cls for cls in _shipped_http_subclasses() if cls is not OTLPSink]
    assert len(siblings) == 7
    for cls in [HTTPSink, *siblings]:
        build = _RECIPES.get(cls.__name__, lambda **kw: HTTPSink("http://h", **kw))
        for status in (500, 501):
            opener = FakeOpener([FakeResponse(status, b""), FakeResponse(200, b"{}")])
            build(opener=opener, max_retries=1).emit([_event()])
            assert len(opener.calls) == 2, f"{cls.__name__} no longer retries {status}"
        whole = FakeResponse(200, big)
        build(opener=FakeOpener([whole])).emit([_event()])
        assert whole.read_sizes == [-1], f"{cls.__name__} bounded its read"
        bare = FakeOpener()
        bare._responses = [_BareRead()]
        build(opener=bare).emit([_event()])
        assert len(bare.calls) == 1
    definers = {
        f"{stem}.{cls.name}"
        for stem, cls in _http_sink_subclasses()
        if any(
            isinstance(node, ast.FunctionDef)
            and node.name in ("_retryable_status", "_response_limit")
            for node in cls.body
        )
    }
    assert definers == {"otlp.OTLPSink"}


def test_an_oversized_non_2xx_is_abandoned_whatever_its_status(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """AC-10 (invariant 7). Exactly the bound is retried; one byte more is not."""
    over = FakeResponse(503, b"x" * (LIMIT + 1))
    sink, opener = _sink(over, FakeResponse(200, b""), max_retries=2)
    with pytest.raises(SinkDeliveryError):
        sink.emit([_event()])
    assert len(opener.calls) == 1 and sink.losses().failed == 1
    assert f"1 attempt(s), HTTP 503, response over {LIMIT} bytes" in capsys.readouterr().err
    assert over.read_sizes == [LIMIT + 1]
    for body in (b"x", b"x" * LIMIT):
        retried = FakeResponse(503, body)
        sink, opener = _sink(retried, FakeResponse(200, b""), max_retries=2)
        sink.emit([_event()])
        assert len(opener.calls) == 2 and sink.losses().failed == 0
        assert retried.read_sizes == [LIMIT + 1]


# --- FR-003: the budget -------------------------------------------------------------------


def _corpus(resources: int) -> list[dict[str, object]]:
    return [
        _event(i, service=f"svc-{i % resources}", fields={"i": i, "pad": "x" * 200})
        for i in range(30)
    ]


class _NoFraming(OTLPSink):
    """The mutant FR-003 AC-11 names: an ``_item_size`` that charges no framing."""

    def _item_size(self, rendered: str, event: dict[str, object]) -> int:
        return len(rendered.encode("utf-8")) + 1


def _budget_for_ten(sink: OTLPSink, events: list[dict[str, object]]) -> int:
    charged = sum(sink._item_size(sink._render(e), e) for e in events[:10])
    return charged + sink._body_reserve()


@pytest.mark.parametrize("resources", [1, 3])
def test_every_body_stays_inside_the_budget_envelope_included(resources: int) -> None:
    """AC-11 (invariant 2). Three resources per request is where an uncharged envelope shows."""
    events = _corpus(resources)
    budget = _budget_for_ten(OTLPSink(), events)
    sink, opener = _sink(max_batch_bytes=budget)
    sink.emit(events)
    assert len(opener.calls) >= 3
    for body in _bodies(opener):
        assert len(body.encode("utf-8")) <= budget
        roundtrip(body)
    assert sorted(m for b in _bodies(opener) for m in _messages(b)) == sorted(
        f"m{i}" for i in range(30)
    )
    mutant = _NoFraming(opener=FakeOpener(), max_batch_bytes=budget)
    mutant.emit(events)
    over = [len(b.encode("utf-8")) > budget for b in _bodies(mutant._opener)]
    if resources == 3:
        assert any(over), "the mutant must be caught by the three-resource half"


def test_body_reserve_is_the_wrapper_and_item_size_the_framing() -> None:
    """A mutation target no spec criterion pins: the wrapper is withheld, the framing charged."""
    sink = OTLPSink()
    assert sink._body_reserve() == len('{"resourceLogs":[]}') == 19
    event = _event()
    rendered = sink._render(event)
    key = _otel.resource_key(event)
    assert sink._item_size(rendered, event) == len(rendered.encode()) + 1 + _otel.framing(
        key, log_foundry.__version__
    )
    body = request_body([(key, rendered)], log_foundry.__version__).encode("utf-8")
    assert len(body) == sink._body_reserve() + sink._item_size(rendered, event) - 1


class _HashRaises(str):
    __slots__ = ()

    def __hash__(self) -> int:
        raise RuntimeError("hash")


def test_five_events_with_hostile_stamps_arrive_in_one_request() -> None:
    """AC-12 (invariants 1 and 2): directly, and the hash-raising stamp through a transform."""
    events = [_event(i) for i in range(5)]
    events[1]["service"] = _HashRaises("svc")
    events[3]["service"] = "a\ud800b"
    sink, opener = _sink()
    sink.emit(events)
    assert len(opener.calls) == 1
    assert sorted(_messages(_bodies(opener)[0])) == [f"m{i}" for i in range(5)]
    roundtrip(_bodies(opener)[0])
    assert sink.losses().dropped == 0

    sink, opener = _sink()
    hostile = [_event(i) for i in range(5)]

    def stamp(event: dict[str, object]) -> dict[str, object]:
        if event["message"] == "m2":
            return {**event, "service": _HashRaises("svc")}
        return event

    TransformSink(sink, stamp).emit(hostile)
    assert len(opener.calls) == 1
    assert sorted(_messages(_bodies(opener)[0])) == [f"m{i}" for i in range(5)]
    roundtrip(_bodies(opener)[0])


def test_int_keys_through_a_transform_arrive_as_attributes() -> None:
    """FR-001 AC-20, the transform half: ``fields={1: "a", 2: "b"}`` passes a transform."""
    sink, opener = _sink()
    TransformSink(sink, lambda e: {**e, "fields": {1: "a", 2: "b"}}).emit([_event()])
    record = json.loads(_bodies(opener)[0])["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]
    attrs = {a["key"]: a["value"] for a in record["attributes"]}
    assert attrs["1"] == {"stringValue": "a"} and attrs["2"] == {"stringValue": "b"}
    assert "droppedAttributesCount" not in record
    roundtrip(_bodies(opener)[0])


def test_an_oversized_record_is_dropped_counted_and_announced(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """AC-13 (invariants 2 and 11)."""
    events = [_event(0), _event(1, fields={"big": "x" * 5000}), _event(2)]
    sink, opener = _sink(max_batch_bytes=2000)
    sink.emit(events)
    assert sink.losses() == SinkLosses(dropped=1, failed=0)
    delivered = [m for b in _bodies(opener) for m in _messages(b)]
    assert sorted(delivered) == ["m0", "m2"]
    lines = [line for line in capsys.readouterr().err.splitlines() if "per-event ceiling" in line]
    assert len(lines) == 1 and "OTLPSink" in lines[0] and "bytes" in lines[0]
    assert "xxxx" not in lines[0]


@pytest.mark.parametrize("mode", ["down", "once"])
def test_the_oversize_drop_is_retry_safe(mode: str, capsys: pytest.CaptureFixture[str]) -> None:
    """AC-14 (invariants 2 and 11): nothing counted on a raise, once on a retried delivery."""
    responses = (
        [FakeResponse(503, b"")]
        if mode == "down"
        else [FakeResponse(503, b""), FakeResponse(200, b"")]
    )
    sink, _ = _sink(*responses, max_batch_bytes=2000, max_retries=0)
    batch = [_event(0), _event(1, fields={"big": "x" * 5000}), _event(2)]
    if mode == "down":
        with pytest.raises(SinkDeliveryError):
            sink.emit(batch)
        assert sink.losses().dropped == 0
        assert "per-event ceiling" not in capsys.readouterr().err
        return
    worker = Worker(sink, batch_size=3, max_retries=3)
    try:
        worker.submit(batch)
        delivered = bool(worker.flush(timeout=5.0))
    finally:
        worker.stop()
    assert delivered and sink.losses().dropped == 1
    err = capsys.readouterr().err
    assert sum(1 for line in err.splitlines() if "per-event ceiling" in line) == 1


def test_the_worker_and_orphan_paths_deliver_the_same_records() -> None:
    """AC-16 (invariant 6)."""
    ledgers: list[SinkLosses | None] = []
    collected: list[list[str]] = []
    for path in ("worker", "orphan"):
        opener = FakeOpener()
        sink = OTLPSink(opener=opener)
        log_foundry.configure(service="t", version="1", env="test", sink=sink)
        if path == "worker":

            @log_foundry.trace
            def work() -> None:
                for i in range(3):
                    log_foundry.info(f"m{i}")

            work()
        else:
            for i in range(3):
                log_foundry.info(f"m{i}")
        assert log_foundry.flush(timeout=5.0)
        ledgers.append(log_foundry.health().sink)
        bodies = _bodies(opener)
        assert bodies, path
        for body in bodies:
            roundtrip(body)
        messages = [m for body in bodies for m in _messages(body)]
        collected.append(sorted(m for m in messages if m.startswith("m")))
    assert collected[0] == collected[1] == ["m0", "m1", "m2"]
    assert ledgers[0] == ledgers[1] == SinkLosses(dropped=0, failed=0)


def test_no_emit_an_idempotent_close_and_the_three_recorded_decisions() -> None:
    """AC-17."""
    from test_sink_concurrency import _sink_classes_with_an_emit

    source = Path(OTLPSink.__module__.replace(".", "/") + ".py")
    tree = ast.parse((Path(log_foundry.__file__).parent.parent / source).read_text("utf-8"))
    defined = {
        node.name
        for cls in ast.walk(tree)
        if isinstance(cls, ast.ClassDef)
        for node in cls.body
        if isinstance(node, ast.FunctionDef)
    }
    assert "emit" not in defined
    sink, opener = _sink()
    sink.close()
    sink.close()
    sink.emit([_event()])
    assert len(opener.calls) == 1, "close releases nothing, so a later batch is still posted"
    doc = " ".join((OTLPSink.__doc__ or "").split())
    assert "**no** transport lock" in doc and "SPEC-028 FR-002" in doc
    assert "**adds no post-close guard**" in doc and "SPEC-032 FR-003" in doc
    assert "**no** client buffer" in doc and "SPEC-036 FR-002" in doc
    assert ("otlp", "OTLPSink") in {(stem, cls.name) for stem, cls in _sink_classes_with_an_emit()}


# --- FR-004: partial success --------------------------------------------------------------


@pytest.mark.parametrize(
    "body",
    [
        b"",
        b"{}",
        b"not json",
        b"[]",
        b"[" * 1_000_000,
        b'{"partialSuccess": null}',
        b'{"partialSuccess": {}}',
        b'{"partialSuccess": {"rejectedLogRecords": null}}',
        b'{"partialSuccess": {"rejectedLogRecords": "0", "errorMessage": "sampled"}}',
        b"\x08\x96\x01",
        gzip.compress(b'{"partialSuccess": {"rejectedLogRecords": "2"}}'),
    ],
    ids=[
        "empty",
        "object",
        "text",
        "array",
        "deep",
        "null",
        "empty-partial",
        "null-count",
        "zero",
        "protobuf",
        "gzip",
    ],
)
def test_full_success_bodies_settle_silently(
    body: bytes, capsys: pytest.CaptureFixture[str]
) -> None:
    """AC-1 and AC-10. OTLP's reading only: ``ElasticsearchSink`` counts some of these."""
    sink, opener = _sink(FakeResponse(200, body))
    sink.emit([_event(i) for i in range(5)])
    assert len(opener.calls) == 1
    assert (sink.rejected_records, sink.dropped_unadjudicated) == (0, 0)
    assert sink.losses() == SinkLosses(dropped=0, failed=0)
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize("count", ["2", 2])
def test_two_rejected_records_are_counted_and_not_retried(
    count: object, capsys: pytest.CaptureFixture[str]
) -> None:
    """AC-2 and AC-3 (invariant 11)."""
    sink, opener = _sink(_partial(count, errorMessage="SECRET-123"), max_retries=3)
    sink.emit([_event(i) for i in range(5)])
    assert len(opener.calls) == 1
    assert sink.rejected_records == 2
    assert sink.losses() == SinkLosses(dropped=0, failed=2)
    err = capsys.readouterr().err
    lines = _lost_lines(err)
    assert len(lines) == 1 and "lost 2 log record(s)" in lines[0] and "OTLPSink" in lines[0]
    assert "SECRET-123" not in err
    assert "SECRET-123" not in repr(sink.losses())


def test_the_count_is_clamped_to_the_chunk() -> None:
    """AC-4."""
    sink, opener = _sink(_partial("9"))
    with pytest.raises(SinkDeliveryError):
        sink.emit([_event(i) for i in range(5)])
    assert sink.rejected_records == 5 and len(opener.calls) == 1


@pytest.mark.parametrize(
    "count",
    ["-1", "x", True, 1.5, [], "1" * 5000, 10**30, -1, b"9" * 5000, "٢", "", "0x2"],
    ids=[
        "neg-str",
        "text",
        "bool",
        "float",
        "list",
        "long-str",
        "beyond-int64",
        "neg-num",
        "long-num",
        "arabic-digit",
        "empty",
        "hex",
    ],
)
def test_an_unreadable_count_is_unadjudicated(
    count: object, capsys: pytest.CaptureFixture[str]
) -> None:
    """AC-5 (invariants 2 and 11). The bare 5,000-digit number is the one ``parse_int=str`` saves."""
    sink, opener = _sink(_partial(count))
    sink.emit([_event(i) for i in range(5)])
    _assert_unadjudicated(sink, opener, capsys)


@pytest.mark.parametrize("partial", [5, [], "x"], ids=repr)
def test_a_partial_success_that_is_not_an_object_is_unadjudicated(
    partial: object, capsys: pytest.CaptureFixture[str]
) -> None:
    """AC-5, the whole-``partialSuccess`` half."""
    body = json.dumps({"partialSuccess": partial}).encode("utf-8")
    sink, opener = _sink(FakeResponse(200, body))
    sink.emit([_event(i) for i in range(5)])
    _assert_unadjudicated(sink, opener, capsys)


def _assert_unadjudicated(
    sink: OTLPSink, opener: FakeOpener, capsys: pytest.CaptureFixture[str]
) -> None:
    assert len(opener.calls) == 1
    assert sink.dropped_unadjudicated == 5 and sink.rejected_records == 0
    assert sink.losses() == SinkLosses(dropped=0, failed=5)
    err = capsys.readouterr().err
    lines = _lost_lines(err)
    assert len(lines) == 1 and "OTLPSink" in lines[0] and "lost 5 log record(s)" in lines[0]
    assert "partialSuccess" not in err and "rejectedLogRecords" not in err


def test_a_2xx_over_the_bound_is_unadjudicated_and_exactly_the_bound_is_read() -> None:
    """AC-6."""
    over = FakeResponse(200, b" " * (LIMIT + 1))
    sink, opener = _sink(over)
    sink.emit([_event(i) for i in range(5)])
    assert over.read_sizes == [LIMIT + 1]
    assert len(opener.calls) == 1
    assert (sink.dropped_unadjudicated, sink.rejected_records) == (5, 0)
    verdict = b'{"partialSuccess": {"rejectedLogRecords": "2"}}'
    exact = FakeResponse(200, verdict + b" " * (LIMIT - len(verdict)))
    sink, opener = _sink(exact)
    sink.emit([_event(i) for i in range(5)])
    assert (sink.rejected_records, sink.dropped_unadjudicated) == (2, 0)


def test_every_record_rejected_raises_and_the_worker_recounts_per_attempt() -> None:
    """AC-7 (invariants 2 and 7)."""
    sink, opener = _sink(_partial("5"))
    with pytest.raises(SinkDeliveryError):
        sink.emit([_event(i) for i in range(5)])
    assert sink.losses().failed == 5 and len(opener.calls) == 1
    sink, opener = _sink(_partial(5))
    worker = Worker(sink, batch_size=5, max_retries=3)
    try:
        worker.submit([_event(i) for i in range(5)])
        delivered = bool(worker.flush(timeout=5.0))
        failed_batches = worker.health().failed_batches
    finally:
        worker.stop()
    assert not delivered and failed_batches > 0
    assert sink.rejected_records == 5 * len(opener.calls) == 20
    assert sink.losses().dropped == 0


def test_a_rejected_first_chunk_beside_a_delivered_second_does_not_raise() -> None:
    """AC-8 (invariant 2)."""
    sink, opener = _sink(_partial("2"), FakeResponse(200, b""), max_batch_count=2)
    sink.emit([_event(i) for i in range(4)])
    assert len(opener.calls) == 2
    assert sink.losses() == SinkLosses(dropped=0, failed=2)


def test_eight_threads_count_exactly_and_losses_does_not_block() -> None:
    """AC-9 (invariant 4)."""
    from conftest import run_concurrently

    sink, _ = _sink(_partial("1"))
    errors = run_concurrently(
        lambda thread, iteration: sink.emit([_event(thread), _event(iteration)]),
        THREADS,
        per_thread=PER_THREAD,
    )
    assert errors == []
    assert sink.rejected_records == THREADS * PER_THREAD == 200
    assert sink.losses().failed == 200
    gate = threading.Event()

    def blocking(request: Any, timeout: float | None = None) -> FakeResponse:
        gate.wait(5.0)
        return FakeResponse(200, b"")

    slow = OTLPSink(opener=blocking)
    thread = threading.Thread(target=lambda: slow.emit([_event()]), daemon=True)
    thread.start()
    started = time.perf_counter()
    assert slow.losses() == SinkLosses(dropped=0, failed=0)
    assert time.perf_counter() - started < 1.0, "losses() waited behind an in-flight emit"
    gate.set()
    thread.join(5.0)


def test_the_rejected_counter_holds_under_an_observable_race(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Plan review MINOR-4d: a preemption point in the counter's storage, and the lock holds."""
    monkeypatch.setattr(
        OTLPSink, "rejected_records", SlowCounter("rejected_records"), raising=False
    )
    sink, _ = _sink(_partial("1"))
    sink.rejected_records = 0
    from conftest import run_concurrently

    errors = run_concurrently(
        lambda thread, iteration: sink.emit([_event(thread), _event(iteration)]),
        THREADS,
        per_thread=PER_THREAD,
    )
    assert errors == []
    assert sink.rejected_records == THREADS * PER_THREAD, "increments were lost under the lock"


def test_handle_response_never_raises_on_hostile_bytes() -> None:
    """FR-004's never-raises rule, over bytes no server should send."""
    sink = OTLPSink()
    for payload in (
        b"\xff\xfe",
        b'{"partialSuccess": {"rejectedLogRecords": {"a": 1}}}',
        b"{" * 100_000,
        b"null",
        b"1",
        b'"s"',
    ):
        assert sink._handle_response(payload, []) is True


# --- FR-005: the README's attribute list, and the surface that did not grow ------------------


def _readme_attribute_list() -> list[str]:
    """The backticked keys on the README subsection's ``Record attributes:`` sentence."""
    import re

    text = (Path(log_foundry.__file__).parent.parent.parent / "README.md").read_text("utf-8")
    start = text.index("##### OTLPSink: what it sends")
    section = text[start : text.index("#### SaaS platforms", start)]
    sentence = section[section.index("Record attributes:") :]
    sentence = sentence[: sentence.index("followed by")]
    return re.findall(r"`([a-z_]+(?:\.[a-z_]+)+)`", sentence)


def test_the_readme_lists_exactly_the_attribute_keys_a_maximal_event_emits() -> None:
    """FR-005 AC-3 (invariant 10): the documented names are what a consumer's queries rest on."""
    maximal = _event(
        message="span.end",
        parent_span_id="00f067aa0ba902b7",
        truncated=True,
        duration_ms=1.5,
        status="error",
        error={"type": "Boom", "module": "m", "message": "x", "stack": "tb"},
        fields={},
    )
    emitted = [a["key"] for a in _otel.log_record(maximal)["attributes"]]
    documented = _readme_attribute_list()
    assert documented == emitted, f"README {documented} != emitted {emitted}"
    assert len(documented) == 9


def test_nothing_from_otel_is_public_and_the_surface_grew_by_one_path_only() -> None:
    """FR-005 AC-8 (invariant 10)."""
    import importlib
    import pkgutil

    import log_foundry.sinks

    otel_names = {name for name in vars(_otel) if not name.startswith("__")}
    assert not hasattr(_otel, "__all__")
    assert not otel_names & set(log_foundry.__all__)
    for module in pkgutil.iter_modules(log_foundry.sinks.__path__):
        exported = getattr(
            importlib.import_module(f"log_foundry.sinks.{module.name}"), "__all__", ()
        )
        assert not otel_names & set(exported), module.name
    assert len(log_foundry.__all__) == 30, "the top-level surface is the 30 names of 1.0.2"
    assert not hasattr(log_foundry.sinks, "__all__")
    assert log_foundry.sinks.otlp.__all__ == ["OTLPSink"]


class _MyOTLP(OTLPSink):
    """A subclass, so a refusal that hard-codes the base class's name is caught by name."""


def test_a_subclass_refusal_names_the_subclass() -> None:
    """System-frame review: the family names `type(self).__name__`, never a literal class name."""
    with pytest.raises(TypeError, match=r"^_MyOTLP endpoint"):
        _MyOTLP(5)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match=r"^_MyOTLP endpoint"):
        _MyOTLP("ftp://h/v1/logs")
    with pytest.raises(ValueError, match=r"^_MyOTLP method"):
        _MyOTLP(method="PUT")
    with pytest.raises(ValueError, match=r"^_MyOTLP timeout"):
        _MyOTLP(timeout=-1)
