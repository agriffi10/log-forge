"""SPEC-026 — the sink loss contract: raise on total failure, report partial loss.

Phase 1 (the contract itself: ``SinkLosses``, ``read_losses``, ``Health.sink``) and Phase 2
(the zero-dependency and composition sinks). Everything here is in-process — sink doubles and
fake sockets/openers — so no test touches the network.
"""

from __future__ import annotations

import pytest

import log_foundry.sinks._socket as socket_mod
from log_foundry.sinks.base import Sink, SinkDeliveryError, SinkLosses, read_losses
from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.multi import MultiSink
from log_foundry.worker import Health, Worker


class QuietSink:
    """A sink that takes everything and reports nothing — the pre-SPEC-026 interface."""

    def __init__(self) -> None:
        self.batches: list[list[dict[str, object]]] = []

    def emit(self, batch: list[dict[str, object]]) -> None:
        self.batches.append(list(batch))

    def close(self) -> None:
        pass


class CountingSink(QuietSink):
    """A sink that reports loss through the optional accessor."""

    def __init__(self, dropped: int = 0, failed: int = 0) -> None:
        super().__init__()
        self._losses = SinkLosses(dropped=dropped, failed=failed)

    def losses(self) -> SinkLosses:
        return self._losses


class BrokenLossesSink(QuietSink):
    def losses(self) -> SinkLosses:
        raise RuntimeError("accessor is broken")


class WrongShapeSink(QuietSink):
    def losses(self) -> object:
        return (1, 2)  # a plain tuple is not a SinkLosses


class DeadSink(QuietSink):
    """Delivers nothing, ever — the total-failure case."""

    def emit(self, batch: list[dict[str, object]]) -> None:
        raise SinkDeliveryError("delivered none")


def _span(name: str) -> list[dict[str, object]]:
    return [{"event": name}]


# --- FR-002: read_losses is the single, total probe --------------------------------------


def test_read_losses_returns_the_snapshot() -> None:
    assert read_losses(CountingSink(dropped=2, failed=3)) == SinkLosses(dropped=2, failed=3)


def test_a_sink_without_the_accessor_reports_nothing() -> None:
    assert read_losses(QuietSink()) is None


def test_a_raising_accessor_is_absorbed() -> None:
    assert read_losses(BrokenLossesSink()) is None


def test_a_wrong_shaped_return_is_refused() -> None:
    """Two ints in a bare tuple would read fine positionally and lie about the contract."""
    assert read_losses(WrongShapeSink()) is None


def test_a_non_callable_losses_attribute_is_refused() -> None:
    sink = QuietSink()
    sink.losses = SinkLosses(dropped=1, failed=1)  # type: ignore[attr-defined]
    assert read_losses(sink) is None


# --- FR-003: health() carries the sink's snapshot ----------------------------------------


def test_health_reports_the_configured_sinks_losses() -> None:
    worker = Worker(CountingSink(dropped=4, failed=5), batch_size=1)
    try:
        assert worker.health().sink == SinkLosses(dropped=4, failed=5)
    finally:
        worker.shutdown()


def test_health_sink_is_none_when_the_sink_reports_nothing() -> None:
    worker = Worker(QuietSink(), batch_size=1)
    try:
        assert worker.health().sink is None
    finally:
        worker.shutdown()


def test_health_never_raises_on_a_broken_accessor() -> None:
    worker = Worker(BrokenLossesSink(), batch_size=1)
    try:
        health = worker.health()
    finally:
        worker.shutdown()
    assert health.sink is None
    assert health.failed_batches == 0, "the rest of the snapshot is intact"


def test_health_sink_tracks_the_counters_live() -> None:
    sink = CountingSink()
    worker = Worker(sink, batch_size=1)
    try:
        assert worker.health().sink == SinkLosses(dropped=0, failed=0)
        sink._losses = SinkLosses(dropped=7, failed=0)
        assert worker.health().sink == SinkLosses(dropped=7, failed=0)
    finally:
        worker.shutdown()


def test_no_worker_reports_no_sink_losses() -> None:
    import log_foundry

    assert log_foundry.health().sink is None


def test_health_sink_defaults_to_none_for_third_party_construction() -> None:
    assert Health(queued=0, dropped=0, failed_batches=0).sink is None


# --- FR-001: a sink that delivered nothing reaches the worker ----------------------------


def test_a_total_failure_reaches_failed_batches_and_flush() -> None:
    """Characterization, not a regression guard — ``DeadSink`` raises either way.

    The guard for the actual defect is ``test_a_dead_syslog_destination_now_reports_loss``
    below, against a shipped sink that used to absorb. This one pins the worker-side half of
    the contract: what a raising sink is supposed to produce.
    """
    worker = Worker(DeadSink(), batch_size=1, max_retries=0)
    try:
        worker.submit(_span("a"))
        assert worker.flush(timeout=2.0) is False
        assert worker.health().failed_batches == 1
        assert worker.health().stopped_reason is None, "the drain thread survived"
    finally:
        worker.shutdown()


def test_the_worker_keeps_draining_after_a_total_failure() -> None:
    class FlakySink(QuietSink):
        def __init__(self) -> None:
            super().__init__()
            self.calls = 0

        def emit(self, batch: list[dict[str, object]]) -> None:
            self.calls += 1
            if self.calls == 1:
                raise SinkDeliveryError("first batch is dead")
            super().emit(batch)

    sink = FlakySink()
    worker = Worker(sink, batch_size=1, max_retries=0)
    try:
        worker.submit(_span("a"))
        worker.flush(timeout=2.0)
        worker.submit(_span("b"))
        assert worker.flush(timeout=2.0) is True
    finally:
        worker.shutdown()
    assert [e["event"] for batch in sink.batches for e in batch] == ["b"]


# --- FR-002: MultiSink aggregates the tree ------------------------------------------------


def test_multisink_sums_its_children() -> None:
    multi = MultiSink(CountingSink(dropped=1, failed=2), CountingSink(dropped=10, failed=20))
    assert multi.losses() == SinkLosses(dropped=11, failed=22)


def test_multisink_skips_children_that_report_nothing_or_break() -> None:
    multi = MultiSink(QuietSink(), BrokenLossesSink(), CountingSink(dropped=3, failed=4))
    assert multi.losses() == SinkLosses(dropped=3, failed=4)


def test_multisink_aggregation_nests() -> None:
    inner = MultiSink(CountingSink(dropped=1, failed=1), CountingSink(dropped=2, failed=2))
    assert MultiSink(inner, CountingSink(dropped=5, failed=0)).losses() == SinkLosses(8, 3)


def test_multisink_excludes_its_own_call_counter() -> None:
    """``MultiSink.failed`` counts child *calls*; adding it to a per-event sum has no unit."""
    multi = MultiSink(DeadSink(), CountingSink(dropped=0, failed=6))
    multi.emit([{"a": 1}])  # a sibling took it, so a partial fan-out failure does not raise
    assert multi.failed == 1, "the child call that raised was counted"
    assert multi.losses() == SinkLosses(dropped=0, failed=6), "and excluded from the aggregate"


def test_an_empty_multisink_reports_zero() -> None:
    assert MultiSink().losses() == SinkLosses(dropped=0, failed=0)


# --- FR-004: the contract is on the interface --------------------------------------------


def test_a_pre_spec026_sink_still_satisfies_the_protocol() -> None:
    """``losses()`` is optional: adding it to the Protocol would break third-party sinks."""
    assert isinstance(QuietSink(), Sink)
    assert not hasattr(Sink, "losses")


# --- FR-001 / FR-002: the socket transport ------------------------------------------------


class _HalfDeadSocket:
    """A socket that takes the first message and refuses every one after it."""

    def __init__(self) -> None:
        self.sent: list[bytes] = []

    def sendto(self, data: bytes, addr: tuple) -> None:
        if self.sent:
            raise OSError(111, "Connection refused")
        self.sent.append(data)

    def close(self) -> None:
        return None


def test_socket_transport_raises_when_nothing_was_sent(monkeypatch) -> None:
    from test_sinks_syslog import RefusingSocket

    monkeypatch.setattr(socket_mod, "_make_udp", RefusingSocket)
    monkeypatch.setattr(socket_mod, "_BACKOFF_BASE", 0.0)
    transport = socket_mod.SocketTransport("h", 1, transport="udp", max_retries=0)

    with pytest.raises(SinkDeliveryError):
        transport.send_all([b"a", b"b"])
    assert transport.losses() == SinkLosses(dropped=0, failed=2)


def test_socket_transport_does_not_raise_on_a_partial_send(monkeypatch) -> None:
    """The worker's retry would re-send the message that already landed."""
    sock = _HalfDeadSocket()
    monkeypatch.setattr(socket_mod, "_make_udp", lambda: sock)
    monkeypatch.setattr(socket_mod, "_BACKOFF_BASE", 0.0)
    transport = socket_mod.SocketTransport("h", 1, transport="udp", max_retries=0)

    transport.send_all([b"a", b"b"])  # must not raise

    assert sock.sent == [b"a"]
    assert transport.losses() == SinkLosses(dropped=0, failed=1)


def test_an_empty_send_is_not_a_total_failure(monkeypatch) -> None:
    monkeypatch.setattr(socket_mod, "_make_udp", lambda: _HalfDeadSocket())
    socket_mod.SocketTransport("h", 1, transport="udp").send_all([])  # must not raise


def test_a_dead_syslog_destination_now_reports_loss(monkeypatch) -> None:
    """The measured reading SPEC-026 was written from: flush() True, health() all zeros."""
    from log_foundry.sinks.syslog import SyslogSink
    from test_sinks_syslog import RefusingSocket

    monkeypatch.setattr(socket_mod, "_make_udp", RefusingSocket)
    monkeypatch.setattr(socket_mod, "_BACKOFF_BASE", 0.0)
    sink = SyslogSink("loghost", transport="udp", max_retries=0)
    worker = Worker(sink, batch_size=1, max_retries=0)
    try:
        worker.submit(_span("a"))
        assert worker.flush(timeout=2.0) is False
        health = worker.health()
    finally:
        worker.shutdown()
    assert health.failed_batches == 1
    assert health.sink == SinkLosses(dropped=0, failed=1)


# --- FR-001 / FR-002: HTTPSink and its platform subclasses --------------------------------


def test_every_platform_subclass_inherits_the_raise() -> None:
    """Each builds its own body and calls ``_send``, so the rule lives there, not in each emit."""
    from log_foundry.sinks.datadog import DatadogSink
    from log_foundry.sinks.elasticsearch import ElasticsearchSink
    from log_foundry.sinks.honeycomb import HoneycombSink
    from log_foundry.sinks.loki import LokiSink
    from log_foundry.sinks.newrelic import NewRelicSink
    from log_foundry.sinks.splunk import SplunkHECSink
    from test_sinks_http import FakeOpener, FakeResponse

    builders = [
        lambda opener: HTTPSink("http://x", max_retries=0, opener=opener),
        lambda opener: DatadogSink("k", max_retries=0, opener=opener),
        lambda opener: SplunkHECSink("http://x", "t", max_retries=0, opener=opener),
        lambda opener: NewRelicSink("k", max_retries=0, opener=opener),
        lambda opener: HoneycombSink("k", "d", max_retries=0, opener=opener),
        lambda opener: LokiSink("http://x", max_retries=0, opener=opener),
        lambda opener: ElasticsearchSink("http://x", index="i", max_retries=0, opener=opener),
    ]
    for build in builders:
        sink = build(FakeOpener([FakeResponse(503, b"down")]))
        with pytest.raises(SinkDeliveryError):
            sink.emit([{"a": 1}])
        assert sink.losses().failed == 1, type(sink).__name__


def test_http_losses_report_oversized_drops_separately() -> None:
    from test_sinks_http import FakeOpener

    sink = HTTPSink("http://x", opener=FakeOpener())
    sink.dropped_oversized = 2
    assert sink.losses() == SinkLosses(dropped=2, failed=0)


def test_elasticsearch_item_errors_are_partial_and_do_not_raise() -> None:
    import json as _json

    from log_foundry.sinks.elasticsearch import ElasticsearchSink
    from test_sinks_http import FakeOpener, FakeResponse

    body = _json.dumps(
        {"errors": True, "items": [{"index": {"error": "nope"}}, {"index": {}}]}
    ).encode("utf-8")
    sink = ElasticsearchSink(
        "http://x", index="i", opener=FakeOpener([FakeResponse(200, body)])
    )

    sink.emit([{"a": 1}, {"b": 2}])  # one item landed, so this must not raise

    assert sink.item_errors == 1
    assert sink.losses() == SinkLosses(dropped=0, failed=1)


def test_an_empty_http_batch_never_raises() -> None:
    from test_sinks_http import FakeOpener

    opener = FakeOpener()
    HTTPSink("http://x", opener=opener).emit([])
    assert opener.calls == []


# --- FR-001 / FR-002: SentrySink sends one envelope per event -----------------------------


def test_sentry_absorbs_a_per_event_failure_when_something_landed() -> None:
    from log_foundry.sinks.sentry import SentrySink
    from test_sinks_http import FakeOpener, FakeResponse

    opener = FakeOpener([FakeResponse(500, b""), FakeResponse(200, b"{}")])
    sink = SentrySink("http://k@sentry.local/1", max_retries=0, opener=opener)

    sink.emit([{"level": "ERROR"}, {"level": "ERROR"}])  # second one lands

    assert sink.sent == 1
    assert sink.losses() == SinkLosses(dropped=0, failed=1)


def test_sentry_raises_when_no_envelope_landed() -> None:
    from log_foundry.sinks.sentry import SentrySink
    from test_sinks_http import FakeOpener, FakeResponse

    sink = SentrySink(
        "http://k@sentry.local/1",
        max_retries=0,
        opener=FakeOpener([FakeResponse(500, b"")]),
    )
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"level": "ERROR"}, {"level": "ERROR"}])


def test_sentry_skipping_every_event_is_a_successful_emit() -> None:
    from log_foundry.sinks.sentry import SentrySink
    from test_sinks_http import FakeOpener

    sink = SentrySink("http://k@sentry.local/1", opener=FakeOpener())
    sink.emit([{"level": "INFO"}, {"level": "DEBUG"}])  # below min_level: never meant for Sentry
    assert (sink.skipped, sink.sent) == (2, 0)
    assert sink.losses() == SinkLosses(dropped=0, failed=0), "a filter is not a loss"


# --- FR-002: LogstashSink delegates to whichever backend it holds -------------------------


def test_logstash_losses_follow_the_active_backend(monkeypatch) -> None:
    from log_foundry.sinks.logstash import LogstashSink
    from test_sinks_http import FakeOpener, FakeResponse
    from test_sinks_syslog import RefusingSocket

    http = LogstashSink(url="http://ls", max_retries=0, opener=FakeOpener([FakeResponse(500)]))
    with pytest.raises(SinkDeliveryError):
        http.emit([{"a": 1}])
    assert http.losses() == SinkLosses(dropped=0, failed=1)

    monkeypatch.setattr(socket_mod, "_make_tcp", lambda host, port, timeout: RefusingSocket())
    monkeypatch.setattr(socket_mod, "_BACKOFF_BASE", 0.0)
    sock = LogstashSink(host="ls", port=5044, max_retries=0)
    with pytest.raises(SinkDeliveryError):
        sock.emit([{"a": 1}])
    assert sock.losses() == SinkLosses(dropped=0, failed=1)


# --- FR-002: the accessor is safe to call while emit is running ---------------------------


def test_losses_is_safe_to_call_concurrently_with_emit() -> None:
    """FR-002: ``health()`` is a poll, so it lands mid-emit routinely."""
    import threading

    started, release = threading.Event(), threading.Event()

    class SlowSink(QuietSink):
        def __init__(self) -> None:
            super().__init__()
            self.failed = 0

        def emit(self, batch: list[dict[str, object]]) -> None:
            started.set()
            release.wait(2.0)
            self.failed += len(batch)
            super().emit(batch)

        def losses(self) -> SinkLosses:
            return SinkLosses(dropped=0, failed=self.failed)

    sink = SlowSink()
    worker = Worker(sink, batch_size=1)
    try:
        worker.submit(_span("a"))
        assert started.wait(2.0), "the emit is in flight"
        assert worker.health().sink == SinkLosses(dropped=0, failed=0)
        release.set()
    finally:
        worker.shutdown()
    assert worker.health().sink == SinkLosses(dropped=0, failed=1)


# --- review follow-ups: wrappers, the SDK path, an all-rejected bulk ----------------------


def test_filtering_and_transform_report_the_inner_sinks_losses() -> None:
    """A wrapper that reported nothing would hide the destination it wraps (FR-003)."""
    from log_foundry.sinks.filtering import FilteringSink
    from log_foundry.sinks.transform import TransformSink

    inner = CountingSink(dropped=2, failed=3)
    assert FilteringSink(inner, min_level="ERROR").losses() == SinkLosses(dropped=2, failed=3)
    assert TransformSink(inner, lambda e: e).losses() == SinkLosses(dropped=2, failed=3)


def test_a_wrapper_passes_an_unreporting_inner_sink_through_as_none() -> None:
    """FR-003 separates "reports nothing" from "reports no loss"; a wrapper must not flatten it."""
    from log_foundry.sinks.filtering import FilteringSink
    from log_foundry.sinks.transform import TransformSink

    assert FilteringSink(QuietSink()).losses() is None
    assert TransformSink(QuietSink(), lambda e: e).losses() is None
    assert Worker(FilteringSink(QuietSink()), batch_size=1).health().sink is None


def test_sentry_sdk_errors_are_isolated_per_event() -> None:
    """A ``capture_event`` that raises mid-batch would duplicate what Sentry already took."""
    from log_foundry.sinks.sentry import SentrySink

    class FlakySDK:
        def __init__(self) -> None:
            self.calls = 0

        def capture_event(self, event: dict) -> None:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("transport down")

    sdk = FlakySDK()
    sink = SentrySink(sdk=sdk)

    sink.emit([{"level": "ERROR"}, {"level": "ERROR"}])  # the second lands: must not raise

    assert (sdk.calls, sink.sent, sink.transport_errors) == (2, 1, 1)
    assert sink.losses() == SinkLosses(dropped=0, failed=1)


def test_sentry_sdk_raises_when_every_capture_failed() -> None:
    from log_foundry.sinks.sentry import SentrySink

    class DeadSDK:
        def capture_event(self, event: dict) -> None:
            raise RuntimeError("transport down")

    with pytest.raises(SinkDeliveryError):
        SentrySink(sdk=DeadSDK()).emit([{"level": "ERROR"}, {"level": "ERROR"}])


def _bulk(*errors: bool) -> bytes:
    import json as _json

    items = [{"index": {"error": "nope"} if bad else {}} for bad in errors]
    return _json.dumps({"errors": any(errors), "items": items}).encode("utf-8")


def test_elasticsearch_raises_when_the_bulk_response_rejected_every_item() -> None:
    """A 200 that indexed nothing is a total failure like any other (FR-001)."""
    from log_foundry.sinks.elasticsearch import ElasticsearchSink
    from test_sinks_http import FakeOpener, FakeResponse

    sink = ElasticsearchSink(
        "http://x", index="i", opener=FakeOpener([FakeResponse(200, _bulk(True, True))])
    )
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}, {"b": 2}])
    assert sink.item_errors == 2


def test_elasticsearch_partial_rejection_still_does_not_raise() -> None:
    from log_foundry.sinks.elasticsearch import ElasticsearchSink
    from test_sinks_http import FakeOpener, FakeResponse

    sink = ElasticsearchSink(
        "http://x", index="i", opener=FakeOpener([FakeResponse(200, _bulk(True, False))])
    )
    sink.emit([{"a": 1}, {"b": 2}])  # one landed: retrying would duplicate it
    assert sink.item_errors == 1


def test_an_unreadable_bulk_body_is_not_read_as_a_total_failure() -> None:
    from log_foundry.sinks.elasticsearch import ElasticsearchSink
    from test_sinks_http import FakeOpener, FakeResponse

    sink = ElasticsearchSink(
        "http://x", index="i", opener=FakeOpener([FakeResponse(200, b"not json")])
    )
    sink.emit([{"a": 1}])  # the request succeeded; an unparseable body is no evidence against it
    assert sink.item_errors == 0


def test_a_negative_max_retries_still_makes_one_attempt() -> None:
    """``Worker._emit`` floors its own for exactly this shape (SPEC-021)."""
    from test_sinks_http import FakeOpener, FakeResponse

    opener = FakeOpener([FakeResponse(500, b"")])
    sink = HTTPSink("http://x", max_retries=-1, opener=opener)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}])
    assert len(opener.calls) == 1, "abandoned with no attempt made is a silent loss"
    assert sink.losses() == SinkLosses(dropped=0, failed=1)


# --- re-review follow-ups -----------------------------------------------------------------


class _RaisingOpener:
    """An opener whose Nth call raises something that is neither URLError nor OSError."""

    def __init__(self, fail_on: int, exc: Exception) -> None:
        self.calls = 0
        self._fail_on = fail_on
        self._exc = exc

    def __call__(self, request, timeout=None):
        from test_sinks_http import FakeResponse

        self.calls += 1
        if self.calls == self._fail_on:
            raise self._exc
        return FakeResponse(200, b"{}")


def test_sentry_absorbs_a_response_error_http_sink_does_not_retry() -> None:
    """``HTTPSink`` catches (URLError, OSError); ``IncompleteRead`` is neither."""
    import http.client

    from log_foundry.sinks.sentry import SentrySink

    opener = _RaisingOpener(3, http.client.IncompleteRead(b""))
    sink = SentrySink("http://k@sentry.local/1", max_retries=0, opener=opener)

    sink.emit([{"level": "ERROR"}] * 5)  # two already accepted: must not propagate

    assert (sink.sent, sink.transport_errors) == (4, 1)
    assert sink.losses() == SinkLosses(dropped=0, failed=1)


def test_a_longer_items_array_does_not_turn_a_partial_success_into_a_raise() -> None:
    """Reading a positional response that does not describe the batch is SPEC-018's rule."""
    from log_foundry.sinks.elasticsearch import ElasticsearchSink
    from test_sinks_http import FakeOpener, FakeResponse

    sink = ElasticsearchSink(
        "http://x",
        index="i",
        opener=FakeOpener([FakeResponse(200, _bulk(True, True, True))]),
    )
    sink.emit([{"a": 1}, {"b": 2}])  # 3 items for 2 events: unadjudicable, never a raise
    assert (sink.item_errors, sink.dropped_unadjudicated) == (0, 2)
    assert sink.losses() == SinkLosses(dropped=0, failed=2)


def test_a_short_items_array_is_not_read_as_a_partial_success() -> None:
    from log_foundry.sinks.elasticsearch import ElasticsearchSink
    from test_sinks_http import FakeOpener, FakeResponse

    sink = ElasticsearchSink(
        "http://x", index="i", opener=FakeOpener([FakeResponse(200, _bulk(True))])
    )
    sink.emit([{"a": 1}, {"b": 2}])  # 1 item for 2 events: the outcome is simply unknown
    assert sink.dropped_unadjudicated == 2


def test_socket_transport_floors_a_negative_max_retries(monkeypatch) -> None:
    from test_sinks_syslog import RefusingSocket

    sock = RefusingSocket()
    monkeypatch.setattr(socket_mod, "_make_udp", lambda: sock)
    monkeypatch.setattr(socket_mod, "_BACKOFF_BASE", 0.0)
    transport = socket_mod.SocketTransport("h", 1, transport="udp", max_retries=-1)

    with pytest.raises(SinkDeliveryError):
        transport.send_all([b"a"])
    assert transport.losses() == SinkLosses(dropped=0, failed=1), "abandoned without a counter"
