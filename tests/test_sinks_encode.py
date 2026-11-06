"""SPEC-069 FR-001 to FR-003 — every record-building sink, run over the derived roster.

The rigs live in ``tests/sink_rigs.py``; their table is held equal to the classes the roster lint
in ``tests/test_encode.py`` derives from the tree, so a sink cannot join the helper untested.
"""

from __future__ import annotations

import copy
import json
import os
import socket
import sys
import types
from typing import TYPE_CHECKING, Any

import pytest

from log_foundry.sinks.base import SinkDeliveryError, SinkLosses, read_losses
from sink_rigs import EIGHT, EVENTS, RIGS, ok, unencodable
from test_encode import on_helper_public

if TYPE_CHECKING:
    from pathlib import Path

A = '{"log_id": "a", "level": "INFO", "timestamp": "2026-01-01T00:00:00.000Z", "message": "one"}'
B = '{"log_id": "b", "level": "ERROR", "timestamp": "2026-01-01T00:00:01.000Z", "message": "two"}'
_A = {"log_id": "a", "level": "INFO", "timestamp": "2026-01-01T00:00:00.000Z", "message": "one"}
_B = {"log_id": "b", "level": "ERROR", "timestamp": "2026-01-01T00:00:01.000Z", "message": "two"}


def _golden() -> dict[str, list[object]]:
    """Each sink's hand-offs for :data:`EVENTS`, as 1.1.0 wrote them (captured at ``ebf9d680``).

    The syslog frame's hostname and PROCID are the only parts that depend on the machine and the
    process; they are filled in here rather than written as literals.
    """
    host, pid = socket.gethostname() or "-", os.getpid()
    a, b = A.encode(), B.encode()
    return {
        "StdoutSink": [A + "\n", B + "\n"],
        "StderrSink": [A + "\n", B + "\n"],
        "FileSink": [A + "\n", B + "\n"],
        "RotatingFileSink": [A + "\n", B + "\n"],
        "SQLiteSink": [
            [
                ("a", None, None, "2026-01-01T00:00:00.000Z", "INFO", None, A),
                ("b", None, None, "2026-01-01T00:00:01.000Z", "ERROR", None, B),
            ]
        ],
        "ClickHouseSink": [
            [["2026-01-01T00:00:00.000Z", "INFO", None, None, None, None, None, None, A]],
            [["2026-01-01T00:00:01.000Z", "ERROR", None, None, None, None, None, None, B]],
        ],
        "RabbitMQSink": [a, b],
        "KafkaSink": [(a, None), (b, None)],
        "AzureEventHubsSink": [[a], [b]],
        "KinesisSink": [
            [
                {"Data": a, "PartitionKey": "log-foundry"},
                {"Data": b, "PartitionKey": "log-foundry"},
            ]
        ],
        "FirehoseSink": [[{"Data": a + b"\n"}, {"Data": b + b"\n"}]],
        "SNSSink": [[{"Id": "0", "Message": A}, {"Id": "1", "Message": B}]],
        "SQSSink": [[{"Id": "0", "MessageBody": A}, {"Id": "1", "MessageBody": B}]],
        "MongoDBSink": [[_A, _B]],
        "SyslogSink": [
            f"<14>1 2026-01-01T00:00:00.000Z {host} log-foundry {pid} - - {A}".encode(),
            f"<11>1 2026-01-01T00:00:01.000Z {host} log-foundry {pid} - - {B}".encode(),
        ],
        "LogstashSink": [a + b"\n", b + b"\n"],
        "PostgresSink": [
            [
                ("2026-01-01T00:00:00.000Z", "INFO", None, None, None, None, A),
                ("2026-01-01T00:00:01.000Z", "ERROR", None, None, None, None, B),
            ]
        ],
        "RedisStreamsSink": [[A, B]],
        "RedisListSink": [[A, B]],
        "GooglePubSubSink": [a, b],
    }


ROSTER = sorted(RIGS)


def test_the_rig_table_is_the_derived_roster() -> None:
    """A sink on the helper with no rig, or a rig for a sink that left it, fails here."""
    assert set(RIGS) == on_helper_public()


@pytest.mark.parametrize("name", ROSTER)
def test_a_clean_batch_writes_the_same_bytes(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-001: a clean batch's wire form is byte-identical to 1.1.0's, and stderr stays silent."""
    rig = RIGS[name](monkeypatch, tmp_path)
    rig.sink.emit([dict(e) for e in EVENTS])
    assert rig.handoffs() == _golden()[name]
    assert capsys.readouterr().err == ""


# --- FR-001: one unencodable event costs only itself ----------------------------------------

_LINE = "encoding raised"


def _encode_lines(err: str) -> list[str]:
    return [line for line in err.splitlines() if _LINE in line]


@pytest.mark.parametrize("name", ROSTER)
def test_one_unencodable_event_costs_only_itself(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-001 AC1: the two encodable events are delivered once, and the third is counted."""
    rig = RIGS[name](monkeypatch, tmp_path)
    rig.sink.emit([ok("a"), unencodable(), ok("b")])
    assert rig.delivered() == ["a", "b"]
    assert rig.count() == 1
    assert read_losses(rig.sink) == SinkLosses(dropped=1, failed=0)
    assert _encode_lines(capsys.readouterr().err) == [
        f"log-foundry: lost 1 event(s); {name}, encoding raised TypeError"
    ]


@pytest.mark.parametrize("position", ["first", "middle", "last"])
@pytest.mark.parametrize("name", EIGHT)
def test_nothing_is_handed_over_before_every_event_is_encoded(
    name: str, position: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """FR-001 AC2: the eight that used to deliver before raising now encode the whole batch first.

    ``json.dumps`` is wrapped to log each encode into the log the double writes its hand-offs to,
    so the order of the two is observed rather than inferred from the outcome.
    """
    import json

    rig = RIGS[name](monkeypatch, tmp_path)
    real = json.dumps

    def logged(obj: object, *args: object, **kwargs: object) -> str:
        rig.state.log.append(("encode", None))
        return real(obj, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(json, "dumps", logged)
    batch = {
        "first": [unencodable(), ok("a"), ok("b")],
        "middle": [ok("a"), unencodable(), ok("b")],
        "last": [ok("a"), ok("b"), unencodable()],
    }[position]
    rig.sink.emit(batch)
    kinds = [kind for kind, _ in rig.state.log]
    assert kinds.count("encode") == 3
    assert "handoff" not in kinds[: kinds.index("encode") + 3], kinds
    assert rig.delivered() == ["a", "b"]
    assert rig.count() == 1


@pytest.mark.parametrize("name", ROSTER)
def test_a_raising_call_counts_nothing_and_the_returning_one_counts_once(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-001 AC3 and FR-002 AC4: a raising emit discards the drop; the call that returns counts it.

    While the rig is down every bulk call fails, so the encodable remainder fails and ``emit``
    raises. The assertion is on the unencodable counter and the encode line only: a sink whose
    retries are spent counts ``failed`` and writes its own line, which is not this spec's count.
    """
    rig = RIGS[name](monkeypatch, tmp_path)
    batch = [ok("a"), unencodable(), ok("b")]
    rig.state.down = True
    for _ in range(2):
        with pytest.raises(Exception):  # noqa: B017
            rig.sink.emit(batch)
        assert rig.count() == 0
    assert _encode_lines(capsys.readouterr().err) == []
    rig.state.down = False
    rig.sink.emit(batch)
    assert rig.count() == 1
    assert len(_encode_lines(capsys.readouterr().err)) == 1
    assert rig.delivered() == ["a", "b"]


@pytest.mark.parametrize("name", ROSTER)
def test_a_baseexception_from_encode_propagates(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-001 AC6: a ``KeyboardInterrupt`` from the encode leaves ``emit`` with nothing recorded."""
    import json

    rig = RIGS[name](monkeypatch, tmp_path)
    real = json.dumps

    def interrupting(obj: object, *args: object, **kwargs: object) -> str:
        if isinstance(obj, dict) and obj.get("log_id") == "stop":
            raise KeyboardInterrupt
        return real(obj, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(json, "dumps", interrupting)
    with pytest.raises(KeyboardInterrupt):
        rig.sink.emit([ok("a"), unencodable(), ok("stop")])
    assert rig.count() == 0
    assert rig.handoffs() == []
    assert _encode_lines(capsys.readouterr().err) == []


# --- FR-002: a batch with nothing encodable settles -----------------------------------------


@pytest.mark.parametrize("name", ROSTER)
def test_a_batch_of_nothing_encodable_settles(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-002 AC1: no client call, every event counted in ``dropped``, one line, a normal return."""
    rig = RIGS[name](monkeypatch, tmp_path)
    rig.sink.emit([unencodable("x"), unencodable("y"), unencodable("z")])
    assert rig.calls() == 0
    assert rig.count() == 3
    assert read_losses(rig.sink) == SinkLosses(dropped=3, failed=0)
    assert capsys.readouterr().err == (
        f"log-foundry: lost 3 event(s); {name}, encoding raised TypeError\n"
    )


CLOSABLE = [
    "FileSink",
    "RotatingFileSink",
    "SQLiteSink",
    "ClickHouseSink",
    "RabbitMQSink",
    "KafkaSink",
    "AzureEventHubsSink",
    "MongoDBSink",
    "PostgresSink",
    "RedisStreamsSink",
    "RedisListSink",
    "GooglePubSubSink",
    "SyslogSink",
    "LogstashSink",
]
"""Every sink on the roster with a closed state; the two socket sinks hold theirs on the transport."""


@pytest.mark.parametrize("name", CLOSABLE)
def test_a_closed_sink_still_raises_for_an_unencodable_batch(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-002 AC6: the closed check comes before the settle, so nothing is counted."""
    rig = RIGS[name](monkeypatch, tmp_path)
    rig.sink.close()
    with pytest.raises(SinkDeliveryError, match="closed"):
        rig.sink.emit([unencodable()])
    assert rig.count() == 0
    assert _encode_lines(capsys.readouterr().err) == []


NEW_LOSSES = ["StdoutSink", "StderrSink", "FileSink", "RotatingFileSink", "SQLiteSink"]


@pytest.mark.parametrize("name", NEW_LOSSES)
def test_a_new_losses_is_none_until_something_is_counted(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """FR-001 AC8: ``None`` after clean batches, then ``SinkLosses(dropped=1, failed=0)``."""
    rig = RIGS[name](monkeypatch, tmp_path)
    rig.sink.emit([ok("a")])
    rig.sink.emit([ok("b")])
    assert rig.sink.losses() is None
    rig.sink.emit([ok("c"), unencodable(), ok("d")])
    assert rig.sink.losses() == SinkLosses(dropped=1, failed=0)


@pytest.mark.parametrize(
    ("name", "method"),
    [
        ("PostgresSink", "_row"),
        ("RedisStreamsSink", "_encode"),
        ("RedisListSink", "_encode"),
        ("ClickHouseSink", "_row"),
    ],
)
def test_encode_runs_once_per_emit_not_per_attempt(
    name: str, method: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """FR-001 AC4: with three attempts, the encode function runs ``len(batch)`` times."""
    rig = RIGS[name](monkeypatch, tmp_path)
    rig.sink.max_retries = 2
    rig.state.fail_next = 2
    real = getattr(rig.sink, method)
    calls: list[object] = []

    def counted(event: dict[str, object]) -> object:
        calls.append(event)
        return real(event)

    monkeypatch.setattr(rig.sink, method, counted)
    batch = [ok("a"), ok("b"), ok("c")]
    rig.sink.emit(batch)
    assert [kind for kind, _ in rig.state.log].count("refused") == 2, "three attempts were made"
    assert len(calls) == len(batch)
    assert rig.delivered() == ["a", "b", "c"]


@pytest.mark.parametrize("name", ["StdoutSink", "HTTPSink"])
def test_the_line_names_class_and_type_only(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-001 AC5, over both twins: the helper's line and ``HTTPSink._items``' follow one rule.

    A sentinel sits in the event and in the exception's message; neither may reach stderr, and
    three failures write one line.
    """
    from log_foundry.sinks.http import HTTPSink
    from test_sinks_http import FakeOpener

    class Hostile:
        def __repr__(self) -> str:
            return "SENTINEL-in-the-message"

    if name == "HTTPSink":
        sink: object = HTTPSink("https://example.invalid/ingest", opener=FakeOpener())
        verb = "rendering"
    else:
        sink = RIGS[name](monkeypatch, tmp_path).sink
        verb = "encoding"
    bad = [{"log_id": f"x{i}", "s": "SENTINEL-in-the-event", "h": Hostile()} for i in range(3)]
    sink.emit([ok("a"), *bad])  # type: ignore[attr-defined]
    err = capsys.readouterr().err
    assert "SENTINEL" not in err
    assert err.splitlines() == [f"log-foundry: lost 3 event(s); {name}, {verb} raised TypeError"]


@pytest.mark.parametrize("name", ["SyslogSink", "LogstashSink"])
def test_the_socket_sinks_count_on_the_transport(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """FR-001 AC10: the count lives on the transport; ``dropped_oversized`` means oversize only."""
    rig = RIGS[name](monkeypatch, tmp_path)
    rig.sink.emit([ok("a"), unencodable()])
    transport = rig.sink._socket
    assert transport._unencodable == 1
    assert transport.dropped_oversized == 0
    assert rig.sink.losses() == SinkLosses(dropped=1, failed=0)


def test_multisink_counts_an_unencodable_event_once(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """FR-001 AC13: one term, once — the child settled, so ``MultiSink`` adds nothing."""
    from log_foundry.sinks.memory import MemorySink
    from log_foundry.sinks.multi import MultiSink

    multi = MultiSink(RIGS["StdoutSink"](monkeypatch, tmp_path).sink, MemorySink())
    multi.emit([ok("a"), unencodable(), ok("b")])
    assert multi.losses() == SinkLosses(dropped=1, failed=0)


def test_multisink_all_unencodable_is_one_term(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """FR-002 AC5: a settled all-unencodable batch is in ``dropped`` alone, never also ``failed``."""
    from log_foundry.sinks.memory import MemorySink
    from log_foundry.sinks.multi import MultiSink

    multi = MultiSink(RIGS["PostgresSink"](monkeypatch, tmp_path).sink, MemorySink())
    multi.emit([unencodable("x"), unencodable("y")])
    assert multi.losses() == SinkLosses(dropped=2, failed=0)


@pytest.mark.parametrize("name", ["PostgresSink", "RedisStreamsSink", "RedisListSink"])
def test_a_spent_bound_counts_the_whole_batch_in_failed(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """``failed`` is the whole batch, the unencodable event included, as before SPEC-069."""
    rig = RIGS[name](monkeypatch, tmp_path)
    rig.state.down = True
    with pytest.raises(SinkDeliveryError):
        rig.sink.emit([ok("a"), unencodable()])
    assert rig.sink.failed == 2
    assert rig.count() == 0


def test_a_failing_remainder_raises_and_counts_nothing_until_a_retry_returns(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-002 AC4: the unencodable count is decided by the call that finally returns."""
    rig = RIGS["KafkaSink"](monkeypatch, tmp_path)
    rig.state.down = True
    with pytest.raises(SinkDeliveryError):
        rig.sink.emit([ok("a"), unencodable(), ok("b")])
    assert rig.count() == 0
    assert _encode_lines(capsys.readouterr().err) == []
    rig.state.down = False
    rig.sink.emit([ok("a"), unencodable(), ok("b")])
    assert rig.count() == 1


# --- the declared exceptions -----------------------------------------------------------------


def _nats() -> object:
    from log_foundry.sinks.nats import NATSSink
    from test_sinks_nats import SlowNATS

    return NATSSink("logs", client=SlowNATS(per_event=0.0), jetstream=True, publish_timeout=30.0)


def _sentry() -> object:
    from log_foundry.sinks.sentry import SentrySink
    from test_sinks_http import FakeOpener

    return SentrySink(
        dsn="https://pubkey@o123.ingest.sentry.io/456", backend="http", opener=FakeOpener()
    )


def _sentry_event(i: int, bad: bool) -> dict[str, object]:
    return {"level": "ERROR", "message": f"m{i}", "fields": {"b": b"x"} if bad else {"n": i}}


@pytest.mark.parametrize("build", [_nats, _sentry], ids=["nats", "sentry"])
def test_the_declared_exceptions_still_count_failed(build) -> None:  # type: ignore[no-untyped-def]
    """FR-001 AC12: one unencodable event among several is ``failed``, and ``dropped`` stays 0."""
    sink = build()
    try:
        sink.emit([_sentry_event(0, False), _sentry_event(1, True), _sentry_event(2, False)])
        assert read_losses(sink) == SinkLosses(dropped=0, failed=1)
    finally:
        sink.close()


@pytest.mark.parametrize("build", [_nats, _sentry], ids=["nats", "sentry"])
def test_the_declared_exceptions_still_raise(build) -> None:  # type: ignore[no-untyped-def]
    """FR-002 AC7: a batch of nothing encodable raises as before, with ``failed`` counted."""
    sink = build()
    try:
        with pytest.raises(SinkDeliveryError):
            sink.emit([_sentry_event(0, True), _sentry_event(1, True)])
        losses = read_losses(sink)
        assert losses is not None and losses.dropped == 0 and losses.failed == 2
    finally:
        sink.close()


# --- the worker and orphan paths -----------------------------------------------------------


class AddsBytes:
    """A third-party wrapper that makes every event unencodable after assembly, as FR-002 needs.

    Assembly makes every event valid JSON (invariant 8), so the only routes to an unencodable
    event are a wrapper like this one and a direct ``emit``. It delegates ``losses()`` so
    ``health().sink`` reads the wrapped sink's counters.
    """

    def __init__(self, inner: object, *, only: str | None = None) -> None:
        self.inner = inner
        self.seen = 0
        self._only = only

    def emit(self, batch: list[dict[str, object]]) -> None:
        self.seen += len(batch)
        self.inner.emit(  # type: ignore[attr-defined]
            [
                e if self._only and e.get("message") != self._only else {**e, "blob": b"x"}
                for e in batch
            ]
        )

    def losses(self) -> SinkLosses | None:
        return read_losses(self.inner)

    def close(self) -> None:
        self.inner.close()  # type: ignore[attr-defined]


@pytest.mark.parametrize("name", ["PostgresSink", "KafkaSink", "GooglePubSubSink", "StdoutSink"])
def test_the_worker_sees_a_settled_batch(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """FR-002 AC2: a batch of nothing encodable settles on the worker path, counted once."""
    import log_foundry

    rig = RIGS[name](monkeypatch, tmp_path)
    wrapper = AddsBytes(rig.sink)
    log_foundry.configure(service="t", sink=wrapper)

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("one")
        log_foundry.info("two")

    work()
    assert log_foundry.flush()
    health = log_foundry.health()
    assert wrapper.seen > 0
    assert health.failed_batches == 0
    assert health.sink == SinkLosses(dropped=wrapper.seen, failed=0)
    assert rig.calls() == 0


def test_the_worker_abandon_line_is_the_only_count(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-001 AC3: when the remainder fails every attempt, the unencodable event is never counted.

    Every worker attempt raises, so every ledger is discarded; the worker's abandon line is the
    batch's only count, and no encode line is ever written.
    """
    import log_foundry

    rig = RIGS["KafkaSink"](monkeypatch, tmp_path)
    rig.state.down = True
    wrapper = AddsBytes(rig.sink, only="bad")
    log_foundry.configure(service="t", sink=wrapper)

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("bad")

    work()
    log_foundry.flush()
    health = log_foundry.health()
    err = capsys.readouterr().err
    assert health.failed_batches == 1
    assert rig.count() == 0
    assert _encode_lines(err) == []
    assert "batch abandoned after" in err


def test_the_orphan_path_settles_inside_the_sink(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """FR-002 AC3: outside a span the one unencodable event is ``dropped``, never ``orphan_lost``."""
    import log_foundry

    rig = RIGS["StdoutSink"](monkeypatch, tmp_path)
    log_foundry.configure(service="t", sink=AddsBytes(rig.sink))
    log_foundry.info("outside any span")
    health = log_foundry.health()
    assert health.orphan_lost == 0
    assert health.sink == SinkLosses(dropped=1, failed=0)
    assert rig.calls() == 0


BASE_PUBLIC: dict[str, frozenset[str]] = {
    "AzureEventHubsSink": frozenset(
        {
            "close",
            "dropped_oversized",
            "emit",
            "failed",
            "log_foundry_stop_signal",
            "losses",
            "max_retries",
            "producer",
        }
    ),
    "ClickHouseSink": frozenset(
        {"client", "close", "emit", "failed", "log_foundry_stop_signal", "losses", "max_retries"}
    ),
    "FileSink": frozenset({"close", "emit", "reacquire_after_fork"}),
    "FirehoseSink": frozenset(
        {
            "MAX_RECORDS",
            "MAX_RECORD_BYTES",
            "MAX_REQUEST_BYTES",
            "client",
            "close",
            "delivery_stream",
            "dropped_oversized",
            "dropped_unadjudicated",
            "emit",
            "failed",
            "log_foundry_stop_signal",
            "losses",
            "max_retries",
        }
    ),
    "GooglePubSubSink": frozenset(
        {
            "client",
            "close",
            "emit",
            "failed",
            "flush",
            "log_foundry_stop_signal",
            "losses",
            "max_pending",
            "overflow_timeout",
            "rejected",
            "topic",
        }
    ),
    "KafkaSink": frozenset(
        {
            "close",
            "emit",
            "failed",
            "flush",
            "flush_timeout",
            "key_field",
            "losses",
            "producer",
            "rejected",
            "topic",
        }
    ),
    "KinesisSink": frozenset(
        {
            "MAX_RECORDS",
            "MAX_RECORD_BYTES",
            "MAX_REQUEST_BYTES",
            "client",
            "close",
            "dropped_oversized",
            "dropped_unadjudicated",
            "emit",
            "failed",
            "log_foundry_stop_signal",
            "losses",
            "max_retries",
            "partition_key_field",
            "stream_name",
        }
    ),
    "LogstashSink": frozenset({"close", "emit", "failed", "log_foundry_stop_signal", "losses"}),
    "MongoDBSink": frozenset(
        {
            "close",
            "dropped_oversized",
            "emit",
            "failed",
            "log_foundry_stop_signal",
            "losses",
            "max_retries",
        }
    ),
    "PostgresSink": frozenset(
        {
            "close",
            "connect_timeout",
            "emit",
            "failed",
            "log_foundry_stop_signal",
            "losses",
            "max_retries",
        }
    ),
    "RabbitMQSink": frozenset({"close", "emit", "failed", "log_foundry_stop_signal", "losses"}),
    "RedisListSink": frozenset(
        {
            "client",
            "close",
            "emit",
            "failed",
            "log_foundry_stop_signal",
            "losses",
            "max_retries",
            "maxlen",
        }
    ),
    "RedisStreamsSink": frozenset(
        {
            "client",
            "close",
            "emit",
            "failed",
            "log_foundry_stop_signal",
            "losses",
            "max_retries",
            "maxlen",
        }
    ),
    "RotatingFileSink": frozenset({"close", "emit", "reacquire_after_fork"}),
    "SNSSink": frozenset(
        {
            "MAX_BATCH",
            "MAX_BYTES",
            "client",
            "close",
            "dropped_oversized",
            "emit",
            "failed",
            "log_foundry_stop_signal",
            "losses",
            "max_retries",
            "topic_arn",
        }
    ),
    "SQLiteSink": frozenset({"close", "emit"}),
    "SQSSink": frozenset(
        {
            "MAX_BATCH",
            "MAX_BYTES",
            "client",
            "close",
            "dropped_oversized",
            "emit",
            "failed",
            "fifo",
            "log_foundry_stop_signal",
            "losses",
            "max_retries",
            "message_deduplication_id",
            "message_group_id",
            "queue_url",
        }
    ),
    "SocketTransport": frozenset(
        {"close", "dropped_oversized", "failed", "log_foundry_stop_signal", "losses", "send_all"}
    ),
    "StderrSink": frozenset({"close", "emit"}),
    "StdoutSink": frozenset({"close", "emit"}),
    "SyslogSink": frozenset({"close", "emit", "failed", "log_foundry_stop_signal", "losses"}),
}
"""Each touched class's public names at ``ebf9d680`` (``origin/main`` before SPEC-069).

Captured by building each rig against that tree and reading ``dir(type(sink))`` with
``vars(sink)``, no leading underscore. A literal rather than a re-derivation, because the point is
to compare against the tree this spec started from.
"""

GAINED_LOSSES = frozenset(
    {"StdoutSink", "StderrSink", "FileSink", "RotatingFileSink", "SQLiteSink"}
)


def _public(obj: object) -> set[str]:
    names = set(dir(type(obj))) | set(vars(obj))
    return {n for n in names if not n.startswith("_")}


def test_the_public_surface_grows_only_by_five_losses(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """FR-004 AC11 / invariant 10: five classes gain ``losses``, every other touched class nothing.

    The counters, the encode functions, the helper and the driver-error class are private; a new
    public name on any of these classes is frozen for 1.x, so one appearing here is a finding.
    """
    seen: dict[str, set[str]] = {}
    for name, factory in RIGS.items():
        rig = factory(monkeypatch, tmp_path)
        seen[name] = _public(rig.sink)
        if name == "SyslogSink":
            seen["SocketTransport"] = _public(rig.sink._socket)
    assert set(seen) == set(BASE_PUBLIC)
    for name, names in seen.items():
        expected = BASE_PUBLIC[name] | ({"losses"} if name in GAINED_LOSSES else set())
        assert names == expected, (name, sorted(names ^ expected))


def test_a_default_process_logging_clean_events_reports_no_sink_losses(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """FR-001 AC8: the default ``StdoutSink`` gained ``losses()``, and it still reads ``None``."""
    import log_foundry

    log_foundry.info("outside a span")

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("inside")

    work()
    assert log_foundry.flush()
    assert log_foundry.health().sink is None
    assert '"message": "inside"' in capsys.readouterr().out


# --- FR-003: PostgreSQL's U+0000 is repaired, marked and delivered --------------------------

_PG_COLUMNS = ("timestamp", "level", "trace_id", "span_id", "function", "service")


def _pg_rows(rig: Any) -> list[tuple[object, ...]]:
    return [row for payload in rig.handoffs() for row in payload]


def _pg_documents(rig: Any) -> list[dict[str, Any]]:
    return [json.loads(row[-1]) for row in _pg_rows(rig)]


def test_postgres_repairs_a_nul_and_delivers_the_batch_in_one_attempt(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-003 AC1: three events, one transaction, one attempt; only the repaired one is marked."""
    rig = RIGS["PostgresSink"](monkeypatch, tmp_path)
    a, b = ok("a"), ok("b")
    nul = {"log_id": "n", "fields": {"s": "x\x00y"}}
    rig.sink.emit([a, nul, b])
    assert [kind for kind, _ in rig.state.log] == ["handoff"], "one executemany, one attempt"
    rows = _pg_rows(rig)
    assert [rows[0][-1], rows[2][-1]] == [json.dumps(a), json.dumps(b)]
    assert json.loads(rows[1][-1]) == {
        "log_id": "n",
        "fields": {"s": "x�y"},
        "truncated": True,
    }
    assert (rig.count(), rig.sink.failed) == (0, 0)
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize(
    ("label", "event", "where"),
    [
        ("key", {"log_id": "n", "fields": {"k\x00": 1}}, None),
        ("nested-list", {"log_id": "n", "fields": {"l": [1, ["a\x00"]]}}, None),
        *[(col, {"log_id": "n", col: "v\x00"}, col) for col in _PG_COLUMNS],
    ],
    ids=lambda v: v if isinstance(v, str) else "",
)
def test_postgres_repairs_a_nul_wherever_it_sits(
    label: str,
    event: dict[str, object],
    where: str | None,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """FR-003 AC2: a key, a nested list and each of the six text columns."""
    rig = RIGS["PostgresSink"](monkeypatch, tmp_path)
    rig.sink.emit([event])
    (row,) = _pg_rows(rig)
    document = json.loads(row[-1])
    assert document["truncated"] is True
    assert "\x00" not in json.dumps(document, ensure_ascii=False)
    if where is not None:
        assert row[_PG_COLUMNS.index(where)] == "v�"
        assert document[where] == "v�"


@pytest.mark.parametrize(
    "text", ["\\u0000", "\\\\u0000", "\x01"], ids=["six-chars", "backslash-six", "u0001"]
)
def test_postgres_leaves_lookalikes_unchanged(
    text: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """FR-003 AC4: no U+0000, no repair, no ``truncated`` key."""
    rig = RIGS["PostgresSink"](monkeypatch, tmp_path)
    event = {"log_id": "n", "fields": {"s": text}}
    rig.sink.emit([event])
    assert _pg_documents(rig) == [event]


def test_postgres_repair_leaves_the_sibling_the_original(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """FR-003 AC7: the repair never mutates the batch a ``MultiSink`` sibling also receives."""
    from log_foundry.sinks.memory import MemorySink
    from log_foundry.sinks.multi import MultiSink

    rig = RIGS["PostgresSink"](monkeypatch, tmp_path)
    memory = MemorySink()
    batch = [ok("a"), {"log_id": "n", "service": "s\x00x"}]
    before = copy.deepcopy(batch)
    MultiSink(rig.sink, memory).emit(batch)
    assert batch == before
    assert memory.events[1]["service"] == "s\x00x"
    assert _pg_documents(rig)[1]["service"] == "s�x"


def test_a_nul_service_stamp_is_repaired_on_every_event(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """FR-003 AC3: ``configure(service="s\\x00x")`` — every event of a traced call lands repaired."""
    import log_foundry

    rig = RIGS["PostgresSink"](monkeypatch, tmp_path)
    log_foundry.configure(service="s\x00x", sink=rig.sink)

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("inside")

    work()
    assert log_foundry.flush()
    rows = _pg_rows(rig)
    assert len(rows) >= 2
    for row in rows:
        assert row[_PG_COLUMNS.index("service")] == "s�x"
        document = json.loads(row[-1])
        assert (document["service"], document["truncated"]) == ("s�x", True)
    health = log_foundry.health()
    assert health.failed_batches == 0
    assert health.sink == SinkLosses(dropped=0, failed=0)


def test_the_orphan_path_delivers_a_repaired_event(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """FR-003 AC10: one U+0000 event outside a span is inserted repaired, nothing counted."""
    import log_foundry

    rig = RIGS["PostgresSink"](monkeypatch, tmp_path)
    log_foundry.configure(service="t", sink=rig.sink)
    log_foundry.info("outside", note="a\x00b")
    (document,) = _pg_documents(rig)
    assert document["fields"]["note"] == "a�b"
    assert document["truncated"] is True
    health = log_foundry.health()
    assert health.orphan_lost == 0
    assert health.sink == SinkLosses(dropped=0, failed=0)


# --- FR-003: MongoDB's BSON refusals are repaired, marked and delivered ---------------------


class InvalidDocument(Exception):
    """Stands in for ``bson.errors.InvalidDocument``."""


def _bson_like(value: object) -> None:
    """What ``bson.encode`` refuses, as the spec measured it: 64-bit overflow and a NUL key."""
    if isinstance(value, bool):
        return
    if isinstance(value, int) and not -(2**63) <= value <= 2**63 - 1:
        raise OverflowError("MongoDB can only handle up to 8-byte ints")
    if isinstance(value, dict):
        for key, item in value.items():
            if isinstance(key, str) and "\x00" in key:
                raise InvalidDocument("key must not contain '\\x00'")
            _bson_like(item)
    if isinstance(value, list):
        for item in value:
            _bson_like(item)


def _install_bson(monkeypatch: pytest.MonkeyPatch, encode: Any) -> None:
    module = types.ModuleType("bson")
    if encode is not None:
        module.encode = encode  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "bson", module)


def _measured_bson(document: object) -> bytes:
    _bson_like(document)
    return b""


@pytest.mark.parametrize(
    ("value", "replaced"),
    [(2**63, True), (-(2**63) - 1, True), (2**64, True), (2**63 - 1, False), (-(2**63), False)],
    ids=["2**63", "-2**63-1", "2**64", "2**63-1", "-2**63"],
)
def test_mongo_repairs_an_integer_bson_refuses(
    value: int, replaced: bool, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """FR-003 AC5: out of int64 becomes ``int_placeholder``'s text, marked; in range is left."""
    from log_foundry.sanitize import int_placeholder

    _install_bson(monkeypatch, _measured_bson)
    rig = RIGS["MongoDBSink"](monkeypatch, tmp_path)
    event = {"log_id": "n", "fields": {"n": value}}
    rig.sink.emit([event])
    (document,) = rig.handoffs()[0]
    if replaced:
        assert document == {
            "log_id": "n",
            "fields": {"n": int_placeholder(value)},
            "truncated": True,
        }
    else:
        assert document == event
    assert rig.count() == 0


def test_mongo_repairs_a_nul_key_and_leaves_a_nul_value(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """FR-003 AC6: a NUL key becomes U+FFFD, marked; a NUL value is untouched; collisions."""
    _install_bson(monkeypatch, _measured_bson)
    rig = RIGS["MongoDBSink"](monkeypatch, tmp_path)
    rig.sink.emit(
        [
            {"log_id": "k", "fields": {"k\x00": 1}},
            {"log_id": "v", "fields": {"k": "v\x00"}},
            {"log_id": "c", "fields": {"k\x00": 1, "k�": 2}},
        ]
    )
    assert rig.handoffs()[0] == [
        {"log_id": "k", "fields": {"k�": 1}, "truncated": True},
        {"log_id": "v", "fields": {"k": "v\x00"}},
        {"log_id": "c", "fields": {"k�": 2}, "truncated": True},
    ]


def test_a_bson_that_refuses_everything_drops_each_event_once(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-003 AC8: refused before and after the repair, each event is unencodable, counted once."""

    def refuse(document: object) -> bytes:
        raise InvalidDocument("refused")

    _install_bson(monkeypatch, refuse)
    rig = RIGS["MongoDBSink"](monkeypatch, tmp_path)
    rig.sink.emit([{"log_id": "a", "fields": {"n": 2**64}}, ok("b")])
    assert rig.calls() == 0
    assert rig.count() == 2
    assert _encode_lines(capsys.readouterr().err) == [
        "log-foundry: lost 2 event(s); MongoDBSink, encoding raised InvalidDocument"
    ]


@pytest.mark.parametrize("shape", ["missing", "no-encode"])
def test_mongo_without_a_usable_bson_inserts_everything(
    shape: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """FR-003 AC8: no ``bson``, or one without ``encode``, disables the check; nothing is imported
    at construction when a client is injected."""
    reads: list[str] = []

    class Spy(types.ModuleType):
        def __getattr__(self, name: str) -> object:
            reads.append(name)
            raise AttributeError(name)

    if shape == "missing":
        monkeypatch.setitem(sys.modules, "bson", None)
    else:
        monkeypatch.setitem(sys.modules, "bson", Spy("bson"))
    rig = RIGS["MongoDBSink"](monkeypatch, tmp_path)
    assert reads == [], "construction over an injected client reads nothing from bson"
    event = {"log_id": "n", "fields": {"n": 2**64, "k\x00": 1}}
    rig.sink.emit([event])
    assert rig.handoffs() == [[event]]
    assert rig.count() == 0
    if shape == "no-encode":
        assert "encode" in reads, "resolved on the first emit"


def test_an_unencodable_oversized_document_counts_once(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """FR-003 AC9: BSON-refused after repair and over 16 MB is ``_unencodable``, not oversize."""

    def refuse(document: object) -> bytes:
        raise InvalidDocument("refused")

    _install_bson(monkeypatch, refuse)
    rig = RIGS["MongoDBSink"](monkeypatch, tmp_path)
    rig.sink.emit([{"log_id": "big", "pad": "x" * (17 * 1024 * 1024), "n": 2**64}])
    assert (rig.sink._unencodable, rig.sink.dropped_oversized) == (1, 0)
