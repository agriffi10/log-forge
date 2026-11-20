"""SPEC-095 FR-002: each shipped sink marks the sends that may have reached its destination.

A ``RETRYABLE`` event is marked when its call may have landed: a raise unless the sink can show
the request never left, a ``5xx``, a Firehose coded record; not a reply that refuses the event.
Every raise from a client that retries inside the call — botocore, the Event Hubs producer,
redis-py, clickhouse-connect — is marked whatever its class. Each case below runs the real sink
through the engine, with one resend allowed, and asserts the run's ``resent`` and that a resend
happened at all, so a zero is the mark's doing and never the absence of a second round. The
roster at the end derives the engine sinks from SPEC-092's population and holds each to a
marking case or an ``EXEMPT`` entry with its reason. Serves invariants 2 and 7.
"""

from __future__ import annotations

import http.client
import threading
import urllib.error
from typing import TYPE_CHECKING, Any, Self

import pytest

import conformance_backend as cb
import conformance_classes as cc
import driver_standins as standins
import sink_rigs
from log_foundry.sinks import _engine
from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.postgres import PostgresSink
from log_foundry.sinks.verdict import SinkCounters
from test_http_rounds import Scripted, _ok, _status
from test_public_surface import _sink_classes_with_emit

if TYPE_CHECKING:
    import types
    from collections.abc import Callable
    from pathlib import Path


def _events(n: int) -> list[dict[str, object]]:
    return [{"log_id": f"e{i}", "level": "INFO", "message": f"m{i}"} for i in range(n)]


def run_once(sink: Any, n: int = 2, retries: int = 1) -> _engine.Delivery:
    """One engine run of ``sink.deliver``, its waits cut short, ``retries`` resends allowed.

    Args:
      sink: An engine sink.
      n: How many events.
      retries: The sink retries the run allows.

    Returns:
      The run's ``Delivery``.

    Raises:
      BaseException: Whatever the run raises.
    """
    stop = threading.Event()
    stop.set()
    return _engine._run(
        sink,
        sink.deliver,
        sink.log_foundry_counters,
        _events(n),
        sink_retries=retries,
        worker_retries=0,
        stop=stop,
        decline=None,
    )


def raise_once(rig: sink_rigs.Rig, error: BaseException) -> None:
    """Makes the rig's next client call raise ``error``, and the rest go through.

    Args:
      rig: The rig.
      error: What its first call raises.

    Returns:
      None.

    Raises:
      None.
    """
    state = rig.state
    original = state.call
    pending = [error]

    def call(payload: Any) -> None:
        if pending:
            state.log.append(("refused", None))
            raise pending.pop()
        original(payload)

    state.call = call  # type: ignore[method-assign]


def _rig(name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> sink_rigs.Rig:
    return sink_rigs.RIGS[name](monkeypatch, tmp_path)


# --- the AWS sinks: every retried raise; no entry a reply names ------------------------------


@pytest.mark.parametrize("name", ["SQSSink", "SNSSink", "KinesisSink", "FirehoseSink"])
def test_an_aws_endpoint_connection_error_counts_the_calls_events(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """AC1 and AC2: botocore's ``EndpointConnectionError`` may follow an attempt that landed."""
    botocore = standins.install(monkeypatch, standins.BOTOCORE)
    rig = _rig(name, monkeypatch, tmp_path)
    raise_once(rig, botocore.EndpointConnectionError("endpoint"))
    delivery = run_once(rig.sink, n=3)
    assert delivery.attempts == 2 and not delivery.failed
    assert delivery.resent == 3
    assert rig.sink.losses().resent == 3


class _Replies:
    """A client of one AWS sink whose first reply names its first entry failed, then all land."""

    def __init__(self, first: dict[str, Any]) -> None:
        self.first: dict[str, Any] | None = first
        self.calls = 0

    def _reply(self, entries: list[dict[str, Any]], landed: Callable[..., Any]) -> dict[str, Any]:
        self.calls += 1
        reply, self.first = self.first, None
        if reply is not None:
            return reply(entries) if callable(reply) else reply
        return landed(entries)

    def send_message_batch(self, *, QueueUrl: str, Entries: list[dict[str, Any]]) -> Any:
        return self._reply(
            Entries, lambda es: {"Successful": [{"Id": e["Id"]} for e in es], "Failed": []}
        )

    def publish_batch(self, *, TopicArn: str, PublishBatchRequestEntries: list[Any]) -> Any:
        return self._reply(
            PublishBatchRequestEntries,
            lambda es: {"Successful": [{"Id": e["Id"]} for e in es], "Failed": []},
        )

    def put_records(self, *, StreamName: str, Records: list[Any]) -> Any:
        return self._reply(
            Records,
            lambda rs: {"FailedRecordCount": 0, "Records": [{"SequenceNumber": "1"} for _ in rs]},
        )

    def put_record_batch(self, *, DeliveryStreamName: str, Records: list[Any]) -> Any:
        return self._reply(
            Records,
            lambda rs: {"FailedPutCount": 0, "RequestResponses": [{"RecordId": "r"} for _ in rs]},
        )


def _keyed_first_failed(entries: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "Successful": [{"Id": e["Id"]} for e in entries[1:]],
        "Failed": [{"Id": entries[0]["Id"], "Code": "InternalError", "SenderFault": False}],
    }


def _coded(results_key: str, count_key: str, code: str) -> Callable[[list[Any]], Any]:
    def reply(records: list[Any]) -> dict[str, Any]:
        landed: dict[str, Any] = (
            {"SequenceNumber": "1"} if results_key == "Records" else {"RecordId": "r"}
        )
        return {
            count_key: 1,
            results_key: [{"ErrorCode": code, "ErrorMessage": "m"}] + [landed] * (len(records) - 1),
        }

    return reply


def _aws(name: str, first: Any) -> tuple[Any, _Replies]:
    from log_foundry.sinks.firehose import FirehoseSink
    from log_foundry.sinks.kinesis import KinesisSink
    from log_foundry.sinks.sns import SNSSink
    from log_foundry.sinks.sqs import SQSSink

    client = _Replies(first)
    build: dict[str, Callable[[], Any]] = {
        "SQSSink": lambda: SQSSink("https://sqs/q", client=client),
        "SNSSink": lambda: SNSSink("arn", client=client),
        "KinesisSink": lambda: KinesisSink("s", client=client),
        "FirehoseSink": lambda: FirehoseSink("d", client=client),
    }
    return build[name](), client


@pytest.mark.parametrize("name", ["SQSSink", "SNSSink"])
def test_an_entry_the_reply_names_failed_is_not_marked(name: str) -> None:
    """AC1: the ``Failed`` list is the service's word on the attempt it answers."""
    sink, client = _aws(name, _keyed_first_failed)
    delivery = run_once(sink, n=3)
    assert client.calls == 2 and delivery.attempts == 2
    assert delivery.resent == 0


def test_a_firehose_internal_failure_is_counted() -> None:
    """AC3: AWS says a record coded ``InternalFailure`` might have completed."""
    sink, client = _aws(
        "FirehoseSink", _coded("RequestResponses", "FailedPutCount", "InternalFailure")
    )
    delivery = run_once(sink, n=3)
    assert client.calls == 2 and delivery.attempts == 2
    assert delivery.resent == 1


def test_a_firehose_service_unavailable_record_is_counted() -> None:
    """AC1: Firehose's other code is marked too; Firehose's "Not marked" column is empty."""
    sink, client = _aws(
        "FirehoseSink",
        _coded("RequestResponses", "FailedPutCount", "ServiceUnavailableException"),
    )
    delivery = run_once(sink, n=3)
    assert client.calls == 2 and delivery.resent == 1


def test_a_kinesis_throughput_code_is_not_counted() -> None:
    """AC3: a Kinesis coded record is the service's refusal of it."""
    sink, client = _aws(
        "KinesisSink",
        _coded("Records", "FailedRecordCount", "ProvisionedThroughputExceededException"),
    )
    delivery = run_once(sink, n=3)
    assert client.calls == 2 and delivery.attempts == 2
    assert delivery.resent == 0


# --- the HTTP family and _send --------------------------------------------------------------

_HTTP_CASES: dict[str, tuple[Any, int]] = {
    "timeout": (TimeoutError("read timed out"), 2),
    "remote-disconnected": (http.client.RemoteDisconnected("no status line"), 2),
    "status-503": (_status(503), 2),
    "status-502": (_status(502), 2),
    "url-error": (urllib.error.URLError(OSError("no route")), 0),
    "connection-refused": (ConnectionRefusedError(61, "refused"), 0),
    "status-429": (_status(429), 0),
}


@pytest.mark.parametrize("case", sorted(_HTTP_CASES))
def test_the_http_round_marks_what_may_have_landed(case: str) -> None:
    """AC1 and AC4: a timeout, a lost status line and a ``5xx`` are marked; the rest are not."""
    first, resent = _HTTP_CASES[case]
    opener = Scripted(first, _ok())
    sink = HTTPSink("http://h", max_retries=1, opener=opener)
    delivery = run_once(sink, n=2)
    assert len(opener.calls) == 2 and delivery.attempts == 2 and not delivery.failed
    assert delivery.resent == resent
    assert sink.losses().resent == resent


@pytest.mark.parametrize("case", sorted(_HTTP_CASES))
def test_send_marks_what_may_have_landed(case: str) -> None:
    """AC1: ``_send``'s rounds are classified by ``_exchange`` too, one request per round."""
    first, resent = _HTTP_CASES[case]
    opener = Scripted(first, _ok())
    sink = HTTPSink("http://h", max_retries=1, opener=opener)
    sink._send(b"x", content_type="text/plain")
    assert len(opener.calls) == 2
    assert sink.log_foundry_counters.resent_events == min(resent, 1)


# --- the TCP and UDP socket transports ------------------------------------------------------


class _Wire:
    """A socket whose first ``sendall`` raises ``error``; the rest go through."""

    def __init__(self, error: BaseException | None) -> None:
        self.error = error
        self.sent: list[bytes] = []

    def sendall(self, data: bytes) -> None:
        if self.error is not None:
            error, self.error = self.error, None
            raise error
        self.sent.append(data)

    def sendto(self, data: bytes, address: object) -> None:
        self.sendall(data)

    def settimeout(self, value: float | None) -> None:
        pass

    def close(self) -> None:
        pass


def _syslog(monkeypatch: pytest.MonkeyPatch, transport: str, opener: Callable[..., Any]) -> Any:
    import log_foundry.sinks._socket as socket_mod
    from log_foundry.sinks.syslog import SyslogSink

    name = "_make_tcp" if transport == "tcp" else "_make_udp"
    monkeypatch.setattr(socket_mod, name, opener)
    return SyslogSink("loghost", 514, transport=transport, max_retries=1)


def _opens(wires: list[_Wire | BaseException]) -> Callable[..., Any]:
    def make(*args: Any) -> _Wire:
        item = wires.pop(0) if len(wires) > 1 else wires[0]
        if isinstance(item, BaseException):
            raise item
        return item

    return make


@pytest.mark.parametrize(
    ("case", "resent"),
    [("timeout", 1), ("broken-pipe", 1), ("refused", 0)],
)
def test_a_tcp_frame_whose_sendall_raised_is_marked_unless_refused(
    case: str, resent: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC1: a ``sendall`` that raised may have written the frame, unless it was refused."""
    error: BaseException = {
        "timeout": TimeoutError("timed out"),
        "broken-pipe": BrokenPipeError(32, "broken pipe"),
        "refused": ConnectionRefusedError(61, "refused"),
    }[case]
    sink = _syslog(monkeypatch, "tcp", _opens([_Wire(error), _Wire(None)]))
    delivery = run_once(sink, n=1)
    assert delivery.attempts == 2 and not delivery.failed
    assert delivery.resent == resent


def test_a_tcp_frame_whose_connection_could_not_open_is_not_marked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AC1: nothing left when the connection itself was never made, whatever the error."""
    sink = _syslog(monkeypatch, "tcp", _opens([TimeoutError("connect timed out"), _Wire(None)]))
    delivery = run_once(sink, n=1)
    assert delivery.attempts == 2 and not delivery.failed
    assert delivery.resent == 0


def test_a_udp_datagram_is_never_marked(monkeypatch: pytest.MonkeyPatch) -> None:
    """AC1: a datagram whose ``sendto`` raised was not sent."""
    sink = _syslog(monkeypatch, "udp", _opens([_Wire(TimeoutError("timed out"))]))
    delivery = run_once(sink, n=1)
    assert delivery.attempts == 2 and not delivery.failed
    assert delivery.resent == 0


# --- RabbitMQ -------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raised", "resent"),
    [
        ("StreamLostError", 1),
        ("ConnectionClosedByBroker", 1),
        ("AMQPHeartbeatTimeout", 1),
        ("ConnectionBlockedTimeout", 1),
        ("builtin-timeout", 1),
        ("ChannelWrongStateError", 0),
        ("ConnectionWrongStateError", 0),
        ("AMQPConnectionError", 0),
        ("builtin-refused", 0),
    ],
)
def test_rabbitmq_marks_an_unconfirmed_publish_by_class(
    raised: str, resent: int, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """AC1: pika does not retry inside a publish, so the class decides."""
    pika = standins.install(monkeypatch, standins.PIKA)
    error = {
        "builtin-timeout": TimeoutError("timed out"),
        "builtin-refused": ConnectionRefusedError(61, "refused"),
    }.get(raised) or getattr(pika, raised)("x")
    rig = _rig("RabbitMQSink", monkeypatch, tmp_path)
    raise_once(rig, error)
    delivery = run_once(rig.sink, n=1)
    assert delivery.attempts == 2 and not delivery.failed, raised
    assert delivery.resent == resent


# --- PostgreSQL -----------------------------------------------------------------------------


class _Cursor:
    def __init__(self, connection: _PG) -> None:
        self.connection = connection

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        pass

    def executemany(self, sql: str, rows: list[Any]) -> None:
        self.connection.step("executemany")


class _PG:
    """A psycopg-shaped connection that raises ``error`` once, at ``stage``."""

    def __init__(self, stage: str, error: BaseException, *, autocommit: bool = False) -> None:
        self.stage = stage
        self.error: BaseException | None = error
        self.autocommit = autocommit
        self.closed = False
        self.broken = False
        self.commits = 0

    def step(self, where: str) -> None:
        if where == self.stage and self.error is not None:
            error, self.error = self.error, None
            raise error

    def cursor(self) -> _Cursor:
        self.step("cursor")
        return _Cursor(self)

    def commit(self) -> None:
        self.commits += 1
        self.step("commit")

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True


def _pg_error(module: types.ModuleType, sqlstate: str | None) -> BaseException:
    cls = module.InternalError if sqlstate == "XX000" else module.OperationalError
    return cls("server said no", sqlstate=sqlstate)  # type: ignore[no-any-return]


@pytest.mark.parametrize(
    ("stage", "sqlstate", "autocommit", "resent"),
    [
        ("executemany", "08006", False, 0),
        ("cursor", "08006", False, 0),
        ("commit", "08006", False, 2),
        ("commit", "57P01", False, 2),
        ("commit", None, False, 2),
        ("commit", "40001", False, 0),
        ("executemany", "08006", True, 2),
        ("executemany", "40001", True, 0),
    ],
)
def test_postgres_marks_by_where_and_sqlstate(
    stage: str,
    sqlstate: str | None,
    autocommit: bool,
    resent: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AC1 and AC5: only a raise at ``commit()``, or under autocommit, in 08, 57, XX or none."""
    psycopg = standins.install(monkeypatch, standins.PSYCOPG)
    connection = _PG(stage, _pg_error(psycopg, sqlstate), autocommit=autocommit)
    sink = PostgresSink("t", connection=connection, max_retries=1)
    delivery = run_once(sink, n=2)
    assert delivery.attempts == 2 and not delivery.failed
    assert delivery.resent == resent


def test_postgres_xx000_at_commit_is_classified_landed_but_never_resent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AC5: class ``XX`` is not retried, so the round is ``REJECTED`` and counts no resend."""
    psycopg = standins.install(monkeypatch, standins.PSYCOPG)
    connection = _PG("commit", _pg_error(psycopg, "XX000"))
    sink = PostgresSink("t", connection=connection, max_retries=1)
    called = sink._insert_round([("a",)])
    assert called.error is not None and called.reached is True and not called.retryable
    connection.error = _pg_error(psycopg, "XX000")
    delivery = run_once(sink, n=2)
    assert delivery.attempts == 1 and delivery.failed
    assert delivery.resent == 0


# --- Event Hubs, ClickHouse and the Redis sinks: every retried raise -------------------------


@pytest.mark.parametrize(
    ("name", "module", "raised"),
    [
        ("AzureEventHubsSink", standins.AZURE, "ConnectError"),
        ("AzureEventHubsSink", standins.AZURE, "ConnectionLostError"),
        ("ClickHouseSink", standins.CLICKHOUSE, "OperationalError"),
        ("RedisStreamsSink", standins.REDIS, "ConnectionError"),
        ("RedisListSink", standins.REDIS, "ConnectionError"),
    ],
)
def test_a_client_that_retries_inside_the_call_marks_every_raise(
    name: str, module: str, raised: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """AC1 and AC2: the client's final exception can follow an attempt that landed."""
    stand = standins.install(monkeypatch, module)
    rig = _rig(name, monkeypatch, tmp_path)
    raise_once(rig, getattr(stand, raised)("x"))
    delivery = run_once(rig.sink, n=1)
    assert delivery.attempts == 2 and not delivery.failed
    assert delivery.resent == 1


# --- AC6: the roster ------------------------------------------------------------------------

EXEMPT: dict[str, str] = {
    "MongoDBSink": "its _id refuses a second copy, read as delivery (FR-003)",
    "FileSink": "a line the file took is never written again (SPEC-082)",
    "RotatingFileSink": "a line the file took is never written again (SPEC-082)",
    "StdoutSink": "a handed line is never handed again (SPEC-084)",
    "StderrSink": "a handed line is never handed again (SPEC-084)",
    "NATSSink": "its resend is the Nats-Msg-Id item of §12, out of SPEC-095's scope",
}
"""The engine sinks that never mark, each with its reason."""

NEVER_RESENT = frozenset({"StdoutSink", "StderrSink"})
"""The exempt sinks that never offer an event twice at all, which their quiet case asserts."""


def _marking(name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> int:
    """The resends one marking case of ``name`` counts."""
    if name == "PostgresSink":
        psycopg = standins.install(monkeypatch, standins.PSYCOPG)
        sink = PostgresSink("t", connection=_PG("commit", _pg_error(psycopg, "08006")))
        return run_once(sink, n=1).resent
    if name in sink_rigs.RIGS and name != "SyslogSink":
        rig = _rig(name, monkeypatch, tmp_path)
        raise_once(rig, TimeoutError("timed out"))
        return run_once(rig.sink, n=1).resent
    if name == "SyslogSink":
        sink = _syslog(monkeypatch, "tcp", _opens([_Wire(TimeoutError("t")), _Wire(None)]))
        return run_once(sink, n=1).resent
    sink = cb_engine_sink(name, monkeypatch)
    return run_once(sink, n=1).resent


def cb_engine_sink(name: str, monkeypatch: pytest.MonkeyPatch) -> Any:
    """An HTTP-family sink over an opener whose first request times out."""
    import conformance_doubles as cd

    variant = next(v for v in cc.VARIANTS if v.sink == name)
    cd.install_drivers(monkeypatch)
    sink, _ = cd.build(cc.Row(variant, "clean"), cb.Backend("clean"), monkeypatch)
    opener = Scripted(TimeoutError("timed out"), _ok())
    sink._opener = opener
    return sink


def _quiet(name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> list[object]:
    """The marks an exempt sink registers over its corpus rows that resend.

    Each of the sink's variants runs every fault SPEC-092 gives it under a spy on
    ``RunContext.may_have_landed``; at least one row must have offered an event twice, so the
    silence is about a resend and not the lack of one.

    Args:
      name: The sink.
      monkeypatch: Where the spy is set.
      tmp_path: Unused; the corpus builds its own doubles.

    Returns:
      Every index marked.

    Raises:
      None.
    """
    seen: list[object] = []
    original = _engine.RunContext.may_have_landed

    def spy(self: _engine.RunContext, indices: Any) -> None:
        marked = list(indices)
        seen.extend(marked)
        original(self, marked)

    monkeypatch.setattr(_engine.RunContext, "may_have_landed", spy)
    resent_any = False
    for variant in (v for v in cc.VARIANTS if v.sink == name):
        for fault in cc.faults_of(variant):
            result = cb.run_row(cc.Row(variant, fault))
            assert not result.report.resent and not result.report.resent_counted, (name, fault)
            calls = [call.carried for call in result.backend.calls]
            tokens = [token for carried in calls for token in carried]
            resent_any = resent_any or len(tokens) > len(set(tokens))
    if name in NEVER_RESENT:
        assert not resent_any, f"{name} resent a line its exemption says it never hands twice"
    else:
        assert resent_any, f"{name}'s corpus rows resent nothing, so its silence proves nothing"
    return seen


def engine_sinks() -> set[str]:
    """SPEC-092's population, less the abstract bases, whose class offers ``deliver`` by its MRO.

    Wrappers forward their inner sink's marks rather than marking (``RunContext.delegate``), so
    the three SPEC-075 wrappers and ``MultiSink`` are not engine sinks here.

    Args:
      None.

    Returns:
      The class names.

    Raises:
      None.
    """
    import importlib

    names = {cls.name for _, cls in _sink_classes_with_emit()} - {"Sink"} - set(cc.ABSTRACT)
    modules = {cls.name: module for module, cls in _sink_classes_with_emit()}
    wrappers = {"FilteringSink", "TransformSink", "LogstashSink", "MultiSink", "_FactorySink"}
    found: set[str] = set()
    for name in names - wrappers:
        cls = getattr(importlib.import_module(f"log_foundry.sinks.{modules[name]}"), name)
        if callable(getattr(cls, "deliver", None)):
            found.add(name)
    return found


def roster_findings(
    population: set[str], exempt: dict[str, str], marks: Callable[[str], bool]
) -> list[str]:
    """One line per engine sink the roster does not account for, or accounts for wrongly.

    Args:
      population: The engine sinks.
      exempt: The exempt sinks with their reasons.
      marks: Whether a sink's marking case counted a resend.

    Returns:
      The findings, empty when every exempt sink is quiet and every other one marks.

    Raises:
      None.
    """
    findings: list[str] = []
    for name in sorted(population):
        if name in exempt:
            if not exempt[name]:
                findings.append(f"{name} is EXEMPT with no reason")
            if marks(name):
                findings.append(f"{name} is EXEMPT and marks in its test")
        elif not marks(name):
            findings.append(f"{name} is neither EXEMPT nor marking in this FR's tests")
    findings.extend(
        f"{name} is EXEMPT but is no engine sink" for name in sorted(set(exempt) - population)
    )
    return findings


def test_every_engine_sink_marks_or_is_exempt(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """AC6: each derived engine sink marks in a case here, or is ``EXEMPT`` and never marks."""
    population = engine_sinks()
    assert {"HTTPSink", "OpenSearchSink", "StderrSink", "SQSSink"} <= population
    assert not {"SinkCounters", "SocketTransport", "_RedisSink"} & population

    def marks(name: str) -> bool:
        with pytest.MonkeyPatch.context() as patch:
            if name in EXEMPT:
                return bool(_quiet(name, patch, tmp_path))
            return _marking(name, patch, tmp_path) > 0

    assert roster_findings(population, EXEMPT, marks) == []


@pytest.mark.parametrize(
    ("population", "exempt", "marking", "findings"),
    [
        ({"A", "B"}, {"B": "why"}, {"A"}, []),
        ({"A", "B"}, {"B": "why"}, {"A", "B"}, ["B is EXEMPT and marks in its test"]),
        ({"A", "B"}, {"B": "why"}, set(), ["A is neither EXEMPT nor marking in this FR's tests"]),
        ({"A"}, {"A": ""}, set(), ["A is EXEMPT with no reason"]),
        ({"A"}, {"A": "why", "Gone": "why"}, set(), ["Gone is EXEMPT but is no engine sink"]),
    ],
    ids=["silent", "exempt-marks", "unaccounted", "no-reason", "stale-exemption"],
)
def test_the_roster_names_each_fault_and_only_those(
    population: set[str], exempt: dict[str, str], marking: set[str], findings: list[str]
) -> None:
    """AC6: the roster's reasons, and its silence."""
    assert roster_findings(population, exempt, lambda name: name in marking) == findings


def test_may_have_landed_clears_only_a_request_that_never_left() -> None:
    """AC1: the shared classes; a ``ConnectionRefusedError`` subclass is still a refusal."""
    from log_foundry.sinks._classify import may_have_landed

    class StillRefused(ConnectionRefusedError):
        pass

    gaierror = __import__("socket").gaierror
    assert not may_have_landed(ConnectionRefusedError())
    assert not may_have_landed(StillRefused())
    assert not may_have_landed(gaierror())
    assert not may_have_landed(urllib.error.URLError("x"))
    http_error = urllib.error.HTTPError("u", 503, "m", None, None)  # type: ignore[arg-type]
    assert may_have_landed(http_error)
    assert may_have_landed(TimeoutError())
    assert may_have_landed(http.client.RemoteDisconnected())
    assert may_have_landed(ConnectionResetError())
    assert may_have_landed(ValueError())


def test_counters_hold_the_mark_types() -> None:
    """The counters' new field starts at zero."""
    assert SinkCounters().resent_events == 0
