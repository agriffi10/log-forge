"""SPEC-069 FR-001 to FR-003 — every record-building sink, run over the derived roster.

The rigs live in ``tests/sink_rigs.py``; their table is held equal to the classes the roster lint
in ``tests/test_encode.py`` derives from the tree, so a sink cannot join the helper untested.
"""

from __future__ import annotations

import os
import socket
from typing import TYPE_CHECKING

import pytest

from log_foundry.sinks.base import SinkDeliveryError, SinkLosses, read_losses
from sink_rigs import EIGHT, EVENTS, RIGS, ok, unencodable
from test_encode import NOT_YET_CONVERTED, on_helper_public

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

CONVERTED = sorted(set(RIGS) - NOT_YET_CONVERTED)
_LINE = "encoding raised"


def _encode_lines(err: str) -> list[str]:
    return [line for line in err.splitlines() if _LINE in line]


@pytest.mark.parametrize("name", CONVERTED)
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
@pytest.mark.parametrize("name", [n for n in EIGHT if n in CONVERTED])
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


@pytest.mark.parametrize("name", CONVERTED)
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


@pytest.mark.parametrize("name", CONVERTED)
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


@pytest.mark.parametrize("name", CONVERTED)
def test_a_batch_of_nothing_encodable_settles(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-002 AC1: no client call, every event counted in ``dropped``, one line, a normal return."""
    rig = RIGS[name](monkeypatch, tmp_path)
    rig.sink.emit([unencodable("x"), unencodable("y"), unencodable("z")])
    assert rig.calls() == 0
    assert rig.count() == 3
    assert read_losses(rig.sink) == SinkLosses(dropped=3, failed=0)
    assert len(_encode_lines(capsys.readouterr().err)) == 1


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


@pytest.mark.parametrize("name", [n for n in CLOSABLE if n in CONVERTED])
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


@pytest.mark.parametrize("name", [n for n in NEW_LOSSES if n in CONVERTED])
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
