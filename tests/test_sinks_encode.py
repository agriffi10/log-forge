"""SPEC-069 FR-001 to FR-003 — every record-building sink, run over the derived roster.

The rigs live in ``tests/sink_rigs.py``; their table is held equal to the classes the roster lint
in ``tests/test_encode.py`` derives from the tree, so a sink cannot join the helper untested.
"""

from __future__ import annotations

import os
import socket
from typing import TYPE_CHECKING

import pytest

from sink_rigs import EVENTS, RIGS
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
