"""SPEC-041 FR-001 — PostgresSink against a real Postgres: the round trip and the projection."""

from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING

import psycopg
import pytest

from log_foundry.sinks.base import SinkDeliveryError
from log_foundry.sinks.postgres import PostgresSink

if TYPE_CHECKING:
    from collections.abc import Iterator

    from integration.conftest import Endpoint


def dsn(endpoint: Endpoint) -> str:
    return f"postgresql://postgres:logfoundry@{endpoint.url_host}/logs"


def event(n: int) -> dict[str, object]:
    return {
        "timestamp": "2026-08-30T12:00:00+00:00",
        "level": "INFO",
        "trace_id": "a" * 32,
        "span_id": "b" * 16,
        "function": "handler",
        "service": "integration",
        "message": f"event {n}",
        "n": n,
    }


@pytest.fixture
def table(services_are_up: dict[str, Endpoint]) -> Iterator[str]:
    name = f"lf_{uuid.uuid4().hex[:12]}"
    yield name
    with psycopg.connect(dsn(services_are_up["postgres"])) as conn, conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {name}")
        conn.commit()


def test_a_batch_lands_as_rows_with_the_whole_event_in_jsonb(
    services_are_up: dict[str, Endpoint], table: str
) -> None:
    endpoint = services_are_up["postgres"]
    sink = PostgresSink(table, dsn=dsn(endpoint), create_table=True)
    sink.emit([event(1), event(2), event(3)])
    sink.close()

    with psycopg.connect(dsn(endpoint)) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT trace_id, function, service, event FROM {table} ORDER BY id")
        rows = cur.fetchall()

    assert len(rows) == 3
    assert [row[0] for row in rows] == ["a" * 32] * 3
    assert [row[1] for row in rows] == ["handler"] * 3
    assert [row[2] for row in rows] == ["integration"] * 3
    # The JSONB column carries the whole event, not only the projected columns.
    assert [json.loads(json.dumps(row[3]))["n"] for row in rows] == [1, 2, 3]


def test_a_batch_larger_than_one_chunk_lands_whole(
    services_are_up: dict[str, Endpoint], table: str
) -> None:
    endpoint = services_are_up["postgres"]
    sink = PostgresSink(table, dsn=dsn(endpoint), create_table=True, chunk_size=7)
    sink.emit([event(n) for n in range(50)])
    sink.close()

    with psycopg.connect(dsn(endpoint)) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table}")
        assert cur.fetchone()[0] == 50


def terminate(endpoint: Endpoint, pid: int) -> None:
    with psycopg.connect(dsn(endpoint)) as admin, admin.cursor() as cur:
        cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
        admin.commit()


def test_delivery_resumes_after_the_server_closes_an_owned_connection(
    services_are_up: dict[str, Endpoint], table: str
) -> None:
    # FR-002 AC-4: the proof that needs a real server. A psycopg connection is permanently
    # unusable once the backend goes away, so before this fix one failover ended delivery for
    # the life of the process -- measured here as three lost batches and one row.
    endpoint = services_are_up["postgres"]
    sink = PostgresSink(table, dsn=dsn(endpoint), create_table=True, max_retries=0)
    sink.emit([event(1)])

    terminate(endpoint, sink._conn.info.backend_pid)

    delivered = 0
    for n in range(2, 5):
        try:
            sink.emit([event(n)])
            delivered += 1
        except SinkDeliveryError:
            pass
    sink.close()

    # max_retries=0 is deliberate: the reconnect sits at the top of each ATTEMPT, so it must
    # recover on the next emit even when no retry remains -- the setting at which a reconnect
    # placed in the retry branch would never run at all.
    assert delivered >= 2, "delivery must resume after the connection is replaced"
    with psycopg.connect(dsn(endpoint)) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table}")
        assert cur.fetchone()[0] >= 3


def test_a_borrowed_connection_is_left_to_its_owner(
    services_are_up: dict[str, Endpoint], table: str
) -> None:
    # arch §13's borrowed-client constraint, against a real server: the sink must not reconnect
    # an object the caller owns, so delivery stays broken and the caller's handle is untouched.
    endpoint = services_are_up["postgres"]
    borrowed = psycopg.connect(dsn(endpoint))
    sink = PostgresSink(table, connection=borrowed, create_table=True, max_retries=0)
    sink.emit([event(1)])

    terminate(endpoint, borrowed.info.backend_pid)

    with pytest.raises(SinkDeliveryError):
        sink.emit([event(2)])
    assert sink._conn is borrowed


def test_a_nul_is_repaired_and_the_whole_batch_lands(
    services_are_up: dict[str, Endpoint], table: str
) -> None:
    """SPEC-069 FR-003: one U+0000 no longer rolls back the batch; three rows land, one repaired.

    The real server is what refuses U+0000 in ``jsonb`` and psycopg what refuses it in a text
    column, so this settles what the unit tests' double only models.
    """
    endpoint = services_are_up["postgres"]
    sink = PostgresSink(table, dsn=dsn(endpoint), create_table=True, max_retries=0)
    nul = {**event(2), "service": "s\x00x", "fields": {"k\x00": "a\x00b"}}
    lookalike = {**event(3), "fields": {"text": "\\u0000"}}
    sink.emit([event(1), nul, lookalike])
    assert sink.losses().failed == 0
    sink.close()

    with psycopg.connect(dsn(endpoint)) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT service, event FROM {table} ORDER BY id")
        rows = cur.fetchall()

    assert len(rows) == 3
    assert rows[1][0] == "s�x"
    assert rows[1][1]["fields"] == {"k�": "a�b"}
    assert rows[1][1]["truncated"] is True
    assert "truncated" not in rows[0][1]
    assert rows[2][1]["fields"] == {"text": "\\u0000"}
    assert "truncated" not in rows[2][1]


def test_a_lone_surrogate_costs_only_its_event_against_a_real_server(
    services_are_up: dict[str, Endpoint], table: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """SPEC-069 follow-up: a direct ``emit``'s lone surrogate no longer rolls back the batch.

    Measured against PostgreSQL 17.11 before the fix: one in a JSON value or key cost the batch
    (JSONB refuses its ``\\ud800`` escape), and one in a text column too (psycopg cannot encode
    it). An emoji, written as an escaped surrogate *pair*, must still land, and so must the same
    pair held as two code points, which ``json.dumps`` writes as the emoji's two escapes.
    """
    endpoint = services_are_up["postgres"]
    sink = PostgresSink(table, dsn=dsn(endpoint), create_table=True, max_retries=0)
    emoji = {**event(4), "fields": {"text": "smile \U0001f600"}}
    sink.emit(
        [
            event(1),
            {**event(2), "fields": {"k\ud800": "v"}},
            {**event(3), "level": "x\udc80"},
            emoji,
            {**event(5), "fields": {"text": "\ud83d\ude00"}},
        ]
    )
    losses = sink.losses()
    sink.close()

    with psycopg.connect(dsn(endpoint)) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT event FROM {table} ORDER BY id")
        rows = cur.fetchall()

    assert [row[0]["n"] for row in rows] == [1, 4, 5]
    assert rows[1][0]["fields"] == {"text": "smile \U0001f600"}
    assert rows[2][0]["fields"] == {"text": "\U0001f600"}
    assert (losses.dropped, losses.failed) == (2, 0)
    assert "PostgresSink, encoding raised UnicodeEncodeError" in capsys.readouterr().err


def test_a_container_in_a_column_lands_as_its_json_text_against_a_real_server(
    services_are_up: dict[str, Endpoint], table: str
) -> None:
    """SPEC-069 follow-up: psycopg cannot adapt a ``dict`` or a mixed list; each lands as JSON."""
    endpoint = services_are_up["postgres"]
    sink = PostgresSink(table, dsn=dsn(endpoint), create_table=True, max_retries=0)
    sink.emit([{**event(1), "level": {"x": 1}}, {**event(2), "service": [1, "a"]}, event(3)])
    assert sink.losses().failed == 0
    sink.close()

    with psycopg.connect(dsn(endpoint)) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT level, service FROM {table} ORDER BY id")
        rows = cur.fetchall()

    assert rows == [('{"x": 1}', "integration"), ("INFO", '[1, "a"]'), ("INFO", "integration")]


def test_a_permanent_refusal_is_one_attempt_against_a_real_server(
    services_are_up: dict[str, Endpoint], table: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """SPEC-070 FR-002 AC-6: a caller's ``VARCHAR(5)`` refuses ``warning``, and once is enough.

    The insert raises ``StringDataRightTruncation`` (SQLSTATE ``22001``, a ``DataError``), which a
    resend of the same rows meets again. Before SPEC-070 it was retried: ``2 attempts`` at
    ``max_retries=1``. A one-event ``info`` batch through the same table lands, the positive
    control that the schema, not the sink, is what refuses.
    """
    endpoint = services_are_up["postgres"]
    with psycopg.connect(dsn(endpoint)) as conn, conn.cursor() as cur:
        cur.execute(
            f"CREATE TABLE {table} (id BIGSERIAL PRIMARY KEY, timestamp TEXT, level VARCHAR(5), "
            "trace_id TEXT, span_id TEXT, function TEXT, service TEXT, event JSONB NOT NULL)"
        )
        conn.commit()
    sink = PostgresSink(table, dsn=dsn(endpoint), create_table=False, max_retries=1)

    with pytest.raises(SinkDeliveryError):
        sink.emit([{**event(1), "level": "info"}, {**event(2), "level": "warning"}])
    assert sink.losses().failed == 2
    err = capsys.readouterr().err
    assert "lost 2 event(s); PostgresSink, 1 attempt(s), StringDataRightTruncation" in err

    sink.emit([{**event(3), "level": "info"}])
    sink.close()
    with psycopg.connect(dsn(endpoint)) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table}")
        assert cur.fetchone()[0] == 1
