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
