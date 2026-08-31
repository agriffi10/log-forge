"""SPEC-041 FR-001 — PostgresSink against a real Postgres: the round trip and the projection."""

from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING

import psycopg
import pytest

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
