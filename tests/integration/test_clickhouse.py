"""SPEC-041 FR-001 — ClickHouseSink against a real ClickHouse."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import clickhouse_connect

from log_foundry.sinks.clickhouse import ClickHouseSink

if TYPE_CHECKING:
    from integration.conftest import Endpoint


def test_a_batch_lands_as_rows(services_are_up: dict[str, Endpoint]) -> None:
    endpoint = services_are_up["clickhouse"]
    table = f"lf_{uuid.uuid4().hex[:8]}"
    sink = ClickHouseSink(table, dsn=f"http://{endpoint.url_host}", create_table=True)
    sink.emit(
        [
            {"timestamp": "2026-08-30T12:00:00+00:00", "level": "INFO", "function": "f", "n": n}
            for n in range(5)
        ]
    )
    sink.close()

    client = clickhouse_connect.get_client(host=endpoint.host, port=endpoint.port)
    assert client.query(f"SELECT count() FROM {table}").result_rows == [(5,)]
    client.command(f"DROP TABLE {table}")
