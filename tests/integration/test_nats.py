"""SPEC-041 FR-001 — NATSSink against a real server, counted at a JetStream stream.

Counted at the stream rather than with a live subscriber on purpose: a subscriber's own
connection is a second thing that can drop during the test, and a test whose *measuring
instrument* shares the failure mode under test cannot distinguish the two.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import TYPE_CHECKING

import nats
import pytest

from log_foundry.sinks.nats import NATSSink

if TYPE_CHECKING:
    from integration.conftest import Endpoint


@pytest.fixture
def stream(services_are_up: dict[str, Endpoint]):
    url = f"nats://{services_are_up['nats'].url_host}"
    name = f"LF{uuid.uuid4().hex[:8].upper()}"
    subject = f"lf.{name.lower()}"

    async def setup() -> None:
        conn = await nats.connect(url)
        await conn.jetstream().add_stream(name=name, subjects=[subject])
        await conn.close()

    async def teardown() -> None:
        conn = await nats.connect(url)
        await conn.jetstream().delete_stream(name)
        await conn.close()

    async def count() -> int:
        conn = await nats.connect(url)
        info = await conn.jetstream().stream_info(name)
        await conn.close()
        return info.state.messages

    asyncio.run(setup())
    yield url, subject, lambda: asyncio.run(count())
    asyncio.run(teardown())


def test_a_batch_reaches_the_stream(stream) -> None:
    url, subject, count = stream
    sink = NATSSink(subject, servers=url)
    sink.emit([{"n": 1}, {"n": 2}, {"n": 3}])
    sink.flush()
    sink.close()

    assert count() == 3


def test_jetstream_mode_publishes_with_acknowledgement(stream) -> None:
    url, subject, count = stream
    sink = NATSSink(subject, servers=url, jetstream=True)
    sink.emit([{"n": 1}, {"n": 2}])
    sink.close()

    assert count() == 2
    assert sink.losses().failed == 0
