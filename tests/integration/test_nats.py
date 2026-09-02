"""SPEC-041 FR-001 — NATSSink against a real server, counted at a JetStream stream.

Counted at the stream rather than with a live subscriber on purpose: a subscriber's own
connection is a second thing that can drop during the test, and a test whose *measuring
instrument* shares the failure mode under test cannot distinguish the two.
"""

from __future__ import annotations

import asyncio
import contextlib
import pathlib
import socket
import subprocess
import time
import uuid
from typing import TYPE_CHECKING

import nats
import pytest

from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.nats import DEFAULT_ACK_TIMEOUT, NATSSink

if TYPE_CHECKING:
    from integration.conftest import Endpoint

COMPOSE = pathlib.Path(__file__).parent / "docker-compose.yml"


def _tcp_up(url: str) -> bool:
    host, _, port = url.removeprefix("nats://").partition(":")
    try:
        with socket.create_connection((host, int(port)), timeout=1.0):
            return True
    except OSError:
        return False


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


def test_only_jetstream_mode_notices_a_subject_no_stream_is_bound_to(stream) -> None:
    # Counting messages at the stream cannot tell the two modes apart -- the core-publish test
    # above does exactly that and passes -- so `jetstream=True` could be ignored entirely and
    # both would stay green. This is the observable difference: JetStream waits for an ack and
    # gets "no responders" for an unbound subject, while a core publish is fire-and-forget and
    # succeeds against nothing at all.
    url, _, _ = stream
    unbound = "lf.no.stream.here"

    # Each NATSSink owns an `asyncio.new_event_loop()`, so both are closed in a `finally`: a
    # failing assertion would otherwise leak a loop and its fd out of the test.
    core = NATSSink(unbound, servers=url)
    try:
        core.emit([{"n": 1}])      # fire-and-forget: nobody is listening, and that is not an error
        assert core.losses().failed == 0
    finally:
        core.close()

    acked = NATSSink(unbound, servers=url, jetstream=True)
    try:
        with pytest.raises(SinkDeliveryError):
            acked.emit([{"n": 1}])
        assert acked.losses().failed == 1
    finally:
        acked.close()


def test_a_disconnected_client_reports_non_delivery(stream) -> None:
    # FR-004 AC-5, against a real outage. Before the guard, five emits with the server stopped
    # each returned in 0.00 s with losses() reading all zeros, and one of six events reached the
    # destination -- the SPEC-026 shape that makes the worker's retry and failed_batches inert.
    url, subject, count = stream
    sink = NATSSink(subject, servers=url)
    sink.emit([{"n": 0}])
    assert count() == 1

    subprocess.run(["docker", "compose", "-f", str(COMPOSE), "stop", "nats"], check=True,
                   capture_output=True)
    try:
        deadline = time.monotonic() + 30
        refused = 0
        while time.monotonic() < deadline and refused == 0:
            try:
                sink.emit([{"n": 99}])
            except SinkDeliveryError:
                refused += 1
            else:
                time.sleep(0.5)
        assert refused, "a sustained outage must be reported, not absorbed"
        # SPEC-032: a refusal is reported to the worker, not absorbed here, so no counter moves.
        assert sink.losses() == SinkLosses(dropped=0, failed=0)
    finally:
        subprocess.run(["docker", "compose", "-f", str(COMPOSE), "start", "nats"], check=True,
                       capture_output=True)
        for _ in range(60):
            if _tcp_up(url):
                break
            time.sleep(1)
        # Close inside the restored window, not while the client is still reconnecting: the
        # driver's `drain()` raises `ConnectionReconnectingError` then, and `close()` shuts the
        # loop down regardless, leaving the client's reader task to be garbage-collected against
        # a closed loop -- which surfaces as a PytestUnraisableExceptionWarning rather than a
        # failure, so it would have gone unnoticed.
        with contextlib.suppress(Exception):
            sink.close()


def test_a_whole_batch_is_bounded_against_a_stalled_server(stream) -> None:
    # SPEC-047 FR-001 AC-1, against a real server rather than a double. `pause` (SIGSTOP) keeps
    # the TCP connection open, so the client stays `is_connected` and simply never acks -- which
    # is what a wedged broker looks like, and what stops FR-004's disconnect guard pre-empting
    # the measurement the way `stop` would.
    url, subject, _ = stream
    sink = NATSSink(subject, servers=url, jetstream=True, publish_timeout=3.0)
    sink.emit([{"n": -1}])                      # prove the path works before stalling it

    subprocess.run(["docker", "compose", "-f", str(COMPOSE), "pause", "nats"], check=True,
                   capture_output=True)
    try:
        began = time.monotonic()
        with contextlib.suppress(SinkDeliveryError):
            sink.emit([{"n": i} for i in range(20)])
        elapsed = time.monotonic() - began
        # Unbounded this is 20 x the 5 s ack timeout = 100 s; bounded it is one budget plus at
        # most one in-flight ack. The generous ceiling is deliberate -- the gap carries the test.
        assert elapsed < 3.0 + DEFAULT_ACK_TIMEOUT + 5.0, f"batch not bounded: {elapsed:.2f}s"
    finally:
        subprocess.run(["docker", "compose", "-f", str(COMPOSE), "unpause", "nats"], check=True,
                       capture_output=True)
        with contextlib.suppress(Exception):
            sink.close()


def test_a_large_batch_reaches_a_healthy_stream_with_a_publish_timeout_set(stream) -> None:
    # SPEC-047 FR-001 AC-4, and the half no unit test can prove: that `timeout=` is the keyword
    # the REAL JetStreamContext.publish takes. `_publish_all` catches every per-event exception,
    # so a wrong kwarg would surface as a counted failure rather than a crash -- the shape
    # SPEC-041 and SPEC-043 paid for with SentrySink. Asserting delivery is what refuses it.
    url, subject, count = stream
    sink = NATSSink(subject, servers=url, jetstream=True, publish_timeout=30.0)
    sink.emit([{"n": i} for i in range(200)])
    sink.close()

    assert count() == 200
    assert sink.losses() == SinkLosses(dropped=0, failed=0)
