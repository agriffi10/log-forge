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
from typing import TYPE_CHECKING, Self

import nats
import pytest

from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.nats import (
    CALL_SLACK,
    DEFAULT_ACK_TIMEOUT,
    DEFAULT_CLOSE_TIMEOUT,
    LOOP_JOIN_TIMEOUT,
    NATSSink,
)

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
        core.emit([{"n": 1}])  # fire-and-forget: nobody is listening, and that is not an error
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

    subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE), "stop", "nats"], check=True, capture_output=True
    )
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
        subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE), "start", "nats"],
            check=True,
            capture_output=True,
        )
        for _ in range(60):
            if _tcp_up(url):
                break
            time.sleep(1)
        # Close inside the restored window, not while the client is still reconnecting: the
        # driver's `drain()` raises `ConnectionReconnectingError` then, and `close()` shuts the
        # loop down regardless, leaving the client's reader task to be garbage-collected against
        # a closed loop -- which surfaces as a PytestUnraisableExceptionWarning rather than a
        # failure, so it would have gone unnoticed. Since SPEC-061 `close()` no longer drains and
        # cancels every pending task before stopping the loop, and the refusal above comes from
        # the driver noticing the end-of-stream between calls rather than inside one.
        with contextlib.suppress(Exception):
            sink.close()


def test_a_whole_batch_is_bounded_against_a_stalled_server(stream) -> None:
    # SPEC-047 FR-001 AC-1, against a real server rather than a double. `pause` (SIGSTOP) keeps
    # the TCP connection open, so the client stays `is_connected` and simply never acks -- which
    # is what a wedged broker looks like, and what stops FR-004's disconnect guard pre-empting
    # the measurement the way `stop` would. Since SPEC-061 the emit ends on the probe's refusal
    # at `publish_timeout` rather than on per-publish ack timeouts, well inside the ceiling.
    url, subject, _ = stream
    sink = NATSSink(subject, servers=url, jetstream=True, publish_timeout=3.0)
    sink.emit([{"n": -1}])  # prove the path works before stalling it

    subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE), "pause", "nats"], check=True, capture_output=True
    )
    try:
        began = time.monotonic()
        with contextlib.suppress(SinkDeliveryError):
            sink.emit([{"n": i} for i in range(20)])
        elapsed = time.monotonic() - began
        # Unbounded this is 20 x the 5 s ack timeout = 100 s; bounded it is one budget plus at
        # most one in-flight ack. The generous ceiling is deliberate -- the gap carries the test.
        assert elapsed < 3.0 + DEFAULT_ACK_TIMEOUT + 5.0, f"batch not bounded: {elapsed:.2f}s"
    finally:
        subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE), "unpause", "nats"],
            check=True,
            capture_output=True,
        )
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


def test_a_bounded_constructor_fails_fast_against_an_unreachable_server(
    services_are_up: dict[str, Endpoint],
) -> None:
    # SPEC-047 FR-002 AC-2, and the only place the forwarding is proved to BIND rather than merely
    # be passed: the unit tests assert kwargs against a fake module that would accept any name.
    # At the driver's defaults this blocks ~120 s (60 reconnect attempts x 2 s), measured at
    # 120.17 s -- on the application's own startup thread, since `configure(sink=NATSSink(...))`
    # is called by the caller.
    began = time.monotonic()
    with pytest.raises(Exception, match=r"(?i)no servers|connect"):
        NATSSink(
            "lf.unreachable",
            servers="nats://127.0.0.1:1",
            max_reconnect_attempts=1,
            reconnect_time_wait=0.1,
        )
    elapsed = time.monotonic() - began

    assert elapsed < 5.0, f"the connect budget was not reached: {elapsed:.2f}s"


# --- SPEC-061: liveness against the real server -----------------------------------------------


class _outage:
    """``docker compose pause|stop nats`` on entry; the inverse on exit, then wait for the port.

    The restore is in ``__exit__`` and always runs, so a failing assertion cannot leave NATS paused
    for the rest of the session; the port is polled for up to 60 s before the test's own ``close()``
    is reached, as the existing stop/start test does.
    """

    def __init__(self, kind: str, url: str) -> None:
        assert kind in {"pause", "stop"}
        self.kind = kind
        self.url = url

    def __enter__(self) -> Self:
        subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE), self.kind, "nats"],
            check=True,
            capture_output=True,
        )
        return self

    def __exit__(self, *exc: object) -> None:
        inverse = "unpause" if self.kind == "pause" else "start"
        subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE), inverse, "nats"],
            check=True,
            capture_output=True,
        )
        for _ in range(60):
            if _tcp_up(self.url):
                break
            time.sleep(1)


def _close_quietly(sink: NATSSink) -> None:
    with contextlib.suppress(Exception):
        sink.close()


def test_a_stalled_server_is_refused_in_about_one_publish_timeout(stream) -> None:
    # SPEC-061 FR-002 AC-11 (M1: 75 of 75 emits accepted over 148 s before this spec). `pause`
    # keeps the TCP connection open and silent, so the driver's flag stays true and only the
    # server's own answer can refuse the publish.
    url, subject, count = stream
    sink = NATSSink(subject, servers=url, publish_timeout=1.0)
    sink.emit([{"n": 0}])
    assert count() == 1
    try:
        with _outage("pause", url):
            began = time.monotonic()
            with pytest.raises(SinkDeliveryError, match="PONG"):
                sink.emit([{"n": 1}])
            elapsed = time.monotonic() - began
            assert 0.9 <= elapsed <= 1.0 + CALL_SLACK + 1.0, f"{elapsed:.2f}s"
            assert sink.losses() == SinkLosses(dropped=0, failed=0)
            print(f"\nSPEC-061 stall refusal: {elapsed:.2f}s with publish_timeout=1.0")
    finally:
        _close_quietly(sink)
    assert count() == 1, "nothing was published to the stalled server"


def test_the_sink_resumes_after_the_server_restarts(stream) -> None:
    # SPEC-061 FR-005 AC-6 (M2: still refusing 40 s after the restart before this spec; M4: the
    # prototype resumed 0.50 s after it). The driver's reconnect task needs loop time between
    # calls, which the sink-owned thread now gives it; `max_reconnect_attempts` is passed
    # explicitly so the budget the resume rests on is the test's, not a default.
    url, subject, count = stream
    sink = NATSSink(
        subject,
        servers=url,
        reconnect_time_wait=0.5,
        max_reconnect_attempts=120,
        publish_timeout=2.0,
    )
    sink.emit([{"n": 0}])
    try:
        with _outage("stop", url):
            deadline = time.monotonic() + 30
            refused = False
            while time.monotonic() < deadline and not refused:
                try:
                    sink.emit([{"n": 99}])
                except SinkDeliveryError:
                    refused = True
                else:
                    time.sleep(0.25)
            assert refused, "a stopped server must be refused"
        restored = time.monotonic()
        budget = 20 * 0.5
        resumed_after: float | None = None
        while time.monotonic() - restored < budget:
            try:
                sink.emit([{"n": 1}])
            except SinkDeliveryError:
                time.sleep(0.25)
            else:
                resumed_after = time.monotonic() - restored
                break
        assert resumed_after is not None, f"the sink never resumed within {budget}s of the restart"
        print(f"\nSPEC-061 resume: accepted {resumed_after:.2f}s after the restore")
    finally:
        _close_quietly(sink)
    assert count() >= 2, "the resumed event must reach the stream"


def test_a_late_pong_leaves_the_reader_alive_and_a_second_stall_is_refused(stream) -> None:
    # SPEC-061 FR-002 AC-5, the M3 regression against the real driver: the PING sent into the
    # first pause is answered late, after the unpause, and must land on a future the sink never
    # cancelled, or the reader dies and the second pause is accepted.
    url, subject, count = stream
    sink = NATSSink(subject, servers=url, publish_timeout=1.0)
    sink.emit([{"n": 0}])
    try:
        with _outage("pause", url):
            with pytest.raises(SinkDeliveryError):
                sink.emit([{"n": 1}])
            time.sleep(1.0)
        deadline = time.monotonic() + 5.0
        accepted = False
        while time.monotonic() < deadline and not accepted:
            try:
                sink.emit([{"n": 2}])
            except SinkDeliveryError:
                time.sleep(0.25)
            else:
                accepted = True
        assert accepted, "emits must be accepted again once the server answers"
        with _outage("pause", url):
            began = time.monotonic()
            with pytest.raises(SinkDeliveryError):
                sink.emit([{"n": 3}])
            elapsed = time.monotonic() - began
            assert elapsed <= 1.0 + CALL_SLACK + 1.0, f"{elapsed:.2f}s"
            print(f"\nSPEC-061 second stall after a late PONG: refused in {elapsed:.2f}s")
    finally:
        _close_quietly(sink)
    assert count() == 2


@pytest.mark.parametrize("jetstream", [False, True], ids=["core", "jetstream"])
def test_close_is_bounded_against_a_stalled_server(stream, jetstream: bool) -> None:
    # SPEC-061 FR-004 AC-7 (M4: 2.0 s; before this spec 10.0 s and `FlushTimeoutError`). The
    # JetStream variant publishes first so the client holds its response-inbox subscription, the
    # precondition under which `wait_for(drain(), 2.0)` took 12.00 s (M7).
    url, subject, _ = stream
    sink = NATSSink(subject, servers=url, jetstream=jetstream, publish_timeout=3.0)
    sink.emit([{"n": 0}])
    closed_in: float | None = None
    try:
        with _outage("pause", url):
            began = time.monotonic()
            with pytest.raises(TimeoutError):
                sink.close()
            closed_in = time.monotonic() - began
            assert closed_in <= DEFAULT_CLOSE_TIMEOUT + LOOP_JOIN_TIMEOUT + 1.0, f"{closed_in:.2f}s"
            assert not sink._thread.is_alive()
    finally:
        _close_quietly(sink)
    print(
        f"\nSPEC-061 close under pause ({'jetstream' if jetstream else 'core'}): {closed_in:.2f}s"
    )


def test_unconfirmed_events_are_counted_and_still_land(stream, capsys) -> None:
    # SPEC-061 FR-003 AC-8, the evidence behind returning rather than raising (M6: 5 of 5 landed).
    # The pause runs inside a hook wrapped around the client's first publish, so the batch
    # straddles the onset of the stall exactly; a blocking `subprocess.run` inside the async hook
    # is accepted in a test. The restore is this test's own, with `check=False`, so a pause that
    # never happened cannot turn the `finally` into a second failure.
    url, subject, count = stream
    sink = NATSSink(subject, servers=url, publish_timeout=3.0)
    sink.emit([{"n": -1}])
    assert count() == 1
    real_publish = sink._client.publish
    paused = False

    async def pausing_publish(subject_: str, payload: bytes, *args, **kwargs):
        nonlocal paused
        if not paused:
            paused = True
            # Blocking on purpose: the pause must land between this publish and the next, and a
            # test is the one place a blocking call inside the loop is the honest shape.
            subprocess.run(  # noqa: ASYNC221
                ["docker", "compose", "-f", str(COMPOSE), "pause", "nats"],
                check=True,
                capture_output=True,
            )
        await real_publish(subject_, payload, *args, **kwargs)

    sink._client.publish = pausing_publish
    try:
        began = time.monotonic()
        sink.emit([{"n": i} for i in range(5)])
        elapsed = time.monotonic() - began
        assert sink.losses() == SinkLosses(dropped=0, failed=5)
        assert "unconfirmed" in capsys.readouterr().err
        assert elapsed <= 3.0 + 1.0, f"{elapsed:.2f}s"
    finally:
        subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE), "unpause", "nats"],
            check=False,
            capture_output=True,
        )
        for _ in range(60):
            if _tcp_up(url):
                break
            time.sleep(1)
        sink._client.publish = real_publish
        _close_quietly(sink)
    landed = 0
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        landed = count()
        if landed >= 6:
            break
        time.sleep(0.5)
    assert landed == 6, (
        f"the unconfirmed batch must still land after the unpause: {landed - 1} of 5"
    )
    print(f"\nSPEC-061 landing: {landed - 1} of 5 unconfirmed events reached the stream")
