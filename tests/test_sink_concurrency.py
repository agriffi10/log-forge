"""Concurrent-emitter tests for the sinks that hold mutable transport state (SPEC-028).

The suite had no concurrent-emitter test before this module, which is why none of the races it
covers ever surfaced. Every test here fails against the pre-SPEC-028 sinks and passes after.
"""

import asyncio
import json
import threading
import time
from pathlib import Path
from typing import Self

import pytest

from conftest import run_concurrently
from log_foundry.sinks import _socket
from log_foundry.sinks.base import SinkDeliveryError
from log_foundry.sinks.file import FileSink, RotatingFileSink
from log_foundry.sinks.sqlite import SQLiteSink
from log_foundry.sinks.syslog import SyslogSink

THREADS = 8
PER_THREAD = 25


def event(thread: int, iteration: int) -> dict[str, object]:
    """Builds one identifiable event, padded so a batch is big enough to be preempted mid-write.

    Args:
      thread: The emitting thread's index.
      iteration: Which call on that thread.

    Returns:
      The event dict.

    Raises:
      None.
    """
    return {"thread": thread, "iteration": iteration, "pad": "x" * 200}


class SplittingSocket:
    """A fake socket whose ``sendall`` writes in two parts, as a real blocking socket may.

    ``socket.sendall`` loops over ``send`` until the buffer is drained, so it is not atomic
    against a concurrent caller — the kernel can accept a partial write and return. Splitting the
    write models that faithfully, and the ``sleep(0)`` between the halves is a yield to the
    scheduler, not synchronization: it makes the interleaving the lock prevents actually happen
    rather than waiting for it to.
    """

    def __init__(self) -> None:
        self.buffer = bytearray()

    def sendall(self, data: bytes) -> None:
        self.buffer.extend(data[:1])
        time.sleep(0)
        self.buffer.extend(data[1:])

    def sendto(self, data: bytes, _address: tuple[str, int]) -> None:
        self.sendall(data)

    def close(self) -> None:
        pass


class ThreadRecording:
    """A sink wrapper recording which threads called ``emit``, and how many were inside at once.

    ``max_concurrent`` is what turns the motivating test from "these threads all logged" into
    "these threads were inside the sink together" — without it the test would pass on a run that
    happened to serialize.
    """

    def __init__(self, inner: object) -> None:
        self._inner = inner
        self._lock = threading.Lock()
        self._inside = 0
        self.max_concurrent = 0
        self.callers: set[str] = set()

    def emit(self, batch: list[dict[str, object]]) -> None:
        with self._lock:
            self._inside += 1
            self.max_concurrent = max(self.max_concurrent, self._inside)
            self.callers.add(threading.current_thread().name)
        try:
            self._inner.emit(batch)  # type: ignore[attr-defined]
        finally:
            with self._lock:
                self._inside -= 1

    def close(self) -> None:
        self._inner.close()  # type: ignore[attr-defined]


def parse_octet_frames(buffer: bytes) -> list[bytes]:
    """Parses an octet-counted (RFC 6587) stream the way a syslog receiver does.

    Args:
      buffer: The bytes the transport put on the wire.

    Returns:
      One entry per frame read.

    Raises:
      ValueError: If the stream cannot be resynchronized — which is exactly what interleaved
        ``sendall`` calls produce, since the length prefix is then read from the middle of
        another frame's payload.
    """
    frames: list[bytes] = []
    offset = 0
    while offset < len(buffer):
        space = buffer.find(b" ", offset)
        if space == -1:
            raise ValueError(f"no length prefix at offset {offset}")
        try:
            length = int(buffer[offset:space])
        except ValueError:
            raise ValueError(f"unparseable length prefix at offset {offset}") from None
        start = space + 1
        end = start + length
        if end > len(buffer):
            raise ValueError(f"frame at {offset} claims {length} bytes, stream is short")
        frames.append(buffer[start:end])
        offset = end
    return frames


def test_file_sink_keeps_each_batch_contiguous_under_concurrent_emitters(
    tmp_path: Path,
) -> None:
    """One emitter's batch lands as consecutive lines rather than interleaved with another's.

    Line *integrity* is not the guarantee at stake here and never was: ``TextIOWrapper.write``
    holds its own lock, so a single line cannot be spliced even unlocked, and a test asserting
    only that passes against the bug. What the sink's lock adds is that the write loop over a
    batch is indivisible — unlocked, a thread preempted between two of its own lines lets
    another thread's batch land in the middle of it.
    """
    path = tmp_path / "events.ndjson"
    sink = FileSink(str(path))
    per_batch = 5

    errors = run_concurrently(
        lambda thread, iteration: sink.emit(
            [{**event(thread, iteration), "line": n} for n in range(per_batch)]
        ),
        THREADS,
        per_thread=PER_THREAD,
    )
    sink.close()

    assert errors == []
    lines = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert len(lines) == THREADS * PER_THREAD * per_batch
    for start in range(0, len(lines), per_batch):
        block = lines[start : start + per_batch]
        owners = {(entry["thread"], entry["iteration"]) for entry in block}
        assert len(owners) == 1, f"a batch was interleaved at line {start}: {owners}"
        assert [entry["line"] for entry in block] == list(range(per_batch))


def test_rotating_file_sink_rotates_without_a_concurrent_writer_seeing_a_closed_stream(
    tmp_path: Path,
) -> None:
    """A rotation never strands a concurrent write on the closed or pre-rotation handle.

    Unlocked, ``_rotate`` closes ``self._stream`` while another thread is between its own
    ``_should_rotate`` check and its ``write``, so the write lands on a closed file and raises
    ``ValueError``. The backup count is high enough that nothing is pruned, so every event
    written must still be findable across the active file and its backups.
    """
    path = tmp_path / "events.ndjson"
    sink = RotatingFileSink(str(path), max_bytes=900, backup_count=200)

    errors = run_concurrently(
        lambda thread, iteration: sink.emit([event(thread, iteration)]),
        THREADS,
        per_thread=PER_THREAD,
    )
    sink.close()

    assert errors == []
    lines: list[str] = []
    for rotated in [path, *sorted(tmp_path.glob("events.ndjson.*"))]:
        lines.extend(rotated.read_text(encoding="utf-8").splitlines())
    assert len(lines) == THREADS * PER_THREAD, "a rotation lost or duplicated events"
    written = {(json.loads(line)["thread"], json.loads(line)["iteration"]) for line in lines}
    assert written == {(t, i) for t in range(THREADS) for i in range(PER_THREAD)}
    assert len(list(tmp_path.glob("events.ndjson.*"))) > 0, "the test never rotated"


def test_socket_transport_frames_stay_parseable_under_concurrent_sends(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Concurrent syslog emits yield a stream a receiver can parse in full.

    Unlocked, two ``sendall`` calls splice their bytes together and the octet-count prefix of
    the next frame is read out of the middle of the previous one's payload, which desynchronizes
    the receiver for the life of the connection.
    """
    fake = SplittingSocket()
    monkeypatch.setattr(_socket, "_make_tcp", lambda host, port, timeout: fake)
    sink = SyslogSink(host="localhost", port=5140, transport="tcp")

    errors = run_concurrently(
        lambda thread, iteration: sink.emit([event(thread, iteration)]),
        THREADS,
        per_thread=PER_THREAD,
    )
    sink.close()

    assert errors == []
    frames = parse_octet_frames(bytes(fake.buffer))
    assert len(frames) == THREADS * PER_THREAD
    for frame in frames:
        assert frame.startswith(b"<"), "a frame did not begin with an RFC 5424 priority"
        assert frame.decode("utf-8")


def test_sqlite_sink_concurrent_emit_commits_every_row(tmp_path: Path) -> None:
    """Concurrent emitters share one connection without a rollback discarding another's rows.

    ``check_same_thread=False`` disables the driver's own guard and ``with connection`` is a
    transaction on the shared connection, so unlocked emitters interleave inside one implicit
    transaction.
    """
    sink = SQLiteSink(str(tmp_path / "events.db"))

    errors = run_concurrently(
        lambda thread, iteration: sink.emit([event(thread, iteration)]),
        THREADS,
        per_thread=PER_THREAD,
    )

    assert errors == []
    rows = sink._conn.execute("SELECT event FROM log_events").fetchall()
    sink.close()
    assert len(rows) == THREADS * PER_THREAD
    written = {(json.loads(row[0])["thread"], json.loads(row[0])["iteration"]) for row in rows}
    assert written == {(t, i) for t in range(THREADS) for i in range(PER_THREAD)}


def test_close_during_an_in_flight_emit_does_not_strand_the_writer(tmp_path: Path) -> None:
    """``close()`` waits for an in-flight ``emit`` rather than releasing the stream under it.

    The emitting thread is parked inside ``emit`` on its first stream write, so the close is
    provably concurrent with a write in progress rather than merely likely to be (SPEC-028
    FR-002). Unlocked, ``close`` flushes and closes the handle between the batch's two writes
    and the second raises ``ValueError: I/O operation on closed file``.
    """
    path = tmp_path / "events.ndjson"
    sink = FileSink(str(path))
    inside_emit = threading.Event()
    may_finish = threading.Event()
    failures: list[BaseException] = []

    class ParkingStream:
        """Wraps the sink's stream and holds the first write until released."""

        def __init__(self, inner) -> None:
            self._inner = inner
            self._writes = 0

        def write(self, data: str) -> int:
            self._writes += 1
            if self._writes == 1:
                inside_emit.set()
                may_finish.wait(timeout=10)
            return self._inner.write(data)

        def flush(self) -> None:
            self._inner.flush()

        def close(self) -> None:
            self._inner.close()

    sink._stream = ParkingStream(sink._stream)  # type: ignore[assignment]

    def emitter() -> None:
        try:
            sink.emit([{"marker": "blocking"}, {"marker": "after"}])
        except BaseException as exc:
            failures.append(exc)

    thread = threading.Thread(target=emitter)
    thread.start()
    assert inside_emit.wait(timeout=10), "the emitting thread never entered emit"

    closer = threading.Thread(target=sink.close)
    closer.start()
    may_finish.set()
    thread.join(timeout=10)
    closer.join(timeout=10)

    assert failures == [], f"close() stranded the in-flight emit: {failures}"
    markers = [json.loads(line)["marker"] for line in path.read_text().splitlines()]
    assert markers == ["blocking", "after"]


def test_the_orphan_path_and_the_worker_emit_into_one_sink_concurrently(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The scenario that motivates the spec: application threads and the worker share a sink.

    A level call with no active span emits synchronously on the caller's thread while the
    background worker drains traced calls into the same object, so the sink sees application
    threads and ``log-foundry-worker`` at once.
    """
    import log_foundry

    path = tmp_path / "events.ndjson"
    sink = ThreadRecording(RotatingFileSink(str(path), max_bytes=20_000, backup_count=100))
    log_foundry.configure(service="test", version="0.0.0", env="test", sink=sink)

    @log_foundry.trace
    def traced(thread: int, iteration: int) -> None:
        log_foundry.info("in-span", thread=thread, iteration=iteration)

    def work(thread: int, iteration: int) -> None:
        log_foundry.info("orphan", thread=thread, iteration=iteration, pad="x" * 200)
        traced(thread, iteration)

    errors = run_concurrently(work, THREADS, per_thread=PER_THREAD)
    assert log_foundry.flush(timeout=15.0)
    assert log_foundry.health().failed_batches == 0
    log_foundry.shutdown()

    assert errors == []
    assert sink.max_concurrent > 1, "the emits never actually overlapped"
    assert any("worker" in name for name in sink.callers), f"no worker emit: {sink.callers}"
    assert len(sink.callers) > 1, f"the sink saw only {sink.callers}"

    lines: list[str] = []
    for rotated in [path, *sorted(tmp_path.glob("events.ndjson.*"))]:
        lines.extend(rotated.read_text(encoding="utf-8").splitlines())
    orphans = [json.loads(line) for line in lines]
    assert len(orphans) == len(lines), "a line was spliced and no longer parses"
    assert sum(1 for e in orphans if e["message"] == "orphan") == THREADS * PER_THREAD


class CountingLock:
    """A real lock that counts acquisitions, so a test can prove a counter is guarded.

    The exact-count assertion these tests also make does **not** distinguish a guarded counter
    from an unguarded one: `+=` on an attribute is formally a read-modify-write and Python
    promises no atomicity, but on a GIL build it was measured losing nothing across 1.6M
    concurrent increments, so a race-reproduction test cannot be written for the interpreters CI
    runs. What *is* deterministic is whether the increment happens inside the critical section,
    which is what the acquisition count shows — and it is the property that keeps holding when
    the GIL is not there to help (SPEC-028 FR-003).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._meta = threading.Lock()
        self.acquisitions = 0

    def __enter__(self) -> Self:
        self._lock.acquire()
        with self._meta:
            self.acquisitions += 1
        return self

    def __exit__(self, *exc: object) -> bool:
        self._lock.release()
        return False


class AlwaysFailingSink:
    """A child sink whose every ``emit`` raises, so ``MultiSink.failed`` moves once per call."""

    def emit(self, batch: list[dict[str, object]]) -> None:
        raise RuntimeError("down")

    def close(self) -> None:
        pass


class ExclusiveConnection:
    """A fake driver connection that reports any overlap between two threads' sequences.

    A real `psycopg` connection carries one transaction, so an interleaved
    ``cursor``/``commit`` pair from another thread commits or rolls back work that is not its
    own. This double cannot corrupt anything, so it records the overlap instead — which is the
    property under test rather than its consequence.
    """

    def __init__(self) -> None:
        self._guard = threading.Lock()
        self._inside = 0
        self.overlaps = 0
        self.commits = 0

    def _enter(self) -> None:
        with self._guard:
            self._inside += 1
            if self._inside > 1:
                self.overlaps += 1

    def _leave(self) -> None:
        with self._guard:
            self._inside -= 1

    def cursor(self):
        connection = self

        class _Cursor:
            def __enter__(self):
                connection._enter()
                return self

            def __exit__(self, *exc: object) -> bool:
                connection._leave()
                return False

            def executemany(self, sql: str, rows: list) -> None:
                time.sleep(0)

            def execute(self, sql: str, params: object = None) -> None:
                pass

        return _Cursor()

    def commit(self) -> None:
        with self._guard:
            self.commits += 1

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        pass


def test_multi_sink_counts_every_child_failure_under_concurrent_emitters() -> None:
    """``MultiSink.failed`` loses no increment when several threads emit at once.

    ``self.failed += 1`` is a read-modify-write, so unlocked it undercounts — and SPEC-026 made
    this one of the numbers an operator alerts on, where an undercount is a missed alert.

    On the exactness of the count: it holds on a GIL build whether or not the increment is
    guarded, so the assertion below is a regression guard rather than a reproduction of the
    race. See :class:`CountingLock` for what actually distinguishes the two.
    """
    from log_foundry.sinks.multi import MultiSink

    children = 2
    sink = MultiSink(*[AlwaysFailingSink() for _ in range(children)])
    lock = CountingLock()
    sink._counter_lock = lock

    errors = run_concurrently(
        lambda thread, iteration: sink.emit([event(thread, iteration)]),
        THREADS,
        per_thread=PER_THREAD,
    )

    assert len(errors) == THREADS * PER_THREAD, "every emit should have raised: all children down"
    assert sink.failed == THREADS * PER_THREAD * children
    assert lock.acquisitions == sink.failed, "an increment ran outside the counter lock"


def test_sqs_sink_counts_every_oversized_drop_under_concurrent_emitters() -> None:
    """``dropped_oversized`` and the ``losses()`` pair are guarded under concurrent emitters.

    ``MAX_BYTES`` is lowered rather than building 256 KB payloads: the counter is what is under
    test, not the threshold. The ``losses()`` call adds one acquisition on top of the drops,
    which is the pair read taken under the same lock.
    """
    sqs_mod = pytest.importorskip("log_foundry.sinks.sqs")

    class NeverCalledClient:
        def send_message_batch(self, **kwargs: object) -> dict:
            raise AssertionError("every event should have been dropped as oversized")

    sink = sqs_mod.SQSSink(queue_url="https://example/q", client=NeverCalledClient())
    sink.MAX_BYTES = 50
    lock = CountingLock()
    sink._counter_lock = lock

    errors = run_concurrently(
        lambda thread, iteration: sink.emit([event(thread, iteration)]),
        THREADS,
        per_thread=PER_THREAD,
    )

    assert errors == []
    assert sink.dropped_oversized == THREADS * PER_THREAD
    assert lock.acquisitions == sink.dropped_oversized, "a drop was counted outside the lock"
    assert sink.losses() == (THREADS * PER_THREAD, 0)
    assert lock.acquisitions == sink.dropped_oversized + 1, "losses() read outside the lock"


def test_postgres_transaction_is_not_interleaved_by_a_concurrent_emit() -> None:
    """One thread's ``cursor``/``commit`` sequence completes before another's begins."""
    from log_foundry.sinks.postgres import PostgresSink

    connection = ExclusiveConnection()
    sink = PostgresSink("events", connection=connection)

    errors = run_concurrently(
        lambda thread, iteration: sink.emit([event(thread, iteration)]),
        THREADS,
        per_thread=PER_THREAD,
    )

    assert errors == []
    assert connection.overlaps == 0, f"{connection.overlaps} interleaved transactions"
    assert connection.commits == THREADS * PER_THREAD


def test_losses_does_not_block_behind_an_in_flight_emit(tmp_path: Path) -> None:
    """A ``health()`` poll reads the counters while an emit is parked inside the sink.

    This is the two-lock decision under test (SPEC-028 FR-003). Counters guarded by the *emit*
    lock — which is what the spec originally called for — would make this call wait for the
    insert and its retry backoff to finish, and ``health()`` is precisely the call an operator
    makes when a destination is already hanging.
    """
    from log_foundry.sinks.postgres import PostgresSink

    parked = threading.Event()
    may_finish = threading.Event()

    class ParkingConnection(ExclusiveConnection):
        def commit(self) -> None:
            parked.set()
            may_finish.wait(timeout=10)
            super().commit()

    sink = PostgresSink("events", connection=ParkingConnection())
    emitter = threading.Thread(target=lambda: sink.emit([event(0, 0)]))
    emitter.start()
    assert parked.wait(timeout=10), "the emitting thread never reached the parked commit"

    read = threading.Thread(target=sink.losses)
    read.start()
    read.join(timeout=5)
    blocked = read.is_alive()

    may_finish.set()
    emitter.join(timeout=10)
    read.join(timeout=5)

    assert not blocked, "losses() blocked behind an in-flight emit"


def test_every_driver_backed_sink_records_a_concurrency_decision() -> None:
    """Every sink holding a driver either locks it or says in writing why it need not.

    SPEC-028's own FR-002 enumerated seven sinks by name, and the review found three it had
    missed — ``NATSSink`` (which could hang an application thread permanently), ``RabbitMQSink``
    and ``AzureEventHubsSink``. That is the SPEC-027 failure repeated: a hand-written roster and
    a test that agrees with the roster rather than with the code.

    Whether a given driver needs a lock is not derivable from syntax — it is the vendor's
    contract, which lives in their docs, not in this tree. So this lint does not try to decide;
    it makes the *decision* mandatory and visible. A sink that lazily imports a driver, or opens
    a socket or event loop of its own, must either take ``self._lock`` or carry the
    ``SPEC-028 FR-002`` marker explaining why it does not. A new sink joins this set the day it
    is written, and the author has to choose rather than inherit silence.
    """
    import ast
    import pathlib

    import log_foundry

    root = pathlib.Path(log_foundry.__file__).parent / "sinks"
    undecided: list[str] = []
    for path in sorted(root.glob("*.py")):
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        lazily_imported = any(
            isinstance(inner, ast.Import | ast.ImportFrom)
            for fn in ast.walk(tree)
            if isinstance(fn, ast.FunctionDef)
            for inner in ast.walk(fn)
        )
        owns_a_handle = any(
            token in source
            for token in ("socket.socket", "create_connection", "new_event_loop")
        )
        if not (lazily_imported or owns_a_handle):
            continue
        if "with self._lock" in source or "SPEC-028 FR-002" in source:
            continue
        undecided.append(path.name)

    assert not undecided, (
        "these sinks hold a driver but neither lock it nor record why they need not: "
        f"{undecided}"
    )


def test_nats_sink_does_not_reenter_its_managed_event_loop() -> None:
    """Concurrent emits never enter one ``asyncio`` loop twice — the worst finding of the arc.

    Unlocked, a second thread calling ``run_until_complete`` on a running loop raises
    ``RuntimeError: This event loop is already running``, and can leave the loop's task
    machinery in a state where a thread **never returns from ``emit``**. On the orphan path that
    is an application thread hung forever inside ``log_foundry.info()``. The 30 s join inside
    ``run_concurrently`` is what turns that hang into a failure rather than a stalled CI run.

    Like the SQLite case, this one kills its mutant by hanging rather than by asserting: measured
    against the unlocked sink, threads wedge inside ``run_until_complete`` and never return. A
    regression here therefore presents as a slow failure, not a clean one.
    """
    nats_mod = pytest.importorskip("log_foundry.sinks.nats")

    class FakeClient:
        def __init__(self) -> None:
            self.published: list[bytes] = []

        async def publish(self, subject: str, payload: bytes) -> None:
            await asyncio.sleep(0)
            self.published.append(payload)

    client = FakeClient()
    sink = nats_mod.NATSSink("events", client=client)

    errors = run_concurrently(
        lambda thread, iteration: sink.emit([event(thread, iteration)]),
        THREADS,
        per_thread=PER_THREAD,
    )
    sink.close()

    assert errors == [], f"emits collided on the loop: {errors[:3]}"
    assert len(client.published) == THREADS * PER_THREAD
    assert sink.losses() == (0, 0)


def test_rabbitmq_publishes_are_not_interleaved_on_one_channel() -> None:
    """One thread's publish completes before another's begins on the shared channel.

    ``pika`` states one connection must not be shared across threads, and this sink also
    *rebinds* the channel on error — so an unlocked ``_reset`` can close the channel object
    another thread is mid-publish on.
    """
    rabbit_mod = pytest.importorskip("log_foundry.sinks.rabbitmq")

    class ExclusiveChannel:
        def __init__(self) -> None:
            self._guard = threading.Lock()
            self._inside = 0
            self.overlaps = 0
            self.published = 0

        def basic_publish(self, **kwargs: object) -> None:
            with self._guard:
                self._inside += 1
                if self._inside > 1:
                    self.overlaps += 1
            time.sleep(0)
            with self._guard:
                self.published += 1
                self._inside -= 1

        def close(self) -> None:
            pass

    channel = ExclusiveChannel()

    class FakeConnection:
        def channel(self) -> ExclusiveChannel:
            return channel

        def close(self) -> None:
            pass

    sink = rabbit_mod.RabbitMQSink(
        exchange="logs", routing_key="events", connection=FakeConnection()
    )

    errors = run_concurrently(
        lambda thread, iteration: sink.emit([event(thread, iteration)]),
        THREADS,
        per_thread=PER_THREAD,
    )
    sink.close()

    assert errors == []
    assert channel.overlaps == 0, f"{channel.overlaps} interleaved publishes on one channel"
    assert channel.published == THREADS * PER_THREAD


def test_clickhouse_insert_is_not_interleaved_by_a_concurrent_emit() -> None:
    """FR-002 names ClickHouse, and its lock was verified by nothing until now."""
    clickhouse_mod = pytest.importorskip("log_foundry.sinks.clickhouse")

    class ExclusiveClient:
        def __init__(self) -> None:
            self._guard = threading.Lock()
            self._inside = 0
            self.overlaps = 0
            self.rows = 0

        def insert(self, table: str, data: list, column_names: list) -> None:
            with self._guard:
                self._inside += 1
                if self._inside > 1:
                    self.overlaps += 1
            time.sleep(0)
            with self._guard:
                self.rows += len(data)
                self._inside -= 1

        def close(self) -> None:
            pass

    client = ExclusiveClient()
    sink = clickhouse_mod.ClickHouseSink("events", client=client)

    errors = run_concurrently(
        lambda thread, iteration: sink.emit([event(thread, iteration)]),
        THREADS,
        per_thread=PER_THREAD,
    )
    sink.close()

    assert errors == []
    assert client.overlaps == 0, f"{client.overlaps} interleaved inserts"
    assert client.rows == THREADS * PER_THREAD


def test_dropped_unadjudicated_is_counted_under_the_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The third loss counter FR-003 names, guarded and asserted (SPEC-018's counter).

    The retry backoff is neutralized as ``test_sinks_sqs.py`` does: this test is about the
    counter, and 200 emits each spending their full 0.7 s budget would take minutes.
    """
    kinesis_mod = pytest.importorskip("log_foundry.sinks.kinesis")
    monkeypatch.setattr(kinesis_mod, "wait", lambda _delay, _stop=None: None)

    class ShortResponseClient:
        """Returns a per-record array too short to adjudicate, which is SPEC-018's case."""

        def put_records(self, *, StreamName: str, Records: list) -> dict:
            return {"FailedRecordCount": 1, "Records": [{"SequenceNumber": "1"}]}

    sink = kinesis_mod.KinesisSink("stream", client=ShortResponseClient())
    lock = CountingLock()
    sink._counter_lock = lock

    errors = run_concurrently(
        lambda thread, iteration: sink.emit([event(thread, iteration), event(thread, iteration)]),
        THREADS,
        per_thread=PER_THREAD,
    )

    # Two records per emit, and the whole chunk is abandoned together, so the counter moves by
    # two per acquisition — the acquisition count tracks emits, not records.
    assert sink.dropped_unadjudicated == THREADS * PER_THREAD * 2
    assert lock.acquisitions == THREADS * PER_THREAD, "a drop was counted outside the lock"
    # SPEC-018 folds an unadjudicable record into ``failed``, not ``dropped``: it may have
    # landed, so it is unconfirmed rather than known-discarded.
    assert sink.losses().failed == THREADS * PER_THREAD * 2
    assert all(isinstance(err, Exception) for err in errors)


@pytest.mark.parametrize("build", ["file", "rotating", "sqlite"])
def test_close_waits_for_an_in_flight_emit_on_every_locked_sink(
    build: str, tmp_path: Path
) -> None:
    """``close()`` never releases a resource under an active writer, for each locked sink.

    Review found five lock sites verified by nothing — the ``close`` half of FR-002's criterion
    was demonstrated for ``FileSink`` alone and asserted for the rest. Each sink is parked by the
    hook that actually reaches it: the file sinks through their stream, which is inside the lock,
    and ``SQLiteSink`` through column projection, which is outside it. That asymmetry is the
    point — the file sinks hold the lock for the whole emit and simply finish, while SQLite has a
    real window and must refuse the batch in the library's own vocabulary.
    """
    parked = threading.Event()
    may_finish = threading.Event()

    class ParkingStream:
        """Holds the first write until released."""

        def __init__(self, inner) -> None:
            self._inner = inner
            self._writes = 0

        def write(self, data: str) -> int:
            self._writes += 1
            if self._writes == 1:
                parked.set()
                may_finish.wait(timeout=10)
            return self._inner.write(data)

        def flush(self) -> None:
            self._inner.flush()

        def close(self) -> None:
            self._inner.close()

    class ParkingEvent(dict):
        """An event whose column projection parks the emitting thread."""

        def get(self, key, default=None):
            parked.set()
            may_finish.wait(timeout=10)
            return super().get(key, default)

    batch: list[dict[str, object]]
    if build == "sqlite":
        sink = SQLiteSink(str(tmp_path / "c.db"))
        batch = [ParkingEvent(log_id="1", marker="x"), {"log_id": "2", "marker": "after"}]
    elif build == "file":
        sink = FileSink(str(tmp_path / "a.ndjson"))
        sink._stream = ParkingStream(sink._stream)  # type: ignore[assignment]
        batch = [{"marker": "x"}, {"marker": "after"}]
    else:
        sink = RotatingFileSink(str(tmp_path / "b.ndjson"), max_bytes=10_000, backup_count=5)
        sink._stream = ParkingStream(sink._stream)  # type: ignore[assignment]
        batch = [{"marker": "x"}, {"marker": "after"}]

    failures: list[BaseException] = []

    def emitter() -> None:
        try:
            sink.emit(batch)
        except BaseException as exc:
            failures.append(exc)

    worker = threading.Thread(target=emitter)
    worker.start()
    assert parked.wait(timeout=10), f"could not park inside {build}'s emit"

    closer = threading.Thread(target=sink.close)
    closer.start()
    may_finish.set()
    worker.join(timeout=10)
    closer.join(timeout=10)
    assert not worker.is_alive() and not closer.is_alive(), "close and emit deadlocked"

    # A close landing mid-emit must never surface as driver internals. The file sinks take the
    # lock before touching anything, so they complete; SQLite projects columns outside the lock,
    # so the batch is refused in the library's own vocabulary instead of "closed database".
    assert all(isinstance(exc, SinkDeliveryError) for exc in failures), (
        f"close() stranded the in-flight emit on {build}: {failures}"
    )
