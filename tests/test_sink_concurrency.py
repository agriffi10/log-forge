"""Concurrent-emitter tests for the sinks that hold mutable transport state (SPEC-028).

The suite had no concurrent-emitter test before this module, which is why none of the races it
covers ever surfaced. Every test here fails against the pre-SPEC-028 sinks and passes after.
"""

import json
import threading
import time
from pathlib import Path

import pytest

from conftest import run_concurrently
from log_foundry.sinks import _socket
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
