"""SPEC-079 FR-004: a child keeps today's delivery and ownership, and inherits no half-written state.

Every case runs in a real forked child. Where a parent thread is parked inside the library to
place the fork, the parking patch is gated on the parent's pid, so the child never runs it.
Serves invariants 2, 5, 6 and 12.
"""

from __future__ import annotations

import json
import os
import socket
import threading
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry
from log_foundry import _lifecycle
from log_foundry.sinks.file import FileSink
from test_fork_lifecycle import _held_by_a_parent_thread, run_in_child
from test_invariants_model import derived_current
from test_sink_ownership import BuriedHolder, RecordingSink, ThirdPartyWrapper

if TYPE_CHECKING:
    import pathlib


def _parked_in(
    monkeypatch: pytest.MonkeyPatch, owner: Any, name: str
) -> tuple[threading.Event, threading.Event]:
    """Replaces ``owner.name`` with a version that parks the parent's caller until released.

    Gated on the parent's pid, so a child that calls the same function runs the original.
    """
    original = getattr(owner, name)
    parent = os.getpid()
    entered, release = threading.Event(), threading.Event()

    def parked(*args: Any, **kwargs: Any) -> Any:
        if os.getpid() == parent:
            entered.set()
            release.wait(10.0)
        return original(*args, **kwargs)

    monkeypatch.setattr(owner, name, parked)
    return entered, release


def _traced() -> int:
    @log_foundry.trace
    def work() -> int:
        return 1

    return work()


def test_a_child_forked_inside_a_no_worker_shutdown_inherits_no_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-004: the count and the late worker are zero in the child, and its worker is not late."""
    log_foundry.configure(service="state", sink=RecordingSink())
    log_foundry.info("an orphan, so a no-worker shutdown has something to close")
    assert _lifecycle._state.worker_exists() is None
    entered, release = _parked_in(monkeypatch, _lifecycle, "_close_owed")
    stopper = threading.Thread(target=log_foundry.shutdown, args=(5.0,))
    stopper.start()
    try:
        assert entered.wait(5.0), "the shutdown never reached its close"
        assert _lifecycle._state._shutdown_running == 1, "the precondition: inside the window"

        def read() -> str:
            count = _lifecycle._state._shutdown_running
            late = _lifecycle._state._late_worker
            _traced()
            built = _lifecycle._state.worker_exists()
            return f"{count},{late is None},{built is not None},{built is not _lifecycle._state._late_worker}"

        child = run_in_child(read)
    finally:
        release.set()
        stopper.join(10.0)
    assert child.output == "0,True,True,True", child.output


def test_a_child_of_a_late_worker_delivers_and_holds_no_late_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-004: the late worker is the child's worker, ``_late_worker`` is None, and it delivers."""
    sink = RecordingSink()
    log_foundry.configure(service="state", sink=sink)
    log_foundry.info("an orphan, so a no-worker shutdown has something to close")
    entered, release = _parked_in(monkeypatch, _lifecycle, "_close_owed")
    stopper = threading.Thread(target=log_foundry.shutdown, args=(5.0,))
    stopper.start()
    try:
        assert entered.wait(5.0), "the shutdown never reached its close"
        _traced()
        late = _lifecycle._state._late_worker
        assert late is not None and late is _lifecycle._state._worker, "the precondition"

        def read() -> str:
            delivered_before = len(sink.events)
            same = _lifecycle._state._worker is late
            cleared = _lifecycle._state._late_worker is None
            _traced()
            flushed = bool(log_foundry.flush(timeout=5.0))
            return f"{same},{cleared},{flushed},{len(sink.events) > delivered_before}"

        child = run_in_child(read)
    finally:
        release.set()
        stopper.join(10.0)
    assert child.output == "True,True,True,True", child.output


def test_a_child_forked_inside_a_move_section_re_settles_it(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FR-004 (A2): a fork between ``_worker =`` and the arm leaves a child that re-settles.

    The parent thread is parked inside ``_get_worker``'s section by blocking the arm. The child
    must read a stored state equal to the derived one, owe the live worker's sink, and close the
    ``FileSink`` it re-acquired at its own ``shutdown()``.
    """
    sink = FileSink(str(tmp_path / "section.ndjson"))
    log_foundry.configure(service="state", sink=sink)
    entered, release = _parked_in(monkeypatch, _lifecycle._state._closes, "arm")
    builder = threading.Thread(target=_traced)
    builder.start()
    try:
        assert entered.wait(5.0), "the build never reached its arm"
        assert id(sink) not in _lifecycle._state._closes.owed, "the precondition: not yet owed"

        def read() -> str:
            settled = _lifecycle._state._current == derived_current(_lifecycle._state)
            owed = id(sink) in _lifecycle._state._closes.owed
            log_foundry.shutdown(3.0)
            return f"{settled},{owed},{sink._closed}"

        child = run_in_child(read)
    finally:
        release.set()
        builder.join(10.0)
    assert child.output == "True,True,True", child.output


def _probe(sink: FileSink) -> str:
    """What the probe read: the lock replaced, acquirable, and the record naming the child."""
    lock = sink._lock
    acquirable = lock.acquire(timeout=1)
    if acquirable:
        lock.release()
    record = _lifecycle._state._owned.get(id(sink), (None,))[0]
    owner = (
        "child"
        if record == os.getpid()
        else ("foreign" if record == _lifecycle._FOREIGN else record)
    )
    return f"acquirable={acquirable} record={owner}"


@pytest.mark.parametrize("shape", ["direct", "buried", "wrapper"])
def test_the_child_repairs_reacquires_and_owns(tmp_path: pathlib.Path, shape: str) -> None:
    """FR-004: each of the probe's three cases reads repaired, acquirable and owned by the child."""
    sink = FileSink(str(tmp_path / f"{shape}.ndjson"))
    configured: Any = {
        "direct": sink,
        "buried": BuriedHolder(sink),
        "wrapper": ThirdPartyWrapper(sink),
    }[shape]
    log_foundry.configure(service="probe", sink=configured)
    before = id(sink._lock)
    released, holder = _held_by_a_parent_thread(sink._lock)
    try:
        child = run_in_child(lambda: f"repaired={id(sink._lock) != before} {_probe(sink)}")
    finally:
        released.set()
        holder.join(5.0)
    assert child.output == "repaired=True acquirable=True record=child", child.output


class _CountingSocketSink:
    """A third-party connection sink: one line per event, a real goodbye on close, counted."""

    def __init__(self, conn: socket.socket) -> None:
        self._conn = conn
        self.closes = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Sends one line per event."""
        for event in batch:
            self._conn.sendall(f"{event.get('message')}\n".encode())

    def close(self) -> None:
        """Counts the close; the destructive step a child must never take."""
        self.closes += 1


def test_a_child_owns_a_reacquired_file_sink_and_refuses_the_others(tmp_path: pathlib.Path) -> None:
    """FR-004: a direct ``FileSink`` is the child's to close; a socket or recording sink is not."""
    path = tmp_path / "owned.ndjson"
    file_sink = FileSink(str(path))
    log_foundry.configure(service="own", sink=file_sink)

    def file_child() -> str:
        log_foundry.info("from-the-child")
        releasable = _lifecycle.releasable(file_sink)
        log_foundry.shutdown(3.0)
        return f"{releasable},{file_sink._closed}"

    assert run_in_child(file_child).output == "True,True"
    messages = [json.loads(line)["message"] for line in path.read_text().splitlines()]
    assert messages.count("from-the-child") == 1, messages

    server, client = socket.socketpair()
    try:
        for sink in (_CountingSocketSink(client), RecordingSink()):
            log_foundry.configure(service="own", sink=sink)

            def other_child(sink: Any = sink) -> str:
                log_foundry.info("from-the-child")
                releasable = _lifecycle.releasable(sink)
                log_foundry.shutdown(3.0)
                closes = getattr(sink, "closes", getattr(sink, "closed", None))
                return f"{releasable},{closes}"

            assert run_in_child(other_child).output == "False,0", type(sink).__name__
        server.settimeout(5.0)
        assert b"from-the-child" in server.recv(4096), "the socket child delivered"
    finally:
        server.close()
        client.close()
