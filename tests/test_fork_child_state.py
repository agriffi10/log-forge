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
import time
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


class _ClosingSink:
    """A sink counting its emits and closes, so a child's close of an inherited one shows."""

    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []
        self.closes = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Keeps the batch."""
        self.events.extend(batch)

    def close(self) -> None:
        """Counts the close."""
        self.closes += 1


class _GetattrProxy:
    """A ``__getattr__`` proxy over a full sink."""

    def __init__(self, inner: object) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class _SubSink(_ClosingSink):
    pass


_KEEP: list[object] = []


def _proxied(shape: str) -> tuple[Any, _ClosingSink]:
    """One of FR-005's accepted shapes that no static read finds sink-shaped, and its target."""
    import weakref

    if shape == "super":
        sub = _SubSink()
        _KEEP.append(sub)
        return super(_SubSink, sub), sub
    inner = _ClosingSink()
    _KEEP.append(inner)
    if shape == "weakref-proxy":
        return weakref.proxy(inner), inner
    return _GetattrProxy(inner), inner


@pytest.mark.parametrize("path", ["orphan", "worker"])
@pytest.mark.parametrize("shape", ["weakref-proxy", "getattr-proxy", "super"])
def test_a_child_does_not_close_an_inherited_proxy_sink(shape: str, path: str) -> None:
    """Invariant 12: a proxy-shaped configured sink stays the parent's, so the child closes nothing.

    Recording a root runs none of its code, so ``stamp`` records the sink it was handed whatever
    the static sink-shape test says, and the child's walk records every live delivery sink. Without
    either, such a sink was unrecorded in a child whose walk succeeded, and the child's
    ``shutdown()`` closed the parent's transport.
    """
    configured, target = _proxied(shape)
    log_foundry.configure(service="proxy", sink=configured)
    if path == "worker":
        _traced()
    else:
        log_foundry.info("parent")
    log_foundry.flush(timeout=5.0)
    parent = os.getpid()
    with _lifecycle._state._owned_lock:
        recorded = _lifecycle._state._owned.get(id(configured))

    def in_child() -> str:
        record = _lifecycle._state._owned.get(id(configured), (None,))[0]
        releasable = _lifecycle.releasable(configured)
        log_foundry.shutdown(3.0)
        return f"record={'parent' if record == parent else record} releasable={releasable} closes={target.closes}"

    child = run_in_child(in_child)
    assert child.output.endswith("releasable=False closes=0"), child.output
    assert recorded is not None and recorded[0] == parent, (
        "the parent recorded the sink it was handed"
    )
    assert child.output == "record=parent releasable=False closes=0", child.output


def test_a_child_does_not_call_a_hook_on_a_class_a_wrapper_holds(tmp_path: pathlib.Path) -> None:
    """A package class held as an attribute is not a hook holder: the child writes no diagnostic.

    ``_is_owned`` accepts a package class, so a wrapper holding ``self.kind = FileSink`` once had
    every child call the unbound ``FileSink.reacquire_after_fork()``, absorb the ``TypeError`` and
    announce a re-acquisition failure that never happened.
    """
    import io
    import sys

    class _Wrapper:
        def __init__(self, inner: FileSink) -> None:
            self.inner = inner
            self.kind = FileSink

        def emit(self, batch: list[dict[str, object]]) -> None:
            self.inner.emit(batch)

        def close(self) -> None:
            self.inner.close()

    log_foundry.configure(service="cls", sink=_Wrapper(FileSink(str(tmp_path / "a.log"))))
    log_foundry.info("parent")
    log_foundry.flush(timeout=5.0)
    buffer = io.StringIO()
    saved = sys.stderr
    sys.stderr = buffer
    try:
        child = run_in_child(buffer.getvalue)
    finally:
        sys.stderr = saved
    assert child.finished, child.ending
    assert child.output == "", f"the child announced something: {child.output!r}"


def test_a_child_records_a_live_proxy_sink_the_parent_never_stamped() -> None:
    """The child's walk records every live delivery sink itself, whatever the parent's record says.

    A ``weakref.proxy`` bound into the config without ``stamp`` was never recorded by the parent,
    and as a root the walk's static shape test does not record it; only ``_live_sinks``'
    unconditional record of the configured sink keeps the child from claiming it.
    """
    from log_foundry import config

    configured, target = _proxied("weakref-proxy")
    config._rebind(sink=configured)
    with _lifecycle._state._owned_lock:
        assert id(configured) not in _lifecycle._state._owned, "the precondition: never stamped"

    def in_child() -> str:
        record = _lifecycle._state._owned.get(id(configured), (None,))[0]
        return f"record={record} releasable={_lifecycle.releasable(configured)}"

    child = run_in_child(in_child)
    assert child.output == f"record={_lifecycle._FOREIGN} releasable=False", child.output
    assert target.closes == 0


def test_a_file_sink_behind_super_is_repaired_in_the_child(tmp_path: pathlib.Path) -> None:
    """A ``super(Sub, fs)`` configured as the sink is transparent to the walk (SPEC-079).

    ``__self__`` is read through ``super``'s own member, in C, so the ``FileSink`` behind it is
    repaired and re-acquired in a child like one configured directly, and the child's first emit
    returns while a parent thread holds its lock. A ``weakref.proxy``'s referent cannot be reached
    without calling into it; that residual is recorded in ``architecture.md`` §13.
    """

    class _SubFile(FileSink):
        pass

    inner = _SubFile(str(tmp_path / "super.ndjson"))
    _KEEP.append(inner)
    log_foundry.configure(service="super", sink=super(_SubFile, inner))
    released, holder = _held_by_a_parent_thread(inner._lock)

    def in_child() -> str:
        log_foundry.info("child")
        record = _lifecycle._state._owned.get(id(inner), (None,))[0]
        return f"returned record={'child' if record == os.getpid() else record}"

    try:
        child = run_in_child(in_child, timeout=5)
    finally:
        released.set()
        holder.join(5.0)
    assert child.output == "returned record=child", f"{child.ending}: {child.output!r}"


class _NameRaises(type):
    """A metaclass whose ``__name__`` raises, as a hostile sink class's may."""

    @property
    def __name__(cls) -> str:  # type: ignore[override]
        raise RuntimeError("name")


def test_a_failing_hook_on_a_class_whose_name_raises_still_runs_the_handlers(
    tmp_path: pathlib.Path,
) -> None:
    """The hook-failure diagnostic names the type by its stored name, so it cannot escape.

    ``type(holder).__name__`` ran a metaclass property; raising there escaped the at-fork handler,
    so no handler ran and the child had no drain thread.
    """
    import io
    import sys

    parent = os.getpid()

    class _HookRaises(FileSink, metaclass=_NameRaises):
        def reacquire_after_fork(self) -> None:
            if os.getpid() != parent:
                raise OSError("cannot reopen")
            super().reacquire_after_fork()

    log_foundry.configure(service="hook", sink=_HookRaises(str(tmp_path / "h.log")))
    _traced()
    log_foundry.flush(timeout=5.0)
    buffer = io.StringIO()
    saved = sys.stderr
    sys.stderr = buffer

    def in_child() -> str:
        worker = _lifecycle._state.worker_exists()
        thread = None if worker is None else worker._thread
        return f"drain={bool(thread and thread.is_alive())}|{buffer.getvalue()}"

    try:
        child = run_in_child(in_child)
    finally:
        sys.stderr = saved
    drain, _, stderr = child.output.partition("|")
    assert drain == "drain=True", child.output
    assert "re-acquiring a transport after a fork (OSError)" in stderr, stderr
    assert "_HookRaises" in stderr, stderr


BACKOFF = 20.0
"""How long :class:`_BackingOff` waits on its stop signal: the wait the defect leaves uncut."""


class _BackingOff:
    """A third-party sink whose armed ``emit`` backs off on ``log_foundry_stop_signal``.

    Its state is plain lists and a bool rather than events, so the at-fork walk has nothing of
    its own to replace and the only signal in play is the one the library offered.
    """

    def __init__(self) -> None:
        self.log_foundry_stop_signal: threading.Event | None = None
        self.back_off = False
        self.entered: list[int] = []
        self.waited: list[float] = []

    def _back_off_once(self) -> None:
        signal = self.log_foundry_stop_signal
        if self.back_off and signal is not None and os.getpid() not in self.entered:
            self.entered.append(os.getpid())
            began = time.monotonic()
            signal.wait(BACKOFF)
            self.waited.append(time.monotonic() - began)

    def emit(self, batch: list[dict[str, Any]]) -> None:
        self._back_off_once()

    def flush(self) -> None:
        self._back_off_once()

    def close(self) -> None:
        pass


def _shutdown_cuts_an_orphan_backoff(sink: _BackingOff, via: str) -> str:
    """Runs in a child: a log or a flush backs off, then ``shutdown()`` must cut that wait short.

    Only the child's first entry backs off, so ``shutdown()`` reaching the sink itself does not.
    """
    no_worker = _lifecycle._state.live_worker() is None
    sink.back_off = True
    call = (lambda: log_foundry.info("child")) if via == "log" else (lambda: log_foundry.flush())
    emitter = threading.Thread(target=call, daemon=True)
    emitter.start()
    for _ in range(500):
        if os.getpid() in sink.entered:
            break
        time.sleep(0.01)
    inside = os.getpid() in sink.entered
    began = time.monotonic()
    log_foundry.shutdown(5.0)
    emitter.join(BACKOFF * 0.6)
    elapsed = time.monotonic() - began
    own = sink.log_foundry_stop_signal is _lifecycle._state._stop
    return (
        f"no_live_worker={no_worker} inside={inside} own_signal={own} "
        f"done={not emitter.is_alive()} cut={elapsed < BACKOFF / 2} elapsed={elapsed:.2f}"
    )


@pytest.mark.parametrize(
    ("depth", "retired", "via"),
    [(1, False, "log"), (2, False, "log"), (1, True, "log"), (1, False, "flush")],
    ids=["child", "grandchild", "retired-worker", "flush-first"],
)
def test_shutdown_in_a_workerless_child_cuts_short_an_orphan_backoff(
    depth: int, retired: bool, via: str
) -> None:
    """Invariant 3 on the orphan path: a child's ``shutdown()`` interrupts a sink's backoff.

    The parent's orphan log hands the sink the parent's stop signal; the roster gives the child a
    fresh ``_state._stop``. The child's orphan log took ``_note_orphan_emit``'s unlocked fast
    path — the sink was already owed and the child's event unset — so the sink kept the parent's
    copy, and ``shutdown()`` set an event nobody was waiting on: the emitting thread served the
    whole backoff, where an unforked process is cut short at once. The grandchild row is the same
    fork taken twice, so the repair must be re-armed by every fork rather than by the first.
    The retired-worker row forks after the parent's worker was shut down, so the child has a
    worker but no live one, and orphan logs are the only thing that reaches the sink. The
    flush-first row backs off in the sink's own ``flush``, reached by a ``flush()`` before the
    child logs anything: the twin of the log (invariant 6), which settling the debt only in the
    log left waiting on the parent's event.

    ``own_signal`` is the reason and ``cut`` the consequence: the ceiling is half the backoff
    the defect leaves uncut (``tests/README.md``).
    """
    sink = _BackingOff()
    _KEEP.append(sink)
    log_foundry.configure(service="orphan-backoff", sink=sink)
    if retired:
        _traced()
        log_foundry.shutdown(5.0)
    log_foundry.info("parent")
    assert (_lifecycle._state.worker_exists() is not None) is retired, "the precondition: a worker"
    assert _lifecycle._state.live_worker() is None, "the precondition: the orphan path"
    assert sink.log_foundry_stop_signal is _lifecycle._state._stop, "the parent's offer landed"

    def in_child() -> str:
        if depth == 1:
            return _shutdown_cuts_an_orphan_backoff(sink, via)
        grandchild = run_in_child(lambda: _shutdown_cuts_an_orphan_backoff(sink, via), timeout=20)
        return grandchild.output

    child = run_in_child(in_child, timeout=25)
    report = child.output
    assert report.startswith("no_live_worker=True inside=True "), f"{child.ending}: {report!r}"
    assert "own_signal=True" in report, f"the child's sink kept the parent's signal: {report!r}"
    assert "done=True cut=True" in report, f"the backoff was not cut short: {report!r}"
