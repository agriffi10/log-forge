"""SPEC-039 — what a forked child inherits, and what the library repairs in it.

Every test here forks for real. The two hazards are only observable across a genuine
``os.fork``: an inherited ``Lock`` is locked with *no owner*, which no in-process double
reproduces, and the child's repair runs from ``os.register_at_fork``, which nothing else fires.

**The window is constructed, never hoped for.** A fork landing after ``emit`` has returned
exercises the non-hazard — the lock is free and the buffer empty by construction — and an
earlier draft of this spec drew a false conclusion from exactly that. So the gate below parks the
drain thread *inside* a locked ``emit`` and the fork happens while it is there.

**Both sides are bounded.** Each child arms ``signal.alarm`` before doing anything, so one that
hangs — the whole subject of FR-003 — dies on its own rather than outliving the test run. That
alone is not enough, and the gap is the case that matters most: ``fork`` clears a pending alarm
in the child and the library's handler runs before any test code, so a repair that hangs *there*
produces a child no alarm can kill, and a parent waiting on the pipe would hang the suite rather
than fail one test. The parent holds its own deadline and sends ``SIGKILL``.
"""

from __future__ import annotations

import ast
import collections
import faulthandler
import io
import os
import pathlib
import select
import signal
import sys
import threading
import time
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry
from log_foundry import _fork, decorator
from log_foundry.sinks.base import SinkDeliveryError
from log_foundry.sinks.file import FileSink
from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.multi import MultiSink
from log_foundry.worker import Worker

if TYPE_CHECKING:
    from collections.abc import Callable

CHILD_TIMEOUT = 10

_PACKAGE = _fork._PACKAGE

_SRC = pathlib.Path(_fork.__file__).parent

_MODULES = sorted(_SRC.rglob("*.py"))

# A floor, for the reason SPEC-038 made floors the convention: an empty parameter set is a
# silent skip, not a failure, so a roster whose glob came back empty would look green. The
# number is well under the module count and moves only if files are deleted.
assert len(_MODULES) >= 40, f"the module roster collapsed to {len(_MODULES)}"


# -- forking, and reaping what was forked ---------------------------------------------------


class _Child:
    """What a forked child reported, once it has been reaped.

    Attributes:
      status: The raw ``os.waitpid`` status.
      output: Whatever the child wrote back down the pipe before exiting.
    """

    __slots__ = ("output", "status")

    def __init__(self, status: int, output: str) -> None:
        """Records one child's result.

        Args:
          status: The raw wait status.
          output: The bytes the child wrote, decoded.

        Returns:
          None.

        Raises:
          None.
        """
        self.status = status
        self.output = output

    @property
    def finished(self) -> bool:
        """Whether the child ran to its own exit rather than being killed by its watchdog.

        This is the assertion FR-003 AC-1 turns on: a child blocked on an inherited lock never
        reaches its own exit, and ``SIGALRM`` is what ends it.

        Args:
          None.

        Returns:
          Whether the child exited normally with status 0.

        Raises:
          None.
        """
        return os.WIFEXITED(self.status) and os.WEXITSTATUS(self.status) == 0

    @property
    def blocked(self) -> bool:
        """Whether a **watchdog** ended the child, which is what a deadlock looks like here.

        Either watchdog counts, and nothing else does. ``SIGALRM`` is the child's own, and
        ``SIGKILL`` is the parent's for the case the child's cannot fire — a repair that hangs
        *inside* the fork handler leaves a child with no pending alarm, since ``fork`` clears one
        and the handler runs before any test code can arm another.

        Any ``WIFSIGNALED`` is deliberately **not** the test. A child killed by ``SIGSEGV`` or
        ``SIGBUS`` would read as blocked and write nothing, which passes both assertions of the
        one test that reads this — the test whose whole job is to *demonstrate* the pre-fix hang.
        SPEC-028 already measured a sink taking the interpreter down with a bus error, so a crash
        reading as a deadlock is a live confusion here, not a hypothetical one.

        Args:
          None.

        Returns:
          Whether a watchdog signal terminated it.

        Raises:
          None.
        """
        return os.WIFSIGNALED(self.status) and os.WTERMSIG(self.status) in (
            signal.SIGALRM,
            signal.SIGKILL,
        )


def run_in_child(work: Callable[[], str | None], *, timeout: int = CHILD_TIMEOUT) -> _Child:
    """Forks, runs ``work`` in the child, and reaps it.

    The child writes ``work``'s return value down a pipe and leaves through ``os._exit``, so it
    never runs the parent's ``atexit`` handlers or pytest's teardown.

    **Both sides are bounded, and the parent's bound is not redundant.** The child arms
    ``signal.alarm`` for the ordinary case, but ``fork`` clears a pending alarm in the child and
    the library's fork handler runs *before* any of this code — so a repair that hangs in the
    handler produces a child no alarm will ever kill, and a parent sitting in ``os.read`` would
    hang the whole suite rather than fail one test. The parent therefore waits on a deadline of
    its own and sends ``SIGKILL``. The kill and the reap sit in the ``finally``, so a child is
    dealt with even when ``select`` or ``read`` raises — an unreaped child of a test process is
    a stray that outlives the run.

    Args:
      work: Called in the child, after the library's fork handler has run. Whatever it returns
        is sent back as the child's output.
      timeout: Seconds before the child's watchdog kills it. The parent allows a little more,
        so an alarm that *can* fire is what ends the child and the diagnosis stays specific.

    Returns:
      The reaped child.

    Raises:
      None.
    """
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:
        code = 1
        try:
            os.close(read_fd)
            signal.alarm(timeout)
            os.write(write_fd, (work() or "").encode())
            code = 0
        except BaseException as exc:
            try:
                os.write(write_fd, f"!{type(exc).__name__}: {exc}".encode())
            except BaseException:
                pass
        finally:
            os._exit(code)
    os.close(write_fd)
    deadline = time.monotonic() + timeout + 2.0
    chunks: list[bytes] = []
    gone = False
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            if not select.select([read_fd], [], [], remaining)[0]:
                continue
            chunk = os.read(read_fd, 65536)
            if not chunk:
                gone = True
                break
            chunks.append(chunk)
    finally:
        os.close(read_fd)
        if not gone:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        _, status = os.waitpid(pid, 0)
    return _Child(status, b"".join(chunks).decode(errors="replace"))


class _Gate:
    """One use of the window inside ``emit``: entered once, released once.

    Attributes:
      entered: Set once a thread is inside the locked region.
      release: Waited on there, and set by the test once it has forked.
    """

    def __init__(self) -> None:
        """Builds an unused gate.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.entered = threading.Event()
        self.release = threading.Event()


class _GatingStream:
    """Wraps a file object so one ``write`` can be parked, per the spec's method note.

    Interposing on the stream is what puts the fork *inside* ``FileSink.emit`` while its lock is
    held. The gate is consumed as it is taken, so a forked child — which inherits this object
    with the gate already spent — writes straight through instead of parking on an event nothing
    in that process will set.
    """

    def __init__(self, stream: Any) -> None:
        """Wraps a stream with no gate armed.

        Args:
          stream: The real file object to forward to.

        Returns:
          None.

        Raises:
          None.
        """
        self._stream = stream
        self.gate: _Gate | None = None

    def write(self, data: str) -> int:
        """Parks if a gate is armed, then forwards the write.

        Args:
          data: The text to write.

        Returns:
          How many characters were written.

        Raises:
          None.
        """
        gate = self.gate
        if gate is not None:
            self.gate = None
            gate.entered.set()
            gate.release.wait(CHILD_TIMEOUT)
        return int(self._stream.write(data))

    def flush(self) -> None:
        """Forwards the flush.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self._stream.flush()

    def close(self) -> None:
        """Forwards the close.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self._stream.close()

    def fileno(self) -> int:
        """Forwards the descriptor.

        Args:
          None.

        Returns:
          The underlying file descriptor.

        Raises:
          None.
        """
        return int(self._stream.fileno())


def _gated_file_sink(
    tmp_path: pathlib.Path, *, name: str = "events.ndjson"
) -> tuple[FileSink, _GatingStream]:
    """Builds a ``FileSink`` whose stream can be parked, and makes it the process sink.

    ``FileSink`` is used rather than a test double on purpose: the traversal descends only into
    instances this package defines (FR-003 AC-2), so a sink written here would not be repaired
    and every assertion below would be measuring the wrong object.

    Args:
      tmp_path: The directory to write into.
      name: The file to append to, so a test asserting on its contents can name it.

    Returns:
      The sink and the stream wrapper that arms its window.

    Raises:
      None.
    """
    sink = FileSink(str(tmp_path / name))
    stream = _GatingStream(sink._stream)
    sink._stream = stream  # type: ignore[assignment]
    log_foundry.configure(service="fork", version="0", env="test", sink=sink)
    return sink, stream


def _park_the_drain_thread(stream: _GatingStream, worker: Worker, seq: int) -> _Gate:
    """Submits one span and returns once the drain thread is inside the locked window.

    Args:
      stream: The wrapper whose gate is armed.
      worker: The worker whose drain thread will take the gate.
      seq: A marker written into the submitted event, so the parked write is identifiable.

    Returns:
      The armed gate, which the caller releases after forking.

    Raises:
      AssertionError: If the drain thread never reached the window, which would make every
        assertion built on it vacuous.
    """
    gate = _Gate()
    stream.gate = gate
    worker.submit([{"msg": f"parent-{seq}"}])
    assert gate.entered.wait(5.0), "the drain thread never reached the window it must fork in"
    return gate


def _log_in_child() -> str:
    """Makes the child's first log call, which is the call FR-003 is about.

    Args:
      None.

    Returns:
      A fixed marker, reached only if the call returned.

    Raises:
      None.
    """
    log_foundry.info("child")
    return "logged"


# -- FR-003: the inherited locks ------------------------------------------------------------


def test_an_inherited_lock_is_dead_in_the_child() -> None:
    """The hazard itself, measured, so nothing below is asserted against a window it missed.

    An inherited ``Lock`` stays locked with *no owner* — the thread holding it does not exist in
    the child — so ``acquire`` can never succeed. This is a plain ``threading.Lock`` the library
    never sees, which is the point: it demonstrates the primitive's behaviour rather than the
    fix's, and it is why re-initialising them is the library's own job.
    """
    lock = threading.Lock()
    holding = threading.Event()
    released = threading.Event()

    def hold() -> None:
        with lock:
            holding.set()
            released.wait(5.0)

    holder = threading.Thread(target=hold, daemon=True)
    holder.start()
    assert holding.wait(5.0)
    try:
        child = run_in_child(lambda: "acquired" if lock.acquire(timeout=1.0) else "dead")
    finally:
        released.set()
        holder.join(5.0)

    assert child.output == "dead", child.output


def test_a_failing_repair_is_absorbed_and_writes_no_message() -> None:
    """SPEC-025. The handler runs on a thread that has not returned from ``fork`` yet.

    An exception escaping it reaches CPython's unraisable hook, which prints a full traceback
    carrying the exception's **message** — the user data arch §6 keeps out of anything the
    library says about itself. Measured with the guard removed: a child's stderr carried the
    provoking value verbatim, and the whole suite stayed green.

    ``sys.stderr`` is replaced before the fork so the child inherits the buffer and can hand
    back what the handler wrote. Patching it here rather than in a fixture is deliberate —
    pytest's capture reverts a fixture's patch before the test body runs.
    """
    original = _fork._reinit_primitives

    def boom() -> None:
        raise RuntimeError("a-value-from-the-event-1234")

    buffer = io.StringIO()
    saved = sys.stderr
    _fork._reinit_primitives = boom  # type: ignore[assignment]
    sys.stderr = buffer
    try:
        child = run_in_child(buffer.getvalue, timeout=4)
    finally:
        sys.stderr = saved
        _fork._reinit_primitives = original  # type: ignore[assignment]

    assert child.finished, child.output
    assert "a-value-from-the-event-1234" not in child.output
    assert "RuntimeError" in child.output, child.output
    assert "absorbed a failure while repairing the library after a fork" in child.output


def test_a_crashed_child_does_not_read_as_blocked() -> None:
    """A crash and a deadlock are different findings, and one test turns on telling them apart.

    ``blocked`` is read by the demonstration half of FR-003 AC-1, which asserts that an
    un-repaired child hangs and writes nothing. A child killed by ``SIGSEGV`` satisfies both of
    those, so a check of "any signal" would let the demonstration pass without demonstrating
    anything — and SPEC-028 measured a shipped sink taking the interpreter down with a bus
    error, so this is a crash mode the suite has already seen.

    ``faulthandler`` is disabled in the child first. pytest enables it, and its ``SIGSEGV``
    dump would print a full traceback into the run's output — a passing test that looks like a
    catastrophe is its own kind of misleading.
    """
    def crash() -> str:
        faulthandler.disable()
        os.kill(os.getpid(), signal.SIGSEGV)
        return "survived"

    child = run_in_child(crash, timeout=4)
    assert not child.finished
    assert not child.blocked, "a crash must not read as a deadlock"


def test_the_childs_first_log_call_does_not_block(tmp_path: pathlib.Path) -> None:
    """FR-003 AC-1. Fifty forks, each landing inside a locked ``emit``, and every child returns.

    The drain thread is parked *inside* ``FileSink.emit`` holding its lock at the instant of each
    fork, so the child inherits a lock no thread will ever release. Its ``info()`` takes that same
    lock on the application's own thread, which is the deadlock this FR exists to remove: 19 of
    60 children hung in the audit's run, and this construction makes it 60 of 60.

    It stops after the third blocked child. A regression makes every iteration wait out the
    child's whole watchdog, and a run that takes 8.5 minutes to report a failure it was sure of
    within seconds is a run people learn to interrupt.
    """
    sink, stream = _gated_file_sink(tmp_path)
    worker = Worker(sink, batch_size=1, flush_interval=0.01)
    decorator._worker = worker

    blocked: list[int] = []
    for iteration in range(50):
        gate = _park_the_drain_thread(stream, worker, iteration)
        try:
            child = run_in_child(_log_in_child)
        finally:
            gate.release.set()
        if not child.finished:
            blocked.append(iteration)
        if len(blocked) >= 3:
            break

    assert not blocked, f"children blocked on an inherited lock: {blocked}"


def test_the_same_child_blocks_when_the_inherited_lock_is_put_back(
    tmp_path: pathlib.Path,
) -> None:
    """FR-003 AC-1's other half: the pre-fix behaviour is **demonstrated**, not asserted.

    The child puts back the very lock object it inherited — still held, still ownerless — over
    the repaired one, which is precisely the state the library shipped in before this spec. It
    then blocks in ``info()`` until its watchdog kills it. Without this, the test above could
    pass against a fork that never entered the window at all, which is the failure mode the
    spec's method note names.
    """
    sink, stream = _gated_file_sink(tmp_path)
    worker = Worker(sink, batch_size=1, flush_interval=0.01)
    decorator._worker = worker
    gate = _park_the_drain_thread(stream, worker, 0)
    inherited = sink._lock

    def unrepair_then_log() -> str:
        sink._lock = inherited  # type: ignore[assignment]
        return _log_in_child()

    try:
        child = run_in_child(unrepair_then_log, timeout=4)
    finally:
        gate.release.set()

    assert child.blocked, f"the un-repaired child did not block: {child.output!r}"
    assert child.output == ""


def test_a_users_subclass_of_a_shipped_sink_is_repaired(tmp_path: pathlib.Path) -> None:
    """FR-003 AC-2. The ownership boundary is about *whose code built the lock*, not whose file.

    Subclassing a shipped sink is a documented extension point — the README offers ``Sink`` to
    subclass, and SPEC-038 rebuilt ``HTTPSink.emit`` as a template method precisely so subclasses
    override its hooks. An instance of one reports its own module, so an ownership test keyed on
    the defining module walked straight past a ``_lock`` that ``FileSink.__init__`` built:
    measured, this child hung in ``info()`` while a plain ``FileSink`` in the same probe
    returned. That is the 19-of-60 hang coming back through the one door users are told to use.
    """

    class _UserSink(FileSink):
        pass

    sink = _UserSink(str(tmp_path / "sub.ndjson"))
    stream = _GatingStream(sink._stream)
    sink._stream = stream  # type: ignore[assignment]
    log_foundry.configure(service="fork", version="0", env="test", sink=sink)
    worker = Worker(sink, batch_size=1, flush_interval=0.01)
    decorator._worker = worker
    gate = _park_the_drain_thread(stream, worker, 0)
    try:
        child = run_in_child(_log_in_child, timeout=4)
    finally:
        gate.release.set()

    assert child.finished, f"a subclassed sink's child blocked: {child.output!r}"


def test_a_lock_that_was_never_held_is_replaced_too() -> None:
    """FR-003 AC-6. Asking whether a lock is held has no answer that is not itself a race.

    ``threading.Lock`` exposes no non-destructive "is it held" test — ``acquire(blocking=False)``
    answers by taking it — so re-initialising only the held ones is not implementable. Identity
    is what shows the unheld one moved.
    """
    before = id(decorator._worker_lock)
    child = run_in_child(lambda: str(id(decorator._worker_lock) != before))
    assert child.output == "True", child.output


def test_two_holders_of_one_event_still_share_it_in_the_child() -> None:
    """FR-003 AC-4. The ``log_foundry_stop_signal`` / ``Worker._stop`` pair, named.

    A sink's stop signal **is** the worker's ``_stop`` (SPEC-027). Two fresh events would leave
    the worker setting one and the sink waiting on the other, so a shutdown would stop cutting a
    backoff short — a fork fix that silently un-fixes an earlier spec. ``HTTPSink`` is the sink
    here because it carries the attribute and needs no network to construct one.
    """
    sink = HTTPSink("http://127.0.0.1:1/ingest", opener=lambda *a, **k: None)
    worker = Worker(sink, batch_size=1, flush_interval=0.01)
    decorator._worker = worker
    assert sink.log_foundry_stop_signal is worker._stop

    def compare() -> str:
        live = decorator._worker
        return f"{sink.log_foundry_stop_signal is live._stop},{live._stop is not None}"

    child = run_in_child(compare)
    assert child.output == "True,True", child.output


def test_a_set_event_is_still_set_in_the_child() -> None:
    """FR-003 AC-5. An ``Event`` carries its set state across, or the child un-does a shutdown.

    ``decorator._orphan_stop`` is set by ``shutdown()`` and an ``Event`` never clears. A
    replacement that started unset would tell a child's sink to go on backing off for delivery
    that has already been retired, which is SPEC-033 FR-004's reasoning inherited by a fork.
    """
    decorator._orphan_stop.set()
    before = id(decorator._orphan_stop)
    child = run_in_child(
        lambda: f"{decorator._orphan_stop.is_set()},{id(decorator._orphan_stop) != before}"
    )
    assert child.output == "True,True", child.output


def test_the_walk_reaches_a_lock_held_inside_a_container(tmp_path: pathlib.Path) -> None:
    """FR-003 AC-2. ``MultiSink._sinks`` is a container, and the locks that matter are inside it.

    A traversal that entered only attributes would leave every child of a ``MultiSink`` holding a
    dead lock while the wrapper itself looked repaired — a fan-out is exactly where one dead lock
    stops several destinations.
    """
    inner = FileSink(str(tmp_path / "inner.ndjson"))
    wrapper = MultiSink(inner)
    log_foundry.configure(service="fork", version="0", env="test", sink=wrapper)
    before = id(inner._lock)
    try:
        child = run_in_child(lambda: str(id(inner._lock) != before))
    finally:
        inner.close()
    assert child.output == "True", child.output


def test_third_party_state_is_left_alone(tmp_path: pathlib.Path) -> None:
    """FR-003 AC-2's other side: the walk stops at the ownership boundary (FR-005).

    A driver's locks, threads and descriptors are not the library's to swap, and a handler that
    reached into them would break the driver rather than the fork. The cost is stated rather than
    hidden: a **third-party sink**'s own lock is not repaired either, which arch §13 records and
    which the worker rebuild's re-offer of the stop signal is what keeps from spreading.
    """

    class _ForeignSink:
        def __init__(self) -> None:
            self.lock = threading.Lock()

        def emit(self, batch: list[dict[str, object]]) -> None:
            pass

        def close(self) -> None:
            pass

    foreign = _ForeignSink()
    log_foundry.configure(service="fork", version="0", env="test", sink=foreign)
    before = id(foreign.lock)
    child = run_in_child(lambda: str(id(foreign.lock) == before))
    assert child.output == "True", child.output


# -- FR-002: the child's worker is rebuilt, not retired --------------------------------------


def _idle_worker(sink: FileSink) -> Worker:
    """Installs a worker that will not drain on its own, so submissions stay queued.

    A long interval and a large batch are what make "the parent's backlog" a fact of the test
    rather than a race against the drain loop — and the backlog is the subject of AC-2.

    Args:
      sink: The sink to deliver through.

    Returns:
      The installed worker.

    Raises:
      None.
    """
    worker = Worker(sink, batch_size=1000, flush_interval=100.0)
    decorator._worker = worker
    return worker


def test_a_child_that_logs_after_forking_delivers(tmp_path: pathlib.Path) -> None:
    """FR-002 AC-1. The child inherits a ``Worker`` whose thread does not exist.

    ``submit`` went on enqueueing and nothing drained: measured, six events never delivered,
    ``atexit`` closing the sink without a drain, and ``health()`` reading ``queued=2`` with
    ``dropped``, ``failed_batches``, ``stopped_reason`` and ``retired`` all clean — the
    documented alert idiom blind on every term.
    """
    path = tmp_path / "child.ndjson"
    sink = FileSink(str(path))
    log_foundry.configure(service="fork", version="0", env="test", sink=sink)
    _idle_worker(sink)

    def work_in_child() -> str:
        @log_foundry.trace
        def child_span() -> None:
            pass

        child_span()
        return str(bool(log_foundry.flush(timeout=5.0)))

    child = run_in_child(work_in_child, timeout=8)
    assert child.output == "True", child.output
    assert "child_span" in path.read_text(encoding="utf-8")


def test_the_parents_backlog_is_not_delivered_twice(tmp_path: pathlib.Path) -> None:
    """FR-002 AC-2. The child starts empty and the parent keeps what was in flight.

    Both halves are asserted across the two processes, because either alone is satisfiable by
    the wrong fix: a child that inherited the queue would deliver the parent's backlog a second
    time, and a child that *drained* it would take those events away from the parent.

    **The backlog has to be in the queue at the instant of the fork, and a quiet worker does
    not put it there.** A long ``flush_interval`` stops the drain thread *emitting*; it does not
    stop it *dequeuing*, and ``_drain``'s ``get`` pulls every submission into a local within
    microseconds — measured, ``qsize`` 5 → 0 in 10 ms. A first version relied on that quiet
    worker and killed the mutation 4 times in 10: when the drain thread won, the events sat in
    its ``pending`` list, where a child inheriting the queue duplicates nothing. So the thread
    is parked *inside* ``emit`` first, and ``qsize`` is asserted before the fork as a
    sensitivity precondition rather than assumed.
    """
    path = tmp_path / "split.ndjson"
    sink, stream = _gated_file_sink(tmp_path, name="split.ndjson")
    worker = Worker(sink, batch_size=1, flush_interval=0.01)
    decorator._worker = worker
    gate = _park_the_drain_thread(stream, worker, 0)
    for index in range(5):
        worker.submit([{"msg": f"backlog-{index}"}])
    assert worker._queue.qsize() == 5, "the backlog never reached the queue; this proves nothing"

    def work_in_child() -> str:
        queued = log_foundry.health().queued
        decorator._worker.submit([{"msg": "child-0"}])
        return f"{queued},{bool(log_foundry.flush(timeout=5.0))}"

    try:
        child = run_in_child(work_in_child, timeout=8)
    finally:
        gate.release.set()
    assert child.output == "0,True", child.output

    assert log_foundry.flush(timeout=5.0)
    written = path.read_text(encoding="utf-8")
    for index in range(5):
        assert written.count(f"backlog-{index}") == 1, written
    assert written.count("child-0") == 1, written


class _FailingSink:
    """A sink that refuses everything, so the worker's ``failed_batches`` moves for real.

    Total failure is the signal SPEC-026 requires a sink to raise, which is what drives the
    worker's retry and then its counter.
    """

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Refuses the batch.

        Args:
          batch: Ignored.

        Returns:
          None.

        Raises:
          SinkDeliveryError: Always.
        """
        raise SinkDeliveryError("refusing everything on purpose")

    def close(self) -> None:
        """Releases nothing.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """


def test_the_childs_counters_describe_the_child(tmp_path: pathlib.Path) -> None:
    """FR-002 AC-3. Inherited counters describe a drain thread that no longer exists.

    Three of the four are driven for real rather than assigned — ``dropped`` by overflowing a
    queue of one, ``failed_batches`` by a sink that refuses everything, and
    ``submitted_after_shutdown`` by logging past a ``shutdown()``. A first version drove only
    ``dropped`` and asserted the rest at zero, which is what the parent already read: three of
    its four cells could not fail.

    ``incomplete_swaps`` is the exception and is set directly, with the reason stated rather
    than hidden. Driving it needs a swap whose drain cannot be confirmed, which is a second
    sink, a parked drain thread and a timeout — machinery that would make this test about
    SPEC-030's swap rather than about the fork. Arranging a *precondition* is not the vacuity
    this suite guards against; arranging the expectation would be.
    """
    sink = _FailingSink()
    log_foundry.configure(service="fork", version="0", env="test", sink=sink)
    worker = Worker(sink, batch_size=1, flush_interval=0.01, max_queue=1, max_retries=0)
    decorator._worker = worker
    for index in range(200):
        worker.submit([{"msg": f"overflow-{index}"}])
    deadline = time.monotonic() + 5.0
    while worker.failed_batches == 0 and time.monotonic() < deadline:
        time.sleep(0.01)
    worker.incomplete_swaps = 3
    log_foundry.shutdown()
    worker.submit([{"msg": "after-shutdown"}])

    assert worker.dropped > 0, "the parent's dropped never moved, so zeroing proves nothing"
    assert worker.failed_batches > 0, "the parent's failed_batches never moved"
    assert worker.submitted_after_shutdown > 0, "the parent's post-shutdown counter never moved"

    def report() -> str:
        health = log_foundry.health()
        return (
            f"{health.dropped},{health.failed_batches},{health.queued},"
            f"{health.submitted_after_shutdown},{health.incomplete_swaps}"
        )

    child = run_in_child(report, timeout=6)
    assert child.output == "0,0,0,0,0", child.output
    assert worker.dropped > 0, "the parent's counters were zeroed too"


def test_a_child_of_a_dead_drain_thread_reports_its_own_health(tmp_path: pathlib.Path) -> None:
    """FR-002 AC-3 and AC-6, against the state that makes both of them bite.

    A parent whose drain thread ended terminally carries ``stopped_reason`` set and both drain
    events set, with ``retired`` still ``False`` — so the child rebuilds. Inheriting any of that
    leaves a **working** child reporting the parent's dead-thread reason forever, latching
    SPEC-019's alert term, and reading ``draining`` as ``False`` while its own thread runs.

    This is the state that covers the clearing statements at all: a parent forked while healthy
    has ``stopped_reason`` at ``None`` and both events clear already, so every assertion about
    them is satisfied by doing nothing.
    """
    path = tmp_path / "terminal.ndjson"
    real = FileSink(str(path))

    class _DiesOnce:
        def __init__(self) -> None:
            self.raised = False

        def emit(self, batch: list[dict[str, object]]) -> None:
            if not self.raised:
                self.raised = True
                raise SystemExit("the drain thread ends here")
            real.emit(batch)

        def close(self) -> None:
            real.close()

    sink = _DiesOnce()
    log_foundry.configure(service="fork", version="0", env="test", sink=sink)
    worker = Worker(sink, batch_size=1, flush_interval=0.01)
    decorator._worker = worker
    worker.submit([{"msg": "kills-the-thread"}])
    deadline = time.monotonic() + 5.0
    while worker.stopped_reason is None and time.monotonic() < deadline:
        time.sleep(0.01)
    assert worker.stopped_reason == "SystemExit", worker.stopped_reason
    assert not worker.draining, "the parent's drain never settled, so this proves nothing"

    def report() -> str:
        live = decorator._worker
        live.submit([{"msg": "child-lives"}])
        delivered = bool(log_foundry.flush(timeout=5.0))
        return f"{log_foundry.health().stopped_reason},{live.draining},{delivered}"

    child = run_in_child(report, timeout=8)
    assert child.output == "None,True,True", child.output
    assert "child-lives" in path.read_text(encoding="utf-8")
    assert worker.stopped_reason == "SystemExit", "the parent's reason was cleared too"


def test_a_retired_parent_forks_a_retired_child(tmp_path: pathlib.Path) -> None:
    """FR-002 AC-4. A fork does not undo a ``shutdown()``.

    The alternative silently revives a worker the caller terminated, which is the library
    overruling an explicit instruction. The child still gets a **fresh queue**, though: a
    retired worker goes on accepting submissions (SPEC-030 FR-001), so an inherited
    ``queue.Queue`` mutex would block the child's next ``submit`` on the application's thread.

    **The thread has to be observed directly, and every indirect signal was tried first.** The
    outcome cannot separate the two worlds: ``health().retired`` reads a flag the child inherits
    either way, ``_thread.is_alive()`` reads ``False`` in both — a resumed thread whose ``_stop``
    is already set leaves within microseconds — and the queue stays put in both, because that
    thread has finished its one final drain before the child's code runs at all. Measured:
    starting the thread regardless left an earlier version of this test green on every one of
    those assertions. So ``Worker._run`` is instrumented to announce itself, which is the fact
    ``resume`` actually governs.

    The wait is a bounded negative, the one thing no synchronization primitive expresses: there
    is no event for "nothing will ever happen". Its soundness is the gap — a started thread
    announces itself in milliseconds, against a second of waiting.
    """
    path = tmp_path / "retired.ndjson"
    sink = FileSink(str(path))
    log_foundry.configure(service="fork", version="0", env="test", sink=sink)
    worker = _idle_worker(sink)
    log_foundry.shutdown()

    started = threading.Event()
    original_run = Worker._run

    def announcing_run(self: Worker) -> None:
        started.set()
        original_run(self)

    Worker._run = announcing_run  # type: ignore[method-assign]

    def report() -> str:
        live = decorator._worker
        live.submit([{"msg": "after-the-fork"}])
        health = log_foundry.health()
        drained = started.wait(1.0)
        return f"{health.retired},{health.queued},{drained}"

    try:
        child = run_in_child(report, timeout=8)
    finally:
        Worker._run = original_run  # type: ignore[method-assign]

    assert child.output == "True,1,False", child.output
    assert worker.retired
    assert path.read_text(encoding="utf-8") == "", "the retired child delivered"


def test_a_retired_child_does_not_pay_the_shutdown_budget_at_exit(tmp_path: pathlib.Path) -> None:
    """FR-002 AC-4's other half: a retired child must not wait for a drain that cannot happen.

    ``Worker.shutdown``'s idempotent path waits on ``_drain_settled``, and a child forked while
    a ``shutdown()`` is mid-join inherits it **unset** with no thread that will ever set it.
    Measured before the fix: the child paid the whole 30 s budget at exit, and with
    ``shutdown(timeout=None)`` — which the API documents as supported — it would never have
    exited at all. Retiring therefore *sets* both drain events rather than leaving them.

    The window is constructed: the drain thread is parked inside ``emit`` so the parent's
    ``shutdown()`` is genuinely still joining when the fork happens.

    **Both events are asserted, not just the one the elapsed time can see.** ``_drain_settled``
    set with ``_drain_finished`` clear is what ``draining``'s own docstring defines as an
    *abandoned* drain — a wedged thread the shutdown gave up on — and a child that reports
    ``stopped_reason=None`` must not read that way to an operator. Nothing consumes
    ``_drain_finished`` on a retired worker today, so the elapsed time cannot distinguish it and
    a first version of this test left that statement unkillable while the commit claimed
    otherwise; the state is asserted directly instead.
    """
    sink, stream = _gated_file_sink(tmp_path, name="mid-shutdown.ndjson")
    worker = Worker(sink, batch_size=1, flush_interval=0.01)
    decorator._worker = worker
    gate = _park_the_drain_thread(stream, worker, 0)

    shutting_down = threading.Thread(target=lambda: worker.shutdown(30.0), daemon=True)
    shutting_down.start()
    deadline = time.monotonic() + 5.0
    while not worker.retired and time.monotonic() < deadline:
        time.sleep(0.01)
    assert worker.retired and worker.draining, "the fork must land while the shutdown is joining"

    def report() -> str:
        live = decorator._worker
        started = time.monotonic()
        log_foundry.shutdown(timeout=30.0)
        prompt = time.monotonic() - started < 5.0
        return f"{prompt},{live._drain_settled.is_set()},{live._drain_finished.is_set()}"

    try:
        child = run_in_child(report, timeout=12)
    finally:
        gate.release.set()
        shutting_down.join(10.0)

    assert child.output == "True,True,True", (
        f"the retired child waited out a drain that cannot run, or reads as abandoned: {child}"
    )


def test_a_retired_childs_sink_is_not_left_backing_off_at_zero(tmp_path: pathlib.Path) -> None:
    """The retired path's drain events reach further than the exit budget they were added for.

    ``_offer_orphan_signal`` skips handing a fresh signal when ``worker.sink is sink and
    worker.draining`` (SPEC-035 FR-001). A retired child that inherited ``_drain_settled``
    **unset** reads as draining, so the skip applies, and the sink keeps the parent's **set**
    ``_stop`` — every ``sinks/_retry`` backoff then returns instantly, which against a
    rate-limited destination is SPEC-033 FR-004's tight retry loop. Measured in both directions.

    This is a second, independent consequence of the same statement, and a stronger one than the
    exit budget: a wedged exit is slow, and a collapsed backoff hammers a destination that is
    already refusing.

    **The window is a shutdown still joining, not a shutdown that finished.** A completed
    ``shutdown()`` leaves ``_drain_settled`` set in the parent, so the child inherits the right
    answer and the statement under test does nothing — a first version of this test did exactly
    that and passed in both worlds. The drain thread is parked inside ``emit`` and the shutdown
    runs on its own thread, so the fork lands while ``_drain_settled`` is genuinely unset.
    """

    class _SignalGateSink:
        def __init__(self) -> None:
            self.log_foundry_stop_signal: threading.Event | None = None
            self.gate: _Gate | None = None

        def emit(self, batch: list[dict[str, object]]) -> None:
            gate = self.gate
            if gate is not None:
                self.gate = None
                gate.entered.set()
                gate.release.wait(CHILD_TIMEOUT)

        def close(self) -> None:
            pass

    sink = _SignalGateSink()
    log_foundry.configure(service="fork", version="0", env="test", sink=sink)
    worker = Worker(sink, batch_size=1, flush_interval=0.01)
    decorator._worker = worker
    gate = _Gate()
    sink.gate = gate
    worker.submit([{"msg": "parks the drain thread"}])
    assert gate.entered.wait(5.0), "the drain thread never parked, so the fork misses the window"

    shutting_down = threading.Thread(target=lambda: worker.shutdown(30.0), daemon=True)
    shutting_down.start()

    def parent_ready() -> bool:
        # `retired` latches before `_stop` is set, so polling on `retired` alone and asserting
        # the signal afterwards has a window between the two where the precondition is false.
        signal = sink.log_foundry_stop_signal
        return worker.retired and signal is not None and signal.is_set()

    deadline = time.monotonic() + 5.0
    while not parent_ready() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert parent_ready(), "the parent's signal must be set to prove this"
    assert worker.draining, "the fork must land while the shutdown is joining"

    def report() -> str:
        log_foundry.info("an orphan log in a retired child")
        live = sink.log_foundry_stop_signal
        return f"{decorator._worker.draining},{live is not None and live.is_set()}"

    try:
        child = run_in_child(report, timeout=6)
    finally:
        gate.release.set()
        shutting_down.join(10.0)

    assert child.output == "False,False", child.output


def test_a_third_party_sink_is_handed_the_childs_own_stop_signal() -> None:
    """FR-002 with SPEC-027: the rebuild re-offers, because the walk cannot reach every holder.

    A sink's ``log_foundry_stop_signal`` **is** the worker's ``_stop``, and FR-003's memo keeps
    that pairing for a sink the traversal enters. A **third-party** sink is outside its
    ownership boundary, so its attribute keeps pointing at the pre-fork event while the worker
    gets a fresh one — the shutdown would then set an event nothing waits on, and the sink's
    backoff would never be cut short. That is SPEC-027's guarantee broken by the repair meant to
    preserve it, in the one sink shape the walk is deliberately blind to.
    """

    class _ForeignSink:
        def __init__(self) -> None:
            self.log_foundry_stop_signal: threading.Event | None = None

        def emit(self, batch: list[dict[str, object]]) -> None:
            pass

        def close(self) -> None:
            pass

    sink = _ForeignSink()
    log_foundry.configure(service="fork", version="0", env="test", sink=sink)
    worker = Worker(sink, batch_size=1, flush_interval=0.01)
    decorator._worker = worker
    assert sink.log_foundry_stop_signal is worker._stop

    def report() -> str:
        live = decorator._worker
        signal_now = sink.log_foundry_stop_signal
        return f"{signal_now is live._stop},{signal_now is not None and not signal_now.is_set()}"

    child = run_in_child(report, timeout=6)
    assert child.output == "True,True", child.output


def test_the_rebuilt_worker_is_the_same_object(tmp_path: pathlib.Path) -> None:
    """FR-002 AC-5. Rebuilding as a new object is the obvious implementation and breaks guards.

    Every ownership guard SPEC-033 shipped is an identity comparison — ``_worker.sink is X``,
    and ``_lifecycle``'s registry — so a replacement worker leaves each of them answering about
    an object nothing else refers to. The queue is asserted to have moved in the same breath,
    since "same object" must not be satisfied by rebuilding nothing at all.
    """
    sink = FileSink(str(tmp_path / "identity.ndjson"))
    log_foundry.configure(service="fork", version="0", env="test", sink=sink)
    worker = _idle_worker(sink)
    worker_id = id(worker)
    queue_id = id(worker._queue)

    def report() -> str:
        live = decorator._worker
        return f"{id(live) == worker_id},{id(live._queue) != queue_id},{live.sink is sink}"

    child = run_in_child(report, timeout=6)
    assert child.output == "True,True,True", child.output


def test_the_rebuild_records_no_stopped_reason(tmp_path: pathlib.Path) -> None:
    """FR-002 AC-6. ``"Forked"`` reads as the more honest answer and is not.

    SPEC-019 defines ``stopped_reason`` as "the drain thread died", and the alert idiom built on
    it treats a reason as a delivery outage. A child that is delivering must not report one.
    """
    sink = FileSink(str(tmp_path / "reason.ndjson"))
    log_foundry.configure(service="fork", version="0", env="test", sink=sink)
    _idle_worker(sink)

    child = run_in_child(lambda: str(log_foundry.health().stopped_reason), timeout=6)
    assert child.output == "None", child.output


# -- FR-001 AC-2 / FR-006: the order of work, and where it is registered from ----------------


def test_a_handler_runs_after_the_locks_are_the_childs_own(tmp_path: pathlib.Path) -> None:
    """FR-001 AC-2. The order is the contract: locks first, then the registered handlers.

    A lock re-initialised *after* a handler that takes it is a handler that hangs — on the
    child's only thread, with nothing left to interrupt it. The probe records what it saw rather
    than asserting in the child, so a wrong order is a reported value rather than a deadlock the
    test would have to survive to report.

    It also **releases** what it takes. A handler is not alone once an earlier one has started a
    thread, which ``decorator``'s rebuild does, so a probe holding a sink's transport lock past
    its own return would park the drain thread at the first emit.
    """
    sink = FileSink(str(tmp_path / "order.ndjson"))
    log_foundry.configure(service="fork", version="0", env="test", sink=sink)
    _idle_worker(sink)
    lock_before = id(sink._lock)
    seen: list[str] = []

    def probe() -> None:
        taken = sink._lock.acquire(timeout=1.0)
        if taken:
            sink._lock.release()
        seen.append(f"{id(sink._lock) != lock_before},{taken}")

    _fork.register_child_handler(probe)
    try:
        child = run_in_child(lambda: ",".join(seen), timeout=6)
    finally:
        _fork._child_handlers.remove(probe)

    assert child.output == "True,True", child.output


def test_one_handler_failing_does_not_stop_the_others(tmp_path: pathlib.Path) -> None:
    """SPEC-025 at the registry level: a child that cannot rebuild still has working locks.

    Handlers are independent pieces of repair, so one raising must not take the rest with it —
    and the failure is announced by type, never by message (arch §6).
    """
    sink = FileSink(str(tmp_path / "isolated.ndjson"))
    log_foundry.configure(service="fork", version="0", env="test", sink=sink)
    reached: list[str] = []

    def boom() -> None:
        raise RuntimeError("a-value-from-the-event-9876")

    def after() -> None:
        reached.append("ran")

    buffer = io.StringIO()
    saved = sys.stderr
    _fork.register_child_handler(boom)
    _fork.register_child_handler(after)
    sys.stderr = buffer
    try:
        child = run_in_child(lambda: f"{reached},{buffer.getvalue()}", timeout=6)
    finally:
        sys.stderr = saved
        _fork._child_handlers.remove(boom)
        _fork._child_handlers.remove(after)

    assert "['ran']" in child.output, child.output
    assert "a-value-from-the-event-9876" not in child.output
    assert "absorbed a failure while running a fork handler (RuntimeError)" in child.output


def test_a_child_of_a_process_that_built_no_worker_is_silent(tmp_path: pathlib.Path) -> None:
    """FR-002. A process that only ever logged outside a span has nothing to rebuild.

    The existence guard is what keeps that quiet. Without it the rebuild takes an
    ``AttributeError`` on ``None.retired``, which ``_fork`` absorbs into a stderr line on
    **every fork** — a library announcing a fault of its own invention, on a path where nothing
    was wrong (SPEC-025).
    """
    sink = FileSink(str(tmp_path / "orphan.ndjson"))
    log_foundry.configure(service="fork", version="0", env="test", sink=sink)
    log_foundry.info("no span, so no worker")
    assert decorator._worker is None, "this test needs a process that never built a worker"

    buffer = io.StringIO()
    saved = sys.stderr
    sys.stderr = buffer
    try:
        child = run_in_child(lambda: buffer.getvalue(), timeout=6)
    finally:
        sys.stderr = saved

    assert child.finished, child.output
    assert child.output == "", f"the child announced something: {child.output!r}"


def test_registering_the_same_handler_twice_is_a_no_op() -> None:
    """FR-006 AC-2's exposure at the registry rather than at ``os.register_at_fork``.

    A reload of a module whose body registers here would otherwise stack a second handler, and
    for the worker rebuild that means two drain threads in the child — the first bound to a
    queue nothing will ever write to. ``install()`` records the same exposure for the fork
    registration itself; this closes it for the handlers.
    """
    before = list(_fork._child_handlers)
    _fork.register_child_handler(decorator._rebuild_worker_after_fork)
    assert _fork._child_handlers == before

    def fresh() -> None:
        pass

    _fork.register_child_handler(fresh)
    _fork.register_child_handler(fresh)
    try:
        assert _fork._child_handlers.count(fresh) == 1
    finally:
        _fork._child_handlers.remove(fresh)


def test_a_registered_object_that_refuses_comparison_does_not_raise() -> None:
    """The dedupe compares by **identity**, and only a hostile ``__eq__`` can show it.

    ``list.__contains__`` short-circuits on identity, so registering plain functions cannot
    distinguish ``in`` from ``any(h is fn)`` — the test above passes against either, which is
    how the identity refinement shipped with a commit message claiming it was mutation-killed.
    A registry compares objects it does not own, and ``in`` asks their ``__eq__``: here that
    raises out of a function documented to raise nothing.
    """

    class _Hostile:
        def __eq__(self, other: object) -> bool:
            raise ValueError("its __eq__ is not this registry's to trust")

        def __hash__(self) -> int:
            return 1

        def __call__(self) -> None:
            pass

    def plain() -> None:
        pass

    hostile = _Hostile()
    _fork.register_child_handler(hostile)
    try:
        _fork.register_child_handler(plain)
        assert sum(1 for handler in _fork._child_handlers if handler is plain) == 1
    finally:
        # `remove` would ask `__eq__` too, which is the very thing under test.
        _fork._child_handlers[:] = [
            handler
            for handler in _fork._child_handlers
            if handler is not hostile and handler is not plain
        ]


def test_a_child_that_cannot_start_a_thread_reports_it_rather_than_raising(
    tmp_path: pathlib.Path,
) -> None:
    """A rebuild that cannot get a thread must leave a worker a later ``shutdown()`` survives.

    ``__init__`` never had to consider this: a constructor whose ``start`` raises lets no
    ``Worker`` escape. Here the worker is already the process's, so assigning the unstarted
    ``Thread`` first would leave it reading ``draining`` forever and hand the next
    ``shutdown()`` a ``RuntimeError`` from ``join`` — out of a public call documented to raise
    nothing, and through ``atexit``, where CPython prints the message arch §6 keeps out of
    anything the library says about itself.
    """
    sink = FileSink(str(tmp_path / "nothreads.ndjson"))
    log_foundry.configure(service="fork", version="0", env="test", sink=sink)
    _idle_worker(sink)

    original_start = threading.Thread.start

    def refusing(self: threading.Thread) -> None:
        raise RuntimeError("no threads left")

    def report() -> str:
        health = log_foundry.health()
        log_foundry.shutdown(timeout=5.0)
        # `queued` after the shutdown is what covers the drain events being set here: with
        # `_drain_finished` clear, `shutdown` queues its sentinel into a queue no thread will
        # ever read, and it stays counted for the life of the process.
        return f"{health.stopped_reason},{decorator._worker.draining},{log_foundry.health().queued}"

    threading.Thread.start = refusing  # type: ignore[method-assign]
    try:
        child = run_in_child(report, timeout=6)
    finally:
        threading.Thread.start = original_start  # type: ignore[method-assign]

    assert child.finished, f"the child's shutdown raised: {child.output!r}"
    assert child.output == "RuntimeError,False,0", child.output


def test_the_worker_rebuild_is_registered_rather_than_reached_for() -> None:
    """FR-006. ``decorator`` registers with ``_fork``; ``_fork`` does not import ``decorator``.

    The import test above states the rule; this states that the rule is being *used* rather
    than satisfied by a module that simply does nothing yet. A registry with no registrations
    would pass every import assertion ever written.
    """
    assert decorator._rebuild_worker_after_fork in _fork._child_handlers


# -- FR-003 AC-3: completeness is proved, not asserted ---------------------------------------


_REPAIRABLE = ("Lock", "RLock", "Event")

# `threading` primitives the walk does **not** know how to replace. They are detected anyway, so
# that adding one is a decision somebody takes rather than a silent hole: a `Condition` owns a
# lock, so an inherited one reintroduces exactly the hang FR-003 removes, and a `Semaphore` or
# `Barrier` carries a count that a naive replacement would reset. `src/` has none today.
_UNREPAIRABLE = ("Condition", "Semaphore", "BoundedSemaphore", "Barrier")

_PRIMITIVES = _REPAIRABLE + _UNREPAIRABLE


def _threading_names(tree: ast.AST) -> tuple[set[str], dict[str, str]]:
    """The names this module can build a primitive through, derived from its own imports.

    A detector hardcoding the literal ``threading`` reads zero constructions from
    ``from threading import Lock`` and from ``import threading as th`` — measured, both green
    with the lock in a list the walk cannot reach. Reading the imports is what makes the rule
    about the *primitive* rather than about one spelling of it.

    Four shapes remain invisible and are disclosed rather than chased: a rebinding
    (``T = threading``), a ``getattr(threading, "Lock")()``, a star-import, and a lock built by
    a helper and returned. Following an arbitrary value is a walker nobody can reason about,
    and every one of the four is a shape no module here uses.

    Args:
      tree: The parsed module.

    Returns:
      The names bound to the ``threading`` module, and a map from each locally bound name to
      the primitive it names — a map rather than a set, because ``from threading import Lock as
      L`` binds a name that says nothing about which primitive it is, and matching the *bound*
      name against the wanted list left the aliased form undetected.

    Raises:
      None.
    """
    modules: set[str] = set()
    direct: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name != "threading" and not alias.name.startswith("threading."):
                    continue
                # A dotted `import threading.x` binds `threading`, not the submodule. An alias
                # binds the submodule instead, which names no primitive — it is added anyway,
                # because `threading` has no submodules and over-matching demands a decision
                # where under-matching skips one.
                modules.add(alias.asname or alias.name.split(".", 1)[0])
        elif isinstance(node, ast.ImportFrom) and node.module == "threading":
            for alias in node.names:
                if alias.name in _PRIMITIVES:
                    direct[alias.asname or alias.name] = alias.name
    return modules, direct


def _primitive_constructions(tree: ast.AST, names: tuple[str, ...] = _REPAIRABLE) -> list[ast.Call]:
    """Every construction of one of ``names`` in a parsed module, however it is spelled.

    Args:
      tree: The parsed module.
      names: The primitive class names to look for.

    Returns:
      One call node per construction.

    Raises:
      None.
    """
    modules, direct = _threading_names(tree)
    found: list[ast.Call] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        through_module = (
            isinstance(func, ast.Attribute)
            and func.attr in names
            and isinstance(func.value, ast.Name)
            and func.value.id in modules
        )
        directly = isinstance(func, ast.Name) and direct.get(func.id) in names
        if directly or through_module:
            found.append(node)
    return found


def _namespace_stores(tree: ast.AST) -> set[int]:
    """The ids of every ``Name`` store the traversal can write back to.

    A bare *local* is not one of them, and that distinction is the whole difficulty: a target's
    shape alone cannot tell ``_worker_lock = threading.Lock()`` at module level from ``fresh =
    threading.Lock()`` inside a function, and treating every ``Name`` as reachable made the rule
    accept a lock no walk could ever find. So the scope is carried down — module and class
    bodies write to a namespace, a function body does not unless the name is declared ``global``,
    which is how ``decorator._offer_orphan_signal`` legitimately rebuilds ``_orphan_stop``.

    Args:
      tree: The parsed module.

    Returns:
      The ids of the store nodes a fresh primitive could be put back at.

    Raises:
      None.
    """
    reachable: set[int] = set()

    def scan(node: ast.AST, declared: set[str] | None) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda):
                scan(
                    child,
                    {
                        name
                        for stmt in ast.walk(child)
                        if isinstance(stmt, ast.Global)
                        for name in stmt.names
                    },
                )
                continue
            if (
                isinstance(child, ast.Name)
                and isinstance(child.ctx, ast.Store)
                and (declared is None or child.id in declared)
            ):
                reachable.add(id(child))
            scan(child, declared)

    scan(tree, None)
    return reachable


def _is_reachable_target(target: ast.expr, namespace_stores: set[int]) -> bool:
    """Whether the traversal can write to this assignment target.

    Two positions and no others: a name bound into a module or class namespace, which the walk
    reaches through ``vars()`` on the holder, and a ``self`` attribute, which it reaches through
    the instance's own namespace.

    Args:
      target: An assignment target node.
      namespace_stores: The ids :func:`_namespace_stores` collected for the same tree.

    Returns:
      Whether a fresh primitive could be put back here.

    Raises:
      None.
    """
    if isinstance(target, ast.Name):
        return id(target) in namespace_stores
    return (
        isinstance(target, ast.Attribute)
        and isinstance(target.value, ast.Name)
        and target.value.id == "self"
    )


def _assignment_targets(tree: ast.AST, call: ast.Call) -> list[ast.expr]:
    """The targets a construction is bound to, or an empty list when it is bound to nothing.

    Args:
      tree: The parsed module the call came from.
      call: The construction.

    Returns:
      The assignment targets.

    Raises:
      None.
    """
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and node.value is call:
            return list(node.targets)
        if isinstance(node, ast.AnnAssign) and node.value is call:
            return [node.target]
    return []


@pytest.mark.parametrize("path", _MODULES, ids=lambda p: p.relative_to(_SRC).as_posix())
def test_every_lock_is_assigned_where_the_walk_can_reach_it(path: pathlib.Path) -> None:
    """FR-003 AC-3. Completeness is proved by the shape rather than claimed by a roster.

    The walk replaces at two positions and no others: a module global and an instance attribute.
    A lock constructed into a list, a tuple, a closure cell or a bare local is invisible to it,
    and would be a silent return of the 19-of-60 hang. Forbidding the shape is also what picks
    up a lock added by a **later** spec with no edit to ``_fork.py`` — SPEC-036 FR-003 adds a
    counter lock after this one ships. A primitive of a *type* the walk cannot replace is a
    different question and is refused by its own test below, rather than folded in here.

    The rule errs toward rejecting, and three reachable shapes are caught by it:
    ``_a, _b = threading.Lock(), threading.Lock()``, a ``C.guard = …`` inside a method, and
    ``self.a.b = …``. Each fails loudly with the position named, which is the safe direction —
    a shape nobody uses costing a rewrite, against a lock nobody repairs costing a deadlock.

    Two reachability gaps are real and neither is this rule's to close, because neither is a
    construction in ``src/`` at all. ``Worker._queue`` is a ``queue.Queue``, which builds **its
    own** mutex and three ``Condition``s: measured, a fork with a thread inside that mutex
    leaves the child's ``submit()`` blocked, and no AST rule over this package can see a lock
    the standard library constructs. ``worker._FlushMarker.event`` is the milder one — it
    satisfies the shape, but a marker lives *inside* that same Queue, which the walk does not
    enter. FR-002 closes both by giving the child a fresh queue rather than by repairing this
    one, and it must replace the **object**: draining it would keep the dead mutex.

    ``_fork.py`` is the one file out of scope, and the test below is what makes that an
    exclusion rather than a hole: it mints the replacements and stores none of them, so it has
    nothing for a child to inherit.
    """
    if path.name == "_fork.py":
        pytest.skip("the module that mints replacements; test_the_fork_module_stores_no_primitive")
    tree = ast.parse(path.read_text(encoding="utf-8"))
    stores = _namespace_stores(tree)
    offenders: list[str] = []
    for call in _primitive_constructions(tree):
        targets = _assignment_targets(tree, call)
        if not targets:
            offenders.append(f"line {call.lineno}: not assigned to anything")
            continue
        offenders.extend(
            f"line {call.lineno}: {ast.unparse(target)}"
            for target in targets
            if not _is_reachable_target(target, stores)
        )
    assert not offenders, (
        "these locks are built where the fork walk cannot reach them — assign each to a module "
        "global or a `self` attribute:\n  " + "\n  ".join(offenders)
    )


@pytest.mark.parametrize("path", _MODULES, ids=lambda p: p.relative_to(_SRC).as_posix())
def test_no_module_builds_a_primitive_the_walk_cannot_repair(path: pathlib.Path) -> None:
    """FR-003 AC-3's boundary, made loud instead of left implicit.

    ``_fresh_primitive`` replaces a ``Lock``, an ``RLock`` and an ``Event``, and nothing else. A
    ``Condition`` owns a lock, so an inherited one is the 19-of-60 hang wearing another name,
    and a ``Semaphore`` or ``Barrier`` carries a count a fresh instance would silently reset —
    each needs a decision about what "the same primitive, minted again" even means. So they are
    detected and refused rather than quietly walked past, which is what the docstring above can
    honestly claim to pick up from a later spec.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found = [
        f"line {call.lineno}: {ast.unparse(call)}"
        for call in _primitive_constructions(tree, _UNREPAIRABLE)
    ]
    assert not found, (
        "the fork walk has no replacement for these, so a child would inherit them dead — "
        "teach `_fresh_primitive` about the type, or use one it knows:\n  " + "\n  ".join(found)
    )


def test_the_fork_module_stores_no_primitive() -> None:
    """FR-003 AC-3's exclusion, stated as the stronger claim it rests on.

    ``_fork.py`` is skipped above because every ``threading.*`` construction in it is either fed
    straight to ``type()`` to name a type or minted as a *replacement* and handed back — none is
    kept. A lock stored here would be inherited by a child like any other and repaired by
    nobody, since the repair is what this module is, so the skip has to be earned rather than
    granted.
    """
    tree = _fork_tree()
    stores = _namespace_stores(tree)
    stored = [
        f"line {call.lineno}: {ast.unparse(target)}"
        for call in _primitive_constructions(tree)
        for target in _assignment_targets(tree, call)
        if _is_reachable_target(target, stores)
    ]
    assert not stored, "_fork.py keeps a primitive nothing will repair:\n  " + "\n  ".join(stored)


# (fixture source, whether the rule accepts it). Every rejected shape is one the walk genuinely
# cannot reach, and `src/` contains none of them today — so without these the parametrized lint
# above cannot tell a working rule from one whose body was deleted.
_IMPORT = "import threading\n"

_SHAPES: list[tuple[str, bool]] = [
    (_IMPORT + "_m = threading.Lock()\n", True),
    (_IMPORT + "_m: object = threading.RLock()\n", True),
    (_IMPORT + "class C:\n    def __init__(self):\n        self.x = threading.Event()\n", True),
    ("import threading as th\n_m = th.Lock()\n", True),
    ("from threading import Lock\n_m = Lock()\n", True),
    (_IMPORT + "_locks = [threading.Lock()]\n", False),
    (_IMPORT + "_pair = (threading.Lock(),)\n", False),
    (_IMPORT + "d = {}\nd['k'] = threading.Lock()\n", False),
    (_IMPORT + "def f():\n    other.attr = threading.Lock()\n", False),
    (_IMPORT + "def f():\n    held = threading.Lock()\n", False),
    (_IMPORT + "threading.Lock()\n", False),
    ("import threading as th\n_locks = [th.Lock()]\n", False),
    ("from threading import Lock as L\n_locks = [L()]\n", False),
]


def test_the_shape_lint_rejects_the_shapes_it_claims_to() -> None:
    """Guards the guard: a rule that accepted everything would pass vacuously on a clean tree.

    ``src/`` satisfies the rule today, so nothing in the parametrized test can distinguish a
    working lint from one whose body was deleted. Each rejected fixture is a position the
    traversal has no way to write to.

    The last four rows exist because the detector was defeated by import *style*: with the
    literal ``threading`` hardcoded, ``import threading as th`` and ``from threading import
    Lock`` both read zero constructions, so a lock in a list passed the rule with nothing
    said. Every fixture must be seen as a construction first — ``assert calls`` below is that
    half — before its target shape is judged.
    """
    for source, accepted in _SHAPES:
        tree = ast.parse(source)
        calls = _primitive_constructions(tree)
        assert calls, source
        stores = _namespace_stores(tree)
        reachable = [
            target
            for call in calls
            for target in _assignment_targets(tree, call)
            if _is_reachable_target(target, stores)
        ]
        assert bool(reachable) is accepted, source


def test_a_module_global_reassigned_inside_a_function_still_counts() -> None:
    """``decorator._orphan_stop`` is rebuilt inside ``_offer_orphan_signal`` under ``global``.

    A rule keyed on *module-level* assignment would reject that live, correct site, so the rule
    is the target's shape rather than its position — and this is what pins which of the two was
    meant, since ``src/`` passes either way today.
    """
    tree = ast.parse(
        "import threading\n_e = threading.Event()\n\n\ndef f():\n"
        "    global _e\n    _e = threading.Event()\n"
    )
    stores = _namespace_stores(tree)
    calls = _primitive_constructions(tree)
    assert len(calls) == 2
    for call in calls:
        targets = _assignment_targets(tree, call)
        assert any(_is_reachable_target(t, stores) for t in targets), ast.dump(call)


# -- the traversal's own shapes, unit-tested where no shipped object exercises them ----------


def test_a_slotted_holder_gives_up_its_primitive() -> None:
    """The slot path has no shipped subject, so it is held by a unit test rather than by luck.

    ``src/`` has two slotted classes and neither is both reachable and primitive-holding, so
    deleting the slot descent leaves the whole suite green. The rule the walk must satisfy is
    the shape lint's: a ``self.<attr>`` is reachable, and a slotted class is one way to write
    one.
    """

    class _Slotted:
        __slots__ = ("guard",)

        def __init__(self) -> None:
            self.guard = threading.Lock()

    holder = _Slotted()
    assert dict(_fork._namespace_items(holder)) == {"guard": holder.guard}

    class _Empty:
        __slots__ = ("guard",)

    assert _fork._namespace_items(_Empty()) == []


def test_a_frozen_holder_cannot_refuse_the_repair() -> None:
    """``_assign`` writes through ``object.__setattr__``, which a frozen dataclass cannot block.

    ``Config`` is frozen (SPEC-034) and the sinks are free to be. An ordinary ``setattr`` raises
    ``FrozenInstanceError`` there, and the repair would be announced and skipped — leaving a
    child holding a lock nothing can release, which is the failure this module exists to remove
    rather than to report.
    """
    import dataclasses

    @dataclasses.dataclass(frozen=True)
    class _Frozen:
        guard: Any

    holder = _Frozen(guard=threading.Lock())
    replacement = threading.Lock()
    _fork._assign(holder, "guard", replacement)
    assert holder.guard is replacement

    with pytest.raises(dataclasses.FrozenInstanceError):
        holder.guard = threading.Lock()  # type: ignore[misc]


def test_the_walk_terminates_on_a_cycle(tmp_path: pathlib.Path) -> None:
    """A self-referencing sink must not stop the child returning from ``fork``.

    Driven through a real fork rather than by re-running the loop here. A first version popped,
    marked and extended in the test body and asserted on its own local ``seen`` — it never
    called the function whose termination it named, so deleting ``seen.add`` from
    ``_reinit_primitives`` left it passing while a cycle spun forever. The expectation has to
    come from the production path, not from a copy of it.

    A cycle is not a slow repair. The handler runs before the forking application gets control
    back, so a walk that does not terminate is a process that never returns from ``fork`` — and
    the child cannot be rescued by its own watchdog, because ``fork`` cleared the alarm and the
    handler runs before anything can arm one. The parent's ``SIGKILL`` is what ends it.
    """
    sink = FileSink(str(tmp_path / "cycle.ndjson"))
    sink.self_ref = sink  # type: ignore[attr-defined]
    loop: list[Any] = []
    loop.append(loop)
    sink.loop = loop  # type: ignore[attr-defined]
    log_foundry.configure(service="fork", version="0", env="test", sink=sink)
    before = id(sink._lock)
    try:
        child = run_in_child(lambda: str(id(sink._lock) != before), timeout=6)
    finally:
        sink.close()

    assert child.finished, f"the walk did not terminate on a cycle: {child.output!r}"
    assert child.output == "True", child.output


def test_an_owned_container_subclass_is_both_walked_and_repaired(tmp_path: pathlib.Path) -> None:
    """Being a container and being a namespace are not exclusive, and were treated as if.

    A ``continue`` after the container branch skipped the attributes of anything that also held
    members, so an owned sink subclassing one kept its ``_lock`` — the hang, in a class the walk
    had already decided to enter. No shipped class has this shape, which is why nothing else
    catches a revert.
    """

    class _BufferSink(FileSink, list):  # type: ignore[misc]
        pass

    sink = _BufferSink(str(tmp_path / "buffer.ndjson"))
    sink.append({"queued": 1})
    log_foundry.configure(service="fork", version="0", env="test", sink=sink)
    before = id(sink._lock)
    try:
        child = run_in_child(lambda: str(id(sink._lock) != before), timeout=6)
    finally:
        sink.close()

    assert child.output == "True", child.output


class _Fanout(list):  # type: ignore[type-arg]
    """A container subclass, which is what pins ``isinstance`` rather than the type tuple.

    A ``deque`` alone cannot: it is a *member* of ``_CONTAINER_TYPES``, so an exact-type test
    enters one too and the mutant survives. Only a subclass separates the two rules — which is
    the shape a caller's own fan-out would actually have.
    """


@pytest.mark.parametrize(
    ("name", "wrap"),
    [
        ("deque", collections.deque),
        ("list subclass", _Fanout),
    ],
)
def test_a_container_the_walk_enters_is_not_one_exact_type(
    tmp_path: pathlib.Path, name: str, wrap: type
) -> None:
    """FR-003 AC-2. Which concrete container a sink holds its children in must not decide this.

    An exact-type test walked past a ``defaultdict``, a caller's own list subclass and every
    other ordinary subclass, so a future sink's children would be unrepaired for no reason a
    reader could predict. ``MultiSink`` is the shipped shape; the container under it is the
    variable.

    Two rows because round 2 made two changes at once and only one of them was pinned. Adding
    ``deque`` to the type tuple and switching to ``isinstance`` are separate rules: with the
    ``deque`` row alone, reverting to an exact-type test left the whole file green, since a
    ``deque`` is in the tuple. The subclass row is the one that fails on that revert.
    """
    inner = FileSink(str(tmp_path / "inner.ndjson"))
    wrapper = MultiSink(inner)
    wrapper._sinks = wrap([inner])  # type: ignore[assignment]
    log_foundry.configure(service="fork", version="0", env="test", sink=wrapper)
    before = id(inner._lock)
    try:
        child = run_in_child(lambda: str(id(inner._lock) != before), timeout=6)
    finally:
        inner.close()

    assert child.output == "True", f"{name}: {child.output}"


def test_a_foreign_container_subclass_is_entered_but_never_rewritten(
    tmp_path: pathlib.Path,
) -> None:
    """FR-003 AC-2 / FR-005. Entering a container is not permission to rewrite what holds it.

    Matching containers by ``isinstance`` created a population that did not exist before: an
    object the walk enters *as a container* while its class is a driver's, not this library's.
    A connection registry or an LRU cache subclassing ``dict`` is an ordinary shape, and
    swapping the lock inside one is the "fork fix that breaks a driver" the ownership test
    exists to prevent.

    ``test_third_party_state_is_left_alone`` does not reach this: its foreign sink is a plain
    class, so it is refused a level earlier and the guard never runs. Measured — with the guard
    removed, the foreign lock is replaced and the whole suite stays green.
    """

    class _ForeignCache(dict):  # type: ignore[type-arg]
        def __init__(self) -> None:
            super().__init__()
            self.lock = threading.Lock()

    sink = FileSink(str(tmp_path / "foreign.ndjson"))
    sink._client = _ForeignCache()  # type: ignore[attr-defined]
    log_foundry.configure(service="fork", version="0", env="test", sink=sink)
    foreign_before = id(sink._client.lock)  # type: ignore[attr-defined]
    own_before = id(sink._lock)

    def compare() -> str:
        foreign = id(sink._client.lock) == foreign_before  # type: ignore[attr-defined]
        return f"{foreign},{id(sink._lock) != own_before}"

    try:
        child = run_in_child(compare, timeout=6)
    finally:
        sink.close()

    assert child.output == "True,True", child.output


# -- FR-001 / FR-006: what is registered, and where the mechanism lives ----------------------


def _fork_tree() -> ast.AST:
    """Parses ``_fork.py``.

    Args:
      None.

    Returns:
      The parsed module.

    Raises:
      None.
    """
    return ast.parse(pathlib.Path(_fork.__file__).read_text(encoding="utf-8"))


def _register_calls(tree: ast.AST) -> list[ast.Call]:
    """Every ``os.register_at_fork(...)`` call in a parsed module.

    Args:
      tree: The parsed module.

    Returns:
      One call node per registration.

    Raises:
      None.
    """
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "register_at_fork"
    ]


def test_only_after_in_child_is_registered() -> None:
    """FR-001 AC-1. A later change has to argue with the FR rather than slip past it.

    ``before`` does not run for a C-level fork at all — uWSGI calls ``PyOS_AfterFork_Child``
    only — so on one of the three deployments this spec names, a parent-side handler would not
    run and every hazard it was meant to close would happen anyway. Buying that partial fix with
    a measured 1.20 s hold on the forking thread is the wrong trade. Asserted at the AST because
    there is no runtime way to enumerate what a process registered.
    """
    calls = _register_calls(_fork_tree())
    assert len(calls) == 1, f"expected exactly one registration, found {len(calls)}"
    assert {kw.arg for kw in calls[0].keywords} == {"after_in_child"}
    assert not calls[0].args, "a positional argument to register_at_fork is `before`"


@pytest.mark.parametrize("path", _MODULES, ids=lambda p: p.relative_to(_SRC).as_posix())
def test_nothing_else_in_the_package_registers_a_fork_handler(path: pathlib.Path) -> None:
    """FR-001 AC-1's scope half: the assertion above is worth nothing if another module registers.

    A ``before`` handler added anywhere else in ``src/`` would leave the single-call check on
    ``_fork.py`` green while reintroducing exactly what that criterion rules out.
    """
    if path.name == "_fork.py":
        pytest.skip("the one module that may register; test_only_after_in_child_is_registered")
    tree = ast.parse(path.read_text(encoding="utf-8"))
    assert not _register_calls(tree), f"{path.name} registers a fork handler; FR-001 says one does"


def test_fork_imports_nothing_from_the_package_but_diag() -> None:
    """FR-006 AC-1. The dependency arrow points one way or there is a cycle.

    The handler's work touches ``decorator``, ``_lifecycle`` and the sinks, and all three would
    then import this module. It reaches them through ``sys.modules`` and an inverted registry
    instead, so the only intra-package imports it may hold are ``_diag`` and ``sinks.base``.

    **Every** imported name is checked, not merely one of them. A first version asked whether
    the set *intersected* the allowed names, which passes ``from log_foundry import _diag,
    decorator`` — and appending to that existing line is the most natural way Phase 2's worker
    rebuild would be written, so the guard would have stayed green over the very cycle this FR
    exists to prevent.

    A name is allowed when the module it comes from is allowed, or when the module plus the
    name is. Both are needed: ``from log_foundry.sinks.base import Sink`` names a member of an
    allowed module, while ``from log_foundry.sinks import base`` names the module itself.
    """
    forbidden = _forbidden_intra_package_imports(_fork_tree())
    assert not forbidden, f"_fork.py may not import {forbidden}"


_ALLOWED_IMPORTS = ("_diag", "sinks.base")


def _forbidden_intra_package_imports(tree: ast.AST) -> list[str]:
    """Every intra-package import in a module that FR-006 does not allow ``_fork.py`` to hold.

    Args:
      tree: The parsed module.

    Returns:
      One entry per offending imported name, sorted.

    Raises:
      None. A relative import is reported as an offence rather than asserted on, so one fixture
        can drive every shape through the same reader.
    """
    allowed = {f"{_PACKAGE}.{name}" for name in _ALLOWED_IMPORTS}
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(
                alias.name
                for alias in node.names
                if alias.name == _PACKAGE or alias.name.startswith(f"{_PACKAGE}.")
            )
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                found.add("." * node.level + (node.module or ""))
                continue
            module = node.module or ""
            if module != _PACKAGE and not module.startswith(f"{_PACKAGE}."):
                continue
            found.update(
                f"{module}.{alias.name}"
                for alias in node.names
                if module not in allowed and f"{module}.{alias.name}" not in allowed
            )
    return sorted(found)


# (fixture source, whether the reader must report an offence). Round 2 found this guard broken
# — it asked whether the imported names *intersected* the allowed set, so a second package
# import on the same line passed — and round 3 fixed the logic with no fixture to hold it, so
# emptying the reader's body passed too. `_fork.py` holds one allowed import, which is exactly
# one of the shapes below.
_IMPORT_SHAPES: list[tuple[str, bool]] = [
    ("from log_foundry import _diag", False),
    ("from log_foundry._diag import absorbed", False),
    ("from log_foundry.sinks import base", False),
    ("from log_foundry.sinks.base import Sink", False),
    ("from log_foundry.sinks.base import Sink, SinkLosses", False),
    ("import os, sys, threading", False),
    ("from threading import Lock", False),
    ("from log_foundry import _diag, decorator", True),
    ("from log_foundry import decorator", True),
    ("from log_foundry import worker as w", True),
    ("from log_foundry import sinks", True),
    ("from log_foundry.sinks import base, file", True),
    ("from log_foundry.decorator import _worker", True),
    ("import log_foundry", True),
    ("import log_foundry.decorator", True),
    ("import log_foundry.decorator as d", True),
    ("from . import decorator", True),
    ("from .sinks import file", True),
]


def test_the_import_guard_reads_every_shape_it_claims_to() -> None:
    """Guards the guard: the reader that round 2 found broken had nothing holding it.

    Both directions matter and each has a real failure. Accepting too much is the round-2
    defect, where ``from log_foundry import _diag, decorator`` passed and Phase 2's worker
    rebuild could have been written straight onto that line. Rejecting too much is round 3's,
    where ``from log_foundry.sinks import base`` — an import FR-006 permits — was refused
    because the reader reduced every alias to its module.
    """
    for source, offends in _IMPORT_SHAPES:
        found = _forbidden_intra_package_imports(ast.parse(source))
        assert bool(found) is offends, f"{source} -> {found}"


def test_the_handler_is_registered_once_for_a_double_import() -> None:
    """FR-006 AC-2. Counted in a child, which is the only place the count is observable.

    The package is re-imported and ``install()`` called again before forking. A second
    registration would run the repair twice — and a repair that runs after another handler has
    already taken one of the locks it replaces is how a fork fix becomes a fork hang.
    """
    import importlib

    importlib.import_module("log_foundry")
    _fork.install()
    _fork.install()

    runs: list[int] = []
    original = _fork._reinit_primitives

    def counted() -> None:
        runs.append(1)
        original()

    _fork._reinit_primitives = counted  # type: ignore[assignment]
    try:
        child = run_in_child(lambda: str(len(runs)))
    finally:
        _fork._reinit_primitives = original  # type: ignore[assignment]
    assert child.output == "1", child.output


def test_the_registration_is_guarded_for_a_platform_without_it() -> None:
    """FR-006 AC-4. CI runs Linux and macOS only, so this is asserted by construction.

    Windows has no ``os.register_at_fork``. An unguarded call would make the whole package
    unimportable there, which is a far worse failure than not repairing a fork that platform
    cannot perform.
    """
    tree = _fork_tree()
    install = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "install"
    )
    assert _register_calls(install), "the registration must live inside install()"
    assert any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "hasattr"
        and "register_at_fork" in ast.unparse(node)
        for node in ast.walk(install)
    ), "the registration must be guarded by a hasattr probe"


# -- FR-001 AC-3: the parent is not the side being repaired ----------------------------------


def test_the_parent_keeps_delivering_across_a_fork(tmp_path: pathlib.Path) -> None:
    """FR-001 AC-3. Only the child is repaired, so the parent must be observably untouched.

    Its worker, queue and counters are the same objects after the fork as before, and a span
    logged afterwards still reaches the sink. A parent-side handler taking the sink's transport
    lock is what this rules out — measured at 1.20 s on the forking thread, with ``HTTPSink``'s
    documented 90 s worst case behind it.
    """
    path = tmp_path / "parent.ndjson"
    sink = FileSink(str(path))
    log_foundry.configure(service="fork", version="0", env="test", sink=sink)
    worker = Worker(sink, batch_size=1, flush_interval=0.01)
    decorator._worker = worker

    queue_before = worker._queue
    lock_before = sink._lock
    dropped_before = worker.dropped

    child = run_in_child(lambda: "ok")
    assert child.finished, child.output

    assert decorator._worker is worker
    assert worker._queue is queue_before
    assert sink._lock is lock_before
    assert worker.dropped == dropped_before
    assert not worker.retired

    @log_foundry.trace
    def work() -> None:
        pass

    work()
    assert log_foundry.flush(timeout=5.0)
    assert "work" in path.read_text(encoding="utf-8")
