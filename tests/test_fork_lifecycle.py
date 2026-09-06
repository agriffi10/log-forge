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
import gc
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
from log_foundry import _fork, _lifecycle
from log_foundry.sinks.base import Sink, SinkDeliveryError
from log_foundry.sinks.file import FileSink, RotatingFileSink
from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.memory import MemorySink
from log_foundry.sinks.multi import MultiSink
from log_foundry.worker import Worker

# FR-004 AC-5's roster is the one `test_sink_concurrency` already derives -- every class in
# `sinks/` defining or inheriting a delivery method, floored so it cannot collapse silently.
# Importing it is the point: two rosters over one package that disagree about scope is the
# defect SPEC-038 FR-001 measured, and a second derivation here would be a second one to drift.
from test_sink_concurrency import _base_names, _sink_classes_with_an_emit

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

    **The parent collects before it forks, and that is a guard rather than tidiness.** A child
    that finalizes an object whose finalizer is not fork-safe dies of ``SIGSEGV`` inside whatever
    it was running -- which here means inside the library's own repair, the child's first
    substantial allocating work and so usually the pass that trips the collector. Nine unclosed
    ``sqlite3.Connection`` objects leaked by ``test_sinks_sqlite`` did exactly that, because
    macOS routes ``sqlite3``'s close through ``os_log``, which is not fork-safe; the crash
    presented under four different test names, since the frame it lands in is only whichever one
    the collector interrupted. Collecting here finalizes that garbage in the **parent**, where it
    is harmless, so nothing is left for the child to finalize. Measured against the nine original
    leaks: 7 of 8 runs crashed without this line, 0 of 10 with it and no other change -- both
    from ``pytest -n 0 tests/test_sinks_sqlite.py tests/test_fork_lifecycle.py``, so either half
    can be re-measured. The cost is one collection per fork, and it is **not** lost in the noise:
    ``pytest -n 0 tests/test_fork_lifecycle.py`` runs about 4% longer with this line than without,
    consistently clear of that command's own run-to-run spread. Both halves come from the same
    command, so the trade is re-measurable rather than asserted. It is deliberately not specific to ``sqlite3``, and not to a Python
    version: the next fork-unsafe finalizer to be leaked gets the same protection without anyone
    having to notice it was needed. The rule is **derived, not listed** --
    :func:`test_every_fork_collects_first` reads every test module and fails on a fork that is
    not immediately preceded by a bare ``gc.collect()``, so a fifth fork site cannot be added
    without either the guard or a deliberate argument against it.

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
    gc.collect()
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

    def __init__(self, stream: Any, *, park_after: int = 0) -> None:
        """Wraps a stream with no gate armed.

        Args:
          stream: The real file object to forward to.
          park_after: How many writes to forward before the armed gate takes one. ``0`` parks
            the first, which is FR-003's window — the lock is held and nothing has been written.
            FR-004 needs the other one: a write already forwarded and **unflushed**, so the fork
            lands with the parent's bytes sitting in the stream's own buffer.

        Returns:
          None.

        Raises:
          None.
        """
        self._stream = stream
        self._park_after = park_after
        self._writes = 0
        self.gate: _Gate | None = None

    def write(self, data: str) -> int:
        """Parks if a gate is armed and enough writes have gone through, then forwards.

        Args:
          data: The text to write.

        Returns:
          How many characters were written.

        Raises:
          None.
        """
        gate = self.gate
        if gate is not None and self._writes >= self._park_after:
            self.gate = None
            gate.entered.set()
            gate.release.wait(CHILD_TIMEOUT)
        self._writes += 1
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
    _lifecycle._state._worker = worker

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
    _lifecycle._state._worker = worker
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
    _lifecycle._state._worker = worker
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
    before = id(_lifecycle._state._lock)
    child = run_in_child(lambda: str(id(_lifecycle._state._lock) != before))
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
    _lifecycle._state._worker = worker
    assert sink.log_foundry_stop_signal is worker._stop

    def compare() -> str:
        live = _lifecycle._state._worker
        return f"{sink.log_foundry_stop_signal is live._stop},{live._stop is not None}"

    child = run_in_child(compare)
    assert child.output == "True,True", child.output


def test_a_set_event_is_still_set_in_the_child() -> None:
    """FR-003 AC-5. An ``Event`` carries its set state across, or the child un-does a shutdown.

    ``_lifecycle._state._stop`` is set by ``shutdown()`` and an ``Event`` never clears. A
    replacement that started unset would tell a child's sink to go on backing off for delivery
    that has already been retired, which is SPEC-033 FR-004's reasoning inherited by a fork.
    """
    _lifecycle._state._stop.set()
    before = id(_lifecycle._state._stop)
    child = run_in_child(
        lambda: f"{_lifecycle._state._stop.is_set()},{id(_lifecycle._state._stop) != before}"
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
    _lifecycle._state._worker = worker
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
    _lifecycle._state._worker = worker
    gate = _park_the_drain_thread(stream, worker, 0)
    for index in range(5):
        worker.submit([{"msg": f"backlog-{index}"}])
    assert worker._queue.qsize() == 5, "the backlog never reached the queue; this proves nothing"

    def work_in_child() -> str:
        queued = log_foundry.health().queued
        _lifecycle._state._worker.submit([{"msg": "child-0"}])
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
    _lifecycle._state._worker = worker
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
    _lifecycle._state._worker = worker
    worker.submit([{"msg": "kills-the-thread"}])
    deadline = time.monotonic() + 5.0
    while worker.stopped_reason is None and time.monotonic() < deadline:
        time.sleep(0.01)
    assert worker.stopped_reason == "SystemExit", worker.stopped_reason
    assert not worker.draining, "the parent's drain never settled, so this proves nothing"

    def report() -> str:
        live = _lifecycle._state._worker
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
    _idle_worker(sink)
    log_foundry.shutdown()

    started = threading.Event()
    original_run = Worker._run

    def announcing_run(self: Worker) -> None:
        started.set()
        original_run(self)

    Worker._run = announcing_run  # type: ignore[method-assign]

    def report() -> str:
        live = _lifecycle._state._worker
        live.submit([{"msg": "after-the-fork"}])
        health = log_foundry.health()
        drained = started.wait(1.0)
        return f"{health.retired},{health.queued},{drained}"

    try:
        child = run_in_child(report, timeout=8)
    finally:
        Worker._run = original_run  # type: ignore[method-assign]

    assert child.output == "True,1,False", child.output
    assert log_foundry.health().retired
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
    _lifecycle._state._worker = worker
    gate = _park_the_drain_thread(stream, worker, 0)

    shutting_down = threading.Thread(target=lambda: worker.shutdown(30.0), daemon=True)
    shutting_down.start()
    deadline = time.monotonic() + 5.0
    while not worker._stopped and time.monotonic() < deadline:
        time.sleep(0.01)
    assert worker._stopped and worker.draining, "the fork must land while the shutdown is joining"

    def report() -> str:
        live = _lifecycle._state._worker
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
    _lifecycle._state._worker = worker
    gate = _Gate()
    sink.gate = gate
    worker.submit([{"msg": "parks the drain thread"}])
    assert gate.entered.wait(5.0), "the drain thread never parked, so the fork misses the window"

    shutting_down = threading.Thread(target=lambda: worker.shutdown(30.0), daemon=True)
    shutting_down.start()

    def parent_ready() -> bool:
        # `_stopped` latches before `_stop` is set, so polling on it alone and asserting
        # the signal afterwards has a window between the two where the precondition is false.
        signal = sink.log_foundry_stop_signal
        return worker._stopped and signal is not None and signal.is_set()

    deadline = time.monotonic() + 5.0
    while not parent_ready() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert parent_ready(), "the parent's signal must be set to prove this"
    assert worker.draining, "the fork must land while the shutdown is joining"

    def report() -> str:
        log_foundry.info("an orphan log in a retired child")
        live = sink.log_foundry_stop_signal
        return f"{_lifecycle._state._worker.draining},{live is not None and live.is_set()}"

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
    _lifecycle._state._worker = worker
    assert sink.log_foundry_stop_signal is worker._stop

    def report() -> str:
        live = _lifecycle._state._worker
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
        live = _lifecycle._state._worker
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
    ``AttributeError`` on ``None._epoch``, which ``_fork`` absorbs into a stderr line on
    **every fork** — a library announcing a fault of its own invention, on a path where nothing
    was wrong (SPEC-025).
    """
    sink = FileSink(str(tmp_path / "orphan.ndjson"))
    log_foundry.configure(service="fork", version="0", env="test", sink=sink)
    log_foundry.info("no span, so no worker")
    assert _lifecycle._state._worker is None, "this test needs a process that never built a worker"

    buffer = io.StringIO()
    saved = sys.stderr
    sys.stderr = buffer
    try:
        child = run_in_child(lambda: buffer.getvalue(), timeout=6)
    finally:
        sys.stderr = saved

    assert child.finished, child.output
    assert child.output == "", f"the child announced something: {child.output!r}"


def test_the_marking_handler_still_runs_before_the_worker_rebuild() -> None:
    """SPEC-042 FR-001 puts a record naming another process on what the child's walk reaches,
    and it must do that before any other handler runs — handler order is registration order.

    The two registrations used to sit in different modules — `_mark_inherited` at the foot of
    `_lifecycle`, the rebuild at the foot of `decorator` — and the order held only because
    `decorator` imports `_lifecycle`. SPEC-040 put both in one module, where the order is now a
    property of two adjacent lines and nothing else. A rebuild that ran first would repair the
    worker while the sinks it holds were still unmarked, and a sink that is neither marked nor
    recorded is claimable.
    """
    order = [handler.__name__ for handler in _fork._child_handlers]
    assert "_mark_inherited" in order and "_rebuild_worker_after_fork" in order, order
    assert order.index("_mark_inherited") < order.index("_rebuild_worker_after_fork"), (
        f"the marking handler must be registered first, got {order}"
    )
    assert order[0] == "_mark_inherited", (
        f"before *any* other handler, which the pairwise check above does not pin: a fourth "
        f"handler registered ahead of it passes that and not this. Got {order}"
    )


def test_registering_the_same_handler_twice_is_a_no_op() -> None:
    """FR-006 AC-2's exposure at the registry rather than at ``os.register_at_fork``.

    A reload of a module whose body registers here would otherwise stack a second handler, and
    for the worker rebuild that means two drain threads in the child — the first bound to a
    queue nothing will ever write to. ``install()`` records the same exposure for the fork
    registration itself; this closes it for the handlers.
    """
    before = list(_fork._child_handlers)
    _fork.register_child_handler(_lifecycle._rebuild_worker_after_fork)
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
        return f"{health.stopped_reason},{_lifecycle._state._worker.draining},{log_foundry.health().queued}"

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
    assert _lifecycle._rebuild_worker_after_fork in _fork._child_handlers


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
    which is how ``_lifecycle._offer_orphan_signal`` legitimately rebuilds ``_stop``.

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
    """``_stop`` is rebuilt as ``self._stop`` inside ``_Lifecycle.refresh_stop_signal``.

    It was a ``global`` rebinding inside ``_offer_orphan_signal`` until SPEC-040 moved the state
    onto one owner, so **no live site in ``src/`` takes the ``global`` branch any more**. The
    branch is kept and exercised by the synthetic fixture below, because the walk must still
    write back to a module global wherever one holds a primitive; what changed is that this
    library no longer has such a site, not that the shape stopped mattering.

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

    def counted() -> list[Any]:
        runs.append(1)
        # The walk's return value is FR-004's roster of buffer-holding sinks, so a double that
        # swallowed it would leave the discard step running against nothing on this path.
        return original()

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
    _lifecycle._state._worker = worker

    queue_before = worker._queue
    lock_before = sink._lock
    dropped_before = worker.dropped

    child = run_in_child(lambda: "ok")
    assert child.finished, child.output

    assert _lifecycle._state._worker is worker
    assert worker._queue is queue_before
    assert sink._lock is lock_before
    assert worker.dropped == dropped_before
    assert _lifecycle._state.live_worker() is worker

    @log_foundry.trace
    def work() -> None:
        pass

    work()
    assert log_foundry.flush(timeout=5.0)
    assert "work" in path.read_text(encoding="utf-8")


# -- FR-004: the child discards the buffered writes it inherited -----------------------------


def _buffered_sink(
    tmp_path: pathlib.Path, kind: str, *, name: str = "buffered.ndjson"
) -> tuple[Any, _GatingStream, pathlib.Path]:
    """Builds a file-backed sink whose *second* write parks, and makes it the process sink.

    The first write is forwarded into the real stream and **not** flushed — ``emit`` flushes
    once at the end of the batch — so a fork taken while the second is parked lands with the
    parent's bytes in a buffer both processes then own. That is the window FR-004 is about, and
    it is the one an earlier draft of this spec measured *after* ``emit`` returned, when the
    buffer is empty by construction.

    **One batch is delivered and flushed before the window is armed**, so the file is not empty
    on disk when the child reopens it. That is not scene-setting: with nothing on disk, opening
    the replacement in ``"w"`` mode is indistinguishable from ``"a"``, and a review measured
    that mutant passing all 1626 tests — a child truncating the shared log on every fork, which
    is strictly worse than the duplication this whole FR removes. It is written before the
    wrapper is installed so it does not consume the wrapper's write count.

    Args:
      tmp_path: The directory to write into.
      kind: ``"file"``, ``"rotating"`` or ``"multi"`` — the last wrapping a ``FileSink`` in a
        ``MultiSink``, so the sink holding the buffer is not the one the worker was given.
      name: The file to append to.

    Returns:
      The sink to give the worker, the stream wrapper arming the window, and the path.

    Raises:
      None.
    """
    path = tmp_path / name
    inner: Any = RotatingFileSink(str(path)) if kind == "rotating" else FileSink(str(path))
    inner.emit([{"msg": "before-the-fork"}])
    stream = _GatingStream(inner._stream, park_after=1)
    inner._stream = stream
    sink = MultiSink(inner) if kind == "multi" else inner
    log_foundry.configure(service="fork", version="0", env="test", sink=sink)
    return sink, stream, path


def _park_inside_a_buffered_batch(
    stream: _GatingStream, worker: Worker
) -> _Gate:
    """Submits a two-event batch and returns once the drain thread is inside the second write.

    Args:
      stream: The wrapper whose gate is armed.
      worker: The worker whose drain thread will take the gate.

    Returns:
      The armed gate, which the caller releases after forking.

    Raises:
      AssertionError: If the drain thread never reached the window.
    """
    gate = _Gate()
    stream.gate = gate
    worker.submit([{"msg": "parent-a"}, {"msg": "parent-b"}])
    assert gate.entered.wait(5.0), "the drain thread never reached the second write of the batch"
    return gate


def _lines_holding(written: str, marker: str) -> int:
    """Counts the *lines* a marker appears in, which is what "delivered once" means here.

    A substring count is the wrong measure and was measured wrong: a built event carries the
    logged text in both ``message`` and ``function``, so one delivery of ``child-0`` reads as
    two occurrences and a duplicate reads as four.

    Args:
      written: The file's contents.
      marker: The text to look for.

    Returns:
      How many lines contain it.

    Raises:
      None.
    """
    return sum(1 for line in written.splitlines() if marker in line)


@pytest.mark.parametrize("kind", ["file", "rotating", "multi"])
def test_the_child_does_not_write_the_parents_buffered_bytes(
    tmp_path: pathlib.Path, kind: str
) -> None:
    """FR-004 AC-1 and AC-2. The fork lands mid-``emit``, with bytes pending on both sides.

    Without the discard both processes flush the same buffer and the event at the fork point
    appears on disk twice — measured against ``5ad6699``, and identical for
    ``RotatingFileSink``. The child must re-emit none of it, and the parent's own copy must
    still reach disk exactly once, which is why both are asserted here rather than only the
    absence of a duplicate: a child that discarded the parent's *file* would satisfy one half.

    The ``multi`` case is not decoration. The sink holding the buffer is then not the one the
    worker was handed, so a repair that asked ``worker.sink`` rather than every sink the walk
    reached would leave a fan-out's children duplicating — the shape FR-003 AC-2 already had to
    close for the locks.

    ``before-the-fork`` is on disk before any of this and is asserted to survive, which is what
    holds the child's reopen to **append** mode. Without a line already written, ``"w"`` and
    ``"a"`` are the same program: a review measured that mutant green across the whole suite,
    with every child truncating the shared log.
    """
    sink, stream, path = _buffered_sink(tmp_path, kind, name=f"{kind}.ndjson")
    worker = Worker(sink, batch_size=1, flush_interval=0.01)
    _lifecycle._state._worker = worker
    gate = _park_inside_a_buffered_batch(stream, worker)
    at_the_fork = path.read_text(encoding="utf-8")
    assert _lines_holding(at_the_fork, "before-the-fork") == 1, at_the_fork
    assert "parent-a" not in at_the_fork, "nothing is buffered, so this proves nothing"

    def log_in_child() -> str:
        log_foundry.info("child-0")
        return "logged"

    try:
        child = run_in_child(log_in_child, timeout=6)
    finally:
        gate.release.set()

    assert child.output == "logged", child.output
    assert log_foundry.flush(timeout=5.0)
    written = path.read_text(encoding="utf-8")
    for marker in ("before-the-fork", "parent-a", "parent-b", "child-0"):
        assert _lines_holding(written, marker) == 1, f"{marker} in {written!r}"


def test_the_same_child_duplicates_when_the_discard_is_taken_away(
    tmp_path: pathlib.Path,
) -> None:
    """FR-004 AC-1's other half: the unfixed behaviour is **demonstrated**, not asserted.

    The discard step is removed for the length of one fork, which puts the child in exactly the
    state the library shipped in before this spec: it inherits the parent's pending bytes, and
    its own first log call flushes them along with its own. The parent's line then lands twice,
    which is the measurement against ``5ad6699`` reproduced rather than quoted.

    Without this, the test above could pass against a fork that never entered the window — an
    empty buffer duplicates nothing, and "each marker appears once" is satisfied by a sink that
    was never in danger.

    Restoring the inherited *stream object* in the child does not express this and was tried:
    ``dup2`` has already pointed that object's descriptor at ``/dev/null``, so putting it back
    sends the child's own line there too and the file shows **less** rather than more. The step
    has to be absent before the handler runs, not undone after it.
    """
    sink, stream, path = _buffered_sink(tmp_path, "file", name="unrepaired.ndjson")
    worker = Worker(sink, batch_size=1, flush_interval=0.01)
    _lifecycle._state._worker = worker
    gate = _park_inside_a_buffered_batch(stream, worker)

    original = _fork._reacquire_transports

    def keep_the_parents_buffer(holders: list[Any]) -> None:
        pass

    _fork._reacquire_transports = keep_the_parents_buffer  # type: ignore[assignment]
    try:
        child = run_in_child(_log_in_child, timeout=6)
    finally:
        _fork._reacquire_transports = original  # type: ignore[assignment]
        gate.release.set()

    assert child.output == "logged", child.output
    assert log_foundry.flush(timeout=5.0)
    written = path.read_text(encoding="utf-8")
    assert _lines_holding(written, "parent-a") == 2, written


def test_the_discard_runs_before_any_registered_handler(tmp_path: pathlib.Path) -> None:
    """FR-001 AC-2's third step, pinned rather than promised.

    The order in the child is the contract — locks, then the discard, then the registered
    handlers — and the reason the discard is inline rather than registered is that
    ``decorator``'s rebuild registers first and has a **live drain thread** by the time any
    later handler runs, emitting into the very sink whose buffer is still the parent's.

    Being straight about what this is: moving the step below the handler loop is not currently
    observable as a duplicate, because the rebuilt worker starts with an empty queue and so has
    nothing to emit in the window. It is an unenforced contract rather than a live defect —
    which is exactly the state SPEC-035's predicate roster was built for, after three reviewers
    each named a different unenforced ordering site and a fourth shipped anyway. The probe
    records what it saw rather than asserting in the child, as the locks' order test does.

    **The probe is registered first, and appending it is what this test was doing wrong.**
    ``decorator``'s rebuild is the only other handler and is the one the contract is about, so a
    probe on the end reads "the discard ran before the *last* handler" — green against a discard
    slid to just after that rebuild, which is verbatim the hazard the contract names: a live
    drain thread emitting into a sink whose buffer is still the parent's. From position 0 the
    same mutant reports ``still-the-parents``.
    """
    sink, stream, _path = _buffered_sink(tmp_path, "file", name="ordering.ndjson")
    worker = Worker(sink, batch_size=1, flush_interval=0.01)
    _lifecycle._state._worker = worker
    gate = _park_inside_a_buffered_batch(stream, worker)
    inherited = sink._stream
    seen: list[str] = []

    def probe() -> None:
        seen.append("discarded" if sink._stream is not inherited else "still-the-parents")

    _fork._child_handlers.insert(0, probe)
    try:
        child = run_in_child(lambda: ",".join(seen), timeout=6)
    finally:
        _fork._child_handlers.remove(probe)
        gate.release.set()

    assert child.output == "discarded", child.output


def test_the_reopened_stream_keeps_the_encoding_it_was_given(tmp_path: pathlib.Path) -> None:
    """A child that reopens under a different encoding writes a second half nobody can decode.

    ``FileSink`` takes an ``encoding`` and the file is shared with the parent, so the
    replacement has to be opened in the same one — dropping it falls back to the locale default
    and the file becomes two encodings deep in one stream. No fork is needed to see it, and
    UTF-16 is the pair that shows it: ``json.dumps`` escapes non-ASCII, so a UTF-8 replacement
    is byte-identical to a Latin-1 one and would prove nothing.
    """
    path = tmp_path / "encoded.ndjson"
    sink = FileSink(str(path), encoding="utf-16")
    try:
        sink.emit([{"msg": "before"}])
        sink.reacquire_after_fork()
        sink.emit([{"msg": "after"}])
    finally:
        sink.close()

    decoded = path.read_bytes().decode("utf-16")
    assert _lines_holding(decoded, "before") == 1, decoded
    assert _lines_holding(decoded, "after") == 1, decoded


def test_a_hostile_attribute_read_does_not_abort_the_lock_repair(tmp_path: pathlib.Path) -> None:
    """The probe for one hazard must not cost the child the repair for the other.

    ``getattr`` propagates anything that is not an ``AttributeError``, so an owned object whose
    ``__getattr__`` raises would end the walk — and what is lost then is not a buffer but every
    lock the walk had not reached yet, which is the hang this module exists to remove. Every
    other read the walk makes is guarded individually; this asserts the probe is too.

    Nothing owned defines ``__getattr__`` today, so the subject is built here — and it must be
    a sink that does **not** already carry the hook, or normal lookup succeeds and
    ``__getattr__`` is never consulted at all. A first version subclassed ``FileSink`` and was
    vacuous for exactly that reason: it inherited the method, and removing the guard left it
    green. It raises only for this one name, so the rest of the child's repair is judged on its
    own behaviour rather than on collateral from the double.

    The stderr assertion is the load-bearing one. An abort is announced by
    ``_reinit_after_fork``'s own guard wherever it happens, while which locks were already
    replaced depends on where the walk was — deterministic only in the world where nothing
    aborts.
    """

    class _Hostile(MemorySink):
        def __getattr__(self, name: str) -> Any:
            if name == "reacquire_after_fork":
                raise RuntimeError("this attribute is not yours to ask about")
            raise AttributeError(name)

    hostile = _Hostile()
    later = FileSink(str(tmp_path / "later.ndjson"))
    log_foundry.configure(service="fork", version="0", env="test", sink=MultiSink(hostile, later))
    before = id(later._lock)

    buffer = io.StringIO()
    saved = sys.stderr
    sys.stderr = buffer
    try:
        child = run_in_child(lambda: f"{id(later._lock) != before}|{buffer.getvalue()}", timeout=6)
    finally:
        sys.stderr = saved
        later.close()

    repaired, _, announced = child.output.partition("|")
    assert repaired == "True", child.output
    assert announced == "", f"the child announced something: {announced!r}"


@pytest.mark.parametrize("value", [None, "disabled"])
def test_a_member_of_that_name_which_is_not_callable_is_not_the_hook(
    tmp_path: pathlib.Path, value: object
) -> None:
    """The probe asks whether the hook is *callable*, as ``read_losses`` does for ``losses()``.

    ``None`` is not a hypothetical value for an optional member here: the fourth one,
    ``log_foundry_stop_signal``, is documented as a plain attribute initialised to ``None``, so
    a sink written against that pattern may well carry this name the same way. Reading it as a
    hook produces a ``TypeError`` absorbed into a stderr line on every fork — a library
    announcing a fault of its own invention, on a sink that simply opted out (SPEC-025).

    Both rows are needed and neither is decoration: a ``None`` check alone passes the first and
    fails the second, so with only ``None`` here the weaker rule is indistinguishable from the
    one the code states.
    """

    class _NotAHook(FileSink):
        pass

    _NotAHook.reacquire_after_fork = value  # type: ignore[assignment]
    sink = _NotAHook(str(tmp_path / "notahook.ndjson"))
    log_foundry.configure(service="fork", version="0", env="test", sink=sink)

    buffer = io.StringIO()
    saved = sys.stderr
    sys.stderr = buffer
    try:
        child = run_in_child(buffer.getvalue, timeout=6)
    finally:
        sys.stderr = saved
        sink.close()

    assert child.finished, child.output
    assert child.output == "", f"the child announced something: {child.output!r}"


def test_the_inherited_buffer_can_only_reach_the_null_device(tmp_path: pathlib.Path) -> None:
    """FR-004. The buffer is **stranded**, not merely detached, and the two differ.

    Rebinding ``self._stream`` alone leaves the inherited object holding the parent's bytes and
    a descriptor still pointing at the file — so the flush nobody wrote lands anyway: CPython
    flushes a ``TextIOWrapper`` when it is garbage-collected, which happens the moment the sink
    drops its last reference, and again at interpreter exit. ``dup2`` is what makes that flush
    harmless, and it is measurement 3 of the spec's prior work.

    The flush is driven explicitly here rather than waited for. A test process holds the wrapper
    from its own frame, so the collection that makes this bite in production never happens
    inside the child — and a test that hoped for it would pass with ``dup2`` deleted.
    """
    sink, stream, path = _buffered_sink(tmp_path, "file", name="stranded.ndjson")
    worker = Worker(sink, batch_size=1, flush_interval=0.01)
    _lifecycle._state._worker = worker
    gate = _park_inside_a_buffered_batch(stream, worker)

    def flush_what_was_inherited() -> str:
        stream.flush()
        return "flushed"

    try:
        child = run_in_child(flush_what_was_inherited, timeout=6)
    finally:
        gate.release.set()

    assert child.output == "flushed", child.output
    assert log_foundry.flush(timeout=5.0)
    written = path.read_text(encoding="utf-8")
    assert _lines_holding(written, "parent-a") == 1, written


def _open_descriptors() -> int:
    """Counts this process's open file descriptors.

    Args:
      None.

    Returns:
      How many descriptors are open, by the directory both Linux and macOS publish.

    Raises:
      None.
    """
    return len(os.listdir("/dev/fd"))


def test_discarding_a_buffer_leaks_no_descriptor(tmp_path: pathlib.Path) -> None:
    """The null device is opened to be duplicated over, and then it is nobody's.

    A prefork server forks continuously, and a descriptor leaked per sink per fork is a
    process that eventually cannot open a file at all — a failure that surfaces nowhere near
    the fork handler that caused it. No fork is needed to see it: the hook is called directly,
    which is also what keeps the count stable enough to assert on.

    The replaced stream closes its own descriptor when the sink drops it, so a correct discard
    is descriptor-neutral rather than merely bounded.
    """
    sink = FileSink(str(tmp_path / "descriptors.ndjson"))
    try:
        sink.reacquire_after_fork()
        before = _open_descriptors()
        for _ in range(20):
            sink.reacquire_after_fork()
        assert _open_descriptors() == before, "the discard leaked a descriptor per call"
    finally:
        sink.close()


@pytest.mark.parametrize("build", [FileSink, RotatingFileSink])
def test_a_closed_sink_is_asked_for_nothing(tmp_path: pathlib.Path, build: type) -> None:
    """A closed sink has no descriptor to redirect, and asking it for one raises.

    ``close()`` is the documented state a sink can be in when a fork happens — ``atexit`` and a
    caller's own cleanup both reach it — and ``fileno()`` on a closed stream raises
    ``ValueError``. Without the guard that is an absorbed failure and a stderr line on a path
    where nothing is wrong, which is the invented fault SPEC-025 removed everywhere else.
    """
    sink = build(str(tmp_path / "closed.ndjson"))
    sink.close()
    sink.reacquire_after_fork()


def test_one_hooks_failure_is_absorbed_and_does_not_stop_the_others(
    tmp_path: pathlib.Path,
) -> None:
    """The hook is an implementer's code, and it runs where an exception cannot be allowed out.

    A raise here reaches CPython's unraisable hook, which prints a full traceback carrying the
    exception's **message** — the user data arch §6 keeps out of anything the library says about
    itself — and it would leave every later step of the repair undone. Each hook is therefore
    absorbed on its own account, exactly as the registered handlers are, so one sink's failure
    costs that sink a duplicated batch rather than costing the next sink its discard.
    """

    class _RaisesOnReacquire(FileSink):
        def reacquire_after_fork(self) -> None:
            raise RuntimeError("a-value-from-the-event-4321")

    class _RecordsItRan(FileSink):
        def __init__(self, path: str) -> None:
            super().__init__(path)
            self.ran: list[str] = []

        def reacquire_after_fork(self) -> None:
            self.ran.append("yes")

    raising = _RaisesOnReacquire(str(tmp_path / "raising.ndjson"))
    recording = _RecordsItRan(str(tmp_path / "recording.ndjson"))
    log_foundry.configure(
        service="fork", version="0", env="test", sink=MultiSink(raising, recording)
    )

    buffer = io.StringIO()
    saved = sys.stderr
    sys.stderr = buffer
    try:
        child = run_in_child(lambda: f"{len(recording.ran)}|{buffer.getvalue()}", timeout=6)
    finally:
        sys.stderr = saved
        raising.close()
        recording.close()

    assert child.finished, child.output
    ran, _, announced = child.output.partition("|")
    assert ran == "1", child.output
    assert "a-value-from-the-event-4321" not in announced
    assert "absorbed a failure while re-acquiring a transport after a fork" in announced
    assert "(RuntimeError)" in announced
    assert "_RaisesOnReacquire may write the parent's pending bytes again" in announced
    assert "and this child will not release it" in announced, (
        "SPEC-042 FR-005 AC-4: a failed re-acquisition is not a claim, so the consequence is "
        "also that the sink stays unreleasable here — the line has to say both"
    )


class _BareSink:
    """A sink with an ``emit`` and a ``close`` and nothing else, per FR-004 AC-3.

    It is deliberately declared outside this library's ownership boundary as well as without
    the hook, since the two exemptions are different: the walk never enters a foreign object at
    all, and an owned sink without the hook is entered and must simply not be asked.
    """

    def __init__(self) -> None:
        """Starts an empty collection.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.events: list[dict[str, object]] = []

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Collects the batch.

        Args:
          batch: The events to collect.

        Returns:
          None.

        Raises:
          None.
        """
        self.events.extend(batch)

    def close(self) -> None:
        """Releases nothing.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """


@pytest.mark.parametrize("build", [MemorySink, _BareSink])
def test_a_sink_without_the_hook_is_unaffected(build: type) -> None:
    """FR-004 AC-3. The hook is optional, and both kinds of "without it" are covered.

    ``MemorySink`` is the case that can actually break: it is owned, so the walk enters it and
    the probe runs against it, and a repair that assumed the member exists would take an
    ``AttributeError`` on every fork of every process using it — a library announcing a fault of
    its own invention (SPEC-025). The bare class is the criterion's own wording, and is refused
    a level earlier, at the ownership boundary.

    Silence is asserted as well as delivery, because a probe that raised and was absorbed would
    still leave the child working while writing a line on every fork.
    """
    sink = build()
    log_foundry.configure(service="fork", version="0", env="test", sink=sink)

    buffer = io.StringIO()
    saved = sys.stderr
    sys.stderr = buffer
    try:
        child = run_in_child(
            lambda: f"{(log_foundry.info('child-0'), len(sink.events))[1]},{buffer.getvalue()}",
            timeout=6,
        )
    finally:
        sys.stderr = saved

    assert child.output == "1,", child.output


def test_the_hook_is_documented_where_an_implementer_reads_the_contract() -> None:
    """FR-004 AC-4. The hook is probed by name, so its only contract is what ``Sink`` says.

    A third-party sink owning a buffered stream has no other way to learn that the member
    exists, that it runs in a child that has not returned from ``fork``, or that blocking there
    produces a process no watchdog can end.

    The **boundary** clause is asserted alongside them, because it is the one a reader acts on
    and the one that was wrong: the first version of this paragraph said a sink defining the
    hook is asked, and a review measured a structurally-satisfying third-party sink — which is
    how every shipped sink satisfies this Protocol — being asked zero times. A claim about who
    is *not* reached can be deleted with every other assertion here still green.
    """
    documented = " ".join((Sink.__doc__ or "").split())
    assert "reacquire_after_fork" in documented
    assert "must not block" in documented
    assert "SPEC-039 FR-004" in documented
    assert "its hook is never called" in documented


_REACQUIRE_HOOK = "reacquire_after_fork"


def _opens_a_stream_into_self(cls: ast.ClassDef) -> bool:
    """Whether a class assigns the result of ``open()`` to one of its own attributes.

    That is the evidence of *ownership* the roster needs, and it is what separates the file
    sinks from ``StdoutSink``, which holds a stream the process owns and is deliberately left
    alone (FR-005 AC-3). A caller-supplied stream is the caller's buffer; one this class opened
    is this class's.

    Two shapes are invisible and are disclosed rather than chased: a stream opened by a helper
    and returned to the assignment, and one opened through something other than the ``open``
    builtin — ``pathlib.Path.open``, ``os.fdopen``, ``gzip.open``. No module in ``sinks/`` uses
    either, and following an arbitrary value is the guesswork SPEC-032 took out of this gate.

    Args:
      cls: The class node to inspect.

    Returns:
      Whether it opens a stream into a ``self`` attribute anywhere in its body.

    Raises:
      None.
    """
    for node in ast.walk(cls):
        if isinstance(node, ast.Assign):
            targets: list[ast.expr] = list(node.targets)
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
        else:
            continue
        value = node.value
        if not (
            isinstance(value, ast.Call)
            and isinstance(value.func, ast.Name)
            and value.func.id == "open"
        ):
            continue
        if any(
            isinstance(target, ast.Attribute)
            and isinstance(target.value, ast.Name)
            and target.value.id == "self"
            for target in targets
        ):
            return True
    return False


def _has_the_discard_hook(cls: ast.ClassDef, nodes: dict[str, ast.ClassDef]) -> bool:
    """Whether a class defines the hook or inherits it from an ancestor in the same scan.

    Defines-**or**-inherits, for the reason SPEC-038 FR-001 made both sink rosters scope that
    way: keying on where a method happens to sit makes membership a function of a refactor, and
    moving five ``emit`` implementations into a base dropped five classes out of two lints in
    one commit with the suite green.

    Args:
      cls: The class node to judge.
      nodes: Every class node in the scan, by name, for resolving bases.

    Returns:
      Whether the hook is reachable on this class.

    Raises:
      None.
    """
    seen: set[str] = set()
    queue = [cls]
    while queue:
        current = queue.pop()
        if any(
            isinstance(member, ast.FunctionDef | ast.AsyncFunctionDef)
            and member.name == _REACQUIRE_HOOK
            for member in current.body
        ):
            return True
        for name in _base_names(current):
            if name in nodes and name not in seen:
                seen.add(name)
                queue.append(nodes[name])
    return False


def _sinks_missing_the_discard_hook(entries: list[tuple[str, ast.ClassDef]]) -> list[str]:
    """Returns the classes that own a buffered stream and cannot discard it after a fork.

    Written as a function over its input rather than over the package, so the red path can be
    exercised directly — a lint whose failure path never runs is one a refactor of its own
    matching can defeat silently.

    Args:
      entries: ``(module_stem, class node)`` pairs to judge.

    Returns:
      The qualified names of the offenders, sorted.

    Raises:
      None.
    """
    nodes = {cls.name: cls for _stem, cls in entries}
    return sorted(
        f"{stem}.{cls.name}"
        for stem, cls in entries
        if _opens_a_stream_into_self(cls) and not _has_the_discard_hook(cls, nodes)
    )


def test_every_sink_that_owns_a_buffered_stream_discards_it_after_a_fork() -> None:
    """FR-004 AC-5. The roster is derived from the sinks, not written next to them.

    Scope is the sink roster ``test_sink_concurrency`` already derives — every class in
    ``sinks/`` defining or inheriting ``emit``/``send_all``/``close``, floored so it cannot
    collapse silently (SPEC-032's scope gate, SPEC-038's floor). What varies is the question:
    which of those classes opens a stream of its own, and therefore inherits a buffer a forked
    child would write a second time.

    The two file sinks are named as a **precondition**, not as the assertion. A detector that
    matched nothing would satisfy the negative below while the next stream-owning sink shipped
    with no hook and no failure — the vacuity this suite exists to keep out. ``StdoutSink`` is
    asserted *out* of scope for the same reason in reverse: it holds a stream the process owns,
    and FR-005 AC-3 records that discarding the application's pending output to protect the
    library's own is not a trade this library may make.
    """
    roster = _sink_classes_with_an_emit()
    assert len(roster) >= 34, f"the sink roster collapsed to {len(roster)}"
    opening = {f"{stem}.{cls.name}" for stem, cls in roster if _opens_a_stream_into_self(cls)}
    assert {"file.FileSink", "file.RotatingFileSink"} <= opening, opening
    assert "stdout.StdoutSink" not in opening, "FR-005 AC-3 keeps the process's stream out"

    missing = _sinks_missing_the_discard_hook(roster)
    assert not missing, (
        "these sinks open a buffered stream of their own but cannot discard what a fork leaves "
        f"pending in it — implement {_REACQUIRE_HOOK}(): {missing}"
    )


def _synthetic_sink(name: str, body: str, *, bases: str = "") -> tuple[str, ast.ClassDef]:
    """Parses a one-off sink class, for the lint's red path.

    Args:
      name: The class name.
      body: The indented class body.
      bases: The base list, without parentheses.

    Returns:
      A ``(module_stem, class node)`` pair shaped like the package scan's.

    Raises:
      None.
    """
    header = f"class {name}({bases}):" if bases else f"class {name}:"
    module = ast.parse(f"{header}\n{body}")
    cls = module.body[0]
    assert isinstance(cls, ast.ClassDef)
    return ("brandnew", cls)


def test_the_buffer_lint_reads_the_shapes_it_claims_to() -> None:
    """Guards the guard: ``sinks/`` satisfies the rule today, so nothing above can fail.

    Four shapes, each a real decision rather than a variation. A sink that opens a stream and
    cannot discard it is the offence. One that opens and can is the fix. A **subclass** of the
    second is why the rule is defines-or-inherits. And a sink assigning a stream it was *given*
    is ``StdoutSink``, which must stay out of scope without a hand-written exemption — an
    exemption list is what this whole gate exists to replace.
    """
    opener = _synthetic_sink(
        "OpenerSink",
        '    def __init__(self, path):\n        self._stream = open(path, "a")\n'
        "    def emit(self, batch): ...\n",
    )
    fixed = _synthetic_sink(
        "FixedSink",
        '    def __init__(self, path):\n        self._stream = open(path, "a")\n'
        "    def emit(self, batch): ...\n"
        f"    def {_REACQUIRE_HOOK}(self): ...\n",
    )
    inheriting = _synthetic_sink(
        "InheritingSink",
        '    def __init__(self, path):\n        self._stream = open(path, "a")\n'
        "    def emit(self, batch): ...\n",
        bases="FixedSink",
    )
    given = _synthetic_sink(
        "GivenSink",
        "    def __init__(self, stream):\n        self._stream = stream\n"
        "    def emit(self, batch): ...\n",
    )

    assert _sinks_missing_the_discard_hook([opener]) == ["brandnew.OpenerSink"]
    assert _sinks_missing_the_discard_hook([fixed]) == []
    assert _sinks_missing_the_discard_hook([fixed, inheriting]) == []
    assert _sinks_missing_the_discard_hook([inheriting]) == ["brandnew.InheritingSink"]
    assert _sinks_missing_the_discard_hook([given]) == []


# -- SPEC-050: the two pieces of new state a fork must not carry across ------------------


def test_the_owed_swap_record_is_skipped_by_the_repair_walk() -> None:
    """FR-004. `Worker._unclosed_swaps` pins superseded sinks, so the walk must not enter it.

    The hazard `_fork._SKIP_ATTRIBUTE` documents, at a new container: reaching a sink the process
    abandoned replaces its locks — merely wasteful — and runs its fork hooks, which is not, since
    `_lifecycle.reclaim` then overwrites the foreign-pid record the child holds for it — the
    parent's own stamp, or the `_FOREIGN` `_mark_inherited` `setdefault`s where the parent
    recorded nothing — and leaves a child able to release a transport it never acquired.

    Asserted against the walk itself rather than against a symptom, because the symptom is a
    child closing a parent's connection and there is no in-process way to observe it. A control
    run pins the other half: without the opt-out the same walk *does* reach the sink, so this
    cannot pass by the walk having stopped reaching anything.
    """
    from log_foundry.worker import Worker

    class _Hooked(Sink):
        """Carries the reacquire hook, which is what the walk collects and would then run."""

        def emit(self, batch: list[dict[str, object]]) -> None:
            """Accepts a batch; this test asserts on the walk, not on delivery."""

        def close(self) -> None:
            """Releases nothing."""

    setattr(_Hooked, _REACQUIRE_HOOK, lambda self: _lifecycle.reclaim(self))

    worker = Worker(_Hooked())
    stranded = _Hooked()
    try:
        worker._unclosed_swaps = [stranded]
        _lifecycle._state._worker = worker
        reached = _fork._reinit_primitives()
        assert not any(obj is stranded for obj in reached), (
            "the repair walk reached a sink the record only pins"
        )

        # Control: the same walk, with the attribute renamed out from under the opt-out.
        worker._unclosed_swaps = []
        worker._not_skipped = [stranded]  # type: ignore[attr-defined]
        reached_control = _fork._reinit_primitives()
        assert any(obj is stranded for obj in reached_control), (
            "the control did not reach it either, so the assertion above proves nothing"
        )

        # The consequence the opt-out exists for, asserted rather than left to follow. The
        # walk only *collects* hooks; `_fork` then calls them, and `_lifecycle.reclaim` is what
        # a called hook reaches — the one write that overrides an inherited record.
        # So "the hook did not run in the child" is the step between the walk and `releasable`.
        worker._not_skipped = []  # type: ignore[attr-defined]
        worker._unclosed_swaps = [stranded]
        reclaimed: list[object] = []
        real_reclaim = _lifecycle.reclaim
        _lifecycle.reclaim = reclaimed.append  # type: ignore[assignment]
        try:
            assert run_in_child(lambda: str(stranded in reclaimed)).output == "False", (
                "a child re-stamped a sink the record only pins, so it could then release it"
            )
            worker._unclosed_swaps = []
            worker._not_skipped = [stranded]  # type: ignore[attr-defined]
            assert run_in_child(lambda: str(stranded in reclaimed)).output == "True", (
                "the control did not re-stamp it either, so the assertion above proves nothing"
            )
        finally:
            _lifecycle.reclaim = real_reclaim  # type: ignore[assignment]
    finally:
        _lifecycle._state._worker = None
        worker.shutdown(timeout=2.0)


def test_the_in_flight_marker_record_is_skipped_by_the_repair_walk() -> None:
    """FR-001. The walk must not spend itself repairing a `flush()` caller in the parent.

    `_taken_markers` names markers whose waiting thread did not survive the fork, so replacing
    their `Event` is work with no consumer. Unlike `_unclosed_swaps` this skip is hygiene rather
    than correctness — `_reinit_after_fork` empties the list either way, which is why removing the
    skip breaks no behavioural test. It is asserted here so the guard is not merely declared:
    without it the walk reaches the marker's `Event` and replaces it, and the control run proves
    the walk would otherwise get there.
    """
    from log_foundry.worker import Worker, _FlushMarker

    class _Quiet(Sink):
        def emit(self, batch: list[dict[str, object]]) -> None:
            """Accepts a batch."""

        def close(self) -> None:
            """Releases nothing."""

    worker = Worker(_Quiet())
    marker = _FlushMarker(seen_failures=0)
    try:
        _lifecycle._state._worker = worker
        worker._taken_markers = [marker]
        before = id(marker.event)
        _fork._reinit_primitives()
        assert id(marker.event) == before, (
            "the repair walk replaced the Event of a flush() caller that is in the parent"
        )

        # Control: the same walk, with the record renamed out from under the opt-out.
        worker._taken_markers = []
        worker._not_skipped = [marker]  # type: ignore[attr-defined]
        _fork._reinit_primitives()
        assert id(marker.event) != before, (
            "the control did not reach it either, so the assertion above proves nothing"
        )
    finally:
        _lifecycle._state._worker = None
        worker.shutdown(timeout=2.0)


def test_a_child_does_not_inherit_a_promise_that_a_close_is_running() -> None:
    """FR-002. Both slots naming an in-flight close are emptied for the child.

    `_fork._fresh_primitive` carries an `Event`'s set state across, and the thread that would
    have cleared these did not survive the fork — so a child inheriting either would answer its
    *own* later close instantly and exit through a close still running, which is the defect
    FR-002 closes, made permanent.
    """
    from log_foundry.worker import Worker

    class _Quiet(Sink):
        def emit(self, batch: list[dict[str, object]]) -> None:
            """Accepts a batch."""

        def close(self) -> None:
            """Releases nothing."""

    worker = Worker(_Quiet())
    try:
        _lifecycle._state._worker = worker  # only the *registered* worker is rebuilt
        worker._closing = threading.Event()
        worker._closing.set()
        _lifecycle._orphan_closing = 1
        _lifecycle._orphan_idle.clear()

        def in_child() -> str:
            """Reports both records as the child sees them, after the handlers have run."""
            return f"{worker._closing is None},{not _lifecycle._orphan_closing}"

        child = run_in_child(in_child)
    finally:
        _lifecycle._orphan_closing = 0
        _lifecycle._orphan_idle.set()
        _lifecycle._state._worker = None
        worker.shutdown(timeout=2.0)

    assert child.output == "True,True", child.output


_TESTS_DIR = pathlib.Path(__file__).parent

_TEST_MODULES = sorted(_TESTS_DIR.rglob("test_*.py"))


def _unguarded_forks(source: str) -> list[int]:
    """Returns the line of every ``os.fork()`` not immediately preceded by ``gc.collect()``.

    Derived rather than listed, for the reason SPEC-038 FR-001 measured: a roster of fork sites
    is a roster that goes stale the first time somebody adds a fifth one, and the failure is
    silent — a child left something fork-unsafe to finalize dies of ``SIGSEGV`` in whatever frame
    the collector happened to interrupt, which is a symptom nobody can search for.

    "Immediately preceded" is deliberately the strict reading. A collect further up the function
    is not equivalent: anything allocated in between is exactly what the guard exists to have
    already finalized, and a rule that accepted it would pass a site the guard does not protect.
    Only real ``ast.Call`` nodes count, so a fork named in a docstring or a string is not a site.

    Args:
      source: The text of one test module.

    Returns:
      The 1-based line numbers of the unguarded calls, in the order they appear.

    Raises:
      SyntaxError: If the source does not parse, which is a broken module rather than a finding.
    """

    def is_call_to(node: ast.AST, module: str, name: str) -> bool:
        return (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == name
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == module
        )

    tree = ast.parse(source)
    statement_of: dict[ast.AST, ast.stmt | None] = {}
    position: dict[ast.stmt, tuple[list[ast.stmt], int]] = {}

    def descend(node: ast.AST, statement: ast.stmt | None) -> None:
        for field, value in ast.iter_fields(node):
            items = value if isinstance(value, list) else [value]
            body = isinstance(value, list) and field in ("body", "orelse", "finalbody")
            for index, child in enumerate(items):
                if not isinstance(child, ast.AST):
                    continue
                if body and isinstance(child, ast.stmt):
                    position[child] = (value, index)
                    descend(child, child)
                else:
                    statement_of[child] = statement
                    descend(child, statement)

    descend(tree, None)
    offenders: list[int] = []
    for node, statement in statement_of.items():
        if statement is None or not is_call_to(node, "os", "fork"):
            continue
        body, index = position[statement]
        previous = body[index - 1] if index else None
        guarded = (
            isinstance(previous, ast.Expr)
            and is_call_to(previous.value, "gc", "collect")
            and not previous.value.args
        )
        if not guarded:
            offenders.append(getattr(node, "lineno", 0))
    return sorted(offenders)


@pytest.mark.parametrize("path", _TEST_MODULES, ids=lambda p: p.name)
def test_every_fork_collects_first(path: pathlib.Path) -> None:
    """Every fork in the suite empties the parent's garbage before the child inherits it.

    The rule this makes non-optional is stated in full on :func:`run_in_child`: a forked child
    that finalizes an object whose finalizer is not fork-safe dies, and the crash lands in
    whatever frame the collector interrupted rather than anywhere near the leak. Collecting in
    the parent leaves the child nothing to finalize. Four sites satisfy it today, and the point
    of deriving the roster is the fifth.
    """
    offenders = _unguarded_forks(path.read_text(encoding="utf-8"))
    assert not offenders, (
        f"{path.name} forks without collecting first, at line(s) "
        f"{', '.join(str(line) for line in offenders)}. Put a bare `gc.collect()` on the line "
        f"immediately above each `os.fork()` — see `run_in_child` for why."
    )


def test_the_fork_collection_rule_can_actually_fail() -> None:
    """The rule above is checked against sources it must reject and sources it must not.

    A lint is not tested by running it on the tree it already passes: that proves the tree is
    clean, not that the check works. Each rejected sample differs from an accepted one by the
    single thing the rule is about, so a rule that stopped discriminating fails here rather than
    going quiet.
    """
    accepted = {
        "guarded": "import gc, os\ngc.collect()\npid = os.fork()\n",
        "guarded inside a function": (
            "import gc, os\ndef f():\n    gc.collect()\n    return os.fork()\n"
        ),
        "guarded in a finally block": (
            "import gc, os\ntry:\n    pass\nfinally:\n    gc.collect()\n    os.fork()\n"
        ),
        "a fork named only in a string": 'import os\n"os.fork()"\n',
        "no fork at all": "import gc\ngc.collect()\n",
    }
    rejected = {
        "no collect": "import os\npid = os.fork()\n",
        "collect too far up": "import gc, os\ngc.collect()\nx = 1\npid = os.fork()\n",
        "collect after the fork": "import gc, os\npid = os.fork()\ngc.collect()\n",
        "collect on a generation": "import gc, os\ngc.collect(0)\npid = os.fork()\n",
        "a different collect": "import os\nother.collect()\npid = os.fork()\n",
        "guarded in the sibling branch": (
            "import gc, os\nif x:\n    gc.collect()\nelse:\n    os.fork()\n"
        ),
    }
    assert {name for name, src in accepted.items() if _unguarded_forks(src)} == set()
    assert {name for name, src in rejected.items() if not _unguarded_forks(src)} == set()
