"""SPEC-085's watched child: the call timer, the watchdog, the child runner and the personalities.

A cell runs in a child made by ``run_in_child`` (``tests/test_fork_lifecycle.py``), which bounds
a child twice, by its own alarm and by the parent's deadline. Those two say only that a child
died. This module adds what says why: every library call a cell or a script makes is registered
with the child's :class:`ChildRun` while it is in flight, every stretch of model sink code is
recorded as an interval, and a watchdog thread ends the child when a call's **own time** passes
its budget plus ``EPSILON`` plus ``HANG_AFTER``, writing one line naming the call and every
thread's stack to the child's pipe first.

**The model files import from ``tests/`` only ``run_in_child`` and ``_child_pipe``**, both of
which exist at every commit FR-005 runs the cells at. ``_child_pipe`` is read as a module
attribute at the moment of use, because the child assigns it after this module is imported. A
test that replaces the runner patches ``model_sinks.run_in_child``, the name this module calls.

**No duration is computed here.** A clock is read only into a recorded instant, and every
comparison of two instants is made by an oracle in ``tests/model_oracles.py``; loops with a time
limit are written as a deadline. That keeps ``tests/test_timing_floors.py``'s gate silent for a
reason rather than by luck, and keeps every assertion about time where its corpus can reach it.
"""

from __future__ import annotations

import contextlib
import faulthandler
import io
import json
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import log_foundry
import test_fork_lifecycle
from model_oracles import (
    CELL_TIMEOUT,
    WATCHDOG,
    Call,
    Interval,
    ModelFailure,
    check_call,
    hang_signature,
    has_watchdog_line,
    overdue,
    own_time,
)
from test_fork_lifecycle import run_in_child

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping, Sequence

    from model_oracles import Verdict

NESTED_TIMEOUT = 45
"""``run_in_child``'s timeout for a child of a child.

Above the exit drain's 36 s watchdog deadline plus ``WAITER_HOLD``, and far enough under
``CELL_TIMEOUT`` that the cell still reaps the nested child and reports. Whichever of the three
bounds ends a nested child, the cell's verdict key is ``ChildBroken:hung``.
"""

WATCH_EVERY = 0.05
"""Seconds between the watchdog's readings."""

SETTLE_EVERY = 0.005
"""Seconds between readings while a cell waits for every call and stretch to end."""

_RUN: ChildRun | None = None


class ChildRun:
    """Everything one watched child records: its calls, its sink code, its sinks, its findings.

    Attributes:
      sinks: Every model sink in this process, in order of creation, so an index names one; a
        forked child starts with the sinks it inherited.
      spans: Traced calls the cell and its scripts made.
      orphans: ``info`` calls the cell and its scripts made.
      abandoned: ``span.end`` events in batches the worker abandoned.
      margins: The worst margins nested children reported.
      properties: Values a body asked to have recorded on the test's report.
    """

    def __init__(self, sinks: Sequence[object] = ()) -> None:
        """Starts an empty record over the sinks given.

        Args:
          sinks: The model sinks this process already holds.

        Returns:
          None.

        Raises:
          None.
        """
        self._lock = threading.Lock()
        self._serial = 0
        self._calls: list[Call] = []
        self._flying: dict[int, tuple[str, int, float, float]] = {}
        self._intervals: list[Interval] = []
        self._open: dict[int, tuple[int, float]] = {}
        self._found: list[str] = []
        self._notes: list[str] = []
        self._local = threading.local()
        self._expected = False
        self._visited = False
        self._stderr: io.StringIO | None = None
        self.sinks: list[object] = list(sinks)
        self.spans = 0
        self.orphans = 0
        self.abandoned = 0
        self.margins: list[float] = []
        self.properties: dict[str, str] = {}

    def _token(self) -> int:
        """Returns a fresh key for an in-flight entry.

        Args:
          None.

        Returns:
          The key.

        Raises:
          None.
        """
        with self._lock:
            self._serial += 1
            return self._serial

    def register(self, sink: object) -> int:
        """Adds a model sink to the run and returns its index.

        Args:
          sink: The sink.

        Returns:
          Its index in :attr:`sinks`.

        Raises:
          None.
        """
        with self._lock:
            self.sinks.append(sink)
            return len(self.sinks) - 1

    def count(self, operation: str) -> None:
        """Counts one traced call or one orphan log for the ledger.

        Args:
          operation: ``"span"`` or ``"info"``.

        Returns:
          None.

        Raises:
          None.
        """
        with self._lock:
            if operation == "span":
                self.spans += 1
            elif operation == "info":
                self.orphans += 1

    def call(self, name: str, budget: float, work: Callable[[], object]) -> object:
        """Makes one library call, registered while it is in flight, and records it.

        An exception the call lets out is absorbed and recorded as ``CallRaised:<name>``, since
        the library promises never to fail its caller (invariant 1). Nothing is compared here.

        Args:
          name: The call's name in the record.
          budget: Its timeout, or the default it spends; ``0.0`` for a call given none.
          work: The call.

        Returns:
          What the call returned, or ``None`` when it raised.

        Raises:
          None.
        """
        token = self._token()
        thread = threading.get_ident()
        began = time.monotonic()
        with self._lock:
            self._flying[token] = (name, thread, budget, began)
        try:
            return work()
        except BaseException as exc:
            self.find(f"CallRaised:{name}", f"{name} raised {type(exc).__name__}: {exc}")
            return None
        finally:
            ended = time.monotonic()
            with self._lock:
                del self._flying[token]
                self._calls.append(Call(name, thread, budget, began, ended))

    @contextlib.contextmanager
    def sink_code(self) -> Iterator[None]:
        """Records the stretch of model sink code it wraps as an interval.

        Args:
          None.

        Returns:
          A context manager.

        Raises:
          None.
        """
        token = self._token()
        thread = threading.get_ident()
        began = time.monotonic()
        with self._lock:
            self._open[token] = (thread, began)
        try:
            yield
        finally:
            ended = time.monotonic()
            with self._lock:
                del self._open[token]
                self._intervals.append(Interval(thread, began, ended))

    def in_script(self) -> bool:
        """Returns whether the calling thread is inside a script.

        Args:
          None.

        Returns:
          Whether a script is running on this thread.

        Raises:
          None.
        """
        return bool(getattr(self._local, "script", False))

    @contextlib.contextmanager
    def scripting(self) -> Iterator[None]:
        """Marks the calling thread as inside a script while the block runs.

        Args:
          None.

        Returns:
          A context manager.

        Raises:
          None.
        """
        self._local.script = True
        try:
            yield
        finally:
            self._local.script = False

    def expect_site(self) -> None:
        """Declares that the body's sink code must run, or the verdict holds ``NotExercised``.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self._expected = True

    def visited(self) -> None:
        """Records that the declared site's sink code ran.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self._visited = True

    def find(self, signature: str, detail: str = "") -> None:
        """Adds a ``Class:key`` signature to the verdict.

        Args:
          signature: The signature.
          detail: Text for the failure message.

        Returns:
          None.

        Raises:
          None.
        """
        with self._lock:
            self._found.append(signature)
            if detail:
                self._notes.append(f"{signature}: {detail}")

    def check(self, oracle: Callable[..., None], *args: object) -> None:
        """Runs an oracle and keeps every signature it raises, so no oracle stops the others.

        Args:
          oracle: An oracle from ``model_oracles``.
          *args: Its arguments.

        Returns:
          None.

        Raises:
          None.
        """
        try:
            oracle(*args)
        except ModelFailure as failure:
            for signature in failure.signatures:
                self.find(signature, failure.detail)

    def note(self, text: str) -> None:
        """Adds a line to the text a failure prints.

        Args:
          text: The line.

        Returns:
          None.

        Raises:
          None.
        """
        with self._lock:
            self._notes.append(text)

    def prop(self, name: str, value: object) -> None:
        """Asks for a value to be recorded on the test's report.

        Args:
          name: The property's name.
          value: Its value, recorded as text.

        Returns:
          None.

        Raises:
          None.
        """
        with self._lock:
            self.properties[name] = str(value)

    def in_flight(self) -> tuple[list[Call], list[Interval]]:
        """Returns the calls in flight and every interval, both read as ending now.

        Args:
          None.

        Returns:
          The in-flight calls, and the closed and open intervals.

        Raises:
          None.
        """
        now = time.monotonic()
        with self._lock:
            flying = [Call(n, t, b, began, now) for n, t, b, began in self._flying.values()]
            stretches = list(self._intervals)
            stretches += [Interval(t, began, now) for t, began in self._open.values()]
        return flying, stretches

    def settle(self) -> None:
        """Waits until no call and no stretch of sink code is in flight.

        A cell whose procedure ends with a call still in flight waits for that call before it
        judges. The wait is bounded by the watchdog, which ends the child when a call it waits
        on stops making progress, and by the child's alarm.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        while True:
            with self._lock:
                if not self._flying and not self._open:
                    return
            time.sleep(SETTLE_EVERY)

    def calls(self) -> tuple[Call, ...]:
        """Returns every call recorded so far.

        Args:
          None.

        Returns:
          The calls.

        Raises:
          None.
        """
        with self._lock:
            return tuple(self._calls)

    def intervals(self) -> tuple[Interval, ...]:
        """Returns every stretch of sink code recorded, open ones read as ending now.

        Args:
          None.

        Returns:
          The intervals.

        Raises:
          None.
        """
        return tuple(self.in_flight()[1])

    def fork(self, work: Callable[[ChildRun], dict[str, object]]) -> tuple[str, dict[str, object]]:
        """Forks a child of this child through ``run_in_child`` and reaps it.

        Threads do not survive a fork, so the nested child starts a watchdog of its own on its
        own pipe, and judges its own calls by the same universal step a cell's child does. Its
        signatures join this run's verdict. A nested child that does not report adds
        ``ChildBroken:hung`` when a watchdog, its alarm or the deadline ended it, and
        ``ChildBroken:crashed`` when it ended any other way.

        Args:
          work: Called in the nested child with its own run; returns the child's report.

        Returns:
          ``("reported", report)``, or ``("hung", {})`` or ``("crashed", {})``.

        Raises:
          None.
        """
        inherited = list(self.sinks)

        def child() -> str:
            run = _begin(inherited)
            report = work(run)
            return json.dumps({**_finish(run), "report": report})

        reaped = run_in_child(child, timeout=NESTED_TIMEOUT)
        if reaped.finished:
            try:
                data = json.loads(reaped.output)
            except ValueError:
                data = None
            if isinstance(data, dict):
                for signature in data["signatures"]:
                    self.find(signature, "found in a forked child")
                if data["margin"] is not None:
                    with self._lock:
                        self.margins.append(float(data["margin"]))
                for line in data["notes"]:
                    self.note(f"forked child: {line}")
                return "reported", dict(data["report"])
        ending = "hung" if has_watchdog_line(reaped.output) or reaped.blocked else "crashed"
        self.find(f"ChildBroken:{ending}", f"a forked child {reaped.ending}: {reaped.output}")
        return ending, {}


def current() -> ChildRun:
    """Returns this process's run.

    Args:
      None.

    Returns:
      The run the watched child started.

    Raises:
      RuntimeError: If no watched child is running in this process.
    """
    if _RUN is None:
        raise RuntimeError("model sink code runs only inside a watched child")
    return _RUN


def _pipe() -> int:
    """Returns the descriptor of this child's pipe, read at the moment of use.

    Args:
      None.

    Returns:
      The write end of the pipe ``run_in_child`` gave this process, or stderr's.

    Raises:
      None.
    """
    pipe = test_fork_lifecycle._child_pipe
    if pipe is not None and pipe[0] == os.getpid():
        return pipe[1]
    return 2


def _end_hung(call: Call) -> None:
    """Writes the watchdog's line and every thread's stack to the pipe, and ends the child.

    Args:
      call: The overdue call.

    Returns:
      None.

    Raises:
      None.
    """
    names = {hex(thread.ident or 0): thread.name for thread in threading.enumerate()}
    record = {"call": call.name, "thread": hex(call.thread), "threads": names}
    fd = _pipe()
    os.write(fd, f"\n{WATCHDOG}{json.dumps(record)}\n".encode())
    faulthandler.dump_traceback(file=fd, all_threads=True)
    os._exit(3)


def _watch(run: ChildRun) -> None:
    """Reads the in-flight calls until one is overdue, then ends the child.

    Args:
      run: The child's run.

    Returns:
      None.

    Raises:
      None.
    """
    while True:
        time.sleep(WATCH_EVERY)
        flying, stretches = run.in_flight()
        late = overdue(flying, stretches)
        if late is not None:
            _end_hung(late)


def start_watchdog(run: ChildRun) -> None:
    """Starts the watchdog thread for a run, from a function so no constructor starts a thread.

    Args:
      run: The child's run.

    Returns:
      None.

    Raises:
      None.
    """
    threading.Thread(target=_watch, args=(run,), name="model-watchdog", daemon=True).start()


def _begin(sinks: Sequence[object]) -> ChildRun:
    """Starts a run in a freshly forked child: its record, its stderr and its watchdog.

    Args:
      sinks: The model sinks the child inherited.

    Returns:
      The run.

    Raises:
      None.
    """
    global _RUN
    run = ChildRun(sinks)
    _RUN = run
    run._stderr = io.StringIO()
    sys.stderr = run._stderr
    _prepare(run)
    start_watchdog(run)
    return run


def _prepare(run: ChildRun) -> None:
    """Prepares the child for the cell's oracles; a hook the personalities' half extends.

    Args:
      run: The child's run.

    Returns:
      None.

    Raises:
      None.
    """
    del run


def _finish(run: ChildRun) -> dict[str, object]:
    """Applies what every child is held to and returns its report.

    Waits for every call and stretch to end, then holds every call to the time oracle, and
    adds ``NotExercised:site`` when a site was declared and never reached. Every oracle runs
    whatever the others found; the verdict is the set of all their signatures.

    Args:
      run: The child's run.

    Returns:
      The report: signatures, notes, worst margin, the library's file and the properties.

    Raises:
      None.
    """
    run.settle()
    calls = run.calls()
    stretches = run.intervals()
    margins = list(run.margins)
    for call in calls:
        run.check(check_call, call, stretches)
        margins.append(own_time(call, stretches) - call.budget)
    if run._expected and not run._visited:
        run.find("NotExercised:site", "the cell's sink code never ran")
    table = ", ".join(
        f"{call.name}={own_time(call, stretches):.3f}/{call.budget:g}" for call in calls
    )
    run.note(f"calls (own time/budget): {table}")
    if run._stderr is not None:
        run.note(f"stderr: {run._stderr.getvalue()[-1500:]}")
    return {
        "signatures": sorted(set(run._found)),
        "notes": list(run._notes),
        "margin": max(margins) if margins else None,
        "file": log_foundry.__file__,
        "properties": dict(run.properties),
    }


@dataclass(frozen=True)
class Outcome:
    """What the parent read from one watched child.

    Attributes:
      verdict: The set of ``Class:key`` signatures; empty for a pass.
      text: What a failure prints: the child's notes, or the watchdog's line and every stack.
      margin: The worst ``own time - budget``; ``None`` when the child did not report.
      file: The child's ``log_foundry.__file__``; ``None`` when it did not report.
      stacks: Each named thread's library frames, innermost first; empty unless it hung.
      properties: Values the body asked to have recorded.
    """

    verdict: Verdict
    text: str
    margin: float | None = None
    file: str | None = None
    stacks: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    properties: Mapping[str, str] = field(default_factory=dict)


def _crash_key(status: int, output: str) -> str:
    """Names how a child ended that neither reported nor was ended by a watchdog.

    Args:
      status: The raw wait status.
      output: What the child wrote.

    Returns:
      ``raised-<Type>`` for an exception out of the cell's own code, the signal's name, or
      ``exit-<code>``.

    Raises:
      None.
    """
    if output.startswith("!") and ":" in output:
        return f"raised-{output[1:].split(':', 1)[0]}"
    if os.WIFSIGNALED(status):
        number = os.WTERMSIG(status)
        try:
            return signal.Signals(number).name
        except ValueError:
            return f"signal-{number}"
    if os.WIFEXITED(status):
        return f"exit-{os.WEXITSTATUS(status)}"
    return f"status-{status}"


def read_child(child: object) -> Outcome:
    """Reads a reaped cell child into an outcome.

    A child that finished gives the verdict it reported. One whose output holds the watchdog's
    line gives ``HangFound:<call>@<module>.<function>``, with every stack as the text. One that
    a watchdog signal ended with no line gives ``HangFound:silent``. Any other ending gives
    ``ChildCrashed``, which no pin may expect.

    Args:
      child: What ``run_in_child`` returned, read through ``finished``, ``blocked``, ``status``
        and ``output``.

    Returns:
      The outcome.

    Raises:
      None.
    """
    output = str(getattr(child, "output", ""))
    status = int(getattr(child, "status", 0))
    if getattr(child, "finished", False):
        try:
            report = json.loads(output)
        except ValueError:
            return Outcome(frozenset({"ChildCrashed:unreadable"}), output[-4000:])
        return Outcome(
            frozenset(report["signatures"]),
            "\n".join(report["notes"]),
            report["margin"],
            report["file"],
            {},
            dict(report["properties"]),
        )
    if has_watchdog_line(output):
        signature, stacks = hang_signature(output, os.path.dirname(log_foundry.__file__))
        return Outcome(frozenset({signature}), output, stacks=stacks)
    if getattr(child, "blocked", False):
        return Outcome(frozenset({"HangFound:silent"}), output[-4000:])
    return Outcome(frozenset({f"ChildCrashed:{_crash_key(status, output)}"}), output[-4000:])


def run_body(body: Callable[[ChildRun], None], *, timeout: int = CELL_TIMEOUT) -> Outcome:
    """Runs a body in a watched child and returns what the parent read from it.

    Args:
      body: Called in the child with the child's run.
      timeout: ``run_in_child``'s timeout for the child.

    Returns:
      The outcome.

    Raises:
      None.
    """

    def work() -> str:
        run = _begin(())
        body(run)
        return json.dumps(_finish(run))

    return read_child(run_in_child(work, timeout=timeout))
