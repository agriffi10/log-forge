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
from log_foundry import _lifecycle
from log_foundry.worker import Worker
from model_oracles import (
    CELL_TIMEOUT,
    REACH_TIMEOUT,
    WATCHDOG,
    Call,
    ChildReport,
    Closes,
    Interval,
    Ledger,
    ModelFailure,
    check_call,
    check_child,
    hang_signature,
    has_watchdog_line,
    overdue,
    own_time,
)
from test_fork_lifecycle import run_in_child

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping, Sequence

    from model_oracles import Verdict
    from model_pins import Hooks

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

    def abandon(self, ends: int) -> None:
        """Counts ``span.end`` events in a batch the worker abandoned.

        Args:
          ends: The events.

        Returns:
          None.

        Raises:
          None.
        """
        with self._lock:
            self.abandoned += ends

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
    """Prepares the child for the ledger: watches ``Worker._emit`` for abandoned batches.

    A batch refused while a factory is inside its own ``flush()`` or ``shutdown()`` is a counted
    failure, not a defect, so its ``span.end`` events are the ledger's abandoned term. They are
    observed by wrapping ``Worker._emit``, **in the child only**: when ``failed_batches`` rose
    across a call, the ``span.end`` events in the event lists that call was handed are abandoned.
    ``failed_batches`` alone will not do, since one batch can hold several traced calls. A child
    of a child inherits the wrapper, which reads the run at the time of the call.

    Args:
      run: The child's run.

    Returns:
      None.

    Raises:
      None.
    """
    del run
    real = Worker._emit
    if getattr(real, "_model_watched", False):
        return

    def watched(self: Worker, event_lists: list[list[dict[str, object]]]) -> None:
        before = self.failed_batches
        try:
            real(self, event_lists)
        finally:
            if self.failed_batches > before:
                ends = sum(
                    1
                    for batch in event_lists
                    for event in batch
                    if event.get("message") == SPAN_END
                )
                current().abandon(ends)

    watched._model_watched = True  # type: ignore[attr-defined]
    Worker._emit = watched  # type: ignore[method-assign]


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


SPAN_END = "span.end"
"""The message of the event that closes a traced call, which the span ledger counts."""

ORPHAN = "orphan"
"""The message of a model orphan log, which the orphan ledger counts."""

FLUSH_TIMEOUT = 2.0
"""The ``flush`` operation's timeout."""

SHUTDOWN_TIMEOUT = 3.0
"""The ``shutdown`` operation's timeout, and a ``waiter``'s ``shutdown`` stop's."""

FINAL_SHUTDOWN_TIMEOUT = 5.0
"""The cell's own ``shutdown``'s timeout."""


@log_foundry.trace
def _model_span() -> None:
    """One traced call that logs once.

    Args:
      None.

    Returns:
      None.

    Raises:
      None.
    """
    log_foundry.info("in")


def install_sink(sink: object) -> None:
    """Changes the sink to one given directly: ``configure(sink=…)``.

    Args:
      sink: The sink.

    Returns:
      None.

    Raises:
      None.
    """
    log_foundry.configure(service="model", sink=sink)  # type: ignore[arg-type]


def install_built(sink: object) -> None:
    """Changes the sink to one a factory returns: ``configure(sink_factory=…)``.

    Args:
      sink: The sink the factory returns.

    Returns:
      None.

    Raises:
      None.
    """
    log_foundry.configure(service="model", sink_factory=lambda: sink)  # type: ignore[arg-type,return-value]


def operate(operation: str, hooks: Hooks, install: Callable[[object], None]) -> None:
    """Makes one operation of a script, through the run's timer, and counts it for the ledger.

    Args:
      operation: One of ``model_pins.OPERATIONS``.
      hooks: The hooks whose ``new_sink`` a ``configure`` installs.
      install: How a ``configure`` installs that sink.

    Returns:
      None.

    Raises:
      ValueError: For an operation no script holds.
    """
    run = current()
    if operation == "info":
        run.count("info")
        run.call("info", 0.0, lambda: log_foundry.info(ORPHAN))
    elif operation == "span":
        run.count("span")
        run.call("span", 0.0, _model_span)
    elif operation == "flush":
        run.call("flush", FLUSH_TIMEOUT, lambda: log_foundry.flush(timeout=FLUSH_TIMEOUT))
    elif operation == "flush-none":
        run.call("flush-none", 0.0, lambda: log_foundry.flush(timeout=None))
    elif operation == "shutdown":
        run.call(
            "shutdown", SHUTDOWN_TIMEOUT, lambda: log_foundry.shutdown(timeout=SHUTDOWN_TIMEOUT)
        )
    elif operation == "shutdown-none":
        run.call("shutdown-none", 0.0, lambda: log_foundry.shutdown(timeout=None))
    elif operation == "configure":
        sink = hooks.new_sink()
        run.call("configure", _lifecycle.DEFAULT_SWAP_TIMEOUT, lambda: install(sink))
    else:
        raise ValueError(f"no such operation: {operation}")


def run_script(script: tuple[str, ...], hooks: Hooks, install: Callable[[object], None]) -> None:
    """Makes a script's operations in order.

    Args:
      script: The operations.
      hooks: The hooks a ``configure`` asks for its sink.
      install: How a ``configure`` installs it.

    Returns:
      None.

    Raises:
      ValueError: For an operation no script holds.
    """
    for operation in script:
        operate(operation, hooks, install)


def reach(path: str) -> None:
    """Reaches the installed sink on a path: one ``info()``, or one traced call and a flush.

    The worker path's flush is ``REACH_TIMEOUT`` long, so it never races a finite wait inside a
    script the delivery runs.

    Args:
      path: ``"orphan"`` or ``"worker"``.

    Returns:
      None.

    Raises:
      ValueError: For an unknown path.
    """
    run = current()
    if path == "orphan":
        run.count("info")
        run.call("info", 0.0, lambda: log_foundry.info(ORPHAN))
    elif path == "worker":
        run.count("span")
        run.call("span", 0.0, _model_span)
        run.call("reach-flush", REACH_TIMEOUT, lambda: log_foundry.flush(timeout=REACH_TIMEOUT))
    else:
        raise ValueError(f"no such path: {path}")


def visit(
    sink: object, site: str, hooks: Hooks, perform: Callable[[tuple[str, ...]], None]
) -> None:
    """Asks the hooks what runs at a visit to a site, and runs it as a stretch of sink code.

    A thread already inside a script returns at once, without asking: a thread inside a script
    starts no second script, and no answer is drawn and discarded during one. Nothing here holds
    a lock of the sink's own.

    Args:
      sink: The sink visited.
      site: The site.
      hooks: The sink's hooks.
      perform: What runs the answer.

    Returns:
      None.

    Raises:
      None.
    """
    run = current()
    if run.in_script():
        return
    script = hooks.script(sink, site)
    if not script:
        return
    with run.sink_code(), run.scripting():
        perform(script)


class Recorder:
    """Counts the delivery calls entering it, and that count at each close's entry and return.

    Every personality is built on it. Its one lock is a plain ``threading.Lock`` held only
    around its own bookkeeping, never across a script, a wait or a fork, and never an ``RLock``:
    a script's ``info`` re-enters the same sink's ``emit`` on the same thread, so a lock held
    there would hang the cell rather than pass it.

    Attributes:
      writes: The delivery calls that entered it.
      events: Every event it was handed.
      index: Its position in the run's sinks.
    """

    def __init__(self) -> None:
        """Starts an empty record and registers the sink with the run.

        Args:
          None.

        Returns:
          None.

        Raises:
          RuntimeError: Outside a watched child.
        """
        self._lock = threading.Lock()
        self.writes = 0
        self._entries: list[int] = []
        self._returns: list[int] = []
        self.events: list[dict[str, object]] = []
        self.index = current().register(self)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Counts the write at entry, keeps the events, then runs whatever the sink adds.

        Args:
          batch: The events.

        Returns:
          None.

        Raises:
          None.
        """
        with self._lock:
            self.writes += 1
            self.events.extend(batch)
        self._after_emit()

    def close(self) -> None:
        """Records the count at entry, runs whatever the sink adds, records the count at return.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        with self._lock:
            self._entries.append(self.writes)
        self._in_close()
        with self._lock:
            self._returns.append(self.writes)

    def _after_emit(self) -> None:
        """Runs after an emit's bookkeeping; a plain recorder runs nothing.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """

    def _in_close(self) -> None:
        """Runs between a close's two counts; a plain recorder runs nothing.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """

    def closes(self) -> Closes:
        """Returns the record the close oracles read.

        Args:
          None.

        Returns:
          The writes and the count at each close's entry and return.

        Raises:
          None.
        """
        with self._lock:
            return Closes(self.writes, tuple(self._entries), tuple(self._returns))

    def count(self, message: str) -> int:
        """Returns how many of its events carry a message.

        Args:
          message: The message.

        Returns:
          The count.

        Raises:
          None.
        """
        with self._lock:
            return sum(1 for event in self.events if event.get("message") == message)


class Reentrant(Recorder):
    """Declares ``log_foundry_stop_signal`` as a property, and calls back from its own code.

    Its setter, its ``emit`` and its ``close`` each visit their site after the recorder's own
    bookkeeping: the setter after the value is stored, on every assignment, ``None`` included;
    ``emit`` after the write is counted and the events kept; ``close`` after the entry count is
    recorded and before the return count.
    """

    def __init__(self, hooks: Hooks, install: Callable[[object], None] = install_sink) -> None:
        """Builds the sink over its hooks.

        Args:
          hooks: What it asks at each visit.
          install: How a script's ``configure`` installs the sink the hooks supply.

        Returns:
          None.

        Raises:
          RuntimeError: Outside a watched child.
        """
        super().__init__()
        self._signal: object = None
        self._hooks = hooks
        self._install = install

    @property
    def log_foundry_stop_signal(self) -> object:
        """Returns the signal the library last handed the sink.

        Args:
          None.

        Returns:
          The signal, or ``None``.

        Raises:
          None.
        """
        return self._signal

    @log_foundry_stop_signal.setter
    def log_foundry_stop_signal(self, value: object) -> None:
        """Stores the signal, then visits ``setter``.

        Args:
          value: The signal, or ``None``.

        Returns:
          None.

        Raises:
          None.
        """
        self._signal = value
        self._visit("setter")

    def _after_emit(self) -> None:
        """Visits ``emit``.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self._visit("emit")

    def _in_close(self) -> None:
        """Visits ``close``.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self._visit("close")

    def _visit(self, site: str) -> None:
        """Asks the hooks at a visit and runs the answer.

        Args:
          site: The site.

        Returns:
          None.

        Raises:
          None.
        """
        visit(self, site, self._hooks, self._perform)

    def _perform(self, script: tuple[str, ...]) -> None:
        """Runs a script the hooks answered.

        Args:
          script: The script.

        Returns:
          None.

        Raises:
          ValueError: For an operation no script holds.
        """
        run_script(script, self._hooks, self._install)


def factory(hooks: Hooks) -> Callable[[], Recorder]:
    """Returns a sink factory that asks its hooks at every build, then returns a new recorder.

    A script the hooks answer runs before the factory returns, so a script's calls are made
    from inside the library's build. Its ``configure`` installs the hooks' sink through
    ``configure(sink_factory=…)``, as the personality itself was installed.

    Args:
      hooks: What the factory asks.

    Returns:
      The factory.

    Raises:
      None.
    """

    def build() -> Recorder:
        recorder = Recorder()
        visit(
            recorder,
            "factory",
            hooks,
            lambda script: run_script(script, hooks, install_built),
        )
        return recorder

    return build


def stranded_markers() -> int:
    """Counts the flush markers left on the worker's queue, which ``health().queued`` counts.

    The rule of ``tests/test_invariants_model.py``'s ``_stranded_markers``, written again here
    because the model files import nothing from ``tests/`` but the child runner.

    Args:
      None.

    Returns:
      The items on the queue that are not a submission.

    Raises:
      None.
    """
    worker = _lifecycle._state.worker_exists()
    if worker is None:
        return 0
    with worker._queue.mutex:
        return sum(1 for item in worker._queue.queue if not isinstance(item, list))


def read_ledger(run: ChildRun) -> Ledger:
    """Reads the ledger's terms from the run, the sinks and ``health()``.

    Args:
      run: The child's run.

    Returns:
      The ledger.

    Raises:
      None.
    """
    health = log_foundry.health()
    sinks = [sink for sink in run.sinks if isinstance(sink, Recorder)]
    return Ledger(
        spans=run.spans,
        span_ends=sum(sink.count(SPAN_END) for sink in sinks),
        queued=health.queued - stranded_markers(),
        dropped=health.dropped,
        abandoned=run.abandoned,
        orphans=run.orphans,
        orphans_delivered=sum(sink.count(ORPHAN) for sink in sinks),
        orphan_lost=health.orphan_lost,
    )


def read_end() -> tuple[str, int]:
    """Reads the lifecycle state's name and the swap and stop tokens held.

    Args:
      None.

    Returns:
      The state's name and the token count.

    Raises:
      None.
    """
    state = _lifecycle._state
    return state.state.name, len(state._swaps_running) + len(state._stops_running)


class Waiter(Recorder):
    """Keeps the signal it is handed in a plain attribute, and waits on it when armed.

    When the driver has **armed** it for ``emit`` or for ``close``, by assigning :attr:`armed`,
    the next such call waits on the event it holds at that moment, with ``Event.wait``, for at
    most its hold; holding none, it sleeps that long. The wait is a stretch of sink code, and
    :attr:`entered` is set as it begins.

    Attributes:
      log_foundry_stop_signal: The signal the library last assigned; ``None`` until then.
      hold: The longest a wait lasts.
      armed: ``"emit"``, ``"close"`` or ``None``; cleared when the wait begins.
      entered: Set when an armed wait begins.
      waits: The began and ended instants of each wait.
    """

    log_foundry_stop_signal: object = None

    def __init__(self, hold: float) -> None:
        """Builds an unarmed waiter.

        Args:
          hold: The longest a wait lasts.

        Returns:
          None.

        Raises:
          RuntimeError: Outside a watched child.
        """
        super().__init__()
        self.hold = hold
        self.armed: str | None = None
        self.entered = threading.Event()
        self.waits: list[tuple[float, float]] = []

    def _after_emit(self) -> None:
        """Waits, when armed for ``emit``.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self._wait("emit")

    def _in_close(self) -> None:
        """Waits, when armed for ``close``.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self._wait("close")

    def _wait(self, site: str) -> None:
        """Waits on the event the sink holds now, or sleeps, for at most the hold.

        Args:
          site: The method waiting.

        Returns:
          None.

        Raises:
          None.
        """
        if self.armed != site:
            return
        self.armed = None
        run = current()
        run.visited()
        held = self.log_foundry_stop_signal
        with run.sink_code():
            began = time.monotonic()
            self.entered.set()
            wait = getattr(held, "wait", None)
            if callable(wait):
                wait(self.hold)
            else:
                time.sleep(self.hold)
            ended = time.monotonic()
        with self._lock:
            self.waits.append((began, ended))


class Forker(Reentrant):
    """Forks once, through ``run_in_child``, where its hooks answer ``("fork",)``, and reaps it.

    The fork and its reaping are one stretch of sink code, on the thread that reached the site,
    with no lock of the sink's own held, so the child can write to the sink it inherited.

    Attributes:
      forks: How many children it has forked.
    """

    forks = 0

    def _perform(self, script: tuple[str, ...]) -> None:
        """Forks for ``("fork",)``, and runs any other script as a ``reentrant`` sink does.

        Args:
          script: The script.

        Returns:
          None.

        Raises:
          ValueError: For an operation no script holds.
        """
        if script != ("fork",):
            super()._perform(script)
            return
        self.forks += 1
        run = current()
        ending, report = run.fork(forked_child)
        if ending == "reported":
            run.check(
                check_child,
                ChildReport(
                    ending,
                    tuple(report["orphan_in"]),  # type: ignore[arg-type]
                    tuple(report["span_in"]),  # type: ignore[arg-type]
                    bool(report["counters_moved"]),
                    tuple(report["closed_inherited"]),  # type: ignore[arg-type]
                    str(report["state"]),
                ),
            )


def forked_child(run: ChildRun) -> dict[str, object]:
    """The script of a ``forker``'s child: one orphan log, one traced call, a flush, a shutdown.

    Each call goes through the child's timer, so the universal step holds all four to the time
    oracle. The report says which sinks the two events landed in, by index, whether a counter
    moved instead, which inherited sinks the child closed, and the state it ended in.

    Args:
      run: The child's run, over the sinks it inherited.

    Returns:
      The report ``check_child`` reads.

    Raises:
      None.
    """
    sinks = [sink for sink in run.sinks if isinstance(sink, Recorder)]
    before = [(s.count(ORPHAN), s.count(SPAN_END), len(s.closes().at_entry)) for s in sinks]
    first = log_foundry.health()
    run.count("info")
    run.call("info", 0.0, lambda: log_foundry.info(ORPHAN))
    run.count("span")
    run.call("span", 0.0, _model_span)
    run.call("flush", FLUSH_TIMEOUT, lambda: log_foundry.flush(timeout=FLUSH_TIMEOUT))
    run.call("shutdown", SHUTDOWN_TIMEOUT, lambda: log_foundry.shutdown(timeout=SHUTDOWN_TIMEOUT))
    run.settle()
    last = log_foundry.health()
    counters = ("queued", "dropped", "failed_batches", "orphan_lost")
    report = {
        "orphan_in": [
            s.index for s, b in zip(sinks, before, strict=True) if s.count(ORPHAN) > b[0]
        ],
        "span_in": [
            s.index for s, b in zip(sinks, before, strict=True) if s.count(SPAN_END) > b[1]
        ],
        "counters_moved": any(getattr(last, c) > getattr(first, c) for c in counters),
        "closed_inherited": [
            s.index for s, b in zip(sinks, before, strict=True) if len(s.closes().at_entry) > b[2]
        ],
        "state": read_end()[0],
    }
    run.note(f"forked child: {report}")
    return report
