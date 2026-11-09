"""A timing bound must measure what its test claims: a floor, a ceiling, or a CPU window.

A floor such as ``assert elapsed >= 0.4`` holds only if the delay behind it begins **after** the
test's ``time.monotonic()`` read. When the delay begins first, every preemption between the two is
subtracted from ``elapsed`` and the floor flakes under load. That happened six times, each fixed
one site at a time: ``test_orphan_sink_handoff`` (a ``Timer`` started on the line before the
clock), the NATS ``SlowFlusherNATS`` fake (a ``call_later`` in the fake's ``__init__``, which ran
inside the sink's constructor), and the four sites PR 314 fixed (a close that sleeps from the
moment the test sees it enter, and a sleeping emit a worker interval could start early). The
structural answer is ``conftest.release_after_clock`` and ``conftest.DelayArmedOnUse``, which make
"arm the delay after the clock" the easy path, and this gate, which refuses the shapes that are not.

**The population is derived, not listed.** Every ``*.py`` under ``tests/`` is parsed, and a
*floor* is any comparison that puts a *duration* on its greater side (``d >= x``, ``d > x``,
``x <= d``, ``x < d``). A duration is ``<clock>() - <name>``, a name assigned from one, a value
returned at a fixed position by a function whose ``return`` is one (``_timed`` in the NATS tests),
or ``watch.elapsed`` on a stopwatch from ``release_after_clock`` or ``Stopwatch()``. The clock a
floor is measured against is the last ``<name> = <clock>()`` before it (an annotated or a tuple
assignment counts), the call to the duration-returning function, or the stopwatch's ``with`` line.

**What it refuses, each with its own reason:**

- :data:`ARMED_BEFORE_CLOCK`: a delay armed in the test's own body on a line before the clock its
  floor is read on, where arming is ``.call_later``, ``.call_at``, ``.arm()`` or ``.start()`` on a
  ``Timer``. The orphan-handoff shape.
- :data:`ARMED_IN_CONSTRUCTOR`: a ``__init__``, ``__new__`` or ``__post_init__`` of any class under
  ``tests/`` that arms a delay. This is refused whether or not a floor uses the class, because a
  constructor runs whenever its caller does, and a sink that connects in its own constructor builds
  its fake before any clock a test can start. The ``SlowFlusherNATS`` shape.
- :data:`SLEEP_AFTER_SIGNAL`: a floor on a raw clock, in a test that waits on a fake's ``E.wait()``
  before that clock, where the test names a class (or a subclass of one) whose method sets
  ``self.E`` and later sleeps. The sleep is then already running when the clock starts. The PR 314
  closer shape. A floor read from ``release_after_clock(seconds, <x>.R.set)`` is exempt when a
  class the test names waits on ``self.R``: the delay is then the helper's release, armed after
  its clock by construction. A helper that releases an event no named class waits on is not.
- :data:`UNRESOLVED_CLOCK`: a floor whose clock the scan cannot find, such as one started on an
  attribute (``self.began``) or in a walrus. Refused rather than skipped, because a floor the gate
  counts but cannot judge would read as guarded.
- :data:`CEILING_UNDER_STALL`: a *ceiling*, the same durations on the lesser side of a comparison,
  whose bound resolves to a number under :data:`STALL_ALLOWANCE`. A call that returns at once took
  123 ms on CI, so ``elapsed < 0.1`` is a coin toss there: the NATS matrix's shared 0.1 s bound
  and the NATS liveness tests' 0.05 s latch bounds were this shape. A ceiling is half the wait
  the defect it guards would make, with that wait raised to a second or more. The bound resolves
  through literals, ``+ - * /`` and module-level names, followed into a sibling test module they
  are imported from.
- :data:`RAW_CPU_READ`: a reference under ``tests/`` to a CPU clock by name (``time.process_time``
  and ``thread_time``, their ``_ns`` forms, ``resource.getrusage``, ``os.times``,
  ``pthread_getcpuclockid`` and the CPU clock ids) other than in the table ``conftest.cpu_window``
  reads from. A process-wide reading counted a full garbage collection and every leaked thread
  against a 0.05 s bound, and flaked at 0.104 s; the helper pauses collection and makes the test
  name its scope.

**What it does not see, recorded rather than chased:**

- control flow: order is by line, so a timer armed in an earlier loop iteration or in a callee is
  invisible, and so is a delay armed in a helper function or a fixture the test calls first;
- a fake's sleep started by anything other than an entry signal the test waited for, such as a
  worker's flush interval starting a sleeping ``emit`` before ``flush`` is called (PR 314's
  ``numeric_doors`` shape, which only the helper prevents);
- a sleeping fake the test reaches through a fixture or a factory rather than by naming its class;
- a duration that crosses a function boundary other than through a returned value, is unpacked by
  a ``for`` loop, or is scaled (``(time.monotonic_ns() - start) / 1e9``), and a floor written as
  ``== pytest.approx(...)``;
- a delay armed indirectly, such as ``loop.call_soon_threadsafe(loop.call_later, ...)``, a
  thread whose target sleeps, or a constructor that starts an asyncio task which sleeps (as
  ``ReconnectingFakeNATS.__init__`` does; refusing it would refuse that fake, which no floor uses);
- a floor written without a ``<clock>() - <start>`` subtraction, such as ``end - start`` with both
  read from a clock, or ``time.monotonic() >= start + 0.4``;
- a fake that lingers by any means other than a call named ``sleep`` (a timed
  ``self.never.wait(0.4)``, or a sleep inside a helper method it calls), and a test that polls the
  entry signal with ``is_set()`` instead of waiting on it;
- classes and duration-returning functions are matched by bare name across every file, so two
  same-named fakes in different modules are judged as one. That fails closed for the sleep check
  and could fail open for the helper exemption, if one module's fake waits on the event another
  module's same-named sleeper ignores.

- for a ceiling, a bound that does not resolve: a local (``took < budget / 2``), an attribute
  (``_lifecycle.DEFAULT_CLOSER_GRACE / 2``), a call, or a constant from ``src/``. Those are
  usually derived bounds, which is the point, but a small one written as a local is not judged;
- a ceiling on a duration the scan does not recognise, by the same rules as a floor: a ``for``
  unpack (the NATS capability test's three calls), a ``max(...)`` over a list of durations, a
  subtraction of two recorded instants (``measured["returned"] - measured["released"]``), or a
  tuple field of a helper that returns a duration it did not compute from a clock itself;
- a ceiling on a nanosecond clock (``time.monotonic_ns() - start < 50_000_000``), which is
  recognised and then judged as that many seconds, so it is never refused;
- a ``while`` condition is not a ceiling and is skipped, so a deadline loop is never refused, and
  neither is a ceiling a test writes as a loop condition;
- a CPU clock reached without its name: ``os`` imported under another name (``o.times()``), or
  ``getattr(time, "process_time")``.

Each is a reason to use ``release_after_clock`` in a new floor, and to derive a new ceiling from
the wait it guards, not a hole this gate claims to close.
Two false positives are accepted because they fail closed: a hang-guard ``Timer`` armed before a
helper floor is refused (move it after the ``with``), and a duration-returning helper's call line is
taken as its clock, so a timer armed between ``began`` and that call is refused (inline the
subtraction).

Deadlines a sink anchors inside the measured call, such as NATS's ``time.monotonic() +
publish_timeout`` on its loop thread, are safe and stay silent: they are read after the test's
clock, and asyncio fires a handle at most ``_clock_resolution`` early.
"""

from __future__ import annotations

import ast
import dataclasses
import functools
import gc
import pathlib
import threading
import time
from typing import TYPE_CHECKING

import pytest

from conftest import cpu_window, release_after_clock

if TYPE_CHECKING:
    from collections.abc import Callable

TESTS = pathlib.Path(__file__).parent

CLOCKS = frozenset({"monotonic", "perf_counter", "monotonic_ns", "perf_counter_ns", "time"})
"""Callable names whose result is a clock reading: ``time.monotonic()`` and ``loop.time()``."""

STOPWATCHES = frozenset({"release_after_clock", "Stopwatch"})
"""The ``conftest`` helpers whose result carries a clock read before any delay it arms."""

CONSTRUCTORS = frozenset({"__init__", "__new__", "__post_init__"})

ARMED_BEFORE_CLOCK = "a delay is armed before the clock its floor is measured against"
ARMED_IN_CONSTRUCTOR = "a constructor arms a delay, so it runs before any clock a test starts"
SLEEP_AFTER_SIGNAL = (
    "the test waits for a fake's entry signal before a raw clock, and the fake sleeps after it"
)
UNRESOLVED_CLOCK = "a floor whose clock the scan cannot find, so it cannot be judged"
RAW_CPU_READ = "a CPU-time clock read outside conftest.cpu_window, which pauses collection"
CEILING_UNDER_STALL = "a ceiling on a duration under STALL_ALLOWANCE, which a CI stall can reach"

STALL_ALLOWANCE = 0.5
"""Seconds below which no upper bound on a measured duration may sit.

CI has stalled a call that returns at once for 123 ms (PR 317's first integration run), so a
ceiling of 0.1 s distinguishes nothing there. A ceiling is derived from the wait the defect it
guards would make, half of it, and that wait is made at least a second long, as
``test_sinks_nats_matrix.FLOOR_PONG_DELAY`` is; half a second is then the smallest ceiling.
"""

CPU_READERS = frozenset(
    {
        "process_time",
        "process_time_ns",
        "thread_time",
        "thread_time_ns",
        "getrusage",
        "pthread_getcpuclockid",
        "CLOCK_PROCESS_CPUTIME_ID",
        "CLOCK_THREAD_CPUTIME_ID",
    }
)
"""Names whose every reference under ``tests/`` is a CPU-time reading, or one in the making."""

CPU_TABLE = ("conftest.py", "CPU_SCOPES")
"""The one place a CPU clock may be named: the table ``conftest.cpu_window`` reads from."""

CEILING_FLOOR = 85
"""Fewest upper bounds on a duration the scan must find across ``tests/``, below today's count.

Re-measure it with ``len(ceilings)`` after ``scan(TESTS, ceilings)``. The same margin as
:data:`POPULATION_FLOOR`, for the same reason: a change that stops the scan recognising a
duration must not pass :data:`CEILING_UNDER_STALL` by leaving it nothing to judge.
"""

POPULATION_FLOOR = 25
"""Fewest floors the scan must find across ``tests/``, below the count it finds today.

Re-measure it with ``len(scan(TESTS)[0])``. The floor sits about a sixth below that count, so
deleting a few timing tests legitimately does not redden the gate, and far enough above zero that
a change which stops the scan recognising a clock (dropping ``monotonic`` from :data:`CLOCKS`, say)
cannot shrink the population silently.
"""

HISTORIC_SITES = {
    ("test_orphan_sink_handoff.py", "test_the_live_orphan_sinks_close_stays_inline_and_unbounded"),
    ("test_numeric_doors.py", "test_flush_with_an_infinite_timeout_waits_for_the_drain"),
    ("test_worker.py", "test_the_waiting_caller_does_not_close_a_second_time"),
    (
        "test_shutdown_lifecycle.py",
        "test_a_second_closer_waits_for_a_close_it_did_not_start_even_while_taking_work",
    ),
    ("test_shutdown_lifecycle.py", "test_a_bystander_does_not_release_the_gate_for_the_next_one"),
}
"""The five sites PR 314 and the orphan handoff fixed by hand, each now on the helper."""


@dataclasses.dataclass(frozen=True)
class Floor:
    """One lower bound on a measured duration.

    Attributes:
      path: The file, relative to ``tests/``.
      line: The comparison's line.
      function: The enclosing function's name.
      clock_line: The line the duration's clock was read on, or None when it cannot be found.
      helper: Whether the duration came from a ``conftest`` stopwatch.
      released: The ``R`` of a ``release_after_clock(seconds, <x>.R.set)`` the duration came from,
        or None.
    """

    path: str
    line: int
    function: str
    clock_line: int | None
    helper: bool
    released: str | None = None


@dataclasses.dataclass(frozen=True)
class Finding:
    """One refusal: where, and the reason constant that names the unsafe shape.

    Attributes:
      path: The file, relative to the scanned root.
      line: The offending line.
      reason: One of the module's four reason constants.
    """

    path: str
    line: int
    reason: str


_NESTED = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)


def _own_nodes(node: ast.AST) -> list[ast.AST]:
    """Returns every node under ``node`` that runs when it does, skipping nested defs and lambdas.

    A nested ``def`` or ``lambda`` body runs when it is called, not where it is written, so a
    ``call_later`` inside a lambda the test passes to its timed call is inside that call.

    Args:
      node: A function definition.

    Returns:
      The nodes, not including ``node`` itself, in no guaranteed order.

    Raises:
      None.
    """
    found: list[ast.AST] = []
    stack = [node]
    while stack:
        for child in ast.iter_child_nodes(stack.pop()):
            if not isinstance(child, _NESTED):
                found.append(child)
                stack.append(child)
    return found


def _callee(call: ast.Call) -> str | None:
    """Returns the last name in a call's callee, ``monotonic`` for ``time.monotonic()``.

    Args:
      call: The call.

    Returns:
      The attribute or name called, or None for any other callee.

    Raises:
      None.
    """
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    if isinstance(call.func, ast.Name):
        return call.func.id
    return None


def _is_clock(node: ast.AST) -> bool:
    """Returns whether ``node`` is a clock reading such as ``time.monotonic()``.

    Args:
      node: Any expression.

    Returns:
      True for a call to one of :data:`CLOCKS` with no arguments.

    Raises:
      None.
    """
    return isinstance(node, ast.Call) and not node.args and _callee(node) in CLOCKS


def _is_sleep(node: ast.AST) -> bool:
    """Returns whether ``node`` is ``time.sleep(...)``, ``asyncio.sleep(...)`` or ``sleep(...)``.

    Args:
      node: Any expression.

    Returns:
      True for a call whose callee is named ``sleep``.

    Raises:
      None.
    """
    return isinstance(node, ast.Call) and _callee(node) == "sleep"


def _arming_lines(own: list[ast.AST]) -> list[int]:
    """Returns the lines among a function's own nodes that arm a delay.

    Arming is ``.call_later(...)``, ``.call_at(...)``, ``.arm()`` (``DelayArmedOnUse``), or
    ``.start()`` on a ``Timer(...)`` call or on a name or attribute this body assigned one to.

    Args:
      own: The function's :func:`_own_nodes`.

    Returns:
      The arming calls' lines.

    Raises:
      None.
    """
    timers = {
        ast.unparse(target)
        for node in own
        if isinstance(node, ast.Assign | ast.AnnAssign)
        and isinstance(node.value, ast.Call)
        and _callee(node.value) == "Timer"
        for target in (node.targets if isinstance(node, ast.Assign) else [node.target])
    }
    lines = []
    for node in own:
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        receiver = node.func.value
        starts_a_timer = node.func.attr == "start" and (
            (isinstance(receiver, ast.Call) and _callee(receiver) == "Timer")
            or ast.unparse(receiver) in timers
        )
        if node.func.attr in ("call_later", "call_at", "arm") or starts_a_timer:
            lines.append(node.lineno)
    return lines


def _returned_positions(own: list[ast.AST]) -> set[int | None]:
    """Returns the tuple positions at which a function's ``return`` gives a duration.

    A returned name counts when the function assigned it a duration, as
    ``test_worker_exit_budget._measured`` does (``elapsed = ...; return elapsed``), so a ceiling
    on such a helper's result is judged like one on an inline subtraction.

    Args:
      own: The function's :func:`_own_nodes`.

    Returns:
      The positions; None means the return value itself is the duration.

    Raises:
      None.
    """
    named: set[str] = set()
    for node in own:
        if isinstance(node, ast.Assign | ast.AnnAssign) and node.value is not None:
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            pairs = [(t, node.value) for t in targets]
            for target, value in pairs:
                if isinstance(target, ast.Tuple) and isinstance(value, ast.Tuple):
                    pairs.extend(zip(target.elts, value.elts, strict=False))
                elif isinstance(target, ast.Name) and _is_duration_expr(value):
                    named.add(target.id)

    def returns_duration(value: ast.AST) -> bool:
        return _is_duration_expr(value) or (isinstance(value, ast.Name) and value.id in named)

    positions: set[int | None] = set()
    for node in own:
        if not isinstance(node, ast.Return) or node.value is None:
            continue
        if isinstance(node.value, ast.Tuple):
            positions.update(i for i, v in enumerate(node.value.elts) if returns_duration(v))
        elif returns_duration(node.value):
            positions.add(None)
    return positions


def _is_duration_expr(node: ast.AST) -> bool:
    """Returns whether ``node`` is ``<clock>() - <anything>``, or that inside a conditional.

    Args:
      node: Any expression.

    Returns:
      True for a subtraction from a clock reading.

    Raises:
      None.
    """
    if isinstance(node, ast.IfExp):
        return _is_duration_expr(node.body) or _is_duration_expr(node.orelse)
    return isinstance(node, ast.BinOp) and isinstance(node.op, ast.Sub) and _is_clock(node.left)


def _last_before(lines: list[int], limit: int) -> int | None:
    """Returns the largest of ``lines`` not after ``limit``.

    Args:
      lines: Candidate lines.
      limit: The line they must not follow.

    Returns:
      The line, or None when every candidate follows ``limit``.

    Raises:
      None.
    """
    earlier = [line for line in lines if line <= limit]
    return max(earlier) if earlier else None


def _floors_in(
    fn: ast.FunctionDef | ast.AsyncFunctionDef,
    own: list[ast.AST],
    path: str,
    returners: dict[str, frozenset[int | None]],
    ceilings: list[Ceiling] | None = None,
) -> list[Floor]:
    """Returns every floor in ``fn``'s own body, each with the line its clock was read on.

    Args:
      fn: The function.
      own: Its :func:`_own_nodes`.
      path: Its file, for the result.
      returners: The duration-returning functions visible to it.
      ceilings: Where to append each upper bound on a duration the body holds, when given.

    Returns:
      The floors, in line order.

    Raises:
      None.
    """
    nodes = sorted(
        (node for node in own if hasattr(node, "lineno")),
        key=lambda node: (node.lineno, node.col_offset),
    )
    looping = {
        id(part) for node in own if isinstance(node, ast.While) for part in ast.walk(node.test)
    }
    clocks: dict[str, list[int]] = {}
    watches: dict[str, list[tuple[int, str | None]]] = {}
    durations: dict[str, list[tuple[int, Measured]]] = {}

    def watch_of(name: str, line: int) -> Measured | None:
        known = [entry for entry in watches.get(name, []) if entry[0] <= line]
        return (known[-1][0], True, known[-1][1]) if known else None

    def duration_of(expr: ast.AST, line: int) -> Measured | None:
        if isinstance(expr, ast.Name) and expr.id in durations:
            known = [entry for entry in durations[expr.id] if entry[0] <= line]
            return known[-1][1] if known else None
        if isinstance(expr, ast.IfExp):
            return duration_of(expr.body, line) or duration_of(expr.orelse, line)
        if _is_duration_expr(expr):
            assert isinstance(expr, ast.BinOp)
            start = expr.right
            if isinstance(start, ast.Name):
                return _last_before(clocks.get(start.id, []), line), False, None
            return None, False, None
        watch = None
        if isinstance(expr, ast.Attribute) and expr.attr == "elapsed":
            watch = expr.value
        elif isinstance(expr, ast.Call) and isinstance(expr.func, ast.Attribute):
            watch = expr.func.value if expr.func.attr == "stop" else None
        if isinstance(watch, ast.Name) and watch.id in watches:
            return watch_of(watch.id, line)
        if isinstance(expr, ast.Call) and None in returners.get(_callee(expr) or "", ()):
            return expr.lineno, False, None
        if isinstance(expr, ast.Subscript) and isinstance(expr.value, ast.Call):
            index = expr.slice
            positions = returners.get(_callee(expr.value) or "", frozenset())
            if isinstance(index, ast.Constant) and index.value in positions:
                return expr.lineno, False, None
        return None

    def bind(target: ast.AST, value: ast.AST, line: int) -> None:
        if isinstance(target, ast.Name):
            if _is_clock(value):
                clocks.setdefault(target.id, []).append(line)
            elif isinstance(value, ast.Call) and _callee(value) in STOPWATCHES:
                watches.setdefault(target.id, []).append((line, None))
            found = duration_of(value, line)
            if found is not None:
                durations.setdefault(target.id, []).append((line, found))
        elif isinstance(target, ast.Tuple) and isinstance(value, ast.Tuple):
            if len(target.elts) == len(value.elts):
                for element, part in zip(target.elts, value.elts, strict=True):
                    bind(element, part, line)
        elif isinstance(target, ast.Tuple) and isinstance(value, ast.Call):
            positions = returners.get(_callee(value) or "", frozenset())
            for index, element in enumerate(target.elts):
                if index in positions and isinstance(element, ast.Name):
                    durations.setdefault(element.id, []).append((line, (line, False, None)))

    floors = []
    for node in nodes:
        if isinstance(node, ast.With | ast.AsyncWith):
            for item in node.items:
                call, name = item.context_expr, item.optional_vars
                is_watch = isinstance(call, ast.Call) and _callee(call) in STOPWATCHES
                if is_watch and isinstance(name, ast.Name):
                    assert isinstance(call, ast.Call)
                    watches.setdefault(name.id, []).append((node.lineno, _released(call)))
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                bind(target, node.value, node.lineno)
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            bind(node.target, node.value, node.lineno)
        elif isinstance(node, ast.Compare):
            sides = [node.left, *node.comparators]
            for index, op in enumerate(node.ops):
                low, high = sides[index], sides[index + 1]
                if isinstance(op, ast.GtE | ast.Gt):
                    measured = duration_of(low, node.lineno)
                    capped = duration_of(high, node.lineno) is not None, low
                elif isinstance(op, ast.LtE | ast.Lt):
                    measured = duration_of(high, node.lineno)
                    capped = duration_of(low, node.lineno) is not None, high
                else:
                    measured, capped = None, (False, low)
                if measured is not None:
                    floors.append(Floor(path, node.lineno, fn.name, *measured))
                if capped[0] and ceilings is not None and id(node) not in looping:
                    ceilings.append(Ceiling(path, node.lineno, fn.name, capped[1]))
    return floors


@dataclasses.dataclass(frozen=True)
class Ceiling:
    """One upper bound on a measured duration, with the expression that bounds it.

    Attributes:
      path: The file, relative to the scanned root.
      line: The comparison's line.
      function: The enclosing function's name.
      bound: The expression on the other side of the comparison.
    """

    path: str
    line: int
    function: str
    bound: ast.expr


Measured = tuple[int | None, bool, str | None]
"""A duration's clock line, whether a ``conftest`` stopwatch read it, and the event it releases."""


def _released(call: ast.Call) -> str | None:
    """Returns ``R`` when a ``release_after_clock`` call's ``release`` is ``<x>.R.set``.

    Args:
      call: The ``release_after_clock(...)`` or ``Stopwatch()`` call.

    Returns:
      The event attribute released, or None for any other ``release`` or none at all.

    Raises:
      None.
    """
    release = call.args[1] if len(call.args) > 1 else None
    for keyword in call.keywords:
        if keyword.arg == "release":
            release = keyword.value
    if (
        isinstance(release, ast.Attribute)
        and release.attr == "set"
        and isinstance(release.value, ast.Attribute)
    ):
        return release.value.attr
    return None


def _class_index(trees: dict[str, ast.Module]) -> dict[str, list[ast.ClassDef]]:
    """Maps every class name under the scanned root to its definitions.

    Args:
      trees: Parsed modules by path.

    Returns:
      ``{name: [definitions]}``; a name defined in several modules keeps every definition.

    Raises:
      None.
    """
    index: dict[str, list[ast.ClassDef]] = {}
    for tree in trees.values():
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                index.setdefault(node.name, []).append(node)
    return index


def _signals_then_sleeps(cls: ast.ClassDef) -> set[str]:
    """Returns the ``self.E`` events a method of ``cls`` sets and later sleeps after.

    Args:
      cls: A class definition.

    Returns:
      The event attribute names.

    Raises:
      None.
    """
    events = set()
    for method in cls.body:
        if not isinstance(method, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        sets: list[tuple[int, str]] = []
        sleeps: list[int] = []
        for node in _own_nodes(method):
            if not isinstance(node, ast.Call):
                continue
            if _is_sleep(node):
                sleeps.append(node.lineno)
            func = node.func
            if (
                isinstance(func, ast.Attribute)
                and func.attr == "set"
                and isinstance(func.value, ast.Attribute)
                and isinstance(func.value.value, ast.Name)
                and func.value.value.id == "self"
            ):
                sets.append((node.lineno, func.value.attr))
        events.update(name for line, name in sets if any(s > line for s in sleeps))
    return events


def _waits_on(cls: ast.ClassDef) -> set[str]:
    """Returns the ``self.E`` events a method of ``cls`` waits on.

    Args:
      cls: A class definition.

    Returns:
      The event attribute names.

    Raises:
      None.
    """
    events = set()
    for method in cls.body:
        if isinstance(method, ast.FunctionDef | ast.AsyncFunctionDef):
            for node in _own_nodes(method):
                func = node.func if isinstance(node, ast.Call) else None
                if (
                    isinstance(func, ast.Attribute)
                    and func.attr == "wait"
                    and isinstance(func.value, ast.Attribute)
                    and isinstance(func.value.value, ast.Name)
                    and func.value.value.id == "self"
                ):
                    events.add(func.value.attr)
    return events


def _inherited(
    index: dict[str, list[ast.ClassDef]], own: Callable[[ast.ClassDef], set[str]]
) -> dict[str, set[str]]:
    """Maps each class name to what ``own`` finds in it and in every base it inherits from.

    Bases are followed by name through the scanned root, so a subclass of a sleeping fake is a
    sleeping fake.

    Args:
      index: Every class definition by name.
      own: What to collect from one class definition.

    Returns:
      ``{class name: events}`` for every class with at least one.

    Raises:
      None.
    """
    memo: dict[str, set[str]] = {}

    def resolve(name: str, seen: frozenset[str]) -> set[str]:
        if name in memo:
            return memo[name]
        events: set[str] = set()
        for cls in index.get(name, []):
            events |= own(cls)
            for base in cls.bases:
                base_name = (
                    base.attr if isinstance(base, ast.Attribute) else getattr(base, "id", "")
                )
                if base_name and base_name not in seen:
                    events |= resolve(base_name, seen | {base_name})
        memo[name] = events
        return events

    return {name: events for name in index if (events := resolve(name, frozenset({name})))}


def _entry_waits(own: list[ast.AST]) -> list[tuple[int, str]]:
    """Returns each ``<x>.E.wait(...)`` among a function's own nodes, as ``(line, E)``.

    Args:
      own: The function's :func:`_own_nodes`.

    Returns:
      The waits.

    Raises:
      None.
    """
    waits = []
    for node in own:
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            receiver = node.func.value
            if node.func.attr == "wait" and isinstance(receiver, ast.Attribute):
                waits.append((node.lineno, receiver.attr))
    return waits


def _cpu_reads(path: str, tree: ast.Module) -> list[int]:
    """Returns the lines of a module that name a CPU clock outside ``conftest.CPU_SCOPES``.

    A reference, not only a call, is refused, so ``clock = time.process_time`` and ``from time
    import process_time`` are caught where they are written, before any call can hide behind the
    alias. ``os.times`` is refused by its receiver, because ``times`` alone names other things.

    Args:
      path: The module's path, relative to the scanned root.
      tree: The parsed module.

    Returns:
      The lines, with repeats.

    Raises:
      None.
    """
    exempt: set[int] = set()
    if path == CPU_TABLE[0]:
        for node in tree.body:
            targets = node.targets if isinstance(node, ast.Assign) else []
            if isinstance(node, ast.AnnAssign):
                targets = [node.target]
            if any(isinstance(t, ast.Name) and t.id == CPU_TABLE[1] for t in targets):
                exempt.update(id(child) for child in ast.walk(node))
    lines = []
    for node in ast.walk(tree):
        if id(node) in exempt:
            continue
        if isinstance(node, ast.Attribute):
            on_os = isinstance(node.value, ast.Name) and node.value.id == "os"
            if node.attr in CPU_READERS or (on_os and node.attr == "times"):
                lines.append(node.lineno)
        elif isinstance(node, ast.Name) and node.id in CPU_READERS:
            lines.append(node.lineno)
        elif isinstance(node, ast.ImportFrom):
            lines.extend(
                node.lineno
                for alias in node.names
                if alias.name in CPU_READERS or (node.module == "os" and alias.name == "times")
            )
    return lines


def _constants(trees: dict[str, ast.Module]) -> dict[str, dict[str, ast.expr | tuple[str, str]]]:
    """Maps each module to its module-level names: an expression, or a scanned module's name.

    A name assigned at module level keeps its last assignment, the value it holds by the time a
    test runs. A name imported by ``from <m> import`` from a module the scan parsed maps to that
    module's path and the name.

    Args:
      trees: Parsed modules by path.

    Returns:
      ``{path: {name: expression or (path, name)}}``.

    Raises:
      None.
    """
    modules = {path.removesuffix(".py").replace("/", "."): path for path in trees}
    table: dict[str, dict[str, ast.expr | tuple[str, str]]] = {}
    for path, tree in trees.items():
        names: dict[str, ast.expr | tuple[str, str]] = {}
        for node in tree.body:
            if isinstance(node, ast.Assign) and len(node.targets) == 1:
                target, value = node.targets[0], node.value
            elif isinstance(node, ast.AnnAssign) and node.value is not None:
                target, value = node.target, node.value
            elif isinstance(node, ast.ImportFrom) and node.module in modules and not node.level:
                for alias in node.names:
                    names[alias.asname or alias.name] = (modules[node.module], alias.name)
                continue
            else:
                continue
            if isinstance(target, ast.Name):
                names[target.id] = value
        table[path] = names
    return table


def _seconds(
    expr: ast.expr, path: str, table: dict[str, dict[str, ast.expr | tuple[str, str]]]
) -> float | None:
    """Returns the number an expression evaluates to from literals and module constants alone.

    Literals, ``+ - * /`` and names resolved through :func:`_constants` are followed; anything
    else (a local, an attribute, a call) is not a constant the scan can judge.

    Args:
      expr: The bound's expression.
      path: The module it is written in.
      table: :func:`_constants`'s result.

    Returns:
      The value, or None when it does not resolve.

    Raises:
      None.
    """

    def value(node: ast.expr, where: str, depth: int) -> float | None:
        if depth > 20:
            return None
        if isinstance(node, ast.Constant) and type(node.value) in (int, float):
            return float(node.value)
        arithmetic = ast.Add | ast.Sub | ast.Mult | ast.Div
        if isinstance(node, ast.BinOp) and isinstance(node.op, arithmetic):
            left, right = value(node.left, where, depth + 1), value(node.right, where, depth + 1)
            if left is None or right is None or (isinstance(node.op, ast.Div) and right == 0):
                return None
            if isinstance(node.op, ast.Add):
                return left + right
            if isinstance(node.op, ast.Sub):
                return left - right
            return left * right if isinstance(node.op, ast.Mult) else left / right
        if isinstance(node, ast.Name):
            bound = table.get(where, {}).get(node.id)
            if isinstance(bound, tuple):
                other, name = bound
                imported = table.get(other, {}).get(name)
                if isinstance(imported, ast.expr):
                    return value(imported, other, depth + 1)
                return None
            if bound is not None:
                return value(bound, where, depth + 1)
        return None

    return value(expr, path, 0)


def _parse(root: pathlib.Path) -> dict[str, ast.Module]:
    """Parses every ``*.py`` under ``root``, this gate included.

    Args:
      root: The directory.

    Returns:
      ``{relative path: module}``.

    Raises:
      SyntaxError: When a file does not parse, which is the suite's failure to report, not ours.
    """
    trees = {}
    for path in sorted(root.rglob("*.py")):
        relative = path.relative_to(root).as_posix()
        trees[relative] = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return trees


def scan(
    root: pathlib.Path, ceilings: list[Ceiling] | None = None
) -> tuple[list[Floor], list[Finding]]:
    """Returns every floor under ``root`` and every refusal of an unsafe timing shape.

    Args:
      root: The directory to scan.
      ceilings: Where to append every upper bound on a duration the scan finds, when given.

    Returns:
      ``(floors, findings)``, findings sorted by path and line.

    Raises:
      None.
    """
    trees = _parse(root)
    index = _class_index(trees)
    sleepers = _inherited(index, _signals_then_sleeps)
    waiters = _inherited(index, _waits_on)
    functions: list[tuple[str, ast.FunctionDef | ast.AsyncFunctionDef, list[ast.AST]]] = []
    returners: dict[str, frozenset[int | None]] = {}
    findings: set[Finding] = set()
    if ceilings is None:
        ceilings = []
    for path, tree in trees.items():
        findings.update(Finding(path, line, RAW_CPU_READ) for line in _cpu_reads(path, tree))
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                own = _own_nodes(node)
                functions.append((path, node, own))
                if positions := _returned_positions(own):
                    returners[node.name] = returners.get(node.name, frozenset()) | positions
            elif isinstance(node, ast.ClassDef):
                for method in node.body:
                    if isinstance(method, ast.FunctionDef) and method.name in CONSTRUCTORS:
                        findings.update(
                            Finding(path, line, ARMED_IN_CONSTRUCTOR)
                            for line in _arming_lines(_own_nodes(method))
                        )
    floors: list[Floor] = []
    for path, fn, own in functions:
        if not any(isinstance(node, ast.Compare) for node in own):
            continue
        found = _floors_in(fn, own, path, returners, ceilings)
        floors.extend(found)
        if not found:
            continue
        arming = _arming_lines(own)
        waits = _entry_waits(own)
        named = {n.id for n in own if isinstance(n, ast.Name)}
        named |= {n.attr for n in own if isinstance(n, ast.Attribute)}
        events = set().union(*(sleepers[name] for name in named if name in sleepers))
        awaited = set().union(*(waiters[name] for name in named if name in waiters))
        for floor in found:
            if floor.clock_line is None:
                findings.add(Finding(path, floor.line, UNRESOLVED_CLOCK))
                continue
            findings.update(
                Finding(path, line, ARMED_BEFORE_CLOCK)
                for line in arming
                if line < floor.clock_line
            )
            if floor.released not in awaited:
                findings.update(
                    Finding(path, line, SLEEP_AFTER_SIGNAL)
                    for line, event in waits
                    if event in events and line < floor.clock_line
                )
    table = _constants(trees)
    for ceiling in ceilings:
        seconds = _seconds(ceiling.bound, ceiling.path, table)
        if seconds is not None and seconds < STALL_ALLOWANCE:
            findings.add(Finding(ceiling.path, ceiling.line, CEILING_UNDER_STALL))
    return floors, sorted(findings, key=lambda f: (f.path, f.line, f.reason))


@functools.cache
def _live() -> tuple[list[Floor], list[Finding]]:
    return scan(TESTS)


def test_no_floor_measures_a_delay_that_opened_before_its_clock() -> None:
    """Every refusal names its file, line and the unsafe shape."""
    _floors, findings = _live()
    assert findings == [], "\n".join(f"{f.path}:{f.line}: {f.reason}" for f in findings)


def test_the_scan_sees_the_population_it_guards() -> None:
    """A scan that stopped recognising a clock would pass the gate by finding nothing to judge."""
    floors, _findings = _live()
    assert len(floors) >= POPULATION_FLOOR, f"only {len(floors)} floors found"
    seen = {(floor.path, floor.function) for floor in floors if floor.helper}
    assert seen >= HISTORIC_SITES, f"not seen as helper floors: {HISTORIC_SITES - seen}"
    assert any(f.path == "test_sinks_nats_liveness.py" and not f.helper for f in floors), (
        "the NATS liveness floors read through `_timed` are no longer recognised"
    )


def test_the_scan_sees_the_ceilings_it_judges() -> None:
    """A scan that stopped recognising a duration would find no ceiling to refuse."""
    ceilings: list[Ceiling] = []
    scan(TESTS, ceilings)
    assert len(ceilings) >= CEILING_FLOOR, f"only {len(ceilings)} ceilings found"
    table = _constants(_parse(TESTS))
    resolved = [c for c in ceilings if _seconds(c.bound, c.path, table) is not None]
    assert len(resolved) >= len(ceilings) // 2, f"only {len(resolved)} ceilings resolve"


# --- The corpus: each shape the gate refuses, with its reason, and each it must leave alone ---

_HEAD = "import asyncio\nimport threading\nimport time\n"

_SLOW_CLOSE = """
class _Slow:
    def __init__(self):
        self.in_close = threading.Event()

    def close(self):
        self.in_close.set()
        time.sleep(1.0)
"""

_GATED_CLOSE = """
class _Gated:
    def __init__(self):
        self.in_close = threading.Event()
        self.release = threading.Event()

    def close(self):
        self.in_close.set()
        self.release.wait(5.0)
"""

REFUSED = {
    "timer-started-before-the-clock": (
        """
def test_it(sink, ev):
    threading.Timer(0.4, ev.set).start()
    start = time.monotonic()
    sink.close()
    elapsed = time.monotonic() - start
    assert elapsed >= 0.4
""",
        ARMED_BEFORE_CLOCK,
        "threading.Timer(0.4, ev.set).start()",
    ),
    "named-timer-started-before-the-clock": (
        """
def test_it(sink, ev):
    timer = threading.Timer(0.4, ev.set)
    timer.start()
    start = time.monotonic()
    sink.close()
    assert time.monotonic() - start >= 0.4
""",
        ARMED_BEFORE_CLOCK,
        "timer.start()",
    ),
    "call-later-before-the-clock": (
        """
def test_it(sink, loop, ev):
    loop.call_later(0.2, ev.set)
    start = time.monotonic()
    sink.close()
    elapsed = time.monotonic() - start
    assert 0.2 <= elapsed
""",
        ARMED_BEFORE_CLOCK,
        "loop.call_later(0.2, ev.set)",
    ),
    "timer-before-a-helper-clock": (
        """
def test_it(sink, ev):
    threading.Timer(0.4, ev.set).start()
    with release_after_clock(0.1, ev.set) as watch:
        sink.close()
    assert watch.elapsed >= 0.4
""",
        ARMED_BEFORE_CLOCK,
        "threading.Timer(0.4, ev.set).start()",
    ),
    "timer-before-a-timed-helper-call": (
        """
def _timed(call):
    began = time.monotonic()
    call()
    return time.monotonic() - began, None


def test_it(sink, ev):
    threading.Timer(0.4, ev.set).start()
    elapsed, error = _timed(sink.close)
    assert elapsed >= 0.4
""",
        ARMED_BEFORE_CLOCK,
        "threading.Timer(0.4, ev.set).start()",
    ),
    "a-timer-between-two-clock-reads": (
        """
def test_it(sink, ev):
    start = time.monotonic()
    threading.Timer(0.4, ev.set).start()
    start = time.monotonic()
    sink.close()
    assert time.monotonic() - start >= 0.4
""",
        ARMED_BEFORE_CLOCK,
        "threading.Timer(0.4, ev.set).start()",
    ),
    "an-annotated-clock": (
        """
def test_it(sink, ev):
    threading.Timer(0.4, ev.set).start()
    start: float = time.monotonic()
    sink.close()
    assert time.monotonic() - start >= 0.4
""",
        ARMED_BEFORE_CLOCK,
        "threading.Timer(0.4, ev.set).start()",
    ),
    "an-annotated-duration": (
        """
def test_it(sink, ev):
    threading.Timer(0.4, ev.set).start()
    start = time.monotonic()
    sink.close()
    elapsed: float = time.monotonic() - start
    assert elapsed >= 0.4
""",
        ARMED_BEFORE_CLOCK,
        "threading.Timer(0.4, ev.set).start()",
    ),
    "a-tuple-assigned-clock": (
        """
def test_it(sink, ev):
    threading.Timer(0.4, ev.set).start()
    start, count = time.monotonic(), 0
    sink.close()
    assert time.monotonic() - start >= 0.4
""",
        ARMED_BEFORE_CLOCK,
        "threading.Timer(0.4, ev.set).start()",
    ),
    "the-helper-releases-an-event-the-fake-only-reads-or-another-object-waits-on": (
        _GATED_CLOSE
        + """
class _Peeks:
    def __init__(self, peer):
        self.peer = peer
        self.in_close = threading.Event()
        self.release = threading.Event()

    def close(self):
        self.in_close.set()
        self.peer.release.wait(0.0)
        if self.release.is_set():
            return
        time.sleep(1.0)


def test_it(closer, peer):
    sink = _Peeks(peer)
    closer(sink)
    assert sink.in_close.wait(5.0)
    with release_after_clock(0.0, sink.release.set) as watch:
        closer(sink)
    assert watch.elapsed >= 0.9
""",
        SLEEP_AFTER_SIGNAL,
        "assert sink.in_close.wait(5.0)",
    ),
    "an-annotated-timer-started-before-the-clock": (
        """
def test_it(sink, ev):
    timer: threading.Timer = threading.Timer(0.4, ev.set)
    timer.start()
    start = time.monotonic()
    sink.close()
    assert time.monotonic() - start >= 0.4
""",
        ARMED_BEFORE_CLOCK,
        "timer.start()",
    ),
    "a-sleeping-fake-named-through-its-module": (
        _SLOW_CLOSE
        + """

def test_it(closer, fakes):
    sink = fakes._Slow()
    closer(sink)
    assert sink.in_close.wait(5.0)
    start = time.monotonic()
    closer(sink)
    assert time.monotonic() - start > 0.3
""",
        SLEEP_AFTER_SIGNAL,
        "assert sink.in_close.wait(5.0)",
    ),
    "a-clock-on-an-attribute": (
        """
def test_it(self, sink):
    self.began = time.monotonic()
    sink.close()
    assert time.monotonic() - self.began >= 0.4
""",
        UNRESOLVED_CLOCK,
        "assert time.monotonic() - self.began >= 0.4",
    ),
    "a-walrus-clock": (
        """
def test_it(sink):
    if (start := time.monotonic()) > 0:
        sink.close()
    assert time.monotonic() - start >= 0.4
""",
        UNRESOLVED_CLOCK,
        "assert time.monotonic() - start >= 0.4",
    ),
    "the-helper-releases-an-event-the-sleeping-fake-never-waits-on": (
        """
class _Slow:
    def __init__(self):
        self.in_close = threading.Event()
        self.release = threading.Event()

    def close(self):
        self.in_close.set()
        time.sleep(1.0)


def test_it(closer):
    sink = _Slow()
    closer(sink)
    assert sink.in_close.wait(5.0)
    with release_after_clock(0.0, sink.release.set) as watch:
        closer(sink)
    assert watch.elapsed >= 0.9
""",
        SLEEP_AFTER_SIGNAL,
        "assert sink.in_close.wait(5.0)",
    ),
    "call-later-in-a-post-init": (
        """
class Fake:
    def __post_init__(self):
        asyncio.get_running_loop().call_later(0.2, self.go)
""",
        ARMED_IN_CONSTRUCTOR,
        "asyncio.get_running_loop().call_later(0.2, self.go)",
    ),
    "timer-started-in-new": (
        """
class Fake:
    def __new__(cls, ev):
        threading.Timer(0.2, ev.set).start()
        return super().__new__(cls)
""",
        ARMED_IN_CONSTRUCTOR,
        "threading.Timer(0.2, ev.set).start()",
    ),
    "call-later-in-a-fake-constructor": (
        """
class SlowFlusher:
    def __init__(self):
        self.parked = True
        asyncio.get_running_loop().call_later(0.2, self.unpark)
""",
        ARMED_IN_CONSTRUCTOR,
        "asyncio.get_running_loop().call_later(0.2, self.unpark)",
    ),
    "call-at-in-a-fake-constructor": (
        """
class Fake:
    def __init__(self, loop):
        loop.call_at(loop.time() + 0.2, self.go)
""",
        ARMED_IN_CONSTRUCTOR,
        "loop.call_at(loop.time() + 0.2, self.go)",
    ),
    "timer-attribute-started-in-a-constructor": (
        """
class Fake:
    def __init__(self, ev):
        self._timer = threading.Timer(0.2, ev.set)
        self._timer.start()
""",
        ARMED_IN_CONSTRUCTOR,
        "self._timer.start()",
    ),
    "delay-armed-in-a-constructor": (
        """
class Fake:
    def __init__(self):
        self.unpark = DelayArmedOnUse(0.2, self.go)
        self.unpark.arm()
""",
        ARMED_IN_CONSTRUCTOR,
        "self.unpark.arm()",
    ),
    "close-sleeps-after-the-signal-the-test-waited-for": (
        _SLOW_CLOSE
        + """

def test_it(closer):
    sink = _Slow()
    closer(sink)
    assert sink.in_close.wait(5.0)
    start = time.monotonic()
    closer(sink)
    waited = time.monotonic() - start
    assert waited > 0.3
""",
        SLEEP_AFTER_SIGNAL,
        "assert sink.in_close.wait(5.0)",
    ),
    "a-conditional-sleep-is-still-a-sleep": (
        """
class _Sink:
    def __init__(self, seconds=0.0):
        self.in_close = threading.Event()
        self.may_finish = threading.Event()
        self.seconds = seconds

    def close(self):
        self.in_close.set()
        if self.seconds:
            time.sleep(self.seconds)
        else:
            self.may_finish.wait(30)


def test_it(retire):
    sink = _Sink(seconds=0.4)
    retire(sink)
    assert sink.in_close.wait(5.0)
    start = time.monotonic()
    retire(sink)
    elapsed = time.monotonic() - start
    assert elapsed >= 0.1
""",
        SLEEP_AFTER_SIGNAL,
        "assert sink.in_close.wait(5.0)",
    ),
    "a-subclass-of-a-sleeping-fake-sleeps": (
        _SLOW_CLOSE
        + """

class _Recording(_Slow):
    pass


def test_it(closer):
    sink = _Recording()
    closer(sink)
    sink.in_close.wait(5.0)
    start = time.monotonic()
    closer(sink)
    assert time.monotonic() - start > 0.3
""",
        SLEEP_AFTER_SIGNAL,
        "sink.in_close.wait(5.0)",
    ),
    "process-time-read-in-a-test": (
        """
def test_it(sink):
    cpu = time.process_time()
    sink.close()
    assert cpu < 0.05
""",
        RAW_CPU_READ,
        "cpu = time.process_time()",
    ),
    "thread-time-ns-read-in-a-helper": (
        """
def _spent(call):
    call()
    return time.thread_time_ns()
""",
        RAW_CPU_READ,
        "return time.thread_time_ns()",
    ),
    "a-cpu-clock-aliased-before-it-is-called": (
        """
CLOCK = time.thread_time
""",
        RAW_CPU_READ,
        "CLOCK = time.thread_time",
    ),
    "a-cpu-clock-imported-by-name": (
        """
from time import process_time_ns as cpu_now
""",
        RAW_CPU_READ,
        "from time import process_time_ns as cpu_now",
    ),
    "a-cpu-clock-named-through-a-star-import": (
        """
from time import *


def test_it(sink):
    cpu = process_time()
""",
        RAW_CPU_READ,
        "cpu = process_time()",
    ),
    "os-times": (
        """
def test_it(sink):
    sink.close()
    assert os.times().user < 0.05
""",
        RAW_CPU_READ,
        "assert os.times().user < 0.05",
    ),
    "os-times-imported-by-name": (
        """
from os import times
""",
        RAW_CPU_READ,
        "from os import times",
    ),
    "getrusage": (
        """
def test_it(sink):
    before = resource.getrusage(resource.RUSAGE_SELF)
""",
        RAW_CPU_READ,
        "before = resource.getrusage(resource.RUSAGE_SELF)",
    ),
    "a-cpu-clock-id-passed-to-clock-gettime": (
        """
def test_it(sink):
    began = time.clock_gettime(time.CLOCK_THREAD_CPUTIME_ID)
""",
        RAW_CPU_READ,
        "began = time.clock_gettime(time.CLOCK_THREAD_CPUTIME_ID)",
    ),
    "a-whole-call-under-a-small-literal": (
        """
def test_it(sink):
    start = time.monotonic()
    sink.close()
    elapsed = time.monotonic() - start
    assert elapsed < 0.1
""",
        CEILING_UNDER_STALL,
        "assert elapsed < 0.1",
    ),
    "a-reversed-ceiling-on-an-inline-duration": (
        """
def test_it(sink):
    start = time.perf_counter()
    sink.close()
    assert 0.2 > time.perf_counter() - start
""",
        CEILING_UNDER_STALL,
        "assert 0.2 > time.perf_counter() - start",
    ),
    "a-ceiling-from-a-module-constant": (
        """
NEXT_BOUND = 0.1


def test_it(sink):
    start = time.monotonic()
    sink.close()
    assert time.monotonic() - start <= NEXT_BOUND
""",
        CEILING_UNDER_STALL,
        "assert time.monotonic() - start <= NEXT_BOUND",
    ),
    "a-ceiling-from-arithmetic-on-constants": (
        """
P = 0.2


def test_it(sink):
    start = time.monotonic()
    sink.close()
    elapsed = time.monotonic() - start
    assert elapsed < P + 0.2
""",
        CEILING_UNDER_STALL,
        "assert elapsed < P + 0.2",
    ),
    "a-ceiling-from-a-product": (
        """
def test_it(sink):
    start = time.monotonic()
    sink.close()
    elapsed = time.monotonic() - start
    assert elapsed < 2 * 0.2
""",
        CEILING_UNDER_STALL,
        "assert elapsed < 2 * 0.2",
    ),
    "a-ceiling-from-a-difference": (
        """
def test_it(sink):
    start = time.monotonic()
    sink.close()
    elapsed = time.monotonic() - start
    assert elapsed < 1.0 - 0.7
""",
        CEILING_UNDER_STALL,
        "assert elapsed < 1.0 - 0.7",
    ),
    "a-ceiling-from-a-quotient": (
        """
HOLD = 0.6


def test_it(sink):
    start = time.monotonic()
    sink.close()
    elapsed = time.monotonic() - start
    assert elapsed < HOLD / 2
""",
        CEILING_UNDER_STALL,
        "assert elapsed < HOLD / 2",
    ),
    "a-ceiling-on-a-helper-that-returns-a-named-duration": (
        """
def _measured(call):
    wall = time.monotonic()
    call()
    elapsed = time.monotonic() - wall
    return elapsed


def test_it(sink):
    assert _measured(sink.close) < 0.1
""",
        CEILING_UNDER_STALL,
        "assert _measured(sink.close) < 0.1",
    ),
    "a-cpu-clock-id-read-for-another-thread": (
        """
def test_it(sink, thread):
    clock = time.pthread_getcpuclockid(thread.ident)
""",
        RAW_CPU_READ,
        "clock = time.pthread_getcpuclockid(thread.ident)",
    ),
    "a-constant-reassigned-down-is-judged-at-its-last-value": (
        """
BOUND = 1.0
BOUND = 0.1


def test_it(sink):
    start = time.monotonic()
    sink.close()
    assert time.monotonic() - start < BOUND
""",
        CEILING_UNDER_STALL,
        "assert time.monotonic() - start < BOUND",
    ),
    "a-small-ceiling-chained-after-a-safe-floor": (
        """
def test_it(sink):
    start = time.monotonic()
    sink.close()
    elapsed = time.monotonic() - start
    assert 0.05 <= elapsed < 0.3
""",
        CEILING_UNDER_STALL,
        "assert 0.05 <= elapsed < 0.3",
    ),
    "a-ceiling-on-a-returned-duration": (
        """
def _timed(call):
    began = time.monotonic()
    call()
    return time.monotonic() - began, None


def test_it(sink):
    elapsed, _error = _timed(sink.close)
    assert elapsed < 0.05
""",
        CEILING_UNDER_STALL,
        "assert elapsed < 0.05",
    ),
    "a-ceiling-on-a-stopwatch": (
        """
def test_it(sink, ev):
    with release_after_clock(1.0, ev.set) as watch:
        sink.close()
    assert watch.elapsed < 0.2
""",
        CEILING_UNDER_STALL,
        "assert watch.elapsed < 0.2",
    ),
}

SILENT = {
    "timer-started-after-the-clock": """
def test_it(sink, ev):
    start = time.monotonic()
    threading.Timer(0.4, ev.set).start()
    sink.close()
    elapsed = time.monotonic() - start
    assert elapsed >= 0.4
""",
    "timer-built-before-the-clock-and-started-after": """
def test_it(sink, ev):
    timer = threading.Timer(0.4, ev.set)
    start = time.monotonic()
    timer.start()
    sink.close()
    assert time.monotonic() - start >= 0.4
""",
    "the-helper-releases-the-event-a-fake-that-could-also-sleep-waits-on": """
class _Slow:
    def __init__(self, seconds):
        self.seconds = seconds
        self.in_close = threading.Event()
        self.release = threading.Event()

    def close(self):
        self.in_close.set()
        if self.seconds is None:
            self.release.wait(5.0)
        else:
            time.sleep(self.seconds)


def test_it(closer):
    sink = _Slow(None)
    closer(sink)
    assert sink.in_close.wait(5.0)
    with release_after_clock(0.4, sink.release.set) as watch:
        closer(sink)
    assert watch.elapsed >= 0.1
""",
    "the-helper-releases-by-keyword": """
class _Slow:
    def __init__(self):
        self.in_close = threading.Event()
        self.release = threading.Event()

    def close(self):
        self.in_close.set()
        if not self.release.wait(5.0):
            time.sleep(1.0)


def test_it(closer):
    sink = _Slow()
    closer(sink)
    assert sink.in_close.wait(5.0)
    with release_after_clock(0.4, release=sink.release.set) as watch:
        closer(sink)
    assert watch.elapsed >= 0.4
""",
    "a-sink-deadline-anchored-after-the-clock": """
class Sink:
    def __init__(self, budget):
        self.budget = budget

    def emit(self, batch):
        deadline = time.monotonic() + self.budget
        while time.monotonic() < deadline:
            pass


def test_it():
    sink = Sink(0.2)
    start = time.monotonic()
    sink.emit([1])
    elapsed = time.monotonic() - start
    assert 0.2 <= elapsed
""",
    "a-delay-armed-at-first-use-not-in-the-constructor": """
class SlowFlusher:
    def __init__(self):
        self.unpark = DelayArmedOnUse(0.2, self.go)

    async def publish(self, payload):
        self.unpark.arm()
        asyncio.get_running_loop().call_later(0.0, self.go)

    def go(self):
        pass
""",
    "a-delay-armed-inside-the-timed-call": """
def _timed(call):
    began = time.monotonic()
    call()
    return time.monotonic() - began, None


def test_it(loop, sink, ev):
    def arm_then_close():
        loop.call_later(0.2, ev.set)
        sink.close()

    elapsed, error = _timed(arm_then_close)
    assert elapsed >= 0.2
""",
    "a-delay-armed-in-a-lambda-the-timed-call-runs": """
def test_it(loop, sink, ev):
    run = lambda: (
        loop.call_later(0.2, ev.set),
        sink.close(),
    )
    start = time.monotonic()
    run()
    assert time.monotonic() - start >= 0.2
""",
    "an-entry-wait-on-a-gated-fake": _GATED_CLOSE
    + """

def test_it(closer):
    sink = _Gated()
    closer(sink)
    assert sink.in_close.wait(5.0)
    start = time.monotonic()
    threading.Timer(1.0, sink.release.set).start()
    closer(sink)
    assert time.monotonic() - start > 0.3
""",
    "a-gated-fake-sharing-a-sleepers-event-name": _SLOW_CLOSE
    + _GATED_CLOSE
    + """

def test_it(closer):
    sink = _Gated()
    closer(sink)
    assert sink.in_close.wait(5.0)
    start = time.monotonic()
    threading.Timer(1.0, sink.release.set).start()
    closer(sink)
    assert time.monotonic() - start > 0.3
""",
    "a-thread-started-before-the-clock": """
def test_it(sink, work):
    threading.Thread(target=work).start()
    start = time.monotonic()
    sink.close()
    assert time.monotonic() - start >= 0.1
""",
    "an-event-cleared-before-the-clock": _SLOW_CLOSE
    + """

def test_it(closer):
    sink = _Slow()
    sink.in_close.clear()
    start = time.monotonic()
    closer(sink)
    assert time.monotonic() - start > 0.3
""",
    "a-fake-that-clears-its-signal-then-sleeps": """
class _Clears:
    def __init__(self):
        self.in_close = threading.Event()

    def close(self):
        self.in_close.clear()
        time.sleep(1.0)


def test_it(closer):
    sink = _Clears()
    closer(sink)
    sink.in_close.wait(5.0)
    start = time.monotonic()
    closer(sink)
    assert time.monotonic() - start > 0.3
""",
    "a-fake-that-sleeps-before-its-signal": """
class _Late:
    def __init__(self):
        self.done = threading.Event()

    def close(self):
        time.sleep(1.0)
        self.done.set()


def test_it(closer):
    sink = _Late()
    closer(sink)
    sink.done.wait(5.0)
    start = time.monotonic()
    closer(sink)
    assert time.monotonic() - start > 0.0
""",
    "an-entry-wait-after-the-clock": _SLOW_CLOSE
    + """

def test_it(closer):
    sink = _Slow()
    start = time.monotonic()
    closer(sink)
    sink.in_close.wait(5.0)
    elapsed = time.monotonic() - start
    assert elapsed > 0.3
""",
    "an-upper-bound-may-follow-an-early-timer": """
def test_it(sink, ev):
    threading.Timer(0.4, ev.set).start()
    start = time.monotonic()
    sink.close()
    elapsed = time.monotonic() - start
    assert elapsed < 1.0
""",
    "cpu-read-through-the-helper": """
def test_it(sink):
    with cpu_window("process") as cpu:
        sink.close()
    assert cpu.spent < 0.05
""",
    "a-ceiling-at-the-allowance": """
def test_it(sink):
    start = time.monotonic()
    sink.close()
    elapsed = time.monotonic() - start
    assert elapsed < 0.5
""",
    "a-ceiling-derived-from-the-wait-it-guards": """
HOLD = 1.5


def test_it(sink):
    start = time.monotonic()
    sink.close()
    elapsed = time.monotonic() - start
    assert elapsed < HOLD / 2
""",
    "a-ceiling-from-a-difference-taken-in-order": """
def test_it(sink):
    start = time.monotonic()
    sink.close()
    elapsed = time.monotonic() - start
    assert elapsed < 2.0 - 0.5
""",
    "a-deadline-loop-is-not-a-ceiling": """
def test_it(sink):
    start = time.monotonic()
    while time.monotonic() - start < 0.3 and not sink.done:
        sink.poll()
    began = time.perf_counter()
    while time.perf_counter() - began < 0.2:
        pass
""",
    "small-bounds-on-things-that-are-not-wall-durations": """
def test_it(sink):
    start = time.monotonic()
    with cpu_window() as cpu:
        sink.close()
    elapsed = time.monotonic() - start
    assert cpu.spent < 0.05
    assert sink.closes < 0.5 * 2
    assert elapsed >= 0.1
""",
    "names-that-only-resemble-a-cpu-clock": """
def test_it(sink, frame):
    process_timeout = 1.0
    sink.thread_timer = None
    frame.times()
    times = [1, 2]
    assert "time.process_time()" not in sink.close(timeout=process_timeout)
""",
}

CONFTEST_CPU = {
    "the-helper-table-is-the-one-exemption": (
        'CPU_SCOPES = {"thread": time.thread_time, "process": time.process_time}\n',
        [],
    ),
    "an-annotated-helper-table-is-exempt-too": (
        "CPU_SCOPES: dict = {'thread': time.thread_time}\n",
        [],
    ),
    "another-name-in-conftest-is-not": (
        "CPU_SCOPES = {}\nOTHER = {'thread': time.thread_time}\n",
        [2],
    ),
    "a-function-in-conftest-is-not": (
        "CPU_SCOPES = {}\ndef cpu():\n    return time.process_time()\n",
        [3],
    ),
}
"""``conftest.py`` sources (one each) and the lines the gate must refuse in them."""


@pytest.mark.parametrize("case", sorted(CONFTEST_CPU))
def test_only_conftests_cpu_table_may_name_a_cpu_clock(case: str, tmp_path) -> None:
    """The exemption is one assignment in one file: a neighbour of it is refused like any site."""
    source, refused = CONFTEST_CPU[case]
    (tmp_path / "conftest.py").write_text("import time\n" + source, encoding="utf-8")
    _floors, findings = scan(tmp_path)
    assert [(f.path, f.line, f.reason) for f in findings] == [
        ("conftest.py", line + 1, RAW_CPU_READ) for line in refused
    ]


@pytest.mark.parametrize(
    ("imported", "alias", "refused"),
    [
        ("BOUND = 0.1\n", "BOUND", True),
        ("BOUND = 2.0\n", "BOUND", False),
        ("SMALL = 0.1\n", "SMALL as BOUND", True),
    ],
    ids=["small", "large", "renamed"],
)
def test_a_ceiling_constant_is_followed_into_the_module_it_is_imported_from(
    imported: str, alias: str, refused: bool, tmp_path
) -> None:
    """``_SLACK`` and its kind are imported from a sibling test module, and judged there."""
    (tmp_path / "bounds_m.py").write_text(imported, encoding="utf-8")
    source = (
        f"from bounds_m import {alias}\n\n\ndef test_it(sink):\n    start = time.monotonic()\n"
        "    sink.close()\n    assert time.monotonic() - start < BOUND\n"
    )
    _floors, findings = _scan_source(tmp_path, source)
    line = (_HEAD + source).splitlines().index("    assert time.monotonic() - start < BOUND") + 1
    expected = [("test_m.py", line, CEILING_UNDER_STALL)] if refused else []
    assert [(f.path, f.line, f.reason) for f in findings] == expected


def test_the_cpu_table_is_exempt_only_in_conftest(tmp_path) -> None:
    """The same table written in a test module is a CPU read, refused on its line."""
    source = 'CPU_SCOPES = {"process": time.process_time}\n'
    _floors, findings = _scan_source(tmp_path, source)
    assert [(f.line, f.reason) for f in findings] == [(_HEAD.count("\n") + 1, RAW_CPU_READ)]


def _scan_source(tmp_path: pathlib.Path, source: str) -> tuple[list[Floor], list[Finding]]:
    (tmp_path / "test_m.py").write_text(_HEAD + source, encoding="utf-8")
    return scan(tmp_path)


@pytest.mark.parametrize("case", sorted(REFUSED))
def test_the_gate_refuses_each_unsafe_shape_for_its_reason(case: str, tmp_path) -> None:
    """Each plant is refused at its line, for its reason, and for nothing else."""
    source, reason, offending = REFUSED[case]
    lines = (_HEAD + source).splitlines()
    expected = [index + 1 for index, line in enumerate(lines) if line.strip() == offending]
    assert len(expected) == 1, f"the plant names one line: {offending!r} matched {expected}"
    _floors, findings = _scan_source(tmp_path, source)
    assert [(f.line, f.reason) for f in findings] == [(expected[0], reason)]


@pytest.mark.parametrize("case", sorted(SILENT))
def test_the_gate_stays_silent_on_each_safe_shape(case: str, tmp_path) -> None:
    """Half a gate's regressions are false positives, so the safe shapes are pinned too."""
    _floors, findings = _scan_source(tmp_path, SILENT[case])
    assert findings == []


POPULATION = {
    "a-named-duration": (
        "start = time.monotonic()\nx()\nd = time.monotonic() - start\nassert d >= 0.1\n",
        [False],
    ),
    "an-inline-duration": (
        "start = time.monotonic()\nassert time.monotonic() - start > 0.1\n",
        [False],
    ),
    "a-reversed-chain": (
        "start = time.perf_counter()\nd = time.perf_counter() - start\nassert 0.1 <= d <= 0.5\n",
        [False],
    ),
    "an-upper-bound-is-not-a-floor": (
        "start = time.monotonic()\nd = time.monotonic() - start\nassert d < 0.5\nassert 0.5 > d\n",
        [],
    ),
    "a-returned-tuple": ("d, err = _timed(x)\nassert d >= 0.1\n", [False]),
    "a-strict-reversed-floor": (
        "start = time.monotonic()\nd = time.monotonic() - start\nassert 0.1 < d\n",
        [False],
    ),
    "a-loop-time-clock": (
        "start = x.time()\nx()\nassert x.time() - start >= 0.1\n",
        [False],
    ),
    "a-clock-call-with-arguments-is-not-a-clock": (
        "start = time.monotonic()\nassert x.time(5) - start > 0.1\n",
        [],
    ),
    "a-sum-is-not-a-duration": ("assert time.monotonic() + 0.1 > x\n", []),
    "a-conditional-duration-in-the-body-branch": (
        "start = time.monotonic()\nd = time.monotonic() - start if x else 0.0\nassert d >= 0.1\n",
        [False],
    ),
    "a-conditional-duration-in-the-else-branch": (
        (
            "start = time.monotonic()\nd = float('inf') if not x else time.monotonic() - start\n"
            "assert d >= 0.1\n"
        ),
        [False],
    ),
    "an-annotated-duration": (
        "start = time.monotonic()\nd: float = time.monotonic() - start\nassert d >= 0.1\n",
        [False],
    ),
    "a-returned-scalar": ("assert _wall(x) >= 0.1\n", [False]),
    "a-conditional-duration": ("assert _until(x) >= 0.1\n", [False]),
    "a-returned-subscript": ("assert _timed(x)[0] >= 0.1\n", [False]),
    "the-helper": (
        "with release_after_clock(0.1, x) as w:\n    x()\nassert w.elapsed >= 0.1\n",
        [True],
    ),
    "a-stopwatch": ("w = Stopwatch()\nx()\nassert w.stop() >= 0.1\n", [True]),
}


@pytest.mark.parametrize("case", sorted(POPULATION))
def test_the_scan_recognises_each_form_of_floor(case: str, tmp_path) -> None:
    """The population is what the gate judges, so each duration form it claims is pinned."""
    body, expected = POPULATION[case]
    timed = (
        "def _timed(call):\n    began = time.monotonic()\n    call()\n"
        "    return time.monotonic() - began, None\n\n\n"
        "def _wall(call):\n    began = time.monotonic()\n    call()\n"
        "    return time.monotonic() - began\n\n\n"
        "def _until(call):\n    began = time.monotonic()\n    done = call()\n"
        "    return time.monotonic() - began if done else float('inf')\n\n\n"
    )
    indented = "".join(f"    {line}\n" for line in body.splitlines())
    floors, _findings = _scan_source(tmp_path, timed + "def test_it(x):\n" + indented)
    got = [floor for floor in floors if floor.function == "test_it"]
    assert [(floor.helper, floor.clock_line is not None) for floor in got] == [
        (helper, True) for helper in expected
    ]


# --- The helpers the gate points to: their promises, pinned on the helpers themselves --------


def test_release_after_clock_releases_once_when_the_timed_call_returns_early() -> None:
    """A floor's bug is an early return: the release then runs at exit, after the stopwatch stops."""
    calls: list[float] = []
    with release_after_clock(5.0, lambda: calls.append(time.monotonic())) as watch:
        pass
    assert watch.ended is not None and len(calls) == 1, calls
    assert watch.ended <= calls[0], "the stopwatch stops before the release runs"
    time.sleep(0.05)
    assert len(calls) == 1, "the cancelled timer did not release a second time"


def test_release_after_clock_releases_once_when_its_timer_fires() -> None:
    """The timer's release and the exit's are one release, and the floor holds from the clock."""
    released = threading.Event()
    calls: list[int] = []

    def release() -> None:
        calls.append(1)
        released.set()

    with release_after_clock(0.1, release) as watch:
        assert released.wait(5.0)
    assert calls == [1], calls
    assert watch.elapsed >= 0.1, f"released {watch.elapsed:.4f}s after the clock"


def _spin_for(cpu_seconds: float) -> None:
    """Burns at least ``cpu_seconds`` of this thread's CPU, measured through the helper."""
    burned = 0.0
    while burned < cpu_seconds:
        with cpu_window() as chunk:
            sum(range(100_000))
        burned += chunk.spent


@pytest.mark.parametrize("scope", ["thread", "process"])
def test_cpu_window_counts_another_threads_spin_only_in_the_process_scope(scope: str) -> None:
    """The thread scope is the caller's alone; the process scope sees a spinner it waits on."""
    spinner = threading.Thread(target=_spin_for, args=(0.2,))
    with cpu_window(scope) as window:
        spinner.start()
        spinner.join(30.0)
    assert not spinner.is_alive()
    if scope == "thread":
        assert window.spent < 0.05, f"the idle caller was charged {window.spent:.3f}s"
    else:
        assert window.spent >= 0.2, f"the spinner's 0.2s was not counted: {window.spent:.3f}s"


def test_cpu_window_pauses_collection_until_the_last_concurrent_window_closes() -> None:
    """Two windows on two threads: the first to close must not resume collection for the other."""
    assert gc.isenabled(), "the premise: collection runs between tests"
    opened = threading.Event()
    close = threading.Event()

    def other() -> None:
        with cpu_window():
            opened.set()
            assert close.wait(10.0)

    thread = threading.Thread(target=other)
    with cpu_window():
        thread.start()
        assert opened.wait(10.0)
    try:
        assert not gc.isenabled(), "the other thread's window is still open"
    finally:
        close.set()
        thread.join(10.0)
    assert gc.isenabled(), "the last window to close resumed collection"


def test_cpu_window_leaves_collection_off_when_it_found_it_off() -> None:
    """A test that turned collection off itself gets it back off."""
    gc.disable()
    try:
        with cpu_window():
            assert not gc.isenabled()
        assert not gc.isenabled()
    finally:
        gc.enable()


def test_cpu_window_refuses_a_reading_taken_inside_the_block() -> None:
    """Read while open, ``spent`` would be a zero every bound passes."""
    with cpu_window() as window, pytest.raises(RuntimeError, match="before the window closed"):
        _ = window.spent
    assert window.spent >= 0.0
