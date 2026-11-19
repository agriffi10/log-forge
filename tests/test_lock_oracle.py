"""SPEC-097 FR-004: every lock a lifecycle call may take is in the ownership oracle or says why not.

A lifecycle call made on a thread that holds one of the library's locks, which is what a signal
handler or a finalizer interrupting the library is, used to wait on its own thread for good. The
locks a lifecycle call takes on its own thread are now ``threading.RLock``s read for ownership
(``_lifecycle._library_locks``), and a call made while its thread owns one is held or refused
rather than waiting. A lock added later that a lifecycle call takes, and that is neither an
``RLock`` in that list nor recorded here with its reason, would bring the hang back with the suite
green, so the population is derived, not listed: every ``threading`` primitive construction in the
lifecycle inventory's files (``tests/test_lifecycle_inventory.py``) and in the sink-factory wrapper
needs a row, and so does every construction of a ``queue.Queue``, a ``LifoQueue``, a
``PriorityQueue`` or a class those files derive from one, whose ``mutex`` is a lock a ``put`` holds.
Two shapes build no lock of their own and need none: a ``Condition`` built on a lock passed to it,
which is that lock, and an ``Event``, for the reason :data:`EVENT_REASON` records (frame-2 review,
m2). A fixture corpus asserts each check's reason, silence included. Serves invariant 1.
"""

from __future__ import annotations

import ast
import pathlib
import threading

import pytest

import log_foundry
from log_foundry import _lifecycle, config, decorator
from test_fork_lifecycle import _primitive_constructions
from test_lifecycle_inventory import FILES, Key, _parents, _qualname, derive

_SRC = pathlib.Path(log_foundry.__file__).resolve().parent
OUR_FILES = (*FILES, "sinks/_factory.py")
"""The inventory's files, and the factory wrapper, whose build lock a worker build may take."""

ORACLE = "oracle"
"""A row naming a lock :func:`_lifecycle._library_locks` returns, read for ownership."""

UNDECLARED = "a lock is built that is neither in the ownership oracle nor recorded with its reason"
NOT_REENTRANT = "an oracle row names a lock that is not an RLock, so ownership cannot be read"
QUEUE_NOT_OWNED = (
    "an oracle row names a queue whose class does not rebuild its mutex as an oracle RLock"
)
STALE = "an oracle row matches no lock construction"

QUEUES = ("Queue", "LifoQueue", "PriorityQueue")
"""The ``queue`` classes that build a ``threading.Lock`` mutex and hold it across a ``put``."""

EVENT_REASON = (
    "an Event's internal lock is held only for the few bytecodes of its own wait(), set() or "
    "clear(), never across a wait, and a lifecycle call takes it only to set or wait on that same "
    "Event; the one site where a handler landing in those bytecodes waited on its own thread, a "
    "flush() inside a flush marker's wait, is held by `_calls_here.flushing` (SPEC-097 FR-004). "
    "Rows for each Event would record that sentence a dozen times and decide nothing, so Events "
    "are exempt as a category; the window that remains is in SPEC-097's report"
)
"""Why an ``Event`` construction needs no row: recorded once, for the category."""


def _c(file: str, qual: str, target: str, ctor: str) -> Key:
    return ("construction", file, qual, target, ctor)


ROWS: dict[Key, str] = {
    _c("_lifecycle.py", "Runtime.__init__", "self._lock", "RLock"): ORACLE,
    _c("_lifecycle.py", "_CloseRegistry.__init__", "self._lock", "RLock"): ORACLE,
    _c("_lifecycle.py", "Runtime.__init__", "self._deferred_lock", "RLock"): ORACLE,
    _c("worker.py", "Worker.__init__", "self._lock", "RLock"): ORACLE,
    _c("worker.py", "_CountingQueue.__init__", "self.mutex", "RLock"): ORACLE,
    _c("config.py", "<module>", "_config_lock", "RLock"): ORACLE,
    _c("decorator.py", "<module>", "_sweep_lock", "RLock"): ORACLE,
    _c("decorator.py", "<module>", "_loss_lock", "RLock"): ORACLE,
    _c("config.py", "<module>", "_sink_lock", "Lock"): (
        "the sink-change lock: a call on its holder's thread is told apart by `_sink_holder`, not "
        "by ownership, and is re-entrant or deferred (SPEC-097 FR-002), never a waiter on it"
    ),
    _c("_lifecycle.py", "Runtime.__init__", "self._owned_lock", "RLock"): (
        "re-entrant by design so a finalizer forwarding a close cannot hang on it (SPEC-081 "
        "FR-003): a lifecycle call inside it re-enters rather than being held"
    ),
    _c("_ledger.py", "_Registry.__init__", "self.lock", "RLock"): (
        "re-entrant by design so a flush() or shutdown() made inside it works (SPEC-089 FR-003 "
        "AC-15, tests/test_flush_loss_counts.py): a lifecycle call inside it re-enters"
    ),
    _c("_diag.py", "<module>", "_FOLD_LOCK", "Lock"): (
        "never waited on unboundedly: a recorder only tries it and stop()'s take waits a bounded "
        "time, written for a handler that calls shutdown() inside the take (SPEC-103)"
    ),
    _c("console.py", "ConsoleWriter.__init__", "self._lock", "Lock"): (
        "no lifecycle call takes it; its section is one counter increment on the echo stream's "
        "failure path, and only an echoing level call made inside that section waits on it"
    ),
    _c("sinks/_factory.py", "_FactorySink.__init__", "self._build_lock", "Lock"): (
        "a factory build's claim: a lifecycle call takes it only through `_lifecycle.yielding`, "
        "for a flush() or shutdown() made inside a running `sink_factory`, for one claim read; "
        "a handler interrupting that read is the one residual (SPEC-097's report)"
    ),
    _c("_fork.py", "<module>", "_RLOCK_TYPE", "RLock"): "a type probe, fed to type()",
    _c("_fork.py", "<module>", "_LOCK_TYPE", "Lock"): "a type probe, fed to type()",
    _c("_fork.py", "_fresh_primitive", "fresh", "RLock"): "the repair's replacement, never kept",
    _c("_fork.py", "_fresh_primitive", "fresh", "Lock"): "the repair's replacement, never kept",
    _c("worker.py", "Worker.__init__", "self._queue", "_CountingQueue"): ORACLE,
    _c("worker.py", "Worker._reinit_after_fork", "self._queue", "_CountingQueue"): ORACLE,
}
"""Every lock construction in :data:`OUR_FILES`: ``oracle``, or the reason it is not read."""


def _sources() -> dict[str, str]:
    return {name: (_SRC / name).read_text(encoding="utf-8") for name in OUR_FILES}


def _key(name: str, parents: dict[int, ast.AST], call: ast.Call, ctor: str) -> Key:
    """A construction's key, as the inventory spells one: its function and assignment target."""
    qual, _ = _qualname(parents, call)
    parent = parents.get(id(call))
    target = "<expr>"
    if isinstance(parent, ast.Assign) and parent.value is call:
        target = ast.unparse(parent.targets[0])
    elif isinstance(parent, ast.AnnAssign) and parent.value is call:
        target = ast.unparse(parent.target)
    return _c(name, qual, target, ctor)


def _conditions_on_a_lock(trees: dict[str, ast.Module]) -> set[Key]:
    """Every ``Condition`` built on a lock passed to it, which builds no lock of its own."""
    found: set[Key] = set()
    for name, tree in trees.items():
        parents = _parents(tree)
        for call in _primitive_constructions(tree, ("Condition",)):
            if call.args or any(keyword.arg == "lock" for keyword in call.keywords):
                found.add(_key(name, parents, call, "Condition"))
    return found


def _queue_constructions(trees: dict[str, ast.Module]) -> tuple[dict[Key, str], dict[str, str]]:
    """Every construction of a ``queue`` class or of a class these files derive from one.

    Args:
      trees: The parsed sources, by file name.

    Returns:
      The constructions, each key's last part the class built, and the file defining each
      derived class, by class name.
    """
    modules: set[str] = set()
    direct: dict[str, str] = {}
    for tree in trees.values():
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                modules.update(a.asname or a.name for a in node.names if a.name == "queue")
            elif isinstance(node, ast.ImportFrom) and node.module == "queue":
                direct.update({a.asname or a.name: a.name for a in node.names if a.name in QUEUES})
    homes: dict[str, str] = {}

    def built(func: ast.expr) -> str | None:
        if isinstance(func, ast.Subscript):
            func = func.value
        if isinstance(func, ast.Name):
            return direct.get(func.id) or (func.id if func.id in homes else None)
        if isinstance(func, ast.Attribute):
            if (
                isinstance(func.value, ast.Name)
                and func.value.id in modules
                and func.attr in QUEUES
            ):
                return func.attr
            return func.attr if func.attr in homes else None
        return None

    classes = [(n, c) for n, t in trees.items() for c in ast.walk(t) if isinstance(c, ast.ClassDef)]
    grew = True
    while grew:
        grew = False
        for name, cls in classes:
            if cls.name not in homes and any(built(base) for base in cls.bases):
                homes[cls.name] = name
                grew = True
    found: dict[Key, str] = {}
    for name, tree in trees.items():
        parents = _parents(tree)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and (cls_name := built(node.func)) is not None:
                found[_key(name, parents, node, cls_name)] = cls_name
    return found, homes


def findings(sources: dict[str, str], rows: dict[Key, str]) -> list[tuple[str, Key]]:
    """Every lock construction checked against the rows, both directions, with its reason.

    Args:
      sources: Source text by file name.
      rows: The roster.

    Returns:
      ``(reason, key)`` per finding.
    """
    _, pops = derive(sources)
    trees = {name: ast.parse(text) for name, text in sources.items()}
    on_a_lock = _conditions_on_a_lock(trees)
    queues, homes = _queue_constructions(trees)
    built = {
        key for key, ctor in pops.constructor.items() if ctor != "Event" and key not in on_a_lock
    }
    built |= set(queues)
    found: list[tuple[str, Key]] = [(UNDECLARED, key) for key in sorted(built) if key not in rows]
    found += [
        (NOT_REENTRANT, key)
        for key, row in sorted(rows.items())
        if row == ORACLE and key not in queues and key[-1] != "RLock"
    ]
    found += [
        (QUEUE_NOT_OWNED, key)
        for key, row in sorted(rows.items())
        if row == ORACLE
        and key in queues
        and rows.get(_c(homes.get(key[-1], ""), f"{key[-1]}.__init__", "self.mutex", "RLock"))
        != ORACLE
    ]
    found += [(STALE, key) for key in sorted(rows) if key not in built and key[1] in sources]
    return found


def test_every_lock_is_in_the_oracle_or_recorded_with_its_reason() -> None:
    """FR-004: the live sources, against :data:`ROWS`."""
    assert findings(_sources(), ROWS) == []


_RUNTIME = """
import threading


class Runtime:
    def __init__(self):
        self._lock = threading.RLock()
"""

_OWN_QUEUE = (
    "import queue\nimport threading\n\n\n"
    "class _Q(queue.Queue[object]):\n"
    "    def __init__(self):\n"
    "        super().__init__()\n"
    "        self.mutex = threading.RLock()\n" + _RUNTIME.removeprefix("\nimport threading\n")
)
_MUTEX = _c("x.py", "_Q.__init__", "self.mutex", "RLock")
_ROOT = {_c("x.py", "Runtime.__init__", "self._lock", "RLock"): ORACLE}


@pytest.mark.parametrize(
    ("source", "rows", "expected"),
    [
        (
            _RUNTIME + "        self._extra = threading.Lock()\n",
            {_c("x.py", "Runtime.__init__", "self._lock", "RLock"): ORACLE},
            [(UNDECLARED, _c("x.py", "Runtime.__init__", "self._extra", "Lock"))],
        ),
        (
            _RUNTIME + "        self._extra = threading.Lock()\n",
            {
                _c("x.py", "Runtime.__init__", "self._lock", "RLock"): ORACLE,
                _c("x.py", "Runtime.__init__", "self._extra", "Lock"): "a reason",
            },
            [],
        ),
        (
            _RUNTIME.replace("RLock()", "Lock()"),
            {_c("x.py", "Runtime.__init__", "self._lock", "Lock"): ORACLE},
            [(NOT_REENTRANT, _c("x.py", "Runtime.__init__", "self._lock", "Lock"))],
        ),
        (
            _RUNTIME,
            {
                _c("x.py", "Runtime.__init__", "self._lock", "RLock"): ORACLE,
                _c("x.py", "Runtime.__init__", "self._gone", "RLock"): ORACLE,
            },
            [(STALE, _c("x.py", "Runtime.__init__", "self._gone", "RLock"))],
        ),
        (_RUNTIME, {_c("x.py", "Runtime.__init__", "self._lock", "RLock"): ORACLE}, []),
        (
            _RUNTIME + "        self._cond = threading.Condition()\n",
            _ROOT,
            [(UNDECLARED, _c("x.py", "Runtime.__init__", "self._cond", "Condition"))],
        ),
        (
            _RUNTIME
            + "        self._cond = threading.Condition(self._lock)\n"
            + "        self._other = threading.Condition(lock=self._lock)\n",
            _ROOT,
            [],
        ),
        (_RUNTIME + "        self._done = threading.Event()\n", _ROOT, []),
        (
            _RUNTIME + "        self._slots = threading.Semaphore(2)\n",
            _ROOT,
            [(UNDECLARED, _c("x.py", "Runtime.__init__", "self._slots", "Semaphore"))],
        ),
        (
            "import queue\n" + _RUNTIME + "        self._q = queue.Queue()\n",
            _ROOT,
            [(UNDECLARED, _c("x.py", "Runtime.__init__", "self._q", "Queue"))],
        ),
        (
            "from queue import LifoQueue as Q\n" + _RUNTIME + "        self._q = Q()\n",
            _ROOT,
            [(UNDECLARED, _c("x.py", "Runtime.__init__", "self._q", "LifoQueue"))],
        ),
        (
            "import queue\n" + _RUNTIME + "        self._q = queue.Queue()\n",
            {**_ROOT, _c("x.py", "Runtime.__init__", "self._q", "Queue"): ORACLE},
            [(QUEUE_NOT_OWNED, _c("x.py", "Runtime.__init__", "self._q", "Queue"))],
        ),
        (
            _OWN_QUEUE + "        self._q = _Q()\n",
            {**_ROOT, _MUTEX: ORACLE},
            [(UNDECLARED, _c("x.py", "Runtime.__init__", "self._q", "_Q"))],
        ),
        (
            _OWN_QUEUE + "        self._q = _Q()\n",
            {**_ROOT, _MUTEX: ORACLE, _c("x.py", "Runtime.__init__", "self._q", "_Q"): ORACLE},
            [],
        ),
        (
            _OWN_QUEUE + "        self._q = _Q()\n",
            {**_ROOT, _MUTEX: "a reason", _c("x.py", "Runtime.__init__", "self._q", "_Q"): ORACLE},
            [(QUEUE_NOT_OWNED, _c("x.py", "Runtime.__init__", "self._q", "_Q"))],
        ),
    ],
    ids=[
        "a-plain-lock-added",
        "recorded-is-silent",
        "a-plain-oracle-lock",
        "stale",
        "clean",
        "a-bare-condition",
        "a-condition-on-a-passed-lock-is-silent",
        "an-event-is-silent",
        "a-semaphore",
        "a-plain-queue",
        "an-aliased-lifo-queue",
        "a-plain-queue-in-the-oracle",
        "a-derived-queue-added",
        "a-derived-queue-with-an-oracle-mutex-is-silent",
        "a-derived-queue-whose-mutex-is-not-oracle",
    ],
)
def test_the_roster_check_fails_for_its_reason(
    source: str, rows: dict[Key, str], expected: list[tuple[str, Key]]
) -> None:
    """FR-004: each check's reason, and silence where nothing is wrong."""
    assert findings({"x.py": source}, rows) == expected


def test_the_oracle_returns_exactly_the_locks_the_rows_name() -> None:
    """FR-004: the live oracle, with a worker built, is the eight locks the oracle rows name."""
    log_foundry.configure(service="oracle")

    @log_foundry.trace
    def work() -> None:
        return None

    work()
    worker = _lifecycle._state.worker_exists()
    assert worker is not None
    expected = [
        _lifecycle._state._lock,
        _lifecycle._state._closes._lock,
        _lifecycle._state._deferred_lock,
        config._config_lock,
        decorator._sweep_lock,
        decorator._loss_lock,
        worker._lock,
        worker._queue.mutex,
    ]
    live = _lifecycle._library_locks()
    oracle_locks = sum(1 for key, row in ROWS.items() if row == ORACLE and key[-1] == "RLock")
    assert len(live) == len(expected) == oracle_locks
    assert type(worker._queue).__name__ == "_CountingQueue", "the class the queue rows name"
    assert all(any(lock is want for lock in live) for want in expected)
    rlock = type(threading.RLock())
    assert all(type(lock) is rlock for lock in live), [type(lock) for lock in live]
    assert not _lifecycle.in_section(), "this thread holds no library lock here"
    for lock in live:
        with lock:  # type: ignore[attr-defined]
            assert _lifecycle.in_section(), f"{lock} owned, and the oracle did not see it"
