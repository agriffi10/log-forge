"""SPEC-039 — what a forked child inherits, and what the library repairs in it.

Every test here forks for real. The two hazards are only observable across a genuine
``os.fork``: an inherited ``Lock`` is locked with *no owner*, which no in-process double
reproduces, and the child's repair runs from ``os.register_at_fork``, which nothing else fires.

**The window is constructed, never hoped for.** A fork landing after ``emit`` has returned
exercises the non-hazard — the lock is free and the buffer empty by construction — and an
earlier draft of this spec drew a false conclusion from exactly that. So the gate below parks the
drain thread *inside* a locked ``emit`` and the fork happens while it is there.

**Children are self-limiting.** Each one arms ``signal.alarm`` before doing anything, so a child
that hangs — which is the whole subject of FR-003 — dies on its own rather than outliving the
test run. A trailing ``kill`` in the parent is not enough: the parent is what blocks first, on
the pipe the hung child still holds open.
"""

from __future__ import annotations

import ast
import os
import pathlib
import signal
import threading
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry
from log_foundry import _fork, decorator
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
        """Whether the child's own watchdog killed it, which is what a deadlock looks like here.

        Args:
          None.

        Returns:
          Whether ``SIGALRM`` terminated it.

        Raises:
          None.
        """
        return os.WIFSIGNALED(self.status) and os.WTERMSIG(self.status) == signal.SIGALRM


def run_in_child(work: Callable[[], str | None], *, timeout: int = CHILD_TIMEOUT) -> _Child:
    """Forks, runs ``work`` in the child, and reaps it.

    The child writes ``work``'s return value down a pipe and leaves through ``os._exit``, so it
    never runs the parent's ``atexit`` handlers or pytest's teardown. It arms ``signal.alarm``
    first, so the pipe reaches EOF and this call returns even when the point of the test is that
    the child blocks forever.

    Args:
      work: Called in the child, after the library's fork handler has run. Whatever it returns
        is sent back as the child's output.
      timeout: Seconds before the child's watchdog kills it.

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
    chunks: list[bytes] = []
    try:
        while True:
            chunk = os.read(read_fd, 65536)
            if not chunk:
                break
            chunks.append(chunk)
    finally:
        os.close(read_fd)
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


def _gated_file_sink(tmp_path: pathlib.Path) -> tuple[FileSink, _GatingStream]:
    """Builds a ``FileSink`` whose stream can be parked, and makes it the process sink.

    ``FileSink`` is used rather than a test double on purpose: the traversal descends only into
    instances this package defines (FR-003 AC-2), so a sink written here would not be repaired
    and every assertion below would be measuring the wrong object.

    Args:
      tmp_path: The directory to write into.

    Returns:
      The sink and the stream wrapper that arms its window.

    Raises:
      None.
    """
    sink = FileSink(str(tmp_path / "events.ndjson"))
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


# -- FR-003 AC-3: completeness is proved, not asserted ---------------------------------------


_PRIMITIVES = ("Lock", "RLock", "Event")


def _threading_names(tree: ast.AST) -> tuple[set[str], set[str]]:
    """The names this module can build a primitive through, derived from its own imports.

    A detector hardcoding the literal ``threading`` reads zero constructions from
    ``from threading import Lock`` and from ``import threading as th`` — measured, both green
    with the lock in a list the walk cannot reach. Reading the imports is what makes the rule
    about the *primitive* rather than about one spelling of it.

    Args:
      tree: The parsed module.

    Returns:
      The names bound to the ``threading`` module, and the names bound directly to a primitive.

    Raises:
      None.
    """
    modules: set[str] = set()
    direct: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(
                alias.asname or alias.name for alias in node.names if alias.name == "threading"
            )
        elif isinstance(node, ast.ImportFrom) and node.module == "threading":
            direct.update(
                alias.asname or alias.name for alias in node.names if alias.name in _PRIMITIVES
            )
    return modules, direct


def _primitive_constructions(tree: ast.AST) -> list[ast.Call]:
    """Every ``Lock`` / ``RLock`` / ``Event`` construction in a parsed module, however spelled.

    Args:
      tree: The parsed module.

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
            and func.attr in _PRIMITIVES
            and isinstance(func.value, ast.Name)
            and func.value.id in modules
        )
        if (isinstance(func, ast.Name) and func.id in direct) or through_module:
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
    counter lock after this one ships.

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


def test_the_walk_terminates_on_a_cycle() -> None:
    """A container holding itself, and two objects holding each other, must not hang the child.

    The walk runs before the forking application gets control back, so a cycle is not a slow
    repair — it is a process that never returns from ``fork``.
    """
    loop: list[Any] = []
    loop.append(loop)
    assert _fork._container_children(loop) == [loop]

    seen: set[int] = set()
    stack: list[Any] = [loop]
    while stack:
        node = stack.pop()
        if id(node) in seen:
            continue
        seen.add(id(node))
        stack.extend(_fork._container_children(node))
    assert seen == {id(loop)}


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
        return
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
    """
    allowed = {f"{_PACKAGE}._diag", f"{_PACKAGE}.sinks.base"}
    for node in ast.walk(_fork_tree()):
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert not alias.name.startswith(_PACKAGE), alias.name
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0, "a relative import is an intra-package import"
            module = node.module or ""
            if not module.startswith(_PACKAGE):
                continue
            imported = {
                f"{module}.{alias.name}" if module == _PACKAGE else module for alias in node.names
            }
            assert imported <= allowed, f"_fork.py may not import {sorted(imported - allowed)}"


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
