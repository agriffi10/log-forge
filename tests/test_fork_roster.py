"""SPEC-079 FR-001: the child re-creates the package's global primitives from one roster.

A lock held by the package rather than by any sink is listed in ``_fork._GLOBAL_PRIMITIVES``
instead of being found by a walk over every package module. A list is only as good as what holds
it complete, so this file holds it from both sides, and each side catches what the other cannot:

- **The live half** imports every package module that imports without an extra and steps from
  each module global through package-owned objects' attributes and containers' members, stopping
  at sinks, a ``Worker``, classes and modules, which are the walk's to reach. A primitive found
  as an attribute on that path must be a roster value; one found inside a container fails, since
  neither the roster nor the walk reaches it, unless the container is declared emptied in the
  child by a named handler.
- **The static half** is an AST scan of ``src/``: every class that assigns a ``Lock``, ``RLock`` or
  ``Event`` to ``self`` is declared in :data:`_PRIMITIVE_HOMES` with one home, which catches a new
  lock-holding class while no instance of it exists.

Both halves are gates, so each has a fixture corpus asserting its failure reason, silence
included. Serves invariants 1 and 12.
"""

from __future__ import annotations

import ast
import importlib
import pathlib
import pkgutil
import threading
import types
from typing import Any

import pytest

import log_foundry
from log_foundry import _fork, _lifecycle
from log_foundry.sinks.file import FileSink
from log_foundry.sinks.memory import MemorySink
from log_foundry.worker import Worker
from test_fork_lifecycle import (
    _REPAIRABLE,
    _held_by_a_parent_thread,
    _primitive_constructions,
    report_from_child,
    run_in_child,
)

_SRC = pathlib.Path(log_foundry.__file__).resolve().parent

_PRIMITIVE_TYPES = (type(threading.Lock()), type(threading.RLock()), threading.Event)

UNROSTERED = "a package global primitive is not on the roster"
STALE_PAIR = "a roster pair resolves to nothing"
CONTAINED = "a primitive inside a container is reached by neither the roster nor the walk"
UNDECLARED_CLASS = "a class assigns a primitive to self and has no _PRIMITIVE_HOMES entry"
STALE_HOME = "a _PRIMITIVE_HOMES entry names no class that assigns a primitive to self"

_EXPECTED_ROSTER = (
    ("_lifecycle", "_state._lock"),
    ("_lifecycle", "_state._stop"),
    ("_lifecycle", "_state._owned_lock"),
    ("_lifecycle", "_state._closes._lock"),
    ("config", "_config_lock"),
    ("config", "_sink_lock"),
    ("decorator", "_sweep_lock"),
    ("decorator", "_loss_lock"),
    ("api", "_console._lock"),
)

_EMPTIED_IN_CHILD = {"_lifecycle._state._closes._in_flight": "_clear_after_fork"}
"""Containers holding primitives that a child handler empties, so nothing needs to reach them."""

_PRIMITIVE_HOMES = {
    "Runtime": "roster",
    "_CloseRegistry": "roster",
    "ConsoleWriter": "roster",
    "ClosedGuard": "walk",
    "FileSink": "walk",
    "RotatingFileSink": "walk",
    "FilteringSink": "walk",
    "HTTPSink": "walk",
    "KafkaSink": "walk",
    "MultiSink": "walk",
    "NATSSink": "walk",
    "NullSink": "walk",
    "GooglePubSubSink": "walk",
    "SentrySink": "walk",
    "SQLiteSink": "walk",
    "StdoutSink": "walk",
    "TransformSink": "walk",
    "SinkCounters": "walk",
    "Worker": "walk",
    "_FactorySink": "walk",
    "_Close": "dropped: _lifecycle._clear_after_fork",
    "_Operation": "dropped: _lifecycle._clear_after_fork",
    "_FlushMarker": "dropped: _lifecycle._rebuild_worker_after_fork",
}
"""Every class that assigns a primitive to ``self``, with the one mechanism that handles it.

``roster``: its instance is on a roster path. ``walk``: it is sink-shaped, a ``Worker``, or held
by an attribute of one (``SinkCounters``, SPEC-076's guard), so the walk from the runtime's roots
reaches it. ``dropped``: a child handler drops its instances.
"""


# -- the live half --------------------------------------------------------------------------------


def _package_modules() -> dict[str, types.ModuleType]:
    """Every package module that imports without an optional extra, keyed by its short name."""
    found: dict[str, types.ModuleType] = {}
    for info in pkgutil.walk_packages(log_foundry.__path__, f"{log_foundry.__name__}."):
        try:
            module = importlib.import_module(info.name)
        except ImportError:
            continue
        found[info.name.removeprefix(f"{log_foundry.__name__}.")] = module
    return found


def _stops(value: object) -> bool:
    """Whether the live half stops at a value: a sink, a worker, a class or a module."""
    return (
        _fork._sink_shaped(value)
        or isinstance(value, Worker)
        or _fork._is_class(value)
        or _fork._is_module(value)
    )


def _resolve(modules: dict[str, types.ModuleType], module: str, path: str) -> object:
    """A roster pair's value in ``modules``, or ``None`` when any hop is missing."""
    holder: object = modules.get(module)
    if holder is None:
        return None
    for hop in path.split("."):
        holder = getattr(holder, hop, None)
        if holder is None:
            return None
    return holder


def live_findings(
    modules: dict[str, types.ModuleType],
    roster: tuple[tuple[str, str], ...],
    emptied: dict[str, str],
) -> list[tuple[str, str]]:
    """The live half's refusals over ``modules`` against ``roster``, each ``(reason, site)``."""
    attribute: dict[int, str] = {}
    findings: list[tuple[str, str]] = []
    emptied_ids = {id(_resolve(modules, *site.split(".", 1))) for site in emptied}
    seen: set[int] = set()
    stack: list[tuple[str, object, bool]] = []
    for short, module in modules.items():
        for name, value in list(vars(module).items()):
            stack.append((f"{short}.{name}", value, False))
    while stack:
        site, value, contained = stack.pop()
        if isinstance(value, _PRIMITIVE_TYPES):
            if contained:
                findings.append((CONTAINED, site))
            else:
                attribute.setdefault(id(value), site)
            continue
        if id(value) in seen or _stops(value):
            continue
        seen.add(id(value))
        if id(value) in emptied_ids:
            continue
        inside = contained
        if _fork._is_container(value):
            for index, member in enumerate(_lifecycle._bounded_children(value)):
                stack.append((f"{site}[{index}]", member, True))
        if _fork._is_owned(value):
            for name, member in _fork._namespace_items(value):
                stack.append((f"{site}.{name}", member, inside))
    resolved: dict[int, str] = {}
    for module, path in roster:
        found = _resolve(modules, module, path)
        if not isinstance(found, _PRIMITIVE_TYPES):
            findings.append((STALE_PAIR, f"{module}.{path}"))
        else:
            resolved[id(found)] = f"{module}.{path}"
    findings.extend(
        (UNROSTERED, site) for key, site in sorted(attribute.items()) if key not in resolved
    )
    return findings


def test_the_roster_is_the_nine_pairs() -> None:
    """FR-001: exactly the nine pairs, in the Data Model's order.

    Eight at SPEC-079; ``config._sink_lock`` joined beside ``_config_lock`` when concurrent
    ``configure(sink=...)`` calls were serialized.
    """
    assert _fork._GLOBAL_PRIMITIVES == _EXPECTED_ROSTER


def test_the_live_tree_matches_the_roster() -> None:
    """FR-001: every primitive a module global reaches is a roster value, and every pair resolves."""
    findings = live_findings(_package_modules(), _fork._GLOBAL_PRIMITIVES, _EMPTIED_IN_CHILD)
    assert findings == [], findings


def test_the_emptied_container_is_empty_in_a_child() -> None:
    """The one declared exception: a close put in flight in the parent leaves nothing in the child."""
    sink = MemorySink()
    close = _lifecycle._state._closes.register(sink, _lifecycle._Rule.GRACED)
    try:
        assert _lifecycle._state._closes._in_flight, "the precondition: a close is in flight"
        child = run_in_child(lambda: str(len(_lifecycle._state._closes._in_flight)))
    finally:
        _lifecycle._state._closes.finish(sink, close)
    assert child.output == "0", child.output


# -- the static half -----------------------------------------------------------------------------


def static_findings(sources: dict[str, str], homes: dict[str, str]) -> list[tuple[str, str]]:
    """The static half's refusals over ``sources`` against ``homes``, each ``(reason, site)``."""
    holders: set[str] = set()
    for source in sources.values():
        tree = ast.parse(source)
        constructions = {id(call) for call in _primitive_constructions(tree, _REPAIRABLE)}
        for cls in [node for node in ast.walk(tree) if isinstance(node, ast.ClassDef)]:
            for node in ast.walk(cls):
                if (
                    isinstance(node, ast.Assign | ast.AnnAssign)
                    and node.value is not None
                    and id(node.value) in constructions
                ):
                    targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                    if any(
                        isinstance(t, ast.Attribute) and ast.unparse(t.value) == "self"
                        for t in targets
                    ):
                        holders.add(cls.name)
    findings = [(UNDECLARED_CLASS, name) for name in sorted(holders - set(homes))]
    findings.extend((STALE_HOME, name) for name in sorted(set(homes) - holders))
    return findings


def _src_sources() -> dict[str, str]:
    return {
        str(path.relative_to(_SRC)): path.read_text(encoding="utf-8")
        for path in sorted(_SRC.rglob("*.py"))
    }


def test_every_class_holding_a_primitive_has_a_home() -> None:
    """FR-001: the AST scan of ``src/`` against ``_PRIMITIVE_HOMES``, both directions."""
    assert static_findings(_src_sources(), _PRIMITIVE_HOMES) == []


def test_every_home_is_one_mechanism() -> None:
    """Each home is ``roster``, ``walk`` or ``dropped: <handler>`` naming a registered handler."""
    handlers = {f"_lifecycle.{handler.__name__}" for handler in _fork._child_handlers}
    for name, home in _PRIMITIVE_HOMES.items():
        if home.startswith("dropped: "):
            assert home.removeprefix("dropped: ") in handlers, (name, home)
        else:
            assert home in {"roster", "walk"}, (name, home)


# -- the gate's own corpus ------------------------------------------------------------------------


class _P:
    """The fixture class: a package-looking object holding a lock on ``self``."""

    def __init__(self) -> None:
        self._lock = threading.Lock()


_P.__module__ = f"{log_foundry.__name__}._planted"


def _planted(**globals_: object) -> dict[str, types.ModuleType]:
    """The live modules plus one planted package module carrying ``globals_``."""
    module = types.ModuleType(f"{log_foundry.__name__}._planted")
    for name, value in globals_.items():
        setattr(module, name, value)
    return {**_package_modules(), "_planted": module}


_P_SOURCE = (
    "import threading\nclass _P:\n    def __init__(self):\n        self._lock = threading.Lock()\n"
)


@pytest.mark.parametrize(
    ("case", "build", "expected"),
    [
        (
            "an-extra-module-global-lock",
            lambda: live_findings(
                _planted(_extra_lock=threading.Lock()), _EXPECTED_ROSTER, _EMPTIED_IN_CHILD
            ),
            [(UNROSTERED, "_planted._extra_lock")],
        ),
        (
            "a-deleted-roster-entry",
            lambda: live_findings(
                _package_modules(),
                tuple(pair for pair in _EXPECTED_ROSTER if pair != ("decorator", "_loss_lock")),
                _EMPTIED_IN_CHILD,
            ),
            [(UNROSTERED, "decorator._loss_lock")],
        ),
        (
            "a-pending-dict-of-lock-holders",
            lambda: live_findings(
                _planted(_pending={1: _P()}), _EXPECTED_ROSTER, _EMPTIED_IN_CHILD
            ),
            [(CONTAINED, "_planted._pending[1]._lock")],
        ),
        (
            "a-stale-roster-entry",
            lambda: live_findings(
                _package_modules(),
                (*_EXPECTED_ROSTER, ("_lifecycle", "_state._gone")),
                _EMPTIED_IN_CHILD,
            ),
            [(STALE_PAIR, "_lifecycle._state._gone")],
        ),
        (
            "a-class-with-no-home",
            lambda: static_findings({"x.py": _P_SOURCE}, {}),
            [(UNDECLARED_CLASS, "_P")],
        ),
        (
            "a-stale-home",
            lambda: static_findings({"x.py": _P_SOURCE}, {"_P": "walk", "_Gone": "walk"}),
            [(STALE_HOME, "_Gone")],
        ),
    ],
    ids=lambda value: value if isinstance(value, str) else "",
)
def test_the_roster_gate_refuses_its_shapes(case: str, build: Any, expected: Any) -> None:
    """Each fixture fails naming its site and its reason; the reason, not a count, is asserted."""
    assert build() == expected


def test_the_roster_gate_is_silent_on_a_walked_lock(tmp_path: pathlib.Path) -> None:
    """A lock under a ``Worker`` or a sink is the walk's, and the live half says nothing of it."""
    worker = Worker(MemorySink())
    try:
        sink = FileSink(str(tmp_path / "walked.ndjson"))
        findings = live_findings(
            _planted(_worker=worker, _sink=sink, _sinks=[sink]),
            _EXPECTED_ROSTER,
            _EMPTIED_IN_CHILD,
        )
        sink.close()
    finally:
        worker.stop(timeout=2.0)
    assert findings == [], findings
    assert static_findings({"x.py": _P_SOURCE}, {"_P": "walk"}) == []


def test_an_undeclared_emptied_container_needs_a_close_in_flight() -> None:
    """The emptied-container case above is only a refusal while something is in flight."""
    sink = MemorySink()
    close = _lifecycle._state._closes.register(sink, _lifecycle._Rule.GRACED)
    try:
        findings = live_findings(_package_modules(), _EXPECTED_ROSTER, {})
    finally:
        _lifecycle._state._closes.finish(sink, close)
    assert findings, "with no declaration, the in-flight entry's event is refused"
    assert {reason for reason, _site in findings} == {CONTAINED}, findings
    assert all("._closes._in_flight[" in site for _r, site in findings), findings


# -- resolution in a child ------------------------------------------------------------------------


def test_each_roster_value_is_replaced_in_the_child() -> None:
    """FR-001: each resolved value is a different object of the same type, an Event's state carried."""
    modules = _package_modules()
    _lifecycle._state._stop.set()
    before = {f"{m}.{p}": _resolve(modules, m, p) for m, p in _EXPECTED_ROSTER}

    def read() -> str:
        rows = []
        for module, path in _EXPECTED_ROSTER:
            now = _resolve(modules, module, path)
            old = before[f"{module}.{path}"]
            rows.append(
                f"{module}.{path}:{now is not old}:{type(now) is type(old)}:"
                f"{now.is_set() if isinstance(now, threading.Event) else '-'}"
            )
        return ";".join(rows)

    child = run_in_child(read)
    for row in child.output.split(";"):
        site, replaced, same_type, state = row.split(":")
        assert replaced == "True" and same_type == "True", row
        if site == "_lifecycle._state._stop":
            assert state == "True", "an Event's set state is carried across"


def test_a_missing_roster_attribute_is_announced_and_the_rest_replaced(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-001: a pair naming a missing attribute is announced by type, every real pair replaced."""
    import io
    import sys

    monkeypatch.setattr(
        _fork, "_GLOBAL_PRIMITIVES", (("decorator", "_gone_lock"), *_EXPECTED_ROSTER)
    )
    modules = _package_modules()
    before = {f"{m}.{p}": id(_resolve(modules, m, p)) for m, p in _EXPECTED_ROSTER}
    buffer = io.StringIO()
    saved = sys.stderr
    sys.stderr = buffer
    try:
        child = run_in_child(
            lambda: (
                buffer.getvalue()
                + "|"
                + ",".join(
                    str(id(_resolve(modules, m, p)) != before[f"{m}.{p}"])
                    for m, p in _EXPECTED_ROSTER
                )
            )
        )
    finally:
        sys.stderr = saved
    announced, _, replaced = child.output.rpartition("|")
    assert "KeyError" in announced and "replacing a global lock after a fork" in announced, (
        announced
    )
    assert replaced == ",".join(["True"] * len(_EXPECTED_ROSTER)), replaced


# -- each roster lock held across the fork --------------------------------------------------------

_HELD = [pair for pair in _EXPECTED_ROSTER if pair != ("_lifecycle", "_state._stop")]

_BLOCKS = [pair for pair in _HELD if pair != ("config", "_sink_lock")]
"""The held locks whose absence from the roster blocks the child outright.

``config._sink_lock`` is acquired with a timeout, the call's swap budget, so a child holding an
unrepaired copy is delayed by that budget and then proceeds rather than blocking. Its control is
``tests/test_concurrent_configure.py``'s, which asserts the delay instead.
"""


def _child_session() -> str:
    """Every call FR-001's criterion names, in a child; returns once all of them have."""
    report_from_child("started;")
    log_foundry.configure(service="roster-child")
    log_foundry.configure(sink=MemorySink())
    log_foundry.info("orphan")

    @log_foundry.trace
    def traced() -> int:
        return 1

    traced()
    log_foundry.flush()
    log_foundry.health()
    log_foundry.shutdown(2.0)
    log_foundry.info("echoed", echo=True)
    return "returned"


class _FailingStream:
    """A console stream whose write fails transiently, so the echo takes the writer's lock."""

    def write(self, text: str) -> int:
        raise OSError("transient")

    def flush(self) -> None:
        """Flushes nothing."""


def _forked_while_held(module: str, path: str, monkeypatch: pytest.MonkeyPatch) -> Any:
    """Holds one roster lock on a parent thread across a fork and runs the child's calls.

    ``ConsoleWriter._lock`` is taken only on a failed echo, so the console stream fails
    transiently for that case: the echo is then the call that must not block.
    """
    log_foundry.configure(service="roster", sink=MemorySink())
    if (module, path) == ("api", "_console._lock"):
        monkeypatch.setattr(log_foundry.api._console, "_stream", _FailingStream())
    lock = _resolve(_package_modules(), module, path)
    released, holder = _held_by_a_parent_thread(lock)
    try:
        return run_in_child(_child_session, timeout=5)
    finally:
        released.set()
        holder.join(5.0)


@pytest.mark.parametrize(("module", "path"), _HELD, ids=[f"{m}.{p}" for m, p in _HELD])
def test_a_child_returns_while_a_parent_holds_a_roster_lock(
    module: str, path: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FR-001: configure, a sink change, an orphan, a trace, flush, health, shutdown and an echo.

    Each returns within 5 s.
    """
    child = _forked_while_held(module, path, monkeypatch)
    assert child.finished, f"the child did not return: it {child.ending}, {child.output!r}"
    assert child.output.endswith("returned"), child.output


@pytest.mark.parametrize(("module", "path"), _BLOCKS, ids=[f"{m}.{p}" for m, p in _BLOCKS])
def test_without_its_entry_the_child_blocks(
    module: str, path: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The control: the same child, with that one pair taken off the roster, blocks."""
    monkeypatch.setattr(
        _fork,
        "_GLOBAL_PRIMITIVES",
        tuple(pair for pair in _EXPECTED_ROSTER if pair != (module, path)),
    )
    child = _forked_while_held(module, path, monkeypatch)
    assert child.blocked, f"the child did not block: it {child.ending}, {child.output!r}"


def test_the_sinks_and_the_workers_stop_signal_are_the_rosters_in_the_child() -> None:
    """FR-002: a sink's stop signal, the worker's ``_stop`` and ``_state._stop`` are one object.

    The roster replaces ``_state._stop`` first and the walk then reaches the worker and the sink,
    all through one memo, so the three stay one ``Event`` in the child (SPEC-027).
    """
    from log_foundry.sinks.multi import MultiSink

    sink = MultiSink(MemorySink())
    log_foundry.configure(service="roster", sink=sink)

    @log_foundry.trace
    def traced() -> int:
        return 1

    traced()
    worker = _lifecycle._state.worker_exists()
    assert worker is not None
    old = _lifecycle._state._stop
    assert sink.log_foundry_stop_signal is worker._stop is old, "the precondition, in the parent"

    def read() -> str:
        live = _lifecycle._state.worker_exists()
        assert live is not None
        stop = _lifecycle._state._stop
        return f"{stop is not old},{live._stop is stop},{sink.log_foundry_stop_signal is stop}"

    child = run_in_child(read)
    assert child.output == "True,True,True", child.output


def test_a_missing_later_hop_is_announced_and_the_rest_replaced(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-001: a pair whose later hop is missing is announced by type, every real pair replaced."""
    import io
    import sys

    monkeypatch.setattr(
        _fork, "_GLOBAL_PRIMITIVES", (("_lifecycle", "_state._gone_lock"), *_EXPECTED_ROSTER)
    )
    modules = _package_modules()
    before = {f"{m}.{p}": id(_resolve(modules, m, p)) for m, p in _EXPECTED_ROSTER}
    buffer = io.StringIO()
    saved = sys.stderr
    sys.stderr = buffer
    try:
        child = run_in_child(
            lambda: (
                buffer.getvalue()
                + "|"
                + ",".join(
                    str(id(_resolve(modules, m, p)) != before[f"{m}.{p}"])
                    for m, p in _EXPECTED_ROSTER
                )
            )
        )
    finally:
        sys.stderr = saved
    announced, _, replaced = child.output.rpartition("|")
    assert (
        "(AttributeError)" in announced and "replacing a global lock after a fork" in announced
    ), announced
    assert replaced == ",".join(["True"] * len(_EXPECTED_ROSTER)), replaced
