"""SPEC-081 FR-004, FR-005 and FR-006: the child pins what it inherited, and marks its heap.

A child pins every inherited entry whose sink is alive before anything else runs; its marking is
seeded by what the parent still holds, less the closed sinks of a shipped class; a marking that
trips the ceiling falls back to marking the sink-shaped objects on the inherited heap; and where
that scan cannot run, the child refuses every ``sink=`` it is handed except a root the library
built. Every case is a real ``os.fork()``.

**The ceiling hazard** (FR-005). A case that trips the marking by lowering ``_MARKING_CEILING``
must restore it in the child before the child's own ``configure()``: the same constant bounds
``_bounded_children``, which is ``stamp``'s read of ``MultiSink._sinks``, and left at zero it hides
the inherited sink from the child's stamp, so a ``MultiSink`` case passes vacuously. The restore
is an ``os.register_at_fork(after_in_child=...)`` handler registered at this module's import,
after the library's own, so it runs once the library's walk is done. It cannot be unregistered,
so it acts only while :data:`_RESTORE` holds a value, which the :func:`trip` fixture sets in the
parent and clears after the test.
"""

from __future__ import annotations

import ast
import gc
import io
import os
import pathlib
import sys
import weakref
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry
from log_foundry import _fork, _lifecycle, config
from log_foundry.sinks.base import Sink
from log_foundry.sinks.file import FileSink
from log_foundry.sinks.filtering import FilteringSink
from log_foundry.sinks.memory import MemorySink
from log_foundry.sinks.multi import MultiSink
from log_foundry.sinks.stdout import StdoutSink
from test_fork_lifecycle import _Child, run_in_child
from test_ownership_record import (
    RecordingSink,
    ThirdPartyWrapper,
    dead_entry_and_lander,
    entry_for,
    join_closers,
    restore,
    stash,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

pytestmark = pytest.mark.usefixtures("no_close_left_in_flight")

_RESTORE: list[int] = []
"""The ceiling a child restores after the library's walk, while a test has set it."""


def _restore_ceiling_in_child() -> None:
    """Puts the shipped ceiling back in a child, after the library's walk has tripped on it."""
    if _RESTORE:
        _lifecycle._MARKING_CEILING = _RESTORE[0]


os.register_at_fork(after_in_child=_restore_ceiling_in_child)

_HEAP_LINE = "this child marked the sink-shaped objects on its inherited heap instead"
_LAST_RESORT = "this child cannot tell what it inherited and will close only a sink the library"
_REFUSED_SINK = "configure(sink=...) in a forked child that could not tell what it inherited"


@pytest.fixture
def trip(monkeypatch: pytest.MonkeyPatch) -> Iterator[Callable[[], None]]:
    """Lowers the ceiling to zero in the parent when called; the child restores it."""
    _RESTORE.append(_lifecycle._MARKING_CEILING)

    def lower() -> None:
        monkeypatch.setattr(_lifecycle, "_MARKING_CEILING", 0)

    yield lower
    _RESTORE.clear()


@pytest.fixture
def heap_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """Makes the heap scan raise, in the parent and so in every child forked from it."""

    def unreadable() -> list[object]:
        raise RuntimeError("the heap cannot be read")

    monkeypatch.setattr(_lifecycle, "_heap_objects", unreadable)


@pytest.fixture
def roots_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    """Makes the walk itself raise, so a child goes to the last resort with no heap scan."""

    def unresolvable() -> list[object]:
        raise RuntimeError("the roots cannot be resolved")

    monkeypatch.setattr(_lifecycle, "_live_roots", unresolvable)


def fork_reading_stderr(work: Callable[[io.StringIO], str], **kwargs: Any) -> _Child:
    """Forks with ``sys.stderr`` replaced, so the child can read what its fork handler wrote."""
    buffer = io.StringIO()
    saved = sys.stderr
    sys.stderr = buffer
    try:
        return run_in_child(lambda: work(buffer), **kwargs)
    finally:
        sys.stderr = saved


def parent_sink_behind_a_wrapper() -> RecordingSink:
    """Configures ``ThirdPartyWrapper(s)``, logs and flushes; ``s`` is never recorded."""
    inner = RecordingSink()
    log_foundry.configure(sink=ThirdPartyWrapper(inner))
    log_foundry.info("parent")
    log_foundry.flush()
    assert entry_for(inner) is None, "the precondition: the parent's stamp cannot see it"
    return inner


def hand_over_and_shut_down(sink: object) -> None:
    """The child's re-hand: configure, one event, shutdown."""
    log_foundry.configure(sink=sink)  # type: ignore[arg-type]
    log_foundry.info("child")
    log_foundry.shutdown(3.0)


# -- FR-004: the child pins what it inherited, and its seeds are what the parent holds ---------


def _reconfigure_500_times(keep: list[MemorySink] | None) -> None:
    for index in range(500):
        sink = MemorySink()
        log_foundry.configure(sink=sink)
        for _ in range(100):
            log_foundry.info("event")
        log_foundry.flush()
        if keep is not None and index < 499:
            keep.append(sink)
        del sink
    join_closers()
    gc.collect()


def test_a_child_of_a_long_reconfiguring_process_does_not_trip_the_marking() -> None:
    """AC-1. Before SPEC-081, a child forked after about 500 configures always tripped."""
    _reconfigure_500_times(None)
    child = fork_reading_stderr(lambda err: str("passed 100000" in err.getvalue()), timeout=30)
    assert child.output == "False", child.output


def test_kept_closed_memory_sinks_do_not_trip_the_childs_marking() -> None:
    """AC-2. Kept alive, the 499 closed ``MemorySink``s are pinned, but they are not seeds.

    **The margin is thin, and that is measured rather than hidden.** The plan review counted 202
    marking visits per closed ``MemorySink`` of 100 events, so 499 of them come to about 100,800
    against a ceiling of 100,000: without the exclusion the child trips by under 1%. The count is
    deterministic, so this is a sharp oracle for the exclusion today; a change that makes an event
    cheaper to visit can make it vacuous, and this test then needs a larger loop, not a lower bar.
    """
    kept: list[MemorySink] = []
    _reconfigure_500_times(kept)
    child = fork_reading_stderr(lambda err: str("passed 100000" in err.getvalue()), timeout=30)
    assert child.output == "False", child.output
    assert len(kept) == 499


def test_the_record_seeds_leave_out_closed_shipped_sinks() -> None:
    """AC-3. In the parent: no closed ``MemorySink`` is among the seeds."""
    kept: list[MemorySink] = []
    _reconfigure_500_times(kept)
    seeds = {id(seed) for seed in _lifecycle._record_seeds()}
    assert all(entry_for(sink).closed for sink in kept)  # type: ignore[union-attr]
    assert not any(id(sink) in seeds for sink in kept)


def test_a_kept_superseded_wrapper_shields_its_inner_sink_in_the_child() -> None:
    """AC-4. Superseded with nothing logged, kept by the application, its sink stays refused."""
    inner = RecordingSink()
    wrapper = ThirdPartyWrapper(inner)
    log_foundry.configure(sink=wrapper)
    log_foundry.configure(sink=RecordingSink())
    parent = os.getpid()

    def in_child() -> str:
        entry = entry_for(inner)
        named = None if entry is None else entry.pid
        hand_over_and_shut_down(MultiSink(inner))
        return f"{named in (parent, _lifecycle._FOREIGN)},{inner.closes}"

    child = run_in_child(in_child)
    assert child.output == "True,0", child.output
    assert wrapper is not None


def test_a_sink_placed_beneath_a_demoted_wrapper_is_marked_foreign_in_the_child() -> None:
    """AC-5. The capture cannot have seen it; the kept wrapper's seed is what refuses it."""
    wrapper = ThirdPartyWrapper(None)
    log_foundry.configure(sink=wrapper)
    log_foundry.configure(sink=RecordingSink())
    late = RecordingSink()
    wrapper.inner = late
    assert entry_for(late) is None, "the precondition: nothing recorded it"

    def in_child() -> str:
        entry = entry_for(late)
        named = None if entry is None else entry.pid
        hand_over_and_shut_down(late)
        return f"{named == _lifecycle._FOREIGN},{late.closes}"

    child = run_in_child(in_child)
    assert child.output == "True,0", child.output


class DuckPool:
    """A third-party pool holding its connection two containers down; ``close`` closes nothing."""

    def __init__(self, conn: object) -> None:
        self.pools = {"primary": [conn]}

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Delivers nothing."""

    def close(self) -> None:
        """Closes nothing."""


class SinkPool(Sink):
    """The same pool, subclassing the documented ``Sink`` base."""

    def __init__(self, conn: object) -> None:
        self.pools = {"primary": [conn]}

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Delivers nothing."""

    def close(self) -> None:
        """Closes nothing."""


class MemoryPool(MemorySink):
    """The same pool, subclassing a shipped sink and overriding its close."""

    def __init__(self, conn: object) -> None:
        super().__init__()
        self.pools = {"primary": [conn]}

    def close(self) -> None:
        """Closes nothing."""


@pytest.mark.parametrize("pool_type", [DuckPool, SinkPool, MemoryPool], ids=lambda t: t.__name__)
def test_a_closed_pool_still_shields_its_connection(pool_type: type) -> None:
    """AC-6. Only an exact shipped class leaves the seeds; a caller's subclass is still one.

    The ``SinkPool`` row is the plan review's blocker: ``_fork._is_owned`` accepts any ``Sink``
    subclass, so an exclusion keyed on it left this pool out of the seeds and the child closed
    the connection (`sinkpool.py`).
    """
    conn = RecordingSink()
    pool = pool_type(conn)
    log_foundry.configure(sink=pool)
    log_foundry.info("owed to the pool")
    log_foundry.flush()
    log_foundry.configure(sink=RecordingSink())
    join_closers()
    entry = entry_for(pool)
    assert entry is not None and entry.closed is True, "the precondition: closed and kept"

    def in_child() -> str:
        hand_over_and_shut_down(conn)
        return str(conn.closes)

    child = run_in_child(in_child)
    assert child.output == "0", child.output


class Finalizing(RecordingSink):
    """A sink whose finalizer closes it, counting on the class so a collected one still counts."""

    total = 0

    def close(self) -> None:
        """Counts the close on the class."""
        type(self).total += 1

    def __del__(self) -> None:
        self.close()


def test_an_inherited_closed_sink_is_pinned_before_the_roster_and_never_finalized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AC-7. The pin runs first, so dropping the child's last reference collects nothing."""
    Finalizing.total = 0
    closed = Finalizing()
    log_foundry.configure(sink=closed)
    log_foundry.info("owed")
    log_foundry.configure(sink=RecordingSink())
    join_closers()
    assert Finalizing.total == 1 and entry_for(closed).strong is None  # type: ignore[union-attr]
    key = id(closed)
    watcher = weakref.ref(closed)
    kept = [closed]
    del closed
    at_roster: list[bool] = []
    original = _fork._replace_roster

    def spy(repair: _fork._Repair) -> None:
        entry = _lifecycle._state._owned.get(key)
        at_roster.append(entry is not None and entry.strong is not None)
        original(repair)

    monkeypatch.setattr(_fork, "_replace_roster", spy)

    def in_child() -> str:
        kept.clear()
        gc.collect()
        return f"{at_roster},{watcher() is not None},{Finalizing.total}"

    child = run_in_child(in_child)
    assert child.output == "[True],True,1", child.output


# -- FR-005: a tripped marking falls back to marking the heap -----------------------------------


@pytest.mark.parametrize(
    "build",
    [
        pytest.param(lambda s: s, id="bare"),
        pytest.param(lambda s: MultiSink(s), id="multisink"),
        pytest.param(lambda s: FilteringSink(s), id="filteringsink"),
    ],
)
def test_a_tripped_child_refuses_a_parents_sink_handed_over_again(
    build: Callable[[object], object], trip: Callable[[], None]
) -> None:
    """AC-1. Every library-wrapper re-hand closed the parent's transport before SPEC-081."""
    inner = parent_sink_behind_a_wrapper()
    trip()

    def in_child() -> str:
        ceiling = _lifecycle._MARKING_CEILING
        failed = _lifecycle._state._marking_failed
        hand_over_and_shut_down(build(inner))
        return f"{ceiling},{failed},{inner.closes}"

    child = run_in_child(in_child)
    assert child.output == "100000,False,0", child.output


def test_a_frozen_heap_sends_a_tripped_child_to_the_last_resort(trip: Callable[[], None]) -> None:
    """AC-3. A frozen heap is a failed scan; the child refuses, and says why."""
    inner = parent_sink_behind_a_wrapper()
    trip()

    def in_child(err: io.StringIO) -> str:
        frozen = gc.get_freeze_count() > 0
        failed = _lifecycle._state._marking_failed
        said = "the heap is frozen (gc.freeze); " + _LAST_RESORT in err.getvalue()
        hand_over_and_shut_down(inner)
        return f"{frozen},{failed},{said},{inner.closes}"

    gc.freeze()
    try:
        child = fork_reading_stderr(in_child)
    finally:
        gc.unfreeze()
    assert child.output == "True,True,True,0", child.output


def test_a_grandchild_of_a_last_resort_child_scans_and_recovers(trip: Callable[[], None]) -> None:
    """AC-4. The flag is inherited, so the grandchild scans even though its walk does not trip.

    The child reaches the last resort through a frozen heap, which it then unfreezes before
    forking: a route the grandchild does not inherit, so the grandchild's scan can succeed.
    """
    inner = parent_sink_behind_a_wrapper()
    trip()

    def in_child() -> str:
        failed = _lifecycle._state._marking_failed
        gc.unfreeze()

        def in_grandchild() -> str:
            mine = RecordingSink()
            flag = _lifecycle._state._marking_failed
            hand_over_and_shut_down(inner)
            hand_over_and_shut_down(mine)
            return f"{flag},{inner.closes},{mine.closes}"

        grandchild = run_in_child(in_grandchild)
        return f"{failed};{grandchild.output}"

    gc.freeze()
    try:
        child = run_in_child(in_child, timeout=20)
    finally:
        gc.unfreeze()
    assert child.output == "True;False,0,1", child.output


class CountingFile(FileSink):
    """A ``FileSink`` counting its closes."""

    closes = 0

    def close(self) -> None:
        """Counts, then closes the file."""
        type(self).closes += 1
        super().close()


@pytest.mark.parametrize("kind", ["built", "reacquired-file-sink"])
def test_a_tripped_child_closes_what_it_builds(
    kind: str, trip: Callable[[], None], tmp_path: pathlib.Path
) -> None:
    """AC-5. The heap is marked, the flag is clear, so the child's own sinks close as usual."""
    if kind == "built":
        parent_sink_behind_a_wrapper()
    else:
        CountingFile.closes = 0
        log_foundry.configure(sink=CountingFile(str(tmp_path / "trip.log")))
        log_foundry.info("parent")
        log_foundry.flush()
    trip()

    def in_child() -> str:
        if kind == "built":
            mine = RecordingSink()
            hand_over_and_shut_down(mine)
            return f"{_lifecycle._state._marking_failed},{mine.closes}"
        configured = config._live_config().sink
        releasable = _lifecycle.releasable(configured)
        log_foundry.info("child")
        log_foundry.shutdown(3.0)
        return f"{releasable},{CountingFile.closes}"

    child = run_in_child(in_child)
    assert child.output == ("False,1" if kind == "built" else "True,1"), child.output


def test_a_tripped_child_refuses_a_sink_the_parent_never_handed_over(
    trip: Callable[[], None],
) -> None:
    """AC-6. The accepted side effect: in a scanning child only, refused rather than closed."""
    loose = RecordingSink()
    parent_sink_behind_a_wrapper()
    trip()

    def in_child() -> str:
        hand_over_and_shut_down(loose)
        return str(loose.closes)

    child = run_in_child(in_child)
    assert child.output == "0", child.output


def test_a_tripped_child_whose_scan_succeeds_says_so_once(trip: Callable[[], None]) -> None:
    """AC-8. One ``pending`` line, and no ``lost`` line from the walk."""
    parent_sink_behind_a_wrapper()
    trip()

    def in_child(err: io.StringIO) -> str:
        text = err.getvalue()
        return f"{text.count(_HEAP_LINE)},{'log-foundry: lost' in text}"

    child = fork_reading_stderr(in_child)
    assert child.output == "1,False", child.output


def test_no_source_says_the_retired_line() -> None:
    """AC-8. The old trip line is gone from ``src/``, read as joined literals and as text.

    A line grep is not enough: the old line was split across two adjacent literals at one site,
    which the parser joins and a grep does not.
    """
    retired = "unless it configures that sink itself"
    root = pathlib.Path(log_foundry.__file__).resolve().parent
    for path in root.rglob("*.py"):
        source = path.read_text(encoding="utf-8")
        assert retired not in " ".join(source.split()), path
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                assert retired not in node.value, path
            if isinstance(node, ast.JoinedStr):
                text = "".join(
                    part.value
                    for part in node.values
                    if isinstance(part, ast.Constant) and isinstance(part.value, str)
                )
                assert retired not in text, path


def test_a_tripped_childs_heap_write_replaces_a_dead_entry(trip: Callable[[], None]) -> None:
    """FR-003 AC-3, the heap writer: the frontier is tripped before it reaches the sink."""
    target, fresh = dead_entry_and_lander()
    dead = stash(target)
    log_foundry.configure(sink=ThirdPartyWrapper(fresh))
    restore(target, dead)
    trip()

    def in_child() -> str:
        entry = entry_for(fresh)
        return "absent" if entry is None else str(entry.pid)

    child = run_in_child(in_child)
    assert child.output == str(_lifecycle._FOREIGN), child.output


# -- FR-006: when the heap scan fails, the child refuses every sink it is handed ---------------


def test_a_last_resort_child_refuses_a_parents_sink_and_marks_it_foreign(
    trip: Callable[[], None], heap_fails: None
) -> None:
    """AC-1. A tripped child whose heap scan raised."""
    inner = parent_sink_behind_a_wrapper()
    trip()

    def in_child() -> str:
        failed = _lifecycle._state._marking_failed
        hand_over_and_shut_down(inner)
        entry = entry_for(inner)
        return f"{failed},{inner.closes},{entry is not None and entry.pid == _lifecycle._FOREIGN}"

    child = run_in_child(in_child)
    assert child.output == "True,0,True", child.output


def test_a_last_resort_child_closes_what_its_factory_builds(
    trip: Callable[[], None], heap_fails: None
) -> None:
    """AC-2. ``sink_factory=`` is the way out the last resort's line names."""
    parent_sink_behind_a_wrapper()
    trip()

    def in_child() -> str:
        failed = _lifecycle._state._marking_failed
        built: list[RecordingSink] = []
        log_foundry.configure(sink_factory=lambda: built.append(RecordingSink()) or built[-1])
        log_foundry.info("child")
        log_foundry.shutdown(3.0)
        return f"{failed},{[sink.closes for sink in built]}"

    child = run_in_child(in_child)
    assert child.output == "True,[1]", child.output


def test_a_last_resort_child_owns_the_lazy_default_it_builds(
    roots_fail: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC-3. The lazy default is a root the library built."""
    config._config = config.Config()
    closes: list[int] = []
    original = StdoutSink.close

    def counting(self: StdoutSink) -> None:
        closes.append(1)
        original(self)

    monkeypatch.setattr(StdoutSink, "close", counting)

    def in_child() -> str:
        failed = _lifecycle._state._marking_failed
        log_foundry.info("child")
        default = config._live_config().sink
        releasable = _lifecycle.releasable(default)
        log_foundry.shutdown(3.0)
        return f"{failed},{releasable},{len(closes)}"

    child = run_in_child(in_child)
    assert child.output == "True,True,1", child.output


def test_a_last_resort_child_announces_its_first_refused_configure_once(
    roots_fail: None,
) -> None:
    """AC-4. Two refused ``sink=`` roots, one line; neither closed; a factory writes none."""
    log_foundry.configure(sink=RecordingSink())

    def refused(err: io.StringIO) -> str:
        first, second = RecordingSink(), RecordingSink()
        log_foundry.configure(sink=first)
        log_foundry.info("to the first")
        log_foundry.configure(sink=second)
        log_foundry.info("to the second")
        log_foundry.shutdown(3.0)
        failed = _lifecycle._state._marking_failed
        return f"{failed},{err.getvalue().count(_REFUSED_SINK)},{first.closes},{second.closes}"

    child = fork_reading_stderr(refused)
    assert child.output == "True,1,0,0", child.output

    def factory(err: io.StringIO) -> str:
        log_foundry.configure(sink_factory=RecordingSink)
        log_foundry.info("child")
        log_foundry.shutdown(3.0)
        return str(err.getvalue().count(_REFUSED_SINK))

    child = fork_reading_stderr(factory)
    assert child.output == "0", child.output


def test_a_last_resort_child_refuses_an_unrecorded_parent_sink_handed_over_bare(
    roots_fail: None,
) -> None:
    """AC-5. The re-hand closed it at ``9f4d4dfd``: the stamp recorded it as the child's."""
    inner = parent_sink_behind_a_wrapper()

    def in_child() -> str:
        failed = _lifecycle._state._marking_failed
        hand_over_and_shut_down(inner)
        return f"{failed},{inner.closes}"

    child = run_in_child(in_child)
    assert child.output == "True,0", child.output


def test_a_last_resort_childs_factory_wrapper_members_are_refused(roots_fail: None) -> None:
    """Known residual 3, asserted: only the root a factory returns is the child's."""
    inner = parent_sink_behind_a_wrapper()

    def in_child() -> str:
        built: list[MultiSink] = []
        log_foundry.configure(sink_factory=lambda: built.append(MultiSink(inner)) or built[-1])
        log_foundry.info("child")
        root = _lifecycle.releasable(built[0])
        log_foundry.shutdown(3.0)
        return f"{_lifecycle._state._marking_failed},{root},{inner.closes}"

    child = run_in_child(in_child)
    assert child.output == "True,True,0", child.output


def test_built_is_passed_at_exactly_the_four_library_built_roots() -> None:
    """AC-6. Read from the AST of ``src/``."""
    root = pathlib.Path(log_foundry.__file__).resolve().parent
    sites: set[str] = set()
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for scope in ast.walk(tree):
            if not isinstance(scope, ast.ClassDef | ast.Module):
                continue
            for node in scope.body:
                if not isinstance(node, ast.FunctionDef):
                    continue
                for call in ast.walk(node):
                    if (
                        isinstance(call, ast.Call)
                        and ast.unparse(call.func).split(".")[-1] == "stamp"
                        and any(kw.arg == "built" for kw in call.keywords)
                    ):
                        owner = f"{scope.name}." if isinstance(scope, ast.ClassDef) else ""
                        sites.add(f"{path.stem}.{owner}{node.name}")
    assert sites == {
        "config._ensure_sink",
        "config._change_sink",
        "_factory._FactorySink._checked",
        "_lifecycle._adopt_factory_after_fork",
    }, sites
