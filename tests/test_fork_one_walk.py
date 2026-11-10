"""SPEC-079 FR-002 and FR-003: one walk from the runtime's roots repairs, hooks and marks.

A forked child runs one traversal where two ran before: a repair frontier from the runtime's
live roots, then a marking frontier from what that frontier forwarded and from the record. Each
test here drives a real child where the promise is a property of two processes, and pairs every
route with a control that takes that one route away and watches the child fail. Serves
invariants 2, 3, 5 and 12.
"""

from __future__ import annotations

import ast
import gc
import pathlib
from typing import Any

import pytest

import log_foundry
from log_foundry import _fork, _lifecycle
from log_foundry.sinks import verdict
from log_foundry.sinks.file import FileSink
from log_foundry.sinks.memory import MemorySink
from log_foundry.sinks.multi import MultiSink
from log_foundry.sinks.verdict import SinkCounters
from log_foundry.worker import Worker
from test_fork_lifecycle import (
    _AdapterEngineSink,
    _GatingStream,
    _held_by_a_parent_thread,
    _lines_holding,
    _log_in_child,
    _park_inside_a_buffered_batch,
    run_in_child,
)
from test_lifecycle_races import _HOOKED, _HookSink
from test_sink_ownership import StructuralSink, ThirdPartyWrapper

_SRC = pathlib.Path(log_foundry.__file__).resolve().parent


# -- FR-003: the deletions -------------------------------------------------------------------------


def test_the_module_walk_and_the_separate_marking_are_deleted() -> None:
    """FR-003: ``_fork`` defines none of the four names, ``_lifecycle`` neither of its two."""

    def defined(name: str) -> set[str]:
        tree = ast.parse((_SRC / name).read_text(encoding="utf-8"))
        names: set[str] = set()
        for node in tree.body:
            if isinstance(node, ast.FunctionDef | ast.ClassDef):
                names.add(node.name)
            elif isinstance(node, ast.Assign):
                names |= {t.id for t in node.targets if isinstance(t, ast.Name)}
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                names.add(node.target.id)
        return names

    fork_names = defined("_fork.py")
    lifecycle_names = defined("_lifecycle.py")
    assert "_reinit_after_fork" in fork_names and "_walk_after_fork" in lifecycle_names, (
        "the scan reads the real modules"
    )
    gone = {"_reinit_primitives", "_container_children", "_is_traversable", "_FORK_SKIP"}
    assert not gone & fork_names, gone & fork_names
    assert not {"_mark_inherited", "_inheritance_roots"} & lifecycle_names


# -- FR-002: what the repair frontier forwards -----------------------------------------------------


def _record_in_child(conn: object) -> str:
    record = _lifecycle._state._owned.get(id(conn))
    return "unrecorded" if record is None else str(record[0])


def test_a_foreign_holders_namespace_is_forwarded_to_the_marking() -> None:
    """FR-002: ``MultiSink(ThirdPartyWrapper(conn))`` records ``conn`` in the child as foreign."""
    conn = StructuralSink()
    log_foundry.configure(service="walk", sink=MultiSink(ThirdPartyWrapper(conn)))
    assert id(conn) not in _lifecycle._state._owned, "the precondition: the parent never saw it"
    child = run_in_child(lambda: _record_in_child(conn))
    assert child.output == str(_lifecycle._FOREIGN), child.output


def test_without_the_forward_the_foreign_holders_sink_goes_unrecorded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The control: a walk that does not forward a foreign holder's namespace never reaches it."""
    monkeypatch.setattr(_lifecycle, "_forward_foreign", lambda holder, after_fork: None)
    conn = StructuralSink()
    log_foundry.configure(service="walk", sink=MultiSink(ThirdPartyWrapper(conn)))
    child = run_in_child(lambda: _record_in_child(conn))
    assert child.output == "unrecorded", child.output


def _spy_namespace_reads(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Records the id of every holder ``_fork._namespace_items`` reads, in this process and its child."""
    read: list[int] = []
    original = _fork._namespace_items

    def spy(holder: Any) -> list[tuple[str, Any]]:
        read.append(id(holder))
        return original(holder)

    monkeypatch.setattr(_fork, "_namespace_items", spy)
    return read


def _a_process_with_a_worker() -> Worker:
    log_foundry.configure(
        service="walk", sink=MultiSink(MemorySink(), ThirdPartyWrapper(StructuralSink()))
    )

    @log_foundry.trace
    def traced() -> int:
        return 1

    traced()
    worker = _lifecycle._state.worker_exists()
    assert worker is not None
    return worker


def test_the_walk_enters_neither_the_queue_nor_the_thread(monkeypatch: pytest.MonkeyPatch) -> None:
    """FR-002: a worker's ``_queue`` and ``_thread`` are not entered, by the spy's record."""
    worker = _a_process_with_a_worker()
    targets = {id(worker._queue), id(worker._thread)}
    read = _spy_namespace_reads(monkeypatch)
    read.clear()
    child = run_in_child(lambda: f"{id(worker) in read},{bool(targets & set(read))}")
    assert child.output == "True,False", child.output


def test_the_walk_reads_each_namespace_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """FR-003: per fork, the spy records each object at most once."""
    _a_process_with_a_worker()
    read = _spy_namespace_reads(monkeypatch)
    read.clear()
    child = run_in_child(lambda: f"{len(read)},{len(read) - len(set(read))}")
    count, repeats = child.output.split(",")
    assert int(count) > 0, "the spy saw the walk at all"
    assert repeats == "0", f"{repeats} namespaces were read twice in one fork"


def test_a_walk_that_raises_still_runs_its_hooks_and_the_handlers() -> None:
    """FR-002: a hook collected before the raise runs, every handler runs, the raise is announced."""
    import io
    import sys

    events: list[str] = []

    class _Hooked:
        def reacquire_after_fork(self) -> None:
            events.append("hook")

    hooked = _Hooked()

    def raising_walk(repair: _fork._Repair) -> None:
        repair.hooked.append(hooked)
        raise LookupError("a-value-from-the-walk-5678")

    def last_handler() -> None:
        events.append("handler")

    original = _fork._walk
    _fork._walk = raising_walk
    _fork._child_handlers.append(last_handler)
    buffer = io.StringIO()
    saved = sys.stderr
    sys.stderr = buffer
    try:
        child = run_in_child(lambda: f"{events}|{buffer.getvalue()}")
    finally:
        sys.stderr = saved
        _fork._walk = original
        _fork._child_handlers.remove(last_handler)
    ran, _, stderr = child.output.partition("|")
    assert ran == "['hook', 'handler']", ran
    assert "LookupError" in stderr and "a-value-from-the-walk-5678" not in stderr, stderr


# -- FR-002: the counters registry -----------------------------------------------------------------


def test_the_registry_holds_every_live_counters_and_drops_a_collected_one() -> None:
    """Every ``SinkCounters`` built is a member, and one whose owner is gone leaves."""
    counters = SinkCounters()
    assert any(member is counters for member in verdict._LIVE_COUNTERS)
    ident = id(counters)
    del counters
    gc.collect()
    assert all(id(member) != ident for member in verdict._LIVE_COUNTERS)


class _BoundEmitWrapper:
    """A third-party wrapper that keeps only an engine sink's bound ``emit``, nothing else."""

    def __init__(self, inner: Any) -> None:
        self._emit = inner.emit

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Forwards through the bound method."""
        self._emit(batch)

    def close(self) -> None:
        """Releases nothing."""


def _forked_bound_method_run() -> Any:
    inner = _AdapterEngineSink()
    log_foundry.configure(service="walk", sink=_BoundEmitWrapper(inner))
    released, holder = _held_by_a_parent_thread(inner.log_foundry_counters._counter_lock)
    del inner

    def run() -> str:
        log_foundry.info("child")
        return "returned"

    try:
        return run_in_child(run, timeout=5)
    finally:
        released.set()
        holder.join(5.0)


def test_a_child_repairs_counters_reached_only_through_a_bound_method() -> None:
    """FR-002 (A4): counters no reading walk reaches are repaired from the registry."""
    child = _forked_bound_method_run()
    assert child.finished, f"the child did not return: it {child.ending}, {child.output!r}"
    assert child.output.endswith("returned"), child.output


def test_without_the_registry_the_bound_method_child_blocks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The control: with the registry read taken away, the same child hangs on the dead lock."""
    monkeypatch.setattr(_lifecycle, "_repair_live_counters", lambda repair: None)
    child = _forked_bound_method_run()
    assert child.blocked, f"the child did not block: it {child.ending}, {child.output!r}"


# -- FR-003: hooks from forwarded seeds, and the ceiling's order -----------------------------------


class _Buried(MemorySink):
    """A package holder whose sink sits two containers down, and which delivers to it."""

    def __init__(self, inner: FileSink) -> None:
        super().__init__()
        self.buried = [[inner]]

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Delivers through the buried sink, so the parent's drain thread parks inside it."""
        self.buried[0][0].emit(batch)


def _forwarded_file_sink(
    tmp_path: pathlib.Path, shape: str
) -> tuple[Any, _GatingStream, pathlib.Path]:
    path = tmp_path / f"{shape}.ndjson"
    inner = FileSink(str(path))
    inner.emit([{"msg": "before-the-fork"}])
    stream = _GatingStream(inner._stream, park_after=1)
    inner._stream = stream  # type: ignore[assignment]
    sink: Any = ThirdPartyWrapper(inner) if shape == "wrapper" else _Buried(inner)
    log_foundry.configure(service="walk", version="0", env="test", sink=sink)
    return sink, stream, path


def _fork_inside_the_batch(tmp_path: pathlib.Path, shape: str) -> str:
    sink, stream, path = _forwarded_file_sink(tmp_path, shape)
    worker = Worker(sink, batch_size=1, flush_interval=0.01)
    _lifecycle._state._worker = worker
    gate = _park_inside_a_buffered_batch(stream, worker)
    try:
        child = run_in_child(_log_in_child, timeout=6)
    finally:
        gate.release.set()
    assert child.output == "logged", child.output
    assert log_foundry.flush(timeout=5.0)
    return path.read_text(encoding="utf-8")


@pytest.mark.parametrize("shape", ["wrapper", "buried"])
def test_a_forwarded_file_sink_strands_the_parents_pending_bytes(
    tmp_path: pathlib.Path, shape: str
) -> None:
    """FR-003: behind a third-party wrapper or two containers down, the child writes none again."""
    written = _fork_inside_the_batch(tmp_path, shape)
    for marker in ("before-the-fork", "parent-a", "parent-b"):
        assert _lines_holding(written, marker) == 1, f"{marker} in {written!r}"


@pytest.mark.parametrize("shape", ["wrapper", "buried"])
def test_without_forwarded_hooks_the_same_child_duplicates(
    tmp_path: pathlib.Path, shape: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The control: a marking frontier that collects no hooks from forwarded seeds duplicates."""
    original = _lifecycle._mark_frontier

    def no_hooks(*args: Any, **kwargs: Any) -> int:
        kwargs["hook"] = False
        return original(*args, **kwargs)

    monkeypatch.setattr(_lifecycle, "_mark_frontier", no_hooks)
    written = _fork_inside_the_batch(tmp_path, shape)
    assert _lines_holding(written, "parent-a") == 2, written


def test_a_forwarded_seed_is_repaired_before_the_ceiling(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FR-003 (A5): a forwarded ``FileSink`` cannot lose its repair to another seed's caller data.

    The ``MemorySink``'s events are forwarded beside the wrapper's ``FileSink``. The marking
    frontier visits its seeds first in first out, so the ``FileSink`` and its guard are reached
    well inside a ceiling the events alone would spend.
    """
    memory = MemorySink()
    memory.emit([{"n": n} for n in range(500)])
    inner = FileSink(str(tmp_path / "seed.ndjson"))
    log_foundry.configure(service="walk", sink=MultiSink(memory, ThirdPartyWrapper(inner)))
    monkeypatch.setattr(_lifecycle, "_MARKING_CEILING", 40)
    released, holder = _held_by_a_parent_thread(inner._guard._lock)

    def run() -> str:
        log_foundry.info("child")
        return "returned"

    try:
        child = run_in_child(run, timeout=5)
    finally:
        released.set()
        holder.join(5.0)
    assert child.finished, f"the forwarded FileSink stayed dead: {child.ending}, {child.output!r}"


# -- FR-002 (A6): a skipped name is not forwarded --------------------------------------------------


def test_a_forked_child_does_not_hook_a_superseded_sink_the_worker_holds() -> None:
    """A name in ``Worker._FORK_SKIP`` is neither entered nor forwarded, so ``_held`` hooks nothing.

    ``_held`` names sinks the drain thread may still be inside, superseded ones among them;
    forwarding it to the marking frontier would collect a superseded sink's hook, which the
    opt-out exists to stop.
    """
    _HOOKED.clear()
    superseded, live = _HookSink("superseded"), _HookSink("live")
    log_foundry.configure(service="t", sink=live)

    @log_foundry.trace
    def traced() -> int:
        return 1

    traced()
    worker = _lifecycle._state.worker_exists()
    assert worker is not None
    worker._held = [worker.sink, superseded]
    try:
        child = run_in_child(lambda: ",".join(sorted(_HOOKED)))
    finally:
        worker._held = [worker.sink]
    hooked = child.output
    assert hooked == "live", f"the child's walk reached {hooked!r}, not the live sink alone"
