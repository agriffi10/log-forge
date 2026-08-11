"""A process releases only the sinks it acquired here (SPEC-042 FR-001, FR-002, FR-003).

A forked child inherits the parent's sink *object*, and before this every path that releases a
sink acted as though it owned one: a `configure(sink=…)` in a child sent a connection sink's
protocol goodbye and the parent's next write failed with `ECONNRESET`, a `shutdown()` closed the
inherited object, and at exit both processes closed their own copy. For a prefork server one
worker's routine startup could take down the transport every other worker logs through.

The record is laid down at the one moment ownership is knowable -- when the library is *handed*
a sink -- and consulted at the one place the library closes one. Everything here is asserted in
a **real forked child**, because the defect is a property of two processes sharing an object and
a same-process double cannot exhibit it.
"""

from __future__ import annotations

import gc
import os
import socket
import threading
from typing import TYPE_CHECKING

import pytest

import log_foundry
from log_foundry import _lifecycle, config, decorator
from log_foundry.sinks.filtering import FilteringSink
from log_foundry.sinks.memory import MemorySink
from log_foundry.sinks.multi import MultiSink
from log_foundry.sinks.stdout import StdoutSink
from test_fork_lifecycle import run_in_child

if TYPE_CHECKING:
    from collections.abc import Iterator


class RecordingSink:
    """A library-owned sink counting its own closes, so a refusal is visible as a zero."""

    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []
        self.closed = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Keeps the batch so a drain can be asserted separately from a close."""
        self.events.extend(batch)

    def close(self) -> None:
        """Counts the close."""
        self.closed += 1


class StructuralSink:
    """A third-party sink in `README.md`'s documented shape: no library base at all.

    It satisfies `Sink` structurally, which is how every shipped sink satisfies it, and it is
    outside `_fork`'s ownership boundary. FR-001 AC-2 turns on this class: a first draft of the
    record reached it neither by stamp nor by mark, and it is the object a child closed twice.
    """

    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []
        self.closed = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Keeps the batch."""
        self.events.extend(batch)

    def close(self) -> None:
        """Counts the close."""
        self.closed += 1


@pytest.fixture(autouse=True)
def _clear_ownership_record() -> Iterator[None]:
    """Empties the process-global record around each test.

    It is write-once and holds a strong reference for the life of the process, which is correct
    in an application and would otherwise carry one test's sinks into the next.
    """
    with _lifecycle._owned_lock:
        _lifecycle._owned.clear()
    yield
    with _lifecycle._owned_lock:
        _lifecycle._owned.clear()


# -- FR-001: the acquisition record ------------------------------------------------------------


def test_a_sink_configured_here_is_releasable_and_the_same_object_after_a_fork_is_not() -> None:
    """AC-1. The whole mechanism in one assertion, and the child is a real one."""
    sink = RecordingSink()
    log_foundry.configure(service="own", sink=sink)

    assert _lifecycle.releasable(sink) is True

    child = run_in_child(lambda: str(_lifecycle.releasable(sink)))
    assert child.output == "False", child.output


@pytest.mark.parametrize("nested", [False, True], ids=["configured", "inside-a-multisink"])
def test_a_structural_third_party_sink_is_refused_in_the_child(nested: bool) -> None:
    """AC-2. The `MultiSink` case is the one both of a first draft's mechanisms missed.

    An *owned* inner would pass this against a record that still had that hole, so the inner is
    deliberately structural -- no library base, nothing `_fork`'s walk will enter.
    """
    inner = StructuralSink()
    installed = MultiSink(StdoutSink(), inner) if nested else inner
    log_foundry.configure(service="own", sink=installed)

    assert _lifecycle.releasable(inner) is True, "the parent acquired it, wrapper or not"

    child = run_in_child(lambda: str(_lifecycle.releasable(inner)))
    assert child.output == "False", child.output


def test_a_sink_the_child_builds_itself_is_releasable_there() -> None:
    """AC-3. Refusing everything would pass every other criterion here and fail this one."""
    log_foundry.configure(service="own", sink=RecordingSink())

    def in_child() -> str:
        own_inner = RecordingSink()
        own = MultiSink(StdoutSink(), own_inner)
        log_foundry.configure(sink=own)
        return f"{_lifecycle.releasable(own)},{_lifecycle.releasable(own_inner)}"

    child = run_in_child(in_child)
    assert child.output == "True,True", child.output


def test_reconfiguring_the_inherited_sink_in_the_child_does_not_claim_it() -> None:
    """AC-4. Write-once: a stamp overwritten on every `configure()` fails exactly here."""
    sink = RecordingSink()
    log_foundry.configure(service="own", sink=sink)

    def in_child() -> str:
        log_foundry.configure(sink=sink)
        return str(_lifecycle.releasable(sink))

    child = run_in_child(in_child)
    assert child.output == "False", child.output


def test_a_grandchild_refuses_a_sink_the_first_child_inherited() -> None:
    """AC-5. The answer survives a second fork because no descendant may re-stamp."""
    sink = RecordingSink()
    log_foundry.configure(service="own", sink=sink)

    def in_child() -> str:
        grandchild = run_in_child(lambda: str(_lifecycle.releasable(sink)))
        return f"{_lifecycle.releasable(sink)},{grandchild.output}"

    child = run_in_child(in_child, timeout=12)
    assert child.output == "False,False", child.output


def test_a_sink_added_to_a_wrapper_after_configure_is_refused() -> None:
    """AC-6. No record at all, reached through a wrapper that has one, so it is refused.

    The consequence is a leaked handle rather than a destructive close, which is the direction
    FR-001 requires every gap to fail in. A default of *releasable* is what this forbids.
    """
    late = RecordingSink()
    wrapper = MultiSink(StdoutSink())
    log_foundry.configure(service="own", sink=wrapper)
    wrapper._sinks = (*wrapper._sinks, late)

    assert _lifecycle.releasable(late, owner=wrapper) is False, (
        "the record was walked before this child was added, and a wrapper the library *does* "
        "hold may not assume an unrecorded member is this process's"
    )
    assert _lifecycle.releasable(late) is True, (
        "asked with no wrapper it is the caller's own object, which is the distinction the "
        "owner parameter exists to draw -- the same sink answers differently by who is asking"
    )

    wrapper.close()
    assert late.closed == 0, "so through the wrapper it leaks rather than being closed on a guess"


def test_a_wrapper_the_library_never_saw_still_closes_its_children() -> None:
    """The other half of AC-6's default, and the reason it is not a flat "no record refuses".

    `FilteringSink(inner).close()` is a documented public API. Every lifecycle path stamps, so
    an unrecorded graph is one the library was never handed -- the caller's own object, whose
    explicit close must not silently become a no-op.
    """
    inner = RecordingSink()
    FilteringSink(inner).close()
    assert inner.closed == 1

    a, b = RecordingSink(), RecordingSink()
    MultiSink(a, b).close()
    assert (a.closed, b.closed) == (1, 1)


def _configure_without_keeping_a_reference() -> int:
    """Configures a sink and returns only its id, so the library holds the sole references.

    A test that keeps its own local -- or closes over one -- keeps the sink alive by itself and
    proves nothing about the record.

    Args:
      None.

    Returns:
      The configured sink's id.

    Raises:
      None.
    """
    sink = RecordingSink()
    log_foundry.configure(service="own", sink=sink)
    return id(sink)


def test_the_record_holds_a_strong_reference() -> None:
    """AC-7. An id is reusable once its object dies, and a collected sink closes itself.

    The positive control is the point: an identical sink that the record does *not* hold is
    first shown to be collected under the same conditions, so "still alive" is evidence about
    the record rather than about this test accidentally holding a reference.
    """
    import weakref

    uncollected = RecordingSink()
    watcher = weakref.ref(uncollected)
    del uncollected
    gc.collect()
    assert watcher() is None, "the control is collectable, so the assertion below can mean this"

    sink_id = _configure_without_keeping_a_reference()

    def in_child() -> str:
        config._config = config.Config()
        decorator._worker = None
        decorator._orphan_sink = None
        decorator._orphan_closed_sink = None
        gc.collect()
        with _lifecycle._owned_lock:
            record = _lifecycle._owned.get(sink_id)
        held = "-" if record is None else type(record[1]).__name__
        return f"{record is not None},{held}"

    child = run_in_child(in_child)
    assert child.output == "True,RecordingSink", child.output


def test_the_parents_records_are_untouched_by_the_child() -> None:
    """AC-8. Asserted by identity, as SPEC-039 FR-001 AC-3 requires of every fork change."""
    sink = RecordingSink()
    log_foundry.configure(service="own", sink=sink)
    with _lifecycle._owned_lock:
        before = dict(_lifecycle._owned)

    run_in_child(lambda: str(_lifecycle.releasable(sink)))

    with _lifecycle._owned_lock:
        after = dict(_lifecycle._owned)
    assert after == before
    assert all(after[key][1] is before[key][1] for key in before), "the same objects, not copies"


def test_the_blind_spot_is_stated_in_the_module_docstring() -> None:
    """AC-9. The one gap a reader must not have to discover by hitting it."""
    assert _lifecycle.releasable.__doc__ is not None
    text = _lifecycle.releasable.__doc__
    assert "never saw" in text or "never handed" in text
    assert "FR-001 AC-6" in text, "and it says which criterion decided it"


def test_an_orphan_log_performs_no_stamp_walk(monkeypatch: pytest.MonkeyPatch) -> None:
    """AC-10. `_ensure_sink`'s fast path runs once per event and must never stamp.

    Counted rather than reasoned about: a graph walk and a lock acquisition on a per-event path
    is what SPEC-034 FR-003 AC-6 built that single unlocked read to avoid.
    """
    walks = 0
    original = _lifecycle._reachable_sinks

    def counting(root: object) -> list[object]:
        nonlocal walks
        walks += 1
        return original(root)

    log_foundry.configure(service="own", sink=RecordingSink())
    monkeypatch.setattr(_lifecycle, "_reachable_sinks", counting)

    for index in range(25):
        log_foundry.info("orphan", index=index)

    assert walks == 0, "the fast-path return stamped, which puts a walk on every event"


def test_the_lazy_default_is_stamped_and_the_fast_path_is_not() -> None:
    """AC-10's other half: the construction branch is an acquisition and must record one."""
    config._config = config.Config()
    with _lifecycle._owned_lock:
        _lifecycle._owned.clear()

    default = config._ensure_sink()
    assert _lifecycle.releasable(default) is True, "the zero-config default is acquired here"

    with _lifecycle._owned_lock:
        size = len(_lifecycle._owned)
    for _ in range(10):
        config._ensure_sink()
    with _lifecycle._owned_lock:
        assert len(_lifecycle._owned) == size, "the fast path recorded nothing further"


def test_the_stamp_walk_is_bounded_and_its_cost_is_stated() -> None:
    """AC-11. The bound is chosen with the measurement in hand, so the shape is pinned here.

    Unbounded descent enters every event dict a buffering sink holds -- measured 1,109 ms for a
    `MemorySink` with 100k events, and 279 ms with the builtin pre-filter alone. Scanning a
    container one level takes it to ~2 ms. This asserts the *property* that makes it cheap
    rather than a wall-clock number, which would be a measurement of this host: caller data is
    not descended into, and every shipped wrapper shape still resolves.
    """
    buffering = MemorySink()
    buffering.events.extend({"i": index, "fields": {"n": index}} for index in range(5_000))
    wrapped = MultiSink(StdoutSink(), FilteringSink(buffering, min_level="ERROR"))

    reached = _lifecycle._reachable_sinks(wrapped)
    assert {type(found).__name__ for found in reached} == {
        "MultiSink",
        "StdoutSink",
        "FilteringSink",
        "MemorySink",
    }, "every shipped wrapper shape still resolves"

    class Owned(MemorySink):
        def __init__(self) -> None:
            super().__init__()
            self.one_hop = [StructuralSink()]
            self.two_hops = [[StructuralSink()]]

    holder = Owned()
    names = [type(found).__name__ for found in _lifecycle._reachable_sinks(holder)]
    assert names.count("StructuralSink") == 1, (
        "a container is scanned one level and never recursed into: the one-hop sink is "
        "recorded and the two-hop one is not, so it is refused and leaked, never closed"
    )


def test_the_record_lock_is_last_in_the_process_order() -> None:
    """AC-12. Three terms, not the two the criterion names.

    `_get_worker` calls `config._ensure_sink()` while holding `_worker_lock`, and that takes
    `_config_lock` before it can stamp, so the real chain is
    `_worker_lock` -> `_config_lock` -> `_owned_lock` and never the reverse in any pair.

    The order is enforced by what runs *under* the record lock rather than by acquiring locks
    here in sequence, which proves nothing without actually deadlocking. The first draft of this
    test did deadlock: it took `_owned_lock` and then called `releasable`, which takes it again.
    That is the hazard this asserts is absent from the library, and it is a real one because the
    lock is deliberately **not** reentrant -- SPEC-028 chose `Lock` over `RLock` precisely so a
    function re-entering its own critical section fails loudly instead of being hidden.
    """
    import ast
    import pathlib

    source = pathlib.Path(_lifecycle.__file__).read_text(encoding="utf-8")
    under_lock: list[str] = []

    def walk(node: ast.AST, held: bool) -> None:
        for child in ast.iter_child_nodes(node):
            here = held
            if isinstance(child, ast.With) and "_owned_lock" in ast.unparse(
                child.items[0].context_expr
            ):
                here = True
            if here and isinstance(child, ast.Call):
                under_lock.append(ast.unparse(child.func))
            walk(child, here)

    walk(ast.parse(source), False)
    assert set(under_lock) <= {"_owned.get", "_owned.setdefault", "_owned.clear", "id"}, (
        f"new work under the record lock: {sorted(set(under_lock))}. It is the last lock in the "
        "order, so anything here that calls out can only invert it -- and anything that "
        "re-enters deadlocks outright, since it is a Lock and not an RLock."
    )

    assert not isinstance(_lifecycle._owned_lock, type(threading.RLock())), (
        "a non-reentrant Lock is the choice (SPEC-028): an RLock would hide the re-entry above"
    )


# -- FR-002: the refusal at every closer --------------------------------------------------------


def test_the_refusal_holds_at_the_worker_shutdown_close() -> None:
    """AC-2, site 1 of 3. A child's `shutdown()` must not close the inherited object."""
    sink = RecordingSink()
    log_foundry.configure(service="own", sink=sink)

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("in-span")

    def in_child() -> str:
        work()
        log_foundry.shutdown(3.0)
        return f"{sink.closed},{len(sink.events) > 0},{log_foundry.health().retired}"

    child = run_in_child(in_child)
    assert child.output == "0,True,True", child.output


def test_the_refusal_holds_at_the_orphan_exit_close() -> None:
    """AC-2, site 2 of 3. The path a process that never opens a span takes."""
    sink = RecordingSink()
    log_foundry.configure(service="own", sink=sink)

    def in_child() -> str:
        log_foundry.info("orphan")
        decorator._close_orphan_sink()
        return f"{sink.closed},{len(sink.events)}"

    child = run_in_child(in_child)
    assert child.output == "0,1", child.output


def test_the_refusal_holds_at_both_swap_paths() -> None:
    """AC-2, site 3 of 3. Both branches of `_swap_sink`: with a worker and without one."""
    inherited = RecordingSink()
    log_foundry.configure(service="own", sink=inherited)

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("in-span")

    def with_worker() -> str:
        work()
        log_foundry.configure(sink=RecordingSink())
        return str(inherited.closed)

    def without_worker() -> str:
        log_foundry.info("orphan")
        log_foundry.configure(sink=RecordingSink())
        return str(inherited.closed)

    assert run_in_child(with_worker).output == "0"
    assert run_in_child(without_worker).output == "0"


def test_a_child_wrapping_an_inherited_sink_closes_it_zero_times() -> None:
    """AC-3. The wrapper route, which guarding only the three lifecycle sites leaves open.

    The wrapper is the child's own, so it is releasable; the inner is the parent's, so it is
    not. Measured at two closes before the wrappers were guarded -- once through the old wrapper
    on the swap, once through the child's own at exit. The inner is **structural** deliberately:
    an owned one passes against a record that still has FR-001's first hole.
    """
    inner = StructuralSink()
    log_foundry.configure(service="own", sink=MultiSink(StdoutSink(), inner))

    def in_child() -> str:
        log_foundry.info("before")
        own = MultiSink(StdoutSink(), inner)
        log_foundry.configure(sink=own)
        log_foundry.info("after")
        log_foundry.shutdown(3.0)
        return str(inner.closed)

    child = run_in_child(in_child)
    assert child.output == "0", child.output


def test_a_refused_release_moves_no_counter_and_does_not_raise() -> None:
    """AC-4. A skip, not a failure: nothing is lost, retried, or reported as loss."""
    sink = RecordingSink()
    log_foundry.configure(service="own", sink=sink)

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("in-span")

    def in_child() -> str:
        work()
        log_foundry.configure(sink=RecordingSink())
        health = log_foundry.health()
        return f"{health.incomplete_swaps},{health.failed_batches},{health.dropped}"

    child = run_in_child(in_child)
    assert child.output == "0,0,0", child.output


def test_a_releasable_sink_is_closed_exactly_as_before() -> None:
    """AC-7. "Refuses everything" passes every refusal test here and fails this one."""
    first, second = RecordingSink(), RecordingSink()
    log_foundry.configure(service="own", sink=first)
    log_foundry.info("orphan")

    log_foundry.configure(sink=second)
    assert first.closed == 1, "the swapped-out sink is still closed in its own process"

    log_foundry.shutdown(3.0)
    assert second.closed == 1, "and so is the live one at shutdown"


def test_the_wrappers_error_handling_is_unchanged() -> None:
    """AC-9. `MultiSink` still counts an absorbed close failure; the other two still propagate."""

    class Angry(RecordingSink):
        def close(self) -> None:
            """Fails the close, so the caller's handler is the thing under test."""
            raise RuntimeError("no")

    angry, quiet = Angry(), RecordingSink()
    multi = MultiSink(angry, quiet)
    log_foundry.configure(service="own", sink=multi)

    multi.close()
    assert multi.failed == 1, "absorbed and counted, and the rest still closed"
    assert quiet.closed == 1

    inner = Angry()
    wrapper = FilteringSink(inner)
    log_foundry.configure(sink=wrapper)
    with pytest.raises(RuntimeError):
        wrapper.close()


# -- FR-003: the drain is untouched -------------------------------------------------------------


def test_a_child_still_delivers_through_the_sink_it_inherited() -> None:
    """AC-1, AC-2. Only the release is refused; every drain still runs."""
    sink = RecordingSink()
    log_foundry.configure(service="own", sink=sink)

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("child-event")

    def in_child() -> str:
        work()
        log_foundry.configure(sink=RecordingSink())
        messages = [event.get("message") for event in sink.events]
        return f"{'child-event' in messages},{sink.closed}"

    child = run_in_child(in_child)
    assert child.output == "True,0", child.output


def test_flush_in_a_child_is_unchanged() -> None:
    """AC-3. Every outcome, including the reason, is what it would be in any other process."""
    log_foundry.configure(service="own", sink=RecordingSink())

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("in-span")

    def in_child() -> str:
        work()
        result = log_foundry.flush(3.0)
        return f"{bool(result)},{result.reason}"

    child = run_in_child(in_child)
    assert child.output == "True,None", child.output


def test_both_processes_events_survive_exactly_once() -> None:
    """AC-4. The end-to-end promise: a shared sink carries both, and neither closes it early.

    A real socket, because the defect is a protocol goodbye on a shared connection and a
    counting double cannot show that the parent's transport still works afterwards.
    """
    server, client = socket.socketpair()
    try:

        class SocketSink:
            """Writes one line per event and shuts the connection down on close."""

            def __init__(self, conn: socket.socket) -> None:
                self._conn = conn

            def emit(self, batch: list[dict[str, object]]) -> None:
                """Sends one line per event."""
                for event in batch:
                    self._conn.sendall(f"{event.get('message')}\n".encode())

            def close(self) -> None:
                """The destructive step: a real goodbye the peer observes."""
                self._conn.shutdown(socket.SHUT_WR)

        log_foundry.configure(service="own", sink=SocketSink(client))

        def in_child() -> str:
            log_foundry.info("from-child")
            log_foundry.shutdown(3.0)
            return "done"

        assert run_in_child(in_child).output == "done"

        log_foundry.info("from-parent")
        client.shutdown(socket.SHUT_WR)
        received = b""
        server.settimeout(5.0)
        while chunk := server.recv(4096):
            received += chunk
        lines = received.decode().split()
        assert lines.count("from-child") == 1, "the child's event arrived"
        assert lines.count("from-parent") == 1, (
            "and the parent could still write after the child shut down -- a closed connection "
            "would have made this send fail or be lost"
        )
    finally:
        server.close()
        client.close()


def test_the_stamp_is_taken_before_the_sink_is_published(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-001's ordering property, observed at the instant of publication.

    `configure()` assigns `_config.sink` through `_rebind` and only then installs it, so a stamp
    written after that leaves a window in which a concurrent orphan `info()` reaches an
    unrecorded sink. The check is deterministic rather than a race: `_rebind` **is** the moment
    of publication, so asking the question from inside it observes the window directly.

    Written first as two threads racing 200 `configure()` calls, which is how not to do it -- the
    reader was starved to a single observation out of fifty, so `all(seen)` was passing on an
    empty-ish sample. Only its own "did you observe anything" precondition caught that.
    """
    observed: list[bool] = []
    original = config._rebind

    def spy(**changed: object) -> None:
        published = changed.get("sink")
        if published is not None:
            observed.append(_lifecycle.releasable(published))
        original(**changed)

    monkeypatch.setattr(config, "_rebind", spy)
    log_foundry.configure(service="own", sink=RecordingSink())

    assert observed == [True], (
        "the sink was already recorded at the instant it became visible in the config"
    )


def test_the_fork_walk_does_not_reach_a_superseded_sink() -> None:
    """The record pins every sink ever acquired, and `_fork` must not treat that as live state.

    Measured before `_FORK_SKIP` existed: a child announced a buffer discard for a sink two
    `configure()` calls out of date, and a `FileSink` there would be reopened on every fork for
    the life of the process.
    """
    hooked: list[str] = []

    class Hooked(RecordingSink):
        def reacquire_after_fork(self) -> None:
            """Records that the walk reached this sink in the child."""
            hooked.append("yes")

    superseded = Hooked()
    log_foundry.configure(service="own", sink=superseded)
    log_foundry.configure(sink=RecordingSink())

    assert "_owned" in _lifecycle._FORK_SKIP
    reached = _lifecycle._reachable_sinks(_lifecycle)
    assert superseded not in reached, "the record is bookkeeping, not a graph to repair"
    assert os.getpid() > 0
