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
import pathlib
import socket
import threading
import time
from typing import TYPE_CHECKING

import pytest

import log_foundry
from log_foundry import _fork, _lifecycle, config, decorator
from log_foundry.sinks.filtering import FilteringSink
from log_foundry.sinks.memory import MemorySink
from log_foundry.sinks.multi import MultiSink
from log_foundry.sinks.stdout import StdoutSink
from log_foundry.sinks.transform import TransformSink
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
    _lifecycle._marking_failed = False
    yield
    with _lifecycle._owned_lock:
        _lifecycle._owned.clear()
    _lifecycle._marking_failed = False


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


def _logstash_wrapping(inner: object) -> object:
    """Builds a `LogstashSink` whose HTTP backend is the given sink.

    The class picks its backend from the URL scheme, so the child is substituted afterwards --
    what is under test is that `close()` forwards its own identity, not how it was constructed.
    """
    from log_foundry.sinks.logstash import LogstashSink

    wrapper = LogstashSink(url="http://example.invalid/_bulk")
    wrapper._http = inner  # type: ignore[assignment]
    return wrapper


def _sentry_wrapping(inner: object) -> object:
    """Builds a `SentrySink` whose HTTP fallback is the given sink, for the same reason."""
    from log_foundry.sinks.sentry import SentrySink

    wrapper = SentrySink(dsn="https://key@example.invalid/1")
    wrapper._http = inner  # type: ignore[assignment]
    return wrapper


@pytest.mark.parametrize(
    "build",
    [
        pytest.param(lambda inner: MultiSink(inner), id="MultiSink"),
        pytest.param(lambda inner: FilteringSink(inner), id="FilteringSink"),
        pytest.param(lambda inner: TransformSink(inner, lambda event: event), id="TransformSink"),
        pytest.param(_logstash_wrapping, id="LogstashSink"),
        pytest.param(_sentry_wrapping, id="SentrySink"),
    ],
)
def test_every_wrapper_forwards_its_own_identity_as_the_owner(build: object) -> None:
    """FR-001 AC-6 is decided by `owner=`, and it is hand-written at five sites.

    Four of the five were caught being tested by nothing: dropping `owner=self` from
    `FilteringSink`, `TransformSink`, `LogstashSink` or `SentrySink` survived the entire suite.
    That is this repo's recurring shape -- "a roster in prose is not a roster the tests check"
    -- so each wrapper is asserted individually rather than through the one that happened to
    have coverage.

    The assertion is on the *argument*, observed at the helper, because the behavioural
    consequence needs a child the wrapper holds but the library has no record of, and building
    that per wrapper would test the record rather than the forwarding.
    """
    seen: list[object] = []
    inner = RecordingSink()
    wrapper = build(inner)  # type: ignore[operator]
    original = _lifecycle.release

    def spy(sink: object, *, detached: bool = False, owner: object = None) -> object:
        seen.append(owner)
        return original(sink, detached=detached, owner=owner)  # type: ignore[arg-type]

    _lifecycle.release = spy  # type: ignore[assignment]
    try:
        wrapper.close()
    finally:
        _lifecycle.release = original  # type: ignore[assignment]

    assert seen, "the wrapper routed its child's close through the release helper at all"
    assert all(owner is wrapper for owner in seen), (
        f"{type(wrapper).__name__} must pass itself as owner=, or an unrecorded child of a "
        f"wrapper the library holds is closed on a guess (FR-001 AC-6); got {seen}"
    )


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


class ThirdPartyWrapper:
    """A wrapper the library does not own, holding a sink the library therefore cannot see.

    `README.md`'s documented shape. The bounded stamp walk records *this object* and then
    declines to descend into it, so whatever it holds is invisible to the parent's record --
    which is what made the child able to claim it.
    """

    def __init__(self, inner: object) -> None:
        self._inner = inner

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Forwards the batch."""
        self._inner.emit(batch)  # type: ignore[attr-defined]

    def close(self) -> None:
        """Forwards the close."""
        self._inner.close()  # type: ignore[attr-defined]


class BuriedHolder(MemorySink):
    """A library-owned sink holding another two container hops down, past the AC-11 bound."""

    def __init__(self, inner: object) -> None:
        super().__init__()
        self.buried = [[inner]]


@pytest.mark.parametrize(
    "hide",
    [
        pytest.param(lambda inner: MultiSink(ThirdPartyWrapper(inner)), id="third-party-wrapper"),
        pytest.param(lambda inner: BuriedHolder(inner), id="two-container-hops"),
    ],
)
def test_a_child_cannot_claim_a_sink_the_parent_never_recorded(hide: object) -> None:
    """The claiming-side hole, and the reason `_mark_inherited` exists (FR-001).

    Write-once defends a record that **already exists**, so where the parent's bounded walk
    recorded nothing there was nothing to defend: a child re-wrapping the inherited object in a
    `MultiSink` of its own reached it, stamped it with its own pid, and closed it. Measured on a
    real socket -- `claimed=True parent_conn_closed=1`, and the parent's next write raised
    `OSError: Socket is not connected`.

    Both shapes here are ones the parent provably cannot record: one hidden behind a wrapper the
    library may not descend into, one below the container bound AC-11 chose. The fix is not to
    record them -- it cannot -- but to make *unrecorded* terminal in a child, which is what the
    fork-time marking walk does.
    """
    conn = RecordingSink()
    log_foundry.configure(service="own", sink=hide(conn))  # type: ignore[operator]

    with _lifecycle._owned_lock:
        assert id(conn) not in _lifecycle._owned, (
            "the precondition: the parent's walk genuinely cannot see this sink, so the test "
            "exercises the claiming hole rather than the ordinary recorded path"
        )

    def in_child() -> str:
        log_foundry.configure(sink=MultiSink(conn))
        log_foundry.info("child event")
        log_foundry.shutdown(3.0)
        return f"{_lifecycle.releasable(conn)},{conn.closed}"

    child = run_in_child(in_child)
    assert child.output == "False,0", child.output


def test_the_marking_walk_leaves_a_childs_own_sinks_alone() -> None:
    """Marking everything inherited must not become marking everything.

    The counterpart to the test above: "refuse in a child" is trivially safe and useless, and
    FR-001 AC-3 requires a child's own sink to close normally. Asserted after the marking walk
    has run, which is the moment it could over-reach.
    """
    log_foundry.configure(service="own", sink=RecordingSink())

    def in_child() -> str:
        own = RecordingSink()
        log_foundry.configure(sink=own)
        log_foundry.info("child event")
        log_foundry.shutdown(3.0)
        return f"{_lifecycle.releasable(own)},{own.closed}"

    child = run_in_child(in_child)
    assert child.output == "True,1", child.output


def test_a_marking_walk_that_fails_refuses_everything_unrecorded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the walk could not finish, an unrecorded sink may be one it missed.

    Refusing then costs a leaked handle, which is the direction FR-001 requires.

    **The failure is induced through the real walk**, not by assigning the flag. Written the
    other way first, and it only proved that `releasable` reads a global — mutating the handler
    so it never sets the flag survived. Driving `_mark_inherited` with a raising root resolver
    exercises the guard, the flag, and the refusal together.
    """
    unrecorded = RecordingSink()
    assert _lifecycle.releasable(unrecorded) is True, "the baseline: unrecorded is the caller's"

    def exploding_roots() -> list[object]:
        raise RuntimeError("the roots cannot be resolved")

    monkeypatch.setattr(_lifecycle, "_inheritance_roots", exploding_roots)
    try:
        _lifecycle._mark_inherited()
        assert _lifecycle._marking_failed is True, "the guard caught it and withdrew the default"
        assert _lifecycle.releasable(unrecorded) is False
        assert _lifecycle.release(unrecorded) is None
        assert unrecorded.closed == 0, "and the refusal is a skip, not a raise"
    finally:
        _lifecycle._marking_failed = False


def test_a_superseded_wrapper_still_shields_the_transport_beneath_it() -> None:
    """`_owned.values()` is the load-bearing root, and nothing else reaches this sink.

    A wrapper one `configure()` has replaced is no live delivery target, so none of the four
    live handles finds it -- but the transport inside it is still the parent's. Dropping the
    recorded-sinks root leaves this a destructive close with the whole suite green, which is
    why it is asserted separately from the live-target roots.
    """
    conn = RecordingSink()
    log_foundry.configure(service="own", sink=MultiSink(ThirdPartyWrapper(conn)))
    log_foundry.configure(sink=StdoutSink())

    live = {id(found) for found in _lifecycle._inheritance_roots()}
    assert id(conn) not in live, "the precondition: it is not reachable as a live target"

    def in_child() -> str:
        log_foundry.configure(sink=MultiSink(conn))
        log_foundry.info("child event")
        log_foundry.shutdown(3.0)
        return f"{_lifecycle.releasable(conn)},{conn.closed}"

    child = run_in_child(in_child)
    assert child.output == "False,0", child.output


@pytest.mark.parametrize(
    "build",
    [
        pytest.param(lambda inner: FilteringSink(inner), id="FilteringSink"),
        pytest.param(lambda inner: TransformSink(inner, lambda event: event), id="TransformSink"),
        pytest.param(lambda inner: MultiSink(inner), id="MultiSink"),
    ],
)
def test_a_sink_subclassing_a_builtin_container_is_still_recorded(build: object) -> None:
    """Sink-shaped must beat container-shaped, or a wrapper stops closing its child.

    `class MySink(dict)` satisfies both tests. With the container branch first it was read as a
    bag of members and never recorded, so `releasable(inner, owner=wrapper)` took the
    "wrapper recorded, child not" arm and refused -- `FilteringSink(MySink()).close()` closed
    nothing, silently, with no fork involved. That is a regression against the unguarded release
    this replaced, and it contradicts the public-API promise beside it.

    `MultiSink` is here as the control: it escaped the bug by accident of position, its children
    arriving through the container branch, so a fix that only reordered *its* path would leave
    the other two broken and this parametrisation green.
    """

    class DictSink(dict):  # type: ignore[type-arg]
        def __init__(self) -> None:
            super().__init__()
            self.closed = 0

        def emit(self, batch: list[dict[str, object]]) -> None:
            """Keeps nothing; the close is what is under test."""

        def close(self) -> None:
            """Counts the close."""
            self.closed += 1

    inner = DictSink()
    wrapper = build(inner)  # type: ignore[operator]
    log_foundry.configure(service="own", sink=wrapper)

    with _lifecycle._owned_lock:
        assert id(inner) in _lifecycle._owned, "a sink that is also a container is still a sink"

    wrapper.close()
    assert inner.closed == 1, "and its wrapper still closes it"


def test_the_marking_walk_does_not_escape_through_a_module_reference() -> None:
    """A sink holding a module must not send the walk through the whole interpreter.

    Measured before the skip: 24,021 objects visited and 29 marked, among them nine class
    objects and a `logging` handler -- objects the library has no business pinning for the life
    of the child.

    **The real damage was not the retention.** The escape sometimes marked the very sinks the
    application-state residual says are unreachable, so the same scenario refused or destroyed
    depending on whether the parent's sink class happened to keep a module reference. A residual
    that holds by luck cannot be documented honestly, and §13 has to record this one.
    """
    import json

    class ModuleHolding(MemorySink):
        def __init__(self) -> None:
            super().__init__()
            self.helper = json

    holder = ModuleHolding()
    log_foundry.configure(service="own", sink=holder)

    def in_child() -> str:
        with _lifecycle._owned_lock:
            marked = sum(1 for pid, _ref in _lifecycle._owned.values() if pid == _lifecycle._FOREIGN)
        return str(marked)

    child = run_in_child(in_child)
    assert child.output is not None
    assert int(child.output) <= 4, (
        f"the walk marked {child.output} objects from a sink holding one module -- it escaped "
        "into the import graph, which is how the residual became conditional"
    )


def test_a_runaway_container_trips_the_ceiling_and_refuses_everything() -> None:
    """The walk is bounded, and tripping the bound degrades the safe way.

    SPEC-039 declined to bound its repair walk because an unfound lock is a hang with no safe
    degradation. This walk has one: `_marking_failed` means "did not finish, trust nothing
    unrecorded", so a trip costs a leaked handle rather than a destructive close. The exposure
    is also wider -- a `list` subclass with a non-terminating `__iter__` reachable through any
    third-party object took a child to 5.7 GB RSS in nine minutes, unkillable by its parent.
    """

    class Endless(list):  # type: ignore[type-arg]
        def __iter__(self):  # type: ignore[no-untyped-def]
            """Never terminates, which is the shape the ceiling exists for."""
            while True:
                yield object()

    class Holder(MemorySink):
        def __init__(self) -> None:
            super().__init__()
            self.runaway = Endless()

    unrecorded = RecordingSink()

    started = time.monotonic()
    log_foundry.configure(service="own", sink=Holder())
    configured = time.monotonic() - started

    assert configured < 10.0, (
        "the *parent's* stamp walk is bounded too. It reads containers through the same helper, "
        "and before that it called `_fork._container_children`, whose `list(container)` never "
        "returns here -- so `configure()` itself hung, with no fork involved"
    )

    _lifecycle._mark_inherited()
    assert _lifecycle._marking_failed is True, "the ceiling tripped rather than the walk running on"
    assert _lifecycle.releasable(unrecorded) is False, "and it degraded toward refusing"

    # Driven in-process rather than through `run_in_child`: a real child never reaches this code,
    # because `_fork._reinit_primitives` reads the same container with an unbounded
    # `list(container)` and hangs first. That is SPEC-039's walk and its recorded hazard, not
    # this spec's to change -- measured, the child was killed at its 30 s watchdog. What is
    # asserted here is the bound on the two walks SPEC-042 owns; §13 records the other.


def test_the_marking_handler_runs_before_the_worker_rebuild() -> None:
    """FR-005 AC-7's ordering: the marks are in place before any handler that may release.

    Robust rather than incidental -- `decorator` imports `_lifecycle`, so `_lifecycle`'s module
    body and its registration always complete first, whatever is imported first at the top. The
    docstring claimed a test pinned this and none did.
    """
    handlers = [handler.__name__ for handler in _fork._child_handlers]
    assert "_mark_inherited" in handlers, "the marking handler is registered at all"
    assert "_rebuild_worker_after_fork" in handlers
    assert handlers.index("_mark_inherited") < handlers.index("_rebuild_worker_after_fork"), (
        "a handler that reaches a release path must not run before the marks exist"
    )


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
    permitted = {
        "_owned.get",
        "_owned.setdefault",
        "_owned.clear",
        "_owned.values",
        "roots.extend",
        "id",
    }
    assert set(under_lock) <= permitted, (
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

    Then written a second time asking `releasable(published)`, which was worse: `releasable`
    answers `True` for an unrecorded sink by design, so the probe could not tell "stamped" from
    "never stamped" and moving the stamp *after* `_rebind` survived the whole suite. The
    question has to be about the **record**, which is the thing being ordered.
    """
    observed: list[bool] = []
    original = config._rebind

    def spy(**changed: object) -> None:
        published = changed.get("sink")
        if published is not None:
            with _lifecycle._owned_lock:
                observed.append(id(published) in _lifecycle._owned)
        original(**changed)

    monkeypatch.setattr(config, "_rebind", spy)
    log_foundry.configure(service="own", sink=RecordingSink())

    assert observed == [True], (
        "the sink already carried a record at the instant it became visible in the config"
    )


def test_the_fork_repair_walk_does_not_reach_a_superseded_sink() -> None:
    """The record pins every sink ever acquired, and `_fork` must not treat that as live state.

    Measured before `_FORK_SKIP` existed: a child announced a buffer discard for a sink two
    `configure()` calls out of date, and a `FileSink` there would be reopened on every fork for
    the life of the process.

    This drives `_fork._reinit_primitives` -- the **repair** walk, the only one that consults
    `_FORK_SKIP`. A first version called `_lifecycle._reachable_sinks(_lifecycle)` instead, which
    is the *ownership* walk and cannot even enter a module (`_is_owned` is `False` for a
    `ModuleType`, so it returns `[]` for any module). That assertion was true for every possible
    input and disabling `_FORK_SKIP` left it passing.
    """
    hooked: list[str] = []

    class Hooked(MemorySink):
        """Subclasses a **library** sink on purpose: `_fork`'s walk descends by ownership.

        Written first against a plain test double, whose class is defined in this module -- so
        `_fork._is_owned` was `False`, the walk declined to enter it, and the assertion below
        held for a reason that had nothing to do with `_FORK_SKIP`. Disabling the skip left it
        passing.
        """

        def reacquire_after_fork(self) -> None:
            """The hook `_fork` collects; named as it is on `main` until FR-005 renames it."""
            hooked.append(type(self).__name__)

    superseded = Hooked()
    log_foundry.configure(service="own", sink=superseded)
    log_foundry.configure(sink=RecordingSink())

    assert "_owned" in _lifecycle._FORK_SKIP
    with _lifecycle._owned_lock:
        assert id(superseded) in _lifecycle._owned, (
            "the record still pins it, which is the precondition that makes this test mean "
            "something -- without the pin there would be nothing for the walk to over-reach to"
        )

    collected = _fork._reinit_primitives()
    assert superseded not in collected, (
        "the repair walk reached a sink two configure() calls out of date and would have called "
        "its fork hook"
    )
    assert hooked == [], "and it did not call the hook either"


# -- FR-005: the hook claims the transport; FR-004: the state is reported ----------------------


def test_the_hook_is_named_for_the_claim_it_makes() -> None:
    """AC-1. The old name described one consequence of the step rather than the step.

    A sink that only dropped a buffer without re-opening would satisfy a name describing only
    the discard -- which this member carried until now -- while leaving the child holding the
    parent's descriptor, making a destructive close look safe. The rename is free now and will not be later: the
    member has never been in a stable release.
    """
    from log_foundry.sinks import base

    assert base.Sink.__doc__ is not None
    documented = base.Sink.__doc__
    assert "reacquire_after_fork" in documented
    assert "claim the transport as this process's own" in documented, "both halves are stated"
    assert _fork._REACQUIRE_HOOK == "reacquire_after_fork"


def test_the_old_hook_name_is_gone_from_src_and_tests() -> None:
    """AC-1. Nowhere in the shipped code or its tests; the completed spec keeps it (AC-2).

    The name is assembled rather than written, or this file matches itself and the scan can
    never pass -- which is how it failed first.
    """
    retired = "discard_buffered" + "_after_fork"
    root = pathlib.Path(_lifecycle.__file__).parent.parent.parent
    scanned = 0
    for area in ("src", "tests"):
        for path in sorted((root / area).rglob("*.py")):
            assert retired not in path.read_text(encoding="utf-8"), path
            scanned += 1
    assert scanned > 50, "the scan actually reached the tree rather than an empty directory"


def test_a_reacquiring_sink_is_releasable_in_the_child_and_one_without_the_hook_is_not(
    tmp_path: pathlib.Path,
) -> None:
    """AC-3. Both directions, and both scoped to a sink the child *inherited*.

    A sink the child constructed itself is releasable with no hook at all (FR-001 AC-3), so an
    unscoped reading of this criterion asserts the opposite of the truth.
    """
    from log_foundry.sinks.file import FileSink

    reacquiring = FileSink(str(tmp_path / "child.log"))
    plain = RecordingSink()
    log_foundry.configure(service="own", sink=MultiSink(reacquiring, plain))

    def in_child() -> str:
        return f"{_lifecycle.releasable(reacquiring)},{_lifecycle.releasable(plain)}"

    child = run_in_child(in_child)
    assert child.output == "True,False", child.output


def test_a_hook_that_raises_leaves_the_sink_unreleasable(tmp_path: pathlib.Path) -> None:
    """AC-4. A failed re-acquisition is not a claim, so it must not make a close look safe."""
    from log_foundry.sinks.file import FileSink

    class Angry(FileSink):
        def reacquire_after_fork(self) -> None:
            """Fails the re-acquisition; SPEC-039 absorbs it, and this pins what it means."""
            raise RuntimeError("cannot reopen")

    angry = Angry(str(tmp_path / "angry.log"))
    log_foundry.configure(service="own", sink=angry)

    def in_child() -> str:
        return str(_lifecycle.releasable(angry))

    child = run_in_child(in_child)
    assert child.output == "False", child.output


def test_reacquisition_restamps_the_sink_and_nothing_above_it(tmp_path: pathlib.Path) -> None:
    """AC-8. The wrapper keeps the parent's mark, which is a stated leak rather than a bug.

    Only the children implement the hook, so a `MultiSink` of two `FileSink`s leaves the
    re-acquired children reachable only through a wrapper nothing will release. Nothing is lost
    -- `FileSink.emit` flushes at the end of every batch -- but AC-6's descriptor test uses a
    bare `FileSink` and passes while this stands, so it is asserted rather than discovered.
    """
    from log_foundry.sinks.file import FileSink

    first = FileSink(str(tmp_path / "a.log"))
    second = FileSink(str(tmp_path / "b.log"))
    wrapper = MultiSink(first, second)
    log_foundry.configure(service="own", sink=wrapper)

    def in_child() -> str:
        return (
            f"{_lifecycle.releasable(first)},"
            f"{_lifecycle.releasable(second)},"
            f"{_lifecycle.releasable(wrapper)}"
        )

    child = run_in_child(in_child)
    assert child.output == "True,True,False", child.output


def test_the_child_holds_its_own_descriptor(tmp_path: pathlib.Path) -> None:
    """AC-6. `FileSink` needs no behavioural change -- the rename makes its claim explicit."""
    from log_foundry.sinks.file import FileSink

    path = tmp_path / "descriptors.log"
    sink = FileSink(str(path))
    log_foundry.configure(service="own", sink=sink)
    parent_fd = sink._stream.fileno()

    def in_child() -> str:
        return str(sink._stream.fileno())

    child = run_in_child(in_child)
    assert child.output is not None
    assert int(child.output) != parent_fd, (
        "the child reopened, which is the claim the rename makes explicit -- identical "
        "descriptors would mean it still holds the parent's"
    )


def test_health_reports_an_inherited_sink() -> None:
    """FR-004 AC-2. False before a fork, True in a child, False again once it installs its own."""
    log_foundry.configure(service="own", sink=RecordingSink())
    assert log_foundry.health().inherited_sink is False

    def inherited() -> str:
        return str(log_foundry.health().inherited_sink)

    def replaced() -> str:
        log_foundry.configure(sink=RecordingSink())
        return str(log_foundry.health().inherited_sink)

    assert run_in_child(inherited).output == "True"
    assert run_in_child(replaced).output == "False"


def test_health_answers_the_inherited_question_with_no_worker() -> None:
    """FR-004 AC-3. Synthesized as `retired` already is; no worker is created to answer it."""
    log_foundry.configure(service="own", sink=RecordingSink())
    log_foundry.info("orphan only")
    assert decorator._worker is None, "the precondition: nothing built a worker"

    def in_child() -> str:
        log_foundry.info("child orphan")
        return f"{log_foundry.health().inherited_sink},{decorator._worker is None}"

    child = run_in_child(in_child)
    assert child.output == "True,True", child.output


def test_the_inherited_field_describes_one_sink_not_the_graph() -> None:
    """FR-004 AC-1. The opposite reading is the natural one, so it is pinned.

    A child that wraps an inherited sink in a `MultiSink` of its own is delivering to a sink it
    *may* release, so this reads `False` -- while the wrapper's child stays refused. Reporting
    the graph would make the field true whenever anything beneath it was inherited, which is a
    different and much noisier signal.
    """
    inner = RecordingSink()
    log_foundry.configure(service="own", sink=inner)

    def in_child() -> str:
        log_foundry.configure(sink=MultiSink(inner))
        return f"{log_foundry.health().inherited_sink},{_lifecycle.releasable(inner)}"

    child = run_in_child(in_child)
    assert child.output == "False,False", child.output


def test_health_reports_an_inherited_sink_through_the_worker_too() -> None:
    """FR-004 AC-1's first candidate: the worker's sink, which has its own `health()` path.

    Every other test here takes the orphan branch, so `_worker_health`'s synthesis covered them
    all and `Worker.health` was untested -- mutating its term to a constant `False` passed 52 of
    52. A span is opened deliberately so a worker exists and answers for itself.
    """
    sink = RecordingSink()
    log_foundry.configure(service="own", sink=sink)

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("in-span")

    work()
    assert decorator._worker is not None, "the precondition: this test is about the worker path"
    assert log_foundry.health().inherited_sink is False

    def in_child() -> str:
        work()
        return f"{decorator._worker is not None},{log_foundry.health().inherited_sink}"

    child = run_in_child(in_child)
    assert child.output == "True,True", child.output
