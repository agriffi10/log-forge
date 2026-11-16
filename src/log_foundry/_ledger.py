"""The caller-side half of the event ledger: one cell per thread, retired when it ends (SPEC-088)."""

from __future__ import annotations

import threading
import weakref
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

type Snapshot = tuple[_Cell, tuple[_Cell, ...]]

_COUNTS = ("span", "handed", "orphan", "orphan_open", "delivered", "skipped", "sink_lost")


class _Cell:
    """One thread's caller-side counts, written only by that thread (SPEC-088 FR-005).

    The single-writer rule is what keeps each count exact without a lock on the accept path: a
    bare ``+=`` shared between threads is only atomic by an interpreter accident that
    ``docs/best-practices/python/python.md`` §13 forbids relying on, while an increment no other
    thread performs needs no atomicity at all. A reader sums the cells, so it sees each count as
    its owner last wrote it.

    ``dead`` is written once, by the finalizer that runs when the owner's thread-local storage is
    freed, and only after the owner has made its last write. A cell published as a registry's
    ``base`` is never written again: a fold builds a new one.

    Attributes:
      span: Events credited inside a span: ``span.start``, ``span.end`` and in-span logs.
      handed: Events this thread handed to ``Worker.submit``, credited after it returned.
      orphan: Orphan logs credited.
      orphan_open: Orphan log calls in progress on this thread, the orphan path's gauge.
      delivered: Orphan events whose ``emit`` returned or whose run answered ``ACCEPTED``.
      skipped: Orphan events a ``deliver`` sink answered ``SKIPPED``.
      sink_lost: Orphan events a settled run ended neither ``ACCEPTED`` nor ``SKIPPED``.
      dead: Whether the owner's thread has ended, so the next fold may retire the cell.
    """

    __slots__ = (*_COUNTS, "dead")

    def __init__(self) -> None:
        """Starts every count at zero.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.span = 0
        self.handed = 0
        self.orphan = 0
        self.orphan_open = 0
        self.delivered = 0
        self.skipped = 0
        self.sink_lost = 0
        self.dead = False


class _Sentinel:
    """An object only a thread's local storage holds, so its collection marks the thread ended.

    ``Thread.is_alive()`` cannot be the test: a thread started through ``_thread`` is seen by
    ``threading`` as a ``_DummyThread``, which reports itself alive forever, so its cell would
    never be retired (measured on the spec's prototype: 2,000 such threads left 2,000 cells).
    A thread's local storage is freed when the thread ends however it was started.
    """

    __slots__ = ("__weakref__",)


class _Registry:
    """Every live cell and the folded counts of the dead ones, published as one pair.

    **The registry is one immutable pair, replaced by one rebinding** (SPEC-088 review, M1). The
    registration lock is re-entrant because the thread holding it can run library code again
    before it lets go: a ``__del__`` the collector runs at an allocation inside the critical
    section, or a signal handler, can log on a thread that has no cell yet, and with a plain
    ``Lock`` that second registration waited on its own thread forever (reproduced in the plan
    review's ``premise_3b``). Re-entry is only safe if the outer call cannot overwrite what the
    inner one published, so every writer computes its new pair from the one it read and installs
    it only while that pair is still the current one, retrying otherwise; under the lock only
    re-entry on the same thread can change it in between. A reader walks a pair it read once, so
    a fold or a registration landing during the walk changes nothing it is summing.

    Attributes:
      lock: The registration lock, taken once per thread on its first credit, and by a fold.
      pair: The folded counts and the live cells, as one tuple.
    """

    __slots__ = ("lock", "pair")

    def __init__(self, span_base: int = 0) -> None:
        """Builds an empty registry whose folded span credits start at ``span_base``.

        Args:
          span_base: Span credits the registry starts holding: a forked child's open-span
            events, which it holds without having credited them (FR-005).

        Returns:
          None.

        Raises:
          None.
        """
        base = _Cell()
        base.span = span_base
        self.lock = threading.RLock()
        self.pair: Snapshot = (base, ())


_registry = _Registry()
"""The process's registry. Its pair is replaced whole, never mutated in place."""

_here = threading.local()
"""This thread's cell and its sentinel. Rebound by :func:`restart`, so every thread re-registers."""

_between: Callable[[str, int], None] | None = None
"""A test seam: when set, called with the pass's name and the cell's index between cell reads.

It exists so a test can hold a snapshot part-way through a pass (SPEC-088 FR-003's fourth
criterion inserts a no-op call there; the read-order tests run another thread's call there). It is
read once per pass, so leaving it ``None`` costs a reading one load per pass.
"""


def _mark_dead(cell: _Cell) -> None:
    """Marks a cell whose thread has ended, for the next fold to retire.

    It runs on whatever thread frees the sentinel, usually the ending one, at any point, so it
    takes no lock and writes one flag the owner never writes.

    Args:
      cell: The ended thread's cell.

    Returns:
      None.

    Raises:
      None.
    """
    cell.dead = True


def _folded(state: Snapshot) -> Snapshot:
    """Returns ``state`` with every dead cell's counts added into a new base and the cell dropped.

    Args:
      state: A published pair.

    Returns:
      ``state`` itself when no cell is dead, otherwise a new pair with the same totals.

    Raises:
      None.
    """
    base, cells = state
    if not any(one.dead for one in cells):
        return state
    merged = _Cell()
    live: list[_Cell] = []
    for one in (base, *cells):
        if one is not base and not one.dead:
            live.append(one)
            continue
        for name in _COUNTS:
            setattr(merged, name, getattr(merged, name) + getattr(one, name))
    return merged, tuple(live)


def _register() -> _Cell:
    """Gives the calling thread a cell, retiring dead cells in the same step.

    The cell and a sentinel go into this thread's local storage, and a finalizer on the sentinel
    marks the cell dead once that storage is freed. The finalizer's ``atexit`` is switched off on
    the object it returns: passed to ``weakref.finalize`` as a keyword, it would be handed to the
    callback instead (SPEC-088 review, m1).

    Args:
      None.

    Returns:
      The new cell.

    Raises:
      Exception: Whatever allocating the cell or its finalizer raises; :func:`cell` absorbs it.
    """
    registry = _registry
    fresh = _Cell()
    with registry.lock:
        while True:
            seen = registry.pair
            base, cells = _folded(seen)
            published = (base, (*cells, fresh))
            if registry.pair is seen:
                registry.pair = published
                break
        sentinel = _Sentinel()
        finalizer = weakref.finalize(sentinel, _mark_dead, fresh)
        finalizer.atexit = False
        here = _here
        here.cell = fresh
        here.sentinel = sentinel
    return fresh


def cell() -> _Cell:
    """Returns the calling thread's cell, registering one on the thread's first credit.

    This is the accept path's one lookup, and after the first call on a thread it takes no lock.
    It never raises: a registration that fails hands back a cell no registry holds, so that
    call's counts are lost to the ledger rather than costing the caller its call, and the gap
    reads as a remainder below zero, which ``unaccounted`` reports, rather than as a balance.

    Args:
      None.

    Returns:
      A cell only this thread writes.

    Raises:
      None.
    """
    try:
        found: _Cell = _here.cell
        return found
    except AttributeError:
        pass
    try:
        return _register()
    except Exception:
        return _Cell()


def snapshot() -> Snapshot:
    """Folds the dead cells and returns the published pair, under the registration lock.

    Each pass of a reading takes its own snapshot, so a cell registered after one pass began is
    still read by the passes after it: a pass that reads a hand-over can then always find, in a
    later pass, the credit it debits, whichever thread wrote it.

    Args:
      None.

    Returns:
      The folded counts and the live cells.

    Raises:
      None.
    """
    registry = _registry
    with registry.lock:
        while True:
            seen = registry.pair
            folded = _folded(seen)
            if folded is seen:
                return seen
            if registry.pair is seen:
                registry.pair = folded
                return folded


def _sum(name: str, field: str) -> int:
    """Sums one count over a fresh snapshot, calling the test seam between cells.

    Args:
      name: The pass's name, handed to the seam.
      field: The count summed.

    Returns:
      The total, the folded base included.

    Raises:
      None.
    """
    base, cells = snapshot()
    step = _between
    total: int = getattr(base, field)
    for index, one in enumerate(cells):
        if step is not None:
            step(name, index)
        total += getattr(one, field)
    return total


def handed_unlocked() -> int:
    """Returns the hand-overs credited so far, read without the lock, for a worker's build.

    ``Worker.__init__`` runs under ``Runtime._lock``, and taking the registration lock there
    nested it inside that lock, while a ``__del__`` collected under the registration lock that
    calls a lifecycle API takes the two in the opposite order (diff review frame 1). Nothing here
    needs the lock: the pair is published by one rebinding and never mutated, so one load of it
    is a consistent snapshot, and a fold keeps every total, so summing the dead cells it still
    holds gives the same count a folded pair would. The count is exact as of that load, which is
    all the build's record needs: every later reading's hand-overs are at least it.

    Args:
      None.

    Returns:
      The events handed to ``Worker.submit``, over every cell, dead or not.

    Raises:
      None.
    """
    base, cells = _registry.pair
    return base.handed + sum(one.handed for one in cells)


def handed() -> int:
    """Returns the hand-overs credited so far, every thread's (pass 1 of a reading).

    Args:
      None.

    Returns:
      The events handed to ``Worker.submit``.

    Raises:
      None.
    """
    return _sum("handed", "handed")


def spans() -> int:
    """Returns the span credits so far, every thread's (pass 2 of a reading).

    Args:
      None.

    Returns:
      The events credited inside a span.

    Raises:
      None.
    """
    return _sum("span", "span")


def orphans() -> tuple[int, int, int, int, int]:
    """Returns the orphan path's counts, read cell by cell in the order each owner writes them.

    An orphan call raises its gauge, credits, writes one exit and lowers its gauge, all in its own
    cell, so reading each cell's credit, then its gauge, then its exits means a call whose credit
    was read is seen either in progress or with its exit (FR-001).

    Args:
      None.

    Returns:
      The credits, the calls in progress, and the delivered, skipped and sink-lost exits.

    Raises:
      None.
    """
    base, cells = snapshot()
    step = _between
    credits, gauge = base.orphan, base.orphan_open
    delivered, skipped, sink_lost = base.delivered, base.skipped, base.sink_lost
    for index, one in enumerate(cells):
        if step is not None:
            step("orphan", index)
        credits += one.orphan
        gauge += one.orphan_open
        delivered += one.delivered
        skipped += one.skipped
        sink_lost += one.sink_lost
    return credits, gauge, delivered, skipped, sink_lost


def open_spans_unlocked() -> int:
    """Returns the span credits less the hand-overs, read without the lock, for a forked child.

    A child runs its handlers on its only thread, so nothing writes a cell while this reads, and
    the parent's lock may be held by a thread that did not survive the fork. It reads one pair,
    so it cannot see half of a fold (SPEC-088 review, m6).

    Args:
      None.

    Returns:
      The span credits less the hand-overs, over every cell, dead or not.

    Raises:
      None.
    """
    base, cells = _registry.pair
    return sum(one.span - one.handed for one in (base, *cells))


def restart(span_base: int = 0) -> None:
    """Empties the registry and rebinds the thread-local, so every thread re-registers.

    Used by a forked child, whose ledger starts as what it holds, and by the test suite's balance
    fixture at each test's setup. It writes no other thread's cell: the old cells are dropped from
    the registry, and a thread that writes into one afterwards writes into a cell nothing reads.
    The registry object and its lock are kept, under the lock, so a forked child relies on the
    fork roster to have replaced a lock a parent thread held, as every other package lock does,
    rather than on this replacing it in passing.

    Args:
      span_base: The span credits the emptied registry starts holding.

    Returns:
      None.

    Raises:
      None.
    """
    global _here
    base = _Cell()
    base.span = span_base
    registry = _registry
    with registry.lock:
        registry.pair = (base, ())
        _here = threading.local()
