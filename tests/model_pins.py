"""SPEC-085's table of cells and its pins: where the cells are red today, and why.

A **cell** is one sink personality, one site its code runs at, one path that first reaches it,
and one script. The table is generated from FR-001's tables, so its size is a derived number
that a test holds to literals. A **pin** names cells that are red at the branch head, the exact
verdict each has, and the open item in ``docs/architecture.md`` §12 that records the defect. A
pinned cell runs, and is an expected failure only at its pin's verdict, so it fails the suite the
day the defect is fixed or a new one appears beside it.

A pin on a one-operation script also **withholds** every longer script that holds that operation
at the same personality, site and path, and every two-operation script holding its sibling: the
pinned defect again, in a form that would cost seconds of waiting per cell. The one-operation
sibling script runs, since it is where a finite timeout spends its budget on a real wait.
Everything withheld is computed from :data:`PINS` here, so deleting a pin brings its cells back.

This module imports the standard library only, and is one of the four files a run at an older
commit copies (FR-005).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from model_oracles import Verdict

OPERATIONS = ("info", "span", "flush", "flush-none", "shutdown", "shutdown-none", "configure")
"""The seven operations a script is made of, in FR-001's order."""

SCRIPTS: tuple[tuple[str, ...], ...] = tuple((first,) for first in OPERATIONS) + tuple(
    (first, second) for first in OPERATIONS for second in OPERATIONS
)
"""The 7 one-operation and 49 two-operation scripts."""

PATHS = ("orphan", "worker")
"""How a sink is first reached: one ``info()``, or one traced call and a reaching flush."""

CALLING_SITES: tuple[tuple[str, str], ...] = (
    *((site, path) for site in ("setter", "emit", "close", "swap-close") for path in PATHS),
    ("swap-emit", "worker"),
)
"""The nine site and path combinations a ``reentrant`` and a ``forker`` are run at."""

WAITER_STOPS = {
    "emit": ("shutdown", "exit", "child-shutdown", "child-exit"),
    "close": ("shutdown", "exit"),
}
"""The stops a ``waiter``'s wait is cut by, at each site; a child never closes what it inherited."""


@dataclass(frozen=True)
class Cell:
    """One deterministic scenario.

    Attributes:
      personality: ``"reentrant"``, ``"factory"``, ``"waiter"`` or ``"forker"``.
      site: ``"setter"``, ``"emit"``, ``"close"``, ``"swap-close"``, ``"swap-emit"`` or
        ``"factory"``.
      path: ``"orphan"`` or ``"worker"``.
      script: One or two operations; ``("fork",)``; or the ``waiter``'s stop.
    """

    personality: str
    site: str
    path: str
    script: tuple[str, ...]


@dataclass(frozen=True)
class Pin:
    """Cells red at the branch head, the verdict each has, and the open item that records it.

    Attributes:
      name: A short name.
      cells: The pinned cells, each run and strict; empty for a pin held by a reproduction.
      verdict: The exact verdict every pinned cell has.
      item: The bold lead of a bullet under ``### Open`` in ``docs/architecture.md`` §12.
      sibling: An operation whose two-operation scripts are withheld with the pinned one: the
        same defect in a form the time oracle cannot see. Its one-operation script runs.
      reproduction: The name of a test function in ``tests/test_model_cells.py`` that holds the
        pin instead of a cell.
    """

    name: str
    cells: tuple[Cell, ...]
    verdict: Verdict
    item: str
    sibling: str | None = None
    reproduction: str | None = None


class Hooks(Protocol):
    """What a personality asks at each visit to a site; the cell and the model supply it."""

    def script(self, sink: object, site: str) -> tuple[str, ...] | None:
        """Returns the script to run at this visit, or ``None``; never asked inside a script.

        Args:
          sink: The sink being visited.
          site: The site.

        Returns:
          The script, or ``None``.

        Raises:
          None.
        """

    def new_sink(self) -> object:
        """Returns the sink a script's ``configure`` installs.

        Args:
          None.

        Returns:
          A sink of the same personality that runs no script.

        Raises:
          None.
        """


class PinnedVerdict(Exception):
    """Raised only when a pinned cell's verdict equals its pin's; the strict mark expects it."""


def cells() -> tuple[Cell, ...]:
    """Returns the whole table, generated from FR-001's tables.

    Args:
      None.

    Returns:
      Every cell: 504 ``reentrant``, 112 ``factory``, 9 ``forker`` and 12 ``waiter``.

    Raises:
      None.
    """
    table = [
        Cell("reentrant", site, path, script) for site, path in CALLING_SITES for script in SCRIPTS
    ]
    table += [Cell("factory", "factory", path, script) for path in PATHS for script in SCRIPTS]
    table += [Cell("forker", site, path, ("fork",)) for site, path in CALLING_SITES]
    table += [
        Cell("waiter", site, path, (stop,))
        for site, stops in WAITER_STOPS.items()
        for path in PATHS
        for stop in stops
    ]
    return tuple(table)


def cell_id(cell: Cell) -> str:
    """Returns a cell's test id, ``personality-site-path-script``, a two-operation script joined.

    Args:
      cell: The cell.

    Returns:
      The id.

    Raises:
      None.
    """
    return f"{cell.personality}-{cell.site}-{cell.path}-{'+'.join(cell.script)}"


def _cell(personality: str, site: str, path: str, *script: str) -> Cell:
    """Builds a cell for a pin.

    Args:
      personality: The personality.
      site: The site.
      path: The path.
      *script: The script's operations.

    Returns:
      The cell.

    Raises:
      None.
    """
    return Cell(personality, site, path, script)


PINS: tuple[Pin, ...] = (
    Pin(
        "drain-flush",
        (
            _cell("reentrant", "emit", "worker", "flush-none"),
            _cell("reentrant", "swap-emit", "worker", "flush-none"),
            _cell("factory", "factory", "worker", "flush-none"),
        ),
        frozenset({"HangFound:flush-none@worker.flush"}),
        "`flush()` called on the drain thread waits on itself",
        sibling="flush",
    ),
    Pin(
        "nested-shutdown",
        tuple(
            _cell("reentrant", site, path, "shutdown-none")
            for site in ("close", "swap-close")
            for path in PATHS
        ),
        frozenset({"Stalled:shutdown-none"}),
        "`shutdown()` called from a sink's own `close()` waits out the closer grace on that same "
        "close",
        sibling="shutdown",
    ),
    Pin(
        "empty-submission",
        tuple(
            _cell("reentrant", "setter", "worker", *pair)
            for flush in ("flush", "flush-none")
            for stop in ("shutdown", "shutdown-none")
            for pair in ((flush, stop), (stop, flush))
        ),
        frozenset({"LedgerBroken:spans+1"}),
        "An empty batch submitted after a `shutdown()` is counted as queued, counted as submitted "
        "after shutdown and announced as a lost submission",
    ),
    Pin(
        "orphan-arm-in-close",
        tuple(_cell("reentrant", "close", path, "configure", "info") for path in PATHS),
        frozenset({"CloseBroken:never-closed@1"}),
        "A sink first written by an orphan log made inside a `shutdown()`'s close is not closed "
        "by that `shutdown()`",
    ),
    Pin(
        "installed-closed-twice",
        (
            _cell("reentrant", "swap-emit", "worker", "info", "shutdown"),
            _cell("reentrant", "swap-emit", "worker", "info", "shutdown-none"),
        ),
        frozenset({"CloseBroken:repeat@1"}),
        "A sink a swap installs is closed twice with nothing written between when an orphan log "
        "armed it and a re-entrant `shutdown()` closed it before the swap decided",
    ),
    Pin(
        "fork-mid-swap",
        (_cell("forker", "swap-emit", "worker", "fork"),),
        frozenset({"ChildBroken:two-sinks"}),
        "A child forked while a sink change is in flight sends its spans to the old sink and its "
        "orphan logs to the new one",
    ),
    Pin(
        "orphan-write-after-close",
        (),
        frozenset({"CloseBroken:unreleased@1"}),
        "An orphan log that passed its owed check before a `shutdown()` took its sink writes after "
        "the close, and the sink is owed nothing",
        reproduction="test_an_orphan_log_parked_after_its_owed_check_writes_after_the_close",
    ),
)
"""The pins, fixed by measurement at the branch head (FR-004)."""


def pin_of(cell: Cell) -> Pin | None:
    """Returns the pin that names a cell, or ``None``.

    Args:
      cell: The cell.

    Returns:
      Its pin.

    Raises:
      None.
    """
    for pin in PINS:
        if cell in pin.cells:
            return pin
    return None


def withheld(cell: Cell) -> bool:
    """Returns whether a pin withholds a cell, read from :data:`PINS` at the time of the call.

    A pinned cell is never withheld. Otherwise a cell is withheld when a pin holds a
    one-operation cell at the same personality, site and path, and the cell's script is longer
    and holds that operation or the pin's sibling. The one-operation sibling script runs: it is
    the one cell at that place whose call with a finite timeout spends its budget on a real wait,
    so it is what exercises ``BoundExceeded`` (FR-003).

    Args:
      cell: The cell.

    Returns:
      Whether it is withheld.

    Raises:
      None.
    """
    if pin_of(cell) is not None:
        return False
    place = (cell.personality, cell.site, cell.path)
    for pin in PINS:
        for held in pin.cells:
            if len(held.script) != 1 or (held.personality, held.site, held.path) != place:
                continue
            if len(cell.script) > 1 and (
                held.script[0] in cell.script
                or (pin.sibling is not None and pin.sibling in cell.script)
            ):
                return True
    return False


def scheduled() -> tuple[Cell, ...]:
    """Returns the cells that run: every cell no pin withholds.

    Args:
      None.

    Returns:
      The scheduled cells, in table order.

    Raises:
      None.
    """
    return tuple(cell for cell in cells() if not withheld(cell))


def judge(pin: Pin | None, verdict: Verdict, text: str) -> None:
    """Decides a verdict: a pass, an expected failure at exactly a pin's verdict, or a failure.

    A crash is never an expected failure, whatever a pin says.

    Args:
      pin: The pin that holds the test, or ``None``.
      verdict: The verdict.
      text: What a failure prints.

    Returns:
      None, when there is no pin and the verdict is empty.

    Raises:
      PinnedVerdict: When the verdict equals the pin's.
      AssertionError: For any other verdict, including a pinned test that passed.
    """
    crashed = any(signature.startswith("ChildCrashed:") for signature in verdict)
    if pin is None:
        assert not verdict, f"{sorted(verdict)}\n{text}"
        return
    if verdict == pin.verdict and not crashed:
        raise PinnedVerdict(f"{pin.name}: {pin.item}")
    if not verdict:
        raise AssertionError(
            f"the pin {pin.name!r} no longer shows its defect: remove the pin and move its item "
            f"to Resolved\n{text}"
        )
    raise AssertionError(
        f"pinned by {pin.name!r} at {sorted(pin.verdict)}, but the verdict is {sorted(verdict)}"
        f"\n{text}"
    )


def settle(cell: Cell, verdict: Verdict, text: str) -> None:
    """Decides a cell's verdict against its pin.

    Args:
      cell: The cell.
      verdict: Its verdict.
      text: What a failure prints.

    Returns:
      None, when the cell is unpinned and passed.

    Raises:
      PinnedVerdict: When the cell is pinned and its verdict equals its pin's.
      AssertionError: For any other outcome.
    """
    judge(pin_of(cell), verdict, text)
