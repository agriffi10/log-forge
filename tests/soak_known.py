"""SPEC-099's known soak shapes, each an open item of §12, and the nightly runs filed so far.

This is a **clearing file** (FR-006): a pull request that files a failed scheduled night of the
model soak changes this file and ``docs/architecture.md`` and nothing else. So it holds only the
data such a pull request must write: the ``KNOWN`` rows, each with its predicate, and
``FILED_NIGHTS``. What matches a run against the rows lives in ``tests/soak_verdict.py`` and the
ceilings in ``tests/soak_ceilings.py``, neither of which a clearing pull request may touch.

A row names the bold lead of one bullet under ``### Open`` in ``docs/architecture.md`` §12, and a
default-suite test fails once that item leaves ``### Open``, so the spec that resolves an item
removes its row in the same change. A row is added only by the pull request that files its item.
**No row may explain Shape A's record**: a pair of closes at one count whose first take is
``_close_owed``'s with only ``_swap_sink`` arms between the takes is either a regression of the
swap item SPEC-097 closed or the open ``fenced`` item, and the two cannot be told apart, so either
stays red (``tests/test_soak_known.py``'s ``test_no_known_row_explains_shape_a``).

The late-write predicate rests on one fact about the library: every write on a caller's thread
follows that thread's own entry to ``_note_orphan_emit``, which holds while the orphan path's
``api._log`` and the worker's ``worker._emit`` are the only places outside ``sinks/`` that hand a
sink a batch. ``test_every_place_the_library_hands_a_sink_a_batch_is_known`` holds that roster.

``FILED_NIGHTS`` is read as text by ``scripts/pr-queue/queue.sh`` on ``refs/pr-queue/main``, so
each entry is one line, four spaces in: ``"<run id>-<attempt>": "<§12 lead>",``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from model_oracles import Arm, Note, Take, Write

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from model_oracles import Closes, SinkEvent


@dataclass(frozen=True)
class Known:
    """A soak shape an open item records, so a red soak item of that shape alone is expected.

    Attributes:
      name: A short name, printed in the run's summary and recorded as ``soak_known``.
      item: The bold lead of a bullet under ``### Open`` in ``docs/architecture.md`` §12.
      kind: The signature kind it explains: ``"repeat"`` or ``"late-write"``.
      predicate: Whether one sink's events and close record have this shape.
    """

    name: str
    item: str
    kind: str
    predicate: Callable[[Sequence[SinkEvent], Closes], bool] = field(compare=False)


def orphan_late_write(events: Sequence[SinkEvent], closes: Closes) -> bool:
    """Returns whether every write after the sink's last close returned is §12's orphan late write.

    The late writes are the last ``writes - at_return[-1]`` :class:`Write` events: a recorder
    appends a write's event under the same lock that counts it, so the event order is the count
    order. Each must be on a thread not named ``log-foundry-*``, so a worker-path late write is
    never read as the orphan path's (invariant 6); that thread's last :class:`Note` for the sink
    must come before the sink's last :class:`Take`, which is a log that passed its owed check
    before the take; and nothing may arm the sink after that take.

    Args:
      events: One sink's events, in the order recorded.
      closes: The sink's close record.

    Returns:
      Whether the sink has late writes and every one of them has this shape.

    Raises:
      None.
    """
    if not closes.at_return or closes.writes <= closes.at_return[-1]:
        return False
    takes = [index for index, event in enumerate(events) if isinstance(event, Take)]
    writes = [index for index, event in enumerate(events) if isinstance(event, Write)]
    late = closes.writes - closes.at_return[-1]
    if not takes or len(writes) < late:
        return False
    last_take = takes[-1]
    if any(isinstance(event, Arm) for event in events[last_take + 1 :]):
        return False
    for index in writes[-late:]:
        write = events[index]
        assert isinstance(write, Write)
        if write.thread.startswith("log-foundry-"):
            return False
        notes = [
            at
            for at in range(index)
            if isinstance(events[at], Note) and getattr(events[at], "thread", None) == write.thread
        ]
        if not notes or notes[-1] > last_take:
            return False
    return True


ORPHAN_LATE_WRITE = Known(
    "orphan-write-after-close",
    "An orphan log that passed its owed check before a `shutdown()` took its sink writes after "
    "the close, and the sink is owed nothing",
    "late-write",
    orphan_late_write,
)

KNOWN: tuple[Known, ...] = (ORPHAN_LATE_WRITE,)
"""Every known shape. A row whose item has left ``### Open`` fails the default suite."""

FILED_NIGHTS: dict[str, str] = {
    "45975518690-1": "Two timing tests fail `test (py3.12)` on a loaded 4-CPU runner",
}
"""Each scheduled night a clearing pull request filed: ``"<run id>-<attempt>"`` to its §12 lead.

One entry a line, four spaces in, the quoted key, a colon, the quoted lead and a comma: the PR
queue reads this file on ``refs/pr-queue/main`` as text, and a night is cleared only by its own
entry (FR-006).
"""
