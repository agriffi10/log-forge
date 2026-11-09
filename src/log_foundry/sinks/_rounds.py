"""A converted sink's per-event send counts and its round's loss lines (SPEC-074 FR-001)."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from log_foundry.sinks._engine import When, run_context

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

__all__ = ["Lost", "mark_sent", "sends_of"]


def _sends(owner: object) -> dict[int, list[object]]:
    """Returns the run's send counts for ``owner``, keyed by event identity.

    Each value holds the event beside its count, so an identity cannot be reused by another
    object within the run.

    Args:
      owner: The sink whose run notes keep the counts.

    Returns:
      The counts.

    Raises:
      None.
    """
    notes = run_context().notes(owner)
    return cast("dict[int, list[object]]", notes.setdefault("sends", {}))


def mark_sent(owner: object, events: Iterable[dict[str, object]]) -> None:
    """Counts one more call carrying each of these events, for the rest of the run.

    Args:
      owner: The sink making the call.
      events: The events the call carried.

    Returns:
      None.

    Raises:
      None.
    """
    sends = _sends(owner)
    for event in events:
        held = sends.get(id(event))
        if held is None or held[0] is not event:
            sends[id(event)] = [event, 1]
        else:
            held[1] = cast("int", held[1]) + 1


def sends_of(owner: object, event: dict[str, object]) -> int:
    """Returns how many calls of this run have carried ``event``.

    Args:
      owner: The sink.
      event: The event.

    Returns:
      The count, zero for an event not yet sent.

    Raises:
      None.
    """
    held = _sends(owner).get(id(event))
    if held is None or held[0] is not event:
        return 0
    return cast("int", held[1])


class Lost:
    """One round's lost events, announced on the run context one line per group.

    The rule every converted sink follows (SPEC-074 FR-001): a drop is announced ``SETTLED``, a
    ``REJECTED`` or ``UNKNOWN`` event ``ALWAYS``, and a round's ``RETRYABLE`` events in ``LAST``
    lines, each naming an attempt count that is **the number of calls that carried those
    events** (:func:`sends_of`), not the round number. Events are grouped by their rule, a key
    naming their reason, and that count, so events with different counts get separate lines;
    groups are announced in first-seen order. A site passes a ``line`` that calls ``_diag.lost``
    itself, so the detail stays inside a ``_diag`` call ``tests/test_diag.py`` reads.
    """

    __slots__ = ("_groups", "_owner")

    def __init__(self, owner: object) -> None:
        """Starts an empty round for ``owner``.

        Args:
          owner: The sink whose send counts the lines read.

        Returns:
          None.

        Raises:
          None.
        """
        self._owner = owner
        self._groups: dict[tuple[When, str, int], list[object]] = {}

    def add(
        self,
        events: Iterable[dict[str, object]],
        *,
        when: When,
        key: str,
        line: Callable[[int, int], None],
    ) -> None:
        """Adds events lost for one reason, to be announced with ``line(attempts, count)``.

        Args:
          events: The events.
          when: When the line commits.
          key: Names the reason, so two reasons with one count stay two lines.
          line: Writes the line, given the attempt count and how many events it covers.

        Returns:
          None.

        Raises:
          None.
        """
        for event in events:
            attempts = sends_of(self._owner, event)
            group = self._groups.get((when, key, attempts))
            if group is None:
                self._groups[(when, key, attempts)] = [line, 1]
            else:
                group[1] = cast("int", group[1]) + 1

    def register(self) -> None:
        """Registers each group's line on the run context.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        context = run_context()
        for (when, _, attempts), (line, count) in self._groups.items():
            write = cast("Callable[[int, int], None]", line)
            total = cast("int", count)
            context.announce(_bound(write, attempts, total), when=when)
        self._groups.clear()


def _bound(write: Callable[[int, int], None], attempts: int, total: int) -> Callable[[], None]:
    """Binds a group's line to its attempt count and size, for the run context to call.

    Args:
      write: The site's line, given the attempt count and the event count.
      attempts: The calls that carried the group's events.
      total: How many events the group holds.

    Returns:
      The line as a no-argument callable.

    Raises:
      None.
    """
    return lambda: write(attempts, total)
