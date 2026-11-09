"""A converted sink's per-event send counts and its round's loss lines (SPEC-074 FR-001)."""

from __future__ import annotations

from typing import TYPE_CHECKING, NamedTuple, cast

from log_foundry import _diag
from log_foundry.sinks._batch import (
    ABSENT,
    adjudicate_keyed,
    adjudicate_positional,
    describe_count,
    results_rejected,
    usable_results,
)
from log_foundry.sinks._engine import When, run_context
from log_foundry.sinks.verdict import Outcome

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    from log_foundry.sinks._deferral import _Counted

__all__ = [
    "Called",
    "KeyedReply",
    "Lost",
    "entry_ids",
    "mark_sent",
    "read_keyed",
    "sends_of",
    "settle_keyed",
    "settle_positional",
]


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


class Called[T](NamedTuple):
    """What one batch call did: its result, or the exception it raised.

    Attributes:
      result: The reply, read or raw, when the call returned; ``None`` when it raised.
      error: The exception the call raised, or ``None``.
      retryable: Whether the sink's ``_retryable_error`` accepted ``error``.
    """

    result: T | None
    error: Exception | None = None
    retryable: bool = False


class KeyedReply(NamedTuple):
    """What one SQS or SNS batch call's reply said about each entry it carried.

    Attributes:
      outcomes: One per entry, in order; empty when the reply could not be read.
      unread: Whether the reply could not be paired to the entries.
      code: The first sender fault's ``Code``, for its line.
    """

    outcomes: list[Outcome]
    unread: bool = False
    code: object = "unknown"


def read_keyed(
    retryable_entry: Callable[[Mapping[str, object]], bool], ids: list[str], response: object
) -> KeyedReply:
    """Reads a keyed batch reply through the keyed reader and the sink's entry predicate.

    SQS and SNS share it (SPEC-074 FR-002): the reply's ``Failed`` list is paired with the ids
    sent by :func:`~log_foundry.sinks._batch.adjudicate_keyed`, a missing ``Failed`` pairs
    nothing, and a reply that is not a mapping, or a list the reader refuses, is unread. A failed
    entry the predicate calls permanent — a sender fault — is ``REJECTED``, any other
    ``RETRYABLE``; an entry the reply does not name is ``ACCEPTED``.

    Args:
      retryable_entry: The sink's ``_retryable_entry``.
      ids: The entry ids sent, in order.
      response: The client's reply, of any shape.

    Returns:
      The reply per entry.

    Raises:
      None. A ``BaseException`` from the predicate or the reply still propagates.
    """
    try:
        failed = cast("dict[str, object]", response).get("Failed", [])
        read = adjudicate_keyed(ids, failed, key="Id")
        if read.unadjudicated:
            return KeyedReply([], unread=True)
        outcomes: list[Outcome] = []
        code: object = "unknown"
        found = False
        for entry in read.failed:
            if entry is None:
                outcomes.append(Outcome.ACCEPTED)
            elif retryable_entry(entry):
                outcomes.append(Outcome.RETRYABLE)
            else:
                outcomes.append(Outcome.REJECTED)
                if not found:
                    code = entry.get("Code", "unknown")
                    found = True
    except Exception:
        return KeyedReply([], unread=True)
    return KeyedReply(outcomes, code=code)


def entry_ids(owner: object, events: list[dict[str, object]]) -> list[str]:
    """Returns each event's batch entry ``Id``: kept across the run's rounds, distinct per call.

    A first send numbers a call's entries ``"0"``, ``"1"``, … as 1.x did, and a resend in a later
    round carries the id the entry was first sent with, as 1.x's loop resent its entries — unless
    another entry of the same call already holds it, when the smallest free number is taken and
    kept. Kept in the run's notes by event identity, one id per occurrence.

    Args:
      owner: The sink.
      events: The call's events, in order.

    Returns:
      One id per event.

    Raises:
      None.
    """
    store = cast(
        "dict[int, tuple[dict[str, object], list[str]]]",
        run_context().notes(owner).setdefault("ids", {}),
    )
    used: set[str] = set()
    seen: dict[int, int] = {}
    ids: list[str] = []
    for position, event in enumerate(events):
        held = store.get(id(event))
        if held is None or held[0] is not event:
            held = (event, [])
            store[id(event)] = held
        occurrence = seen.get(id(event), 0)
        seen[id(event)] = occurrence + 1
        if occurrence < len(held[1]) and held[1][occurrence] not in used:
            chosen = held[1][occurrence]
        else:
            free = position
            while str(free) in used:
                free += 1
            chosen = str(free)
            if occurrence < len(held[1]):
                held[1][occurrence] = chosen
            else:
                held[1].append(chosen)
        used.add(chosen)
        ids.append(chosen)
    return ids


def settle_keyed(
    owner: object,
    *,
    positions: list[int],
    carried: list[dict[str, object]],
    error: Exception | None,
    retryable: bool,
    reply: KeyedReply | None,
    call: str,
    fates: list[Outcome],
    final: list[int],
    lost: Lost,
) -> list[int]:
    """Sets one SQS or SNS call's outcomes and registers its lines; both sinks share it.

    A raise makes the call's entries ``RETRYABLE`` (a ``LAST`` line) or ``REJECTED`` (an
    ``ALWAYS`` line) as ``_retryable_error`` answered, the line SPEC-070's ``<Class>, N
    attempt(s), <Type>``; an unread reply makes them ``UNKNOWN`` with one ``ALWAYS`` line, never
    resent; otherwise each entry takes its reply's outcome, a sender fault marked a final
    rejection with 1.x's ``rejected as invalid (first code: …); not retried`` line, and an entry
    still failing a ``LAST`` line ``still failing after N attempt(s); abandoned``.

    Args:
      owner: The sink, whose class name the lines carry.
      positions: Each carried entry's position in the round's batch.
      carried: The carried events, in the same order.
      error: What the call raised, or ``None``.
      retryable: Whether ``_retryable_error`` accepted ``error``.
      reply: The reply per entry, or ``None`` when the call raised.
      call: The API's name, for the unread line.
      fates: The round's outcomes, updated in place.
      final: The round's final rejections, extended in place.
      lost: The round's lost events.

    Returns:
      The positions the reply accepted.

    Raises:
      None.
    """
    context = run_context()
    name = type(owner).__name__
    if error is not None:
        kind = type(error).__name__
        for position in positions:
            fates[position] = Outcome.RETRYABLE if retryable else Outcome.REJECTED
        lost.add(
            carried,
            when=When.LAST if retryable else When.ALWAYS,
            key=f"raised {kind}",
            line=lambda n, count: _diag.lost("message", count, f"{name}, {n} attempt(s), {kind}"),
        )
        return []
    if reply is None or reply.unread:
        for position in positions:
            fates[position] = Outcome.UNKNOWN
        unread = len(positions)
        context.announce(
            lambda: _diag.lost(
                "message",
                unread,
                f"{name} could not adjudicate a {call} reply ({unread} entry(ies) sent); not "
                "retried",
            ),
            when=When.ALWAYS,
        )
        return []
    accepted: list[int] = []
    faults = 0
    retrying: list[dict[str, object]] = []
    for position, event, outcome in zip(positions, carried, reply.outcomes, strict=True):
        fates[position] = outcome
        if outcome is Outcome.ACCEPTED:
            accepted.append(position)
        elif outcome is Outcome.REJECTED:
            faults += 1
            final.append(position)
        else:
            retrying.append(event)
    if faults:
        code = reply.code
        context.announce(
            lambda: _diag.lost(
                "message", faults, f"{name}, rejected as invalid (first code: {code}); not retried"
            ),
            when=When.ALWAYS,
        )
    lost.add(
        retrying,
        when=When.LAST,
        key="entry",
        line=lambda n, count: _diag.lost(
            "message", count, f"{name}, still failing after {n} attempt(s); abandoned"
        ),
    )
    return accepted


def settle_positional(
    owner: object,
    *,
    positions: list[int],
    carried: list[dict[str, object]],
    records: list[dict[str, object]],
    error: Exception | None,
    retryable: bool,
    response: object,
    call: str,
    count_key: str,
    results_key: str,
    fates: list[Outcome],
    lost: Lost,
) -> None:
    """Sets one Kinesis or Firehose call's outcomes and registers its lines; both share it.

    A raise is settled as :func:`settle_keyed` settles one. A reply goes whole to
    :func:`~log_foundry.sinks._batch.adjudicate_positional` with its failed count, as before
    (SPEC-070 FR-003): a record it codes as failed is ``RETRYABLE`` — the entry predicate of both
    services answers every coded record retryable — the rest ``ACCEPTED``; a reply it cannot
    adjudicate, or one that is not a mapping, makes the call's records ``UNKNOWN``, never
    resent, counted on ``dropped_unadjudicated`` ``ALWAYS`` with 1.x's line, so the sub-count
    keeps its meaning and ``.failed`` keeps excluding it.

    Args:
      owner: The sink, whose class name the lines carry.
      positions: Each carried record's position in the round's batch.
      carried: The carried events, in the same order.
      records: The request entries sent, in the same order.
      error: What the call raised, or ``None``.
      retryable: Whether ``_retryable_error`` accepted ``error``.
      response: The client's reply, when the call returned.
      call: The API's name, for the unadjudicated line.
      count_key: The reply's failed-count field.
      results_key: The reply's per-record field.
      fates: The round's outcomes, updated in place.
      lost: The round's lost events.

    Returns:
      None.

    Raises:
      None.
    """
    context = run_context()
    name = type(owner).__name__
    if error is not None:
        kind = type(error).__name__
        for position in positions:
            fates[position] = Outcome.RETRYABLE if retryable else Outcome.REJECTED
        lost.add(
            carried,
            when=When.LAST if retryable else When.ALWAYS,
            key=f"raised {kind}",
            line=lambda n, count: _diag.lost("record", count, f"{name}, {n} attempt(s), {kind}"),
        )
        return
    try:
        reply = cast("dict[str, object]", response)
        failed_count = reply.get(count_key, ABSENT)
        raw = reply.get(results_key)
    except Exception:
        failed_count, raw = ABSENT, object()
    results = usable_results(raw)
    rejected = results_rejected(raw)
    verdict = adjudicate_positional(records, results, failed_count=failed_count, rejected=rejected)
    if verdict.unadjudicated:
        coded = sum(1 for result in results if result.get("ErrorCode"))
        returned = (
            "an unreadable result array" if rejected else f"{len(results)} result(s) returned"
        )
        sent = len(records)
        unadjudicated = verdict.unadjudicated
        described = describe_count(failed_count)
        for position in positions:
            fates[position] = Outcome.UNKNOWN
        context.count(
            cast("_Counted", owner), "dropped_unadjudicated", unadjudicated, when=When.ALWAYS
        )
        context.announce(
            lambda: _diag.lost(
                "record",
                unadjudicated,
                f"{name} could not adjudicate a {call} response ({sent} record(s) sent, "
                f"{returned}, {coded} coded, {described}); abandoned, not retried",
            ),
            when=When.ALWAYS,
        )
        return
    retry = {id(record) for record in verdict.retry}
    retrying: list[dict[str, object]] = []
    for position, event, record in zip(positions, carried, records, strict=True):
        if id(record) in retry:
            fates[position] = Outcome.RETRYABLE
            retrying.append(event)
        else:
            fates[position] = Outcome.ACCEPTED
    lost.add(
        retrying,
        when=When.LAST,
        key="entry",
        line=lambda n, count: _diag.lost(
            "record", count, f"{name}, still failing after {n} attempt(s); abandoned"
        ),
    )
