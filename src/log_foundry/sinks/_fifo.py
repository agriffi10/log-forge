"""The FIFO ids, the per-run entry cache and the group hold that SQSSink and SNSSink share."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import NamedTuple, Protocol, cast

from log_foundry import _diag
from log_foundry.sinks._chunk import chunk_items
from log_foundry.sinks._engine import run_context
from log_foundry.sinks.verdict import Outcome

DEFAULT_GROUP_ID = "log-foundry"
"""Fallback group for an event carrying no usable ``trace_id`` — never send an empty group id."""

MAX_ID_LEN = 128
"""The SQS and SNS maximum length for ``MessageGroupId`` and ``MessageDeduplicationId``."""

GroupIdSource = str | Callable[[dict[str, object]], str] | None
"""What ``message_group_id=`` accepts: the group itself, a callable of the event, or ``None``.

A ``str`` is the group every event is sent under, never an event key to read; a callable is
asked once per event per run; ``None`` groups each event by its own ``trace_id``.
"""

DedupIdSource = Callable[[dict[str, object]], str] | None
"""What ``message_deduplication_id=`` accepts: a callable of the event, or ``None``.

``None`` means the event's own ``log_id``, the library's idempotency key (SPEC-095); only a blank
answer falls back to a freshly minted id.
"""


class Prepared(NamedTuple):
    """One event serialized and costed, with its FIFO ids resolved, at its place in the round.

    The ids are ``None`` on a standard queue or topic, and are derived once per run rather than
    again at send time or in a later round: the deduplication fallback mints a fresh UUID, so
    deriving twice would bill the byte budget for one value and put a different one on the wire,
    and a resend with a new id would lose the service's five-minute de-duplication of it
    (SPEC-074 FR-002, SPEC-100 FR-002).

    Attributes:
      position: The event's position in the batch the round was handed, so each chunk's outcomes
        map back to the verdict.
      body: The serialized event.
      group_id: The resolved ``MessageGroupId``, or ``None``.
      dedup_id: The resolved ``MessageDeduplicationId``, or ``None``.
      size: The costed bytes: the body's UTF-8 length plus, on a FIFO destination, both ids'.
        Carried so :func:`~log_foundry.sinks._chunk.chunk_items` reads the cost rather than
        recomputing it (SPEC-071 FR-004).
    """

    position: int
    body: str
    group_id: str | None
    dedup_id: str | None
    size: int


class Plan(NamedTuple):
    """One round's sends and drops, every drop decided before the first send (SPEC-074 FR-002).

    Attributes:
      chunks: The round's chunks, each valid for one request, built with ``chunk_items``.
      dropped: Each dropped position of the round's batch and its reason: ``"oversize"``,
        ``"id failure"`` or ``"unencodable"``.
      refused: The first id callable's exception, set only when every event's id callable
        raised; the round is then a refusal.
      limit: The run's effective message limit, read once per run of ``emit``, which the chunks
        were bounded by and the round's oversize lines name (SPEC-096 FR-002).
    """

    chunks: list[list[Prepared]]
    dropped: dict[int, str]
    refused: Exception | None
    limit: int


@dataclass(slots=True)
class Entry:
    """One event occurrence's prepared entry or drop reason, kept for its delivery run.

    Attributes:
      body: The serialized event, or ``""`` for a drop.
      group_id: The resolved ``MessageGroupId``, or ``None``.
      dedup_id: The resolved ``MessageDeduplicationId``, or ``None``.
      size: The costed bytes.
      drop: The drop reason, or ``None`` for an entry that can be sent.
      error: The id callable's exception, for an id failure.
      landed: Whether the service has accepted this occurrence.
    """

    body: str = ""
    group_id: str | None = None
    dedup_id: str | None = None
    size: int = 0
    drop: str | None = None
    error: Exception | None = None
    landed: bool = False


class PrepareOne(Protocol):
    """The shape of a sink's ``_prepare``, which :func:`plan_round` calls once per occurrence."""

    def __call__(self, event: dict[str, object], body: str | None, *, limit: int) -> Entry:
        """Prepares one event occurrence's entry, or decides its drop.

        Args:
          event: The event.
          body: Its encoded body, or ``None`` when the encode step could not encode it.
          limit: The run's effective message limit, in bytes, passed by keyword.

        Returns:
          The entry.

        Raises:
          None. A ``BaseException`` from a caller's callable propagates.
        """


def bounded(raw: str, fallback: str) -> str:
    """Normalizes a derived group id, falling back when blank and truncating when over-long.

    Args:
      raw: The derived id.
      fallback: What a blank id becomes.

    Returns:
      The id, at most :data:`MAX_ID_LEN` characters.

    Raises:
      None.
    """
    cleaned = raw.strip()
    if not cleaned:
        return fallback
    return cleaned[:MAX_ID_LEN]


def group_id(source: GroupIdSource, event: dict[str, object]) -> str:
    """Resolves one event's ``MessageGroupId`` (SPEC-016 FR-002).

    A constant is used as given, a callable is asked, and otherwise the event's own ``trace_id``
    groups it. The constant is told apart by its real type and read through ``str.__str__``
    (SPEC-071 FR-005), so a callable whose ``__class__`` claims ``str`` is still asked, and a
    ``str`` subclass's own ``strip`` never runs per event. Every route is bounded, so a caller's
    callable returning an empty string cannot put an empty parameter on the wire.

    Args:
      source: The sink's ``message_group_id``.
      event: The event being sent.

    Returns:
      The group id.

    Raises:
      Exception: A caller-supplied callable's own exception, which :func:`prepare` turns into a
        drop of this one event (SPEC-058 FR-004).
    """
    if issubclass(type(source), str):
        raw = _diag._STR_STR(cast("str", source))
    elif source is not None:
        raw = cast("Callable[[dict[str, object]], str]", source)(event)
    else:
        raw = str(event.get("trace_id", ""))
    return bounded(raw, DEFAULT_GROUP_ID)


def dedup_id(source: DedupIdSource, event: dict[str, object]) -> str:
    """Resolves one event's ``MessageDeduplicationId`` (SPEC-016 FR-003).

    This defaults to the event's ``log_id``, already a per-event UUID, so the service's
    five-minute deduplication window can never collapse two genuinely distinct records. The
    fallback mints a fresh id for the same reason: a shared constant would make unrelated events
    deduplicate each other.

    Args:
      source: The sink's ``message_deduplication_id``.
      event: The event being sent.

    Returns:
      The deduplication id.

    Raises:
      Exception: A caller-supplied callable's own exception, which :func:`prepare` turns into a
        drop of this one event (SPEC-058 FR-004).
    """
    raw = source(event) if source is not None else str(event.get("log_id", ""))
    if raw.strip():
        return bounded(raw, "")
    from log_foundry.ids import new_log_id

    return new_log_id()


def prepare(
    body: str | None,
    event: dict[str, object],
    *,
    fifo: bool,
    group: GroupIdSource,
    dedup: DedupIdSource,
    limit: int,
) -> Entry:
    """Prepares one event occurrence's entry, or decides its drop.

    An entry is costed as its body's UTF-8 length plus, on a FIFO destination, both ids' UTF-8
    lengths (SPEC-016 FR-005), and dropped as oversize when that cost exceeds ``limit``, the
    run's effective message limit (SPEC-096 FR-002).

    Args:
      body: The encoded body, or ``None`` when the encode step could not encode it.
      event: The event.
      fifo: Whether the destination is FIFO, so the ids are resolved and costed.
      group: The sink's ``message_group_id``.
      dedup: The sink's ``message_deduplication_id``.
      limit: The run's effective message limit, in bytes.

    Returns:
      The entry.

    Raises:
      None. A ``BaseException`` from a caller's callable propagates.
    """
    if body is None:
        return Entry(drop="unencodable")
    size = len(body.encode("utf-8"))
    gid: str | None = None
    did: str | None = None
    if fifo:
        try:
            gid = group_id(group, event)
            did = dedup_id(dedup, event)
        except Exception as err:
            return Entry(drop="id failure", error=err)
        size += len(gid.encode("utf-8")) + len(did.encode("utf-8"))
    if size > limit:
        return Entry(size=size, drop="oversize")
    return Entry(body=body, group_id=gid, dedup_id=did, size=size)


def plan_round(
    owner: object,
    batch: list[dict[str, object]],
    bodies: dict[int, str],
    prepare_one: PrepareOne,
    *,
    max_count: int,
    limit: int,
) -> Plan:
    """Prepares each event once per run and splits the round into sends within both limits.

    The boundaries are :func:`~log_foundry.sinks._chunk.chunk_items`' (SPEC-071 FR-004): every
    per-event drop is decided first, so an event over the limit is filtered out before any
    boundary is drawn, and the chunks are materialised as a list, so all of them are known before
    the first send.

    **Each entry is prepared once per run** (SPEC-074 FR-002). Its body comes from the sink's own
    encode step, and on a FIFO destination its ids are resolved once (SPEC-016 FR-005), because
    they travel in the same request; the prepared entry — body, ``MessageGroupId``,
    ``MessageDeduplicationId`` and costed size — or its drop reason is kept in
    ``run_context().notes(owner)["prepared"]`` keyed by event identity, so the id callables and
    the random fallback run **once per event per run** and every round resends the identical
    entry. Only the grouping and each entry's index are per round. An identity that appears twice
    keeps one entry per occurrence, and a round hands an occurrence the first entry of its
    identity that has not landed. The round's entries, by position, are kept under
    ``"prepared_round"`` for the sink to mark the landed ones.

    **A FIFO id callable that raises for one event costs only that event** (SPEC-058 FR-004),
    under the wrappers' ledger rule: when every event of the round failed in its id callable,
    ``refused`` carries the first exception; otherwise the failed events are dropped. This counts
    and announces nothing: the sink turns the drops into ``DROPPED`` outcomes and their lines.

    Args:
      owner: The sink, whose run notes hold the cache.
      batch: The events the round was handed.
      bodies: Each encodable position's body, from the sink's own ``encode_round`` call, which
        stays in the sink's module so ``tests/test_encode.py``'s roster still derives the sink.
      prepare_one: The sink's ``_prepare``, called ``prepare_one(event, body, limit=limit)``.
      max_count: The most entries one request may carry.
      limit: The run's effective message limit, in bytes: it bounds each entry's cost and each
        chunk's summed cost, and rides on the plan.

    Returns:
      The round's plan.

    Raises:
      None. A ``BaseException`` from a caller's callable propagates.
    """
    notes = run_context().notes(owner)
    cache = cast(
        "dict[int, tuple[dict[str, object], list[Entry]]]", notes.setdefault("prepared", {})
    )
    round_entries: list[Entry | None] = [None] * len(batch)
    notes["prepared_round"] = round_entries
    taken: dict[int, int] = {}
    prepared: list[Prepared] = []
    dropped: dict[int, str] = {}
    failures = 0
    first: Exception | None = None
    for position, event in enumerate(batch):
        held = cache.get(id(event))
        if held is None or held[0] is not event:
            held = (event, [])
            cache[id(event)] = held
        live = [cached for cached in held[1] if not cached.landed]
        skip = taken.get(id(event), 0)
        taken[id(event)] = skip + 1
        if skip < len(live):
            entry = live[skip]
        else:
            entry = prepare_one(event, bodies.get(position), limit=limit)
            held[1].append(entry)
        round_entries[position] = entry
        if entry.drop is not None:
            dropped[position] = entry.drop
            if entry.drop == "id failure":
                failures += 1
                first = first if first is not None else entry.error
            continue
        prepared.append(Prepared(position, entry.body, entry.group_id, entry.dedup_id, entry.size))
    chunks = list(
        chunk_items(prepared, max_count=max_count, max_bytes=limit, size_of=lambda item: item.size)
    )
    refused = first if batch and failures == len(batch) else None
    return Plan(chunks, dropped, refused, limit)


def hold(chunk: list[Prepared], blocked: set[str], sends: list[int], limit: int) -> list[bool]:
    """Decides, at send time, which entries of a FIFO chunk wait behind their group.

    SQS and SNS document ordering within a message group as a FIFO guarantee, and 1.x's SQS loop
    kept it for as long as it retried a chunk, ``max_retries + 1`` sends. So an entry is held
    when its group is ``blocked`` — an earlier message of it, sent in an earlier chunk of this
    round or held itself, has neither landed nor used its sends — or when an earlier entry of its
    group in this chunk was already sent in an earlier round and this send will not be its last.
    Entries of a group never sent before travel together in one chunk, as 1.x sent them, so a
    healthy one-group batch is one call. A held entry blocks its group for the rest of the round.
    This is the one function deciding a hold for both FIFO sinks, so another reading of it is a
    change here alone.

    Args:
      chunk: The chunk's entries, in batch order.
      blocked: Groups blocked so far this round, updated in place.
      sends: Each entry's sends so far this run.
      limit: ``max_retries + 1``.

    Returns:
      Per entry, whether it is held.

    Raises:
      None.
    """
    held: list[bool] = []
    for item, count in zip(chunk, sends, strict=True):
        group = item.group_id or ""
        if group in blocked:
            held.append(True)
            continue
        held.append(False)
        if count and count + 1 < limit:
            blocked.add(group)
    return held


def block(
    sent: list[Prepared], fates: list[Outcome], sends: list[int], blocked: set[str], limit: int
) -> None:
    """Blocks the group of every sent entry that neither landed nor used its sends.

    Args:
      sent: The entries a call carried.
      fates: The round's outcomes, by position.
      sends: Each sent entry's sends so far this run, this call's included.
      blocked: Groups blocked so far this round, updated in place.
      limit: ``max_retries + 1``.

    Returns:
      None.

    Raises:
      None.
    """
    for item, count in zip(sent, sends, strict=True):
        if fates[item.position] is not Outcome.ACCEPTED and count < limit:
            blocked.add(item.group_id or "")
