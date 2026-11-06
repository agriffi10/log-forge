"""The one encode step every record-building sink runs before its first bulk call (SPEC-069)."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from log_foundry import _diag
from log_foundry.sanitize import int_placeholder

if TYPE_CHECKING:
    from collections.abc import Callable

    from log_foundry.sinks._deferral import Deferral

__all__ = ["NUL_ESCAPE", "encode_events", "repair_bson", "repair_nul"]

NUL_ESCAPE = re.compile(r"(?<!\\)(?:\\\\)*\\u0000")
"""Matches a JSON ``\\u0000`` escape — a real U+0000 — and never the six characters as text.

A ``u0000`` counts only when the run of backslashes before it has odd length, so its last
backslash is the escape's own: ``json.dumps`` writes a real NUL as ``\\u0000`` and the six
characters ``\\u0000`` as ``\\\\u0000``, which JSONB accepts. A plain substring test would
match both and repair an event PostgreSQL takes. Run over the JSON text ``PostgresSink`` already
builds, so a clean event costs one search (SPEC-069 FR-003).
"""

_NUL = "\x00"
_REPLACEMENT = "\ufffd"
_TRUNCATED = "truncated"
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


def encode_events[T](
    batch: list[dict[str, object]],
    encode: Callable[[dict[str, object]], T],
    pending: Deferral,
    *,
    sink: str,
    counter: str = "_unencodable",
) -> list[tuple[dict[str, object], T]]:
    """Encodes every event under its own guard, dropping the ones that cannot be encoded.

    A batch is the worker's unit of retry, so one event a sink cannot encode used to cost every
    event beside it — and in the sinks that hand events over one at a time it cost more, because
    the events ahead of it were already written when the encode raised and the retry wrote them
    again. Running this over the whole batch before the first write, produce, request or insert
    closes both: an event that cannot be encoded costs only itself, and nothing reaches the
    destination until every event has been through this step.

    The guard catches ``Exception`` and never ``BaseException``, so a ``KeyboardInterrupt``
    raised by an encode function leaves the call as itself, with nothing recorded. A failure is
    recorded on ``pending``, the calling ``emit``'s deferral, rather than on the sink: it is
    counted and announced only when that ``emit`` returns normally (SPEC-058, SPEC-059), so a
    raising call counts nothing and the worker's retry decides it afresh. The count goes to one
    named counter and the announcement is one ``_diag.lost`` line per call however many events
    failed, naming the sink and the first failure's exception **type** — never its message,
    which can quote the event, and never the event (invariant 11). Only the type name is kept,
    so no exception object, and with it no traceback holding the batch, outlives the loop.

    This is the HTTP family's ``HTTPSink._items`` rule written once for every other sink: an
    event the sink's own code cannot encode fails identically on every attempt, so a batch of
    nothing but such events settles rather than raising, and the caller sees an empty result.

    Args:
      batch: The events to encode, in order.
      encode: The sink's per-event encode function, returning the event's wire form.
      pending: The calling ``emit``'s deferral, which holds the count and the line until that
        ``emit`` is known not to raise.
      sink: The sink's class name, written on the line.
      counter: The owner's attribute the count is committed to.

    Returns:
      Each encodable event with its wire form, in batch order.

    Raises:
      None. A ``BaseException`` from ``encode`` propagates unchanged.
    """
    encoded: list[tuple[dict[str, object], T]] = []
    failures = 0
    kind = ""
    for event in batch:
        try:
            wire = encode(event)
        except Exception as err:
            failures += 1
            kind = kind or type(err).__name__
            continue
        encoded.append((event, wire))
    if failures:
        pending.count(counter, failures)
        pending.announce(lambda: _diag.lost("event", failures, f"{sink}, encoding raised {kind}"))
    return encoded


def repair_nul(event: dict[str, object]) -> dict[str, object] | None:
    """Replaces every U+0000 in the event's strings and keys with U+FFFD, on a marked copy.

    PostgreSQL refuses U+0000 in ``jsonb`` and in a text column, and one such value used to cost
    the whole batch (SPEC-069 FR-003). The repair runs on the strings before encoding rather
    than on the JSON text, so the JSONB document and the projected columns — top-level values of
    the same event, ``service`` and the other stamps included — carry one representation. Every
    string value and every string key is repaired at every depth; two keys that repair to the
    same text keep the later value, as assembly resolves two keys on one placeholder.

    The copy carries ``truncated: True``, the mark assembly sets when it replaces a lone
    surrogate with U+FFFD (invariant 8). The batch is never mutated: a ``MultiSink`` sibling and
    the worker's retry are handed the same objects.

    Args:
      event: The event to repair.

    Returns:
      The repaired, marked copy, or ``None`` when the event holds no U+0000.

    Raises:
      RecursionError: If the event nests deeper than the interpreter allows, which only a direct
        ``emit`` can hand a sink and the encode helper turns into a drop.
    """
    repaired, changed = _walk(event, values=True, ints=False)
    return _marked(repaired) if changed else None


def repair_bson(event: dict[str, object]) -> dict[str, object] | None:
    """Replaces what BSON refuses — a 64-bit overflow, a NUL in a key — on a marked copy.

    ``bson.encode`` refuses an integer outside ``[-2**63, 2**63 - 1]`` and a key holding
    U+0000, and one such value used to cost ``MongoDBSink`` the whole batch (SPEC-069 FR-003).
    The integer becomes assembly's too-large-number placeholder, through
    :func:`~log_foundry.sanitize.int_placeholder`, and the NUL in a key becomes U+FFFD. A NUL in
    a *value* is left, because BSON accepts it, and a ``bool`` is never read as an integer. The
    copy carries ``truncated: True``, as assembly marks the same substitutions (invariant 8),
    and the batch is never mutated.

    Args:
      event: The event to repair.

    Returns:
      The repaired, marked copy, or ``None`` when nothing in it needed repair.

    Raises:
      RecursionError: If the event nests deeper than the interpreter allows, as above.
    """
    repaired, changed = _walk(event, values=False, ints=True)
    return _marked(repaired) if changed else None


def _marked(repaired: object) -> dict[str, object]:
    """Sets the ``truncated`` mark on a repaired event.

    Args:
      repaired: The rebuilt top-level event, a ``dict`` because its input was one.

    Returns:
      The same dict, marked.

    Raises:
      None.
    """
    event = repaired if isinstance(repaired, dict) else {}
    event[_TRUNCATED] = True
    return event


def _walk(value: object, *, values: bool, ints: bool) -> tuple[object, bool]:
    """Rebuilds one value with the requested repairs, reporting whether anything changed.

    Containers are rebuilt — a ``dict`` as a ``dict``, a ``list`` or ``tuple`` as a ``list``,
    the JSON array either encodes as — and scalars are shared, so the input is never touched.
    Keys that are strings always have U+0000 replaced; ``values`` repairs string values too, and
    ``ints`` replaces an integer outside the signed 64-bit range.

    Args:
      value: The value to rebuild.
      values: Whether a string value's U+0000 is replaced.
      ints: Whether an out-of-range integer is replaced by its placeholder.

    Returns:
      The rebuilt value and whether any repair was made in it.

    Raises:
      RecursionError: If the value nests deeper than the interpreter allows.
    """
    if isinstance(value, str):
        if values and _NUL in value:
            return value.replace(_NUL, _REPLACEMENT), True
        return value, False
    if isinstance(value, bool):
        return value, False
    if isinstance(value, int):
        if ints and (int.__lt__(value, _INT64_MIN) or int.__gt__(value, _INT64_MAX)):
            return int_placeholder(value), True
        return value, False
    if isinstance(value, dict):
        rebuilt: dict[object, object] = {}
        changed = False
        for key, item in value.items():
            if isinstance(key, str) and _NUL in key:
                key = key.replace(_NUL, _REPLACEMENT)
                changed = True
            rebuilt[key], item_changed = _walk(item, values=values, ints=ints)
            changed = changed or item_changed
        return rebuilt, changed
    if isinstance(value, list | tuple):
        pairs = [_walk(item, values=values, ints=ints) for item in value]
        return [item for item, _ in pairs], any(changed for _, changed in pairs)
    return value, False
