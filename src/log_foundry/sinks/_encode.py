"""The one encode step every record-building sink runs before its first bulk call (SPEC-069)."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from log_foundry import _diag
from log_foundry.sanitize import int_placeholder

if TYPE_CHECKING:
    from collections.abc import Callable

    from log_foundry.sinks._deferral import Deferral

__all__ = [
    "NUL_ESCAPE",
    "SURROGATE_ESCAPE",
    "column_value",
    "encode_events",
    "encode_round",
    "encoded_landed",
    "refuse_surrogate",
    "repair_bson",
    "repair_nul",
]

NUL_ESCAPE = re.compile(r"(?<!\\)(?:\\\\)*\\u0000")
"""Matches a JSON ``\\u0000`` escape — a real U+0000 — and never the six characters as text.

A ``u0000`` counts only when the run of backslashes before it has odd length, so its last
backslash is the escape's own: ``json.dumps`` writes a real NUL as ``\\u0000`` and the six
characters ``\\u0000`` as ``\\\\u0000``, which JSONB accepts. It is a filter, not the decision:
:func:`repair_nul` walks the event for a real U+0000 and returns ``None`` when it finds none, so
correctness rests on the walk, and this search only keeps the walk off clean text and off the
six-character lookalike. Run over the JSON text ``PostgresSink`` already builds, so a clean event
costs one search (SPEC-069 FR-003).
"""

SURROGATE_ESCAPE = re.compile(r"(?<!\\)(?:\\\\)*\\ud[89a-f]")
"""Matches a JSON escape of a UTF-16 surrogate, ``\\ud800`` to ``\\udfff``, never the text.

The backslash rule is :data:`NUL_ESCAPE`'s. It is a filter, not the decision: a character outside
the Basic Multilingual Plane, an emoji, is written as a *pair* of such escapes, which every
destination accepts, so a match only sends the event to :func:`refuse_surrogate`'s strict
re-encode, which decides (SPEC-069 follow-up).
"""

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
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
        ok, wire = _guarded(encode, event)
        if not ok:
            failures += 1
            kind = kind or cast("str", wire)
            continue
        encoded.append((event, cast("T", wire)))
    if failures:
        pending.count(counter, failures)
        pending.announce(lambda: _diag.lost("event", failures, f"{sink}, encoding raised {kind}"))
    return encoded


def _guarded[T](
    encode: Callable[[dict[str, object]], T], event: dict[str, object]
) -> tuple[bool, object]:
    """Runs one event through ``encode`` under the one guard both encode steps share.

    ``Exception`` is caught and ``BaseException`` never is, so a ``KeyboardInterrupt`` leaves as
    itself. A failure keeps only its exception's **type name**, so no exception object, and no
    traceback holding the batch, outlives the call.

    Args:
      encode: The sink's per-event encode function.
      event: The event.

    Returns:
      ``(True, wire form)``, or ``(False, the exception's type name)``.

    Raises:
      None. A ``BaseException`` from ``encode`` propagates unchanged.
    """
    try:
        return True, encode(event)
    except Exception as err:
        return False, type(err).__name__


@dataclass(slots=True)
class _Cell:
    """One event occurrence's encode result, kept for the rest of its delivery run.

    Attributes:
      ok: Whether the event encoded.
      value: Its wire form, or the failure's type name.
      landed: Whether the sink has reported this occurrence accepted.
    """

    ok: bool
    value: object
    landed: bool = False


def encode_round[T](
    owner: object,
    batch: list[dict[str, object]],
    encode: Callable[[dict[str, object]], T],
    *,
    sink: str,
) -> list[tuple[int, T]]:
    """Encodes a delivery round's events once per run, returning each encodable one's wire form.

    The engine-mode sibling of :func:`encode_events` (SPEC-074 FR-002), sharing its guard. A
    run spans several ``deliver`` calls, so a ``Deferral`` and its ``_unencodable`` counter
    cannot carry the drop: every position this omits is unencodable and the caller makes it
    ``DROPPED``, which the engine counts in ``dropped_events`` only when the run settles. No
    count is made here. SPEC-069's line, unchanged in text, is registered on the run context as
    ``SETTLED``, once per run, on the round that first meets a failure.

    **Each event is encoded once per run.** Its wire form, or its failure, is kept in the run's
    ``notes(owner)`` keyed by event identity, and a later round looks it up rather than calling
    ``encode`` again — which keeps SPEC-069 FR-001's "``encode`` is called ``len(batch)`` times"
    across retries, and lets ``MongoDBSink`` resend the very document pymongo stamped an ``_id``
    into. An identity that appears more than once in a batch keeps one result per occurrence;
    each round hands an occurrence the first result of its identity that the sink has not
    reported landed (:func:`encoded_landed`), so a resend never reuses a landed occurrence's
    form. Outside any run (an immediate context) the batch is encoded once and the line written
    at once.

    Args:
      owner: The object whose run notes keep the results — the sink.
      batch: The events this round was handed, in order.
      encode: The sink's per-event encode function.
      sink: The sink's class name, written on the line.

    Returns:
      ``(position, wire form)`` for each encodable event, in batch order.

    Raises:
      None. A ``BaseException`` from ``encode`` propagates unchanged.
    """
    from log_foundry.sinks._engine import When, run_context

    context = run_context()
    notes = context.notes(owner)
    cache = cast("dict[int, tuple[object, list[_Cell]]]", notes.setdefault("encoded", {}))
    cells: list[_Cell] = []
    notes["encoded_round"] = cells
    taken: dict[int, int] = {}
    encoded: list[tuple[int, T]] = []
    failures = 0
    kind = ""
    for position, event in enumerate(batch):
        held = cache.get(id(event))
        if held is None or held[0] is not event:
            held = (event, [])
            cache[id(event)] = held
        live = [cell for cell in held[1] if not cell.landed]
        skip = taken.get(id(event), 0)
        taken[id(event)] = skip + 1
        if skip < len(live):
            cell = live[skip]
        else:
            ok, value = _guarded(encode, event)
            cell = _Cell(ok, value)
            held[1].append(cell)
            if not ok:
                failures += 1
                kind = kind or cast("str", value)
        cells.append(cell)
        if cell.ok:
            encoded.append((position, cast("T", cell.value)))
    if failures:
        lost = failures
        context.announce(
            lambda: _diag.lost("event", lost, f"{sink}, encoding raised {kind}"), when=When.SETTLED
        )
    return encoded


def unencodable_in_run(owner: object) -> int:
    """Returns how many event occurrences :func:`encode_round` has found unencodable this run.

    A round is handed only the events still pending, so the run's unencodable events are read
    from the run's cache rather than the round's batch. ``PostgresSink`` and the Redis sinks
    count them in ``failed`` with a batch they lose whole, as 1.x did (SPEC-074).

    Args:
      owner: The object :func:`encode_round` was given.

    Returns:
      The count, 0 outside a run that has encoded nothing.

    Raises:
      None.
    """
    from log_foundry.sinks._engine import run_context

    cache = cast(
        "dict[int, tuple[object, list[_Cell]]]", run_context().notes(owner).get("encoded", {})
    )
    found = 0
    for _, cells in cache.values():
        found += sum(1 for cell in cells if not cell.ok)
    return found


def encoded_landed(owner: object, positions: list[int]) -> None:
    """Records that the occurrences at these positions of the round's batch were accepted.

    Only a sink whose wire form is a mutable object it must not resend for another occurrence —
    ``MongoDBSink``'s document, which carries the ``_id`` pymongo stamped on it — needs to call
    it; :func:`encode_round` then hands a later round's occurrence of the same identity a form
    that has not landed.

    Args:
      owner: The object :func:`encode_round` was given.
      positions: Positions in the batch the current round was handed.

    Returns:
      None.

    Raises:
      None.
    """
    from log_foundry.sinks._engine import run_context

    cells = cast("list[_Cell]", run_context().notes(owner).get("encoded_round", []))
    for position in positions:
        if 0 <= position < len(cells):
            cells[position].landed = True


def column_value(value: object, *, int64: bool) -> object:
    """Returns a projected column's value as the driver can bind it, or as its JSON text.

    ``SQLiteSink`` and ``PostgresSink`` project a few top-level values into text columns beside
    the whole event's JSON, and a value the driver cannot bind used to cost the whole batch:
    ``sqlite3`` refuses a ``dict``, a ``list`` and an integer outside the signed 64-bit range
    (that one as a bare ``OverflowError``, nothing counted), and psycopg a ``dict`` and a
    ``list`` that is mixed, ragged or holds a ``dict``, each measured at ``a8e91fac``. A
    ``TransformSink`` can put any of them there, so the repair is admitted (*A destination's
    refusal is repaired at the sink*).

    A container, and with ``int64`` an integer outside the range, is bound as its JSON text, the
    text the event column already holds for it: the column is a filter beside that event, not its
    record, and the event is not changed, so nothing is marked, counted or announced. Every
    container takes the rule, including the lists psycopg did bind as array literals
    (``{1,2}``), so a column holds one representation whichever driver wrote it (Andrew's choice,
    2026-11-13, over a rule predicting which lists psycopg adapts). A ``str``, ``int``,
    ``float``, ``bool`` or ``None`` binds as before; the range test reads the value through
    ``int.__int__``, so no comparison a subclass defines runs.

    A string that is not UTF-8 — a lone surrogate, or a surrogate pair held as two code points,
    which neither driver encodes and which assembly replaces and ``configure()`` refuses, so only
    a direct ``emit`` brings one — is not repaired: it raises here, and the encode helper drops
    that event alone, counted and announced.

    Args:
      value: The column's value as the event holds it.
      int64: Whether the driver binds only signed 64-bit integers (``sqlite3``).

    Returns:
      ``value`` itself when the driver binds it, else its JSON text.

    Raises:
      UnicodeEncodeError: If ``value`` is a string holding a lone surrogate.
    """
    if value is None or isinstance(value, float):
        return value
    if isinstance(value, str):
        if not str.isascii(value):
            str.encode(value, "utf-8")
        return value
    if isinstance(value, int):
        number = int.__int__(value)
        if not int64 or _INT64_MIN <= number <= _INT64_MAX:
            return value
        return int.__repr__(number)
    return json.dumps(value)


def refuse_surrogate(event: dict[str, object], text: str) -> None:
    """Raises when an event holds a lone surrogate, which PostgreSQL refuses in JSONB and text.

    ``json.dumps`` writes a lone surrogate as a ``\\ud800`` escape, and PostgreSQL refuses the
    escape in ``jsonb`` (``InvalidTextRepresentation``) and psycopg the string in a text column
    (``UnicodeEncodeError``); either used to cost the whole transaction, measured against
    PostgreSQL 17.11 at ``a8e91fac``. Assembly replaces a lone surrogate, ``TransformSink``
    coerces one and ``configure()`` refuses one in a stamp, so only a direct ``emit`` brings one,
    and the admission rule repairs nothing the pipeline cannot produce: the event is dropped
    alone, counted and announced by the encode helper, rather than repaired.

    :data:`SURROGATE_ESCAPE` keeps a clean event to one search. On a match the event is encoded
    again without ASCII escapes and round-tripped through UTF-16, which fails only for a
    surrogate with no partner beside it. So an emoji, the text ``\\ud800`` written with an
    escaped backslash, and a surrogate pair held as two code points all pass: ``json.dumps``
    writes the last as the same two escapes as the emoji, which JSONB reads as one character.
    Only then is the text encoded as UTF-8, which raises with the lone surrogate named. A pair
    held as two code points in a *projected column* still fails, in ``column_value``, because
    psycopg encodes that column's string as UTF-8.

    Args:
      event: The event, as it will be written.
      text: Its JSON text, as ``json.dumps`` built it.

    Returns:
      None.

    Raises:
      UnicodeEncodeError: If a string value or key holds a lone surrogate.
    """
    if not SURROGATE_ESCAPE.search(text):
        return
    wide = json.dumps(event, ensure_ascii=False)
    try:
        wide.encode("utf-16-le", "surrogatepass").decode("utf-16-le")
    except UnicodeDecodeError:
        wide.encode("utf-8")


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
      repaired: The rebuilt top-level event. :func:`_walk` rebuilds a ``dict`` as a ``dict``
        and both callers hand it the event, so it is always one; the cast says so to the type
        checker rather than testing it, since a test with a fallback would turn an impossible
        shape into a silently emptied event.

    Returns:
      The same dict, marked.

    Raises:
      None.
    """
    event = cast("dict[str, object]", repaired)
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
