"""SPEC-099's reading of a red soak run against the known shapes; not a clearing file.

``tests/soak_known.py`` holds the rows a clearing pull request writes; the code that decides
whether a run's findings are all known lives here, outside the clearing files, so a clearing pull
request cannot widen what reads as known through this machinery (FR-002). The findings are
judged by the soak's own two rules, ``model_oracles.check_unowed_repeat`` and
``check_late_write``, so the soak item and this reading can never disagree about what was found.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from model_oracles import CloseBroken, check_late_write, check_unowed_repeat
from soak_known import KNOWN, Known

if TYPE_CHECKING:
    from collections.abc import Sequence

    from model_oracles import Closes, SinkEvent

KNOWN_PREFIX = "known soak finding:"
"""How a known soak item's reason starts; the nightly report's floor reads it."""

KINDS = ("repeat", "late-write")
"""The signature kinds a row may explain. Any other row, ``untaken`` above all, is never honoured.

``untaken`` is a close with no take to discharge, a wiring failure of the soak itself, and a row
in the clearing file claiming to explain it would excuse a broken record; the kinds are held here,
outside the clearing file, so a clearing pull request cannot widen them.
"""


def signatures_of(events: Sequence[SinkEvent], closes: Closes) -> list[str]:
    """Returns one sink's soak signatures, from both of the soak's rules.

    Args:
      events: The sink's events, in the order recorded.
      closes: The sink's close record.

    Returns:
      Every ``CloseBroken:<kind>@<count>`` either rule raised, repeat rule first.

    Raises:
      None.
    """
    found: list[str] = []
    try:
        check_unowed_repeat(events)
    except CloseBroken as exc:
        found.extend(exc.signatures)
    try:
        check_late_write(closes)
    except CloseBroken as exc:
        found.extend(exc.signatures)
    return found


def explain(
    signatures: Sequence[str],
    events: Sequence[SinkEvent],
    closes: Closes,
    known: Sequence[Known] = KNOWN,
) -> list[Known]:
    """Returns the known shape behind each of one sink's signatures, or ``[]`` if any is new.

    A signature is explained by the first row of its kind whose predicate holds for the sink. Only
    rows of a kind in :data:`KINDS` are read, so ``untaken`` is never explained, whatever row a
    clearing pull request plants.

    Args:
      signatures: The sink's ``CloseBroken:<kind>@<count>`` signatures.
      events: Its events.
      closes: Its close record.
      known: The rows to read against; the live ``KNOWN`` unless a test plants another.

    Returns:
      The shapes, each once, or an empty list when any signature has none.

    Raises:
      None.
    """
    found: list[Known] = []
    for signature in signatures:
        kind = signature.removeprefix("CloseBroken:").split("@", 1)[0]
        shape = next(
            (
                row
                for row in known
                if row.kind in KINDS and row.kind == kind and row.predicate(events, closes)
            ),
            None,
        )
        if shape is None:
            return []
        if shape not in found:
            found.append(shape)
    return found


def explain_run(
    sinks: Sequence[tuple[Sequence[SinkEvent], Closes]], known: Sequence[Known] = KNOWN
) -> list[Known]:
    """Returns the known shapes behind a red soak run, or ``[]`` if any sink's finding is new.

    Args:
      sinks: Each sink's events and close record, as the soak judged them.
      known: The rows to read against; the live ``KNOWN`` unless a test plants another.

    Returns:
      The shapes, each once, or an empty list when the run found nothing or any sink found
      something with no known shape, so one known sink beside an unknown one is never known.

    Raises:
      None.
    """
    found: list[Known] = []
    for events, closes in sinks:
        signatures = signatures_of(events, closes)
        if not signatures:
            continue
        shapes = explain(signatures, events, closes, known)
        if not shapes:
            return []
        found.extend(shape for shape in shapes if shape not in found)
    return found


def reason(known: Sequence[Known]) -> str:
    """Returns the one-line reason a known red soak item reports.

    Args:
      known: The shapes; a repeated one is named once.

    Returns:
      ``known soak finding: <name> (§12: <item>)``, joined by ``; `` for each shape.

    Raises:
      None.
    """
    unique: list[Known] = []
    for shape in known:
        if shape not in unique:
            unique.append(shape)
    return f"{KNOWN_PREFIX} " + "; ".join(f"{k.name} (§12: {k.item})" for k in unique)
