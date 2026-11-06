"""The one encode step every record-building sink runs before its first bulk call (SPEC-069)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from log_foundry import _diag

if TYPE_CHECKING:
    from collections.abc import Callable

    from log_foundry.sinks._deferral import Deferral

__all__ = ["encode_events"]


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
        pending.announce(
            lambda: _diag.lost("event", failures, f"{sink}, encoding raised {kind}")
        )
    return encoded
