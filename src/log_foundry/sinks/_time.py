"""ISO-8601 → epoch helpers for sinks that need numeric time (SPEC-009)."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

__all__ = ["epoch_nanos", "epoch_seconds"]

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MICROSECOND = timedelta(microseconds=1)


def epoch_seconds(timestamp: object) -> float:
    """Converts a SPEC-001 ISO-8601 timestamp to epoch seconds.

    An event's ``timestamp`` is an ISO-8601 string rather than an epoch, so sinks like Splunk
    HEC derive numeric time by parsing it.

    Args:
      timestamp: The event's timestamp, of any type.

    Returns:
      The epoch seconds, or emit-time ``now`` when the value is absent or unparseable.

    Raises:
      None.
    """
    if isinstance(timestamp, str):
        try:
            return datetime.fromisoformat(timestamp).timestamp()
        except ValueError:
            pass
    return time.time()


def epoch_nanos(timestamp: object) -> int:
    """Converts a SPEC-001 ISO-8601 timestamp to epoch nanoseconds, as Loki requires.

    Scaling float seconds by ``1e9`` is inexact at current epochs, turning ``.789Z`` into
    ``...788999936`` ns, which truncates to the wrong millisecond, so the conversion counts whole
    microseconds instead. An aware timestamp is subtracted from the epoch as it stands: converting
    it to UTC first overflows near ``datetime``'s year limits, where :func:`epoch_seconds` returns a
    value. A naive one is read as local time through the ``timestamp()`` call
    :func:`epoch_seconds` makes, taken on its whole seconds, which a float holds exactly; that call
    ignores the microseconds, so it rejects exactly the naive inputs :func:`epoch_seconds` rejects,
    and anything rejected falls back to emit-time ``now`` as there.

    Args:
      timestamp: The event's timestamp, of any type.

    Returns:
      The exact epoch nanoseconds, or emit-time ``now`` when the value is absent or unparseable.

    Raises:
      None.
    """
    if isinstance(timestamp, str):
        try:
            parsed = datetime.fromisoformat(timestamp)
            if parsed.utcoffset() is None:
                seconds = int(parsed.replace(microsecond=0).timestamp())
                return seconds * 1_000_000_000 + parsed.microsecond * 1000
            return (parsed - _EPOCH) // _MICROSECOND * 1000
        except ValueError:
            pass
    return time.time_ns()
