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

    The conversion is integer arithmetic on the parsed ``datetime``: scaling float seconds by
    ``1e9`` is inexact at current epochs, turning ``.789Z`` into ``...788999936`` ns, which
    truncates to the wrong millisecond. A naive timestamp is read as local time and anything
    :func:`epoch_seconds` would reject falls back to emit-time ``now``, exactly as there.

    Args:
      timestamp: The event's timestamp, of any type.

    Returns:
      The exact epoch nanoseconds, or emit-time ``now`` when the value is absent or unparseable.

    Raises:
      None.
    """
    if isinstance(timestamp, str):
        try:
            parsed = datetime.fromisoformat(timestamp).astimezone(UTC)
            return (parsed - _EPOCH) // _MICROSECOND * 1000
        except ValueError:
            pass
    return time.time_ns()
