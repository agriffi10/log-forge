"""ISO-8601 → epoch helpers for sinks that need numeric time (SPEC-009)."""

from __future__ import annotations

import time
from datetime import datetime

__all__ = ["epoch_nanos", "epoch_seconds"]


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

    Args:
      timestamp: The event's timestamp, of any type.

    Returns:
      The epoch nanoseconds, derived from :func:`epoch_seconds`.

    Raises:
      None.
    """
    return int(epoch_seconds(timestamp) * 1_000_000_000)
