"""ISO-8601 → epoch helpers for sinks that need numeric time (SPEC-009).

A SPEC-001 ``LogEvent``'s ``timestamp`` is an ISO-8601 *string* (``2026-07-11T00:00:00.123Z``), not
an epoch. Sinks like Loki (nanoseconds) and Splunk HEC (epoch seconds) derive numeric time by
parsing that string, falling back to emit-time ``now`` when it is absent or unparseable.
"""

from __future__ import annotations

import time
from datetime import datetime

__all__ = ["epoch_seconds", "epoch_nanos"]


def epoch_seconds(timestamp: object) -> float:
    """Epoch seconds from a SPEC-001 ISO-8601 string; emit-time ``now`` if absent/unparseable."""
    if isinstance(timestamp, str):
        try:
            return datetime.fromisoformat(timestamp).timestamp()
        except ValueError:
            pass
    return time.time()


def epoch_nanos(timestamp: object) -> int:
    """Epoch nanoseconds (as required by Loki), derived from :func:`epoch_seconds`."""
    return int(epoch_seconds(timestamp) * 1_000_000_000)
