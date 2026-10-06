"""ISO-8601 → epoch helpers: exact nanoseconds, and agreement with the seconds twin."""

from __future__ import annotations

import time

import pytest

from log_foundry.sinks._time import epoch_nanos, epoch_seconds


@pytest.mark.parametrize(
    ("timestamp", "expected"),
    [
        ("2026-10-05T12:34:56.789Z", 1791203696789000000),
        ("2026-10-05T12:34:56.123Z", 1791203696123000000),
        ("2026-10-05T12:34:56.001Z", 1791203696001000000),
        ("2026-10-05T12:34:56.999Z", 1791203696999000000),
        ("1970-01-01T00:00:00.001Z", 1000000),
        ("1969-12-31T23:59:59.999Z", -1000000),
        ("0001-01-01T00:00:00+01:00", -62135600400000000000),
        ("9999-12-31T23:59:59-01:00", 253402304399000000000),
    ],
)
def test_epoch_nanos_is_exact(timestamp: str, expected: int) -> None:
    assert epoch_nanos(timestamp) == expected


@pytest.mark.parametrize(
    "timestamp",
    [
        "2026-10-05T12:34:56.789Z",
        "2026-10-05T12:34:56.789+02:00",
        "2026-10-05T12:34:56.789-23:59:59.999999",
        "2026-10-05T12:34:56.789",
        "2026-10-05",
        "0001-01-01T00:00:00Z",
        "0001-01-01T00:00:00+01:00",
        "0001-01-01T00:00:00",
        "0001-01-02T00:00:00",
        "9999-12-31T23:59:59.999999Z",
        "9999-12-31T23:59:59-01:00",
        "9999-12-31T23:59:59.999",
        "9999-12-30T00:00:00",
        "garbage",
        "",
        "2026-10-05T12:34:56\x00",
        None,
        5,
        b"2026-10-05T12:34:56.789Z",
    ],
)
def test_epoch_nanos_agrees_with_epoch_seconds_and_never_raises(timestamp: object) -> None:
    """Each input lands where the seconds twin puts it, including both falling back to ``now``.

    The tolerance is a float's spacing at year 9999 plus the time between the two calls, which
    is what separates two reads of the clock when both helpers fall back.
    """
    before = time.time_ns()
    nanos = epoch_nanos(timestamp)
    seconds = epoch_seconds(timestamp)
    elapsed = time.time_ns() - before
    assert abs(nanos - seconds * 1e9) <= 100_000 + elapsed
