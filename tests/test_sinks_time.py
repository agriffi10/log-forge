"""ISO-8601 → epoch helpers: exact nanoseconds, and agreement with the seconds twin."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import pytest

from log_foundry.sinks._time import epoch_nanos, epoch_seconds

if TYPE_CHECKING:
    from collections.abc import Iterator


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


@pytest.fixture
def local_time_is_utc(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Pins the process's local zone to UTC, so a naive timestamp has one exact answer."""
    monkeypatch.setenv("TZ", "UTC")
    time.tzset()
    yield
    monkeypatch.undo()
    time.tzset()


@pytest.mark.usefixtures("local_time_is_utc")
@pytest.mark.parametrize(
    ("timestamp", "expected"),
    [
        ("2026-10-05T12:34:56.789", 1791203696789000000),
        ("2026-10-05T12:34:56.789123", 1791203696789123000),
        ("1969-12-31T23:59:59.5", -500000000),
        ("9999-12-31T23:59:59.999999", 253402300799999999000),
    ],
)
def test_epoch_nanos_is_exact_for_a_naive_timestamp(timestamp: str, expected: int) -> None:
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

    The fixed 100 us is about three times a float's spacing at year 9999 (2**-15 s), the most
    ``epoch_seconds`` can be off by. The clock time between the two calls is added because it is
    what separates two reads of the clock when both helpers fall back, taken as a magnitude so a
    wall clock stepping backward inside the window cannot fail a pair that agrees exactly. This
    test cannot see the float defect itself, which is under 100 us; the exact tests above can.
    """
    before = time.time_ns()
    nanos = epoch_nanos(timestamp)
    seconds = epoch_seconds(timestamp)
    elapsed = abs(time.time_ns() - before)
    assert abs(nanos - seconds * 1e9) <= 100_000 + elapsed
