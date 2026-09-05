"""SPEC-049, system-frame review — `MemorySink(maxlen=)` refuses a bound that keeps nothing."""

from __future__ import annotations

import pytest

from log_foundry.sinks.memory import MemorySink


@pytest.mark.parametrize("bad", [0, -1])
def test_a_non_positive_maxlen_is_refused(bad: int) -> None:
    """`0` and every negative constructed and emptied the list on every emit — not a ring."""
    with pytest.raises(ValueError, match="MemorySink maxlen must be a positive integer"):
        MemorySink(maxlen=bad)


def test_a_positive_maxlen_is_a_ring_and_none_keeps_everything() -> None:
    ring = MemorySink(maxlen=2)
    ring.emit([{"i": 1}, {"i": 2}, {"i": 3}])
    assert ring.events == [{"i": 2}, {"i": 3}]
    unbounded = MemorySink()
    unbounded.emit([{"i": 1}, {"i": 2}, {"i": 3}])
    assert len(unbounded.events) == 3
