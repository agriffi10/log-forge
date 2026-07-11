"""Shared batch-chunking for the queue/stream sinks (SPEC-010).

The worker batches by its own count/time; each transport then re-chunks to *its* per-request limits
(no assumption the incoming batch already fits). This mirrors the SPEC-005 ``SQSSink`` conventions:
split into groups within a count limit **and** a total-byte limit, assuming each item already fits a
single request (callers drop oversized items first, counting ``dropped_oversized``).
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import TypeVar

__all__ = ["chunk_items"]

_T = TypeVar("_T")


def chunk_items(
    items: list[_T], *, max_count: int, max_bytes: int, size_of: Callable[[_T], int]
) -> Iterator[list[_T]]:
    """Yield groups of ≤ ``max_count`` items whose ``size_of`` sizes sum to ≤ ``max_bytes``.

    Each item is assumed to fit one request on its own (``size_of(item) <= max_bytes``); oversized
    items are the caller's responsibility to filter out beforehand (counting ``dropped_oversized``).
    """
    current: list[_T] = []
    current_bytes = 0
    for item in items:
        size = size_of(item)
        if current and (len(current) >= max_count or current_bytes + size > max_bytes):
            yield current
            current = []
            current_bytes = 0
        current.append(item)
        current_bytes += size
    if current:
        yield current
