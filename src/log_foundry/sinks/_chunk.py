"""Shared batch-chunking for the queue/stream sinks (SPEC-010).

The worker batches by its own count/time; each transport then re-chunks to *its* per-request limits
(no assumption the incoming batch already fits). This mirrors the SPEC-005 ``SQSSink`` conventions:
split into groups within a count limit **and** a total-byte limit, assuming each item already fits a
single request (callers drop oversized items first, counting ``dropped_oversized``).
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterator
from typing import TypeVar

__all__ = ["chunk_items", "chunk_list", "valid_identifier"]

_T = TypeVar("_T")

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def chunk_list(items: list[_T], size: int) -> Iterator[list[_T]]:
    """Yield ``items`` in successive slices of at most ``size`` (a positive chunk size)."""
    for start in range(0, len(items), size):
        yield items[start : start + size]


def valid_identifier(name: str) -> str:
    """Return ``name`` if it is a plain SQL identifier, else raise ``ValueError``.

    Table names are config, not untrusted input, but they are interpolated into DDL/DML (SQL cannot
    parameterize identifiers), so restrict them to ``[A-Za-z_][A-Za-z0-9_]*`` to foreclose injection.
    """
    if not _IDENTIFIER.match(name):
        raise ValueError(f"invalid table name {name!r}; expected a plain SQL identifier")
    return name


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
