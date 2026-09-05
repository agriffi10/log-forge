"""Shared batch-chunking for the queue/stream sinks (SPEC-010)."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, TypeVar

from log_foundry.sinks._retry import require_positive

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

__all__ = ["chunk_items", "chunk_list", "valid_identifier"]

_T = TypeVar("_T")

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def chunk_list(items: list[_T], size: int) -> Iterator[list[_T]]:
    """Yields the items in successive slices of at most the given size.

    Args:
      items: The items to split.
      size: The maximum slice length, which must be positive.

    Returns:
      An iterator over the slices.

    Raises:
      ValueError: If the size is not positive, raised at the first ``next()`` because this is a
        generator (SPEC-049 FR-002) — ``range`` used to raise its own for ``0`` and yield nothing
        for a negative.
    """
    require_positive(size, "size", "chunk_list")
    for start in range(0, len(items), size):
        yield items[start : start + size]


def valid_identifier(name: str) -> str:
    """Returns a table name if it is a plain SQL identifier.

    Table names are config rather than untrusted input, but they are interpolated into DDL and
    DML because SQL cannot parameterize identifiers, so they are restricted to
    ``[A-Za-z_][A-Za-z0-9_]*`` to foreclose injection.

    Args:
      name: The proposed table name.

    Returns:
      The name unchanged.

    Raises:
      ValueError: If the name is not a plain SQL identifier.
    """
    if not _IDENTIFIER.match(name):
        raise ValueError(f"invalid table name {name!r}; expected a plain SQL identifier")
    return name


def chunk_items(
    items: list[_T], *, max_count: int, max_bytes: int, size_of: Callable[[_T], int]
) -> Iterator[list[_T]]:
    """Yields groups bounded by both a count limit and a total-byte limit.

    The worker batches by its own count and time, and each transport then re-chunks to its own
    per-request limits, assuming nothing about whether the incoming batch already fits. This
    mirrors the SPEC-005 ``SQSSink`` conventions.

    Args:
      items: The items to group.
      max_count: The most items one group may hold.
      max_bytes: The most bytes one group's sizes may sum to.
      size_of: Measures one item. Each item is assumed to fit one request on its own, so
        oversized items are the caller's responsibility to filter out beforehand, counting
        ``dropped_oversized``.

    Returns:
      An iterator over the groups.

    Raises:
      Exception: Whatever ``size_of`` raises.
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
