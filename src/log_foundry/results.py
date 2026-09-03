"""Result types for the two public calls that answered five questions with one bit (SPEC-034)."""

from __future__ import annotations

from dataclasses import dataclass

__all__ = ["ContinueResult", "FlushResult"]


@dataclass(frozen=True, kw_only=True)
class _Result:
    """A verdict that reads as a boolean and can say why (SPEC-034 FR-007).

    ``flush()`` returned one bit for five distinct outcomes — timed out, worker retired, drain
    thread died, queue too full for the marker, a batch abandoned — and a Lambda handler needs
    "the worker is retired, my code is wrong" separated from "the sink is slow". A ``NamedTuple``
    cannot be retrofitted here: a non-empty tuple is always truthy, so every ``if flush():``
    would silently start passing.

    The type exists now, before ``1.0.0``, precisely so that later reasons are additive. It grows
    by new ``reason`` values and never by changing :meth:`__bool__`.

    Attributes:
      ok: Whether the operation succeeded, and what ``bool()`` reports.
      reason: A short stable token naming *why* when it did not, or ``None`` when it did. New
        tokens may appear in any release; code should branch on ``bool()`` and treat an unknown
        reason as "some other failure" rather than matching exhaustively.
    """

    ok: bool
    reason: str | None = None

    def __bool__(self) -> bool:
        """Reports the verdict, so every existing ``if flush():`` keeps its meaning.

        Args:
          None.

        Returns:
          Whether the operation succeeded.

        Raises:
          None.
        """
        return self.ok


@dataclass(frozen=True)
class FlushResult(_Result):
    """What :func:`log_foundry.flush` returns.

    ``reason`` is ``None`` on success. The tokens it can carry today are ``"timed-out"``,
    ``"retired"``, ``"thread-died"``, ``"queue-full"``, ``"abandoned"`` and ``"sink-flush"``.

    ``"sink-flush"`` is the one SPEC-036 added (FR-002 AC-8) and it is worth distinguishing: the
    queue drained cleanly and the **sink's own client buffer** did not, so the events are past
    this library and inside a driver. ``"abandoned"`` is the neighbouring case where this call
    could not confirm they were handed over — it has three producers (a drain that spent its
    retries, an expiring ``shutdown()`` answering pessimistically, and a marker that arrived
    after that), so it means "not confirmed delivered" and never "confirmed lost" (SPEC-050
    FR-001). ``ok=True`` carries the guarantee instead: the sink took them. New tokens may appear
    in any release, which is what this type exists for — branch on ``bool()``.
    """


@dataclass(frozen=True)
class ContinueResult(_Result):
    """What :func:`log_foundry.continue_trace` returns.

    ``reason`` is ``None`` on success. Today it distinguishes ``"nothing-supplied"`` — no
    argument carried a context — from ``"rejected"``, which means something *was* supplied and
    was malformed. Those two read identically as ``False``, and the second is a caller bug where
    the first is often a deliberate "continue if there is one to continue".
    """
