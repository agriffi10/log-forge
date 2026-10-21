"""The deferral rule: a call's per-event drops count only if that call returns (SPEC-058)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, Self

from log_foundry.sinks.base import SinkLosses

if TYPE_CHECKING:
    import threading
    from collections.abc import Callable
    from types import TracebackType

__all__ = ["Deferral", "wrapped_losses"]


class _Counted(Protocol):
    """An owner whose loss counters a deferral commits into, under the owner's counter lock.

    Every shipped owner already guards its other counters with this one lock, which is the point:
    a deferred ``dropped_oversized`` increment and an immediate one from ``_drop_refused`` are
    serialised by the same lock, where two locks guarding one counter would lose increments.

    Attributes:
      _counter_lock: The lock every write to the owner's counters takes.
    """

    _counter_lock: threading.Lock


class Deferral:
    """One call's pending drops, counted and announced only if the call returns normally.

    The principle SPEC-058 states once — a raising ``emit`` counts nothing, and whoever catches
    the raise owns the batch's count — seen from inside a sink. The worker retries a raised batch
    whole, so the same per-event drops are decided again on the next attempt, and counting them
    inside a raising ``emit`` counts each one once per attempt. The other catchers (the orphan
    path, ``MultiSink``) count the batch themselves, so a count here as well would put one event
    in two terms.

    **One instance per call, never stored.** It is constructed as a ``with`` item inside
    ``emit``, so its pending counts and lines are local to that call: the drain thread and any
    number of orphan callers on other threads never share them (SPEC-028). Shared state is
    touched only in the commit, under the owner's ``_counter_lock``, and the AST check in
    ``tests/test_deferral.py`` refuses a construction anywhere else.

    **The lock is read from the owner at commit time**, not held from construction. A fork
    repair rebinds an owned instance's lock attributes, so a commit after the repair must take
    the fresh lock; reading it late is what makes that true even for a fork made from inside
    the call itself, by a transform function or a predicate.

    **The commit never runs under the lock it announces from.** Counts are committed first,
    under the lock, then every announcement runs outside it, in the order it was registered —
    the counter moves before its line, which is ``_diag.lost``'s own contract, and a
    ``health()`` poll never waits on a stderr write. A call with nothing pending takes no lock
    at all, which keeps the hot path lock-free and the existing acquisition-count tests exact.
    """

    __slots__ = ("_counts", "_lines", "_owner")

    def __init__(self, owner: _Counted) -> None:
        """Starts an empty ledger for one call on ``owner``.

        Args:
          owner: The sink whose counters a clean exit commits into.

        Returns:
          None.

        Raises:
          None.
        """
        self._owner = owner
        self._counts: dict[str, int] = {}
        self._lines: list[Callable[[], None]] = []

    def count(self, counter: str, n: int = 1) -> None:
        """Adds ``n`` to the owner's ``counter``, pending the call's clean exit.

        The counter is named rather than typed, so one helper serves five owner types; the cost
        is that mypy cannot check the name, and a misspelt one raises ``AttributeError`` from the
        commit on the first clean call that drops something, which every site's tests exercise.
        A zero is not recorded, so it cannot make a clean exit take the lock for nothing.

        Args:
          counter: The owner's attribute to increment.
          n: How much to add.

        Returns:
          None.

        Raises:
          None.
        """
        if n:
            self._counts[counter] = self._counts.get(counter, 0) + n

    def announce(self, line: Callable[[], None]) -> None:
        """Registers a diagnostic line to write once the counts are committed.

        A site passes a ``lambda`` wrapping its own ``_diag.lost(...)`` call rather than the
        pieces of one, so the detail string stays inside a ``_diag`` call where
        ``tests/test_diag.py`` reads every call site for a ``repr`` — moving the f-string here
        would hide it from that check.

        Args:
          line: Called with no arguments after the commit, outside the owner's lock.

        Returns:
          None.

        Raises:
          None.
        """
        self._lines.append(line)

    def __enter__(self) -> Self:
        """Returns the ledger the ``with`` block records into.

        Args:
          None.

        Returns:
          This deferral.

        Raises:
          None.
        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Commits and announces on a clean exit, and discards everything on a raise.

        A ``return`` inside the block is a clean exit and commits. A raise of any kind —
        ``BaseException`` included — commits nothing and is never suppressed, so the exception
        propagates by identity with its own traceback.

        Args:
          exc_type: The type of the exception leaving the block, or ``None``.
          exc: The exception itself, or ``None``.
          tb: Its traceback, or ``None``.

        Returns:
          None, so nothing is suppressed.

        Raises:
          AttributeError: If a counter named to :meth:`count` is not an attribute of the owner.
        """
        if exc_type is not None:
            return
        if self._counts:
            owner = self._owner
            with owner._counter_lock:
                for name, n in self._counts.items():
                    setattr(owner, name, getattr(owner, name) + n)
        for line in self._lines:
            line()


def wrapped_losses(inner: SinkLosses | None, own: int) -> SinkLosses | None:
    """Combines a wrapper's own drop count with what its inner sink reports (SPEC-058 FR-003).

    The rule ``MultiSink.losses`` applies to a child that reports nothing (SPEC-036 FR-005): an
    inner ``None`` reads as zeros once the wrapper has something of its own to say, and stays
    ``None`` while it has not, so "reports nothing" is still told apart from "reports no loss".
    Written once here because two wrappers share it.

    Args:
      inner: What the wrapped sink reports, through ``read_losses``.
      own: The wrapper's own drop count.

    Returns:
      ``None`` only when the inner sink reports ``None`` and ``own`` is zero; the inner report
      unchanged when ``own`` is zero; otherwise ``own`` added to ``dropped`` and ``failed``
      passed through.

    Raises:
      None.
    """
    if not own:
        return inner
    if inner is None:
        return SinkLosses(dropped=own, failed=0)
    return SinkLosses(dropped=inner.dropped + own, failed=inner.failed)
