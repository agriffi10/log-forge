"""FilteringSink — drop events by predicate and/or minimum level (arch §8, SPEC-006 FR-003)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from log_foundry import _diag, _lifecycle
from log_foundry.sinks.base import read_losses

if TYPE_CHECKING:
    import threading
    from collections.abc import Callable

    from log_foundry.sinks.base import Sink, SinkLosses

__all__ = ["FilteringSink"]

_LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}


class FilteringSink:
    """A :class:`~log_foundry.sinks.base.Sink` that forwards only events passing a filter.

    This is a static, emit-time filter in front of an inner sink, not the reserved
    tail-sampling ``should_send`` seam (arch §10) — that stays deferred and owns rate policy at
    span-decision time, while this only reshapes an already-built batch on its way to a sink.

    It takes **no** transport lock (SPEC-028 FR-002) and **adds no post-close guard**
    (SPEC-032 FR-003): it holds no transport and its ``close()`` only forwards, so both decisions
    belong to the inner sink. A guard here would refuse batches the inner sink would have taken.
    """

    def __init__(
        self,
        inner: Sink,
        *,
        predicate: Callable[[dict[str, object]], bool] | None = None,
        min_level: str | None = None,
    ) -> None:
        """Binds the filter to an inner sink and its criteria.

        Args:
          inner: The sink that receives whatever passes.
          predicate: An event-level test, or ``None`` to apply none.
          min_level: The lowest severity to forward, compared case-insensitively, or ``None``.

        Returns:
          None.

        Raises:
          ValueError: If the minimum level is not one of the five standard names.
        """
        self._inner = inner
        self._stop_signal: threading.Event | None = None
        self._predicate = predicate
        self._min_rank: int | None = None
        if min_level is not None:
            rank = _LEVELS.get(min_level.upper())
            if rank is None:
                raise ValueError(
                    f"unknown min_level: {min_level!r} (expected one of {list(_LEVELS)})"
                )
            self._min_rank = rank

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Forwards the events that clear both the predicate and the level floor, in order.

        When nothing passes, the inner sink's ``emit`` is not called at all (FR-003).

        Args:
          batch: The events to filter.

        Returns:
          None.

        Raises:
          Exception: Whatever the predicate or the inner sink raises.
        """
        kept = [event for event in batch if self._passes(event)]
        if kept:
            self._inner.emit(kept)

    def _passes(self, event: dict[str, object]) -> bool:
        """Reports whether an event clears the predicate and the minimum level.

        A known level below the floor is dropped, while an unknown or missing level fails open
        and is forwarded.

        Args:
          event: The event to test.

        Returns:
          True when the event should be forwarded.

        Raises:
          Exception: Whatever the predicate raises.
        """
        if self._predicate is not None and not self._predicate(event):
            return False
        if self._min_rank is not None:
            level = event.get("level")
            rank = _LEVELS.get(level.upper()) if isinstance(level, str) else None
            if rank is not None and rank < self._min_rank:
                return False
        return True

    @property
    def log_foundry_stop_signal(self) -> threading.Event | None:
        """The worker's shutdown event, forwarded to whatever actually holds the retry loop.

        The worker sets this on the configured sink (SPEC-027 FR-002), and a wrapper is not
        where the waiting happens. Without the forward the attribute is set on an object that
        never waits, and the backoff one level down stays uninterruptible — which is the whole
        defect, moved rather than fixed.

        Args:
          None.

        Returns:
          The stop signal, or ``None`` if none was offered.

        Raises:
          None.
        """
        return self._stop_signal

    @log_foundry_stop_signal.setter
    def log_foundry_stop_signal(self, signal: threading.Event | None) -> None:
        """Forwards the stop signal to the inner sink.

        Args:
          signal: The worker's shutdown event, or ``None``.

        Returns:
          None.

        Raises:
          None.
        """
        self._stop_signal = signal
        try:
            self._inner.log_foundry_stop_signal = signal  # type: ignore[attr-defined]
        except Exception as err:
            _diag.absorbed(
                "handing the inner sink its stop signal",
                err,
                f"{type(self._inner).__name__} stays uninterruptible",
            )

    def losses(self) -> SinkLosses | None:
        """Reports the inner sink's losses (SPEC-026 FR-002).

        A wrapper that reported nothing would hide the destination it wraps: ``health().sink``
        would read ``None`` for a filter in front of a sink that counts perfectly well. Events
        this sink itself declines to forward are not loss — they are the configuration working,
        the same reason ``NullSink`` reports nothing.

        Args:
          None.

        Returns:
          The inner sink's losses. ``None`` passes through unchanged rather than becoming
          ``SinkLosses(0, 0)``: FR-003 distinguishes "the sink reports nothing" from "the sink
          reports no loss", and flattening the two would claim a clean bill of health on a sink
          that never gave one.

        Raises:
          None.
        """
        return read_losses(self._inner)

    def close(self) -> None:
        """Closes the inner sink (FR-003).

        Routed through ``_lifecycle.release`` so a sink this process may not release is refused
        here as it is at the lifecycle's own closers (SPEC-042 FR-002). The propagation is
        unchanged: the helper re-raises, and this method's ``Raises:`` is the reason it does.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the inner sink raises on close.
        """
        _lifecycle.release(self._inner, owner=self)
