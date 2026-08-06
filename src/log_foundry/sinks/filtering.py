"""FilteringSink — drop events by predicate and/or minimum level (arch §8, SPEC-006 FR-003).

A static, emit-time filter in front of an inner sink: forward only the events that satisfy a
``predicate`` and/or meet a ``min_level``. This is *not* the reserved tail-sampling
``should_send`` seam (arch §10) — that stays deferred and owns rate policy at span-decision
time; this only reshapes an already-built batch on its way to a sink.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from log_foundry import _diag
from log_foundry.sinks.base import read_losses

if TYPE_CHECKING:
    import threading
    from collections.abc import Callable

    from log_foundry.sinks.base import Sink, SinkLosses

__all__ = ["FilteringSink"]

# Standard severity ranks; higher is more severe. Level names are the arch §6 set.
_LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}


class FilteringSink:
    """A :class:`~log_foundry.sinks.base.Sink` that forwards only events passing a filter.

    Wraps ``inner`` and forwards the subset of each batch that satisfies ``predicate`` and/or
    meets ``min_level`` (both, when both are given). An event whose ``level`` is unknown or
    missing is forwarded fail-open rather than dropped; when nothing passes, ``inner.emit`` is
    not called. Level comparison is case-insensitive.

    Raises:
        ValueError: if ``min_level`` is not one of the five standard names (case-insensitive).
    """

    def __init__(
        self,
        inner: Sink,
        *,
        predicate: Callable[[dict[str, object]], bool] | None = None,
        min_level: str | None = None,
    ) -> None:
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
        """Forward the events that clear both the predicate and min_level, in order (FR-003)."""
        kept = [event for event in batch if self._passes(event)]
        if kept:
            self._inner.emit(kept)

    def _passes(self, event: dict[str, object]) -> bool:
        """Whether ``event`` clears the predicate and the minimum level (fail-open on level)."""
        if self._predicate is not None and not self._predicate(event):
            return False
        if self._min_rank is not None:
            level = event.get("level")
            rank = _LEVELS.get(level.upper()) if isinstance(level, str) else None
            # A known level below the floor is dropped; an unknown/missing level fails open.
            if rank is not None and rank < self._min_rank:
                return False
        return True

    @property
    def stop_signal(self) -> threading.Event | None:
        """The worker's shutdown event, forwarded to whatever actually holds the retry loop.

        The worker sets this on the *configured* sink (SPEC-027 FR-002), and a wrapper is not
        where the waiting happens. Without the forward the attribute is set on an object that
        never waits, and the backoff one level down stays uninterruptible — which is the whole
        defect, moved rather than fixed.
        """
        return self._stop_signal

    @stop_signal.setter
    def stop_signal(self, signal: threading.Event | None) -> None:
        self._stop_signal = signal
        try:
            self._inner.stop_signal = signal  # type: ignore[attr-defined]
        except Exception as err:
            _diag.absorbed(
                "handing the inner sink its stop signal",
                err,
                f"{type(self._inner).__name__} stays uninterruptible",
            )

    def losses(self) -> SinkLosses | None:
        """Report the inner sink's losses (SPEC-026 FR-002). Never raises.

        A wrapper that reported nothing would hide the destination it wraps: ``health().sink``
        would read ``None`` for a ``FilteringSink`` in front of a sink that counts perfectly well.
        Events this sink itself declines to forward are not loss — they are the configuration
        working, the same reason ``NullSink`` reports nothing.

        ``None`` passes through unchanged rather than becoming ``SinkLosses(0, 0)``: FR-003
        distinguishes "the sink reports nothing" from "the sink reports no loss", and a wrapper
        that flattened the two would claim a clean bill of health on a sink that never gave one.
        """
        return read_losses(self._inner)

    def close(self) -> None:
        """Close the inner sink (FR-003)."""
        self._inner.close()
