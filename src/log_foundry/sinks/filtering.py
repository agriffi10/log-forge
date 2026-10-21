"""FilteringSink — drop events by predicate and/or minimum level (arch §8, SPEC-006 FR-003)."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from log_foundry import _diag, _lifecycle
from log_foundry.sinks._deferral import Deferral, wrapped_losses
from log_foundry.sinks.base import flush_sink, read_losses

if TYPE_CHECKING:
    from collections.abc import Callable

    from log_foundry.sinks.base import Sink, SinkLosses

__all__ = ["FilteringSink"]

_LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}


class FilteringSink:
    """A :class:`~log_foundry.sinks.base.Sink` that forwards only events passing a filter.

    This is a static, emit-time filter in front of an inner sink, not the reserved
    tail-sampling ``should_send`` seam (arch §10) — that stays deferred and owns rate policy at
    span-decision time, while this only reshapes an already-built batch on its way to a sink.

    **A predicate that fails for one event costs only that event** (SPEC-058 FR-002). The
    predicate runs before the level floor, as it always has, so a below-floor event whose
    predicate raises is a failure rather than a quiet drop. A failure drops that event, the rest
    of the batch is forwarded, and the failures are counted in :meth:`losses` and announced once
    per batch, only once the inner sink's ``emit`` returned. When every event of a batch failed,
    the first exception is re-raised unchanged and nothing is counted, because whoever catches it
    owns the batch.

    It takes **no** transport lock (SPEC-028 FR-002) and **adds no post-close guard**
    (SPEC-032 FR-003): it holds no transport and its ``close()`` only forwards, so both decisions
    belong to the inner sink. A guard here would refuse batches the inner sink would have taken.


    It holds **no** client buffer of its own, but it **forwards** ``flush()`` to what it wraps
    (SPEC-036 FR-002). Holding nothing is not the same as having nothing to do: a wrapper that
    did not forward would leave a buffering child unreachable through ``log_foundry.flush()``
    while looking fine — the SPEC-027 lesson about ``log_foundry_stop_signal``, that a signal
    stopped at a wrapper reaches nothing and moves the defect rather than fixing it.
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
        self._counter_lock = threading.Lock()
        self._dropped = 0
        if min_level is not None:
            rank = _LEVELS.get(min_level.upper())
            if rank is None:
                raise ValueError(
                    f"unknown min_level: {min_level!r} (expected one of {list(_LEVELS)})"
                )
            self._min_rank = rank

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Forwards the events that clear both the predicate and the level floor, in order.

        When nothing passes, the inner sink's ``emit`` is not called at all (FR-003). Each event
        is tested under its own guard (SPEC-058 FR-002), which catches ``Exception`` and never
        ``BaseException``, so a predicate that raises for one event drops that event only. A
        ``False`` predicate or the level floor declines an event, which is not loss.

        The failures are counted and announced through the deferral helper, so they land only
        if this call returns normally: an inner sink that raises propagates with nothing counted,
        because the worker retries the whole batch and the predicate runs again.

        Args:
          batch: The events to filter.

        Returns:
          None.

        Raises:
          Exception: The predicate's first exception, unchanged and with nothing counted, when
            every event of the batch failed and none was declined. Otherwise whatever the inner
            sink's ``emit`` raises.
        """
        kept: list[dict[str, object]] = []
        declined = False
        failures = 0
        first: Exception | None = None
        with Deferral(self) as pending:
            for event in batch:
                try:
                    passes = self._passes(event)
                except Exception as err:
                    failures += 1
                    first = first if first is not None else err
                    continue
                if passes:
                    kept.append(event)
                else:
                    declined = True
            if first is not None and not kept and not declined:
                raise first
            if failures:
                kind = type(first).__name__
                pending.count("_dropped", failures)
                pending.announce(
                    lambda: _diag.lost(
                        "event", failures, f"FilteringSink, the predicate raised {kind}"
                    )
                )
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
          Exception: The predicate's own exception, propagated so :meth:`emit` can count it as
            that one event's failure (SPEC-058 FR-002).
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

    def flush(self) -> None:
        """Forwards the flush to the wrapped sink (SPEC-036 FR-002).

        This wrapper holds nothing itself, but a wrapper that did not forward would leave a
        buffering inner sink unreachable through ``log_foundry.flush()`` while looking fine —
        the SPEC-027 lesson that a signal stopped at a wrapper reaches nothing.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the wrapped sink raises, so the failure reaches the caller as a
            ``FlushResult`` reason rather than being swallowed here.
        """
        flush_sink(self._inner)

    def losses(self) -> SinkLosses | None:
        """Reports the inner sink's losses plus the events this wrapper's predicate failed on.

        A wrapper that reported nothing would hide the destination it wraps: ``health().sink``
        would read ``None`` for a filter in front of a sink that counts perfectly well
        (SPEC-026 FR-002). Events this sink itself declines to forward are not loss — they are
        the configuration working, the same reason ``NullSink`` reports nothing. Events the
        predicate *failed* on are: :meth:`emit` counts them once the batch is settled, and they
        are added to ``dropped`` here (SPEC-058 FR-003).

        Args:
          None.

        Returns:
          ``None`` only when the inner sink reports ``None`` and this wrapper has counted
          nothing: FR-003 distinguishes "the sink reports nothing" from "the sink reports no
          loss", and flattening the two would claim a clean bill of health on a sink that never
          gave one. Otherwise the inner sink's losses, an inner ``None`` read as zeros, with this
          wrapper's own count added to ``dropped`` (SPEC-058 FR-003).

        Raises:
          None.
        """
        inner = read_losses(self._inner)
        with self._counter_lock:
            own = self._dropped
        return wrapped_losses(inner, own)

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
