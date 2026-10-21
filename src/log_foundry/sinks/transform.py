"""TransformSink — reshape or redact each event before forwarding (arch §8, SPEC-006 FR-004)."""

from __future__ import annotations

import json
import threading
from typing import TYPE_CHECKING

from log_foundry import _diag, _lifecycle
from log_foundry.sanitize import _coerce_unbounded
from log_foundry.sinks._deferral import Deferral, wrapped_losses
from log_foundry.sinks.base import flush_sink, read_losses

if TYPE_CHECKING:
    from collections.abc import Callable

    from log_foundry.sinks.base import Sink, SinkLosses

__all__ = ["TransformSink"]


def _check(result: object) -> None:
    """Raises unless ``result`` meets invariant 8's observable, in one expression (SPEC-058 FR-001).

    ``allow_nan=False`` and the UTF-8 encode are what make it the stricter of the two readings:
    a NaN and a lone surrogate both pass a bare ``json.dumps``, and the second is what the SQL
    sinks' drivers reject once it reaches a column (SPEC-055).

    Args:
      result: What the transform returned, or its coercion.

    Returns:
      None.

    Raises:
      Exception: Whatever ``json.dumps`` or the encode raises — ``TypeError``, ``ValueError``,
        ``UnicodeEncodeError``, a ``RecursionError`` on a nest deeper than the stack allows, or a
        ``dict`` subclass's own ``items()`` failure.
    """
    json.dumps(result, allow_nan=False, ensure_ascii=False).encode("utf-8")


def _json_safe(result: dict[str, object]) -> dict[str, object]:
    """Returns ``result`` itself when it passes the check, else a new types-only coercion of it.

    Check first, so a valid result is untouched by construction and only one that already failed
    ever meets the coercer. The coercion runs **outside** the ``except`` block, so an exception it
    or the post-check raises is not chained onto the check's own: it is the exception SPEC-058
    FR-002's rule carries, and its type is what the wrapper's line names. ``truncated`` is set
    only when the coercion made a substitution assembly marks; a coercion is not loss, so nothing
    is counted and nothing is announced here.

    Args:
      result: The mapping the transform returned. It is never mutated.

    Returns:
      ``result`` when it passes, or the coerced mapping.

    Raises:
      Exception: Whatever the coercion raises, or the post-coercion check when the coercion did
        not rescue the result.
    """
    try:
        _check(result)
    except Exception:
        pass
    else:
        return result
    coerced, marked = _coerce_unbounded(result)
    if marked:
        coerced["truncated"] = True
    _check(coerced)
    return coerced


class TransformSink:
    """A :class:`~log_foundry.sinks.base.Sink` that maps each event before forwarding.

    The function runs on every event on its way out — to redact a field, add host metadata,
    rename keys — and returning ``None`` drops that event. The caller's batch and event dicts
    are never mutated in place: only the function's return values are forwarded, so a transform
    must copy before mutating.

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
        fn: Callable[[dict[str, object]], dict[str, object] | None],
    ) -> None:
        """Binds the transform to an inner sink and its mapping function.

        Args:
          inner: The sink that receives the transformed events.
          fn: Applied to every event, returning the replacement or ``None`` to drop it.

        Returns:
          None.

        Raises:
          None.
        """
        self._inner = inner
        self._stop_signal: threading.Event | None = None
        self._fn = fn
        self._counter_lock = threading.Lock()
        self._dropped = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Applies the function to each event, forwarding the non-``None`` results (FR-004).

        When every event is dropped, the inner sink's ``emit`` is not called at all.

        Args:
          batch: The events to transform.

        Returns:
          None.

        Raises:
          Exception: Whatever the mapping function or the inner sink raises.
        """
        forwarded: list[dict[str, object]] = []
        declined = False
        failures = 0
        first: Exception | None = None
        with Deferral(self) as pending:
            for event in batch:
                try:
                    result = self._apply(event)
                except Exception as err:
                    failures += 1
                    first = first if first is not None else err
                    continue
                if result is None:
                    declined = True
                else:
                    forwarded.append(result)
            if first is not None and not forwarded and not declined:
                raise first
            if failures:
                kind = type(first).__name__
                pending.count("_dropped", failures)
                pending.announce(
                    lambda: _diag.lost(
                        "event", failures, f"TransformSink, the function raised {kind}"
                    )
                )
            if forwarded:
                self._inner.emit(forwarded)

    def _apply(self, event: dict[str, object]) -> dict[str, object] | None:
        """Runs the function on one event and makes its result safe to forward (SPEC-058).

        A result that is neither ``None`` nor a ``dict`` is the same failure as a raise, so the
        ``TypeError`` is built here — naming this class and the type returned, never the value —
        and raised into :meth:`emit`'s per-event guard. Raising in a helper rather than inside
        that guard's ``try`` keeps the ``try`` body to the one call that can fail.

        Args:
          event: The event to transform.

        Returns:
          ``None`` when the function declined the event, else its result, checked and, when it
          fails the check, coerced (FR-001).

        Raises:
          TypeError: If the function returned neither ``None`` nor a ``dict``.
          Exception: Whatever the function raises, or the coercion when it cannot rescue the
            result.
        """
        result = self._fn(event)
        if result is None:
            return None
        if not isinstance(result, dict):
            raise TypeError(
                f"TransformSink's function returned {type(result).__name__}, not a dict or None"
            )
        return _json_safe(result)

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
        """Reports the inner sink's losses (SPEC-026 FR-002).

        A wrapper that reported nothing would hide the destination it wraps: ``health().sink``
        would read ``None`` for a transform in front of a sink that counts perfectly well.
        Events this sink itself declines to forward are not loss — they are the configuration
        working, the same reason ``NullSink`` reports nothing.

        Args:
          None.

        Returns:
          The inner sink's losses. ``None`` passes through unchanged rather than becoming
          ``SinkLosses(dropped=0, failed=0)``: FR-003 distinguishes "the sink reports
          nothing" from "the sink reports no loss", and flattening the two would claim a
          clean bill of health on a sink that never gave one.

        Raises:
          None.
        """
        inner = read_losses(self._inner)
        with self._counter_lock:
            own = self._dropped
        return wrapped_losses(inner, own)

    def close(self) -> None:
        """Closes the inner sink (FR-004).

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
