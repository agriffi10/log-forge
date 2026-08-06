"""TransformSink — reshape or redact each event before forwarding (arch §8, SPEC-006 FR-004).

Wraps an inner sink and maps every event through a user function on its way out — to redact a
field, add host metadata, rename keys, and so on. The function returning ``None`` drops that
event. The caller's batch and event dicts are never mutated in place: only the function's
return values are forwarded, so a transform must copy before mutating (see the spec's redact
example).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from log_foundry.sinks.base import SinkLosses, read_losses

if TYPE_CHECKING:
    from collections.abc import Callable

    from log_foundry.sinks.base import Sink

__all__ = ["TransformSink"]


class TransformSink:
    """A :class:`~log_foundry.sinks.base.Sink` that maps each event before forwarding.

    ``fn`` is applied to every event; a new list of the non-``None`` results is forwarded to
    ``inner``. When every event is dropped, ``inner.emit`` is not called.
    """

    def __init__(
        self,
        inner: Sink,
        fn: Callable[[dict[str, object]], dict[str, object] | None],
    ) -> None:
        self._inner = inner
        self._fn = fn

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Apply ``fn`` to each event, forwarding the non-``None`` results (FR-004)."""
        transformed: list[dict[str, object]] = []
        for event in batch:
            result = self._fn(event)
            if result is not None:
                transformed.append(result)
        if transformed:
            self._inner.emit(transformed)

    def losses(self) -> SinkLosses:
        """Report the inner sink's losses (SPEC-026 FR-002). Never raises.

        A wrapper that reported nothing would hide the destination it wraps: ``health().sink``
        would read ``None`` for a ``TransformSink`` in front of a sink that counts perfectly well.
        Events this sink itself declines to forward are not loss — they are the configuration
        working, the same reason ``NullSink`` reports nothing.
        """
        return read_losses(self._inner) or SinkLosses(dropped=0, failed=0)

    def close(self) -> None:
        """Close the inner sink (FR-004)."""
        self._inner.close()
