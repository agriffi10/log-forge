"""CallbackSink — turn a plain callable into a Sink (arch §8, SPEC-006 FR-001).

The ultimate escape hatch: point log-foundry at any destination by handing it a function,
without writing a ``Sink`` subclass. Like every sink it receives *already-built* event dicts
and knows nothing about spans or context — that dumbness is what makes sinks swappable.
"""

from __future__ import annotations

from collections.abc import Callable

__all__ = ["CallbackSink"]


class CallbackSink:
    """A :class:`~log_foundry.sinks.base.Sink` that delegates each batch to a callable.

    Attributes are internal; the observable contract is that ``emit`` hands the batch to
    ``fn`` unchanged and ``close`` invokes ``on_close`` once when one was supplied.
    """

    def __init__(
        self,
        fn: Callable[[list[dict[str, object]]], None],
        *,
        on_close: Callable[[], None] | None = None,
    ) -> None:
        self._fn = fn
        self._on_close = on_close

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Hand ``batch`` to the callable, unchanged and exactly once (FR-001).

        Any exception the callable raises propagates out — the worker's retry/backoff path
        handles it; this sink never swallows it.
        """
        self._fn(batch)

    def close(self) -> None:
        """Call ``on_close`` once if one was supplied, otherwise a no-op (FR-001)."""
        if self._on_close is not None:
            self._on_close()
