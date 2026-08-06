"""CallbackSink — turn a plain callable into a Sink (arch §8, SPEC-006 FR-001)."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["CallbackSink"]


class CallbackSink:
    """A :class:`~log_foundry.sinks.base.Sink` that delegates each batch to a callable.

    This is the ultimate escape hatch: point log-foundry at any destination by handing it a
    function, without writing a ``Sink`` implementation. Attributes are internal; the observable
    contract is that ``emit`` hands the batch to the callable unchanged and ``close`` invokes
    the close hook once when one was supplied.
    """

    def __init__(
        self,
        fn: Callable[[list[dict[str, object]]], None],
        *,
        on_close: Callable[[], None] | None = None,
    ) -> None:
        """Binds the sink to its callable and optional close hook.

        Args:
          fn: Receives each batch of already-built event dicts.
          on_close: Called once by :meth:`close`, or ``None`` for no cleanup.

        Returns:
          None.

        Raises:
          None.
        """
        self._fn = fn
        self._on_close = on_close

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Hands the batch to the callable, unchanged and exactly once (FR-001).

        Args:
          batch: The events to deliver.

        Returns:
          None.

        Raises:
          Exception: Whatever the callable raises. It propagates out to the worker's
            retry/backoff path; this sink never swallows it.
        """
        self._fn(batch)

    def close(self) -> None:
        """Calls the close hook once if one was supplied, otherwise does nothing (FR-001).

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the close hook raises.
        """
        if self._on_close is not None:
            self._on_close()
