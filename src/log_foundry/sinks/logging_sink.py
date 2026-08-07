"""LoggingSink — bridge events into the stdlib ``logging`` framework (arch §8, SPEC-007)."""

from __future__ import annotations

import logging

__all__ = ["LoggingSink"]

_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

_RESERVED = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime"}

_IDENTITY = (
    "trace_id",
    "span_id",
    "parent_span_id",
    "log_id",
    "function",
    "service",
    "version",
    "env",
)


class LoggingSink:
    """A :class:`~log_foundry.sinks.base.Sink` that dispatches each event as a ``LogRecord``.

    Rather than reimplement rotating files, syslog, or the dozens of third-party log handlers
    that already exist, this hands users their entire existing ``logging`` setup for free with
    no new dependency. The sink only emits into that pipeline: it never configures loggers,
    handlers or formatters, and never tears the framework down. The module is named
    ``logging_sink`` so it never shadows the stdlib module.

    It takes **no** transport lock (SPEC-028 FR-002): ``logging`` serializes its own handlers,
    and this sink holds nothing else. And it **accepts emit after close** (SPEC-032 FR-003) —
    ``close()`` is a no-op by design, since tearing down handlers this sink did not configure is
    not its to do, so a later batch still reaches the framework.
    """

    def __init__(
        self,
        logger: logging.Logger | None = None,
        *,
        default_level: str = "INFO",
    ) -> None:
        """Binds the sink to a target logger and its fallback level.

        Args:
          logger: The logger to dispatch through, defaulting to ``log_foundry``.
          default_level: The level name used for events whose ``level`` is unknown or missing.

        Returns:
          None.

        Raises:
          None.
        """
        self._logger = logger if logger is not None else logging.getLogger("log_foundry")
        self._default_level = _LEVELS.get(default_level.upper(), logging.INFO)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Dispatches one record per event, in batch order, through the target logger (FR-001).

        Args:
          batch: The events to dispatch.

        Returns:
          None.

        Raises:
          Exception: Whatever the logger's handlers raise.
        """
        for event in batch:
            self._logger.handle(self._to_record(event))

    def close(self) -> None:
        """Does nothing, since the sink does not own the user's logging configuration (FR-005).

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """

    def _level_of(self, event: dict[str, object]) -> int:
        """Maps the event's textual level to a numeric one, case-insensitively (FR-002).

        Args:
          event: The event whose ``level`` is read.

        Returns:
          The stdlib numeric level, or the configured default.

        Raises:
          None.
        """
        level = event.get("level")
        if isinstance(level, str):
            return _LEVELS.get(level.upper(), self._default_level)
        return self._default_level

    def _to_record(self, event: dict[str, object]) -> logging.LogRecord:
        """Builds one record: verbatim message, mapped level, structured attributes.

        No ``%``-args are passed, so a literal ``%`` in the message is never interpolated
        (FR-004).

        Args:
          event: The event to convert.

        Returns:
          The record, ready to hand to the logger.

        Raises:
          None.
        """
        record = logging.LogRecord(
            name=self._logger.name,
            level=self._level_of(event),
            pathname="",
            lineno=0,
            msg=event.get("message", ""),
            args=(),
            exc_info=None,
        )
        self._attach(record, event)
        return record

    def _attach(self, record: logging.LogRecord, event: dict[str, object]) -> None:
        """Attaches identity keys and structured fields without clobbering reserved ones.

        The nested payload is set on ``record.fields`` first, so it stays lossless even when a
        flat key collides and is skipped. Reserved ``LogRecord`` attributes, the identity keys
        and ``fields`` itself are all excluded from the flattening — the sink owns
        ``record.fields``, so a user field of that name must not overwrite it (FR-003).

        Args:
          record: The record being built.
          event: The event supplying the values.

        Returns:
          None.

        Raises:
          None.
        """
        for key in _IDENTITY:
            if key in event:
                setattr(record, key, event[key])
        fields = event.get("fields")
        if isinstance(fields, dict):
            record.fields = fields
            for key, value in fields.items():
                if key not in _RESERVED and key not in _IDENTITY and key != "fields":
                    setattr(record, key, value)
