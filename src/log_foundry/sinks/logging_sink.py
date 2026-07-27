"""LoggingSink — bridge events into the stdlib ``logging`` framework (arch §8, SPEC-007).

Rather than reimplement rotating files, syslog, or the dozens of third-party log handlers that
already exist, this sink turns each built event dict into a ``logging.LogRecord`` and dispatches it
through a configurable logger — handing users their entire existing ``logging`` setup (handlers,
``logging.config``, third-party handlers) for free with no new dependency. The module is named
``logging_sink`` (not ``logging``) so it never shadows the stdlib module. Like every sink it
receives *already-built* event dicts and knows nothing about spans or context.
"""

from __future__ import annotations

import logging

__all__ = ["LoggingSink"]

# Level name -> stdlib numeric level; compared case-insensitively (arch §6 level names).
_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

# Attributes a LogRecord already owns; a user field must never overwrite one of these. Computed
# from a sample record plus the two names ``logging`` itself reserves against ``extra``.
_RESERVED = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime"}

# Identity keys copied verbatim onto the record as flat attributes (all known non-reserved).
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
    """A :class:`~log_foundry.sinks.base.Sink` that dispatches each event as a ``logging.LogRecord``.

    The sink only *emits* into the user's logging pipeline; it never configures loggers, handlers,
    or formatters, and never tears the framework down. ``default_level`` (a level name) is used for
    events whose ``level`` is unknown or missing.
    """

    def __init__(
        self,
        logger: logging.Logger | None = None,
        *,
        default_level: str = "INFO",
    ) -> None:
        self._logger = logger if logger is not None else logging.getLogger("log_foundry")
        self._default_level = _LEVELS.get(default_level.upper(), logging.INFO)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Dispatch one LogRecord per event, in batch order, through the target logger (FR-001)."""
        for event in batch:
            self._logger.handle(self._to_record(event))

    def close(self) -> None:
        """No-op — the sink does not own the user's logging configuration (FR-005)."""

    # -- internals ----------------------------------------------------------------------

    def _level_of(self, event: dict[str, object]) -> int:
        """Map the event's textual ``level`` to a numeric level, case-insensitively (FR-002)."""
        level = event.get("level")
        if isinstance(level, str):
            return _LEVELS.get(level.upper(), self._default_level)
        return self._default_level

    def _to_record(self, event: dict[str, object]) -> logging.LogRecord:
        """Build one record: verbatim message, mapped level, structured attrs (FR-002..FR-004)."""
        record = logging.LogRecord(
            name=self._logger.name,
            level=self._level_of(event),
            pathname="",
            lineno=0,
            msg=event.get("message", ""),
            args=(),  # no %-args, so a literal '%' in the message is never interpolated (FR-004)
            exc_info=None,
        )
        self._attach(record, event)
        return record

    def _attach(self, record: logging.LogRecord, event: dict[str, object]) -> None:
        """Attach identity keys + structured fields without clobbering reserved attrs (FR-003)."""
        for key in _IDENTITY:
            if key in event:
                setattr(record, key, event[key])
        fields = event.get("fields")
        if isinstance(fields, dict):
            # Nested payload is lossless even when a flat key collides and is skipped below.
            record.fields = fields
            for key, value in fields.items():
                # Skip reserved LogRecord attrs, identity keys, and "fields": the sink owns
                # record.fields (the nested payload); a field named "fields" must not overwrite it.
                if key not in _RESERVED and key not in _IDENTITY and key != "fields":
                    setattr(record, key, value)
