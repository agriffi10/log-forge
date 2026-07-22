"""SPEC-007 — LoggingSink: dispatch, level mapping, verbatim message, reserved-attr-safe fields.

Tests attach a capturing handler to an injected logger (no network, no real handlers) and assert
on the emitted ``LogRecord``s. Each test uses a distinct logger name so the process-global logger
registry can't leak state between tests.
"""

from __future__ import annotations

import logging

from log_foundry.sinks.base import Sink
from log_foundry.sinks.logging_sink import LoggingSink


class ListHandler(logging.Handler):
    """Capture every record it handles for later assertion."""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def make_logger(name: str) -> tuple[logging.Logger, ListHandler]:
    """A fresh, isolated logger + capturing handler (no propagation to root)."""
    logger = logging.getLogger(name)
    logger.handlers = []
    handler = ListHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    return logger, handler


def ev(level: str = "INFO", message: str = "m", **fields: object) -> dict[str, object]:
    """A minimal event with identity keys and a nested ``fields`` dict."""
    return {
        "level": level,
        "message": message,
        "trace_id": "t",
        "span_id": "s",
        "function": "fn",
        "fields": dict(fields),
    }


# --- FR-001: dispatch -------------------------------------------------------------------


def test_dispatches_one_record_per_event_in_order() -> None:
    logger, handler = make_logger("test.dispatch.order")
    LoggingSink(logger).emit([ev(message="a"), ev(message="b"), ev(message="c")])
    assert [r.getMessage() for r in handler.records] == ["a", "b", "c"]


def test_default_logger_is_log_foundry() -> None:
    assert LoggingSink()._logger is logging.getLogger("log_foundry")


def test_is_sink_instance() -> None:
    assert isinstance(LoggingSink(), Sink)


# --- FR-002: level mapping --------------------------------------------------------------


def test_level_names_map_to_numeric() -> None:
    logger, handler = make_logger("test.levels.map")
    LoggingSink(logger).emit([ev("DEBUG"), ev("INFO"), ev("WARNING"), ev("ERROR"), ev("CRITICAL")])
    assert [r.levelno for r in handler.records] == [10, 20, 30, 40, 50]
    assert [r.levelname for r in handler.records] == [
        "DEBUG",
        "INFO",
        "WARNING",
        "ERROR",
        "CRITICAL",
    ]


def test_unknown_or_missing_level_uses_default() -> None:
    logger, handler = make_logger("test.levels.default")
    LoggingSink(logger, default_level="WARNING").emit([ev("TRACE"), {"message": "no level"}])
    assert [r.levelno for r in handler.records] == [logging.WARNING, logging.WARNING]


def test_level_mapping_is_case_insensitive() -> None:
    logger, handler = make_logger("test.levels.case")
    LoggingSink(logger).emit([ev("debug"), ev("Error")])
    assert [r.levelno for r in handler.records] == [logging.DEBUG, logging.ERROR]


# --- FR-004: verbatim message -----------------------------------------------------------


def test_message_with_literal_percent_not_interpolated() -> None:
    logger, handler = make_logger("test.message.percent")
    LoggingSink(logger).emit([ev(message="100% done: %s %d %(name)s")])
    assert handler.records[0].getMessage() == "100% done: %s %d %(name)s"


# --- FR-005: close is a no-op -----------------------------------------------------------


def test_close_does_not_shut_down_logging() -> None:
    logger, handler = make_logger("test.close.noop")
    LoggingSink(logger).close()
    assert handler in logger.handlers  # handlers untouched


# --- FR-003: structured fields on the record --------------------------------------------


def test_identity_and_fields_attached_flat_and_nested() -> None:
    logger, handler = make_logger("test.fields.attach")
    LoggingSink(logger).emit([ev(message="m", user="alice", count=5)])
    rec = handler.records[0]
    assert rec.trace_id == "t" and rec.span_id == "s" and rec.function == "fn"
    assert rec.user == "alice" and rec.count == 5  # flat
    assert rec.fields == {"user": "alice", "count": 5}  # nested payload


def test_reserved_collision_does_not_overwrite_but_survives_nested() -> None:
    logger, handler = make_logger("test.fields.reserved")
    # A field literally named "module" (a reserved LogRecord attr) must not clobber the real one.
    LoggingSink(logger).emit([ev(message="m", module="myfield")])
    rec = handler.records[0]
    assert rec.module != "myfield"  # reserved attr intact
    assert rec.fields["module"] == "myfield"  # preserved in the nested payload


def test_attach_never_raises_on_reserved_field_keys() -> None:
    logger, handler = make_logger("test.fields.noraise")
    LoggingSink(logger).emit([ev(message="m", msg="x", args="y", name="z", levelno=1)])
    rec = handler.records[0]  # no KeyError/AttributeError raised
    assert rec.fields == {"msg": "x", "args": "y", "name": "z", "levelno": 1}


def test_field_named_fields_does_not_clobber_nested_payload() -> None:
    logger, handler = make_logger("test.fields.selfcollision")
    event = {"level": "INFO", "message": "m", "fields": {"fields": "CLOBBER", "user": "alice"}}
    LoggingSink(logger).emit([event])
    rec = handler.records[0]
    assert rec.fields == {"fields": "CLOBBER", "user": "alice"}  # nested payload stays intact
    assert rec.user == "alice"  # other fields still flat-attached


def test_formatter_can_read_structured_data() -> None:
    logger, handler = make_logger("test.fields.formatter")
    LoggingSink(logger).emit([ev(message="hi", user="bob")])
    rec = handler.records[0]
    # A json formatter reads flat record attributes; here it reproduces the field + identity.
    formatted = logging.Formatter("%(trace_id)s %(user)s %(message)s").format(rec)
    assert formatted == "t bob hi"
