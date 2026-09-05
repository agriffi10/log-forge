"""W3C Trace Context-compatible id generation and ``traceparent`` codec (arch §3.1, §12)."""

from __future__ import annotations

import os
import uuid

__all__ = [
    "format_traceparent",
    "is_valid_span_id",
    "is_valid_trace_id",
    "new_log_id",
    "new_span_id",
    "new_trace_id",
    "parse_traceparent",
]

_HEX = frozenset("0123456789abcdef")

_FLAGS_SAMPLED = "01"

_VERSION = "00"
_INVALID_VERSION = "ff"


def new_trace_id() -> str:
    """Returns a fresh trace id: 32 lowercase hex chars from 16 random bytes.

    Args:
      None.

    Returns:
      The new trace id.

    Raises:
      None.
    """
    return os.urandom(16).hex()


def new_span_id() -> str:
    """Returns a fresh span id: 16 lowercase hex chars from 8 random bytes.

    Args:
      None.

    Returns:
      The new span id.

    Raises:
      None.
    """
    return os.urandom(8).hex()


def new_log_id() -> str:
    """Returns a fresh log id as a UUID4 hex string.

    The log id is internal-only, so the format is this library's own choice rather than
    a W3C one.

    Args:
      None.

    Returns:
      The new log id.

    Raises:
      None.
    """
    return uuid.uuid4().hex


def _is_hex(value: str, length: int) -> bool:
    """Reports whether a string is exactly the given number of lowercase hex characters.

    Uppercase is rejected rather than normalized: the W3C formats are defined as
    lowercase, and silently accepting ``4BF9...`` here would let a non-conforming id
    reach the event stream.

    Args:
      value: The string to test.
      length: The exact number of characters required.

    Returns:
      True when the string is hex of exactly that length.

    Raises:
      None.
    """
    return len(value) == length and not (set(value) - _HEX)


def is_valid_trace_id(value: object) -> bool:
    """Reports whether a value is a W3C-valid trace id.

    A valid trace id is exactly 32 lowercase hex characters and not all zero, the
    all-zero form being the W3C "unset" sentinel rather than an id.

    Args:
      value: The candidate trace id, of any type.

    Returns:
      True when the value is a valid trace id.

    Raises:
      None.
    """
    return isinstance(value, str) and _is_hex(value, 32) and value != "0" * 32


def is_valid_span_id(value: object) -> bool:
    """Reports whether a value is a W3C-valid span id.

    A valid span id is exactly 16 lowercase hex characters and not all zero, the
    all-zero form being the W3C "unset" sentinel rather than an id.

    Args:
      value: The candidate span id, of any type.

    Returns:
      True when the value is a valid span id.

    Raises:
      None.
    """
    return isinstance(value, str) and _is_hex(value, 16) and value != "0" * 16


def parse_traceparent(value: object) -> tuple[str, str] | None:
    """Parses a W3C ``traceparent`` header into its trace and span ids.

    The format is ``version-trace_id-span_id-flags``. A version-``00`` header must carry
    exactly four fields and the reserved version ``ff`` is rejected, while a higher
    version is accepted with extra fields ignored, per the W3C forward-compatibility
    rule. The returned span id is the caller's span — the adopting span's parent, never
    its own identity.

    Args:
      value: An inbound header value of any type, from outside the process. Surrounding
        whitespace is stripped before parsing, since a header value arrives trimmed from some
        transports and not others; whitespace anywhere else is a malformed field.

    Returns:
      A ``(trace_id, span_id)`` tuple, or ``None`` if the value is unusable.

    Raises:
      None. A logging call must never be the reason a caller's function fails.
    """
    if not isinstance(value, str):
        return None
    parts = value.strip().split("-")
    if len(parts) < 4:
        return None
    version, trace_id, span_id, flags = parts[0], parts[1], parts[2], parts[3]
    if not _is_hex(version, 2) or version == _INVALID_VERSION:
        return None
    if version == _VERSION and (len(parts) != 4 or not _is_hex(flags, 2)):
        return None
    if not is_valid_trace_id(trace_id) or not is_valid_span_id(span_id):
        return None
    return trace_id, span_id


def format_traceparent(trace_id: str, span_id: str) -> str:
    """Formats a trace id and span id as a version-``00`` W3C ``traceparent`` string.

    The flags byte is always the sampled bit, because this library records every span
    and never propagates a sampling decision it did not make.

    Args:
      trace_id: A valid 32-hex-character trace id.
      span_id: A valid 16-hex-character span id.

    Returns:
      The formatted ``traceparent`` header value.

    Raises:
      None.
    """
    return f"{_VERSION}-{trace_id}-{span_id}-{_FLAGS_SAMPLED}"
