"""W3C Trace Context-compatible id generation and ``traceparent`` codec (arch §3.1, §12).

``trace_id`` and ``span_id`` use the W3C wire formats (not arbitrary UUIDs) so adopting an
inbound request's trace is just a header parse and the emitted records stay compatible with
standard distributed-tracing tooling. ``log_id`` is internal-only, so a UUID is fine.

SPEC-014 cashes that in: the ``traceparent`` parser and formatter live here, beside the
generators whose formats they mirror, so the definition of "a valid trace id" has exactly one
home. Everything here is **total** — the parser takes ``object`` and returns ``None`` rather than
raising, because its input arrives from outside the process and a logging call must never be the
reason a caller's function fails.
"""

from __future__ import annotations

import os
import uuid

__all__ = [
    "new_trace_id",
    "new_span_id",
    "new_log_id",
    "parse_traceparent",
    "format_traceparent",
    "is_valid_trace_id",
    "is_valid_span_id",
]

_HEX = frozenset("0123456789abcdef")

# W3C: the sampled bit. This library records every span, so "sampled" is always true. An
# *inbound* flags byte is deliberately never round-tripped — honouring another system's sampling
# decision would mean dropping spans, which this library does not do (arch §10).
_FLAGS_SAMPLED = "01"

_VERSION = "00"
# Version ff is reserved and invalid per the W3C spec — it is not a "higher version".
_INVALID_VERSION = "ff"


def new_trace_id() -> str:
    """Return a fresh trace id: 32 lowercase hex chars (16 random bytes)."""
    return os.urandom(16).hex()


def new_span_id() -> str:
    """Return a fresh span id: 16 lowercase hex chars (8 random bytes)."""
    return os.urandom(8).hex()


def new_log_id() -> str:
    """Return a fresh log id: a UUID4 hex string (internal-only, format is our choice)."""
    return uuid.uuid4().hex


def _is_hex(value: str, length: int) -> bool:
    """True when ``value`` is exactly ``length`` *lowercase* hex characters.

    Uppercase is rejected rather than normalized: the W3C formats are defined as lowercase, and
    silently accepting ``4BF9...`` here would let a non-conforming id reach the event stream.
    """
    return len(value) == length and not (set(value) - _HEX)


def is_valid_trace_id(value: object) -> bool:
    """True for a W3C-valid trace id: exactly 32 lowercase hex chars, not all zero."""
    # All-zero is explicitly invalid per the W3C spec — it is the "unset" sentinel, not an id.
    return isinstance(value, str) and _is_hex(value, 32) and value != "0" * 32


def is_valid_span_id(value: object) -> bool:
    """True for a W3C-valid span id: exactly 16 lowercase hex chars, not all zero."""
    return isinstance(value, str) and _is_hex(value, 16) and value != "0" * 16


def parse_traceparent(value: object) -> tuple[str, str] | None:
    """Parse a W3C ``traceparent`` into ``(trace_id, span_id)``, or ``None`` if unusable.

    The format is ``version-trace_id-span_id-flags``, e.g.
    ``00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01``. Takes ``object`` and never
    raises: the value arrives from outside the process and may be any type at all.

    The returned ``span_id`` is the *caller's* span — the adopting span's parent, never its own
    identity. ``flags`` is parsed for validity and then ignored (see ``_FLAGS_SAMPLED``).
    """
    if not isinstance(value, str):
        return None
    parts = value.strip().split("-")
    if len(parts) < 4:
        return None
    version, trace_id, span_id, flags = parts[0], parts[1], parts[2], parts[3]
    if not _is_hex(version, 2) or version == _INVALID_VERSION:
        return None
    if version == _VERSION:
        # Version 00 is defined as exactly four fields; a trailing field is malformed, not a
        # forward-compatible extension.
        if len(parts) != 4 or not _is_hex(flags, 2):
            return None
    # A *higher* version is deliberately not rejected. The W3C forward-compatibility rule says to
    # parse the first three fields and ignore any extra ones — "reject anything that isn't 00" is
    # the intuitive-and-wrong reading, and would strand us the moment version 01 ships.
    if not is_valid_trace_id(trace_id) or not is_valid_span_id(span_id):
        return None
    return trace_id, span_id


def format_traceparent(trace_id: str, span_id: str) -> str:
    """Format ``(trace_id, span_id)`` as a version-``00`` W3C ``traceparent`` string."""
    return f"{_VERSION}-{trace_id}-{span_id}-{_FLAGS_SAMPLED}"
