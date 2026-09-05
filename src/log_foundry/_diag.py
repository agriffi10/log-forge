"""The library's own diagnostic channel — one line on stderr, for what it lost or absorbed.

Three rules, all load-bearing (SPEC-029 FR-004):

* The exception's type, never its message. A message can carry a value from the event that
  provoked it, and arch §6 keeps user data out of anything the library emits about itself.
  ``PostgresSink`` is the sharpest case: a psycopg error repr reprints the failing statement
  and its bound parameters, so a naive diagnostic would reprint the event, PII included.
* No stream fault escapes. These writers are called from the guards that exist to stop an
  exception reaching the caller. A ``BaseException`` still passes through, as everywhere else
  in this library: a ``KeyboardInterrupt`` mid-write is the operator's intent.
* Record first, announce second. A counter moves before its line is attempted, at every call
  site, because the write is best-effort and the counter ``health()`` reports is not.

Stderr stays the channel: routing through the ``logging`` module would make the library's own
failure reports depend on machinery that may be the thing failing. This module must import
nothing from its own package — it is reached at module scope while the package is still
partially initialised, and resolves only because it is a leaf.
"""

from __future__ import annotations

import sys

__all__ = ["WARN_EVERY", "absorbed", "errno_of", "lost", "rejected"]

WARN_EVERY = 1000
"""The period of every per-event throttle in the library (SPEC-017 FR-005, SPEC-054 FR-005).

A site that can fail once per event writes its line on the first failure and then on every
``WARN_EVERY``-th, carrying the running total, because a line per failure is its own outage and
silence is how the failure survives to production. One definition, here, because the worker's
queue-full and post-shutdown sites and the console writer's stream-fault site all throttle on
the same period, and two constants stating one number disagree eventually.
"""

_MAX_DETAIL = 200

_MAX_REJECTED_ECHO = 64

_ESCAPES = {"\t": "\\t", "\n": "\\n", "\r": "\\r"}


def absorbed(where: str, exc: BaseException, detail: str = "") -> None:
    """Reports a failure the library swallowed rather than propagated.

    Only the exception's type is written, never its message: a message can carry a value
    from the event that provoked it, and arch §6 keeps user data out of anything the library
    emits about itself. A stream fault is swallowed because these writers are called from the
    ``except`` and ``finally`` blocks that exist to stop an exception reaching the caller,
    though a ``BaseException`` from the write still propagates.

    Args:
      where: What was being attempted, as a participle phrase that reads after "while", such
        as ``"closing a span"``. A literal, never a runtime value.
      exc: The absorbed exception. Only ``type(exc).__name__`` is written.
      detail: Optional consequence for the reader, appended after a semicolon — most usefully
        what was lost, since an absorbed failure is invisible apart from this line. Escaped
        and truncated to ``_MAX_DETAIL``; must not be derived from the exception's text.

    Returns:
      None.

    Raises:
      None on a stream fault. The channel of last resort has no fallback of its own, and
        raising from the guard that protects the caller's exception would be worse (arch §4).
    """
    try:
        sys.stderr.write(
            f"log-foundry: absorbed a failure while {_escape(where)} "
            f"({_escape(type(exc).__name__)}){_suffix(detail)}\n"
        )
    except Exception:
        pass


def lost(what: str, count: int, detail: str = "") -> None:
    """Reports a counted loss — events, messages, batches or rows that will not be delivered.

    Callers record the counter before calling this, because the write is best-effort by
    construction and the counter ``health()`` reports must not be able to ride on it.

    Args:
      what: Singular noun for the unit lost, such as ``"event"`` or ``"row"``. Rendered with
        the corpus-wide ``(s)`` suffix, so it must be a literal, never a runtime value.
      count: How many this line reports as lost, normally the increment the caller's counter
        has just taken. A throttled site instead passes its running total, because a line
        written on every thousandth drop saying "lost 1" would read as one loss rather than a
        thousand, and such a site says so in ``detail``.
      detail: Optional circumstances — the sink's class name, the attempt count, an
        ``errno``, an exception type. Escaped and truncated to ``_MAX_DETAIL``.

    Returns:
      None.

    Raises:
      None on a stream fault; a ``BaseException`` from the write still propagates.
    """
    try:
        sys.stderr.write(f"log-foundry: lost {count} {_escape(what)}(s){_suffix(detail)}\n")
    except Exception:
        pass


def rejected(reason: str, value: object) -> None:
    """Reports an inbound trace context the library refused to adopt (SPEC-014).

    The offending value is echoed as a bounded ``repr`` — the one place in the library where a
    ``repr`` is correct, because the input is an inbound header rather than an exception and
    its exact shape is what makes a rejection diagnosable. The ``repr`` is escaped afterwards
    even though ``repr`` of a ``str`` is already printable, because ``repr`` runs
    ``__repr__``, which is user code free to return a raw newline; escaping the result is the
    difference between "the built-ins happen to escape" and "this cannot forge a line".

    Args:
      reason: Why it was refused, such as ``"unparseable traceparent"``. A literal, never a
        runtime value: unlike the echo, it is not bounded.
      value: The refused value, echoed as a ``repr`` bounded to ``_MAX_REJECTED_ECHO``,
        because unbounded it would be a log-injection surface.

    Returns:
      None.

    Raises:
      None on a stream fault; a ``BaseException`` from the write still propagates.
    """
    try:
        shown = _escape(repr(value))
        if len(shown) > _MAX_REJECTED_ECHO:
            shown = shown[:_MAX_REJECTED_ECHO] + "…"
        sys.stderr.write(
            f"log-foundry: ignoring inbound trace context ({_escape(reason)}): {shown}\n"
        )
    except Exception:
        pass


def errno_of(exc: BaseException) -> str:
    """Returns ``"errno=N"`` for an exception carrying one, else an empty string.

    This is the library-controlled detail that makes an ``OSError`` line actionable:
    "connection refused" and "host unknown" share a type name and are told apart only by the
    code, and an integer from the OS is not caller data. ``urllib``'s ``URLError`` carries the
    underlying socket error on ``.reason`` rather than setting its own ``errno``, so that is
    consulted too.

    The value is rendered through ``int()`` rather than interpolated as found, because
    ``isinstance(x, int)`` admits any subclass and a driver's error code is routinely one — an
    ``IntEnum`` or a bespoke class whose ``__str__`` returns whatever its author chose, which
    an f-string would write verbatim. That is the leak this helper exists to be the
    alternative to.

    Args:
      exc: The exception to inspect.

    Returns:
      The rendered ``errno=N``, or an empty string when there is no integer code.

    Raises:
      None. Attribute access on an arbitrary exception can run a property that raises, and a
        helper for a diagnostic must not become one.
    """
    try:
        code = getattr(exc, "errno", None)
        if code is None:
            code = getattr(getattr(exc, "reason", None), "errno", None)
        return f"errno={int(code)}" if isinstance(code, int) else ""
    except Exception:
        return ""


def _suffix(detail: str) -> str:
    """Renders a detail as a bounded, escaped ``"; ..."`` suffix.

    Escaping precedes truncation so the bound applies to what is written, and so truncation
    can only remove characters from an already-safe string rather than split an escape into
    something that is not one.

    Args:
      detail: The caller-supplied detail, possibly empty.

    Returns:
      The suffix, or an empty string when there is no detail or it cannot be rendered.

    Raises:
      None. This runs inside the caller's f-string, so a detail that cannot be rendered would
        otherwise take the whole line with it, including the count an operator cannot
        reconstruct.
    """
    if not detail:
        return ""
    try:
        shown = _escape(detail)
        if len(shown) > _MAX_DETAIL:
            shown = shown[:_MAX_DETAIL] + "…"
        return f"; {shown}"
    except Exception:
        return ""


def _escape(text: str) -> str:
    """Renders every non-printable character visibly, so nothing in the text can forge a line.

    ``str.isprintable()`` is the test rather than a table over ``range(0x20)``: it is
    ``False`` for the C0 block and for DEL, the C1 block, U+0085, U+2028, U+2029, and the bidi
    format characters. Python's own ``splitlines()`` breaks on three of those a C0 table
    misses, so a reader doing the obvious thing would see a forged ``log-foundry:`` line that
    a newline count says is not there. The whole-string fast path keeps the common case to a
    single C-level call, and space is printable, so ordinary text is untouched.

    Args:
      text: The string to render safely.

    Returns:
      The text with every non-printable character escaped.

    Raises:
      None.
    """
    if text.isprintable():
        return text
    return "".join(char if char.isprintable() else _escaped(char) for char in text)


def _escaped(char: str) -> str:
    """Escapes one non-printable character, following Python's own ``repr`` conventions.

    Args:
      char: The single character to escape.

    Returns:
      The escape sequence for that character.

    Raises:
      None.
    """
    escape = _ESCAPES.get(char)
    if escape is not None:
        return escape
    code = ord(char)
    if code <= 0xFF:
        return f"\\x{code:02x}"
    return f"\\u{code:04x}" if code <= 0xFFFF else f"\\U{code:08x}"
