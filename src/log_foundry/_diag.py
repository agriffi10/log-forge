"""The library's own diagnostic channel — one line on stderr, for what it lost or absorbed.

This library *is* the logger, so it cannot report its own faults through itself: an event
describing a broken sink would be handed to that same broken sink. Stderr is the channel of last
resort, and **every** line the library writes about itself goes through here, so the wording and
the safety rules are decided once rather than remembered at twenty-odd call sites — which is how
twelve of them came to disagree with the other eight (SPEC-029).

Three rules, all load-bearing:

* **The exception's type, never its message.** A message can carry a value from the event that
  provoked it, and arch §6 keeps user data out of anything the library emits about itself — the
  same rule SPEC-019 applies to ``Health.stopped_reason`` and ``sanitize`` applies by refusing
  ``repr(value)``. ``PostgresSink`` is the sharpest case: ``_row`` binds the whole
  ``json.dumps(event)`` as a statement parameter, and a psycopg error repr routinely reprints the
  failing statement *and its parameters* — so a naive diagnostic for a failed insert would reprint
  the event, PII included, into a stream nobody was asked to secure. A type name tells a
  ``ConnectionError`` from a ``TypeError``, which is what the reader needs. Where a bare type is
  genuinely not diagnosable — an ``OSError`` alone does not say "refused" from "host unknown" —
  the caller passes a ``detail`` built from values the *library* controls: an ``errno`` (see
  :func:`errno_of`), an HTTP status, an attempt count, a sink's class name. Never the exception's
  text. Any ``detail`` is escaped and bounded regardless, so a value that reached one could not
  forge a second line in an operator's console.
* **No stream fault escapes.** These writers are called from ``except`` and ``finally`` blocks
  whose entire purpose is to stop an exception reaching the caller, and from the worker thread
  where an escaping exception ends delivery for good. A failure here — a closed stderr at
  interpreter shutdown, a stream that rejects the write, ``sys.stderr`` set to ``None`` — must not
  become the failure those guards existed to prevent. A ``BaseException`` still passes through, as
  everywhere else in this library: a ``KeyboardInterrupt`` landing mid-write is the operator's
  intent, not a stream fault to swallow.
* **Record first, announce second.** A counter moves *before* its line is attempted, at every call
  site. The write is best-effort by construction; the counter that ``health()`` reports is not, and
  must not be able to ride on it.

Stderr stays the channel: routing through the ``logging`` module would make the library's own
failure reports depend on machinery that may be the thing failing, and ``LoggingSink`` already
exists for the opposite direction. These lines are for humans — the structured stream is the sink.

**This module must import nothing from its own package.** ``decorator``, ``api``, ``worker`` and
every sink reach it with ``from log_foundry import _diag`` at module scope, which executes while
the package is still partially initialised and resolves only because ``_diag`` is a leaf — it needs
``sys`` and nothing else. Give it an intra-package import and every module that is imported *first*
in a fresh interpreter starts failing.
"""

from __future__ import annotations

import sys

__all__ = ["absorbed", "errno_of", "lost", "rejected"]

# Bound on any caller-supplied detail string, applied after escaping so it bounds what is
# *written*. Generous enough for an attempt count plus a sink name plus an errno; far short of a
# payload.
_MAX_DETAIL = 200

# Bound on how much of a rejected inbound value is echoed (SPEC-014). Tighter than _MAX_DETAIL:
# the value is attacker-controllable, and its shape is the diagnosis, not its contents.
_MAX_REJECTED_ECHO = 64

_ESCAPES = {"\t": "\\t", "\n": "\\n", "\r": "\\r"}


def absorbed(where: str, exc: BaseException, detail: str = "") -> None:
    """Report a failure the library swallowed rather than propagated.

    Never raises on a stream fault; a ``BaseException`` from the write still propagates.

    Args:
        where: What was being attempted, as a participle phrase that reads after "while" —
            ``"closing a span"``, ``"emitting an orphan log"``. A literal, never a runtime value.
        exc: The absorbed exception. Only ``type(exc).__name__`` is written.
        detail: Optional consequence for the reader, appended after a semicolon — most usefully
            what was lost, since an absorbed failure is invisible apart from this line. Escaped
            and truncated to ``_MAX_DETAIL``; must not be derived from ``exc``'s text.
    """
    try:
        sys.stderr.write(
            f"log-foundry: absorbed a failure while {_escape(where)} "
            f"({_escape(type(exc).__name__)}){_suffix(detail)}\n"
        )
    except Exception:
        # The channel of last resort has no fallback of its own. Reporting is best-effort by
        # construction: losing the line is bad, raising from the guard that exists to protect
        # the caller's exception would be worse (arch §4).
        pass


def lost(what: str, count: int, detail: str = "") -> None:
    """Report a counted loss — events, messages, batches or rows that will not be delivered.

    Never raises on a stream fault; a ``BaseException`` from the write still propagates.

    Args:
        what: Singular noun for the unit lost — ``"event"``, ``"message"``, ``"row"``. Rendered
            with the corpus-wide ``(s)`` suffix, so it must be a literal, never a runtime value.
        count: How many this line reports as lost — normally the increment the caller's counter
            has just taken. A **throttled** site instead passes its running total, because a line
            written on every thousandth drop that said "lost 1" would read as one loss rather than
            a thousand; such a site says so in ``detail``. Recorded before this call either way.
        detail: Optional circumstances — the sink's class name, the attempt count, an ``errno``,
            an exception *type*. Escaped and truncated to ``_MAX_DETAIL``.
    """
    try:
        sys.stderr.write(f"log-foundry: lost {count} {_escape(what)}(s){_suffix(detail)}\n")
    except Exception:
        pass  # best-effort, as above; the counter this line describes is already recorded.


def rejected(reason: str, value: object) -> None:
    """Report an inbound trace context the library refused to adopt (SPEC-014).

    The offending value is echoed as a **bounded ``repr``** — the one place in the library where a
    ``repr`` is correct, because the input is an inbound *header* rather than an exception, and its
    exact shape is what makes a rejection diagnosable. Unbounded it would be a log-injection
    surface: the value is attacker-controllable.

    The ``repr`` is escaped afterwards even though ``repr`` of a ``str`` is already printable
    throughout, which makes this a no-op for every value the call sites actually pass. It is not a
    no-op in general: ``repr`` runs ``__repr__``, which is user code free to *return* a raw newline
    no matter what the built-in reprs would have done. Escaping the result is the difference
    between "the built-ins happen to escape" and "this cannot forge a line".

    Never raises on a stream fault; a ``BaseException`` from the write still propagates.

    Args:
        reason: Why it was refused — ``"unparseable traceparent"``, ``"invalid trace_id"``. A
            literal, never a runtime value: unlike the echo, it is not bounded.
        value: The refused value, echoed as a ``repr`` bounded to ``_MAX_REJECTED_ECHO``.
    """
    try:
        shown = _escape(repr(value))
        if len(shown) > _MAX_REJECTED_ECHO:
            shown = shown[:_MAX_REJECTED_ECHO] + "…"
        sys.stderr.write(
            f"log-foundry: ignoring inbound trace context ({_escape(reason)}): {shown}\n"
        )
    except Exception:
        pass  # best-effort, as above.


def errno_of(exc: BaseException) -> str:
    """Return ``"errno=N"`` for an exception carrying one, else ``""``.

    The library-controlled detail that makes an ``OSError`` line actionable: "connection refused"
    and "host unknown" are the same type name and are told apart only by the code. An integer from
    the OS is not caller data, which is what makes it safe to write where the message is not.

    ``urllib``'s ``URLError`` carries the underlying socket error on ``.reason`` rather than
    setting its own ``errno``, so that is consulted too. Total: attribute access on an arbitrary
    exception can run a property that raises, and a helper for a diagnostic must not become one.

    The value is rendered through ``int()`` rather than interpolated as found. ``isinstance(x,
    int)`` admits any *subclass*, and a driver's error code is routinely one — an ``IntEnum`` or a
    bespoke class whose ``__str__`` returns whatever its author chose, which the f-string would
    then have written verbatim. That is the leak this helper exists to be the alternative to.
    """
    try:
        code = getattr(exc, "errno", None)
        if code is None:
            code = getattr(getattr(exc, "reason", None), "errno", None)
        return f"errno={int(code)}" if isinstance(code, int) else ""
    except Exception:
        return ""


def _suffix(detail: str) -> str:
    """Render a detail as a bounded, escaped ``"; ..."`` suffix, or ``""`` when there is none.

    Escaping precedes truncation so the bound applies to what is written, and so truncation can
    only ever remove characters from an already-safe string rather than split an escape into
    something that isn't one.

    Guarded in its own right, and returning ``""`` rather than propagating: this runs inside the
    caller's f-string, so a detail that cannot be rendered would otherwise take the *whole* line
    with it — including the count, which is the part an operator cannot reconstruct.
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
    """Render every non-printable character visibly, so nothing in ``text`` can forge a line.

    ``str.isprintable()`` is the test rather than a table over ``range(0x20)``: it is ``False`` for
    the C0 block *and* for DEL, the C1 block (U+009B is CSI, which a terminal reads as the start of
    an escape sequence), U+0085, U+2028, U+2029, and the bidi format characters. Python's own
    ``splitlines()`` breaks on three of those a C0 table misses, so a log shipper or a reader doing
    the obvious thing would see a forged ``log-foundry:`` line that a newline count says is not
    there. Space is printable, so ordinary text is untouched.

    The whole-string fast path keeps the overwhelmingly common case — a sink name, an attempt
    count, an exception type — to a single C-level call.
    """
    if text.isprintable():
        return text
    return "".join(char if char.isprintable() else _escaped(char) for char in text)


def _escaped(char: str) -> str:
    """Escape one non-printable character, following Python's own ``repr`` conventions."""
    escape = _ESCAPES.get(char)
    if escape is not None:
        return escape
    code = ord(char)
    if code <= 0xFF:
        return f"\\x{code:02x}"
    return f"\\u{code:04x}" if code <= 0xFFFF else f"\\U{code:08x}"
