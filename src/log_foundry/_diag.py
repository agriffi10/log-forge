"""The library's own diagnostic channel — one line on stderr, for failures it absorbed.

This library *is* the logger, so it cannot report its own faults through itself: an event
describing a broken sink would be handed to that same broken sink. Stderr is the channel of last
resort, and every site that swallows an exception to keep architecture §4's promise (logging never
breaks the application) announces it through here, so the wording and the safety rules are decided
once rather than per call site.

Two rules, both load-bearing:

* **The exception's type, never its message.** A message can carry a value from the event that
  provoked it, and arch §6 keeps user data out of anything the library emits about itself — the
  same rule SPEC-019 applies to ``Health.stopped_reason``. A type name is enough to tell a
  ``ConnectionError`` from a ``TypeError``, which is what the reader needs.
* **No stream fault escapes.** It is called from ``except`` and ``finally`` blocks whose entire
  purpose is to stop an exception reaching the caller, so a failure here — a closed stderr at
  interpreter shutdown, a stream that rejects the write, ``sys.stderr`` set to ``None`` — must not
  become the exception those guards existed to prevent. A ``BaseException`` still passes through,
  as everywhere else in this library: a ``KeyboardInterrupt`` landing mid-write is the operator's
  intent, not a stream fault to swallow.

**This module must import nothing from its own package.** ``decorator``, ``api`` and ``worker``
all reach it with ``from log_foundry import _diag`` at module scope, which executes while the
package is still partially initialised and resolves only because ``_diag`` is a leaf — it needs
``sys`` and nothing else. Give it an intra-package import and every module that is imported
*first* in a fresh interpreter starts failing.

SPEC-029 takes ownership of this module and moves the ``repr(exception)`` sink sites onto it.
"""

from __future__ import annotations

import sys

__all__ = ["absorbed"]


def absorbed(where: str, exc: BaseException, detail: str = "") -> None:
    """Report a failure the library swallowed rather than propagated.

    Never raises on a stream fault; a ``BaseException`` from the write still propagates.

    Args:
        where: What was being attempted, as a participle phrase that reads after "while" —
            ``"closing a span"``, ``"emitting an orphan log"``.
        exc: The absorbed exception. Only ``type(exc).__name__`` is written.
        detail: Optional consequence for the reader, appended after a semicolon — most usefully
            what was lost, since an absorbed failure is invisible apart from this line.
    """
    try:
        suffix = f"; {detail}" if detail else ""
        sys.stderr.write(
            f"log-foundry: absorbed a failure while {where} ({type(exc).__name__}){suffix}\n"
        )
    except Exception:
        # The channel of last resort has no fallback of its own. Reporting is best-effort by
        # construction: losing the line is bad, raising from the guard that exists to protect
        # the caller's exception would be worse (arch §4).
        pass
