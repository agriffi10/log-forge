"""Context propagation: span stack + baggage via contextvars (arch §5, guide Phase 4)."""

from __future__ import annotations

import contextvars
import itertools
from typing import TYPE_CHECKING
from urllib.parse import quote, unquote

from log_foundry import _diag, config, sanitize
from log_foundry.ids import format_traceparent

if TYPE_CHECKING:
    from log_foundry.model import Span

__all__ = [
    "current_baggage_header",
    "current_trace_context",
    "current_traceparent",
    "get_baggage",
    "reset_context",
    "set_baggage",
]
"""Exactly the six names ``log_foundry`` re-exports (SPEC-051 FR-003).

The five that left -- ``current_span``, ``push_span``, ``pop_span``, ``push_baggage_scope``
and ``pop_baggage_scope`` -- are internal and stay importable; what was withdrawn is the
claim, not the symbol. ``current_span`` is why this matters: it hands back a **mutable**
:class:`~.model.Span`, which is what :func:`current_trace_context` exists to keep callers
away from, and a name still in ``__all__`` at ``1.0`` could not be withdrawn afterwards.
"""

_span_stack: contextvars.ContextVar[tuple[Span, ...]] = contextvars.ContextVar(
    "log_foundry_span_stack", default=()
)
_baggage: contextvars.ContextVar[dict[str, object]] = contextvars.ContextVar(
    "log_foundry_baggage",
    default={},  # noqa: B039
)
_adopted: contextvars.ContextVar[tuple[str, str | None] | None] = contextvars.ContextVar(
    "log_foundry_adopted_context", default=None
)

BAGGAGE_MAX_BYTES = 8192

_dropped_members: itertools.count[int] = itertools.count(1)
"""The running total of baggage members the outbound header has dropped (SPEC-064 FR-003).

Each member dropped by the :data:`BAGGAGE_MAX_BYTES` cap, or skipped for an empty or unrenderable
key, takes one ``next()``. That is atomic under the GIL, so every value drawn is unique and each
throttle crossing is announced by exactly one call, even under concurrent calls. A bare ``+=``
could lose an increment, and then a line's total would not reproduce from the drops behind it
(invariant 11); a lock here would be a new fork hazard (invariant 12). It is diagnostic throttle
state only: a forked child holds its own copy, which only shifts which line it writes, so nothing
is stranded.
"""


def current_span() -> Span | None:
    """Returns the innermost active span.

    Args:
      None.

    Returns:
      The current span, or ``None`` when no span is active.

    Raises:
      None.
    """
    stack = _span_stack.get()
    return stack[-1] if stack else None


def _live_span_stack() -> tuple[Span, ...]:
    """Returns every span open on this context, outermost first (SPEC-036 FR-001).

    The library's non-copying read, named as :func:`_live_baggage` is and distinguished from the
    public accessors for the same reason (SPEC-034 FR-003): a caller gets a copy, the library
    reads the live object. There is nothing to copy here in any case — the stack is a tuple, so
    handing it out cannot let anyone mutate it. :func:`current_span` returns only the innermost,
    which is what an event needs and not what a sweep does.

    Args:
      None.

    Returns:
      Every span open on this context, outermost first. Empty when none is.

    Raises:
      None.
    """
    return _span_stack.get()


def push_span(span: Span) -> contextvars.Token[tuple[Span, ...]]:
    """Pushes a span onto the stack.

    Args:
      span: The span being opened.

    Returns:
      A token to hand back to :func:`pop_span`.

    Raises:
      None.
    """
    return _span_stack.set((*_span_stack.get(), span))


def pop_span(token: contextvars.Token[tuple[Span, ...]]) -> None:
    """Restores the span stack to its state before the matching :func:`push_span`.

    This is total and never raises, on the same terms and for the same reason as
    :func:`pop_baggage_scope`: it runs in the decorator's ``finally``, where an exception
    would replace the one the caller's own function raised (arch §4, SPEC-025). ``reset``
    rejects a token minted in another context and one already used, so the fallback
    restores the captured stack directly, treating anything that is not a tuple as empty.

    Args:
      token: The token returned by :func:`push_span`.

    Returns:
      None.

    Raises:
      None.
    """
    try:
        _span_stack.reset(token)
    except Exception:
        old = getattr(token, "old_value", None)
        _span_stack.set(old if isinstance(old, tuple) else ())


def get_baggage() -> dict[str, object]:
    """Returns a copy of the current trace's baggage.

    The trace's own keys are discarded when the enclosing root span closes, restoring the
    baggage in effect before it, so a later trace sees only a process-level default set
    before any span opened. With no span open at all nothing releases them and they
    accumulate for the life of the context; :func:`reset_context` is the release there.

    It is a **shallow** copy (SPEC-034 FR-005). This became public at 1.0, and a public accessor
    that hands out the live mapping documented as "do not mutate" is FR-003's ``get_config()``
    defect under another name. Shallow rather than deep because :func:`set_baggage` accepts
    arbitrary values: a deep copy would run user objects through ``copy.deepcopy`` inside a
    getter that must never raise, which trades a narrow sharing bound for a wide new failure —
    and the library never mutates a baggage value either, so the sharing is only reachable
    through the caller's own object. The library's own hot path does not pay even for the
    shallow copy: :func:`_live_baggage` is the internal read, for the reason FR-003 AC-6 gives.

    Args:
      None.

    Returns:
      A shallow copy of the baggage mapping. Rebinding a key does not affect the library; a
        nested mutable value is shared, and mutating one *does* reach every later event.

    Raises:
      None.
    """
    return dict(_baggage.get())


def _live_baggage() -> dict[str, object]:
    """Returns the baggage mapping itself, for callers inside the package.

    :func:`api._log` reads baggage once per event, so a copy here would allocate per event —
    the cost FR-003 AC-6 measures and refuses on the config's own hot path. Callers must treat
    the result as read-only; the freeze is a guarantee to the library's *users*, not one the
    library needs against itself.

    Args:
      None.

    Returns:
      The live baggage mapping.

    Raises:
      None.
    """
    return _baggage.get()


def set_baggage(**kv: object) -> None:
    """Merges key/values into the current trace's baggage.

    The keys live until the enclosing root span closes, then the baggage in effect before
    that span is restored, so they ride every event at or below this point in the trace and
    none of the next trace's. Called with no span open they become a process-level default
    that later traces inherit and restore to, which ``configure(defaults=...)`` expresses
    better.

    Args:
      **kv: Fields to merge into the baggage.

    Returns:
      None.

    Raises:
      None.
    """
    _baggage.set({**_baggage.get(), **kv})


def push_baggage_scope() -> contextvars.Token[dict[str, object]]:
    """Opens a root span's baggage scope.

    Setting the variable to its own current value is what mints the token — there is no
    other way to say "restore whatever was here", including the case where nothing was ever
    set. It is safe because no baggage dict is ever mutated in place.

    Args:
      None.

    Returns:
      A token to hand back to :func:`pop_baggage_scope`.

    Raises:
      None.
    """
    return _baggage.set(_baggage.get())


def pop_baggage_scope(token: contextvars.Token[dict[str, object]]) -> None:
    """Closes a root span's scope: restores baggage, discards any adopted trace context.

    The two are deliberately asymmetric. Baggage is restored, so a process-level default set
    before any span outlives the traces that run under it while a request's own keys do not
    (SPEC-024 FR-001); the adopted context is cleared, because it is a one-shot handoff and
    restoring it would leave the next invocation joining the previous caller's trace
    (FR-002). The adopted context is cleared first, ahead of anything that could fail: a
    stale trace id puts wrong data in the log stream, while a missed baggage restore only
    leaves a stale field.

    Both writes land in the context the root span's ``finally`` runs in, so an adoption made
    outside a span that then runs in a child context — any ``asyncio.Task`` — is cleared in
    the copy rather than the parent. :func:`continue_trace` is documented to be called on the
    entry point's first line, which is inside the span and unaffected; a caller who adopts
    before dispatching across a task boundary clears it with :func:`reset_context`.

    Args:
      token: The token returned by :func:`push_baggage_scope`.

    Returns:
      None.

    Raises:
      None. A decorated function must not fail on the way out (arch §4), so a token minted
        in another context or already used falls back to setting the captured value.
    """
    _adopted.set(None)
    try:
        _baggage.reset(token)
    except Exception:
        old = getattr(token, "old_value", None)
        _baggage.set(old if isinstance(old, dict) else {})


def reset_context() -> None:
    """Clears baggage and any adopted trace context outright (SPEC-024 FR-003).

    This is for the caller who uses the emitters without ``@trace``: the orphan path opens no
    span, so there is no root-span exit to hang the release on. It is also the remedy for the
    one case that scope cannot reach, an adoption made outside a span whose root span then
    runs in a child context, and must be called in the context that made the adoption.

    Unlike the scope release this clears rather than restores, erasing a process-level
    baggage default too. Prefer calling it outside a span: inside one it does the obvious
    thing to the events that follow, but SPEC-015 backfills ``span.start`` and ``span.end``
    from the baggage live at close, so a mid-span reset also empties the two boundary events
    of baggage that was live for most of the span.

    Args:
      None.

    Returns:
      None.

    Raises:
      None.
    """
    _adopted.set(None)
    _baggage.set({})


def get_adopted_context() -> tuple[str, str | None] | None:
    """Returns the inbound trace context adopted for the next root span.

    Args:
      None.

    Returns:
      The adopted ``(trace_id, parent_span_id)``, or ``None`` if none was adopted.

    Raises:
      None.
    """
    return _adopted.get()


def set_adopted_context(trace_id: str, parent_span_id: str | None) -> None:
    """Records an inbound trace context for the next root span opened in this context.

    Args:
      trace_id: The inbound trace id to join.
      parent_span_id: The caller's span id, or ``None`` if the header carried none.

    Returns:
      None.

    Raises:
      None.
    """
    _adopted.set((trace_id, parent_span_id))


def current_traceparent() -> str | None:
    """Returns the current span as a W3C ``traceparent`` string.

    Args:
      None.

    Returns:
      The formatted header value, or ``None`` if no span is active.

    Raises:
      None.
    """
    span = current_span()
    return format_traceparent(span.trace_id, span.span_id) if span else None


def current_trace_context() -> tuple[str, str] | None:
    """Returns the current span's trace and span ids as a pair.

    This is for callers who would rather move two fields than a formatted string — a Step
    Functions payload, a queue message attribute. It exists so nobody is pushed into
    :func:`current_span`, which is internal and hands back a mutable :class:`~.model.Span`.

    Args:
      None.

    Returns:
      A ``(trace_id, span_id)`` tuple, or ``None`` if no span is active.

    Raises:
      None.
    """
    span = current_span()
    return (span.trace_id, span.span_id) if span else None


def current_baggage_header() -> str:
    """Returns the current baggage in W3C ``baggage`` header format.

    The baggage store holds arbitrary objects but the wire format is text, so each value is
    first coerced as an event's fields are, under the same ceilings and work budgets (SPEC-064),
    and the coerced value is then rendered with ``str()``. A dict still arrives at the next
    process as its repr, with its contents coerced, not as a dict; a ``datetime`` arrives in ISO
    form, a tuple as a list, and a value nothing can render as an ``<unserializable: Type>``
    placeholder. A string is sent as it is unless it is past ``max_value_bytes`` or carries a
    lone surrogate, which becomes U+FFFD.

    The header is capped at :data:`BAGGAGE_MAX_BYTES`, the size the next process's
    :func:`parse_baggage_header` accepts, by whole members in baggage order: the first member
    that does not fit ends it, and a member with an empty key is left out. Either drop is
    announced on stderr as a count, never a key or a value. See :func:`format_baggage_header`.

    Args:
      None.

    Returns:
      The formatted header value, empty when there is no baggage or none of it fits.

    Raises:
      None.
    """
    return format_baggage_header(_live_baggage())


def format_baggage_header(baggage: dict[str, object]) -> str:
    """Serializes a baggage mapping to the W3C ``key1=value1,key2=value2`` format.

    The mapping is coerced by one ``sanitize.sanitize_fields`` pass over the whole of it, under
    the live config's ``max_keys``, ``max_depth`` and ``max_value_bytes`` and the pass's node and
    byte budgets (SPEC-057), so the budgets bound the header as a whole as they bound an event.
    This is what makes the call return on a value whose own ``str()`` never would, such as a
    shared-sublist DAG, and agree with event assembly on what a hostile value becomes (SPEC-064
    FR-001). Each coerced value is rendered by :func:`_render_member`, which guards the whole
    render of one member, so a failure costs that member its value and nothing else (FR-002).

    Keys and values are percent-encoded with ``safe=""``, so the separators themselves are
    encoded rather than corrupting the framing — the whole reason a value containing a comma
    survives the hop and round-trips through :func:`parse_baggage_header`.

    Members are kept in the coerced mapping's order while the header, commas included, is at
    most :data:`BAGGAGE_MAX_BYTES`. The first member that does not fit ends the header, so it and
    every member after it are dropped even where a later one is small: a prefix, never a packing,
    and never a partial member, which W3C Baggage permits past 8192 bytes. The length is that of
    the percent-encoded text, which is ASCII. A member whose encoded key is empty, or whose key
    or placeholder cannot be encoded, is skipped without ending the header, because the parser
    refuses an empty key and with it the whole header (FR-003). Everything dropped is counted
    and announced by :func:`_announce_dropped`; a member dropped by ``max_keys`` or a value
    clipped by a ceiling is coercion doing what it does for an event, and is not announced.

    Args:
      baggage: The mapping to serialize.

    Returns:
      The formatted header value, at most :data:`BAGGAGE_MAX_BYTES` long.

    Raises:
      None. Only a ``BaseException`` from a value's own code passes through, as everywhere else
        in this library (invariant 1).
    """
    if not baggage:
        return ""
    coerced, _ = sanitize.sanitize_fields(baggage, cfg=config._live_config())
    members: list[str] = []
    size = 0
    dropped = 0
    ended = False
    for key, value in coerced.items():
        if ended:
            dropped += 1
            continue
        member = _render_member(key, value)
        if member is None:
            dropped += 1
            continue
        grown = size + len(member) + (1 if members else 0)
        if grown > BAGGAGE_MAX_BYTES:
            ended = True
            dropped += 1
            continue
        members.append(member)
        size = grown
    if dropped:
        _announce_dropped(dropped)
    return ",".join(members)


def _render_member(key: str, value: object) -> str | None:
    """Renders one coerced baggage member as ``key=value``, or ``None`` when it must be skipped.

    After coercion a value is a scalar, a list or a dict, so ``str()`` of it cannot raise except
    for an object that spoofs a scalar type and survives coercion (arch §12). The guard therefore
    wraps the whole value render, ``str()`` and ``quote`` both, because a ``__str__`` returning a
    ``str`` subclass with a raising ``encode`` gets past ``str()`` and raises inside ``quote``.
    Such a value becomes ``<unserializable: TypeName>``, named by ``sanitize._type_name`` so that
    an unreadable type name is ``"?"`` rather than a second exception, and never by ``repr`` or
    the exception's text (invariant 11). Only ``Exception`` is caught, so a ``KeyboardInterrupt``
    still reaches the caller. A coerced key is always a ``str``, so the key's guard is defensive.

    Args:
      key: The coerced key.
      value: The coerced value.

    Returns:
      The percent-encoded member, or ``None`` when the key is empty or cannot be encoded, or
        when even the placeholder cannot be.

    Raises:
      None.
    """
    try:
        encoded_key = quote(key, safe="")
    except Exception:
        return None
    if not encoded_key:
        return None
    try:
        text = value if type(value) is str else str(value)
        encoded_value = quote(text, safe="")
    except Exception:
        try:
            encoded_value = quote(f"<unserializable: {sanitize._type_name(value)}>", safe="")
        except Exception:
            return None
    return f"{encoded_key}={encoded_value}"


def _announce_dropped(count: int) -> None:
    """Counts dropped header members and announces the throttle crossings among them.

    Each member takes one value from :data:`_dropped_members`. The call writes one line if any
    value it drew is 1 or a multiple of ``_diag.WARN_EVERY``, carrying the last value it drew as
    the running total, the throttle every per-event site in the library uses. Each value is
    unique, so each crossing is announced by exactly one call even under concurrent calls. The
    line names a count only; ``_diag.lost`` cannot raise on a stream fault.

    Args:
      count: How many members this call dropped, at least 1.

    Returns:
      None.

    Raises:
      None.
    """
    crossed = False
    total = 0
    for _ in range(count):
        total = next(_dropped_members)
        if total == 1 or total % _diag.WARN_EVERY == 0:
            crossed = True
    if crossed:
        _diag.lost(
            "baggage member",
            total,
            "running total; members dropped from the outbound baggage header",
        )


def parse_baggage_header(header: object) -> dict[str, object] | None:
    """Parses a W3C ``baggage`` header into a mapping.

    This is total, like the ``traceparent`` parser, because the header arrives from outside
    the process. W3C allows per-member properties after a ``;`` and nothing here consumes
    them, so they are dropped rather than treated as part of the value.

    Args:
      header: An inbound header value of any type.

    Returns:
      The parsed mapping, or ``None`` for a non-string, an empty header, one over
      :data:`BAGGAGE_MAX_BYTES`, or a malformed member — never a partial parse, which would
      silently drop correlating fields.

    Raises:
      None.
    """
    if not isinstance(header, str) or not header.strip():
        return None
    if len(header.encode("utf-8", "replace")) > BAGGAGE_MAX_BYTES:
        return None
    parsed: dict[str, object] = {}
    for member in header.split(","):
        entry = member.split(";", 1)[0].strip()
        if not entry:
            continue
        key, sep, value = entry.partition("=")
        if not sep:
            return None
        key = unquote(key.strip())
        if not key:
            return None
        parsed[key] = unquote(value.strip())
    return parsed or None
