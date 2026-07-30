"""Coercion and size-bounding for event values (SPEC-017 FR-001, FR-002).

An event dict must never contain a value ``json.dumps`` would reject, and no value may be
unbounded. Both guarantees are established **once**, where the event is assembled, rather than
in each sink: one pass per event instead of one per destination (which matters under
``MultiSink``), and the guarantee then holds for the non-JSON sinks — ``postgres``, ``mongo``,
``sqlite`` — for free. Every bare ``json.dumps`` in ``sinks/`` is correct by consequence.

Three rules worth knowing before changing anything here:

* **Total by contract.** Nothing in this module raises. It runs on the caller's own stack (a
  level call with no active span emits synchronously, ``api.py``), so an exception escaping
  here is precisely the failure SPEC-017 exists to remove.
* **The ``truncated`` marker means a *ceiling* fired** — ``max_value_bytes``, ``max_stack_bytes``,
  ``max_keys`` or ``max_depth``. It is *not* set by :data:`_CIRCULAR` or an unserializable
  placeholder, which are coercion outcomes, not clipping.
* **``max_depth`` is what bounds the recursion**, not cycle detection. A cycle shallower than the
  depth limit terminates there regardless; the ancestor tracking exists only so the value reads
  ``<circular>`` rather than ``<depth limit>``. Don't "harden" it into a safety mechanism.

The unserializable fallback is a type-name placeholder rather than ``repr(value)`` on purpose.
Architecture §6 refuses to auto-capture argument and return values so the library cannot leak
secrets or PII, and ``repr()`` of an arbitrary object routinely prints attribute values — a
credential held on a client object would land in the log. The placeholder identifies what was
dropped without disclosing it.
"""

from __future__ import annotations

import sys
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any
from uuid import UUID

if TYPE_CHECKING:
    # Type-only import: this module then has *no* runtime dependency on any other package
    # module, so it can never take part in an import cycle. Same idiom as ``config.Sink``.
    from log_foundry.config import Config

__all__ = ["TRUNCATION_MARKER", "coerce", "sanitize_fields", "truncate_str", "truncate_tail"]

TRUNCATION_MARKER = "…[truncated]"
_MARKER_BYTES = len(TRUNCATION_MARKER.encode("utf-8"))

_CIRCULAR = "<circular>"
_DEPTH_LIMIT = "<depth limit>"

# ``log10(2)`` as an integer ratio, rounded *up*: ``|n| < 2**b``, so ``n`` has at most
# ``b * _LOG10_2_NUM // _LOG10_2_DEN + 1`` decimal digits. Rounding the ratio up makes that an
# over-estimate, which is the safe direction — it can replace an integer marginally short of the
# ceiling, but never admit one past it (SPEC-020 FR-002).
_LOG10_2_NUM = 30103
_LOG10_2_DEN = 100000

# Exact-type membership, deliberately not ``isinstance``. ``IntEnum``/``StrEnum`` members *are*
# ``int``/``str`` instances, so an isinstance check would pass the enum member itself through and
# hand a sink an ``Enum`` where a plain value was promised. Exact typing lets them fall to the
# ``Enum`` branch and degrade to ``.value``. It is also one hash lookup rather than an
# ``ABCMeta.__instancecheck__``.
_PLAIN_SCALARS: frozenset[type] = frozenset({int, float, bool})

# ``str``/``bytes``/``bytearray`` are Sequences, and so — less obviously — is ``memoryview``.
# Without this guard ``memoryview(b"x")`` would render as ``[120]`` instead of ``"x"``.
_TEXTLIKE: tuple[type, ...] = (str, bytes, bytearray, memoryview)


def _int_digit_ceiling(max_value_bytes: int) -> int:
    """The largest decimal length an integer may have and still be rendered (SPEC-020 FR-001).

    CPython 3.11+ refuses to convert an integer past ``sys.get_int_max_str_digits()`` digits and
    raises ``ValueError``, which ``json.dumps`` inherits. A configured ceiling above that cannot be
    honoured — rendering such an integer is the very thing that raises — so the interpreter's limit
    wins whenever it is lower. A limit of ``0`` means the interpreter imposes none.
    """
    limit = sys.get_int_max_str_digits()
    return max_value_bytes if limit <= 0 else min(max_value_bytes, limit)


def _measured(value: str) -> bytes:
    """UTF-8 bytes of ``value``, tolerating lone surrogates.

    A ``str`` carrying an unpaired surrogate (anything that went through ``surrogateescape``,
    e.g. ``os.fsdecode`` of an undecodable filename) raises ``UnicodeEncodeError`` on a bare
    ``.encode("utf-8")`` — inside a function contracted never to raise. ``errors="replace"``
    is the same tolerance ``context`` applies when measuring an inbound baggage header.
    """
    return value.encode("utf-8", errors="replace")


def truncate_str(value: str, max_bytes: int) -> tuple[str, bool]:
    """Clip ``value`` to ``max_bytes`` UTF-8 bytes, keeping the head.

    The returned string — marker included — never exceeds ``max_bytes``, so a caller sizing
    against a hard downstream limit can rely on the ceiling being a ceiling. Cuts on a character
    boundary, so the result always decodes cleanly even when the budget falls mid-sequence.

    Returns ``(value, was_truncated)``.
    """
    raw = _measured(value)
    if len(raw) <= max_bytes:
        return value, False
    budget = max_bytes - _MARKER_BYTES
    if budget <= 0:
        # No room for anything but the marker — and a marker alone is still the honest answer.
        return TRUNCATION_MARKER, True
    # ``errors="ignore"`` drops a partial trailing sequence rather than emitting U+FFFD, which
    # is what makes the cut land on a character boundary.
    return raw[:budget].decode("utf-8", errors="ignore") + TRUNCATION_MARKER, True


def truncate_tail(value: str, max_bytes: int) -> tuple[str, bool]:
    """Clip ``value`` to ``max_bytes`` UTF-8 bytes, keeping the **tail**.

    For ``error.stack``: ``traceback.format_exception`` puts the exception type, its message and
    the innermost frames *last*, so the head of an over-long traceback is the least useful part
    of it. The marker is prepended, and the total stays within ``max_bytes`` as above.

    Returns ``(value, was_truncated)``.
    """
    raw = _measured(value)
    if len(raw) <= max_bytes:
        return value, False
    budget = max_bytes - _MARKER_BYTES
    if budget <= 0:
        return TRUNCATION_MARKER, True
    # ``raw[-budget:]`` would return the *whole* string at ``budget == 0``, which is why the
    # guard above is not merely defensive: ``max_stack_bytes`` may legally be smaller than the
    # marker, and silently truncating nothing would be worse than truncating everything.
    return TRUNCATION_MARKER + raw[-budget:].decode("utf-8", errors="ignore"), True


class _Coercer:
    """One event's coercion pass: the ceilings, the truncation flag, and the ancestor chain.

    A pass object rather than a pure function because the ceilings fire *deep* in the recursion
    — a 300-key mapping eight levels down still has to set ``truncated`` on the top-level event —
    and a recursive function returning a bare value has nowhere to report that. One instance per
    event, ``__slots__``-ed; against the timestamp, UUID and two dicts ``build_event`` already
    allocates per event, it is noise.
    """

    __slots__ = ("_cfg", "_parents", "truncated")

    def __init__(self, cfg: Config) -> None:
        self._cfg = cfg
        self.truncated = False
        # ``id()``s of the containers currently being descended through. Ancestors only, never a
        # global "visited" set: an ancestor is held alive by the recursion for the whole descent,
        # so its ``id()`` cannot be recycled underneath us, and two siblings referencing the same
        # object are not a cycle and must both render.
        self._parents: list[int] = []

    def value(self, value: object, depth: int) -> object:
        """Coerce one node. Total — never raises, whatever the value does."""
        try:
            return self._dispatch(value, depth)
        except Exception:  # a hostile __iter__/__str__/__eq__ must not reach the
            return self._placeholder(value)  # caller's stack; this is the whole point (FR-001).

    def _dispatch(self, value: object, depth: int) -> object:
        if depth >= self._cfg.max_depth:
            self.truncated = True
            return _DEPTH_LIMIT
        if value is None:
            return None

        # Exact types first: the common case, one hash lookup, and it is what lets IntEnum and
        # StrEnum fall through to the Enum branch below instead of passing through as themselves.
        kind = type(value)
        if kind is str:
            return self.text(value)  # type: ignore[arg-type]
        if kind is int:  # before _PLAIN_SCALARS: `int` is the one scalar with no natural ceiling.
            return self.integer(value)  # type: ignore[arg-type]
        if kind in _PLAIN_SCALARS:  # float (IEEE-754-bounded) and bool
            return value
        if kind is dict:
            return self.mapping(value, depth)  # type: ignore[arg-type]
        if kind is list:
            return self.members(value, depth)  # type: ignore[arg-type]

        if isinstance(value, Enum):
            member = value.value
            if type(member) is int:  # an IntEnum member is as unbounded as a bare int.
                return self.integer(member)
            if type(member) in _PLAIN_SCALARS or member is None:
                return member
            if isinstance(member, str):
                return self.text(member)
            # A structured ``.value`` (a tuple payload, say) has no plain form; ``str(value)``
            # renders the member as ``Class.NAME``, which identifies it without a ``repr``.
            return self.text(str(value))
        if isinstance(value, bool):  # bool before int: it is an int subclass.
            return value
        if isinstance(value, int):  # int *subclasses* — bounded like the exact type above.
            return self.integer(value)
        if isinstance(value, float):  # float *subclasses*.
            return value
        if isinstance(value, (datetime, date, time)):
            return self.text(value.isoformat())
        if isinstance(value, UUID):
            return self.text(str(value))
        if isinstance(value, Decimal):
            # A string, not a float: ``Decimal("0.1")`` must not become ``0.1000000000000000055``.
            return self.text(str(value))
        if isinstance(value, (bytes, bytearray, memoryview)):
            return self.text(bytes(value).decode("utf-8", errors="replace"))
        if isinstance(value, str):  # ``str`` subclass.
            return self.text(str(value))
        if isinstance(value, Mapping):
            return self.mapping(value, depth)
        if isinstance(value, (set, frozenset)):  # sets are not Sequences.
            return self.members(value, depth)
        if isinstance(value, Sequence) and not isinstance(value, _TEXTLIKE):
            return self.members(value, depth)
        return self._placeholder(value)

    def mapping(self, value: Mapping[Any, object], depth: int) -> object:
        """Coerce a mapping, capping it at ``max_keys`` and guarding against a cycle.

        The key type is ``Any`` rather than ``object`` because ``Mapping`` is invariant in its
        key: a ``Mapping[str, object]`` (which is what ``build_event`` passes) is not a
        ``Mapping[object, object]``. :meth:`key` coerces whatever actually arrives.
        """
        ident = id(value)
        if ident in self._parents:
            return _CIRCULAR
        self._parents.append(ident)
        try:
            out: dict[str, object] = {}
            for key, item in value.items():
                if len(out) >= self._cfg.max_keys:
                    self.truncated = True
                    break
                out[self.key(key)] = self.value(item, depth + 1)
            return out
        finally:
            self._parents.pop()

    def members(self, value: Iterable[object], depth: int) -> object:
        """Coerce an iterable's members into a list, capping length at ``max_keys``."""
        ident = id(value)
        if ident in self._parents:
            return _CIRCULAR
        self._parents.append(ident)
        try:
            out: list[object] = []
            for item in value:
                if len(out) >= self._cfg.max_keys:
                    self.truncated = True
                    break
                out.append(self.value(item, depth + 1))
            return out
        finally:
            self._parents.pop()

    def key(self, key: object) -> str:
        """Coerce a mapping key to a bounded ``str`` — JSON object keys are always strings.

        An integer key goes through :meth:`integer` first. A bare ``str()`` here would raise on an
        over-long one — the very ``ValueError`` this module exists to keep away from a sink — and
        the failure would be caught up in :meth:`value`, replacing the *whole mapping* with a
        placeholder. One hostile key would take every sibling key with it, unmarked. ``bool`` is
        excluded because ``True`` must render as the key ``"True"``, not ``"1"``.
        """
        if isinstance(key, str):
            return self.text(key)
        if isinstance(key, int) and not isinstance(key, bool):
            rendered = self.integer(key)
            return self.text(rendered if isinstance(rendered, str) else str(rendered))
        return self.text(str(key))

    def integer(self, value: int) -> object:
        """Return ``value`` unchanged, or a placeholder when it is too long to render (FR-001).

        The size test is ``bit_length()``, never ``len(str(value))``: converting an over-long
        integer to a string raises the very ``ValueError`` this bound exists to prevent, so the
        obvious check would move the crash rather than remove it (FR-002). ``bit_length()`` is
        O(1), total, and ignores the sign, so ``n`` and ``-n`` are bounded identically.

        An over-long integer is *replaced*, not clipped. Dropping digits would silently change the
        value, and a wrong number is worse than a visibly elided one — so this reuses the
        type-naming shape of :meth:`_placeholder` rather than inventing a second elision style.

        The bound trusts ``bit_length()``. An ``int`` subclass that overrides it to understate its
        own magnitude defeats this, and nothing here can tell — the call does not raise, so the
        totality guard in :meth:`value` never engages either. That is the same trust every coercion
        rule extends to a subclass's dunders, and narrowing it would cost the common path a
        ``type()`` check to catch only a value engineered to lie about itself.
        """
        digits = value.bit_length() * _LOG10_2_NUM // _LOG10_2_DEN + 1
        if digits <= _int_digit_ceiling(self._cfg.max_value_bytes):
            return value
        self.truncated = True
        return f"<int: ~{digits} digits>"

    def text(self, value: str) -> str:
        """Apply ``max_value_bytes``, recording whether it fired."""
        clipped, was_truncated = truncate_str(value, self._cfg.max_value_bytes)
        if was_truncated:
            self.truncated = True
        return clipped

    def _placeholder(self, value: object) -> str:
        """Name the type that could not be coerced, without disclosing the value."""
        try:
            name = type(value).__name__
        except Exception:  # a pathological metaclass is still not our problem.
            name = "?"
        return f"<unserializable: {name}>"


def coerce(value: object, *, cfg: Config) -> object:
    """Return a JSON-serializable, size-bounded equivalent of ``value``. Never raises.

    ``value`` is treated as a field value, i.e. depth 0 — the same level it would occupy inside
    an event's ``fields``. Use :func:`sanitize_fields` for a whole event mapping; it reports
    whether a ceiling fired, which this cannot.
    """
    return _Coercer(cfg).value(value, 0)


def sanitize_fields(
    fields: Mapping[str, object], *, cfg: Config
) -> tuple[dict[str, object], bool]:
    """Coerce and bound a whole ``fields`` mapping.

    Returns ``(fields, any_ceiling_was_applied)``. The flag drives the event's ``truncated``
    marker, so it must reflect ceilings that fired arbitrarily deep in the structure — hence the
    accumulator rather than a pure recursive function.
    """
    coercer = _Coercer(cfg)
    try:
        # ``-1`` so the *values* of the top-level mapping sit at depth 0: ``fields`` is the event's
        # payload container, not a level of nesting the caller chose. Otherwise ``max_depth=1``
        # would replace every field value with ``<depth limit>`` and emit a uniformly empty event,
        # which is exactly what FR-006's validation exists to prevent.
        result = coercer.mapping(fields, -1)
    except Exception:  # belt and braces: `_Coercer.value` is already total, but
        return {}, True  # a hostile top-level mapping must not reach the caller either.
    if not isinstance(result, dict):
        # ``fields`` was itself circular — impossible from ``build_event``, which always passes a
        # freshly merged dict, but ``sanitize_fields`` is public and must stay total.
        return {}, True
    return result, coercer.truncated
