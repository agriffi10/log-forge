"""Coercion and size-bounding for event values (SPEC-017 FR-001, FR-002)."""

from __future__ import annotations

import sys
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any
from uuid import UUID

if TYPE_CHECKING:
    from log_foundry.config import Config

__all__ = ["TRUNCATION_MARKER", "coerce", "sanitize_fields", "truncate_str", "truncate_tail"]

TRUNCATION_MARKER = "…[truncated]"
_MARKER_BYTES = len(TRUNCATION_MARKER.encode("utf-8"))

_CIRCULAR = "<circular>"
_DEPTH_LIMIT = "<depth limit>"

_LOG10_2_NUM = 30103
_LOG10_2_DEN = 100000

_INT_LT = int.__lt__

_PLAIN_SCALARS: frozenset[type] = frozenset({int, float, bool})

_TEXTLIKE: tuple[type, ...] = (str, bytes, bytearray, memoryview)


def _int_digit_ceiling(max_value_bytes: int) -> int:
    """Returns the largest decimal length an integer may have and still render (SPEC-020 FR-001).

    CPython 3.11+ refuses to convert an integer past ``sys.get_int_max_str_digits()`` digits
    and raises ``ValueError``, which ``json.dumps`` inherits, so a configured ceiling above
    that cannot be honoured and the interpreter's limit wins whenever it is lower. A limit of
    ``0`` means the interpreter imposes none. The caller measures a negative value's rendered
    length, sign included, against this while the interpreter's own limit counts digits only,
    so a negative integer sitting exactly on the interpreter bound is replaced rather than
    rendered — one value at the far edge, in the direction this module always errs.

    Args:
      max_value_bytes: The configured per-value ceiling.

    Returns:
      The effective digit ceiling.

    Raises:
      None.
    """
    limit = sys.get_int_max_str_digits()
    return max_value_bytes if limit <= 0 else min(max_value_bytes, limit)


def _measured(value: str) -> bytes:
    """Returns the UTF-8 bytes of a string, tolerating lone surrogates.

    A string carrying an unpaired surrogate — anything that went through
    ``surrogateescape``, such as ``os.fsdecode`` of an undecodable filename — raises
    ``UnicodeEncodeError`` on a bare ``.encode("utf-8")``, inside a module contracted never to
    raise. ``errors="replace"`` is the same tolerance ``context`` applies when measuring an
    inbound baggage header.

    Args:
      value: The string to measure.

    Returns:
      The encoded bytes.

    Raises:
      None.
    """
    return value.encode("utf-8", errors="replace")


def truncate_str(value: str, max_bytes: int) -> tuple[str, bool]:
    """Clips a string to a ceiling of UTF-8 bytes, keeping the head.

    The returned string, marker included, never exceeds the ceiling, so a caller sizing
    against a hard downstream limit can rely on the ceiling being a ceiling. The cut lands on
    a character boundary, so the result always decodes cleanly even when the budget falls
    mid-sequence; where there is no room for anything but the marker, a marker alone is still
    the honest answer.

    Args:
      value: The string to clip.
      max_bytes: The ceiling, in UTF-8 bytes.

    Returns:
      The clipped string and whether the ceiling fired.

    Raises:
      None.
    """
    raw = _measured(value)
    if len(raw) <= max_bytes:
        return value, False
    budget = max_bytes - _MARKER_BYTES
    if budget <= 0:
        return TRUNCATION_MARKER, True
    return raw[:budget].decode("utf-8", errors="ignore") + TRUNCATION_MARKER, True


def truncate_tail(value: str, max_bytes: int) -> tuple[str, bool]:
    """Clips a string to a ceiling of UTF-8 bytes, keeping the tail.

    This is for ``error.stack``: ``traceback.format_exception`` puts the exception type, its
    message and the innermost frames last, so the head of an over-long traceback is the least
    useful part of it. The zero-budget guard is not merely defensive — ``raw[-budget:]`` would
    return the whole string at a budget of zero, and ``max_stack_bytes`` may legally be smaller
    than the marker.

    Args:
      value: The string to clip.
      max_bytes: The ceiling, in UTF-8 bytes.

    Returns:
      The clipped string and whether the ceiling fired.

    Raises:
      None.
    """
    raw = _measured(value)
    if len(raw) <= max_bytes:
        return value, False
    budget = max_bytes - _MARKER_BYTES
    if budget <= 0:
        return TRUNCATION_MARKER, True
    return TRUNCATION_MARKER + raw[-budget:].decode("utf-8", errors="ignore"), True


class _Coercer:
    """One event's coercion pass: the ceilings, the truncation flag, and the ancestor chain.

    This is a pass object rather than a pure function because the ceilings fire deep in the
    recursion — a 300-key mapping eight levels down still has to set ``truncated`` on the
    top-level event — and a recursive function returning a bare value has nowhere to report
    that. One instance per event, ``__slots__``-ed; against the timestamp, UUID and two dicts
    ``build_event`` already allocates per event, it is noise.
    """

    __slots__ = ("_cfg", "_parents", "truncated")

    def __init__(self, cfg: Config) -> None:
        """Starts a pass with the configured ceilings and an empty ancestor chain.

        ``_parents`` holds the ``id()``s of the containers currently being descended through.
        Ancestors only, never a global "visited" set: an ancestor is held alive by the
        recursion for the whole descent, so its ``id()`` cannot be recycled underneath us, and
        two siblings referencing the same object are not a cycle and must both render.

        Args:
          cfg: The config supplying ``max_value_bytes``, ``max_keys`` and ``max_depth``.

        Returns:
          None.

        Raises:
          None.
        """
        self._cfg = cfg
        self.truncated = False
        self._parents: list[int] = []

    def value(self, value: object, depth: int) -> object:
        """Coerces one node of the structure.

        Args:
          value: The node to coerce.
          depth: How deep the node sits, measured against ``max_depth``.

        Returns:
          A JSON-serializable, bounded equivalent, or a placeholder naming the type.

        Raises:
          None. A hostile ``__iter__``, ``__str__`` or ``__eq__`` must not reach the caller's
            stack, which is the whole point of FR-001.
        """
        try:
            return self._dispatch(value, depth)
        except Exception:
            return self._placeholder(value)

    def _dispatch(self, value: object, depth: int) -> object:
        """Routes one node to the rule for its type.

        Exact types are tested first: it is the common case at one hash lookup, and it is what
        lets ``IntEnum`` and ``StrEnum`` members fall through to the ``Enum`` branch instead of
        passing through as themselves and handing a sink an ``Enum`` where a plain value was
        promised. ``int`` is tested ahead of the other scalars because it is the one with no
        natural ceiling, ``bool`` ahead of ``int`` among the subclass branches because it is an
        ``int`` subclass, and ``Decimal`` renders as a string so that ``Decimal("0.1")`` does
        not become ``0.1000000000000000055``.

        Args:
          value: The node to route.
          depth: How deep the node sits, measured against ``max_depth``.

        Returns:
          The coerced node.

        Raises:
          Exception: Whatever the value's own dunders raise; :meth:`value` is the guard.
        """
        if depth >= self._cfg.max_depth:
            self.truncated = True
            return _DEPTH_LIMIT
        if value is None:
            return None

        kind = type(value)
        if kind is str:
            return self.text(value)  # type: ignore[arg-type]
        if kind is int:
            return self.integer(value)  # type: ignore[arg-type]
        if kind in _PLAIN_SCALARS:
            return value
        if kind is dict:
            return self.mapping(value, depth)  # type: ignore[arg-type]
        if kind is list:
            return self.members(value, depth)  # type: ignore[arg-type]

        if isinstance(value, Enum):
            member = value.value
            if type(member) is int:
                return self.integer(member)
            if type(member) in _PLAIN_SCALARS or member is None:
                return member
            if isinstance(member, str):
                return self.text(member)
            return self.text(str(value))
        if isinstance(value, bool):
            return value
        if isinstance(value, int):
            return self.integer(value)
        if isinstance(value, float):
            return value
        if isinstance(value, (datetime, date, time)):
            return self.text(value.isoformat())
        if isinstance(value, UUID):
            return self.text(str(value))
        if isinstance(value, Decimal):
            return self.text(str(value))
        if isinstance(value, (bytes, bytearray, memoryview)):
            return self.text(bytes(value).decode("utf-8", errors="replace"))
        if isinstance(value, str):
            return self.text(str(value))
        if isinstance(value, Mapping):
            return self.mapping(value, depth)
        if isinstance(value, (set, frozenset)):
            return self.members(value, depth)
        if isinstance(value, Sequence) and not isinstance(value, _TEXTLIKE):
            return self.members(value, depth)
        return self._placeholder(value)

    def mapping(self, value: Mapping[Any, object], depth: int) -> object:
        """Coerces a mapping, capping it at ``max_keys`` and guarding against a cycle.

        The key type is ``Any`` rather than ``object`` because ``Mapping`` is invariant in its
        key: a ``Mapping[str, object]``, which is what ``build_event`` passes, is not a
        ``Mapping[object, object]``. :meth:`key` coerces whatever actually arrives.

        Args:
          value: The mapping to coerce.
          depth: How deep the mapping sits, measured against ``max_depth``.

        Returns:
          The coerced mapping, or ``"<circular>"`` when it is its own ancestor.

        Raises:
          Exception: Whatever the mapping's own iteration raises; :meth:`value` is the guard.
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
        """Coerces an iterable's members into a list, capping length at ``max_keys``.

        Args:
          value: The iterable to coerce.
          depth: How deep the iterable sits, measured against ``max_depth``.

        Returns:
          The coerced list, or ``"<circular>"`` when it is its own ancestor.

        Raises:
          Exception: Whatever the iterable's own iteration raises; :meth:`value` is the guard.
        """
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
        """Coerces a mapping key to a bounded string, since JSON object keys are always strings.

        An integer key goes through :meth:`integer` first. A bare ``str()`` here would raise on
        an over-long one — the very ``ValueError`` this module exists to keep away from a sink
        — and the failure would be caught up in :meth:`value`, replacing the whole mapping with
        a placeholder, so one hostile key would take every sibling with it, unmarked. ``bool``
        is excluded because ``True`` must render as the key ``"True"``, not ``"1"``.

        Args:
          key: The mapping key, of any type.

        Returns:
          The bounded string form of the key.

        Raises:
          Exception: Whatever the key's own ``__str__`` raises; :meth:`value` is the guard.
        """
        if isinstance(key, str):
            return self.text(key)
        if isinstance(key, int) and not isinstance(key, bool):
            rendered = self.integer(key)
            return self.text(rendered if isinstance(rendered, str) else str(rendered))
        return self.text(str(key))

    def integer(self, value: int) -> object:
        """Returns an integer unchanged, or a placeholder when it is too long to render.

        The size test is ``bit_length()``, never ``len(str(value))``: converting an over-long
        integer to a string raises the very ``ValueError`` this bound exists to prevent, so the
        obvious check would move the crash rather than remove it (FR-002). It ignores the sign,
        so the minus sign is added back explicitly, read through an unbound ``int.__lt__`` that
        an ``int`` subclass cannot divert — ``value < 0`` dispatches to user code, and a raise
        there would be caught up in :meth:`value` and take every sibling key with it.

        An over-long integer is replaced, not clipped, because dropping digits would silently
        change the value and a wrong number is worse than a visibly elided one. The bound
        trusts ``bit_length()``: a subclass that overrides it to understate its own magnitude
        defeats this and nothing here can tell, which is the same trust every coercion rule
        extends to a subclass's dunders.

        Args:
          value: The integer to bound.

        Returns:
          The integer itself, or a ``<int: ~N digits>`` placeholder naming the digit count.

        Raises:
          None.
        """
        digits = value.bit_length() * _LOG10_2_NUM // _LOG10_2_DEN + 1
        rendered = digits + 1 if _INT_LT(value, 0) else digits
        if rendered <= _int_digit_ceiling(self._cfg.max_value_bytes):
            return value
        self.truncated = True
        return f"<int: ~{digits} digits>"

    def text(self, value: str) -> str:
        """Applies ``max_value_bytes`` to a string, recording whether it fired.

        Args:
          value: The string to bound.

        Returns:
          The bounded string.

        Raises:
          None.
        """
        clipped, was_truncated = truncate_str(value, self._cfg.max_value_bytes)
        if was_truncated:
            self.truncated = True
        return clipped

    def _placeholder(self, value: object) -> str:
        """Names the type that could not be coerced, without disclosing the value.

        A type name rather than ``repr(value)`` on purpose: arch §6 refuses to auto-capture
        argument and return values so the library cannot leak secrets or PII, and ``repr()`` of
        an arbitrary object routinely prints attribute values — a credential held on a client
        object would land in the log.

        Args:
          value: The value that could not be coerced.

        Returns:
          A ``<unserializable: type>`` placeholder.

        Raises:
          None. A pathological metaclass is still not this module's problem.
        """
        try:
            name = type(value).__name__
        except Exception:
            name = "?"
        return f"<unserializable: {name}>"


def coerce(value: object, *, cfg: Config) -> object:
    """Returns a JSON-serializable, size-bounded equivalent of a value.

    The value is treated as a field value, at depth 0 — the same level it would occupy inside
    an event's ``fields``. Use :func:`sanitize_fields` for a whole event mapping; it reports
    whether a ceiling fired, which this cannot.

    Args:
      value: The value to coerce.
      cfg: The config supplying the ceilings.

    Returns:
      The coerced value.

    Raises:
      None.
    """
    return _Coercer(cfg).value(value, 0)


def sanitize_fields(
    fields: Mapping[str, object], *, cfg: Config
) -> tuple[dict[str, object], bool]:
    """Coerces and bounds a whole ``fields`` mapping.

    The mapping is entered at depth ``-1`` so its values sit at depth 0: ``fields`` is the
    event's payload container, not a level of nesting the caller chose, and otherwise
    ``max_depth=1`` would replace every field value with ``<depth limit>`` and emit a
    uniformly empty event. The truncation flag drives the event's ``truncated`` marker, so it
    must reflect ceilings that fired arbitrarily deep in the structure — hence the accumulator
    rather than a pure recursive function.

    Args:
      fields: The mapping to coerce.
      cfg: The config supplying the ceilings.

    Returns:
      The coerced mapping and whether any ceiling was applied.

    Raises:
      None. :meth:`_Coercer.value` is already total, but this is public and must stay total
        even for a hostile top-level mapping or one that is itself circular.
    """
    coercer = _Coercer(cfg)
    try:
        result = coercer.mapping(fields, -1)
    except Exception:
        return {}, True
    if not isinstance(result, dict):
        return {}, True
    return result, coercer.truncated
