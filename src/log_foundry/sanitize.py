"""Coercion and size-bounding for event values (SPEC-017 FR-001, FR-002)."""

from __future__ import annotations

import re
import sys
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from math import isfinite
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
_FLOAT_REPR = float.__repr__
"""``float.__repr__``, unbound, so a subclass cannot divert the placeholder's text.

The f-string form ``f"<float: {value}>"`` calls ``format(value, "")``, which is the value's own
``__str__`` — so a ``float`` subclass chose what the library printed. Measured: a subclass whose
``__str__`` names its source emitted ``<float: inf from probe-7 (token=sk-live-…)>`` into the event
stream, and one returning two megabytes emitted all of it against a configured
``max_value_bytes`` of 8192. Both are the disclosure arch §6 forbids and the ceiling this module
exists to apply, defeated by the one new rule that handed the decision back to the value.

The same reasoning as :data:`_INT_LT` beside it, and as ``_placeholder``'s refusal to use
``repr()``: what the library writes about a value must be computed by the library.
"""

_STR_STR = str.__str__
"""``str.__str__``, unbound, so a ``str`` subclass cannot divert the measurement (SPEC-055 FR-001).

On an exact ``str`` it is the identity, so the hot path pays nothing; on a subclass it returns a
plain copy without consulting the subclass's own ``__str__`` or ``encode``. Measured before it:
``info(BadEncode("x"))``, where ``encode`` raises, cost the event — absorbed and announced, but
lost — because ``truncate_str`` called the value's own ``encode`` to measure it. The same
reasoning as :data:`_INT_LT` and :data:`_FLOAT_REPR`: what the library measures about a value
must be computed by the library.
"""

_SURROGATES = re.compile("[\ud800-\udfff]")
_REPLACEMENT = "\ufffd"
"""One U+FFFD per lone surrogate, which neither built-in error handler gives (SPEC-055 FR-001).

``errors="replace"`` on an *encode* writes ``?`` and loses the byte; a ``surrogatepass`` encode
followed by a ``replace`` decode writes three U+FFFD for one surrogate, because the decoder
rejects each of the three bytes separately. The substitution runs only after a strict encode has
already failed, so the common case never reaches it.
"""

_PLAIN_SCALARS: frozenset[type] = frozenset({int, bool})

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


def _measured(value: str) -> tuple[str, bytes, bool]:
    """Returns a string as an exact ``str`` that encodes strictly, its bytes, and whether it changed.

    A string carrying an unpaired surrogate — anything that went through ``surrogateescape``,
    such as ``os.fsdecode`` of an undecodable filename — raises ``UnicodeEncodeError`` on a bare
    ``.encode("utf-8")``. Before SPEC-055 this tolerated that with ``errors="replace"`` to
    *measure* the string and then returned the caller's string unchanged, so the surrogate left
    assembly intact: the JSON sinks escaped it, and every sink that hands a raw ``str`` to a
    driver — ``SQLiteSink``'s ``function`` column above all — raised on it and cost the whole
    batch its innocent neighbours. Invariant 8 says every string in the event encodes as UTF-8,
    so the replaced string is what is returned now, and the flag reports the substitution.

    The strict encode is tried first, because it is the common case and the C-level fast path;
    the regex runs only after it has failed. The value is first passed through
    :data:`_STR_STR`, so what is measured, and what is returned, is always an exact ``str``.

    Args:
      value: The string to measure.

    Returns:
      The exact ``str`` as measured, its strict UTF-8 bytes, and ``True`` when a surrogate was
      replaced.

    Raises:
      None.
    """
    plain = _STR_STR(value)
    try:
        return plain, plain.encode("utf-8"), False
    except UnicodeEncodeError:
        clean = _SURROGATES.sub(_REPLACEMENT, plain)
        return clean, clean.encode("utf-8"), True


def truncate_str(value: str, max_bytes: int) -> tuple[str, bool]:
    """Clips a string to a ceiling of UTF-8 bytes, keeping the head.

    The returned string, marker included, never exceeds the ceiling, so a caller sizing
    against a hard downstream limit can rely on the ceiling being a ceiling. The cut lands on
    a character boundary, so the result always decodes cleanly even when the budget falls
    mid-sequence; where there is no room for anything but the marker, a marker alone is still
    the honest answer.

    The result is an exact ``str`` that encodes as UTF-8 strictly, never the caller's own object
    (SPEC-055 FR-001): a lone surrogate is replaced by U+FFFD before the measurement, and the
    flag is therefore "the string was altered" rather than only "the ceiling fired" — a
    substitution nobody can see is a silent change to the data, which is the rule
    :meth:`_Coercer.real` already applies to a non-finite float.

    Args:
      value: The string to clip.
      max_bytes: The ceiling, in UTF-8 bytes.

    Returns:
      The clipped string and whether it was altered — clipped, or a surrogate replaced.

    Raises:
      None.
    """
    plain, raw, replaced = _measured(value)
    if len(raw) <= max_bytes:
        return plain, replaced
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
    than the marker. The result is an exact, strictly encodable ``str`` on the same terms as
    :func:`truncate_str`.

    Args:
      value: The string to clip.
      max_bytes: The ceiling, in UTF-8 bytes.

    Returns:
      The clipped string and whether it was altered — clipped, or a surrogate replaced.

    Raises:
      None.
    """
    plain, raw, replaced = _measured(value)
    if len(raw) <= max_bytes:
        return plain, replaced
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

    __slots__ = ("_cfg", "_int_ceiling", "_parents", "truncated")

    def __init__(self, cfg: Config) -> None:
        """Starts a pass with the configured ceilings and an empty ancestor chain.

        ``_parents`` holds the ``id()``s of the containers currently being descended through.
        Ancestors only, never a global "visited" set: an ancestor is held alive by the
        recursion for the whole descent, so its ``id()`` cannot be recycled underneath us, and
        two siblings referencing the same object are not a cycle and must both render.

        The integer ceiling is resolved here, once per pass rather than once per integer
        (SPEC-031 FR-004): it reads ``sys.get_int_max_str_digits()``, which cannot change
        during a coercion pass, and the alternative sat four lines below an ``int.__lt__``
        binding justified by this being a per-value hot path.

        Args:
          cfg: The config supplying ``max_value_bytes``, ``max_keys`` and ``max_depth``.

        Returns:
          None.

        Raises:
          None.
        """
        self._cfg = cfg
        self._int_ceiling = _int_digit_ceiling(cfg.max_value_bytes)
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
        if kind is float:
            return self.real(value)  # type: ignore[arg-type]
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
            if isinstance(member, float) and not isinstance(member, bool):
                return self.real(member)
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
            return self.real(value)
        if isinstance(value, (datetime, date, time)):
            return self.text(value.isoformat())
        if isinstance(value, UUID):
            return self.text(str(value))
        if isinstance(value, Decimal):
            return self.text(str(value))
        if isinstance(value, (bytes, bytearray, memoryview)):
            return self.text(self._decoded(bytes(value)))
        if isinstance(value, str):
            return self.text(value)
        if isinstance(value, Mapping):
            return self.mapping(value, depth)
        if isinstance(value, (set, frozenset)):
            return self.members(value, depth)
        if isinstance(value, Sequence) and not isinstance(value, _TEXTLIKE):
            return self.members(value, depth)
        return self._placeholder(value)

    def _decoded(self, value: bytes) -> str:
        """Decodes bytes as UTF-8, replacing what does not decode and recording that it did.

        Strict first, and only on ``UnicodeDecodeError`` with ``errors="replace"`` (SPEC-055
        FR-001): before this the replacement was unconditional and silent, so the same
        undecodable byte was marked when it arrived as a ``str`` and unmarked when it arrived as
        ``bytes``. The rule is the one every other substitution here follows — a change nobody
        can see is a change that sets ``truncated``.

        Args:
          value: The bytes to decode.

        Returns:
          The decoded text, with U+FFFD where a byte sequence was not UTF-8.

        Raises:
          None.
        """
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            self.truncated = True
            return value.decode("utf-8", errors="replace")

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
        """Coerces a mapping key to a bounded string, and is total (SPEC-055 FR-004).

        Whatever the key's own ``__str__``, ``bit_length`` or ``__lt__`` raises is caught here
        rather than in :meth:`value`, whose guard sits around the *enclosing* mapping: measured,
        ``{"sib": 1, KeyBoom(): 2}`` reached the sink as ``<unserializable: dict>`` with
        ``truncated`` unset, so one hostile key took every sibling with it — the exact outcome
        the integer branch below was written to prevent for one kind of key. The key becomes a
        ``<unserializable key: T>`` placeholder naming its type, on :meth:`_placeholder`'s
        no-``repr`` rule, and ``truncated`` is set: a value placeholder is visible on its own,
        but a key that could not be rendered is a substitution the reader must be told about.

        Two hostile keys of one type collide on one placeholder and the later wins, which is
        accepted: the alternative is a placeholder carrying something of the key's identity,
        which is what arch §6 forbids, and the collision is still marked because each replaced
        key sets ``truncated``. The guard is here rather than in :meth:`mapping`'s loop because
        the loop's other two failures — the ``max_keys`` cap and a value that cannot be coerced
        — already have owners, and a third ``try`` around the loop would hide which one fired.

        Args:
          key: The mapping key, of any type.

        Returns:
          The bounded string form of the key, or the key placeholder.

        Raises:
          None.
        """
        try:
            return self._key(key)
        except Exception:
            self.truncated = True
            return self.text(f"<unserializable key: {_type_name(key)}>")

    def _key(self, key: object) -> str:
        """Renders one mapping key, since JSON object keys are always strings.

        An integer key goes through :meth:`integer` first. A bare ``str()`` here would raise on
        an over-long one — the very ``ValueError`` this module exists to keep away from a sink
        — and the failure would be caught up in :meth:`key`, replacing the key with a
        placeholder where its digits would have done. ``bool``
        is excluded because ``True`` must render as the key ``"True"``, not ``"1"``.

        A float key goes through :meth:`real` for the same reason, and its ``text()`` wrap is
        defence in depth rather than load-bearing: ``real`` computes its placeholder through
        :data:`_FLOAT_REPR`, so it cannot hand back a long string. No test distinguishes the wrap
        from its absence, which is recorded here rather than pinned by an assertion that cannot
        fail.

        Args:
          key: The mapping key, of any type.

        Returns:
          The bounded string form of the key.

        Raises:
          Exception: Whatever the key's own ``__str__`` raises; :meth:`key` is the guard.
        """
        if isinstance(key, str):
            return self.text(key)
        if isinstance(key, int) and not isinstance(key, bool):
            rendered = self.integer(key)
            return self.text(rendered if isinstance(rendered, str) else str(rendered))
        if isinstance(key, float):
            replaced = self.real(key)
            return self.text(replaced if isinstance(replaced, str) else str(replaced))
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
        if rendered <= self._int_ceiling:
            return value
        self.truncated = True
        return f"<int: ~{digits} digits>"

    def real(self, value: float) -> object:
        """Replaces a non-finite float, since ``NaN`` and ``Infinity`` are not JSON.

        ``json.dumps`` writes ``NaN``, ``Infinity`` and ``-Infinity`` happily, and RFC 8259
        defines none of them: a strict consumer — Fluent Bit, a Logstash ``json`` codec, Jackson
        behind Elasticsearch — rejects the whole record, with nothing on the library side to see.
        That contradicts ``build_event``'s promise that an event is safe for any sink to
        serialize (SPEC-017).

        Replaced rather than coerced to ``None`` or ``0.0``, on SPEC-020's reasoning for the
        over-long integer this mirrors: a wrong number is worse than a visibly elided one, and
        the marker says which of the three it was. ``truncated`` is set for the same reason it is
        set everywhere else — a substitution nobody can see is a silent change to the data.

        Args:
          value: The float to check.

        Returns:
          The float itself when finite, or a ``<float: nan>`` / ``<float: inf>`` /
          ``<float: -inf>`` placeholder naming which it was.

        Raises:
          None.
        """
        if isfinite(value):
            return value
        self.truncated = True
        return f"<float: {_FLOAT_REPR(value)}>"

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
        object would land in the log. The placeholder goes through :meth:`text` like any other
        string (SPEC-055 FR-004): ``type.__name__`` is writable, so a type named with more than
        ``max_value_bytes`` would otherwise be the one string in the event no ceiling reached.

        Args:
          value: The value that could not be coerced.

        Returns:
          A ``<unserializable: type>`` placeholder.

        Raises:
          None. A pathological metaclass is still not this module's problem.
        """
        return self.text(f"<unserializable: {_type_name(value)}>")


def _type_name(value: object) -> str:
    """Returns a value's type name, or ``"?"`` when even that cannot be read.

    Shared by both placeholders so the no-``repr`` rule has one implementation. A metaclass
    whose ``__name__`` is a raising property is the case the fallback exists for.

    Args:
      value: The value whose type is named.

    Returns:
      The type's ``__name__``, or ``"?"``.

    Raises:
      None.
    """
    try:
        return str(type(value).__name__)
    except Exception:
        return "?"


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
