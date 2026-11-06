"""Coercion and size-bounding for event values (SPEC-017 FR-001, FR-002)."""

from __future__ import annotations

import json
import re
import sys
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from math import isfinite
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

if TYPE_CHECKING:
    from log_foundry.config import Config

__all__ = ["TRUNCATION_MARKER", "coerce", "sanitize_fields", "truncate_str", "truncate_tail"]

TRUNCATION_MARKER = "…[truncated]"
_MARKER_BYTES = len(TRUNCATION_MARKER.encode("utf-8"))

_CIRCULAR = "<circular>"
_DEPTH_LIMIT = "<depth limit>"
_NODE_LIMIT = "<node limit>"
_SIZE_LIMIT = "<size limit>"

_MAX_NODES: int = 131_072
"""The most values one budgeted coercion pass visits, scalar or container (SPEC-057 FR-001).

Twice a fully populated two-level structure at the default ``max_keys`` (256 by 256, 65,536
values), so a wide but ordinary event is not cut, and no larger, so the worst pass stays small. A
shared sublist is a DAG, not a cycle: ``y = [y] * 16`` applied seven times is 16^7 visits to the
leaf, which the ancestor check never cuts and the default ``max_depth`` and ``max_keys`` allow. Memoising by ``id()`` was rejected
because the output must be a JSON tree, so a DAG's output is exponential anyway; a global visited
set because two siblings referencing one object must both render; and a placeholder per value
past the budget, the shape of ``<depth limit>``, because it still writes up to (``max_depth`` + 1)
times ``max_keys`` of them after the breach. Private and not a ``configure()`` key:
:class:`_Coercer` reads it once per pass, so a test may patch it, which carries no promise.
"""

_MAX_RENDERED_BYTES: int = 16 * 1024 * 1024
"""The most bytes of text one budgeted coercion pass charges, a work budget (SPEC-057 FR-002).

Not a sink's size limit: a destination's hard limit is met by lowering ``max_value_bytes`` or
``max_keys`` (``SQSSink`` sends at most 256 KiB). ``MongoDBSink``'s 16 MiB document limit is the
same number, so through assembly one string over it becomes ``<size limit>`` before that sink's
check sees it; escaping or the envelope can still carry an event past that limit. And reaching this budget takes at least 2,048 full strings at the default 8 KiB ceiling, so in
practice it only catches amplification — one ``Decimal`` of ten million digits referenced 256
times is 2,560,000,000 characters of ``str()``. ``json.dumps`` escapes a control character as
six bytes, so the serialised worst case is about six times this per pass, and a boundary event
takes two passes. Read once per pass, like :data:`_MAX_NODES`.
"""

_LOG10_2_NUM = 30103
_LOG10_2_DEN = 100000

_INT_LT = int.__lt__
_INT_BIT_LENGTH = int.bit_length
"""``int.bit_length``, unbound, so an ``int`` subclass cannot divert the measure of its own size.

Every integer measurement here starts from it: the integer ceiling, the rendered length an
integer value is charged against the byte budget, and the lower bound that decides whether an
integer key is rendered at all (SPEC-057 FR-002). Measured before it, at ``b282c1d``: an ``int``
subclass whose own ``bit_length`` returned ``nan``, written ahead of 2,500 references to one
8 KiB string, made the pass's running charge ``nan``, so every later test against the budget
was false and all 2,500 were written, 20,480,000 bytes against 16 MiB; one returning a negative
number credited the budget instead. The unbound accessor reads the magnitude the object stores,
and refuses with ``TypeError`` an object that only claims to be an ``int`` through
``__class__``. The same rule as :data:`_INT_LT` beside it and :data:`_FLOAT_REPR` below: what
the library measures about a value must be computed by the library.
"""
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

_STR_LEN = str.__len__
"""The unbound accessors that bound a read by its ceiling, not its length (SPEC-057 FR-003).

``_STR_LEN`` and the three slices below it. A string longer than its ceiling in characters cannot
fit, since every character is at least one byte, so :func:`truncate_str` and :func:`truncate_tail`
read only the ceiling plus one character of it, and the coercer's bytes-like branch only as many
bytes as cover the ceiling plus one. Each read goes through its type's own unbound accessor for
the reason :data:`_STR_STR` gives: a subclass's ``__len__`` or ``__getitem__`` would otherwise
choose what the library measures. ``str.__getitem__`` returns an exact ``str`` of the stored
characters, and ``bytes.__getitem__`` and ``bytearray.__getitem__`` an exact ``bytes`` and
``bytearray`` of the stored bytes, on a subclass as on the base type. A slice past the end is
clamped, so a bytes-like needs no length read: the slice is the whole value when it is shorter.
"""
_STR_SLICE = str.__getitem__
_BYTES_SLICE = bytes.__getitem__
_BYTEARRAY_SLICE = bytearray.__getitem__

_SURROGATES = re.compile("[\ud800-\udfff]")
_REPLACEMENT = "\ufffd"
"""One U+FFFD per lone surrogate, which neither built-in error handler gives (SPEC-055 FR-001).

``errors="replace"`` on an *encode* writes ``?`` and loses the byte; a ``surrogatepass`` encode
followed by a ``replace`` decode writes three U+FFFD for one surrogate, because the decoder
rejects each of the three bytes separately. The substitution runs only after a strict encode has
already failed, so the common case never reaches it.
"""

_INT_INT = int.__int__
"""``int.__int__``, unbound, the one way an ``int`` subclass value enters the event (SPEC-063).

A route that returns the caller's object, or lets the object's own method decide a measurement,
admits it only by an exact-type identity test or by converting it through an unbound builtin
accessor first. ``isinstance`` consults ``__class__`` and a type-set membership test consults the
metaclass's ``__hash__`` and ``__eq__``, both of which the caller's object controls. Measured at
``71e42dd``: an object whose ``__class__`` returned ``bool``, one whose ``__class__`` returned
``float`` with ``__float__`` returning ``1.0``, and an instance of a class whose metaclass hashed
and compared equal to ``int`` each reached the assembled event as the object itself, and
``json.dumps`` then raised ``TypeError`` in a sink. ``int.__int__`` returns an exact ``int`` of the
stored magnitude for a genuine subclass, whatever its own ``__int__``, ``__str__`` or ``__repr__``
say, and raises ``TypeError`` for an object that only claims to be an ``int``, which
:meth:`_Coercer.value` turns into the type-name placeholder.
"""
_FLOAT_FLOAT = float.__float__
"""``float.__float__``, unbound, for every route into the float rule (SPEC-063).

The reason :data:`_INT_INT` gives. A ``float`` subclass's own ``__float__`` decided ``isfinite()``
in :meth:`_Coercer.real` and the subclass instance itself reached the event; the unbound accessor
returns an exact ``float`` of the stored value, measured at ``71e42dd`` as ``1.5`` for a subclass
whose ``__float__`` returned ``9.0``, and refuses an impostor with ``TypeError``. On an exact
``float`` it returns the same object, so the routes that apply it unconditionally pay one C call.
"""
_STORED_NAME = type.__dict__["__name__"].__get__
"""The getter of ``type.__name__``, which reads the name a type stores past its metaclass.

``type(value).__name__`` is resolved through the metaclass, so a metaclass that defines
``__name__`` as a property chose the placeholder's text or raised (SPEC-063 FR-002). This reads the
stored name whatever the metaclass says. Its result is still passed through :data:`_STR_STR`,
because ``type.__name__``'s setter accepts a ``str`` subclass.
"""

_CONTAINER_KEYS: tuple[type, ...] = (tuple, list, dict, set, frozenset)
"""The builtin containers a mapping key is coerced as, tested on the key's real type (SPEC-063)."""

_REAL_ROUTED: tuple[type, ...] = (
    str,
    bytes,
    bytearray,
    dict,
    tuple,
    list,
    set,
    frozenset,
    int,
    float,
)
"""The builtins :meth:`_Coercer._dispatch` routes by real type, so a claim of ``Enum`` cannot.

An object whose real type subclasses one of these is routed by that type, never by an ``Enum``
claim made through ``__class__``, unless its real type is an ``Enum`` as well: the ``Enum``
branch's fallback is ``str(value)``, the builtin ``repr`` for a real container (SPEC-063 FR-002).
``int`` and ``float`` are here too, because their own branches bound and convert them, where the
``Enum`` fallback would render a real ``int`` subclass by ``str()`` whatever its digit count.
"""

_ENUM_TEXT_BUILTINS: tuple[type, ...] = (tuple, list, dict, set, frozenset, bytes, bytearray)
"""The builtins whose ``Enum`` member's text the library computes rather than ``str()`` (SPEC-063).

An ``Enum`` member whose real type subclasses one of these, as a value that reaches the ``Enum``
fallback or as a key, is written as :func:`_enum_text`, never through ``str()``. ``str()`` of such
a member is the builtin container's or bytes' ``repr`` whenever the member's ``__str__`` is not
``Enum.__str__``: a ``ReprEnum`` mixin restores the builtin's own ``__str__``, and a metaclass
``mro()`` can put ``Enum`` into a real ``tuple`` subclass's MRO without inheriting from it.
Measured at ``fda134a``: an mro-forged ``tuple`` around an object whose ``__repr__`` named a secret
wrote it, value and key, and one around the shared ``frozenset`` DAG did not return on any route;
a ``(tuple, ReprEnum)`` member holding the DAG did not return either, and a 16 MiB
``(bytes, ReprEnum)`` member peaked at 64 MiB. ``str``, ``int`` and ``float`` mixins are not here:
their members keep their routes.
"""

_BYTES_LEN = bytes.__len__
"""The unbound lengths that price a bytes-like key before it is rendered (SPEC-063 FR-001).

A subclass's own ``__len__`` cannot divert them, on :data:`_STR_LEN`'s reasoning.
``_BYTEARRAY_LEN`` beside it is the same for ``bytearray``.
"""
_BYTEARRAY_LEN = bytearray.__len__

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


def int_placeholder(value: int) -> str:
    """Renders the ``<int: ~N digits>`` placeholder for an integer, never converting it.

    The expression :func:`_int_text` uses for its placeholder, written once so a sink that must
    replace an integer its destination refuses — ``MongoDBSink``, for one outside BSON's signed
    64-bit range (SPEC-069 FR-003) — writes exactly the text assembly writes. ``N`` comes from
    ``bit_length()`` through :data:`_INT_BIT_LENGTH`, so it is never the value's own digits and
    a subclass cannot choose it, and it can overcount by one. The other copies keep the
    expression inline — ``_Coercer``'s two, because they run once per integer, and
    ``_otel._int_digits``, a leaf that imports nothing from the package — and a test holds all
    of them equal to this one.

    Args:
      value: The integer to name, possibly a subclass instance.

    Returns:
      The placeholder naming the estimated digit count.

    Raises:
      None.
    """
    digits = _INT_BIT_LENGTH(value) * _LOG10_2_NUM // _LOG10_2_DEN + 1
    return f"<int: ~{digits} digits>"


def _int_text(value: int) -> str:
    """Returns an integer's digits for a refusal message, or ``<int: ~N digits>`` when too long.

    A validator that names the value it refuses used to write ``{value!r}``, and CPython refuses
    to convert an integer past ``sys.get_int_max_str_digits()`` digits: measured at ``7dc881f``,
    ``SocketTransport("h", 10**5000)`` raised ``ValueError: Exceeds the limit (4300 digits) for
    integer string conversion`` out of the port check's own message, naming neither ``port`` nor
    the class, and ``require_positive`` did the same for ``-10**5000`` (SPEC-066). The placeholder
    is this module's convention for an integer too large to render (SPEC-020): replaced, never
    clipped, and sized by ``bit_length()`` through :data:`_INT_BIT_LENGTH`, never by converting
    the value, which would raise the very error being avoided. The digit estimate is the one
    :meth:`_Coercer._bounded_int` uses and overcounts by at most one, so a value at the edge is
    replaced rather than rendered, and a negative counts its sign as one more character, as it
    does there and in ``_otel._int_digits``, so under the default limit and ``max_value_bytes``
    the three replace the same integers.

    The ceiling is the interpreter's limit, and never more than its default
    (``sys.int_info.default_max_str_digits``): with the limit lifted, or set to ``0`` for none, a
    message would otherwise render millions of digits, which takes quadratic time and makes a
    refusal slow to raise. The digits are read through ``int.__repr__``, unbound, so an ``int``
    subclass cannot choose the text a refusal prints.

    Args:
      value: The integer to name, possibly a subclass instance.

    Returns:
      The integer's decimal digits, sign included, or the placeholder naming the digit count.

    Raises:
      None.
    """
    digits = _INT_BIT_LENGTH(value) * _LOG10_2_NUM // _LOG10_2_DEN + 1
    rendered = digits + 1 if _INT_LT(value, 0) else digits
    default = sys.int_info.default_max_str_digits
    limit = sys.get_int_max_str_digits()
    ceiling = default if limit <= 0 else min(limit, default)
    if rendered > ceiling:
        return int_placeholder(value)
    return int.__repr__(value)


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

    The work is bounded by the ceiling, not by the string (SPEC-057 FR-003): a string of more
    than ``max_bytes`` characters cannot fit, so only its first ``max_bytes + 1`` are read, through
    :data:`_STR_LEN` and :data:`_STR_SLICE`. One more than the ceiling, because the clip fires when
    the encoding *exceeds* ``max_bytes``: a prefix of exactly ``max_bytes`` ASCII characters encodes
    to exactly ``max_bytes`` bytes and would be returned whole where the whole string is clipped.
    The result is byte-identical to reading the whole string. The prefix's UTF-8 is a prefix of the
    whole string's, a lone surrogate is replaced one character at a time wherever it sits, and the
    clip keeps at most ``max_bytes`` minus the marker's 14 bytes, all of it inside the prefix. A
    string of ``max_bytes`` characters or fewer is read whole, as before.

    Args:
      value: The string to clip.
      max_bytes: The ceiling, in UTF-8 bytes.

    Returns:
      The clipped string and whether it was altered — clipped, or a surrogate replaced.

    Raises:
      TypeError: When ``value`` is not a ``str``, from the first unbound read, as it was raised at
        :data:`_STR_STR` before the read was bounded.
    """
    clipped, altered, _ = _clip(value, max_bytes)
    return clipped, altered


def _clip(value: str, max_bytes: int) -> tuple[str, bool, int]:
    """Returns :func:`truncate_str`'s answer and the UTF-8 length of the string it returns.

    The body of :func:`truncate_str`, whose docstring carries the reasoning, with the size the
    coercer charges against the pass's byte budget (SPEC-057 FR-002). The size comes from bytes
    this already holds: the whole encoding when nothing is clipped, the marker's 14 bytes when
    the marker stands alone, and otherwise the kept head re-encoded, which is bounded by the
    ceiling. The common unclipped case pays no second encode.

    Args:
      value: The string to clip.
      max_bytes: The ceiling, in UTF-8 bytes.

    Returns:
      The clipped string, whether it was altered, and its length in UTF-8 bytes.

    Raises:
      TypeError: When ``value`` is not a ``str``, as :func:`truncate_str` documents.
    """
    if _STR_LEN(value) > max_bytes:
        value = _STR_SLICE(value, slice(0, max_bytes + 1))
    plain, raw, replaced = _measured(value)
    size = len(raw)
    if size <= max_bytes:
        return plain, replaced, size
    budget = max_bytes - _MARKER_BYTES
    if budget <= 0:
        return TRUNCATION_MARKER, True, _MARKER_BYTES
    head = raw[:budget].decode("utf-8", errors="ignore")
    return head + TRUNCATION_MARKER, True, len(head.encode("utf-8")) + _MARKER_BYTES


def truncate_tail(value: str, max_bytes: int) -> tuple[str, bool]:
    """Clips a string to a ceiling of UTF-8 bytes, keeping the tail.

    This is for ``error.stack``: ``traceback.format_exception`` puts the exception type, its
    message and the innermost frames last, so the head of an over-long traceback is the least
    useful part of it. The zero-budget guard is not merely defensive — ``raw[-budget:]`` would
    return the whole string at a budget of zero, and ``max_stack_bytes`` may legally be smaller
    than the marker. The result is an exact, strictly encodable ``str`` on the same terms as
    :func:`truncate_str`.

    The read is bounded by the ceiling on :func:`truncate_str`'s terms, mirrored (SPEC-057
    FR-003): a string of more than ``max_bytes`` characters is read as its last ``max_bytes + 1``.
    The suffix's UTF-8 is a suffix of the whole string's, and the clip keeps at most ``max_bytes``
    minus the marker's 14 bytes from the end, all of it inside the suffix, so the result is
    byte-identical to reading the whole string.

    Args:
      value: The string to clip.
      max_bytes: The ceiling, in UTF-8 bytes.

    Returns:
      The clipped string and whether it was altered — clipped, or a surrogate replaced.

    Raises:
      TypeError: When ``value`` is not a ``str``, from the first unbound read, as
        :func:`truncate_str` documents.
    """
    length = _STR_LEN(value)
    if length > max_bytes:
        value = _STR_SLICE(value, slice(length - max_bytes - 1, length))
    plain, raw, replaced = _measured(value)
    if len(raw) <= max_bytes:
        return plain, replaced
    budget = max_bytes - _MARKER_BYTES
    if budget <= 0:
        return TRUNCATION_MARKER, True
    return TRUNCATION_MARKER + raw[-budget:].decode("utf-8", errors="ignore"), True


class _Coercer:
    """One coercion pass: the ceilings, the two budgets, the truncation flag, the ancestor chain.

    This is a pass object rather than a pure function because the ceilings fire deep in the
    recursion — a 300-key mapping eight levels down still has to set ``truncated`` on the
    top-level event — and a recursive function returning a bare value has nowhere to report
    that. One instance per pass, ``__slots__``-ed; against the timestamp, UUID and two dicts
    ``build_event`` already allocates per event, it is noise. ``build_event`` makes one pass and
    ``backfill_baggage`` a second, so a boundary event can hold up to twice each budget.

    The two budgets (SPEC-057) bound the pass as a whole, where the ceilings bound one value
    each: at most :data:`_MAX_NODES` values visited and :data:`_MAX_RENDERED_BYTES` of text
    charged. Their counters live on this object, not on the module, so nothing is stranded
    across a fork (invariant 12). Only :func:`_coerce_unbounded` asks for an unbudgeted pass.

    Attributes:
      truncated: Whether any ceiling or budget fired, or any substitution was made, in the pass.
    """

    __slots__ = (
        "_cfg",
        "_charged",
        "_int_ceiling",
        "_max_bytes",
        "_max_nodes",
        "_nodes",
        "_parents",
        "_stopped",
        "truncated",
    )

    def __init__(self, cfg: Config, *, budgeted: bool = True) -> None:
        """Starts a pass with the configured ceilings, empty counters and an empty ancestor chain.

        ``_parents`` holds the ``id()``s of the containers currently being descended through.
        Ancestors only, never a global "visited" set: an ancestor is held alive by the
        recursion for the whole descent, so its ``id()`` cannot be recycled underneath us, and
        two siblings referencing the same object are not a cycle and must both render.

        The integer ceiling is resolved here, once per pass rather than once per integer
        (SPEC-031 FR-004): it reads ``sys.get_int_max_str_digits()``, which cannot change
        during a coercion pass, and the alternative sat four lines below an ``int.__lt__``
        binding justified by this being a per-value hot path. The two budgets are read here
        too, from the module, so a patched value applies from the next pass on. An unbudgeted
        pass lifts both, because a budget's cut would clip what a transform's coercion promises
        to keep; only :func:`_coerce_unbounded` asks for one.

        Args:
          cfg: The config supplying ``max_value_bytes``, ``max_keys`` and ``max_depth``.
          budgeted: Whether the pass stops at the two budgets.

        Returns:
          None.

        Raises:
          None.
        """
        self._cfg = cfg
        self._int_ceiling = _int_digit_ceiling(cfg.max_value_bytes)
        self.truncated = False
        self._parents: list[int] = []
        self._max_nodes = _MAX_NODES if budgeted else sys.maxsize
        self._max_bytes = _MAX_RENDERED_BYTES if budgeted else sys.maxsize
        self._nodes = 0
        self._charged = 0
        self._stopped: str | None = None

    def _halt(self, placeholder: str) -> None:
        """Stops the pass at a budget, marking the event.

        Every container still open ends after the member it was writing, by reading
        ``_stopped`` after each member, so nothing later in walk order is visited.

        Args:
          placeholder: The placeholder of the budget that was spent.

        Returns:
          None.

        Raises:
          None.
        """
        self._stopped = placeholder
        self.truncated = True

    def value(self, value: object, depth: int) -> object:
        """Coerces one node of the structure, first spending one of the pass's node budget.

        Every value counts, scalar or container, including one that becomes ``<depth limit>``
        or ``<circular>`` (SPEC-057 FR-001). The order of checks is the node budget, then
        ``max_depth`` (at the top of :meth:`_dispatch`), then the type's own rule, whose text or
        integer charge is the byte check: a value past the budget is ``<node limit>`` even where
        it also sits past ``max_depth``. The value past the budget is not dispatched, so a
        container there is replaced unread, and the pass stops rather than writing a placeholder
        per remaining value, which is what bounds it: the shape of ``<depth limit>`` would still
        write up to (``max_depth`` + 1) times ``max_keys`` of them.

        A mapping key is not a node visited here, with one exception: a builtin-container key
        spends one node for itself in :meth:`_container_key` and one per member per path through
        this method, as a value at the entry's value depth would (SPEC-063 FR-001), so a key and
        its members are counted where a ``str`` or scalar key is not.

        Args:
          value: The node to coerce.
          depth: How deep the node sits, measured against ``max_depth``.

        Returns:
          A JSON-serializable, bounded equivalent, or a placeholder naming the type or the budget.

        Raises:
          None. A hostile ``__iter__``, ``__str__`` or ``__eq__`` must not reach the caller's
            stack, which is the whole point of SPEC-017 FR-001.
        """
        if self._nodes >= self._max_nodes:
            self._halt(_NODE_LIMIT)
            return _NODE_LIMIT
        self._nodes += 1
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
        natural ceiling, and ``Decimal`` renders as a string so that ``Decimal("0.1")`` does not
        become ``0.1000000000000000055``. Text the caller supplied goes to :meth:`text`, and text
        this pass makes with ``str()`` or ``isoformat()`` to :meth:`produced`, because the two are
        charged differently against the byte budget (SPEC-057 FR-002).

        **A route that returns the caller's object, or lets its method decide a measurement,
        admits it by exact type or through an unbound accessor** (SPEC-063 FR-002). The exact
        block is identity tests; ``bool`` is one of them, because ``bool`` cannot be subclassed,
        so the ``isinstance(value, bool)`` branch that followed the ``Enum`` one admitted only an
        object claiming ``bool`` through ``__class__``, and returned it. An ``int`` or ``float``
        subclass is converted by :data:`_INT_INT` or :data:`_FLOAT_FLOAT` before it is bounded,
        so the event holds an exact number and an impostor raises into :meth:`value`'s guard. An
        ``Enum`` member's value is admitted the same way, and a member whose value claims a type
        it is not falls to the enum's own text, unless it claims ``float`` or ``str``, whose
        conversion or unbound read raises and names the ``Enum`` class, the node.

        **The ``Enum`` branch is entered only by a real ``Enum``, or by an object whose real type
        is none of the builtins in** :data:`_REAL_ROUTED`. Its fallback is ``str(value)``, so a
        real container claiming ``Enum`` through ``__class__`` reached the builtin ``repr``:
        measured at ``71e42dd``, and unchanged until this test, a real ``tuple`` subclass
        claiming ``Enum`` wrote ``(SECRET,)`` for an object whose ``__repr__`` returned
        ``"SECRET"``, did not return around the shared ``frozenset`` DAG, and a ``bytes`` subclass
        of 16 MiB peaked at 64 MiB. A proxy of an ``Enum`` keeps the branch, its real type being
        no builtin, and so does every ``StrEnum``, ``IntEnum`` and ``(tuple, Enum)`` member.

        **Then the real type routes the containers and the text.** A ``str``, ``bytes`` or
        ``bytearray`` subclass, the exact ``memoryview``, a ``dict`` subclass and a ``tuple``,
        ``list``, ``set`` or ``frozenset`` subclass are routed by ``issubclass(type(value), …)``,
        ahead of every render-through ``isinstance`` branch and after the ``int`` and ``float``
        branches, which convert or refuse what they admit. A real container that claims ``UUID``
        or ``Decimal`` through ``__class__`` otherwise reached their ``str()``, the builtin
        ``repr``: measured at ``71e42dd``, a real ``tuple`` subclass claiming ``UUID`` around a
        shared ``frozenset`` DAG did not return, one holding an object whose ``__repr__``
        returned ``"SECRET"`` wrote ``SECRET``, and a real ``bytes`` subclass claiming
        ``Decimal`` was rendered whole by ``str()``. **``isinstance`` stays, on purpose, for the
        render-through routes** — ``isoformat()``, ``str()`` of a ``UUID`` or ``Decimal``,
        ``Mapping``, ``set`` and ``Sequence`` iteration, and the ``Enum`` branch for an object
        that is no builtin — whose output is the library's own text and containers: a
        transparent proxy
        (``wrapt.ObjectProxy``, Django's ``SimpleLazyObject``) sets ``__class__`` legitimately and
        renders correctly through them. An object that only claims ``str`` or a bytes-like type
        falls through to the placeholder, the ``Sequence`` branch excluding the text-like types.

        Args:
          value: The node to route.
          depth: How deep the node sits, measured against ``max_depth``.

        Returns:
          The coerced node.

        Raises:
          Exception: Whatever the value's own dunders raise, or the ``TypeError`` an unbound
            accessor raises for an impostor; :meth:`value` is the guard.
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
        if kind is bool:
            return value
        if kind is dict:
            return self.mapping(value, depth)  # type: ignore[arg-type]
        if kind is list:
            return self.members(value, depth)  # type: ignore[arg-type]

        if isinstance(value, Enum) and (
            issubclass(kind, Enum) or not issubclass(kind, _REAL_ROUTED)
        ):
            member = value.value
            if type(member) is int:
                return self.integer(member)
            if isinstance(member, float):
                return self.real(_FLOAT_FLOAT(member))
            if type(member) is bool or member is None:
                return member
            if isinstance(member, str):
                return self.text(member)
            if issubclass(kind, _ENUM_TEXT_BUILTINS):
                return self.produced(_enum_text(value))
            return self.produced(str(value))
        if isinstance(value, int):
            return self.integer(_INT_INT(value))
        if isinstance(value, float):
            return self.real(_FLOAT_FLOAT(value))
        if issubclass(kind, str):
            return self.text(value)  # type: ignore[arg-type]
        if issubclass(kind, (bytes, bytearray)) or kind is memoryview:
            return self.text(self._decoded(self._head_bytes(value)))  # type: ignore[arg-type]
        if issubclass(kind, dict):
            return self.mapping(value, depth)  # type: ignore[arg-type]
        if issubclass(kind, (tuple, list, set, frozenset)):
            return self.members(value, depth)  # type: ignore[arg-type]
        if isinstance(value, (datetime, date, time)):
            return self.produced(value.isoformat())
        if isinstance(value, UUID):
            return self.produced(str(value))
        if isinstance(value, Decimal):
            return self.produced(str(value))
        if isinstance(value, Mapping):
            return self.mapping(value, depth)
        if isinstance(value, (set, frozenset)):
            return self.members(value, depth)
        if isinstance(value, Sequence) and not isinstance(value, _TEXTLIKE):
            return self.members(value, depth)
        return self._placeholder(value)

    def _head_bytes(self, value: bytes | bytearray | memoryview[int]) -> bytes:
        """Reads the first ``max_value_bytes + 1`` bytes of a bytes-like, never more (SPEC-057).

        Enough to decide the clip: :meth:`text` fires when the encoding *exceeds* the ceiling, and
        a decoded unit is never shorter than its source, so a prefix one byte past the ceiling
        decodes to text that is clipped exactly where the whole value's would be. The decoder
        works left to right and a sequence split by the cut starts no earlier than three bytes
        before it, while the clip keeps at most the ceiling minus the marker's 14 bytes, so
        everything kept was decoded identically. The rendering is byte-identical to decoding the
        whole value, flag included.

        A ``bytes`` or ``bytearray``, exact or a subclass, is read through :data:`_BYTES_SLICE` or
        :data:`_BYTEARRAY_SLICE` and never through its buffer: a subclass can override
        ``__buffer__`` (PEP 688), which makes ``memoryview(value)`` report another length and
        other bytes, while the unbound accessor reads what the object stores. That is a recorded
        exception, hostile-only: a ``bytes`` subclass overriding ``__bytes__``, or a ``bytearray``
        subclass overriding ``__bytes__`` or ``__buffer__``, was rendered through the override by
        the ``bytes(value)`` this replaced, and is now rendered from the bytes it stores, as
        :data:`_STR_STR` already does for a ``str`` subclass. An exact short ``bytes`` slices to
        itself, so the common case copies nothing.

        A ``memoryview`` cannot be subclassed, so it is read through its own view: whole when it
        is at or below the ceiling, as before, and otherwise as whole rows up to the cut. A
        C-contiguous view, a zero-dimensional one included, is cast to bytes and sliced. Any other
        is sliced along its first dimension to enough rows to cover the prefix, a row being one
        item of a one-dimensional view; a non-contiguous view is never zero-dimensional, so it has
        a first dimension. That is an accepted limit: a single row is read whole, so a view whose
        one row exceeds the ceiling costs that row.

        Every branch tests the real type before any of the value is read, because ``isinstance``
        consults ``__class__``, which any object may override: one claiming to be a
        ``memoryview`` reached this method, and its own ``nbytes`` and ``__bytes__`` chose how
        much the library read and decoded — measured, a cached 16 MiB ``__bytes__`` behind an
        ``nbytes`` of zero was decoded whole on every visit, the read FR-003 bounds. Since
        SPEC-063 :meth:`_dispatch` routes here by the real type, so an impostor no longer
        arrives; the refusal stays so that this method's contract does not rest on its caller.
        A real ``bytearray`` subclass that claims ``bytes`` is read through its own slice rather
        than refused by the ``bytes`` one, which an ``isinstance`` test here would choose.

        Args:
          value: The bytes-like to read.

        Returns:
          An exact ``bytes`` of at least ``max_value_bytes + 1`` bytes, or of the whole value when
          it is shorter.

        Raises:
          TypeError: When ``value`` only claims to be a ``bytes``, ``bytearray`` or
            ``memoryview`` through ``__class__``; :meth:`value` is the guard.
          Exception: Whatever a released or exotic ``memoryview`` raises; :meth:`value` is the
            guard.
        """
        limit = self._cfg.max_value_bytes + 1
        kind = type(value)
        if issubclass(kind, bytes):
            return _BYTES_SLICE(cast("bytes", value), slice(0, limit))
        if issubclass(kind, bytearray):
            return bytes(_BYTEARRAY_SLICE(cast("bytearray", value), slice(0, limit)))
        if kind is not memoryview:
            raise TypeError("not a memoryview")
        view = cast("memoryview[int]", value)
        if view.nbytes < limit:
            return bytes(view)
        if view.c_contiguous:
            return bytes(view.cast("B")[:limit])
        rows = view.shape[0] if view.shape else 1
        row = view.nbytes // rows
        return bytes(view[: -(-limit // row)])

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

        An entry's key is rendered and charged *before* its value is visited (SPEC-057 FR-002),
        so a key that stops the pass leaves its value unread: the entry is written as the
        placeholder of whichever budget stopped it, key and value both — ``"<size limit>":
        "<size limit>"`` or ``"<node limit>": "<node limit>"`` — and the mapping ends. That
        reversed the order of the single assignment it replaced,
        ``out[self.key(key)] = self.value(item, depth + 1)``, because Python evaluates the
        right-hand side of a subscript assignment before the subscript. Before SPEC-063 only a
        key's byte charge could stop the pass between the two; a container key now visits its
        members as values at the entry's value depth, so either budget can stop it there, and
        the placeholder written is the one in ``_stopped`` (SPEC-063 FR-001). After the value, a
        stop from anywhere beneath it ends this mapping too.

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
            child = depth + 1
            for key, item in value.items():
                if len(out) >= self._cfg.max_keys:
                    self.truncated = True
                    break
                name = self.key(key, child)
                if self._stopped is not None:
                    out[self._stopped] = self._stopped
                    break
                out[name] = self.value(item, child)
                if self._stopped is not None:
                    break
            return out
        finally:
            self._parents.pop()

    def members(self, value: Iterable[object], depth: int) -> object:
        """Coerces an iterable's members into a list, capping length at ``max_keys``.

        A member that spends either budget is written as its placeholder and the list ends
        there, so nothing later in walk order is visited (SPEC-057).

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
                if self._stopped is not None:
                    break
            return out
        finally:
            self._parents.pop()

    def key(self, key: object, depth: int) -> str:
        """Coerces a mapping key to a bounded string, and is total (SPEC-055 FR-004).

        Whatever rendering the key raises — its own ``__str__``, or an unbound accessor refusing
        an object that only claims through ``__class__`` to be an ``int`` or a ``str`` — is
        caught here rather than in :meth:`value`, whose guard sits around the *enclosing*
        mapping: measured, ``{"sib": 1, KeyBoom(): 2}`` reached the sink as
        ``<unserializable: dict>`` with ``truncated`` unset, so one hostile key took every
        sibling with it — the exact outcome the integer branch below was written to prevent for
        one kind of key. The key becomes a
        ``<unserializable key: T>`` placeholder naming its type, on :meth:`_placeholder`'s
        no-``repr`` rule, and ``truncated`` is set: a value placeholder is visible on its own,
        but a key that could not be rendered is a substitution the reader must be told about.

        Two hostile keys of one type collide on one placeholder and the later wins, which is
        accepted: the alternative is a placeholder carrying something of the key's identity,
        which is what arch §6 forbids, and the collision is still marked because each replaced
        key sets ``truncated``. The guard is here rather than in :meth:`mapping`'s loop because
        the loop's other two failures — the ``max_keys`` cap and a value that cannot be coerced
        — already have owners, and a third ``try`` around the loop would hide which one fired.

        The placeholder is text this pass produced, so it is charged as :meth:`produced` text
        (SPEC-057 FR-002). A container key's own failure — its iteration raising — reaches this
        guard too, which is why :meth:`_container_key` walks it without :meth:`value`'s guard:
        that guard would turn the failure into the value placeholder ``<unserializable: T>``,
        unmarked, written inside the key's text (SPEC-063 FR-001). The nodes and bytes such a key
        spent before it failed stay spent.

        Args:
          key: The mapping key, of any type.
          depth: The entry's value depth, at which a container key's members are coerced.

        Returns:
          The bounded string form of the key, the key placeholder, or the placeholder of the
          budget the key spent.

        Raises:
          None.
        """
        try:
            return self._key(key, depth)
        except Exception:
            self.truncated = True
            return self.produced(f"<unserializable key: {_type_name(key)}>")

    def _key(self, key: object, depth: int) -> str:
        """Renders one mapping key, since JSON object keys are always strings.

        An integer key is bounded by :meth:`_bounded_int` first. A bare ``str()`` here would
        raise on an over-long one — the very ``ValueError`` this module exists to keep away from
        a sink — and the failure would be caught up in :meth:`key`, replacing the key with a
        placeholder where its digits would have done. ``bool``
        is excluded because ``True`` must render as the key ``"True"``, not ``"1"``. It does not
        go through :meth:`integer`, which charges an integer *value*: a key is charged once, as
        the string it becomes, so an integer key is never charged twice (SPEC-057 FR-002).

        An integer key that will render as its digits is priced before it is rendered, because
        rendering is the work: ``str()`` of an integer costs more than its length, and with the
        interpreter's digit limit lifted a key of seventeen million digits was rendered in full
        and only then became ``<size limit>``. The pass's charge is tested against a lower bound
        of the rendered length, one less than :meth:`_bounded_int`'s estimate, sign included;
        when even that crosses, the key becomes ``<size limit>`` unrendered and the pass halts,
        as it does when a rendered key crosses. Otherwise the key is rendered and charged its
        actual length, as before, so the charge stays exact. It is a true lower bound because the
        estimate overcounts by at most one digit below about 48 million digits — its ratio,
        30103/100000, exceeds ``log10(2)`` by under 5e-9, too little to add a second digit
        before then — and the byte budget admits no key that long. It bounds the integer's own
        digits, while ``str()`` of a subclass is the subclass's ``__str__``: one that renders
        shorter than its digits can be cut where its text would have fitted, which errs toward
        the cut.

        The integer branch admits by ``type(key) is not bool`` rather than
        ``not isinstance(key, bool)``, because ``isinstance`` consults ``__class__``: a real ``int``
        subclass claiming ``bool`` skipped the bound and was rendered by ``str()`` whole —
        measured at ``71e42dd``, three million digits under ``sys.set_int_max_str_digits(0)``.
        An object that only claims ``bool`` or ``int`` reaches :meth:`_bounded_int`, whose
        unbound reads refuse it, and becomes the key placeholder.

        A float key goes through :meth:`real` for the same reason, converted first through
        :data:`_FLOAT_FLOAT`, so a ``float`` subclass key renders as its exact float rather than
        through its own ``__str__``, and an impostor becomes the key placeholder (SPEC-063
        FR-002). A ``(float, Enum)`` member is excluded and keeps ``str()``, as a ``(int, Enum)``
        member does through its own branch: an ``Enum`` key reads as its name, ``FE.A``, as it
        did at ``71e42dd``. Its rendered form is short — ``real`` computes its placeholder through
        :data:`_FLOAT_REPR` — but it is still text this pass produced, so it is charged as such.
        A ``str`` key is the caller's own text and goes to :meth:`text`.

        **A container key is coerced, not rendered** (SPEC-063 FR-001). A key whose *real* type
        subclasses ``tuple``, ``list``, ``dict``, ``set`` or ``frozenset`` and not ``Enum`` is
        handed to :meth:`_container_key`, whose docstring carries the rule, ahead of the
        claim-based ``str``, ``int`` and ``float`` tests, so a real container claiming one of
        those is still walked rather than refused by that type's unbound read; a real ``str``
        subclass, a ``StrEnum`` included, is no container and reaches :meth:`text`. The real type,
        because
        an object that only claims through ``__class__`` to be a ``tuple`` is not walked and keeps
        its own ``str()``, and not an ``Enum``, because a ``(tuple, Enum)`` member keeps
        ``str()`` as ``Color(int, Enum)`` does, the value route sending an ``Enum`` to its own
        branch ahead of the sequence one. A ``bytes`` or ``bytearray`` key is priced by
        :meth:`_bytes_key_fits` before it is rendered. Every other key's ``str()`` is produced
        text and goes to :meth:`produced`, an object key's default ``str()`` being its ``repr``,
        the caller's code (SPEC-063, Accepted Limits).

        Args:
          key: The mapping key, of any type.
          depth: The entry's value depth, at which a container key's members are coerced.

        Returns:
          The bounded string form of the key, or the placeholder of the budget it spent.

        Raises:
          Exception: Whatever the key's own ``__str__`` or iteration raises, or the
            ``TypeError`` an unbound accessor raises for an impostor; :meth:`key` is the guard.
        """
        kind = type(key)
        if issubclass(kind, _CONTAINER_KEYS) and not issubclass(kind, Enum):
            return self._container_key(key, kind, depth)
        if issubclass(kind, Enum) and issubclass(kind, _ENUM_TEXT_BUILTINS):
            return self.produced(_enum_text(key))
        if isinstance(key, str):
            return self.text(key)
        if isinstance(key, int) and type(key) is not bool:
            rendered, length = self._bounded_int(key)
            if length is not None and self._charged + length - 1 > self._max_bytes:
                self._halt(_SIZE_LIMIT)
                return _SIZE_LIMIT
            return self.produced(rendered if isinstance(rendered, str) else str(rendered))
        if isinstance(key, float) and not issubclass(kind, Enum):
            replaced = self.real(_FLOAT_FLOAT(key))
            return self.produced(replaced if isinstance(replaced, str) else str(replaced))
        if issubclass(kind, (bytes, bytearray)) and not self._bytes_key_fits(key, kind):
            self._halt(_SIZE_LIMIT)
            return _SIZE_LIMIT
        return self.produced(str(key))

    def _container_key(self, key: object, kind: type, depth: int) -> str:
        """Coerces a builtin-container key as a value and returns the JSON text of the result.

        Before SPEC-063 such a key was rendered by ``str(key)``, the builtin ``repr``, before any
        budget applied: it recurses through a shared member once per path, so
        ``f = frozenset({f, (f, i)})`` applied forty times as one key never returned, a tuple of
        64 references to one 1 MiB string rendered 64 MiB before the byte budget turned it into
        ``<size limit>``, and each member's ``repr`` was written into the key, against arch §6's
        no-``repr`` rule. Coerced instead, the key spends one node for itself and the pass's
        nodes and bytes for its members exactly as a value at the entry's value depth would, and
        obeys ``max_depth``, ``max_keys`` and the cycle check, so a member past the depth is
        ``<depth limit>`` inside the text and one that cannot be coerced is its
        ``<unserializable: T>`` placeholder there.

        The members are walked by :meth:`mapping` for a real ``dict`` subclass and by
        :meth:`members` otherwise, never through :meth:`_dispatch` or :meth:`value`. Not
        ``_dispatch``, because its render-through ``isinstance`` branches would hand a real
        ``tuple`` subclass claiming ``UUID`` or ``Decimal`` to ``str()`` — the builtin ``repr``
        again, which hung on the DAG and wrote a member's ``repr`` when measured at ``71e42dd``
        through the value route — and its ``int`` and ``float`` branches would refuse a real
        container claiming either. Not ``value``, because its guard would turn the key's own
        failure into a value placeholder where :meth:`key` must see it.

        The text is ``json.dumps`` of the coerced form with the default separators, so ``(1, 2)``
        is the key ``"[1, 2]"`` and a key whose coerced form is a string, ``<depth limit>`` or
        ``<circular>``, reads as that string in quotes and cannot collide with a ``str`` key of
        the same text. It is clipped to ``max_value_bytes`` and marks the event when clipped, but
        it is not charged: its members were charged as values are, and the punctuation and
        escapes are bounded by the node count and six times the charged text, like the
        serialised event (SPEC-057). ``json.dumps`` cannot fail on a coerced tree, whose leaves
        are exact scalars, finite floats and bounded integers under ``str`` keys. When the walk
        stops the pass, the stop placeholder is returned unrendered and :meth:`mapping` writes it
        for the entry's key and value both.

        Args:
          key: The container key.
          kind: The key's real type, already tested.
          depth: The entry's value depth, measured against ``max_depth``.

        Returns:
          The key's text, or the placeholder of the budget its walk spent.

        Raises:
          Exception: Whatever the key's own iteration raises; :meth:`key` is the guard.
        """
        if self._nodes >= self._max_nodes:
            self._halt(_NODE_LIMIT)
            return _NODE_LIMIT
        self._nodes += 1
        coerced: object
        if depth >= self._cfg.max_depth:
            self.truncated = True
            coerced = _DEPTH_LIMIT
        elif issubclass(kind, dict):
            coerced = self.mapping(cast("Mapping[Any, object]", key), depth)
        else:
            coerced = self.members(cast("Iterable[object]", key), depth)
        if self._stopped is not None:
            return self._stopped
        clipped, altered, _ = _clip(
            json.dumps(coerced, ensure_ascii=False), self._cfg.max_value_bytes
        )
        if altered:
            self.truncated = True
        return clipped

    def _bytes_key_fits(self, key: object, kind: type) -> bool:
        """Returns whether a bytes-like key's rendered form can fit the pass's byte budget.

        A ``bytes`` or ``bytearray`` key is rendered by ``str(key)``, its repr, up to four
        characters per byte, so before SPEC-063 it cost its repr before the budget saw it:
        measured at ``71e42dd``, a 64 MiB ``b"\\x00"`` key was rendered to 256 MiB (a
        ``tracemalloc`` peak) and only then became ``<size limit>``. It is priced first at the
        repr's upper bound, four characters per byte plus the ``b''`` around them, from its
        length read through :data:`_BYTES_LEN` or :data:`_BYTEARRAY_LEN`, which the caller's
        ``__len__`` cannot divert. When the pass's charge plus that bound would cross the budget,
        the key is refused unrendered, as an integer key is (SPEC-057 FR-002); otherwise it is
        rendered and charged its actual length by :meth:`produced`. The bound errs toward the
        cut, refusing a short key whose repr would have fitted, and a ``bytearray``'s repr adds
        its type name to it, which the actual charge then counts; what it guarantees is that a
        refused key is never rendered and a rendered one costs at most the bound plus a type
        name.

        Args:
          key: The bytes-like key.
          kind: The key's real type, a ``bytes`` or ``bytearray`` subclass.

        Returns:
          ``True`` when the key may be rendered.

        Raises:
          None.
        """
        if issubclass(kind, bytes):
            length = _BYTES_LEN(cast("bytes", key))
        else:
            length = _BYTEARRAY_LEN(cast("bytearray", key))
        return self._charged + 4 * length + 3 <= self._max_bytes

    def _bounded_int(self, value: int) -> tuple[object, int | None]:
        """Returns an integer unchanged with its rendered length, or a placeholder when too long.

        This is the bound for an integer *key*. :meth:`integer` carries the same bound inline for
        an integer value, the hot path, and a test holds the two to one answer on both sides of
        every edge.

        The size test is ``bit_length()``, never ``len(str(value))``: converting an over-long
        integer to a string raises the very ``ValueError`` this bound exists to prevent, so the
        obvious check would move the crash rather than remove it (SPEC-020 FR-002). It ignores
        the sign, so the minus sign is added back explicitly, read through an unbound
        ``int.__lt__`` that an ``int`` subclass cannot divert — ``value < 0`` dispatches to user
        code, and a raise there would be caught up in :meth:`value` and take every sibling key
        with it.

        An over-long integer is replaced, not clipped, because dropping digits would silently
        change the value and a wrong number is worse than a visibly elided one. The magnitude
        is read through :data:`_INT_BIT_LENGTH`, never the value's own ``bit_length``: a
        subclass's override once chose the size the bound tested, so one understating its
        magnitude passed the ceiling and one returning ``nan`` was named a ``nan``-digit
        placeholder, and the rendered length returned here is now also the lower bound that
        decides whether :meth:`_key` renders the key at all (SPEC-057).

        Args:
          value: The integer to bound.

        Returns:
          The integer itself with its estimated rendered length, digits plus one for a sign; or
          a ``<int: ~N digits>`` placeholder naming the digit count, with ``None``.

        Raises:
          None.
        """
        digits = _INT_BIT_LENGTH(value) * _LOG10_2_NUM // _LOG10_2_DEN + 1
        rendered = digits + 1 if _INT_LT(value, 0) else digits
        if rendered <= self._int_ceiling:
            return value, rendered
        self.truncated = True
        return f"<int: ~{digits} digits>", None

    def integer(self, value: int) -> object:
        """Bounds an integer written as a value, and charges its rendered length to the pass.

        The bound is :meth:`_bounded_int`'s, whose docstring carries the reasoning, and the charge
        is :meth:`text`'s; both are written out here rather than called, because this runs once
        per integer in the event and two calls and a tuple were most of what the byte budget
        cost it. A rendered integer is charged the length it estimates, digits plus one
        for a sign (SPEC-057 FR-002); the estimate from ``bit_length()`` can overcount by one
        digit, which errs toward the cut. A placeholder is charged nothing, like every fixed
        placeholder, since the node budget bounds those.

        The charge is computed from the magnitude, so the magnitude is read through
        :data:`_INT_BIT_LENGTH`, never the value's own ``bit_length``: a subclass's override
        returning ``nan`` made the running charge ``nan`` and switched the byte budget off for
        the rest of the pass, and one returning a negative number credited it.

        Args:
          value: The integer to bound.

        Returns:
          The integer itself, a ``<int: ~N digits>`` placeholder naming the digit count, or
          ``<size limit>`` when its charge spent the byte budget.

        Raises:
          None.
        """
        digits = _INT_BIT_LENGTH(value) * _LOG10_2_NUM // _LOG10_2_DEN + 1
        rendered = digits + 1 if _INT_LT(value, 0) else digits
        if rendered > self._int_ceiling:
            self.truncated = True
            return f"<int: ~{digits} digits>"
        charged = self._charged + rendered
        if charged > self._max_bytes:
            self._halt(_SIZE_LIMIT)
            return _SIZE_LIMIT
        self._charged = charged
        return value

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
        """Applies ``max_value_bytes`` to the caller's text, and charges what is written.

        Text from the caller — a ``str`` value or key, decoded bytes, an enum member's ``str``
        value — already exists, and :func:`truncate_str` reads only its ceiling, so it is charged
        at its UTF-8 length *after* clipping (SPEC-057 FR-002). Charging its full length would
        turn a clipped 20 MB string field into ``<size limit>``, a regression for a legitimate
        input.

        The charge is the byte check, and :meth:`integer` and :meth:`produced` repeat it. A
        charge that lands exactly on the budget fits, so the comparison is strict; one that does
        not fit adds nothing, stops the pass, and its value is written as ``<size limit>`` whole,
        never clipped to fit. It is written out in each of the three rather than called, because
        they run once per string and per integer in the event and a method call there was a
        large part of what the budget cost the hot path; each copy has its own exactness test,
        one byte either side of the budget.

        Args:
          value: The string to bound.

        Returns:
          The bounded string, or ``<size limit>`` when its charge spent the byte budget.

        Raises:
          None.
        """
        clipped, altered, size = _clip(value, self._cfg.max_value_bytes)
        charged = self._charged + size
        if charged > self._max_bytes:
            self._halt(_SIZE_LIMIT)
            return _SIZE_LIMIT
        self._charged = charged
        if altered:
            self.truncated = True
        return clipped

    def produced(self, value: str) -> str:
        """Applies ``max_value_bytes`` to text this pass made, and charges the making of it.

        Text the pass produces by calling ``str()`` or ``isoformat()`` — a ``Decimal``, ``UUID``,
        date or time, an enum's fallback, a key that is neither a ``str`` nor a builtin
        container (whose members are charged as values instead, SPEC-063), a type-name
        placeholder —
        did not exist until the pass made it, so its length *is* the work: one ``Decimal`` of ten
        million digits referenced 256 times is 2,560,000,000 characters of ``str()``, which
        clipping afterwards does not undo. It is charged the larger of its length in characters
        before clipping and its UTF-8 bytes after (SPEC-057 FR-002), on :meth:`text`'s terms.
        The length is read through :data:`_STR_LEN`, because ``str()`` of an object can return a
        ``str`` subclass.

        Args:
          value: The produced string to bound.

        Returns:
          The bounded string, or ``<size limit>`` when its charge spent the byte budget.

        Raises:
          None.
        """
        chars = _STR_LEN(value)
        clipped, altered, size = _clip(value, self._cfg.max_value_bytes)
        charged = self._charged + max(chars, size)
        if charged > self._max_bytes:
            self._halt(_SIZE_LIMIT)
            return _SIZE_LIMIT
        self._charged = charged
        if altered:
            self.truncated = True
        return clipped

    def _placeholder(self, value: object) -> str:
        """Names the type that could not be coerced, without disclosing the value.

        A type name rather than ``repr(value)`` on purpose: arch §6 refuses to auto-capture
        argument and return values so the library cannot leak secrets or PII, and ``repr()`` of
        an arbitrary object routinely prints attribute values — a credential held on a client
        object would land in the log. The placeholder goes through :meth:`produced` like any
        other string the pass makes (SPEC-055 FR-004, SPEC-057 FR-002): ``type.__name__`` is
        writable, so a type named with more than ``max_value_bytes`` would otherwise be the one
        string in the event no ceiling reached, and the one no budget charged.

        Args:
          value: The value that could not be coerced.

        Returns:
          A ``<unserializable: type>`` placeholder, or ``<size limit>`` when its charge spent
          the byte budget.

        Raises:
          None. A pathological metaclass is still not this module's problem.
        """
        return self.produced(f"<unserializable: {_type_name(value)}>")


def _enum_text(member: object) -> str:
    """Returns ``Enum.__str__``'s default text for a member, computed without calling ``str()``.

    The stored type name, through :func:`_type_name`, then ``.``, then the member's ``_name_``
    through :data:`_STR_STR`, which returns an exact ``str`` for a ``str`` subclass and raises
    ``TypeError`` for anything else. For an ordinary ``(tuple, Enum)`` member that is the text
    ``str()`` gave, ``TE.A``; for a ``ReprEnum`` or an mro-forged member, whose ``str()`` is the
    builtin's ``repr``, it is the same text where the ``repr`` wrote its members and recursed
    through a shared one once per path (:data:`_ENUM_TEXT_BUILTINS`). The ``_name_`` read is the
    caller's attribute, which may raise; the coercer's guard then writes the type-name placeholder,
    as for any value or key whose rendering raised.

    Args:
      member: The ``Enum`` member, whose real type subclasses a builtin container or bytes.

    Returns:
      The member's ``Type.NAME`` text.

    Raises:
      TypeError: When the member's ``_name_`` is not a ``str``; the coercer is the guard.
      Exception: Whatever reading ``_name_`` raises; the coercer is the guard.
    """
    return f"{_type_name(member)}.{_STR_STR(cast('Enum', member)._name_)}"


def _type_name(value: object) -> str:
    """Returns a value's type name, or ``"?"`` when even that cannot be read.

    Shared by both placeholders so the no-``repr`` rule has one implementation, and by
    ``model``'s ``error`` sub-document and stack fallback. The name is the one the type stores,
    read through :data:`_STORED_NAME` and then :data:`_STR_STR` (SPEC-063 FR-002). Before it,
    ``type(value).__name__`` was resolved through the metaclass: measured at ``71e42dd``, a
    metaclass whose ``__name__`` property raised made every placeholder ``?``, one returning
    ``42`` or ``"renamed"`` named the type that, and in ``model`` the first two cost a span its
    events. The ``str.__str__`` is needed because ``type.__name__``'s setter accepts a ``str``
    subclass, which the descriptor then returns: measured on ``71e42dd``'s code with the read
    alone in place, a subclass whose ``__format__`` returned ``"HIJACK"`` renamed the placeholder
    through the f-string that embeds it, and one whose ``__str__`` raised took every sibling in
    ``fields`` with it. The fallback ``"?"`` stays as a guard.

    Args:
      value: The value whose type is named.

    Returns:
      The type's stored name as an exact ``str``, or ``"?"``.

    Raises:
      None.
    """
    try:
        return _STR_STR(_STORED_NAME(type(value)))
    except Exception:
        return "?"


def coerce(value: object, *, cfg: Config) -> object:
    """Returns a JSON-serializable, size-bounded equivalent of a value.

    The value is treated as a field value, at depth 0 — the same level it would occupy inside
    an event's ``fields``. Use :func:`sanitize_fields` for a whole event mapping; it reports
    whether a ceiling fired, which this cannot. The call is one pass, with its own node and byte
    budgets (SPEC-057).

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
    rather than a pure recursive function. The call is one pass, with its own node and byte
    budgets (SPEC-057). The top-level mapping is not a value and spends no node; a key that is a
    builtin container spends one, and its members spend theirs, as a value would (SPEC-063).

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


_TRANSFORM_MAX_DEPTH = 100
"""The one structural bound on a transform's coercion, the same on every thread (SPEC-058 FR-001).

Without a bound the pass is cut by the interpreter instead, and unmarked: an unbounded
``_Coercer`` pass over a 20,000-deep nest stopped near depth 330, varying by thread, with
``<unserializable: dict>`` and ``truncated`` false — measured by the spec review at ``69dac4f``
and re-measured by the implementation plan at ``7042810``. A fixed bound puts assembly's own
``<depth limit>`` marker at the same place on the drain thread, the orphan path and a small
stack. The cost is accepted in the spec: a ``max_depth`` above 99 does not reach a transform's
output that fails the check.
"""


class _TypesOnly:
    """The three ceilings ``_Coercer`` reads, set so that only the structural bound applies.

    A transform's output is coerced for type only (SPEC-058 FR-001): what a transform adds is the
    caller's code choosing the size, so ``max_value_bytes`` and ``max_keys`` are lifted to
    ``sys.maxsize`` and ``max_depth`` is the fixed bound rather than the configured one. A
    stand-in rather than a ``Config`` because the coercer reads exactly these three attributes —
    which ``tests/test_transform_coercion.py`` derives from ``_Coercer``'s source, so a fourth
    read there fails a test rather than an event. Validation is not the reason:
    ``configure()`` validates the ceilings; a hand-built ``Config`` does not.

    Attributes:
      max_value_bytes: No per-value ceiling.
      max_keys: No per-container ceiling.
      max_depth: The fixed structural bound, :data:`_TRANSFORM_MAX_DEPTH`.
    """

    __slots__ = ()
    max_value_bytes = sys.maxsize
    max_keys = sys.maxsize
    max_depth = _TRANSFORM_MAX_DEPTH


def _coerce_unbounded(event: Mapping[str, object]) -> tuple[dict[str, object], bool]:
    """Coerces a transform's output for type only, under no configured ceiling (SPEC-058 FR-001).

    The rules are assembly's, unchanged — ``bytes`` decoded, a ``datetime`` to ISO text,
    ``Decimal`` and ``UUID`` to text, a non-finite float to its marker, a lone surrogate to
    U+FFFD, a cycle to its marker, anything else to its type-name placeholder — applied by the
    same ``_Coercer`` with :class:`_TypesOnly` for its config. The event is entered at depth
    ``-1``, so its top-level values sit at depth 0 and the values of ``fields`` at depth 1.

    It runs outside SPEC-057's two budgets, which bound work at assembly. A cut here would drop
    the values after it, envelope keys included, from output this pass promises not to clip. The
    cost is accepted: ``json.dumps`` stops at the first fault, so a result that faults early reaches
    this pass nearly unwalked, and a shared-sublist result then costs its whole expansion here,
    several times what the same shape costs the check with no fault, because this pass builds the
    expansion where the check only serializes it. The acceptance rests on that being the caller's
    own code; the register entry carries the measurement.

    **Unlike** :func:`sanitize_fields` **it has no top-level guard.** A mapping that cannot be
    entered — a ``dict`` subclass whose ``items()`` raises — propagates its exception to the
    caller, because an empty event delivered in place of a hostile one is a silent change to the
    data, where a raise reaches the wrapper's per-event guard and is counted there.

    Args:
      event: The mapping the transform returned. It is never mutated.

    Returns:
      A new coerced mapping, and whether the pass made a substitution assembly marks with
      ``truncated``.

    Raises:
      Exception: Whatever the top-level mapping's own iteration raises.
      TypeError: If the pass returned something other than a mapping, which a fresh pass over a
        mapping cannot; checked so the return type is a promise rather than a cast.
    """
    coercer = _Coercer(cast("Config", _TypesOnly()), budgeted=False)
    result = coercer.mapping(event, -1)
    if not isinstance(result, dict):
        raise TypeError(f"a transform's coercion produced {type(result).__name__}, not a dict")
    return result, coercer.truncated
