"""A private, total mapping from an assembled event to OTLP/JSON (SPEC-056) — a leaf module."""

from __future__ import annotations

import json
import math
import re
import sys
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

SEMCONV_VERSION = "1.44.0"
"""The semantic-conventions release the attribute names follow (SPEC-056 FR-002).

The schema URL is derived from it at call time, in :func:`request_body`, and nowhere else: a URL
computed at import would not follow a patched version, and moving the pin is one edit and a
release-noted change. ``1.44.0`` was the release current on 2026-10-05, when the direction was
decided.
"""

SCOPE_NAME = "log_foundry"
"""The instrumentation scope name every record is sent under."""

MAX_NEST = 24
"""Nested containers that render; the 25th becomes ``<depth limit>`` (SPEC-056 FR-001 rule 6).

Counted from the attribute value, which is level 1, and applied to containers only, so a scalar
inside the 24th container still renders. Twenty-four because ``json_format.Parse`` refuses a
document past 100 nested messages and the deepest shape — a ``kvlistValue`` chain, three messages
a level — is accepted at 31 levels and refused at 32, which leaves seven levels of margin with
the envelope included. Reachable: ``configure(max_depth=)`` has no upper bound.
"""

MAX_NODES = 131_072
"""Values one record may hold before the walk stops (SPEC-056 FR-001 rule 7).

A shared subtree is a DAG, not a cycle, and path detection cannot catch it: ``y = [y, y]``
repeated expands to ``2**n`` values, doubling per level, so a record needs a budget on the work it
may cost. Every value, scalar or container, in the body, the attributes and everything nested in
them counts one; a mapping key counts nothing; the first value that would exceed the budget
becomes ``<node limit>``, nothing after it is visited, and the record is marked. The number, the
placeholder text, the stop and the order of checks are SPEC-057's for an assembly pass
(``sanitize._MAX_NODES``), equal on purpose so one hostile shape costs the same wherever it is
walked; a test pins the two together, since this module imports nothing from the package. It is
twice a fully populated two-level structure at the default ``max_keys`` (256 by 256, 65,536
values), so a wide but ordinary event is not cut, and small enough that the worst record is a
fraction of a second on the drain thread, or the caller's on the orphan path (invariant 4):
measured at ``6ac6b3d`` on the authoring machine, a 24-level shared ``[y, y]`` record cost 0.13 s
through :func:`log_record` and ``json.dumps``, and a 256-by-256 field 0.02 s. A smaller budget
would cut legitimate events; a larger one makes the hostile record cost seconds.
"""

type ResourceKey = tuple[str | None, str | None, str | None]

_NODE_LIMIT = "<node limit>"
_DEPTH_LIMIT = "<depth limit>"
_CIRCULAR = "<circular>"
_REPLACEMENT = "�"
_RESERVED_PREFIX = "log_foundry."
_START_MESSAGE = "span.start"
_END_MESSAGE = "span.end"

_SEVERITY = {"DEBUG": 5, "INFO": 9, "WARNING": 13, "ERROR": 17, "CRITICAL": 21}

_TIMESTAMP = re.compile(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\.(\d{3})Z")
_TRACE_ID = re.compile(r"[0-9a-fA-F]{32}")
_SPAN_ID = re.compile(r"[0-9a-fA-F]{16}")
_SURROGATES = re.compile("[\ud800-\udfff]")
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_UINT64_MAX = 2**64 - 1
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
_LOG10_2_NUM = 30103
_LOG10_2_DEN = 100000

_STR_STR = str.__str__
_INT_REPR = int.__repr__
_INT_BIT_LENGTH = int.bit_length
_INT_LT = int.__lt__
_FLOAT_FLOAT = float.__float__
_FLOAT_REPR = float.__repr__
_DICT_GET = dict.get
_DICT_ITEMS = dict.items
_LIST_ITER = list.__iter__
_TUPLE_ITER = tuple.__iter__
"""Unbound builtin accessors: a subclass cannot steer its own mapping (SPEC-056 FR-001 rule 2).

Dispatch is on the builtin base, read through that base's own method, which is the rule
``sanitize`` follows with ``_INT_LT`` and ``_FLOAT_REPR``. An object that only claims a builtin
through ``__class__`` passes ``isinstance`` and then fails the unbound read with ``TypeError``,
which the guard around every value turns into a placeholder.
"""

_MISSING = object()


def _repaired(text: str) -> tuple[str, bool]:
    """Returns a string that encodes as UTF-8, and whether a lone surrogate was replaced.

    One U+FFFD per surrogate, the SPEC-055 rule ``sanitize`` applies at assembly, re-implemented
    here because this module imports nothing from the package. The strict encode is the common
    case and the fast path; the regex runs only after it has failed.

    Args:
      text: An exact ``str``.

    Returns:
      The string, repaired if it had to be, and ``True`` when it was.

    Raises:
      None.
    """
    try:
        text.encode("utf-8")
    except UnicodeEncodeError:
        return _SURROGATES.sub(_REPLACEMENT, text), True
    return text, False


def _text(value: object) -> tuple[str, bool] | None:
    """Reads a string target: an exact ``str`` through ``str.__str__``, repaired, or ``None``.

    Args:
      value: Any value.

    Returns:
      The exact string and whether a surrogate was replaced, or ``None`` when the value is not a
      ``str`` — an object claiming ``str`` only through ``__class__`` included, since the unbound
      read refuses it.

    Raises:
      None.
    """
    if not isinstance(value, str):
        return None
    try:
        plain = _STR_STR(value)
    except Exception:
        return None
    return _repaired(plain)


def _type_name(value: object) -> str:
    """Returns a value's type name for a placeholder, or ``"?"`` when even that cannot be read.

    ``type()`` ignores ``__class__``, so an impostor is named for what it is. A metaclass whose
    ``__name__`` is a raising property is the case the fallback exists for; the name is repaired
    like any other string written, without a mark, since a placeholder is visible on its own.

    Args:
      value: The value whose type is named.

    Returns:
      The type's ``__name__``, or ``"?"``.

    Raises:
      None.
    """
    try:
        name = str(type(value).__name__)
    except Exception:
        return "?"
    return _repaired(name)[0]


def _int_digits(value: int) -> tuple[str, bool]:
    """Renders an integer's digits, or the ``<int: ~N digits>`` placeholder when it cannot.

    Decided by ``bit_length()`` and never by converting, because converting past
    ``sys.get_int_max_str_digits()`` is what raises (SPEC-020). The estimate can overcount by one
    digit, which errs toward the placeholder, as ``sanitize`` accepts.

    Args:
      value: An ``int`` or a subclass of one.

    Returns:
      The digits through ``int.__repr__``, or the placeholder, and whether it was the placeholder.

    Raises:
      TypeError: If the value only claims to be an ``int``; the caller's guard owns that.
    """
    digits = _INT_BIT_LENGTH(value) * _LOG10_2_NUM // _LOG10_2_DEN + 1
    rendered = digits + 1 if _INT_LT(value, 0) else digits
    limit = sys.get_int_max_str_digits()
    if 0 < limit < rendered:
        return f"<int: ~{digits} digits>", True
    return _INT_REPR(value), False


def _time_unix_nano(value: object) -> str | None:
    """Converts the library's own timestamp shape to epoch nanoseconds, with integer arithmetic.

    Exactly ``YYYY-MM-DDTHH:MM:SS.mmmZ`` is accepted: anything else, an impossible date, a time
    at or before the epoch (OTLP defines ``0`` as unknown, and a zero ``fixed64`` is a proto3
    default the round trip omits) and one past the largest ``uint64`` are unusable, and an
    unusable timestamp omits both time fields rather than reading a clock. ``sinks/_time`` is not
    reused: it reads ``time.time_ns()`` for what it cannot parse, accepts every ``fromisoformat``
    shape, and lives outside this leaf (SPEC-056 FR-001).

    Args:
      value: The event's ``timestamp``, of any type.

    Returns:
      The nanoseconds as a decimal string, or ``None`` when unusable.

    Raises:
      None.
    """
    read = _text(value)
    if read is None:
        return None
    match = _TIMESTAMP.fullmatch(read[0])
    if match is None:
        return None
    year, month, day, hour, minute, second, millis = (int(part) for part in match.groups())
    try:
        moment = datetime(year, month, day, hour, minute, second, tzinfo=UTC)
    except ValueError:
        return None
    elapsed = moment - _EPOCH
    nanos = (elapsed.days * 86_400 + elapsed.seconds) * 1_000_000_000 + millis * 1_000_000
    if nanos <= 0 or nanos > _UINT64_MAX:
        return None
    return str(nanos)


class _Walker:
    """One record's value walk: the node budget, the depth cap, the mark and the drop count.

    Attributes:
      nodes: Values written so far, scalar or container; a key is not a value.
      marked: Whether anything was cut, replaced or substituted in a way the reader must be told
        about, which becomes ``log_foundry.truncated``.
      dropped: Attributes dropped at any level — an empty, repeated, colliding or reserved key.
      stopped: Whether the node budget has been spent, so every enclosing container ends.
    """

    __slots__ = ("dropped", "marked", "nodes", "stopped")

    def __init__(self) -> None:
        """Starts a walk with every counter at zero.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.nodes = 0
        self.marked = False
        self.dropped = 0
        self.stopped = False

    def value(self, value: object, depth: int, ancestors: list[int]) -> dict[str, object]:
        """Encodes one value as an ``AnyValue``, totally.

        The node budget is checked first, before the depth cap, so a value past both is
        ``<node limit>`` (rule 7); the placeholder is the one value written past the budget and
        is not itself counted. Everything else runs under one guard, so a value whose unbound
        read raises becomes ``<unserializable: T>`` (rule 9).

        Args:
          value: The value to encode.
          depth: The container nesting level, with an attribute value at 1.
          ancestors: The ``id()`` of every container being descended through.

        Returns:
          The ``AnyValue`` dict.

        Raises:
          None.
        """
        if self.nodes >= MAX_NODES:
            self.stopped = True
            self.marked = True
            return {"stringValue": _NODE_LIMIT}
        self.nodes += 1
        try:
            return self._value(value, depth, ancestors)
        except Exception:
            return {"stringValue": f"<unserializable: {_type_name(value)}>"}

    def _value(self, value: object, depth: int, ancestors: list[int]) -> dict[str, object]:
        """Dispatches on the builtin base through its unbound methods (rules 2, 4, 5, 6, 9).

        Args:
          value: The value to encode.
          depth: The container nesting level.
          ancestors: The ``id()`` of every container being descended through.

        Returns:
          The ``AnyValue`` dict.

        Raises:
          TypeError: From an unbound read of an object that only claims its type.
          Exception: Whatever a hostile object raises; the caller's guard owns it.
        """
        if value is None:
            return {}
        if isinstance(value, bool):
            return {"boolValue": value is True}
        if isinstance(value, int):
            text, placeholder = _int_digits(value)
            if placeholder:
                self.marked = True
                return {"stringValue": text}
            if _INT_LT(value, _INT64_MIN) or _INT_LT(_INT64_MAX, value):
                return {"stringValue": text}
            return {"intValue": text}
        if isinstance(value, float):
            number = _FLOAT_FLOAT(value)
            if math.isfinite(number):
                return {"doubleValue": number}
            if math.isnan(number):
                return {"doubleValue": "NaN"}
            return {"doubleValue": "Infinity" if number > 0 else "-Infinity"}
        if isinstance(value, str):
            text, repaired = _repaired(_STR_STR(value))
            if repaired:
                self.marked = True
            return {"stringValue": text}
        if isinstance(value, dict | list | tuple):
            if depth > MAX_NEST:
                self.marked = True
                return {"stringValue": _DEPTH_LIMIT}
            if id(value) in ancestors:
                return {"stringValue": _CIRCULAR}
            if isinstance(value, dict):
                return self._kvlist(value, depth, ancestors)
            return self._array(value, depth, ancestors)
        raise TypeError(_type_name(value))

    def _kvlist(
        self, mapping: dict[object, object], depth: int, ancestors: list[int]
    ) -> dict[str, object]:
        """Encodes a ``dict`` as a ``kvlistValue`` with unique, non-empty keys (rule 8).

        Args:
          mapping: The dict, read through ``dict.items`` unbound.
          depth: The container's nesting level.
          ancestors: The ``id()`` of every container being descended through.

        Returns:
          The ``AnyValue`` dict, with no ``values`` key when empty.

        Raises:
          Exception: Whatever iterating the dict raises; the caller's guard owns it.
        """
        values: list[dict[str, object]] = []
        seen: set[str] = set()
        ancestors.append(id(mapping))
        try:
            for raw_key, raw_value in _DICT_ITEMS(mapping):
                if self.stopped:
                    break
                key = self.key(raw_key)
                if key is None or key in seen:
                    self.dropped += 1
                    continue
                seen.add(key)
                values.append({"key": key, "value": self.value(raw_value, depth + 1, ancestors)})
        finally:
            ancestors.pop()
        return {"kvlistValue": {"values": values}} if values else {"kvlistValue": {}}

    def _array(
        self, sequence: list[object] | tuple[object, ...], depth: int, ancestors: list[int]
    ) -> dict[str, object]:
        """Encodes a ``list`` or ``tuple`` as an ``arrayValue`` (rule 4).

        Args:
          sequence: The list or tuple, iterated through its base's ``__iter__`` unbound.
          depth: The container's nesting level.
          ancestors: The ``id()`` of every container being descended through.

        Returns:
          The ``AnyValue`` dict, with no ``values`` key when empty.

        Raises:
          Exception: Whatever iterating the sequence raises; the caller's guard owns it.
        """
        iterator = _LIST_ITER(sequence) if isinstance(sequence, list) else _TUPLE_ITER(sequence)
        values: list[dict[str, object]] = []
        ancestors.append(id(sequence))
        try:
            for item in iterator:
                if self.stopped:
                    break
                values.append(self.value(item, depth + 1, ancestors))
        finally:
            ancestors.pop()
        return {"arrayValue": {"values": values}} if values else {"arrayValue": {}}

    def key(self, key: object) -> str | None:
        """Renders a mapping key as ``sanitize`` renders one, totally (rule 8).

        A key that renders is used as it is; one that cannot be rendered becomes
        ``<unserializable key: T>`` and marks the record. The empty string is reported as ``None``
        so the caller drops and counts it.

        Args:
          key: The key, of any type.

        Returns:
          The rendered key, or ``None`` for an empty one.

        Raises:
          None.
        """
        try:
            text = self._key(key)
        except Exception:
            self.marked = True
            text = f"<unserializable key: {_type_name(key)}>"
        return text or None

    def _key(self, key: object) -> str:
        """Renders one key through its builtin base, marking the placeholders that replace one.

        Args:
          key: The key, of any type.

        Returns:
          The rendered key.

        Raises:
          TypeError: For a type outside the domain, or an object that only claims one.
        """
        if isinstance(key, str):
            text, repaired = _repaired(_STR_STR(key))
            if repaired:
                self.marked = True
            return text
        if isinstance(key, bool):
            return "True" if key else "False"
        if isinstance(key, int):
            text, placeholder = _int_digits(key)
            if placeholder:
                self.marked = True
            return text
        if isinstance(key, float):
            number = _FLOAT_FLOAT(key)
            if math.isfinite(number):
                return _FLOAT_REPR(number)
            self.marked = True
            if math.isnan(number):
                return "<float: nan>"
            return "<float: inf>" if number > 0 else "<float: -inf>"
        if key is None:
            return "None"
        raise TypeError(_type_name(key))


def _read(event: dict[str, object], name: str) -> object:
    """Reads one library key through ``dict.get`` unbound, ``_MISSING`` when absent.

    Args:
      event: The event dict.
      name: The key.

    Returns:
      The value, or ``_MISSING``.

    Raises:
      TypeError: If the event only claims to be a ``dict``.
    """
    return _DICT_GET(event, name, _MISSING)


def _exception_type(error: dict[str, object]) -> str | None:
    """Joins ``error.module`` and ``error.type`` as the semantic conventions spell a type.

    ``module.Type``, except that a ``builtins`` exception, and one whose module is empty, absent
    or not a string, uses the bare name, as the convention's own examples do. Neither part is
    re-clipped: both are bounded at assembly.

    Args:
      error: The event's ``error`` dict.

    Returns:
      The qualified name, or ``None`` when ``type`` is not a string.

    Raises:
      TypeError: If the error only claims to be a ``dict``.
    """
    type_read = _text(_read(error, "type"))
    if type_read is None:
        return None
    module_read = _text(_read(error, "module"))
    if module_read is None or module_read[0] in ("", "builtins"):
        return type_read[0]
    return f"{module_read[0]}.{type_read[0]}"


def log_record(event: Mapping[str, object]) -> dict[str, object]:
    """Maps one assembled event to one OTLP/JSON ``LogRecord`` dict, totally (SPEC-056 FR-001).

    Pure: no clock, no I/O, no mutation of the argument. Keys are lowerCamelCase, ids are
    lowercase hex, enums are integers and a ``fixed64`` is a decimal string. The walk order is
    fixed — the library's attributes first, then ``body``, then the user's ``fields`` in event
    order — so a hostile message can spend the budget on itself and the fields after it, but
    never on ``code.function.name``, ``log.record.uid`` or an ``exception.*`` attribute. The
    ``log_foundry.truncated`` mark is decided after the walk, written in its table position and
    not counted, so a cut record is always marked.

    Args:
      event: The event ``build_event`` or ``end_event`` assembled, or whatever a caller hands a
        sink directly. Anything that is not a ``dict`` maps to ``{}``.

    Returns:
      The ``LogRecord`` as a dict ready for ``json.dumps``.

    Raises:
      None.
    """
    if not isinstance(event, dict):
        return {}
    try:
        return _log_record(event)
    except Exception:
        return {}


def _log_record(event: dict[str, object]) -> dict[str, object]:
    """Builds the record for an event whose reads are made under :func:`log_record`'s guard.

    Args:
      event: The event dict.

    Returns:
      The ``LogRecord`` dict.

    Raises:
      TypeError: If the event only claims to be a ``dict``; the caller's guard owns it.
    """
    walker = _Walker()
    record: dict[str, object] = {}
    nanos = _time_unix_nano(_read(event, "timestamp"))
    if nanos is not None:
        record["timeUnixNano"] = nanos
        record["observedTimeUnixNano"] = nanos
    level = _text(_read(event, "level"))
    if level is not None and level[0]:
        number = _SEVERITY.get(level[0])
        if number is not None:
            record["severityNumber"] = number
        record["severityText"] = level[0]
        walker.marked = walker.marked or level[1]
    attributes = _library_attributes(event, walker)
    library_keys = {name for name, _ in attributes if name != _RESERVED_PREFIX + "truncated"}
    message = _read(event, "message")
    if message is not _MISSING:
        record["body"] = walker.value(message, 1, [])
        event_name = _text(message)
        if event_name is not None and event_name[0] in (_START_MESSAGE, _END_MESSAGE):
            record["eventName"] = event_name[0]
    attributes.extend(_user_attributes(_read(event, "fields"), library_keys, walker))
    truncated = _read(event, "truncated") is True or walker.marked
    written = [
        {"key": name, "value": value}
        for name, value in attributes
        if name != _RESERVED_PREFIX + "truncated" or truncated
    ]
    if written:
        record["attributes"] = written
    if walker.dropped:
        record["droppedAttributesCount"] = walker.dropped
    trace_id = _hex_id(_read(event, "trace_id"), _TRACE_ID)
    if trace_id is not None:
        record["flags"] = 1
        record["traceId"] = trace_id
    span_id = _hex_id(_read(event, "span_id"), _SPAN_ID)
    if span_id is not None:
        record["spanId"] = span_id
    return record


def _hex_id(value: object, shape: re.Pattern[str]) -> str | None:
    """Reads a trace or span id: hex of the right length, not all zero, written lowercase.

    Args:
      value: The event's id, of any type.
      shape: The pattern for the id's length.

    Returns:
      The lowercase id, or ``None`` for anything else and for the all-zero W3C sentinel.

    Raises:
      None.
    """
    read = _text(value)
    if read is None or shape.fullmatch(read[0]) is None:
        return None
    lowered = read[0].lower()
    if lowered.strip("0") == "":
        return None
    return lowered


def _library_attributes(
    event: dict[str, object], walker: _Walker
) -> list[tuple[str, dict[str, object]]]:
    """Builds the library's attributes in table order, each read at the type its target needs.

    The ``log_foundry.truncated`` slot is written with its final value here and filtered by the
    caller once the mark is known; it costs no node either way.

    Args:
      event: The event dict.
      walker: The record's walk, charged for every value written.

    Returns:
      The ``(key, AnyValue)`` pairs, in order.

    Raises:
      TypeError: If the event only claims to be a ``dict``; an ``error`` that does costs only
        its three attributes, through :func:`_exception_reads`.
    """
    pairs: list[tuple[str, dict[str, object]]] = []
    for name, key in (("function", "code.function.name"), ("log_id", "log.record.uid")):
        read = _text(_read(event, name))
        if read is not None:
            pairs.append((key, walker.value(read[0], 1, [])))
    parent = _text(_read(event, "parent_span_id"))
    if parent is not None:
        pairs.append((_RESERVED_PREFIX + "parent_span_id", walker.value(parent[0], 1, [])))
    pairs.append((_RESERVED_PREFIX + "truncated", {"boolValue": True}))
    duration = _read(event, "duration_ms")
    if isinstance(duration, float | int) and not isinstance(duration, bool):
        pairs.append((_RESERVED_PREFIX + "duration_ms", walker.value(duration, 1, [])))
    status = _text(_read(event, "status"))
    if status is not None:
        pairs.append((_RESERVED_PREFIX + "status", walker.value(status[0], 1, [])))
    error = _read(event, "error")
    if isinstance(error, dict):
        for key, text in _exception_reads(error):
            pairs.append((key, walker.value(text, 1, [])))
    return pairs


def _exception_reads(error: dict[str, object]) -> list[tuple[str, str]]:
    """Reads the three ``exception.*`` strings an ``error`` dict carries, under their own guard.

    An ``error`` that only claims ``dict`` through ``__class__`` passes ``isinstance`` and fails
    the unbound ``dict.get`` (rule 2); guarded here, that costs the three exception attributes and
    nothing else, where a raise reaching :func:`log_record`'s outer guard would cost the whole
    record (rule 1: a key read at the wrong type is omitted, not fatal).

    Args:
      error: The event's ``error`` value, known to pass ``isinstance(error, dict)``.

    Returns:
      ``(attribute key, text)`` pairs in table order, empty when the dict cannot be read.

    Raises:
      None.
    """
    try:
        exception_type = _exception_type(error)
        message = _text(_read(error, "message"))
        stack = _text(_read(error, "stack"))
    except Exception:
        return []
    pairs: list[tuple[str, str]] = []
    if exception_type is not None:
        pairs.append(("exception.type", exception_type))
    if message is not None:
        pairs.append(("exception.message", message[0]))
    if stack is not None:
        pairs.append(("exception.stacktrace", stack[0]))
    return pairs


def _user_attributes(
    fields: object, library_keys: set[str], walker: _Walker
) -> list[tuple[str, dict[str, object]]]:
    """Builds the user's attributes in event order, dropping and counting what cannot stand.

    A field whose key the library set on this record is displaced by it; one whose key starts
    with ``log_foundry.`` is dropped whether or not the library set it, so it can never pose as a
    library attribute; an empty or repeated key is dropped; every drop is counted. The values of a
    dropped attribute are never walked.

    Args:
      fields: The event's ``fields``; anything but a ``dict`` contributes nothing.
      library_keys: The attribute keys the library wrote on this record.
      walker: The record's walk.

    Returns:
      The ``(key, AnyValue)`` pairs, in order.

    Raises:
      None. A ``fields`` that only claims ``dict`` through ``__class__`` fails the unbound
        ``dict.items`` under its own guard and contributes nothing, so the record keeps its
        timestamp, body and library attributes (rules 1 and 2).
    """
    pairs: list[tuple[str, dict[str, object]]] = []
    if not isinstance(fields, dict):
        return pairs
    try:
        items = list(_DICT_ITEMS(fields))
    except Exception:
        return pairs
    seen: set[str] = set()
    for raw_key, raw_value in items:
        if walker.stopped:
            break
        key = walker.key(raw_key)
        if key is None or key.startswith(_RESERVED_PREFIX) or key in library_keys or key in seen:
            walker.dropped += 1
            continue
        seen.add(key)
        pairs.append((key, walker.value(raw_value, 1, [])))
    return pairs


def resource_key(event: Mapping[str, object]) -> ResourceKey:
    """Reads an event's three resource stamps as the key its ``resourceLogs`` group is formed on.

    Total, because ``HTTPSink._body`` calls it outside any guard, so a raise here fails the whole
    ``emit`` (SPEC-056 FR-002). Each stamp is read as any string target is: an exact ``str``, or a
    subclass through ``str.__str__``, with a lone surrogate replaced by U+FFFD; anything else is
    ``None``, an object claiming ``str`` through ``__class__`` included, and so is every stamp
    of an event that is not a ``dict``. The repair is not marked: a stamp is configuration, not
    record data, and ``configure()`` already refuses one that cannot encode.

    Args:
      event: The event, or whatever a caller handed the sink.

    Returns:
      ``(service, version, env)``, each an exact ``str`` or ``None`` — hashable, valid UTF-8.

    Raises:
      None.
    """
    if not isinstance(event, dict):
        return (None, None, None)
    try:
        stamps = [_text(_read(event, name)) for name in ("service", "version", "env")]
    except Exception:
        return (None, None, None)
    service, version, env = (None if read is None else read[0] for read in stamps)
    return (service, version, env)


_RESOURCE_ATTRIBUTES = ("service.name", "service.version", "deployment.environment.name")


def _dumps(value: object) -> str:
    """Serializes one envelope part compactly, non-ASCII raw, refusing a non-finite number.

    Args:
      value: A dict or string built here, never a caller's value.

    Returns:
      The JSON text.

    Raises:
      None.
    """
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _group(key: ResourceKey, records: Sequence[str], scope: str, schema_url: str) -> str:
    """Assembles one ``resourceLogs`` entry around its records, in canonical form.

    A field at its proto3 default is omitted and an empty list is never written: a stamp that is
    ``None`` leaves its attribute out, and all three leave ``resource`` an empty object, which is
    what ``MessageToDict`` writes back, so the round trip can compare.

    Args:
      key: The group's resource key.
      records: The rendered records, already JSON.
      scope: The ``scope`` object, already JSON.
      schema_url: The schema URL, already JSON.

    Returns:
      The entry's JSON text.

    Raises:
      None.
    """
    attributes = [
        {"key": name, "value": {"stringValue": stamp}}
        for name, stamp in zip(_RESOURCE_ATTRIBUTES, key, strict=True)
        if stamp is not None
    ]
    resource = _dumps({"attributes": attributes}) if attributes else "{}"
    return (
        '{"resource":'
        + resource
        + ',"scopeLogs":[{"scope":'
        + scope
        + ',"logRecords":['
        + ",".join(records)
        + '],"schemaUrl":'
        + schema_url
        + '}],"schemaUrl":'
        + schema_url
        + "}"
    )


def request_body(records: Sequence[tuple[ResourceKey, str]], scope_version: str) -> str:
    """Builds the ``ExportLogsServiceRequest`` JSON for rendered records (SPEC-056 FR-002).

    One ``resourceLogs`` per distinct key, in first-seen order, because ``configure()`` mid-run
    changes a stamp for events not yet built and one batch can hold both; within a group the
    records keep event order. The schema URL is derived from :data:`SEMCONV_VERSION` here, at
    call time. Records are joined with a single comma and no whitespace, which is the separator
    byte :func:`framing`'s arithmetic and ``OTLPSink._item_size`` count.

    Args:
      records: ``(resource_key, rendered_record)`` pairs in event order, each record already
        serialized by ``json.dumps``.
      scope_version: The library version for ``scope.version``; empty leaves the key out.

    Returns:
      The request document as a string; ``{"resourceLogs":[]}`` for no records.

    Raises:
      None.
    """
    groups: dict[ResourceKey, list[str]] = {}
    for key, rendered in records:
        groups.setdefault(key, []).append(rendered)
    scope: dict[str, str] = {"name": SCOPE_NAME}
    if scope_version:
        scope["version"] = scope_version
    scope_json = _dumps(scope)
    schema_url = _dumps(f"https://opentelemetry.io/schemas/{SEMCONV_VERSION}")
    return (
        '{"resourceLogs":['
        + ",".join(_group(key, items, scope_json, schema_url) for key, items in groups.items())
        + "]}"
    )


def framing(key: ResourceKey, scope_version: str) -> int:
    """Returns what one resource group costs around its records, in UTF-8 bytes.

    The unit ``HTTPSink._item_size`` charges: a ``resource``, a ``scopeLogs`` wrapper and two
    schema URLs. Computed as the difference between a request holding one empty record under
    this key and an empty request, so it can never disagree with :func:`request_body`, and in
    bytes rather than characters because ``ensure_ascii=False`` makes a stamp such as ``café``
    longer in bytes than in characters.

    Args:
      key: The resource key.
      scope_version: The library version the scope carries.

    Returns:
      The group's framing in bytes.

    Raises:
      None.
    """
    with_one = len(request_body([(key, "")], scope_version).encode("utf-8"))
    return with_one - len(request_body([], scope_version).encode("utf-8"))
