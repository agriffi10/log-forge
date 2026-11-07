"""SPEC-071 FR-005 — every argument door asks the type, never the value, and names the argument.

Each door is driven, through its public entry and directly, with the four lying shapes the spec
measured at `4fbfd60`:

- `Claims`: a `__class__` property returning the door's declared type;
- `Raises`: a `__class__` property that raises;
- `Disguised`: a class whose metaclass `__name__` property returns the declared type's name;
- `Hidden`: a class whose metaclass `__name__` property raises.

Every getter counts its calls, and every door must leave the count at zero: `isinstance` fell
back to the value's own `__class__`, and `type(value).__name__` asked the metaclass, so at
`4fbfd60` the first shape got a descriptor error naming no argument, the second raised its own
exception, and the third was told `must be a str, got str`.

The callable doors (`_args.require_callable`) and the HTTP sinks' `auth`, routed the same way,
are held here too. Serves invariants 13 and 6: one verdict, with the argument named, on every
twin path.
"""

from __future__ import annotations

import contextlib
import sys
import types
from typing import TYPE_CHECKING, Any, ClassVar

import pytest

import log_foundry
from log_foundry import config
from log_foundry.sinks import _retry
from log_foundry.sinks._args import require_callable
from log_foundry.sinks.callback import CallbackSink
from log_foundry.sinks.filtering import FilteringSink
from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.memory import MemorySink
from log_foundry.sinks.otlp import OTLPSink
from log_foundry.sinks.sqs import SQSSink
from log_foundry.sinks.transform import TransformSink

if TYPE_CHECKING:
    from collections.abc import Iterator


class _Counted:
    """Counts every call of a lying getter, across all the shapes, and arms the raising ones.

    The raising getters raise only while armed, inside the call under test. Outside it they
    answer honestly, so that pytest can still render a failing case: its own ``saferepr``
    dispatches on ``type(x).__name__`` and would otherwise turn one red case into an
    INTERNALERROR that ends the whole session.
    """

    calls: ClassVar[int] = 0
    armed: ClassVar[bool] = False


@contextlib.contextmanager
def _armed() -> Iterator[None]:
    """Arms the raising getters for the duration of the call under test."""
    _Counted.armed = True
    try:
        yield
    finally:
        _Counted.armed = False


def _claims(declared: type) -> object:
    """Returns an instance whose ``__class__`` property claims ``declared``."""

    class Claims:
        @property
        def __class__(self) -> type:  # type: ignore[override]
            _Counted.calls += 1
            return declared

    return Claims()


def _raises() -> object:
    """Returns an instance whose ``__class__`` property raises."""

    class Raises:
        @property
        def __class__(self) -> type:  # type: ignore[override]
            _Counted.calls += 1
            if _Counted.armed:
                raise RuntimeError("the __class__ getter ran")
            return type(self)

    return Raises()


def _disguised(name: str) -> object:
    """Returns an instance of ``Disguised``, whose metaclass ``__name__`` property says ``name``."""

    class Meta(type):
        @property
        def __name__(cls) -> str:  # type: ignore[override]
            _Counted.calls += 1
            return name

    return Meta("Disguised", (), {})()


def _hidden() -> object:
    """Returns an instance of ``Hidden``, whose metaclass ``__name__`` property raises."""

    class Meta(type):
        @property
        def __name__(cls) -> str:  # type: ignore[override]
            _Counted.calls += 1
            if _Counted.armed:
                raise RuntimeError("the metaclass __name__ getter ran")
            return "Hidden"

    return Meta("Hidden", (), {})()


def _shapes(declared: type) -> list[tuple[str, object, str]]:
    """The four lying shapes for a door declaring ``declared``: id, value, stored name."""
    return [
        ("claims", _claims(declared), "Claims"),
        ("raises", _raises(), "Raises"),
        ("disguised", _disguised(declared.__name__), "Disguised"),
        ("hidden", _hidden(), "Hidden"),
    ]


URL = "http://h/x"

_DOORS: dict[str, tuple[type, list[tuple[str, Any]], str]] = {
    "config._require_positive": (
        int,
        [
            ("configure", lambda v: log_foundry.configure(max_keys=v)),
            ("direct", lambda v: config._require_positive("max_keys", v)),
        ],
        "max_keys must be an int, got {}",
    ),
    "config._require_text": (
        str,
        [
            ("configure", lambda v: log_foundry.configure(service=v)),
            ("direct", lambda v: config._require_text("service", v)),
        ],
        "service must be a str, got {}",
    ),
    "_retry.exact_seconds": (
        float,
        [
            ("max_retry_after", lambda v: HTTPSink(URL, max_retry_after=v)),
            ("direct", lambda v: _retry.exact_seconds(v, "max_retry_after", "HTTPSink")),
        ],
        "HTTPSink max_retry_after must be a number of seconds, not {}",
    ),
    "_retry.require_timeout": (
        float,
        [
            ("timeout", lambda v: HTTPSink(URL, timeout=v)),
            ("direct", lambda v: _retry.require_timeout(v, "timeout", "HTTPSink")),
        ],
        "HTTPSink timeout must be a number of seconds, not {}",
    ),
    "_retry.require_positive": (
        int,
        [
            ("max_batch_bytes", lambda v: HTTPSink(URL, max_batch_bytes=v)),
            ("direct", lambda v: _retry.require_positive(v, "max_batch_bytes", "HTTPSink")),
        ],
        "HTTPSink max_batch_bytes must be an integer, not {}",
    ),
    "_retry.exact_int": (
        int,
        [
            ("max_retries", lambda v: HTTPSink(URL, max_retries=v)),
            ("direct", lambda v: _retry.exact_int(v, "max_retries", "HTTPSink")),
        ],
        "HTTPSink max_retries must be an integer, not {}",
    ),
    "_retry.wait_budget": (
        float,
        [
            ("flush", lambda v: log_foundry.flush(timeout=v)),
            ("shutdown", lambda v: log_foundry.shutdown(timeout=v)),
            ("direct", lambda v: _retry.wait_budget(v, "timeout")),
        ],
        "timeout must be an int or float number of seconds, or None, not {}",
    ),
    "OTLPSink.__init__": (
        str,
        [("endpoint", lambda v: OTLPSink(v))],
        "OTLPSink endpoint must be a str, not {}",
    ),
}
"""Each door: its declared type, its entries, and the message with the stored name to fill."""

_CASES = [
    pytest.param(door, entry, call, shape, value, stored, id=f"{door}-{entry}-{shape}")
    for door, (declared, entries, _) in _DOORS.items()
    for entry, call in entries
    for shape, value, stored in _shapes(declared)
]


@pytest.mark.parametrize(("door", "entry", "call", "shape", "value", "stored"), _CASES)
def test_a_lying_value_is_refused_naming_the_argument_and_its_real_type(
    door: str, entry: str, call: Any, shape: str, value: object, stored: str
) -> None:
    _Counted.calls = 0
    with pytest.raises(TypeError) as info, _armed():
        call(value)
    assert str(info.value) == _DOORS[door][2].format(stored)
    assert _Counted.calls == 0, f"{door} via {entry} ran the {shape} value's own getter"


@pytest.mark.parametrize("shape", ["claims", "raises", "disguised", "hidden"])
def test_flush_and_shutdown_give_one_verdict_for_each_shape(shape: str) -> None:
    """Invariant 6: the two answerers of a flush marker judge ``timeout`` through one door."""
    messages = []
    for call in (log_foundry.flush, log_foundry.shutdown):
        value = {s: v for s, v, _ in _shapes(float)}[shape]
        with pytest.raises(TypeError) as info, _armed():
            call(timeout=value)  # type: ignore[arg-type]
        messages.append(str(info.value))
    assert messages[0] == messages[1]
    assert "timeout" in messages[0]


@pytest.mark.parametrize("shape", ["claims", "raises", "disguised", "hidden"])
def test_the_two_require_positive_twins_give_one_verdict(shape: str) -> None:
    """``config._require_positive`` and ``_retry.require_positive`` judge one value one way."""
    value = {s: v for s, v, _ in _shapes(int)}[shape]
    with pytest.raises(TypeError), _armed():
        log_foundry.configure(max_keys=value)  # type: ignore[arg-type]
    with pytest.raises(TypeError), _armed():
        _retry.require_positive(value, "max_keys", "S")  # type: ignore[arg-type]


class _FloatClaimingInt(float):
    """A real ``float`` whose ``__class__`` property claims ``int``."""

    @property
    def __class__(self) -> type:  # type: ignore[override]
        _Counted.calls += 1
        return int


@pytest.mark.parametrize(
    "door",
    [
        lambda v: _retry.exact_seconds(v, "max_retry_after", "HTTPSink"),
        lambda v: _retry.wait_budget(v, "timeout"),
    ],
    ids=["exact_seconds", "wait_budget"],
)
def test_a_real_float_claiming_int_is_read_as_the_float_it_holds(door: Any) -> None:
    """At ``4fbfd60`` both raised ``descriptor '__index__' requires a 'int' object``."""
    _Counted.calls = 0
    result = door(_FloatClaimingInt(2.5))
    assert result == 2.5
    assert type(result) is float
    assert _Counted.calls == 0


# --- the callable doors -------------------------------------------------------------------------


class _CallableClaims:
    """A callable whose ``__class__`` property claims ``str``, counting the getter's calls."""

    def __call__(self, event: dict[str, object]) -> str:
        return "group-from-callable"

    @property
    def __class__(self) -> type:  # type: ignore[override]
        _Counted.calls += 1
        return str


class _Call:
    """A class instance with ``__call__``."""

    def __call__(self, *_args: object) -> Any:
        return True


@pytest.fixture
def boto3(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """A ``boto3`` stand-in that counts every client built."""
    built: list[str] = []
    module = types.SimpleNamespace(client=lambda service: built.append(service))
    monkeypatch.setitem(sys.modules, "boto3", module)
    return built


_REFUSED: list[tuple[str, Any, str]] = [
    (
        "sqs-fifo-group-int",
        lambda: SQSSink("https://q.fifo", message_group_id=5),
        "SQSSink message_group_id must be a str, a callable or None, not int",
    ),
    (
        "sqs-standard-group-int",
        lambda: SQSSink("https://q", message_group_id=5),
        "SQSSink message_group_id must be a str, a callable or None, not int",
    ),
    (
        "sqs-fifo-dedup-str",
        lambda: SQSSink("https://q.fifo", message_deduplication_id="x"),
        "SQSSink message_deduplication_id must be callable or None, not str",
    ),
    (
        "sqs-standard-dedup-str",
        lambda: SQSSink("https://q", message_deduplication_id="x"),
        "SQSSink message_deduplication_id must be callable or None, not str",
    ),
    (
        "filtering-predicate",
        lambda: FilteringSink(MemorySink(), predicate=5),
        "FilteringSink predicate must be callable or None, not int",
    ),
    (
        "http-opener",
        lambda: HTTPSink(URL, opener="x"),
        "HTTPSink opener must be callable or None, not str",
    ),
    (
        "callback-on-close",
        lambda: CallbackSink(lambda batch: None, on_close=5),
        "CallbackSink on_close must be callable or None, not int",
    ),
    ("callback-fn-int", lambda: CallbackSink(5), "CallbackSink fn must be callable, not int"),
    (
        "callback-fn-none",
        lambda: CallbackSink(None),
        "CallbackSink fn must be callable, not NoneType",
    ),
    (
        "transform-fn-str",
        lambda: TransformSink(MemorySink(), "x"),
        "TransformSink fn must be callable, not str",
    ),
    (
        "transform-fn-none",
        lambda: TransformSink(MemorySink(), None),
        "TransformSink fn must be callable, not NoneType",
    ),
]


@pytest.mark.parametrize(("case", "build", "message"), _REFUSED, ids=[c[0] for c in _REFUSED])
def test_a_non_callable_is_refused_at_construction(
    boto3: list[str], case: str, build: Any, message: str
) -> None:
    with pytest.raises(TypeError) as info:
        build()
    assert str(info.value) == message
    assert boto3 == [], f"{case}: a client was built before the refusal"


class _InstanceCall:
    """An object carrying ``__call__`` as an instance attribute, which no call slot reads."""

    def __init__(self) -> None:
        self.__call__ = lambda *_args: True


class _CountingGetattr:
    """An object whose ``__getattr__`` counts every attribute it is asked for."""

    asked: ClassVar[list[str]] = []

    def __getattr__(self, name: str) -> Any:
        type(self).asked.append(name)
        return lambda *_args: True


def test_the_callable_test_reads_the_type_and_runs_nothing_of_the_value() -> None:
    """``callable()`` reads the type's call slot: an instance ``__call__`` does not make one."""
    with pytest.raises(TypeError, match=r"^S fn must be callable, not _InstanceCall$"):
        require_callable(_InstanceCall(), "fn", "S", optional=False)
    _CountingGetattr.asked = []
    with pytest.raises(TypeError, match=r"^S fn must be callable, not _CountingGetattr$"):
        require_callable(_CountingGetattr(), "fn", "S", optional=False)
    assert _CountingGetattr.asked == []


def test_a_subclass_of_sqs_is_named_in_its_callable_refusals(boto3: list[str]) -> None:
    class MySQS(SQSSink):
        pass

    with pytest.raises(TypeError) as group:
        MySQS("https://q.fifo", message_group_id=5)
    assert str(group.value) == "MySQS message_group_id must be a str, a callable or None, not int"
    with pytest.raises(TypeError) as dedup:
        MySQS("https://q.fifo", message_deduplication_id="x")
    assert str(dedup.value) == "MySQS message_deduplication_id must be callable or None, not str"


@pytest.mark.parametrize("value", [lambda e: True, _Call(), None], ids=["lambda", "class", "None"])
def test_a_callable_or_none_is_still_accepted_where_optional(boto3: list[str], value: Any) -> None:
    FilteringSink(MemorySink(), predicate=value)
    HTTPSink(URL, opener=value)
    CallbackSink(lambda batch: None, on_close=value)
    SQSSink("https://q", message_group_id=value, message_deduplication_id=value)


@pytest.mark.parametrize("value", [lambda e: e, _Call()], ids=["lambda", "class"])
def test_a_required_callable_is_still_accepted(value: Any) -> None:
    CallbackSink(value)
    TransformSink(MemorySink(), value)


def test_a_group_id_str_and_str_subclass_are_accepted_and_a_blank_one_refused(
    boto3: list[str],
) -> None:
    class Group(str):
        pass

    assert SQSSink("https://q.fifo", message_group_id="g").message_group_id == "g"
    assert SQSSink("https://q.fifo", message_group_id=Group("g")).message_group_id == "g"
    with pytest.raises(ValueError, match="message_group_id must be a non-empty string"):
        SQSSink("https://q.fifo", message_group_id="  ")


@pytest.mark.parametrize(
    ("make", "stored"),
    [
        (lambda: _claims(str), "Claims"),
        (lambda: _claims(types.FunctionType), "Claims"),
        (_raises, "Raises"),
        (lambda: _disguised("function"), "Disguised"),
        (_hidden, "Hidden"),
    ],
    ids=["claims-str", "claims-function", "raises", "disguised", "hidden"],
)
def test_a_lying_value_is_refused_by_the_callable_door_without_running_its_getter(
    make: Any, stored: str
) -> None:
    _Counted.calls = 0
    value = make()
    with pytest.raises(TypeError) as info, _armed():
        require_callable(value, "message_group_id", "SQSSink", text=True)
    assert str(info.value) == (
        f"SQSSink message_group_id must be a str, a callable or None, not {stored}"
    )
    assert _Counted.calls == 0


class _Client:
    """Accepts every entry and records each one sent."""

    def __init__(self) -> None:
        self.entries: list[dict[str, Any]] = []

    def send_message_batch(self, *, QueueUrl: str, Entries: list[dict[str, Any]]) -> dict[str, Any]:
        self.entries.extend(Entries)
        return {
            "Successful": [{"Id": e["Id"], "MessageId": e["Id"]} for e in Entries],
            "Failed": [],
        }


def test_a_callable_claiming_str_is_asked_per_event_and_its_getter_never_runs() -> None:
    """M2 of the plan review: ``_group_id`` routed the per-event constant test on ``isinstance``."""
    _Counted.calls = 0
    client = _Client()
    sink = SQSSink("https://q.fifo", client=client, message_group_id=_CallableClaims())
    sink.emit([{"log_id": "a", "trace_id": "t"}])
    assert [e["MessageGroupId"] for e in client.entries] == ["group-from-callable"]
    assert _Counted.calls == 0


def test_a_str_subclass_group_ids_own_strip_never_runs() -> None:
    stripped: list[str] = []

    class Group(str):
        def strip(self, chars: str | None = None) -> str:
            stripped.append(str.__str__(self))
            return str.strip(self, chars)

    client = _Client()
    sink = SQSSink("https://q.fifo", client=client, message_group_id=Group(" g "))
    sink.emit([{"log_id": "a", "trace_id": "t"}, {"log_id": "b", "trace_id": "t"}])
    assert [e["MessageGroupId"] for e in client.entries] == ["g", "g"]
    assert stripped == []


# --- the HTTP sinks' auth -------------------------------------------------------------------------


@pytest.mark.parametrize("make", [lambda: _claims(str), _raises], ids=["claims-str", "raises"])
def test_a_lying_auth_is_refused_naming_auth_and_its_getter_never_runs(make: Any) -> None:
    """At ``4fbfd60``: ``argument of type 'Liar' is not iterable``, and the ``RuntimeError``."""
    _Counted.calls = 0
    value = make()
    with pytest.raises(ValueError) as info, _armed():
        HTTPSink(URL, auth=value)
    assert str(info.value) == "HTTPSink auth cannot be sent over HTTP (TypeError)"
    assert _Counted.calls == 0


def test_an_honest_token_and_pair_are_judged_as_before() -> None:
    assert HTTPSink(URL, auth="token")._auth == "token"
    assert HTTPSink(URL, auth=("user", "pw"))._auth == ("user", "pw")
    with pytest.raises(ValueError, match="auth token must not contain CR or LF"):
        HTTPSink(URL, auth="to\r\nken")
