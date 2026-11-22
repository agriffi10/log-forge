"""A value whose ``__class__`` property raises is type-tested by its real type and never asked.

The behavioural half of ``tests/test_class_check_roster.py``. ``isinstance(value, X)`` reads the
value's ``__class__`` through attribute lookup whenever the real type is not ``X`` or a subclass,
so a caller's exception, stream result, sink result or event field whose class defines a
``__class__`` property ran that property inside the library, and raised from paths documented
``Raises: None`` (invariant 1). Each test here drives a site the roster moved onto
``_diag.is_a`` with a value whose ``__class__`` records that it ran, then raises, and asserts it
never ran and the path settled as it settles for any other value of the same real type.
Serves invariant 1.
"""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path
from typing import Any

import pytest

import driver_standins as standins
from log_foundry.sinks.base import SinkDeliveryError
from sink_rigs import RIGS, ok

_RAN: list[str] = []
"""Every time a hostile hook ran, by the stored name of the class it was asked of."""

_ASKED_FROM: list[str] = []
"""For each entry in :data:`_RAN`, the file of the frame whose code asked for the hook."""

_SRC = str(Path(__file__).resolve().parents[1] / "src" / "log_foundry")
"""The library's own tree, derived from this file rather than ``log_foundry.__file__``."""


def _record(obj: object) -> None:
    _RAN.append(type.__dict__["__name__"].__get__(type(obj)))
    _ASKED_FROM.append(sys._getframe(2).f_code.co_filename)
    raise RuntimeError("the __class__ property ran")


class Claimant(Exception):
    """An exception whose ``__class__`` property records that it ran, then raises."""

    @property  # type: ignore[misc]
    def __class__(self) -> type:  # type: ignore[override]
        _record(self)
        raise AssertionError("unreachable")


class ClaimantValue:
    """A plain value whose ``__class__`` property records that it ran, then raises."""

    @property  # type: ignore[misc]
    def __class__(self) -> type:  # type: ignore[override]
        _record(self)
        raise AssertionError("unreachable")


class ClaimantInt(int):
    """An ``int`` subclass, so a real-type test admits it before ``__class__`` is consulted."""

    @property  # type: ignore[misc]
    def __class__(self) -> type:  # type: ignore[override]
        _record(self)
        raise AssertionError("unreachable")


@pytest.fixture(autouse=True)
def _ran() -> list[str]:
    _RAN.clear()
    _ASKED_FROM.clear()
    return _RAN


def _asked_by_the_library() -> list[str]:
    """The files under ``src/log_foundry`` whose own code asked a hostile hook."""
    return [where for where in _ASKED_FROM if where.startswith(_SRC)]


def _escaped(call: Any) -> type | None:
    """Runs ``call`` and returns the real type of what escaped it, or ``None``.

    Caught here rather than by ``pytest.raises`` because pytest's reporter formats an escaping
    exception through ``isinstance``, which runs the very property under test and crashes the run
    with ``INTERNALERROR`` instead of failing the test.
    """
    try:
        call()
    except Exception as err:
        return type(err)
    return None


def _lines(err: str, needle: str) -> list[str]:
    return [line for line in err.splitlines() if needle in line]


_RIG_SILENT = {
    "KafkaSink": "its rig's producer never raises State.error",
    "SQLiteSink": "its rig writes through a real connection that State.error never reaches",
    "StdoutSink": "its rig's stream never raises State.error",
    "StderrSink": "its rig's stream never raises State.error",
}
"""Rigs whose emit writes no loss line naming ``State.error``, each with its reason."""


@pytest.mark.parametrize("name", sorted(RIGS))
def test_a_client_raise_whose_class_property_raises_is_settled_by_its_real_type(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Every sink's retry predicate and loss line test the raise's real type, never asking it."""
    rig = RIGS[name](monkeypatch, tmp_path)
    rig.state.error = Claimant("SENTINEL")
    escaped = _escaped(lambda: rig.sink.emit([ok("a")]))
    assert _RAN == []
    lost = _lines(capsys.readouterr().err, "log-foundry: lost")
    if name in _RIG_SILENT:
        assert lost == [], _RIG_SILENT[name]
    else:
        assert escaped is not None and issubclass(escaped, SinkDeliveryError)
        assert len(lost) == 1 and lost[0].endswith(", Claimant"), lost


def test_every_silent_rig_is_still_a_rig() -> None:
    """An entry for a rig that no longer exists is stale and would excuse nothing."""
    assert set(_RIG_SILENT) <= set(RIGS)


class _Raising(io.StringIO):
    def __init__(self, error: BaseException) -> None:
        super().__init__()
        self.error = error

    def write(self, s: str) -> int:
        raise self.error


def test_the_console_writer_reads_a_stream_fault_by_its_real_type(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``_is_permanent`` never asks the fault; an unknown type stays a transient, counted fault."""
    from log_foundry.console import ConsoleWriter

    writer = ConsoleWriter(stream=_Raising(Claimant("SENTINEL")))
    assert _escaped(lambda: writer.write({"level": "INFO", "message": "m"})) is None
    assert _RAN == []
    assert writer._disabled is False
    assert writer._failures == 1
    assert _lines(capsys.readouterr().err, "echoing to the console")[0].startswith(
        "log-foundry: absorbed a failure while echoing to the console (Claimant)"
    )


@pytest.mark.parametrize("name", ["StdoutSink", "StderrSink"])
def test_a_stream_sink_reads_a_write_fault_by_its_real_type(
    name: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """``_lose`` and ``_retryable_error`` test the real type; the round is refused and counted."""
    from log_foundry.sinks import stdout as mod

    sink = getattr(mod, name)(stream=_Raising(Claimant("SENTINEL")))
    escaped = _escaped(lambda: sink.emit([ok("a")]))
    assert escaped is not None and issubclass(escaped, SinkDeliveryError)
    assert _RAN == []
    lost = _lines(capsys.readouterr().err, "log-foundry: lost")
    assert len(lost) == 1 and lost[0].endswith(", Claimant"), lost


def test_an_http_opener_raise_is_classified_by_its_real_type(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``_exchange`` refuses the raise without asking whether it is a ``SinkDeliveryError``."""
    from log_foundry.sinks.http import HTTPSink

    def opener(*args: Any, **kwargs: Any) -> Any:
        raise Claimant("SENTINEL")

    sink = HTTPSink("http://127.0.0.1:9/x", opener=opener, max_retries=0)
    escaped = _escaped(lambda: sink.emit([ok("a")]))
    assert escaped is not None and issubclass(escaped, SinkDeliveryError)
    assert _RAN == []
    lost = _lines(capsys.readouterr().err, "log-foundry: lost")
    assert len(lost) == 1 and "Claimant" in lost[0], lost


def test_json_of_an_ok_event_is_unchanged() -> None:
    """A guard on the rig itself: the event every case above emits encodes."""
    assert json.loads(json.dumps(ok("a")))["log_id"] == "a"


class _Errno(Exception):
    """An exception whose ``errno`` is a claimant, as a driver's malformed error can carry."""

    def __init__(self) -> None:
        super().__init__("SENTINEL")
        self.errno = ClaimantValue()


class _KafkaError:
    """A delivery error whose ``code()`` returns a claimant."""

    def code(self) -> object:
        return ClaimantValue()


def _losses_of_a_claimant() -> object:
    from log_foundry.sinks.base import read_losses

    class Reporting:
        def losses(self) -> object:
            return ClaimantValue()

    return read_losses(Reporting())


def _usable(results: object) -> object:
    from log_foundry.sinks._batch import usable_results

    return usable_results(results)


def _errno_of() -> str:
    from log_foundry import _diag

    return _diag.errno_of(_Errno())


def _call(dotted: str, *args: Any, **kwargs: Any) -> Any:
    module, _, name = dotted.rpartition(".")
    return getattr(__import__(module, fromlist=[name]), name)(*args, **kwargs)


_UNCHECKED = object()
"""The expected outcome of a case whose value this test does not pin, only that it settles."""


@pytest.mark.parametrize(
    ("call", "expected"),
    [
        (_errno_of, ""),
        (lambda: _call("log_foundry._otel.log_record", ClaimantValue()), {}),
        (lambda: _call("log_foundry._otel.resource_key", ClaimantValue()), (None, None, None)),
        (lambda: _call("log_foundry.sinks._time.epoch_seconds", ClaimantValue()), _UNCHECKED),
        (lambda: _call("log_foundry.sinks._time.epoch_nanos", ClaimantValue()), _UNCHECKED),
        (lambda: _call("log_foundry.sinks.file._taken", [ClaimantValue()], 5), 0),
        (lambda: _call("log_foundry.sinks.http._read_failed", ClaimantValue()), False),
        (lambda: _call("log_foundry.sinks.otlp._rejected_count", ClaimantValue()), None),
        (lambda: _call("log_foundry.sinks.honeycomb._status", {"status": ClaimantValue()}), None),
        (lambda: _call("log_foundry.sinks.kafka._code", _KafkaError()), ""),
        (
            lambda: _call("log_foundry.sinks.elasticsearch._has_error", {"index": ClaimantValue()}),
            False,
        ),
        (lambda: _usable(ClaimantValue()), []),
        (lambda: _usable([ClaimantValue()]), []),
        (_losses_of_a_claimant, None),
    ],
    ids=[
        "errno_of",
        "otel-log_record",
        "otel-resource_key",
        "epoch_seconds",
        "epoch_nanos",
        "file-taken",
        "http-read_failed",
        "otlp-rejected_count",
        "honeycomb-status",
        "kafka-code",
        "elasticsearch-has_error",
        "usable_results",
        "usable_results-item",
        "read_losses",
    ],
)
def test_a_total_reader_tests_a_claimant_by_its_real_type(call: Any, expected: object) -> None:
    """Each reader documented ``Raises: None`` neither raises nor asks the claimant."""
    outcome: list[object] = []
    escaped = _escaped(lambda: outcome.append(call()))
    assert _RAN == []
    assert escaped is None
    if expected is not _UNCHECKED:
        assert outcome == [expected]


def test_the_aws_predicate_reads_a_client_errors_response_by_its_real_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ``ClientError`` whose ``response`` is a claimant is not retryable, and is never asked."""
    from log_foundry.sinks._classify import aws_retryable

    botocore = standins.install(monkeypatch, standins.BOTOCORE)
    err = standins.client_error(botocore, 503, "Throttling")
    err.response = ClaimantValue()  # type: ignore[attr-defined]
    outcome: list[bool] = []
    assert _escaped(lambda: outcome.append(aws_retryable(err))) is None
    assert _RAN == []
    assert outcome == [False]


def _claimant_event() -> dict[str, object]:
    return {
        "log_id": "c",
        "level": ClaimantValue(),
        "timestamp": ClaimantValue(),
        "message": "m",
        "fields": {"x": ClaimantValue()},
        "error": ClaimantValue(),
    }


@pytest.mark.parametrize("name", sorted(RIGS))
def test_an_event_a_caller_hands_a_sink_is_read_by_its_fields_real_types(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A direct ``emit`` of an event full of claimants settles, and no library frame asks one.

    The property does run, inside the per-event encode guard: ``json``'s own ``default`` names an
    unencodable value through ``o.__class__.__name__``, and ``bson``'s encoder asks ``Mapping``.
    That is the standard library's and the driver's code under the guard that makes an event a
    sink cannot encode cost only itself, not a type test of the library's, so the assertion is on
    the frame that asked.
    """
    rig = RIGS[name](monkeypatch, tmp_path)
    escaped = _escaped(lambda: rig.sink.emit([_claimant_event(), ok("a")]))
    assert _asked_by_the_library() == []
    assert escaped is None or issubclass(escaped, SinkDeliveryError), escaped


def test_a_filtering_sink_reads_a_level_by_its_real_type() -> None:
    """A level that is not a ``str`` by real type is unknown and fails open, never asked."""
    from log_foundry.sinks.filtering import FilteringSink
    from log_foundry.sinks.memory import MemorySink

    inner = MemorySink()
    sink = FilteringSink(inner, min_level="INFO")
    assert _escaped(lambda: sink.emit([_claimant_event(), ok("a")])) is None
    assert _RAN == []
    assert [e["log_id"] for e in inner.events] == ["c", "a"]


def test_a_transform_returning_a_claimant_costs_only_its_event(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A raising ``__class__`` is read as not a ``dict`` under ``_apply``'s guard: one event lost.

    ``_apply`` keeps ``isinstance`` on purpose, for the proxy below, so the property does run;
    what this holds is that the raise costs its own event and nothing else.
    """
    from log_foundry.sinks.memory import MemorySink
    from log_foundry.sinks.transform import TransformSink

    inner = MemorySink()
    sink = TransformSink(inner, lambda e: ClaimantValue() if e["log_id"] == "c" else e)  # type: ignore[arg-type,return-value]
    assert _escaped(lambda: sink.emit([{"log_id": "c"}, ok("a")])) is None
    assert _RAN == ["ClaimantValue"]
    assert [e["log_id"] for e in inner.events] == ["a"]
    lost = _lines(capsys.readouterr().err, "log-foundry: lost")
    assert lost == [
        "log-foundry: lost 1 event(s); TransformSink, the transform failed with TypeError"
    ], lost


class _Proxy:
    """A transparent proxy that forwards ``__class__`` and the mapping protocol, as ``wrapt`` does."""

    def __init__(self, wrapped: object) -> None:
        object.__setattr__(self, "_wrapped", wrapped)

    @property  # type: ignore[misc]
    def __class__(self) -> type:  # type: ignore[override]
        return type(object.__getattribute__(self, "_wrapped"))

    def __getattr__(self, name: str) -> Any:
        return getattr(object.__getattribute__(self, "_wrapped"), name)

    def __iter__(self) -> Any:
        return iter(object.__getattribute__(self, "_wrapped"))

    def __len__(self) -> int:
        return len(object.__getattribute__(self, "_wrapped"))

    def __getitem__(self, key: Any) -> Any:
        return object.__getattribute__(self, "_wrapped")[key]


def test_a_transform_returning_a_dict_proxy_still_delivers_it_as_an_exact_dict() -> None:
    """A proxy of a ``dict`` has always passed ``_apply`` and been coerced; a real-type test lost it."""
    from log_foundry.sinks.memory import MemorySink
    from log_foundry.sinks.transform import TransformSink

    inner = MemorySink()
    sink = TransformSink(inner, lambda e: _Proxy(dict(e)))  # type: ignore[arg-type,return-value]
    assert _escaped(lambda: sink.emit([ok("a")])) is None
    assert [type(e) for e in inner.events] == [dict]
    assert inner.events[0]["log_id"] == "a"


def test_a_proxy_is_now_refused_where_the_decision_names_it() -> None:
    """``read_losses`` reads a proxied ``SinkLosses`` as none, and a direct ``LoggingSink`` emit
    reads a proxied level as unknown: the two costs the decision states, pinned."""
    import logging

    from log_foundry.sinks.base import SinkLosses, read_losses
    from log_foundry.sinks.logging_sink import LoggingSink

    class Reporting:
        def losses(self) -> object:
            return _Proxy(SinkLosses(dropped=1, failed=0))

    assert read_losses(Reporting()) is None
    records: list[logging.LogRecord] = []
    logger = logging.getLogger("test_class_claim.proxy")
    logger.propagate = False
    logger.setLevel(logging.DEBUG)

    class Keep(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    logger.addHandler(Keep())
    LoggingSink(logger).emit([{"log_id": "a", "level": _Proxy("ERROR"), "message": "m"}])
    assert [r.levelno for r in records] == [logging.INFO]


def test_a_sink_factory_returning_a_claimant_is_refused_as_not_a_sink(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The protocol check's raise is read as a refusal, never escaping past ``SinkDeliveryError``.

    ``_checked`` keeps ``isinstance(result, Sink)`` on purpose, so the property does run, inside
    the ``typing`` and ``abc`` machinery; what this holds is that it is announced as a refusal.
    """
    from log_foundry.sinks._factory import _FactorySink

    sink = _FactorySink(ClaimantValue)  # type: ignore[arg-type]
    escaped = _escaped(lambda: sink.emit([ok("a")]))
    assert escaped is SinkDeliveryError
    assert "it returned ClaimantValue, not a sink" in capsys.readouterr().err


def test_a_sink_factory_still_accepts_what_the_protocol_check_accepts() -> None:
    """A ``MagicMock(spec=…)`` of a sink passes, as it does at ``configure(sink=…)``."""
    from unittest.mock import MagicMock

    from log_foundry.sinks._factory import _FactorySink
    from log_foundry.sinks.memory import MemorySink

    inner = MagicMock(spec=MemorySink)
    sink = _FactorySink(lambda: inner)
    assert _escaped(lambda: sink.emit([ok("a")])) is None
    inner.emit.assert_called_once()


class _HashRaising(type):
    """A metaclass whose ``__hash__`` records that it ran, then raises."""

    def __hash__(cls) -> int:
        _RAN.append(type.__dict__["__name__"].__get__(cls))
        raise RuntimeError("the metaclass __hash__ ran")


class Pair(tuple, metaclass=_HashRaising):  # type: ignore[type-arg]
    """A ``(user, password)`` tuple whose class cannot be hashed into an ABC's cache."""


def test_an_http_auth_whose_class_cannot_be_hashed_is_refused_as_documented() -> None:
    """A metaclass ``__hash__`` the ``Mapping`` check runs is read as unordered: ``ValueError``."""
    from log_foundry.sinks.http import _copied_pair

    escaped = _escaped(lambda: _copied_pair(Pair(("alice", "pw")), "HTTPSink"))
    assert escaped is ValueError
    assert _RAN == ["Pair"]


class _Registered:
    """A map ``Mapping`` knows only by ``register``, as ``pyrsistent.pmap`` is known."""

    def __init__(self, keys: tuple[str, str]) -> None:
        self._keys = keys

    def __iter__(self) -> Any:
        return iter(self._keys)

    def __len__(self) -> int:
        return len(self._keys)

    def __getitem__(self, key: str) -> str:
        return key


__import__("collections.abc").abc.Mapping.register(_Registered)


@pytest.mark.parametrize(
    "auth",
    [{"alice": 1, "pw": 2}, frozenset({"alice", "pw"}), _Registered(("alice", "pw"))],
    ids=["dict", "frozenset", "registered-mapping"],
)
def test_an_unordered_auth_is_still_refused(auth: object) -> None:
    """A map is refused before its two keys could be sent as the credentials, registered or not."""
    from log_foundry.sinks.http import _copied_pair

    with pytest.raises(ValueError, match="must be a str token or a pair of two str"):
        _copied_pair(auth, "HTTPSink")


class _ClaimsInt:
    """A value whose ``__class__`` answers ``int``, as a transparent proxy of one does."""

    @property  # type: ignore[misc]
    def __class__(self) -> type:  # type: ignore[override]
        _record_claim(self)
        return int


def _record_claim(obj: object) -> None:
    _RAN.append(type.__dict__["__name__"].__get__(type(obj)))


@pytest.mark.parametrize(
    ("value", "classes", "expected"),
    [
        (ClaimantInt(3), int, True),
        (True, (str, int), True),
        (Claimant("S"), Exception, True),
        (Claimant("S"), (OSError, ValueError), False),
        (ClaimantValue(), int, False),
        (_ClaimsInt(), int, False),
        ({}, dict, True),
        (Pair(("a", "b")), tuple, True),
    ],
    ids=[
        "subclass",
        "bool-is-an-int-in-a-tuple",
        "exception-subclass",
        "exception-elsewhere",
        "raising-claimant",
        "claim-is-not-admitted",
        "exact",
        "hash-raising-metaclass",
    ],
)
def test_is_a_answers_by_real_type_and_asks_nothing(
    value: object, classes: Any, expected: bool
) -> None:
    """A genuine subclass passes, a claim through ``__class__`` neither passes nor is asked."""
    from log_foundry import _diag

    assert _diag.is_a(value, classes) is expected
    assert _RAN == []


@pytest.mark.parametrize(
    "classes",
    [
        pytest.param(__import__("collections.abc").abc.Mapping, id="abstract"),
        pytest.param((set, __import__("collections.abc").abc.Mapping), id="abstract-in-a-tuple"),
    ],
)
def test_is_a_asks_no_abstract_target_and_reads_its_mro_alone(classes: Any) -> None:
    """An ABC's cache is never asked, so a metaclass ``__hash__`` never runs.

    The MRO is the whole answer, so ``dict``, which ``Mapping`` knows only by ``register``, is no
    ``Mapping`` here, which is why the roster refuses an abstract target.
    """
    from log_foundry import _diag

    assert _diag.is_a(Pair(("a", "b")), classes) is False
    assert _RAN == []
    assert _diag.is_a({}, classes) is False


def test_is_a_narrows_a_tuple_only_on_its_true_branch() -> None:
    """The tuple overload is ``TypeGuard``: ``TypeIs`` there made five retry predicates unreachable.

    ``mypy --strict`` does not warn on unreachable code, so the regression is silent in the gate;
    this pins the overloads' return annotations instead, read as the strings they are stored as.
    """
    from typing import get_overloads

    from log_foundry import _diag

    returns = [o.__annotations__["return"] for o in get_overloads(_diag.is_a)]
    assert returns == ["TypeIs[T]", "TypeGuard[T]"]
