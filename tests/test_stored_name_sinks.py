"""A raise whose class's metaclass ``__name__`` raises is still settled, named by its stored name.

The behavioural half of ``tests/test_stored_name_roster.py``: that roster holds that no module in
``src/`` reads a type's name through attribute lookup, and these drive the sites it moved, so a
metaclass property that raises — counted here every time it runs — is shown never to run on the
path. Measured with ``a2cd4b36``'s ``src/`` under these tests, the property ran in every test
that reaches a name read — every rig case but the four ``_RIG_SILENT`` names as never reaching
one — to three effects. Kafka's three sites, ``GooglePubSubSink`` and the ``auth`` unpack let its
``RuntimeError`` escape, from paths documented ``Raises: None`` or ``Raises: ValueError``. The
engine sinks' guard absorbed it as ``absorbed a failure while delivering a batch (RuntimeError);
its round's events not confirmed``, so a refused round was reported as unconfirmed and its loss
line was never written. The two file sinks could not build the dual-typed exception
(``_driver_error``) and raised the bare original, which an ``except SinkDeliveryError`` misses;
the same third effect followed from a metaclass ``__module__``, ``__doc__`` or
``__getattribute__`` on the class ``_driver_error`` copies, or a descriptor stored as its
``__doc__``, and pickling the dual-typed exception
raised under a metaclass ``__bases__``. Serves invariant 1.
"""

from __future__ import annotations

import pickle
from typing import TYPE_CHECKING, Any

import pytest

from log_foundry.sinks._driver_error import delivery_error
from log_foundry.sinks.base import SinkDeliveryError
from sink_rigs import RIGS, ok

if TYPE_CHECKING:
    from pathlib import Path

_RAN: list[str] = []
"""Every time a hostile metaclass ``__name__`` ran, by the class it was asked of."""


class _NamelessMeta(type):
    """A metaclass whose ``__name__`` records that it ran, then raises."""

    @property
    def __name__(cls) -> str:  # type: ignore[override]
        _RAN.append(type.__dict__["__name__"].__get__(cls))
        raise RuntimeError("the metaclass __name__ ran")


class Nameless(Exception, metaclass=_NamelessMeta):
    """An exception whose name is readable only as the one its class stores."""


class NamelessTypeError(TypeError, metaclass=_NamelessMeta):
    """A ``TypeError`` an ``__iter__`` raises, named only by its stored name."""


@pytest.fixture(autouse=True)
def _ran() -> list[str]:
    _RAN.clear()
    return _RAN


def _lines(err: str, needle: str) -> list[str]:
    return [line for line in err.splitlines() if needle in line]


_RIG_SILENT = {
    "KafkaSink": "its rig's producer never raises State.error; Kafka is driven directly below",
    "SQLiteSink": "its rig writes through a real connection that State.error never reaches",
    "StdoutSink": "its rig's stream never raises State.error",
    "StderrSink": "its rig's stream never raises State.error",
    "GooglePubSubSink": (
        "every publish() is refused, and a refusal of every event raises before the line is "
        "announced; its partial refusal is driven directly below"
    ),
}
"""Rigs whose emit writes no loss line naming ``State.error``, each with its reason.

The first four never reach a name read here, so their case asserts nothing this change moved.
``GooglePubSubSink`` does reach one, ``publish()``'s refusal, and its case still asserts that the
property never ran there.
"""


@pytest.mark.parametrize("name", sorted(RIGS))
def test_a_client_raise_whose_class_name_raises_is_settled_by_its_stored_name(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The run fails as any refused run does, and its loss line names ``Nameless``."""
    rig = RIGS[name](monkeypatch, tmp_path)
    rig.state.error = Nameless("SENTINEL")
    raised: SinkDeliveryError | None = None
    try:
        rig.sink.emit([ok("a")])
    except SinkDeliveryError as err:
        raised = err
    assert _RAN == []
    lost = _lines(capsys.readouterr().err, "log-foundry: lost")
    if name in _RIG_SILENT:
        assert lost == [], _RIG_SILENT[name]
    else:
        assert raised is not None
        assert len(lost) == 1 and lost[0].endswith(", Nameless"), lost


def test_every_silent_rig_is_still_a_rig() -> None:
    """An entry for a rig that no longer exists is stale and would excuse nothing."""
    assert set(_RIG_SILENT) <= set(RIGS)


class _Producer:
    def __init__(self, refuse: BaseException | None = None) -> None:
        self.refuse = refuse
        self.callbacks: list[Any] = []

    def produce(self, topic: str, value: Any = None, key: Any = None, callback: Any = None) -> None:
        if self.refuse is not None and b'"bad"' in value:
            raise self.refuse
        self.callbacks.append(callback)

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float | None = None) -> int:
        return 0


def test_a_kafka_produce_refusal_whose_class_name_raises_is_named_by_its_stored_name(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A message ``produce()`` refuses is counted and its line names ``Nameless``; the other lands.

    A refusal of every message fails the emit with ``SinkDeliveryError`` and announces nothing,
    so one message of two is refused here, which is the route whose line carries the name.
    """
    from log_foundry.sinks.kafka import KafkaSink

    producer = _Producer(refuse=Nameless("SENTINEL"))
    KafkaSink("topic", producer=producer).emit([ok("a"), ok("bad")])
    assert _RAN == []
    assert len(producer.callbacks) == 1
    err = capsys.readouterr().err
    assert "SENTINEL" not in err
    assert len(_lines(err, "Nameless")) == 1, err


def test_a_kafka_encode_raise_whose_class_name_raises_costs_only_its_event(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The shared encode guard names the raise by its stored name; the other event is produced."""
    from log_foundry.sinks.kafka import KafkaSink

    class Refusing(KafkaSink):
        def _encode(self, event: dict[str, object]) -> tuple[bytes, bytes | None]:
            if event["log_id"] == "bad":
                raise Nameless("SENTINEL")
            return super()._encode(event)

    producer = _Producer()
    Refusing("topic", producer=producer).emit([ok("a"), ok("bad")])
    assert _RAN == []
    assert len(producer.callbacks) == 1
    lost = _lines(capsys.readouterr().err, "log-foundry: lost")
    assert len(lost) == 1 and lost[0].endswith("encoding raised Nameless"), lost


def test_a_kafka_delivery_error_whose_class_name_raises_is_counted_and_named(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The delivery callback, run on librdkafka's thread, counts the failure and cannot raise."""
    from log_foundry.sinks.kafka import KafkaSink

    producer = _Producer()
    sink = KafkaSink("topic", producer=producer)
    sink.emit([ok("a")])
    producer.callbacks[0](Nameless("SENTINEL"), None)
    assert _RAN == []
    assert sink.failed == 1
    lost = _lines(capsys.readouterr().err, "log-foundry: lost")
    assert lost == ["log-foundry: lost 1 message(s); KafkaSink delivery, Nameless"], lost


def test_an_http_auth_unpack_raising_a_nameless_class_is_refused_as_documented() -> None:
    """``auth``'s one documented refusal is ``ValueError``, naming the unpack's stored type name."""
    from log_foundry.sinks.http import HTTPSink

    class Pair:
        def __iter__(self) -> Any:
            raise NamelessTypeError("SENTINEL")

    with pytest.raises(ValueError, match=r"auth cannot be sent over HTTP \(NamelessTypeError\)$"):
        HTTPSink("http://x", auth=Pair())
    assert _RAN == []


class _PartialPublisher:
    def __init__(self) -> None:
        self.published: list[bytes] = []

    def publish(self, topic: str, data: bytes | None = None, **attrs: Any) -> Any:
        assert data is not None
        if b'"bad"' in data:
            raise Nameless("SENTINEL")
        self.published.append(data)
        from sink_rigs import _Future

        return _Future()


def test_a_pubsub_publish_refusal_whose_class_name_raises_is_named_by_its_stored_name(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A message ``publish()`` refuses is counted and its line names ``Nameless``; the other lands."""
    from log_foundry.sinks.pubsub import GooglePubSubSink

    publisher = _PartialPublisher()
    GooglePubSubSink("projects/p/topics/t", client=publisher).emit([ok("a"), ok("bad")])
    assert _RAN == []
    assert len(publisher.published) == 1
    err = capsys.readouterr().err
    assert "SENTINEL" not in err
    assert len(_lines(err, "Nameless")) == 1, err


def _raising_property(name: str) -> property:
    def get(cls: type) -> object:
        _RAN.append(name)
        raise RuntimeError(f"the metaclass {name} ran")

    return property(get)


class _GetattributeMeta(type):
    """A metaclass whose ``__getattribute__`` raises for every name ``_dual`` copies."""

    def __getattribute__(cls, name: str) -> Any:
        if name in {"__name__", "__qualname__", "__module__", "__doc__", "__bases__"}:
            _RAN.append(name)
            raise RuntimeError(f"the metaclass __getattribute__ ran for {name}")
        return super().__getattribute__(name)


class _RaisingDoc:
    """A descriptor stored as a class's own ``__doc__``, which ``type``'s ``__doc__`` would call."""

    def __get__(self, obj: object, cls: type | None = None) -> str:
        _RAN.append("__doc__ descriptor")
        raise RuntimeError("the __doc__ descriptor ran")


class _RaisingSetName:
    """A non-``str`` ``__doc__`` whose ``__set_name__`` ``type()`` would run if it were copied."""

    def __set_name__(self, owner: type, name: str) -> None:
        _RAN.append("__doc__ __set_name__")
        raise RuntimeError("the __doc__ value's __set_name__ ran")


def _hostile_os_error(meta: type | str) -> type[OSError]:
    if meta == "doc-descriptor":
        return type("HostileOSError", (OSError,), {"__doc__": _RaisingDoc()})
    if meta == "doc-set-name":
        error_class = type("HostileOSError", (OSError,), {})
        error_class.__doc__ = _RaisingSetName()  # type: ignore[assignment]
        return error_class
    assert isinstance(meta, type)
    return meta("HostileOSError", (OSError,), {})  # type: ignore[no-any-return]


@pytest.mark.parametrize(
    "meta",
    [
        type("_ModuleMeta", (type,), {"__module__": _raising_property("__module__")}),
        type("_DocMeta", (type,), {"__doc__": _raising_property("__doc__")}),
        _GetattributeMeta,
        "doc-descriptor",
        "doc-set-name",
    ],
    ids=[
        "module-property",
        "doc-property",
        "getattribute",
        "doc-descriptor-in-class",
        "doc-not-a-str",
    ],
)
@pytest.mark.parametrize("name", ["FileSink", "RotatingFileSink"])
def test_a_file_sink_raises_the_dual_type_whatever_the_driver_class_metaclass_hides(
    name: str, meta: type | str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The dual-typed exception is built from what the class stores, so both ``except`` match."""
    error_class = _hostile_os_error(meta)
    rig = RIGS[name](monkeypatch, tmp_path)
    rig.state.error = error_class(5, "SENTINEL")
    caught: BaseException | None = None
    try:
        rig.sink.emit([ok("a")])
    except BaseException as err:
        caught = err
    escaped = "the bare driver error escaped, so delivery_error could not build the dual type"
    assert isinstance(caught, SinkDeliveryError), escaped
    assert isinstance(caught, error_class)
    assert _RAN == []


class _BasesMeta(type):
    """A metaclass whose ``__bases__`` records that it ran, then raises."""

    @property
    def __bases__(cls) -> tuple[type, ...]:  # type: ignore[override]
        _RAN.append("__bases__")
        raise RuntimeError("the metaclass __bases__ ran")


class PicklableHostile(OSError, metaclass=_BasesMeta):
    """A module-level driver error, so pickle can find it, whose metaclass hides ``__bases__``."""


def test_a_dual_type_pickles_as_the_original_past_a_metaclass_bases_property() -> None:
    """``__reduce__`` is documented ``Raises: None``; it reads the stored bases, never the property."""
    dual = delivery_error(PicklableHostile(5, "SENTINEL"))
    assert isinstance(dual, SinkDeliveryError)
    restored = pickle.loads(pickle.dumps(dual))  # noqa: S301
    assert type(restored) is PicklableHostile
    assert restored.args == (5, "SENTINEL")
    assert _RAN == []


class FlippingFilename(OSError):
    """A module-level driver error whose ``filename`` reads cleanly once, then raises."""

    reads = 0

    @property
    def filename(self) -> str | None:
        FlippingFilename.reads += 1
        if FlippingFilename.reads > 1:
            raise RuntimeError("filename raised at pickle time")
        return "SENTINEL-path"

    @filename.setter
    def filename(self, value: object) -> None:
        pass


def test_a_dual_type_still_pickles_when_an_attribute_raises_after_it_was_built() -> None:
    """``__reduce__`` is documented ``Raises: None``; it pickles the original with nothing copied."""
    FlippingFilename.reads = 0
    dual = delivery_error(FlippingFilename(5, "SENTINEL"))
    assert isinstance(dual, SinkDeliveryError)
    restored = pickle.loads(pickle.dumps(dual))  # noqa: S301
    assert type(restored) is FlippingFilename
    assert restored.args == (5, "SENTINEL")
