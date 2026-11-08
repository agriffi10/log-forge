"""A platform sink honours any mapping of headers, and a caller's ``Content-Encoding`` in any case.

Two places that judged the caller's headers by a narrower rule than ``HTTPSink`` itself.
``merge_headers`` tested ``isinstance(caller, dict)``, so a ``MappingProxyType`` or a list of pairs
was dropped silently by the four sinks that use it — an ``Authorization`` beside ``auth=`` included,
which ``HTTPSink`` refuses — where ``HTTPSink`` reads either with ``dict()``. And ``_prepare``
tested ``"Content-Encoding" not in self._headers`` by exact case, so ``content-encoding: identity``
beside ``gzip=True`` gzipped the body and wrote ``Content-Encoding: gzip`` over the caller's.
"""

from __future__ import annotations

import gzip
import inspect
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

import pytest

from log_foundry.sinks.datadog import DatadogSink
from log_foundry.sinks.honeycomb import HoneycombSink
from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.logstash import LogstashSink
from log_foundry.sinks.newrelic import NewRelicSink
from log_foundry.sinks.sentry import SentrySink
from log_foundry.sinks.splunk import SplunkHECSink
from test_sinks_http import FakeOpener
from test_sinks_http_render import _RECIPES, _shipped_http_subclasses

if TYPE_CHECKING:
    from collections.abc import Callable

URL = "http://h/x"


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Neutralize the HTTP backoff so a failing case cannot stall the suite."""
    monkeypatch.setattr("log_foundry.sinks.http.wait", lambda _delay, _stop=None: None)


_MERGERS: dict[str, Callable[..., Any]] = {
    "DatadogSink": lambda **kw: DatadogSink("key", **kw),
    "HoneycombSink": lambda **kw: HoneycombSink("key", "d", **kw),
    "NewRelicSink": lambda **kw: NewRelicSink("key", **kw),
    "SplunkHECSink": lambda **kw: SplunkHECSink(URL, "tok", **kw),
    "HTTPSink": lambda **kw: HTTPSink(URL, **kw),
}
"""The four sinks that pass the caller's headers through ``merge_headers``, and ``HTTPSink``, the
behaviour each must match."""

_SHAPES = {
    "mappingproxy": lambda pairs: MappingProxyType(dict(pairs)),
    "list-of-pairs": list,
}


def _sent(sink: Any, opener: FakeOpener) -> dict[str, Any]:
    """Emits one event and returns the request the opener recorded."""
    sink.emit([{"i": 1}])
    return opener.calls[-1]


def test_the_merger_roster_is_every_sink_calling_merge_headers() -> None:
    """Derived from the source, so a fifth caller without a row fails here."""
    import pathlib

    import log_foundry.sinks

    root = pathlib.Path(log_foundry.sinks.__file__).parent
    callers = {path.stem for path in root.glob("*.py") if "merge_headers(" in path.read_text()} - {
        "http"
    }
    assert callers == {"datadog", "honeycomb", "newrelic", "splunk"}


@pytest.mark.parametrize("shape", sorted(_SHAPES))
@pytest.mark.parametrize("cls", sorted(_MERGERS))
def test_a_non_dict_mapping_of_headers_is_sent(cls: str, shape: str) -> None:
    """At ``39a3f08c``: the four platform sinks dropped it; ``HTTPSink`` sent it."""
    opener = FakeOpener()
    headers = _SHAPES[shape]([("X-Extra", "1")])

    request = _sent(_MERGERS[cls](headers=headers, opener=opener), opener)

    assert request["headers"]["x-extra"] == "1"


@pytest.mark.parametrize("shape", sorted(_SHAPES))
@pytest.mark.parametrize("cls", sorted(_MERGERS))
def test_a_non_dict_authorization_beside_auth_is_refused_as_httpsink_refuses_it(
    cls: str, shape: str
) -> None:
    """At ``39a3f08c``: the header was dropped, so ``auth`` alone was sent and nothing refused."""
    headers = _SHAPES[shape]([("Authorization", "Bearer HEADER-VALUE")])

    with pytest.raises(ValueError) as caught:
        _MERGERS[cls](auth="SECRET-BEARER", headers=headers)

    message = str(caught.value)
    if cls == "SplunkHECSink":
        assert message == (
            "SplunkHECSink header 'Authorization' collides with token, which this sink sends as "
            "Authorization; drop the header"
        )
    else:
        assert message == (
            f"{cls} auth and an Authorization header cannot both be given, since each sets the "
            "one Authorization header a request carries; drop auth= or the header"
        )
    assert "SECRET-BEARER" not in message and "HEADER-VALUE" not in message


@pytest.mark.parametrize("cls", sorted(_MERGERS))
def test_a_non_dict_whitespace_name_is_refused_as_httpsink_refuses_it(cls: str) -> None:
    with pytest.raises(ValueError) as caught:
        _MERGERS[cls](headers=MappingProxyType({"X Extra": "SECRET-VALUE"}))

    assert str(caught.value) == f"{cls} header name must not contain whitespace: 'X Extra'"


_ENCODING_NAMES = ["Content-Encoding", "content-encoding", "CONTENT-ENCODING"]
_NOT_GZIP = ["identity", "br", "gzip, br", "", 5]


def _gzip_refused(owner: str, name: str) -> str:
    """The refusal of ``gzip=True`` beside ``name``, written out."""
    return (
        f"{owner} gzip=True cannot apply beside header {name!r}, which names another encoding; "
        "drop gzip= or the header"
    )


@pytest.mark.parametrize("value", _NOT_GZIP, ids=repr)
@pytest.mark.parametrize("name", _ENCODING_NAMES)
def test_gzip_beside_a_callers_other_encoding_is_refused(name: str, value) -> None:
    """At ``d7638711``: constructs, the header wins, and ``gzip=True`` is silently ignored."""
    with pytest.raises(ValueError) as caught:
        HTTPSink(URL, headers={name: value}, gzip=True)

    assert str(caught.value) == _gzip_refused("HTTPSink", name)
    if value:
        assert str(value) not in str(caught.value).replace("gzip=True", "")
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


_A_CODING = ["gzip", "GZIP", " x-gzip\t", "br", "identity, gzip", "identity, identity", 5, b"gzip"]
"""Values naming a coding the sink never applies under ``gzip=False``: a token other than
``identity``, a list, and a value that is not a ``str``, which names no coding this module reads."""

_NO_CODING = ["identity", "IDENTITY", " Identity\t", "", " ", "\t \t"]
"""``identity`` in any case and padding, and a value empty once space and tab are stripped — an
empty list of codings — both true of a body sent as built."""


def _gzip_false_refused(owner: str, name: str) -> str:
    """The refusal of ``gzip=False`` beside ``name``, written out."""
    return (
        f"{owner} gzip=False, the default, cannot apply beside header {name!r}, which names an "
        "encoding the body is not in; drop the header, or pass gzip=True if it names gzip"
    )


@pytest.mark.parametrize("gzip_given", [{}, {"gzip": False}], ids=["default", "explicit"])
@pytest.mark.parametrize("value", _A_CODING, ids=repr)
@pytest.mark.parametrize("name", _ENCODING_NAMES)
def test_gzip_false_beside_a_callers_coding_is_refused(name: str, value, gzip_given) -> None:
    """At ``c4e51fbd``: constructs and labels a body sent as built with a coding it is not in, on
    every request. The sink builds the body, so the caller cannot have encoded it."""
    with pytest.raises(ValueError) as caught:
        HTTPSink(URL, headers={name: value}, **gzip_given)

    assert str(caught.value) == _gzip_false_refused("HTTPSink", name)
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


def _logstash(**kw: Any) -> LogstashSink:
    """``LogstashSink`` wraps an ``HTTPSink`` in HTTP mode; its refusals name ``HTTPSink``."""
    return LogstashSink(url=URL, **kw)


_GZIP_TAKERS: dict[str, tuple[Callable[..., Any], str]] = {
    **{name: (make, name) for name, make in _RECIPES.items()},
    "HTTPSink": (lambda **kw: HTTPSink(URL, **kw), "HTTPSink"),
    "LogstashSink": (_logstash, "HTTPSink"),
}
"""Every class taking ``gzip`` and ``headers``: a builder, and the owner its message names.
``SentrySink`` takes neither."""


def test_the_gzip_roster_covers_every_shipped_subclass() -> None:
    assert {cls.__name__ for cls in _shipped_http_subclasses()} <= set(_GZIP_TAKERS)


@pytest.mark.parametrize("name", sorted(_GZIP_TAKERS))
def test_every_class_refuses_gzip_false_beside_a_coding_and_sends_identity(name: str) -> None:
    make, owner = _GZIP_TAKERS[name]

    with pytest.raises(ValueError) as caught:
        make(headers={"content-encoding": "br"})

    assert str(caught.value) == _gzip_false_refused(owner, "content-encoding")
    opener = FakeOpener()
    request = _sent(make(headers={"content-encoding": "Identity"}, opener=opener), opener)
    assert request["headers"]["content-encoding"] == "Identity"


@pytest.mark.parametrize("value", _NO_CODING, ids=repr)
@pytest.mark.parametrize("name", _ENCODING_NAMES)
def test_no_coding_beside_gzip_false_is_sent_over_an_uncompressed_body(name: str, value) -> None:
    plain_opener = FakeOpener()
    plain = _sent(HTTPSink(URL, opener=plain_opener), plain_opener)
    opener = FakeOpener()

    request = _sent(HTTPSink(URL, headers={name: value}, opener=opener), opener)

    assert request["body"] == plain["body"]
    assert request["headers"]["content-encoding"] == value


def test_gzip_set_true_after_construction_beside_identity_still_sends_it_uncompressed() -> None:
    """``gzip`` is a public attribute, so ``_prepare`` still reads the caller's coding per
    request: a compressed body must never go out labelled ``identity``."""
    plain_opener = FakeOpener()
    plain = _sent(HTTPSink(URL, opener=plain_opener), plain_opener)
    opener = FakeOpener()
    sink = HTTPSink(URL, headers={"content-encoding": "identity"}, opener=opener)
    sink.gzip = True

    request = _sent(sink, opener)

    assert request["body"] == plain["body"]
    assert request["headers"]["content-encoding"] == "identity"


def test_gzip_without_a_callers_content_encoding_still_compresses() -> None:
    opener = FakeOpener()
    sink = HTTPSink(URL, headers={"X-Content-Encoding": "identity"}, gzip=True, opener=opener)

    request = _sent(sink, opener)

    assert request["headers"]["content-encoding"] == "gzip"
    assert gzip.decompress(request["body"]).startswith(b"{")


@pytest.mark.parametrize("value", ["gzip", "GZIP", " gzip "], ids=repr)
@pytest.mark.parametrize("name", ["Content-Encoding", "content-encoding", "CONTENT-ENCODING"])
def test_a_callers_gzip_encoding_beside_gzip_is_compressed_under_the_callers_header(
    name: str, value: str
) -> None:
    """At ``1b201316``: every spelling sent the body uncompressed and labelled gzip; at
    ``39a3f08c`` the exact-case one did. Content-codings are case-insensitive (RFC 9110 §8.4.1).
    """
    plain_opener = FakeOpener()
    plain = _sent(HTTPSink(URL, opener=plain_opener), plain_opener)
    opener = FakeOpener()

    request = _sent(HTTPSink(URL, headers={name: value}, gzip=True, opener=opener), opener)

    assert gzip.decompress(request["body"]) == plain["body"]
    assert request["headers"]["content-encoding"] == value, "the caller's header, and only it"


@pytest.mark.parametrize("headers", [0, 1.5, object()], ids=["zero", "float", "object"])
@pytest.mark.parametrize("cls", sorted(_MERGERS))
def test_headers_that_are_no_mapping_are_refused_by_every_twin_alike(cls: str, headers) -> None:
    """At ``1b201316``: ``HTTPSink(headers=0)`` read the falsy value as no headers and sent none,
    while every platform sink raised from ``dict(0)``."""
    with pytest.raises(TypeError, match="object is not iterable"):
        _MERGERS[cls](headers=headers)


@pytest.mark.parametrize("headers", [None, {}, [], ()], ids=["none", "dict", "list", "tuple"])
@pytest.mark.parametrize("cls", sorted(_MERGERS))
def test_empty_or_absent_headers_construct_on_every_twin(cls: str, headers) -> None:
    assert _MERGERS[cls](headers=headers)


def _logstash(**kw: Any) -> LogstashSink:
    """``LogstashSink`` forwards ``gzip`` to the ``HTTPSink`` it builds; refusals name that sink."""
    return LogstashSink(url=URL, **kw)


_GZIP_TAKERS: dict[str, tuple[Callable[..., Any], str]] = {
    **{name: (make, name) for name, make in _RECIPES.items()},
    "HTTPSink": (lambda **kw: HTTPSink(URL, **kw), "HTTPSink"),
    "LogstashSink": (_logstash, "HTTPSink"),
}
"""Every class taking or forwarding ``gzip``: a builder, and the owner its refusal names.

``SentrySink`` is absent because it takes no ``gzip`` and no caller headers, asserted below.
"""


def test_the_gzip_roster_covers_every_shipped_subclass_and_sentry_takes_none() -> None:
    shipped = {cls.__name__ for cls in _shipped_http_subclasses()}
    assert shipped <= set(_GZIP_TAKERS), shipped - set(_GZIP_TAKERS)
    parameters = inspect.signature(SentrySink.__init__).parameters
    assert "gzip" not in parameters
    assert not any(p.kind is inspect.Parameter.VAR_KEYWORD for p in parameters.values())


@pytest.mark.parametrize("cls", sorted(_GZIP_TAKERS))
def test_every_class_forwarding_gzip_refuses_it_beside_another_encoding(cls: str) -> None:
    """At ``d7638711``: each constructed and sent its body uncompressed, ``gzip=True`` ignored."""
    make, owner = _GZIP_TAKERS[cls]

    with pytest.raises(ValueError) as caught:
        make(headers={"content-encoding": "identity"}, gzip=True)

    assert str(caught.value) == _gzip_refused(owner, "content-encoding")


@pytest.mark.parametrize("cls", sorted(_GZIP_TAKERS))
def test_every_class_forwarding_gzip_compresses_under_a_callers_gzip_header(cls: str) -> None:
    make, _owner = _GZIP_TAKERS[cls]
    opener = FakeOpener()

    request = _sent(make(headers={"CONTENT-ENCODING": "GZIP"}, gzip=True, opener=opener), opener)

    assert request["headers"]["content-encoding"] == "GZIP"
    assert gzip.decompress(request["body"])


@pytest.mark.parametrize("value", ["x-gzip", "X-GZIP", " x-gzip\t"], ids=repr)
def test_x_gzip_names_gzip_beside_gzip(value: str) -> None:
    """RFC 9110 §8.4.1.3 has ``x-gzip`` read as ``gzip``. At ``635c9edf``: refused."""
    plain_opener = FakeOpener()
    plain = _sent(HTTPSink(URL, opener=plain_opener), plain_opener)
    opener = FakeOpener()

    request = _sent(
        HTTPSink(URL, headers={"content-encoding": value}, gzip=True, opener=opener), opener
    )

    assert gzip.decompress(request["body"]) == plain["body"]
    assert request["headers"]["content-encoding"] == value
