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
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

import pytest

from log_foundry.sinks.datadog import DatadogSink
from log_foundry.sinks.honeycomb import HoneycombSink
from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.newrelic import NewRelicSink
from log_foundry.sinks.splunk import SplunkHECSink
from test_sinks_http import FakeOpener

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


@pytest.mark.parametrize("name", ["Content-Encoding", "content-encoding", "CONTENT-ENCODING"])
def test_a_callers_content_encoding_in_any_case_suppresses_gzip_and_is_sent(name: str) -> None:
    """At ``39a3f08c``: any spelling but the exact one was gzipped and overwritten with ``gzip``."""
    opener = FakeOpener()
    sink = HTTPSink(URL, headers={name: "identity"}, gzip=True, opener=opener)

    plain_opener = FakeOpener()
    plain = _sent(HTTPSink(URL, headers={name: "identity"}, opener=plain_opener), plain_opener)

    request = _sent(sink, opener)

    assert request["headers"]["content-encoding"] == "identity"
    assert request["body"] == plain["body"], "the body is sent uncompressed"


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


@pytest.mark.parametrize("value", ["identity", "br", "gzip, br"])
def test_a_callers_other_encoding_beside_gzip_still_suppresses_compression(value: str) -> None:
    """Behaviour kept as it was at ``39e365f5``, pending a decision on refusing the pairing."""
    plain_opener = FakeOpener()
    plain = _sent(HTTPSink(URL, opener=plain_opener), plain_opener)
    opener = FakeOpener()

    request = _sent(
        HTTPSink(URL, headers={"content-encoding": value}, gzip=True, opener=opener), opener
    )

    assert request["body"] == plain["body"]
    assert request["headers"]["content-encoding"] == value


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
