"""A URL ``http.client`` cannot send is refused at construction; surrounding whitespace is floored.

``_valid_url`` accepted ``"http://h\\ud800/x"``: the lone surrogate surfaced only at ``emit``, as a
``UnicodeEncodeError`` with ``failed == 0`` and no stderr line. By the bad-argument rule (SPEC-049 FR-001, the
``sink-delivery`` decision) a value that never delivered is refused and one that delivers is
floored: surrounding whitespace delivered, because ``urllib`` strips it before parsing.
"""

from __future__ import annotations

import http.client
import urllib.request

import pytest

from log_foundry.sinks.datadog import DatadogSink
from log_foundry.sinks.elasticsearch import ElasticsearchSink, OpenSearchSink
from log_foundry.sinks.honeycomb import HoneycombSink
from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.logstash import LogstashSink
from log_foundry.sinks.loki import LokiSink
from log_foundry.sinks.newrelic import NewRelicSink
from log_foundry.sinks.sentry import SentrySink
from log_foundry.sinks.splunk import SplunkHECSink
from test_sinks_http import FakeOpener
from test_sinks_http_render import _shipped_http_subclasses


def _assert_printable(exc: BaseException) -> None:
    """A lone surrogate in a message makes the message itself fail to encode wherever it is shown."""
    assert not any(0xD800 <= ord(char) <= 0xDFFF for char in str(exc)), str(exc).encode(
        "utf-8", "backslashreplace"
    )


_UNSENDABLE = [
    "http://h\ud800/x",
    "http://h/x\ud800",
    "http://h/x?a=\udc80",
    "http://h/\u00e9",
    "http://h/a b",
    "http://h/a\tb",
    "http://h/x\x00",
    "http://h :8/x",
    "http://u:p@h/x",
    "http://h:abc/x",
    "http://\u4f8b.example/x",
    "http://h:\u0668\u0660/x",
    "http://h:8\n/x",
]
"""URLs that pass the scheme and host checks but that ``http.client`` cannot put on the wire.

The last three fail only in the ``Host`` header ``urllib`` writes itself, as the raw host in
Latin-1: a CJK host ``http.client`` would have IDNA-encoded, a port in Arabic-Indic digits that
``int`` reads, and a newline ``int`` strips from the port.
"""


@pytest.mark.parametrize("url", _UNSENDABLE)
def test_an_unsendable_url_raised_uncounted_out_of_every_emit(url: str, capsys) -> None:
    """The premise, against the real default opener: no request, no count, no stderr line.

    The sink is built on a good URL and pointed at the bad one afterwards, which is the only way
    to reach ``emit`` with it now that construction refuses it.
    """
    sink = HTTPSink("http://h/x", max_retries=0)
    sink.url = url

    with pytest.raises((ValueError, http.client.InvalidURL, OSError)) as caught:
        sink.emit([{"i": 1}])

    assert not isinstance(caught.value, OSError), "an OSError would have been counted"
    assert sink.losses().failed == 0
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize("url", _UNSENDABLE)
def test_an_unsendable_url_is_refused_at_construction(url: str) -> None:
    with pytest.raises(ValueError, match=r"^HTTPSink url cannot be sent over HTTP \(") as caught:
        HTTPSink(url)

    _assert_printable(caught.value)


@pytest.mark.parametrize("url", ["http://h:99999/x", "http://h:0/x"])
def test_a_url_naming_no_usable_port_is_refused(url: str) -> None:
    with pytest.raises(ValueError, match=r"^HTTPSink url names no usable port: "):
        HTTPSink(url)


@pytest.mark.parametrize("url", ["http:/\t/h/x", "http:\n//h/x"])
def test_a_host_only_urlparse_finds_is_refused(url: str) -> None:
    """``urlparse`` drops the tab or newline and finds a host; ``urllib`` keeps it and finds none."""
    with pytest.raises(ValueError, match=r"^HTTPSink url names no host: "):
        HTTPSink(url)


def test_a_url_urlparse_cannot_parse_is_refused_naming_the_argument() -> None:
    with pytest.raises(ValueError, match=r"^HTTPSink url is not a URL \(Invalid IPv6 URL\)"):
        HTTPSink("http://[::1/x")


def test_a_scheme_only_urlparse_strips_is_refused() -> None:
    """``urlparse`` drops a leading NUL and reads ``http``; ``urllib`` reads ``\\x00http``."""
    with pytest.raises(ValueError, match=r"^HTTPSink url must use http or https, not '\\x00http'"):
        HTTPSink("\x00http://h/x")


@pytest.mark.parametrize(
    "url", ["http://b\u00fccher.example/x", "http://h/x?q=%zz", "https://h/x#f", "http://h:+80/x"]
)
def test_a_url_http_client_can_send_constructs(url: str) -> None:
    """A Latin-1 host is not refused with the path: the connection resolves it through IDNA, and
    the ``Host`` header carries it as Latin-1.

    ``http://h:+80/x`` delivers: ``http.client`` reads ``+80`` as port 80, where ``urlparse``
    refuses it, which is why the port is read from the connection.
    """
    assert HTTPSink(url).url == url


@pytest.mark.parametrize("pad", [" ", "\t", "\n", "\r\n", "\u00a0", " \t\n"])
def test_surrounding_whitespace_is_floored_to_what_urllib_sent(pad: str) -> None:
    url = f"{pad}http://h/x{pad}"
    assert urllib.request.Request(url).full_url == "http://h/x", "the premise"  # noqa: S310
    opener = FakeOpener()

    sink = HTTPSink(url, opener=opener)
    sink.emit([{"i": 1}])

    assert sink.url == "http://h/x"
    assert opener.calls[0]["url"] == "http://h/x"


@pytest.mark.parametrize("url", ["<URL:http://h/x>", "<http://h/x>", " <URL: http://h/x > "])
def test_a_wrapped_url_is_floored_to_what_urllib_sent(url: str) -> None:
    assert urllib.request.Request(url).full_url == "http://h/x", "the premise"  # noqa: S310

    assert HTTPSink(url).url == "http://h/x"


@pytest.mark.parametrize("method", ["POST\r\n", "PO\x00ST", "P\u00d6ST"])
def test_an_unsendable_method_is_refused_at_construction(method: str) -> None:
    with pytest.raises(ValueError, match=r"^HTTPSink method cannot be sent over HTTP \("):
        HTTPSink("http://h/x", method=method)


def test_an_unsendable_method_raised_uncounted_out_of_every_emit() -> None:
    sink = HTTPSink("http://h/x", max_retries=0)
    sink.method = "POST\r\n"

    with pytest.raises(ValueError, match="method can't contain control characters"):
        sink.emit([{"i": 1}])

    assert sink.losses().failed == 0


_SURROGATE = "\ud800"

_URL_ARGUMENTS = {
    "DatadogSink": ("site", lambda bad: DatadogSink("k", site=f"h{bad}")),
    "ElasticsearchSink": ("url", lambda bad: ElasticsearchSink(f"http://h{bad}", index="i")),
    "HoneycombSink": ("dataset", lambda bad: HoneycombSink("k", f"d{bad}")),
    "LokiSink": ("url", lambda bad: LokiSink(f"http://h{bad}")),
    "NewRelicSink": ("region", lambda bad: NewRelicSink("k", region=f"US{bad}")),
    "OpenSearchSink": ("url", lambda bad: OpenSearchSink(f"http://h{bad}", index="i")),
    "SplunkHECSink": ("url", lambda bad: SplunkHECSink(f"http://h{bad}", "tok")),
}
"""Each shipped subclass, the argument its URL is built from, and how to put a bad value in it.

Keyed by name and swept over the derived roster, so a new subclass with no entry fails the sweep
rather than going unchecked. ``NewRelicSink`` builds its host from a fixed table, so its region
is refused by that table before any URL exists.
"""


@pytest.mark.parametrize("cls", _shipped_http_subclasses(), ids=lambda cls: cls.__name__)
def test_every_http_subclass_refuses_a_surrogate_naming_itself_and_the_argument(
    cls: type[HTTPSink],
) -> None:
    assert cls.__name__ in _URL_ARGUMENTS, f"{cls.__name__} has no entry; add one to _URL_ARGUMENTS"
    argument, build = _URL_ARGUMENTS[cls.__name__]

    with pytest.raises(ValueError, match=rf"^{cls.__name__} {argument} ") as caught:
        build(_SURROGATE)

    _assert_printable(caught.value)


@pytest.mark.parametrize("cls", _shipped_http_subclasses(), ids=lambda cls: cls.__name__)
def test_every_http_subclass_entry_builds_with_a_good_value(cls: type[HTTPSink]) -> None:
    """The positive control: each recipe constructs its class when the value is clean."""
    _argument, build = _URL_ARGUMENTS[cls.__name__]

    assert type(build("")) is cls


@pytest.mark.parametrize(
    "headers",
    [{"X-A": "\u20ac"}, {"X:A": "secret-value"}, {"X-A": "\ud800"}],
    ids=["euro", "colon", "surrogate"],
)
def test_an_unsendable_header_raised_uncounted_out_of_every_emit(headers, capsys) -> None:
    sink = HTTPSink("http://h/x", max_retries=0)
    sink._headers = headers

    with pytest.raises(ValueError):
        sink.emit([{"i": 1}])

    assert sink.losses().failed == 0
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize(
    "headers",
    [{"X-A": "\u20ac"}, {"X:A": "secret-value"}, {"X-A": "\ud800"}],
    ids=["euro", "colon", "surrogate"],
)
def test_an_unsendable_header_is_refused_naming_the_header_not_its_value(headers) -> None:
    [(name, value)] = headers.items()

    with pytest.raises(
        ValueError, match=rf"^HTTPSink header cannot be sent over HTTP .*{name!r}$"
    ) as caught:
        HTTPSink("http://h/x", headers=headers)

    assert value not in str(caught.value)


def test_a_latin_1_header_value_constructs() -> None:
    assert HTTPSink("http://h/x", headers={"X-A": "caf\u00e9"})


def test_a_platform_key_reaches_the_header_check_through_the_subclass() -> None:
    with pytest.raises(ValueError, match=r"^DatadogSink header cannot be sent over HTTP "):
        DatadogSink("\u20ac")


@pytest.mark.parametrize(
    "auth", ["\u20ac", ("u", "p\ud800"), ("\ud800", "p")], ids=["token", "password", "user"]
)
def test_unsendable_credentials_raised_uncounted_out_of_every_emit(auth, capsys) -> None:
    sink = HTTPSink("http://h/x", max_retries=0)
    sink._auth = auth

    with pytest.raises(ValueError):
        sink.emit([{"i": 1}])

    assert sink.losses().failed == 0
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize(
    "auth", ["\u20ac", ("u", "p\ud800"), ("\ud800", "p")], ids=["token", "password", "user"]
)
def test_unsendable_credentials_are_refused_without_echoing_them(auth) -> None:
    with pytest.raises(ValueError, match=r"^HTTPSink auth cannot be sent over HTTP \(\w+\)$"):
        HTTPSink("http://h/x", auth=auth)


@pytest.mark.parametrize("auth", ["caf\u00e9", ("\u00fc", "\u20ac")], ids=["token", "pair"])
def test_sendable_credentials_construct(auth) -> None:
    """A Latin-1 token goes on the wire as is; a pair is base64 of UTF-8, so any text sends."""
    assert HTTPSink("http://h/x", auth=auth)


def test_a_wrapper_refuses_the_url_under_its_own_name() -> None:
    with pytest.raises(ValueError, match=r"^LogstashSink url cannot be sent over HTTP "):
        LogstashSink(url="http://h/a b")


def test_sentry_refuses_an_unsendable_dsn_under_dsn() -> None:
    with pytest.raises(ValueError, match=r"^SentrySink dsn cannot be sent over HTTP "):
        SentrySink(dsn="https://k@h/1 2", backend="http")


def test_sentry_refuses_an_unsendable_public_key_without_echoing_it() -> None:
    with pytest.raises(ValueError, match=r"^SentrySink dsn public key cannot be sent over HTTP$"):
        SentrySink(dsn="https://\u20ac@h/1", backend="http")
