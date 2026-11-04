"""A URL ``http.client`` cannot send is refused at construction; surrounding whitespace is floored.

``_valid_url`` accepted ``"http://h\\ud800/x"``: the lone surrogate surfaced only at ``emit``, as a
``UnicodeEncodeError`` with ``failed == 0`` and no stderr line. By the bad-argument rule
(SPEC-049 FR-001, the ``sink-delivery`` decision) a value that never delivered is refused and one
that delivers is floored: surrounding whitespace delivered, because ``urllib`` strips it before
parsing.
"""

from __future__ import annotations

import ast
import http.client
import pathlib
import re
import urllib.parse
import urllib.request

import pytest

import log_foundry.sinks
from log_foundry.sinks.base import SinkDeliveryError
from log_foundry.sinks.datadog import DatadogSink
from log_foundry.sinks.elasticsearch import ElasticsearchSink, OpenSearchSink
from log_foundry.sinks.honeycomb import HoneycombSink
from log_foundry.sinks.http import (
    HTTPSink,
    _carries_userinfo,
    _quotable_head,
    _started_request,
)
from log_foundry.sinks.logstash import LogstashSink
from log_foundry.sinks.loki import LokiSink
from log_foundry.sinks.newrelic import NewRelicSink
from log_foundry.sinks.otlp import OTLPSink
from log_foundry.sinks.sentry import SentrySink
from log_foundry.sinks.splunk import SplunkHECSink
from test_sinks_http import FakeOpener
from test_sinks_http_render import _shipped_http_subclasses


def _assert_chains_nothing(exc: BaseException) -> None:
    """The exception ``putheader`` raised carries the whole value, so a refusal chains nothing."""
    assert exc.__cause__ is None
    assert exc.__context__ is None


def _assert_printable(exc: BaseException) -> None:
    """A lone surrogate in a message makes the message itself fail to encode where it is shown."""
    assert not any(0xD800 <= ord(char) <= 0xDFFF for char in str(exc)), str(exc).encode(
        "utf-8", "backslashreplace"
    )


_UNCOUNTED = [
    "http://h\ud800/x",
    "http://h/x\ud800",
    "http://h/x?a=\udc80",
    "http://h/\u00e9",
    "http://\u4f8b.example/x",
    "http://h:\u0668\u0660/x",
    "http://h:8\n/x",
]
"""URLs whose request ``http.client`` cannot encode, so ``emit`` raised them uncounted.

The last three fail only in the ``Host`` header ``urllib`` writes itself, as the raw host in
Latin-1: a CJK host ``http.client`` would have IDNA-encoded, a port in Arabic-Indic digits that
``int`` reads, and a newline ``int`` strips from the port.
"""

_COUNTED = [
    "http://h/a b",
    "http://h/a\tb",
    "http://h/x\x00",
    "http://h :8/x",
    "http://h:abc/x",
]
"""URLs ``http.client`` refuses with ``InvalidURL``, which ``_send`` counts as a connection error.

Counted, but on every request for the life of the process, so they never deliver: refused for the
reason a missing host is.
"""

_UNSENDABLE = _UNCOUNTED + _COUNTED


@pytest.mark.parametrize("url", _UNCOUNTED)
def test_an_unencodable_url_raised_uncounted_out_of_every_emit(url: str, capsys) -> None:
    """The premise, against the real default opener: no request, no count, no stderr line.

    The sink is built on a good URL and pointed at the bad one afterwards, which is the only way
    to reach ``emit`` with it now that construction refuses it.
    """
    sink = HTTPSink("http://h/x", max_retries=0)
    sink.url = url

    with pytest.raises(ValueError):
        sink.emit([{"i": 1}])

    assert sink.losses().failed == 0
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize("url", [*_COUNTED, "http://u:p@h/x"])
def test_an_invalid_url_failed_every_request_without_a_socket(url: str, capsys) -> None:
    """The premise: every attempt is abandoned as ``InvalidURL`` before any connection opens.

    ``http://u:p@h/x`` is the userinfo form ``http.client`` read as a port ``p@h``; construction
    now refuses it as userinfo, before this refusal is reached, so it is not in ``_COUNTED``.
    """
    sink = HTTPSink("http://h/x", max_retries=1)
    sink._sleep_backoff = lambda attempt, retry_after: None

    sink.url = url
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"i": 1}])

    assert sink.losses().failed == 1
    assert "2 attempt(s), connection error, InvalidURL" in capsys.readouterr().err


@pytest.mark.parametrize("url", _UNSENDABLE)
def test_an_unsendable_url_is_refused_at_construction(url: str) -> None:
    with pytest.raises(ValueError, match=r"^HTTPSink url cannot be sent over HTTP \(") as caught:
        HTTPSink(url)

    _assert_printable(caught.value)


@pytest.mark.parametrize("url", ["http://h:99999/x", "http://h:0/x"])
def test_a_url_naming_no_usable_port_is_refused(url: str) -> None:
    with pytest.raises(ValueError, match=r"^HTTPSink url names no usable port: "):
        HTTPSink(url)


_UNQUOTED = "; the URL is not quoted, since its host cannot be told apart from the rest of it"
"""How a refusal of a URL with no quotable scheme-and-host head ends."""

_HEAD_ONLY = (
    "; only the scheme and host are quoted, since a path, query or fragment may hold a credential"
)
"""How a refusal that quotes a URL's head, with something after it, ends."""


@pytest.mark.parametrize("url", ["http:/\t/h/x", "http:\n//h/x"])
def test_a_host_only_urlparse_finds_is_refused(url: str) -> None:
    """``urlparse`` drops the tab or newline and finds a host; ``urllib`` keeps it, finding none.

    Neither starts with ``scheme://``, so neither has a head a refusal can quote.
    """
    with pytest.raises(ValueError, match=r"^HTTPSink url names no host; ") as caught:
        HTTPSink(url)

    assert str(caught.value).endswith(_UNQUOTED)


def test_a_url_urlparse_cannot_parse_is_refused_naming_the_argument() -> None:
    with pytest.raises(
        ValueError, match=r"^HTTPSink url is not a URL \(ValueError\): 'http://\[::1'; "
    ) as caught:
        HTTPSink("http://[::1/x")

    assert str(caught.value).endswith(_HEAD_ONLY)


def test_a_scheme_only_urlparse_strips_is_refused() -> None:
    """``urlparse`` drops a leading NUL and reads ``http``; ``urllib`` reads ``\\x00http``.

    The NUL comes before any scheme, so there is no head to quote, and the type ``urllib`` read is
    not named either.
    """
    with pytest.raises(ValueError, match=r"^HTTPSink url must use http or https; ") as caught:
        HTTPSink("\x00http://h/x")

    assert str(caught.value).endswith(_UNQUOTED)


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
    "OTLPSink": ("endpoint", lambda bad: OTLPSink(f"http://h{bad}/v1/logs")),
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
    _assert_chains_nothing(caught.value)


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
    with pytest.raises(
        ValueError, match=r"^HTTPSink auth cannot be sent over HTTP \(\w+\)$"
    ) as caught:
        HTTPSink("http://h/x", auth=auth)

    _assert_chains_nothing(caught.value)


def test_credentials_that_are_neither_form_are_refused_naming_auth() -> None:
    with pytest.raises(ValueError, match=r"^HTTPSink auth cannot be sent over HTTP \(TypeError\)$"):
        HTTPSink("http://h/x", auth=123)  # type: ignore[arg-type]


def test_credentials_an_authorization_header_overrides_are_not_checked() -> None:
    """``_apply_auth`` never sends them, so they delivered; a lowercase twin is not an override."""
    opener = FakeOpener()
    sink = HTTPSink("http://h/x", headers={"Authorization": "x"}, auth="\u20ac", opener=opener)
    sink.emit([{"i": 1}])
    assert opener.calls[0]["headers"]["authorization"] == "x"

    with pytest.raises(ValueError, match=r"^HTTPSink auth cannot be sent over HTTP "):
        HTTPSink("http://h/x", headers={"authorization": "x"}, auth="\u20ac")


@pytest.mark.parametrize("name", ["x-\u00df", "x-\ufb01"])
def test_a_header_name_urllib_folds_to_ascii_constructs(name: str) -> None:
    """``capitalize`` then ``title``, urllib's two steps, turn these into ``X-Ss`` and ``X-Fi``."""
    assert HTTPSink("http://h/x", headers={name: "1"})


@pytest.mark.parametrize("dsn", ["https://k@[::1/1", "https://k@h:99999/1"])
def test_sentry_refuses_a_dsn_urlparse_cannot_read_under_dsn(dsn: str) -> None:
    with pytest.raises(ValueError, match=r"^SentrySink dsn is not a URL urlsplit can read: ") as e:
        SentrySink(dsn=dsn, backend="http")

    _assert_chains_nothing(e.value)


@pytest.mark.parametrize("auth", ["caf\u00e9", ("\u00fc", "\u20ac")], ids=["token", "pair"])
def test_sendable_credentials_construct(auth) -> None:
    """A Latin-1 token goes on the wire as is; a pair is base64 of UTF-8, so any text sends."""
    assert HTTPSink("http://h/x", auth=auth)


def test_a_wrapper_refuses_the_url_under_its_own_name() -> None:
    with pytest.raises(ValueError, match=r"^LogstashSink url cannot be sent over HTTP "):
        LogstashSink(url="http://h/a b")


def test_sentry_refuses_an_unsendable_dsn_under_dsn() -> None:
    with pytest.raises(ValueError, match=r"^SentrySink dsn cannot be sent over HTTP "):
        SentrySink(dsn="https://k@h/a b/1", backend="http")


def test_sentry_refuses_an_unsendable_public_key_without_echoing_it() -> None:
    with pytest.raises(
        ValueError, match=r"^SentrySink dsn public key cannot be sent over HTTP \(\w+\)$"
    ) as caught:
        SentrySink(dsn="https://k\u20acSECRET@h/1", backend="http")

    _assert_chains_nothing(caught.value)


_AUTH_REMEDY = "auth=(user, password), auth=<token>, or an Authorization header in headers="
"""The remedy a sink taking no credentials of its own names, written out rather than imported."""

_APPENDS = {
    "ElasticsearchSink": ("/_bulk", lambda url: ElasticsearchSink(url, index="i"), _AUTH_REMEDY),
    "HoneycombSink": (
        "/1/batch/d", lambda url: HoneycombSink("k", "d", url=url), "this sink's api_key argument"
    ),
    "LokiSink": ("/loki/api/v1/push", lambda url: LokiSink(url), _AUTH_REMEDY),
    "OpenSearchSink": ("/_bulk", lambda url: OpenSearchSink(url, index="i"), _AUTH_REMEDY),
}
"""Each shipped subclass that appends a path to the caller's base URL: the path, a builder, and
where the userinfo refusal tells the caller to put credentials instead."""

_VERBATIM = {
    "OTLPSink": ("endpoint", lambda url: OTLPSink(url), _AUTH_REMEDY),
    "SplunkHECSink": ("url", lambda url: SplunkHECSink(url, "tok"), "this sink's token argument"),
}
"""Each shipped subclass that sends to the caller's URL as written, so a query is the caller's:
the argument its URL is, a builder, and the credentials remedy."""

_NO_CALLER_BASE = {
    "DatadogSink": (
        "builds its host from site, checked as a host name below",
        lambda site: DatadogSink("k", site=site),
        "this sink's api_key argument",
    ),
    "NewRelicSink": ("builds its host from a fixed table keyed by region", None, None),
}
"""Each shipped subclass that takes no URL from the caller, what it builds its host from, and,
where caller text reaches that host, a builder from that text and the credentials remedy."""

_MISDIRECTING_BASES = [
    "http://h?x=1",
    "http://h?",
    "http://h/base?x",
    "http://h#f",
    "http://h/#",
    "http://u?x@h",
    "http://h?api_key=SECRET",
]
"""Bases whose ``?`` or ``#`` moved the appended path out of the request path."""


def test_every_http_subclass_declares_how_its_url_is_built() -> None:
    """Swept over the derived roster, so a new subclass with no entry fails rather than passing.

    ``SentrySink`` and ``LogstashSink`` wrap an ``HTTPSink`` rather than subclassing one, so this
    roster cannot see them; the DSN's path is tested in ``test_sentry_backend.py``, and
    ``LogstashSink`` sends to its ``url`` as written.
    """
    shipped = [cls.__name__ for cls in _shipped_http_subclasses()]
    declared = [*_APPENDS, *_VERBATIM, *_NO_CALLER_BASE]

    assert sorted(declared) == sorted(shipped)
    assert len(set(declared)) == len(declared), "a class is declared twice"


@pytest.mark.parametrize("base", _MISDIRECTING_BASES)
@pytest.mark.parametrize("name", sorted(_APPENDS))
def test_the_appended_path_never_reached_the_request_path(name: str, base: str) -> None:
    """The premise, on the composition each class performed at 2e790e9: the suffix left the path."""
    suffix, _build, _remedy = _APPENDS[name]
    composed = urllib.request.Request(base.rstrip("/") + suffix)  # noqa: S310

    path, _, _query = composed.selector.partition("?")
    assert not path.endswith(suffix), composed.selector


@pytest.mark.parametrize("base", _MISDIRECTING_BASES)
@pytest.mark.parametrize("name", sorted(_APPENDS))
def test_a_base_with_a_query_or_fragment_is_refused_without_echoing_it(
    name: str, base: str
) -> None:
    _suffix, build, _remedy = _APPENDS[name]

    with pytest.raises(
        ValueError, match=rf"^{name} url cannot carry a query or fragment: "
    ) as caught:
        build(base)

    assert "SECRET" not in str(caught.value)
    assert "api_key" not in str(caught.value)


@pytest.mark.parametrize("name", sorted(_APPENDS))
def test_a_base_with_a_path_still_composes_under_it(name: str) -> None:
    suffix, build, _remedy = _APPENDS[name]

    assert build("https://h/base/").url == f"https://h/base{suffix}"


@pytest.mark.parametrize("name", sorted(_VERBATIM))
def test_a_url_sent_as_written_keeps_its_query(name: str) -> None:
    url = "https://h/collector?channel=c1"

    _argument, build, _remedy = _VERBATIM[name]
    sink = build(url)

    assert sink.url == url
    assert urllib.request.Request(sink.url).selector == "/collector?channel=c1"  # noqa: S310


def test_the_wrappers_send_a_query_as_written() -> None:
    sink = LogstashSink(url="http://h/x?y=1")

    assert sink._http.url == "http://h/x?y=1"  # type: ignore[union-attr]


_DATADOG_SITES = [
    "datadoghq.com",
    "us3.datadoghq.com",
    "us5.datadoghq.com",
    "datadoghq.eu",
    "ap1.datadoghq.com",
    "ap2.datadoghq.com",
    "ddog-gov.com",
]
"""Every site Datadog's documentation names."""


@pytest.mark.parametrize("site", _DATADOG_SITES)
def test_every_datadog_site_reaches_its_intake(site: str) -> None:
    sink = DatadogSink("k", site=site)

    request = urllib.request.Request(sink.url)  # noqa: S310
    assert (request.host, request.selector) == (f"http-intake.logs.{site}", "/api/v2/logs")


@pytest.mark.parametrize(
    ("site", "host", "selector"),
    [
        ("datadoghq.com#", "http-intake.logs.datadoghq.com", ""),
        ("datadoghq.com?x", "http-intake.logs.datadoghq.com", "/?x/api/v2/logs"),
        ("datadoghq.com/x", "http-intake.logs.datadoghq.com", "/x/api/v2/logs"),
        ("datadoghq.com@evil.example", "http-intake.logs.datadoghq.com@evil.example", None),
        ("..", "http-intake.logs...", "/api/v2/logs"),
    ],
)
def test_a_site_moved_the_request(site: str, host: str, selector: str | None) -> None:
    """The premise, on the composition the class performed at 2e790e9: the request moved."""
    request = urllib.request.Request(f"https://http-intake.logs.{site}/api/v2/logs")

    assert request.host == host
    if selector is not None:
        assert request.selector == selector


@pytest.mark.parametrize(
    "site",
    [
        "",
        ".",
        "..",
        "%2e",
        "a..b",
        "-a.com",
        "a-.com",
        "datadoghq.com.",
        "datadoghq.com#",
        "datadoghq.com?x",
        "datadoghq.com/x",
        "us5/x.datadoghq.com",
        "datadoghq.com:443",
        "datadoghq.com\\x",
        "dat adog.com",
        " datadoghq.com",
        "b\u00fccher.com",
        "a" * 64 + ".com",
    ],
)
def test_a_site_that_is_not_a_host_name_is_refused_naming_site(site: str) -> None:
    with pytest.raises(ValueError, match=r"^DatadogSink site must be a Datadog site's host name"):
        DatadogSink("k", site=site)


_SECRETS = ("USERX", "PWX")
"""The user and password every userinfo case carries: tokens no refusal's own text contains."""

_FULLWIDTH_HASH = chr(0xFF03)
"""A character NFKC turns into ``#``, which ``urlsplit`` refuses in a netloc, quoting it."""

_FULLWIDTH_AT = chr(0xFF20)
"""A character NFKC turns into ``@``."""


def _assert_never_echoes(exc: BaseException, *secrets: str) -> None:
    """No secret in the exception or in anything reachable through ``__cause__``/``__context__``.

    A ``UnicodeError`` carries the whole string it failed on as ``object``, so that is read too.
    """
    pending: list[BaseException] = [exc]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        texts = [str(current), repr(current), repr(current.args)]
        if isinstance(current, UnicodeError):
            texts.append(repr(getattr(current, "object", "")))
        for secret in secrets:
            assert not any(secret in text for text in texts), (type(current).__name__, secret)
        pending.extend(link for link in (current.__cause__, current.__context__) if link)


_USERINFO = [
    "http://USERX:PWX@h:9200/x",
    "http://USERX:PWX@h/x",
    "http://USERX@h/x",
    "http://:PWX@h/x",
    "http://@h/x",
    "http://USERX\t:PWX@h/x",
    "http://USERX:PWX\n@h/x",
    "http:\t//USERX:PWX@h:9200/x",
    "http:/\t/USERX:PWX@h/x",
    "http:\n//USERX@h/x",
    "ftp://USERX:PWX@h/x",
    "\x00http://USERX:PWX@h/x",
    "<URL:http://USERX:PWX@h/x>",
    " http://USERX:PWX@h:9200/x ",
    "HTTPS://USERX:PWX@h/x",
    "http://USERX:PWX@[::1/x",
    "http://USERX:PWX@[zz]/x",
    "http://USERX:PWX@[::1]:9200/x",
    f"http://USERX:PWX@h{_FULLWIDTH_HASH}x/y",
    f"http://USERX:PWX{_FULLWIDTH_AT}h/x",
    "http://USERX:PWX@h:99999/x",
    "http://USERX:PWX@h/a b",
    "http://USERX:PWX@h\ud800/x",
    "//USERX:PWX@h/x",
    "\x00//USERX:PWX@h/x",
    "http:USERX:PWX@h",
    "http://USERX\\@h/x",
    "http://USERX%40h/x",
    "http://USERX%3APWX%40h:9200/x",
    "http://USERX%3APWX%40h/x",
    "http://USERX%3APWX%EF%BC%A0h/x",
    "http://h%2FUSERX%40x/",
]
"""URLs with a user or password before the host, as ``urlsplit`` or ``urllib`` reads it.

Measured at ``fda134a``, nine of these constructed: a port, bare and space-padded, a bare user,
a bare ``@``, a bracketed host with a port, a backslash user, and the three whose host holds an
``@`` only once percent-decoded (``USERX%40h``, ``USERX%3APWX%40h:9200``, ``h%2FUSERX%40x``).
Every other one was refused with the user or password in its message or its chain — among the
causes, ``http.client`` read ``PWX@h`` as a port, ``urllib`` found no host behind a tab
``urlsplit`` drops, the scheme was wrong, or ``urlsplit`` raised on a bracket or an NFKC character
and its reason quoted the netloc. The
percent-encoded forms are the host ``urllib`` connects to once it unquotes it (``Request.host``);
a fullwidth ``@`` is one under NFKC, as ``urlsplit`` reads it, encoded or not. ``h%2FUSERX%40x``
is decoded as the host alone, as ``urllib`` decodes it: decoding the whole URL first would end the
host at the ``/``.
"""


@pytest.mark.parametrize("url", _USERINFO)
def test_a_url_carrying_userinfo_is_refused_without_echoing_it(url: str) -> None:
    with pytest.raises(
        ValueError,
        match=r"^HTTPSink url must not contain a user or password \(user:password@host\): urllib ",
    ) as caught:
        HTTPSink(url)

    assert str(caught.value).endswith(f"authenticate with {_AUTH_REMEDY} instead")
    _assert_never_echoes(caught.value, *_SECRETS)
    _assert_chains_nothing(caught.value)
    _assert_printable(caught.value)


_USERINFO_PREFIXES = ["USERX:PWX@", "USERX@", "USERX%3APWX%40"]
"""Userinfo as a caller writes it into a URL: a pair, a user alone, and the pair percent-encoded."""


def _userinfo_builds(name: str) -> list[tuple[str, object, str]]:
    """Each way to put userinfo into the class's URL: the argument, a thunk, and the remedy."""
    builds: list[tuple[str, object, str]] = []
    for prefix in _USERINFO_PREFIXES:
        if name in _APPENDS:
            _suffix, build, remedy = _APPENDS[name]
            bases = (f"http://{prefix}h:9200", f"<URL:http://{prefix}h:9200>")
            builds.extend(("url", lambda b=base, f=build: f(b), remedy) for base in bases)
        elif name in _VERBATIM:
            argument, build, remedy = _VERBATIM[name]
            urls = (f"http://{prefix}h:9200/x", f"<URL:http://{prefix}h:9200/x>")
            builds.extend((argument, lambda u=url, f=build: f(u), remedy) for url in urls)
        else:
            _reason, build, remedy = _NO_CALLER_BASE[name]
            if build is not None:
                site = f"{prefix}datadoghq.com"
                builds.append(("site", lambda s=site, f=build: f(s), remedy))
    return builds


@pytest.mark.parametrize("cls", _shipped_http_subclasses(), ids=lambda cls: cls.__name__)
def test_every_http_subclass_refuses_userinfo_naming_itself_the_argument_and_the_remedy(
    cls: type[HTTPSink],
) -> None:
    """Swept over the derived roster, through the rosters above, so a new subclass is declared.

    A wrapped base (``<URL:…>``) is in the sweep because an appending class composes before
    ``_valid_url`` unwraps, and the composed string no longer unwraps: its userinfo is caught on
    the base as written, or the scheme refusal quoted it.
    """
    builds = _userinfo_builds(cls.__name__)
    if cls is NewRelicSink:
        assert builds == []
        assert NewRelicSink("k", region="eu").url == "https://log-api.eu.newrelic.com/log/v1"
        return
    assert builds, f"{cls.__name__} has no userinfo builder"
    for argument, build, remedy in builds:
        with pytest.raises(
            ValueError, match=rf"^{cls.__name__} {argument} must not contain a user or password"
        ) as caught:
            build()  # type: ignore[operator]

        assert str(caught.value).endswith(f"authenticate with {remedy} instead"), str(caught.value)
        _assert_never_echoes(caught.value, *_SECRETS)
        _assert_chains_nothing(caught.value)


def _wrapping_classes() -> set[str]:
    """Every class in the sinks package whose body calls a class of the ``HTTPSink`` family."""
    family = {"HTTPSink", *(cls.__name__ for cls in _shipped_http_subclasses())}
    found: set[str] = set()
    for path in pathlib.Path(log_foundry.sinks.__file__).parent.glob("*.py"):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if not isinstance(node, ast.ClassDef):
                continue
            for call in ast.walk(node):
                if not isinstance(call, ast.Call):
                    continue
                func = call.func
                called = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
                if called in family:
                    found.add(node.name)
    return found


_WRAPPERS = {
    "LogstashSink": ("url", lambda url: LogstashSink(url=url), _AUTH_REMEDY),
    "SentrySink": "a DSN's public key is its userinfo by Sentry's format, and the ingest URL "
    "it builds carries only the host after the last '@'",
}
"""Each class that wraps an HTTP sink rather than subclassing one, and how it treats userinfo."""


def test_every_http_wrapper_is_declared() -> None:
    """Derived from the source, so a new wrapper the subclass sweep cannot see fails here."""
    assert _wrapping_classes() == set(_WRAPPERS) == {"LogstashSink", "SentrySink"}


@pytest.mark.parametrize("prefix", _USERINFO_PREFIXES)
def test_a_wrapper_refuses_userinfo_under_its_own_name(prefix: str) -> None:
    argument, build, remedy = _WRAPPERS["LogstashSink"]

    with pytest.raises(
        ValueError, match=rf"^LogstashSink {argument} must not contain a user or password"
    ) as caught:
        build(f"http://{prefix}h:8080")

    assert str(caught.value).endswith(f"authenticate with {remedy} instead")
    _assert_never_echoes(caught.value, *_SECRETS)


def test_sentry_keeps_its_dsn_userinfo_out_of_the_url_it_posts_to() -> None:
    """The exemption: the key is userinfo by Sentry's format and never reaches the ingest URL."""
    sink = SentrySink(dsn="https://USERX:PWX@h/1", backend="http", opener=FakeOpener())

    assert sink._http.url == "https://h/api/1/envelope/"  # type: ignore[union-attr]


_NOT_USERINFO = [
    "http://h/a@b",
    "http://h/?x=a@b",
    "http://h#a@b",
    "http://h?x@y",
    "http://h/a%40b",
    "http://[::1]:80/a@b",
    "http://USERX%2540h/x",
    "http://h/?a=%40",
    "http://h#%40",
]
"""An ``@`` outside the host, and a ``%2540``, which ``urllib`` decodes once, to ``%40``.

None is userinfo, which is what the predicate is held to below; the literal ``@`` forms are refused
anyway, by the stray-``@`` check that follows the userinfo one (:data:`_STRAY_AT`).
"""

_PERCENT_AT = [url for url in _NOT_USERINFO if "@" not in url]
"""The forms with no literal ``@``: the ``%40`` remedy the stray-``@`` refusal names."""

_STRAY_AT_MESSAGE = r"must not hold an '@' outside its host: a password holding "
"""The stray-``@`` refusal's text after the owner and argument."""

_STRAY_AT_REMEDY = "write an '@' that belongs in a path or query as '%40'"
"""How the stray-``@`` refusal ends."""

_HOST_ENDED_EARLY = [
    ("http://elastic:12#ss@es:9200/x", "elastic:12"),
    ("http://elastic:12?ss@es:9200/x", "elastic:12"),
    ("http://elastic:12/ss@es:9200/x", "elastic:12"),
    ("http://elastic#ss@es:9200/x", "elastic"),
    ("http://elastic:#ss@es:9200/x", "elastic:"),
]
"""A password holding ``#``, ``?`` or ``/``, and the host ``urllib`` read instead of ``es:9200``."""


@pytest.mark.parametrize(("url", "host"), _HOST_ENDED_EARLY)
def test_a_password_holding_a_delimiter_sent_the_request_to_another_host(
    url: str, host: str
) -> None:
    """The premise, against the real stdlib: no userinfo is read, and the host is the wrong one."""
    request = urllib.request.Request(url)  # noqa: S310

    assert not _carries_userinfo(url)
    assert request.host == host
    assert urllib.parse.urlsplit(url).netloc == host


_STRAY_AT = [
    *(url for url, _host in _HOST_ENDED_EARLY),
    *(url for url in _NOT_USERINFO if "@" in url),
    "http:/USERX:PWX@h/1",
    "http:///USERX:PWX@h/1",
    "https:/USERX:PWX@h",
]
"""URLs whose every literal ``@`` is outside the host: refused by the stray-``@`` check alone.

The single- and triple-slash forms read no host before the ``@`` and were already refused unquoted
at ``253432f``; they reach the stray-``@`` refusal first now.
"""


@pytest.mark.parametrize("url", _STRAY_AT)
def test_an_at_sign_outside_the_host_is_refused_without_echoing_the_url(url: str) -> None:
    assert not _carries_userinfo(url), "the premise: no parser reads userinfo here"

    with pytest.raises(ValueError, match=rf"^HTTPSink url {_STRAY_AT_MESSAGE}") as caught:
        HTTPSink(url)

    assert str(caught.value).endswith(_STRAY_AT_REMEDY)
    _assert_never_echoes(caught.value, *_SECRETS, "PWY", "ss@es", "es:9200")
    _assert_chains_nothing(caught.value)


@pytest.mark.parametrize("url", _PERCENT_AT)
def test_an_at_sign_written_as_percent_40_still_constructs(url: str) -> None:
    assert HTTPSink(url).url == url


def test_an_at_sign_in_a_base_path_or_a_query_is_refused_and_its_percent_40_composes() -> None:
    with pytest.raises(ValueError, match=rf"^ElasticsearchSink url {_STRAY_AT_MESSAGE}"):
        ElasticsearchSink("http://h/a@b/", index="i")
    assert ElasticsearchSink("http://h/a%40b/", index="i").url == "http://h/a%40b/_bulk"
    for name in sorted(_VERBATIM):
        argument, build, _remedy = _VERBATIM[name]
        with pytest.raises(ValueError, match=rf"^{name} {argument} {_STRAY_AT_MESSAGE}"):
            build("https://h/collector?channel=a@b")
        assert build("https://h/c?channel=a%40b").url == "https://h/c?channel=a%40b"


_PARSED_BY_BOTH = [
    *_NOT_USERINFO,
    "http://USERX:PWX@h:9200/x",
    "http://USERX@h/x",
    "http://@h/x",
    "http://USERX\\@h/x",
    "http://USERX:PWX@[::1]:9200/x",
    "http://USERX%40h/x",
    "http://USERX%3APWX%40h:9200/x",
    "http://bücher.example/x",
    "http://h/x?q=%zz",
    "https://h/x#f",
    "http://h:+80/x",
]
"""URLs ``urlsplit`` and ``urllib`` both parse into a scheme and a host, which is where the
predicate has a ground truth to agree with; elsewhere it is refused anyway and only the message
changes. A fullwidth ``@`` is left out: it is userinfo only under NFKC, which neither host reads."""


@pytest.mark.parametrize("url", _PARSED_BY_BOTH)
def test_the_userinfo_predicate_reads_the_host_as_the_parsers_do(url: str) -> None:
    """The premise, against the stdlib on whichever Python runs it, not a copy of its rules."""
    _scheme, rest = urllib.parse._splittype(url)  # type: ignore[attr-defined]
    raw_host = urllib.parse._splithost(rest)[0] or ""  # type: ignore[attr-defined]
    truth = (
        "@" in urllib.parse.urlsplit(url).netloc
        or "@" in raw_host
        or "@" in (urllib.request.Request(url).host or "")  # noqa: S310
    )

    assert _carries_userinfo(url) is truth


def test_userinfo_was_sent_as_the_host_and_never_as_credentials() -> None:
    """The premise: ``urllib`` puts userinfo in the host it resolves, and nowhere else."""
    request = urllib.request.Request("http://USERX:PWX@h:9200/x")

    assert request.host == "USERX:PWX@h:9200"
    assert http.client.HTTPConnection(request.host).host == "USERX:PWX@h"


def test_userinfo_went_to_an_http_proxy_in_the_request_line_and_host_header() -> None:
    """The premise behind the release note: the proxy got the credentials in cleartext."""
    request = urllib.request.Request("http://USERX:PWX@h:9200/x")
    request.set_proxy("127.0.0.1:1", "http")
    handler = urllib.request.HTTPHandler()
    handler.add_parent(urllib.request.build_opener())

    handler.do_request_(request)

    assert request.selector == "http://USERX:PWX@h:9200/x"
    assert request.unredirected_hdrs["Host"] == "USERX:PWX@h:9200"
    assert "Authorization" not in {**request.headers, **request.unredirected_hdrs}


@pytest.mark.parametrize(
    "site", ["USERX:PWX@datadoghq.com", "USERX@datadoghq.com", "datadoghq.com@evil.example"]
)
def test_a_datadog_site_carrying_userinfo_is_refused_before_the_echoing_check(site: str) -> None:
    with pytest.raises(
        ValueError, match=r"^DatadogSink site must not contain a user or password"
    ) as caught:
        DatadogSink("k", site=site)

    _assert_never_echoes(caught.value, *_SECRETS, "evil")


_SENTRY_SECRETS = ("SECRETKEY", "SECRETPW")

_UNREADABLE = r"^SentrySink dsn is not a URL urlsplit can read: "

_SENTRY_REFUSALS = [
    ("https://SECRETKEY@h/", r"^SentrySink dsn is missing its project id$"),
    ("https://SECRETKEY:SECRETPW@/1", r"^SentrySink dsn is missing its host$"),
    ("SECRETKEY@h/1", r"^SentrySink dsn is missing its scheme, host, public key$"),
    ("https://SECRETKEY@[::1/1", _UNREADABLE),
    ("https://SECRETKEY@h:99999/1", _UNREADABLE),
    ("https://SECRETKEY@h:xx/1", _UNREADABLE),
    (f"https://SECRETKEY@h{_FULLWIDTH_HASH}x/1", _UNREADABLE),
    (f"https://SECRETKEY{_FULLWIDTH_HASH}x@h/1", _UNREADABLE),
    (f"https://SECRETKEY@h{_FULLWIDTH_AT}x/1", _UNREADABLE),
    ("https://SECRETKEY@[zz]/1", _UNREADABLE),
    ("https://[SECRETKEY]@h/1", _UNREADABLE),
    ("https://SECRETKEY@h/abc", r"^SentrySink dsn project id must be an integer"),
    ("https://SECRETKEY@h%40x/1", r"^SentrySink dsn host must not hold an '@'"),
    ("https://SECRETKEY@h%EF%BC%A0x/1", r"^SentrySink dsn host must not hold an '@'"),
    ("https://SECRETKEY@h h/1", r"^SentrySink dsn cannot be sent over HTTP "),
    ("https://SECRETKEY@h/a b/1", r"^SentrySink dsn cannot be sent over HTTP "),
    ("https://SECRETKEY@h\ud800/1", r"^SentrySink dsn cannot be sent over HTTP "),
    ("https://SECRETKEY€@h/1", r"^SentrySink dsn public key cannot be sent over HTTP "),
    ("https:/SECRETKEY@h/1", r"^SentrySink dsn is missing its host, public key$"),
    ("https://SECRETKEY@h:0/1", r"^SentrySink dsn names no usable port: 'https://h:0'; "),
]
"""Every refusal ``_parse_dsn`` raises, each reached by its own message, all carrying the key.

The ``missing`` refusal printed the DSN whole; ``urlsplit``'s reason, which the not-a-URL refusal
carried and chained, quotes the netloc for an NFKC character, and on some Python releases
(3.13.1, for one) the bracketed text for a bad bracket — the key, in either.
"""


@pytest.mark.parametrize(("dsn", "message"), _SENTRY_REFUSALS)
def test_no_sentry_dsn_refusal_echoes_the_key(dsn: str, message: str) -> None:
    with pytest.raises(ValueError, match=message) as caught:
        SentrySink(dsn=dsn, backend="http", opener=FakeOpener())

    _assert_never_echoes(caught.value, *_SENTRY_SECRETS)


_DELIMITED_PASSWORDS = [
    "http://USERX:PWX#PWY@h:9200/x",
    "http://USERX:PWX?PWY@h:9200/x",
    "http://USERX:PWX/PWY@h:9200/x",
    "http://USERX:0#PWY@h/x",
    "http://[USERX:PWX#PWY@h/x",
    "ftp://USERX:PWX#PWY@h/x",
    "\x00http://USERX:PWX#PWY@h/x",
    "http:///USERX:PWX#PWY@h",
]
"""A password holding ``#``, ``?`` or ``/``, which ends the authority before its ``@``.

Neither parser reads userinfo in these, so the userinfo refusal rightly stays quiet. Each once
reached a different later refusal, one per refusal ``_valid_url`` gives after the userinfo check,
which quoted the URL and chained a reason quoting part of it (``nonnumeric port: 'PWX'``); the
stray-``@`` refusal now reaches every one of them first.
"""


@pytest.mark.parametrize("url", _DELIMITED_PASSWORDS)
def test_a_password_holding_a_delimiter_is_refused_as_an_at_sign_outside_the_host(url: str) -> None:
    assert not _carries_userinfo(url), "the premise: no parser reads userinfo here"

    with pytest.raises(ValueError, match=rf"^HTTPSink url {_STRAY_AT_MESSAGE}") as caught:
        HTTPSink(url)

    assert str(caught.value).endswith(_STRAY_AT_REMEDY)
    _assert_never_echoes(caught.value, *_SECRETS, "PWY")
    _assert_chains_nothing(caught.value)


def test_the_parser_errors_a_refusal_chained_quoted_what_follows_the_host() -> None:
    """The premise, against the real ``http.client``: its reason quotes the request target."""
    with pytest.raises(http.client.InvalidURL) as invalid:
        _started_request("h", "POST", "/x y?token=SECRET")
    with pytest.raises(UnicodeEncodeError) as unencodable:
        _started_request("h", "POST", "/x?a=\udc80SECRET")

    assert "SECRET" in str(invalid.value)
    assert "SECRET" in unencodable.value.object


_AFTER_THE_HOST = [
    ("http://[::1/x?token=SECRET", r"is not a URL \(ValueError\)", "http://[::1"),
    ("ftp://h/x?token=SECRET", "must use http or https, not ftp scheme", "ftp://h"),
    ("http:///x?token=SECRET", "names no host", "http://"),
    ("http://h:abc/x?token=SECRET", r"cannot be sent over HTTP \(InvalidURL\)", "http://h:abc"),
    ("http://h:abc/services/SECRET", r"cannot be sent over HTTP \(InvalidURL\)", "http://h:abc"),
    ("http://h:abc/a%40SECRET", r"cannot be sent over HTTP \(InvalidURL\)", "http://h:abc"),
    ("http://h/x y?token=SECRET", r"cannot be sent over HTTP \(InvalidURL\)", "http://h"),
    ("http://h/x?a=\udc80SECRET", r"cannot be sent over HTTP \(UnicodeEncodeError\)", "http://h"),
    (
        f"http://h/a{_FULLWIDTH_AT}SECRET",
        r"cannot be sent over HTTP \(UnicodeEncodeError\)",
        "http://h",
    ),
    ("http://h:0/x#SECRET", "names no usable port", "http://h:0"),
    ("https://h:99999/x#access_token=SECRET", "names no usable port", "https://h:99999"),
]
"""A refused URL with a credential after its host — in a query, a fragment or a path — the reason
it is refused for, and the head the refusal quotes. One per refusal ``_valid_url`` gives after the
two ``@`` checks, and one per ``http.client`` route that put the rest in its reason: ``InvalidURL``'s
message and a ``UnicodeEncodeError``'s ``object``."""


@pytest.mark.parametrize(("url", "reason", "head"), _AFTER_THE_HOST)
def test_a_refused_url_quotes_only_its_scheme_and_host(url: str, reason: str, head: str) -> None:
    with pytest.raises(
        ValueError, match=rf"^HTTPSink url {reason}: {re.escape(repr(head))}; "
    ) as caught:
        HTTPSink(url)

    assert str(caught.value).endswith(_HEAD_ONLY)
    _assert_never_echoes(caught.value, "SECRET", "token", "services")
    _assert_chains_nothing(caught.value)
    _assert_printable(caught.value)


_NO_HEAD = [
    ("es.example.com?api_key=Splunk:SECRET", "must use http or https"),
    ("elastic%3ASECRET%40es:9200/x", "must use http or https"),
    ("elastic%3ASECRET%40es:9200", "must use http or https"),
    (f"u{_FULLWIDTH_AT}SECRET:9200/x", "must use http or https"),
    ("h?api_key=SECRET://x/y", "must use http or https"),
    ("h#SECRET://x/y", "must use http or https"),
    ("h%3ASECRET://x/y", "must use http or https"),
    ("\x00http://h/x?token=SECRET", "must use http or https"),
    ("http:SECRET?x", "names no host"),
    ("http:token=SECRET", "names no host"),
    ("http:\t//h/x?token=SECRET", "names no host"),
]
"""Refused URLs that do not start with ``scheme://``, so no host can be told from the rest.

The first two are ``_AUTHORITY``'s run quoting a secret: its scheme group, which mirrors
``urllib``'s ``_splittype``, runs to the first ``:`` over ``?``, ``#`` and ``%``. The ``://`` ones
are a quotable head only if the scheme may hold those, which RFC 3986's scheme characters do not.
"""


@pytest.mark.parametrize(("url", "reason"), _NO_HEAD)
def test_a_refused_url_with_no_head_is_not_quoted_at_all(url: str, reason: str) -> None:
    with pytest.raises(ValueError, match=rf"^HTTPSink url {reason}{re.escape(_UNQUOTED)}$") as e:
        HTTPSink(url)

    _assert_never_echoes(e.value, "SECRET", "token", "es:9200")
    _assert_chains_nothing(e.value)


@pytest.mark.parametrize(
    ("url", "head"),
    [
        ("http://h/x", "http://h"),
        ("HTTPS://h:9200?x", "HTTPS://h:9200"),
        ("svn+ssh.1-a://h#f", "svn+ssh.1-a://h"),
        ("http://", "http://"),
        ("http://u%40h/x", None),
        (f"http://u{_FULLWIDTH_AT}h/x", None),
        ("http://u@h/x", None),
        ("1http://h/x", None),
        ("http:/h/x", None),
        (" http://h/x", None),
    ],
)
def test_the_quotable_head_is_a_scheme_and_a_host_with_no_at_sign(
    url: str, head: str | None
) -> None:
    """Held directly, because ``_valid_url`` refuses every ``@`` in a host before any refusal."""
    assert _quotable_head(url) == head


@pytest.mark.parametrize(
    ("url", "message"),
    [
        ("http://h:abc", r"^HTTPSink url cannot be sent over HTTP \(nonnumeric port: 'abc'\): "),
        ("http://[::1", r"^HTTPSink url is not a URL \(Invalid IPv6 URL\): "),
    ],
)
def test_a_refused_url_that_is_all_head_is_still_quoted_and_chained(url: str, message: str) -> None:
    """The negative control: with nothing after the host the URL and the parser's reason are the
    useful part, and there is nothing past the host for either to leak."""
    with pytest.raises(ValueError, match=message) as caught:
        HTTPSink(url)

    assert str(caught.value).endswith(f": {url!r}")
    assert caught.value.__cause__ is not None


_UNQUOTED_SITE = "; the site is not quoted, since it holds an '@', '/', '?' or '#'"


@pytest.mark.parametrize(
    "site",
    [
        "x/USERX@y",
        "a#USERX@b",
        "x/USERX",
        "datadoghq.com?api_key=USERX",
        "datadoghq.com#USERX",
        "us5/USERX.datadoghq.com",
    ],
)
def test_a_datadog_site_holding_an_at_sign_or_a_delimiter_is_refused_unquoted(site: str) -> None:
    assert not _carries_userinfo(f"https://http-intake.logs.{site}/api/v2/logs"), "the premise"

    with pytest.raises(
        ValueError, match=r"^DatadogSink site must be a Datadog site's host name, such as "
    ) as caught:
        DatadogSink("k", site=site)

    assert "USERX" not in str(caught.value)
    assert str(caught.value).endswith(_UNQUOTED_SITE)


@pytest.mark.parametrize("site", ["dat adog.com", "datadoghq.com:443", "a..b"])
def test_a_datadog_site_with_no_at_sign_or_delimiter_is_still_quoted(site: str) -> None:
    with pytest.raises(ValueError, match=rf"{re.escape(repr(site))}$"):
        DatadogSink("k", site=site)


def _stray_at_builds(name: str) -> list[tuple[str, object]]:
    """Each way to put an ``@`` outside the host into the class's URL: the argument and a thunk."""
    if name in _APPENDS:
        _suffix, build, _remedy = _APPENDS[name]
        bases = ("http://h/a@b", "http://elastic:12/ss@es:9200")
        return [("url", lambda b=base, f=build: f(b)) for base in bases]
    if name in _VERBATIM:
        argument, build, _remedy = _VERBATIM[name]
        urls = ("http://h/a@b", "http://elastic:12#ss@es:9200/x", "http://h/x?channel=a@b")
        return [(argument, lambda u=url, f=build: f(u)) for url in urls]
    return []


@pytest.mark.parametrize("cls", _shipped_http_subclasses(), ids=lambda cls: cls.__name__)
def test_every_http_subclass_refuses_an_at_sign_outside_the_host_naming_the_argument(
    cls: type[HTTPSink],
) -> None:
    """Swept over the derived roster, through the rosters above, so a new subclass is declared.

    An appending class is given path forms: a ``?`` or ``#`` base is refused before the
    composed URL is checked, by the base check's own fixed text (the test below).
    """
    builds = _stray_at_builds(cls.__name__)
    if cls.__name__ in _NO_CALLER_BASE:
        assert builds == []
        return
    assert builds, f"{cls.__name__} has no stray-'@' builder"
    for argument, build in builds:
        with pytest.raises(
            ValueError, match=rf"^{cls.__name__} {argument} {_STRAY_AT_MESSAGE}"
        ) as caught:
            build()  # type: ignore[operator]

        assert str(caught.value).endswith(_STRAY_AT_REMEDY)
        _assert_never_echoes(caught.value, "ss@es", "a@b", "es:9200")
        _assert_chains_nothing(caught.value)


@pytest.mark.parametrize("base", ["http://elastic:12#ss@es:9200", "http://elastic:12?ss@es:9200"])
@pytest.mark.parametrize("name", sorted(_APPENDS))
def test_an_appended_base_with_a_stray_at_after_a_delimiter_is_refused_unquoted(
    name: str, base: str
) -> None:
    _suffix, build, _remedy = _APPENDS[name]

    with pytest.raises(
        ValueError,
        match=rf"^{name} url (cannot carry a query or fragment: |{_STRAY_AT_MESSAGE})",
    ) as caught:
        build(base)

    _assert_never_echoes(caught.value, "ss@es", "es:9200")
    _assert_chains_nothing(caught.value)


def test_the_wrapper_and_the_dataset_refuse_a_stray_at_under_their_own_names() -> None:
    with pytest.raises(ValueError, match=rf"^LogstashSink url {_STRAY_AT_MESSAGE}"):
        LogstashSink(url="http://elastic:12#ss@es:9200/x")
    with pytest.raises(ValueError, match=rf"^HoneycombSink dataset {_STRAY_AT_MESSAGE}"):
        HoneycombSink("k", "a@b")
    with pytest.raises(ValueError, match=r"^DatadogSink site must be a Datadog site's") as e:
        DatadogSink("k", site="a#USERX@b")
    assert "USERX" not in str(e.value)


def test_sentry_refuses_a_dsn_on_port_0_under_dsn_without_the_key() -> None:
    """``0`` was dropped as a falsy port, so the fallback posted to the default port, where the
    SDK posts to ``:0``, which never delivers; ``HTTPSink`` already refused port ``0``."""
    with pytest.raises(
        ValueError, match=r"^SentrySink dsn names no usable port: 'https://h:0'; "
    ) as caught:
        SentrySink(dsn="https://SECRETKEY@h:0/1", backend="http", opener=FakeOpener())

    _assert_never_echoes(caught.value, *_SENTRY_SECRETS)
    _assert_chains_nothing(caught.value)


_URLSPLIT_REMOVES = sorted(urllib.parse._UNSAFE_URL_BYTES_TO_REMOVE)  # type: ignore[attr-defined]
"""What ``urlsplit`` deletes before it parses, read from the stdlib itself."""


@pytest.mark.parametrize("char", _URLSPLIT_REMOVES)
def test_userinfo_behind_any_byte_urlsplit_deletes_is_refused(char: str) -> None:
    """Derived from the stdlib's own list, not from ``_URLSPLIT_DROPS``, so a byte it lost fails."""
    with pytest.raises(ValueError, match=r"^HTTPSink url must not contain a user or password") as e:
        HTTPSink(f"http:/{char}/USERX:PWX@h/x")

    _assert_never_echoes(e.value, *_SECRETS)
