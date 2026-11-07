"""``auth`` is read once at construction, and is refused beside an ``Authorization`` header.

Two gaps in how ``HTTPSink`` held credentials. A pair that unpacks only once — ``iter(("u", "p"))``,
a generator — was unpacked by the construction check and again on every ``emit``, so the sink
constructed and then raised ``not enough values to unpack`` out of every batch: the pair is now
copied into a real ``tuple`` once, and every request is built from the copy. And ``auth`` beside an
``Authorization`` header constructed and silently sent only one of the two — the header when it was
spelled exactly ``Authorization``, the credentials under any other case, because ``urllib`` folds
the names and keeps the value written last — so for 1.2.0 that pairing is refused at construction,
naming the conflict and never a value (invariants 11 and 13).

Every shipped subclass is swept through its own constructor, found transitively and held to
``_RECIPES`` by the same rule as the render sweep, so a new subclass without a recipe fails here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from log_foundry.sinks.http import HTTPSink, _valid_headers
from log_foundry.sinks.logstash import LogstashSink
from log_foundry.sinks.splunk import SplunkHECSink
from test_sinks_http import FakeOpener
from test_sinks_http_render import _RECIPES, _shipped_http_subclasses

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

URL = "http://h/x"
BEARER = "SECRET-BEARER"
PAIR = ("SECRET-USER", "SECRET-PASSWORD")
HEADER_VALUE = "Bearer HEADER-VALUE"
BASIC_U_P = "Basic dTpw"
"""``base64("u:p")`` — what a ``("u", "p")`` pair sends, written out rather than computed."""

BESIDE_HEADER = (
    "auth and an Authorization header cannot both be given, since each sets the one "
    "Authorization header a request carries; drop auth= or the header"
)
"""The shared refusal's tail, written out rather than imported."""

SPLUNK = (
    "SplunkHECSink auth cannot be given, since this sink sends its token as the Authorization "
    "header; drop auth="
)

HEC_VALUE = "SECRET-HEC-VALUE"
"""The token the Splunk sink is built with here, so its refusal can be shown not to echo it."""

TRAILING_WHITESPACE = [" ", "\t", "\v", "\f", "\x1c", "\x1d", "\x1e", "\x1f", " \t "]
"""Every whitespace character ``_valid_headers`` lets trail a header name, plus a run of them.

``http.client`` refuses whitespace only as a name's first character, so these reach the wire;
``test_the_trailing_whitespace_roster_is_what_valid_headers_admits`` re-derives the set."""


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Neutralize the HTTP backoff so a failing case cannot stall the suite."""
    monkeypatch.setattr("log_foundry.sinks.http.wait", lambda _delay, _stop=None: None)


def _logstash(**kw: Any) -> LogstashSink:
    """``LogstashSink`` wraps an ``HTTPSink`` rather than subclassing one; its refusals name it."""
    return LogstashSink(url=URL, **kw)


_FORWARDERS: dict[str, tuple[Callable[..., Any], str]] = {
    **{name: (make, name) for name, make in _RECIPES.items() if name != "SplunkHECSink"},
    "HTTPSink": (lambda **kw: HTTPSink(URL, **kw), "HTTPSink"),
    "LogstashSink": (_logstash, "HTTPSink"),
}
"""Every class taking ``auth`` and ``headers`` together: a builder, and the owner its message names.

Splunk is apart because it writes ``Authorization`` itself. ``LogstashSink``'s owner is
``HTTPSink``, the sink it wraps, as for every HTTP-mode refusal it does not make first itself.
"""


def _sent_authorization(sink: Any, opener: FakeOpener) -> str:
    """Emits one event and returns the ``Authorization`` the request carried."""
    sink.emit([{"i": len(opener.calls)}])
    return opener.calls[-1]["headers"]["authorization"]


def _assert_chains_nothing(exc: BaseException) -> None:
    """A refusal about credentials chains nothing that could carry one."""
    assert exc.__cause__ is None
    assert exc.__context__ is None


def _assert_names_no_value(exc: BaseException) -> None:
    """Neither the credentials nor the header value appear anywhere in the message."""
    for secret in (BEARER, *PAIR, HEADER_VALUE, "HEADER-VALUE"):
        assert secret not in str(exc), str(exc)


def test_the_forwarder_roster_covers_every_shipped_subclass() -> None:
    """A subclass with no recipe would be silently missing from every sweep below."""
    shipped = {cls.__name__ for cls in _shipped_http_subclasses()}
    assert shipped - {"SplunkHECSink"} <= set(_FORWARDERS), shipped - set(_FORWARDERS)
    assert "SplunkHECSink" in shipped


# --- one read of auth -------------------------------------------------------------------------


def _pair_generator() -> Iterator[str]:
    """A ``(user, password)`` pair that can be iterated exactly once."""
    yield "u"
    yield "p"


@pytest.mark.parametrize(
    "make", [lambda: iter(("u", "p")), _pair_generator], ids=["iterator", "generator"]
)
def test_a_one_shot_pair_is_copied_once_and_sent_on_every_request(make) -> None:
    """At ``39a3f08c``: constructs, then ``not enough values to unpack`` out of every ``emit``."""
    opener = FakeOpener()
    sink = HTTPSink(URL, auth=make(), opener=opener)

    assert type(sink._auth) is tuple
    assert sink._auth == ("u", "p")
    assert _sent_authorization(sink, opener) == BASIC_U_P
    assert _sent_authorization(sink, opener) == BASIC_U_P, "the copy is read again, not consumed"


@pytest.mark.parametrize("name", sorted(_FORWARDERS))
def test_a_one_shot_pair_reaches_every_request_through_every_forwarder(name: str) -> None:
    make, _owner = _FORWARDERS[name]
    opener = FakeOpener()
    sink = make(auth=iter(("u", "p")), opener=opener)

    assert _sent_authorization(sink, opener) == BASIC_U_P
    assert _sent_authorization(sink, opener) == BASIC_U_P


def test_a_list_pair_is_copied_so_a_later_mutation_is_not_sent() -> None:
    """The stored pair is the copy, so the request is the pair as it was when constructed."""
    pair = ["u", "p"]
    opener = FakeOpener()
    sink = HTTPSink(URL, auth=pair, opener=opener)  # type: ignore[arg-type]
    pair[1] = "changed"

    assert _sent_authorization(sink, opener) == BASIC_U_P


def test_a_long_iterator_is_refused_having_read_no_more_than_the_unpack_does() -> None:
    """The copy is the unpack itself, which stops at the third item — never a ``tuple()`` drain.

    A copy built by ``tuple(auth)`` first would read all ten here, and hang on an endless one.
    """
    consumed: list[int] = []

    def longer() -> Iterator[str]:
        for index in range(10):
            consumed.append(index)
            yield str(index)

    with pytest.raises(ValueError) as caught:
        HTTPSink(URL, auth=longer())  # type: ignore[arg-type]

    assert str(caught.value) == "HTTPSink auth cannot be sent over HTTP (ValueError)"
    assert consumed == [0, 1, 2]
    _assert_chains_nothing(caught.value)


@pytest.mark.parametrize(
    ("auth", "reason"),
    [
        (123, "TypeError"),
        (("a", "b", "c"), "ValueError"),
        (("a",), "ValueError"),
        ((), "ValueError"),
    ],
    ids=["not-iterable", "three", "one", "empty"],
)
def test_a_value_that_is_not_a_pair_keeps_its_refusal(auth, reason: str) -> None:
    with pytest.raises(ValueError) as caught:
        HTTPSink(URL, auth=auth)

    assert str(caught.value) == f"HTTPSink auth cannot be sent over HTTP ({reason})"
    _assert_chains_nothing(caught.value)


def test_a_token_is_stored_as_given() -> None:
    sink = HTTPSink(URL, auth=BEARER)

    assert sink._auth is BEARER


# --- auth beside an Authorization header ------------------------------------------------------


_CASES = ["Authorization", "authorization", "AUTHORIZATION", "aUtHoRiZaTiOn"]
_AUTHS = pytest.mark.parametrize("auth", [BEARER, PAIR], ids=["token", "pair"])


@_AUTHS
@pytest.mark.parametrize("header", _CASES)
def test_auth_beside_an_authorization_header_is_refused_in_any_case(header, auth) -> None:
    """At ``39a3f08c``: constructs, and only one of the two is ever sent."""
    with pytest.raises(ValueError) as caught:
        HTTPSink(URL, auth=auth, headers={header: HEADER_VALUE})

    assert str(caught.value) == f"HTTPSink {BESIDE_HEADER}"
    _assert_names_no_value(caught.value)
    _assert_chains_nothing(caught.value)


@_AUTHS
@pytest.mark.parametrize("header", ["Authorization", "authorization"])
@pytest.mark.parametrize("name", sorted(_FORWARDERS))
def test_every_forwarder_refuses_auth_beside_an_authorization_header(name, header, auth) -> None:
    make, owner = _FORWARDERS[name]

    with pytest.raises(ValueError) as caught:
        make(auth=auth, headers={header: HEADER_VALUE})

    assert str(caught.value) == f"{owner} {BESIDE_HEADER}"
    _assert_names_no_value(caught.value)
    _assert_chains_nothing(caught.value)


@_AUTHS
@pytest.mark.parametrize(
    "headers", [None, {"X-Env": "prod"}, {"authorization": HEADER_VALUE}], ids=["alone", "x", "hdr"]
)
def test_splunk_refuses_any_auth_saying_it_sends_its_token_there(headers, auth) -> None:
    """At ``39a3f08c``: constructs and drops ``auth``, sending only ``Splunk <token>``."""
    kw: dict[str, Any] = {"auth": auth}
    if headers is not None:
        kw["headers"] = headers

    with pytest.raises(ValueError) as caught:
        SplunkHECSink(URL, HEC_VALUE, **kw)

    assert str(caught.value) == SPLUNK
    assert "passed" not in str(caught.value) and "or the header" not in str(caught.value)
    _assert_names_no_value(caught.value)
    assert HEC_VALUE not in str(caught.value), "the sink's own token is never echoed"
    _assert_chains_nothing(caught.value)


@_AUTHS
@pytest.mark.parametrize("trailing", TRAILING_WHITESPACE, ids=repr)
@pytest.mark.parametrize("header", ["Authorization", "authorization"])
def test_auth_beside_an_authorization_header_with_trailing_whitespace_is_refused(
    header, trailing, auth
) -> None:
    """At ``7845f66f``: constructs, and ``http.client`` sent both headers on one request."""
    with pytest.raises(ValueError) as caught:
        HTTPSink(URL, auth=auth, headers={header + trailing: HEADER_VALUE})

    assert str(caught.value) == f"HTTPSink {BESIDE_HEADER}"
    _assert_names_no_value(caught.value)
    _assert_chains_nothing(caught.value)


@pytest.mark.parametrize("trailing", TRAILING_WHITESPACE, ids=repr)
def test_a_header_only_containing_the_word_is_not_matched_with_trailing_whitespace(
    trailing,
) -> None:
    opener = FakeOpener()
    sink = HTTPSink(
        URL, auth=("u", "p"), headers={"X-Authorization" + trailing: "v"}, opener=opener
    )

    assert _sent_authorization(sink, opener) == BASIC_U_P


def test_the_trailing_whitespace_roster_is_what_valid_headers_admits() -> None:
    """Re-derives the roster above, so a ``_valid_headers`` that admits more fails here.

    Only whitespace by ``str.isspace`` is swept: the conflict check strips exactly that, and any
    other character is a different name to a server, not a spelling of ``Authorization``.
    """
    admitted = set()
    for code in range(0x110000):
        char = chr(code)
        if not char.isspace():
            continue
        try:
            _valid_headers({"Authorization" + char: "v"}, "P")
        except ValueError:
            continue
        admitted.add(char)

    assert admitted == {item for item in TRAILING_WHITESPACE if len(item) == 1}


def test_an_unsendable_token_beside_the_header_is_refused_by_the_conflict_in_either_case() -> None:
    """Retires "credentials an ``Authorization`` header overrides are not checked".

    At ``39a3f08c`` an exact-case header made the sink skip the token's check and construct, and a
    lowercase one made it refuse the token's value instead. With the conflict refused there is no
    overridden credential left to skip; the conflict is judged first, reading no credential.
    """
    for header in ("Authorization", "authorization"):
        with pytest.raises(ValueError) as caught:
            HTTPSink(URL, headers={header: "x"}, auth="€")
        assert str(caught.value) == f"HTTPSink {BESIDE_HEADER}"
        _assert_chains_nothing(caught.value)


# --- what stays silent ------------------------------------------------------------------------


@_AUTHS
@pytest.mark.parametrize(
    "headers",
    [None, {}, {"X-Env": "prod"}, {"X-Authorization": "v"}, {"Proxy-Authorization": "v"}],
    ids=["none", "empty", "other", "x-authorization", "proxy-authorization"],
)
def test_auth_without_an_authorization_header_constructs_and_is_sent(headers, auth) -> None:
    opener = FakeOpener()
    sink = HTTPSink(URL, auth=auth, headers=headers, opener=opener)
    sink.emit([{"i": 1}])

    sent = opener.calls[0]["headers"]
    assert sent["authorization"].startswith("Bearer " if isinstance(auth, str) else "Basic ")
    for name, value in (headers or {}).items():
        assert sent[name.lower()] == value


@pytest.mark.parametrize("name", sorted(_FORWARDERS))
def test_every_forwarder_sends_auth_beside_a_header_that_only_contains_the_word(name) -> None:
    make, _owner = _FORWARDERS[name]
    opener = FakeOpener()
    sink = make(auth=("u", "p"), headers={"X-Authorization": "v"}, opener=opener)

    assert _sent_authorization(sink, opener) == BASIC_U_P
    assert opener.calls[-1]["headers"]["x-authorization"] == "v"


@pytest.mark.parametrize("header", _CASES)
@pytest.mark.parametrize("given", [{}, {"auth": None}], ids=["omitted", "none"])
def test_an_authorization_header_without_auth_is_what_is_sent(given, header) -> None:
    opener = FakeOpener()
    sink = HTTPSink(URL, headers={header: HEADER_VALUE}, opener=opener, **given)

    assert _sent_authorization(sink, opener) == HEADER_VALUE


def test_auth_is_applied_above_a_per_request_authorization() -> None:
    """``extra_headers`` sits beneath the caller's own (SPEC-048 FR-007), and ``auth`` is theirs.

    At ``39a3f08c`` ``_apply_auth`` deferred to any ``Authorization`` already in the map, so a
    per-request one silently replaced the caller's credentials. No shipped subclass passes one.
    """
    sink = HTTPSink(URL, auth=("u", "p"))

    headers, _body = sink._prepare(b"{}", "application/json", {"Authorization": "Bearer extra"})

    assert headers["Authorization"] == BASIC_U_P
