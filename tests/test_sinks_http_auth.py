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

import ast
import pathlib
from collections import OrderedDict
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, ClassVar

import pytest

import log_foundry
from log_foundry.sinks.http import HTTPSink
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
"""Every whitespace character ``http.client`` let trail a header name, plus a run of them.

``_valid_headers`` refuses them all now; ``tests/test_sinks_http_header_names.py`` holds the
general rule and the sweep that no whitespace character is admitted in any position."""


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


class _Shifty(str):
    """A ``str`` subclass whose every method answers differently from the text it stores."""

    calls: ClassVar[list[str]] = []

    def __format__(self, spec: str) -> str:
        _Shifty.calls.append("__format__")
        return "tok€"

    def __str__(self) -> str:
        _Shifty.calls.append("__str__")
        return "HIJACK"

    def __contains__(self, item: object) -> bool:
        _Shifty.calls.append("__contains__")
        return False

    def encode(self, *args: Any, **kwargs: Any) -> bytes:  # type: ignore[override]
        _Shifty.calls.append("encode")
        return b"HIJACK"


@pytest.mark.parametrize(
    ("make", "sent"),
    [
        (lambda: _Shifty("x"), "Bearer x"),
        (lambda: (_Shifty("u"), _Shifty("p")), BASIC_U_P),
    ],
    ids=["token", "pair"],
)
def test_a_str_subclass_is_copied_as_the_text_it_stores_and_never_asked(make, sent) -> None:
    """At ``39a3f08c``: ``__format__`` ran at the check and on every request, free to change.

    The probe's subclass answered ``tok`` at the check and ``tok€`` after, so the sink constructed
    and then failed every ``emit``.
    """
    _Shifty.calls.clear()
    opener = FakeOpener()
    sink = HTTPSink(URL, auth=make(), opener=opener)

    assert _sent_authorization(sink, opener) == sent
    assert _sent_authorization(sink, opener) == sent
    assert _Shifty.calls == []
    stored = sink._auth if isinstance(sink._auth, tuple) else (sink._auth,)
    assert all(type(part) is str for part in stored)


def test_the_authorization_value_is_built_once_at_construction(monkeypatch) -> None:
    """At ``39a3f08c``: ``_apply_auth`` rebuilt it from the stored credentials on every request."""
    built: list[object] = []
    sink = HTTPSink(URL, auth=("u", "p"), opener=(opener := FakeOpener()))
    monkeypatch.setattr(
        "log_foundry.sinks.http._authorization", lambda auth: built.append(auth) or "Basic X"
    )

    assert _sent_authorization(sink, opener) == BASIC_U_P
    assert _sent_authorization(sink, opener) == BASIC_U_P
    assert built == []


_NOT_A_PAIR = "HTTPSink auth must be a str token or a pair of two str, not "


@pytest.mark.parametrize(
    ("auth", "named"),
    [
        ({"SECRET-USER", "SECRET-PASSWORD"}, "set"),
        (frozenset({"SECRET-USER", "SECRET-PASSWORD"}), "frozenset"),
        ({"SECRET-USER": "x", "SECRET-PASSWORD": "y"}, "dict"),
        (MappingProxyType({"SECRET-USER": "x", "SECRET-PASSWORD": "y"}), "mappingproxy"),
        (OrderedDict({"SECRET-USER": "x", "SECRET-PASSWORD": "y"}), "OrderedDict"),
        (b"to", "a pair of (int, int)"),
        ((b"SECRET-USER", b"SECRET-PASSWORD"), "a pair of (bytes, bytes)"),
        (("SECRET-USER", None), "a pair of (str, NoneType)"),
        ((None, "SECRET-PASSWORD"), "a pair of (NoneType, str)"),
        (["SECRET-USER", 7], "a pair of (str, int)"),
    ],
    ids=[
        "set",
        "frozenset",
        "dict",
        "mappingproxy",
        "ordereddict",
        "bytes",
        "bytes-pair",
        "none-password",
        "none-user",
        "int-password",
    ],
)
def test_a_pair_outside_two_str_is_refused_naming_only_types(auth, named: str) -> None:
    """At ``39a3f08c``: each constructed and sent credentials nobody wrote — a set in hash order,
    a mapping's keys, ``b"to"`` as ``116:111``, a ``bytes`` or ``None`` half as its ``repr``.
    """
    with pytest.raises(ValueError) as caught:
        HTTPSink(URL, auth=auth)

    assert str(caught.value) == _NOT_A_PAIR + named
    _assert_names_no_value(caught.value)
    _assert_chains_nothing(caught.value)


@pytest.mark.parametrize(
    "auth",
    [b"tok", {"a", "b", "c"}, {"a": 1}],
    ids=["bytes-of-three", "set-of-three", "dict-of-one"],
)
def test_a_container_of_the_wrong_length_keeps_the_unpack_refusal(auth) -> None:
    """The container is judged after the unpack, so these read exactly as they did."""
    with pytest.raises(ValueError) as caught:
        HTTPSink(URL, auth=auth)

    assert str(caught.value) == "HTTPSink auth cannot be sent over HTTP (ValueError)"


@pytest.mark.parametrize("name", sorted(_FORWARDERS))
def test_every_forwarder_refuses_a_set_pair(name: str) -> None:
    make, owner = _FORWARDERS[name]

    with pytest.raises(ValueError) as caught:
        make(auth={"SECRET-USER", "SECRET-PASSWORD"})

    assert str(caught.value) == _NOT_A_PAIR.replace("HTTPSink", owner, 1) + "set"


# --- per-request credentials ------------------------------------------------------------------


_SRC = pathlib.Path(log_foundry.__file__).parent


def _extra_header_sites(source: str, where: str) -> tuple[list[str], list[str]]:
    """Every ``extra_headers=`` keyword in ``source``: the sites, and the ones carrying credentials.

    A site whose value is not a ``dict`` literal with ``str`` keys cannot be judged, and is a
    finding too, so a refactor that computes the map is caught rather than waved through. A key is
    folded as ``urllib`` folds it, ``capitalize`` then ``title``, written out here.
    """
    sites: list[str] = []
    findings: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call):
            continue
        for keyword in node.keywords:
            if keyword.arg != "extra_headers":
                continue
            site = f"{where}:{keyword.value.lineno}"
            sites.append(site)
            if not isinstance(keyword.value, ast.Dict):
                findings.append(f"{site}: not a dict literal")
                continue
            for key in keyword.value.keys:
                if not (isinstance(key, ast.Constant) and isinstance(key.value, str)):
                    findings.append(f"{site}: a key that is not a str literal")
                elif key.value.capitalize().title() == "Authorization":
                    findings.append(f"{site}: {key.value!r} is an Authorization header")
    return sites, findings


def test_no_extra_headers_in_src_carries_an_authorization_header() -> None:
    """``extra_headers`` sits beneath the caller's own, and ``auth`` is written over it.

    A subclass passing ``Authorization`` there would be silently replaced by the caller's
    credentials, or would send its own beside a caller who never gave any. Today the one site is
    ``SentrySink``'s ``X-Sentry-Auth``; the floor keeps the scan from passing on nothing.
    """
    sites: list[str] = []
    findings: list[str] = []
    for path in sorted(_SRC.rglob("*.py")):
        found, bad = _extra_header_sites(path.read_text(), str(path.relative_to(_SRC)))
        sites += found
        findings += bad

    assert any(site.startswith("sinks/sentry.py:") for site in sites), sites
    assert findings == []


@pytest.mark.parametrize(
    ("source", "finding"),
    [
        ('f(extra_headers={"authorization": t})', "'authorization' is an Authorization header"),
        ('f(extra_headers={"AUTHORIZATION": t})', "'AUTHORIZATION' is an Authorization header"),
        ("f(extra_headers=build())", "not a dict literal"),
        ("f(extra_headers={name: t})", "a key that is not a str literal"),
    ],
    ids=["lower", "upper", "computed", "computed-key"],
)
def test_the_scan_names_each_site_it_cannot_pass(source: str, finding: str) -> None:
    sites, findings = _extra_header_sites(source, "x.py")

    assert sites == ["x.py:1"]
    assert findings == [f"x.py:1: {finding}"]


def test_the_scan_passes_a_per_request_header_that_is_not_authorization() -> None:
    sites, findings = _extra_header_sites('f(extra_headers={"X-Sentry-Auth": a})', "x.py")

    assert sites == ["x.py:1"]
    assert findings == []


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


@pytest.mark.parametrize(
    "given", [{"auth": BEARER}, {"auth": PAIR}, {}], ids=["token", "pair", "no"]
)
@pytest.mark.parametrize("trailing", TRAILING_WHITESPACE, ids=repr)
@pytest.mark.parametrize("header", ["Authorization", "authorization"])
def test_an_authorization_name_with_trailing_whitespace_is_refused_as_a_header_name(
    header, trailing, given
) -> None:
    """At ``7845f66f``: constructs, and ``http.client`` sent the name verbatim beside ``auth``.

    ``7226b9fe`` matched it as a conflict by stripping the name; the header refusal now runs
    first, with or without ``auth``, so the strip is gone and these meet the header's reason.
    """
    name = header + trailing
    with pytest.raises(ValueError) as caught:
        HTTPSink(URL, headers={name: HEADER_VALUE}, **given)

    assert str(caught.value) == f"HTTPSink header name must not contain whitespace: {name!r}"
    _assert_names_no_value(caught.value)
    _assert_chains_nothing(caught.value)


@pytest.mark.parametrize("trailing", TRAILING_WHITESPACE, ids=repr)
def test_a_name_only_containing_the_word_with_trailing_whitespace_is_refused_too(
    trailing,
) -> None:
    """``X-Authorization`` is no conflict, but with whitespace after it it is no header name."""
    name = "X-Authorization" + trailing
    with pytest.raises(ValueError) as caught:
        HTTPSink(URL, auth=("u", "p"), headers={name: "v"})

    assert str(caught.value) == f"HTTPSink header name must not contain whitespace: {name!r}"


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
