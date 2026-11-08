"""A header name holding whitespace anywhere is refused at construction (1.2.0).

``http.client`` refuses whitespace only as a name's first character — and its pattern's ``\\s`` is
the ASCII six, so a leading ``0x1C``–``0x1F`` passed too — and sends any other name verbatim:
``Authorization : V`` went on the wire. RFC 9112 §5.1 has a server answer whitespace between a
field name and its colon with ``400``, so every such sink was already broken against a conforming
server, failing on every batch (invariant 13), and a lenient one might trim the name into a header
the caller never wrote. The refusal names the header and never its value (invariant 11). A name
that ``http.client``, the CR/LF check or the ASCII encode already refused keeps its message.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.logstash import LogstashSink
from test_sinks_http import FakeOpener
from test_sinks_http_render import _RECIPES, _shipped_http_subclasses

if TYPE_CHECKING:
    from collections.abc import Callable

URL = "http://h/x"
HEADER_VALUE = "SECRET-HEADER-VALUE"
WHITESPACE = [chr(code) for code in range(0x110000) if chr(code).isspace()]
"""Every character ``str.isspace`` is true for — the population the rule covers, derived."""

_POSITIONS = {
    "leading": lambda char: f"{char}X-Foo",
    "trailing": lambda char: f"X-Foo{char}",
    "internal": lambda char: f"X{char}Foo",
}

NEWLY_REFUSED = [
    *((char, position) for char in " \t\v\f" for position in ("trailing", "internal")),
    *((char, position) for char in "\x1c\x1d\x1e\x1f" for position in _POSITIONS),
]
"""Every whitespace name ``39a3f08c`` constructed, written out rather than derived from the code.

Space, tab, ``VT`` and ``FF`` were refused by ``http.client`` only in first position; the four
ASCII separators were not refused in any, since its pattern's ``\\s`` does not include them.
"""


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Neutralize the HTTP backoff so a failing case cannot stall the suite."""
    monkeypatch.setattr("log_foundry.sinks.http.wait", lambda _delay, _stop=None: None)


def _logstash(**kw: Any) -> LogstashSink:
    """``LogstashSink`` builds an ``HTTPSink`` from its headers; the refusal names that sink."""
    return LogstashSink(url=URL, **kw)


_BUILDERS: dict[str, tuple[Callable[..., Any], str]] = {
    **{name: (make, name) for name, make in _RECIPES.items()},
    "HTTPSink": (lambda **kw: HTTPSink(URL, **kw), "HTTPSink"),
    "LogstashSink": (_logstash, "HTTPSink"),
}
"""Every class taking ``headers``: a builder, and the owner its refusal names.

The platform sinks among them merge the caller's headers with their own (``merge_headers``) before
``HTTPSink`` judges them, so the caller's name reaches the same check.
"""


def _refused(name: str) -> str:
    """The new refusal for ``name``, written out."""
    return f"HTTPSink header name must not contain whitespace: {name!r}"


def _assert_chains_nothing(exc: BaseException) -> None:
    """A refusal about a header chains nothing that could carry its value."""
    assert exc.__cause__ is None
    assert exc.__context__ is None


def test_the_builder_roster_covers_every_shipped_subclass() -> None:
    shipped = {cls.__name__ for cls in _shipped_http_subclasses()}
    assert shipped <= set(_BUILDERS), shipped - set(_BUILDERS)


def test_the_whitespace_population_is_the_one_the_rule_names() -> None:
    """A floor, so the sweep below cannot pass by sweeping nothing."""
    assert {" ", "\t", "\v", "\f", "\x1c", "\x1d", "\x1e", "\x1f", "\xa0", "　"} <= set(WHITESPACE)


@pytest.mark.parametrize(("char", "position"), NEWLY_REFUSED, ids=repr)
def test_a_name_that_used_to_construct_is_refused_naming_it(char: str, position: str) -> None:
    """At ``39a3f08c``: constructs, and the name goes on the wire verbatim."""
    name = _POSITIONS[position](char)

    with pytest.raises(ValueError) as caught:
        HTTPSink(URL, headers={name: HEADER_VALUE})

    assert str(caught.value) == _refused(name)
    assert HEADER_VALUE not in str(caught.value)
    _assert_chains_nothing(caught.value)


@pytest.mark.parametrize(
    "name", ["X-Foo ", "X\tFoo", "\x1cX-Foo"], ids=["trailing-space", "internal-tab", "leading-fs"]
)
@pytest.mark.parametrize("cls", sorted(_BUILDERS))
def test_every_class_taking_headers_refuses_a_whitespace_name(cls: str, name: str) -> None:
    make, owner = _BUILDERS[cls]

    with pytest.raises(ValueError) as caught:
        make(headers={name: HEADER_VALUE})

    assert str(caught.value) == _refused(name).replace("HTTPSink", owner, 1)
    assert HEADER_VALUE not in str(caught.value)
    _assert_chains_nothing(caught.value)


def test_no_whitespace_character_is_admitted_in_any_position() -> None:
    """The silent-failure guard: every ``isspace`` character, leading, trailing and internal.

    At ``39a3f08c`` it admits twenty names (``NEWLY_REFUSED``). Every refusal names the header and
    neither echoes its value nor chains anything, whichever check made it.
    """
    admitted = []
    for char in WHITESPACE:
        for position, build in _POSITIONS.items():
            name = build(char)
            try:
                HTTPSink(URL, headers={name: HEADER_VALUE})
            except ValueError as err:
                assert str(err).endswith(repr(name)), str(err)
                assert HEADER_VALUE not in str(err)
                _assert_chains_nothing(err)
                continue
            admitted.append((hex(ord(char)), position))

    assert admitted == []


@pytest.mark.parametrize(
    ("name", "message"),
    [
        (" X-Foo", "HTTPSink header cannot be sent over HTTP (ValueError): ' X-Foo'"),
        ("\tX-Foo", "HTTPSink header cannot be sent over HTTP (ValueError): '\\tX-Foo'"),
        ("X\xa0Foo", "HTTPSink header cannot be sent over HTTP (UnicodeEncodeError): 'X\\xa0Foo'"),
        ("X-Foo\n", "HTTPSink header name must not contain CR or LF: 'X-Foo\\n'"),
    ],
    ids=["leading-space", "leading-tab", "nbsp", "lf"],
)
def test_a_name_already_refused_keeps_its_message(name: str, message: str) -> None:
    """The whitespace check runs after the others, so these read exactly as they did."""
    with pytest.raises(ValueError) as caught:
        HTTPSink(URL, headers={name: HEADER_VALUE})

    assert str(caught.value) == message


@pytest.mark.parametrize("name", ["X-Foo", "X-Authorization", "X_Foo", "x-foo-bar", "X-Foo2"])
def test_a_token_name_constructs_and_is_sent(name: str) -> None:
    opener = FakeOpener()
    HTTPSink(URL, headers={name: "v"}, opener=opener).emit([{"i": 1}])

    assert opener.calls[0]["headers"][name.lower()] == "v"


@pytest.mark.parametrize("headers", [None, {}], ids=["none", "empty"])
def test_no_headers_construct(headers) -> None:
    assert HTTPSink(URL, headers=headers)


@pytest.mark.parametrize("cls", sorted(_BUILDERS))
def test_a_value_holding_spaces_is_still_sent_by_every_class(cls: str) -> None:
    make, _owner = _BUILDERS[cls]
    opener = FakeOpener()
    make(headers={"X-Env": "Bearer abc def"}, opener=opener).emit([{"i": 1}])

    assert opener.calls[0]["headers"]["x-env"] == "Bearer abc def"
