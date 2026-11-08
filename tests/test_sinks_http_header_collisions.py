"""A caller header never silently replaces another header: refused at construction (1.2.0).

``urllib`` sends one value per folded header name, the last written, so two gaps let one header
silently replace another. A platform sink laid the caller's ``headers=`` over its own credential
header (``merge_headers``), so ``DatadogSink(api_key=k, headers={"dd-api-key": x})`` sent ``x`` and
never ``api_key`` — Splunk's ``Authorization``, New Relic's ``Api-Key`` and Honeycomb's
``X-Honeycomb-Team`` likewise, in any spelling. And two caller names folding to one,
``{"X-Foo": "1", "x-foo": "2"}``, sent only the last. Both are refused, naming the headers and
never a value (invariants 11 and 13); a caller overriding a non-credential default such as
``Content-Type`` still wins (FR-002).

Each platform sink's credential headers are derived from its own code — every key of the map it
hands ``merge_headers`` — and a roster scan fails naming any sink that sets a header of its own by
a route the rule does not see.
"""

from __future__ import annotations

import ast
import inspect
import pathlib
import re
import socket
import threading
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry.sinks
from log_foundry.sinks.datadog import DatadogSink
from log_foundry.sinks.honeycomb import HoneycombSink
from log_foundry.sinks.http import HTTPSink, merge_headers
from log_foundry.sinks.logstash import LogstashSink
from log_foundry.sinks.newrelic import NewRelicSink
from log_foundry.sinks.sentry import SentrySink
from log_foundry.sinks.splunk import SplunkHECSink
from test_sinks_http import FakeOpener
from test_sinks_http_render import _RECIPES

if TYPE_CHECKING:
    from collections.abc import Callable

URL = "http://h/x"
CALLER_VALUE = "SECRET-CALLER-VALUE"
OWN_VALUE = "SECRET-OWN-CREDENTIAL"
"""The platform sink's own token or key, so a refusal can be shown never to echo it."""
_SINKS = pathlib.Path(log_foundry.sinks.__file__).parent


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Neutralize the HTTP backoff so a failing case cannot stall the suite."""
    monkeypatch.setattr("log_foundry.sinks._retry.wait", lambda _delay, _stop=None: None)


def _enclosing_classes(tree: ast.Module) -> dict[ast.AST, str]:
    """Maps every node inside a class body to that class's name."""
    owner: dict[ast.AST, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            for inner in ast.walk(node):
                owner.setdefault(inner, node.name)
    return owner


def _merge_sites(source: str) -> dict[str, tuple[set[str], str]]:
    """Every ``merge_headers`` call in ``source``: class -> (its own header names, the argument).

    Derived from the code: the keys of the ``dict`` literal handed as ``base``, and the
    ``argument=`` it names. A ``base`` that is not a literal is reported by the roster scan.
    """
    tree = ast.parse(source)
    owner = _enclosing_classes(tree)
    found: dict[str, tuple[set[str], str]] = {}
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and getattr(node.func, "id", None) == "merge_headers"):
            continue
        base = node.args[0] if node.args else None
        keys = (
            {key.value for key in base.keys if isinstance(key, ast.Constant)}
            if isinstance(base, ast.Dict)
            else set()
        )
        argument = next(
            (
                kw.value.value
                for kw in node.keywords
                if kw.arg == "argument" and isinstance(kw.value, ast.Constant)
            ),
            "",
        )
        found[owner.get(node, "<module>")] = (keys, argument)
    return found


def _derived_credentials() -> dict[str, tuple[set[str], str]]:
    """Every shipped platform sink's own credential headers, read from its module."""
    derived: dict[str, tuple[set[str], str]] = {}
    for path in sorted(_SINKS.glob("*.py")):
        if path.name != "http.py":
            derived.update(_merge_sites(path.read_text()))
    return derived


_CREDENTIALS = _derived_credentials()
_COLLISIONS = [
    (cls, own, spelled, argument)
    for cls, (names, argument) in sorted(_CREDENTIALS.items())
    for own in sorted(names)
    for spelled in (own, own.lower(), own.upper())
]


def test_the_derived_credential_headers_are_the_four_platform_sinks() -> None:
    """A floor on the derivation, written out, so a scan that finds nothing cannot pass."""
    expected = {
        "DatadogSink": ({"DD-API-KEY"}, "api_key"),
        "HoneycombSink": ({"X-Honeycomb-Team"}, "api_key"),
        "NewRelicSink": ({"Api-Key"}, "api_key"),
        "SplunkHECSink": ({"Authorization"}, "token"),
    }
    assert expected == _CREDENTIALS
    assert set(_WITH_CREDENTIAL) == set(_CREDENTIALS), "a platform sink has no builder below"


_WITH_CREDENTIAL: dict[str, Callable[..., Any]] = {
    "DatadogSink": lambda **kw: DatadogSink(OWN_VALUE, **kw),
    "HoneycombSink": lambda **kw: HoneycombSink(OWN_VALUE, "d", **kw),
    "NewRelicSink": lambda **kw: NewRelicSink(OWN_VALUE, **kw),
    "SplunkHECSink": lambda **kw: SplunkHECSink(URL, OWN_VALUE, **kw),
}
"""Each platform sink built with ``OWN_VALUE`` as its credential; held to the derived set above."""


@pytest.mark.parametrize(("cls", "own", "spelled", "argument"), _COLLISIONS, ids=str)
def test_a_caller_header_naming_the_sinks_credential_header_is_refused(
    cls: str, own: str, spelled: str, argument: str
) -> None:
    """At ``39e365f5``: constructs, and the caller's value is sent instead of the argument."""
    with pytest.raises(ValueError) as caught:
        _WITH_CREDENTIAL[cls](headers={"X-Extra": "1", spelled: CALLER_VALUE})

    assert str(caught.value) == (
        f"{cls} header {spelled!r} collides with {argument}, which this sink sends as {own}; "
        "drop the header"
    )
    assert CALLER_VALUE not in str(caught.value)
    assert OWN_VALUE not in str(caught.value), "the sink's own credential is never echoed"
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


@pytest.mark.parametrize("cls", sorted(_CREDENTIALS))
def test_the_collision_is_refused_before_the_duplicate_name_check(cls: str) -> None:
    """Two spellings of the sink's own header get the credential message, not the generic one."""
    [own] = _CREDENTIALS[cls][0]

    with pytest.raises(ValueError) as caught:
        _WITH_CREDENTIAL[cls](headers={own.lower(): "1", own.upper(): "2"})

    assert str(caught.value).startswith(
        f"{cls} header {own.lower()!r} collides with {_CREDENTIALS[cls][1]}, "
    )


@pytest.mark.parametrize("cls", sorted(_CREDENTIALS))
def test_a_non_credential_caller_header_is_sent_beside_the_sinks_own(cls: str) -> None:
    """``X-Extra``, a ``Content-Type`` override (FR-002) and a name that only starts like the
    sink's own all construct, and the caller's values are what is sent."""
    [own] = _CREDENTIALS[cls][0]
    headers = {"X-Extra": "1", "Content-Type": "text/plain", f"{own}-2": "2"}
    opener = FakeOpener()

    _WITH_CREDENTIAL[cls](headers=headers, opener=opener).emit([{"i": 1}])

    sent = opener.calls[0]["headers"]
    assert sent["x-extra"] == "1"
    assert sent["content-type"] == "text/plain"
    assert sent[f"{own}-2".lower()] == "2"
    assert OWN_VALUE in sent[own.lower()], "the sink's own credential is still what is sent"


def test_sentrysink_takes_no_caller_headers_so_its_own_cannot_be_replaced() -> None:
    """``X-Sentry-Auth`` goes per request through ``extra_headers``; no caller header reaches it."""
    parameters = inspect.signature(SentrySink.__init__).parameters
    assert "headers" not in parameters
    assert not any(p.kind is inspect.Parameter.VAR_KEYWORD for p in parameters.values())


# --- the roster: every route a sink sets a header of its own -----------------------------------


def _header_routes(source: str, where: str) -> list[str]:
    """Every site in ``source`` that sets a header by a route ``merge_headers`` does not see.

    Seen: a ``headers=`` handed to ``HTTPSink`` built by ``merge_headers`` from a ``dict``
    literal, and ``extra_headers=`` in a class that takes no caller headers at all. Not seen, and
    reported: a ``merge_headers`` base that is not a literal; ``headers=`` built any other way; an
    ``extra_headers=`` in a class whose constructor takes ``headers`` or ``**kwargs``; an override
    of ``_prepare``, ``_apply_auth`` or ``_send``; a subscript write to, or an ``.update`` or
    ``.setdefault`` call on, a name or attribute holding ``headers`` (``self._headers``), wherever
    it sits — after ``super().__init__``, under a condition, in any method; and a write of a
    ``"headers"`` key, ``http_kwargs["headers"] = ...``.
    """
    tree = ast.parse(source)
    owner = _enclosing_classes(tree)
    merged: set[str] = set()
    findings: list[str] = []
    inits: dict[str, ast.FunctionDef] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name == "__init__":
                    inits[node.name] = item
                if isinstance(item, ast.FunctionDef) and item.name in {
                    "_prepare",
                    "_apply_auth",
                    "_send",
                }:
                    findings.append(f"{where}:{item.lineno}: {node.name} overrides {item.name}")
        if (
            isinstance(node, ast.Assign)
            and isinstance(node.value, ast.Call)
            and getattr(node.value.func, "id", None) == "merge_headers"
        ):
            if not (node.value.args and isinstance(node.value.args[0], ast.Dict)):
                findings.append(f"{where}:{node.lineno}: merge_headers base is not a literal")
            merged.update(t.id for t in node.targets if isinstance(t, ast.Name))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            for kw in node.keywords:
                if kw.arg == "headers" and not (
                    isinstance(kw.value, ast.Name) and kw.value.id in merged
                ):
                    findings.append(f"{where}:{kw.value.lineno}: headers= not from merge_headers")
                if kw.arg == "extra_headers":
                    cls = owner.get(node, "<module>")
                    init = inits.get(cls)
                    takes = init is not None and (
                        init.args.kwarg is not None
                        or any(a.arg == "headers" for a in init.args.args + init.args.kwonlyargs)
                    )
                    if init is None or takes:
                        findings.append(
                            f"{where}:{kw.value.lineno}: {cls} sends extra_headers and takes "
                            "caller headers"
                        )
        targets = (
            node.targets
            if isinstance(node, ast.Assign)
            else [node.target]
            if isinstance(node, ast.AugAssign)
            else []
        )
        for target in targets:
            if not isinstance(target, ast.Subscript):
                continue
            mapping = _headers_mapping(target.value)
            key = target.slice
            if mapping is not None:
                findings.append(f"{where}:{node.lineno}: writes {mapping}[...]")
            elif isinstance(key, ast.Constant) and key.value == "headers":
                findings.append(f"{where}:{node.lineno}: writes [{key.value!r}]")
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in {"update", "setdefault"}
        ):
            mapping = _headers_mapping(node.func.value)
            if mapping is not None:
                findings.append(f"{where}:{node.lineno}: calls {mapping}.{node.func.attr}")
    return findings


def _headers_mapping(node: ast.expr) -> str | None:
    """Names an expression read as a headers mapping (``self._headers``), or returns ``None``."""
    if isinstance(node, ast.Name) and "headers" in node.id:
        return node.id
    if isinstance(node, ast.Attribute) and "headers" in node.attr:
        return ast.unparse(node)
    return None


def test_no_shipped_sink_sets_a_header_by_a_route_the_rule_does_not_see() -> None:
    """Supplementary to the runtime roster in ``tests/test_sinks_http_header_roster.py``.

    That roster is the guard: it reads what each sink actually sends and tries to replace it, so
    it sees routes this scan cannot name — ``self._headers[...]`` after ``super().__init__``,
    ``.update``, a ``**{"headers": ...}`` splat, an ``http_kwargs["headers"]`` write, an
    ``_attempt`` override, each measured missed here. It sees only what one emit of one event
    exercises, though, and this scan reads code on every path, so the two are kept together;
    neither covers the other.
    """
    findings: list[str] = []
    scanned = 0
    for path in sorted(_SINKS.glob("*.py")):
        if path.name == "http.py":
            continue
        source = path.read_text()
        if "HTTPSink" not in source:
            continue
        scanned += 1
        findings += _header_routes(source, path.name)

    assert scanned >= 10, scanned
    assert findings == []


_PLATFORM = """
class P(HTTPSink):
    def __init__(self, key, **http_kwargs):
        headers = merge_headers({"X-Key": key}, http_kwargs, argument="key")
        super().__init__("u", headers=headers, **http_kwargs)
"""


@pytest.mark.parametrize(
    ("source", "finding"),
    [
        (
            _PLATFORM.replace('{"X-Key": key}', "own"),
            "merge_headers base is not a literal",
        ),
        (
            (
                "class P(HTTPSink):\n    def __init__(self, key):\n"
                "        super().__init__('u', headers={'X-Key': key})\n"
            ),
            "headers= not from merge_headers",
        ),
        (
            (
                "class P:\n    def __init__(self, headers=None):\n        pass\n"
                "    def emit(self, b):\n        self._http._send(b, extra_headers={'X-K': 1})\n"
            ),
            "P sends extra_headers and takes caller headers",
        ),
        (
            "class P(HTTPSink):\n    def _prepare(self, body, ct, extra):\n        pass\n",
            "P overrides _prepare",
        ),
        (
            (
                "class P(HTTPSink):\n    def _apply_auth(self, headers):\n"
                "        headers['X-Key'] = 'k'\n"
            ),
            "P overrides _apply_auth",
        ),
    ],
    ids=["computed-base", "literal-headers", "extra-with-caller", "prepare", "apply-auth"],
)
def test_the_roster_names_each_route_it_does_not_see(source: str, finding: str) -> None:
    findings = _header_routes(source, "x.py")

    assert any(item.endswith(finding) for item in findings), findings


def _after_super(line: str) -> str:
    """The platform fixture with ``line`` added after its ``super().__init__``."""
    return _PLATFORM + f"        {line}\n"


def _in_method(method: str, line: str) -> str:
    """The platform fixture with a ``method`` override whose body is ``line``."""
    return _PLATFORM + f"\n    def {method}(self, *args):\n        {line}\n"


@pytest.mark.parametrize(
    ("source", "finding"),
    [
        (
            _after_super('self._headers["X-Key"] = "k"'),
            "writes self._headers[...]",
        ),
        (
            _after_super('self._headers["Content-Type"] = "text/plain"'),
            "writes self._headers[...]",
        ),
        (
            _after_super('if self.gzip:\n            self._headers["Content-Type"] = "x"'),
            "writes self._headers[...]",
        ),
        (
            _in_method("_round", 'self._headers["Content-Type"] = "x"'),
            "writes self._headers[...]",
        ),
        (
            _after_super('self._headers.update({"X-Key": "k"})'),
            "calls self._headers.update",
        ),
        (
            _after_super('self._headers.setdefault("X-Key", "k")'),
            "calls self._headers.setdefault",
        ),
        (
            _in_method("_render", 'self._http._headers.update(x="k")'),
            "calls self._http._headers.update",
        ),
        (
            _PLATFORM.replace(
                "        super()",
                '        http_kwargs["headers"] = {"X-Key": key}\n        super()',
            ),
            "writes ['headers']",
        ),
    ],
    ids=[
        "subscript-after-super",
        "content-type-after-super",
        "content-type-under-if",
        "in-round",
        "update",
        "setdefault",
        "wrapped-update",
        "http-kwargs-headers",
    ],
)
def test_the_roster_names_a_write_to_a_headers_attribute(source: str, finding: str) -> None:
    """The third review's runtime-roster escapes, and the attribute routes the scan used to miss:
    at ``457016f0`` it matched a subscript write only on a bare name."""
    findings = _header_routes(source, "x.py")

    assert any(item.endswith(finding) for item in findings), findings


def test_the_roster_passes_the_merge_route_and_a_closed_extra_headers_route() -> None:
    closed = (
        "class S:\n    def __init__(self, dsn=None, *, opener=None):\n        pass\n"
        "    def emit(self, b):\n        self._http._send(b, extra_headers={'X-Sentry-Auth': 1})\n"
    )
    assert _header_routes(_PLATFORM, "x.py") == []
    assert _header_routes(closed, "x.py") == []


# --- two caller names that fold to one ----------------------------------------------------------


def _logstash(**kw: Any) -> LogstashSink:
    """``LogstashSink`` builds an ``HTTPSink`` from its headers; the refusal names that sink."""
    return LogstashSink(url=URL, **kw)


_BUILDERS: dict[str, tuple[Callable[..., Any], str]] = {
    **{name: (make, name) for name, make in _RECIPES.items()},
    "HTTPSink": (lambda **kw: HTTPSink(URL, **kw), "HTTPSink"),
    "LogstashSink": (_logstash, "HTTPSink"),
}

_DUPLICATES = [
    ("X-Foo", "x-foo"),
    ("X-Foo", "X-FOO"),
    ("x-foo", "X-Foo"),
    ("Authorization", "authorization"),
    ("content-type", "Content-Type"),
    ("x-\u00df", "X-SS"),
]


@pytest.mark.parametrize(("first", "second"), _DUPLICATES, ids=repr)
@pytest.mark.parametrize("cls", sorted(_BUILDERS))
def test_two_names_folding_to_one_are_refused_naming_both(
    cls: str, first: str, second: str
) -> None:
    """At ``39e365f5``: constructs, and only the second value is ever sent."""
    make, owner = _BUILDERS[cls]
    with pytest.raises(ValueError) as caught:
        make(headers={first: CALLER_VALUE, second: CALLER_VALUE + "-2"})

    if cls == "SplunkHECSink" and first.lower() == "authorization":
        expected = (
            f"SplunkHECSink header {first!r} collides with token, which this sink sends as "
            "Authorization; drop the header"
        )
    else:
        expected = (
            f"{owner} headers {first!r} and {second!r} name the same header, which is sent "
            "once; drop one"
        )
    assert str(caught.value) == expected
    assert CALLER_VALUE not in str(caught.value)
    assert caught.value.__context__ is None


def test_names_colliding_only_past_whitespace_meet_the_whitespace_refusal() -> None:
    with pytest.raises(ValueError) as caught:
        HTTPSink(URL, headers={"X-Foo": "1", "X-Foo ": "2"})

    assert str(caught.value) == "HTTPSink header name must not contain whitespace: 'X-Foo '"


@pytest.mark.parametrize(
    "headers",
    [
        {"X-Foo": "1", "X_Foo": "2"},
        {"X-Foo": "1", "X-Fo0": "2"},
        {"X-Foo": "1", "X-Foo-": "2"},
        {"X-Foo": "1", "X-Foo2": "2"},
    ],
    ids=["underscore", "zero", "trailing-hyphen", "suffix"],
)
def test_names_differing_by_a_character_that_does_not_fold_construct(headers) -> None:
    opener = FakeOpener()
    HTTPSink(URL, headers=headers, opener=opener).emit([{"i": 1}])

    sent = opener.calls[0]["headers"]
    for name, value in headers.items():
        assert sent[name.lower()] == value


# --- merge_headers without argument= keeps its pre-1.2.0 answer (invariant 10) -----------------


@pytest.mark.parametrize(
    ("base", "caller", "merged"),
    [
        ({"X-Default": "1"}, {"X-Default": "2"}, {"X-Default": "2"}),
        ({"X-Default": "1"}, {"x-default": "2"}, {"x-default": "2"}),
        ({"X-Default": "1"}, {"X-DEFAULT": "2"}, {"X-DEFAULT": "2"}),
        (
            {"Content-Type": "text/plain"},
            {"content-type": "application/json"},
            {"content-type": "application/json"},
        ),
        ({"X-Default": "1"}, [("X-Extra", "3")], {"X-Default": "1", "X-Extra": "3"}),
    ],
    ids=["exact", "lower", "upper", "content-type", "pairs"],
)
def test_merge_headers_without_argument_lets_the_caller_win(base, caller, merged) -> None:
    """At ``635c9edf``: the exact, lower and upper rows raised ``... collides with a credential``.

    ``merge_headers`` is public, and a third-party sink built on it may hand it defaults a caller
    is meant to override; the refusal is opt-in through ``argument=``. The exact row is what
    ``39a3f08c`` returned. The lower and upper rows returned both spellings there, which
    ``urllib`` folded to the caller's value; they return the caller's spelling alone now, so the
    map meets no duplicate refusal and the wire is unchanged (the next test).
    """
    kwargs: dict[str, Any] = {"headers": caller, "gzip": True}

    assert merge_headers(dict(base), kwargs) == merged
    assert kwargs == {"gzip": True}, "headers is popped, the rest left to forward"


def test_merge_headers_without_argument_replaces_a_default_at_its_position() -> None:
    base = {"X-A": "a", "X-Default": "1", "X-B": "b"}

    merged = merge_headers(base, {"headers": {"x-default": "2", "X-New": "n"}})

    assert merged is base, "updated in place, as documented"
    assert list(merged.items()) == [("X-A", "a"), ("x-default", "2"), ("X-B", "b"), ("X-New", "n")]


def test_two_caller_names_folding_together_still_meet_the_duplicate_refusal() -> None:
    """The merge replaces a default at most once, so a caller's own contradiction is reported."""

    class ThirdParty(HTTPSink):
        def __init__(self, **http_kwargs: Any) -> None:
            super().__init__(URL, headers=merge_headers({"X-Default": "1"}, http_kwargs))

    with pytest.raises(ValueError) as caught:
        ThirdParty(headers={"x-default": "2", "X-DEFAULT": "3"})

    assert str(caught.value) == (
        "ThirdParty headers 'x-default' and 'X-DEFAULT' name the same header, which is sent "
        "once; drop one"
    )


class _ThirdParty(HTTPSink):
    """A sink outside the library handing ``merge_headers`` defaults for the caller to override."""

    def __init__(self, url: str, **http_kwargs: Any) -> None:
        headers = merge_headers({"X-Default": "1", "X-Keep": "k"}, http_kwargs)
        super().__init__(url, headers=headers, **http_kwargs)


def _raw_request(send: Callable[[str], None]) -> bytes:
    """Serves one request on a raw socket and returns its head, port and Python version scrubbed."""
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    port = server.getsockname()[1]
    received: list[bytes] = []

    def serve() -> None:
        conn, _ = server.accept()
        with conn:
            data = b""
            while b"\r\n\r\n" not in data:
                data += conn.recv(65536)
            received.append(data.partition(b"\r\n\r\n")[0])
            conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        send(f"http://127.0.0.1:{port}/x")
    finally:
        thread.join(5)
        server.close()
    return re.sub(rb"Python-urllib/[0-9.]+", b"Python-urllib/V", received[0]).replace(
        str(port).encode(), b"PORT"
    )


def test_a_third_party_override_in_another_case_sends_the_bytes_1x_sent() -> None:
    """At ``2913ee5d``: refused as a duplicate. The head below was captured at ``39a3f08c`` on a raw
    socket, with this sink and these headers; the current tree sends it byte for byte."""
    head = _raw_request(
        lambda url: _ThirdParty(url, headers={"x-default": "2", "X-New": "n"}).emit([{"i": 1}])
    )

    assert head == (
        b"POST /x HTTP/1.1\r\nAccept-Encoding: identity\r\nContent-Length: 9\r\n"
        b"Host: 127.0.0.1:PORT\r\nUser-Agent: Python-urllib/V\r\n"
        b"Content-Type: application/x-ndjson\r\nX-Default: 2\r\nX-Keep: k\r\nX-New: n\r\n"
        b"Connection: close"
    )


def test_merge_headers_with_argument_refuses_naming_owner_and_argument() -> None:
    with pytest.raises(ValueError) as caught:
        merge_headers(
            {"X-Key": "own"}, {"headers": {"x-key": CALLER_VALUE}}, owner="P", argument="key"
        )

    assert str(caught.value) == (
        "P header 'x-key' collides with key, which this sink sends as X-Key; drop the header"
    )


# --- a str subclass is judged and stored as the text it holds ----------------------------------


class _Flip(str):
    """A name whose ``capitalize`` folds harmlessly at first and to ``Dd-api-key`` afterwards."""

    calls = 0

    def capitalize(self) -> str:
        _Flip.calls += 1
        return "X-foo" if _Flip.calls <= 3 else "Dd-api-key"

    def __repr__(self) -> str:
        _Flip.calls += 1
        return "'HIJACK'"


def test_a_name_whose_fold_flips_after_construction_cannot_reach_the_credential() -> None:
    """At ``635c9edf``: constructed and sent ``Dd-Api-Key: <caller's value>`` on the wire."""
    _Flip.calls = 0
    opener = FakeOpener()
    sink = DatadogSink(OWN_VALUE, headers={_Flip("x-foo"): CALLER_VALUE}, opener=opener)
    sink.emit([{"i": 1}])
    sink.emit([{"i": 2}])

    sent = opener.calls[-1]["headers"]
    assert OWN_VALUE in sent["dd-api-key"]
    assert sent["x-foo"] == CALLER_VALUE
    assert _Flip.calls == 0, "none of the caller's methods runs, at construction or per emit"
    assert all(type(name) is str for name in sink._headers)


class _Shifty(str):
    """A value or name whose methods count and lie."""

    calls = 0

    def __str__(self) -> str:
        _Shifty.calls += 1
        return "HIJACK"

    def __repr__(self) -> str:
        _Shifty.calls += 1
        return "'HIJACK'"

    def encode(self, *args: Any, **kwargs: Any) -> bytes:  # type: ignore[override]
        _Shifty.calls += 1
        return b"HIJACK"


def test_a_str_subclass_value_is_sent_as_the_text_it_stores() -> None:
    _Shifty.calls = 0
    opener = FakeOpener()
    sink = HTTPSink(URL, headers={"X-Env": _Shifty("prod")}, opener=opener)
    sink.emit([{"i": 1}])

    assert opener.calls[0]["headers"]["x-env"] == "prod"
    assert type(opener.calls[0]["headers"]["x-env"]) is str
    assert _Shifty.calls == 0


def test_a_duplicate_message_prints_the_stored_text_not_a_lying_repr() -> None:
    _Shifty.calls = 0
    with pytest.raises(ValueError) as caught:
        HTTPSink(URL, headers={"X-Foo": "1", _Shifty("x-foo"): "2"})

    assert str(caught.value) == (
        "HTTPSink headers 'X-Foo' and 'x-foo' name the same header, which is sent once; drop one"
    )
    assert _Shifty.calls == 0


@pytest.mark.parametrize(
    ("name", "kind"), [(5, "int"), (b"X-Foo", "bytes"), (None, "NoneType")], ids=repr
)
@pytest.mark.parametrize("cls", sorted(_BUILDERS))
def test_a_header_name_that_is_not_a_str_is_refused_naming_its_type(cls, name, kind) -> None:
    """At ``635c9edf``: a bare ``TypeError`` from the first check that read the name."""
    make, owner = _BUILDERS[cls]

    with pytest.raises(TypeError) as caught:
        make(headers={name: CALLER_VALUE})

    assert str(caught.value) == f"{owner} header name must be a str, not {kind}"
    assert caught.value.__context__ is None


def test_a_header_value_that_is_not_a_str_is_sent_as_before() -> None:
    opener = FakeOpener()
    HTTPSink(URL, headers={"X-Count": 5}, opener=opener).emit([{"i": 1}])  # type: ignore[dict-item]

    assert opener.calls[0]["headers"]["x-count"] == 5


class _Unstable(str):
    """A name whose hash changes on every call, and whose hash and equality count."""

    calls = 0
    seed = 0

    def __hash__(self) -> int:
        _Unstable.calls += 1
        _Unstable.seed += 1
        return _Unstable.seed

    def __eq__(self, other: object) -> bool:
        _Unstable.calls += 1
        return str.__eq__(self, other)


@pytest.mark.parametrize("spelled", ["x-default", "X-Other"], ids=["overrides", "adds"])
def test_merge_headers_reads_an_unstable_hash_name_by_position(spelled: str) -> None:
    """At ``8bfdaaa4``: ``given.pop`` looked the caller's key up by its own hash and raised a bare
    ``KeyError``. The merge reads by iteration and stores the exact text, so no lookup can miss."""
    given = {_Unstable(spelled): "2"}
    _Unstable.calls = 0

    merged = merge_headers({"X-Default": "1"}, {"headers": given})

    assert _Unstable.calls == 0, "no caller __hash__ or __eq__ runs inside the merge"
    assert all(type(name) is str for name in merged)
    expected = {"x-default": "2"} if spelled == "x-default" else {"X-Default": "1", "X-Other": "2"}
    assert merged == expected
