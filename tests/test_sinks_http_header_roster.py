"""Every header an HTTP sink sends of its own is one a caller cannot silently replace — at run time.

The rule (1.2.0) is that a caller header never silently replaces another: a platform sink's own
credential header is refused in any spelling (``merge_headers`` with ``argument=``), and two caller
names folding to one are refused (``_valid_headers``). Two AST scans tried to hold every sink to
it and were blind to routes a source pattern does not name — ``self._headers[...] = ...`` after
``super().__init__``, ``.update``, a ``**{"headers": ...}`` splat, a ``_attempt`` override calling
``add_header`` — so this roster asks the running code instead.

Each class is built from its recipe with a recording opener and emits once; what the request
carried, read from ``Request.header_items()`` — every header ``urllib`` will send, however it got
there — minus what ``urllib`` and the transport own, is the sink's own. Each such name is then
passed back as a caller header, lower and upper case, and must be refused; one that folds to
``Authorization`` must refuse ``auth=`` too. A class taking no caller headers is skipped by its
signature, asserted. The population is every shipped ``HTTPSink`` subclass and every class that
builds an ``HTTPSink`` itself, so a sink with no recipe fails here by name.
"""

from __future__ import annotations

import ast
import inspect
import pathlib
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry.sinks
from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.logstash import LogstashSink
from log_foundry.sinks.sentry import SentrySink
from test_sinks_http import FakeResponse
from test_sinks_http_render import _RECIPES, _shipped_http_subclasses

if TYPE_CHECKING:
    import urllib.request
    from collections.abc import Callable

URL = "http://h/x"
CALLER = "SECRET-CALLER-VALUE"
_SINKS = pathlib.Path(log_foundry.sinks.__file__).parent

_TRANSPORT_OWN = {
    "Content-Type",
    "Content-Encoding",
    "Content-Length",
    "Host",
    "User-Agent",
    "Connection",
    "Accept-Encoding",
    "Transfer-Encoding",
}
"""Headers ``urllib``, the transport or the body's own encoding own: not a credential of the sink's,
and ``Content-Type`` is one a caller may override (SPEC-009 FR-002)."""


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Neutralize the HTTP backoff so a failing case cannot stall the suite."""
    monkeypatch.setattr("log_foundry.sinks.http.wait", lambda _delay, _stop=None: None)


class _Recording:
    """An opener recording every header each request will send, in ``urllib``'s own spelling."""

    def __init__(self) -> None:
        self.sent: list[dict[str, str]] = []

    def __call__(self, request: urllib.request.Request, timeout: float | None = None) -> Any:
        self.sent.append(dict(request.header_items()))
        return FakeResponse(200, b"{}")


def _fold(name: str) -> str:
    """The name ``urllib`` sends, ``capitalize`` then ``title``, written out."""
    return name.capitalize().title()


def _logstash(**kw: Any) -> LogstashSink:
    """``LogstashSink`` in HTTP mode."""
    return LogstashSink(url=URL, **kw)


def _sentry(**kw: Any) -> SentrySink:
    """``SentrySink``'s HTTP-envelope backend, the one that sends ``X-Sentry-Auth``."""
    return SentrySink("https://pubkey@o123.ingest.sentry.io/456", backend="http", **kw)


_BUILDERS: dict[str, Callable[..., Any]] = {
    **_RECIPES,
    "HTTPSink": lambda **kw: HTTPSink(URL, **kw),
    "LogstashSink": _logstash,
    "SentrySink": _sentry,
}

_EVENTS: dict[str, dict[str, object]] = {"SentrySink": {"level": "ERROR", "message": "boom"}}
"""An event each class sends, where the default ``{"i": 1}`` would be filtered out."""

_CLASSES: dict[str, type] = {
    "HTTPSink": HTTPSink,
    "LogstashSink": LogstashSink,
    "SentrySink": SentrySink,
    **{cls.__name__: cls for cls in _shipped_http_subclasses()},
}


def _wrappers() -> set[str]:
    """Every class in ``log_foundry.sinks`` whose own code builds an ``HTTPSink``."""
    found: set[str] = set()
    for path in sorted(_SINKS.glob("*.py")):
        if path.name == "http.py":
            continue
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.ClassDef) and any(
                isinstance(call, ast.Call) and getattr(call.func, "id", None) == "HTTPSink"
                for call in ast.walk(node)
            ):
                found.add(node.name)
    return found


def test_the_roster_has_a_builder_for_every_http_sink() -> None:
    """A floor and a completeness check: no HTTP sink escapes the harvest by having no recipe."""
    population = {cls.__name__ for cls in _shipped_http_subclasses()} | _wrappers() | {"HTTPSink"}
    assert population - set(_BUILDERS) == set(), "no recipe in _BUILDERS"
    assert {"LogstashSink", "SentrySink", "DatadogSink", "SplunkHECSink"} <= population


def _takes(cls: type, parameter: str) -> bool:
    """Whether ``cls``'s constructor accepts ``parameter``, by name or through ``**kwargs``."""
    parameters = inspect.signature(cls.__init__).parameters
    return parameter in parameters or any(
        p.kind is inspect.Parameter.VAR_KEYWORD for p in parameters.values()
    )


def _harvest(name: str) -> dict[str, str]:
    """The headers ``name`` sends of its own, by emitting once with none from the caller."""
    opener = _Recording()
    sink = _BUILDERS[name](opener=opener)
    sink.emit([dict(_EVENTS.get(name, {"i": 1}))])
    assert opener.sent, f"{name} sent no request to harvest"
    return {
        header: value
        for header, value in opener.sent[0].items()
        if _fold(header) not in _TRANSPORT_OWN
    }


def _unprotected(name: str) -> list[str]:
    """Every way a caller could replace one of ``name``'s own headers and still construct."""
    cls = _CLASSES[name]
    own = _harvest(name)
    if not _takes(cls, "headers"):
        assert not _takes(cls, "auth"), f"{name} takes auth= but not headers="
        return []
    findings: list[str] = []
    for header, value in own.items():
        assert value != CALLER
        for spelled in (header.lower(), header.upper()):
            try:
                _BUILDERS[name](headers={spelled: CALLER}, opener=_Recording())
            except ValueError as err:
                if repr(spelled) not in str(err) or CALLER in str(err):
                    findings.append(f"{name}: {spelled!r} refused without naming it: {err}")
                continue
            findings.append(
                f"{name}: caller header {spelled!r} constructs beside its own {header!r}"
            )
        if _fold(header) == "Authorization" and _takes(cls, "auth"):
            try:
                _BUILDERS[name](auth="SECRET-AUTH", opener=_Recording())
            except ValueError:
                continue
            findings.append(f"{name}: auth= constructs beside its own {header!r}")
    return findings


@pytest.mark.parametrize("name", sorted(_BUILDERS))
def test_no_header_a_sink_sends_of_its_own_can_be_replaced_by_a_caller(name: str) -> None:
    assert _unprotected(name) == []


_OWN_HEADERS = {
    "DatadogSink": {"Dd-Api-Key"},
    "NewRelicSink": {"Api-Key"},
    "HoneycombSink": {"X-Honeycomb-Team"},
    "SplunkHECSink": {"Authorization"},
    "SentrySink": {"X-Sentry-Auth"},
}
"""Every class's own headers, written out; a class not named here sends none of its own."""


@pytest.mark.parametrize("name", sorted(_BUILDERS))
def test_the_harvest_is_what_each_class_is_known_to_send(name: str) -> None:
    """A floor on the harvest, so a roster reading nothing cannot pass, and a pin on each class:
    a new header of a sink's own is a visible change here even where a caller's spelling of it
    meets the duplicate-name refusal rather than a credential one."""
    assert {_fold(h) for h in _harvest(name)} == _OWN_HEADERS.get(name, set())


def test_sentrysink_takes_neither_headers_nor_auth_so_its_header_has_no_rival() -> None:
    """The skip by signature, asserted rather than assumed."""
    assert not _takes(SentrySink, "headers")
    assert not _takes(SentrySink, "auth")
