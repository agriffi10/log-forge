"""A platform sink refuses an empty credential argument, as ``HTTPSink`` refuses an empty ``auth``.

``DatadogSink("")`` sent ``DD-API-KEY:`` with nothing after it on every request,
``NewRelicSink("")`` an empty ``Api-Key``, ``HoneycombSink("", ...)`` an empty
``X-Honeycomb-Team`` and ``SplunkHECSink(url, "")`` ``Authorization: Splunk`` with no token.
Each is refused at construction, naming the argument and never the value, and so is a value of
only spaces and tabs, which a recipient strips (RFC 9110 §5.5) and receives as no credential
(invariants 11 and 13).

The population is derived, not hand-listed: every ``merge_headers(..., argument=)`` call in
``src/log_foundry/sinks/`` names a credential argument, and every constructor parameter of a
shipped HTTP sink whose name says it carries one is collected too. A new one that this file does
not cover fails :func:`test_the_credential_roster_covers_every_credential_argument` by name.
``auth`` has its own sweep in ``tests/test_sinks_http_auth.py``; ``SentrySink``'s ``dsn``
already refuses a missing public key.
"""

from __future__ import annotations

import ast
import inspect
import pathlib
import re
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry
from log_foundry.sinks.datadog import DatadogSink
from log_foundry.sinks.honeycomb import HoneycombSink
from log_foundry.sinks.logstash import LogstashSink
from log_foundry.sinks.newrelic import NewRelicSink
from log_foundry.sinks.splunk import SplunkHECSink
from test_sinks_http import FakeOpener
from test_sinks_http_render import _shipped_http_subclasses

if TYPE_CHECKING:
    from collections.abc import Callable

URL = "http://h/x"
_SINKS = pathlib.Path(log_foundry.__file__).parent / "sinks"

_CREDENTIAL_NAME = re.compile(r"(^|_)(key|token|secret|password|credentials?)$")
"""A constructor parameter whose name says it carries a credential."""


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Neutralize the HTTP backoff so a failing case cannot stall the suite."""
    monkeypatch.setattr("log_foundry.sinks.http.wait", lambda _delay, _stop=None: None)


_CREDENTIALS: dict[tuple[str, str], tuple[Callable[..., Any], str, Callable[[str], str]]] = {
    ("DatadogSink", "api_key"): (lambda v, **kw: DatadogSink(v, **kw), "dd-api-key", str),
    ("NewRelicSink", "api_key"): (lambda v, **kw: NewRelicSink(v, **kw), "api-key", str),
    ("HoneycombSink", "api_key"): (
        lambda v, **kw: HoneycombSink(v, "dataset", **kw),
        "x-honeycomb-team",
        str,
    ),
    ("SplunkHECSink", "token"): (
        lambda v, **kw: SplunkHECSink(URL, v, **kw),
        "authorization",
        lambda v: f"Splunk {v}",
    ),
}
"""Each credential argument: a builder taking the value, the header it is sent as, and the value
that header carries for a given credential."""


def _merge_header_arguments() -> set[tuple[str, str]]:
    """Every ``(module, argument)`` a ``merge_headers(..., argument=...)`` call in sinks/ names."""
    found: set[tuple[str, str]] = set()
    for path in sorted(_SINKS.glob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
            if name != "merge_headers":
                continue
            for keyword in node.keywords:
                if keyword.arg == "argument" and isinstance(keyword.value, ast.Constant):
                    found.add((path.stem, str(keyword.value.value)))
    return found


def _credential_parameters() -> set[tuple[str, str]]:
    """Every ``(class, parameter)`` of a shipped HTTP sink's constructor named as a credential."""
    found: set[tuple[str, str]] = set()
    for cls in [*_shipped_http_subclasses(), LogstashSink]:
        for name in inspect.signature(cls.__init__).parameters:
            if _CREDENTIAL_NAME.search(name):
                found.add((cls.__name__, name))
    return found


def test_the_credential_roster_covers_every_credential_argument() -> None:
    by_module = {
        cls.__module__.rsplit(".", 1)[-1]: cls.__name__ for cls in _shipped_http_subclasses()
    }
    merged = {(by_module[module], argument) for module, argument in _merge_header_arguments()}
    assert len(merged) >= 4, merged
    population = merged | _credential_parameters()

    missing = sorted(population - set(_CREDENTIALS))
    assert not missing, f"credential arguments with no case here: {missing}"


def test_the_parameter_scan_sees_a_credential_argument() -> None:
    """The name rule is not vacuous: it matches the shipped credential parameters."""
    assert {("DatadogSink", "api_key"), ("SplunkHECSink", "token")} <= _credential_parameters()


def _message(owner: str, argument: str) -> str:
    """The refusal, written out."""
    return (
        f"{owner} {argument} is empty, so every request would carry an empty credential; give it "
        "a value"
    )


class _Lying(str):
    """A ``str`` subclass holding no text whose methods would say it holds some."""

    def strip(self, chars: str | None = None) -> str:  # type: ignore[override]
        return "key"

    def __len__(self) -> int:
        return 3

    def __bool__(self) -> bool:
        return True


_EMPTY = ["", " ", "\t", "  \t ", _Lying(""), _Lying("  ")]
_EMPTY_IDS = ["empty", "space", "tab", "blank", "lying-empty", "lying-blank"]


@pytest.mark.parametrize("value", _EMPTY, ids=_EMPTY_IDS)
@pytest.mark.parametrize("case", sorted(_CREDENTIALS), ids=lambda case: f"{case[0]}.{case[1]}")
def test_an_empty_or_blank_credential_is_refused_naming_the_argument(case, value) -> None:
    """At ``c4e51fbd``: constructs, and sends the credential header with nothing in it."""
    make, _header, _sent = _CREDENTIALS[case]

    with pytest.raises(ValueError) as caught:
        make(value)

    assert str(caught.value) == _message(*case)
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


_BYTES_EMPTY = [b"", b"  ", b"\t", bytearray(b" ")]
_HEADER_CASES = sorted(case for case in _CREDENTIALS if case[0] != "SplunkHECSink")
"""The credentials sent as the whole header value, which a ``bytes`` value is sent as."""


@pytest.mark.parametrize("value", _BYTES_EMPTY, ids=["empty", "space", "tab", "bytearray"])
@pytest.mark.parametrize("case", _HEADER_CASES, ids=lambda case: f"{case[0]}.{case[1]}")
def test_an_empty_or_blank_bytes_credential_is_refused_as_sent(case, value) -> None:
    """At ``48d7d7d6``: only a ``str`` was judged, so ``DatadogSink(b"")`` sent ``DD-API-KEY:``
    with nothing after it. The value is judged by the bytes it sends, as the header path reads
    it."""
    make, _header, _sent = _CREDENTIALS[case]

    with pytest.raises(ValueError) as caught:
        make(value)

    assert str(caught.value) == _message(*case)


@pytest.mark.parametrize("case", _HEADER_CASES, ids=lambda case: f"{case[0]}.{case[1]}")
def test_a_bytes_credential_with_text_constructs_and_is_sent(case) -> None:
    make, header, _sent = _CREDENTIALS[case]
    opener = FakeOpener()

    make(b"key", opener=opener).emit([{"i": 1}])

    assert opener.calls[-1]["headers"][header] == b"key"


def test_splunk_formats_a_bytes_token_as_1_x_did_and_judges_that_text() -> None:
    """Splunk writes its token into ``Splunk <token>`` with ``format``, so ``b""`` was sent as
    ``Splunk b''`` — a wrong credential, not an empty one — and is judged as that text."""
    opener = FakeOpener()

    SplunkHECSink(URL, b"", opener=opener).emit([{"i": 1}])  # type: ignore[arg-type]

    assert opener.calls[-1]["headers"]["authorization"] == "Splunk b''"


class _Formats(str):
    """A token whose ``__format__`` answers something other than the text it holds."""

    calls = 0
    answer = ""

    def __format__(self, spec: str) -> str:
        type(self).calls += 1
        return type(self).answer


def test_splunk_judges_and_sends_the_exact_str_copy_and_never_formats_a_str_token() -> None:
    """At ``48d7d7d6``: the check read the copy but the header was ``f"Splunk {token}"``, so a
    ``__format__`` answering ``""`` constructed and sent ``Authorization: Splunk`` with no token."""
    _Formats.calls = 0
    _Formats.answer = ""
    opener = FakeOpener()

    SplunkHECSink(URL, _Formats("tok"), opener=opener).emit([{"i": 1}])

    assert opener.calls[-1]["headers"]["authorization"] == "Splunk tok"
    assert _Formats.calls == 0


def test_splunk_refuses_an_empty_token_whatever_its_format_answers() -> None:
    _Formats.calls = 0
    _Formats.answer = "text"

    with pytest.raises(ValueError) as caught:
        SplunkHECSink(URL, _Formats(""))

    assert str(caught.value) == _message("SplunkHECSink", "token")
    assert _Formats.calls == 0


@pytest.mark.parametrize("value", ["k", " key ", "a b"], ids=["short", "padded", "inner-space"])
@pytest.mark.parametrize("case", sorted(_CREDENTIALS), ids=lambda case: f"{case[0]}.{case[1]}")
def test_a_credential_with_text_constructs_and_is_sent_as_given(case, value: str) -> None:
    make, header, sent = _CREDENTIALS[case]
    opener = FakeOpener()

    make(value, opener=opener).emit([{"i": 1}])

    assert opener.calls[-1]["headers"][header] == sent(value)


@pytest.mark.parametrize("case", sorted(_CREDENTIALS), ids=lambda case: f"{case[0]}.{case[1]}")
def test_an_empty_credential_beside_a_colliding_header_meets_the_collision_first(case) -> None:
    """The refusal runs last, so a configuration refused before keeps its message."""
    make, header, _sent = _CREDENTIALS[case]

    with pytest.raises(ValueError) as caught:
        make("", headers={header: "x"})

    assert "collides with" in str(caught.value)
