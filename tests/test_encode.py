"""SPEC-069 — the one encode helper, the destination repairs, and the roster that holds every sink.

The helper's own contract is pinned first. Then the roster lint (FR-001): every class under
``src/log_foundry`` whose ``emit`` reaches ``json.dumps`` — directly or through a method or function
it calls — must reach it only through an encode function handed to ``encode_events``, or be one of
the declared exceptions: the HTTP family (its ``HTTPSink._items`` guard), ``NATSSink``,
``SentrySink`` and ``TransformSink``. It is a gate, so it carries a fixture corpus asserting each
check's failure text and its silent cases, and a floor of the population it must find.

The roster is derived at run time rather than from syntax alone, because the population depends on
the method resolution order: ``StderrSink`` inherits ``StdoutSink.emit``, the Redis sinks override
the ``_stage`` their base ``emit`` calls, and the HTTP subclasses override ``_render``.
"""

from __future__ import annotations

import ast
import importlib
import importlib.util
import inspect
import json
import pkgutil
import sys
import textwrap
import types
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry
from log_foundry.sinks._deferral import Deferral
from log_foundry.sinks._encode import encode_events

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from pathlib import Path

# --- the helper's own contract --------------------------------------------------------------


class _Owner:
    def __init__(self) -> None:
        self._unencodable = 0
        self.rejected = 0
        import threading

        self._counter_lock = threading.Lock()


def _strict(event: dict[str, object]) -> str:
    return json.dumps(event)


def test_encodable_events_come_back_in_order_with_their_wire_forms(capsys) -> None:
    owner = _Owner()
    batch = [{"i": 0}, {"i": 1}, {"i": 2}]
    with Deferral(owner) as pending:
        out = encode_events(batch, _strict, pending, sink="S")
    assert out == [(batch[0], '{"i": 0}'), (batch[1], '{"i": 1}'), (batch[2], '{"i": 2}')]
    assert out[0][0] is batch[0], "the event itself, not a copy"
    assert owner._unencodable == 0
    assert capsys.readouterr().err == ""


def test_failures_are_counted_once_with_one_line_naming_the_first_type(capsys) -> None:
    owner = _Owner()
    sentinel = "SENTINEL-in-the-event"

    def encode(event: dict[str, object]) -> str:
        if event.get("bad") == "value":
            raise ValueError(f"SENTINEL-in-the-message {event}")
        return json.dumps(event)

    batch = [{"ok": 1}, {"x": b"1", "s": sentinel}, {"bad": "value", "s": sentinel}, {"ok": 2}]
    with Deferral(owner) as pending:
        out = encode_events(batch, encode, pending, sink="MySink")
        assert owner._unencodable == 0, "nothing lands before the call returns"
    assert [e for e, _ in out] == [batch[0], batch[3]]
    assert owner._unencodable == 2
    err = capsys.readouterr().err
    assert err == "log-foundry: lost 2 event(s); MySink, encoding raised TypeError\n"
    assert "SENTINEL" not in err


def test_a_raising_call_counts_nothing_and_writes_nothing(capsys) -> None:
    owner = _Owner()
    with pytest.raises(RuntimeError), Deferral(owner) as pending:
        encode_events([{"x": b"1"}], _strict, pending, sink="S")
        raise RuntimeError("the bulk call failed")
    assert owner._unencodable == 0
    assert capsys.readouterr().err == ""


def test_a_baseexception_from_encode_propagates_with_nothing_recorded(capsys) -> None:
    owner = _Owner()

    def interrupt(event: dict[str, object]) -> str:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt), Deferral(owner) as pending:
        encode_events([{"x": b"1"}, {"a": 1}], interrupt, pending, sink="S")
    assert owner._unencodable == 0
    assert capsys.readouterr().err == ""


def test_the_counter_is_named_by_the_caller(capsys) -> None:
    owner = _Owner()
    with Deferral(owner) as pending:
        encode_events([{"x": b"1"}], _strict, pending, sink="K", counter="rejected")
    assert (owner.rejected, owner._unencodable) == (1, 0)


def test_nothing_unencodable_takes_no_lock() -> None:
    owner = _Owner()

    class Refusing:
        def __enter__(self) -> None:
            raise AssertionError("a clean call must not take the counter lock")

        def __exit__(self, *exc: object) -> None:
            return None

    owner._counter_lock = Refusing()  # type: ignore[assignment]
    with Deferral(owner) as pending:
        encode_events([{"a": 1}], _strict, pending, sink="S")


# --- the roster lint (FR-001) ---------------------------------------------------------------


@dataclass
class Roster:
    """What one run of the derivation found."""

    kinds: dict[str, str] = field(default_factory=dict)
    findings: list[str] = field(default_factory=list)

    def of(self, kind: str) -> set[str]:
        """The class names classified as ``kind``."""
        return {name for name, k in self.kinds.items() if k == kind}


def _parse(fn: Callable[..., Any]) -> ast.AST | None:
    try:
        return ast.parse(textwrap.dedent(inspect.getsource(fn)))
    except (OSError, TypeError, SyntaxError):
        return None


def _resolve(node: ast.expr, fn: Callable[..., Any], cls: type) -> object:
    """The object a callee expression names: ``self.m`` on the class, else through globals."""
    g = getattr(fn, "__globals__", {})
    if isinstance(node, ast.Name):
        return g.get(node.id)
    if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
        if node.value.id == "self":
            found = inspect.getattr_static(cls, node.attr, None)
            if isinstance(found, staticmethod | classmethod):
                return found.__func__
            return found
        base = g.get(node.value.id)
        if isinstance(base, types.ModuleType):
            return getattr(base, node.attr, None)
    return None


def derive(
    classes: Iterable[type],
    *,
    owned: tuple[str, ...],
    exempt: tuple[type, ...],
    http_emit: object,
    http_items: object,
) -> Roster:
    """Classifies every class whose ``emit`` reaches ``json.dumps``, and reports what it refuses.

    From the resolved ``emit`` it follows every call it can resolve into a function whose module
    starts with one of ``owned``: ``self.<m>(...)`` through the class's MRO, a bare name or a
    ``module.attr`` through the calling function's globals. A call whose callee resolves to
    ``json.dumps`` — however it was imported — is a hit. A call to ``encode_events`` has its
    ``encode`` argument resolved the same way and walked as *guarded*; every other reach is
    *unguarded*. It does not follow a callable passed uncalled (other than the helper's encode
    argument), a property, or a call through another object.
    """
    roster = Roster()
    for cls in classes:
        emit = inspect.getattr_static(cls, "emit", None)
        if not inspect.isfunction(emit):
            continue
        hits: list[tuple[str, bool]] = []
        stack: list[tuple[Callable[..., Any], bool]] = [(emit, False)]
        seen: set[tuple[int, bool]] = set()
        while stack:
            fn, guarded = stack.pop()
            if (id(fn), guarded) in seen:
                continue
            seen.add((id(fn), guarded))
            tree = _parse(fn)
            if tree is None:
                roster.findings.append(
                    f"{fn.__qualname__}: source unreadable; the roster is blind there"
                )
                continue
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                callee = _resolve(node.func, fn, cls)
                if callee is json.dumps:
                    hits.append((fn.__qualname__, guarded))
                elif callee is encode_events:
                    arg = (
                        node.args[1]
                        if len(node.args) > 1
                        else next((k.value for k in node.keywords if k.arg == "encode"), None)
                    )
                    target = _resolve(arg, fn, cls) if arg is not None else None
                    if inspect.isfunction(target):
                        stack.append((target, True))
                    else:
                        shown = ast.unparse(arg) if arg is not None else "<missing>"
                        roster.findings.append(
                            f"{cls.__module__}: {cls.__qualname__}.emit hands encode_events an "
                            f"encode argument the roster cannot resolve: {shown}"
                        )
                elif inspect.isfunction(callee) and (callee.__module__ or "").startswith(owned):
                    stack.append((callee, guarded))
        if not hits:
            continue
        name = cls.__qualname__
        if issubclass(cls, exempt):
            roster.kinds[name] = "exempt"
        elif emit is http_emit and inspect.getattr_static(cls, "_items", None) is http_items:
            roster.kinds[name] = "http"
        else:
            bare = sorted({where for where, guarded in hits if not guarded})
            roster.kinds[name] = "unconverted" if bare else "helper"
            roster.findings.extend(
                f"{cls.__module__}: {name}.emit reaches json.dumps in {where} outside an encode "
                f"function handed to encode_events"
                for where in bare
            )
    return roster


def _package_classes() -> list[type]:
    """Every class defined at module level anywhere in the package, in a stable order."""
    modules = [log_foundry] + [
        importlib.import_module(info.name)
        for info in pkgutil.walk_packages(log_foundry.__path__, "log_foundry.")
    ]
    return sorted(
        (
            obj
            for mod in modules
            for obj in vars(mod).values()
            if inspect.isclass(obj) and obj.__module__ == mod.__name__
        ),
        key=lambda c: (c.__module__, c.__qualname__),
    )


def _exempt() -> tuple[type, ...]:
    from log_foundry.sinks.nats import NATSSink
    from log_foundry.sinks.sentry import SentrySink
    from log_foundry.sinks.transform import TransformSink

    return (NATSSink, SentrySink, TransformSink)


def _tree_roster() -> Roster:
    from log_foundry.sinks.http import HTTPSink

    return derive(
        _package_classes(),
        owned=("log_foundry",),
        exempt=_exempt(),
        http_emit=inspect.getattr_static(HTTPSink, "emit"),
        http_items=inspect.getattr_static(HTTPSink, "_items"),
    )


ROSTER_FLOOR = frozenset(
    {
        "StdoutSink",
        "StderrSink",
        "FileSink",
        "RotatingFileSink",
        "SQLiteSink",
        "ClickHouseSink",
        "RabbitMQSink",
        "KafkaSink",
        "AzureEventHubsSink",
        "KinesisSink",
        "FirehoseSink",
        "SNSSink",
        "SQSSink",
        "MongoDBSink",
        "SyslogSink",
        "LogstashSink",
        "PostgresSink",
        "RedisStreamsSink",
        "RedisListSink",
        "GooglePubSubSink",
        "HTTPSink",
        "DatadogSink",
        "ElasticsearchSink",
        "OpenSearchSink",
        "HoneycombSink",
        "LokiSink",
        "NewRelicSink",
        "OTLPSink",
        "SplunkHECSink",
        "NATSSink",
        "SentrySink",
        "TransformSink",
    }
)
"""The population the derivation must find; a floor, not the set.

The 32 classes found at ``ebf9d680``: the twenty sinks this spec puts on the helper, the nine HTTP
classes on ``HTTPSink._items``, and the three declared exceptions. A refactor that blinds the
derivation — a renamed helper, a call it no longer resolves — would otherwise shrink the gate as
silently as a deletion.
"""

def test_every_sink_encodes_through_the_helper() -> None:
    """FR-001's roster over the real package: nothing reaches ``json.dumps`` unguarded."""
    roster = _tree_roster()
    assert roster.findings == [], "\n".join(roster.findings)
    assert roster.of("unconverted") == set()


def test_the_roster_finds_its_floor() -> None:
    roster = _tree_roster()
    lost = ROSTER_FLOOR - set(roster.kinds)
    assert not lost, f"the derivation lost {sorted(lost)}"
    assert roster.of("exempt") == {"NATSSink", "SentrySink", "TransformSink"}
    assert roster.of("http") == {
        "HTTPSink",
        "DatadogSink",
        "ElasticsearchSink",
        "OpenSearchSink",
        "HoneycombSink",
        "LokiSink",
        "NewRelicSink",
        "OTLPSink",
        "SplunkHECSink",
    }


def on_helper_public() -> set[str]:
    """The roster's on-helper classes that their module exports."""
    roster = _tree_roster()
    names = roster.of("helper")
    exported: set[str] = set()
    for cls in _package_classes():
        module = sys.modules[cls.__module__]
        if cls.__qualname__ in names and cls.__qualname__ in getattr(module, "__all__", ()):
            exported.add(cls.__qualname__)
    return exported


# --- the roster's fixture corpus ------------------------------------------------------------

_HEADER = """
import json
import json as j
from json import dumps
from log_foundry.sinks._encode import encode_events
from log_foundry.sinks.http import HTTPSink


def module_dumps(event):
    return json.dumps(event)


class Ledger:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False
"""


def _bare(cls: str, where: str) -> str:
    """The C1 finding text, with ``{m}`` left for the fixture module's name."""
    tail = f"{cls}.emit reaches json.dumps in {where} outside an encode function handed to"
    return "{m}: " + tail + " encode_events"


def _unresolved(cls: str, shown: str) -> str:
    """The C2 finding text, with ``{m}`` left for the fixture module's name."""
    tail = f"{cls}.emit hands encode_events an encode argument the roster cannot resolve:"
    return "{m}: " + tail + f" {shown}"


_CASES: dict[str, tuple[str, list[str], dict[str, str]]] = {
    "bare-in-emit": (
        """
class Sink:
    def emit(self, batch):
        for event in batch:
            json.dumps(event)
""",
        [_bare("Sink", "Sink.emit")],
        {"Sink": "unconverted"},
    ),
    "in-a-helper-method": (
        """
class Sink:
    def emit(self, batch):
        return [self._row(event) for event in batch]

    def _row(self, event):
        return json.dumps(event)
""",
        [_bare("Sink", "Sink._row")],
        {"Sink": "unconverted"},
    ),
    "in-a-subclass-override": (
        """
class Base:
    def emit(self, batch):
        for event in batch:
            self._stage(event)

    def _stage(self, event):
        return None


class Child(Base):
    def _stage(self, event):
        return json.dumps(event)
""",
        [_bare("Child", "Child._stage")],
        {"Child": "unconverted"},
    ),
    "in-a-module-function": (
        """
class Sink:
    def emit(self, batch):
        return [module_dumps(event) for event in batch]
""",
        [_bare("Sink", "module_dumps")],
        {"Sink": "unconverted"},
    ),
    "aliased-module": (
        """
class Sink:
    def emit(self, batch):
        return [j.dumps(event) for event in batch]
""",
        [_bare("Sink", "Sink.emit")],
        {"Sink": "unconverted"},
    ),
    "imported-name": (
        """
class Sink:
    def emit(self, batch):
        return [dumps(event) for event in batch]
""",
        [_bare("Sink", "Sink.emit")],
        {"Sink": "unconverted"},
    ),
    "inherited-emit": (
        """
class Parent:
    def emit(self, batch):
        return [json.dumps(event) for event in batch]


class Heir(Parent):
    pass
""",
        [
            _bare("Parent", "Parent.emit"),
            _bare("Heir", "Parent.emit"),
        ],
        {"Parent": "unconverted", "Heir": "unconverted"},
    ),
    "half-converted": (
        """
class Sink:
    def emit(self, batch):
        with Ledger() as pending:
            encoded = encode_events(batch, self._encode, pending, sink="Sink")
            json.dumps(batch[0])
            return encoded

    def _encode(self, event):
        return json.dumps(event)
""",
        [_bare("Sink", "Sink.emit")],
        {"Sink": "unconverted"},
    ),
    "lambda-encode": (
        """
class Sink:
    def emit(self, batch):
        with Ledger() as pending:
            return encode_events(batch, lambda e: json.dumps(e), pending, sink="Sink")
""",
        [_unresolved("Sink", "lambda e: json.dumps(e)"), _bare("Sink", "Sink.emit")],
        {"Sink": "unconverted"},
    ),
    "local-encode": (
        """
class Sink:
    def emit(self, batch):
        enc = self._encode
        with Ledger() as pending:
            return encode_events(batch, enc, pending, sink="Sink")

    def _encode(self, event):
        return json.dumps(event)
""",
        [_unresolved("Sink", "enc")],
        {},
    ),
    "http-items-overridden": (
        """
class Rogue(HTTPSink):
    def _items(self, batch, pending):
        return [json.dumps(event) for event in batch]
""",
        [_bare("Rogue", "Rogue._items")],
        {"Rogue": "unconverted"},
    ),
    "silent-converted": (
        """
class Sink:
    def emit(self, batch):
        with Ledger() as pending:
            return encode_events(batch, self._encode, pending, sink="Sink")

    def _encode(self, event):
        return json.dumps(event)
""",
        [],
        {"Sink": "helper"},
    ),
    "silent-two-deep-under-encode": (
        """
class Sink:
    def emit(self, batch):
        with Ledger() as pending:
            return encode_events(batch, encode=self._encode, pending=pending, sink="Sink")

    def _encode(self, event):
        return self._frame(event)

    def _frame(self, event):
        return module_dumps(event)
""",
        [],
        {"Sink": "helper"},
    ),
    "silent-module-function-encode": (
        """
class Sink:
    def emit(self, batch):
        with Ledger() as pending:
            return encode_events(batch, module_dumps, pending, sink="Sink")
""",
        [],
        {"Sink": "helper"},
    ),
    "silent-http-subclass": (
        """
class Fine(HTTPSink):
    def _render(self, event):
        return json.dumps({"wrapped": event})
""",
        [],
        {"Fine": "http"},
    ),
    "silent-exempt": (
        """
class Exempt:
    def emit(self, batch):
        return [json.dumps(event) for event in batch]
""",
        [],
        {"Exempt": "exempt"},
    ),
    "silent-no-dumps": (
        """
class Quiet:
    def emit(self, batch):
        return list(batch)
""",
        [],
        {},
    ),
}


def _fixture_module(tmp_path: Path, case: str, body: str) -> types.ModuleType:
    name = f"rosterfx_{case.replace('-', '_')}"
    path = tmp_path / f"{name}.py"
    path.write_text(_HEADER + body, encoding="utf-8")
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("case", sorted(_CASES))
def test_the_roster_lint_reports_what_it_claims_to(case: str, tmp_path: Path) -> None:
    from log_foundry.sinks.http import HTTPSink

    body, expected, kinds = _CASES[case]
    module = _fixture_module(tmp_path, case, body)
    try:
        classes = [
            obj
            for obj in vars(module).values()
            if inspect.isclass(obj) and obj.__module__ == module.__name__
        ]
        exempt = tuple(c for c in classes if c.__name__ == "Exempt")
        roster = derive(
            classes,
            owned=(module.__name__, "log_foundry"),
            exempt=exempt,
            http_emit=inspect.getattr_static(HTTPSink, "emit"),
            http_items=inspect.getattr_static(HTTPSink, "_items"),
        )
    finally:
        sys.modules.pop(module.__name__, None)
    assert sorted(roster.findings) == sorted(e.format(m=module.__name__) for e in expected)
    assert {k: v for k, v in roster.kinds.items() if k != "Ledger"} == kinds


def test_an_unreadable_source_fails_closed() -> None:
    namespace: dict[str, Any] = {"json": json}
    exec(  # noqa: S102
        "class Blind:\n    def emit(self, batch):\n        return [json.dumps(e) for e in batch]\n",
        namespace,
    )
    roster = derive(
        [namespace["Blind"]],
        owned=("log_foundry",),
        exempt=(),
        http_emit=None,
        http_items=None,
    )
    assert roster.findings == ["Blind.emit: source unreadable; the roster is blind there"]
