"""SPEC-071 FR-002 — every import of an optional extra goes through `import_extra`, by AST.

Five checks over `src/log_foundry/**/*.py`, derived rather than listed:

1. No import of a module outside the standard library and the package, outside
   `if TYPE_CHECKING:`, is left anywhere — reported as `file:line (qualname): imports module`.
2. The `(file, qualname, module, extra)` tuples of the `import_extra` calls equal, exactly, the
   seventeen sites written out below from `2368ade3`, the commit SPEC-071 was built from.
3. Every extra named is a key of `[project.optional-dependencies]`.
4. Both arguments of every call are string literals, so checks 2 and 3 can read them.
5. No call to `importlib.import_module`, `import_module` or `__import__` is made outside
   `sinks/_extras.py`, which is `import_extra` itself — reported as `file:line (qualname)`. Check 1
   sees only import statements, so a sink written with a dynamic import would otherwise escape it.

The guard carries its own fixture corpus, including the shapes it must stay silent on, and a
derived mutation sweep that rewrites each real call back into a bare import, in memory, and
requires check 1 to report it. A site added later is swept without an edit here.

Serves invariant 13: a missing extra is refused, named, at the constructor that needs it.
"""

from __future__ import annotations

import ast
import sys
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_PACKAGE = _REPO / "src" / "log_foundry"
"""Derived from this file, never from ``log_foundry.__file__``: an editable install can resolve to
another tree, and a guard scanning the wrong tree passes because it found nothing."""

_IGNORED_TOP = frozenset(sys.stdlib_module_names) | {"__future__", "log_foundry"}

_DYNAMIC = frozenset({"import_module", "__import__"})
"""Callees that import a module by name: ``importlib.import_module``, a bare ``import_module``
imported from ``importlib``, and ``__import__``, bare or as an attribute."""

EXPECTED_SITES = frozenset(
    {
        ("sinks/clickhouse.py", "ClickHouseSink.__init__", "clickhouse_connect", "clickhouse"),
        ("sinks/eventhubs.py", "AzureEventHubsSink.__init__", "azure.eventhub", "azure-eventhubs"),
        ("sinks/eventhubs.py", "_event_data_cls", "azure.eventhub", "azure-eventhubs"),
        ("sinks/firehose.py", "FirehoseSink.__init__", "boto3", "aws"),
        ("sinks/kafka.py", "KafkaSink.__init__", "confluent_kafka", "kafka"),
        ("sinks/kinesis.py", "KinesisSink.__init__", "boto3", "aws"),
        ("sinks/mongodb.py", "MongoDBSink.__init__", "pymongo", "mongo"),
        ("sinks/mongodb.py", "MongoDBSink._resolve_bson", "bson", "mongo"),
        ("sinks/nats.py", "NATSSink.__init__", "nats", "nats"),
        ("sinks/postgres.py", "PostgresSink._connect", "psycopg", "postgres"),
        ("sinks/pubsub.py", "GooglePubSubSink.__init__", "google.cloud.pubsub_v1", "gcp-pubsub"),
        ("sinks/rabbitmq.py", "RabbitMQSink._connect", "pika", "amqp"),
        ("sinks/rabbitmq.py", "RabbitMQSink._persistent_properties", "pika", "amqp"),
        ("sinks/redis.py", "_RedisSink.__init__", "redis", "redis"),
        ("sinks/sentry.py", "_import_sdk", "sentry_sdk", "sentry"),
        ("sinks/sns.py", "SNSSink.__init__", "boto3", "aws"),
        ("sinks/sqs.py", "SQSSink.__init__", "boto3", "aws"),
    }
)
"""The seventeen sites on ``2368ade3``: fourteen bare, three soft (bson, pika's properties, sentry)."""


@dataclass
class Scan:
    """What one pass over a set of sources found.

    Attributes:
      imports: ``file:line (qualname): imports module`` for each import check 1 refuses.
      sites: The ``(file, qualname, module, extra)`` tuple of each literal ``import_extra`` call.
      calls: ``(file, line, qualname, module, extra)`` for each literal call, for the sweep.
      nonliteral: ``file:line`` for each call whose arguments are not two string literals.
      dynamic: ``file:line (qualname): …`` for each dynamic import outside ``sinks/_extras.py``.
    """

    imports: list[str] = field(default_factory=list)
    sites: set[tuple[str, str, str, str]] = field(default_factory=set)
    calls: list[tuple[str, int, str, str, str]] = field(default_factory=list)
    nonliteral: list[str] = field(default_factory=list)
    dynamic: list[str] = field(default_factory=list)


def _is_type_checking(test: ast.expr) -> bool:
    """Returns whether an ``if`` test is ``TYPE_CHECKING`` or ``typing.TYPE_CHECKING``.

    Args:
      test: The ``if`` statement's test.

    Returns:
      Whether the block runs only under a type checker.

    Raises:
      None.
    """
    return (isinstance(test, ast.Name) and test.id == "TYPE_CHECKING") or (
        isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"
    )


def _callee(func: ast.expr) -> str | None:
    """Returns the name a call's callee is spelled with, bare or as an attribute.

    Args:
      func: The call's ``func`` node.

    Returns:
      The name, or ``None`` for any other callee shape.

    Raises:
      None.
    """
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _scan_one(rel: str, source: str, scan: Scan) -> None:
    """Adds one module's findings to ``scan``.

    Args:
      rel: The module's path relative to the package, as reported.
      source: Its source text.
      scan: The accumulator.

    Returns:
      None.

    Raises:
      SyntaxError: If the source does not parse.
    """

    def visit(node: ast.AST, scope: tuple[str, ...], typing_only: bool) -> None:
        for child in ast.iter_child_nodes(node):
            child_scope = scope
            if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
                child_scope = (*scope, child.name)
            qualname = ".".join(child_scope) or "<module>"
            if isinstance(child, ast.If) and _is_type_checking(child.test):
                for stmt in child.body:
                    visit_stmt(stmt, child_scope, True)
                for stmt in child.orelse:
                    visit_stmt(stmt, child_scope, typing_only)
                continue
            if isinstance(child, ast.Import | ast.ImportFrom) and not typing_only:
                for module in _imported(child):
                    scan.imports.append(f"{rel}:{child.lineno} ({qualname}): imports {module}")
            if (
                isinstance(child, ast.Call)
                and _callee(child.func) in _DYNAMIC
                and rel != "sinks/_extras.py"
            ):
                scan.dynamic.append(
                    f"{rel}:{child.lineno} ({qualname}): imports dynamically through "
                    f"{ast.unparse(child.func)}"
                )
            if isinstance(child, ast.Call) and _callee(child.func) == "import_extra":
                _record_call(rel, child, ".".join(scope) or "<module>", scan)
            visit(child, child_scope, typing_only)

    def visit_stmt(stmt: ast.stmt, scope: tuple[str, ...], typing_only: bool) -> None:
        holder = ast.Module(body=[stmt], type_ignores=[])
        visit(holder, scope, typing_only)

    visit(ast.parse(source), (), False)


def _imported(node: ast.Import | ast.ImportFrom) -> list[str]:
    """Returns the modules an import statement names that check 1 refuses.

    Args:
      node: The import statement.

    Returns:
      Each third-party module it imports; a relative import is the package's own.

    Raises:
      None.
    """
    if isinstance(node, ast.ImportFrom):
        if node.level or node.module is None:
            return []
        names = [node.module]
    else:
        names = [alias.name for alias in node.names]
    return [name for name in names if name.partition(".")[0] not in _IGNORED_TOP]


def _record_call(rel: str, call: ast.Call, qualname: str, scan: Scan) -> None:
    """Records one ``import_extra`` call, or reports its non-literal arguments.

    Args:
      rel: The module's path relative to the package.
      call: The call node.
      qualname: The enclosing function's qualified name.
      scan: The accumulator.

    Returns:
      None.

    Raises:
      None.
    """
    args = call.args
    if (
        len(args) != 2
        or call.keywords
        or not all(isinstance(a, ast.Constant) and isinstance(a.value, str) for a in args)
    ):
        scan.nonliteral.append(f"{rel}:{call.lineno}")
        return
    module, extra = (a.value for a in args if isinstance(a, ast.Constant))
    scan.sites.add((rel, qualname, str(module), str(extra)))
    scan.calls.append((rel, call.lineno, qualname, str(module), str(extra)))


def scan_sources(sources: dict[str, str]) -> Scan:
    """Scans every module in ``sources``.

    Args:
      sources: Source text keyed by path relative to the package.

    Returns:
      The combined findings.

    Raises:
      SyntaxError: If a source does not parse.
    """
    scan = Scan()
    for rel in sorted(sources):
        _scan_one(rel, sources[rel], scan)
    return scan


def findings(
    sources: dict[str, str],
    extras: frozenset[str],
    expected: frozenset[tuple[str, str, str, str]],
) -> list[str]:
    """Runs the four checks and names every violation.

    Args:
      sources: Source text keyed by path relative to the package.
      extras: The ``[project.optional-dependencies]`` keys.
      expected: The site tuples written out by name.

    Returns:
      One line per violation; empty when the tree is clean.

    Raises:
      SyntaxError: If a source does not parse.
    """
    scan = scan_sources(sources)
    out = list(scan.imports)
    out += scan.dynamic
    out += [
        f"{where}: import_extra arguments are not two string literals" for where in scan.nonliteral
    ]
    out += [
        f"{rel}:{line}: extra {extra!r} is not a [project.optional-dependencies] key"
        for rel, line, _, _, extra in scan.calls
        if extra not in extras
    ]
    out += [f"unexpected site {site}" for site in sorted(scan.sites - expected)]
    out += [f"missing site {site}" for site in sorted(expected - scan.sites)]
    return out


def _tree() -> dict[str, str]:
    return {
        path.relative_to(_PACKAGE).as_posix(): path.read_text(encoding="utf-8")
        for path in sorted(_PACKAGE.rglob("*.py"))
    }


def _extras() -> frozenset[str]:
    with (_REPO / "pyproject.toml").open("rb") as handle:
        return frozenset(tomllib.load(handle)["project"]["optional-dependencies"])


def test_the_real_tree_is_clean() -> None:
    assert findings(_tree(), _extras(), EXPECTED_SITES) == []


def test_the_derived_population_is_not_empty() -> None:
    """A scan that matched nothing would pass check 1 vacuously."""
    assert len(scan_sources(_tree()).calls) == len(EXPECTED_SITES) == 17


# --- corpus: what it reports ----------------------------------------------------------------------


def _one(source: str, expected: frozenset[tuple[str, str, str, str]] = frozenset()) -> list[str]:
    return findings({"sinks/m.py": source}, frozenset({"aws", "mongo"}), expected)


def test_it_reports_a_bare_function_level_import() -> None:
    assert _one("def build():\n    import boto3\n    return boto3\n") == [
        "sinks/m.py:2 (build): imports boto3"
    ]


def test_it_reports_a_try_import_except_import_error_pair() -> None:
    source = (
        "class S:\n"
        "    def f(self):\n"
        "        try:\n"
        "            import pika\n"
        "        except ImportError:\n"
        "            pika = None\n"
    )
    assert _one(source) == ["sinks/m.py:4 (S.f): imports pika"]


def test_it_reports_a_module_level_from_import() -> None:
    assert _one("from pymongo import MongoClient\n") == ["sinks/m.py:1 (<module>): imports pymongo"]


def test_it_reports_an_extra_that_is_not_a_key() -> None:
    site = ("sinks/m.py", "f", "boto3", "nosuch")
    assert _one('def f():\n    return import_extra("boto3", "nosuch")\n', frozenset({site})) == [
        "sinks/m.py:2: extra 'nosuch' is not a [project.optional-dependencies] key"
    ]


def test_it_reports_a_non_literal_argument() -> None:
    source = (
        "def f(name, extra):\n"
        '    import_extra(name, "aws")\n'
        '    import_extra("boto3", extra)\n'
        '    import_extra(module="boto3", extra="aws")\n'
    )
    assert _one(source) == [
        "sinks/m.py:2: import_extra arguments are not two string literals",
        "sinks/m.py:3: import_extra arguments are not two string literals",
        "sinks/m.py:4: import_extra arguments are not two string literals",
    ]


def test_it_reports_a_site_the_table_lacks_and_one_the_tree_lacks() -> None:
    expected = frozenset({("sinks/m.py", "g", "pymongo", "mongo")})
    assert _one('def f():\n    return import_extra("boto3", "aws")\n', expected) == [
        "unexpected site ('sinks/m.py', 'f', 'boto3', 'aws')",
        "missing site ('sinks/m.py', 'g', 'pymongo', 'mongo')",
    ]


# --- corpus: what it must stay silent on ----------------------------------------------------------


def test_it_is_silent_on_a_standard_library_import_in_a_function() -> None:
    assert _one("def f():\n    import threading\n    import os.path\n") == []


def test_it_is_silent_on_the_packages_own_imports() -> None:
    source = (
        "import log_foundry\n"
        "from log_foundry.sinks import _chunk\n"
        "from . import base\n"
        "from .base import Sink\n"
        "def f():\n"
        "    from log_foundry.ids import new_log_id\n"
    )
    assert _one(source) == []


def test_it_is_silent_on_a_third_party_import_under_type_checking() -> None:
    source = (
        "from typing import TYPE_CHECKING\n"
        "import typing\n"
        "if TYPE_CHECKING:\n"
        "    import boto3\n"
        "    from pymongo import MongoClient\n"
        "if typing.TYPE_CHECKING:\n"
        "    import pika\n"
    )
    assert _one(source) == []


def test_type_checking_covers_its_body_but_not_its_else() -> None:
    source = "if TYPE_CHECKING:\n    import boto3\nelse:\n    import pika\n"
    assert _one(source) == ["sinks/m.py:4 (<module>): imports pika"]


def test_it_reports_a_dynamic_import_in_either_form() -> None:
    source = (
        "import importlib\n"
        "from importlib import import_module\n"
        "def f():\n"
        "    a = importlib.import_module('boto3')\n"
        "    b = import_module('pika')\n"
        "    c = __import__('redis')\n"
    )
    assert _one(source) == [
        "sinks/m.py:4 (f): imports dynamically through importlib.import_module",
        "sinks/m.py:5 (f): imports dynamically through import_module",
        "sinks/m.py:6 (f): imports dynamically through __import__",
    ]


def test_it_is_silent_on_the_dynamic_import_inside_import_extra() -> None:
    source = (_PACKAGE / "sinks" / "_extras.py").read_text(encoding="utf-8")
    assert "importlib.import_module(" in source, "the premise: _extras.py imports dynamically"
    assert findings({"sinks/_extras.py": source}, frozenset(), frozenset()) == []
    line = next(
        n for n, text in enumerate(source.splitlines(), 1) if "importlib.import_module(" in text
    )
    assert findings({"sinks/other.py": source}, frozenset(), frozenset()) == [
        f"sinks/other.py:{line} (import_extra): imports dynamically through importlib.import_module"
    ], "the same source anywhere else is reported"


def test_it_is_silent_on_a_call_to_import_extra() -> None:
    site = ("sinks/m.py", "C.__init__", "boto3", "aws")
    source = (
        "class C:\n"
        "    def __init__(self):\n"
        '        self.client = import_extra("boto3", "aws").client("sqs")\n'
    )
    assert _one(source, frozenset({site})) == []


# --- the mutation sweep -------------------------------------------------------------------------


class _Plant(ast.NodeTransformer):
    """Rewrites one ``import_extra`` call into a bare import inside its function."""

    def __init__(self, line: int, module: str) -> None:
        self.line = line
        self.module = module
        self.planted = False

    def visit_FunctionDef(self, node: ast.FunctionDef) -> ast.FunctionDef:
        self.generic_visit(node)
        hit = any(
            isinstance(n, ast.Name) and n.id == "__planted__" for n in ast.walk(node)
        ) and not any(
            isinstance(s, ast.Import) and s.names[0].name == self.module for s in node.body
        )
        if hit:
            node.body.insert(0, ast.Import(names=[ast.alias(name=self.module)]))
            self.planted = True
        return node

    def visit_Call(self, node: ast.Call) -> ast.AST:
        self.generic_visit(node)
        if _callee(node.func) == "import_extra" and node.lineno == self.line:
            return ast.Name(id="__planted__", ctx=ast.Load())
        return node


def test_every_call_rewritten_into_a_bare_import_is_reported() -> None:
    tree = _tree()
    calls = scan_sources(tree).calls
    planted = 0
    for rel, line, qualname, module, _extra in calls:
        mutant = ast.parse(tree[rel])
        plant = _Plant(line, module)
        mutant = ast.fix_missing_locations(plant.visit(mutant))
        assert plant.planted, f"the premise: {rel}:{line} was rewritten"
        reported = scan_sources({**tree, rel: ast.unparse(mutant)}).imports
        assert any(
            r.startswith(f"{rel}:") and r.endswith(f"({qualname}): imports {module}")
            for r in reported
        ), f"{rel}:{line} ({qualname}) rewritten to `import {module}` went unreported"
        planted += 1
    assert planted == len(calls) == len(EXPECTED_SITES)
