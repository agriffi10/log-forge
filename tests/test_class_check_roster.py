"""A type test reads the value's real type, never its ``__class__``: held over the AST.

``isinstance(value, X)`` reads ``value.__class__`` through attribute lookup whenever the real type
is not ``X`` or a subclass, so a ``__class__`` property or ``__getattribute__`` of a caller's
exception, stream result, sink result or event field ran inside the library and raised from paths
documented ``Raises: None`` (invariant 1). ``issubclass(type(value), X)`` never reads
``__class__``, but asks ``X``'s metaclass, and an abstract ``X`` — an ABC or a runtime
``Protocol`` — hashes ``type(value)`` into its cache, running the caller's metaclass ``__hash__``.
``_diag.is_a`` reads the MRO alone. The change that added this roster moved every site in ``src/``
onto it, or onto ``issubclass(type(x), X)`` in the leaf modules that import nothing from the
package, and holds the rule.

The check walks every module under ``src/log_foundry/``, subpackages included, and reports:

1. Any use of the name ``isinstance`` — a call, ``builtins.isinstance``, the builtin passed as a
   value (``map(isinstance, …)``), imported under another name (``from builtins import isinstance
   as check``) or looked up (``getattr(builtins, "isinstance")``) — and the same of
   ``singledispatch`` and ``singledispatchmethod``, which dispatch on ``__class__``.
2. A read of ``__class__`` or ``__instancecheck__`` as an attribute, or through ``getattr``,
   ``hasattr``, ``operator.attrgetter`` or ``__getattribute__`` given it as a string literal or a
   dotted literal naming it, and a class pattern (``case int():``), which ``match`` tests with
   ``isinstance``.
3. An ``issubclass``, ``is_a`` or ``_diag.is_a`` whose target is, or holds, an abstract class: one
   whose metaclass defines its own ``__subclasscheck__``. The target is resolved by evaluating its
   expression in the module's namespace, when it is built only of names, attributes, tuples and
   ``|``; through ``issubclass`` the abstract check runs caller code, and through ``is_a`` it
   answers by the MRO alone, which misses a class the ABC knows only by ``register`` (``dict`` for
   ``Mapping``), so the concrete classes must be named.

Exempt, held exactly by function and expression, so a second copy in the same function is still
reported. ``sanitize._Coercer._dispatch`` and ``_Coercer._key`` admit a claim through ``isinstance``
on purpose: SPEC-063's decision converts what a claim admits through ``int.__int__`` or
``float.__float__``, which refuses an impostor, and keeps the render-through routes on
``isinstance`` so a transparent proxy of a ``dict`` or ``datetime`` still renders; both run under
the coercer's own guard, which turns a raise into a placeholder. ``decorator._refuse_unusable``
reads a decorated object's claim of ``classmethod`` or ``staticmethod`` at decoration, documented
to raise what that lookup raises in the caller's frame (invariant 13). ``sinks._batch.
adjudicate_keyed`` asks a driver's response item whether it is a ``Mapping`` under a guard that
reads any raise as an unreadable response, and calls the item's own ``__contains__`` and
``__getitem__`` on the next line by design. ``sinks.http._copied_pair`` asks ``Mapping``'s ABC check under a
guard that reads a raise as a refusal, because a map ``Mapping`` knows only by ``register`` would
otherwise send its two keys as the credentials. ``sinks._factory._FactorySink._checked`` keeps the
``Sink`` protocol check under a guard that reads a raise as a refusal, so what ``sink_factory=``
accepts stays what ``isinstance`` accepts, a ``MagicMock(spec=…)`` included. ``sinks.transform.TransformSink.
_apply`` judges a transform's result the same way, because a transparent proxy of a ``dict`` has
always been accepted there and coerced into an exact one. ``sinks.http._sent_value`` reproduces ``putheader``'s own ``isinstance(value,
int)`` once, at construction and under its own guard, because its point is to send what
``putheader`` would have sent, and a transparent proxy of an ``int`` header value is one.

Each exemption must still match its site, at its count, or it is reported stale (:func:`stale`).

**What it does not see**, each asserted silent by the corpus below: a target it cannot resolve —
a local bound by assignment, such as a driver's exception tuple, or a call — which ``is_a`` makes
safe anyway since it asks no target's metaclass, but ``issubclass(type(x), …)`` does not: there
the target's own metaclass is asked, which for ``sinks._classify``'s driver classes is the
driver's code, not the caller's, and plain ``type`` for every driver that module names today. A
target named by an import inside a function, a ``typing`` alias (``typing.Mapping``) and a
``typing.Union`` are resolved. Also unseen: a ``__class__`` store or delete, a name passed to
``getattr`` in a variable, a class-dict read (``type.__dict__["__class__"]``) and text that only
mentions a test. Serves invariant 1.
"""

from __future__ import annotations

import ast
import importlib
import typing
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from types import UnionType

import pytest

_PACKAGE = Path(__file__).resolve().parents[1] / "src" / "log_foundry"
"""Derived from this file, never from ``log_foundry.__file__``: an editable install can resolve to
another tree, and a guard scanning the wrong tree passes because it found nothing."""

_BANNED_NAMES = frozenset({"isinstance", "singledispatch", "singledispatchmethod"})

_HOOKS = frozenset({"__class__", "__instancecheck__"})

_LOOKUPS = frozenset({"getattr", "hasattr", "attrgetter", "__getattribute__"})

_SUBCLASS_TESTS = frozenset({"issubclass", "is_a"})

_RESOLVABLE = (ast.Name, ast.Attribute, ast.Tuple, ast.BinOp, ast.Subscript, ast.Load, ast.BitOr)
"""The node types a target may be built of to be resolved; anything else is left unresolved.

No call is among them, so resolving a target runs no code of the module's, only name lookups and
the ``[]`` of a ``typing`` alias such as ``Union[int, Mapping]``.
"""

_SPEC_063 = "SPEC-063: a claim is converted and refused, or rendered through for a proxy"

EXEMPT: dict[str, Counter[str]] = {
    "sanitize._Coercer._dispatch": Counter(
        {
            "isinstance(value, Enum)": 1,
            "isinstance(member, float)": 1,
            "isinstance(member, str)": 1,
            "isinstance(value, int)": 1,
            "isinstance(value, float)": 1,
            "isinstance(value, (datetime, date, time))": 1,
            "isinstance(value, UUID)": 1,
            "isinstance(value, Decimal)": 1,
            "isinstance(value, Mapping)": 1,
            "isinstance(value, (set, frozenset))": 1,
            "isinstance(value, Sequence)": 1,
            "isinstance(value, _TEXTLIKE)": 1,
        }
    ),
    "sanitize._Coercer._key": Counter(
        {
            "isinstance(key, str)": 1,
            "isinstance(key, int)": 1,
            "isinstance(rendered, str)": 1,
            "isinstance(key, float)": 1,
            "isinstance(replaced, str)": 1,
        }
    ),
    "decorator._refuse_unusable": Counter(
        {
            "isinstance(fn, (classmethod, staticmethod))": 1,
            "isinstance(fn, classmethod)": 1,
        }
    ),
    "sinks._batch.adjudicate_keyed": Counter({"isinstance(item, Mapping)": 1}),
    "sinks.http._copied_pair": Counter({"issubclass(type(auth), _NOT_A_PAIR)": 1}),
    "sinks._factory._FactorySink._checked": Counter({"isinstance(result, Sink)": 1}),
    "sinks.transform.TransformSink._apply": Counter({"isinstance(result, dict)": 1}),
    "sinks.http._sent_value": Counter({"isinstance(value, int)": 1}),
}
"""The sites this rule does not govern, by enclosing function and exact expression.

Each one's reason is in the module docstring; ``sanitize``'s are :data:`_SPEC_063`'s decision,
``docs/decisions/event-assembly/a-route-that-trusts-the-callers-object-admits-it-by-exact-type-or-
an-unbound-accessor.md``.
"""


@dataclass(frozen=True)
class Site:
    """One type test that may run caller code, where it sits and why it was reported.

    Attributes:
      where: The dotted module path and enclosing function, such as ``console._is_permanent``.
      line: The line the site starts on.
      expr: The site, unparsed.
      reason: Which of the module docstring's three checks reported it, and how.
    """

    where: str
    line: int
    expr: str
    reason: str


def _abstract(target: object) -> list[str]:
    """The abstract classes ``target`` is or holds, by stored name; empty when there are none."""
    if isinstance(target, tuple):
        return [name for each in target for name in _abstract(each)]
    origin = typing.get_origin(target)
    if isinstance(target, UnionType) or origin is typing.Union:
        return [name for each in typing.get_args(target) for name in _abstract(each)]
    if origin is not None:
        return _abstract(origin)
    if isinstance(target, type) and type(target).__subclasscheck__ is not type.__subclasscheck__:
        return [type.__dict__["__name__"].__get__(target)]
    return []


class _Finder(ast.NodeVisitor):
    """Collects every type test that may run caller code in one module, by enclosing function."""

    def __init__(self, module: str, namespace: dict[str, object] | None) -> None:
        self.module = module
        self.namespace = namespace
        self.scope: list[str] = []
        self.sites: list[Site] = []
        self._called: set[int] = set()

    def _where(self) -> str:
        return ".".join([self.module, *self.scope]) if self.module else ".".join(self.scope)

    def _report(self, node: ast.AST, reason: str) -> None:
        self.sites.append(
            Site(self._where(), getattr(node, "lineno", 0), ast.unparse(node), reason)
        )

    def _enter(self, node: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef) -> None:
        self.scope.append(node.name)
        self.generic_visit(node)
        self.scope.pop()

    visit_FunctionDef = _enter
    visit_AsyncFunctionDef = _enter
    visit_ClassDef = _enter

    def _resolve(self, target: ast.expr) -> object:
        if self.namespace is None:
            return None
        if not all(isinstance(node, _RESOLVABLE) for node in ast.walk(target)):
            return None
        try:
            return eval(compile(ast.Expression(target), "<target>", "eval"), dict(self.namespace))  # noqa: S307
        except Exception:
            return None

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        called = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
        if called in _BANNED_NAMES:
            self._report(node, f"calls {called}")
            self._called.add(id(func))
        if called in _LOOKUPS:
            for arg in node.args:
                if (
                    isinstance(arg, ast.Constant)
                    and isinstance(arg.value, str)
                    and any(part in _HOOKS | _BANNED_NAMES for part in arg.value.split("."))
                ):
                    self._report(node, f"{called} of {arg.value}")
        if called in _SUBCLASS_TESTS and len(node.args) == 2:
            abstract = _abstract(self._resolve(node.args[1]))
            if abstract:
                self._report(node, f"{called} of abstract {', '.join(abstract)}")
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        if (
            node.id in _BANNED_NAMES
            and isinstance(node.ctx, ast.Load)
            and id(node) not in self._called
        ):
            self._report(node, f"passes {node.id} as a value")
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if isinstance(node.ctx, ast.Load):
            if node.attr in _HOOKS:
                self._report(node, f"attribute read of {node.attr}")
            elif node.attr in _BANNED_NAMES and id(node) not in self._called:
                self._report(node, f"passes {node.attr} as a value")
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        for alias in node.names:
            if alias.name in _BANNED_NAMES:
                self._report(node, f"imports {alias.name}")
        self.generic_visit(node)

    def visit_MatchClass(self, node: ast.MatchClass) -> None:
        self._report(node, "class pattern tests isinstance")
        self.generic_visit(node)


def sites(source: str, module: str = "", namespace: dict[str, object] | None = None) -> list[Site]:
    """Every type test in ``source`` that may run caller code, in visiting order.

    ``namespace`` resolves check 3's targets; ``None`` leaves every target unresolved.
    """
    tree = ast.parse(source)
    finder = _Finder(module, None if namespace is None else {**_imported(tree), **namespace})
    finder.visit(tree)
    return finder.sites


def _imported(tree: ast.Module) -> dict[str, object]:
    """Every name an import binds anywhere in ``tree``, function scopes included, run fresh.

    Module globals win over these, so a name a function imports resolves even though the module
    never binds it, which is how ``def f(x): from collections.abc import Mapping`` is still seen.
    """
    bound: dict[str, object] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import | ast.ImportFrom) and getattr(node, "level", 0) == 0:
            try:
                exec(compile(ast.Module([node], []), "<import>", "exec"), bound)  # noqa: S102
            except Exception:  # noqa: S112
                continue
    bound.pop("__builtins__", None)
    return bound


def corpus_sites(source: str) -> list[Site]:
    """:func:`sites` for a corpus source, resolved in the namespace running it builds."""
    namespace: dict[str, object] = {}
    exec(compile(source, "<corpus>", "exec"), namespace)  # noqa: S102
    return sites(source, namespace=namespace)


def _modules() -> dict[str, str]:
    """Every module under the package, by dotted path relative to it, mapped to its source."""
    found: dict[str, str] = {}
    for path in sorted(_PACKAGE.rglob("*.py")):
        dotted = ".".join(path.relative_to(_PACKAGE).with_suffix("").parts)
        found[dotted] = path.read_text(encoding="utf-8")
    return found


def _namespace(module: str) -> dict[str, object]:
    """The module's globals, imported from the package; ``__init__`` names the package itself."""
    dotted = (
        "log_foundry" if module == "__init__" else f"log_foundry.{module.removesuffix('.__init__')}"
    )
    return vars(importlib.import_module(dotted))


def unexempt(found: list[Site], exempt: dict[str, Counter[str]]) -> list[Site]:
    """The sites ``exempt`` does not cover, each exemption spending one site of its expression."""
    budget = {where: Counter(exprs) for where, exprs in exempt.items()}
    left: list[Site] = []
    for site in found:
        spare = budget.get(site.where)
        if spare is not None and spare[site.expr] > 0:
            spare[site.expr] -= 1
        else:
            left.append(site)
    return left


def stale(found: list[Site], exempt: dict[str, Counter[str]]) -> list[str]:
    """Each exemption whose function no longer holds its expression exactly as many times."""
    held = Counter((s.where, s.expr) for s in found)
    return [
        f"{where}: {expr!r} held {held[(where, expr)]} time(s), exempted {count}"
        for where, exprs in exempt.items()
        for expr, count in exprs.items()
        if held[(where, expr)] != count
    ]


def _all_sites() -> list[Site]:
    found: list[Site] = []
    for module, source in _modules().items():
        found.extend(sites(source, module, _namespace(module)))
    return found


def test_the_walk_reaches_the_sinks_subpackage() -> None:
    """A glob that stopped at the top level would find none of the sink sites and pass."""
    modules = _modules()
    assert "sinks.http" in modules
    assert "sinks.stdout" in modules
    assert "console" in modules


def test_the_walk_resolves_targets_in_the_real_module() -> None:
    """A namespace that resolved nothing would make check 3 silent on every module."""
    found = sites(_modules()["sinks.http"], "sinks.http", _namespace("sinks.http"))
    assert [s.reason for s in found if s.where == "sinks.http._copied_pair"] == [
        "issubclass of abstract Mapping"
    ]


def test_no_module_in_src_runs_a_values_class_hook_in_a_type_test() -> None:
    """Every unguarded site is reported with its place and the spelling that replaces it."""
    left = unexempt(_all_sites(), EXEMPT)
    assert left == [], (
        "test a value's type with _diag.is_a(value, X), or issubclass(type(value), X) in a leaf "
        "module, naming concrete classes (dict, not Mapping); if a site admits a claim on "
        "purpose under a guard or a spec, record it in EXEMPT with its reason:\n"
        + "\n".join(f"  {s.where}:{s.line}: {s.expr} ({s.reason})" for s in left)
    )


def test_every_exemption_still_matches_its_site_exactly() -> None:
    """An exemption whose site moved or vanished is stale, and a stale one would hide a new site."""
    assert stale(_all_sites(), EXEMPT) == []


_STALE_SOURCE = (
    "def g(fn):\n"
    "    return isinstance(fn, int)\n"
    "def twice(fn):\n"
    "    isinstance(fn, int)\n"
    "    return isinstance(fn, int)\n"
)


@pytest.mark.parametrize(
    ("exempt", "expected"),
    [
        (
            {"m.g": Counter({"isinstance(fn, str)": 1})},
            ["m.g: 'isinstance(fn, str)' held 0 time(s), exempted 1"],
        ),
        (
            {"m.g": Counter({"isinstance(fn, int)": 2})},
            ["m.g: 'isinstance(fn, int)' held 1 time(s), exempted 2"],
        ),
        (
            {"m.renamed": Counter({"isinstance(fn, int)": 1})},
            ["m.renamed: 'isinstance(fn, int)' held 0 time(s), exempted 1"],
        ),
        (
            {"m.twice": Counter({"isinstance(fn, int)": 1})},
            ["m.twice: 'isinstance(fn, int)' held 2 time(s), exempted 1"],
        ),
        ({"m.g": Counter({"isinstance(fn, int)": 1})}, []),
    ],
    ids=["site-vanished", "count-too-high", "function-renamed", "count-too-low", "exact-is-silent"],
)
def test_stale_reports_an_exemption_that_no_longer_matches_exactly(
    exempt: dict[str, Counter[str]], expected: list[str]
) -> None:
    """A vanished site, a wrong count and a renamed function are each reported by name."""
    assert stale(sites(_STALE_SOURCE, "m"), exempt) == expected


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("def f(e):\n    return isinstance(e, OSError)\n", [("f", "calls isinstance")]),
        (
            "def f(e):\n    return isinstance(e, (OSError, ValueError))\n",
            [("f", "calls isinstance")],
        ),
        ("x = isinstance(1, int)\n", [("", "calls isinstance")]),
        (
            "class S:\n    def r(self, e):\n        return (lambda: isinstance(e, int))()\n",
            [("S.r", "calls isinstance")],
        ),
        (
            "async def f(e):\n    return isinstance(\n        e,\n        int,\n    )\n",
            [("f", "calls isinstance")],
        ),
        (
            "import builtins\ndef f(e):\n    return builtins.isinstance(e, int)\n",
            [("f", "calls isinstance")],
        ),
        (
            "def f(xs):\n    return list(map(isinstance, xs, [int] * len(xs)))\n",
            [("f", "passes isinstance as a value")],
        ),
        (
            "import builtins\ncheck = builtins.isinstance\n",
            [("", "passes isinstance as a value")],
        ),
        (
            "from functools import singledispatch\n@singledispatch\ndef f(x):\n    return x\n",
            [("", "imports singledispatch"), ("f", "passes singledispatch as a value")],
        ),
        ("def f(e):\n    return e.__class__\n", [("f", "attribute read of __class__")]),
        (
            "def f(e):\n    return type(e).__class__\n",
            [("f", "attribute read of __class__")],
        ),
        (
            "def f(e):\n    return int.__instancecheck__(e)\n",
            [("f", "attribute read of __instancecheck__")],
        ),
        ("def f(e):\n    return getattr(e, '__class__')\n", [("f", "getattr of __class__")]),
        ("def f(e):\n    return hasattr(e, '__class__')\n", [("f", "hasattr of __class__")]),
        (
            "import operator\ndef f(e):\n    return operator.attrgetter('__class__.__name__')(e)\n",
            [("f", "attrgetter of __class__.__name__")],
        ),
        (
            "def f(e):\n    return object.__getattribute__(e, '__class__')\n",
            [("f", "__getattribute__ of __class__")],
        ),
        (
            "from builtins import isinstance as check\ndef f(e):\n    return check(e, int)\n",
            [("", "imports isinstance")],
        ),
        (
            "import builtins\ndef f(e):\n    return getattr(builtins, 'isinstance')(e, int)\n",
            [("f", "getattr of isinstance")],
        ),
        (
            (
                "from functools import singledispatchmethod\n"
                "class C:\n    @singledispatchmethod\n    def m(self, x):\n        return x\n"
            ),
            [
                ("", "imports singledispatchmethod"),
                ("C.m", "passes singledispatchmethod as a value"),
            ],
        ),
        (
            "def f(e):\n    match e:\n        case int():\n            return 1\n",
            [("f", "class pattern tests isinstance")],
        ),
        (
            "from collections.abc import Mapping\ndef f(x):\n    return issubclass(type(x), Mapping)\n",
            [("f", "issubclass of abstract Mapping")],
        ),
        (
            (
                "from collections.abc import Sequence\nfrom log_foundry import _diag\n"
                "def f(x):\n    return _diag.is_a(x, (int, Sequence))\n"
            ),
            [("f", "is_a of abstract Sequence")],
        ),
        (
            (
                "from collections.abc import Mapping\nfrom log_foundry._diag import is_a\n"
                "def f(x):\n    return is_a(x, Mapping)\n"
            ),
            [("f", "is_a of abstract Mapping")],
        ),
        (
            (
                "from collections.abc import Mapping\n_PAIRS = (set, Mapping)\n"
                "def f(x):\n    return issubclass(type(x), _PAIRS)\n"
            ),
            [("f", "issubclass of abstract Mapping")],
        ),
        (
            (
                "from collections.abc import Mapping\n"
                "def f(x):\n    return issubclass(type(x), int | Mapping)\n"
            ),
            [("f", "issubclass of abstract Mapping")],
        ),
        (
            (
                "def f(x):\n    from collections.abc import Mapping\n"
                "    return issubclass(type(x), Mapping)\n"
            ),
            [("f", "issubclass of abstract Mapping")],
        ),
        (
            "import typing as _t\ndef f(x):\n    return issubclass(type(x), _t.Mapping)\n",
            [("f", "issubclass of abstract Mapping")],
        ),
        (
            (
                "from typing import Mapping, Union\n"
                "def f(x):\n    return issubclass(type(x), Union[int, Mapping])\n"
            ),
            [("f", "issubclass of abstract Mapping")],
        ),
        (
            (
                "import collections.abc\n"
                "def f(x):\n    return issubclass(type(x), collections.abc.Sized)\n"
            ),
            [("f", "issubclass of abstract Sized")],
        ),
        (
            (
                "from typing import Protocol, runtime_checkable\n"
                "@runtime_checkable\nclass P(Protocol):\n    def emit(self) -> None: ...\n"
                "def f(x):\n    return issubclass(type(x), P)\n"
            ),
            [("f", "issubclass of abstract P")],
        ),
    ],
    ids=[
        "call",
        "call-tuple",
        "module-level",
        "lambda-in-method",
        "async-wrapped-over-lines",
        "builtins-call",
        "passed-as-value",
        "builtins-as-value",
        "singledispatch",
        "class-attr",
        "metaclass-class-attr",
        "instancecheck",
        "getattr",
        "hasattr",
        "attrgetter-dotted",
        "getattribute",
        "imported-under-another-name",
        "looked-up-by-name",
        "singledispatchmethod",
        "class-pattern",
        "issubclass-abc",
        "is-a-abc-in-tuple",
        "bare-is-a-abc",
        "module-tuple-holding-abc",
        "union-holding-abc",
        "function-scope-import",
        "typing-alias",
        "typing-union",
        "dotted-abc",
        "runtime-protocol",
    ],
)
def test_the_finder_reports_each_site_with_its_reason(
    source: str, expected: list[tuple[str, str]]
) -> None:
    """Each spelling is reported once, at its enclosing function, for its reason."""
    assert [(s.where, s.reason) for s in corpus_sites(source)] == expected


@pytest.mark.parametrize(
    "source",
    [
        "from log_foundry import _diag\ndef f(e):\n    return _diag.is_a(e, OSError)\n",
        "from log_foundry import _diag\ndef f(e):\n    return _diag.is_a(e, (OSError, dict))\n",
        "def f(e):\n    return issubclass(type(e), (OSError, ValueError))\n",
        "def f(e):\n    return issubclass(type(e), int | float)\n",
        "def f(e):\n    return type(e) is int\n",
        "from enum import Enum\ndef f(e):\n    return issubclass(type(e), Enum)\n",
        "def f(e, retried):\n    return issubclass(type(e), retried)\n",
        "def f(e, load):\n    return issubclass(type(e), load('m', 'Error'))\n",
        (
            "from collections.abc import Mapping\n"
            "def _pick():\n    return Mapping\n"
            "def f(e):\n    return issubclass(type(e), _pick())\n"
        ),
        "def f():\n    isinstance = int\n    return 0\n",
        "def f(e):\n    return issubclass(type(e), _UNDEFINED)\n",
        "def f(c):\n    return type.__dict__['__class__']\n",
        "def f(c):\n    c.__class__ = int\n",
        "def f(c):\n    del c.__class__\n",
        "def f(c, attr):\n    return getattr(c, attr)\n",
        "def f(c):\n    return getattr(c, '__classes__')\n",
        "def f(c):\n    return c.__class_x\n",
        "def f(c):\n    return c.isinstance_ok\n",
        "def isinstance_like(x):\n    return x\n",
        'def f(e):\n    """Formerly ``isinstance(e, OSError)``."""\n    return 1\n',
        "def f(e):\n    # isinstance(e, OSError)\n    return 1\n",
        "def f(e):\n    match e:\n        case 1:\n            return 1\n",
    ],
    ids=[
        "helper",
        "helper-tuple",
        "inline-real-type",
        "inline-union",
        "identity",
        "enum-metaclass-is-not-abstract",
        "local-target",
        "call-target",
        "resolvable-call-target",
        "name-store",
        "unresolved-name",
        "class-dict",
        "store",
        "delete",
        "getattr-variable",
        "getattr-lookalike",
        "lookalike-attribute",
        "lookalike-attribute-isinstance",
        "lookalike-def",
        "docstring",
        "comment",
        "value-pattern",
    ],
)
def test_the_finder_is_silent_on_tests_that_run_no_caller_code(source: str) -> None:
    """The real-type spellings, stores, deletes and text that only mentions a test stay silent."""
    assert corpus_sites(source) == []


def test_an_exemption_covers_only_its_own_expression_and_count() -> None:
    """A second copy, another expression, or the same one elsewhere is still reported."""
    source = (
        "def g(fn):\n"
        "    a = isinstance(fn, int)\n"
        "    b = isinstance(fn, int)\n"
        "    return isinstance(fn, str)\n"
        "def h(fn):\n"
        "    return isinstance(fn, int)\n"
    )
    exempt = {"m.g": Counter({"isinstance(fn, int)": 1})}
    left = unexempt(sites(source, "m"), exempt)
    assert [(s.where, s.line, s.reason) for s in left] == [
        ("m.g", 3, "calls isinstance"),
        ("m.g", 4, "calls isinstance"),
        ("m.h", 6, "calls isinstance"),
    ]
