"""A type's name is read as the name it stores, never through attribute lookup: held over the AST.

``type(err).__name__`` is resolved through the metaclass, so an exception class whose metaclass
defines a raising ``__name__`` property made a sink path documented ``Raises: None`` raise
(invariant 1), and one returning another string renamed the type in the library's own lines.
``_diag.type_name`` and ``_diag._STR_STR(_diag._STORED_NAME(...))`` read the stored name instead.
SPEC-084 moved the stream and file sinks onto it; the change that added this roster moved every
other read in ``src/`` and holds the rule, so a new raw read reddens here rather than in a caller's
process.

The check walks every module under ``src/log_foundry/``, subpackages included, and reports:

1. An attribute *read* of ``__name__`` or ``__qualname__`` on any receiver — ``type(x).__name__``,
   ``x.__class__.__name__``, ``cls.__name__`` on a class held in a local — including the read an
   augmented assignment (``x.__name__ += ...``) makes. Any receiver, because a class reached
   through a local alias is as much a caller's class as ``type(x)`` is, and no syntax tells the
   two apart.
2. A call of ``getattr``, ``hasattr``, ``operator.attrgetter`` or ``__getattribute__`` given either
   name as a string literal, or a dotted literal ending in one (``attrgetter("__class__.__name__")``),
   since each runs the same metaclass property.
3. A ``str.format`` or ``format_map`` on a literal whose replacement field reads either name
   (``"{0.__class__.__name__}".format(err)``), and a class pattern matching on either name as a
   keyword (``case type(__name__=n)``): both resolve the attribute through ``getattr``.

Exempt, held exactly by function and expression, so a second copy in the same function is still
reported: ``decorator._misordered`` and ``decorator._span_name`` read a *callable's own*
``__qualname__`` at decoration, which is the function's name rather than its type's, and both are
documented to raise what that lookup raises, in the caller's frame. ``_otel._type_name`` keeps
``type(value).__name__`` inside its own ``except Exception``, so it cannot raise, and SPEC-056
pins its ``<unserializable: ?>`` placeholder for a raising metaclass name; moving it would change
an OTLP record a caller can see, which is a spec's decision, not this roster's.

Each exemption must still match its read, at its count, or it is reported stale (:func:`stale`):
an exemption that outlives its read would excuse the next one written in that function.

**What it does not see**, each asserted silent by the corpus below: a store or a delete of the
attribute, the bare module global ``__name__``, a name passed to ``getattr`` in a variable, a
class-dict read (``vars(cls)["__name__"]``, ``type.__dict__["__name__"]``),
``inspect.getattr_static``, which runs no descriptor, a format field that indexes or names a
keyword rather than reading an attribute (``"{0[__name__]}"``, ``"{__name__}"``), and a format
string that is not a literal. Serves invariant 1.
"""

from __future__ import annotations

import ast
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import pytest

_PACKAGE = Path(__file__).resolve().parents[1] / "src" / "log_foundry"
"""Derived from this file, never from ``log_foundry.__file__``: an editable install can resolve to
another tree, and a guard scanning the wrong tree passes because it found nothing."""

_NAMES = frozenset({"__name__", "__qualname__"})

_LOOKUPS = frozenset({"getattr", "hasattr", "attrgetter", "__getattribute__"})
"""Callables that run a metaclass property when handed the attribute's name."""

_FORMATS = frozenset({"format", "format_map"})

_FIELD = re.compile(r"\{[^{}]*\.(__name__|__qualname__)(?![\w])[^{}]*\}")
"""A replacement field whose attribute chain reads either name, such as ``{0.__class__.__name__}``."""

EXEMPT: dict[str, Counter[str]] = {
    "decorator._misordered": Counter(
        {"getattr(getattr(fn, '__func__', None), '__qualname__', None)": 1}
    ),
    "decorator._span_name": Counter({"getattr(fn, '__qualname__', None)": 1}),
    "_otel._type_name": Counter({"type(value).__name__": 1}),
}
"""The reads this rule does not govern, by enclosing function and exact expression.

The two ``decorator`` reads name a callable rather than a type. Both run at decoration, under
``Raises: Exception`` for whatever the lookup raises, and both keep the value only when its real
type is ``str``, so a hostile ``__qualname__`` can raise in the caller's frame, as documented, but
cannot rename a span. ``_otel._type_name`` is guarded and its fallback is pinned by SPEC-056's
mapping tests (``tests/test_otel_mapping.py``), as the module docstring says.
"""


@dataclass(frozen=True)
class Read:
    """One raw read of a type's name, where it sits and why it was reported.

    Attributes:
      where: The dotted module path and enclosing function, such as ``sinks.http._send``.
      line: The line the read starts on.
      expr: The read, unparsed.
      reason: Which of the module docstring's three checks reported it, and how.
    """

    where: str
    line: int
    expr: str
    reason: str


class _Finder(ast.NodeVisitor):
    """Collects every raw name read in one module, keyed by its enclosing function."""

    def __init__(self, module: str) -> None:
        self.module = module
        self.scope: list[str] = []
        self.reads: list[Read] = []

    def _where(self) -> str:
        return ".".join([self.module, *self.scope]) if self.module else ".".join(self.scope)

    def _report(self, node: ast.expr | ast.pattern, reason: str) -> None:
        self.reads.append(Read(self._where(), node.lineno, ast.unparse(node), reason))

    def _enter(self, node: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef) -> None:
        self.scope.append(node.name)
        self.generic_visit(node)
        self.scope.pop()

    visit_FunctionDef = _enter
    visit_AsyncFunctionDef = _enter
    visit_ClassDef = _enter

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if node.attr in _NAMES and isinstance(node.ctx, ast.Load):
            self._report(node, f"attribute read of {node.attr}")
        self.generic_visit(node)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        target = node.target
        if isinstance(target, ast.Attribute) and target.attr in _NAMES:
            self._report(target, f"augmented assignment reads {target.attr}")
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        called = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
        if called in _LOOKUPS:
            for arg in node.args:
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                    last = arg.value.rsplit(".", 1)[-1]
                    if last in _NAMES:
                        self._report(node, f"{called} of {arg.value}")
        if called in _FORMATS and isinstance(func, ast.Attribute):
            template = func.value
            if isinstance(template, ast.Name) and template.id == "str" and node.args:
                template = node.args[0]
            if isinstance(template, ast.Constant) and isinstance(template.value, str):
                field = _FIELD.search(template.value)
                if field is not None:
                    self._report(node, f"{called} field reads {field.group(1)}")
        self.generic_visit(node)

    def visit_MatchClass(self, node: ast.MatchClass) -> None:
        for attr in node.kwd_attrs:
            if attr in _NAMES:
                self._report(node, f"class pattern reads {attr}")
        self.generic_visit(node)


def reads(source: str, module: str = "") -> list[Read]:
    """Every raw name read in ``source``, in visiting order."""
    finder = _Finder(module)
    finder.visit(ast.parse(source))
    return finder.reads


def _modules() -> dict[str, str]:
    """Every module under the package, by dotted path relative to it, mapped to its source."""
    found: dict[str, str] = {}
    for path in sorted(_PACKAGE.rglob("*.py")):
        dotted = ".".join(path.relative_to(_PACKAGE).with_suffix("").parts)
        found[dotted] = path.read_text(encoding="utf-8")
    return found


def unexempt(found: list[Read], exempt: dict[str, Counter[str]]) -> list[Read]:
    """The reads ``exempt`` does not cover, each exemption spending one read of its expression."""
    budget = {where: Counter(exprs) for where, exprs in exempt.items()}
    left: list[Read] = []
    for read in found:
        spare = budget.get(read.where)
        if spare is not None and spare[read.expr] > 0:
            spare[read.expr] -= 1
        else:
            left.append(read)
    return left


def stale(found: list[Read], exempt: dict[str, Counter[str]]) -> list[str]:
    """Each exemption whose function no longer holds its expression exactly as many times."""
    held = Counter((r.where, r.expr) for r in found)
    return [
        f"{where}: {expr!r} held {held[(where, expr)]} time(s), exempted {count}"
        for where, exprs in exempt.items()
        for expr, count in exprs.items()
        if held[(where, expr)] != count
    ]


def _all_reads() -> list[Read]:
    found: list[Read] = []
    for module, source in _modules().items():
        found.extend(reads(source, module))
    return found


def test_the_walk_reaches_the_sinks_subpackage() -> None:
    """A glob that stopped at the top level would find none of the sink sites and pass."""
    modules = _modules()
    assert "sinks.http" in modules
    assert "_diag" in modules


def test_no_module_in_src_reads_a_type_name_through_the_attribute_protocol() -> None:
    """Every raw read is reported with its site and the spelling that replaces it."""
    left = unexempt(_all_reads(), EXEMPT)
    assert left == [], (
        "read a type's name with _diag.type_name(value), or "
        "_diag._STR_STR(_diag._STORED_NAME(cls)) for a class in hand; if a read is not a type's "
        "name, or a spec pins its output, record it in EXEMPT with its reason:\n"
        + "\n".join(f"  {r.where}:{r.line}: {r.expr} ({r.reason})" for r in left)
    )


def test_every_exemption_still_matches_its_read_exactly() -> None:
    """An exemption whose read moved or vanished is stale, and a stale one would hide a new read."""
    assert stale(_all_reads(), EXEMPT) == []


_STALE_SOURCE = (
    "def g(fn):\n"
    "    return getattr(fn, '__qualname__', None)\n"
    "def twice(fn):\n"
    "    getattr(fn, '__qualname__', None)\n"
    "    return getattr(fn, '__qualname__', None)\n"
)


@pytest.mark.parametrize(
    ("exempt", "expected"),
    [
        (
            {"m.g": Counter({"type(fn).__name__": 1})},
            ["m.g: 'type(fn).__name__' held 0 time(s), exempted 1"],
        ),
        (
            {"m.g": Counter({"getattr(fn, '__qualname__', None)": 2})},
            ["m.g: \"getattr(fn, '__qualname__', None)\" held 1 time(s), exempted 2"],
        ),
        (
            {"m.renamed": Counter({"getattr(fn, '__qualname__', None)": 1})},
            ["m.renamed: \"getattr(fn, '__qualname__', None)\" held 0 time(s), exempted 1"],
        ),
        (
            {"m.twice": Counter({"getattr(fn, '__qualname__', None)": 1})},
            ["m.twice: \"getattr(fn, '__qualname__', None)\" held 2 time(s), exempted 1"],
        ),
        ({"m.g": Counter({"getattr(fn, '__qualname__', None)": 1})}, []),
    ],
    ids=["read-vanished", "count-too-high", "function-renamed", "count-too-low", "exact-is-silent"],
)
def test_stale_reports_an_exemption_that_no_longer_matches_exactly(
    exempt: dict[str, Counter[str]], expected: list[str]
) -> None:
    """A vanished read, a count too high and a renamed function are each reported by name."""
    assert stale(reads(_STALE_SOURCE, "m"), exempt) == expected


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("def f(err):\n    return type(err).__name__\n", [("f", "attribute read of __name__")]),
        ("def f(x):\n    return type(x).__qualname__\n", [("f", "attribute read of __qualname__")]),
        ("def f(err):\n    return err.__class__.__name__\n", [("f", "attribute read of __name__")]),
        (
            "def f(err):\n    cls = type(err)\n    return cls.__name__\n",
            [("f", "attribute read of __name__")],
        ),
        (
            "def f(err):\n    return f'lost; {type(err).__name__}'\n",
            [("f", "attribute read of __name__")],
        ),
        (
            "def f(err):\n    return type(\n        err\n    ).__name__\n",
            [("f", "attribute read of __name__")],
        ),
        (
            "class S:\n    def r(self):\n        return (lambda: type(self).__name__)()\n",
            [("S.r", "attribute read of __name__")],
        ),
        ("def f(e):\n    return getattr(type(e), '__name__')\n", [("f", "getattr of __name__")]),
        (
            "def f(c):\n    return getattr(c, '__qualname__', '?')\n",
            [("f", "getattr of __qualname__")],
        ),
        ("def f(c):\n    return hasattr(c, '__name__')\n", [("f", "hasattr of __name__")]),
        (
            "import operator\ndef f(c):\n    return operator.attrgetter('__name__')(c)\n",
            [("f", "attrgetter of __name__")],
        ),
        (
            "def f(c):\n    return type.__getattribute__(c, '__name__')\n",
            [("f", "__getattribute__ of __name__")],
        ),
        (
            "def f(c):\n    c.__name__ += 'x'\n",
            [("f", "augmented assignment reads __name__")],
        ),
        ("x = type(1).__name__\n", [("", "attribute read of __name__")]),
        ("def f(e):\n    return type(e).__name__.lower()\n", [("f", "attribute read of __name__")]),
        ("def f(e):\n    return str(type(e).__name__)\n", [("f", "attribute read of __name__")]),
        ("def f(e, s):\n    s += type(e).__name__\n", [("f", "attribute read of __name__")]),
        (
            "async def f(e):\n    return type(e).__name__\n",
            [("f", "attribute read of __name__")],
        ),
        (
            "import operator\ndef f(e):\n    return operator.attrgetter('__class__.__name__')(e)\n",
            [("f", "attrgetter of __class__.__name__")],
        ),
        (
            "def f(e):\n    return '{0.__class__.__name__}'.format(e)\n",
            [("f", "format field reads __name__")],
        ),
        (
            "def f(c):\n    return 'x {c.__qualname__!r:>9}'.format_map({'c': c})\n",
            [("f", "format_map field reads __qualname__")],
        ),
        (
            "def f(e):\n    return str.format('{0.__name__}', type(e))\n",
            [("f", "format field reads __name__")],
        ),
        (
            "def f(c):\n    match c:\n        case type(__name__=n):\n            return n\n",
            [("f", "class pattern reads __name__")],
        ),
    ],
    ids=[
        "type-name",
        "type-qualname",
        "class-attr",
        "local-alias",
        "f-string",
        "wrapped-over-lines",
        "lambda-in-method",
        "getattr",
        "getattr-default",
        "hasattr",
        "attrgetter",
        "getattribute",
        "augmented-assignment",
        "module-level",
        "under-another-attribute",
        "inside-a-call",
        "augmented-value",
        "async-def",
        "attrgetter-dotted",
        "format-field",
        "format-map-field",
        "str-format",
        "class-pattern",
    ],
)
def test_the_finder_reports_each_raw_read_with_its_reason(
    source: str, expected: list[tuple[str, str]]
) -> None:
    """Each spelling of the read is reported once, at its enclosing function, for its reason."""
    assert [(r.where, r.reason) for r in reads(source)] == expected


@pytest.mark.parametrize(
    "source",
    [
        "from log_foundry import _diag\ndef f(err):\n    return _diag.type_name(err)\n",
        (
            "from log_foundry import _diag\n"
            "def f(e):\n    return _diag._STR_STR(_diag._STORED_NAME(type(e)))\n"
        ),
        "_STORED = type.__dict__['__name__'].__get__\n",
        "def f(c):\n    return vars(c)['__name__']\n",
        "def f(c):\n    c.__name__ = 'renamed'\n",
        "def f(c):\n    del c.__qualname__\n",
        "if __name__ == '__main__':\n    pass\n",
        "def f(c, attr):\n    return getattr(c, attr)\n",
        "def f(c):\n    return getattr(c, '__module__')\n",
        "import inspect\ndef f(c):\n    return inspect.getattr_static(c, '__name__')\n",
        "def f(g):\n    return dict.get(g, '__name__')\n",
        "def f(c):\n    return c.__name_x\n",
        'def f(err):\n    """Formerly read ``type(err).__name__``."""\n    return 1\n',
        "def f(err):\n    # type(err).__name__\n    return 1\n",
        "def f(e):\n    return '{0.__module__}'.format(e)\n",
        "def f(d):\n    return '{0[__name__]}'.format(d)\n",
        "def f(d):\n    return '{__name__}'.format(**d)\n",
        "def f(e):\n    return '{0.__name__x}'.format(e)\n",
        "def f(t, e):\n    return t.format(e)\n",
        "import operator\ndef f(e):\n    return operator.attrgetter('a.b')(e)\n",
        "def f(c):\n    match c:\n        case type(x=n):\n            return n\n",
    ],
    ids=[
        "helper",
        "inline-stored-read",
        "accessor-binding",
        "class-dict",
        "store",
        "delete",
        "module-global",
        "getattr-variable",
        "getattr-other-dunder",
        "getattr-static",
        "dict-get",
        "lookalike-attribute",
        "docstring",
        "comment",
        "format-other-dunder",
        "format-index",
        "format-keyword",
        "format-lookalike",
        "format-non-literal",
        "attrgetter-dotted-other",
        "class-pattern-other",
    ],
)
def test_the_finder_is_silent_on_reads_that_run_no_metaclass_property(source: str) -> None:
    """The stored-name spellings, stores, deletes and text that only mentions a read stay silent."""
    assert reads(source) == []


def test_an_exemption_covers_only_its_own_expression_and_count() -> None:
    """A second copy, another expression, or the same one elsewhere is still reported."""
    source = (
        "def g(fn):\n"
        "    a = getattr(fn, '__qualname__', None)\n"
        "    b = getattr(fn, '__qualname__', None)\n"
        "    return type(fn).__name__\n"
        "def h(fn):\n"
        "    return getattr(fn, '__qualname__', None)\n"
    )
    exempt = {"m.g": Counter({"getattr(fn, '__qualname__', None)": 1})}
    left = unexempt(reads(source, "m"), exempt)
    assert [(r.where, r.line, r.reason) for r in left] == [
        ("m.g", 3, "getattr of __qualname__"),
        ("m.g", 4, "attribute read of __name__"),
        ("m.h", 6, "getattr of __qualname__"),
    ]
