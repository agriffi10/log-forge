"""SPEC-071 FR-005 — an argument door asks the type, never the value: held over the AST.

A *door* is a function raising a `TypeError` that names the type of one of its parameters (not
`self` or `cls`) through the type's stored name: `_diag._STORED_NAME`, `_STORED_NAME` or
`decorator._stored_name`. Three checks over `src/log_foundry/**/*.py`:

1. A `raise TypeError(...)` whose message formats `type(x).__name__` or `x.__class__.__name__`, for
   an `x` other than `self` or `cls`, is reported — a metaclass `__name__` property answers the
   first and the value's own `__class__` the second. Two raises are exempt, named and held
   exactly: `sanitize._coerce_unbounded` and `TransformSink._apply` judge a transform's *result*
   once per event, where invariants 2 and 8 govern and invariant 13 does not.
2. An `isinstance(p, ...)` on a parameter of a door, or on the parameter of a same-module function
   a door passes `p` to (one level, as `test_numeric_door_roster.py` follows), is reported:
   `isinstance` falls back to the value's `__class__`. The one exemption is
   `decorator._refuse_unusable`'s kept proxy test, SPEC-080's on purpose, and it is held as an
   exact multiset of its two expressions *after* the function's `not callable(fn)` refusal, so a
   rewritten earlier test, or a second copy of an exempt one, is still reported. A read of
   `p.__class__` anywhere in a door's body (`if not value.__class__ is int:`) is reported too,
   since it asks the value what it is by another spelling.
3. The derived set of doors equals, exactly, the ten written out below from `2368ade3`: a door
   removed, renamed or inlined back reddens by name, and a new one is a visible edit.

**What it does not see**, each a deliberate edge its corpus asserts silence on: a type named by a
helper (`raise TypeError(_type_name(value))`), a message held in a local and raised later, a
`ValueError` door (the HTTP sinks' `auth`, held by `tests/test_type_doors.py`), a
`TypeError` naming no type, and a parameter rebound to a local first (`v = value;
isinstance(v, int)`), which check 2 does not follow.

Its mutation sweep rewrites each door's `issubclass(type(p), T)` back to `isinstance(p, T)`, and
each stored-name read back to `type(p).__name__`, in memory, and requires a report for every one;
every door must contribute at least one of each. Serves invariants 13 and 6.
"""

from __future__ import annotations

import ast
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

_PACKAGE = Path(__file__).resolve().parents[1] / "src" / "log_foundry"
"""Derived from this file, never from ``log_foundry.__file__``: an editable install can resolve to
another tree, and a guard scanning the wrong tree passes because it found nothing."""

EXPECTED_DOORS = frozenset(
    {
        "decorator.trace",
        "decorator._refuse_unusable",
        "config._require_positive",
        "config._require_text",
        "sinks._retry.exact_seconds",
        "sinks._retry.require_positive",
        "sinks._retry.exact_int",
        "sinks._retry.wait_budget",
        "sinks.otlp.OTLPSink.__init__",
        "sinks._args.require_callable",
    }
)
"""The ten doors: SPEC-080's two, the seven SPEC-071 converts, and ``require_callable``."""

RAISE_EXEMPT = frozenset({"sanitize._coerce_unbounded", "sinks.transform.TransformSink._apply"})
"""Raises judging a transform's result per event, not an argument."""

PROXY_EXEMPT = "decorator._refuse_unusable"
PROXY_EXPRESSIONS = Counter(
    {"isinstance(fn, (classmethod, staticmethod))": 1, "isinstance(fn, classmethod)": 1}
)
"""The kept proxy test, SPEC-080's, as an exact multiset after the ``not callable(fn)`` refusal."""

_STORED = frozenset(
    {"_diag._STORED_NAME", "_STORED_NAME", "_stored_name", "decorator._stored_name"}
)
_OWNERS = frozenset({"self", "cls"})


@dataclass
class _Function:
    """One function, as the checks need it.

    Attributes:
      qual: ``module.Qual.name``.
      rel: The file, relative to the package.
      node: Its AST.
      params: Its parameters, ``self`` and ``cls`` excluded, in order.
      calls: Same-module calls in its own body, by callee name.
    """

    qual: str
    rel: str
    node: ast.FunctionDef | ast.AsyncFunctionDef
    params: list[str]
    calls: list[ast.Call] = field(default_factory=list)


def _own_nodes(func: ast.AST) -> list[ast.AST]:
    """Returns every node of a function's body, not descending into nested functions.

    Args:
      func: The function.

    Returns:
      Its own nodes.

    Raises:
      None.
    """
    out: list[ast.AST] = []
    stack = list(ast.iter_child_nodes(func))
    while stack:
        node = stack.pop()
        out.append(node)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda):
            continue
        stack.extend(ast.iter_child_nodes(node))
    return out


def _functions(rel: str, tree: ast.Module) -> dict[str, _Function]:
    """Indexes every function and method of a module by qualified name.

    Args:
      rel: The file, relative to the package.
      tree: Its AST.

    Returns:
      Every function, keyed ``module.Qual.name``.

    Raises:
      None.
    """
    module = rel.removesuffix(".py").replace("/", ".")
    found: dict[str, _Function] = {}

    def visit(node: ast.AST, scope: tuple[str, ...]) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.ClassDef):
                visit(child, (*scope, child.name))
            elif isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef):
                args = child.args
                params = [
                    a.arg
                    for a in [*args.posonlyargs, *args.args, *args.kwonlyargs]
                    if a.arg not in _OWNERS
                ]
                qual = ".".join((module, *scope, child.name))
                found[qual] = _Function(qual, rel, child, params)
                visit(child, (*scope, child.name))

    visit(tree, ())
    return found


def _type_of_param(node: ast.AST, params: list[str]) -> str | None:
    """Returns ``p`` when ``node`` is ``type(p)`` for a parameter ``p``.

    Args:
      node: Any node.
      params: The parameters that count.

    Returns:
      The parameter, or ``None``.

    Raises:
      None.
    """
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "type"
        and len(node.args) == 1
        and isinstance(node.args[0], ast.Name)
        and node.args[0].id in params
    ):
        return node.args[0].id
    return None


def _type_errors(func: _Function) -> list[ast.Raise]:
    return [
        n
        for n in _own_nodes(func.node)
        if isinstance(n, ast.Raise)
        and isinstance(n.exc, ast.Call)
        and isinstance(n.exc.func, ast.Name)
        and n.exc.func.id == "TypeError"
    ]


def _is_door(func: _Function) -> bool:
    for raise_ in _type_errors(func):
        for node in ast.walk(raise_):
            if (
                isinstance(node, ast.Call)
                and ast.unparse(node.func) in _STORED
                and node.args
                and _type_of_param(node.args[0], func.params)
            ):
                return True
    return False


def _proxy_findings(func: _Function, findings: list[tuple[int, str]]) -> list[str]:
    """Applies the kept proxy test's exemption to ``_refuse_unusable``'s ``isinstance`` findings.

    Args:
      func: ``decorator._refuse_unusable``.
      findings: Its ``(line, expression)`` findings.

    Returns:
      The findings that remain, and a line for a missing anchor or a missing exempt expression.

    Raises:
      None.
    """
    anchors = [
        n.lineno
        for n in _own_nodes(func.node)
        if isinstance(n, ast.If)
        and isinstance(n.test, ast.UnaryOp)
        and isinstance(n.test.op, ast.Not)
        and isinstance(n.test.operand, ast.Call)
        and isinstance(n.test.operand.func, ast.Name)
        and n.test.operand.func.id == "callable"
    ]
    if not anchors:
        return [
            f"{func.rel} ({func.qual}): the `not callable` refusal anchoring the exemption is gone"
        ]
    anchor = min(anchors)
    out = [
        f"{func.rel}:{line} ({func.qual}): {expr} in a door"
        for line, expr in findings
        if line <= anchor
    ]
    after = Counter(expr for line, expr in findings if line > anchor)
    for expr, count in sorted((after - PROXY_EXPRESSIONS).items()):
        out.append(f"{func.rel} ({func.qual}): {expr} in a door, {count} beyond the exemption")
    out += [
        f"{func.rel} ({func.qual}): stale exemption {expr}"
        for expr in sorted(PROXY_EXPRESSIONS - after)
    ]
    return out


def scan(sources: dict[str, str], expected: frozenset[str] = EXPECTED_DOORS) -> list[str]:
    """Runs the three checks and names every violation.

    Args:
      sources: Source text keyed by path relative to the package.
      expected: The door set written out by name.

    Returns:
      One line per violation; empty when the tree is clean.

    Raises:
      SyntaxError: If a source does not parse.
    """
    out: list[str] = []
    doors: set[str] = set()
    matched_exempt: set[str] = set()
    for rel in sorted(sources):
        functions = _functions(rel, ast.parse(sources[rel]))
        by_name = {q.rsplit(".", 1)[1]: f for q, f in functions.items()}
        for func in functions.values():
            for raise_ in _type_errors(func):
                named = []
                for node in ast.walk(raise_):
                    if isinstance(node, ast.Attribute) and node.attr == "__name__":
                        value = node.value
                        if (
                            isinstance(value, ast.Call)
                            and isinstance(value.func, ast.Name)
                            and value.func.id == "type"
                            and value.args
                            and ast.unparse(value.args[0]) not in _OWNERS
                        ):
                            named.append(f"type({ast.unparse(value.args[0])}).__name__")
                        if (
                            isinstance(value, ast.Attribute)
                            and value.attr == "__class__"
                            and ast.unparse(value.value) not in _OWNERS
                        ):
                            named.append(f"{ast.unparse(value.value)}.__class__.__name__")
                if named and func.qual in RAISE_EXEMPT:
                    matched_exempt.add(func.qual)
                    continue
                out += [
                    f"{rel}:{raise_.lineno} ({func.qual}): a TypeError names {n}" for n in named
                ]
            if not _is_door(func):
                continue
            doors.add(func.qual)
            out += [
                f"{rel}:{node.lineno} ({func.qual}): reads {node.value.id}.__class__ in a door"
                for node in _own_nodes(func.node)
                if isinstance(node, ast.Attribute)
                and node.attr == "__class__"
                and isinstance(node.value, ast.Name)
                and node.value.id in func.params
            ]
            findings: list[tuple[int, str]] = []
            checked = [(func, set(func.params))]
            for call in (n for n in _own_nodes(func.node) if isinstance(n, ast.Call)):
                callee = call.func.id if isinstance(call.func, ast.Name) else None
                helper = by_name.get(callee or "")
                if helper is None or helper is func:
                    continue
                received = {
                    helper.params[i]
                    for i, a in enumerate(call.args)
                    if i < len(helper.params) and isinstance(a, ast.Name) and a.id in func.params
                }
                received |= {
                    k.arg
                    for k in call.keywords
                    if k.arg in helper.params
                    and isinstance(k.value, ast.Name)
                    and k.value.id in func.params
                }
                if received:
                    checked.append((helper, received))
            for owner, params in checked:
                for node in _own_nodes(owner.node):
                    if (
                        isinstance(node, ast.Call)
                        and isinstance(node.func, ast.Name)
                        and node.func.id == "isinstance"
                        and node.args
                        and isinstance(node.args[0], ast.Name)
                        and node.args[0].id in params
                    ):
                        expr = ast.unparse(node)
                        if owner is func:
                            findings.append((node.lineno, expr))
                        else:
                            out.append(
                                f"{rel}:{node.lineno} ({func.qual} -> {owner.qual}): {expr} in a "
                                "helper a door passes its argument to"
                            )
            if func.qual == PROXY_EXEMPT:
                out += _proxy_findings(func, findings)
            else:
                out += [f"{rel}:{line} ({func.qual}): {expr} in a door" for line, expr in findings]
    out += [f"door set: unexpected {q}" for q in sorted(doors - expected)]
    out += [f"door set: missing {q}" for q in sorted(expected - doors)]
    present = {q for rel in sources for q in _functions(rel, ast.parse(sources[rel]))}
    out += [f"stale exemption {q}" for q in sorted((RAISE_EXEMPT & present) - matched_exempt)]
    return out


def _tree() -> dict[str, str]:
    return {
        p.relative_to(_PACKAGE).as_posix(): p.read_text(encoding="utf-8")
        for p in sorted(_PACKAGE.rglob("*.py"))
    }


def test_the_real_tree_is_clean_and_holds_the_ten_doors() -> None:
    assert scan(_tree()) == []


def test_the_two_raise_exemptions_still_match_a_raise() -> None:
    """A stale exemption is reported, so the exempt set cannot outlive what it excused."""
    tree = _tree()
    tree["sanitize.py"] = tree["sanitize.py"].replace("type(result).__name__", "'gone'")
    assert "stale exemption sanitize._coerce_unbounded" in scan(tree)


# --- corpus: what it reports ----------------------------------------------------------------------


_DOOR_OK = (
    "def door(value, name):\n"
    "    if not issubclass(type(value), int):\n"
    "        raise TypeError(f'{name} must be an int, not {_diag._STORED_NAME(type(value))}')\n"
)
"""A well-formed door, which the corpus mutates one way per case."""


def _one(source: str, expected: frozenset[str] = frozenset({"m.door"})) -> list[str]:
    return scan({"m.py": source}, expected)


def test_it_is_silent_on_a_well_formed_door() -> None:
    assert _one(_DOOR_OK) == []


def test_it_reports_a_door_written_with_isinstance() -> None:
    source = _DOOR_OK.replace("issubclass(type(value), int)", "isinstance(value, int)")
    assert _one(source) == ["m.py:2 (m.door): isinstance(value, int) in a door"]


def test_it_reports_a_type_named_through_dunder_name() -> None:
    source = _DOOR_OK.replace("_diag._STORED_NAME(type(value))", "type(value).__name__")
    assert _one(source, frozenset()) == ["m.py:3 (m.door): a TypeError names type(value).__name__"]


def test_it_reports_a_type_named_through_dunder_class() -> None:
    source = _DOOR_OK.replace("_diag._STORED_NAME(type(value))", "value.__class__.__name__")
    assert _one(source, frozenset()) == [
        "m.py:3 (m.door): a TypeError names value.__class__.__name__"
    ]


def test_it_reports_a_door_whose_helper_tests_with_isinstance() -> None:
    source = _DOOR_OK + (
        "    return _exact(value)\n"
        "def _exact(number):\n"
        "    if isinstance(number, int):\n"
        "        return int.__index__(number)\n"
        "    return number\n"
    )
    assert _one(source) == [
        (
            "m.py:6 (m.door -> m._exact): isinstance(number, int) in a helper a door passes "
            "its argument to"
        )
    ]


def test_it_reports_a_door_set_that_differs_from_the_written_one() -> None:
    assert _one(_DOOR_OK, frozenset({"m.other"})) == [
        "door set: unexpected m.door",
        "door set: missing m.other",
    ]


# --- corpus: what it stays silent on --------------------------------------------------------------


def test_it_is_silent_on_an_owner_named_through_type_self() -> None:
    source = (
        "class S:\n"
        "    def __init__(self, value):\n"
        "        if not issubclass(type(value), str):\n"
        "            raise TypeError(f'{type(self).__name__} value must be a str, not '\n"
        "                            f'{_diag._STORED_NAME(type(value))}')\n"
    )
    assert scan({"m.py": source}, frozenset({"m.S.__init__"})) == []


def test_it_is_silent_on_isinstance_in_a_function_that_raises_no_type_error() -> None:
    source = "def f(value):\n    if isinstance(value, int):\n        return 1\n    return 0\n"
    assert _one(source, frozenset()) == []


def test_it_reports_a_dunder_class_read_anywhere_in_a_door() -> None:
    source = _DOOR_OK.replace(
        "    if not issubclass(type(value), int):\n",
        "    if not value.__class__ is int:\n",
    )
    assert _one(source) == ["m.py:2 (m.door): reads value.__class__ in a door"]


def test_it_is_silent_on_a_parameter_rebound_before_its_isinstance() -> None:
    source = _DOOR_OK.replace(
        "    if not issubclass(type(value), int):\n",
        "    v = value\n    if not isinstance(v, int):\n",
    )
    assert _one(source) == []


def test_it_is_silent_on_its_four_recorded_edges() -> None:
    helper_built = (
        "def a(value):\n"
        "    if isinstance(value, int):\n"
        "        raise TypeError(_type_name(value))\n"
    )
    held_in_a_local = (
        "def b(value):\n"
        "    message = f'not {type(value).__name__}'\n"
        "    if isinstance(value, int):\n"
        "        raise TypeError(message)\n"
    )
    value_error_door = (
        "def c(value):\n"
        "    if isinstance(value, int):\n"
        "        raise ValueError(f'not {type(value).__name__}')\n"
    )
    names_no_type = (
        "def d(value):\n    if isinstance(value, int):\n        raise TypeError('bad')\n"
    )
    for source in (helper_built, held_in_a_local, value_error_door, names_no_type):
        assert _one(source, frozenset()) == [], source


_PROXY = (
    "def _refuse_unusable(fn):\n"
    "    if issubclass(type(fn), str):\n"
    "        raise TypeError('str')\n"
    "    if issubclass(type(fn), (classmethod, staticmethod)):\n"
    "        raise TypeError(_misordered(_stored_name(type(fn)), fn))\n"
    "    if not callable(fn):\n"
    "        raise TypeError(f'needs a callable, got {_stored_name(type(fn))}')\n"
    "    if isinstance(fn, (classmethod, staticmethod)):\n"
    "        kind = 'classmethod' if isinstance(fn, classmethod) else 'staticmethod'\n"
    "        raise TypeError(_misordered(kind, fn))\n"
)
"""SPEC-080's shape: two real-type tests, the refusal, then the kept proxy test."""


def _proxy(source: str) -> list[str]:
    return scan({"decorator.py": source}, frozenset({"decorator._refuse_unusable"}))


def test_the_proxy_exemption_covers_the_kept_test_and_nothing_else() -> None:
    assert _proxy(_PROXY) == []
    rewritten = _PROXY.replace(
        "issubclass(type(fn), (classmethod, staticmethod))",
        "isinstance(fn, (classmethod, staticmethod))",
    )
    assert _proxy(rewritten) == [
        (
            "decorator.py:4 (decorator._refuse_unusable): "
            "isinstance(fn, (classmethod, staticmethod)) in a door"
        )
    ]


def test_a_second_copy_of_an_exempt_expression_is_reported() -> None:
    duplicated = _PROXY + "    if isinstance(fn, classmethod):\n        raise TypeError('again')\n"
    assert _proxy(duplicated) == [
        (
            "decorator.py (decorator._refuse_unusable): isinstance(fn, classmethod) in a door, "
            "1 beyond the exemption"
        )
    ]


def test_a_missing_anchor_or_exempt_expression_is_reported() -> None:
    no_anchor = _PROXY.replace("if not callable(fn):", "if fn is None:")
    assert _proxy(no_anchor) == [
        (
            "decorator.py (decorator._refuse_unusable): the `not callable` refusal anchoring "
            "the exemption is gone"
        )
    ]
    gone = _PROXY.replace(
        "kind = 'classmethod' if isinstance(fn, classmethod) else 'staticmethod'", "kind = 'x'"
    )
    assert _proxy(gone) == [
        "decorator.py (decorator._refuse_unusable): stale exemption isinstance(fn, classmethod)"
    ]


# --- the mutation sweep -------------------------------------------------------------------------


class _Rewrite(ast.NodeTransformer):
    """Rewrites the one call at ``(line, col)`` back into the shape the guard refuses."""

    def __init__(self, line: int, col: int) -> None:
        self.line = line
        self.col = col
        self.done = False

    def visit_Call(self, node: ast.Call) -> ast.AST:
        self.generic_visit(node)
        if (node.lineno, node.col_offset) != (self.line, self.col):
            return node
        self.done = True
        if isinstance(node.func, ast.Name) and node.func.id == "issubclass":
            subject = node.args[0]
            assert isinstance(subject, ast.Call)
            return ast.Call(ast.Name("isinstance", ast.Load()), [subject.args[0], node.args[1]], [])
        return ast.Attribute(node.args[0], "__name__", ast.Load())


def _plants(tree: dict[str, str]) -> list[tuple[str, str, int, int, str]]:
    """Returns every site the sweep rewrites: (file, door, line, col, kind).

    Args:
      tree: Source text keyed by path relative to the package.

    Returns:
      Each ``issubclass(type(p), T)`` in a door or a helper it passes ``p`` to, and each
      stored-name read in a door's ``TypeError``.

    Raises:
      None.
    """
    plants: list[tuple[str, str, int, int, str]] = []
    for rel in sorted(tree):
        functions = _functions(rel, ast.parse(tree[rel]))
        by_name = {q.rsplit(".", 1)[1]: f for q, f in functions.items()}
        for func in functions.values():
            if func.qual not in EXPECTED_DOORS:
                continue
            owners = [(func, set(func.params))]
            for call in (n for n in _own_nodes(func.node) if isinstance(n, ast.Call)):
                helper = by_name.get(call.func.id if isinstance(call.func, ast.Name) else "")
                if (
                    helper is not None
                    and helper is not func
                    and any(isinstance(a, ast.Name) and a.id in func.params for a in call.args)
                ):
                    owners.append((helper, set(helper.params)))
            plants += [
                (rel, func.qual, node.lineno, node.col_offset, "issubclass")
                for owner, params in owners
                for node in _own_nodes(owner.node)
                if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "issubclass"
                and _type_of_param(node.args[0], list(params))
            ]
            plants += [
                (rel, func.qual, node.lineno, node.col_offset, "stored")
                for raise_ in _type_errors(func)
                for node in ast.walk(raise_)
                if isinstance(node, ast.Call)
                and ast.unparse(node.func) in _STORED
                and node.args
                and _type_of_param(node.args[0], func.params)
            ]
    return sorted(set(plants))


def test_every_door_site_rewritten_back_is_reported() -> None:
    tree = _tree()
    plants = _plants(tree)
    for rel, door, line, col, kind in plants:
        rewrite = _Rewrite(line, col)
        mutant = ast.unparse(ast.fix_missing_locations(rewrite.visit(ast.parse(tree[rel]))))
        assert rewrite.done, f"the premise: {rel}:{line}:{col} was rewritten"
        reported = scan({**tree, rel: mutant})
        assert any(f"({door}" in r or f"missing {door}" in r for r in reported), (
            f"{door}: the {kind} site at {rel}:{line} went unreported: {reported}"
        )
    kinds = {door: {kind for _, d, _, _, kind in plants if d == door} for door in EXPECTED_DOORS}
    assert {door: sorted(k) for door, k in kinds.items() if k != {"issubclass", "stored"}} == {}, (
        "every door contributes at least one issubclass plant and one stored-name plant"
    )
