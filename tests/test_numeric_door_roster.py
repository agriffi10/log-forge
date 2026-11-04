"""SPEC-066 FR-005 — a structural guard that derives the int doors and fails naming one.

The defect SPEC-066 closes — an ``int``-annotated constructor argument floored by ``max``, capped
by ``min`` or range-checked by a comparison, and never judged for type — was found three times
(SPEC-049, SPEC-065, SPEC-066), each time by a sweep and never by a test. So the sites are derived
from the AST of every ``__init__`` in ``sinks/`` (subpackages included) and ``worker.py`` rather
than listed: a new door left on a bare ``max`` is a finding, and there is no exemption list — a
legitimate unjudged site is a finding on the helper or the spec, not an entry here.

**Two outputs.** *Findings*: a parameter annotated ``int``, or ``int`` unioned only with ``None``
however it is spelled (``int | None``, ``Optional[int]``, ``Union[int, None]``, a string
annotation), that appears in a ``max(...)`` or ``min(...)`` call or a comparison other than
``is``/``is not`` and is not also an argument of one of the three judges, ``exact_int``,
``floor_count`` or ``require_positive``. ``min`` is a shape because ``min(nan, 10)`` is ``nan``:
a cap passes a float through exactly as a floor does. Nothing else silences it — ``isinstance``,
``require_timeout`` and ``usable_timeout`` refuse nothing for an ``int``. *Judged set*: every such
parameter that is an argument of a judge, which the ``max_retries`` roster of FR-001 is compared
with.

**One call level is followed.** When ``__init__`` passes the parameter itself (a bare name, not an
expression) to a function defined in the same module or to a method of the same class called as
``self.<name>(...)``, the callee's matching parameter is judged in the callee's body by the same
rules, and what is found there is reported against the ``__init__`` parameter, the shape naming
the callee (``max in _floor``). So moving a judgement into a helper keeps the parameter judged,
and moving a bare ``max`` into one is still a finding.

**The derived sweep** rewrites each ``floor_count``/``exact_int`` call that judges a parameter,
in ``__init__`` or in a callee followed from it, in memory — ``floor_count(x, N, ...)`` to
``max(x, N)``, ``exact_int(x, ...)`` to ``x`` — and asserts the scan then reports that call's
site, so deleting one helper call in a source file reddens this module. No file is written. The
population floors (at least nineteen judged, at least sixteen helper calls) keep a glob, a rename
or a call-finder that matches nothing from passing vacuously; both leave headroom below the
build's counts, so an unrelated constructor can go without tripping them.

**Limits, stated rather than discovered.** Removing an ``exact_int`` call is reported only because
a comparison on the rebound parameter follows it (``port``'s range, ``max_reconnect_attempts``'s
positivity). A future door that is a pure type judgement — ``self.n = exact_int(n, ...)`` with no
comparison downstream — leaves nothing for the scan to see once removed, so it would fail the
sweep with no exemption available: that spec must add a shape to the scan or route the value
through ``floor_count``. And the follow is one level deep, into the same module only, for a
parameter passed as a bare name: a bare ``max`` two calls down, in a helper imported from another
module, or on ``p + 1`` passed to a helper is not seen. Those are the edges of the population, not
exemptions; a door built that way is a review finding.
"""

from __future__ import annotations

import ast
import pathlib
import textwrap
from dataclasses import dataclass

import pytest

_PKG = pathlib.Path(__file__).resolve().parents[1] / "src" / "log_foundry"

JUDGES = frozenset({"exact_int", "floor_count", "require_positive"})
_HELPERS = frozenset({"exact_int", "floor_count"})
_BOUNDS = frozenset({"max", "min"})
_REMEDY = (
    "route each through floor_count, exact_int or require_positive (sinks/_retry.py) before "
    "anything is built; if a judge was renamed, add the new name to JUDGES (and _HELPERS) in "
    "tests/test_numeric_door_roster.py"
)


@dataclass(frozen=True)
class HelperCall:
    """One ``floor_count``/``exact_int`` call judging an examined ``__init__``'s parameter."""

    file: str
    site: str
    kind: str
    lineno: int
    col: int
    names: tuple[str, ...]


@dataclass(frozen=True)
class Scan:
    """What a scan of some source found."""

    findings: tuple[str, ...]
    judged: frozenset[str]
    helper_calls: tuple[HelperCall, ...]


@dataclass
class _Body:
    """What one function body says about the parameters it was asked about."""

    judged: set[str]
    shapes: dict[str, set[str]]
    helpers: list[tuple[ast.Call, str]]


def _callee(node: ast.Call) -> str | None:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


def _names_in(nodes: list[ast.expr], params: set[str]) -> set[str]:
    """The parameters named anywhere inside the given expressions' subtrees."""
    found: set[str] = set()
    for expr in nodes:
        for node in ast.walk(expr):
            if isinstance(node, ast.Name) and node.id in params:
                found.add(node.id)
    return found


def _members(node: ast.expr) -> list[str] | None:
    """The members an annotation unions, unparsed, or ``None`` when it is not a plain union."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        try:
            return _members(ast.parse(node.value, mode="eval").body)
        except SyntaxError:
            return None
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
        left, right = _members(node.left), _members(node.right)
        return None if left is None or right is None else [*left, *right]
    if isinstance(node, ast.Subscript):
        head = ast.unparse(node.value)
        parts = node.slice.elts if isinstance(node.slice, ast.Tuple) else [node.slice]
        if head in ("Optional", "typing.Optional"):
            parts = [*parts, ast.Constant(None)]
        elif head not in ("Union", "typing.Union"):
            return None
        found: list[str] = []
        for part in parts:
            inner = _members(part)
            if inner is None:
                return None
            found.extend(inner)
        return found
    return [ast.unparse(node)]


def _is_int_annotation(node: ast.expr) -> bool:
    """Whether an annotation is ``int`` alone or unioned only with ``None``, however spelled."""
    members = _members(node)
    return members is not None and "int" in members and set(members) <= {"int", "None"}


def _int_params(fn: ast.FunctionDef) -> set[str]:
    args = fn.args
    return {
        arg.arg
        for arg in (*args.posonlyargs, *args.args, *args.kwonlyargs)
        if arg.annotation is not None and _is_int_annotation(arg.annotation)
    }


def _judge_body(fn: ast.FunctionDef, params: set[str]) -> _Body:
    """Reads one body for judge calls, bound calls and comparisons on the given parameters."""
    body = _Body(set(), {}, [])
    for node in ast.walk(fn):
        if isinstance(node, ast.Call):
            callee = _callee(node)
            arguments = [*node.args, *(k.value for k in node.keywords)]
            if callee in JUDGES:
                body.judged |= _names_in(arguments, params)
            if callee in _HELPERS:
                first = sorted(_names_in(node.args[:1], params))
                body.helpers.append((node, first[0] if len(first) == 1 else ""))
            if isinstance(node.func, ast.Name) and node.func.id in _BOUNDS:
                for name in _names_in(arguments, params):
                    body.shapes.setdefault(name, set()).add(node.func.id)
        if isinstance(node, ast.Compare) and not all(
            isinstance(op, ast.Is | ast.IsNot) for op in node.ops
        ):
            for name in _names_in([node], params):
                body.shapes.setdefault(name, set()).add("compare")
    return body


def _positional(fn: ast.FunctionDef, is_method: bool) -> list[str]:
    """The callee's positional parameter names, ``self`` dropped for a method called on it."""
    names = [arg.arg for arg in (*fn.args.posonlyargs, *fn.args.args)]
    static = any(isinstance(d, ast.Name) and d.id == "staticmethod" for d in fn.decorator_list)
    return names[1:] if is_method and not static else names


def _followed(
    init: ast.FunctionDef,
    params: set[str],
    functions: dict[str, ast.FunctionDef],
    methods: dict[str, ast.FunctionDef],
) -> list[tuple[ast.FunctionDef, dict[str, str]]]:
    """Each same-module callee ``__init__`` passes a parameter to, mapped callee name to ours."""
    followed: list[tuple[ast.FunctionDef, dict[str, str]]] = []
    for node in ast.walk(init):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Name) and func.id in functions:
            target, is_method = functions[func.id], False
        elif (
            isinstance(func, ast.Attribute)
            and isinstance(func.value, ast.Name)
            and func.value.id == "self"
            and func.attr in methods
        ):
            target, is_method = methods[func.attr], True
        else:
            continue
        positional = _positional(target, is_method)
        accepted = {*positional, *(arg.arg for arg in target.args.kwonlyargs)}
        mapping: dict[str, str] = {}
        for index, arg in enumerate(node.args):
            if isinstance(arg, ast.Starred):
                break
            if isinstance(arg, ast.Name) and arg.id in params and index < len(positional):
                mapping[positional[index]] = arg.id
        for keyword in node.keywords:
            value = keyword.value
            if keyword.arg in accepted and isinstance(value, ast.Name) and value.id in params:
                mapping[keyword.arg] = value.id
        if mapping:
            followed.append((target, mapping))
    return followed


def scan(source: str, filename: str) -> Scan:
    """Scans one module's source text for unjudged int doors in its constructors."""
    findings: list[str] = []
    judged: set[str] = set()
    helpers: dict[tuple[str, int, int], HelperCall] = {}
    module = ast.parse(source)
    functions = {node.name: node for node in module.body if isinstance(node, ast.FunctionDef)}
    for cls in ast.walk(module):
        if not isinstance(cls, ast.ClassDef):
            continue
        methods = {node.name: node for node in cls.body if isinstance(node, ast.FunctionDef)}
        init = methods.get("__init__")
        if init is None:
            continue
        params = _int_params(init)
        if not params:
            continue
        own = _judge_body(init, params)
        named_by_judge = set(own.judged)
        shapes = {name: set(found) for name, found in own.shapes.items()}
        located = list(own.helpers)
        for target, mapping in _followed(init, params, functions, methods):
            inner = _judge_body(target, set(mapping))
            named_by_judge |= {mapping[name] for name in inner.judged}
            for name, found in inner.shapes.items():
                shapes.setdefault(mapping[name], set()).update(
                    f"{shape} in {target.name}" for shape in found
                )
            located.extend((call, mapping[name]) for call, name in inner.helpers if name in mapping)
        for call, name in located:
            site = f"{filename}:{cls.name}.{name}" if name else ""
            helpers.setdefault(
                (site, call.lineno, call.col_offset),
                HelperCall(
                    filename,
                    site,
                    _callee(call) or "",
                    call.lineno,
                    call.col_offset,
                    (name,) if name else (),
                ),
            )
        for name in sorted(params):
            key = f"{filename}:{cls.name}.{name}"
            if name in named_by_judge:
                judged.add(key)
            else:
                findings.extend(f"{key}: {shape}" for shape in sorted(shapes.get(name, ())))
    return Scan(tuple(sorted(findings)), frozenset(judged), tuple(helpers.values()))


def scanned_files(root: pathlib.Path) -> list[pathlib.Path]:
    """The population: every module under ``sinks/``, subpackages included, and the worker."""
    return [*sorted((root / "sinks").rglob("*.py")), root / "worker.py"]


def scan_tree(root: pathlib.Path) -> Scan:
    """Scans the package rooted at ``root``, naming each file relative to it."""
    findings: list[str] = []
    judged: set[str] = set()
    helpers: list[HelperCall] = []
    for path in scanned_files(root):
        result = scan(path.read_text(encoding="utf-8"), path.relative_to(root).as_posix())
        findings.extend(result.findings)
        judged |= result.judged
        helpers.extend(result.helper_calls)
    return Scan(tuple(sorted(findings)), frozenset(judged), tuple(helpers))


class _RemoveHelper(ast.NodeTransformer):
    """Rewrites the one helper call at a position into what it replaced."""

    def __init__(self, lineno: int, col: int) -> None:
        self.lineno = lineno
        self.col = col
        self.replaced = 0

    def visit_Call(self, node: ast.Call) -> ast.AST:
        self.generic_visit(node)
        if node.lineno != self.lineno or node.col_offset != self.col:
            return node
        callee = _callee(node)
        if callee == "floor_count":
            assert len(node.args) >= 2, (
                f"floor_count at line {node.lineno} has no positional floor argument: pass the "
                "value and the floor positionally, as every call does, so the mutant can be built"
            )
            self.replaced += 1
            return ast.Call(ast.Name("max", ast.Load()), list(node.args[:2]), [])
        if callee == "exact_int":
            self.replaced += 1
            return node.args[0]
        return node


def without_helper(source: str, call: HelperCall) -> str:
    """Returns ``source`` with one helper call rewritten, never touching a file."""
    remover = _RemoveHelper(call.lineno, call.col)
    tree = ast.fix_missing_locations(remover.visit(ast.parse(source)))
    assert remover.replaced == 1, (
        f"the mutator replaced {remover.replaced} calls at {call}: two helper calls share a line "
        "and column, or the call moved after the scan, so split them onto separate lines"
    )
    return ast.unparse(tree)


# --- The fixture corpus: each asserts the reason, or silence -----------------------------------


def _src(signature: str, body: str, after: str = "") -> str:
    """A one-class module whose ``__init__`` has ``signature`` and ``body``, then ``after``."""
    return (
        "from typing import Optional\n"
        "class A:\n"
        f"    def __init__(self, {signature}):\n"
        f"{textwrap.indent(textwrap.dedent(body), '        ')}\n"
        f"{after}"
    )


_METHOD_FLOOR = """
    def _floor(self, value):
        return max(value, 0)
"""
_FUNCTION_CAP = """
def _cap(value, limit=10):
    return min(value, limit)
"""
_METHOD_RANGE = """
    def _check(self, *, value):
        if value <= 0:
            raise ValueError(value)
        return value
"""
_STATIC_FLOOR = """
    @staticmethod
    def _floor(value):
        return max(value, 0)
"""

_FIRES = {
    "max-int": (_src("p: int = 0", "self.x = max(p, 0)"), ("f.py:A.p: max",)),
    "max-ifexp": (
        _src("p: int | None = None", "self.x = max(p if p is not None else D, 1)"),
        ("f.py:A.p: max",),
    ),
    "compare-optional": (
        _src("p: int | None = None", "if p is not None and p <= 0:\n    raise ValueError(p)"),
        ("f.py:A.p: compare",),
    ),
    "compare-bare": (
        _src("p: int = 0", "if p <= 0:\n    raise ValueError(p)"),
        ("f.py:A.p: compare",),
    ),
    "optional-spelling": (
        _src("p: Optional[int] = None", "self.x = max(p, 0)"),
        ("f.py:A.p: max",),
    ),
    "chained": (
        _src("p: int = 0", "if not 0 <= p <= 65535:\n    raise ValueError(p)"),
        ("f.py:A.p: compare",),
    ),
    "two-shapes": (
        _src("p: int = 0", "self.x = max(p, 0)\nif p < 5:\n    pass"),
        ("f.py:A.p: compare", "f.py:A.p: max"),
    ),
    "keyword-only": (_src("*, p: int = 0", "self.x = max(p, 0)"), ("f.py:A.p: max",)),
    "union-spelling": (_src("p: Union[int, None] = 3", "self.x = max(p, 0)"), ("f.py:A.p: max",)),
    "typing-union-reversed": (
        _src("p: typing.Union[None, int] = 3", "self.x = max(p, 0)"),
        ("f.py:A.p: max",),
    ),
    "string-annotation": (_src('p: "int | None" = 3', "self.x = max(p, 0)"), ("f.py:A.p: max",)),
    "min-cap": (_src("p: int = 3", "self.x = min(p, 10)"), ("f.py:A.p: min",)),
    "helper-method-max": (
        _src("p: int = 3", "self.x = self._floor(p)", _METHOD_FLOOR),
        ("f.py:A.p: max in _floor",),
    ),
    "helper-function-min": (
        _src("p: int = 3", "self.x = _cap(p)", _FUNCTION_CAP),
        ("f.py:A.p: min in _cap",),
    ),
    "helper-keyword-compare": (
        _src("p: int = 3", "self.x = self._check(value=p)", _METHOD_RANGE),
        ("f.py:A.p: compare in _check",),
    ),
    "helper-staticmethod": (
        _src("p: int = 3", "self.x = self._floor(p)", _STATIC_FLOOR),
        ("f.py:A.p: max in _floor",),
    ),
}


@pytest.mark.parametrize("fixture", list(_FIRES))
def test_a_fixture_door_is_reported_with_its_shape(fixture: str) -> None:
    source, expected = _FIRES[fixture]
    assert scan(source, "f.py").findings == expected


_NOT_JUDGES = {
    "isinstance": "self._b = isinstance(p, bool)\nself.p = max(p, 0)",
    "require_timeout": 'require_timeout(p, "p", "A")\nself.p = max(p, 0)',
    "usable_timeout": "usable_timeout(p, 1)\nself.p = max(p, 0)",
}


@pytest.mark.parametrize("fixture", list(_NOT_JUDGES))
def test_a_non_judge_does_not_silence_an_int_floor(fixture: str) -> None:
    """None of these refuses a float: `isinstance(p, bool)` then `max(p, 0)` accepts 2.5."""
    result = scan(_src("p: int = 0", _NOT_JUDGES[fixture]), "f.py")
    assert result.findings == ("f.py:A.p: max",)
    assert result.judged == frozenset()


_METHOD_JUDGE = """
    def _judge(self, value):
        return floor_count(value, 0, "p", "A")
"""

_JUDGED = {
    "floor_count": _src("p: int = 0", 'self.x = max(p, 0)\nfloor_count(p, 0, "p", "A")'),
    "exact_int": _src("p: int = 0", 'self.x = max(p, 0)\nexact_int(p, "p", "A")'),
    "require_positive": _src("p: int = 0", 'self.x = max(p, 0)\nrequire_positive(p, "p", "A")'),
    "attribute-callee": _src(
        "p: int = 0", 'self.x = max(p, 0)\n_retry.floor_count(p, 0, "p", "A")'
    ),
    "rebound": _src(
        "p: int = 0", 'p = exact_int(p, "p", "A")\nif not 0 <= p <= 65535:\n    raise ValueError(p)'
    ),
    "judged-in-helper": _src("p: int = 0", "self.x = self._judge(p)", _METHOD_JUDGE),
    "min-after-judge": _src("p: int = 0", 'p = exact_int(p, "p", "A")\nself.x = min(p, 10)'),
}


@pytest.mark.parametrize("fixture", list(_JUDGED))
def test_a_judged_fixture_is_silent_and_in_the_judged_set(fixture: str) -> None:
    result = scan(_JUDGED[fixture], "f.py")
    assert result.findings == ()
    assert result.judged == frozenset({"f.py:A.p"})


_SILENT = {
    "float-arith": _src("deadline: float = 0.0", "self.r = max(0.0, deadline - time.monotonic())"),
    "float-annotated": _src("timeout: float = 1.0", "self.t = max(timeout, 0.0)"),
    "is-none": _src("p: int | None = None", "if p is None:\n    p = D"),
    "forwarded": _src("p: int = 0", "self.c = Client(p=p)"),
    "not-init": (
        "class A:\n    def build(self, p: int):\n        return max(p, 0)\n"
        "def f(p: int):\n    return max(p, 0)\n"
    ),
    "bool-annotated": _src("p: bool = False", "self.x = max(p, 0)"),
    "int-or-float-annotated": _src("p: int | float = 1", "self.x = max(p, 0)"),
    "optional-str-annotated": _src("p: Optional[str] = None", "self.x = max(p, 0)"),
    "helper-bounds-another-argument": _src(
        "p: int = 0",
        "self.x = _cap(3, p)",
        "\ndef _cap(value, limit):\n    return min(value, 10) if limit else value\n",
    ),
    "helper-only-forwards": _src(
        "p: int = 0",
        "self.c = self._client(p)",
        "\n    def _client(self, value):\n        return Client(n=value)\n",
    ),
    "helper-not-given-the-parameter": _src(
        "p: int = 0", "self.n = floor_count(p, 0, 'p', 'A')\nself._floor(3)", _METHOD_FLOOR
    ),
}


@pytest.mark.parametrize("fixture", list(_SILENT))
def test_a_fixture_that_is_not_an_int_door_is_silent(fixture: str) -> None:
    assert scan(_SILENT[fixture], "f.py").findings == ()


_METHOD_JUDGES_ANOTHER = """
    def _seconds(self, value, unit=1):
        unit = exact_int(unit, "unit", "A")
        return unit * value
"""


def test_a_followed_helper_judging_its_own_argument_is_not_in_the_sweep_population() -> None:
    """A callee judging a parameter `__init__` did not pass adds no site-less call to the sweep.

    `RotatingFileSink._rollover_seconds` is followed for `interval`; an `exact_int` on its own
    `unit` there judges nothing of the constructor's, so it is neither a site the sweep can name
    nor a call naming no parameter.
    """
    source = _src(
        "p: int = 1",
        'p = floor_count(p, 1, "p", "A")\nself.s = self._seconds(p)',
        _METHOD_JUDGES_ANOTHER,
    )
    result = scan(source, "f.py")
    assert [(call.site, call.kind) for call in result.helper_calls] == [("f.py:A.p", "floor_count")]
    assert result.findings == ()
    assert result.judged == frozenset({"f.py:A.p"})


def test_a_helper_judgement_is_in_the_sweep_population_and_its_removal_is_reported() -> None:
    """A judgement moved into a followed helper is swept like one written in `__init__`."""
    source = _JUDGED["judged-in-helper"]
    calls = scan(source, "f.py").helper_calls
    assert [(call.site, call.kind) for call in calls] == [("f.py:A.p", "floor_count")]
    mutated = scan(without_helper(source, calls[0]), "f.py")
    assert mutated.findings == ("f.py:A.p: max in _judge",)


def test_a_sink_in_a_subpackage_is_scanned(tmp_path: pathlib.Path) -> None:
    """The population is every module under `sinks/`, not only its top level."""
    nested = tmp_path / "sinks" / "vendor" / "deep.py"
    nested.parent.mkdir(parents=True)
    nested.write_text(_src("p: int = 0", "self.x = max(p, 0)"), encoding="utf-8")
    (tmp_path / "sinks" / "flat.py").write_text("", encoding="utf-8")
    (tmp_path / "worker.py").write_text("", encoding="utf-8")
    files = [path.relative_to(tmp_path).as_posix() for path in scanned_files(tmp_path)]
    assert "sinks/vendor/deep.py" in files, files
    assert scan_tree(tmp_path).findings == ("sinks/vendor/deep.py:A.p: max",)


# --- The tree ----------------------------------------------------------------------------------


def test_the_tree_has_no_unjudged_int_door() -> None:
    """FR-005 AC-1: every int floor, cap and range check in a constructor is judged for type."""
    findings = scan_tree(_PKG).findings
    assert findings == (), "unjudged int doors:\n" + "\n".join(findings) + f"\n{_REMEDY}"


def test_the_scan_reads_the_real_population() -> None:
    """FR-005 AC-2: a glob, a rename or a call-finder matching nothing fails here, not silently."""
    files = {path.relative_to(_PKG).as_posix() for path in scanned_files(_PKG)}
    missing = {"sinks/http.py", "sinks/file.py", "sinks/_socket.py", "worker.py"} - files
    assert not missing, (
        f"the scan does not read {sorted(missing)}: if a module moved, fix scanned_files()"
    )
    result = scan_tree(_PKG)
    renamed = (
        "if a judge was renamed, add the new name to JUDGES (and to _HELPERS if it is "
        "exact_int's or floor_count's) in tests/test_numeric_door_roster.py; if sites were "
        "deliberately removed, lower the floor here and in SPEC-066 FR-005, keeping headroom"
    )
    assert len(result.judged) >= 19, (
        f"only {len(result.judged)} judged: {sorted(result.judged)} — {renamed}"
    )
    assert len(result.helper_calls) >= 16, (
        f"only {len(result.helper_calls)} helper calls — {renamed}"
    )
    unnamed = [call for call in result.helper_calls if len(call.names) != 1]
    assert not unnamed, (
        f"helper calls naming no single int parameter: {unnamed} — pass the parameter itself, "
        "not an expression, as the helper's first argument, so the sweep can name its site"
    )


def _sweep_ids() -> list[HelperCall]:
    return list(scan_tree(_PKG).helper_calls)


@pytest.mark.parametrize(
    "call", _sweep_ids(), ids=lambda call: f"{call.site or call.file}@{call.lineno}"
)
def test_removing_a_helper_call_is_reported(call: HelperCall) -> None:
    """FR-005 AC-5 and FR-003 AC-4: deleting any one judgement makes the scan name its site."""
    source = (_PKG / call.file).read_text(encoding="utf-8")
    mutated = scan(without_helper(source, call), call.file)
    sites = {finding.rsplit(": ", 1)[0] for finding in mutated.findings}
    assert call.site in sites, (
        f"removing {call.kind} at {call.site} went unreported: nothing the scan reads guards "
        "that value once the call is gone — rebind the result to the parameter and keep the "
        "comparison on it, route the value through floor_count, or add the shape to scan()"
    )


def test_the_max_retries_roster_matches_the_scans_judged_set() -> None:
    """FR-001 AC-9: the roster names concrete classes; the scan names the class that judges."""
    from log_foundry.worker import Worker
    from test_numeric_doors import MAX_RETRIES_DOORS

    roster = [cls for cls, _factory, _attr in MAX_RETRIES_DOORS.values()] + [Worker]
    judged = {site for site in scan_tree(_PKG).judged if site.endswith(".max_retries")}

    def key(cls: type) -> str:
        module = cls.__module__.removeprefix("log_foundry.").replace(".", "/")
        return f"{module}.py:{cls.__qualname__}.max_retries"

    resolved: dict[str, str] = {}
    unresolved: list[str] = []
    for cls in roster:
        hit = next((key(k) for k in cls.__mro__ if key(k) in judged), None)
        if hit is None:
            unresolved.append(cls.__name__)
        else:
            resolved[cls.__name__] = hit
    unclaimed = sorted(judged - set(resolved.values()))
    assert not unresolved, (
        f"roster classes no judged site resolves to: {unresolved} — route max_retries through "
        "floor_count in the class's own __init__ or a base's; if a judgement moved into a "
        "helper, keep it within one call of __init__, in the same module, given the parameter "
        "itself, which is as far as the scan follows; if floor_count was renamed, add the new "
        "name to JUDGES and _HELPERS"
    )
    assert not unclaimed, (
        f"judged max_retries sites no roster class resolves to: {unclaimed} — add each class "
        "to MAX_RETRIES_DOORS in tests/test_numeric_doors.py, with a factory that builds it "
        "without a network, and raise the roster count below"
    )
    assert len(roster) == 14, (
        f"the roster has {len(roster)} classes, not 14 — a class added to or removed from "
        "MAX_RETRIES_DOORS changes this count: update it here and FR-001's roster in SPEC-066"
    )
    assert resolved["RedisStreamsSink"] == resolved["RedisListSink"], (
        f"the Redis pair resolve to {resolved['RedisStreamsSink']} and "
        f"{resolved['RedisListSink']}: both forward to _RedisSink, so if either now judges "
        "max_retries itself, update FR-001's roster in SPEC-066 and this check"
    )
