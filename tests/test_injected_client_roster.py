"""SPEC-071 FR-003 — a sink that would ignore an option beside an injected client is reported.

The guard does not look for the refusal's message; it derives, from each constructor's AST, the
options the sink would build its client from, and holds each to the refusal. So a sink that
*ignores* an option fails here, not only one that lost a copy of the check.

**The build.** For every class in `src/log_foundry/sinks/*.py` whose `__init__` takes `client`,
`producer` or `connection` (`P`), the build is located as:

- the body of `if P is None:`, or the `else` of `if P is not None:`;
- the `else` arm of `X if P is not None else <build>` (or the `if` arm of `... if P is None ...`);
- or, for a constructor that has neither, a `super().__init__(..., P=P, ...)` that forwards the
  injected object, in which case the parent's build in the same module is attributed through the
  keywords the call passes.

**Fails closed.** A constructor where none of these is found is reported as `build not located`,
so a build written as `if not client:` or `client = client or make(url)` cannot pass by
contributing no options at all.

**Build options** are the parameters the build reads, directly or through what they flowed into:
a local or a `self` attribute assigned from them (subscript stores, `or`/`if` expressions, calls,
dict displays and `**` spreads, comprehensions and `for` targets included, and a container a
method call such as `.append(x)` fills), and one level into a
`self.<method>()` the build calls (`_connect`), reading the attributes the constructor stored.
The analysis is flow-insensitive over the constructor, which over-reports rather than misses.

**Five checks.**

- (a) Every build option whose default is `None` is a key of the constructor's (or its parent's)
  `refuse_with_injected` mapping, or it is reported as `<file>:<Class>.<param>`.
- (b) Build options with any other default, or none, form a derived set compared exactly with
  `SQLiteSink.database`, invariant 13's recorded exception.
- (c) The class population is compared exactly with the sixteen public classes on `2368ade3`.
- (d) Each `refuse_with_injected` call passes the injected parameter itself and a dict literal
  whose keys are strings and whose every value is that key's parameter, or a local assigned
  directly from it (`PostgresSink`'s `given_timeout`). A spread or a constant is refused: either
  would let a mutation that drops a name stay silent, or refuse nothing at all.
- (e) No `ValueError` raised in `sinks/*.py` outside `_args.py` carries `to an injected`.

Serves invariant 13: no argument is silently ignored because another was given.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path

_SINKS = Path(__file__).resolve().parents[1] / "src" / "log_foundry" / "sinks"
"""Derived from this file, never from ``log_foundry.__file__``: an editable install can resolve to
another tree, and a guard scanning the wrong tree passes because it found nothing."""

_INJECTED = ("client", "producer", "connection")

EXPECTED_CLASSES = frozenset(
    {
        "AzureEventHubsSink",
        "ClickHouseSink",
        "FirehoseSink",
        "GooglePubSubSink",
        "KafkaSink",
        "KinesisSink",
        "MongoDBSink",
        "NATSSink",
        "PostgresSink",
        "RabbitMQSink",
        "RedisListSink",
        "RedisStreamsSink",
        "SNSSink",
        "SQLiteSink",
        "SQSSink",
        "SentrySink",
    }
)
"""The public classes taking an injected client, producer or connection on ``2368ade3``."""

RECORDED_EXCEPTION = frozenset({"sqlite.py:SQLiteSink.database"})
"""Required, positional and documented as ignored beside an injected connection (invariant 13)."""


@dataclass
class _Init:
    """One constructor's facts, before a parent's build is attributed.

    Attributes:
      file: The module's file name.
      cls: The class name.
      injected: The injected parameter's name.
      defaults: Each parameter's default kind: ``"none"``, ``"other"`` or ``"required"``.
      located: Whether a build was found in this constructor.
      options: The build options read by this constructor's own build.
      supplied: The keys of its ``refuse_with_injected`` mapping.
      forwards: ``parent class -> {parent param: own param}`` for a super call forwarding ``P``.
      problems: Check (d) and ``build not located`` lines.
    """

    file: str
    cls: str
    injected: str
    defaults: dict[str, str]
    located: bool = False
    options: set[str] = field(default_factory=set)
    supplied: set[str] = field(default_factory=set)
    forwards: dict[str, dict[str, str]] = field(default_factory=dict)
    problems: list[str] = field(default_factory=list)


@dataclass
class Report:
    """What one pass found.

    Attributes:
      unrefused: Check (a): ``<file>:<Class>.<param>: …`` per option missing from the mapping.
      required: Check (b): ``<file>:<Class>.<param>`` per build option without a ``None`` default.
      population: Check (c): the public classes walked.
      malformed: Check (d) and ``build not located``.
      stray: Check (e): ``<file>:<line>`` per stray refusal message.
    """

    unrefused: list[str] = field(default_factory=list)
    required: set[str] = field(default_factory=set)
    population: set[str] = field(default_factory=set)
    malformed: list[str] = field(default_factory=list)
    stray: list[str] = field(default_factory=list)


def _params(func: ast.FunctionDef) -> dict[str, str]:
    """Returns each parameter's default kind, ``self`` excluded.

    Args:
      func: The constructor.

    Returns:
      ``"none"`` for a ``None`` default, ``"other"`` for any other, ``"required"`` for none.

    Raises:
      None.
    """
    args = func.args
    positional = [*args.posonlyargs, *args.args]
    padded: list[ast.expr | None] = [None] * (len(positional) - len(args.defaults))
    pairs = list(zip(positional, [*padded, *args.defaults], strict=True))
    pairs += list(zip(args.kwonlyargs, args.kw_defaults, strict=True))
    kinds: dict[str, str] = {}
    for arg, default in pairs:
        if arg.arg == "self":
            continue
        if default is None:
            kinds[arg.arg] = "required"
        elif isinstance(default, ast.Constant) and default.value is None:
            kinds[arg.arg] = "none"
        else:
            kinds[arg.arg] = "other"
    return kinds


def _is_none_test(test: ast.expr, name: str) -> bool | None:
    """Returns True for ``name is None``, False for ``name is not None``, None otherwise.

    Args:
      test: An ``if`` or conditional expression's test.
      name: The injected parameter.

    Returns:
      Which way the test reads, or ``None`` when it is not that comparison.

    Raises:
      None.
    """
    if (
        isinstance(test, ast.Compare)
        and isinstance(test.left, ast.Name)
        and test.left.id == name
        and len(test.ops) == 1
        and isinstance(test.comparators[0], ast.Constant)
        and test.comparators[0].value is None
    ):
        if isinstance(test.ops[0], ast.Is):
            return True
        if isinstance(test.ops[0], ast.IsNot):
            return False
    return None


def _builds(func: ast.FunctionDef, injected: str) -> list[ast.AST]:
    """Returns the regions of the constructor taken when nothing was injected.

    Args:
      func: The constructor.
      injected: The injected parameter.

    Returns:
      Statements and expressions forming the build.

    Raises:
      None.
    """
    regions: list[ast.AST] = []
    for node in ast.walk(func):
        if isinstance(node, ast.If | ast.IfExp):
            reading = _is_none_test(node.test, injected)
            if reading is None:
                continue
            taken = node.body if reading else node.orelse
            regions.extend(taken if isinstance(taken, list) else [taken])
    return regions


def _names(node: ast.AST) -> set[str]:
    """Returns every name a node reads.

    Args:
      node: Any node.

    Returns:
      The ids of its ``Load`` names.

    Raises:
      None.
    """
    return {n.id for n in ast.walk(node) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)}


def _self_attrs(node: ast.AST) -> set[str]:
    """Returns every ``self.<attr>`` a node reads.

    Args:
      node: Any node.

    Returns:
      The attribute names.

    Raises:
      None.
    """
    return {
        n.attr
        for n in ast.walk(node)
        if isinstance(n, ast.Attribute)
        and isinstance(n.value, ast.Name)
        and n.value.id == "self"
        and isinstance(n.ctx, ast.Load)
    }


def _taint(
    func: ast.FunctionDef, params: set[str]
) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    """Returns which parameters each local and each ``self`` attribute may hold.

    Args:
      func: The constructor.
      params: Its parameters.

    Returns:
      ``(locals, attributes)``, each mapping a name to the parameters that flow into it.

    Raises:
      None.
    """
    local: dict[str, set[str]] = {p: {p} for p in params}
    attrs: dict[str, set[str]] = {}

    def deps(expr: ast.AST) -> set[str]:
        found: set[str] = set()
        for name in _names(expr) - {"self"}:
            found |= local.get(name, set())
        for attr in _self_attrs(expr):
            found |= attrs.get(attr, set())
        return found

    def bind(target: ast.AST, flow: set[str]) -> None:
        if isinstance(target, ast.Name):
            local.setdefault(target.id, set()).update(flow)
        elif (
            isinstance(target, ast.Attribute)
            and isinstance(target.value, ast.Name)
            and target.value.id == "self"
        ):
            attrs.setdefault(target.attr, set()).update(flow)
        elif isinstance(target, ast.Subscript) and isinstance(target.value, ast.Name):
            local.setdefault(target.value.id, set()).update(flow)
        elif isinstance(target, ast.Tuple | ast.List):
            for element in target.elts:
                bind(element, flow)
        elif isinstance(target, ast.Starred):
            bind(target.value, flow)

    def store(target: ast.AST, flow: set[str]) -> bool:
        before = (sum(len(v) for v in local.values()), sum(len(v) for v in attrs.values()))
        bind(target, flow)
        after = (sum(len(v) for v in local.values()), sum(len(v) for v in attrs.values()))
        return after != before

    changed = True
    while changed:
        changed = False
        for node in ast.walk(func):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    changed |= store(target, deps(node.value))
            elif isinstance(node, ast.AnnAssign | ast.AugAssign) and node.value is not None:
                changed |= store(node.target, deps(node.value))
            elif isinstance(node, ast.For | ast.comprehension):
                changed |= store(node.target, deps(node.iter))
            elif isinstance(node, ast.NamedExpr):
                changed |= store(node.target, deps(node.value))
            elif (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id != "self"
            ):
                flow = set().union(*(deps(a) for a in [*node.args, *node.keywords]))
                changed |= store(node.func.value, flow)
    return local, attrs


def _is_super_init(call: ast.Call) -> bool:
    func = call.func
    return (
        isinstance(func, ast.Attribute)
        and func.attr == "__init__"
        and isinstance(func.value, ast.Call)
        and isinstance(func.value.func, ast.Name)
        and func.value.func.id == "super"
    )


def _check_refusal(call: ast.Call, init: _Init, func: ast.FunctionDef) -> None:
    """Applies check (d) to one ``refuse_with_injected`` call and records its keys.

    Args:
      call: The call.
      init: The constructor's record.
      func: The constructor.

    Returns:
      None.

    Raises:
      None.
    """
    where = f"{init.file}:{init.cls}"
    if len(call.args) != 4 or call.keywords:
        init.problems.append(f"{where}: refuse_with_injected takes four positional arguments")
        return
    injected, mapping = call.args[2], call.args[3]
    if not (isinstance(injected, ast.Name) and injected.id == init.injected):
        init.problems.append(f"{where}: refuse_with_injected is not passed {init.injected}")
    if not isinstance(mapping, ast.Dict):
        init.problems.append(f"{where}: supplied is not a dict literal")
        return
    aliases = {
        target.id: node.value.id
        for node in ast.walk(func)
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Name)
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    for key, value in zip(mapping.keys, mapping.values, strict=True):
        if not (isinstance(key, ast.Constant) and isinstance(key.value, str)):
            init.problems.append(f"{where}: supplied has a key that is not a string literal")
            continue
        own = isinstance(value, ast.Name) and (
            value.id == key.value or aliases.get(value.id) == key.value
        )
        if not own:
            init.problems.append(f"{where}: supplied[{key.value!r}] is not the parameter itself")
            continue
        init.supplied.add(key.value)


def _walk_init(file: str, cls: ast.ClassDef, methods: dict[str, ast.FunctionDef]) -> _Init | None:
    """Derives one constructor's build options, refusal keys and forwarding.

    Args:
      file: The module's file name.
      cls: The class.
      methods: Every method of the class, and of its same-module bases, by name.

    Returns:
      The record, or ``None`` when the class takes no injected object.

    Raises:
      None.
    """
    func = next(
        (m for m in cls.body if isinstance(m, ast.FunctionDef) and m.name == "__init__"), None
    )
    if func is None:
        return None
    defaults = _params(func)
    injected = next((p for p in _INJECTED if p in defaults), None)
    if injected is None:
        return None
    init = _Init(file, cls.name, injected, defaults)
    params = set(defaults)
    local, attrs = _taint(func, params)
    regions = _builds(func, injected)
    init.located = bool(regions)
    reads: set[str] = set()
    for region in regions:
        for name in _names(region):
            reads |= local.get(name, set())
        for attr in _self_attrs(region):
            reads |= attrs.get(attr, set())
        for call in (n for n in ast.walk(region) if isinstance(n, ast.Call)):
            target = call.func
            if (
                isinstance(target, ast.Attribute)
                and isinstance(target.value, ast.Name)
                and target.value.id == "self"
                and target.attr in methods
            ):
                for attr in _self_attrs(methods[target.attr]):
                    reads |= attrs.get(attr, set())
    init.options = reads & params - {injected}
    for call in (n for n in ast.walk(func) if isinstance(n, ast.Call)):
        if isinstance(call.func, ast.Name) and call.func.id == "refuse_with_injected":
            _check_refusal(call, init, func)
        if _is_super_init(call) and any(
            isinstance(k.value, ast.Name) and k.value.id == injected for k in call.keywords
        ):
            mapping = {
                k.arg: k.value.id
                for k in call.keywords
                if k.arg and isinstance(k.value, ast.Name) and k.value.id in params
            }
            for base in cls.bases:
                if isinstance(base, ast.Name):
                    init.forwards[base.id] = mapping
    return init


def scan(sources: dict[str, str]) -> Report:
    """Runs the five checks over ``sources``.

    Args:
      sources: Source text keyed by file name.

    Returns:
      Everything the checks found.

    Raises:
      SyntaxError: If a source does not parse.
    """
    report = Report()
    for file in sorted(sources):
        tree = ast.parse(sources[file])
        classes = {n.name: n for n in tree.body if isinstance(n, ast.ClassDef)}
        inits: dict[str, _Init] = {}
        for cls in classes.values():
            methods = {
                m.name: m
                for owner in [
                    cls,
                    *(
                        classes[b.id]
                        for b in cls.bases
                        if isinstance(b, ast.Name) and b.id in classes
                    ),
                ]
                for m in owner.body
                if isinstance(m, ast.FunctionDef)
            }
            init = _walk_init(file, cls, methods)
            if init is not None:
                inits[cls.name] = init
        for init in inits.values():
            if init.cls.startswith("_"):
                continue
            report.population.add(init.cls)
            options, supplied = set(init.options), set(init.supplied)
            located = init.located
            for parent, mapping in init.forwards.items():
                parent_init = inits.get(parent)
                if parent_init is None:
                    continue
                located = located or parent_init.located
                options |= {mapping[p] for p in parent_init.options if p in mapping}
                supplied |= {mapping[p] for p in parent_init.supplied if p in mapping}
                report.malformed.extend(parent_init.problems)
            report.malformed.extend(init.problems)
            if not located:
                report.malformed.append(f"{file}:{init.cls}: build not located")
                continue
            for option in sorted(options):
                kind = init.defaults.get(option, "required")
                if kind == "none":
                    if option not in supplied:
                        report.unrefused.append(
                            f"{file}:{init.cls}.{option}: a build option not refused beside an "
                            f"injected {init.injected}"
                        )
                else:
                    report.required.add(f"{file}:{init.cls}.{option}")
        if file != "_args.py":
            for node in ast.walk(tree):
                if isinstance(node, ast.Raise) and isinstance(node.exc, ast.Call):
                    texts = [
                        n.value
                        for n in ast.walk(node.exc)
                        if isinstance(n, ast.Constant) and isinstance(n.value, str)
                    ]
                    if any("to an injected" in t for t in texts):
                        report.stray.append(f"{file}:{node.lineno}")
    report.malformed = sorted(set(report.malformed))
    report.unrefused.sort()
    return report


def _tree() -> dict[str, str]:
    return {p.name: p.read_text(encoding="utf-8") for p in sorted(_SINKS.glob("*.py"))}


def test_the_real_sinks_refuse_every_build_option_beside_an_injected_object() -> None:
    report = scan(_tree())
    assert report.unrefused == []
    assert report.malformed == []
    assert report.stray == []


def test_the_only_build_option_without_a_none_default_is_the_recorded_exception() -> None:
    assert scan(_tree()).required == RECORDED_EXCEPTION


def test_the_class_population_is_the_sixteen() -> None:
    assert scan(_tree()).population == EXPECTED_CLASSES


# --- corpus --------------------------------------------------------------------------------------


def _one(source: str) -> Report:
    return scan({"m.py": source})


def test_it_reports_a_build_that_refuses_nothing() -> None:
    source = (
        "class S:\n"
        "    def __init__(self, client=None, url=None):\n"
        "        if client is None:\n"
        "            client = make(url)\n"
        "        self.client = client\n"
    )
    assert _one(source).unrefused == [
        "m.py:S.url: a build option not refused beside an injected client"
    ]


def test_it_reports_an_option_stored_and_read_only_in_connect() -> None:
    source = (
        "class S:\n"
        "    def __init__(self, connection=None, url=None):\n"
        "        self._url = url\n"
        "        self._conn = connection if connection is not None else self._connect()\n"
        "    def _connect(self):\n"
        "        return dial(self._url)\n"
    )
    assert _one(source).unrefused == [
        "m.py:S.url: a build option not refused beside an injected connection"
    ]


def test_it_follows_a_subscript_store_an_or_and_a_spread() -> None:
    source = (
        "class S:\n"
        "    def __init__(self, producer=None, timeout=None, config=None, servers=None):\n"
        "        bounds = {}\n"
        "        bounds['t'] = check(timeout)\n"
        "        if producer is None:\n"
        "            producer = build(servers or 'x', {**(config or {})}, **bounds)\n"
    )
    assert _one(source).unrefused == [
        "m.py:S.config: a build option not refused beside an injected producer",
        "m.py:S.servers: a build option not refused beside an injected producer",
        "m.py:S.timeout: a build option not refused beside an injected producer",
    ]


def test_it_follows_a_comprehension_and_a_for_target() -> None:
    source = (
        "class S:\n"
        "    def __init__(self, client=None, a=None, b=None):\n"
        "        options = {'a': a}\n"
        "        kept = {k: v for k, v in options.items() if v is not None}\n"
        "        extra = []\n"
        "        for item in [b]:\n"
        "            extra.append(item)\n"
        "        if not (client is not None):\n"
        "            pass\n"
        "        if client is None:\n"
        "            client = build(**kept, more=extra)\n"
    )
    assert _one(source).unrefused == [
        "m.py:S.a: a build option not refused beside an injected client",
        "m.py:S.b: a build option not refused beside an injected client",
    ]


def test_it_fails_closed_when_the_build_cannot_be_located() -> None:
    truthy = (
        "class S:\n"
        "    def __init__(self, client=None, url=None):\n"
        "        if not client:\n"
        "            client = make(url)\n"
    )
    defaulted = (
        "class T:\n"
        "    def __init__(self, client=None, url=None):\n"
        "        self.client = client or make(url)\n"
    )
    assert _one(truthy).malformed == ["m.py:S: build not located"]
    assert _one(defaulted).malformed == ["m.py:T: build not located"]


def test_it_attributes_a_parents_build_through_a_forwarding_super_call() -> None:
    source = (
        "class _Base:\n"
        "    def __init__(self, *, client, url, retries):\n"
        "        refuse_with_injected('X', 'client', client, {'url': url})\n"
        "        if client is None:\n"
        "            client = make(url)\n"
        "class Child(_Base):\n"
        "    def __init__(self, key, client=None, url=None, retries=3):\n"
        "        super().__init__(client=client, url=url, retries=retries)\n"
        "class Bare(_Base):\n"
        "    def __init__(self, client=None, url=None):\n"
        "        super().__init__(client=client, url=url, retries=0)\n"
    )
    report = _one(source)
    assert report.unrefused == []
    assert report.malformed == []
    assert report.population == {"Child", "Bare"}
    unrefused_parent = source.replace(
        "refuse_with_injected('X', 'client', client, {'url': url})", "pass"
    )
    assert _one(unrefused_parent).unrefused == [
        "m.py:Bare.url: a build option not refused beside an injected client",
        "m.py:Child.url: a build option not refused beside an injected client",
    ]


def test_it_is_silent_when_the_option_is_refused() -> None:
    source = (
        "class S:\n"
        "    def __init__(self, client=None, url=None):\n"
        "        refuse_with_injected('S', 'client', client, {'url': url})\n"
        "        if client is None:\n"
        "            client = make(url)\n"
    )
    assert _one(source).unrefused == []
    assert _one(source).malformed == []


def test_it_is_silent_on_a_parameter_used_outside_the_build() -> None:
    source = (
        "class S:\n"
        "    def __init__(self, client=None, uri=None, database='d'):\n"
        "        refuse_with_injected('S', 'client', client, {'uri': uri})\n"
        "        if client is None:\n"
        "            client = make(uri)\n"
        "        self._collection = client[database]\n"
    )
    report = _one(source)
    assert report.unrefused == []
    assert report.required == set()


def test_it_derives_a_required_build_option_into_the_second_set() -> None:
    source = (
        "class S:\n"
        "    def __init__(self, path, retries=3, connection=None):\n"
        "        self.conn = connection if connection is not None else dial(path, retries)\n"
    )
    assert _one(source).required == {"m.py:S.path", "m.py:S.retries"}


def test_it_reports_a_malformed_refusal() -> None:
    def refusal(args: str) -> list[str]:
        source = (
            "class S:\n"
            "    def __init__(self, client=None, url=None, other=None):\n"
            "        alias = url\n"
            f"        refuse_with_injected({args})\n"
            "        if client is None:\n"
            "            client = make(url)\n"
        )
        return _one(source).malformed

    assert refusal("'S', 'client', client, {'url': url}") == []
    assert refusal("'S', 'client', client, {'url': alias}") == []
    assert refusal("'S', 'client', client, {'url': None}") == [
        "m.py:S: supplied['url'] is not the parameter itself"
    ]
    assert refusal("'S', 'client', client, {'url': other}") == [
        "m.py:S: supplied['url'] is not the parameter itself"
    ]
    assert refusal("'S', 'client', client, {**{'url': url}}") == [
        "m.py:S: supplied has a key that is not a string literal"
    ]
    assert refusal("'S', 'client', client, dict(url=url)") == [
        "m.py:S: supplied is not a dict literal"
    ]
    assert refusal("'S', 'client', None, {'url': url}") == [
        "m.py:S: refuse_with_injected is not passed client"
    ]


def test_it_reports_a_stray_refusal_message_outside_args() -> None:
    source = (
        "def f(c, x):\n"
        "    if c is not None and x is not None:\n"
        "        raise ValueError('S cannot apply x to an injected client')\n"
    )
    assert scan({"m.py": source}).stray == ["m.py:3"]
    assert scan({"_args.py": source}).stray == []


def test_it_reports_an_extra_and_a_missing_class() -> None:
    tree = _tree()
    tree["extra.py"] = (
        "class ExtraSink:\n"
        "    def __init__(self, client=None):\n"
        "        if client is None:\n"
        "            client = make()\n"
    )
    del tree["sns.py"]
    population = scan(tree).population
    assert population - EXPECTED_CLASSES == {"ExtraSink"}
    assert EXPECTED_CLASSES - population == {"SNSSink"}


# --- the mutation sweep -------------------------------------------------------------------------


class _DropKey(ast.NodeTransformer):
    """Drops one key from the ``n``-th ``refuse_with_injected`` mapping of a module."""

    def __init__(self, index: int, key: str) -> None:
        self.index = index
        self.key = key
        self.seen = -1
        self.dropped = False

    def visit_Call(self, node: ast.Call) -> ast.AST:
        self.generic_visit(node)
        if isinstance(node.func, ast.Name) and node.func.id == "refuse_with_injected":
            self.seen += 1
            mapping = node.args[3]
            if self.seen == self.index and isinstance(mapping, ast.Dict):
                kept = [
                    (k, v)
                    for k, v in zip(mapping.keys, mapping.values, strict=True)
                    if not (isinstance(k, ast.Constant) and k.value == self.key)
                ]
                self.dropped = len(kept) < len(mapping.keys)
                mapping.keys = [k for k, _ in kept]
                mapping.values = [v for _, v in kept]
        return node


def test_every_name_dropped_from_every_mapping_is_reported() -> None:
    tree = _tree()
    planted = 0
    for file, source in tree.items():
        calls = [
            n
            for n in ast.walk(ast.parse(source))
            if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Name)
            and n.func.id == "refuse_with_injected"
        ]
        for index, call in enumerate(calls):
            mapping = call.args[3]
            assert isinstance(mapping, ast.Dict)
            for key in mapping.keys:
                assert isinstance(key, ast.Constant)
                drop = _DropKey(index, str(key.value))
                mutant = ast.unparse(drop.visit(ast.parse(source)))
                assert drop.dropped, f"the premise: {file} lost {key.value}"
                reported = scan({**tree, file: mutant}).unrefused
                assert any(f".{key.value}: a build option not refused" in r for r in reported), (
                    f"{file}: dropping {key.value!r} from the refusal went unreported"
                )
                planted += 1
    assert planted == 21, (
        "2 ClickHouse, 2 Azure, 2 Kafka, 3 Mongo, 5 NATS, 2 Postgres, 4 RabbitMQ, 1 Redis"
    )
