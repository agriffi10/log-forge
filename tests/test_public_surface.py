"""SPEC-034 — the public surface, asserted where it freezes at 1.0.

Every criterion here is about a *name* rather than a behaviour, which is why they live together
rather than beside the code they check: at `1.0.0` a name in a shipped signature stops being a
wart and becomes a promise, and fixing one afterwards costs a major version. The tests are
derived from the package rather than hand-listed wherever that is possible, on the roster lesson
SPEC-032 and SPEC-035 both paid for.
"""

import ast
import inspect
import pathlib
import pkgutil
import re
import threading

import pytest

log_foundry = pytest.importorskip("log_foundry")

_ROOT = pathlib.Path(__file__).resolve().parents[1]
_SINK_PKG = _ROOT / "src" / "log_foundry" / "sinks"

# FR-001 AC-2: a blacklist of names, not a rule derived from syntax. Role is not inferable from
# a name alone -- `stream` is a positional *transport* in StdoutSink and a positional
# *destination* in RedisStreamsSink -- so a syntactic rule would have to guess, and this does not.
_INJECTED = ("client", "sdk", "producer", "connection", "opener")


def _sink_classes_with_emit() -> list[tuple[str, ast.ClassDef]]:
    """Every class in `sinks/` that defines an `emit`, by source rather than by import.

    This is SPEC-032's lint scope and deliberately not "every class named ``*Sink``": the two
    rosters differ (34 against 39 at the time SPEC-034 was written), and a roster whose whole
    point is completeness cannot rest on a naming convention. Reading the source rather than
    importing keeps every sink in scope in an environment with no optional extras installed,
    which is what CI is.

    Args:
      None.

    Returns:
      (module name, class node) for each.

    Raises:
      None.
    """
    found: list[tuple[str, ast.ClassDef]] = []
    for path in sorted(_SINK_PKG.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            if any(
                isinstance(b, ast.FunctionDef | ast.AsyncFunctionDef) and b.name == "emit"
                for b in node.body
            ):
                found.append((path.stem, node))
    return found


def _positional_params(cls: ast.ClassDef) -> list[str]:
    """The names `__init__` accepts positionally, excluding `self`.

    Args:
      cls: The class node.

    Returns:
      Positional parameter names, or an empty list when the class defines no ``__init__``.

    Raises:
      None.
    """
    for node in cls.body:
        if isinstance(node, ast.FunctionDef) and node.name == "__init__":
            args = node.args
            return [a.arg for a in (*args.posonlyargs, *args.args)][1:]
    return []


def test_the_roster_is_the_emit_defining_classes_and_is_not_empty() -> None:
    """A derived roster that silently matched nothing would pass every test below.

    Stated as its own test because the two tests that use it are both *negative* -- they assert
    an absence -- and an empty roster satisfies an absence perfectly.
    """
    roster = _sink_classes_with_emit()
    assert len(roster) >= 30, f"the sink roster collapsed to {len(roster)}: {roster}"


def test_no_sink_takes_an_injected_transport_positionally() -> None:
    """FR-001 AC-2. A sink's positional parameters identify its *destination*.

    `SQSSink(queue_url, client)` was the one violation: an injected transport sitting second,
    beside an identifier that already names the destination. `StdoutSink(stream)` and
    `LoggingSink(logger)` are not violations, and are not caught here, because there the stream
    or logger **is** the destination identity -- which is exactly why the check is a blacklist
    of five names rather than something derived from the syntax.
    """
    offenders = [
        f"{module}.{cls.name}({name})"
        for module, cls in _sink_classes_with_emit()
        for name in _positional_params(cls)
        if name in _INJECTED
    ]
    assert not offenders, f"injected transports must be keyword-only: {offenders}"


def test_sqs_client_is_keyword_only() -> None:
    """FR-001 AC-1, at the one site the rename was taken for."""
    sqs = pytest.importorskip("log_foundry.sinks.sqs")
    params = inspect.signature(sqs.SQSSink.__init__).parameters
    assert params["client"].kind is inspect.Parameter.KEYWORD_ONLY
    assert params["queue_url"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD


def test_sentry_injects_through_client() -> None:
    """FR-002 AC-1 and AC-3: renamed with no alias, and the attribute renamed with it.

    An alias would have to live for the whole of `1.x`, which is the cost this spec exists to
    avoid.
    """
    saas = pytest.importorskip("log_foundry.sinks.sentry")
    params = inspect.signature(saas.SentrySink.__init__).parameters
    assert "sdk" not in params
    assert params["client"].kind is inspect.Parameter.KEYWORD_ONLY

    class FakeSDK:
        def capture_event(self, event: dict) -> None: ...

    sdk = FakeSDK()
    assert saas.SentrySink(client=sdk).client is sdk
    with pytest.raises(TypeError):
        saas.SentrySink(sdk=sdk)  # type: ignore[call-arg]


def test_no_sdk_keyword_or_attribute_survives() -> None:
    """FR-002 AC-2, narrowed to what it can mean.

    The AC says "`sdk` appears nowhere in src/, tests/ or README.md". Taken literally that
    forbids `sentry_sdk`, `_import_sdk` and a local holding a fake SDK, none of which is the
    rename -- so what is asserted is the part that freezes: no `sdk=` keyword argument and no
    `.sdk` attribute access anywhere. The narrowing is recorded in the spec, not only here.
    """
    offenders = []
    scanned = 0
    paths = [*(_ROOT / "src").rglob("*.py"), *(_ROOT / "tests").rglob("*.py"), _ROOT / "README.md"]
    for path in paths:
        if path.name == pathlib.Path(__file__).name:
            continue
        scanned += 1
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if re.search(r"(?<![\w.])sdk\s*=(?!=)", line) or re.search(r"\.sdk(?![\w])", line):
                offenders.append(f"{path.relative_to(_ROOT)}:{lineno}: {line.strip()}")
    assert scanned > 100, f"the scan collapsed to {scanned} files -- an absence it cannot see"
    assert not offenders, "the `sdk` injection name survives:\n" + "\n".join(offenders)


def test_the_extension_points_are_exported() -> None:
    """FR-005 AC-1 and AC-2. A name absent from `__all__` at 1.0 says "not part of the API"."""
    for name in ("Sink", "Config", "read_losses", "get_baggage"):
        assert name in log_foundry.__all__, name
        assert getattr(log_foundry, name, None) is not None, name


def test_every_exported_name_is_importable() -> None:
    """FR-005 AC-5, first half: `__all__` cannot name something that is not there."""
    missing = [n for n in log_foundry.__all__ if not hasattr(log_foundry, n)]
    assert not missing, f"named in __all__ but absent: {missing}"


def _names_the_readme_imports() -> set[str]:
    """Every name the README tells a reader to import from `log_foundry`.

    Derived from the README text rather than listed, which is the half of FR-005 AC-5 that makes
    the two unable to drift. Only the `from log_foundry import ...` form is read: `import
    log_foundry as lf` followed by `lf.info(...)` says the *module* is public and says nothing
    about `__all__`, and treating every attribute reached that way as a claim would make the
    README's prose examples define the API surface.

    Args:
      None.

    Returns:
      The names imported from the top-level package anywhere in the README.

    Raises:
      None.
    """
    text = (_ROOT / "README.md").read_text(encoding="utf-8")
    names: set[str] = set()
    for match in re.finditer(r"^from log_foundry import (.+)$", text, re.MULTILINE):
        # An inline comment is part of the line, and dropping the `isidentifier` survivors
        # without cutting it first silently loses the name: the README's own `Sink` import
        # carries one, so the first version of this scan saw nothing and passed the mutant that
        # removed `Sink` from `__all__`. Caught by running that mutation, not by reading.
        imported = match.group(1).split("#", 1)[0]
        names.update(part.strip() for part in imported.split(","))
    return {n for n in names if n and n.isidentifier()}


def test_the_readme_and_all_cannot_drift() -> None:
    """FR-005 AC-5, second half — the anti-drift one, and the reason the AC exists.

    A name the README tells you to import that is not exported is a broken documented import;
    the check is derived from the README so neither side can move without the other. Review of
    the first build of this file found the AC ticked with only the first half implemented, and
    this test failing would have caught two of that review's own findings on its first run.
    """
    documented = _names_the_readme_imports()
    assert documented, "the README-derived roster is empty -- the scan stopped matching"
    unexported = sorted(documented - set(log_foundry.__all__))
    assert not unexported, (
        f"the README imports these from log_foundry, but __all__ omits them: {unexported}"
    )


def test_get_baggage_round_trips_with_set_baggage() -> None:
    """FR-005 AC-3. Baggage was publicly *settable* and readable only as a serialized header."""
    log_foundry.reset_context()
    log_foundry.set_baggage(user_id="u42", tenant="acme")
    assert log_foundry.get_baggage() == {"user_id": "u42", "tenant": "acme"}


def test_get_baggage_hands_back_a_copy() -> None:
    """A reader that could mutate the live baggage is the FR-003 defect in another place.

    Not an AC; asserted because exporting the accessor is what makes it reachable, and the
    check is one line. Both baggage tests reset first: baggage set with no span open is a
    process-level default that nothing releases, so without it one test's keys reach the next
    -- which is SPEC-024's own finding, reproduced here between two tests.
    """
    log_foundry.reset_context()
    log_foundry.set_baggage(user_id="u42")
    got = log_foundry.get_baggage()
    got["user_id"] = "someone else"
    assert log_foundry.get_baggage() == {"user_id": "u42"}


def test_the_per_event_path_does_not_pay_for_the_baggage_copy() -> None:
    """FR-005 AC-3a's cost argument, pinned rather than asserted in prose.

    `get_baggage()` copies because it is public; `api._log` reads baggage **once per event**, so
    routing it through the public accessor would allocate per event. Nothing failed if a future
    refactor put `get_baggage()` back there -- the behaviour is identical and only the cost
    changes, which is the kind of regression no behavioural test can see. FR-003 AC-6 requires a
    benchmark for the config equivalent; this is the same guarantee, checked structurally.
    """
    api_src = (_ROOT / "src" / "log_foundry" / "api.py").read_text(encoding="utf-8")
    calls = [
        ast.unparse(node)
        for node in ast.walk(ast.parse(api_src))
        if isinstance(node, ast.Call) and ast.unparse(node).endswith("baggage()")
    ]
    assert calls == ["context._live_baggage()"], f"api._log's baggage read is {calls}"


def test_the_stop_signal_attribute_is_namespaced_everywhere() -> None:
    """FR-006 AC-2 and AC-3, derived from the package rather than a list of sinks.

    The library assigns this onto an object it does not own, by `hasattr` probe, so a bare
    `stop_signal` silently overwrote any attribute of that name a third-party sink already had.
    A survivor of the old name in `src/` is a sink the worker can no longer interrupt, which is
    SPEC-027's global pause reintroduced -- and it fails no other test, because the probe simply
    finds nothing.

    Scanned by **AST, not by line**, so prose is out of scope: `sinks/base.py` documents the
    rename and therefore has to name the attribute it replaced, and a line-oriented check reads
    that sentence as the defect. A lint whose scope includes the text explaining it is the
    SPEC-035 failure in a new place -- there it was a lint seeding its vocabulary from the text
    it linted; caught here by this test failing on its own first run.
    """
    offenders = []
    scanned = 0
    for path in [*(_ROOT / "src").rglob("*.py"), *(_ROOT / "tests").rglob("*.py")]:
        if path.name == pathlib.Path(__file__).name:
            continue
        scanned += 1
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            named = (
                (isinstance(node, ast.Attribute) and node.attr == "stop_signal")
                or (isinstance(node, ast.Name) and node.id == "stop_signal")
                or (isinstance(node, ast.arg) and node.arg == "stop_signal")
                or (isinstance(node, ast.Constant) and node.value == "stop_signal")
            )
            if named:
                offenders.append(f"{path.relative_to(_ROOT)}:{getattr(node, 'lineno', 0)}")
    assert scanned > 100, f"the scan collapsed to {scanned} files -- an absence it cannot see"
    assert not offenders, "the un-namespaced attribute survives in code:\n" + "\n".join(offenders)


def test_a_sink_offering_the_namespaced_attribute_is_handed_the_event() -> None:
    """FR-006 AC-1's contract, exercised: define the attribute and the library fills it in."""
    lifecycle = pytest.importorskip("log_foundry._lifecycle")

    class Offering:
        def __init__(self) -> None:
            self.log_foundry_stop_signal: threading.Event | None = None

        def emit(self, batch: list[dict]) -> None: ...
        def close(self) -> None: ...

    class NotOffering:
        def emit(self, batch: list[dict]) -> None: ...
        def close(self) -> None: ...

    stop = threading.Event()
    offering, bare = Offering(), NotOffering()
    lifecycle.offer_stop_signal(offering, stop)
    lifecycle.offer_stop_signal(bare, stop)

    assert offering.log_foundry_stop_signal is stop
    assert not hasattr(bare, "log_foundry_stop_signal"), "a sink that declines must not gain one"


def test_every_shipped_sink_module_imports() -> None:
    """A rename across twenty files is a rename that can leave one module unimportable.

    The extras are not installed in CI, so a sink whose driver is missing still imports -- the
    imports are lazy by design (SPEC-005) -- and that is what makes this check meaningful here.
    """
    import log_foundry.sinks

    failed = []
    for info in pkgutil.iter_modules(log_foundry.sinks.__path__):
        try:
            __import__(f"log_foundry.sinks.{info.name}")
        except Exception as exc:
            failed.append(f"{info.name}: {type(exc).__name__}: {exc}")
    assert not failed, "sink modules failed to import:\n" + "\n".join(failed)
