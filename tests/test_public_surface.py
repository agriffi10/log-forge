"""SPEC-034 — the public surface, asserted where it freezes at 1.0.

Every criterion here is about a *name* rather than a behaviour, which is why they live together
rather than beside the code they check: at `1.0.0` a name in a shipped signature stops being a
wart and becomes a promise, and fixing one afterwards costs a major version. The tests are
derived from the package rather than hand-listed wherever that is possible, on the roster lesson
SPEC-032 and SPEC-035 both paid for.
"""

import ast
import dataclasses
import inspect
import io
import pathlib
import pkgutil
import re
import threading
import time

import pytest

log_foundry = pytest.importorskip("log_foundry")
config = pytest.importorskip("log_foundry.config")
model = pytest.importorskip("log_foundry.model")

_ROOT = pathlib.Path(__file__).resolve().parents[1]
_SINK_PKG = _ROOT / "src" / "log_foundry" / "sinks"

# FR-001 AC-2: a blacklist of names, not a rule derived from syntax. Role is not inferable from
# a name alone -- `stream` is a positional *transport* in StdoutSink and a positional
# *destination* in RedisStreamsSink -- so a syntactic rule would have to guess, and this does not.
_INJECTED = ("client", "sdk", "producer", "connection", "opener")


class _Recorder:
    """A minimal sink, so a test can configure one without touching the real destinations."""

    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []

    def emit(self, batch: list[dict[str, object]]) -> None:
        self.events.extend(batch)

    def close(self) -> None: ...


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


def test_an_incomplete_sink_subclass_cannot_be_instantiated() -> None:
    """FR-005 made `Sink` public, which invites inheritance — and inheritance was unsafe.

    A `Protocol` whose members have empty bodies is an ABC to its subclasses only if those
    members are `@abstractmethod`. Without them, `class MySink(Sink)` with `def emmit` (one
    typo) instantiated happily and its inherited `emit` returned `None`: measured, three events
    gone, `flush()` returning `True`, every counter at zero — the "sink the worker believes"
    failure SPEC-018/026/032 were spent removing. `mypy` refused it; only the runtime did not,
    and the runtime is the side that loses data.
    """
    with pytest.raises(TypeError, match="abstract"):

        class Typo(log_foundry.Sink):  # type: ignore[misc]
            def emmit(self, batch: list[dict[str, object]]) -> None: ...
            def close(self) -> None: ...

        Typo()


def test_a_complete_sink_subclass_and_a_structural_one_both_work() -> None:
    """The guard must not cost the structural satisfaction every shipped sink relies on.

    No sink in this package inherits `Sink` — they all satisfy it structurally — so a fix that
    made inheritance mandatory would break the entire sink family and the third-party contract
    with it.
    """

    class Inherited(log_foundry.Sink):
        def emit(self, batch: list[dict[str, object]]) -> None: ...
        def close(self) -> None: ...

    class Structural:
        def emit(self, batch: list[dict[str, object]]) -> None: ...
        def close(self) -> None: ...

    Inherited()
    assert isinstance(Structural(), log_foundry.Sink)


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


# -- FR-003: get_config() cannot be used to mutate the live config --------------------------


def test_the_returned_config_refuses_a_sink_reassignment() -> None:
    """FR-003 AC-1. SPEC-030's defect, reachable publicly with no underscore in sight.

    Assigning `.sink` retargeted what the config *reported* while every event continued to the
    sink the worker had already captured: measured before the fix as A got 4, B got 0, the
    config claiming B, `incomplete_swaps` at zero and A never closed.
    """
    with pytest.raises(dataclasses.FrozenInstanceError):
        log_foundry.get_config().sink = object()  # type: ignore[misc]


def test_the_returned_config_refuses_a_ceiling_that_configure_would_reject() -> None:
    """FR-003 AC-2. `max_value_bytes = 0` empties every event it touches."""
    with pytest.raises(dataclasses.FrozenInstanceError):
        log_foundry.get_config().max_value_bytes = 0  # type: ignore[misc]
    with pytest.raises(ValueError):
        log_foundry.configure(max_value_bytes=0)


def test_every_documented_read_still_works() -> None:
    """FR-003 AC-3. The freeze must cost a reader nothing."""
    log_foundry.configure(service="billing", version="2.1", env="prod", defaults={"team": "core"})
    cfg = log_foundry.get_config()
    assert (cfg.service, cfg.version, cfg.env) == ("billing", "2.1", "prod")
    assert cfg.defaults == {"team": "core"}
    assert cfg.sink is not None
    assert (cfg.max_value_bytes, cfg.max_stack_bytes, cfg.max_keys, cfg.max_depth) == (
        8192,
        32768,
        256,
        8,
    )


def test_the_returned_config_is_a_copy_not_the_live_object() -> None:
    """FR-003 AC-4. `object.__setattr__` reaches through any frozen dataclass.

    So the freeze alone is not the guarantee — the guarantee is that what a caller can reach is
    not the object the library reads.
    """
    cfg = log_foundry.get_config()
    assert cfg is not config._live_config()
    object.__setattr__(cfg, "service", "defeated")
    assert config._live_config().service != "defeated"


def test_the_returned_defaults_dict_is_copied_too() -> None:
    """FR-003 AC-5, and the one the FR's own recipe would have failed.

    The Description proposes `dataclasses.replace(_config)` as "the straightforward answer".
    Measured while building this: `replace` **shares** the `defaults` dict, so that recipe hands
    back a frozen shell around the live mapping and the freeze is cosmetic at the one field that
    is not a scalar.
    """
    log_foundry.configure(service="t", defaults={"team": "core"})
    returned = log_foundry.get_config().defaults
    returned["team"] = "MUTATED"
    returned["injected"] = True
    assert config._live_config().defaults == {"team": "core"}


def test_no_module_imports_the_config_singleton_by_value() -> None:
    """FR-003 AC-7. The rebind is only safe because nothing holds the pre-rebind object.

    `configure()` replaces the module global; a `from log_foundry.config import _config`
    anywhere would keep the object it bound at import time forever, reading stale settings for
    the life of the process and never failing a test that did not look for it.
    """
    offenders = []
    for path in (_ROOT / "src").rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            by_import = isinstance(node, ast.ImportFrom) and any(
                a.name == "_config" for a in node.names
            )
            # `from log_foundry import config` then `_C = config._config` holds the same stale
            # object by a route no ImportFrom scan sees. Review found the first version blind to
            # it, so the check is on the binding, not on one syntax for it.
            by_alias = (
                isinstance(node, ast.Assign)
                and isinstance(node.value, ast.Attribute)
                and node.value.attr == "_config"
            )
            if by_import or by_alias:
                offenders.append(f"{path.relative_to(_ROOT)}:{node.lineno}")
    assert not offenders, f"these hold the pre-rebind config: {offenders}"


def test_a_held_config_reference_does_not_track_later_calls() -> None:
    """`get_config()` is a snapshot now, where it used to track. Pin the behaviour change.

    Correct and intended — it is what makes the copy a copy — but a caller who held the result
    across a `configure()` used to see the new values and now does not, and nothing said so.
    """
    log_foundry.configure(service="before", sink=_Recorder())
    held = log_foundry.get_config()
    log_foundry.configure(service="after")

    assert held.service == "before", "a held reference is a snapshot"
    assert log_foundry.get_config().service == "after", "a fresh read sees the change"


def test_configure_distinguishes_an_empty_value_from_an_omitted_one() -> None:
    """`configure()` filters on `is not None`, and a truthiness filter would be wrong.

    `configure(defaults={})` must *clear* the defaults and `configure(service="")` must apply —
    both are values the caller supplied. Untested before review; the mutant `if value` survived
    the whole suite.
    """
    log_foundry.configure(service="svc", defaults={"team": "core"}, sink=_Recorder())
    log_foundry.configure(defaults={})
    assert log_foundry.get_config().defaults == {}, "an empty dict is a value, not an omission"

    log_foundry.configure(service="")
    assert log_foundry.get_config().service == "", "an empty string is a value, not an omission"


def test_a_nested_default_is_shared_and_says_so() -> None:
    """FR-003 AC-5's bound, pinned rather than left to the docstring.

    `dict(defaults)` copies one level. A nested mutable stays live and reaches real events —
    review reproduced it. Deep-copying arbitrary caller objects inside an accessor that must not
    raise is the wider failure, so the bound is stated and asserted instead of closed.
    """
    log_foundry.configure(service="t", defaults={"team": {"name": "core"}}, sink=_Recorder())
    returned = log_foundry.get_config().defaults

    returned["added"] = True
    assert "added" not in config._live_config().defaults, "the top level is copied"

    returned["team"]["name"] = "MUTATED"  # type: ignore[index]
    assert config._live_config().defaults["team"] == {"name": "MUTATED"}, (
        "a nested value is shared -- documented, not fixed"
    )


def test_the_zero_config_path_still_resolves_a_sink() -> None:
    """The gap the spec's design missed, found by reading `config.py` before writing any of it.

    `_ensure_sink()` assigns `_config.sink = StdoutSink()` when nothing was configured — a
    second in-place mutation the FR never names, on the path a process takes when it calls
    `info()` without ever calling `configure(sink=...)`. Under `frozen=True` and without the
    rebind it raises `FrozenInstanceError` into the orphan guard, and every zero-config log is
    absorbed and lost.
    """
    config._rebind(sink=None)
    resolved = config._ensure_sink()
    assert resolved is not None
    assert config._live_config().sink is resolved, "the resolved default is retained, not rebuilt"


def test_configure_applies_every_field_in_one_rebind(monkeypatch) -> None:
    """The second gap: nine rebindings would leave a window on a half-applied config.

    Counted at **runtime**, not by shape. A first version counted `_rebind` call *nodes* in
    `configure()`'s source and passed against the exact defect it names — one syntactic call
    inside `for k, v in changed.items(): _rebind(**{k: v})` is nine executions and nine windows,
    with the whole suite green. Found by review, with that mutant.
    """
    log_foundry.configure(service="pre", sink=_Recorder())  # so _ensure_sink cannot rebind below
    calls = 0
    real = config._rebind

    def counting(**changed: object) -> None:
        nonlocal calls
        calls += 1
        real(**changed)

    monkeypatch.setattr(config, "_rebind", counting)
    log_foundry.configure(
        service="s",
        version="v",
        env="e",
        defaults={"a": 1},
        max_value_bytes=100,
        max_stack_bytes=200,
        max_keys=10,
        max_depth=3,
    )
    assert calls == 1, f"configure() rebound {calls} times; a reader can see a partial config"


def test_a_concurrent_orphan_log_cannot_revert_configure() -> None:
    """The regression the freeze introduced, and the reason `_config_lock` exists.

    Freezing turned each field assignment into a whole-config read-modify-write, and one of the
    two writers — `_ensure_sink()` — runs on the orphan logging path, on whatever application
    thread called `info()`. A stale snapshot there puts back the pre-`configure()` service,
    version, env, defaults **and** sink, permanently. Measured on the unlocked version: 268 of
    2000 trials, against 0 before the freeze. Wrong data in the log stream, silently, for the
    life of the process — SPEC-024's category.

    The window is widened deliberately rather than raced for. A first harness used a barrier and
    a 1 microsecond switch interval and reported 0 reversions **with the defect present**, which
    would have certified the fix against a measurement that never entered the window it claimed
    to clear.
    """
    real_replace = config.replace
    reached_the_window = threading.Event()

    def slow_replace(obj: object, **kw: object) -> object:
        if "sink" in kw and not isinstance(kw.get("sink"), _Recorder):
            reached_the_window.set()
            time.sleep(0.2)
        return real_replace(obj, **kw)

    chosen = _Recorder()
    config._rebind(sink=None, service="unknown", version="0.0.0")
    config.replace = slow_replace  # type: ignore[assignment]
    try:
        worker = threading.Thread(target=lambda: log_foundry.info("orphan resolves the default"))
        worker.start()
        assert reached_the_window.wait(5.0), "the harness never entered the window"
        log_foundry.configure(service="billing", version="2.1", sink=chosen)
        worker.join(10.0)
    finally:
        config.replace = real_replace  # type: ignore[assignment]

    live = config._live_config()
    assert (live.service, live.version) == ("billing", "2.1"), "configure() was reverted"
    assert live.sink is chosen, "configure()'s sink was reverted"


def test_two_threads_resolving_the_default_sink_get_the_same_object() -> None:
    """`_ensure_sink` returning its own local handed racing threads two different sinks.

    Measured 996 of 3000 trials before the double check. One of the two then received events and
    was referenced by nothing, so nothing closed it — SPEC-031 FR-006's invariant, broken from
    the other end.

    The window is injected, not raced for. A first version started eight threads on a barrier and
    **passed against the defect**: the unlocked form still re-read the global on its fast path, so
    seven of the eight found the first thread's sink and only a hair-fine interleave produced two.
    Holding the first resolver inside the replace makes every later thread arrive while the sink
    is still `None`, which is the state the defect needs.
    """
    config._rebind(sink=None)
    real_replace = config.replace
    first_call = threading.Event()

    def slow_replace(obj: object, **kw: object) -> object:
        if not first_call.is_set():
            first_call.set()
            time.sleep(0.2)
        return real_replace(obj, **kw)

    resolved: list[object] = []
    lock = threading.Lock()

    def resolve() -> None:
        sink = config._ensure_sink()
        with lock:
            resolved.append(sink)

    config.replace = slow_replace  # type: ignore[assignment]
    try:
        threads = [threading.Thread(target=resolve) for _ in range(8)]
        threads[0].start()
        assert first_call.wait(5.0), "the harness never entered the window"
        for t in threads[1:]:
            t.start()
        for t in threads:
            t.join(10.0)
    finally:
        config.replace = real_replace  # type: ignore[assignment]

    assert len(resolved) == 8, "a resolver did not finish"
    assert len({id(sink) for sink in resolved}) == 1, "racing threads got different default sinks"
    assert config._live_config().sink is resolved[0], "the global names the sink handed out"


def test_the_per_event_path_does_not_pay_for_the_config_copy() -> None:
    """FR-003 AC-6. `build_event` reads the config one to three times per event.

    Routing those through `get_config()` would allocate a `Config` **and** a `defaults` dict per
    event. Checked structurally for the same reason the baggage equivalent is: the behaviour is
    identical and only the cost changes, which no behavioural test can see.
    """
    model_src = (_ROOT / "src" / "log_foundry" / "model.py").read_text(encoding="utf-8")
    calls = {
        ast.unparse(node)
        for node in ast.walk(ast.parse(model_src))
        if isinstance(node, ast.Call) and ast.unparse(node).endswith("config()")
    }
    assert calls == {"_live_config()"}, f"model.py's config reads are {sorted(calls)}"


def test_no_config_copy_is_allocated_per_event(monkeypatch) -> None:
    """FR-003 AC-6's benchmark, counted rather than timed.

    The AC asks for a benchmark rather than an assertion. A wall-clock one would be a race
    against the machine — this repo has been bitten by timing tests that failed on their own
    setup — so what is measured is the thing the cost *is*: how many `Config` copies the
    per-event path allocates. `get_config()` calls `replace`; `_live_config()` does not. Five
    hundred events must allocate none.
    """
    copies = 0
    real_replace = config.replace

    def counting_replace(*args: object, **kwargs: object) -> object:
        nonlocal copies
        copies += 1
        return real_replace(*args, **kwargs)

    monkeypatch.setattr(config, "replace", counting_replace)
    log_foundry.configure(service="bench", defaults={"team": "core"})
    copies = 0  # discard the configure() rebind; only the per-event path is under measurement

    span = model.Span(
        trace_id="0" * 32, span_id="0" * 16, parent_span_id=None, name="bench", start_ts=0.0
    )
    for i in range(500):
        model.build_event(span, "INFO", "m", fields={"i": i}, baggage={"team": "core"})

    assert copies == 0, f"{copies} Config copies allocated across 500 events"


def test_nothing_expensive_or_reentrant_runs_under_the_config_lock() -> None:
    """A new lock's real risk is what runs inside it, and that is what rots.

    `_ensure_sink()` is called with `decorator._worker_lock` already held
    (`decorator.py:235`), so the order is worker -> config and must stay one-way: anything under
    `_config_lock` that reached back for `_worker_lock`, or blocked on I/O, would deadlock or
    stall every configure and every zero-config log behind it. Today only `replace()` and
    `StdoutSink()` run there, and `StdoutSink.__init__` is a single attribute assignment.

    Asserted as a whitelist rather than a search for the bad case: the set of things that would
    be unsafe is open, and the set that is currently safe is two.
    """
    under_lock: list[str] = []

    def walk(node: ast.AST, held: bool) -> None:
        for child in ast.iter_child_nodes(node):
            here = held
            if isinstance(child, ast.With) and "_config_lock" in ast.unparse(
                child.items[0].context_expr
            ):
                here = True
            if here and isinstance(child, ast.Call):
                under_lock.append(ast.unparse(child.func))
            walk(child, here)

    source = (_ROOT / "src" / "log_foundry" / "config.py").read_text(encoding="utf-8")
    walk(ast.parse(source), False)

    assert set(under_lock) == {"replace", "StdoutSink"}, (
        f"new work under _config_lock: {sorted(set(under_lock))}. "
        "Anything that blocks, or that reaches for _worker_lock, deadlocks or stalls "
        "every configure() and every zero-config log."
    )


# -- FR-004: echo and message stop being reserved words --------------------------------------


def _emitters() -> list[str]:
    """The public emitters, derived from `api.py` rather than listed.

    AC-6 asks that the treatment reach all five "derived rather than hand-applied". The five
    functions stay hand-written — the repo's docstring convention wants each one readable, and
    metaprogramming them would trade that for nothing — so what is derived is the *check*: any
    module-level public function that routes through `_log` must conform, and a sixth emitter
    fails this until it does.

    Args:
      None.

    Returns:
      The emitter function names.

    Raises:
      None.
    """
    source = (_ROOT / "src" / "log_foundry" / "api.py").read_text(encoding="utf-8")
    found = []
    for node in ast.parse(source).body:
        if not isinstance(node, ast.FunctionDef) or node.name.startswith("_"):
            continue
        if any(
            isinstance(inner, ast.Call) and getattr(inner.func, "id", None) == "_log"
            for inner in ast.walk(node)
        ):
            found.append(node.name)
    return found


def test_the_emitter_roster_is_the_five_and_is_not_empty() -> None:
    """The check below is per-emitter; an empty roster would satisfy it perfectly."""
    assert sorted(_emitters()) == ["critical", "debug", "error", "info", "warning"]


@pytest.mark.parametrize("name", _emitters())
def test_every_emitter_takes_the_escape_hatch(name: str) -> None:
    """FR-004 AC-6, derived. A sixth emitter fails this until it conforms."""
    params = inspect.signature(getattr(log_foundry, name)).parameters
    assert params["fields"].kind is inspect.Parameter.KEYWORD_ONLY, name
    assert params["fields"].default is None, name
    assert any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()), name


@pytest.mark.parametrize("name", _emitters())
def test_every_emitter_routes_reserved_names_through_fields(name: str) -> None:
    """FR-004 AC-1, on every emitter rather than on `info` alone."""
    recorder = _Recorder()
    log_foundry.configure(service="t", sink=recorder)
    getattr(log_foundry, name)(
        "m", fields={"echo": "v", "message": "w", "not-an-identifier": 1, "fields": "self"}
    )
    assert recorder.events[-1]["fields"] == {
        "echo": "v",
        "message": "w",
        "not-an-identifier": 1,
        "fields": "self",
    }


def test_the_escape_hatch_can_express_its_own_name() -> None:
    """`fields=` is the third reserved word, and the FR does not say so.

    Making it a real parameter is itself breaking in the other direction: the var-keyword used
    to be named `**fields`, so `info("x", fields={"a": 1})` *worked* and produced a field
    literally called `fields` holding that dict. What makes that survivable — and what makes
    "reserved" tolerable at all — is that every reserved name, including this one, has exactly
    one route through.
    """
    recorder = _Recorder()
    log_foundry.configure(service="t", sink=recorder)
    log_foundry.info("m", fields={"fields": {"nested": True}})
    assert recorder.events[-1]["fields"] == {"fields": {"nested": True}}


def test_the_keyword_form_wins_a_collision() -> None:
    """FR-004 AC-3. The docstring says which wins; this is what makes it true."""
    recorder = _Recorder()
    log_foundry.configure(service="t", sink=recorder)
    log_foundry.info("m", user="from-kwargs", fields={"user": "from-fields", "only": 1})
    assert recorder.events[-1]["fields"] == {"user": "from-kwargs", "only": 1}


def test_the_keyword_form_still_works_unchanged() -> None:
    """FR-004 AC-2. The common call must not have moved."""
    recorder = _Recorder()
    log_foundry.configure(service="t", sink=recorder)
    log_foundry.info("m", user_id="u42", count=3)
    assert recorder.events[-1]["fields"] == {"user_id": "u42", "count": 3}


def test_echo_still_controls_the_console(monkeypatch) -> None:
    """FR-004 AC-4. This FR adds a route to the *field*; it does not move the flag.

    Injects the writer's stream, which is what `tests/test_console_echo.py` already does.
    Neither capture fixture works here: `ConsoleWriter` resolves `sys.stderr` once at
    construction, so it writes past the object `capsys` swaps in, and `capfd` did not see it
    either. A first version used `capsys`, read an empty string, and would have passed just as
    happily had the echo been broken — while pytest's own "Captured stderr call" section printed
    the line the assertion could not see.
    """
    console_mod = pytest.importorskip("log_foundry.console")
    stream = io.StringIO()
    monkeypatch.setattr("log_foundry.api._console", console_mod.ConsoleWriter(stream=stream))
    log_foundry.configure(service="t", sink=_Recorder())

    log_foundry.info("quiet")
    assert stream.getvalue() == "", "no echo without echo=True"

    log_foundry.info("loud", echo=True)
    assert "loud" in stream.getvalue()


def test_the_callers_fields_mapping_is_not_mutated() -> None:
    """A caller reusing one dict across calls must not accumulate the others' keywords."""
    recorder = _Recorder()
    log_foundry.configure(service="t", sink=recorder)
    shared = {"base": 1}
    log_foundry.info("first", extra="a", fields=shared)
    log_foundry.info("second", fields=shared)

    assert shared == {"base": 1}, "the caller's mapping was mutated"
    assert recorder.events[-1]["fields"] == {"base": 1}
