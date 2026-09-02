"""SPEC-051 FR-005 — the forwarded HTTP keywords, held to `HTTPSink` by derivation.

Seven platform sinks took `**http_kwargs: object` and carried a `# type: ignore[arg-type]` on
the call that forwarded it, so `timeout="not-a-float"` passed `mypy --strict` and construction
alike and failed at the first request. `Unpack[TypedDict]` replaced that, and a `TypedDict` is a
hand-written restatement of another signature — exactly the shape that rots. Everything here is
derived from `HTTPSink.__init__` and from each sink's own AST, so a keyword added to one side and
not the other fails rather than drifts.

Two checks, not three. A third asserted that each narrower shape agrees with `HTTPKwargs`, and
mutation testing could not kill it: the five compose by inheritance *upward*, so every edit to a
narrower shape lands in `HTTPKwargs` too and is caught by the anchor below, and the one failure
mode left to it — a key redeclared with a different type — is refused by `mypy` before any test
runs. Three mutants, none of them killed by it alone. It read as coverage and was not.
"""

import ast
import collections.abc
import inspect
import pathlib
import typing

import pytest

from log_foundry.sinks import http

_SINK_PKG = pathlib.Path(http.__file__).resolve().parent

# `sinks/http.py` carries `from __future__ import annotations`, so every annotation is a string.
# The names still resolve because that module imports `Callable` at runtime, behind a ruff TC003
# suppression -- these five shapes are public and frozen at 1.0, and a public type a consumer
# cannot pass to `get_type_hints` is a poor one. This mapping is the belt to that braces: move
# the import back under `TYPE_CHECKING` and the roster keeps working while a consumer breaks.
_LOCALNS = {"Callable": collections.abc.Callable, "Any": typing.Any}

_FORWARDING_SINKS = (
    "datadog",
    "elasticsearch",
    "honeycomb",
    "logstash",
    "loki",
    "newrelic",
    "splunk",
)


def _hints(shape: type) -> dict[str, object]:
    """Resolves a TypedDict's annotations, including the ones only importable for typing.

    Args:
      shape: The TypedDict to resolve.

    Returns:
      Its field names mapped to resolved annotation objects.

    Raises:
      None.
    """
    return dict(typing.get_type_hints(shape, localns=_LOCALNS))


def _httpsink_keyword_params() -> dict[str, object]:
    """Reads `HTTPSink.__init__`'s keyword-only parameters and their resolved annotations.

    Args:
      None.

    Returns:
      Parameter names mapped to resolved annotation objects, `url` excluded as positional.

    Raises:
      None.
    """
    resolved = typing.get_type_hints(http.HTTPSink.__init__, localns=_LOCALNS)
    return {
        name: resolved[name]
        for name, param in inspect.signature(http.HTTPSink.__init__).parameters.items()
        if param.kind is inspect.Parameter.KEYWORD_ONLY
    }


def test_the_widest_shape_is_httpsink_exactly() -> None:
    """FR-005 AC-4. Names *and* annotations, which is the half a key-set check cannot see.

    The first draft of `HTTPForwardKwargs` declared `headers: dict[str, str]` where `HTTPSink`
    takes `dict[str, str] | None`, and `DatadogSink(headers=None)` — valid at runtime — became a
    type error. Every key name was right, so a roster over names alone was green over it.
    """
    expected = _httpsink_keyword_params()
    actual = _hints(http.HTTPKwargs)
    assert set(actual) == set(expected), (
        f"only in HTTPKwargs: {sorted(set(actual) - set(expected))}; "
        f"only in HTTPSink: {sorted(set(expected) - set(actual))}"
    )
    mismatched = {k: (actual[k], expected[k]) for k in expected if actual[k] != expected[k]}
    assert not mismatched, f"annotation drift: {mismatched}"


def _popped_by_merge_headers() -> set[str]:
    """Reads which keys `merge_headers` removes from a caller's kwargs before forwarding.

    Derived rather than listed: a key it pops is one a caller may pass *and* the sink may set,
    which is the single exception to "a forwarded shape excludes what the sink sets".

    Args:
      None.

    Returns:
      The popped key names.

    Raises:
      None.
    """
    tree = ast.parse((_SINK_PKG / "http.py").read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "merge_headers":
            return {
                call.args[0].value
                for call in ast.walk(node)
                if isinstance(call, ast.Call)
                and isinstance(call.func, ast.Attribute)
                and call.func.attr == "pop"
                and call.args
                and isinstance(call.args[0], ast.Constant)
                and isinstance(call.args[0].value, str)
            }
    raise AssertionError("merge_headers is gone -- this roster's exception no longer derives")


def _forwarding_init(module_name: str) -> ast.FunctionDef:
    """Finds the `__init__` in a sink module that takes `**http_kwargs`.

    Args:
      module_name: The module's stem under `sinks/`.

    Returns:
      Its AST node.

    Raises:
      AssertionError: If no such `__init__` is there any more.
    """
    tree = ast.parse((_SINK_PKG / f"{module_name}.py").read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.FunctionDef)
            and node.name == "__init__"
            and node.args.kwarg is not None
            and node.args.kwarg.arg == "http_kwargs"
        ):
            return node
    raise AssertionError(f"{module_name} no longer forwards **http_kwargs")


@pytest.mark.parametrize("module_name", _FORWARDING_SINKS)
def test_a_sink_forwards_a_shape_that_excludes_what_it_sets(module_name: str) -> None:
    """FR-005 AC-4, the constraint `mypy` enforces, asserted where a reader can see why.

    A sink forwards exactly the `HTTPSink` keywords it does not set or shadow. It cannot forward
    the others — the first is a duplicate argument, the second is `mypy`'s "overlap between
    parameter names and ** TypedDict items" — and it must forward the rest, or a keyword the sink
    still accepts at runtime has been withdrawn from every typed caller. `headers` is the sole
    exception and is not hard-coded: `merge_headers` pops it, and the pop is read out of
    `http.py`.

    Both call forms are matched. `LogstashSink` subclasses nothing and constructs an `HTTPSink`
    inside a conditional, so a test keyed on `super().__init__` alone would skip it silently and
    still pass — its own parameters already cover the three names at issue.
    """
    node = _forwarding_init(module_name)
    annotation = node.args.kwarg.annotation
    assert isinstance(annotation, ast.Subscript), f"{module_name} is not Unpack[...] any more"
    shape = getattr(http, annotation.slice.id)

    own = {arg.arg for arg in node.args.kwonlyargs}
    passed = {
        keyword.arg
        for call in ast.walk(node)
        if isinstance(call, ast.Call)
        and (
            (isinstance(call.func, ast.Name) and call.func.id == "HTTPSink")
            or (isinstance(call.func, ast.Attribute) and call.func.attr == "__init__")
        )
        for keyword in call.keywords
        if keyword.arg is not None
    }
    assert passed, f"{module_name}: found no forwarding call -- the scan stopped matching"

    calls_merge = any(
        isinstance(call.func, ast.Name) and call.func.id == "merge_headers"
        for call in ast.walk(node)
        if isinstance(call, ast.Call)
    )
    exempt = _popped_by_merge_headers() if calls_merge else set()
    owned = (own | passed) - exempt
    expected = set(_hints(http.HTTPKwargs)) - owned
    actual = set(_hints(shape))

    # Equality, not disjointness. Disjointness catches only the direction `mypy` already
    # refuses; a shape that is too *narrow* silently withdraws a keyword the sink still accepts
    # at runtime -- drop `timeout` from `HTTPRetryKwargs` and `ElasticsearchSink(timeout=...)`
    # becomes a type error with nothing to notice it.
    assert actual == expected, (
        f"{module_name}: forwards but should not {sorted(actual - expected)}; "
        f"should forward but does not {sorted(expected - actual)}"
    )


def test_no_forwarding_call_still_suppresses_arg_type() -> None:
    """FR-005 AC-3. `[arg-type]` blinded the whole call; what is left covers one popped key."""
    offenders = [
        f"{name}.py"
        for name in _FORWARDING_SINKS
        if "type: ignore[arg-type]" in (_SINK_PKG / f"{name}.py").read_text(encoding="utf-8")
    ]
    assert not offenders, f"the total suppression survives in: {offenders}"
    assert len(_FORWARDING_SINKS) == 7, "the roster shrank -- an absence it cannot see"
