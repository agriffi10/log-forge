"""SPEC-035 FR-002 — every guard that asks about the worker declares which question it asks.

Four reviewers told SPEC-033 "ownership, not liveness", each naming a different call site; each
was fixed, and a fourth shipped broken. SPEC-035 FR-001 was that fourth one, and its own first
draft prescribed a predicate that would have re-broken SPEC-033 in the other direction. The fix
for a defect that recurs at *sites* is not another site-by-site correction: it is a roster the
tests derive, so a new or changed site must be classified before it can pass.

The roster is walked out of `decorator.py`'s AST rather than hand-listed, for the reason the sink
rosters are (SPEC-028 FR-002, SPEC-032): a hand-maintained list rots, and this one is about
completeness.
"""

import ast
import inspect
import textwrap

import pytest

decorator = pytest.importorskip("log_foundry.decorator")

# Tokens that make an expression a question about the worker. Every one names the worker, and
# that is load-bearing rather than incidental: SPEC-035 FR-003 answers "who owns the new sink"
# with a **return value** rather than a predicate, so the variable holding it is named
# `worker_holds_sink` — a roster keyed on worker-naming tokens cannot see a verdict stored under
# a name that hides what it is about. A first draft used the token `adopted` instead and matched
# `continue_trace`'s trace-context variable, which is not a worker question at all: a heuristic
# broad enough to catch every phrasing also catches things that are not the subject.
_SENTINELS = ("_worker", "worker", "draining", "retired")

EXISTENCE = "existence — is there a worker at all, and therefore anything to do"
LIVENESS = "liveness — who *performs* an action, and a retired worker performs nothing"
OWNERSHIP = "ownership — who *owns* a close, which a retired worker still does"
OWNERSHIP_AND_MOMENT = "ownership ∧ moment — whose stop event the sink should be holding *now*"
NOT_A_WORKER_QUESTION = "not a worker predicate — a module flag about the orphan path"

# One row per site: (enclosing function, the expression as `ast.unparse` renders it) -> why.
# Adding a call site without adding a row fails `test_every_worker_predicate_is_classified`,
# which is the whole point of the FR: a new site must be *decided*, not defaulted.
ROSTER: dict[tuple[str, str], tuple[str, str]] = {
    ("_get_worker", "_worker is None"): (
        EXISTENCE,
        (
            "the double-checked build. Neither liveness nor ownership: a retired worker is still the "
            "process worker, and rebuilding one would fight a process trying to exit (SPEC-019)."
        ),
    ),
    ("_live_worker", "worker is None or worker.retired"): (
        LIVENESS,
        "the definition of the liveness helper itself, rather than a consumer of it.",
    ),
    ("_offer_orphan_signal", "worker is not None and worker.sink is sink and worker.draining"): (
        OWNERSHIP_AND_MOMENT,
        (
            "SPEC-035 FR-001. Ownership alone skips for a worker whose shutdown has finished, leaving "
            "a sink still being written to holding a set event - SPEC-033 FR-004's tight retry loop. "
            "Liveness alone un-skips for the whole drain, handing the drain thread a fresh event "
            "nobody will set. Both were measured; only the conjunction is right."
        ),
    ),
    ("_close_orphan_sink", "_worker is not None and _worker.sink is owed"): (
        OWNERSHIP,
        (
            "a retired worker still owns its sink's close, and deliberately declines it when its "
            "shutdown expired, because the drain thread may still be inside emit (SPEC-027 FR-004)."
        ),
    ),
    ("_shutdown_worker", "_worker is not None"): (
        EXISTENCE,
        (
            "which exit path to take. A worker that exists drains; otherwise the orphan sink is "
            "closed directly (SPEC-031 FR-006)."
        ),
    ),
    ("_swap_sink", "_live_worker()"): (
        LIVENESS,
        (
            "the call that answers who performs. Kept as its own row rather than folded into the test "
            "below, because the two are separable: reverting FR-001 moves a _live_worker() call into "
            "_offer_orphan_signal, and a roster counting only predicates would not notice."
        ),
    ),
    ("_swap_sink", "worker is not None"): (
        LIVENESS,
        (
            "who performs the swap. Worker.swap_sink returns early once shut down, so routing a swap "
            "to a retired worker loses the handoff entirely (SPEC-033 FR-002)."
        ),
    ),
    ("_swap_sink", "_worker is None or _worker.sink is not old"): (
        OWNERSHIP,
        (
            "who owns the *old* sink's close. Answering this one with liveness closes it twice on a "
            "clean shutdown and under a live writer on an expired one - both measured."
        ),
    ),
    ("_swap_sink", "not worker_holds_sink"): (
        OWNERSHIP,
        (
            "who owns the *new* sink when the worker declined mid-swap (SPEC-035 FR-003). Carried by "
            "a return value because only the worker knows whether it got as far as reassigning."
        ),
    ),
    ("_flush_worker", "worker is None"): (
        EXISTENCE,
        (
            "a process that never logged has nothing to drain, and building a thread to prove it "
            "would be pure cost (SPEC-013)."
        ),
    ),
    ("_worker_health", "worker is None"): (
        EXISTENCE,
        "health() creates no worker; the zeros describe a process that never logged.",
    ),
    ("_worker_health", "_orphan_retired and (not health.retired)"): (
        NOT_A_WORKER_QUESTION,
        (
            "_orphan_retired is a module flag and health.retired is the snapshot being corrected - "
            "synthesizing retired for a process with no worker (SPEC-031 FR-006)."
        ),
    ),
}


def _collect(node: ast.AST, fn: str, found: dict[tuple[str, str], str]) -> None:
    """Records the outermost boolean expression that mentions the worker, then stops descending.

    Stopping matters: `worker is not None and worker.sink is sink and worker.draining` is one
    site, not four, and descending would file the same guard under three more rows nobody could
    keep in step with it.

    Args:
      node: The AST node to inspect.
      fn: The enclosing function's name.
      found: The mapping being built, mutated in place.

    Returns:
      None.

    Raises:
      None.
    """
    is_boolean = isinstance(node, ast.Compare | ast.BoolOp) or (
        isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not)
    )
    is_liveness_call = isinstance(node, ast.Call) and getattr(node.func, "id", None) == (
        "_live_worker"
    )
    if is_boolean or is_liveness_call:
        rendered = ast.unparse(node)
        if any(token in rendered for token in _SENTINELS):
            found[(fn, rendered)] = rendered
            return
    for child in ast.iter_child_nodes(node):
        _collect(child, fn, found)


def _predicates() -> dict[tuple[str, str], str]:
    """Derives every worker-question site in `decorator.py` from its AST.

    Every **boolean-valued** expression naming the worker, wherever it appears — not only the
    tests of `if`/`while`. A first draft walked conditions only, and a new site written as
    `return _worker is not None and ...` passed the roster unclassified: the phrasing changes,
    the question does not. Calls to `_live_worker` are collected too, so `worker =
    _live_worker()` is seen even though the assignment is not itself a predicate.

    Args:
      None.

    Returns:
      A mapping of (enclosing function, unparsed expression) to the unparsed expression.

    Raises:
      None.
    """
    tree = ast.parse(textwrap.dedent(inspect.getsource(decorator)))
    found: dict[tuple[str, str], str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            for child in ast.iter_child_nodes(node):
                _collect(child, node.name, found)
    return found


def test_every_worker_predicate_is_classified() -> None:
    """AC-1, AC-2. A new or changed site must be decided, not defaulted.

    This is the criterion that would have caught SPEC-035 FR-001: the shipped
    `_offer_orphan_signal` guard read `_live_worker()`, which no row here would have declared.
    """
    found = set(_predicates())
    declared = set(ROSTER)

    unclassified = found - declared
    assert not unclassified, (
        "these worker-question sites are not in the roster — classify each one:\n  "
        + "\n  ".join(f"{fn}: {expr}" for fn, expr in sorted(unclassified))
    )
    stale = declared - found
    assert not stale, (
        "these roster rows match no site — the code moved and the roster did not:\n  "
        + "\n  ".join(f"{fn}: {expr}" for fn, expr in sorted(stale))
    )


def test_every_row_states_a_category_and_a_reason() -> None:
    """AC-2. A category with no reason is a row that will be copied rather than thought about."""
    for site, (category, reason) in ROSTER.items():
        assert category in {
            EXISTENCE,
            LIVENESS,
            OWNERSHIP,
            OWNERSHIP_AND_MOMENT,
            NOT_A_WORKER_QUESTION,
        }, f"{site} has an unknown category"
        assert len(reason) > 40, f"{site}'s reason is too short to be one"


def test_the_roster_finds_the_bare_form_that_shipped_unseen() -> None:
    """AC-1. `_worker is not None` on its own is the phrasing SPEC-033's docstrings warn about,
    and a walk looking only for `_live_worker()` and `.sink is` comparisons would never see it."""
    assert ("_shutdown_worker", "_worker is not None") in _predicates()


def test_the_roster_finds_a_verdict_carried_by_a_return_value() -> None:
    """AC-1. SPEC-035 FR-003 answers an ownership question with a bool, not a predicate — the
    form a roster built only from worker-name comparisons would miss."""
    assert ("_swap_sink", "not worker_holds_sink") in _predicates()


def test_the_walker_matches_every_shape_it_claims_to() -> None:
    """Guards the guard: a walker that matched nothing would classify nothing, vacuously.

    The `return` case is here because a draft that walked only `if`/`while` tests let a new site
    written that way through the roster unclassified, which is the completeness the FR is for.
    """
    source = textwrap.dedent(
        """
        def f():
            if _worker is None:
                pass
            while worker.draining:
                pass
            x = None if worker is None or worker.retired else worker
            assert _worker.sink is owed
            y = _live_worker()
            if not worker_holds_sink:
                pass
            return _worker is not None and _worker.sink is None
        """
    )
    found: dict[tuple[str, str], str] = {}
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for child in ast.iter_child_nodes(node):
                _collect(child, node.name, found)
    assert {expr for _, expr in found} == {
        "_worker is None",
        "worker is None or worker.retired",
        "_worker.sink is owed",
        "_live_worker()",
        "not worker_holds_sink",
        "_worker is not None and _worker.sink is None",
    }, found


def test_the_walker_ignores_uses_that_are_not_questions() -> None:
    """A roster that filed every mention of the worker would be noise nobody maintains."""
    source = textwrap.dedent(
        """
        def f():
            _worker = Worker(_ensure_sink())
            worker.shutdown(timeout)
            worker.flush(timeout)
            _worker.sink = new_sink
        """
    )
    found: dict[tuple[str, str], str] = {}
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for child in ast.iter_child_nodes(node):
                _collect(child, node.name, found)
    assert not found, f"these are actions on the worker, not questions about it: {found}"
