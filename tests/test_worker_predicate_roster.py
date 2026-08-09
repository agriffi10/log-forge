"""SPEC-035 FR-002 — every guard that asks about the worker declares which question it asks.

Three reviewers told SPEC-033 "ownership, not liveness", each naming a different call site;
each was fixed, and a fourth site shipped broken. SPEC-035 FR-001 was that fourth one, and its own first
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

# A plain import, not `importorskip`: a roster whose value is that it cannot be bypassed must
# not skip itself when the module it walks is renamed or fails to import.
from log_foundry import decorator

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

# One row per site: (enclosing function, the expression as `ast.unparse` renders it) -> why.
# Adding a call site without adding a row fails `test_every_worker_predicate_is_classified`,
# which is the whole point of the FR: a new site must be *decided*, not defaulted.
# Keyed by (function, expression, occurrence index within that function): two textually
# identical guards in one function ask two questions and get two rows.
ROSTER: dict[tuple[str, str, int], tuple[str, str]] = {
    ("_get_worker", "_worker is None", 1): (
        EXISTENCE,
        (
            "the second half of the double-check, re-read under the lock. Two rows for one "
            "idiom is the honest count: each is a separate decision the compiler will not "
            "merge, and collapsing them hides that the outer one is deliberately unlocked."
        ),
    ),
    ("_get_worker", "_worker is None", 0): (
        EXISTENCE,
        (
            "the double-checked build. Neither liveness nor ownership: a retired worker is still the "
            "process worker, and rebuilding one would fight a process trying to exit (SPEC-019)."
        ),
    ),
    ("_live_worker", "worker is None or worker.retired", 0): (
        LIVENESS,
        "the definition of the liveness helper itself, rather than a consumer of it.",
    ),
    ("_offer_orphan_signal", "worker is not None and worker.sink is sink and worker.draining", 0): (
        OWNERSHIP_AND_MOMENT,
        (
            "SPEC-035 FR-001. Ownership alone skips for a worker whose shutdown has finished, leaving "
            "a sink still being written to holding a set event - SPEC-033 FR-004's tight retry loop. "
            "Liveness alone un-skips for the whole drain, handing the drain thread a fresh event "
            "nobody will set. Both were measured; only the conjunction is right."
        ),
    ),
    ("_close_orphan_sink", "_worker is not None and _worker.sink is owed", 0): (
        OWNERSHIP,
        (
            "a retired worker still owns its sink's close, and deliberately declines it when its "
            "shutdown expired, because the drain thread may still be inside emit (SPEC-027 FR-004)."
        ),
    ),
    ("_shutdown_worker", "_worker is not None", 0): (
        EXISTENCE,
        (
            "which exit path to take. A worker that exists drains; otherwise the orphan sink is "
            "closed directly (SPEC-031 FR-006)."
        ),
    ),
    ("_swap_sink", "_live_worker()", 0): (
        LIVENESS,
        (
            "the call that answers who performs. Kept as its own row rather than folded into the test "
            "below, because the two are separable: reverting FR-001 moves a _live_worker() call into "
            "_offer_orphan_signal, and a roster counting only predicates would not notice."
        ),
    ),
    ("_swap_sink", "worker is not None", 0): (
        LIVENESS,
        (
            "the in-lock branch: a live worker means the orphan path relinquishes its record, "
            "because that worker is about to own the handoff. Liveness rather than ownership - "
            "a retired worker performs no swap, so the record must stay with the orphan path."
        ),
    ),
    ("_swap_sink", "worker is not None", 1): (
        LIVENESS,
        (
            "the out-of-lock branch: who performs the swap. Worker.swap_sink returns early once "
            "shut down, so routing a swap to a retired worker loses the handoff entirely "
            "(SPEC-033 FR-002). Textually identical to the row above and a different question, "
            "which is why the key carries an occurrence index rather than only the text."
        ),
    ),
    ("_swap_sink", "_worker is None or _worker.sink is not old", 0): (
        OWNERSHIP,
        (
            "who owns the *old* sink's close. Answering this one with liveness closes it twice on a "
            "clean shutdown and under a live writer on an expired one - both measured."
        ),
    ),
    ("_swap_sink", "not worker_holds_sink", 0): (
        OWNERSHIP,
        (
            "who owns the *new* sink when the worker declined mid-swap (SPEC-035 FR-003). Carried by "
            "a return value because only the worker knows whether it got as far as reassigning."
        ),
    ),
    ("_flush_worker", "worker is None", 0): (
        EXISTENCE,
        (
            "a process that never logged has nothing to drain, and building a thread to prove it "
            "would be pure cost (SPEC-013)."
        ),
    ),
    ("_worker_health", "worker is None", 0): (
        EXISTENCE,
        "health() creates no worker; the zeros describe a process that never logged.",
    ),
    ("_worker_health", "_orphan_retired and (not health.retired)", 0): (
        LIVENESS,
        (
            "reporting, not deciding: health.retired is the worker's own retirement read off its "
            "snapshot, so this is a liveness question even though the other operand is a module "
            "flag. A draft filed it under a fifth category, not-a-worker-question, which was an "
            "unbounded escape hatch - a site nobody wanted to think about could be filed there "
            "and pass. Four categories, matching architecture.md 9.2, and no escape hatch. - "
            "synthesizing retired for a process with no worker (SPEC-031 FR-006)."
        ),
    ),
}


_BOOL_ATTRS = ("retired", "draining")


def _record(expr: ast.AST, fn: str, found: list[tuple[str, str]]) -> None:
    """Files one boolean-position expression, without descending into it.

    Not descending matters: `worker is not None and worker.sink is sink and worker.draining` is
    one guard, not four, and splitting it would file the same decision under rows nobody could
    keep in step with each other.

    Args:
      expr: The expression in boolean position.
      fn: The enclosing function's name.
      found: The accumulating list, mutated in place.

    Returns:
      None.

    Raises:
      None.
    """
    rendered = ast.unparse(expr)
    if any(token in rendered for token in _SENTINELS):
        found.append((fn, rendered))


def _boolean_positions(node: ast.AST) -> list[ast.AST]:
    """Returns the expressions this node evaluates for truth.

    A bare attribute or name **is** a question when it sits here — `if _worker.retired:` asks
    exactly what `if not _worker.retired:` asks, and a first version of this walker recognised
    only the second because it matched on node *shape* (`Compare`/`BoolOp`/`Not`) rather than on
    position. That let a real, natural guard into `decorator.py` with the whole suite green, and
    the walker's own self-test encoded the gap as expected behaviour. Position is the property
    that holds; shape is not.

    Args:
      node: Any AST node.

    Returns:
      Its sub-expressions evaluated for truth, which may be empty.

    Raises:
      None.
    """
    if isinstance(node, ast.If | ast.While | ast.IfExp | ast.Assert):
        return [node.test]
    if isinstance(node, ast.BoolOp):
        return list(node.values)
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
        return [node.operand]
    if isinstance(node, ast.Return) and node.value is not None:
        # A return is a boolean position only when the expression is unambiguously one, or reads
        # a known boolean attribute — `return worker` hands back the object, not an answer.
        if isinstance(node.value, ast.Compare | ast.BoolOp) or (
            isinstance(node.value, ast.UnaryOp) and isinstance(node.value.op, ast.Not)
        ):
            return [node.value]
        if isinstance(node.value, ast.Attribute) and node.value.attr in _BOOL_ATTRS:
            return [node.value]
    return []


def _predicates() -> list[tuple[str, str]]:
    """Derives every worker question in `decorator.py` from its AST, in source order.

    A question is an expression **in boolean position** that names the worker, plus any call to
    `_live_worker`, so `worker = _live_worker()` is seen even though the assignment is not itself
    a test. Outermost only: a `BoolOp` naming the worker is filed whole rather than per operand.

    Two limitations are real and disclosed rather than papered over. The subject is recognised by
    **name** (`_SENTINELS`), so a guard whose local is called `owner` rather than `worker` is
    invisible — though *rewriting* an existing site that way trips the stale-row check, so the
    exposure is net-new sites. And call-shaped questions (`getattr(_worker, "retired", False)`,
    `bool(_worker)`, `match _worker:`) are not recognised; none is idiomatic here, and the cost
    of chasing every shape is a walker nobody can reason about.

    Args:
      None.

    Returns:
      Every (enclosing function, unparsed expression) pair, duplicates included and ordered.

    Raises:
      None.
    """
    tree = ast.parse(textwrap.dedent(inspect.getsource(decorator)))
    found: list[tuple[str, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        filed: set[int] = set()
        for inner in ast.walk(node):
            if isinstance(inner, ast.Call) and getattr(inner.func, "id", None) == "_live_worker":
                _record(inner, node.name, found)
            for expr in _boolean_positions(inner):
                if id(expr) in filed:
                    continue
                for descendant in ast.walk(expr):
                    filed.add(id(descendant))
                _record(expr, node.name, found)
    return found


def _numbered() -> set[tuple[str, str, int]]:
    """Adds an occurrence index so two textually identical guards are two rows.

    `_swap_sink` holds two `worker is not None` tests that ask different questions — one decides
    whether the orphan path relinquishes its record, the other who performs the swap — and a key
    of (function, text) collapsed them into a single row whose reason described only the second.
    Line numbers are deliberately not used: they rot on every edit, which is what an earlier
    draft of the spec's own AC-1 cited and then had to correct.

    Args:
      None.

    Returns:
      Every site as (function, expression, occurrence index within that function).

    Raises:
      None.
    """
    seen: dict[tuple[str, str], int] = {}
    numbered = set()
    for site in _predicates():
        seen[site] = seen.get(site, -1) + 1
        numbered.add((*site, seen[site]))
    return numbered


def test_every_worker_predicate_is_classified() -> None:
    """AC-1, AC-2. A new or changed site must be decided, not defaulted.

    This is the criterion that would have caught SPEC-035 FR-001: the shipped
    `_offer_orphan_signal` guard read `_live_worker()`, which no row here would have declared.
    """
    found = _numbered()
    declared = set(ROSTER)

    unclassified = found - declared
    assert not unclassified, (
        "these worker-question sites are not in the roster — classify each one:\n  "
        + "\n  ".join(f"{fn}[{n}]: {expr}" for fn, expr, n in sorted(unclassified))
    )
    stale = declared - found
    assert not stale, (
        "these roster rows match no site — the code moved and the roster did not:\n  "
        + "\n  ".join(f"{fn}[{n}]: {expr}" for fn, expr, n in sorted(stale))
    )


def test_every_row_states_a_category_and_a_reason() -> None:
    """AC-2. A category with no reason is a row that will be copied rather than thought about."""
    for site, (category, reason) in ROSTER.items():
        assert category in {EXISTENCE, LIVENESS, OWNERSHIP, OWNERSHIP_AND_MOMENT}, (
            f"{site} has an unknown category"
        )
        assert len(reason) > 40, f"{site}'s reason is too short to be one"


def test_the_roster_finds_the_bare_form_that_shipped_unseen() -> None:
    """AC-1. `_worker is not None` on its own is the phrasing SPEC-033's docstrings warn about,
    and a walk looking only for `_live_worker()` and `.sink is` comparisons would never see it."""
    assert ("_shutdown_worker", "_worker is not None", 0) in _numbered()


def test_the_roster_finds_a_verdict_carried_by_a_return_value() -> None:
    """AC-1. SPEC-035 FR-003 answers an ownership question with a bool, not a predicate — the
    form a roster built only from worker-name comparisons would miss."""
    assert ("_swap_sink", "not worker_holds_sink", 0) in _numbered()


def _walk_source(source: str) -> set[str]:
    """Runs the real walker over a fixture, so the guards below cannot drift from it."""
    tree = ast.parse(textwrap.dedent(source))
    found: list[tuple[str, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        filed: set[int] = set()
        for inner in ast.walk(node):
            if isinstance(inner, ast.Call) and getattr(inner.func, "id", None) == "_live_worker":
                _record(inner, node.name, found)
            for expr in _boolean_positions(inner):
                if id(expr) in filed:
                    continue
                for descendant in ast.walk(expr):
                    filed.add(id(descendant))
                _record(expr, node.name, found)
    return {expr for _, expr in found}


def test_the_walker_matches_every_shape_it_claims_to() -> None:
    """Guards the guard: a walker that matched nothing would classify nothing, vacuously.

    Every entry here was a hole at some point. `return ...` was missed by a version that walked
    only `if`/`while` tests. **The bare-attribute and bare-name forms were missed by the version
    after it**, which matched on node shape — and this test's own fixture already contained
    `while worker.draining:` while its expected set omitted it, so the test certified the gap it
    was written to close. Anything in the fixture must appear below.
    """
    found = _walk_source(
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
            if _worker.retired:
                pass
            if _worker:
                pass
            return _worker is not None and _worker.sink is None
        """
    )
    assert found == {
        "_worker is None",
        "worker.draining",
        "worker is None or worker.retired",
        "_worker.sink is owed",
        "_live_worker()",
        "not worker_holds_sink",
        "_worker.retired",
        "_worker",
        "_worker is not None and _worker.sink is None",
    }, found


def test_a_conjunction_is_one_site_not_four() -> None:
    """Filing the operands as well would put one decision under rows nobody keeps in step."""
    found = _walk_source(
        """
        def f():
            if worker is not None and worker.sink is sink and worker.draining:
                pass
        """
    )
    assert found == {"worker is not None and worker.sink is sink and worker.draining"}, found


def test_the_walker_ignores_uses_that_are_not_questions() -> None:
    """A roster that filed every mention of the worker would be noise nobody maintains."""
    found = _walk_source(
        """
        def f():
            _worker = Worker(_ensure_sink())
            worker.shutdown(timeout)
            worker.flush(timeout)
            _worker.sink = new_sink
            return worker
        """
    )
    assert not found, f"these are actions on the worker, not questions about it: {found}"
