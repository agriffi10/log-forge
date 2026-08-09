"""SPEC-035 FR-002 — every guard that asks about the worker declares which question it asks.

Three reviewers told SPEC-033 "ownership, not liveness", each naming a different call site; each
was fixed, and a fourth site shipped broken. SPEC-035 FR-001 was that fourth one, and its own
first draft prescribed a predicate that would have re-broken SPEC-033 the other way. The fix
for a defect that recurs at *sites* is not another site-by-site correction: it is a roster the
tests derive, so a new or changed site must be classified before it can pass.

The roster is walked out of `decorator.py`'s AST rather than hand-listed, for the reason the sink
rosters are (SPEC-028 FR-002, SPEC-032): a hand-maintained list rots, and this one is about
completeness.
"""

import ast
import inspect
import pathlib
import re
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
    ("_get_worker", "_worker is None", 0): (
        EXISTENCE,
        (
            "the double-checked build. Neither liveness nor ownership: a retired worker is still "
            "the"
            "process worker, and rebuilding one would fight a process trying to exit (SPEC-019)."
        ),
    ),
    ("_get_worker", "_worker is None", 1): (
        EXISTENCE,
        (
            "the second half of the double-check, re-read under the lock. Two rows for one "
            "idiom is the honest count: each is a separate decision the compiler will not "
            "merge, and collapsing them hides that the outer one is deliberately unlocked."
        ),
    ),
    ("_live_worker", "worker is None or worker.retired", 0): (
        LIVENESS,
        "the definition of the liveness helper itself, rather than a consumer of it.",
    ),
    ("_offer_orphan_signal", "worker is not None and worker.sink is sink and worker.draining", 0): (
        OWNERSHIP_AND_MOMENT,
        (
            "SPEC-035 FR-001. Ownership alone skips for a worker whose shutdown has finished, "
            "leaving "
            "a sink still being written to holding a set event - SPEC-033 FR-004's tight retry "
            "loop."
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
            "the call that answers who performs. Kept as its own row rather than folded into "
            "the test below, because the two are separable: reverting FR-001 moves a "
            "_live_worker() call into _offer_orphan_signal, and a roster that counted only "
            "predicates would not notice."
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
            "who owns the *old* sink's close. Answering this one with liveness closes it twice on "
            "a"
            "clean shutdown and under a live writer on an expired one - both measured."
        ),
    ),
    ("_swap_sink", "not worker_holds_sink", 0): (
        OWNERSHIP,
        (
            "who owns the *new* sink when the worker declined mid-swap (SPEC-035 FR-003). Carried "
            "by"
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
            "reporting rather than deciding: this synthesizes `retired` for a process that shut "
            "down without ever building a worker (SPEC-031 FR-006). It is a liveness question "
            "even though one operand is a module flag, because `health.retired` is the worker's "
            "own retirement read off its snapshot. A draft filed it under a fifth category, "
            "not-a-worker-question, which was an unbounded escape hatch: a site nobody wanted "
            "to think about could be filed there and pass both tests."
        ),
    ),
}


_KNOWN_LONG_WORDS = frozenset(
    {
        "architecture",
        "classification",
        "deliberately",
        "distinguishable",
        "identically",
        "implementation",
        "occurrence",
        "reclassification",
        "relinquishes",
        "retirement",
        "synthesizing",
        "unambiguously",
        "unclassified",
    }
)

_BOOL_ATTRS = ("retired", "draining")


def _is_boolean_expr(node: ast.AST | None) -> bool:
    """Whether this expression is unambiguously an answer rather than an object.

    `return worker` hands back the object; `return worker.retired` hands back an answer. The
    distinction is what lets assignments and returns be searched without filing every mention.

    Args:
      node: The expression, or None.

    Returns:
      Whether it reads as a boolean.

    Raises:
      None.
    """
    if isinstance(node, ast.Compare | ast.BoolOp):
        return True
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
        return True
    if isinstance(node, ast.Attribute) and node.attr in _BOOL_ATTRS:
        return True
    if isinstance(node, ast.IfExp):
        return _is_boolean_expr(node.body) or _is_boolean_expr(node.orelse)
    return isinstance(node, ast.Call) and getattr(node.func, "id", None) == "_live_worker"


def _boolean_positions(node: ast.AST) -> list[ast.AST]:
    """Returns the expressions this node evaluates for truth.

    A bare attribute or name **is** a question when it sits here — `if _worker.retired:` asks
    exactly what `if not _worker.retired:` asks, and a draft that matched on node *shape* rather
    than position recognised only the second, letting a real guard into `decorator.py` with the
    whole suite green.

    Assignments and returns are searched too, but only for expressions that read as answers
    (:func:`_is_boolean_expr`). Hoisting a condition into a named local — `alive = _worker is not
    None` then `if alive:` — is an ordinary refactor, and a position model that only looked at
    `if` tests lost the site entirely.

    Neither a `BoolOp` nor a `not` is decomposed into its operand. It is filed whole wherever it appears, so
    one guard is one row; an earlier version returned the operands here, which filed a hoisted
    conjunction as two rows while the same conjunction in an `if` was one.

    Args:
      node: Any AST node.

    Returns:
      Its sub-expressions evaluated for truth, which may be empty.

    Raises:
      None.
    """
    if isinstance(node, ast.If | ast.While | ast.IfExp | ast.Assert):
        return [node.test]
    if isinstance(node, ast.comprehension):
        return list(node.ifs)
    if isinstance(node, ast.Lambda) and _is_boolean_expr(node.body):
        return [node.body]
    if isinstance(node, ast.Return | ast.Assign | ast.AnnAssign) and _is_boolean_expr(node.value):
        return [node.value]
    return []


def _own_nodes(scope: ast.AST) -> list[ast.AST]:
    """Yields the nodes belonging to one scope, without descending into nested ones.

    A guard inside `@trace`'s wrapper belongs to that wrapper, not also to `decorate` and
    `trace`. A draft that used a bare `ast.walk` filed one such guard three times and would have
    demanded three identical roster rows. `Lambda` is not descended into either — its body is
    filed at the `Lambda` node itself, so descending would file it twice.

    Args:
      scope: The module or function node whose own nodes are wanted.

    Returns:
      Every node in the scope, excluding nested function and lambda bodies.

    Raises:
      None.
    """
    own: list[ast.AST] = []
    stack = [scope]
    while stack:
        node = stack.pop()
        own.append(node)
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda):
                own.append(child)
                continue
            stack.append(child)
    return own


def _named_scopes(node: ast.AST, path: tuple[str, ...]) -> list[tuple[str, ast.AST]]:
    """Every function scope under `node`, named by its **path** rather than its bare name.

    Two same-named nested functions — `decorate._inner` under two different decorators — are two
    sites, and a bare-name key put both under one roster row, which is the "two sites, one row"
    defect the occurrence index exists to prevent, one level up.

    Args:
      node: The node to search.
      path: The enclosing scope names.

    Returns:
      (dotted path, scope node) for every function beneath `node`.

    Raises:
      None.
    """
    scopes: list[tuple[str, ast.AST]] = []
    for child in ast.iter_child_nodes(node):
        if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef):
            here = (*path, child.name)
            scopes.append((".".join(here), child))
            scopes.extend(_named_scopes(child, here))
        else:
            scopes.extend(_named_scopes(child, path))
    return scopes


def _sites(tree: ast.AST) -> list[tuple[str, str, int]]:
    """Every worker question in a parsed module, keyed by scope, text and source ordinal.

    The ordinal is assigned in **source order**, which `ast.walk` does not give: it is
    breadth-first, so a draft that numbered in walk order handed `_swap_sink`'s two identical
    `worker is not None` guards indices that swapped the moment either one changed nesting depth
    — and, on the commit that introduced it, filed each site under the other's reason. Ordering
    by `lineno` means an index only moves when the guards themselves move, which is a
    reclassification a human should be asked about.

    Module level is walked as well as functions, since a guard does not stop being one for
    sitting outside a `def`.

    Three limitations are real, measured, and disclosed rather than papered over. A draft of this
    paragraph claimed `bool(_worker)` and `getattr(_worker, "retired", False)` were "both caught"
    and that `match` was the only gap; that is true in a **test** position and false in the
    hoisted one this walker had just added, so the note was over-optimistic in the direction that
    costs a contributor a green suite.

    1. **The subject is recognised by name** (`_SENTINELS`), so a guard whose local is called
       `owner` rather than `worker` is invisible — though *rewriting* an existing site that way
       trips the stale-row check, so the exposure is net-new sites only.
    2. **A hoisted question is only followed through a bare boolean operator.**
       `alive = _worker is not None` is caught; `alive = bool(_worker)`,
       `alive = getattr(_worker, "retired", False)`, `alive, _ = _worker is not None, 1`,
       `flags = [_worker is not None]` and `alive |= _worker is not None` are not. In a test
       position all of these are caught, because there the position alone settles it.
    3. **A lambda body is searched only when it is itself boolean**, so
       `lambda: [x for x in y if _worker.retired]` is missed even though the same comprehension
       at statement level is caught.

    Each is a scope decision rather than an oversight: the alternative is following every value
    an arbitrary expression could carry, which is a walker nobody can reason about — and the
    roster's failure mode is a *missed* site, which the next audit finds, not a wrong one.
    `match` is likewise uncovered and unused here.

    Args:
      tree: The parsed module.

    Returns:
      Every (scope, unparsed expression, source ordinal) triple.

    Raises:
      None.
    """
    scopes: list[tuple[str, ast.AST]] = [("<module>", tree)]
    scopes.extend(_named_scopes(tree, ()))

    found: list[tuple[str, str, int]] = []
    for name, scope in scopes:
        filed: set[int] = set()
        hits: list[tuple[int, int, str]] = []
        for inner in _own_nodes(scope):
            if isinstance(inner, ast.Call) and getattr(inner.func, "id", None) == "_live_worker":
                candidates = [inner]
            else:
                candidates = _boolean_positions(inner)
            for expr in candidates:
                if id(expr) in filed:
                    continue
                for descendant in ast.walk(expr):
                    filed.add(id(descendant))
                rendered = ast.unparse(expr)
                if any(token in rendered for token in _SENTINELS):
                    hits.append((getattr(expr, "lineno", 0), getattr(expr, "col_offset", 0), rendered))
        seen: dict[str, int] = {}
        for _, _, rendered in sorted(hits):
            seen[rendered] = seen.get(rendered, -1) + 1
            found.append((name, rendered, seen[rendered]))
    return found


def _numbered() -> set[tuple[str, str, int]]:
    """The roster derived from the real `decorator.py`.

    Args:
      None.

    Returns:
      Every site as (scope, expression, source ordinal within that scope).

    Raises:
      None.
    """
    return set(_sites(ast.parse(textwrap.dedent(inspect.getsource(decorator)))))


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
    """Runs **the real walker** over a fixture, so these guards cannot drift from it.

    A draft duplicated `_sites`' loop here and the copy immediately diverged — it filtered
    `FunctionDef` only, so an async fixture was missed by the self-test and caught by the real
    walker. A self-test that can certify behaviour the walker does not have is the failure mode
    this file exists to prevent, one level up.

    Args:
      source: Python source to walk.

    Returns:
      The set of unparsed expressions the walker filed.

    Raises:
      None.
    """
    return {expr for _, expr, _ in _sites(ast.parse(textwrap.dedent(source)))}


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


def test_no_reason_has_fused_words() -> None:
    """AC-2's deliverable is the reasons, and three rounds of scripted rewrapping fused words.

    Implicit concatenation drops the seam silently — `"...into the " "test"` reads fine in the
    source and renders as `theprocess`, `testbelow`, `into_offer_orphan_signal`. Nothing else in
    the suite reads these strings, so only a human would ever have noticed, which is exactly the
    kind of rot a lint is for.
    """
    # Built from the *code*, never from the reasons: a first version seeded the vocabulary with
    # the reasons' own words, so every fused word validated itself and the lint passed against
    # the exact defect it was written for.
    vocabulary = set()
    for path in ("src/log_foundry/decorator.py", "src/log_foundry/worker.py"):
        vocabulary.update(re.findall(r"[A-Za-z_]+", pathlib.Path(path).read_text()))

    suspect: list[tuple[str, str]] = []
    for site, (_, reason) in ROSTER.items():
        for word in re.findall(r"[A-Za-z_]{11,}", reason):
            if word in vocabulary or word.lower() in _KNOWN_LONG_WORDS:
                continue
            suspect.append((site[0], word))
    assert not suspect, f"these read as two words fused at a string seam: {suspect}"
