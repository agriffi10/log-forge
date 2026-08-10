"""SPEC-035 FR-002 — every guard that asks about the worker declares which question it asks.

Three reviewers told SPEC-033 "ownership, not liveness", each naming a different call site; each
was fixed, and a fourth site shipped broken. SPEC-035 FR-001 was that fourth one, and its own
first draft prescribed a predicate that would have re-broken SPEC-033 the other way. The fix
for a defect that recurs at *sites* is not another site-by-site correction: it is a roster the
tests derive, so a new or changed site must be classified before it can pass.

The roster is walked out of `decorator.py`'s AST rather than hand-listed, for the reason the sink
rosters are (SPEC-028 FR-002, SPEC-032): a hand-maintained list rots, and this one is about
completeness.

**What it is complete about is guards that name the worker**, and that is narrower than "who owns
a close". The orphan path answers the same ownership question with no worker in it —
`_note_orphan_emit`'s `sink is _orphan_sink`, `_close_orphan_sink`'s `owed is None`,
`_adopt_declined_swap`'s re-arm guard, `_swap_sink`'s `old is None or old is new_sink` — and none
of them is filed here. That is scope, not oversight: the recurring defect this exists to stop is a
*worker* predicate answered with the wrong category. But SPEC-035 FR-003's own defect lived in
that other family, as an assignment rather than a predicate, so a reader must not take a green
roster as covering it. Recorded rather than quietly widened (SPEC-021), because widening the
sentinels to reach it would also match every sink comparison in the module.

**One cost is recorded rather than paid down here.** The seam lint below and its exclusive
helpers are ~43% of this file, and four consecutive review rounds found a defect in them while
finding none in the roster they sit beside — what they protect is prose in a data table. A
line-oriented seam check over the same derived scope would need no source segment, no tokenizer
and no shape refusal, collapsing roughly half of it. It is **not** done here: the current reader
was just measured correct in both directions and pinned by six mutations, and replacing a
verified implementation with an unverified one during the review that verified it is how scope
added mid-review earns confidence it has not (`docs/process.md`). It belongs in its own change.
"""

import ast
import inspect
import io
import itertools
import pathlib
import textwrap
import tokenize

import pytest

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
LIVENESS = (
    "liveness — who *performs* an action, and a retired worker performs nothing. Reading that "
    "same retirement in order to *report* it is this question asked for a different purpose, "
    "not a fifth category"
)
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
            "the "
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
    ("_live_worker", "_worker", 0): (
        LIVENESS,
        (
            "the snapshot the helper's own test reads. A binding is classified by the question "
            "it feeds, which is the only category a binding can have - and it is on the roster "
            "because rebinding is how a guard changes category without its text changing."
        ),
    ),
    ("_live_worker", "worker is None or worker.retired", 0): (
        LIVENESS,
        "the definition of the liveness helper itself, rather than a consumer of it.",
    ),
    ("_offer_orphan_signal", "_worker", 0): (
        OWNERSHIP_AND_MOMENT,
        (
            "the snapshot FR-001's conjunction below reads. Rebinding it to _live_worker() is "
            "precisely the revert FR-001 exists to stop, and it changes this row's text as well "
            "as the one below, so the stale-row check catches it from two directions."
        ),
    ),
    ("_offer_orphan_signal", "worker is not None and worker.sink is sink and worker.draining", 0): (
        OWNERSHIP_AND_MOMENT,
        (
            "SPEC-035 FR-001. Ownership alone skips for a worker whose shutdown has finished, "
            "leaving "
            "a sink still being written to holding a set event - SPEC-033 FR-004's tight retry "
            "loop. "
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
            "which exit path to take. A worker that exists drains first, and only then is the "
            "orphan sink considered. What the else branch adds is not the orphan close - that "
            "is attempted on both branches and declines under _close_orphan_sink's ownership "
            "guard (SPEC-033 FR-002) - but the closer grace, which the worker branch leaves to "
            "Worker.shutdown so it is charged once rather than twice (SPEC-031 FR-006)."
        ),
    ),
    ("_swap_sink", "_live_worker()", 0): (
        LIVENESS,
        (
            "the call that answers who performs. What is classified here is the *call*, not a "
            "second decision: the two rows below consume this verdict and cannot be answered "
            "with another category without editing this line. It is kept as its own row "
            "because the call moves independently of the tests that read it - reverting FR-001 "
            "puts a _live_worker() call back into _offer_orphan_signal, and a roster that "
            "counted only predicates would not notice."
        ),
    ),
    ("_swap_sink", "worker is not None", 0): (
        LIVENESS,
        (
            "the in-lock branch: a live worker means the orphan path relinquishes its record, "
            "because that worker is *expected* to own the handoff - and reports through "
            "swap_sink's return value when it does not, which is the row two below (SPEC-035 "
            "FR-003). Liveness rather than ownership - a retired worker performs no swap, so "
            "the record must stay with the orphan path. This is the branch the loss is "
            "measured at: answering it with existence leaves a sink adopted after a retired "
            "worker closed by nobody."
        ),
    ),
    ("_swap_sink", "worker is not None", 1): (
        LIVENESS,
        (
            "the out-of-lock branch: whether to delegate at all. It consumes the verdict the "
            "row above produced rather than re-reading _worker, and that is deliberate - "
            "_get_worker can assign the global between the two reads, so a re-read would let "
            "the two branches act on different answers. Textually identical to the row above "
            "and a separate read, which is why the key carries an occurrence index rather than "
            "only the text. Disclosed rather than claimed: no shipped test distinguishes this "
            "from the existence form, because the in-lock branch has already performed the "
            "orphan handoff and _adopt_declined_swap absorbs the delegation a retired worker "
            "then declines. A first version of this row said a retired worker here loses the "
            "handoff entirely - a sentence lifted from SPEC-033's delivery doc, where it "
            "described _live_worker() as a whole; measurement puts that loss at the row above, "
            "and a reason copied rather than thought about is the failure AC-2 exists to stop."
        ),
    ),
    ("_swap_sink", "_worker is None or _worker.sink is not old", 0): (
        OWNERSHIP,
        (
            "who owns the *old* sink's close. Answering this one with liveness closes it twice on "
            "a "
            "clean shutdown and under a live writer on an expired one - both measured."
        ),
    ),
    ("_swap_sink", "not worker_holds_sink", 0): (
        OWNERSHIP,
        (
            "who owns the *new* sink when the worker declined mid-swap (SPEC-035 FR-003). "
            "Carried by a return value because a decline is a decision taken inside "
            "Worker.swap_sink, between its two lock acquisitions, and nothing outside observes "
            "it. What is *not* claimed: that no other predicate would do. swap_sink returns "
            "False only where _shutdown_done latched, which is exactly what _live_worker() "
            "reads, so a liveness re-read agrees with it at this site by construction and "
            "measurement cannot separate the two - replacing it leaves the behavioural suite "
            "green. The return value is the form that stays correct if swap_sink ever gains a "
            "second reason to decline; today that is a design argument, not a measured one."
        ),
    ),
    ("_flush_worker", "_worker", 0): (
        EXISTENCE,
        (
            "the snapshot the existence test below reads. Deliberately the raw global rather "
            "than _live_worker(): a retired worker still has a queue worth draining, and "
            "flush() is the call a serverless process makes when it is not exiting."
        ),
    ),
    ("_flush_worker", "worker is None", 0): (
        EXISTENCE,
        (
            "a process that never logged has nothing to drain, and building a thread to prove it "
            "would be pure cost (SPEC-013)."
        ),
    ),
    ("_worker_health", "_worker", 0): (
        EXISTENCE,
        (
            "the snapshot the existence test below reads. The raw global again, and for the "
            "same reason: a retired worker's counters are exactly what health() is asked for "
            "(SPEC-030 FR-001), so resolving liveness here would report zeros instead."
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


_SOURCE = pathlib.Path(__file__).read_text(encoding="utf-8")

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

    An assignment whose value is a bare name or attribute is filed as well, and that one is not
    a question at all — it is a **binding**, filed because rebinding is how a guard changes
    category while its text stays identical. One inserted `worker = _worker` above
    `_swap_sink`'s existing `if worker is not None:` turned that guard from liveness into
    existence with the roster, the suite, ruff and mypy all green, while the row below went on
    declaring liveness; measured, `configure()` then stopped fencing the previous sink's close
    (2.01 s → 0.00 s, `B.closed` 1 → 0). A binding is classified by the question it feeds,
    which is the only category a binding can have. `Return` is deliberately excluded — `return
    worker` hands the object to a caller who must ask their own question — and a `Call` value
    is excluded too, so `worker = worker.health()` stays out while `worker = _live_worker()` is
    caught by :func:`_is_boolean_expr` instead.

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
    if isinstance(node, ast.Assign | ast.AnnAssign) and isinstance(
        node.value, ast.Name | ast.Attribute
    ):
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


def _named_scopes(
    node: ast.AST,
    path: tuple[str, ...],
    seen: dict[tuple[tuple[str, ...], str], int] | None = None,
) -> list[tuple[str, ast.AST]]:
    """Every function scope under `node`, named by its **path** rather than its bare name.

    Two same-named nested functions — `decorate._inner` under two different decorators — are two
    sites, and a bare-name key put both under one roster row, which is the "two sites, one row"
    defect the occurrence index exists to prevent, one level up.

    Args:
      node: The node to search.
      path: The enclosing scope names.
      seen: How many times each `(enclosing path, name)` has been used, threaded through the
        whole walk so two same-named functions collide even when they are not siblings of one
        node — a `try:`/`except ImportError:` fallback pair being the ordinary shape a per-call
        counter missed. The enclosing path is part of the key, so two classes may each have a
        method of the same name without either being suffixed.

    Returns:
      (dotted path, scope node) for every function beneath `node`.

    Raises:
      None.
    """
    scopes: list[tuple[str, ast.AST]] = []
    seen = {} if seen is None else seen
    for child in ast.iter_child_nodes(node):
        if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            key = (path, child.name)
            seen[key] = seen.get(key, -1) + 1
            # A same-named sibling — two `def f()` in the arms of an `if`, or two classes with
            # the same method — is a second scope, and a bare name would collapse both onto one
            # roster row: the defect the occurrence index exists to prevent, one level up.
            suffix = "" if seen[key] == 0 else f"#{seen[key]}"
            here = (*path, f"{child.name}{suffix}")
            if not isinstance(child, ast.ClassDef):
                scopes.append((".".join(here), child))
            scopes.extend(_named_scopes(child, here, seen))
        else:
            scopes.extend(_named_scopes(child, path, seen))
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

    1. **The subject is recognised by name** (`_SENTINELS`), matched as a substring of the
       rendered text. So `if owner is None:` is invisible where `if _worker is None:` is not —
       though `if owner.retired:` **is** caught, because `retired` is itself a token. What
       *rewriting* an existing site cannot do is hide, since the stale-row check fires on the
       text that disappeared regardless of what replaced it; so the exposure is **net-new sites
       under a name of the author's choosing**, and it is not confined to existence guards. An
       aliased ownership guard (`held = _worker` … `held.sink is owed` — with the alias filed,
       but a second alias would not be) and an aliased `_live_worker` call are equally
       unfilterable by name. Two drafts of this note claimed less: one that the failure mode was
       purely a missed site, one that only pure-existence guards were exposed. Both measured
       false, which is why the claim is now stated at its widest. The substring model also
       over-matches — `if networker:` and `if ticket.retired:` are filed — and that direction is
       the safe one, since it demands a classification rather than skipping one.
    2. **A hoisted question is only followed through a bare boolean operator, or a bare name.**
       `alive = _worker is not None` and `held = _worker` are caught; `alive = bool(_worker)`,
       `alive = getattr(_worker, "retired", False)`, `alive, _ = _worker is not None, 1`,
       `flags = [_worker is not None]` and `alive |= _worker is not None` are not. In a test
       position all of these are caught, because there the position alone settles it.
    3. **The *use* of a filed expression is not followed.** `owns = _worker is not None and
       _worker.sink is owed` is filed, and `if not owns:` below it carries no sentinel, so
       inverting a guard by negating its hoisted local passes with the roster green. Filing the
       binding is what makes the category reviewable; it does not make every later reference so.
    4. **A lambda body is searched only when it is itself boolean**, so
       `lambda: [x for x in y if _worker.retired]` is missed even though the same comprehension
       at statement level is caught.

    Each is a scope decision rather than an oversight: the alternative is following every value
    an arbitrary expression could carry, which is a walker nobody can reason about. `match` is
    likewise uncovered and unused here. The backstop for all four is that the four historical
    defects were mis-answered *existing* guards, which the stale-row check catches without
    exception.

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
    renumbered = (
        "\n\nAt least one carries an index above 0, so this function already had rows for that "
        "text and inserting a site renumbered them. Re-read every row sharing it: the reasons "
        "are distinct per site and the check above cannot tell you they now name the wrong one."
        if any(n for _, _, n in unclassified)
        else ""
    )
    assert not unclassified, (
        "these worker-question sites are not in the roster — classify each one:\n  "
        + "\n  ".join(f"{fn}[{n}]: {expr}" for fn, expr, n in sorted(unclassified))
        + renumbered
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
            held = _worker.sink
            return _worker is not None and _worker.sink is None
        """
    )
    assert found == {
        "_worker.sink",
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
            snapshot = worker.health()
            return worker
        """
    )
    assert not found, f"these are actions on the worker, not questions about it: {found}"


def test_no_reason_fuses_words_at_a_string_seam() -> None:
    """AC-2's deliverable is the reasons, and scripted rewrapping fused words three times.

    Detected at the **seam**, not in the result. A first version scored the rendered text —
    long words checked against the vocabulary of the code — and could not see `theprocess` (ten
    characters), which was simultaneously one of its own cited examples and live in the file; it
    also rejected any long English word the source happened not to use. Both failures come from
    guessing at the output. The seam itself is exact: in an implicitly concatenated literal,
    every part but the last must end with whitespace, or the next must begin with one. Exact
    against false *negatives*, which is the direction that matters; a part ending in a tab, a
    carriage return or a non-breaking space is reported as a fusion when it is not.
    """
    shapes: list[str] = []
    fused: list[str] = []
    unreadable: list[str] = []
    for subtree in _linted_nodes(_SOURCE):
        for node in ast.walk(subtree):
            if _is_unreadable_shape(node):
                shapes.append(ast.unparse(node)[:48])
                continue
            if not isinstance(node, ast.Constant) or not isinstance(node.value, str | bytes):
                continue
            try:
                siblings = _literal_parts(node, _SOURCE)
            except _UnsupportedLiteral as exc:
                unreadable.append(str(exc))
                continue
            for left, right in itertools.pairwise(siblings):
                if not left.endswith((" ", "\n")) and not right.startswith((" ", "\n")):
                    fused.append(f"...{left[-24:]}|{right[:24]}...")
    assert not shapes, (
        "write this as one plain literal or an implicit concatenation — this lint cannot read "
        "an interpolated or `+`-joined one:\n  " + "\n  ".join(shapes)
    )
    assert not unreadable, (
        "this lint could not read these literals, so it did not check them:\n  "
        + "\n  ".join(unreadable)
    )
    assert not fused, "these string seams fuse two words together:\n  " + "\n  ".join(fused)


def _linted_nodes(source: str) -> list[ast.AST]:
    """Every subtree whose prose this lint answers for, **derived** rather than named.

    The scope is ``ROSTER`` plus every module-level constant ``ROSTER`` references by name —
    the four categories today, and a fifth automatically if one is ever added. Scoping to
    ``ROSTER`` alone was a guess, and the guess was wrong: it excluded ``LIVENESS``, a
    three-part literal carrying the *category* half of what AC-2 calls "a category and a
    reason", and two live fusions inserted there passed the whole file. A lint whose value is
    completeness cannot rest on a hand-drawn boundary (SPEC-032's roster lesson, one level up).

    The module is deliberately **not** linted whole: it is full of legitimate f-strings, and
    refusing those shapes only makes sense where the string is prose a human reads.

    Args:
      source: This module's own source.

    Returns:
      The `ROSTER` node first, then each referenced constant's assignment.

    Raises:
      StopIteration: If the source declares no ``ROSTER``.
    """
    module = ast.parse(source)
    roster = next(
        node
        for node in ast.walk(module)
        if isinstance(node, ast.AnnAssign) and getattr(node.target, "id", None) == "ROSTER"
    )
    referenced = {node.id for node in ast.walk(roster) if isinstance(node, ast.Name)}
    linted: list[ast.AST] = [roster]
    for node in module.body:
        if node is roster:
            continue
        if isinstance(node, ast.AnnAssign):
            targets: list[ast.expr] = [node.target]
        elif isinstance(node, ast.Assign):
            targets = list(node.targets)
        else:
            continue
        if any(isinstance(t, ast.Name) and t.id in referenced for t in targets):
            linted.append(node)
    return linted


def _is_unreadable_shape(node: ast.AST) -> bool:
    """Whether this node builds a string in a way the seam reader cannot follow.

    Two shapes, refused rather than read, and both at the **AST** because neither is visible
    downstream. An f-string cannot be caught by looking for an ``f`` prefix in the source:
    CPython folds ``f"a" "b"`` into one ``Constant`` whose recorded position starts *inside* the
    f-string, so :func:`ast.get_source_segment` hands back ``a" "b"`` — a fragment that happens
    to tokenize as an ordinary single-part string and was skipped in silence. A ``+`` join is
    the opposite problem: it is never folded at all, so each operand reads as a complete
    one-part literal and a fusion across the operator is invisible with nothing raised.

    A reason is static prose, so neither shape has any business here and refusing them is
    cheaper and more honest than teaching the reader to follow them. An earlier version refused
    only the f-string and called that "both exact and sufficient", which measured false.

    Args:
      node: Any AST node.

    Returns:
      Whether it is an f-string or a binary operation.

    Raises:
      None.
    """
    return isinstance(node, ast.JoinedStr | ast.BinOp)


class _UnsupportedLiteral(Exception):
    """A literal this lint cannot read, raised rather than answered with an empty list.

    Every earlier version returned ``[]`` for both "there is no seam here" and "I could not
    look", which is how a whole quote shape went unchecked across two review rounds. The two
    answers are now different kinds of thing, and only one of them is silent.
    """


def _literal_parts(node: ast.Constant, source: str) -> list[str]:
    """Returns the **decoded** parts of an implicitly concatenated literal, in order.

    Tokenized rather than pattern-matched. A quote regex saw only double-quoted parts, so a
    reason written with single quotes or mixed quotes was skipped with no signal — and
    ``ruff format`` is deliberately not run here and the ``Q`` rules are not selected, so nothing
    else would have caught it. It also compared *source* text, where a trailing ``\\n`` escape
    reads as two characters and a triple-quoted part yields spurious empty pieces; decoding first
    makes the whitespace test mean what the docstring says it means. F-strings are not read here
    at all — :func:`_is_fstring` rejects them before this is reached.

    Args:
      node: The string constant whose concatenation run is wanted.
      source: The source the node was parsed from, passed rather than read from the module
        global so the shapes this claims to handle can be driven through it directly.

    Returns:
      The decoded parts, or an empty list when the literal has only one part.

    Raises:
      _UnsupportedLiteral: If the segment cannot be recovered, cannot be tokenized, or decodes
        to something other than text — each of which was previously an empty list, and so
        indistinguishable from a literal with nothing to check.
    """
    segment = ast.get_source_segment(source, node)
    if segment is None:
        raise _UnsupportedLiteral(f"no source segment for the constant at line {node.lineno}")
    readline = io.StringIO(f"({segment})").readline
    parts: list[str] = []
    try:
        for token in tokenize.generate_tokens(readline):
            if token.type != tokenize.STRING:
                continue
            part = ast.literal_eval(token.string)
            if not isinstance(part, str):
                raise _UnsupportedLiteral(f"{type(part).__name__} literal: {segment[:48]}")
            parts.append(part)
    except (tokenize.TokenError, SyntaxError, ValueError) as exc:
        raise _UnsupportedLiteral(f"{type(exc).__name__}: {segment[:48]}") from exc
    return parts if len(parts) > 1 else []


def _only_constant(source: str) -> ast.Constant:
    """Returns the one literal in a one-expression fixture.

    Args:
      source: Python source whose only constant is the literal under test.

    Returns:
      That constant's node.

    Raises:
      StopIteration: If the source has no literal.
    """
    return next(node for node in ast.walk(ast.parse(source)) if isinstance(node, ast.Constant))


_UNREADABLE = "unreadable"

# (fixture source, the parts the reader must recover — or `_UNREADABLE`). Every row was a real
# gap: the regex this replaced read only double quotes, compared *source* text so an escape and
# a triple-quoted part both lied, and answered "I could not look" with the same empty list it
# uses for "there is nothing to check".
_LITERAL_SHAPES: list[tuple[str, list[str] | str]] = [
    ('x = "ab" "cd"', ["ab", "cd"]),
    ("x = 'ab' 'cd'", ["ab", "cd"]),
    ("x = \"ab\" 'cd'", ["ab", "cd"]),
    ('x = """ab""" "cd"', ["ab", "cd"]),
    ('x = "ab\\n" "cd"', ["ab\n", "cd"]),
    ('x = "ab" "cd" "ef"', ["ab", "cd", "ef"]),
    ('x = "abcd"', []),
    ('x = b"ab" b"cd"', _UNREADABLE),
    ('x = f"the" "process"', _UNREADABLE),
]


def test_the_seam_lint_reads_every_literal_shape_it_claims_to() -> None:
    """Guards the guard: a reader that recovers nothing checks nothing, vacuously.

    Deleting :func:`_literal_parts`' body outright left the whole file green before this
    existed, which is what let two successive versions of the lint ship blind to a quote shape.

    All three ways of being unreadable are covered, and the coverage is uneven for a reason:
    the folded-f-string row is the one that has actually fired, the bytes row is reachable from
    the real lint only because it stopped pre-filtering constants to `str`, and the third —
    a node whose source segment cannot be recovered at all — has no source form, so it is
    driven from a synthesised node rather than a fixture. Two of the three reverted to
    `return []` with the whole file green before this said so.
    """
    for source, expected in _LITERAL_SHAPES:
        node = _only_constant(source)
        if expected == _UNREADABLE:
            with pytest.raises(_UnsupportedLiteral):
                _literal_parts(node, source)
            continue
        assert _literal_parts(node, source) == expected, source

    positionless = ast.Constant(value="x")
    positionless.lineno = 1
    with pytest.raises(_UnsupportedLiteral):
        _literal_parts(positionless, 'x = "x"')


def test_a_reason_built_by_interpolation_or_addition_is_refused() -> None:
    """Rounds 7 and 8: two ways of building a string that were not checked and said nothing.

    Each is pinned in both halves — that the reader genuinely cannot see the fusion, and that
    the shape is refused at the AST where it is unambiguous. The f-string case is proved with
    one in the **middle**, since CPython gives each surrounding literal its own one-part segment
    and the reader stays silent; a *leading* one only mangles the segment and is now at least
    loud. The `+` case never folds at all, so both operands read as complete one-part literals.
    """
    silent = 'x = "the" f"{y}" "process"'
    assert [_literal_parts(node, silent) for node in ast.walk(ast.parse(silent)) if
            isinstance(node, ast.Constant) and isinstance(node.value, str)] == [[], []]

    joined = 'x = "the" + "process"'
    assert [_literal_parts(node, joined) for node in ast.walk(ast.parse(joined)) if
            isinstance(node, ast.Constant)] == [[], []]

    for refused in ('(EXISTENCE, f"a{1}" "b")', '(EXISTENCE, "a" + "b")'):
        source = f'ROSTER: dict = {{("f", "x", 0): {refused}}}'
        subtrees = _linted_nodes(source)
        assert any(_is_unreadable_shape(node) for sub in subtrees for node in ast.walk(sub)), source

    plain = 'ROSTER: dict = {("f", "x", 0): (EXISTENCE, "a " "b")}'
    assert not any(
        _is_unreadable_shape(node) for sub in _linted_nodes(plain) for node in ast.walk(sub)
    )


def _linted_names(source: str) -> set[str]:
    """The names of the assignments :func:`_linted_nodes` selected.

    Args:
      source: Python source declaring a `ROSTER`.

    Returns:
      One name per selected assignment.

    Raises:
      None.
    """
    names: set[str] = set()
    for node in _linted_nodes(source):
        targets = [node.target] if isinstance(node, ast.AnnAssign) else getattr(node, "targets", [])
        names.update(t.id for t in targets if isinstance(t, ast.Name))
    return names


def test_the_lint_covers_the_categories_and_not_the_whole_module() -> None:
    """Round 8: scoping to `ROSTER` alone excluded `LIVENESS`, and two fusions there passed.

    Both bounds are asserted, because each failure mode is real. Too narrow leaves the category
    half of "a category and a reason" unchecked. Too wide — the whole module — refuses the
    f-strings this file legitimately uses to build its own failure messages, so the lint could
    not run at all. Deriving the scope from what `ROSTER` *references* is what makes a fifth
    category picked up with no edit here.
    """
    assert _linted_names(_SOURCE) == {
        "ROSTER",
        "EXISTENCE",
        "LIVENESS",
        "OWNERSHIP",
        "OWNERSHIP_AND_MOMENT",
    }

    unreferenced = 'UNUSED = "x"\nUSED = "y"\nROSTER: dict = {("f", "e", 0): (USED, "a")}\n'
    assert _linted_names(unreferenced) == {"ROSTER", "USED"}


# (fixture source, the scope names the walker must produce). The `try:`/`except ImportError:`
# pair is the shape a per-call counter missed: the two `def f()` are not siblings of one node,
# so both were named `f` and collapsed onto one roster row.
_SCOPE_SHAPES: list[tuple[str, list[str]]] = [
    ("def f():\n    pass\n\n\ndef f():\n    pass\n", ["f", "f#1"]),
    ("try:\n    def f():\n        pass\nexcept ImportError:\n    def f():\n        pass\n", ["f", "f#1"]),
    ("if x:\n    def f():\n        pass\nelse:\n    def f():\n        pass\n", ["f", "f#1"]),
    ("class A:\n    def m(self):\n        pass\n\n\nclass B:\n    def m(self):\n        pass\n", ["A.m", "B.m"]),
    ("def outer():\n    def inner():\n        pass\n", ["outer", "outer.inner"]),
]


def test_two_same_named_scopes_are_two_rows_wherever_they_sit() -> None:
    """Guards the guard: collapsing two scopes onto one name is the defect one level up.

    Replacing the threaded counter with a per-call one, or the `(path, name)` key with a bare
    name, leaves `decorator.py` unchanged — the module has no such pair today — so nothing but a
    fixture can hold this.
    """
    for source, expected in _SCOPE_SHAPES:
        found = [name for name, _ in _named_scopes(ast.parse(source), ())]
        assert found == expected, source
