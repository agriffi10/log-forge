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

**The module boundary is `decorator.py`, and that is a scope choice too.** `worker.py` decides
the same kind of question inside the object — `_close_if_owed`, `swap_sink`'s three
`_shutdown_done` re-checks, `shutdown`'s own gates — and none is filed. The four defects this
exists for were all in `decorator.py`, where the question is "which of the module's several
paths owns this", while inside `Worker` the answer is always "this worker"; but a fifth defect
is not obliged to respect that, and the FR's single-module scope is stated here rather than
left to be inferred from what the walker happens to be pointed at.

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
from log_foundry import _lifecycle, decorator

# Tokens that make an expression a question about the worker. Every one names the worker, and
# that is load-bearing rather than incidental: SPEC-035 FR-003 answers "who owns the new sink"
# with a **return value** rather than a predicate, so the variable holding it is named
# `worker_holds_sink` — a roster keyed on worker-naming tokens cannot see a verdict stored under
# a name that hides what it is about. A first draft used the token `adopted` instead and matched
# `continue_trace`'s trace-context variable, which is not a worker question at all: a heuristic
# broad enough to catch every phrasing also catches things that are not the subject.
#
# These four are the *base*. The set actually used is derived per-tree by
# :func:`_accessor_names`, which adds any function that hands the worker back, because a
# hand-written token list is the one hand-list left in a file whose whole argument is that
# hand-lists rot.
_BASE_SENTINELS = ("_worker", "worker", "draining", "retired")

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
    ("_get_worker", "_state.worker_exists()", 0): (
        EXISTENCE,
        (
            "the double-checked build, outer half, and deliberately **unlocked** - this is the "
            "@trace hot path, so the question must not acquire anything (SPEC-040 FR-002). "
            "Neither liveness nor ownership: a retired worker is still the process worker, and "
            "rebuilding one would fight a process trying to exit (SPEC-019)."
        ),
    ),
    ("_get_worker", "worker is None", 0): (
        EXISTENCE,
        (
            "the test the unlocked binding above feeds. Filed apart from its binding because "
            "the binding can be retargeted at a liveness helper without this line changing, "
            "which would rebuild a worker on every call once one retired."
        ),
    ),
    ("_get_worker", "_state.worker_exists()", 1): (
        EXISTENCE,
        (
            "the second half of the double-check, re-read under the lock. Two bindings for one "
            "idiom is the honest count: each is a separate decision the compiler will not "
            "merge, and collapsing them hides that the outer one is deliberately unlocked."
        ),
    ),
    ("_get_worker", "worker is None", 1): (
        EXISTENCE,
        (
            "the locked half's test, and the one that actually decides whether a worker is "
            "built. Concurrent first-flushes both reach it; exactly one sees None."
        ),
    ),
    ("_get_worker", "worker", 0): (
        EXISTENCE,
        (
            "the **publication** of the newly built worker into the global, under the lock - the "
            "right-hand side of `_worker = worker`, not the return, which the walker excludes "
            "deliberately (`_boolean_positions`: a return hands the object to a caller who asks "
            "their own question). It is a write rather than a question, and it is on the roster "
            "as a stale-detector on the one line in `src/` that assigns `_worker`: any edit to "
            "how the worker is published changes this text and forces a re-decision. No claim "
            "is made that binding a local fixed anything - `_worker` is assigned in exactly one "
            "place and only under `_worker_lock`, so it is monotone None -> W and the previous "
            "`return _worker` after the lock could never have handed back a different object."
        ),
    ),
    ("_get_worker", "worker", 1): (
        EXISTENCE,
        (
            "the registration of that same object as the **late worker** for a `shutdown()` "
            "already running (SPEC-044 FR-001) - the right-hand side of `_late_worker = worker`. "
            "Existence for the reason the publication above is: it is a write of the object just "
            "built, not a question about it, and what decides whether it happens is "
            "`_state._shutdown_running`, which names no worker and gets no row. Ordering matters "
            "and was checked - `_state._worker = worker` stays **above** this line, so index 0 "
            "keeps naming the publication and this one is index 1."
        ),
    ),
    ("_get_worker", "stale is not worker.sink", 0): (
        OWNERSHIP,
        (
            "who owns the close the orphan record was holding, at the moment the record is "
            "cleared (SPEC-044 FR-002). Ownership, not existence: a worker exists by "
            "construction three lines up, and what is undecided is whether it *adopted* the "
            "recorded sink - `configure()` writes `_config.sink` before taking this lock, so a "
            "`configure(sink=B)` blocked here leaves `_ensure_sink()` returning B while the "
            "record still names A, which is the close that used to be discarded. Not liveness: "
            "the worker was built moments ago and cannot be retired, so liveness answers the "
            "same thing today and stops doing so the day a worker is built retired."
        ),
    ),
    ("_rebuild_worker_after_fork", "_state.worker_exists()", 0): (
        EXISTENCE,
        (
            "the snapshot the two questions below read, taken once. The existence question "
            "rather than the liveness one, because both of those questions have to be "
            "answered and a helper "
            "that folds retirement into None collapses them into one: a retired worker still "
            "needs its queue replaced, which is what stops the child's next submit blocking on "
            "an inherited queue.Queue mutex. **This is the first binding on the roster that "
            "feeds two categories**, and the category filed is the first question it answers "
            "rather than a claim that the second is the same. _worker_health's rule - each "
            "binding classified by the one question it feeds - was written where two questions "
            "had two bindings, because the second read a different object; here there is one "
            "object and no second binding to invent honestly. Filing it still does the work a "
            "binding row is for: rebinding changes this text and the stale-row check fires."
        ),
    ),
    ("_rebuild_worker_after_fork", "worker is None", 0): (
        EXISTENCE,
        (
            "whether this process ever built a worker. A child of a parent that only ever "
            "logged outside a span has nothing to rebuild, and standing one up here would give "
            "it a drain thread the parent never had - SPEC-013's refusal to create a worker in "
            "order to prove there is nothing to drain."
        ),
    ),
    ("_rebuild_worker_after_fork", "not worker.retired", 0): (
        LIVENESS,
        (
            "who performs: a retired worker performs no delivery, so the child gets no drain "
            "thread and a fork does not undo a shutdown() (SPEC-039 FR-002 AC-4). Hoisted into "
            "a named binding rather than left inline in the call, because a keyword argument is "
            "not a position this roster files - the decision would have been invisible. It is "
            "deliberately not ownership: nothing here is deciding who closes a sink."
        ),
    ),
    ("_Lifecycle.live_worker", "self._worker", 0): (
        LIVENESS,
        (
            "the snapshot the helper's own test reads. A binding is classified by the question "
            "it feeds, which is the only category a binding can have - and it is on the roster "
            "because rebinding is how a guard changes category without its text changing."
        ),
    ),
    ("_Lifecycle.live_worker", "worker is None or worker.retired", 0): (
        LIVENESS,
        "the definition of the liveness helper itself, rather than a consumer of it.",
    ),
    ("_Lifecycle.worker_owns_now", "self._worker", 0): (
        OWNERSHIP_AND_MOMENT,
        (
            "the snapshot the conjunction below reads, now inside the question rather than at "
            "its caller (SPEC-040 FR-002). Rebinding it to _live_worker() is precisely the "
            "revert SPEC-035 FR-001 exists to stop, and it is still caught here rather than "
            "below: the conjunction's text does not change at all, so this row goes stale and "
            "a new unclassified site appears, both about this line. What moving it bought is "
            "that there is now one such line in the module instead of one per caller."
        ),
    ),
    ("_Lifecycle.worker_owns_now", "worker is not None and worker.sink is sink and worker.draining", 0): (
        OWNERSHIP_AND_MOMENT,
        (
            "SPEC-035 FR-001, stated once. Ownership alone skips for a worker whose shutdown has "
            "finished, leaving a sink still being written to holding a set event - SPEC-033 "
            "FR-004's tight retry loop. Liveness alone un-skips for the whole drain, handing the "
            "drain thread a fresh event nobody will set. Both were measured; only the "
            "conjunction is right. It is a conjunction and not a fifth subject, which is why "
            "the category names both terms."
        ),
    ),
    ("_Lifecycle.worker_owns", "self._worker", 0): (
        OWNERSHIP,
        (
            "the snapshot the ownership test below reads. Filed for the reason every binding "
            "here is: rebinding it to _live_worker() changes the category of the predicate "
            "below without changing that predicate's text, which is SPEC-033's defect exactly "
            "- and now it can only be made once, in this function, rather than at each of the "
            "two call sites that used to compose it."
        ),
    ),
    ("_Lifecycle.worker_owns", "worker is not None and worker.sink is sink", 0): (
        OWNERSHIP,
        (
            "the definition of the ownership question itself. A retired worker still owns its "
            "sink's close - Worker.swap_sink returns early once _shutdown_done, so it keeps "
            "that sink forever - which is why this deliberately does not consult retired. "
            "Answering it with liveness closes the sink twice on a clean shutdown and under a "
            "live writer on an expired one, both measured (SPEC-033 FR-002)."
        ),
    ),
    ("_offer_orphan_signal", "_state.worker_owns_now(sink)", 0): (
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
    (
        "_close_orphan_sink",
        "[sink for sink in _state._orphan_owed.values() if not _state.worker_owns(sink)]",
        0,
    ): (
        OWNERSHIP,
        (
            "a retired worker still owns its sink's close, and deliberately declines it when its "
            "shutdown expired, because the drain thread may still be inside emit (SPEC-027 FR-004)."
        ),
    ),
    ("_shutdown_worker", "_state.worker_exists()", 0): (
        EXISTENCE,
        (
            "which exit path to take. A worker that exists drains first, and only then is the "
            "orphan sink considered. What the else branch adds is not the orphan close - that "
            "is attempted on both branches and declines under _close_orphan_sink's ownership "
            "guard (SPEC-033 FR-002) - but the closer grace, which the worker branch leaves to "
            "Worker.shutdown so it is charged once rather than twice (SPEC-031 FR-006). "
            "Bound rather than read twice, so the branch and the shutdown it performs cannot "
            "name two different workers."
        ),
    ),
    ("_shutdown_worker", "worker is not None", 0): (
        EXISTENCE,
        (
            "the test the binding above feeds, filed separately because the binding can be "
            "retargeted without this line changing - the shape SPEC-035 FR-002 exists to catch."
        ),
    ),
    ("_shutdown_worker", "worker is None", 0): (
        EXISTENCE,
        (
            "whether to raise the shutdown-in-progress counter (SPEC-044 FR-001), asked in the "
            "**same critical section** as the binding above so the retirement latch and the "
            "worker read cannot straddle a `_get_worker`. Existence rather than liveness: the "
            "counter exists to catch a worker that does not exist *yet*, and a retired worker "
            "found here is still the process worker and takes the branch that drains it. A "
            "second test of one binding rather than a re-read, which is what makes it safe to "
            "file apart from `worker is not None`."
        ),
    ),
    ("_shutdown_worker", "_state._late_worker", 0): (
        EXISTENCE,
        (
            "the read of whatever `_get_worker` registered while this call was running, in the "
            "last critical section - the right-hand side of `late_worker = _state._late_worker`, "
            "taken together with lowering the counter so nothing can be registered into a gap "
            "between the two. Existence: the question is only whether anything appeared. Bound "
            "rather than read twice for the reason the rows above are, and the local is named "
            "`late_worker` deliberately so that its test below is itself filed."
        ),
    ),
    ("_shutdown_worker", "late_worker is not None", 0): (
        EXISTENCE,
        (
            "the test the binding above feeds. Filed separately on the same rule as every other "
            "binding/test pair here, and reachable only because the local carries a worker name: "
            "`_accessor_names` runs its fixpoint over function returns, so a local bound from an "
            "attribute read is a subject only when its **name** says so. Calling it `late` would "
            "have hidden this guard from the walker with the file green - the shape SPEC-040 "
            "recorded when a rename killed two of its own lint rules silently."
        ),
    ),
    ("_swap_sink", "_state.live_worker()", 0): (
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
    ("_delivering_to_an_inherited_sink", "_state.worker_exists()", 0): (
        EXISTENCE,
        (
            "which of the three delivery targets to ask about, for Health.inherited_sink "
            "(SPEC-042 FR-004 AC-3). Existence, not liveness, and the distinction bites: a "
            "*retired* worker still holds the sink this process last delivered through, and "
            "whether that sink was inherited is exactly what the field reports. Answering it "
            "with liveness would fall through to the orphan record after shutdown() and "
            "describe a different object than the one the events went to. No worker is created "
            "to answer it, the same refusal _worker_health and _flush_worker already make."
        ),
    ),
    (
        "_delivering_to_an_inherited_sink",
        "worker.sink if worker is not None else owed or _live_config().sink",
        0,
    ): (
        EXISTENCE,
        (
            "the same question, consuming the snapshot the row above took rather than "
            "re-reading the global - a re-read could see _get_worker assign between the two "
            "and report on a sink that is not the one the first branch selected. The three "
            "candidates are in delivery order because SPEC-033 measured them disagreeing, and "
            "AC-1 requires the field to name which one it describes."
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
    ("_swap_sink", "not _state.worker_owns(stale)", 0): (
        OWNERSHIP,
        (
            "who owns the *old* sink's close. Answering this one with liveness closes it twice on "
            "a "
            "clean shutdown and under a live writer on an expired one - both measured."
        ),
    ),
    ("_swap_sink", "worker.swap_sink(new_sink, timeout)", 0): (
        OWNERSHIP,
        (
            "the call that produces the ownership verdict the row below reads. Filed because "
            "the roster files bindings positionally: a delegation retargeted at _worker rather "
            "than the live snapshot is the round-9 attack wearing a different value shape."
        ),
    ),
    ("_swap_sink", "stale is not new_sink and stale is not worker.sink", 0): (
        OWNERSHIP,
        (
            "whether the orphan record names a **third** sink - neither the one the worker holds "
            "nor the one being installed - which nothing else would then close (SPEC-044 "
            "FR-002). Ownership: it asks what the worker holds, not whether it is alive or "
            "exists, and it is the same question `_get_worker` asks at the sibling site. Only a "
            "preempted orphan emit re-arming across an earlier swap can make it true, which is "
            "why it is a guard rather than the common path."
        ),
    ),
    ("_swap_sink", "worker.sink is not new_sink", 0): (
        OWNERSHIP,
        (
            "whether this swap actually hands a sink over, and therefore whether there is a "
            "close for the latch to record (SPEC-044 FR-004). Ownership: `worker.sink` is the "
            "sink `Worker.swap_sink` is about to close, and it is the **right** subject where "
            "the orphan record is the wrong one - the record is `None` in the reproduced case, "
            "because `_get_worker` cleared it when the worker was built, so a latch keyed on it "
            "recorded nothing and left the double close exactly where it was."
        ),
    ),
    ("_swap_sink", "worker.sink", 0): (
        OWNERSHIP,
        (
            "the sink written into the latch - the right-hand side of "
            "`_orphan_closed_sink = worker.sink`, filed for the reason `_get_worker`'s "
            "publication row is: a write rather than a question, on the roster as a "
            "stale-detector over the one line that decides which sink a later re-arm is refused "
            "for. It is set even where the drain could not be confirmed and the swap therefore "
            "leaves that sink **open**, because the reason is not `it was closed` but that a "
            "racing emit must not re-arm a sink whose drain thread may still be inside `emit`."
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
    ("_flush_worker", "_state.worker_exists()", 0): (
        EXISTENCE,
        (
            "the snapshot the existence test below reads. Deliberately the existence question "
            "rather than _live_worker(), and what that buys is the honest verdict, not a "
            "drain: "
            "Worker.flush returns False immediately once _shutdown_done, so resolving liveness "
            "here would answer True for a queue nothing will ever read - SPEC-021's false "
            "success, in the call SPEC-013 built for a process that is not exiting. Measured "
            "with one post-shutdown call queued, and pinned by "
            "test_module_flush_after_shutdown_returns_false_promptly."
        ),
    ),
    ("_worker_health", "_state.worker_exists()", 0): (
        EXISTENCE,
        (
            "the snapshot the existence test below reads. The existence question again, and for "
            "the same reason: a retired worker's counters are exactly what health() is "
            "asked for "
            "(SPEC-030 FR-001), so resolving liveness here reports zeros instead - measured, "
            "queued, failed_batches and submitted_after_shutdown all collapse to 0, which "
            "leaves SPEC-030's retired-plus-submitted pair with a term that can never fire."
        ),
    ),
    ("_worker_health", "worker.health()", 0): (
        LIVENESS,
        (
            "the snapshot whose retired field the row below reads, and the reason this "
            "function's binding is not ambiguous: the two questions here are fed by two "
            "bindings, so each is classified by the one question it feeds rather than by a "
            "choice between them."
        ),
    ),
    ("_worker_health", "worker is None", 0): (
        EXISTENCE,
        "health() creates no worker; the zeros describe a process that never logged.",
    ),
    ("_flush_worker", "worker is not None", 0): (
        EXISTENCE,
        (
            "is there a queue to drain at all. Was `worker is None` guarding an early return; "
            "SPEC-036 FR-002 inverted it because the sink's own flush must still run in a "
            "process that has no worker — an orphan-only process with a client-buffering sink "
            "would otherwise never reach its buffer, the shape SPEC-031 FR-006 and SPEC-033 each "
            "found on the close path. Not liveness: a retired worker's queue is still drained "
            "here, and `Worker.flush` reports `retired` itself. ~~`worker is None`: a process "
            "that never logged has nothing to drain, and building a thread to prove it would be "
            "pure cost (SPEC-013)~~ — the same reason, inverted: that half is now the `is not "
            "None` branch, and the fall-through still builds nothing."
        ),
    ),
    ("_flush_worker", "worker.flush(timeout)", 0): (
        LIVENESS,
        (
            "who *performs* the drain, and a retired worker performs nothing — `Worker.flush` "
            "returns a falsy result with reason `retired` as its first statement, before the "
            "liveness check and before any queue work. It is a **binding** because SPEC-036 "
            "FR-002 sequences the sink's own flush *after* the drain, so the drain's verdict has "
            "to be held while that runs; it was `return worker.flush(timeout)`, which the walker "
            "does not file because a Return is filed only for a boolean-shaped value. The "
            "precedent is `(\"_worker_health\", \"worker.health()\", 0)` — a method-call binding "
            "on the worker, filed here. SPEC-036 FR-002 AC-9 settles this rather than leaving it "
            "to be discovered at build time against a red test, and settles it as a category that "
            "already exists: an earlier draft claimed none of the four described performing a "
            "drain, which is false against LIVENESS's own definition."
        ),
    ),
    ("_flush_live_sink", "_state.live_worker()", 0): (
        LIVENESS,
        (
            "who is *being delivered to*, asked once here rather than composed in the "
            "conditional below (SPEC-040 FR-002). It was the raw global with `not "
            "worker.retired` spelled out at the call site, and the two forms are equivalent by "
            "construction - _live_worker returns None exactly when the worker is absent or "
            "retired - so the branches this function exists to distinguish are unchanged. The "
            "row's category moves with the binding: it is the liveness question now, where it "
            "used to be an existence binding feeding a liveness test.\n"
            "**Nothing behavioural distinguishes the two categories at this site** (SPEC-040 "
            "FR-003 AC-5), and it is recorded rather than accepted, as the "
            "`not worker_holds_sink` row above records the same thing: replacing this call with "
            "_worker_exists() leaves the whole behavioural suite green - measured twice, 1802 "
            "passed with the roster excluded, and only the roster caught it. What would separate "
            "them is a flush against a *retired* worker whose sink is not the orphan record, "
            "which no test arranges. The liveness form is kept because it is the one that stays "
            "correct: a retired worker delivers nothing, so its sink is not the live target."
        ),
    ),
    ("_flush_live_sink", "worker is not None", 0): (
        LIVENESS,
        (
            "which sink is *being delivered to*, which is the sink whose own client buffer a "
            "flush must drain. A retired worker performs no further delivery, so its sink is not "
            "the live target and the orphan record is — the definition of this category, applied "
            "to a target rather than to an action. Deliberately not ownership: nothing here "
            "decides who *closes* a sink, and answering that question instead would flush the "
            "sink a retired worker still owns while events were going somewhere else."
        ),
    ),
    ("_flush_live_sink", "[worker.sink]", 0): (
        LIVENESS,
        (
            "which sinks hold a client buffer to drain. A live worker owns exactly one, so the "
            "list is a singleton; with no worker every sink the orphan path still owes a close "
            "for may hold one, since SPEC-045 made that record a set rather than a slot. "
            "Liveness rather than existence: a retired worker delivers nothing, so flushing its "
            "sink would drain a client nothing is filling while the live target went unflushed."
        ),
    ),
    ("_sweep_open_spans", "_lifecycle._get_worker()", 0): (
        LIVENESS,
        (
            "who *performs* the submit of a swept buffer. It is a binding rather than a "
            "predicate, filed by the question it feeds: `worker.submit(buffered)`. Deliberately "
            "not existence — `_get_worker` answers that by construction, building one when none "
            "exists, which is SPEC-036 FR-001 narrowing SPEC-013's refusal for a sweep that "
            "found events. A retired worker still performs nothing here and the submission is "
            "counted in `submitted_after_shutdown` (SPEC-030), which is the documented "
            "behaviour for logging after shutdown rather than a case this site decides. It is "
            "bound rather than called inline because it must be resolved **before** the buffer "
            "is detached: `_get_worker` can raise out of `Thread.start()`, and a detach that "
            "already happened leaves the events in a discarded local — measured, 3 of 4 "
            "destroyed with `flush()` reporting success and every counter zero."
        ),
    ),
    ("_inheritance_roots", "_state.worker_exists()", 0): (
        EXISTENCE,
        (
            "which sinks a forked child must mark _FOREIGN before any other handler runs "
            "(SPEC-042 FR-001). Existence rather than liveness, deliberately: a retired "
            "worker's sink is still a transport this process inherited and must not release, "
            "and resolving liveness here would leave it unmarked and therefore claimable. "
            "This site is **new to the roster** rather than new to the code - it read "
            "`decorator._worker` directly and composed the question on it, one module away "
            "from a roster that walked only `decorator.py`, so it passed silently for two "
            "specs. That is the gap SPEC-040 FR-004 widens the scope to close."
        ),
    ),
    (
        "_inheritance_roots",
        (
            "(config._live_config().sink, None if worker is None else worker.sink, "
            "*_state._orphan_owed.values(), _state._orphan_closed_sink)"
        ),
        0,
    ): (
        EXISTENCE,
        (
            "the three delivery targets plus the superseded one, filed by the question the "
            "conditional inside it asks. It is one site and not four: the tuple is a single "
            "filed expression, and the only worker question in it is the existence test that "
            "chooses between `worker.sink` and nothing."
        ),
    ),
    ("_worker_health", "_state._orphan_retired or health.retired", 0): (
        LIVENESS,
        (
            "reporting rather than deciding: this synthesizes `retired` for a process that shut "
            "down without ever building a worker (SPEC-031 FR-006). It is a liveness question "
            "even though one operand is a module flag, because `health.retired` is the worker's "
            "own retirement read off its snapshot. A draft filed it under a fifth category, "
            "not-a-worker-question, which was an unbounded escape hatch: a site nobody wanted "
            "to think about could be filed there and pass both tests. "
            "Was `_orphan_retired and (not health.retired)`, guarding an early `replace()`; "
            "SPEC-036 FR-003 made the field set unconditional (two loss counters are merged on "
            "this branch whether or not `retired` needs correcting), so the same question is now "
            "asked as a value rather than as a branch. The category is unchanged, which is the "
            "point of filing a binding by the question it feeds rather than by its shape."
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
    return _is_liveness_call(node)


def _is_liveness_call(node: ast.AST) -> bool:
    """Whether a node is a call to the liveness question, in either spelling.

    The question is a *value* rather than a comparison, so nothing else in the walker would
    recognise it: `worker = _state.live_worker()` has no sentinel-bearing boolean in it, and
    SPEC-033's whole defect was a site answering with the wrong one of these.

    Both spellings are matched deliberately. SPEC-040 made it a method, so every shipped site is
    now `_state.live_worker()` — an `ast.Attribute` callee — and a rule keyed only on the old
    free-function `ast.Name` went dead in that commit while staying green, because the walker's
    own fixture still exercised the dead form. The `ast.Name` arm is kept so that reintroducing a
    module-level `_live_worker()` cannot slip past, and the `ast.Attribute` arm is what covers
    the code as it now ships.

    Args:
      node: The node to test.

    Returns:
      Whether it is a liveness call.

    Raises:
      None.
    """
    if not isinstance(node, ast.Call):
        return False
    if isinstance(node.func, ast.Name):
        return node.func.id == "_live_worker"
    return isinstance(node.func, ast.Attribute) and node.func.attr == "live_worker"


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

    **Every** assignment value is filed, whatever its shape, and those are not questions at all
    — they are **bindings**, filed because rebinding is how a guard changes category while its
    text stays identical. One inserted `worker = _worker` above `_swap_sink`'s existing `if
    worker is not None:` turned that guard from liveness into existence with the roster, the
    suite, ruff and mypy all green, while the row below went on declaring liveness; measured,
    `configure()` then stopped fencing the previous sink's close (2.01 s → 0.00 s, `B.closed`
    1 → 0).

    The rule is **positional, not a list of value shapes**, and that is the whole lesson. A
    first version filed `Name` and `Attribute` values only; the same attack then went straight
    back through `worker, _unused = _worker, None`, through `worker = [_worker][0]`, and
    through a second accessor `worker = _process_worker()` — each reproducing the identical
    measurement. Enumerating shapes loses ground every round, because the set is open. Filing
    the value and letting `_sites`' sentinel filter decide over-matches instead, which is the
    safe direction: `snapshot = worker.health()` becomes a row nobody minds, while
    `_worker = Worker(_ensure_sink())` stays out on its own, the sentinel being lower-case.

    A binding is classified by the question it feeds. `Return` is deliberately excluded —
    `return worker` hands the object to a caller who must ask their own question, and the call
    that fetches it is filed at that caller.

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
    if isinstance(node, ast.Assign | ast.AnnAssign) and node.value is not None:
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


def _accessor_names(tree: ast.AST, base: tuple[str, ...]) -> tuple[str, ...]:
    """The sentinel tokens for one tree: `base`, plus every function that hands the worker back.

    A roster keyed on names has exactly one way to be defeated that rewriting cannot reach — a
    **new** site under a name of the author's choosing — and the cheapest such name is a fresh
    accessor: `def _snapshot(): return _worker`, then `worker = _snapshot()` above an existing
    guard. Measured on `7a10348`, that converted `_swap_sink`'s out-of-lock guard from liveness
    to existence with the roster at 13 passed and the suite at 1196 passed. The binding *was*
    filed — every assignment value is — but `_snapshot()` named nothing the filter recognised.

    So the filter's vocabulary is derived from the module rather than written beside it: a
    function whose return value names the worker **is** a worker name, and one that returns the
    result of such a function is one too, which is why this runs to a fixpoint rather than one
    pass. The direction is the same over-match the substring model already chooses — a function
    returning `span.retired_at` would be added and cost a row nobody minds — because demanding a
    classification is the safe failure and skipping one is not.

    `Return` is the only shape read. An accessor that hands the worker back through a mutable
    argument or a module global is not covered, and is limitation 3 in :func:`_sites`.

    Args:
      tree: The parsed module.
      base: The tokens that name the worker outright.

    Returns:
      Every token that makes an expression a question about the worker, in this tree.

    Raises:
      None.
    """
    tokens = set(base)
    scopes = [
        (path.rsplit(".", 1)[-1].split("#", 1)[0], node) for path, node in _named_scopes(tree, ())
    ]
    while True:
        added = False
        for name, scope in scopes:
            if name in tokens:
                continue
            returns = [
                node.value
                for node in _own_nodes(scope)
                if isinstance(node, ast.Return) and node.value is not None
            ]
            if any(token in ast.unparse(value) for value in returns for token in tokens):
                tokens.add(name)
                added = True
        if not added:
            return tuple(sorted(tokens))


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

    1. **The subject is recognised by name**, matched as a substring of the rendered text
       against tokens :func:`_accessor_names` derives from the module. So `if owner is None:`
       is invisible where `if _worker is None:` is not — though `if owner.retired:` **is**
       caught, because `retired` is itself a token. What *rewriting* an existing site cannot do
       is hide, since the stale-row check fires on the text that disappeared regardless of what
       replaced it; so the exposure is **net-new sites under a name of the author's choosing**,
       and it is not confined to existence guards. Two drafts of this note claimed less: one
       that the failure mode was purely a missed site, one that only pure-existence guards were
       exposed. Both measured false, which is why the claim is stated at its widest.

       ~~An aliased `_live_worker` call, and an accessor added under a neutral name, are equally
       unfilterable.~~ — struck (SPEC-021) rather than deleted, because it read as a permanent
       property of a name-keyed filter and is not one. A function that hands the worker back is
       now itself a token, to a fixpoint, so `worker = _snapshot()` is filed; what remains is a
       fresh **local** whose value never passes through a `return` the derivation can read —
       a worker fetched into a mutable argument or a module global. The substring model also
       over-matches — `if networker:` and `if ticket.retired:` are filed — and that direction is
       the safe one, since it demands a classification rather than skipping one.
    2. **A hoist is followed only through an assignment.** Every assignment value is filed
       whatever its shape, so `held = _worker`, `held, _ = _worker, 1`, `held = [_worker][0]`
       and `held = _process_worker()` are all caught. An augmented assignment
       (`alive |= _worker is not None`), a `global`/`nonlocal` rebinding, a `setattr`, and a
       walrus outside a filed position are not. In a test position all of them are caught,
       because there the position alone settles it.
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
    sentinels = _accessor_names(tree, _BASE_SENTINELS)
    scopes: list[tuple[str, ast.AST]] = [("<module>", tree)]
    scopes.extend(_named_scopes(tree, ()))

    found: list[tuple[str, str, int]] = []
    for name, scope in scopes:
        filed: set[int] = set()
        hits: list[tuple[int, int, str]] = []
        for inner in _own_nodes(scope):
            candidates = [inner] if _is_liveness_call(inner) else _boolean_positions(inner)
            for expr in candidates:
                if id(expr) in filed:
                    continue
                for descendant in ast.walk(expr):
                    filed.add(id(descendant))
                rendered = ast.unparse(expr)
                if any(token in rendered for token in sentinels):
                    hits.append(
                        (getattr(expr, "lineno", 0), getattr(expr, "col_offset", 0), rendered)
                    )
        seen: dict[str, int] = {}
        for _, _, rendered in sorted(hits):
            seen[rendered] = seen.get(rendered, -1) + 1
            found.append((name, rendered, seen[rendered]))
    return found


_WALKED = (_lifecycle, decorator)
"""The modules the roster is complete about (SPEC-040 FR-004 AC-1).

`_lifecycle` first, because that is where the state and the four questions now live. `decorator`
stays in scope rather than being dropped: it still reaches the worker through
`_lifecycle._get_worker()` in `_flush` and `_sweep_open_spans`, and a roster pointed only at the
new home would go green while covering strictly less than it did — the vacuous case FR-004 names.
"""

_SITE_FLOOR = {"log_foundry._lifecycle": 46, "log_foundry.decorator": 1}
"""The floor each walked module must still meet, by name.

A refactor that relocates guards can shrink a derived roster silently, which is the one failure
a roster cannot catch about itself. **Per module, and keyed by name, because a single total is
not enough**: with one number, dropping `decorator` from `_WALKED` left 37 sites against a floor
of 36 and passed — the exact scenario the test below is named for, measured. A name-keyed
mapping fails instead, because the dropped module's entry has nothing to satisfy it.

The counts are measured and may rise; either may fall only in a change that deliberately removes
guards and says so here. `decorator`'s 1 is `_sweep_open_spans`'s `worker = _lifecycle._get_worker()`
— small, and that is the point: it is the site that makes walking both modules necessary.
"""


def _numbered() -> set[tuple[str, str, int]]:
    """The roster derived from the real modules named in :data:`_WALKED`.

    Args:
      None.

    Returns:
      Every site as (scope, expression, source ordinal within that scope).

    Raises:
      None.
    """
    found: set[tuple[str, str, int]] = set()
    for module in _WALKED:
        found |= set(_sites(ast.parse(textwrap.dedent(inspect.getsource(module)))))
    return found


def _per_module_sites() -> dict[str, int]:
    """Returns how many sites each walked module contributes, by module name.

    Args:
      None.

    Returns:
      One entry per module in :data:`_WALKED`.

    Raises:
      None.
    """
    return {
        module.__name__: len(_sites(ast.parse(textwrap.dedent(inspect.getsource(module)))))
        for module in _WALKED
    }


def test_the_roster_covers_at_least_as_many_sites_as_before_the_move() -> None:
    """FR-004 AC-2. A roster walking the wrong module is the vacuous case, and it passes.

    Asserted **per module against a name-keyed floor**, not against a total. A total is the
    version that fails to catch its own scenario: `_WALKED = (_lifecycle,)` yields 37 sites,
    clears a total floor of 36, and passes while every site in `decorator` has stopped being
    checked. Measured, which is why this test is written the way it is rather than the obvious
    way.
    """
    counted = _per_module_sites()
    assert set(counted) == set(_SITE_FLOOR), (
        f"the walked module set changed: expected {sorted(_SITE_FLOOR)}, got {sorted(counted)}"
    )
    for name, floor in _SITE_FLOOR.items():
        assert counted[name] >= floor, (
            f"{name} shrank to {counted[name]} sites, under its floor of {floor}"
        )


def test_no_scope_name_is_shared_across_the_walked_modules() -> None:
    """The three-part key has no module term, so a shared scope name would merge two sites.

    Keying by module instead would be the other fix and costs every row a rewrite; this asserts
    the property that makes the cheaper key sound, rather than assuming it.
    """
    scopes = {
        module.__name__: {
            scope for scope, _, _ in _sites(ast.parse(textwrap.dedent(inspect.getsource(module))))
        }
        for module in _WALKED
    }
    for (left, right) in itertools.combinations(sorted(scopes), 2):
        shared = scopes[left] & scopes[right]
        assert not shared, f"{left} and {right} share these scopes, which would collide: {shared}"


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
    """AC-2. A category with no reason is a row that will be copied rather than thought about.

    The distinctness check is not pedantry. The valid set below is built from the same four
    constants the rows are written with, so a constant edited to another's text validates
    itself: setting `OWNERSHIP` to `EXISTENCE`'s string left the whole file green with all six
    ownership rows silently reading as existence.
    """
    categories = {EXISTENCE, LIVENESS, OWNERSHIP, OWNERSHIP_AND_MOMENT}
    assert len(categories) == 4, "two categories carry the same text, so the rows using them agree"

    for site, (category, reason) in ROSTER.items():
        assert category in categories, f"{site} has an unknown category"
        assert len(reason) > 40, f"{site}'s reason is too short to be one"


def test_the_two_identical_swap_guards_keep_their_own_reasons() -> None:
    """A reason swap between two rows sharing one expression is otherwise undetectable.

    Prose cannot be checked in general, but these two name the branch they describe, so the
    pairing is assertable — and this is the pair the occurrence index exists for, where getting
    it wrong is exactly round 3's defect re-introduced by hand.
    """
    in_lock = ROSTER[("_swap_sink", "worker is not None", 0)][1]
    out_of_lock = ROSTER[("_swap_sink", "worker is not None", 1)][1]
    assert "the in-lock branch" in in_lock and "out-of-lock" not in in_lock
    assert "the out-of-lock branch" in out_of_lock


def test_the_roster_finds_the_bare_form_that_shipped_unseen() -> None:
    """AC-1. A bare `is not None` on its own is the phrasing SPEC-033's docstrings warn about,
    and a walk looking only for `_live_worker()` and `.sink is` comparisons would never see it.

    The site moved from `_worker is not None` to `worker is not None` when SPEC-040 FR-002 made
    `_shutdown_worker` bind the existence question rather than read the global twice. The
    property under test is unchanged — a bare comparison in boolean position, named only by a
    worker sentinel, is still found — and the local form is the *weaker* of the two for the
    walker, since it rests on the bare `worker` sentinel rather than on `_worker`.
    """
    assert ("_shutdown_worker", "worker is not None", 0) in _numbered()


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
            emit(None if _worker.draining else worker)
            assert _worker.sink is owed
            y = _live_worker()
            if not worker_holds_sink:
                pass
            if _worker.retired:
                pass
            if _worker:
                pass
            held = _worker.sink
            pair, _ = _worker, None
            boxed = [_worker][0]
            fetched = _process_worker()
            snapshot = worker.health()
            return _worker is not None and _worker.sink is None
        """
    )
    assert found == {
        "_worker.sink",
        "(_worker, None)",
        "[_worker][0]",
        "_process_worker()",
        "worker.health()",
        "_worker is None",
        "worker.draining",
        "None if worker is None or worker.retired else worker",
        "_worker.draining",
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


def test_an_accessor_under_a_neutral_name_cannot_hide_a_binding() -> None:
    """The round-11 attack: a fresh accessor whose own name says nothing about the worker.

    `worker = _snapshot()` above `_swap_sink`'s out-of-lock guard converts it from liveness to
    existence. Measured on `7a10348`, before :func:`_accessor_names`: roster 13 passed, suite
    1196 passed, `configure()` 2.01 s → 0.00 s, `B.closed` 1 → 0. The binding was filed and the
    filter did not recognise it.
    """
    found = _walk_source(
        """
        def _snapshot():
            return _worker

        def _swap_sink(new_sink):
            worker = _snapshot()
            if worker is not None:
                pass
        """
    )
    assert "_snapshot()" in found, f"the accessor's call site was not filed: {found}"


def test_an_accessor_of_an_accessor_is_reached_in_either_order() -> None:
    """The wrapper is filed whether or not it is defined after what it calls.

    This is why :func:`_accessor_names` runs to a fixpoint. **Source order is the whole test**:
    a single pass already reaches `_fetch` when `_snapshot` is above it, since the token set is
    updated as the pass goes — a first version of this test used that order, and the
    no-fixpoint mutant passed it. Defining the wrapper *first* is the case that needs the
    second pass, and no rule says an attacker must add their accessor at the top of the file.
    """
    wrapper_first = """
        def _fetch():
            return _snapshot()

        def _snapshot():
            return _worker
    """
    snapshot_first = """
        def _snapshot():
            return _worker

        def _fetch():
            return _snapshot()
    """
    for order, source in (("wrapper first", wrapper_first), ("snapshot first", snapshot_first)):
        tokens = _accessor_names(ast.parse(textwrap.dedent(source)), _BASE_SENTINELS)
        assert "_fetch" in tokens, f"{order}: {tokens}"


def test_a_function_that_hands_back_no_worker_is_not_a_sentinel() -> None:
    """The derivation must not degenerate into "every function name is a token".

    That would file every call in the module and make the roster unmaintainable — which is the
    failure mode of over-matching, and the reason the added tokens are the ones a `return`
    justifies rather than all of them.
    """
    tokens = _accessor_names(
        ast.parse(
            textwrap.dedent(
                """
                def _ensure_sink():
                    return _config.sink

                def _open_span(name):
                    return Span(name=name)
                """
            )
        ),
        _BASE_SENTINELS,
    )
    assert tokens == tuple(sorted(_BASE_SENTINELS)), tokens


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
    assert [
        _literal_parts(node, silent)
        for node in ast.walk(ast.parse(silent))
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    ] == [[], []]

    joined = 'x = "the" + "process"'
    assert [
        _literal_parts(node, joined)
        for node in ast.walk(ast.parse(joined))
        if isinstance(node, ast.Constant)
    ] == [[], []]

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
    (
        "try:\n    def f():\n        pass\nexcept ImportError:\n    def f():\n        pass\n",
        ["f", "f#1"],
    ),
    ("if x:\n    def f():\n        pass\nelse:\n    def f():\n        pass\n", ["f", "f#1"]),
    (
        "class A:\n    def m(self):\n        pass\n\n\nclass B:\n    def m(self):\n        pass\n",
        ["A.m", "B.m"],
    ),
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
