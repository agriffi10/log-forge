# Completed Spec — SPEC-087: A Swap Keeps the Sinks the Graph It Installs Still Holds

## What was completed?

- **The kept read and the cede** (FR-001), in `_lifecycle.py`. `_walk_beneath` is `_capture`'s
  loop, factored out; `_installed_graph` reads the graph a swap installs, once, before its first
  section, and writes its own stopped-short line when it trips or raises. `_swap_sink` cedes an
  owed sink in that graph with `_CloseRegistry.cede` instead of releasing it.
- **One close per decision** (FR-002). `_Keep` (kept, remaining, closed) rides every release a
  swap or a `_close_owed` take starts; `release(..., keeping=)` holds it per thread and a
  forwarded release asks `spares` after the `releasable` check.
- **The guard** (FR-003). `_close_owed` passes over a taken sink lying beneath one it left owed,
  through its existing put-back; `_swap_sink`'s stale take passes over one lying beneath a sink in
  use (`_lies_beneath`). Kept is tested first.
- **The record** (FR-004, FR-006). `docs/architecture.md` §7 step 4, §12 (the item struck under
  *Resolved* with its two corrections; two new open items, the wrapper that leaves its member
  alone and the stop-signal hand), §13 (the four shapes, re-measured at `f4d45665`); invariant 5;
  the new decision in `docs/decisions/pipeline/` and four rewritten in place; the README;
  `docs/release-notes/v1.2.1.md`, created here, which also carries SPEC-082's owed notes; the new
  probe `docs/audits/2026-11-14-probes/lifecycle/leaves_remaining_holder.py`.
- **Tests**: `tests/test_shared_sink_swap.py` (126 collected); the old pin in `tests/test_owed_closes.py`
  rewritten to both directions and one close.
- **Deviation, FR-003's concurrent criterion** (revised in the spec). Under four logging threads a
  non-shared member was sometimes closed twice, in sequence: a log with no span read the wrapper, a
  swap took it, the log armed it again — invariant 5's SPEC-045 re-arm. Unchanged, the assertion
  failed 3 of 6 serial runs; held to the wrapper's arms (counted by wrapping the registry's `arm`)
  it passed 6 of 6 serial and 5 of 5 parallel. Every assertion on A is unchanged.

## Red against `5cb0f449` (FR-005)

The built test file, copied into a worktree of `5cb0f449` (whose `src` the import was shown to
come from), collects there. All 28 rows of `test_a_shared_sink_is_kept_open_and_closed_once`
fail on the event count, each also showing the shared sink closed twice; the refusing double
and `FileSink` gave identical counts. Losses are `(dropped, failed_batches, orphan_lost,
in_span_lost, submitted_after_shutdown)`.

| Wrapper | Direction | No span | Spans |
|---|---|---|---|
| `MultiSink` | forward | 1/3, `(0,0,0,0,0)` | 2/6, `(0,0,0,0,0)` |
| `MultiSink` | reverse | 1/3, `(0,0,2,0,0)` | 2/6, `(0,1,0,0,0)` |
| `FilteringSink` | forward | 1/3, `(0,0,2,0,0)` | 2/6, `(0,1,0,0,0)` |
| `FilteringSink` | reverse | 1/3, `(0,0,2,0,0)` | 2/6, `(0,1,0,0,0)` |
| `TransformSink` | forward | 1/3, `(0,0,2,0,0)` | 2/6, `(0,1,0,0,0)` |
| `TransformSink` | reverse | 1/3, `(0,0,2,0,0)` | 2/6, `(0,1,0,0,0)` |
| `_FactorySink` | reverse | 1/3, `(0,0,2,0,0)` | 2/6, `(0,1,0,0,0)` |

Forward `MultiSink` moves no top-level term because it absorbs a member's refusal into its own
counters, as before. The seeded concurrency test failed every seed there, A closed 116 to 136
times.

## Probes on the built tree (`f4d45665`)

`p4_shared_child_swap.py`: `X.closes=1 X.events_after_its_close=0` on all four rows (`multi` and
`filter`, orphan and worker), stderr empty. `p4b_forward.py` and `p4b_shared_child_shipped_sink.py`:
`lines in the file=4` of 4 and `=8` of 8, every loss term 0, stderr empty.
`leaves_remaining_holder.py`: `A.closes=0 A.closed_while_inside=0 A.refused=0 A.events=5
B.closes=1` after the expected timed-out line, against `A.closes=1 A.closed_while_inside=1` at
`5cb0f449` (quoted in full in §12).

## FR-006's search on the final tree

The spec's pipeline, run verbatim on the committed tree: **41 matches**. Every one now states the
rule as built, records its correction, or is a named exception.

- **`src/` and `tests/`, 21, rewritten by the build.** `_lifecycle.py` 11: `_CloseRegistry`'s "taken
  to be closed", its four moves, "a ceded sink is neither owed nor in flight, by design" and
  "arms, takes, cedes or puts back"; `cede`'s "one route out of the record that closes nothing"
  and its own by-design line; `_close_owed`'s and `_swap_sink`'s "a sink this call takes";
  `_swap_sink`'s fenced bullet (taker, then "means taken", which stands with its reason now in
  front) and its `owes` paragraph. `worker.py` 1: `Worker.retarget`, standing, with why
  `declined` still follows. `test_lifecycle_races.py` 4: the moves listed with `cede`, and the
  take row's "a sink it takes". `test_sink_release_roster.py` 1 and `test_owed_closes.py` 1:
  narrowed with the cede exception. `test_worker_predicate_roster.py` 1: the fenced row's reason.
  `test_owed_closes.py` 2 **stand**: the spec's two named exceptions, a test name and message
  about concurrent takers.
- **`docs/`, 20, rewritten here.** *A shutdown waits for a swap…* 5: Part 1's "taker" now the
  worker's sink only, with why `cede` and the guard never reach it; "means taken" stands behind
  it; the retirement paragraph's taker with the same reason; two in the new History entry naming
  the old claim. *Lifecycle state has one holder…* 5: the moves with `cede` (twice, the list and
  the Do-NOT line), the narrowed claim, the by-design line, and its History entry.
  *One lifecycle owner…* 3: narrowed, by-design, History. *Concurrent sink changes…* 6: the body's
  new reason, the 2026-11-12 entry's clause struck in place with its correction beside it (three
  matches), and two in the new History entry naming the old claim. The new decision 1: "a new
  route out of the owed record is a `_CloseRegistry` method".
- `_RECORD_MOVES` in `test_lifecycle_races.py` lists the moves too and the pipeline does not see
  it; it names `cede`.

## Rosters and the floor (FR-005)

`test_worker_predicate_roster.py` gained **two rows, not the four predicted**, both `MOMENT` in
`_swap_sink`: the stale take's test of what something may be inside, and the join after the
retarget. The fenced branch's condition text is unchanged, so its row keeps its key and only its
reason was rewritten; the keep's own condition, `kept.get(id(stale)) is stale`, holds no
sentinel token and is not a site. `_SITE_FLOOR` stays at 92 for `_lifecycle` with the derived
count now 94: FR-005's criterion lets this change add rows to that file and edit existing text
only at a search site, and the floor is a minimum, which 94 meets. That leaves two sites of the
slack the floor's own docstring says each raise absorbs, so a later shrink of up to two goes
unseen until the floor is raised to 94 by a change that may edit it.
`test_lifecycle_races.py` gained `cede` among the moves and two dispositions (no worker; after the
retarget), and the put-back row's reason names the passed-over sink. `test_child_sink_slots.py`
gained one exemption, `_Keep.remaining`. `test_runtime_holder.py`, `test_lock_held_discharges.py`,
`test_lifecycle_inventory.py`, `test_runtime_state.py`, `test_sink_ownership.py` and
`test_ownership_record.py` are unedited and pass.

## Verification

Mutants in `_lifecycle.py`, each restored by copy with `__pycache__` cleared, run against
`test_shared_sink_swap.py` (M10 against `test_ownership_record.py`): the keep's skip removed
(killed: the table's 16 reverse rows fail, its 12 forward rows pass); `spares` ignoring the closed
claim, or remaining (killed); the closer's guard off, the swap's guard off (killed); `cede` a no-op
(killed); either decision's `took` loop removed (killed); `_installed_graph` returning the root
only (killed); a forward not inheriting the enclosing keep (killed); `_capture` bound at
definition time (killed). Testing the guard before kept in the stale loop **survived** at first —
on the worker path the second section cedes in either order — and is killed by the `[retired]`
case of `test_a_stale_sink_both_kept_and_beneath_a_sink_in_use_is_ceded`, added in `f4d45665`.
The completion ran `gen-index.sh` (no diff), `docs-lint.sh`, `spec-lint.sh` and
`docstring-lint.py` on the branch. `f4d45665` is the build's head; the completion's commit
changes no file under `src/` or `tests/`, so measurements taken there hold for the merged tree.

## What changed from earlier specs?

- SPEC-045: its pin that the shared sink "must not become a loss" is replaced; both markers say
  closed by SPEC-087.
- SPEC-078: `_CloseRegistry` gains a fourth move, `cede`, and the rule that every route out of
  the record is a take holds for a sink taken to be closed, not for a ceded one.
- SPEC-081: `_capture`'s walk is `_walk_beneath`, shared with the swap's read; its reach, bound
  and once-per-process line are unchanged.
