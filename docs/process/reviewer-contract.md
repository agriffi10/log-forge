# The reviewer contract

**The reviewer contract — three artifacts, one gate**

Nothing reaches the next stage on its author's own judgement. A **spec**, an **implementation plan**
and a **diff** each go to a reviewer in a **fresh context** — a subagent or a new session, never the
context that produced the artifact. An author reviewing their own work assumes its output was
intended and rubber-stamps it; that is as true of a plan as of code, and a wrong plan is the more
expensive one to leave *undetected*, because the code that follows will faithfully implement it.

**How many reviewers, and where.** **One** on the spec when it is written, **one** on the
implementation plan when the build actually starts, and **two** on the diff before it is pushed.
Those are the only artifacts that draw a reviewer — the PR grouping is the implementer's call, made
in a sentence (`session-rhythm.md`, step 7). The diff gets two because it is the **widest**
artifact and the last one before the branch goes public: a spec or a plan is one document a single
reader can hold whole, while a diff spans code, tests and config *and* the criteria they are
supposed to satisfy, and this repo's own measured history (*Rotating the frame*, below) is that the
first differently-framed pass over a change finds a class the previous frame could not see. That
evidence argues for a **different frame**, not for the number two; two is the floor because one
frame is demonstrably not enough and a floor has to be a number.

The two are **two frames, not two rounds** — the same frame run twice buys wording — and they are
not free picks. One starts from the **change** and reads it against what it is supposed to
satisfy; the other starts from **something other than the change** — the system it lands in, or
the document it claims to implement. On a code diff those are: the spec's acceptance criteria plus
the `best-practices/` rules for the domains it touches, and a pass that **builds** the thing and
runs the suites rather than reading it. On a diff with no code in it — a spec, a plan, a docs
change — they are:
the artifact against its own sources, and the artifact against every *other* place the same rule
is stated. Both land **before** the push; a review that only happens once the branch is public
does not count toward the two.

Every count here is a **floor**, not a cap, but the two floors work differently and should not be
read across. **On a diff**, two is a minimum before a push: a clean first review does not close
the gate, because "the reviewer found nothing" means nothing within the frame it was given, and
the exit rules below decide when to stop *above* two. **On a spec or a plan**, one review closes
the gate — a clean review there is an answer. What makes that count a floor is **re-entry**: a
revised artifact is a new artifact and goes back through the gate as one, which is why a phase
that rewrites the plan sends the new plan through again.

**A replaced artifact restarts its gate; a revised one does not.** Fixing findings in place is a
revision, and the gate that produced them is answered. Throwing the design away and writing a new
one is a **replacement**, and the replacement has been reviewed by nobody — which is the shape
*A rewrite under review pressure is the highest-risk artifact in the loop* warns about, stated as
a counting rule rather than as advice. **Where it happens decides what is owed.** Replaced at the
spec or plan gate, the later gates still see it fresh and nothing extra is due. Replaced *after*
the diff reviews have run, those reviews examined something that no longer exists, so it owes one
more — in the frame that would catch the replacement's own class of defect, which for a mechanism
change is execution — and the fact that this exceeds the cycle's budget is said out loud rather
than done quietly.

Measured on the two specs that produced the rule. SPEC-045's design was replaced **late**, after
both diff reviews; the extra reviewer found both remaining blocking gaps, each a mutant that
passed the entire suite. SPEC-046's was replaced by its **spec** review, and the ordinary cycle
absorbed it: the plan review found two correctness defects in the new design before a line of it
was written — an identity test written as value equality, and an unguarded `Thread.start()` on a
path documented `Raises: None` — and both diff reviews then found more, with no fifth spent.

- **The gate is blocking, and what it asks for is an answer, not a filing.** A spec is not
  Draft-ready, a plan does not start code, and a branch does not reach the remote, until every
  finding has been either **fixed** or **flagged** — said out loud, so the call is visible and can be
  argued with. A finding silently dropped is a finding that was not reviewed; a finding rejected in
  one sentence is a finding that was. **Write a rejection down only when it carries a lesson worth
  keeping** — then it belongs in the spec, its delivery doc, or its area's register entry, as
  reasoning, not as a paper trail. (The register is `docs/decisions/`, one file per area, added
  2026-09-02; each area file opens with its fences and `architecture.md` holds the system-shape
  reasoning behind them.)
- **The reviewer gets the artifact and its sources, never the author's reasoning.** For a spec: the
  spec file, its build-order entry in `INDEX.md`, the `architecture.md` sections it claims to follow,
  and the specs it depends on. For a plan: the plan, the spec, and `component-inventory.md`. For a
  diff: the diff, the spec's acceptance criteria, and the `best-practices/` rules for the domains it
  touches (route via their INDEX). Handing over the authoring rationale tells the reviewer what to
  conclude.
- **What each review is for.** A **spec** review asks: is every FR testable and binary; does any
  acceptance criterion pass vacuously; is anything in Out of Scope actually required by an FR; does
  it contradict a settled decision or silently supersede one without saying so; are there Open
  Questions wearing declarative clothes. A **plan** review asks: does every FR and acceptance
  criterion have a phase that delivers it; is existing reuse used rather than re-built; has
  out-of-scope work crept in; is a phase resting on a premise nobody has checked. A **diff** review is
  the rules below.
- **Cap same-frame rounds at two, then rotate the frame** — the rule the next block earns, and it
  applies to specs and plans as much as to code. A second round of the same reviewer on the same
  artifact converges on wording. Rotate instead: for a spec, a reviewer briefed only on the
  *dependencies* it claims, or one asked to build the thing from the spec alone and report what it
  could not determine; for a plan, one asked to find the phase that will be discovered impossible.
- **Exit on a new frame finding nothing**, never on a round count — and never below the counts
  above. The exit rules say when to stop *above* a floor, not that a floor can be skipped: a
  diff still gets its two frames when the first comes back clean.
- **A reviewer finding is not an instruction.** The author decides. Rejecting one is ordinary and
  costs a sentence; the thing to avoid is deciding silently, because then nobody can tell a
  considered rejection from a finding that was never read. A fixed finding that was wrong is a
  silent regression, which is the same failure in the other direction.

**What a diff review checks.** That is the *first* of the two frames a diff gets (above): the spec's acceptance criteria **and the relevant `best-practices/` rules** (route via
its INDEX) — not just "does it look fine." The second starts from the system rather than the
diff, and on code it builds the thing and runs the suites. Its starting point is
`docs/invariants.md`: for each numbered invariant the diff touches, it asks whether the change
keeps it on every **twin path** (invariant 6), not only the path the spec names — a fix applied to
one twin is a recurring shape in this repo's history, and the page names examples.

**Rotating the frame — why round count is the wrong exit criterion**

Review rounds converge on the frame they are given, not on correctness. Measured on SPEC-033
(2026-08-07): **eight** rounds of independent review — two on the spec, three on its revisions,
three on the code — after which the merged result still carried two defects, one of which was a
regression of a guarantee an earlier spec had shipped. Rounds 1–3 found design defects; rounds
4–8 increasingly found wording. The *first* differently-framed pass afterwards found ~25 defects,
several of them years old.

So the loop rotates the frame instead of adding rounds:

- **Cap same-frame review at two rounds.** After the second, switch frame rather than iterate:
  adversarial *execution*, whole-module fresh eyes (not the diff), or concurrency.
- **Exit on a new frame finding nothing**, not on the current frame converging. "The reviewer
  found nothing" means "nothing within the frame I gave it."
- **The exit rules govern the ceiling, never the floor.** "Never exit on a round count" is about when
  to *stop*; it does not license stopping below the counts in *The reviewer contract*. A diff still
  gets its two frames even when the first one comes back clean.
- **Every frame is entered at most twice, and when they are exhausted, stop.** SPEC-035 FR-002
  ran to **eleven** rounds: six same-frame on the walker (the cap above, ignored five times over),
  one library-first, three attacker, and an eleventh killed mid-run. The last two attacker rounds
  did find real holes — so the rule is not "stop sooner regardless", it is that a *third* round in
  one frame is evidence the frame is exhausted, and the choice then is a **different** frame or
  the decision to ship. Rounds 5–8 all found defects in the lint added at round 4, never in the
  roster it guards: when consecutive rounds find defects in the *previous round's fix* rather than
  in the subject, the review has become its own subject.
- **When a change is a guard on a guard, bound it before starting.** A test that polices a rule is
  worth having; a test that polices the test that polices the rule is where eleven rounds went.
  Decide up front what it is allowed to cost, and if it exceeds that, record the residual exposure
  and merge — the exposure is usually smaller than the delay. Better still, ask whether the rule
  could be made unrepresentable instead of policed (SPEC-040 is that question, asked late).
- **At least one pass must start from the library, not the diff.** Every diff-scoped review is
  structurally blind to a defect that predates the diff — which is how `flush()` came to be blind
  to an open span, breaking the documented serverless recipe, through ten specs of review.
- **A review SAMPLES; when the risk is ABSENCE, enumerate instead.** Review is the right instrument
  for "is this new code correct" and a weak one for "what did we stop checking" — a reviewer reads
  what is there, and a deletion leaves nothing to read. Measured on a sibling repo
  (`s3-upload-portal`, 2026-08-28, a test-suite reduction): three fresh frames returned **4, then 7,
  then 17** findings, diverging rather than converging, because each sampled a population of
  hundreds. The mechanical sweep that answered it completely — reason codes raised vs reason codes
  asserted — ran in seconds. **Before reviewing a change whose risk is what it removed, write the
  sweep that lists the whole population.**
- **Spot-check a negative claim before acting on it, and again before writing it down.** "Only
  documented on page X", "no test covers this", "nothing imports this file", "there is no scrubber"
  — each is a claim about an absence, and an absence is what a search proves badly. Verify it
  yourself whether a reviewer said it or you did. The asymmetry that makes this worth a rule: a
  wrong claim entering **code** has tests to catch it later, while one entering a spec, a register
  entry, a docstring or a commit message has nothing, and the next session reads it as settled. Two
  such claims on the sibling repo shipped as source comments and were believed for weeks — a
  "registry guard" the router advertised and did not have, and a credential "scrub" a config module
  named as the thing keeping tokens out of logs. Both were found by writing the test that assumed
  them.
- **A mechanical sweep REPORTS before it applies.** Print what it would change, read the list, then
  re-run with `--apply`, and check the result by diffing **test names** (`pytest --collect-only`)
  rather than a pass count. A classifier that looks right is routinely wrong at its edges: a sweep
  for unused module-level helpers classified pytest's `Test*` classes as dead and would have deleted
  every test inside them — caught only because it printed first. A regex over source is the same
  hazard one level down; parse instead where a parser exists.
- **A verification method has a frame, exactly as a review does.** Three sweeps that all ask "is the
  old content still present" are one check run three times, however exhaustive each is — they cover the
  mechanical half of a change and say nothing about the half that was authored. Before trusting a
  verification, ask what question it asks, and whether anything asks a different one. Proving that moved content survived says
  nothing about content that was newly written, and the newly-written half is where the errors are.
  Its stopping rule is the sweep's: stop when a fresh question would not change what you conclude.
- **Never carry an evidence sentence from another repo's commit message or script header — re-measure
  it where you are putting it.** This is the mechanism behind every false claim a review has caught in
  these docs, and it does not feel like guessing: the sentence was written by someone who had the repo
  open, it reads as measured, and it is repeated verbatim. A claim about another codebase is unverifiable in this one, and a
  reader who checks it finds nothing. The sentences that survive checking come from the same
  place as the ones that do not, which is exactly why the habit persists.
  **Anchor both ends of any measurement to a commit** a reader can re-derive it from: anchoring only
  the end is what let "eight weeks" through, because "from" was then whichever commit the writer had
  in mind. If you cannot cite both ends, state the principle and drop the number.
- **A finding should carry a reproducing mutation, and the fix is verified by re-planting it.**
  Then fix-verification is mechanical and needs no second reviewer — which is what stops a review
  round spawning a review of its fixes. A frame only finds what its mutations probe, so a fix that
  satisfies a weak mutation can still be wrong: the same reduction had a CI-watch guard restored
  against one frame's probe and broken under the next frame's.
- **A gate is not tested by running it on the thing it guards.** Running a linter over the repo's own
  files proves the files pass; it proves nothing about whether any check still fires, and a gate whose
  checks have gone quiet is indistinguishable from a healthy repo. Every gate owes a **fixture corpus**:
  one case per construct, asserting the specific **failure text** rather than the exit code, because a
  check that fails for the wrong reason gets "fixed" by changing the wrong thing. Include cases that
  assert **silence** — a corpus of only-failures cannot see a false positive, and false positives are
  a large share of what a gate gets wrong. Prove the corpus bites by defeating each check in turn and watching it redden.
  Measured here, not carried: `scripts/docs-lint.sh`, `scripts/docstring-lint.py` and
  `scripts/spec-lint.sh` each have a corpus of their own. The last arrived only with the
  invariant-citation check, and its first run found a parser defect the live specs could not
  show — every second FR skipped — which is the argument for a corpus in one line.
- **One fixture per guard is not enough when the guard has more than one exit.** A check with two
  terminating conditions is satisfied by a fixture exercising either, so the mutant that breaks the
  other survives with the suite green. Count the ways a check can stop, and write that many cases.
- **Reasoning finds wording; execution finds defects.** Every finding that mattered in the
  2026-08-07 audit was *run*: 5,980 events in one emit, 19 of 60 forked children hung, 0 of 9
  events delivered. Require a reproduction, not an argument.
- **A spec touching lifecycle or concurrency gets an execution harness, not a review.**
  `tests/conftest.py::run_concurrently` and injected preemption points exist for this; a race is
  not findable by reading.

**When a review changes a rule, re-audit the rule — not the line**

A review finding is usually reported as an instance ("this call site uses the wrong predicate").
Fixing the cited line and moving on is how the same defect survives repeated review: three
separate reviewers told SPEC-033 "ownership, not liveness", each naming a different call site,
each was fixed, and a fourth site shipped broken.

- When a finding is an instance of a **rule**, the fix is a test that enumerates **every** site of
  that rule and asserts each one's category — the same discipline the sink rosters already use,
  and for the same reason: a hand-maintained list rots and a derived one does not.
- **Scope added mid-review restarts the clock.** Widening a spec in response to a finding is often
  right, but the widening arrives late, gets the least scrutiny, and inherits confidence it has
  not earned. Two of SPEC-033's regressions were in scope added during its own review.

**Briefing the reviewer — four lines that change what comes back**

Measured on a sibling repo (`s3-upload-portal`, SPEC-205, 2026-08-18) over four rounds on one spec:
two adversarial-audit rounds, then two implementer rounds. The frame decides the *class* of finding;
the briefing decides whether the answer is honest.

- **Say that "this is sound" is a valid verdict.** A reviewer that infers it is expected to produce
  findings will produce them. The round that returned the most useful report was the one told
  explicitly that a short "implementable as written, here is what I built" was a valuable outcome —
  and it still returned real defects, so the line costs nothing.
- **Require it to cite where it looked** before declaring something missing — the spec line it read
  before concluding the spec is silent. That separates a genuine gap from a miss, and it is the
  cheapest way to keep a long report's false-positive rate down.
- **Tell round N+1 what round N fixed, and forbid re-auditing it.** Otherwise the second pass
  re-derives the first pass's findings and never reaches the new work.
- **Withhold the history from a frame that is testing self-sufficiency.** The opposite of the line
  above, and both are right: a pass asking *"is this spec buildable by someone who wasn't here"* must
  be given it cold.

**A rewrite under review pressure is the highest-risk artifact in the loop**

Adjacent to *scope added mid-review restarts the clock*, and stronger. When a finding causes a
substantial **replacement** rather than a widening, the replacement is written quickly, confidently,
and with nobody having reviewed it. On the sibling repo's SPEC-205 the second round found **four
blockers, all of them inside machinery the first round's fixes introduced** — two of which
independently broke a path the spec had a requirement to preserve. Budget a round for the fix, aimed
at the fix. This repo has the same shape on record: rounds 5–8 of SPEC-035 FR-002 all found defects
in the lint added at round 4, never in the roster it guards.

And **when the same area is wrong twice, the signal is about the area, not the draft.** Both SPEC-205
drafts were wrong about the same thing. A repeat in one place is where to spend the next frame, ahead
of anything the reviewer ranked higher.

**The implementer frame must actually build, and every frame must run the gates**

*The build-it-from-the-spec rotation named above, made concrete — it earns its own block because it
yields a different class of finding than any reading-based frame.*

- **Have it write real code, off-repo, and run the suites.** What it found by running that no reader
  found: nine existing tests that invert (a list the spec claimed was four, including one in a
  *different* suite from the one the spec's list implied), fixtures across two suites that silently
  become the new failure case, a hard-coded count in an unrelated coverage test, and a fake whose
  signature diverges from the real client.
- **Its output class is contradictions, unspecified shapes and sequencing** — two requirements that
  cannot both hold; a return type nobody named; a phase boundary that leaves `main` exposed. An
  adversarial reader finds defects and does not find these; the implementer finds these and is worse
  at defects. Run both, not one twice.
- **Every reviewer runs the repo's gates against the branch** — `ruff check`, `mypy`, `pytest`,
  `docs-lint.sh`, `docstring-lint.py`, and `spec-lint.sh` on any spec it touched. The two local-only
  gates are the ones a reviewer is likeliest to skip, and skipping one is how a branch stays red on
  a gate nobody in CI will ever run. Four rounds reviewed SPEC-205 and none ran the
  sibling repo's doc-layout gate; the branch was red on it throughout, for a reason unrelated to the
  spec, and it took an agent that *built* the change to notice. A review of a change touching gated
  files runs the gates.

**Exit on the trajectory of the findings, not on an empty round**

*Refines "exit on a new frame finding nothing".* In practice a fresh frame rarely finds literally
nothing — it finds something smaller. The honest signal is the **class** changing round over round:
on SPEC-205, blockers → machinery introduced by the fixes → sequencing and contradictions →
coin-flips no test would catch. Stop when a new frame's worst finding is one that would not change
the built result, and say which finding that was. **This governs the ceiling, never the floor** —
it says when to stop *above* the counts in `reviewer-contract.md`, not that a count can be
skipped, so a diff still gets its two frames when the first comes back with nothing that would
change the built result.

**An Open Question can wear a declarative sentence**

`CLAUDE.md` forbids a spec carrying Open Questions, and `spec-lint.sh` fails an "Open Questions"
heading — and the check is easy to pass while failing. SPEC-205 shipped the sentence *"either the
exception moves to a shared location or the call returns a refusal the caller raises. The spec states
which"* — and then did not. A sentence that **promises a decision** is an Open Question with a
declarative shape; so is an acceptance criterion demanding a **computed** bound while supplying none
of its inputs. Both were caught by an implementer who had to pick, and neither by a reader.
