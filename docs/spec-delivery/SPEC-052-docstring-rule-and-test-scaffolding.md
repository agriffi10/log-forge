# Completed Spec — SPEC-052: The Ungated Docstring Rule, and the Scaffolding It Left Behind

## What was completed?

- **The docstring cap moved to the summary line** (`CLAUDE.md` → Code Conventions). It read "a
  description of ≤3 sentences" while the sentence beside it invited unbounded reasoning in the
  docstring, and the code followed the neighbour: at `451edf9`, of 506 documented defs, 158 exceeded
  it reading "description" as the text before `Args:` and 492 reading it as the whole docstring.
  Re-scoped, compliance was already total, so the change cost no edit to `src/`.
- Three further false clauses in the same bullet went with it: the `Args:`/`Returns:`/`Raises:` trio
  is scoped to **functions and methods** (58 of 59 classes carry none), `Attributes:` is stated
  **conditionally** (an unconditional wording would have installed a rule violated 37-of-59,
  ungated), and the "three docstrings are asserted by tests" enumeration — there are seven — became
  the query that answers it.
- **`scripts/docstring-lint.py`** — four checks over `src/log_foundry`, standard library only, a
  local pre-push gate like `docs-lint.sh`. Registered in all five places that list gates; two of
  them (`docs/process/reviewer-contract.md`'s reviewer list and `.github/PULL_REQUEST_TEMPLATE.md`) had already drifted by
  omitting `docs-lint.sh`.
- **`scripts/docstring-lint-test.sh` + `tests/docstring-lint/`** — the fixture corpus, with one
  failing case per terminating condition, a silence case per check and per exemption, a
  `harness-reject` meta-case, and a direct assertion for the interpreter floor.
- **The dead test scaffolding is gone**: every `pytest.importorskip("log_foundry…")` call became a
  plain import, the four pre-implementation skip sites and two docstrings describing them went, and
  `tests/README.md` was rewritten. The three `importorskip("nats", …)` guards stay — they can fire.

**Three things the spec got wrong and the build corrected — all one defect.**

The population is **derived, never carried** — it moved three times while this spec was open
(SPEC-048, SPEC-050 and SPEC-051 each added guards), so FR-004 was amended to say the rewriter
counts them at run time. A fixed number would have left guards standing with the gate green.

And the rewriter matched **assignments only**. That was true of every site when the spec was
written and false by the time it ran: SPEC-051 introduced *inline* uses
(`pytest.importorskip("…").__all__` inside a parametrize tuple, `.StdoutSink()` in a body), which
the AST walk did not see. FR-004's own grep criterion caught the five survivors — the criterion was
scoped to the call, not to the shape, which is the only reason it did. A criterion that had been
written as "the rewriter reports N substitutions" would have passed.

And a third, found by the diff review rather than by the build: an acceptance criterion predicted
"exactly two" sites where the substituted import itself would be unused. There were **seven**. The
normative half held — all seven were listed and decided by hand, never auto-fixed — but the number
was written in advance against a population that moved, which is the same mistake as the other two,
sitting in the same file as the sentence naming it. All three criteria now derive their counts.

## What changed from earlier specs?

- `CLAUDE.md`'s Code Conventions bullet is rewritten; three of its claims were false.
- The **Review paragraph** in Session Workflow is cut to its rules, with the measured evidence left
  in `docs/process/reviewer-contract.md`, which it already pointed at. Every rule was verified present afterwards by
  enumerating the whole population, not by reading. This bought the byte-budget room the rule change
  needed without pruning any area's fences — two other sessions were blocked on the same 34,000-byte
  cap at the time.
- The PR-queue operating detail in "PRs & main" is likewise cut to its invariant plus a pointer.
- `tests/` no longer skips on a `log_foundry` module, so an `ImportError` in one is a failure again
  rather than a silently skipped file. Measured both ways on the same break — removing
  `sinks/sqs.py` gave **exit 5, "no tests ran"** before and an **`ImportError` collection error**
  after. Only 16 of 54 modules are reachable from a bare `import log_foundry`; the other 38, every
  sink among them, are where the guard was still hiding a real failure.

## Verification

All eight gates green locally on every push: `ruff`, `mypy`, `pytest`, `spec-lint`, `docs-lint`,
`docs-lint-test`, `docstring-lint`, `docstring-lint-test`. The gate was run against `main` and every
live peer branch and produced exactly one finding — the file this spec fixes — so it reds nobody's
in-flight work. The corpus was proved by mutation: every check, every terminating condition, every
exemption and every regex component, each killed by its named case. The sweep was verified by
diffing collected test **names** and skip **identities** before and after, never a pass count —
both captured on this branch's own start commit rather than quoted, and both byte-identical across
the sweep and again across the hand edits.

**One rule this spec earned, worth keeping.** *A sweep produces candidates, never verdicts.*
The cut to the Review paragraph was verified by two sweeps — did every rule survive, and does
the pointer's target carry what left — and the second flagged three units as missing. All three
were false positives: a shingle test keyed on exact wording reports a **rewrite** as an absence,
so the instrument built to detect absences manufactured them, and two of the three were text
that had never been removed. Thirty seconds of `grep` per hit settled all three. The population
a sweep enumerates is mechanical; the adjudication of each hit is not. This is the sharper form
of "a verification method has a frame".

Two candidates left for a follow-up, deliberately not taken here: a fifth check for docstring body
indentation (SPEC-049 fixes the single violation; the gate ships at zero and should stay there), and
a `docs-lint` check for a `Measured <date>` with no commit SHA — `docs/architecture.md` carries the
rotted example the rule itself cites.
