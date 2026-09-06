# Spec: The Marking Walk's Restated Claims, and the Gate That Would Have Caught Them

**ID:** SPEC-053
**Status:** In Progress
**Last Updated:** 2026-09-04
**Depends On:** SPEC-042, SPEC-052

## Overview

`_lifecycle._mark_inherited()` is described in prose across sixteen files in `src/`, `docs/` and
`tests/`, and a large share of those descriptions said the same false thing: that it stamps
**everything** a forked child inherited `_FOREIGN`. It `setdefault`s, so a sink `configure()`
already stamped keeps the parent's real pid, and `_FOREIGN` lands only where nothing was recorded.
PR #218 corrected it across twelve files over four rounds and three fresh-context review frames,
and every round found defects in the previous round's fixes — including two in `src/` that all
three frames read past, and a header in `architecture.md` §13 that contradicted item 7 of its own
list forty lines below.

The corrections shipped. What did not is any answer to why one mechanism carries sixteen files'
worth of description, or why nothing mechanical noticed that so many of them disagreed with the
code and with each other. This spec adds a gate for the one spelling that recurred, a corpus that
proves the gate works, and it collapses the two `src`-side restatements onto the authoritative one.

**The gate is harder than it looks, and this spec's job is to hand the implementer the three ways
it is already known to fail rather than let them discover each one.** All three were measured, two
of them on real prose in this repository.

## Scope

### In Scope

- A check in `scripts/docs-lint.sh` failing a sentence that carries a **universal quantifier**
  alongside a **named reference to the marking walk** without a scoping term (FR-001).
- A fixture corpus in `scripts/docs-lint-test.sh` proving each clause fires and, more importantly,
  proving the check stays silent on correct prose — including the three known defeats (FR-002).
- Collapsing the **two** `src`-side restatements — `_FOREIGN`'s docstring and
  `docs/component-inventory.md`'s row — onto `_mark_inherited`'s docstring (FR-003).

### Out of Scope

- **The other two spellings, and this is measured rather than assumed.** The contrapositive ("an
  unmarked sink is claimable") and the possessive ("the `_FOREIGN` stamp `_mark_inherited` set")
  mostly carry no universal quantifier, so FR-001 cannot see them. It catches 6 sentences at
  `23fe6cc` against the thirteen that PR #218's first commit corrected, so **rather less than half
  the population is inside this check, and that is deliberate.** Widening the anchor to reach them
  is what produces the false-positive rate FR-001 rejects.
- **Rewriting `docs/specs/` or `docs/spec-delivery/` to match today's code.** They record what was
  decided and what shipped, at a past moment. FR-001 still *polices* them, which is the point below.
- **Collapsing the restatements in `tests/`.** Not because test prose is exempt — the first draft of
  this spec claimed test docstrings are "the reasoning for that test, not a description of the
  mechanism", and the spec reviewer showed that is false of exactly the prose that was wrong:
  `tests/test_fork_lifecycle.py:1253` at `23fe6cc` opened by restating FR-001's premise, and was
  false. The real reason is narrower and better: **FR-001's population includes `tests/` and
  `docs/specs/`, so the gate covers them whether or not the prose is collapsed.** A collapse buys
  nothing there and costs a reader the argument at its point of use.
- **Enforcing the check in CI.** `docs-lint.sh` is deliberately local, for the reason it gives.
- **Any change to `_lifecycle.py`'s logic.** `setdefault` is correct.

---

## Functional Requirements

### FR-001: A check that fails a universal claim about the marking walk

#### Description:

A sentence violates when it carries a **named** anchor (`_mark_inherited`, `` `_FOREIGN` ``, or the
phrase "marking walk"), a **universal quantifier** (`every`, `everything`, `all`, `any`, `always`,
`never`), and **no scoping term**. Sentences are built by the pipeline in *Data Model*, which is
part of the requirement rather than an implementation note: three of the four sweeps run during PR
#218 failed because of how they built their units, not because of what they matched.

**The anchor decides whether this is a gate or a nuisance.** This is the **lexical baseline** —
the instrument exactly as *Data Model* states it, before FR-001's syntactic scoping clause replaces
the lexical one. Both trees materialised with `git archive <sha> | tar -x -C <dir>`:

| anchor | scoping | `23fe6cc` (pre-fix) | `ac938c0` (post-fix) |
|---|---|---|---|
| named | applied | **6** | **0** |
| named | ignored | 10 | 9 |

A **descriptive** anchor — one that also matches "a forked child", "it inherited", "inherited sink"
and their neighbours — is rejected, and the reason does not need a precise count: every widening of
the anchor tried raised the clean-tree figure from 0 into the tens or hundreds against zero true
positives. An earlier draft of this spec quoted 22 and 31 for it; those numbers were not
reproducible, because a descriptive anchor is a family rather than a pattern. The principle is what
survives, and it is what `CLAUDE.md` warns about — half a gate's regressions being false positives.

Three things follow, and each is load-bearing:

- **6 to 0 is the discrimination.** All six pre-fix hits are real defects PR #218 corrected — three
  in `_lifecycle.py` (`_FOREIGN`'s docstring, `_MARKING_CEILING`'s summary, `releasable`'s
  paragraph), one in `docs/decisions.md`, one in `docs/specs/SPEC-050-lifecycle-residue.md`, and
  one in `tests/test_fork_lifecycle.py`. None is an artifact.
- **The scoping clause is load-bearing**: dropping it takes `ac938c0` from 0 to 9, and every one of
  those 9 is correct prose.

**Four known defeats. The implementer must close all four; none is hypothetical, and three were
measured on real prose in this repository.**

1. **`UNIVERSAL` matches words that are not quantifiers, and the correcting sentence is where that
   bites.** Two mechanisms, both live. A *negated* universal reads exactly like the claim it
   corrects, so the check cannot tell `every X is Y` from `not every X is Y`. And `\ball\b` matches
   "at **all**", which quantifies nothing. Both were measured on the same sentence — the repair to
   `releasable`'s docstring made on this branch fired first on a negated "every", and after that
   was rewritten it fired again on "a sentinel that is no process at **all**". A gate that reddens
   twice on two different repairs of the defect it exists to catch trains authors away from
   repairing it.
2. **A lexical scoping term is a magic word.** The list is tested against the whole sentence, so a
   term anywhere excuses a universal anywhere: inserting `setdefault` into the real `23fe6cc`
   defect sentences — without touching their claims — silences 5 of the 6. The word an author
   reaches for while fixing this defect is the word that defeats the gate. **This is why FR-001
   requires a syntactic scoping clause**, and why the table above is labelled a baseline.
3. **Quoting the false claim on purpose has to stay possible.** Five sentences in this spec file
   fire today — two are quotations of the false claim and three are the spec's own description of
   the pattern, which a "quotation" escape does not obviously cover. `scripts/docs-lint.sh` solved
   this class for check 9 with a fenced-block escape at `:742-743`, documented at `:727`; this check
   needs an equivalent that covers meta-description as well as quotation.
4. **The length cap is an unconditional escape.** *Data Model* drops sentences over 700 characters
   before any clause is consulted, so padding a false sentence past the cap silences it with its
   claim untouched. The six true positives run 85–398 characters, so there is headroom today, but a
   cap with no stated rationale is the shape `CLAUDE.md` warns about — a threshold that can be
   invalidated by its own success.

#### Acceptance Criteria:

- [ ] On **the tree the check ships on** the check reports zero violations — not on `ac938c0`, which
      is a historical tree that can pass while the branch carrying the check is red.
- [ ] On a tree materialised from `23fe6cc` the check reports the **6** violations listed above, by
      file. A check that cannot redden against the defect it was built for is evidence of nothing.
- [ ] Removing the scoping clause takes `ac938c0` from 0 to 9 — that figure is pinned to that tree,
      not to the shipping one, where the spec file and this branch's own corrections move it. On
      the shipping tree the same removal must yield a non-zero count, every member correct prose.
- [ ] **The scoping clause is syntactic, not lexical:** a scoping term excuses a universal only in
      the same clause, and the 9 correct sentences the lexical clause silences on `ac938c0` stay
      silent. Seven of those nine rest on the literal words `reach` or `setdefault`, so this is the
      change most likely to move the table and must be re-measured, not assumed.
- [ ] **Defeat 1 is closed:** both repairs made to `releasable`'s docstring on this branch pass —
      the negated-universal one and the "at all" one — and the sentence they replaced still fails.
- [ ] **Defeat 2 is closed:** inserting a scoping term into a false sentence *without changing its
      claim* does not silence the check, for all 6 of the `23fe6cc` sentences.
- [ ] **Defeat 3 is closed:** a documented escape makes both a deliberate quotation and a
      meta-description passable, and all five currently-firing sentences in this spec file pass
      under it.
- [ ] **Defeat 4 is closed:** the length cap either carries a stated rationale with deliberate
      headroom, or padding a false sentence past it no longer silences the check.
- [ ] The check names the file, the starting line and the sentence, so the failure is actionable
      without re-running a search by hand.
- [ ] It runs inside `scripts/docs-lint.sh` and is covered by that script's exit status.
- [ ] Serves no invariant: a gate over prose, which `docs/invariants.md` says is judged by
      `process.md` §5's rules rather than by a promise on that page.

### FR-002: A corpus proving the check fires, and proving it stays silent

#### Description:

`scripts/docs-lint-test.sh` already carries `.case` fixtures and is where this belongs. Running a
check against the repository it guards proves the repository passes, not that the check works — so
the corpus asserts failure text on planted violations and asserts **silence** on correct prose.

Silence matters more here than failure. On `ac938c0` the named anchor with the scoping clause
ignored reaches 9 sentences, every one of them correct. The one that matters most is in the decision
register itself: `docs/decisions.md`'s "A fork handler marks everything inherited that the parent
never recorded `_FOREIGN` …" is correct prose that fires on anchor and universal alike, and is
silenced today **only** by the lexical `setdefault`/`reach` in the same sentence. A syntactic
scoping clause must keep it silent. A check that reddens on the register's statement of the
decision is worse than no check.

#### Acceptance Criteria:

- [ ] Each clause — anchor, universal, scoping, and each pipeline step in *Data Model* — is
      mutation-tested: breaking it alone makes at least one named fixture fail. `CLAUDE.md` requires
      this of every check in a gate, not a sample.
- [ ] A failure fixture for each of the six `23fe6cc` sentences, verbatim from that tree, so the
      corpus is a regression record rather than an invention.
- [ ] Silence fixtures for: `docs/decisions.md`'s register sentence above; a negated universal and
      an "at all" (defeat 1, both mechanisms); a universal inside a fenced block; inside a Markdown
      table row; in a heading; and in a sentence with no named anchor. **Not** the "must be
      unclaimable" wording — it carries no named anchor at any of its four sites, so it cannot fire
      whatever the clause does, and a fixture that cannot fail proves nothing.
- [ ] A failure fixture for the `setdefault` null edit (defeat 2), so a lexical-only scoping
      implementation cannot pass this corpus.
- [ ] A fixture pinning the wrap behaviour: a violation split across a line break is caught, which
      is what separates this from the line-based sweeps that missed it.
- [ ] The corpus proves the check does **not** fire on `scripts/docs-lint.sh` itself, which will
      carry the anchor pattern and prose describing it. `scripts/docs-lint-test.sh` already warns
      that `scripts/` sits inside check 9's population; this check must exclude it or be written so
      it cannot self-match.
- [ ] Serves no invariant: the corpus proves FR-001's gate, and that gate polices prose.

### FR-003: One authoritative statement in `src`, and pointers to it

#### Description:

Three places describe `_mark_inherited` for a reader of the shipped library: its own docstring,
`_FOREIGN`'s docstring, and `docs/component-inventory.md`'s row. The first is authoritative — it
sits on the function and its `Raises:` block already carries the partial-walk residual. The other
two restate it, and `component-inventory.md` was still wrong after PR #218's fourth round precisely
because it had been written to match a summary line that PR then changed, in a file the branch did
not touch until its final commit.

**This FR collides with FR-001, and the collision is the design constraint.** `_FOREIGN`'s defining
property is "never a real pid" and a deferral names `_mark_inherited`, so any sentence doing both
carries a universal and a named anchor. Today's text survives only by an accident of sentence
splitting. Merging them — which "state what it is and defer" invites — reddens the gate. This is the
strongest argument for making FR-001's scoping clause syntactic rather than lexical, and FR-003 must
not be implemented by contorting prose to dodge a check that should not have fired.

#### Acceptance Criteria:

- [ ] `_FOREIGN`'s docstring states what the sentinel **is** and defers to `_mark_inherited` for
      when it is written, in a single merged passage, and that passage passes FR-001's **syntactic**
      clause. It fires under the lexical baseline — both natural merged wordings were built and both
      fire — so this criterion is the one that proves FR-001 was implemented as specified rather
      than as the baseline.
- [ ] `docs/component-inventory.md`'s row names `_mark_inherited`'s role and makes no claim about
      which pid a given sink's record ends up carrying. The row already satisfies this on today's
      tree, so this is a regression floor rather than a change.
- [ ] `_mark_inherited`'s docstring is unchanged — it is what the others defer to.
- [ ] The docstring assertions in `tests/` still pass; `grep -rn '__doc__' tests/` names sixteen
      sites, one of which reads `_lifecycle.releasable.__doc__`.
- [ ] Serves no invariant: this FR changes which docstring is authoritative, not what
      `_mark_inherited` does.

---

## Data Model

```python
# The instrument. This is part of FR-001, not an implementation note: three of the four
# sweeps run during PR #218 failed on how they built units, not on what they matched.

ANCHOR    = r"_mark_inherited|``_FOREIGN``|`_FOREIGN`|marking walk"   # named symbols only
UNIVERSAL = r"\b(every|everything|all|any|always|never)\b"            # re.IGNORECASE required
SCOPED    = r"reach|missed|residual|partial|setdefault|item 7"        # see defeat 2

# `must be` / `has to be` are NOT scoping terms: ordinary English carrying no scoping
# force, and including them excuses a false universal outright.

# Population: docs/**/*.md + root *.md + src/**/*.py + tests/**/*.py,
#   excluding scripts/ and tests/docs-lint/*.case (both self-match; the corpus
#   deliberately carries the wrong form).
#
#   This DIFFERS from check 9's population, and check 9 excludes its three for three
#   DIFFERENT reasons, none of which is "frozen records" alone (scripts/docs-lint.sh:650,
#   :654, :658): docs/specs and four sibling docs trees are frozen records; src/ is out
#   because its docstrings anchor to SPEC numbers rather than commits and whether that
#   counts is a decision nobody has taken; tests/ is out because the .case corpus carries
#   the wrong form on purpose. Check 9 also EXCLUDES four further docs trees this check
#   includes, and INCLUDES scripts/, pyproject.toml and .github/*.yml, which this one
#   must not. Copy the divergence into a comment beside both checks with all three
#   reasons intact — one reason standing in for three is how the wrong one gets cited.

# Units:
#   .py  -> ast-extracted docstrings ONLY (module, class, def, and attribute docstrings).
#           A line heuristic was measured and rejected: `^\s*(def |class |return |...)`
#           wrongly keeps 58.6% of code lines and wrongly drops 0.5% of docstring lines,
#           splitting wrapped prose at exactly the sentences FR-001 calls load-bearing.
#   .md  -> fenced blocks, headings and table rows dropped. A table has no sentence
#           terminator, so a whole table flattens into one "sentence" pairing any row's
#           anchor with any other row's universal.
#   then -> collapse `\n\s*` to one space, split on /(?<=[.!?])\s+/, drop len > 700.
#           The cap is an unconditional escape (defeat 4): it owes a stated rationale and
#           deliberate headroom over the 85-398 characters the six true positives occupy,
#           or it must stop being a bare drop.
```

---

## API / Interface Contract

No public API change. `scripts/docs-lint.sh` keeps its contract: exit 0 clean, non-zero with one
line per violation, run by hand before a push.

## Configuration / Environment

None. `docs-lint.sh` already shells to `python3` for its existing checks.

## File & Folder Structure

```
scripts/
├── docs-lint.sh              # + the FR-001 check, + a comment on the population divergence
└── docs-lint-test.sh         # + the FR-002 corpus
tests/docs-lint/
└── marking-walk-*.case       # failure and silence fixtures
src/log_foundry/
└── _lifecycle.py             # FR-003: _FOREIGN's docstring only
docs/
└── component-inventory.md    # FR-003: one row (already compliant; a floor)
```

## Implementation Phases

### Phase 1: The instrument

- Build the *Data Model* pipeline: ast for `.py`, fences/headings/table-rows dropped for `.md`.
- Reproduce the FR-001 table against both trees before writing the check into `docs-lint.sh`.

### Phase 2: The three defeats

- Close defeat 1 (negated universals), defeat 2 (syntactic rather than lexical scoping) and
  defeat 3 (a documented quotation escape).
- Re-run the table; 6 to 0 must survive all three.

### Phase 3: The corpus

- Add `marking-walk-*.case` fixtures, failure cases verbatim from `23fe6cc`.
- Mutation-test every clause and every pipeline step; record which fixture catches each.

### Phase 4: The pointers

- Rewrite `_FOREIGN`'s docstring; confirm it passes FR-001 without evasive wording.
- Re-run the six gates and the `__doc__` assertions in `tests/`.
