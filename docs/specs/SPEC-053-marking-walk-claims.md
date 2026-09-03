# Spec: The Marking Walk's Restated Claims, and the Gate That Would Have Caught Them

**ID:** SPEC-053
**Status:** Draft
**Last Updated:** 2026-09-03
**Depends On:** SPEC-042, SPEC-052

## Overview

`_lifecycle._mark_inherited()` is described in prose at sixteen files across `src/`, `docs/` and
`tests/`, and for most of the project's life a large share of those descriptions were wrong in the
same direction: they said it stamps **everything** a forked child inherited `_FOREIGN`. It
`setdefault`s, so a sink `configure()` already stamped keeps the parent's real pid and `_FOREIGN`
lands only where nothing was recorded. PR #218 rewrote 27 hunks across 12 files over four rounds
and three fresh-context review frames, and each round found defects in the previous round's fixes —
including two in `src/` that all three frames read past, and one where `architecture.md` §13's own
header contradicted item 7 of the list three lines beneath it.

The corrections are shipped. What is not addressed is why one mechanism needs sixteen files'
worth of description to begin with, and why nothing mechanical noticed that so many of them
disagreed with the code and with each other for that long. This spec closes that: it puts a gate in front of the
spelling that recurred, and it removes the two restatements in `src/` that have no reason to exist
separately from the authoritative one. It deliberately leaves the specs and the delivery doc alone,
because those are historical record and rewriting them to match today's code destroys the thing they
are for.

## Scope

### In Scope

- A check in `scripts/docs-lint.sh` that fails a sentence carrying a **universal quantifier** in the
  same sentence as a **named reference to the marking walk**, unless that sentence also carries a
  scoping term. Measured against the tree this spec is written on (`ac938c0`) and the pre-fix tree
  (`23fe6cc`) — see FR-001.
- A fixture corpus in `scripts/docs-lint-test.sh` proving the check fires **and** proving it stays
  silent on the correct normative phrasings, which are the majority.
- Pointer-ising the **two** `src`-side restatements — `docs/component-inventory.md`'s one-line
  description and `_FOREIGN`'s own docstring — onto the single authoritative statement, so there is
  one place to correct rather than three.

### Out of Scope

- **The other two spellings of the same false claim, and this is measured rather than assumed.**
  The contrapositive ("an unmarked sink is claimable") and the possessive ("the `_FOREIGN` stamp
  `_mark_inherited` set") mostly carry no universal quantifier, so FR-001's check cannot see them.
  It fires on 6 sentences at `23fe6cc` while PR #218 touched 27 hunks, so **most of the population
  is outside this check's reach and stays outside it.** Widening it was tried and
  rejected in FR-001's evidence: the anchor that reaches those spellings is the anchor that
  produces 16 false positives. A gate that catches one spelling reliably is worth more than one
  that catches three and gets switched off.
- **Rewriting `docs/specs/` or `docs/spec-delivery/`.** A spec records what was decided and a
  delivery doc records what shipped, both at a past moment. `spec-delivery/SPEC-042` deliberately
  narrates the pre-fix state, and this repo has already been bitten by editing a spec's past-tense
  narration into false history.
- **Collapsing the restatements in `tests/`.** Five of the sixteen files are test modules, whose
  prose is the reasoning for that specific test rather than a description of the mechanism.
  Replacing it with a pointer costs a reader the argument at the point of use. The other eleven
  split 9 `docs/` and 2 `src/`, and only the `src` pair is in scope here (FR-003).
- **Enforcing the check in CI.** `docs-lint.sh` is deliberately a local pre-push gate; promoting it
  is a separate decision owned by whoever owns `.github/`.
- **Any change to `_lifecycle.py`'s logic.** `setdefault` is correct and PR #218 changed no source
  logic; neither does this.

---

## Functional Requirements

### FR-001: A check that fails a universal claim about the marking walk

#### Description:

A sentence is a violation when it carries all of: a **named** reference to the marking walk
(`_mark_inherited`, `` `_FOREIGN` ``, or the phrase "marking walk"), a **universal quantifier**
(`every`, `everything`, `all`, `any`, `always`, `never`), and **no scoping term** (`reach`,
`missed`, `residual`, `§13`, `partial`, `setdefault`, `must be`, `has to be`, `item 7`).

Three properties are load-bearing and each is a measured choice, not a preference:

**Sentences, not lines.** Prose here wraps at 100 characters, so a claim longer than a few words is
routinely split across a line break at whatever column the wrap falls. Four line-based sweeps ran
during PR #218 and each missed what the next found; the sentence-level one — wraps reassembled
before matching — found `architecture.md` §13's header, which three review frames had read past.
The check reassembles `\n\s*` to a space before splitting on sentence boundaries.

**A named anchor, not a descriptive one.** Measured on `ac938c0`, with the scoping term ignored so
the raw ratio is visible:

| anchor | fires on a clean tree |
|---|---|
| descriptive (`a forked child`, `it inherited`, `inherited sink`, …) | **16** |
| named symbols only (`_mark_inherited`, `` `_FOREIGN` ``, `marking walk`) | **2** |
| named symbols, code excluded | **1**, and that one is an FR heading |

The descriptive anchor is where a gate dies. There are 46 sentences on this tree carrying an anchor
and a universal; 23 already scope themselves and 23 do not, and of those 23 exactly **zero** are
defects, because PR #218 fixed the last two. A check at 21-plus false positives to zero true ones is
a check somebody deletes within a week — which is the same failure `CLAUDE.md` records as "half a
gate's regressions are false positives".

**Code excluded, and headings excluded.** Fenced blocks in Markdown and code lines in `.py` are
dropped before matching, because flattening them manufactures sentences that never existed. A
Markdown heading is dropped because `### FR-004: Every reader of the record …` is a title, not a
claim.

#### Acceptance Criteria:

- [ ] On the tree at `ac938c0` the check reports **zero** violations, so it ships green rather than
      shipping with a waiver for its own findings.
- [ ] On the tree at `23fe6cc` — the commit before PR #218 — the check reports **6** violations,
      distributed 3 in `src/log_foundry/_lifecycle.py` (`_FOREIGN`'s docstring, `_MARKING_CEILING`'s
      summary line and `releasable`'s paragraph), 1 in `docs/decisions.md`, 1 in
      `docs/specs/SPEC-050-lifecycle-residue.md`, and 1 in `tests/test_fork_lifecycle.py` that is a
      **flattening artifact** — a docstring running into the code below it — which the implementer
      should either eliminate by tightening the code-exclusion or accept and record. A check that
      cannot redden against the defect it was built for is not evidence of anything.
- [ ] The check names the file, the line the sentence starts on, and the offending sentence, so the
      failure is actionable without re-running a search by hand.
- [ ] It runs inside `scripts/docs-lint.sh` and is covered by that script's existing exit status —
      no new command for a contributor to remember.
- [ ] Removing the scoping-term clause makes the check fire on the tree at `ac938c0`, proving that
      clause is load-bearing rather than decorative.

### FR-002: A fixture corpus that proves the check fires, and proves it stays silent

#### Description:

`scripts/docs-lint-test.sh` already carries `.case` fixtures for the dated-measurement checks and is
where this belongs. Running the check against the repository it guards proves the repository passes,
not that the check works — so the corpus must assert the failure text on planted violations and must
assert **silence** on the phrasings that are correct.

The silence cases matter more than the failure cases here, because they are the majority: 23 of the
46 anchored sentences on `ac938c0` are correct normative statements — "unrecorded **must be**
unclaimable, not merely unreleasable" is the settled decision's own wording and appears in
`CLAUDE.md`, `docs/decisions.md` and `architecture.md` §9. A check that reddens on the decision
register's statement of the decision is worse than no check.

#### Acceptance Criteria:

- [ ] Each of the three clauses (anchor, universal, scoping term) is mutation-tested: breaking it
      alone makes at least one fixture fail, and the corpus says which. `CLAUDE.md` requires this of
      every check in a gate, not a sample.
- [ ] A failure fixture for each of the five real sentences the check catches at `23fe6cc`, taken
      verbatim from that tree rather than invented, so the corpus is a regression record.
- [ ] Silence fixtures for: the normative "must be unclaimable" wording; a sentence scoped with
      "the walk reaches"; a universal inside a fenced code block; a universal in a Markdown heading;
      and a universal in a sentence with no marking-walk anchor at all.
- [ ] A fixture pinning the wrap behaviour: one violation split across a line break is caught, which
      is what distinguishes this check from the four line-based sweeps that missed it.

### FR-003: One authoritative statement in `src`, and pointers to it

#### Description:

Three places describe what `_mark_inherited` does for a reader of the shipped library:
`_mark_inherited`'s own docstring, `_FOREIGN`'s docstring, and `docs/component-inventory.md`'s
one-line row. The first is authoritative — it sits on the function and its `Raises:` block already
carries the partial-walk residual. The other two restate it, and both were wrong at some point
during PR #218: `component-inventory.md` still said "making inherited sinks unclaimable" after that
claim had been removed from the summary line it was written to match, in the one file no commit on
the branch had touched.

This is the narrow, defensible half of "collapse the restatements". It does not extend to `tests/`
or to the specs, for the reasons under *Out of Scope*.

#### Acceptance Criteria:

- [ ] `_FOREIGN`'s docstring states what the sentinel **is** and defers to `_mark_inherited` for
      when it is written, rather than restating the write.
- [ ] `docs/component-inventory.md`'s row describes `_mark_inherited` by its role in one clause and
      makes no claim about which record a given sink ends up carrying.
- [ ] `_mark_inherited`'s docstring is unchanged by this FR — it is the statement the others defer
      to, and PR #218 already corrected it.
- [ ] FR-001's check passes over the result, and the docstring assertions in `tests/` still pass:
      `grep -rn '__doc__' tests/` names sixteen sites, one of which reads
      `_lifecycle.releasable.__doc__`.

---

## Data Model

```python
# scripts/docs-lint.sh — a shell gate; the check is a POSIX-awk or python3 filter beside
# the dated-measurement check, over the files git-grep names.

ANCHOR   = r"_mark_inherited|``_FOREIGN``|`_FOREIGN`|marking walk"
UNIVERSAL= r"\b(every|everything|all|any|always|never)\b"
SCOPED   = r"reach|missed|residual|§13|partial|setdefault|must be|has to be|item 7"

# violation(sentence) := ANCHOR and UNIVERSAL and not SCOPED
# sentences are produced AFTER: fenced blocks dropped (.md), code lines dropped (.py),
# `\n\s*` collapsed to a single space, split on /(?<=[.!?])\s+/, headings dropped.
```

---

## API / Interface Contract

No public API changes. `scripts/docs-lint.sh` keeps its current contract: exit 0 clean, non-zero
with one line per violation, run by hand before a push.

## Configuration / Environment

None. No new dependency — `docs-lint.sh` already shells to `python3` for its existing checks.

## File & Folder Structure

```
scripts/
├── docs-lint.sh              # + the FR-001 check
└── docs-lint-test.sh         # + the FR-002 corpus
tests/docs-lint/
└── marking-walk-*.case       # + failure and silence fixtures
src/log_foundry/
└── _lifecycle.py             # FR-003: _FOREIGN's docstring only
docs/
└── component-inventory.md    # FR-003: one row
```

## Implementation Phases

### Phase 1: The check

- Add the FR-001 filter to `scripts/docs-lint.sh`, sentence-level with code and headings excluded.
- Verify the two anchors against both trees: 0 violations at `ac938c0`, 6 at `23fe6cc`.
- Confirm the scoping clause is load-bearing by removing it and watching the clean tree redden.

### Phase 2: The corpus

- Add `marking-walk-*.case` fixtures to `scripts/docs-lint-test.sh` — failure cases taken verbatim
  from `23fe6cc`, silence cases for the normative wording, fences, headings and unanchored text.
- Mutation-test each of the three clauses and record which fixture catches each.

### Phase 3: The pointers

- Rewrite `_FOREIGN`'s docstring to define the sentinel and defer to `_mark_inherited`.
- Rewrite `docs/component-inventory.md`'s row to one role clause.
- Re-run the six gates and the `__doc__` assertions in `tests/`.
