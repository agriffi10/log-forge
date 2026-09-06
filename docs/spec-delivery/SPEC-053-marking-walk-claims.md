# Completed Spec — SPEC-053: The Marking Walk's Restated Claims, and the Gate That Would Have Caught Them

## What was completed?

- **Check 11 in `scripts/docs-lint.sh`** — a sentence fails when it names the marking walk
  (`_mark_inherited`, `` `_FOREIGN` ``, "marking walk"), quantifies what the walk acts on with a
  bare universal (`every`, `everything`, `all`), and attaches no restriction to it. Measured on
  trees materialised with `git archive`: **6** violations at `23fe6cc`, **0** at `ac938c0`, **0**
  here. It is the one check in that script that shells to `python3`, because it reads `.py`
  **docstrings and nothing else**, which needs `ast`.
- **`tests/docs-lint/marking-walk-*.case`** — 30 fixtures. Six failure cases verbatim from
  `23fe6cc`, one per pipeline branch that carries them; silence cases for the register's own
  statement of the decision, the negated universal, fences, table rows, headings, a sentence with
  no anchor, and a universal that names the walk without quantifying what it acts on.
- **A `python3` self-test in `scripts/docs-lint-test.sh`** — one fixture tree run twice, clean with
  a working interpreter and red with a shim that exits 127, asserting the did-not-run report.
- **FR-003** — `_FOREIGN`'s docstring states what the sentinel is, keeps one restricted clause for
  what a child lays it over, and defers to `_mark_inherited` for **when**, which is the half that
  docstring actually answers. `docs/component-inventory.md`'s row was already compliant, untouched.

## What changed from earlier specs?

- `scripts/docs-lint.sh` and `scripts/docs-lint-test.sh` (SPEC-050, SPEC-052) lose the header claim
  "no dependencies": check 11 needs `python3`. Both headers now say so and why.
- `src/log_foundry/_lifecycle.py` — `_FOREIGN`'s docstring only. No logic changed; `setdefault` is
  correct and `_mark_inherited`'s own docstring is untouched, being what the others now defer to.
- `CLAUDE.md`'s Key Decisions line for the gate area and its `docs/decisions.md` entry are
  **extended, not appended to** — the area gains "a gate's scoping test binds to what it scopes".

## What the build measured that the spec had wrong

Six spec claims did not reproduce. Each is corrected in the spec file itself; the two that changed
a criterion:

- **Two acceptance criteria were jointly unsatisfiable.** *Report the 6 named sentences* and *make
  the scoping clause syntactic* cannot both hold. `SPEC-050-lifecycle-residue.md:292`'s only
  trigger is `never`, in a clause that survives byte-identically into its own correction and into
  two more correct sentences; what separated them was a `setdefault` elsewhere in the sentence,
  which is the lexical accident the syntactic clause exists to abolish. Its real defect is the
  possessive spelling the spec's Out of Scope already excludes. Re-derived on the shipped
  instrument the count is still six: that sentence drops and `reclaim`'s docstring enters —
  a false universal PR #218 rewrote and the sentence-wide instrument **missed**, silenced by a
  `setdefault` fourteen words later in its own sentence.
- **"The sentence they replaced still fails" cannot hold** — it carries no named anchor, so it can
  never fire, which is the vacuity the spec's own FR-002 rejects elsewhere. The criterion now rests
  on the repair committed at `84c494e`, which carries both defeat-1 mechanisms in one line.

Also corrected: `docs-lint.sh` shelled to `python3` nowhere (every check was `awk`); the null edit
silences the lexical baseline on **6 of 6**, not 5; **four** sentences in the spec file fired, not
five; and defeat 4's 700-character cap is **deleted** rather than justified, on the measurement
that removing it moves no count on any of the three trees.

The spec's *Data Model* now carries **two** rules — the draft's and the shipped one — because the
first diff review found the normative block still describing an instrument that had not shipped:
`any|always|never` are gone from the universal, the about-the-claim clause is not in the draft at
all, the restriction is tested inside the noun phrase and gains the relatives, and `` ``_FOREIGN`` ``
was dead alternation (`` `_FOREIGN` `` is a substring of it). The same review found **two pipeline
steps surviving mutation** — the population's root-`*.md` branch and the list-marker unit split,
both now fixtured — and two clauses that were wrong rather than untested: the negation guard
reached a negator plus one word, which left five of eight ordinary corrections reddening, and
`RESTRICT` carried `it|its`, which are not relatives, so any comma-free continuation carrying "it"
silenced a claim. The separate "at all" guard is **deleted**: it went on passing with the guard
removed, and where "at all" is not idiomatic it is a preposition before a real universal
("looks at all inherited sinks"), which that guard silenced wrongly. None of those four changes
moves the 6 / 0 / 0 table.

## Two things worth carrying forward

- **A scoping test must bind to what it scopes.** Tested sentence-wide, the excusing word is the
  vocabulary of the repair, so the gate goes quiet exactly when someone fixes the defect. Bound to
  the noun phrase the universal quantifies, an insertion anywhere OUTSIDE the quantified noun phrase leaves the sentence firing, and one inside it silences the check by construction — which is what "bound to the noun phrase" means, not a leak in it.
- **A universal must be *about* the mechanism.** Requiring only that a name and a universal
  co-occur fires on "all of the buffer repair happens after it". Adding that clause took the live
  tree from 3 findings to 1.

## Known limits, stated rather than discovered later

- Markdown **table rows are dropped** (a table has no sentence terminator, so one flattens into a
  single "sentence" pairing any row's anchor with any other row's universal). So
  `docs/component-inventory.md`'s row — the third restatement site, and the one still wrong after
  PR #218's fourth round — is the one place this gate cannot police.
- An overt relative pronoun excuses a universal, so "marks everything **that** the child inherited"
  would pass while being as false as the sentence it replaces. The check catches the **bare**
  universal, which is the one spelling that recurred.
- The escape `docs-lint: marking-walk` is a magic word by design — explicit and greppable, so
  unlike `setdefault` nobody reaches it while paraphrasing — but it is the one clause an author can
  reach for instead of fixing a claim. It covers the unit that carries it and no other, and a
  fixture pins that. A consequence worth knowing: any unit that merely **names** the literal is
  silenced, so this decision's own register entry and this document are outside the gate. Both were
  checked to be silent without it.
- An overt relative pronoun is not the only excuse a paraphrase can reach: `reach`, `missed`,
  `residual`, `partial`, `setdefault` and `item 7` still excuse a universal when they sit **inside**
  its noun phrase. That is the bound, not a leak — the null edit that defeats a sentence-wide test
  cannot reach inside the phrase without changing the claim — but it is the surface to watch.

## Verification

Six gates green locally, on exit codes rather than summary lines: `ruff`, `mypy --strict`, `pytest`
(2552 passed, 10 skipped), `spec-lint.sh`, `docs-lint.sh`, `docstring-lint.py`. `docs-lint-test.sh`:
116 cases, the three fixture guards, and the interpreter self-test. **Every clause and every
pipeline step is mutation-tested and each mutant reddens a *named* fixture; none survives.** Three
survived along the way and all three were fixtures that could not fail — a silence case subsumed by
a clause beside it, a wrapped case asserting only a FAIL line and never the collapsed sentence, and
the "at all" guard, whose survival was the evidence that deleted it.

`CLAUDE.md` closes at 35,970 of its 36,000-byte budget. That is 30 bytes, which is the state the
budget's own comment calls the mistake — the next spec to settle a decision cannot close without
pruning another area's fences. The wave the fence was sized for has landed, so the rule says
re-derive rather than re-check; that is a cut, and it is not this spec's.
