# Authoring a spec

Specs are written from `docs/templates/spec-template.md`. What makes a spec *buildable*:

- **Overview** — user/business intent, no implementation detail. Understandable cold.
- **Scope: In / Out** — explicitly list what's *excluded*, especially anything a reader would
  reasonably assume is included.
- **Functional Requirements** — one FR per discrete, testable behavior, with binary pass/fail
  **Acceptance Criteria** covering happy path, error path, and edges, and naming the
  invariant(s) the FR serves from `docs/invariants.md` by number, so the reviewer knows which
  promise to check on every twin path. `scripts/spec-lint.sh` fails a Draft or In Progress spec
  whose FR cites none, or cites a number the page does not have; an FR that keeps no invariant —
  prose, lint, hygiene — says so with the exact phrase `serves no invariant`, and the spec
  reviewer accepts or rejects that as they do the FR ceiling. Completed specs are exempt because
  they predate the page. The spellings are in the template. Sequential IDs so a prompt can
  say "implement FR-001 through FR-003 only."
- **Size: aim for 3–6 FRs, and split above 8.** A spec is one coherent slice of behavior, not a
  feature's whole surface, and a spec past eight is not a big spec — it is a spec that should have
  been two. (Measured once, over `docs/specs/` as it stood at `0fc94b9`, where this rule landed:
  the median spec carried 5 FRs and 38 of 43 carried 8 or fewer. An observation about a frozen
  tree, not a standing claim about today's corpus — `completion-ritual.md` forbids a rule resting on a number that
  rots, and the anchor is what makes this one re-measurable rather than merely dated.) Growing one
  spec is the wrong repair;
  the right one is a second spec beside it, with the pair's build order recorded in `INDEX.md` as
  an **arc** (`spec-lifecycle.md`). Splitting keeps paying off past the
  first cut — a three-spec arc reads better than one twelve-FR spec, and each piece earns its own
  reviewed plan, its own review gate and its own delivery doc. Cut along a seam the system already
  has — a layer, a surface, a switchover, the point where something inert goes live — never at
  "FR-009, because that is where the count ran out." **The second spec restarts at FR-001**: IDs are
  spec-local, which is why a dependency cites them as "SPEC-035 FR-002" and never as a number on
  its own.
- **The ceiling is a warn, not a fail.** A genuinely indivisible spec can sit above 8, and the lint
  warns rather than blocking so that it can. That call is made **out loud** — one line under
  *Scope → In Scope* saying why the FRs cannot be cut apart — and it is the reviewer's to accept or
  reject, not the author's to assert. "It is all one feature" is the claim to be most skeptical of,
  because it is what every over-scoped spec says about itself.
- **Data Model / Interface Contract** — language-native types, not prose. Explicit shapes produce
  better-typed output. Note the target path.
- **Implementation Phases** — each phase is one session's worth of work and maps to a discrete,
  reviewable unit. Phases are the input to the implementation plan generated at build time (`session-rhythm.md`); don't
  bake per-phase checkpoints into the spec.
- **No Open Questions.** Resolve every decision while authoring — a spec doesn't reach Draft-ready with
  unanswered questions. Issues that only surface during the build are handled in-session (`session-rhythm.md`), not
  parked in the spec. A sentence that *promises* a decision is an Open Question in declarative
  clothes (`session-rhythm.md`).
- **Then the reviewer gate** (`reviewer-contract.md`). A freshly-authored spec goes to a
  fresh-context reviewer before it is Draft-ready, and its findings are fixed or flagged.
  The commonest thing this catches is not a wrong requirement — it is an acceptance criterion that
  cannot fail, and an Out of Scope bullet that an FR quietly needs.

**An acceptance criterion is a pass/fail test, not an argument for itself.** Measured on this
repo: the specs with short criteria took 2–3 review rounds, and the two with the longest took 8 and
11. The extra prose did not buy correctness — SPEC-033 shipped two regressions to `main` after
eight rounds. ~~SPEC-029 and SPEC-030 carried 19–25 criteria averaging ~20 words … SPEC-033 and
SPEC-036 carry 48–57 averaging ~95~~ — struck: a standing rule must not cite volatile numbers (`completion-ritual.md`),
and these rotted the moment SPEC-036 was right-sized before its build. So:

- **Keep the criterion itself to a sentence or two.** If a decision needs a paragraph to justify,
  the paragraph belongs in the FR's Description, where a reader meets it once, not inside a
  checkbox they re-read every session.
- **Provenance goes at the bottom, not inside the requirement.** "An earlier draft said X and was
  measurably wrong" is genuinely valuable — SPEC-021's rule is that a superseded decision is
  struck in place, never deleted — but it is *history*, and `INDEX.md` says the builder reads the whole
  spec every session. Put it under a `## Revision history` heading, or in the delivery doc, and
  leave a one-line strike-through where it was.
- **Don't cite another unbuilt spec's AC number.** SPEC-034, 036 and 037 cited each other's
  criteria 21 times, to the point where one AC had to disclaim its own circularity in writing.
  Cross-spec coupling is a sequencing decision; make it in `INDEX.md`'s build order, where it can
  be read in one place and changed in one place.
- **Reserve mutation testing for assertions that guard a fixed defect.** "Every assertion is
  mutation-tested" applied to a README-wording criterion multiplies the work without protecting
  anything. The rule that earns its keep is narrower: *a test that claims to catch a bug must be
  run against that bug.*

`scripts/spec-lint.sh` enforces the structural side of this in CI: it **fails** a spec that is missing
a required section, one that contains an "Open Questions" / "Checkpoint" heading, and a Draft or
In Progress spec whose FR names no invariant in its Acceptance Criteria — or has no such block at
all — or names one `docs/invariants.md` does not number; it **warns** on unfilled placeholders, a
spec with FRs but no acceptance criteria anywhere in it, and a spec carrying more than 8 FRs. It
cannot see a vacuous acceptance criterion, a citation of the wrong invariant, or a decision
promised in a declarative sentence — that is what the reviewer gate is for.
`scripts/spec-lint-test.sh` is its fixture corpus, run in CI beside it.

