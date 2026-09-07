# Completion ritual (keep the always-loaded tier lean)

When a spec is done, in the same pass:
1. Set the spec file header `Status: Completed`.
2. Update its one-line row in `docs/specs/INDEX.md` (**status only** — no prose).
3. Write a **short** delivery doc at `docs/spec-delivery/SPEC-XXX-<name>.md` from
   `docs/templates/spec-completion-template.md` — *what shipped + what changed*, aimed well under a
   page, **no code/config pasted** (the code + component-inventory are the source of truth for
   reuse). `DELIVERY_MAX_LINES` in `scripts/docs-lint.sh` is a ratchet set by one frozen historical
   doc, not a target: it means nobody may lengthen that doc, never that a new one may run to it.
4. If reusable modules/services/components were added, add a **one-line** row to
   `docs/component-inventory.md`.
5. A *new architectural decision* gets its **full `###` entry in its area file under
   `docs/decisions/` first, plus a row in that file's `## Contents`** — an entry the Contents does
   not reach is findable only by reading the whole file, and `scripts/docs-lint.sh` fails for it
   locally, before the push. Then, and only then, **one fence** under that file's `## Fences`: the
   claim and its constraint in one line, its bold label equal to the entry's heading. Entry first,
   fence second: the fence is **never the only home of a fact**, never a paragraph — past
   `DIGEST_MAX_BYTES` it has stopped being a reminder and become the reasoning, which belongs behind
   it — and **never a line in `CLAUDE.md`**. Only a new *area* touches `CLAUDE.md`: a row in its Key
   Decisions table, mirrored — with its *Governs* column — in `docs/decisions/INDEX.md` in the same
   order, a file copied from an existing area's, and — only when the area governs a tree — a
   `.claude/rules/decisions-<slug>.md` carrying exactly the globs *Governs* names. An area whose
   *Governs* is `none` has no rule, and docs-lint refuses one. A reversal that changes an entry's **heading** must move three
   copies of it with the heading — its Contents row, its fence label, and the entry's own opening
   bold label — or the row points at a dead anchor, the label still names the old decision, and
   docs-lint fails. If the decision **supersedes an earlier one**, update the old entry in place and
   add a superseded marker (short blockquote: what changed, which spec, where the full entry lives)
   at every *other* doc site that still states the old claim — `architecture.md` sections, `INDEX.md`
   build-order notes. The new entry alone is not enough; an agent reading only the old site must see
   the reversal.

**Anti-regrowth & doc hygiene** (each rule below was earned by a real doc defect in a project run
this way).

- If a memory or doc disagrees with the code, fix or delete it — don't let stale state accumulate.
  Don't add prose to the always-loaded tier.
- **Documentation lives beside the code it describes.** A page named for a source file, a module or
  a directory belongs in that tree, not in `docs/`; `docs/` carries what spans trees or belongs to
  no tree. The test is where a reader is standing when they need it — a doc they must leave the tree
  to find is a doc they will not open.
- **One organising axis per subject, and one doc that owns it.** If a subject is documented per-sink,
  exactly one place is per-sink. A second doc on the same axis is not redundancy, it is a fork with
  no merge — and the two will already have diverged by the time anyone notices.
- **A register is grouped by AREA; ordering it by spec number turns it into a changelog.** The
  question a reader arrives with is "what has been settled about X", never "what did SPEC-033
  decide". A register is the only home of the rejected alternatives and the fences, so a shape that
  reads as disposable gets treated as disposable. **Paid 2026-09-02, and routed since:**
  `docs/decisions/` holds the entries, one file per area, each opening with its own fences; the Key
  Decisions table in `CLAUDE.md` names the areas and nothing more. A completion **extends its area's
  file** rather than appending to the always-loaded tier — appending is what took `CLAUDE.md` from
  7,350 bytes at `ad898fc8` to 89,340 at `e60b60d`, with this rule stated here throughout. Both ends
  are anchored so a reader can re-measure them; neither the entry count nor the area count is stated
  here, because both move every time a spec closes.
- **`scripts/docs-lint.sh` enforces the structural half of these rules, and is a LOCAL PRE-PUSH
  gate — deliberately not a CI job.** Keeping it local puts the failure in front of the person who
  caused it, while they can still fix it silently, rather than on a shared branch where it reds
  someone else's unrelated work and becomes a thing to be waived. It holds the always-loaded set — `CLAUDE.md` and
  the two files the router's *Loaded every session* table names — to a byte budget, and holds Key
  Decisions to an intro and one area table, refusing every other construct in that section; requires
  `docs/decisions/INDEX.md` to name the same areas in the same order, every area to have a file and
  every file a row; holds each area file to fences-first, each **fence** — a bullet with its
  continuations — to a length, a `###` entry for every fence and a fence for every entry, every entry
  listed in its Contents under a name that matches the anchor beside it, and an entry's opening bold
  label — where it has one, below any superseded marker — equal to its own heading; holds
  `docs/process/` to its router (imports match the loaded table, every part a row and every row a
  file, no nested imports), and `.claude/rules/` and `.claude/agents/` to their shapes — the two rule
  templates, the allowed glob forms, the allowed models, the routing table; requires every Completed
  spec to have a delivery doc; checks that the pointers out of the always-loaded files resolve; and
  refuses one
  shape of unanchored evidence — "measured" or "as of" followed by an ISO date, immediately or
  across one adverb of time from a closed set, with no commit SHA in the same bullet, paragraph or
  comment block. That is a
  sub-shape, not the rule: an undated volatile number is invisible to it and stays this file's
  to catch by reading. It reads source files as well as markdown, because the rule three bullets
  down says a markdown-only sweep reports the tree clean — and two of the eight sites that
  prompted the check were `pyproject.toml` comments. **The population, the exclusions and the
  reasons for each live beside the check in the script and nowhere else**, so that the list a
  reader trusts is the one the code uses; a population restated here is a population that will
  disagree with itself.
  **`scripts/docs-lint-test.sh` is the corpus that proves those checks still fire** — running the
  linter against the repo's own documents proves the documents pass and nothing about whether any
  check works, which is how four rounds of regressions reached main. A change to the linter runs
  the corpus.
  **A threshold can be invalidated by its own success, not only by being wrong.** A cap
  catches things until a cut shrinks everything below it, after which it sits far above anything it
  governs and can never fire — still advertised as a fence, and now not one. After any structural
  change to what a threshold measures, **re-derive it rather than re-checking it**. Measured against
  the Key Decisions section as it stood at `e60b60d`, which is frozen and re-measurable:
  `DIGEST_MAX_BYTES` was 1400 and caught nine of that section's 48 units; the cut took the longest
  unit to 550 bytes at `74c928d`, leaving the cap at more than twice the worst thing it governed
  until it was re-derived to 800. The opposite error is as tempting — a budget pinned at the
  post-cut measurement leaves the next decision to settle paying for itself out of another area's
  fences. **The delivery cap is a ratchet at the
  measured level** — when one fires, move detail down a tier and re-ratchet, rather
  than raising the cap. The two byte budgets — `CLAUDE.md` alone, and the whole
  always-loaded set the router's *Loaded every session* table names — are deliberately NOT pinned at
  their measurements: each carries headroom, because a budget with none forces the next closing spec
  to prune another area's fences to buy room for its own, which is the gate causing the damage it
  exists to prevent. How much
  headroom, and derived from what, is stated in `scripts/docs-lint.sh` beside the constant and
  nowhere else — a number restated in two files is a number that will disagree with itself, which is
  how the byte pair above came to be written two ways. Every rule it checks was already written here, and every one was violated
  anyway; that is the argument for a script over a paragraph.
- **When a doc moves, the pointers that rot unseen are in SOURCE files** — `.py` docstrings, `.toml`
  comments, `.yml` steps. A markdown-only sweep reports the tree clean. Grep the path, not the
  filename, and fix the Draft specs too: a Draft is an unbuilt instruction, and pointing one at a
  deleted file sends the next builder nowhere.
- **A doc's own statement of when to read it must agree with CLAUDE.md's.** This file told readers it
  was read once and on demand while it was the contract CLAUDE.md only summarised. Both are cheap to
  write and neither is checked, so they drift silently and the reader follows the wrong one.
- **Status never appears in the heading of a doc whose status can change** — an arc, an
  `architecture.md` section, a register entry. It rots the day the next spec lands, and a reader who
  greps the heading gets an answer that was true once. Status lives in `INDEX.md` and the spec
  header — the two places the completion ritual keeps in step **by hand**, since `spec-lint.sh` does
  not compare them. (A delivery doc's `# Completed Spec — …` title is not this: it names a finished
  record whose status cannot change.)
- **A heading in a doc read by SUBJECT names the subject, not the spec that produced it** — that is
  `architecture.md` and the rulebooks, where a reader arrives asking "how does the worker shut down",
  never "what did SPEC-030 decide". The scope is deliberate and stops there: a decisions entry IS a
  record of what one spec settled, and its number is part of its identity when you arrive from a
  delivery doc or a superseded marker, so those headings keep theirs.
- **Standing rules never cite volatile numbers** (line counts, row counts, section ranges) — state the
  principle. The numbers rot, and a rule resting on false evidence teaches readers to distrust it.
  **Dating the measurement does not save it**, which is the tempting half-fix: a dated number still
  reads as current to anyone not checking the date against the calendar. Measured here: the
  `architecture.md` §12 entry added by `dcb07c3` justified leaving two modules unsplit by citing
  their line counts and the date it measured them, and argued in as many words that the date was
  what made the numbers safe. `a58dfff`, the very next commit, moved one of the two files; by
  `3c973b9` both counts were wrong, and the date was what made that invisible — a reader meets a
  dated measurement and reads it as current rather than as history to check. Either delete the
  number and state the principle, or anchor both ends of the evidence to a commit a reader can
  re-measure from. Anchoring is also what a reader can *check*: a date says when someone looked, a
  SHA says at what. `scripts/docs-lint.sh` holds the standing tier to the second half of that —
  it fails a dated measurement carrying no commit to re-measure from — but only for the
  `Measured <date>` / `as of <date>` idiom every site of this defect actually used. A number with no
  date at all is still this bullet's to catch, not the script's.
- **A rule practice consistently violates gets reconciled or deleted.** A dead rule trains agents to
  ignore the live ones.
- **Routers and indexes carry only what self-describes.** Hand-maintained metadata (symbol counts,
  "§1–§N" ranges) rots silently; drop it or let the structure carry the information.
- **Any doc pulled entry-by-entry gets one heading per entry plus a TOC**, and pointer phrases in
  other docs must match a greppable heading — "read the entry for your area" must be a jump, not a
  full-file read.
- **Live findings and obligations never live in historical or cancelled narrative** — rehome them to
  the right `docs/decisions/` area file (entry first, then its fence) or the relevant
  `architecture.md` section, and leave a pointer behind.
  `docs/audits/` is history: a live obligation parked in a handoff doc is one nobody will read.

