#!/bin/sh
# docs-lint.sh — hold the ALWAYS-LOADED tier to the shape the layering assumes.
#
# Why this exists as a script rather than a rule. Every check below already existed in
# prose — in `docs/process.md` §5 (*Anti-regrowth & doc hygiene*), in `CLAUDE.md`'s own
# doc-size guardrail, and in `docs/decisions.md`'s rules header — and several were
# violated here anyway. The history says something sharper than "a rule was ignored".
# `CLAUDE.md` grew from 7,350 bytes at `ad898fc8` to 89,340 at `e60b60d`, more
# than tenfold, and for most of that `docs/process.md` carried only a TWO-SENTENCE version
# of the rule, naming no shape, no register and no budget. The full set arrived at
# `690d2a55`, days before the cut, named the violation in the present tense and
# correctly — and the file grew by nearly a third again anyway. Both ends are anchored
# rather than restated: an earlier version of this comment carried three numbers and two
# were wrong by the time it shipped.
#
# A rule a reader has to remember is a rule that rots. This is the same rules where a
# script can see them — run before every push, deliberately not in CI, so the failure
# lands on whoever caused it rather than on a shared branch.
#
# FAIL (exit 1): the always-loaded file is over budget or has been removed outright, a
#   Key Decisions unit has become the reasoning, the register is missing or has inverted
#   with its digest, an entry is unreachable from the Contents, a Contents row names one
#   decision and links to another, an entry body opens with a bold label its own heading
#   does not match, a Completed spec has no delivery doc, a delivery doc has become an
#   essay, a pointer out of CLAUDE.md goes nowhere, or a standing document dates a
#   measurement without anchoring it to a commit.
#
# There is no WARN tier: `spec-lint.sh` owns the soft per-spec judgements, and every rule
# here is a shape the layering depends on — a shape is either held or it isn't.
# Deliberately NOT checked here: anything `spec-lint.sh` already owns (required spec
# sections, banned headers, the FR ceiling). A rule with two enforcement homes gets
# qualified in one of them and read from the other.
#
# Usage: sh scripts/docs-lint.sh          (run from anywhere; resolves its own root)
# POSIX sh — no bashisms, no dependencies; runs anywhere /bin/sh exists.
#
# NOTE for maintainers: the awk programs below are single-quoted. An apostrophe anywhere
# inside one — including in a comment — closes the quote, and the shell then parses awk
# source as shell. That failed *silently with status 0* once during authoring, which is
# why `scripts/docs-lint-test.sh` runs `sh -n` on this file before anything else.

set -eu

# Both byte budgets rely on awk length() counting BYTES. It does in the one-true-awk
# that ships on macOS, but gawk in a UTF-8 locale counts CHARACTERS — every em dash in a
# digest would then count 1 instead of 3, so the caps would measure something different
# in CI than they do locally. C locale makes it bytes everywhere.
LC_ALL=C
export LC_ALL

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# ── Budgets ────────────────────────────────────────────────────────────────────
#
# RATCHETS AT THE MEASURED LEVEL, not targets. When a doc grows past one, the fix is
# to move detail down a tier — into `docs/`, behind a pointer — which is the entire
# reason the budget is here. Lowering the bar to fit the edit is the failure mode, and
# it is how `CLAUDE.md` reached 89 KB one justified exception at a time.
#
# CLAUDE_MAX_BYTES is set deliberately and is the one budget here NOT pinned at the
# measurement. The file was cut to a digest over docs/decisions.md on 2026-09-02, and a
# ratchet at what it then measured left about two digest lines of room.
#
# s3-upload-portal already paid for that mistake and recorded it in its own
# scripts/validate_docs.py: its budget sat at
# 34,000 with 161 bytes free, so the next spec to settle a decision could not close
# without pruning another area's fences to pay for its own — the gate causing the exact
# damage it exists to prevent. A ratchet at the measurement assumes the file only grows
# by accretion, and after a structural cut that is no longer true: what remains is
# fences, and a genuinely new architectural decision folds a clause in beside them.
#
# ~~34,000 … If this file approaches 34,000 the answer is another cut, not another
# raise.~~ — superseded 2026-09-02 (SPEC-050). That sentence was right about the failure
# mode and wrong about the premise under it, which was stated one line earlier: "most
# closing specs add nothing here". True while specs land one at a time. The pre-1.0 audit
# is being closed by five specs running in PARALLEL, several settling genuinely new
# architectural decisions, so the wave folds in several clauses between one cut and the
# next — and the file reached 33,691 with 309 bytes free, one digest line, which is the
# state the paragraph above calls the mistake. What changed is the RATE, not the rule:
# process.md §5's "a threshold can be invalidated by its own success" is about exactly
# this, and it says re-derive rather than re-check.
#
# So the rule that survives is sharper than the one it replaces. A raise is legitimate
# ONLY when the rate the fence was sized against has changed, and it must say which wave
# it was sized for. A raise to fit the edit in hand is still the failure mode, and it is
# still how CLAUDE.md reached 89 KB one justified exception at a time.
#
# 36,000 is derived, not chosen: 33,691 measured on main at 74c928d, plus ~2,300 for the
# in-flight wave (SPEC-050 +410 and SPEC-051 +636 measured; two more specs unmeasured at
# roughly the same size). It is not a per-spec allowance. When this wave has landed the
# number is re-derived DOWNWARD against what the file then measures — a cap that can no
# longer fire is not a fence, which is the other half of the §5 rule.
CLAUDE_MAX_BYTES=36000

# The whole Key Decisions section, measured as bytes. This is the guard that cannot be
# evaded by reformatting: a per-bullet cap is escaped by splitting one decision into
# five, and every shape that escaped the old parser still costs bytes here.
# (measured after the 2026-09-02 cut, with working room)
KEY_DECISIONS_MAX_BYTES=22000

# The longest a single Key Decisions unit may be. Measured on the LOGICAL unit — a
# bullet with its continuation lines joined, or a prose paragraph — because measuring
# the physical line looks equivalent and is not: the moment the section is rewritten as
# wrapped prose the longest physical line collapses to the wrap width, and the guard can
# never fire again while still being advertised in process.md. A PROSE PARAGRAPH COUNTS
# AS A UNIT, and that is the point: keying only on bullets left the section rewritable
# as prose to escape both this cap and the register cross-check below.
#
# BYTES, not characters: awk length() is byte-based in the one-true-awk that ships on
# macOS, so em dashes and smart quotes count for more than one.
#
# 800 rather than the inherited 1400. The important part is WHY 1400 stopped working,
# because the obvious reading is wrong. It was not miscalibrated: against the pre-cut
# section it caught a substantial share of the units and was a working fence. What
# floated it was the cut SUCCEEDING — shrinking every unit well below the cap left it
# sitting several times above anything it governed, so process.md went on advertising
# as a fence the one check that could no longer fire.
#
# **A fence can be invalidated by its own success, not only by being wrong**, and this
# one has the same exposure: 800 clears today's worst unit with room to spare, so ANY
# FUTURE STRUCTURAL CUT of Key Decisions must re-derive this constant rather than
# merely check it. A cut that leaves this number alone hands the next reader a fence
# that passes everything.
#
# Deliberately carries NO count of what it catches. The evidence is the Key Decisions
# section as it stood at e60b60d, which is frozen and can be re-measured by anyone who
# wants to check the claim. A cap comment is read exactly when someone is about to
# change what it counts, and process.md §5 forbids a standing rule citing a volatile
# number for that reason: the architecture.md §12 entry added by dcb07c3 justified
# leaving two modules unsplit by citing their line counts and the date it measured them.
# a58dfff, the very next commit, moved one of the two; by 3c973b9 both were wrong. Dating
# the measurement did not save it — a dated number still reads as current. Check 9 below
# is that lesson with a gate on it.
DIGEST_MAX_BYTES=800

# A delivery doc answers "what shipped and what changed"; the completion template aims
# for under ~40 lines. Applies to every *.md in docs/spec-delivery/, not only those tied
# to a Completed spec.
#
# 270 rather than the template default of 150 because three docs already sit above that
# (SPEC-028 at 267, SPEC-030 at 230, SPEC-032 at 165), written before anything checked,
# and failing main on three historical docs on day one is how a new gate gets switched
# off. Read it accurately, though: the binding doc is SPEC-028 at 267 and it is frozen
# history, so this constant does not mean "a delivery doc may run to 270 lines" — it
# means nobody may add four lines to SPEC-028. New docs aim well under a page and will
# not approach it. Trimming any of the three LOWERS this number in the same PR; nothing
# may raise it.
DELIVERY_MAX_LINES=270

CLAUDE="CLAUDE.md"
REGISTER="docs/decisions.md"
SPEC_DIR="docs/specs"
DELIVERY_DIR="docs/spec-delivery"

# Every failure lands in one file rather than incrementing a counter. A `| while` loop
# runs in a subshell, so a count raised inside one is lost the moment the pipeline ends
# — the bug reads as "the check found nothing" and is invisible in a green run.
FAILS="${TMPDIR:-/tmp}/docs-lint.$$"
trap 'rm -f "$FAILS"' EXIT INT TERM
: > "$FAILS"

note() { printf 'FAIL  %s\n' "$1" >> "$FAILS"; }

report() {
  count=$(grep -c '^FAIL  ' "$FAILS" || true)
  count=${count:-0}
  [ "$count" -gt 0 ] && cat "$FAILS"
  echo "----"
  if [ "$count" -eq 0 ]; then
    echo "docs-lint: ok — $CLAUDE is $size/$CLAUDE_MAX_BYTES bytes."
    exit 0
  fi
  echo "docs-lint: $count check(s) failed."
  exit 1
}

# ── 0. The always-loaded file exists ───────────────────────────────────────────
# A repo with no docs/ yet is simply not scaffolded, and there is nothing to hold. But
# once docs/ exists, a MISSING CLAUDE.md is a deletion rather than a pre-scaffold state,
# and going green on the removal of the very file this script constrains is the emptiest
# pass available.
if [ ! -f "$CLAUDE" ]; then
  if [ -d "$SPEC_DIR" ] || [ -f "$REGISTER" ]; then
    size=0
    note "$CLAUDE does not exist, but docs/ is scaffolded. The always-loaded file has been
      removed, not merely not-yet-written."
    report
  fi
  echo "docs-lint: no $CLAUDE at $ROOT and no docs/ scaffold — nothing to check."
  exit 0
fi

# ── 1. The always-loaded file is within budget ─────────────────────────────────
# Not `wc | tr` in one pipeline: that takes tr's status, so a wc failure leaves size
# empty and the `if` below — exempt from set -e as a condition — skips the check.
size=$(wc -c < "$CLAUDE") || { echo "error: cannot measure $CLAUDE" >&2; exit 2; }
# An emptied file passes every byte budget trivially, and feeding an empty first file to
# the two-file awk below makes NR==FNR true for the whole REGISTER — which silently
# disarms all three cross-check arms. Truncating instead of deleting was the emptiest
# pass available.
if ! grep -q '[^[:space:]]' "$CLAUDE"; then
  note "$CLAUDE is empty. The always-loaded file cannot be emptied any more than it can be
      deleted: every budget passes trivially and the digest/register cross-check disarms."
  size=0
  report
fi
size=$(printf '%s' "$size" | tr -d '[:space:]')
case "$size" in
  ''|*[!0-9]*) echo "error: unreadable size for $CLAUDE" >&2; exit 2 ;;
esac
if [ "$size" -gt "$CLAUDE_MAX_BYTES" ]; then
  note "$CLAUDE is $size bytes against a $CLAUDE_MAX_BYTES budget. It loads every session:
      move the detail into docs/ behind a pointer. Raising this number to fit an edit is the
      failure mode it exists to prevent — cut first, then re-ratchet at the measurement."
fi

# ── 2. Key Decisions has a FIXED SHAPE, and is measured whole ────────────────
#
# This replaced a markdown parser, after that parser shipped three rounds of fixes and
# each round introduced a fresh escape: prose evaded a bullet-only cap; widening the cap
# to accept indented bullets let a parent-plus-children decision escape; adding boundary
# rules for tables, blockquotes and fences made every one of those a place where content
# was consumed and never measured at all. Eight escapes in the end, five of them
# regressions from the previous fix.
#
# The lesson is that this file's format is OURS. Parsing arbitrary markdown is an
# unbounded problem; validating a fixed shape is a bounded one. So the section may
# contain only four things, and anything else fails LOUDLY rather than silently sliding
# past a cap that cannot see it:
#
#   - a `### ` area heading at column 0
#   - a `- **Label** — …` bullet at column 0
#   - an indented continuation of the bullet above it
#   - a blank line
#
# plus free prose BEFORE the first area heading, which is the section intro. A table, a
# blockquote, a fenced block, an indented bullet, an ordered list, a task list and
# `__bold__` are all refused by name. That is not a limitation to work around: every one
# of them was an escape.
#
# The section is also measured WHOLE, against KEY_DECISIONS_MAX_BYTES. A per-unit cap
# can always be evaded by splitting; a section total cannot be evaded by reformatting,
# because every escape still costs bytes.
kd_report=$(awk -v ucap="$DIGEST_MAX_BYTES" -v scap="$KEY_DECISIONS_MAX_BYTES" -v reg="$REGISTER" '
  function flush(   n) {
    if (unit == "") return
    n = length(unit)
    if (n > ucap)
      printf "FAIL  A Key Decisions bullet is %d bytes (cap %d): %s…\n      Keep the claim and the fence in the digest; the reasoning goes in %s.\n",
             n, ucap, substr(unit, 1, 70), reg
    # A bullet whose ** never closes is a decision the register cross-check cannot see:
    # it parses as a valid bullet and yields no label, so nothing demands an entry.
    if (unit ~ /^- \*\*/ && !has_close(unit))
      printf "FAIL  A Key Decisions bullet never closes its `**` label: %s…\n      An unclosed label yields no label at all, so nothing requires a register entry for it.\n",
             substr(unit, 1, 70)
    unit = ""
  }
  function has_close(u,   s, i, j, k, p) {
    s = u; sub(/^- \*\*/, "", s)
    p = 1
    while ((j = index(substr(s, p), "**")) > 0) {
      k = p + j - 1
      if (substr(s, k + 2, 1) != "*") return (k > 1)
      p = k + 1
    }
    return 0
  }
  function bad(why) {
    printf "FAIL  Key Decisions, line %d: %s\n      The section has a fixed shape — area headings, `- **Label**` bullets at column 0,\n      indented continuations, blank lines, and plain intro prose before the first heading.\n      Anything else is refused because every one of them was a way past this check.\n      Offending line: %s\n", FNR, why, substr($0, 1, 60)
  }
  # The section CLOSES on any level-2 heading, tested before the opener. Leaving the
  # opener first meant a second `## Key Decisions — see also …` line was swallowed at
  # zero cost while still inside the section: 14 KB of decisions measured as 51 bytes.
  in_sec && /^## /    { flush(); in_sec = 0 }
  # Fences are tracked file-wide so a fenced example cannot open a phantom section.
  /^[ \t]*(```|~~~)/ { if (in_sec) { bytes += length($0) + 1; bad("a fenced block") }
                      fence = !fence; next }
  fence               { if (in_sec) bytes += length($0) + 1; next }
  !in_sec && /^## Key Decisions/ { if (!fence) { in_sec = 1; found = 1 } next }
  !in_sec             { next }
  { bytes += length($0) + 1 }
  /^###[ \t]/          { flush(); seen_area = 1; next }
  /^[ \t]*\r?$/       { flush(); next }
  # The intro, before the first area heading, may be PLAIN PROSE and nothing else. It
  # was previously exempt from every rule, which made it a hole the size of the section
  # budget — and the shipped scaffold had no area heading at all, so its whole section
  # sat in that hole with every shape check switched off.
  !seen_area && /^[|>]/            { bad("a table or blockquote row in the intro") ; next }
  !seen_area && /^[0-9]+[.)][ \t]/ { bad("an ordered-list item in the intro") ; next }
  !seen_area && /^[-*+][ \t]/      { bad("a bullet before the first `### ` area heading") ; next }
  !seen_area && /^[ \t]+[^ \t]/    { bad("an indented line in the intro") ; next }
  !seen_area && /^</                { bad("raw HTML in the intro") ; next }
  !seen_area          { next }
  /^- \*\*/            { flush(); unit = $0; next }
  /^- /               { bad("a bullet that does not open with a **bold label**") ; next }
  /^[ \t]+[^ \t]/     { if (unit == "") { bad("indented line with no bullet above it to continue") ; next }
                        s = $0; sub(/^[ \t]+/, "", s); unit = unit " " s; next }
  /^[|>]/             { bad("a table or blockquote row") ; next }
  /^[0-9]+[.)][ \t]/  { bad("an ordered-list item") ; next }
  /^[*+][ \t]/        { bad("a `*` or `+` bullet — use `-`") ; next }
                      { bad("prose after the first area heading") ; next }
  END {
    flush()
    # Nothing else in this file notices a section that is missing, misspelled, or hidden
    # behind an unbalanced fence earlier in the document — and each of those turned every
    # check above into a silent pass.
    if (!found)
      printf "FAIL  No `## Key Decisions` section found. It cannot be renamed, cased differently\n      or hidden behind an unclosed fence earlier in the file: every check on the digest\n      goes quiet when the section cannot be located, which is a silent pass.\n"
    else if (!seen_area)
      printf "FAIL  Key Decisions has no `### ` area heading. It is grouped by AREA, not by spec, and\n      the shape checks on bullets only begin at the first heading — a section with none\n      sits entirely in the intro, unvalidated.\n"
    if (bytes > scap)
      printf "FAIL  The Key Decisions section is %d bytes (cap %d). It is the bulk of the file that\n      loads every session: move decisions into %s and leave one line each.\n", bytes, scap, reg
  }
' "$CLAUDE")
[ -z "$kd_report" ] || printf '%s\n' "$kd_report" >> "$FAILS"

# ── 3-7. The register: present, not inverted with its digest, reachable, self-consistent ─
#
# The inversion is the specific failure this template shipped into a project and did
# not catch. That repo had no register at all, so its digest WAS the register: every
# settled decision landed full-length in the file that loads on every turn. A digest
# line with no entry behind it is the first step there — and so is the reverse, since
# an entry nobody digested is a decision no session will be pointed at.
#
# Both sides skip the scaffold "(example)" placeholder, so a fresh checkout is green
# before the first real decision lands.
if [ ! -f "$REGISTER" ]; then
  note "$REGISTER is missing. Key Decisions in $CLAUDE is a DIGEST — one line per settled
      decision, pointing at its full entry. With no register the digest becomes the only home
      of every fact, which is how an always-loaded file turns into the archive."
else
  # Two files, one pass: NR==FNR is CLAUDE.md, the rest is the register. Comparing the
  # two sets with comm would want process substitution, which is a bashism.
  awk -v claude="$CLAUDE" -v reg="$REGISTER" '
    function trim(s) { sub(/^[ \t\r]+/, "", s); sub(/[ \t\r]+$/, "", s); return s }
    # The label a `**…**` span opens with, given the text AFTER that opening `**`, or ""
    # when it never closes. Shared by the digest bullet and the register entry body: both
    # write the same label, so both meet the same two traps, and a second copy of this scan
    # is a second place for one of them to be fixed and the other left alone.
    function label_of(body,   s, i, j, k, p, pad) {
      s = body
      # Blank out inline code spans first: a span containing ** would otherwise close
      # the label early, the same class as the italic-suffix bug below.
      while (match(s, /`[^`]*`/)) {
        pad = sprintf("%*s", RLENGTH, "")
        s = substr(s, 1, RSTART - 1) pad substr(s, RSTART + RLENGTH)
      }
      # The closing ** is the first NOT followed by another *. A label ending in an
      # italic (...skip *work*) is stored as *work***, and taking the first pair
      # truncates it by one character — reported as both halves of the cross-check
      # missing, for a label that is correct.
      i = 0; p = 1
      while ((j = index(substr(s, p), "**")) > 0) {
        k = p + j - 1
        if (substr(s, k + 2, 1) != "*") { i = k; break }
        p = k + 1
      }
      if (i > 1) return trim(substr(body, 1, i - 1))
      return ""
    }
    function emit(   s, label) {
      if (unit == "" || unit !~ /^- \*\*/) { unit = ""; return }
      s = unit; sub(/^- \*\*/, "", s)
      label = label_of(s)
      if (label != "" && label !~ /^\(example\)/) digest[label] = 1
      unit = ""
    }
    function anchor(s,   t) {
      t = tolower(trim(s))
      gsub(/`/, "", t); gsub(/\*/, "", t)
      gsub(/[^a-z0-9 _-]/, "", t)
      gsub(/ /, "-", t)
      return t
    }
    # ---- first file: the always-loaded digest ----
    # Check 2 has already refused every shape but `- **Label**` at column 0 with indented
    # continuations, so this only has to join a wrapped label and find its closing `**`.
    NR == FNR {
      if ($0 ~ /^[ \t]*(```|~~~)/) { kfence = !kfence; next }
      if (kfence) next
      if (kd && $0 ~ /^## /)        { emit(); kd = 0 }
      if (!kd && $0 ~ /^## Key Decisions/) { if (!kfence) kd = 1; next }
      if (!kd) next
      sub(/\r$/, "")
      if ($0 ~ /^- /)               { emit(); unit = $0; next }
      if ($0 ~ /^[ \t]+[^ \t]/)     { if (unit != "") { s = $0; sub(/^[ \t]+/, "", s); unit = unit " " s } next }
      emit()
      next
    }
    # ---- second file: the register ----
    # Anchors are collected ONLY from the Contents section. Collecting them from the
    # whole file let one entry cross-reference another and satisfy the check for it,
    # so an entry absent from the Contents passed while the message said it was there.
    /^## Contents/ { in_toc = 1; next }
    in_toc && /^## / { in_toc = 0 }
    # `\r?` or the section never closes under CRLF, and every line after the break is eaten
    # by the in_toc block: the anchors of entries below it join `seen` (the whole-file
    # collection this rule exists to stop) and the entry-label check never runs at all.
    # Measured on byte-identical registers whose Contents ends only at the break — LF exits
    # 1 on a stale label, CRLF exits 0.
    in_toc && /^[ \t]*---[ \t]*\r?$/ { in_toc = 0 }
    /^### / {
      s = trim(substr($0, 5))
      want_label = 0
      if (s !~ /^\(example\)/) { entry[s] = 1; head[anchor(s)] = s; cur_head = s; want_label = 1 }
      next
    }
    in_toc {
      line = $0
      while (match(line, /\(#[a-z0-9_-]+\)/)) {
        seen[substr(line, RSTART + 2, RLENGTH - 3)] = 1
        line = substr(line, RSTART + RLENGTH)
      }
      # Every link on the row is read as a row of its own, so a trailing "see also" link is
      # checked as though it were one. The Contents is one link per row today, and the shape
      # that would break this is the shape the layering forbids anyway.
      #
      # A link text containing `]` never matches the pattern below and is exempt from the
      # name check in silence. Reachability is unaffected — the bare-anchor loop above has
      # already recorded it — and widening the pattern to balance brackets is markdown
      # parsing, which the section above this one is the standing argument against.
      #
      # A SECOND pass over the same line for the row itself, deliberately not folded into
      # the loop above. Narrowing that one to the full `[text](#anchor)` form would stop a
      # bare `(#anchor)` reaching `seen`, and the entry it names would then be reported as
      # absent from a Contents that lists it — weakening a working check in order to add one.
      line = $0
      while (match(line, /\[[^]]*\]\(#[a-z0-9_-]+\)/)) {
        m = substr(line, RSTART, RLENGTH)
        line = substr(line, RSTART + RLENGTH)
        t = m; sub(/^\[/, "", t); sub(/\]\(#[a-z0-9_-]+\)$/, "", t)
        a = m; sub(/^.*\]\(#/, "", a); sub(/\)$/, "", a)
        # ONE-BASED, and the increment comes first. An uninitialised awk variable used as a
        # subscript is the empty string, not zero, so `rowtext[nrow]` with nrow unset stored
        # the first row under "" and a 0-based loop then read past it — the FIRST Contents
        # row went unchecked while every later one worked, which is why the single-row
        # fixture beside this file exists.
        if (t !~ /^\(example\)/) { nrow++; rowtext[nrow] = t; rowanchor[nrow] = a }
      }
      next
    }
    # ---- the opening bold label of an entry body ----
    # A decision heading is written down in FIVE places and three of them were checked: the
    # Contents anchor, the heading itself, and the digest label in CLAUDE.md. The two added
    # here are the two nothing else can see. A Contents row can name decision A while
    # linking to decision B — the link still works, so the register reads as healthy from
    # either end, and only a reader who trusts the name is misled. An entry can also open
    # its body with a bold label that disagrees with the heading above it, which costs more
    # here than it would elsewhere: the whole register model is that a digest line greps
    # straight to its entry, and that bold label is what such a grep lands on.
    #
    # Scoped to DISAGREEMENT, not to presence. An entry opening with plain prose is left
    # alone: the live register has one (the entry on extra floors as a published contract),
    # and so does most of the fixture corpus beside this script, so demanding the
    # restatement would be a gate inventing a rule the register rules header never stated.
    # The escape is therefore real and deliberate — delete the label and nothing fires —
    # and it is the right one to leave open. A heading with no restatement contradicts
    # nothing; a heading with the WRONG restatement contradicts itself.
    !want_label { next }
    /^[ \t]*\r?$/ { next }
    # A superseded marker is a BLOCKQUOTE, and the completion ritual puts one at every doc
    # site still stating the old claim — so it lands directly under the heading, on exactly
    # the path this check exists for: superseding is WHEN a heading gets renamed. Read as
    # the body line it silenced the stale label below it, measured green on a register whose
    # entry was headed one decision and labelled another.
    /^[ \t]*>/ { next }
    {
      want_label = 0
      if ($0 !~ /^\*\*/) next
      # No CR strip here, and none is needed: a trailing CR sits past the closing `**`, so
      # it is never inside the label, and `trim` would take it anyway. Proved equivalent by
      # mutation on the one shape where it could matter — a label closing at end of line.
      s = $0; sub(/^\*\*/, "", s)
      lab = label_of(s)
      if (lab == "")
        printf "FAIL  %s: \"### %s\" opens its body with a `**` that never closes.\n      An unclosed label yields no label at all, so the one line that has to restate the\n      heading is never compared against it. It opens and closes on the FIRST body line: a\n      label wrapped onto a second line reads here as one that never closes.\n", reg, cur_head
      else if (lab != cur_head)
        printf "FAIL  %s: \"### %s\" opens its body with the label \"%s\".\n      The bold label opening an entry restates its heading, and is what a digest line greps\n      to. A label that does not match its heading is visible to no other check here: the\n      heading is right, the Contents row is right, and the two disagree only with each other.\n      A superseded marker belongs in a blockquote above the label, where it is skipped.\n", reg, cur_head, lab
      next
    }
    END {
      emit()
      for (l in digest)
        if (!(l in entry))
          printf "FAIL  Key Decisions carries \"%s\" with no \"### %s\" in %s.\n      Entry first, line second: a digest line is never the only home of a fact.\n", l, l, reg
      for (l in entry)
        if (!(l in digest))
          printf "FAIL  %s has \"### %s\" with no matching bold label in %s Key Decisions.\n      An entry no session is pointed at is a decision that gets re-litigated.\n", reg, l, claude
      for (a in head)
        if (!(a in seen))
          printf "FAIL  %s: \"### %s\" is absent from the Contents — findable only by reading the\n      whole file, which is the cost the layering exists to avoid.\n", reg, head[a]
      # Indexed rather than `for (i in rowtext)`: awk iterates an associative array in an
      # unspecified order, so two bad rows would report in a different order on a different
      # awk and the corpus would be flaky on exactly the machines it is meant to protect.
      for (i = 1; i <= nrow; i++) {
        if (anchor(rowtext[i]) == rowanchor[i]) continue
        if (rowanchor[i] in head)
          printf "FAIL  %s Contents: the row named \"%s\" links to \"#%s\", the anchor of \"### %s\".\n      A row that names one decision and points at another reads as correct from either end,\n      because the link still works — nothing looks broken until a reader trusts the name.\n", reg, rowtext[i], rowanchor[i], head[rowanchor[i]]
        else
          printf "FAIL  %s Contents: the row named \"%s\" links to \"#%s\", but its own anchor is\n      \"#%s\". The Contents row is the only place a name and the link under it are written\n      side by side, so a disagreement between the two is checkable nowhere else.\n", reg, rowtext[i], rowanchor[i], anchor(rowtext[i])
      }
    }
  ' "$CLAUDE" "$REGISTER" >> "$FAILS"
fi

# ── 8 & 9. The delivery tier ───────────────────────────────────────────────────
# The Status match is deliberately permissive about what sits between "Status" and
# "Completed" — ": ", " | " in a table row, "**" — because a spec whose header form
# this fails to recognise is skipped SILENTLY, and a silent skip of the delivery-doc
# check is indistinguishable from a pass.
if [ -d "$SPEC_DIR" ]; then
  for f in "$SPEC_DIR"/SPEC-*.md; do
    [ -f "$f" ] || continue
    # Fence-aware: a Draft spec that quotes the completion ritual in a fenced block
    # would otherwise be read as Completed and told it owes a delivery doc.
    awk '
      /^[ \t]*(```|~~~)/ { fence = !fence; next }
      fence { next }
      tolower($0) ~ /^[^a-z]*status[^a-z]+completed/ { found = 1 }
      END { exit !found }
    ' "$f" || continue
    num=$(basename "$f" | sed -n 's/^\(SPEC-[0-9][0-9]*\).*/\1/p')
    [ -n "$num" ] || continue
    found=0
    for d in "$DELIVERY_DIR/$num"-*.md; do
      if [ -f "$d" ]; then found=1; break; fi
    done
    [ "$found" -eq 1 ] || note "$f is Completed with no delivery doc at $DELIVERY_DIR/$num-*.md.
      Step 3 of the completion ritual: what shipped belongs one tier down, not in the digest."
  done
fi

if [ -d "$DELIVERY_DIR" ]; then
  for d in "$DELIVERY_DIR"/*.md; do
    [ -f "$d" ] || continue
    n=$(wc -l < "$d" | tr -d '[:space:]')
    [ "$n" -le "$DELIVERY_MAX_LINES" ] || note "$d is $n lines (cap $DELIVERY_MAX_LINES). A
      delivery doc says what shipped and what changed; past this it is re-explaining the code."
  done
fi

# ── 10. Pointers out of the always-loaded file ─────────────────────────────────
# CLAUDE.md only, deliberately. A pointer that goes nowhere defeats the layering this
# file defends: a session sent to a missing register reads the digest and stops there.
# Link-checking every doc in the repo is a different job with a far wider false-positive
# surface, and is not this script business.
awk '
  /^[ \t]*(```|~~~)/ { fence = !fence; next }
  fence { next }
  {
    line = $0
    while (match(line, /\]\([^)]+\)/)) {
      p = substr(line, RSTART + 2, RLENGTH - 3)
      line = substr(line, RSTART + RLENGTH)
      sub(/#.*$/, "", p)
      if (p != "" && p !~ /^(https?:|mailto:)/ && p !~ /[*?]/) print p
    }
    line = $0
    while (match(line, /@[A-Za-z0-9_.*?\/-]+\.md/)) {
      p = substr(line, RSTART + 1, RLENGTH - 1)
      line = substr(line, RSTART + RLENGTH)
      # A pointer written as a glob (@docs/specs/SPEC-XXX-*.md) names a shape, not a
      # file. Skipped ON PURPOSE and matched first, so that a real broken pointer is
      # not silently excused by a character class that happened to exclude the star.
      if (p !~ /[*?]/) print p
    }
  }
' "$CLAUDE" | sort -u | while IFS= read -r p; do
  [ -n "$p" ] || continue
  [ -e "$p" ] || printf 'FAIL  %s points at "%s", which does not exist.\n' "$CLAUDE" "$p" >> "$FAILS"
done

# ── 9. A dated measurement in the standing tier carries an anchor ──────────────
#
# process.md §5: "Standing rules never cite volatile numbers ... Dating the measurement
# does not save it." That rule was stated, and violated in the tier it governs, at eight
# sites at once — including process.md §5 itself, which carried the dated-measurement
# example AND argued that the date was what made it safe. The architecture.md §12 entry
# added by dcb07c3 was one line stale in dcb07c3 itself and outright wrong in a58dfff,
# the very next commit.
#
# So this is the half of that rule a script can hold: a dated measurement must carry a
# commit a reader can re-measure from. A date says WHEN someone looked. It does not say
# AT WHAT, and a reader who does not check it against a calendar reads it as current.
#
# WHAT IT CATCHES, exactly: a bullet, paragraph or comment block in which "measured" or
# "as of" is followed immediately by an ISO date, with no short SHA or version tag
# anywhere in that same unit.
#
# THAT IS ONE SHAPE, NOT THE POPULATION, and the difference is worth stating plainly
# because the next person to widen this will read it. Of the eight sites the commit that
# added this check had to fix, exactly ONE matched — architecture.md §12. Both
# pyproject.toml sites carried a bare count with no date at all; process.md §4 wrote the
# date six words from the word; process.md §5 carried a byte pair with no date. So this
# check does not close the defect class. It closes the sub-shape that DATING creates: a
# number sitting beside a recent date, which is the form that reads as current and
# therefore never gets re-checked. An undated number at least still looks like something
# to verify.
#
# Narrow on purpose, and the narrowness is a false-negative choice, not a coverage claim.
# A trigger wide enough to catch the other seven would have to fire on any number near
# any date, which is ordinary prose in every one of these files — and a doc gate that
# fires on ordinary prose is a doc gate that gets commented out. The rest stays with
# process.md §5, to be caught by reading. If a future sweep finds a second idiom actually
# in use, add it here and add a fixture for it; do not widen this into a number detector.
#
# THE ANCHOR IS SOUGHT OVER THE WHOLE UNIT, not the line, because the anchor and the
# number routinely wrap apart. That is a deliberate false-NEGATIVE — an unrelated SHA
# elsewhere in the same unit silences the check for it — and it is only tolerable while a
# UNIT STAYS SHORT ENOUGH TO READ. So what ends a unit is load-bearing, not incidental:
# a blank line, a fence, a list marker, a markdown heading, and, outside markdown, ANY
# LINE THAT IS NOT A COMMENT. That last one was missing when this check first covered
# .toml/.sh/.yml, and the cost was measured before it was added: sweeping an unanchored
# measurement through every insertion position of this repo left 27-41% of positions in
# those files silent, against 8-16% in its markdown, because a comment block ran on
# across intervening CODE until the next blank line. One SHA in a comment silenced a
# 15-line array 13 lines below it.
#
# What remains unbounded: a run of ordinary non-comment lines is one unit. In markdown
# that is a paragraph, which is the reading-distance case this rule was designed around.
# In a source file it is code — and the tempting sentence, that a dated measurement is
# not written in code, is FALSE and was written here before it was checked. An INLINE
# comment is not a comment LINE: `fetch-depth: 0  # Measured 2026-09-05: 42 s` joins the
# surrounding code run, and any SHA in that run silences it. Under .github that is not a
# corner case but the norm, since every `uses:` pins its action by SHA — measured on
# release.yml, an inline measurement two lines below a pin is silent while the same text
# on its own comment line fires.
#
# Not closed here, because closing it means telling a comment from a `#` inside a string
# in four languages. The remedy is one sentence to an author instead: WRITE A MEASUREMENT
# ON ITS OWN COMMENT LINE, where the bound applies. An unbalanced fence is the other
# knowing escape — a lone ``` at column 0, in any file type, silences everything below
# it to EOF, and nothing here notices.
#
# ── the population ──
#
# NOT MARKDOWN ONLY. §5s neighbouring rule says exactly why: "the pointers that rot
# unseen are in SOURCE files — .py docstrings, .toml comments, .yml steps. A markdown-only
# sweep reports the tree clean." Two of the eight sites this check was written for were
# pyproject.toml comments, and a third was a docstring in scripts/. A gate blind to the
# files where a quarter of the known instances lived is half a gate.
#
#   IN:  every *.md at the root (CLAUDE.md, README.md, SECURITY.md and any that join
#        them); docs/**.md except the frozen-record trees below; pyproject.toml;
#        scripts/**; every *.yml and *.yaml under .github, which is workflows AND the
#        composite actions and dependabot.yml beside them — those pin third-party code
#        by SHA and carry the same kind of comment. Both spellings of the extension,
#        because a glob that knows only one silently drops the other.
#   OUT: docs/audits, docs/specs, docs/spec-delivery, docs/release-notes, docs/templates
#        — FROZEN RECORDS, not standing rules. An audit dated 2026-08-07 is a report of
#        what was true then, and demanding an anchor from it demands an anchor from
#        history.
#   OUT: src/ — deliberately, and not because it is clean. Its docstrings anchor to SPEC
#        numbers by convention rather than to commits, which is a different anchoring
#        scheme; whether a spec number is an acceptable anchor is a decision nobody has
#        taken, and this check must not take it by accident. Recorded, not chased.
#   OUT: tests/ — tests/docs-lint/*.case carry the WRONG form on purpose. A gate that
#        fails on its own fixture corpus is a gate that gets switched off within a week.
#
# The docs half is derived BY EXCLUSION so a standing doc written tomorrow, in a
# directory that does not exist yet, is covered without anyone remembering to list it.
{
  for f in *.md; do [ -f "$f" ] && printf '%s\n' "$f"; done
  [ -f pyproject.toml ] && printf 'pyproject.toml\n'
  if [ -d docs ];    then find docs -type f -name '*.md' -print | sort; fi
  if [ -d scripts ]; then find scripts -type f -print | sort; fi
  if [ -d .github ]; then find .github -type f \( -name '*.yml' -o -name '*.yaml' \) -print | sort; fi
} | while IFS= read -r f; do
    case "$f" in
      docs/audits/*|docs/specs/*|docs/spec-delivery/*|docs/release-notes/*|docs/templates/*) continue ;;
    esac
    [ -f "$f" ] || continue
    # `#` opens a heading in markdown and a COMMENT in everything else, and the two want
    # opposite unit rules: a heading is its own unit, while consecutive comment lines are
    # one block that an anchor may wrap into. Getting this wrong makes every comment line
    # its own unit, so an anchor on the next line stops counting — which is precisely how
    # the pyproject.toml sites are written.
    md=0
    case "$f" in *.md) md=1 ;; esac
    awk -v file="$f" -v md="$md" '
      # An anchor is a short SHA, and ONLY a short SHA. Two other forms were tried and
      # rejected on evidence rather than taste. A version TAG names a tree as exactly as
      # a SHA does, so it looked like an obvious second form — but version numbers appear
      # in these files for a dozen unrelated reasons, and accepting `vN.N.N` silenced a
      # 37-line region of pyproject.toml on the strength of two release numbers in a
      # classifiers block that anchor nothing. A 7-hex token in prose is almost always an
      # anchor; a version number almost never is. A PR number was never a candidate: it
      # names a change, not a tree, and cannot be re-measured without the network.
      #
      # A short SHA is a hex run of 7+ starting and ending at a non-alphanumeric
      # boundary, containing at least one DIGIT. The digit is what keeps English out:
      # "defaced" is seven hex characters and a word, and silencing this check on it
      # would be a false negative dressed as a feature. A real short SHA without a
      # digit is possible and vanishingly rare (about one in 700). The converse is
      # accepted knowingly: a bare seven-digit decimal is all hex characters and would
      # silence the check too — and the likeliest such number is THE MEASURED VALUE
      # ITSELF, not some unrelated token elsewhere in the unit, so
      # "Measured 2026-09-05: 1234567 events" silences its own violation while the same
      # figure written 1,234,567 does not. Requiring a LETTER as well would close it and
      # would reject the roughly one short SHA in 25 that is all digits, which is the
      # worse trade: that failure is loud and fixed by pasting one more character, and
      # this one is silent.
      function has_anchor(s,   i, n, c, run, digits, dirty) {
        n = length(s); run = 0; digits = 0; dirty = 0
        for (i = 1; i <= n + 1; i++) {
          c = (i <= n) ? substr(s, i, 1) : " "
          if (c ~ /^[0-9a-f]$/)        { run++; if (c ~ /^[0-9]$/) digits++ }
          else if (c ~ /^[0-9A-Za-z]$/) { dirty = 1; run = 0; digits = 0 }
          else {
            if (!dirty && run >= 7 && digits > 0) return 1
            run = 0; digits = 0; dirty = 0
          }
        }
        return 0
      }
      # The separators are the ones this repo actually writes: "Measured 2026-09-01",
      # "Measured, on `f17edd4`", "measured at e565e22". Leaving `at` and the comma out
      # made the two likeliest honest spellings escape while the rule was being followed.
      function dated(s) {
        return tolower(s) ~ /(measured|as of)[ \t]*[,:]?[ \t]*((on|at|in)[ \t]+)?20[0-9][0-9]-[0-9][0-9]-[0-9][0-9]/
      }
      function flush(   where, what) {
        if (unit != "" && dated(unit) && !has_anchor(unit)) {
          where = hitline ? hitline : ustart
          what  = hitline ? hittext : unit
          printf "FAIL  %s:%d carries a dated measurement with no commit to re-measure from.\n      %s\n      A date says WHEN someone looked, not at what: a dated number still reads as\n      current to anyone not checking it against a calendar. Either drop the number and\n      state the principle, or anchor the evidence to a short commit SHA in the same\n      bullet, paragraph or comment block. A version tag is not accepted; the comment\n      beside this check says why. Quoting the wrong form on purpose? Put it in a fenced\n      block, which this check skips. process.md 5, never cite volatile numbers.\n",
                 file, where, substr(what, 1, 90)
        }
        unit = ""; hitline = 0; ukind = ""
      }
      # In a non-markdown file, is this line a comment or is it not? A unit may not span
      # the two. `kind` reads $0 directly, so it is only meaningful inside a line rule.
      function kind() { return ($0 ~ /^[ \t]*#/) ? "c" : "p" }
      function take(   s) {
        if (unit == "") { unit = $0; ustart = FNR; ukind = kind() }
        else { s = $0; sub(/^[ \t]+/, "", s); unit = unit " " s }
        if (!hitline && dated($0)) { hitline = FNR; hittext = $0 }
      }
      FNR == 1 { flush(); fence = 0 }
      { sub(/\r$/, "") }
      /^[ \t]*(```|~~~)/                { flush(); fence = !fence; next }
      fence                             { next }
      /^[ \t]*$/                        { flush(); next }
      md && /^[ \t]*#/                  { flush(); take(); next }
      /^[ \t]*([-*+]|[0-9]+[.)])[ \t]/  { flush(); take(); next }
      !md && unit != "" && kind() != ukind { flush() }
                                        { take() }
      END { flush() }
    ' "$f" >> "$FAILS"
  done


report
