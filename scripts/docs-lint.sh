#!/bin/sh
# docs-lint.sh — hold the ALWAYS-LOADED tier to the shape the layering assumes.
#
# Why this exists as a script rather than a rule. Every check below already existed in
# prose — in `docs/process/completion-ritual.md` (*Anti-regrowth & doc hygiene*), in
# `CLAUDE.md`'s own doc-size guardrail, and in the register's rules header (now
# `docs/decisions/INDEX.md`) — and several were violated here anyway. This repo's
# `CLAUDE.md` grew from 7,350 bytes at `ad898fc8` to 89,340 at `e60b60d`, more than
# tenfold — most of it while the repo carried only a TWO-SENTENCE version of the rule,
# naming no shape, no register and no budget. The full set arrived at `690d2a55`, days
# before the cut, named the violation in the present tense and correctly, and did not stop
# the next edit either. It was cut back to a digest over its register at
# `561a9f6`, the change that first ran this script, and the digest itself then moved out
# beside its reasoning when the process tier was routed. Both ends are anchored to commits
# rather than restated: an earlier version of this comment carried three numbers, and two
# of them were wrong by the time it shipped.
#
# A rule a reader has to remember is a rule that rots. This is the same rules where a
# script can see them — run before every push, deliberately not in CI, so the failure
# lands on whoever caused it rather than on a shared branch.
#
# FAIL (exit 1): an always-loaded file is over budget or has been removed outright, a
#   Key Decisions section carries anything but its intro and area table, the register INDEX or an
#   area file is missing or disagrees with the table, a fence has no entry or an entry no fence,
#   an entry is unreachable from its Contents or is named by a row that links elsewhere, an entry
#   opens its body with a label that contradicts its heading, a fence has become the reasoning, a
#   Completed spec has no delivery doc, a delivery doc has become an essay, a pointer out of an
#   always-loaded file goes nowhere, the routed process tier has lost its shape (router, imports,
#   parts, rules, agents — checks 9–13), a stub has reappeared at either old single-file path, a
#   dated measurement carries no commit to re-measure from (14), or a universal claim about the
#   marking walk carries no scope (15).
#
# There is no WARN tier: `spec-lint.sh` owns the soft per-spec judgements, and every rule
# here is a shape the layering depends on — a shape is either held or it isn't.
# Deliberately NOT checked here: anything `spec-lint.sh` already owns (required spec
# sections, banned headers, the FR ceiling). A rule with two enforcement homes gets
# qualified in one of them and read from the other.
#
# Usage: sh scripts/docs-lint.sh          (run from anywhere; resolves its own root)
# POSIX sh — no bashisms. One dependency, and only for check 15: `python3`, because that
# check reads sentences rather than lines and the sentence splitter it needs is not
# expressible in awk. Every other check runs on awk and the shell alone.
#
# NOTE for maintainers: the awk programs below are single-quoted. An apostrophe anywhere
# inside one — including in a comment — closes the quote, and the shell then parses awk
# source as shell. That failed *silently with status 0* once during authoring, which is
# why `scripts/docs-lint-test.sh` runs `sh -n` on this file before anything else.

set -eu

# Both byte budgets rely on awk length() counting BYTES. It does in the one-true-awk
# that ships on macOS, but gawk in a UTF-8 locale counts CHARACTERS — every em dash in a
# fence would then count 1 instead of 3, so the caps would measure something different
# in CI than they do locally. C locale makes it bytes everywhere.
LC_ALL=C
export LC_ALL

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# ── Budgets ────────────────────────────────────────────────────────────────────
#
# RATCHETS AGAINST ACCRETION. When a doc grows past one a line at a time, the fix is to
# move detail down a tier — behind a pointer — which is the entire reason the budget is
# here. Raising the bar to fit the edit is the failure mode, and it is how this repo
# reached 89 KB one justified exception at a time.
#
# AFTER A STRUCTURAL CUT THE REGIME CHANGES, and pinning at the measurement becomes the
# trap: what remains is fences and routing, not accretion, so a budget left at the new
# measurement leaves the next change that legitimately needs a line nothing to spend —
# and it takes those bytes from somewhere else. The first two below are set after such a
# cut (the method moved out of CLAUDE.md into docs/process/ behind a router) and carry
# room deliberately, stated in words rather than as a second number that goes stale.
#
# RE-DERIVE, DON'T RE-CHECK, after any further structural change. A cap that can no longer
# fire is still advertised as a fence: `DIGEST_MAX_BYTES` was an inherited 1400, and the cut
# at `561a9f6` — which took the section's longest unit to 550 — would have left it several
# times above anything it governed, so that same commit re-derived it to 800 rather than
# checking it and finding it green. A check that passes proves nothing about a cap that
# cannot fail.

# CLAUDE.md alone. Measured at the routing cut, where the file went from the always-loaded
# archive to a router over docs/process/ and docs/decisions/: room above that for a
# current-work paragraph, a Tech Stack row and a couple of area rows, which is roughly a
# fifth. It is the one budget here that is NOT pinned at its measurement, and the reason is
# the paragraph above.
CLAUDE_MAX_BYTES=21000

# The whole always-loaded set — CLAUDE.md plus every file the router's "Loaded every
# session" table names (the same files CLAUDE.md imports). The SET is derived from that
# table at run time, never transcribed here: a transcribed copy would go stale the first
# time a row moved. Room above the measurement is the same fifth, plus one router row.
ALWAYS_LOADED_MAX_BYTES=38000

# The whole Key Decisions section, measured as bytes. Since the fences left CLAUDE.md the
# section is an intro paragraph and one row per AREA, so this bounds the number of AREAS,
# not the number of decisions: the intro is well under a kilobyte and a row about eighty
# bytes, leaving room for a dozen more areas. It sits WELL under CLAUDE_MAX_BYTES on purpose —
# the previous value (22000) was inside a file capped at 36000 and could be reached only by
# a file that had already failed check 1, so it was a fence that could not fire.
KEY_DECISIONS_MAX_BYTES=2500

# The longest a single FENCE may be — a bullet under `## Fences` in an area file, with its
# continuation lines joined. Measured on the LOGICAL unit because measuring the physical
# line looks equivalent and is not: the moment the section is rewritten as wrapped prose the
# longest physical line collapses to the wrap width, and the guard can never fire again
# while still being advertised in the process docs.
#
# UNCHANGED ACROSS THE ROUTING CUT, deliberately and after re-derivation rather than by
# omission: that cut moved the fences from CLAUDE.md into their area files without
# rewriting one of them, so what this cap measures is the same KIND of unit it was derived
# against at `561a9f6` — 48 fences there, longest 550. The population has grown since
# rather than shrunk (55 fences on this branch, longest 553), which is the direction that
# leaves a cap able to fire, so the re-derivation ends at the same number instead of a new
# one. A cut that shrinks the fences again re-derives it downward.
#
# BYTES, not characters: awk length() is byte-based in the one-true-awk that ships on
# BSD and macOS, so em dashes and smart quotes count for more than one. Named DIGEST for
# history — the fences ARE the digest, now beside their entries.
DIGEST_MAX_BYTES=800

# A delivery doc answers "what shipped and what changed"; the completion template aims for
# well under a page. Applies to every *.md in docs/spec-delivery/, not only those tied to a
# Completed spec. A RATCHET at the measured level: when it fires, cut and re-ratchet, never
# raise it to fit the doc in hand.
DELIVERY_MAX_LINES=270

CLAUDE="CLAUDE.md"
REGISTER_DIR="docs/decisions"
REGISTER_INDEX="$REGISTER_DIR/INDEX.md"
OLD_REGISTER="docs/decisions.md"
SPEC_DIR="docs/specs"
DELIVERY_DIR="docs/spec-delivery"
PROCESS_DIR="docs/process"
ROUTER="$PROCESS_DIR/INDEX.md"
ROUTING="$PROCESS_DIR/model-routing.md"
OLD_PROCESS="docs/process.md"
RULES_DIR=".claude/rules"
AGENTS_DIR=".claude/agents"

# Every failure lands in one file rather than incrementing a counter. A `| while` loop
# runs in a subshell, so a count raised inside one is lost the moment the pipeline ends
# — the bug reads as "the check found nothing" and is invisible in a green run.
total=""
FAILS="${TMPDIR:-/tmp}/docs-lint.$$"
trap 'rm -f "$FAILS" "${TMPDIR:-/tmp}"/docs-lint-kdrows.$$' EXIT INT TERM
: > "$FAILS"

note() { printf 'FAIL  %s\n' "$1" >> "$FAILS"; }

report() {
  count=$(grep -c '^FAIL  ' "$FAILS" || true)
  count=${count:-0}
  [ "$count" -gt 0 ] && cat "$FAILS"
  echo "----"
  if [ "$count" -eq 0 ]; then
    if [ -n "${total:-}" ]; then
      echo "docs-lint: ok — $CLAUDE is $size/$CLAUDE_MAX_BYTES bytes; the always-loaded set is $total/$ALWAYS_LOADED_MAX_BYTES."
    else
      echo "docs-lint: ok — $CLAUDE is $size/$CLAUDE_MAX_BYTES bytes."
    fi
    exit 0
  fi
  echo "docs-lint: $count check(s) failed."
  exit 1
}

# Shared by checks 4 and 8: every relative link and `@path.md` pointer in a file resolves.
# Defined here because the register check (4) runs before the always-loaded check (8).
# Code-span-BLIND on purpose — see the note at check 8.
check_pointers() {
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
  ' "$1" | sort -u | while IFS= read -r p; do
    [ -n "$p" ] || continue
    [ -e "$p" ] || printf 'FAIL  %s points at "%s", which does not exist.\n' "${2:-$1}" "$p" >> "$FAILS"
  done
}

# The four governed trees — docs/process/, docs/decisions/, .claude/rules/ and
# .claude/agents/ — are each ONE FLAT SET, and each is enumerated through this pair so the
# set cannot differ by tree. It differed three times: a glob skipped the dotfiles in two
# trees, `ls` skipped them in a third, and a subdirectory was invisible in two. Each miss
# has the same shape — a file Claude Code loads and no check here sees — and each was found
# one tree later than the last, which is why this is a helper and not a third fix.
#
# `find -maxdepth 1` takes dotfiles; the glob and `ls` do not. Callers read it through a
# `while IFS= read -r`, which runs in a subshell: safe here because every caller reports
# through `note`, which appends to a FILE, and none of them accumulates a variable.
list_flat_md() { find "$1" -maxdepth 1 -name '*.md' | sort; }
refuse_deep_md() {
  find "$1" -mindepth 2 -name '*.md' | sort | while IFS= read -r deep; do
    note "$deep is in a subdirectory of $1/. $2"
  done
}

# ── 0. The always-loaded file exists ───────────────────────────────────────────
# A repo with no docs/ yet is simply not scaffolded, and there is nothing to hold. But
# once docs/ exists, a MISSING CLAUDE.md is a deletion rather than a pre-scaffold state,
# and going green on the removal of the very file this script constrains is the emptiest
# pass available.
if [ ! -f "$CLAUDE" ]; then
  if [ -d "$SPEC_DIR" ] || [ -d "$REGISTER_DIR" ] || [ -f "$OLD_REGISTER" ]; then
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
      deleted: every budget passes trivially and the table/register cross-check disarms."
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
      failure mode it exists to prevent — cut first, then re-ratchet. After a structural cut, leave
      headroom rather than pinning at the new measurement, and record why beside the number."
fi

# ── 2. Key Decisions is an INTRO and ONE AREA TABLE, measured whole ───────────
#
# The digest left this file: decisions live as FENCES at the top of one register file per
# area (docs/decisions/<slug>.md), and this section names the areas. The fixed-shape rule
# stays — it replaced a markdown parser that shipped three rounds of fixes, each opening a
# fresh escape — and the shape is now smaller: plain prose, then exactly one table with the
# header `| Area | Fences |` and rows `| <name> | \`docs/decisions/<slug>.md#fences\` |`.
# Bullets, a second table, prose after the table, blockquotes, headings and fenced blocks
# are refused by name. Every one of them was a way past a check once.
#
# The table is the AUTHORITY for the closed set of areas and their order; the register
# INDEX and the rules are held to it below. An empty table is therefore a parse anchor
# missing, not an empty set: with no row, every check on the register iterates nothing and
# passes — so no row FAILS.
#
# Terminating conditions (each with a fixture): 2a no section  2b no table row  2c header
# not | Area | Fences |  2d malformed row  2e duplicate area name  2f duplicate slug  2g a
# second table  2h content after the table  2i bullet/list in the intro  2j blockquote in
# the intro  2k indented line in the intro  2l raw HTML in the intro  2m heading inside the
# section  2n fenced block  2o section over KEY_DECISIONS_MAX_BYTES  2p separator row missing
# 2q an | Area | Fences | table outside the section
KD_ROWS="${TMPDIR:-/tmp}/docs-lint-kdrows.$$"
awk -v scap="$KEY_DECISIONS_MAX_BYTES" '
  function bad(why) {
    printf "FAIL  Key Decisions, line %d: %s\n      The section is plain intro prose and ONE table (| Area | Fences |) whose rows name an area\n      and its register file — nothing else. A bullet, a second table, prose after the table, a\n      blockquote, a heading or a fenced block is refused: each was a way past this check once.\n      Offending line: %s\n", FNR, why, substr($0, 1, 60)
  }
  in_sec && /^## /    { in_sec = 0 }
  /^[ \t]*(```|~~~)/ { if (in_sec) { bytes += length($0) + 1; bad("a fenced block") }
                      fence = !fence; next }
  fence               { if (in_sec) bytes += length($0) + 1; next }
  !in_sec && /^## Key Decisions/ { if (!fence) { in_sec = 1; found = 1 } next }
  !in_sec && /^\|[ \t]*Area[ \t]*\|[ \t]*Fences[ \t]*\|/ {
    printf "FAIL  %s, line %d: an | Area | Fences | table outside the Key Decisions section. The section holds the\n      one area table; a second one elsewhere names areas nothing checks.\n", FILENAME, FNR; next }
  !in_sec             { next }
  { bytes += length($0) + 1; sub(/\r$/, "") }
  /^[ \t]*$/          { if (in_table) { in_table = 0; after_table = 1 } next }
  /^\|/ {
    if (after_table) { bad("a second table after the area table"); next }
    if (!in_table) {
      in_table = 1; want_sep = 1
      if ($0 !~ /^\|[ \t]*Area[ \t]*\|[ \t]*Fences[ \t]*\|[ \t]*$/) bad("a table whose header is not | Area | Fences |")
      next
    }
    if (want_sep) { want_sep = 0; if ($0 !~ /^\|[ \t]*:?-+:?[ \t]*\|[ \t]*:?-+:?[ \t]*\|[ \t]*$/) bad("a table without a |---|---| separator row under its header"); next }
    if ($0 ~ /^\|[ \t]*[^|`][^|`]*\|[ \t]*`docs\/decisions\/[A-Za-z0-9_-]+\.md#fences`[ \t]*\|[ \t]*$/) {
      name = $0; sub(/^\|[ \t]*/, "", name); sub(/[ \t]*\|.*$/, "", name)
      slug = $0; sub(/^.*`docs\/decisions\//, "", slug); sub(/\.md#fences`.*$/, "", slug)
      if (name in names) bad("a duplicate area name: " name)
      if (slug in slugs) bad("a duplicate register file: docs/decisions/" slug ".md")
      names[name] = 1; slugs[slug] = 1; rows++
      printf "ROW\t%s\t%s\n", name, slug
    } else bad("a row that is not | <area> | `docs/decisions/<slug>.md#fences` | — one area, one file, nothing else in the row")
    next
  }
  in_table            { in_table = 0; after_table = 1 }
  after_table         { bad("content after the area table"); next }
  /^[-*+][ \t]/       { bad("a bullet in the intro — a decision belongs in its area file as a fence"); next }
  /^[0-9]+[.)][ \t]/  { bad("an ordered-list item in the intro"); next }
  /^>/                { bad("a blockquote in the intro"); next }
  /^[ \t]+[^ \t]/     { bad("an indented line in the intro"); next }
  /^</                { bad("raw HTML in the intro"); next }
  /^#/                { bad("a heading inside the section — areas are table rows, not headings"); next }
  { next }
  END {
    if (!found)
      printf "FAIL  No `## Key Decisions` section found. It cannot be renamed, cased differently\n      or hidden behind an unclosed fence earlier in the file: every check on the register\n      goes quiet when the section cannot be located, which is a silent pass.\n"
    else if (!rows)
      printf "FAIL  Key Decisions has no | Area | Fences | table row. The table is the authority for the set\n      of areas, and with no row every check on the register iterates nothing — an empty table is\n      a missing parse anchor, not an empty set.\n"
    if (bytes > scap)
      printf "FAIL  The Key Decisions section is %d bytes (cap %d). It is an intro and one row per AREA —\n      a decision belongs in its area file as a fence, never here.\n", bytes, scap
  }
' "$CLAUDE" > "${TMPDIR:-/tmp}/docs-lint-kd.$$"
grep -v '^ROW	' "${TMPDIR:-/tmp}/docs-lint-kd.$$" >> "$FAILS" || true
grep '^ROW	' "${TMPDIR:-/tmp}/docs-lint-kd.$$" | cut -f2- > "$KD_ROWS" || true
rm -f "${TMPDIR:-/tmp}/docs-lint-kd.$$"

# ── 3, 4 & 5. The register: INDEX equals the table, every area file fences-first ──
#
# The inversion this repo shipped and did not catch — a digest that WAS the register, every
# settled decision full-length in the file that loads on every turn — is now structurally
# impossible: CLAUDE.md carries area names, and each area file carries its fences first and
# its entries behind them, so the label match is within one file. What is held instead:
#
#   3a docs/decisions.md still exists (a stub)   3b INDEX missing   3c INDEX has no row
#   3d INDEX rows differ from the table (name, slug or order)
#   4a a table row with no file   4b an area file with no row   4c H1 not "# <name> — decisions"
#   4d a `##` other than Contents and Fences   4e no Contents   4f Contents first item not Fences
#   4g no Fences / Fences not first after Contents   4h a fence of the wrong shape
#   4i a fence over DIGEST_MAX_BYTES   4j an unclosed fence label   4k a fence with no entry
#   4l an entry with no fence   4m an entry absent from Contents   4n a Fences pointer unresolved
#   4o an area file in a subdirectory   4p a second `## Contents`   4q a fence-shaped bullet outside Fences
#   4r a Contents anchor no entry carries   ((example) labels are exempt only in a file titled `# (example) …`)
#   5a an area declaring globs with no rule   5b a rule whose globs differ from Governs
#   5c a rule for an area declaring none   5d Governs neither `none` nor backticked globs
#   5e the file at decisions-<slug>.md is not this area's rule (body not template B for it)
[ ! -e "$OLD_REGISTER" ] || note "$OLD_REGISTER exists. The register is one file per area under $REGISTER_DIR/; there is no
      stub at the old path — a stale pointer must fail, not land on an empty file."
if [ ! -f "$REGISTER_INDEX" ]; then
  note "$REGISTER_INDEX is missing. The register is one file per area behind that index, and the
      Key Decisions table in $CLAUDE names the areas: without the index there is nothing to hold the
      table and the files to."
else
  IDX_ROWS="${TMPDIR:-/tmp}/docs-lint-idxrows.$$"
  # Rows of the INDEX table: | name | `docs/decisions/<slug>.md#fences` | governs |. Header and
  # separator rows carry no backticked fences path and fall out.
  awk '
    /^[ \t]*(```|~~~)/ { fence = !fence; next }
    fence { next }
    /^\|/ && /`docs\/decisions\/[A-Za-z0-9_-]+\.md#fences`/ {
      line = $0; sub(/\r$/, "", line)
      n = split(line, c, /\|/)
      # c[1] is empty (leading pipe); c[2] name, c[3] fences, c[4] governs
      name = c[2]; gsub(/^[ \t]+|[ \t]+$/, "", name)
      slug = c[3]; sub(/^.*`docs\/decisions\//, "", slug); sub(/\.md#fences`.*$/, "", slug)
      gov = (n >= 5) ? c[4] : ""; gsub(/^[ \t]+|[ \t]+$/, "", gov)
      printf "%s\t%s\t%s\n", name, slug, gov
    }
  ' "$REGISTER_INDEX" > "$IDX_ROWS"
  if [ ! -s "$IDX_ROWS" ]; then
    note "$REGISTER_INDEX has no | Area | Fences | Governs | row. With no row the register cannot be held to
      the table — an empty table is a missing parse anchor, not an empty set."
  elif ! cmp -s "$KD_ROWS" "$(cut -f1,2 "$IDX_ROWS" > "${TMPDIR:-/tmp}/docs-lint-idx2.$$"; echo "${TMPDIR:-/tmp}/docs-lint-idx2.$$")"; then
    note "$REGISTER_INDEX names areas that differ from the Key Decisions table in $CLAUDE — by name, file or
      order. The table is the authority: the index repeats its rows in the same order and adds only the
      Governs column.
      table:  $(tr '\n\t' '; ' < "$KD_ROWS")
      index:  $(cut -f1,2 "$IDX_ROWS" | tr '\n\t' '; ')"
  fi
  rm -f "${TMPDIR:-/tmp}/docs-lint-idx2.$$"

  # Every area file is a row, every row a file — and the register is one flat set, so it can be
  # enumerated by eye and by this loop; a file one directory deeper is invisible to both.
  refuse_deep_md "$REGISTER_DIR" "Area files are one flat set, one per table row;
      a file a level down is a register nothing checks."
  list_flat_md "$REGISTER_DIR" | while IFS= read -r f; do
    [ -f "$f" ] || continue
    [ "$f" = "$REGISTER_INDEX" ] && continue
    slug=$(basename "$f" .md)
    cut -f2 "$KD_ROWS" | grep -qxF "$slug" || note "$f is not a row in the Key Decisions table in $CLAUDE. An area file the table does not
      name is a register nobody is pointed at — add the row (here and in $REGISTER_INDEX), or move
      its decisions into an area that has one."
  done

  while IFS='	' read -r name slug; do
    [ -n "$slug" ] || continue
    f="$REGISTER_DIR/$slug.md"
    if [ ! -f "$f" ]; then
      note "$CLAUDE routes area \"$name\" to $f, which does not exist. A row with no file sends the next
      session nowhere."
      continue
    fi
    FENCES="${TMPDIR:-/tmp}/docs-lint-fences.$$"
    awk -v ucap="$DIGEST_MAX_BYTES" -v name="$name" -v file="$f" -v fences="$FENCES" '
      function trim(s) { sub(/^[ \t\r]+/, "", s); sub(/[ \t\r]+$/, "", s); return s }
      function anchor(s,   t) {
        t = tolower(trim(s)); gsub(/`/, "", t); gsub(/\*/, "", t)
        gsub(/[^a-z0-9 _-]/, "", t); gsub(/ /, "-", t); return t
      }
      function has_close(u,   s, j, k, p) {
        s = u; sub(/^- \*\*/, "", s); p = 1
        while ((j = index(substr(s, p), "**")) > 0) {
          k = p + j - 1
          if (substr(s, k + 2, 1) != "*") return (k > 1)
          p = k + 1
        }
        return 0
      }
      function label_of(u,   s, i, j, k, p, pad, orig) {
        s = u; sub(/^- \*\*/, "", s)
        while (match(s, /`[^`]*`/)) { pad = sprintf("%*s", RLENGTH, ""); s = substr(s, 1, RSTART - 1) pad substr(s, RSTART + RLENGTH) }
        i = 0; p = 1
        while ((j = index(substr(s, p), "**")) > 0) {
          k = p + j - 1
          if (substr(s, k + 2, 1) != "*") { i = k; break }
          p = k + 1
        }
        if (i <= 1) return ""
        orig = u; sub(/^- \*\*/, "", orig)
        return trim(substr(orig, 1, i - 1))
      }
      function flush(   n, l) {
        if (unit == "") return
        n = length(unit)
        if (n > ucap)
          printf "FAIL  %s: a fence is %d bytes (cap %d): %s…\n      A fence is the claim and its constraint in one line; the reasoning goes in the entry below it.\n", file, n, ucap, substr(unit, 1, 70)
        if (!has_close(unit))
          printf "FAIL  %s: a fence never closes its `**` label: %s…\n      An unclosed label yields no label at all, so nothing requires an entry for it.\n", file, substr(unit, 1, 70)
        else { l = label_of(unit); if (l != "" && (!example_file || l !~ /^\(example\)/)) fence_lbl[l] = 1 }
        unit = ""
      }
      function bad(why) {
        printf "FAIL  %s, line %d (Fences): %s\n      Fences are `- **Label** — …` bullets at column 0 with indented continuations and blank lines,\n      nothing else — every other construct was a way past the cap once.\n      Offending line: %s\n", file, FNR, why, substr($0, 1, 60)
      }
      { sub(/\r$/, "") }
      /^[ \t]*(```|~~~)/ { if (in_fences) { bad("a fenced block"); } fence = !fence; next }
      fence { next }
      FNR == 1 {
        example_file = ($0 ~ /^# \(example\)/)
        if ($0 != "# " name " — decisions")
          printf "FAIL  %s does not open with `# %s — decisions` (the name in the Key Decisions table). The\n      row, the title and any rule name the area identically.\n", file, name
        next
      }
      /^## / {
        if (in_fences) flush()
        in_fences = 0; in_toc = 0
        h = trim(substr($0, 4))
        if (h == "Contents") {
          if (toc_seen) printf "FAIL  %s carries a second `## Contents`. One Contents, at the top: a second one lower down would\n      let an entry be reachable from a list nobody reads first.\n", file
          toc_seen = 1; in_toc = 1; toc_first = 1
        }
        else if (h == "Fences") {
          fences_seen = 1; in_fences = 1
          if (!toc_seen) fences_before_toc = 1
          if (toc_seen && sections_since_toc > 0) fences_not_first = 1
        } else {
          printf "FAIL  %s carries a `## %s` section. An area file has exactly two: Contents, then Fences; the\n      entries are `###` headings. A fence stated as prose under another section is a fence nothing\n      checks.\n", file, h
        }
        if (toc_seen && h != "Contents") sections_since_toc++
        next
      }
      in_toc && /^[ \t]*---[ \t]*$/ { in_toc = 0; next }
      in_toc {
        if ($0 ~ /^[ \t]*$/) next
        if (toc_first) {
          toc_first = 0
          if (trim($0) != "- [Fences](#fences)") first_not_fences = 1
        }
        line = $0
        while (match(line, /\(#[a-z0-9_-]+\)/)) { seen[substr(line, RSTART + 2, RLENGTH - 3)] = 1; line = substr(line, RSTART + RLENGTH) }
        # A SECOND pass over the same line, for the row itself rather than its
        # reachability. Deliberately not folded into the loop above: narrowing that one to
        # the full `[text](#anchor)` form would stop a bare `(#anchor)` reaching `seen`, and
        # the entry it names would then be reported as absent from a Contents that lists it
        # — weakening a working check in order to add one. A link text containing `]` never
        # matches and is exempt in silence; balancing brackets here is markdown parsing,
        # which this script is the standing argument against.
        line = $0
        while (match(line, /\[[^]]*\]\(#[a-z0-9_-]+\)/)) {
          m = substr(line, RSTART, RLENGTH)
          line = substr(line, RSTART + RLENGTH)
          rt = m; sub(/^\[/, "", rt); sub(/\]\(#[a-z0-9_-]+\)$/, "", rt)
          ra = m; sub(/^.*\]\(#/, "", ra); sub(/\)$/, "", ra)
          # ONE-BASED, and the increment comes first. An uninitialised awk variable used as
          # a subscript is the empty string, not zero, so `rowtext[nrow]` with nrow unset
          # stored the first row under "" and a 0-based loop then read past it — the FIRST
          # Contents row went unchecked while every later one worked.
          if (ra != "fences" && (!example_file || rt !~ /^\(example\)/)) { nrow++; rowtext[nrow] = rt; rowanchor[nrow] = ra }
        }
        next
      }
      /^### / {
        if (in_fences) { flush(); in_fences = 0 }
        s = trim(substr($0, 5))
        allhead[anchor(s)] = 1
        want_label = 0
        if (!example_file || s !~ /^\(example\)/) { entry_lbl[s] = 1; head[anchor(s)] = s; cur_head = s; want_label = 1 }
        next
      }
      # The label is `.+`, not `[^*]+`: nine fences in this very register carry an italic or a
      # code span inside the label, and excluding `*` let every one of those shapes be restated
      # outside `## Fences`, where nothing cross-checks it — the plain-label control was caught
      # and the italic one was not. Greedy is right here because only the EXISTENCE of the fence
      # signature matters, not which `**` pair closes the label.
      !in_fences && !in_toc && /^- \*\*.+\*\* — / {
        printf "FAIL  %s, line %d: a fence-shaped bullet (`- **Label** — …`) outside `## Fences`. A fence stated\n      anywhere else is a fence nothing cross-checks; move it under Fences, or write the entry text\n      without the fence signature.\n", file, FNR; next
      }
      in_fences {
        print > fences
        if ($0 ~ /^[ \t]*---[ \t]*$/) { flush(); in_fences = 0; next }
        if ($0 ~ /^[ \t]*\r?$/) { flush(); next }
        if ($0 ~ /^- \*\*/) { flush(); unit = $0; next }
        if ($0 ~ /^- /) { bad("a bullet that does not open with a **bold label**"); next }
        if ($0 ~ /^[ \t]+[^ \t]/) { if (unit == "") { bad("indented line with no fence above it to continue"); next }
                                    x = $0; sub(/^[ \t]+/, "", x); unit = unit " " x; next }
        if ($0 ~ /^[|>]/) { bad("a table or blockquote row"); next }
        if ($0 ~ /^[0-9]+[.)][ \t]/) { bad("an ordered-list item"); next }
        if ($0 ~ /^[*+][ \t]/) { bad("a `*` or `+` bullet — use `-`"); next }
        bad("prose in the Fences section"); next
      }
      # ---- the opening bold label of an entry body ----
      # A decision heading is written down in four places here and two of them are checked
      # elsewhere: the Contents anchor and the heading itself. This is the one nothing else
      # can see. An entry can open its body with a bold label that disagrees with the heading
      # above it, which costs more here than it would elsewhere: the whole register model is
      # that a fence greps straight to its entry, and that bold label is what such a grep
      # lands on.
      #
      # Scoped to DISAGREEMENT, not to presence. An entry opening with plain prose is left
      # alone — the live register has such entries — so demanding the restatement would be a
      # gate inventing a rule the rules header of the register never stated. The escape is
      # therefore real and deliberate: delete the label and nothing fires. A heading with no
      # restatement contradicts nothing; a heading with the WRONG restatement contradicts
      # itself. These rules sit below the Fences block so that a line inside `## Fences`
      # never reaches them.
      !want_label { next }
      /^[ \t]*$/ { next }
      # A superseded marker is a BLOCKQUOTE, and the completion ritual puts one at every doc
      # site still stating the old claim — so it lands directly under the heading, on exactly
      # the path this check exists for: superseding is WHEN a heading gets renamed. Read as
      # the body line, it silences the stale label below it.
      /^[ \t]*>/ { next }
      {
        want_label = 0
        if ($0 !~ /^\*\*/) next
        s = $0; sub(/^\*\*/, "", s)
        lab = label_of("- **" s)
        if (lab == "")
          printf "FAIL  %s: \"### %s\" opens its body with a `**` that never closes.\n      An unclosed label yields no label at all, so the one line that has to restate the\n      heading is never compared against it. It opens and closes on the FIRST body line: a\n      label wrapped onto a second line reads here as one that never closes.\n", file, cur_head
        else if (lab != cur_head)
          printf "FAIL  %s: \"### %s\" opens its body with the label \"%s\".\n      The bold label opening an entry restates its heading, and is what a fence greps to. A\n      label that does not match its heading is visible to no other check here: the heading is\n      right, the Contents row is right, and the two disagree only with each other.\n      A superseded marker belongs in a blockquote above the label, where it is skipped.\n", file, cur_head, lab
        next
      }
      END {
        flush()
        if (!toc_seen) printf "FAIL  %s has no `## Contents`. Fences and entries are found through it; without it the file is\n      read whole, which is the cost the layering exists to avoid.\n", file
        else if (first_not_fences) printf "FAIL  %s: the first item of Contents is not `- [Fences](#fences)`. Fences come first so a session that\n      opens the file for them stops reading after them.\n", file
        if (!fences_seen) printf "FAIL  %s has no `## Fences` section. Every area file opens with its fences — the one-line claims\n      the always-loaded tier used to carry.\n", file
        else if (fences_before_toc || fences_not_first) printf "FAIL  %s: `## Fences` is not the first section after `## Contents`. Fences first, entries after.\n", file
        for (l in fence_lbl) if (!(l in entry_lbl))
          printf "FAIL  %s: fence \"%s\" has no `### %s` entry in the same file.\n      Entry first, fence second: a fence is never the only home of a fact.\n", file, l, l
        for (l in entry_lbl) if (!(l in fence_lbl))
          printf "FAIL  %s: entry \"### %s\" has no fence under `## Fences`.\n      An entry no fence points at is a decision that gets re-litigated.\n", file, l
        for (a in head) if (!(a in seen))
          printf "FAIL  %s: \"### %s\" is absent from the Contents — findable only by reading the whole file,\n      which is the cost the layering exists to avoid.\n", file, head[a]
        for (a in seen) if (a != "fences" && !(a in allhead))
          printf "FAIL  %s: Contents links to #%s, which no `###` entry carries. A stale anchor sends the reader to\n      the top of the file; fix the link or the heading.\n", file, a
        # Indexed rather than `for (i in rowtext)`: awk iterates an associative array in an
        # unspecified order, so two bad rows would report in a different order on a different
        # awk and the corpus would be flaky on exactly the machines it is meant to protect.
        for (i = 1; i <= nrow; i++) {
          if (anchor(rowtext[i]) == rowanchor[i]) continue
          if (rowanchor[i] in head)
            printf "FAIL  %s Contents: the row named \"%s\" links to \"#%s\", the anchor of \"### %s\".\n      A row that names one decision and points at another reads as correct from either end,\n      because the link still works — nothing looks broken until a reader trusts the name.\n", file, rowtext[i], rowanchor[i], head[rowanchor[i]]
          else
            printf "FAIL  %s Contents: the row named \"%s\" links to \"#%s\", but its own anchor is\n      \"#%s\". The Contents row is the only place a name and the link under it are written\n      side by side, so a disagreement between the two is checkable nowhere else.\n", file, rowtext[i], rowanchor[i], anchor(rowtext[i])
        }
      }
    ' "$f" >> "$FAILS"
    [ -f "$FENCES" ] && check_pointers "$FENCES" "$f (Fences)"
    rm -f "$FENCES"
  done < "$KD_ROWS"

  # 5 — Governs ↔ rules. An area that governs a tree has a rule with exactly those globs; an
  # area that governs none has no rule. Either direction missing fails by name.
  while IFS='	' read -r name slug gov; do
    [ -n "$slug" ] || continue
    rule="$RULES_DIR/decisions-$slug.md"
    gov=$(printf '%s' "$gov" | sed -e 's/^`none`$/none/')
    if [ "$gov" = "none" ]; then
      [ ! -e "$rule" ] || note "$rule exists, but $REGISTER_INDEX says area \"$name\" governs none. Declare the globs
      in the Governs column, or delete the rule."
      continue
    fi
    want=$(printf '%s\n' "$gov" | tr ',' '\n' | sed -e 's/^[ \t]*`//' -e 's/`[ \t]*$//' | grep -v '^$' | sort -u)
    if [ -z "$want" ] || printf '%s\n' "$gov" | grep -qv '`'; then
      note "$REGISTER_INDEX: area \"$name\" has Governs \"$gov\", which is neither \`none\` nor a comma-separated
      list of backticked globs."
      continue
    fi
    if [ ! -f "$rule" ]; then
      note "$REGISTER_INDEX says area \"$name\" governs $gov, but $rule does not exist. A governed tree has a
      path-scoped rule with exactly those globs, so the fences fire when a matching file is opened."
      continue
    fi
    # The file must BE this area's rule — template B naming this area — not merely a rule
    # with matching globs. Area rules carry the decisions- prefix so they can never collide
    # with the process rules that share the directory.
    rbody=$(tr -d '\r' < "$rule" | awk 'NR == 1 { next } b { print; next } /^---$/ { b = 1 }')
    rexpect=$(printf 'Files matching these paths are governed by the **%s** decisions in `docs/decisions/%s.md`.\nRead its fences first — they are not loaded with CLAUDE.md.' "$name" "$slug")
    if [ "$rbody" != "$rexpect" ]; then
      note "$rule is not the rule for area \"$name\": its body is not the area template naming
      docs/decisions/$slug.md. A rule is bound to its area by its body, not by its globs."
      continue
    fi
    have=$(tr -d '\r' < "$rule" | awk 'NR == 1 { next } /^---$/ { exit } { print }' | sed -n 's/^  - "\([^"]*\)"$/\1/p' | sort -u)
    [ "$want" = "$have" ] || note "$rule lists globs that differ from what $REGISTER_INDEX says area \"$name\" governs.
      Governs: $(printf '%s' "$want" | tr '\n' ' ')
      rule:    $(printf '%s' "$have" | tr '\n' ' ')"
  done < "$IDX_ROWS"
  rm -f "$IDX_ROWS"
fi

# ── 6 & 7. The delivery tier ───────────────────────────────────────────────────
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
      Step 3 of the completion ritual: what shipped belongs one tier down, not in a fence."
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

# ── 8. Pointers out of the always-loaded files ─────────────────────────────────
# The always-loaded set only, deliberately. A pointer that goes nowhere defeats the
# layering these files defend: a session sent to a missing area file reads the fence in the
# router and stops there. Link-checking every doc in the repo is a different job with a far wider
# false-positive surface, and is not this script business.
#
# This parser is code-span-BLIND on purpose: a pointer written in backticks is still one a
# reader follows, so it must resolve. Check 9 below has a code-span-AWARE parser for the
# opposite question (what does Claude Code actually IMPORT), and the two are different by
# design — do not unify them.
check_pointers "$CLAUDE"

# ── 9–13. The routed process tier: router, imports, parts, rules, agents ───────
#
# `docs/process/` is one file per part behind a router (INDEX.md). Two of them load WITH
# CLAUDE.md as bare `@` imports — the router "Loaded every session" table is the authority
# for which — and the rest are pulled when the router "One file per part" table says.
# Every check here holds a shape the routing depends on. All are gated on the DIRECTORY
# existing, never on the router file: gating on the file would let one change delete the
# router and the two import lines together and take every check below quiet with it. A
# directory with no router fails by name.
#
# Terminating conditions, each with a fixture in tests/docs-lint/ — count them, do not
# guess (a check with two exits is satisfied by a fixture exercising either):
#   9a  the table does not name CLAUDE.md          9b  a table row CLAUDE.md does not import
#   9c  an import the table does not name           9d  the set is over ALWAYS_LOADED_MAX_BYTES
#   9e  a bare @ import in a non-CLAUDE set file    9f  a pointer in a set file goes nowhere
#   10a a part with no router row                   10b a router row with no file
#   11  a stub at the old single-file path           11b docs/process/ exists with no router
#   9h  no Loaded-every-session table (heading is a parse anchor)   10c no One-file-per-part table
#   12a frontmatter not at byte 0                   12b paths missing or written inline
#   12c an illegal glob                             12d a glob matching no file
#   12e a body that is not the template             12f the template names an unrouted part
#   12g a rule in a subdirectory                    12h an unexpected frontmatter key
#   12i an area body that is not template B           12j area rule naming a non-row area
#   12k area rule not named <slug>.md
#   13a an agent with no frontmatter                13b name not equal to the file stem
#   13c a model outside the allowed set             13d a routed agent with no file
#   13e an agent file the routing table omits       13f agents present with no routing table
#   12c also covers a trailing-slash glob; CRLF rule files are normalised before comparing (crlf -ok case)
if [ -d "$PROCESS_DIR" ]; then
  # 11 — no stub. A file at the old path is where a stale pointer lands softly and a new
  # paragraph accretes; a stale pointer should FAIL check 8, not find an empty file.
  [ ! -e "$OLD_PROCESS" ] || note "$OLD_PROCESS exists beside $PROCESS_DIR/. There is no stub at the old
      path: a stale pointer must fail, not land on an empty file, and a new paragraph must land
      in a routed part."
  if [ ! -f "$ROUTER" ]; then
    note "$PROCESS_DIR/ exists with no $ROUTER. The router is the authority for what loads every
      session and for which parts exist; without it every check on the routed tier is blind."
  else
    # Table rows: the first backticked token of each `| ... |` row under a heading, until
    # the next `## `. Header and separator rows carry no backtick and fall out on their own.
    table_paths() {
      awk -v h="$2" '
        index($0, h) == 1 { t = 1; next }
        t && /^## / { t = 0 }
        t && /^\|[ \t]*`[^`]+`/ { match($0, /`[^`]+`/); print substr($0, RSTART + 1, RLENGTH - 2) }
      ' "$1"
    }
    LOADED="${TMPDIR:-/tmp}/docs-lint-loaded.$$"; PARTS="${TMPDIR:-/tmp}/docs-lint-parts.$$"
    IMPORTS="${TMPDIR:-/tmp}/docs-lint-imports.$$"
    table_paths "$ROUTER" "## Loaded every session" | sort -u > "$LOADED"
    table_paths "$ROUTER" "## One file per part"    | sort -u > "$PARTS"
    # 9h / 10c — the two headings are PARSE ANCHORS. A renamed or re-cased heading yields an
    # empty table, and an empty table would make every check below report a confident falsehood
    # (rows "missing" that are present, rules "unrouted" that are fine). Assert the anchor and
    # skip what depends on it; the Key Decisions check guards its own heading the same way.
    have_loaded=1; have_parts=1
    if [ ! -s "$LOADED" ]; then
      have_loaded=0
      note "$ROUTER has no \`## Loaded every session\` table with backticked-path rows. The heading is a
      parse anchor (exact text, exact case) and the rows must backtick the path — without it the
      always-loaded set cannot be derived and checks 9a–9f do not run."
    fi
    if [ ! -s "$PARTS" ]; then
      have_parts=0
      note "$ROUTER has no \`## One file per part\` table with backticked-path rows. The heading is a
      parse anchor (exact text, exact case) and the rows must backtick the path — without it no part
      is routed and checks 10a and 12f do not run."
    fi

    # 9a — the set must name the file whose imports define it.
    [ "$have_loaded" -eq 0 ] || grep -qx "$CLAUDE" "$LOADED" || note "$ROUTER: the Loaded-every-session table does not name $CLAUDE (rows must
      backtick the path). The table is the authority for the always-loaded set, and the set starts
      with the file that imports the rest."

    # Imports: bare `@path.md` outside code spans and fences — what Claude Code actually loads.
    # Code-span-AWARE, unlike check 8, and on purpose (see the note there).
    bare_imports() {
      awk '
        /^[ \t]*(```|~~~)/ { fence = !fence; next }
        fence { next }
        {
          line = $0
          while (match(line, /`[^`]*`/)) {
            pad = sprintf("%*s", RLENGTH, "")
            line = substr(line, 1, RSTART - 1) pad substr(line, RSTART + RLENGTH)
          }
          while (match(line, /@[A-Za-z0-9_.\/-]+\.md/)) {
            pre = (RSTART > 1) ? substr(line, RSTART - 1, 1) : " "
            p = substr(line, RSTART + 1, RLENGTH - 1)
            line = substr(line, RSTART + RLENGTH)
            if (pre !~ /[A-Za-z0-9]/) print p
          }
        }
      ' "$1" | sort -u
    }
    bare_imports "$CLAUDE" > "$IMPORTS"
    if [ "$have_loaded" -eq 1 ]; then
    # 9b / 9c — the two sets agree row for row.
    while IFS= read -r p; do
      [ -n "$p" ] && [ "$p" != "$CLAUDE" ] || continue
      grep -qxF "$p" "$IMPORTS" || note "$ROUTER names \"$p\" as loaded every session, but $CLAUDE does not import
      it (a bare @$p outside backticks). The table and the imports must agree row for row."
    done < "$LOADED"
    while IFS= read -r p; do
      [ -n "$p" ] || continue
      grep -qxF "$p" "$LOADED" || note "$CLAUDE imports \"$p\" (a bare @ outside backticks), which the router
      Loaded-every-session table does not name (rows must backtick the path). An import IS an
      always-loaded file — add the row and make the case, or put the pointer in backticks."
    done < "$IMPORTS"
    # 9d / 9e / 9f — over the set: budget, no nested imports, pointers resolve.
    total=0
    while IFS= read -r p; do
      [ -n "$p" ] || continue
      [ -f "$p" ] || continue   # a missing set file is reported by 9b/check 8, not here
      n=$(wc -c < "$p" | tr -d '[:space:]'); total=$((total + n))
      if [ "$p" != "$CLAUDE" ]; then
        if [ -s "$(bare_imports "$p" > "${TMPDIR:-/tmp}/docs-lint-nested.$$"; echo "${TMPDIR:-/tmp}/docs-lint-nested.$$")" ]; then
          note "$p carries a bare @ import ($(head -1 "${TMPDIR:-/tmp}/docs-lint-nested.$$")). Only $CLAUDE imports; an import in
      an always-loaded file pulls its target in at launch too, through recursion, which is the cost
      this layout exists to avoid. Put the pointer in backticks."
        fi
        rm -f "${TMPDIR:-/tmp}/docs-lint-nested.$$"
        check_pointers "$p"
      fi
    done < "$LOADED"
    [ "$total" -le "$ALWAYS_LOADED_MAX_BYTES" ] || note "The always-loaded set ($(tr '\n' ' ' < "$LOADED")) is $total bytes against a
      $ALWAYS_LOADED_MAX_BYTES budget. Every byte here is paid on every session: move a part behind
      the router, or shorten what the table names — and re-ratchet with headroom, never to fit the
      edit in hand."
    fi
# 10a / 10b — router rows and part files are the same set. Flat, and enumerated with find
    # rather than a glob: a part one directory down is where the next paragraph of process
    # accretes unread, which is the reason 10a exists, and a dotfile is invisible to `*.md`.
    refuse_deep_md "$PROCESS_DIR" "The method is one flat set, one file per router row;
      a part a level down is routed by nothing and read by no one."
    list_flat_md "$PROCESS_DIR" | while IFS= read -r f; do
      [ -f "$f" ] || continue
      [ "$f" = "$ROUTER" ] && continue
      grep -qxF "$f" "$LOADED" && continue   # always-loaded parts are routed by the OTHER table
      [ "$have_parts" -eq 1 ] && [ "$have_loaded" -eq 1 ] || continue   # a missing table is reported once above, not once per part
      grep -qxF "$f" "$PARTS" || note "$f is not a row in the router One-file-per-part table. An unrouted part is where
      the next paragraph of process accretes unread — add the row (what it holds, when to pull it)."
    done
    while IFS= read -r p; do
      [ -n "$p" ] || continue
      [ -f "$p" ] || note "$ROUTER routes to \"$p\", which does not exist. A row with no file sends the next
      session nowhere."
    done < "$PARTS"

    # 12 — rules: one path-scoped pointer per governed tree, body equal to the template.
    if [ -d "$RULES_DIR" ]; then
      # The WORKING TREE, not the tracked set: `git ls-files` would be deterministic, and would
      # also be empty in the corpus harness, which runs this script over a plain directory. The
      # cost is stated rather than hidden — a glob matching only untracked or ignored files
      # (a venv, a build dir) passes here and fails on a clean checkout, so 12d means "matches
      # a file you have", not "matches a file the repo has".
      FILES="${TMPDIR:-/tmp}/docs-lint-files.$$"
      find . -path ./.git -prune -o -type f -print | sed 's|^\./||' > "$FILES"
      # Does any file match a glob of one of the four allowed forms? Anchored on a literal
      # prefix and a literal extension, so no glob engine is involved and nothing is
      # translated: a translation is where two implementations disagree.
      glob_hits() {
        case "$1" in
          */'**')      p="${1%/\*\*}/";              awk -v p="$p" 'index($0, p) == 1 { f = 1; exit } END { exit !f }' "$FILES" ;;
          */'**/*.'*)  p="${1%/\*\*/\*.*}/"; e=".${1##*/\*\*/\*.}"
                       awk -v p="$p" -v e="$e" 'index($0, p) == 1 && substr($0, length($0) - length(e) + 1) == e { f = 1; exit } END { exit !f }' "$FILES" ;;
          */'*.'*)     p="${1%/\*.*}/"; e=".${1##*/\*.}"
                       awk -v p="$p" -v e="$e" 'index($0, p) == 1 && index(substr($0, length(p) + 1), "/") == 0 && substr($0, length($0) - length(e) + 1) == e { f = 1; exit } END { exit !f }' "$FILES" ;;
          *)           grep -qxF "$1" "$FILES" ;;
        esac
      }
      refuse_deep_md "$RULES_DIR" "Rules are one flat set, one per governed tree, so the set can be
      enumerated by eye and by this script."
      list_flat_md "$RULES_DIR" | while IFS= read -r r; do
        # 12a — frontmatter opens the file. Anything before it makes the rule ALWAYS-loaded.
        if [ "$(sed -n '1p' "$r" | tr -d '\r')" != "---" ]; then
          note "$r does not open with frontmatter at byte 0. Without it Claude Code loads the rule
      into EVERY session, which turns a scoped pointer into always-loaded prose."
          continue
        fi
        fm=$(tr -d '\r' < "$r" | awk 'NR == 1 { next } /^---$/ { exit } { print }')
        body=$(tr -d '\r' < "$r" | awk 'NR == 1 { next } b { print; next } /^---$/ { b = 1 }')
        # 12b / 12h — the frontmatter is `paths:` and its globs, nothing else.
        printf '%s\n' "$fm" | grep -qx 'paths:' || { note "$r has no \`paths:\` block list in its frontmatter (an inline \`paths: [...]\`
      is refused too). A rule without a block-list paths is always-loaded, or unparseable — both
      silently."; continue; }
        printf '%s\n' "$fm" | grep -v -E '^paths:$|^  - "[^"]+"$' | grep -q . && note "$r carries a frontmatter line that is not \`paths:\` or a \`  - \"glob\"\` entry:
      $(printf '%s\n' "$fm" | grep -v -E '^paths:$|^  - "[^"]+"$' | head -1). A rule is a pointer and nothing else."
        # 12c / 12d — each glob is one of four literal-prefixed forms, and matches something.
        printf '%s\n' "$fm" | sed -n 's/^  - "\([^"]*\)"$/\1/p' | while IFS= read -r g; do
          case "$g" in
            *'{'*|*'}'*|*'['*|*']'*|'*'*|'/'*|*'/'|'.'|'..'|*'/./'*|*'/../'*)
              note "$r: glob \"$g\" is not allowed. Globs are \`dir/**\`, \`dir/**/*.ext\`, \`dir/*.ext\` or
      an exact path, with a literal first segment — braces and brackets are where two glob engines
      disagree, a leading wildcard scopes a rule to the whole repo, and a trailing slash names a
      directory, which no rule can match."; continue ;;
            */'**'|*/'**/*.'*|*/'*.'*) ;;
            *'*'*|*'?'*) note "$r: glob \"$g\" is not one of the four allowed forms (\`dir/**\`, \`dir/**/*.ext\`,
      \`dir/*.ext\`, exact path)."; continue ;;
          esac
          glob_hits "$g" || note "$r: glob \"$g\" matches no file in the repo. A rule for a tree that does not exist
      never fires and is still advertised as a backstop."
        done
        # 12e / 12f — the body IS one of two templates: A, rendered for a routed process part;
        # B, rendered for a Key Decisions area (name and slug from the table, filename = slug).
        part=$(printf '%s\n' "$body" | sed -n '1s/^Files matching these paths are governed by `docs\/process\/\([A-Za-z0-9_-]*\)\.md`\.$/\1/p')
        expect=$(printf 'Files matching these paths are governed by `docs/process/%s.md`.\nRead it before changing one — it is not loaded with CLAUDE.md.' "$part")
        aslug=$(printf '%s\n' "$body" | sed -n '1s/^Files matching these paths are governed by the \*\*.*\*\* decisions in `docs\/decisions\/\([A-Za-z0-9_-]*\)\.md`\.$/\1/p')
        if [ -n "$aslug" ]; then
          aname=$(printf '%s\n' "$body" | sed -n '1s/^Files matching these paths are governed by the \*\*\(.*\)\*\* decisions in `docs\/decisions\/.*$/\1/p')
          bexpect=$(printf 'Files matching these paths are governed by the **%s** decisions in `docs/decisions/%s.md`.\nRead its fences first — they are not loaded with CLAUDE.md.' "$aname" "$aslug")
          if [ "$body" != "$bexpect" ]; then
            note "$r: the body is not the two-line area template (\"Files matching these paths are governed by the
      **<Area>** decisions in \`docs/decisions/<slug>.md\`.\" / \"Read its fences first — they are not loaded
      with CLAUDE.md.\")."
          elif ! grep -qxF "$(printf '%s\t%s' "$aname" "$aslug")" "$KD_ROWS" 2>/dev/null; then
            note "$r names area \"$aname\" in docs/decisions/$aslug.md, which is not a row of the Key Decisions
      table in $CLAUDE (name and file must both match a row)."
          elif [ "$(basename "$r" .md)" != "decisions-$aslug" ]; then
            note "$r points at docs/decisions/$aslug.md but is not named decisions-$aslug.md. An area rule is named
      decisions-<slug>.md — for its register file, and with a prefix no process rule can collide with."
          fi
          continue
        fi
        if [ -z "$part" ] || [ "$body" != "$expect" ]; then
          note "$r: the body is not the two-line process template (\"Files matching these paths are governed by
      \`docs/process/<part>.md\`.\" / \"Read it before changing one — it is not loaded with CLAUDE.md.\") nor
      the area template (\"…governed by the **<Area>** decisions in \`docs/decisions/<slug>.md\`.\").
      A committed rule reaches every teammate on every matching Read; a template leaves no room for a
      line that is not a pointer."
        elif [ "$have_parts" -eq 1 ] && ! grep -qxF "$PROCESS_DIR/$part.md" "$PARTS"; then
          note "$r points at docs/process/$part.md, which is not a row in the One-file-per-part table. A rule
      may only route to an on-demand part — an always-loaded file needs no rule, and a pointer to it
      would say something false about when it is read."
        fi
      done
      rm -f "$FILES"
    fi

    # 13 — agents: the roles the routing table names, each carrying an allowed model.
    # 13d runs whenever the TABLE exists, whether or not any agent file does: with the
    # roles directory gone entirely, a table routing to four ghosts must still fail.
    ROUTED="${TMPDIR:-/tmp}/docs-lint-routed.$$"
    : > "$ROUTED"
    have_routing_table=1
    if [ -f "$ROUTING" ]; then
      awk -F'|' '
        index($0, "## The table") == 1 { t = 1; next }
        t && /^## / { t = 0 }
        t && NF >= 4 {
          c = $4
          while (match(c, /`[a-z][a-z0-9-]*`/)) { print substr(c, RSTART + 1, RLENGTH - 2); c = substr(c, RSTART + RLENGTH) }
        }
      ' "$ROUTING" | sort -u > "$ROUTED"
      # 13f — the heading is a PARSE ANCHOR (exact text, exact case), like the two in the
      # router. Renaming it yields an empty roster, and an empty roster makes 13e report one
      # confident falsehood per agent file — "not named in the routing table" for a table that
      # names them all. Assert the anchor and skip what depends on it.
      if [ ! -s "$ROUTED" ]; then
        have_routing_table=0
        note "$ROUTING has no \`## The table\` section with backticked agent names in its Agent column.
      The heading is a parse anchor: with no roster, every check that depends on it reports a
      falsehood — one \"not named in the routing table\" per role — instead of naming this."
      fi
      while IFS= read -r n; do
        [ -n "$n" ] || continue
        [ -f "$AGENTS_DIR/$n.md" ] || note "$ROUTING routes to agent \`$n\`, but $AGENTS_DIR/$n.md does not exist."
      done < "$ROUTED"
    fi
    # 13g — one flat set, the same guard the other three trees carry. Claude Code scans this
    # directory RECURSIVELY, so a file one level down is loaded into a session while every
    # check below — allowed model, name equals stem, named by the routing table — would
    # never see it; a dotfile role escaped the same way until this used list_flat_md.
    if [ -d "$AGENTS_DIR" ]; then
      refuse_deep_md "$AGENTS_DIR" "Roles are one flat set, one per row of the routing table:
      Claude Code loads a role a level down, and the checks here would not see it, so the file would
      be live and unchecked at the same time."
    fi
    if [ -d "$AGENTS_DIR" ] && [ -n "$(list_flat_md "$AGENTS_DIR")" ]; then
      if [ ! -f "$ROUTING" ]; then
        note "$AGENTS_DIR/ has agent files but $ROUTING does not exist. The routing table is what says
      which job each agent and model is for; agents without it are habit, not routing."
      else
        list_flat_md "$AGENTS_DIR" | while IFS= read -r a; do
          stem=$(basename "$a" .md)
          if [ "$(sed -n '1p' "$a" | tr -d '\r')" != "---" ]; then
            note "$a has no frontmatter at byte 0, so it declares no name and no model — the
      routing table cannot bind to it."; continue
          fi
          name=$(tr -d '\r' < "$a" | awk 'NR == 1 { next } /^---$/ { exit } /^name:/ { sub(/^name:[ \t]*/, ""); print }')
          model=$(tr -d '\r' < "$a" | awk 'NR == 1 { next } /^---$/ { exit } /^model:/ { sub(/^model:[ \t]*/, ""); print }')
          [ "$name" = "$stem" ] || note "$a declares name \"$name\" but is named $stem.md. The routing table names
      agents by file stem; a mismatch routes to nothing."
          case "$model" in
            sonnet|opus|haiku|fable) ;;
            *) note "$a declares model \"$model\". Allowed: sonnet, opus, haiku, fable. \`inherit\` and an empty
      model both take whatever the parent runs on, which is exactly what routing by job exists to stop." ;;
          esac
          [ "$have_routing_table" -eq 1 ] || continue   # a missing anchor is reported once above, not once per agent
          grep -qxF "$stem" "$ROUTED" || note "$a is not named in $ROUTING (## The table, Agent column). An agent the table
      does not route to is a role nobody is told to use."
        done
      fi
    fi
    rm -f "$ROUTED"
    rm -f "$LOADED" "$PARTS" "$IMPORTS"
  fi
fi


# ── 14. A dated measurement in the standing tier carries an anchor ────────────
#
# completion-ritual.md: "Standing rules never cite volatile numbers ... Dating the measurement
# does not save it." That rule was stated, and violated in the tier it governs, at eight
# sites at once — including that section itself, which carried the dated-measurement
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
# pyproject.toml sites carried a bare count with no date at all; the spec-authoring rules wrote the
# date six words from the word; the completion ritual carried a byte pair with no date. So this
# check does not close the defect class. It closes the sub-shape that DATING creates: a
# number sitting beside a recent date, which is the form that reads as current and
# therefore never gets re-checked. An undated number at least still looks like something
# to verify.
#
# Narrow on purpose, and the narrowness is a false-negative choice, not a coverage claim.
# A SIBLING TRIGGER WAS BUILT AND REJECTED ON MEASUREMENT, not on taste: a transition
# ("146 -> 222") whose unit carries fewer than two distinct SHAs. It failed both ways at
# once. It fired on CODE — an awk program inside this script and two in spec-lint.sh, where
# a digit, a `->` and another digit share a block — and it stayed SILENT on the exact site
# it was written for, because the unit is the whole comment block and a neighbouring
# sentence three lines up carried two SHAs of its own. Catching that site needs
# sentence-granularity, which is the machinery check 15 pays python3 for. What replaced it
# is a rule rather than a check: a standing comment states the current measurement with one
# anchor, or states the principle — it does not write a transition with one end anchored
# and the other on the word "here", which is the shape that went stale one commit later.
#
# A trigger wide enough to catch the other seven would have to fire on any number near
# any date, which is ordinary prose in every one of these files — and a doc gate that
# fires on ordinary prose is a doc gate that gets commented out. The rest stays with
# completion-ritual.md, to be caught by reading. If a future sweep finds a second idiom actually
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
    # A NUMERIC TRANSITION ("N -> M") is held only where the commit-anchor convention is
    # settled: the two linters and the always-loaded files. The register is out for the same
    # reason src/ is out of the check above — its entries anchor to SPEC numbers by
    # convention, and whether a spec number counts as an anchor is a decision nobody has
    # taken; taking it here by accident is what that exclusion exists to prevent. Recorded,
    # not chased. The scope is exactly where the class recurred: both sites that shipped an
    # unanchored transition were in these files.
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
      # ONE short word may sit between the verb and the date, because two sentences written
      # while this very rule was being documented spelled it "Measured here on 2026-09-06"
      # and escaped — a false negative found by reading the diff, which is the reader this
      # check exists to relieve. Bounded to a single [a-z]+ of at most eight characters and
      # no punctuation: that admits "here", "again", "once", and stops well short of a
      # clause, which is where an unrelated date in the same sentence would start matching.
      function dated(s) {
        return tolower(s) ~ /(measured|as of)[ \t]*[,:]?[ \t]*([a-z]{1,8}[ \t]+)?((on|at|in)[ \t]+)?20[0-9][0-9]-[0-9][0-9]-[0-9][0-9]/
      }
      function flush(   where, what) {
        if (unit != "" && dated(unit) && !has_anchor(unit)) {
          where = hitline ? hitline : ustart
          what  = hitline ? hittext : unit
          printf "FAIL  %s:%d carries a dated measurement with no commit to re-measure from.\n      %s\n      A date says WHEN someone looked, not at what: a dated number still reads as\n      current to anyone not checking it against a calendar. Either drop the number and\n      state the principle, or anchor the evidence to a short commit SHA in the same\n      bullet, paragraph or comment block. A version tag is not accepted; the comment\n      beside this check says why. Quoting the wrong form on purpose? Put it in a fenced\n      block, which this check skips. See completion-ritual.md, never cite volatile numbers.\n",
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


# ── 15. A universal claim about the marking walk carries its scope ────────────
#
# `_lifecycle._mark_inherited()` is described in prose across sixteen files in src/, docs/ and
# tests/, and a large share of those descriptions said the same false thing: that it stamps
# EVERYTHING a forked child inherited `_FOREIGN`. It `setdefault`s, so a sink `configure()`
# already stamped keeps the parent's real pid. PR #218 corrected it across twelve files over four
# rounds and three fresh-context review frames, every round finding defects in the previous
# round's fixes — including two in src/ that all three frames read past. Nothing mechanical
# noticed that sixteen descriptions of one mechanism disagreed with the code and with each other.
# This is the one spelling that recurred, where a script can see it (SPEC-053 FR-001).
#
# WHAT IT CATCHES, exactly: a sentence that NAMES the mechanism, quantifies what the walk acts on
# with a bare universal, and attaches no restriction to it. All four parts are load-bearing and
# each was measured; the numbers below are reproducible with `git archive <sha> | tar -x`.
#
#   ANCHOR    _mark_inherited, `_FOREIGN`, ``_FOREIGN``, or the phrase "marking walk". NAMED
#             symbols only. A descriptive anchor — one that also matches "a forked child",
#             "it inherited", "inherited sink" — is rejected: every widening tried took the
#             clean-tree count from 0 into the tens or hundreds against zero true positives,
#             which is the shape process.md 5 warns about, half a gate's regressions being
#             false positives.
#   UNIVERSAL every, everything, each, all. NOT any/always/never, and that is measured rather than
#             assumed: they buy no true positive this rule can discriminate and they are the
#             mechanism of every false positive on the corrected tree — "before ANY other
#             handler runs" appears identically in the false register sentence and in its
#             correction, "far above ANY real graph" is correct prose, and "a sink it NEVER
#             acquired" is correct in three of the nine sentences that fire without scoping.
#   SUBJECT   the universal must quantify what the walk ACTS ON — a sink, a record, something
#             inherited or stamped, a transport, a descriptor, an object — or be governed
#             directly by a marking verb, as in "refuses everything". Every alternative in
#             each of these lists is a STEM with `\w*` after it, and that is a testability
#             decision, not brevity: a list of inflections is a list of members no fixture
#             exercises, and the corpus owes one per alternative. Without this the rule
#             needs only an anchor and a universal to CO-OCCUR, and it then fires on ordinary
#             prose that happens to mention the walk: "all of the buffer repair happens after
#             it", "every one of the sixteen files describing the marking walk was corrected".
#             Measured: adding this clause took the shipping tree from 3 hits to 1.
#   SCOPE     a restriction inside the QUANTIFIED NOUN PHRASE excuses the universal — a relative
#             (that/which/whose/it/its) or one of the scoping words this repo actually writes
#             (reach, missed, residual, partial, setdefault, item 7).
#
# THE SCOPE TEST IS POSITIONAL, AND THAT IS THE WHOLE DIFFERENCE BETWEEN A GATE AND A NUISANCE.
# Tested against the whole sentence — the obvious spelling — a scoping word ANYWHERE excuses a
# universal ANYWHERE, so the word an author reaches for while fixing this very defect is the word
# that silences the check. Measured twice over: inserting a scoping clause into the six known-bad
# sentences, WITHOUT touching their claims, silences the sentence-wide form on 6 of 6, while the
# positional form survives every insertion a null edit can make. And it already happened for
# real: `reclaim`'s docstring
# said "_mark_inherited has already stamped everything inherited ``_FOREIGN``" and the
# sentence-wide form MISSED it, silenced by the word `setdefault` fourteen words later in its own
# sentence. The positional form catches it.
#
# The positional form's own bound is stated as a property rather than a score, because a score
# here names no instrument: an insertion anywhere OUTSIDE the quantified noun phrase leaves the
# sentence firing, and one inside it silences the check by construction. That is what "bound to
# the noun phrase" means; it is not a leak in it. Anyone can re-derive it from the rule.
#
# A NEGATED UNIVERSAL IS NOT A CLAIM, and it is the CORRECTING sentence that needs this. "does not
# stamp every inherited sink" reads to a regex exactly like the false claim it replaces, so without
# this the gate reddens on the repair — measured on the real one: `releasable`'s docstring fired
# first on a negated "every" and, once rewritten, again on "at all". A gate that reddens on two
# successive repairs of the defect it exists to catch trains authors away from repairing it.
#
# The negator's reach is ITS OWN CLAUSE — up to 40 characters, no sentence punctuation, and no
# coordinating conjunction. Both bounds were measured. A negator plus at most one word (the first
# spelling) leaves five of eight ordinary corrections reddening, including "does not, as the old
# docstring said, mark every inherited sink"; letting it cross an `and` makes "the next one is not,
# and it says X stamps everything inherited" read as a correction of itself.
#
# "AT ALL" NEEDS NO GUARD OF ITS OWN, and the one written here was deleted rather than kept. The
# idiom means "whatsoever" and only ever appears inside a negated clause, which the rule above
# already covers — measured, its own fixture went on passing with the guard gone, which is the
# definition of a check that cannot fire. Where "at all" is NOT idiomatic it is a preposition
# followed by a real universal ("looks at all inherited sinks"), and a guard there would silence a
# genuine claim. A clause that is dead where it is right and wrong where it is live is not a fence.
#
# NO LENGTH CAP. An earlier draft dropped sentences over 700 characters, which is an unconditional
# escape: padding a false sentence past the cap silences it with its claim untouched. Removing it
# was measured to move no count on any of the three trees, so it is gone rather than justified.
#
# QUOTING THE FALSE CLAIM ON PURPOSE stays possible two ways: a fenced block, which the population
# below skips, and the literal escape `docs-lint: marking-walk` anywhere in the unit. The escape
# is a magic word and that is deliberate — it is EXPLICIT and greppable, so nobody reaches it while
# paraphrasing, which is exactly how the scoping words above defeat a sentence-wide test. It
# covers the unit it appears in and never a file.
#
# ── the population ──
#
# IN:  every *.md at the root; docs/**.md; src/**.py; tests/**.py.
# OUT: scripts/ — this check's own patterns and the prose describing them live here, so the check
#      would fail on itself. That one needs no exclusion: the population is four INCLUSION globs
#      and none of them reaches scripts/, so there is nothing there to mutate and a fixture over
#      it proves only that the harness runs.
# OUT: any directory named docs-lint, which IS a live exclusion and is the one to mutate. The
#      .case corpus carries the wrong form on purpose, and while a .case is already outside the
#      globs by extension, a .py planted beside it is not — which is exactly what that fixture
#      plants. The skip does not prune the walk, so a subdirectory of one is still descended into;
#      nothing puts prose there today and this says so rather than implying otherwise.
#
# THIS POPULATION DIFFERS FROM CHECK 14's, and check 14 excludes its three trees for three DIFFERENT
# reasons, none of which is "frozen records" alone (the three `OUT:` bullets under check 14 above):
# docs/specs and
# four sibling docs trees are frozen records; src/ is out because its docstrings anchor to SPEC
# numbers rather than commits and whether that counts is a decision nobody has taken; tests/ is
# out because the .case corpus carries the wrong form on purpose. This check INCLUDES all three —
# docs/specs and tests/ are exactly where the false claim was restated, and a frozen record that
# states a false universal is still read as a description of today's code — and EXCLUDES
# scripts/, pyproject.toml and .github/*.yml, which check 9 includes. One reason standing in for
# three is how the wrong one gets cited later, so all three are written out here.
#
# KNOWN AND STATED, not discovered later: a markdown TABLE ROW is dropped (a table has no sentence
# terminator, so a whole table flattens into one "sentence" pairing any row's anchor with any
# other row's universal), which means docs/component-inventory.md's row — one of the three places
# that restate this mechanism — cannot be policed here. An overt relative pronoun excuses a
# universal, so "marks everything THAT the child inherited" would pass while being as false as the
# sentence it replaces. This check catches the BARE universal, which is the one spelling that
# recurred; the contrapositive ("an unmarked sink is claimable") and the possessive ("the
# `_FOREIGN` stamp `_mark_inherited` set") carry no universal and are out of reach by construction.
#
# AND WHAT REDDENS THAT SHOULD NOT — the more important list, because a false negative costs a
# missed claim while a false positive costs the gate. All four are the price of the positional
# scope test, and none is fixable without paying a true positive; each was built and measured.
#
#   A RESTRICTION WITH NO RELATIVE PRONOUN reddens. "Every sink the walk missed" is restricted in
#   English and not to this check, which needs `that`, `which`, `whose` or one of the scoping
#   words. Adding `it|its|the parent|the child` closes it and drops one of the six true positives,
#   so the pronoun stays required and the FAIL text says to insert it. This is the one authors will
#   hit.
#   AN UNBALANCED FENCE silences everything below it to EOF, in any file. Check 9 has the same
#   hole and names it; this check inherits it rather than closing it, because balancing fences is
#   a markdown parser's job.
#   A 4-SPACE INDENTED CODE BLOCK is not a fence and is not skipped, so a false claim quoted that
#   way reddens. Use a fenced block, which is what the FAIL text already recommends.
#   TWO ADJACENT LINES WITH NO TERMINATOR are one sentence, so an anchor on one and a universal on
#   the next pair up. That is the table-row flattening in another costume, and dropping table rows
#   closes only the instance where it is unavoidable.
# ONE branch, not two, and that is a testability decision. The obvious spelling guards a
# `command -v python3` and reports a missing interpreter separately from a failing one — but the
# absent-interpreter branch cannot be reached from the fixture harness without unbuilding PATH far
# enough to take `grep` and `awk` with it, so it would ship untested, which is the state this
# check exists to argue against. Collapsed to one invocation, a missing python3 exits 127 and a
# broken one exits non-zero, and both land on the same `note` — which a self-test in
# docs-lint-test.sh reaches with a python3 shim that fails. The report is a FAIL either way: a
# check that could not run must never read as a check that found nothing. stderr is deliberately
# NOT swallowed — the note says the check did not run, and the traceback or the shell's own
# "command not found" beside it is the only thing that says why.
python3 - . >> "$FAILS" <<'PYEOF' || note "check 15 (the marking walk) did not run — python3 is missing or the check failed. Its findings, if any, are NOT in this report."
"""SPEC-053 FR-001. Reads the tree given as argv[1]; prints FAIL lines; never exits non-zero."""
import ast
import os
import re
import sys

ANCHOR = re.compile(r"_mark_inherited|`_FOREIGN`|marking walk")
UNIVERSAL = re.compile(r"\b(every|everything|each|all)\b", re.IGNORECASE)
SUBJECT = re.compile(
    r"\b(sink|record|inherit|stamp|transport|descriptor|object)\w*",
    re.IGNORECASE,
)
VERB = re.compile(
    r"\b(mark|record|stamp|refuse|claim)\w*\s+(\w+\s+){0,1}$",
    re.IGNORECASE,
)
RESTRICT = re.compile(
    r"\b(that|which|whose)\b|reach|missed|residual|partial|setdefault|item 7",
    re.IGNORECASE,
)
NEGATED = re.compile(
    r"\b(not|no|never|nothing|rather than|false|used to|corrected)"
    r"\b(?:(?!\b(?:and|but|or|so)\b)[^.;:]){0,40}$",
    re.IGNORECASE,
)
NP_END = re.compile(r"[,;:.()—–]")
ESCAPE = "docs-lint: marking-walk"
REPORT = (
    "FAIL  %s:%d claims the marking walk acts on EVERYTHING, with no scope.\n"
    "      %s\n"
    "      `_mark_inherited` ``setdefault``s: a sink `configure()` already stamped keeps the\n"
    "      parent's real pid, and ``_FOREIGN`` lands only where nothing was recorded. Say what\n"
    "      the universal is restricted TO, INSIDE its own noun phrase — \"every inherited sink\n"
    "      its walk reaches\", \"everything inherited that the parent never recorded\". A scoping\n"
    "      word elsewhere in the sentence does NOT count and is not meant to, and a restriction\n"
    "      with no relative pronoun (\"every sink the walk missed\") is not seen as one — insert\n"
    "      the `that`. Quoting the false claim on purpose? Use a fenced block, or put\n"
    "      `docs-lint: marking-walk` in the same unit — the paragraph, or the single bullet,\n"
    "      that carries it, since a bullet is a unit of its own. SPEC-053 FR-001."
)
LIST_ITEM = re.compile(r"^[ \t]*([-*+]|[0-9]+[.)])[ \t]")
SENTENCE = re.compile(r"(?<=[.!?])[*_`\"')\]]*\s+")


def violates(sentence):
    """Whether one sentence makes an unrestricted universal claim about the marking walk.

    The escape is not consulted here. It covers the whole UNIT — a paragraph, a bullet, one
    docstring paragraph — because a marker written as its own sentence would otherwise excuse
    only itself, which is the one sentence that never violates.
    """
    if not ANCHOR.search(sentence):
        return False
    for found in UNIVERSAL.finditer(sentence):
        before = sentence[max(0, found.start() - 60):found.start()]
        if NEGATED.search(before):
            continue
        end = NP_END.search(sentence, found.end())
        phrase = sentence[found.start():end.start()] if end else sentence[found.start():]
        if not (SUBJECT.search(phrase) or VERB.search(before)):
            continue
        if RESTRICT.search(phrase[found.end() - found.start():]):
            continue
        return True
    return False


def population(root):
    """Every file this check reads, as paths relative to root."""
    for name in sorted(os.listdir(root)):
        if name.endswith(".md") and os.path.isfile(os.path.join(root, name)):
            yield name
    for sub, suffix in (("docs", ".md"), ("src", ".py"), ("tests", ".py")):
        for dirpath, dirnames, filenames in os.walk(os.path.join(root, sub)):
            dirnames.sort()
            if os.path.basename(dirpath) == "docs-lint":
                continue
            for name in sorted(filenames):
                if name.endswith(suffix):
                    yield os.path.relpath(os.path.join(dirpath, name), root)


def blocks(lines, first, markdown):
    """Split lines into units at blank lines, list markers, and (in markdown) headings/rows."""
    held, start, fenced = [], None, False
    for number, line in enumerate(lines, first):
        stripped = line.strip()
        if stripped.startswith("```") or stripped.startswith("~~~"):
            if held:
                yield start, "\n".join(held)
            held, start, fenced = [], None, not fenced
            continue
        if fenced:
            continue
        if not stripped or (markdown and (stripped.startswith("#") or stripped.startswith("|")
                                          or stripped.startswith(">"))):
            if held:
                yield start, "\n".join(held)
            held, start = [], None
            continue
        if LIST_ITEM.match(line):
            if held:
                yield start, "\n".join(held)
            held, start = [line], number
            continue
        if not held:
            start = number
        held.append(line)
    if held:
        yield start, "\n".join(held)


def docstrings(text):
    """Every docstring in a module: def, class and module bodies, plus attribute docstrings."""
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            found = ast.get_docstring(node, clean=False)
            if found:
                yield node.body[0].lineno, found
        body = getattr(node, "body", None)
        if not isinstance(body, list):
            continue
        for index, statement in enumerate(body):
            if not index or not isinstance(statement, ast.Expr):
                continue
            value = statement.value
            if (isinstance(value, ast.Constant) and isinstance(value.value, str)
                    and isinstance(body[index - 1], (ast.Assign, ast.AnnAssign))):
                yield statement.lineno, value.value


def units(relative, text):
    """The prose units of one file, as (starting line, text) pairs."""
    if relative.endswith(".py"):
        for line, found in docstrings(text):
            yield from blocks(found.splitlines(), line, False)
    else:
        yield from blocks(text.splitlines(), 1, True)


def sentences(block, first):
    """Every sentence of one unit, each with the source line it starts on."""
    flat, lines = [], []
    for offset, line in enumerate(block.splitlines()):
        stripped = line.strip() if offset else line
        flat.append(stripped)
        lines.append(first + offset)
    joined = " ".join(flat)
    starts, cursor = [], 0
    for offset, piece in enumerate(flat):
        starts.append((cursor, lines[offset]))
        cursor += len(piece) + 1
    found, cursor = [], 0
    for sentence in SENTENCE.split(joined):
        line = first
        for at, number in starts:
            if at <= cursor:
                line = number
        found.append((line, sentence.strip()))
        cursor += len(sentence) + 1
    return found


def main(root):
    """Print one FAIL block per violating sentence, in file and line order."""
    findings = []
    for relative in population(root):
        try:
            with open(os.path.join(root, relative), encoding="utf-8") as handle:
                text = handle.read()
        except (OSError, UnicodeDecodeError) as failure:
            # NOT a skip. A file this check could not read is a file it did not examine, and the
            # whole argument for the interpreter guard above applies again one level down: a gate
            # that goes quiet on what it could not open reads exactly like a gate that found
            # nothing. Reported by TYPE, per arch 6 and SPEC-029.
            findings.append((relative, 0,
                             "FAIL  %s could not be read (%s), so this check did NOT examine it."
                             % (relative, type(failure).__name__)))
            continue
        for line, block in units(relative, text):
            if ESCAPE in block:
                continue
            for at, sentence in sentences(block, line):
                if not sentence or not violates(sentence):
                    continue
                findings.append((relative, at, REPORT % (relative, at, sentence[:160])))
    for _, _, report in sorted(findings, key=lambda found: (found[0], found[1])):
        print(report)


main(sys.argv[1])
PYEOF



report
