#!/bin/sh
# spec-lint.sh — structural linter for spec files.
#
# FAIL (exit 1):
#   - a spec is missing a required top-level section, OR
#   - a spec contains a banned "Open Questions" / "Checkpoint" header
#     (these are resolved during authoring or in-session, never parked in the spec), OR
#   - a Draft or In Progress spec has an FR whose Acceptance Criteria name no invariant
#     from docs/invariants.md, or name a number the page does not have (see the check
#     below for the accepted spellings and the Completed exemption).
# WARN (exit 0): soft issues — unfilled template placeholders, FRs with no acceptance
#   criteria, and a spec carrying more FRs than one buildable slice should (see
#   FR_CEILING below). The ceiling is a warn, not a fail: an indivisible spec can
#   legitimately sit above it, and that call belongs to the reviewer, not the lint.
#
# Usage: scripts/spec-lint.sh [spec-dir]     (default: docs/specs)
# POSIX sh — no bashisms; runs anywhere /bin/sh exists. The awk is kept to what mawk
# accepts too, since CI's awk is not the one on a Mac: no interval expressions.
#
# `scripts/spec-lint-test.sh` is the fixture corpus that proves each check still fires,
# and stays silent where it should. Running this over docs/specs/ proves the specs pass
# and nothing about whether any check works. A change here runs the corpus.

set -eu

SPEC_DIR="${1:-docs/specs}"
# The invariants page sits beside the spec directory, not beside this script: the lint is
# run with a spec directory argument, and the corpus runs it against a scratch tree.
INVARIANTS="$(dirname "$SPEC_DIR")/invariants.md"

# Above this many FRs, a spec has usually stopped being one buildable slice and
# wants splitting into two specs recorded as an arc (docs/process.md §4).
FR_CEILING=8
fail=0
warn=0

# Required top-level sections every spec must have (exact "## " headings).
required_sections="## Overview
## Scope
## Functional Requirements
## Implementation Phases"

# Headings (any level) that must NOT appear.
banned_headers="Open Questions|Checkpoint"

specs=$(find "$SPEC_DIR" -maxdepth 1 -type f -name 'SPEC-*.md' 2>/dev/null | sort || true)

# Split the list on newlines only — an unquoted expansion on default IFS turns a
# spec filename containing a space into two nonexistent paths and four bogus FAILs.
IFS='
'

if [ -z "$specs" ]; then
  echo "spec-lint: no SPEC-*.md files in $SPEC_DIR (nothing to check)."
  exit 0
fi

# --- the invariant numbers the page actually carries ---
# Read once, from the `## N.` headings, outside fences. The list is what the citation
# check below validates against, so a cited number the page does not have fails rather
# than passing as "a number was written". A MISSING page fails outright, and fails here
# rather than per spec: the page is this check's input, and a check whose input has
# vanished must be loud — a run that goes quiet when its evidence disappears is the
# shape docs/process.md §3 names as a green run over nothing.
inv_numbers=""
if [ -f "$INVARIANTS" ]; then
  inv_numbers=$(awk '
    /^[[:space:]]*(```|~~~)/ { fence = !fence; next }
    fence { next }
    match($0, /^## [0-9]+\./) { print substr($0, 4, RLENGTH - 4) }
  ' "$INVARIANTS" | sort -n | tr '\n' ' ' | sed 's/ $//')
fi
if [ -z "$inv_numbers" ]; then
  if [ -f "$INVARIANTS" ]; then
    echo "FAIL  $INVARIANTS has no '## N.' headings, so no invariant can be cited. The citation"
    echo "      check needs the numbered page; a page that numbers nothing is not the page."
  else
    echo "FAIL  $INVARIANTS is missing. Every Draft or In Progress spec cites its invariants by"
    echo "      number from that page, so the page is this lint's input, not a nicety."
  fi
  fail=$((fail + 1))
fi

for f in $specs; do
  file_fail=0

  # --- required sections ---
  echo "$required_sections" | while IFS= read -r sec; do
    [ -n "$sec" ] || continue
    grep -qiE "^${sec}([[:space:]]|\$)" "$f" || echo "MISSING|$sec"
  done > /tmp/spec-lint-missing.$$
  if [ -s /tmp/spec-lint-missing.$$ ]; then
    while IFS='|' read -r _ sec; do
      echo "FAIL  $f: missing required section '$sec'"
    done < /tmp/spec-lint-missing.$$
    file_fail=1
  fi
  rm -f /tmp/spec-lint-missing.$$

  # --- banned headers (any heading level) ---
  if grep -qiE "^#{1,6}[[:space:]].*(${banned_headers})" "$f"; then
    echo "FAIL  $f: contains banned header(s) — resolve in spec/session, don't park them:"
    grep -inE "^#{1,6}[[:space:]].*(${banned_headers})" "$f" | head -3 | sed 's/^/        line /'
    file_fail=1
  fi

  # --- FAIL: an FR of a live spec names no invariant, or names one the page lacks ---
  #
  # docs/process.md §4 and the spec template say every FR's Acceptance Criteria name the
  # invariant(s) it serves, by number from docs/invariants.md, so the system-frame diff
  # reviewer knows which promise to check on every twin path. This is that rule where a
  # script can see it — a rule with no gate is a rule that rots (CLAUDE.md, Working rules).
  #
  # ACCEPTED SPELLINGS, case-insensitive, read from the joined text of the FR's Acceptance
  # Criteria block so a citation wrapped across a line break is still one citation:
  #
  #     invariant 13          invariants 3 and 11        invariants 3, 11 and 13
  #     (inv. 13)             inv. 3, 11                 invariants 3 & 11
  #
  # The keyword is `invariant`, `invariants` or `inv.`, then one number, then any run of
  # further numbers joined by a comma, `and`, `&` or `, and`. The list ends at the first
  # token that is none of those. A RANGE IS REFUSED — `invariants 1–5`, with a hyphen, an
  # en dash or an em dash — rather than read as its first number: a range names an interior
  # nobody checked, and reading it as `1` would silently pass a dash over a number the page
  # does not have. Each number is listed on purpose. An FR that
  # serves no invariant — a prose, lint or hygiene requirement, which docs/invariants.md
  # says its own rules judge — writes the exact phrase `serves no invariant` instead,
  # with its reason, and the spec reviewer accepts or rejects that the way the FR ceiling
  # is accepted or rejected. An FR that both cites a number and says it serves none is
  # contradicting itself and fails.
  #
  # COMPLETED SPECS ARE EXEMPT. They predate the page: docs/invariants.md arrived at
  # 98c7e78 with fifty-odd specs already Completed, and a rule that reddens frozen history
  # is a rule that gets switched off within the week. The exemption keys on the word
  # `Completed` in the first Status line outside a fence; a spec whose status is anything
  # else — Draft, In Progress, misspelled or absent — is checked, because the exemption
  # must fail closed. What ends an FR is the next level 1–3 heading; what opens its
  # Acceptance Criteria is any heading containing those words, and what closes that block
  # is the next heading of any level — so a citation sitting in the Description does not
  # count, which is the point: the criteria are what the reviewer reads.
  status=$(awk '
    /^[[:space:]]*(```|~~~)/ { fence = !fence; next }
    fence { next }
    tolower($0) ~ /^[^a-z]*status[^a-z]+(draft|in progress|completed)/ {
      s = tolower($0); sub(/^[^a-z]*status[^a-z]+/, "", s)
      if (s ~ /^completed/) print "completed"; else if (s ~ /^in progress/) print "in progress"; else print "draft"
      exit
    }
  ' "$f")
  if [ "$status" != "completed" ] && [ -n "$inv_numbers" ]; then
    inv_report=$(awk -v file="$f" -v have="$inv_numbers" -v page="$INVARIANTS" '
      BEGIN {
        n = split(have, arr, " ")
        for (i = 1; i <= n; i++) { exists[arr[i]] = 1; pretty = pretty (i > 1 ? ", " : "") arr[i] }
        forms = "`invariant 13`, `invariants 3 and 11` or `(inv. 13)`"
      }
      # Everything an FR owes is decided when it closes, so a wrapped citation has been
      # joined by then. `ac` is the Acceptance Criteria text with newlines turned to spaces.
      function close_fr(   s, k, cited, bogus, optout, range) {
        if (fr == "") return
        s = tolower(ac); cited = 0; bogus = ""; range = ""
        while (match(s, /(^|[^a-z0-9_])(invariants?|inv\.)[ \t]+[0-9]+/)) {
          k = substr(s, RSTART, RLENGTH); sub(/.*[ \t]/, "", k)
          s = substr(s, RSTART + RLENGTH)
          cited++; if (!(k in exists)) bogus = bogus " " k
          if (match(s, /^[ \t]*(-|–|—)[ \t]*[0-9]+/)) range = k substr(s, RSTART, RLENGTH)
          while (match(s, /^[ \t]*(,[ \t]*(and[ \t]+)?|&[ \t]*|and[ \t]+)[0-9]+/)) {
            k = substr(s, RSTART, RLENGTH); sub(/.*[^0-9]/, "", k)
            s = substr(s, RSTART + RLENGTH)
            cited++; if (!(k in exists)) bogus = bogus " " k
            if (match(s, /^[ \t]*(-|–|—)[ \t]*[0-9]+/)) range = k substr(s, RSTART, RLENGTH)
          }
        }
        optout = (tolower(ac) ~ /serves no invariant/)
        if (!seen_ac)
          printf "FAIL  %s: %s has no Acceptance Criteria block, so it names no invariant.\n      Every FR of a spec not marked Completed — the exemption keys on that exact word — carries\n      criteria naming the invariant(s) it serves — %s — from %s, which numbers %s.\n", file, fr, forms, page, pretty
        else if (range != "")
          printf "FAIL  %s: %s cites a range of invariants (%s).\n      A range names an interior nobody checked. List each number: %s.\n", file, fr, range, forms
        else if (bogus != "")
          printf "FAIL  %s: %s cites invariant%s%s, which %s does not have.\n      The page numbers %s. A citation the reader cannot follow is worse than none:\n      it reads as checked. Cite a number from the page, or fix the page first.\n", file, fr, (split(bogus, tmp, " ") > 1 ? "s" : ""), bogus, page, pretty
        else if (cited && optout)
          printf "FAIL  %s: %s cites an invariant and also says it serves no invariant.\n      One or the other: name the number(s) the FR keeps, or say why it keeps none.\n", file, fr
        else if (!cited && !optout)
          printf "FAIL  %s: %s names no invariant in its Acceptance Criteria.\n      Every FR of a spec not marked Completed — the exemption keys on that exact word — names\n      the invariant(s) it serves — %s — from %s, which numbers %s. A prose or lint FR that\n      keeps none says so with the exact phrase `serves no invariant`, and its reason. A\n      citation in the Description does not count: the criteria are what the reviewer reads.\n", file, fr, forms, page, pretty
        fr = ""; ac = ""; seen_ac = 0; in_ac = 0
      }
      { line = $0; sub(/\r$/, "", line) }
      # The same fence and comment skips as the FR count below, for the same reason: a
      # quoted or commented-out FR heading is not an FR that owes anything.
      in_comment { if (line ~ /-->/) in_comment = 0; next }
      in_fence   { if (line ~ /^[[:space:]]*(```|~~~)/) in_fence = 0; next }
      line ~ /^[[:space:]]*(```|~~~)/ { in_fence = 1; next }
      { gsub(/<!--[^>]*-->/, "", line) }
      line ~ /<!--/ { in_comment = 1; next }
      # The id is taken BEFORE the close: close_fr() calls match(), which overwrites the
      # RLENGTH this rule just set, so reading it afterwards named every second FR "" and
      # skipped it — measured on a seven-FR spec, where FR-002, -004 and -006 went unchecked.
      match(line, /^### FR-[0-9]+/) { id = substr(line, 5, RLENGTH - 4); close_fr(); fr = id; next }
      fr == "" { next }
      line ~ /^#[[:space:]]/ || line ~ /^##[[:space:]]/ || line ~ /^###[[:space:]]/ { close_fr(); next }
      line ~ /^#+[[:space:]]/ {
        in_ac = (tolower(line) ~ /acceptance criteria/)
        if (in_ac) seen_ac = 1
        next
      }
      in_ac { ac = ac " " line }
      END { close_fr() }
    ' "$f")
    if [ -n "$inv_report" ]; then
      printf '%s\n' "$inv_report"
      file_fail=1
    fi
  fi

  # --- WARN: unfilled template placeholders ---
  if grep -qE '\[Feature Name\]|\[Requirement Name\]|SPEC-XXX|YYYY-MM-DD' "$f"; then
    echo "WARN  $f: unfilled template placeholder(s) (e.g. [Feature Name], SPEC-XXX, YYYY-MM-DD)"
    warn=$((warn + 1))
  fi

  # --- WARN: FRs present but no acceptance criteria ---
  if grep -qE '^### FR-' "$f" && ! grep -qiE 'Acceptance Criteria' "$f"; then
    echo "WARN  $f: has FR-* requirements but no 'Acceptance Criteria'"
    warn=$((warn + 1))
  fi

  # --- WARN: more FRs than one spec should carry ---
  # Level-3 headings only (what the spec template emits, and what the check
  # above already uses), outside fenced blocks and HTML comments, counted as
  # distinct IDs. Without the skips, a spec that quotes FR headings in an
  # example — or comments some out while splitting — is told to split on
  # requirements it does not have. A comment is closed before a fence is
  # opened, so a ``` inside a comment cannot swallow the rest of the file;
  # ~~~ counts as a fence; and a one-line <!-- … --> is stripped rather than
  # skipped, so a heading with a trailing note still counts.
  fr_count=$(awk '
    { line = $0 }
    in_comment { if (line ~ /-->/) in_comment = 0; next }
    in_fence   { if (line ~ /^[[:space:]]*(```|~~~)/) in_fence = 0; next }
    line ~ /^[[:space:]]*(```|~~~)/ { in_fence = 1; next }
    { gsub(/<!--[^>]*-->/, "", line) }
    line ~ /<!--/ { in_comment = 1; next }
    match(line, /^### FR-[0-9]+/) { print substr(line, RSTART + 4, RLENGTH - 4) }
  ' "$f" | sort -u | wc -l | tr -d '[:space:]')
  if [ "${fr_count:-0}" -gt "$FR_CEILING" ]; then
    echo "WARN  $f: $fr_count FRs (over the $FR_CEILING ceiling) — split into two specs and record them as an arc"
    warn=$((warn + 1))
  fi

  [ "$file_fail" -eq 0 ] && echo "ok    $f" || fail=$((fail + 1))
done

echo "----"
echo "spec-lint: $fail file(s) failed, $warn warning(s)."
[ "$fail" -eq 0 ] || exit 1
