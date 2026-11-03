#!/bin/sh
# spec-lint.sh — structural linter for spec files.
#
# FAIL (exit 1):
#   - a spec's frontmatter breaks the subset or a rule below, OR
#   - a spec is missing a required top-level section, OR
#   - a spec contains a banned "Open Questions" / "Checkpoint" header
#     (these are resolved during authoring or in-session, never parked in the spec), OR
#   - a spec not `spec_status: Completed` has an FR whose Acceptance Criteria name no invariant
#     from docs/invariants.md, or name a number the page does not have (see the check
#     below for the accepted spellings and the Completed exemption).
# ERROR (exit 2): the shared frontmatter reader (`frontmatter.awk`, beside this script) is missing,
#   failed, or returned no verdict for a file — the frontmatter checks cannot run, and a check that
#   cannot run must not read as one that passed.
# WARN (exit 0): soft issues — unfilled template placeholders, FRs with no acceptance
#   criteria, and a spec carrying more FRs than one buildable slice should (see
#   FR_CEILING below). The ceiling is a warn, not a fail: an indivisible spec can
#   legitimately sit above it, and that call belongs to the reviewer, not the lint.
#
# The frontmatter is the ONE home of a spec's id, title, status and dependencies
# (docs/process/knowledge-layout.md); `docs/specs/index.yaml` is generated from it. Checked here,
# because this linter owns the per-spec structure and its population is exactly the spec set — so
# the checks that look across specs (a dependency resolves, an arc order is unambiguous) are checks
# within that population — and because this is the linter CI runs, so a dangling dependency or an
# ambiguous arc cannot merge when the local gate is skipped:
#   F1  the reader refused a line (each printed with its line and the rule it broke)
#   F2  a required key missing or empty: type, id, title, description, spec_status, depends_on
#   F3  type is not Spec          F4  id is not SPEC-<number>, or not the filename's SPEC-<number>
#   F5  spec_status is not Draft, In Progress or Completed
#   F6  depends_on is not a flow list, names a non-id, names no spec in the directory, or itself
#   F7  arc without arc_order or the reverse; arc not a slug; arc_order not a positive integer
#   F8  two specs in one arc with the same arc_order (the build order is ambiguous)
#   F9  a depends_on naming a spec LATER in the same arc (the order contradicts the dependency)
#   F10 two files with one SPEC number (compared as numbers: SPEC-1 and SPEC-001 collide)
#   F13 a depends_on entry listed twice, or a dependency cycle (a spec built after itself)
#   F11 a bold header line (**Status:** and kin) in the body: a second copy of the frontmatter
#   F12 a `# ` title line in the body: the title's one home is the frontmatter `title`
#
# Usage: scripts/spec-lint.sh [spec-dir]     (default: docs/specs)
# POSIX sh — no bashisms; runs anywhere /bin/sh exists. The awk is kept to what mawk
# accepts too, since CI's awk is not the one on a Mac: no interval expressions.
#
# `scripts/spec-lint-test.sh` is the fixture corpus that proves each check still fires, and
# stays silent where it should. Running this over docs/specs/ proves the specs pass and nothing
# about whether any check works. A change here — or to the reader — runs the corpus; CI runs it
# before the lint.

set -eu

SPEC_DIR="${1:-docs/specs}"
# The reader is resolved beside this script, not from the working directory: this may be run
# from anywhere with a directory argument.
READER="$(cd "$(dirname "$0")" && pwd)/frontmatter.awk"
# Scratch, under $TMPDIR rather than a hardcoded /tmp, and removed on every exit path rather
# than only after a clean pass.
SCRATCH="${TMPDIR:-/tmp}/spec-lint.$$"
MISSING="$SCRATCH.missing"
REC="$SCRATCH.rec"
ONE="$SCRATCH.one"
FMF="$SCRATCH.fm"
trap 'rm -f "$MISSING" "$REC" "$ONE" "$ONE.err" "$FMF"' EXIT
trap 'exit 1' INT TERM
# The invariants page sits beside the spec directory, not beside this script: the lint is
# run with a spec directory argument, and the corpus runs it against a scratch tree.
INVARIANTS="$(dirname "$SPEC_DIR")/invariants.md"

# Above this many FRs, a spec has usually stopped being one buildable slice and
# wants splitting into two specs recorded as an arc (docs/process/authoring-a-spec.md).
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

# A spec directory that does not exist is not an empty one: a renamed or mis-typed directory
# reported as "nothing to check" goes green in CI for the same reason a vanished corpus would.
if [ ! -d "$SPEC_DIR" ]; then
  echo "FAIL  $SPEC_DIR is not a directory, so nothing was linted. Pass the spec directory, or create it."
  exit 1
fi
specs=$(find "$SPEC_DIR" -maxdepth 1 -type f -name 'SPEC-*.md' 2>/dev/null | LC_ALL=C sort || true)

# Split the list on newlines only — an unquoted expansion on default IFS turns a
# spec filename containing a space into two nonexistent paths and four bogus FAILs.
IFS='
'

if [ -z "$specs" ]; then
  echo "spec-lint: no SPEC-*.md files in $SPEC_DIR (nothing to check)."
  exit 0
fi

if [ ! -f "$READER" ]; then
  echo "ERROR  the frontmatter reader $READER is missing. spec-lint.sh and frontmatter.awk ship together."
  exit 2
fi

# --- the invariant numbers the page actually carries ---
# Read once, from the `## N.` headings, outside fences. The list is what the citation
# check below validates against, so a cited number the page does not have fails rather
# than passing as "a number was written". A MISSING page fails outright, and fails here
# rather than per spec: the page is this check's input, and a check whose input has
# vanished must be loud — a run that goes quiet when its evidence disappears is the
# shape `docs/process/reviewer-contract.md` names as a green run over nothing.
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

# ── pass 1: every spec through the reader ──────────────────────────────────────
# Two guards, and neither alone is enough: the reader exit status, and exactly one F record
# per file. A reader that dies on a syntax error prints nothing, and every check below would
# then iterate over nothing and pass.
: > "$REC"
for f in $specs; do
  if ! LC_ALL=C awk -f "$READER" "$f" > "$ONE" 2> "$ONE.err"; then
    echo "ERROR  the frontmatter reader failed on $f: $(head -1 "$ONE.err")"
    echo "       Its frontmatter checks cannot run, and a check that cannot run is not a pass."
    exit 2
  fi
  n=$(awk -F'\t' '$2 == "F"' "$ONE" | wc -l | tr -d '[:space:]')
  if [ "$n" != 1 ]; then
    echo "ERROR  the frontmatter reader returned $n verdict records for $f, not one. A reader that"
    echo "       prints nothing makes every frontmatter check pass over nothing."
    exit 2
  fi
  cat "$ONE" >> "$REC"
done

# ── pass 2: the frontmatter checks, per spec and across the set ───────────────
# Output: one `<path><TAB><FAIL line>` per finding, in file order. Cross-spec lookups resolve
# from FILENAMES, not from frontmatter, so one broken neighbour does not cascade into a FAIL on
# every spec that depends on it. A file whose frontmatter the reader refused gets its refusals
# and nothing else: what the other checks would say about a frontmatter that was not read is noise.
awk '
  BEGIN { FS = "\t"; split("type id title description spec_status", req, " ") }
  $2 == "E" { ne[$1]++; e[$1, ne[$1]] = $3 "\t" $4; next }
  $2 == "K" { kind[$1, $3] = "K"; kv[$1, $3] = $4; next }
  $2 == "L" { kind[$1, $3] = "L"; nl[$1, $3] = $4; next }
  $2 == "I" { ni[$1, $3]++; li[$1, $3, ni[$1, $3]] = $4; next }
  $2 == "F" {
    nf++; file[nf] = $1; verdict[$1] = $3
    b = $1; sub(/.*\//, "", b)
    pre[$1] = (match(b, /^SPEC-[0-9]+/) ? substr(b, 1, RLENGTH) : "")
    if (pre[$1] != "") {
      npre[pre[$1]]++; ppath[pre[$1], npre[pre[$1]]] = $1
      # The NUMBER, for F10: SPEC-1 and SPEC-001 are two strings and one spec number.
      num[$1] = substr(pre[$1], 6) + 0; nnum[num[$1]]++; npath[num[$1], nnum[num[$1]]] = $1
    }
    next
  }
  function out(f, msg) { printf "%s\tFAIL  %s: %s\n", f, f, msg }
  function scalar(f, k) { return (kind[f, k] == "K") ? kv[f, k] : "" }
  END {
    for (i = 1; i <= nf; i++) {
      f = file[i]
      if (verdict[f] == "err") {
        for (j = 1; j <= ne[f]; j++) {
          split(e[f, j], p, "\t")
          printf "%s\tFAIL  %s:%s: frontmatter: %s\n", f, f, p[1], p[2]
        }
        printf "%s\t      The frontmatter subset: docs/process/knowledge-layout.md (Frontmatter). If this spec predates\n", f
        printf "%s\t      frontmatter, see the same part, Migrating. No other frontmatter check ran on this file.\n", f
        continue
      }
      # F10 — one spec number per file, from the filename (so it holds whatever the frontmatter
      # says), compared as a NUMBER so SPEC-1 and SPEC-001 collide.
      if (pre[f] != "" && nnum[num[f]] > 1)
        for (j = 1; j <= nnum[num[f]]; j++) if (npath[num[f], j] != f)
          out(f, "its spec number " num[f] " (" pre[f] ") is shared with " npath[num[f], j] ". Two specs cannot have one number: renumber one.")
      # F2 — required keys.
      for (j = 1; j in req; j++) {
        k = req[j]
        if (kind[f, k] == "L") out(f, "frontmatter `" k "` is a list; it takes one value.")
        else if (scalar(f, k) == "") out(f, "frontmatter has no `" k "`, or it is empty. A spec carries type, id, title, description, spec_status and depends_on.")
      }
      if (kind[f, "depends_on"] == "") out(f, "frontmatter has no `depends_on`. Write `depends_on: []` when it depends on nothing.")
      # F3
      if (scalar(f, "type") != "" && scalar(f, "type") != "Spec") out(f, "type is \"" scalar(f, "type") "\", not \"Spec\".")
      # F4
      id = scalar(f, "id")
      if (pre[f] == "") out(f, "the filename has no SPEC-<number> prefix, so its id cannot be checked against it.")
      if (id != "") {
        if (id !~ /^SPEC-[0-9]+$/) out(f, "id \"" id "\" is not SPEC-<number>.")
        else if (pre[f] != "" && id != pre[f]) out(f, "id \"" id "\" is not the filename prefix " pre[f] ". The id and the filename name one spec.")
      }
      # F5
      s = scalar(f, "spec_status")
      if (s != "" && s != "Draft" && s != "In Progress" && s != "Completed")
        out(f, "spec_status \"" s "\" is not one of Draft, In Progress, Completed.")
      # F6
      if (kind[f, "depends_on"] == "K") out(f, "depends_on is not a flow list. Write `depends_on: [SPEC-001, SPEC-002]`, or `[]`.")
      else for (j = 1; j <= ni[f, "depends_on"]; j++) {
        d = li[f, "depends_on", j]
        if ((f, d) in listed) { out(f, "depends_on lists " d " twice."); continue }
        listed[f, d] = 1
        if (d !~ /^SPEC-[0-9]+$/) out(f, "depends_on names \"" d "\", which is not SPEC-<number>.")
        else if (d == pre[f]) out(f, "depends_on names " d ", which is this spec.")
        else if (!(d in npre)) out(f, "depends_on names " d ", but no " d "-*.md is in the spec directory.")
      }
      # F7
      a = kind[f, "arc"]; o = kind[f, "arc_order"]
      if (a == "L") out(f, "frontmatter `arc` is a list; it takes one value.")
      if (o == "L") out(f, "frontmatter `arc_order` is a list; it takes one value.")
      if (a == "K" && o != "K") out(f, "arc \"" kv[f, "arc"] "\" has no arc_order. The two go together: arc names the arc, arc_order the place in it.")
      if (o == "K" && a != "K") out(f, "arc_order has no arc. The two go together: arc names the arc, arc_order the place in it.")
      if (a == "K" && kv[f, "arc"] !~ /^[a-z0-9][a-z0-9-]*$/) out(f, "arc \"" kv[f, "arc"] "\" is not a slug (lowercase letters, digits and hyphens).")
      if (o == "K" && kv[f, "arc_order"] !~ /^[1-9][0-9]*$/) out(f, "arc_order \"" kv[f, "arc_order"] "\" is not a positive integer.")
      if (a == "K" && o == "K" && kv[f, "arc_order"] ~ /^[1-9][0-9]*$/) {
        arc[f] = kv[f, "arc"]; ord[f] = kv[f, "arc_order"] + 0
        nslot[arc[f], ord[f]]++; slot[arc[f], ord[f], nslot[arc[f], ord[f]]] = f
      }
    }
    # F13 — a dependency cycle: a walk from this spec through depends_on (resolved by filename)
    # that comes back to it. Depending on itself directly is F6; this is the longer loop.
    for (i = 1; i <= nf; i++) {
      f = file[i]
      if (verdict[f] != "ok") continue
      split("", seen); sp = 0; cyc = 0
      for (j = 1; j <= ni[f, "depends_on"]; j++) { d = li[f, "depends_on", j]
        for (m = 1; m <= npre[d]; m++) if (ppath[d, m] != f) stack[++sp] = ppath[d, m] }
      while (sp > 0) {
        g = stack[sp--]
        if (g == f) { cyc = 1; break }
        if ((g in seen) || verdict[g] != "ok") continue
        seen[g] = 1
        for (j = 1; j <= ni[g, "depends_on"]; j++) { d = li[g, "depends_on", j]
          for (m = 1; m <= npre[d]; m++) stack[++sp] = ppath[d, m] }
      }
      if (cyc) out(f, "depends_on leads back to this spec — a dependency cycle, so no build order exists. Break the cycle.")
    }
    # F8 and F9 need every file read first.
    for (i = 1; i <= nf; i++) {
      f = file[i]
      if (!(f in arc)) continue
      n = nslot[arc[f], ord[f]]
      if (n > 1) for (j = 1; j <= n; j++) if (slot[arc[f], ord[f], j] != f)
        out(f, "arc \"" arc[f] "\" has arc_order " ord[f] " here and in " slot[arc[f], ord[f], j] ". The build order is ambiguous: give each spec its own place.")
      for (j = 1; j <= ni[f, "depends_on"]; j++) {
        d = li[f, "depends_on", j]
        for (m = 1; m <= npre[d]; m++) {
          g = ppath[d, m]
          if ((g in arc) && arc[g] == arc[f] && ord[g] > ord[f])
            out(f, "depends_on names " d ", which comes later in arc \"" arc[f] "\" (arc_order " ord[g] " after " ord[f] "). The authored order contradicts the dependency.")
        }
      }
    }
  }
' "$REC" > "$FMF"

# ── pass 3: per spec, the frontmatter findings and the body checks ────────────
for f in $specs; do
  file_fail=0

  # --- frontmatter (pass 2) ---
  if F="$f" awk -F'\t' '$1 == ENVIRON["F"] { found = 1; sub(/^[^\t]*\t/, ""); print } END { exit !found }' "$FMF"; then
    file_fail=1
  fi

  # --- F11 / F12: a bold header line, or a `# ` title line, in the body is a second copy of the
  # frontmatter. Outside fenced blocks and HTML comments, so a spec that quotes the old header form
  # in an example is not told to delete its example. Scanned from the first body line; a file with
  # no frontmatter is scanned whole, since that is exactly the file still carrying its header.
  body=$(F="$f" awk -F'\t' '$1 == ENVIRON["F"] && $2 == "B" { print $3 }' "$REC")
  if awk -v start="${body:-1}" -v file="$f" '
    FNR < start { next }
    { line = $0; sub(/\r$/, "", line) }
    in_comment { if (line ~ /-->/) in_comment = 0; next }
    in_fence   { if (line ~ /^[[:space:]]*(```|~~~)/) in_fence = 0; next }
    line ~ /^[[:space:]]*(```|~~~)/ { in_fence = 1; next }
    { gsub(/<!--[^>]*-->/, "", line) }
    line ~ /<!--/ { in_comment = 1; next }
    line ~ /^\*\*(ID|Status|Last Updated|Depends On):\*\*/ {
      printf "FAIL  %s: line %d is a bold header line (%s) — if this spec predates frontmatter, see docs/process/knowledge-layout.md, Migrating.\n", file, FNR, substr(line, 1, 40)
      printf "      Its field lives in the frontmatter now; delete the line.\n"
      found = 1
    }
    line ~ /^# / {
      printf "FAIL  %s: line %d is a `# ` title line (%s) — if this spec predates frontmatter, see docs/process/knowledge-layout.md, Migrating.\n", file, FNR, substr(line, 1, 40)
      printf "      The title has one home, the frontmatter `title`; delete the line.\n"
      found = 1
    }
    END { exit !found }
  ' "$f"; then
    file_fail=1
  fi

  # --- required sections ---
  echo "$required_sections" | while IFS= read -r sec; do
    [ -n "$sec" ] || continue
    grep -qiE "^${sec}([[:space:]]|\$)" "$f" || echo "MISSING|$sec"
  done > "$MISSING"
  if [ -s "$MISSING" ]; then
    while IFS='|' read -r _ sec; do
      echo "FAIL  $f: missing required section '$sec'"
    done < "$MISSING"
    file_fail=1
  fi
  rm -f "$MISSING"

  # --- banned headers (any heading level) ---
  if grep -qiE "^#{1,6}[[:space:]].*(${banned_headers})" "$f"; then
    echo "FAIL  $f: contains banned header(s) — resolve in spec/session, don't park them:"
    grep -inE "^#{1,6}[[:space:]].*(${banned_headers})" "$f" | head -3 | sed 's/^/        line /'
    file_fail=1
  fi

  # --- FAIL: an FR of a live spec names no invariant, or names one the page lacks ---
  #
  # `docs/process/authoring-a-spec.md` and the spec template say every FR's Acceptance Criteria name the
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
  # further numbers joined by a comma, `and`, `&`, `/`, `or` or `, and` — `/` and `or`
  # because an author reaches for `3/11`, and a joiner the parser does not know ends the
  # list with the rest unread, which passed `3/99` as checked. The list ends at the first
  # token that is none of those. A RANGE IS REFUSED — `invariants 1–5`, with a hyphen, an
  # en dash or an em dash — rather than read as its first number: a range names an interior
  # nobody checked, and reading it as `1` would silently pass a dash over a number the page
  # does not have. Each number is listed on purpose. An FR that
  # serves no invariant — a prose, lint or hygiene requirement, which docs/invariants.md
  # says its own rules judge — writes the exact phrase `serves no invariant` instead,
  # with its reason, and the spec reviewer accepts or rejects that the way the FR ceiling
  # is accepted or rejected. An FR that both cites a number and says it serves none is
  # contradicting itself and fails. Every match here is case-insensitive.
  #
  # COMPLETED SPECS ARE EXEMPT. They predate the page: docs/invariants.md arrived at
  # 98c7e78 with fifty-odd specs already Completed, and a rule that reddens frozen history
  # is a rule that gets switched off within the week. The exemption keys on exactly
  # `spec_status: Completed`, read through the shared reader from a frontmatter it accepted —
  # the body is not consulted, so a `**Status:** Completed` quoted in a fence or left in a
  # comment exempts nothing. A spec whose status is anything else — Draft, In Progress,
  # misspelled, absent, or in a frontmatter the reader refused — is checked, because the
  # exemption must fail closed. What ends an FR is the next level 1–3 heading; what opens its
  # Acceptance Criteria is any heading containing those words, and what closes that block
  # is the next heading of any level — so a citation sitting in the Description does not
  # count, which is the point: the criteria are what the reviewer reads.
  status=$(F="$f" awk -F'\t' '
    $1 == ENVIRON["F"] && $2 == "K" && $3 == "spec_status" { s = $4 }
    $1 == ENVIRON["F"] && $2 == "F" { v = $3 }
    END { if (v == "ok") print s }
  ' "$REC")
  if [ "$status" != "Completed" ] && [ -n "$inv_numbers" ]; then
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
          while (match(s, /^[ \t]*(,[ \t]*(and[ \t]+)?|&[ \t]*|\/[ \t]*|and[ \t]+|or[ \t]+)[0-9]+/)) {
            k = substr(s, RSTART, RLENGTH); sub(/.*[^0-9]/, "", k)
            s = substr(s, RSTART + RLENGTH)
            cited++; if (!(k in exists)) bogus = bogus " " k
            if (match(s, /^[ \t]*(-|–|—)[ \t]*[0-9]+/)) range = k substr(s, RSTART, RLENGTH)
          }
        }
        optout = (tolower(ac) ~ /serves no invariant/)
        if (!seen_ac)
          printf "FAIL  %s: %s has no Acceptance Criteria block, so it names no invariant.\n      Every FR of a spec not `spec_status: Completed` carries\n      criteria naming the invariant(s) it serves — %s — from %s, which numbers %s.\n", file, fr, forms, page, pretty
        else if (range != "")
          printf "FAIL  %s: %s cites a range of invariants (%s).\n      A range names an interior nobody checked. List each number: %s.\n", file, fr, range, forms
        else if (bogus != "")
          printf "FAIL  %s: %s cites invariant%s%s, which %s does not have.\n      The page numbers %s. A citation the reader cannot follow is worse than none:\n      it reads as checked. Cite a number from the page, or fix the page first.\n", file, fr, (split(bogus, tmp, " ") > 1 ? "s" : ""), bogus, page, pretty
        else if (cited && optout)
          printf "FAIL  %s: %s cites an invariant and also says it serves no invariant.\n      One or the other: name the number(s) the FR keeps, or say why it keeps none.\n", file, fr
        else if (!cited && !optout)
          printf "FAIL  %s: %s names no invariant in its Acceptance Criteria.\n      Every FR of a spec not `spec_status: Completed` names\n      the invariant(s) it serves — %s — from %s, which numbers %s. A prose or lint FR that\n      keeps none says so with the exact phrase `serves no invariant`, and its reason. A\n      citation in the Description does not count: the criteria are what the reviewer reads.\n", file, fr, forms, page, pretty
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
  if grep -qE '\[Feature Name\]|\[Requirement Name\]|SPEC-XXX' "$f"; then
    echo "WARN  $f: unfilled template placeholder(s) (e.g. [Feature Name], [Requirement Name], SPEC-XXX)"
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
