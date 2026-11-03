#!/bin/sh
# spec-lint-test.sh — the fixture corpus for scripts/spec-lint.sh.
#
# Why this exists. Until the invariant-citation check arrived, `spec-lint.sh` had no
# corpus at all: two of its five branches could fail a build and three only warned, and
# nothing proved any of the five still fired — CI ran it over `docs/specs/`, which proves
# the specs pass and nothing about whether a check works. The citation check is a gate in
# CLAUDE.md's sense, and a gate owes a fixture corpus asserting the failure REASON,
# silence cases included. Its first run found a real defect the live specs could not show:
# the parser named every second FR "" and skipped it, so a seven-FR spec reported four.
#
# MOST OF THESE CASES ARE THE TEMPLATE'S, AND ARE DERIVED RATHER THAN EDITED. The spec-driven
# template owns them; log-forge needs exactly two deltas on each, both forced by the citation
# check the template's linter does not have. Insert the 14-line `@@@ file docs/invariants.md`
# block (lift it verbatim from any shared case) immediately before the case's FIRST `@@@ file`
# line — without it, each fails the missing-invariants check — and flip every line that is
# exactly `spec_status: Draft` to `spec_status: Completed`, without which a case with an FR fails
# the per-FR citation check. Change nothing else — in BYTES: `fm-crlf-ok` is CRLF on purpose, and a
# text-mode rewrite turns it vacuous with the corpus still green — and re-derive rather than
# hand-edit when the template moves:
#
#   for c in <template>/tests/spec-lint/*.case; do ... insert block; sed Draft->Completed ... done
#
# Derived from the template at `cc00cae`. The `cite-*` cases are log-forge's own and are NOT
# derived; they cover the citation check, which the template has no equivalent of.
#
# THE DERIVATION IS NOT ENFORCED BY ANYTHING. Nothing in this repo or the template compares
# the two corpora, so a shared case that is hand-edited here, or one the template hardens
# later, drifts silently: the floor below counts files and the self-tests prove the guards
# fire, and neither reads content. That is exactly how four assertions went missing — a
# take-down added the 17 cases this repo lacked and left the 6 it already had untouched,
# five of which were already behind. Re-derive when you touch a shared case.
#
# Each case asserts the specific FAIL or WARN TEXT, not just the exit code. A check that
# fails for the wrong reason is a check that will be "fixed" by changing the wrong thing.
# Cases named `*-ok.case` assert the linter stays SILENT — no `FAIL  ` line at all — and
# that is where the exemptions live: the Completed spec, each accepted spelling, the
# wrapped citation, the fenced or commented-out FR heading, the opt-out phrase.
#
# Directives are `@@@ `-prefixed, as in `docs-lint-test.sh`, and for the same reason: a
# fixture whose content began `--- ` was once truncated there silently. `@@@ absent` is
# the one directive that harness lacks — a WARN check has no `-ok` shape, since silence
# there means "no WARN line" rather than "no FAIL line", and a case that plants one
# uncited FR among cited ones has to prove the cited ones were NOT reported.
#
# Usage: sh scripts/spec-lint-test.sh [case-name-substring]
# POSIX sh — no dependencies.

set -eu
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# Overridable so the SELF-TEST at the bottom can point this script at a directory of
# deliberately malformed cases. Pointing the harness at different fixtures cannot make a
# real case pass.
CASES="${SPEC_LINT_TEST_CASES:-$ROOT/tests/spec-lint}"
FILTER="${1:-}"
WORK="${TMPDIR:-/tmp}/spec-lint-test.$$"
trap 'rm -rf "$WORK" "$WORK.self"' EXIT
trap 'exit 1' INT TERM

# Parse the linter before exercising it. A syntax error partway through a shell script can
# end a run with status 0 — the linter never reaches its checks and reports success on a
# script that did nothing.
if ! sh -n "$ROOT/scripts/spec-lint.sh"; then
  echo "FAIL  scripts/spec-lint.sh does not parse — a syntax error can exit 0 and look green."
  exit 1
fi
# And the shared frontmatter reader, by RUNNING it: an empty operand is "no frontmatter", so the
# reader must exit 0 and print an `F err` verdict. That proves the program both parses and
# classifies; a parse alone would pass a reader whose every rule had been commented out.
if ! LC_ALL=C awk -f "$ROOT/scripts/frontmatter.awk" /dev/null 2>/dev/null | grep -q "	F	err$"; then
  echo "FAIL  scripts/frontmatter.awk does not run, or does not refuse an empty file — every"
  echo "      frontmatter check reads through it."
  exit 1
fi

pass=0; fail=0
for case_file in "$CASES"/*.case; do
  [ -f "$case_file" ] || continue
  name=$(basename "$case_file" .case)
  case "$name" in *"$FILTER"*) ;; *) continue ;; esac

  rm -rf "$WORK"; mkdir -p "$WORK/scripts" "$WORK/docs/specs"

  # A small FR ceiling, so the over-the-ceiling case can be four short FRs rather than
  # nine. Applied by rewriting the constant in a COPY — not an env override, because a
  # ceiling a caller can lower is a ceiling CI can be told to ignore.
  sed -e 's/^FR_CEILING=.*/FR_CEILING=3/' "$ROOT/scripts/spec-lint.sh" > "$WORK/scripts/spec-lint.sh"
  chmod +x "$WORK/scripts/spec-lint.sh"
  cp "$ROOT/scripts/frontmatter.awk" "$WORK/scripts/frontmatter.awk"

  want_exit=$(sed -n 's/^@@@ expect exit=//p' "$case_file")
  awk -v work="$WORK" '
    /^@@@ file / { path = work "/" substr($0, 10); system("mkdir -p \"$(dirname \"" path "\")\""); out = path; next }
    /^@@@ / { out = ""; next }
    out { print >> out }
  ' "$case_file"

  # A fixture that never reaches disk makes its case silent for the WRONG reason, and an
  # `-ok` case cannot tell that apart from a pass. So the directives are checked before the
  # linter is allowed to have an opinion: every case declares at least one file, every
  # declared file was written, and every `@@@` line is a directive the splitter knows —
  # counted on `^@@@`, not `^@@@ `, because `@@@file`, `@@@<tab>file` and `@@@@ file` break
  # the prefix the splitter keys on and would otherwise be discarded with their body.
  decl=$(sed -n 's|^@@@ file ||p' "$case_file")
  if [ -z "$decl" ]; then
    fail=$((fail + 1)); echo "FAIL  $name: declares no '@@@ file' directive, so nothing reached disk."
    continue
  fi
  missing=""
  # Declared paths may carry a space, so they are read line by line rather than word-split.
  printf '%s\n' "$decl" | while IFS= read -r d; do
    [ -n "$d" ] || continue
    [ -f "$WORK/$d" ] || printf '%s\n' "$d"
  done > "$WORK/.missing"
  if [ -s "$WORK/.missing" ]; then
    fail=$((fail + 1)); echo "FAIL  $name: declared file(s) never written: $(tr '\n' ' ' < "$WORK/.missing")"
    continue
  fi
  if [ "$(grep -c '^@@@' "$case_file")" != "$(grep -cE '^@@@ (file |expect exit=|match |absent )' "$case_file")" ]; then
    fail=$((fail + 1)); echo "FAIL  $name: an '@@@' line is not one of file/expect exit=/match/absent."
    continue
  fi

  got=$(cd "$WORK" && sh scripts/spec-lint.sh 2>&1) && rc=0 || rc=$?
  ok=1
  [ "$rc" = "${want_exit:-0}" ] || { ok=0; why="exit $rc, wanted ${want_exit:-0}"; }
  if [ "$ok" = 1 ]; then
    sed -n 's/^@@@ match //p' "$case_file" | while IFS= read -r m; do
      [ -n "$m" ] || continue
      case "$got" in *"$m"*) ;; *) echo "MISSING|$m" ;; esac
    done > "$WORK/.miss"
    if [ -s "$WORK/.miss" ]; then ok=0; why="output lacked: $(sed 's/^MISSING|//' "$WORK/.miss" | head -1)"; fi
  fi
  if [ "$ok" = 1 ]; then
    sed -n 's/^@@@ absent //p' "$case_file" | while IFS= read -r m; do
      [ -n "$m" ] || continue
      case "$got" in *"$m"*) echo "PRESENT|$m" ;; esac
    done > "$WORK/.present"
    if [ -s "$WORK/.present" ]; then ok=0; why="output carried: $(sed 's/^PRESENT|//' "$WORK/.present" | head -1)"; fi
  fi
  # A `-ok` case must be silent: no FAIL line may appear at all.
  case "$name" in
    *-ok) case "$got" in *"FAIL  "*) ok=0; why="expected silence, got a FAIL" ;; esac ;;
  esac

  if [ "$ok" = 1 ]; then
    pass=$((pass + 1)); echo "ok    $name"
  else
    fail=$((fail + 1)); echo "FAIL  $name: $why"
    printf '%s\n' "$got" | sed 's/^/        /' | head -8
  fi
done

# The live template, as two cases of its own. An unfilled copy of `docs/templates/spec-template.md`
# must pass its structure — every required section present, no banned header — WARN on its
# placeholders, and FAIL on two things: its id, since `SPEC-XXX` is the one placeholder that is also
# a frontmatter defect, and its FRs, since a placeholder line carrying a number or the opt-out
# phrase would let an unfilled copy through the citation gate — a rule its comment states, and the
# kind this corpus exists to gate. The second case binds the placeholder regex to EACH string the
# template actually uses: the copy is scrubbed of all but one placeholder at a time, and that one
# alone must still warn. A single run over the whole template could not say that — any one surviving
# placeholder keeps the WARN firing, so renaming `[Feature Name]` in the template stayed green until
# this was split. Run under the same filter as the cases, so `sh scripts/spec-lint-test.sh template`
# reaches both.
if [ -z "${SPEC_LINT_TEST_CASES:-}" ] && case template-unfilled in *"$FILTER"*) true ;; *) false ;; esac; then
  rm -rf "$WORK"; mkdir -p "$WORK/scripts" "$WORK/docs/specs"
  cp "$ROOT/scripts/spec-lint.sh" "$WORK/scripts/spec-lint.sh"
  cp "$ROOT/scripts/frontmatter.awk" "$WORK/scripts/frontmatter.awk"
  cp "$ROOT/docs/templates/spec-template.md" "$WORK/docs/specs/SPEC-000-template.md"
  printf '# Invariants\n\n## 1. One\n\n## 2. Two\n\n## 3. Three\n\n## 11. Eleven\n\n## 13. Thirteen\n' > "$WORK/docs/invariants.md"
  got=$(cd "$WORK" && sh scripts/spec-lint.sh 2>&1) && rc=0 || rc=$?
  nfail=$(printf '%s\n' "$got" | grep -c '^FAIL  ' || true)
  case "$rc:$nfail:$got" in
    *"missing required section"*|*"banned header"*) verdict=bad ;;
    1:3:*'FAIL  docs/specs/SPEC-000-template.md: id "SPEC-XXX" is not SPEC-<number>.'*"SPEC-000-template.md: FR-001 names no invariant in its Acceptance Criteria."*"SPEC-000-template.md: FR-002 names no invariant in its Acceptance Criteria."*"WARN  docs/specs/SPEC-000-template.md: unfilled template placeholder(s)"*) verdict=good ;;
    *) verdict=bad ;;
  esac
  if [ "$verdict" = good ]; then
    pass=$((pass + 1)); echo "ok    template-unfilled"
  else
    fail=$((fail + 1)); echo "FAIL  template-unfilled: exit $rc, $nfail FAIL line(s); an unfilled copy of the template must fail on its id and its two FRs alone, warn on its placeholders, and pass its structure"
    printf '%s\n' "$got" | sed 's/^/        /' | head -8
  fi
fi
if [ -z "${SPEC_LINT_TEST_CASES:-}" ] && case template-placeholders in *"$FILTER"*) true ;; *) false ;; esac; then
  # Each placeholder the linter's regex names, as a sed pattern that removes it. The list is
  # kept here rather than read from the linter so that dropping an alternative from the regex
  # is a red case, not a quieter corpus. `SPEC-XXX` is scrubbed to SPEC-000, the copy's own
  # filename prefix, so a scrubbed copy has a valid id.
  unbound=""
  for keep in 'Feature Name' 'Requirement Name' 'SPEC-XXX'; do
    rm -rf "$WORK"; mkdir -p "$WORK/scripts" "$WORK/docs/specs"
    cp "$ROOT/scripts/spec-lint.sh" "$WORK/scripts/spec-lint.sh"
    cp "$ROOT/scripts/frontmatter.awk" "$WORK/scripts/frontmatter.awk"
    # Scrub every placeholder except the one under test. Bracketed names are matched with the
    # brackets escaped; the others are plain strings.
    sed -e "$([ "$keep" = 'Feature Name' ] || printf 's/\\[Feature Name\\]/feature/g')" \
        -e "$([ "$keep" = 'Requirement Name' ] || printf 's/\\[Requirement Name\\]/requirement/g')" \
        -e "$([ "$keep" = 'SPEC-XXX' ] || printf 's/SPEC-XXX/SPEC-000/g')" \
        "$ROOT/docs/templates/spec-template.md" > "$WORK/docs/specs/SPEC-000-template.md"
    got=$(cd "$WORK" && sh scripts/spec-lint.sh 2>&1) && rc=0 || rc=$?
    # The exit code is not asserted here: an unfilled copy of THIS repo's template also fails the
    # invariant-citation check, which is the template-unfilled case's claim, not this one's.
    case "$got" in
      *"unfilled template placeholder(s)"*) ;;
      *) unbound="$unbound '$keep'" ;;
    esac
  done
  if [ -z "$unbound" ]; then
    pass=$((pass + 1)); echo "ok    template-placeholders"
  else
    fail=$((fail + 1)); echo "FAIL  template-placeholders: left alone in the template, these placeholder(s) did not warn:$unbound"
  fi
fi

# A spec directory that does not exist must FAIL, not report "nothing to check": the cases above
# always build `docs/specs`, so this one drives the linter at a path that is not there. Under the
# same filter as the cases.
if [ -z "${SPEC_LINT_TEST_CASES:-}" ] && case missing-dir in *"$FILTER"*) true ;; *) false ;; esac; then
  rm -rf "$WORK"; mkdir -p "$WORK/scripts"
  cp "$ROOT/scripts/spec-lint.sh" "$WORK/scripts/spec-lint.sh"
  cp "$ROOT/scripts/frontmatter.awk" "$WORK/scripts/frontmatter.awk"
  got=$(cd "$WORK" && sh scripts/spec-lint.sh docs/nowhere 2>&1) && rc=0 || rc=$?
  case "$rc:$got" in
    1:*"docs/nowhere is not a directory"*) pass=$((pass + 1)); echo "ok    missing-dir" ;;
    *) fail=$((fail + 1)); echo "FAIL  missing-dir: exit $rc; a spec directory that does not exist must fail, not pass as empty"
       printf '%s\n' "$got" | sed 's/^/        /' | head -4 ;;
  esac
fi

echo "----"
echo "spec-lint-test: $pass passed, $fail failed."
[ "$fail" -eq 0 ] || exit 1

# A FLOOR, NOT AN EXIT CODE. Every mechanical way to lose the corpus — a renamed directory,
# a glob that stops matching — ends with zero cases run and `0 passed, 0 failed`, which
# exits 0 and prints a success line. Deliberately BELOW the measurement, so adding cases needs no
# edit here; removing them does, and that is the point. RE-DERIVED, not re-checked, when specs gained
# frontmatter and the template's corpus for them came down: the headroom is TWO, as in
# `docs-lint-test.sh`, because a floor with room for a quarter of the corpus let every `cite-*` case
# vanish on a success line — measured — and that family is the one check here the template does not
# have. A family smaller than three is not protected by a floor at all; that is what its own case is
# for. Not applied to the self-test invocation, which runs a handful of planted cases.
# A filter typo is the same shape at a smaller scale and gets its own line: a filtered run
# that matched nothing has proved nothing.
if [ -n "$FILTER" ] && [ "$pass" -eq 0 ]; then
  echo "spec-lint-test: the filter '$FILTER' matched no case. Nothing ran, so nothing passed."
  exit 1
fi
CASES_MIN=137
if { [ -z "${SPEC_LINT_TEST_CASES:-}" ] || [ -n "${SPEC_LINT_TEST_FLOOR:-}" ]; } && [ -z "$FILTER" ] && [ "$pass" -lt "$CASES_MIN" ]; then
  echo "spec-lint-test: only $pass cases ran, against a floor of $CASES_MIN. The corpus has"
  echo "shrunk or gone missing — a run over nothing exits 0 and looks exactly like a healthy"
  echo "one. If cases were removed on purpose, lower CASES_MIN in the same change."
  exit 1
fi

# ── self-test: the three fixture guards above have to fire ─────────────────────
#
# A case that trips a guard is by construction a FAILING case, so none can live in
# tests/spec-lint without leaving the corpus red. They get a corpus of their own, run
# here against a temp directory and asserting the FAIL TEXT rather than the exit code.
# Removing any one of the three guards turns its planted case green, which is the claim.
if [ -z "${SPEC_LINT_TEST_CASES:-}" ] && [ -z "$FILTER" ]; then
  SELF="$WORK.self"
  rm -rf "$SELF"; mkdir -p "$SELF"
  skeleton='@@@ file docs/invariants.md
## 1. One
@@@ file docs/specs/SPEC-001-x.md
---
type: Spec
id: SPEC-001
title: X
description: A spec.
spec_status: Completed
depends_on: []
---

## Overview
## Scope
## Functional Requirements
## Implementation Phases'

  # 1. No `@@@ file` at all: nothing is written, so the linter examines an empty tree.
  printf '@@@ expect exit=0\n' > "$SELF/no-file-ok.case"
  # 2. A recognised directive naming a file the splitter never writes.
  printf '@@@ expect exit=0\n@@@ file docs/never.md\n%s\n' "$skeleton" > "$SELF/unwritten-ok.case"
  # 3. Three typos that each break the `@@@ ` prefix the splitter keys on, named one at a
  #    time rather than derived from the typo, so two cannot collide on one filename.
  set -- 'double-space:@@@  file' 'no-space:@@@file' 'four-at:@@@@ file'
  for pair in "$@"; do
    n=${pair%%:*}; typo=${pair#*:}
    printf '@@@ expect exit=0\n%s docs/typo.md\ncontent\n%s\n' "$typo" "$skeleton" > "$SELF/typo-$n-ok.case"
  done

  planted=$(ls "$SELF" | wc -l | tr -d ' ')
  self_out=$(SPEC_LINT_TEST_CASES="$SELF" sh "$0" 2>&1) && self_rc=0 || self_rc=$?
  rm -rf "$SELF"
  self_fail=0
  for want in \
    "no-file-ok: declares no '@@@ file' directive" \
    "unwritten-ok: declared file(s) never written" \
    "an '@@@' line is not one of file/expect exit=/match/absent"
  do
    case "$self_out" in
      *"$want"*) ;;
      *) echo "FAIL  self-test: a guard did not fire — expected \"$want\"."; self_fail=1 ;;
    esac
  done
  case "$self_out" in *"ok    "*) echo "FAIL  self-test: a deliberately vacuous case reported ok."; self_fail=1 ;; esac
  [ "$self_rc" -ne 0 ] || { echo "FAIL  self-test: the harness exited 0 on a directory of vacuous cases."; self_fail=1; }
  if [ "$self_fail" -ne 0 ]; then
    echo "----"
    echo "spec-lint-test: the fixture guards are not firing. A case whose files never reach"
    echo "disk would pass silently, which is how a harness goes green over nothing."
    exit 1
  fi
  echo "spec-lint-test: fixture guards verified — 3 guards, $planted vacuous cases refused."

  # The FLOOR has to fire too, and nothing above proves it: the self-test invocation sets
  # SPEC_LINT_TEST_CASES, which disables the floor so a handful of planted cases can run.
  # SPEC_LINT_TEST_FLOOR re-enables it for this one run over a copy of three real cases —
  # far under the floor — which must exit non-zero and name the floor. A floor that cannot
  # be shown to fire is a success line waiting to be printed over nothing.
  FLOOR="$WORK.floor"
  rm -rf "$FLOOR"; mkdir -p "$FLOOR"
  n=0
  for c in "$CASES"/*.case; do
    [ -f "$c" ] || continue
    cp "$c" "$FLOOR/"; n=$((n + 1)); [ "$n" -ge 3 ] && break
  done
  floor_out=$(SPEC_LINT_TEST_CASES="$FLOOR" SPEC_LINT_TEST_FLOOR=1 sh "$0" 2>&1) && floor_rc=0 || floor_rc=$?
  rm -rf "$FLOOR"
  floor_fail=0
  [ "$floor_rc" -ne 0 ] || { echo "FAIL  self-test: the harness exited 0 on $n cases, under a floor of $CASES_MIN."; floor_fail=1; }
  case "$floor_out" in
    *"against a floor of $CASES_MIN"*) ;;
    *) echo "FAIL  self-test: a run under the floor did not name the floor."; floor_fail=1 ;;
  esac
  if [ "$floor_fail" -ne 0 ]; then
    echo "----"
    echo "spec-lint-test: the case floor is not firing. A corpus that shrinks to nothing would"
    echo "print a success line, which is the failure the floor exists to refuse."
    exit 1
  fi
  echo "spec-lint-test: the case floor fires — $n cases refused against $CASES_MIN."
fi

# ── self-test: spec-lint's two reader guards have to fire ──────────────────────
#
# spec-lint trusts the shared reader only after two checks — its exit status, and exactly one
# verdict record per file — and neither alone is enough. An awk that does not parse exits
# non-zero and prints nothing; an awk that parses and prints nothing exits 0, which is the
# vacuous pass the second guard exists for: every frontmatter check would iterate over nothing.
# Each is planted against a copy, with a third plant for a reader that is not there at all.
# Every one must stop spec-lint with exit 2 and say why; a green lint here is the defect.
if [ -z "${SPEC_LINT_TEST_CASES:-}" ] && [ -z "$FILTER" ]; then
  rg_fail=0
  for plant in 'syntax:the frontmatter reader failed on' 'silent:returned 0 verdict records' 'missing:is missing'; do
    kind=${plant%%:*}; want=${plant#*:}
    rm -rf "$WORK"; mkdir -p "$WORK/scripts" "$WORK/docs/specs"
    cp "$ROOT/scripts/spec-lint.sh" "$WORK/scripts/spec-lint.sh"
    case "$kind" in
      syntax) printf 'BEGIN {\n' > "$WORK/scripts/frontmatter.awk" ;;
      silent) printf 'BEGIN { exit 0 }\n' > "$WORK/scripts/frontmatter.awk" ;;
    esac
    printf '%s\n' '---' 'type: Spec' '---' > "$WORK/docs/specs/SPEC-001-x.md"
    got=$(cd "$WORK" && sh scripts/spec-lint.sh 2>&1) && rc=0 || rc=$?
    case "$rc:$got" in
      2:*"$want"*) ;;
      *) echo "FAIL  self-test: a reader planted as '$kind' did not stop spec-lint — exit $rc, wanted 2 and \"$want\"."
         printf '%s\n' "$got" | sed 's/^/        /' | head -4
         rg_fail=1 ;;
    esac
  done
  if [ "$rg_fail" -ne 0 ]; then
    echo "----"
    echo "spec-lint-test: the reader guards are not firing. A reader that fails or prints nothing"
    echo "would leave every frontmatter check passing over nothing."
    exit 1
  fi
  echo "spec-lint-test: reader guards verified — a reader that does not parse, prints nothing, or is missing stops spec-lint."
fi
