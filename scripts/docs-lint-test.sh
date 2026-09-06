#!/bin/sh
# docs-lint-test.sh — the fixture corpus for scripts/docs-lint.sh.
#
# Why this exists. `docs-lint.sh` shipped four rounds of fixes, and three of those rounds
# introduced a fresh escape while closing the previous one. Every regression was found by
# hand, after shipping, because CI ran the linter only against the repo's own live
# documents — which proves those documents pass and proves NOTHING about whether any
# check works. A green run on a linter whose checks have stopped firing looks exactly
# like a green run on a healthy repo.
#
# Each case asserts the specific FAIL TEXT, not just the exit code. A check that fails
# for the wrong reason is a check that will be "fixed" by changing the wrong thing, and
# several of the historical regressions produced a real failure with a misleading
# message. Cases named `*-ok.case` assert the linter stays SILENT: half of the
# regressions were false positives, and a corpus of only-failures would have missed them.
#
# Directives are `@@@ `-prefixed, not `--- `: a fixture whose CONTENT began "--- " was
# silently truncated at that line, so the construct it existed to test was not on disk and
# the case passed for the wrong reason. A thematic break (bare `---`) is legitimate markdown
# and every register fixture uses one.
#
# Usage: sh scripts/docs-lint-test.sh [case-name-substring]
# POSIX sh. It inherits docs-lint.sh's one dependency — `python3`, for check 15 — because it
# runs that script; see the note in its header.

set -eu
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# Overridable so the SELF-TEST at the bottom can point this script at a directory of
# deliberately malformed cases. Not a budget and not a threshold — pointing the harness
# at different fixtures cannot make a real case pass.
CASES="${DOCS_LINT_TEST_CASES:-$ROOT/tests/docs-lint}"
FILTER="${1:-}"
WORK="${TMPDIR:-/tmp}/docs-lint-test.$$"
trap 'rm -rf "$WORK"' EXIT INT TERM

# Parse the linter before exercising it. A syntax error partway through a shell script can
# end a run with status 0 — the linter never reaches its checks and reports success on a
# script that did nothing. That happened once while it was being written, and this used to
# be a separate CI step; the corpus is where it lives now that the gate is local.
if ! sh -n "$ROOT/scripts/docs-lint.sh"; then
  echo "FAIL  scripts/docs-lint.sh does not parse — a syntax error can exit 0 and look green."
  exit 1
fi

pass=0; fail=0
for case_file in "$CASES"/*.case; do
  [ -f "$case_file" ] || continue
  name=$(basename "$case_file" .case)
  case "$name" in *"$FILTER"*) ;; *) continue ;; esac

  rm -rf "$WORK"; mkdir -p "$WORK/scripts" "$WORK/docs/specs" "$WORK/docs/spec-delivery"

  # Small caps, so a fixture can be a few lines rather than a few kilobytes. Applied by
  # rewriting the constants in a COPY — deliberately not env overrides, because a budget
  # a caller can lower is a budget CI can be told to ignore.
  sed -e 's/^CLAUDE_MAX_BYTES=.*/CLAUDE_MAX_BYTES=2000/' \
      -e 's/^KEY_DECISIONS_MAX_BYTES=.*/KEY_DECISIONS_MAX_BYTES=1200/' \
      -e 's/^DIGEST_MAX_BYTES=.*/DIGEST_MAX_BYTES=300/' \
      -e 's/^DELIVERY_MAX_LINES=.*/DELIVERY_MAX_LINES=10/' \
      -e 's/^ALWAYS_LOADED_MAX_BYTES=.*/ALWAYS_LOADED_MAX_BYTES=1500/' \
      "$ROOT/scripts/docs-lint.sh" > "$WORK/scripts/docs-lint.sh"
  chmod +x "$WORK/scripts/docs-lint.sh"

  # Split the case into its expectations and its files.
  want_exit=$(sed -n 's/^@@@ expect exit=//p' "$case_file")
  awk -v work="$WORK" '
    /^@@@ file / { path = work "/" substr($0, 10); system("mkdir -p $(dirname \"" path "\")"); out = path; next }
    /^@@@ / { out = ""; next }
    out { print >> out }
  ' "$case_file"

  # A fixture that never reaches disk makes its case silent for the WRONG reason, and an
  # `-ok` case cannot tell that apart from a pass: two planted cases — one with a mistyped
  # `@@@  file` directive, one writing outside the linter population — both reported ok.
  # So the directives are checked before the linter is allowed to have an opinion.
  decl=$(sed -n 's|^@@@ file ||p' "$case_file")
  if [ -z "$decl" ]; then
    fail=$((fail + 1)); echo "FAIL  $name: declares no '@@@ file' directive, so nothing reached disk."
    continue
  fi
  missing=""
  for d in $decl; do [ -f "$WORK/$d" ] || missing="$missing $d"; done
  if [ -n "$missing" ]; then
    fail=$((fail + 1)); echo "FAIL  $name: declared file(s) never written:$missing"
    continue
  fi
  # A directive the splitter does not recognise is DISCARDED silently along with the body
  # it introduced, which is how the mistyped one got through. Count them instead — and
  # count on `^@@@`, not `^@@@ `, because `@@@file`, `@@@<tab>file` and `@@@@ file` break
  # the prefix the splitter keys on and would otherwise escape this guard as well as the
  # one above it.
  if [ "$(grep -c '^@@@' "$case_file")" != "$(grep -cE '^@@@ (file |expect exit=|match )' "$case_file")" ]; then
    fail=$((fail + 1)); echo "FAIL  $name: an '@@@' line is not one of file/expect exit=/match."
    continue
  fi

  got=$(cd "$WORK" && sh scripts/docs-lint.sh 2>&1) && rc=0 || rc=$?
  ok=1
  [ "$rc" = "${want_exit:-0}" ] || { ok=0; why="exit $rc, wanted ${want_exit:-0}"; }
  if [ "$ok" = 1 ]; then
    sed -n 's/^@@@ match //p' "$case_file" | while IFS= read -r m; do
      [ -n "$m" ] || continue
      case "$got" in *"$m"*) ;; *) echo "MISSING|$m" ;; esac
    done > "$WORK/.miss"
    if [ -s "$WORK/.miss" ]; then ok=0; why="output lacked: $(sed 's/^MISSING|//' "$WORK/.miss" | head -1)"; fi
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

echo "----"
echo "docs-lint-test: $pass passed, $fail failed."
[ "$fail" -eq 0 ] || exit 1

# A FLOOR, NOT AN EXIT CODE. Every mechanical way to lose the corpus — a renamed
# directory, a glob that stops matching, a filter typo, an empty CASES path — ends with
# zero cases run and `0 passed, 0 failed`, which exits 0 and prints a success line. That
# is the whole-corpus version of the defect the per-case guards below exist to close, and
# a green run over nothing is indistinguishable from a green run over everything.
#
# Deliberately BELOW the measurement rather than at it, so adding cases needs no edit
# here. Removing them does, and that is the point: a case pruned on purpose is a
# one-line, deliberate lowering in the same change; a case lost by accident reds the run.
# Not applied to the self-test invocation, which runs a handful of planted cases.
# Raised from 80 with SPEC-053's corpus (86 cases at `212fd16` -> 146 at `fe54f6a`; the 137
# written here while that change was in flight was stale before it merged). The old floor was slack enough that
# deleting EVERY marking-walk case left 86 cases running, a success line and exit 0 — measured,
# which is the whole failure this floor is written against. It stays deliberately below the
# measurement so adding cases needs no edit here; it just is not 36 cases below any more.
#
# RE-DERIVED, not re-checked, when the routed-tier and per-area checks brought their own cases in
# from the template (146 at `fe54f6a` -> 222 here): a floor left at 125 could no longer fire on losing either whole
# group, which is the same defect one tier up — a fence that cannot fail is still advertised as one.
CASES_MIN=200
if [ -z "${DOCS_LINT_TEST_CASES:-}" ] && [ -z "$FILTER" ] && [ "$pass" -lt "$CASES_MIN" ]; then
  echo "docs-lint-test: only $pass cases ran, against a floor of $CASES_MIN. The corpus has"
  echo "shrunk or gone missing — a run over nothing exits 0 and looks exactly like a healthy"
  echo "one. If cases were removed on purpose, lower CASES_MIN in the same change."
  exit 1
fi

# ── self-test: the three guards above have to fire ─────────────────────────────
#
# The guards exist because a case whose fixture never reached disk reported `ok` — a
# silence case passing for the wrong reason, in the one file whose entire job is to make
# silence mean something. Nothing in tests/docs-lint can cover them: a case that trips a
# guard is by construction a FAILING case, so shipping one would leave the corpus red.
#
# WATCH OUT when adding one: `scripts/**` is inside check 9's own population, and the
# `tests/` exclusion that lets a .case file carry the gated form does not reach this file.
# A self-test case that must contain "measured <ISO date>" therefore has to build the
# string from a variable or sit inside a fenced block, or `docs-lint.sh` fails on this
# very script. None of the cases below needs it, which is why none of them does it.
#
# So the guards get a corpus of their own, run here against a temp directory and asserting
# the FAIL TEXT rather than the exit code — each guard has a distinct message, and a guard
# that fires for the wrong reason is one that gets fixed in the wrong place. Removing any
# one of these three guards turns its case green, which is the whole claim.
if [ -z "${DOCS_LINT_TEST_CASES:-}" ] && [ -z "$FILTER" ]; then
  SELF="$WORK.self"
  rm -rf "$SELF"; mkdir -p "$SELF"
  skeleton='@@@ file CLAUDE.md
# Project

## Key Decisions

Intro prose.

| Area | Fences |
|---|---|
| Area one | `docs/decisions/area-one.md#fences` |
@@@ file docs/decisions/INDEX.md
# Register

## Areas

| Area | Fences | Governs |
|---|---|---|
| Area one | `docs/decisions/area-one.md#fences` | none |
@@@ file docs/decisions/area-one.md
# Area one — decisions

intro

## Contents

- [Fences](#fences)
- [Alpha rule](#alpha-rule)

## Fences

- **Alpha rule** — short claim.

---

### Alpha rule

Reasoning.'

  # 1. No `@@@ file` at all: nothing is written, so the linter examines an empty tree.
  printf '@@@ expect exit=0\n' > "$SELF/no-file-ok.case"
  # 2. A recognised directive naming a file the splitter never writes.
  printf '@@@ expect exit=0\n@@@ file docs/never.md\n%s\n' "$skeleton" > "$SELF/unwritten-ok.case"
  # 3. Three typos that each break the `@@@ ` prefix the splitter keys on. The body is
  #    discarded with the directive, so the fixture is absent and the case is vacuous.
  #    Named one at a time rather than derived from the typo: deriving them with
  #    `tr -c 'a-z' '-'` mapped two of the three to the same filename, so one silently
  #    overwrote the other and the count printed below was wrong. A generated name is a
  #    name nobody checks for collisions.
  set -- 'double-space:@@@  file' 'no-space:@@@file' 'four-at:@@@@ file'
  for pair in "$@"; do
    n=${pair%%:*}; typo=${pair#*:}
    printf '@@@ expect exit=0\n%s docs/typo.md\ncontent\n%s\n' "$typo" "$skeleton" > "$SELF/typo-$n-ok.case"
  done

  # Counted from disk, never asserted as a literal: the count that was hand-written here
  # was wrong the moment two planted names collided, in a change whose whole subject is
  # numbers that stop being true.
  planted=$(ls "$SELF" | wc -l | tr -d ' ')
  self_out=$(DOCS_LINT_TEST_CASES="$SELF" sh "$0" 2>&1) && self_rc=0 || self_rc=$?
  rm -rf "$SELF"
  self_fail=0
  for want in \
    "no-file-ok: declares no '@@@ file' directive" \
    "unwritten-ok: declared file(s) never written" \
    "an '@@@' line is not one of file/expect exit=/match"
  do
    case "$self_out" in
      *"$want"*) ;;
      *) echo "FAIL  self-test: a guard did not fire — expected \"$want\"."; self_fail=1 ;;
    esac
  done
  # Every planted case must be reported as a failure; none may report ok.
  case "$self_out" in *"ok    "*) echo "FAIL  self-test: a deliberately vacuous case reported ok."; self_fail=1 ;; esac
  [ "$self_rc" -ne 0 ] || { echo "FAIL  self-test: the harness exited 0 on a directory of vacuous cases."; self_fail=1; }
  if [ "$self_fail" -ne 0 ]; then
    echo "----"
    echo "docs-lint-test: the fixture guards are not firing. A case whose files never reach"
    echo "disk would pass silently, which is how this harness went green over nothing."
    exit 1
  fi
  echo "docs-lint-test: fixture guards verified — 3 guards, $planted vacuous cases refused."
fi

# ── self-test: a check that could not RUN must not read as a check that found nothing ──
#
# Check 15 is the one check here that shells to another interpreter, so it is the one that can
# stop working without any of its logic changing. It has no `command -v` guard on purpose (the
# comment beside it says why): a missing python3 and a broken one both exit non-zero and land on
# the same `note`. This is the only way to reach that line, and it cannot be a .case — the harness
# runs docs-lint.sh with the ambient PATH and a fixture cannot change it.
#
# The positive control is the point. Asserting only that the shimmed run reddens would pass on a
# fixture tree that was dirty for some unrelated reason, so the same tree is run BOTH ways: clean
# with a working python3, red with a broken one, and red for THIS reason.
if [ -z "${DOCS_LINT_TEST_CASES:-}" ] && [ -z "$FILTER" ]; then
  PYT="$WORK.py3"
  rm -rf "$PYT"; mkdir -p "$PYT/bin" "$PYT/tree/scripts"
  cp "$ROOT/scripts/docs-lint.sh" "$PYT/tree/scripts/docs-lint.sh"
  mkdir -p "$PYT/tree/docs/decisions"
  printf '# Project\n\n## Key Decisions\n\nIntro prose.\n\n| Area | Fences |\n|---|---|\n| Area one | `docs/decisions/area-one.md#fences` |\n' > "$PYT/tree/CLAUDE.md"
  printf '# Register\n\n## Areas\n\n| Area | Fences | Governs |\n|---|---|---|\n| Area one | `docs/decisions/area-one.md#fences` | none |\n' > "$PYT/tree/docs/decisions/INDEX.md"
  printf '# Area one — decisions\n\nintro\n\n## Contents\n\n- [Fences](#fences)\n- [Alpha rule](#alpha-rule)\n\n## Fences\n\n- **Alpha rule** — short claim.\n\n---\n\n### Alpha rule\n\nReasoning.\n' > "$PYT/tree/docs/decisions/area-one.md"
  printf '#!/bin/sh\nexit 127\n' > "$PYT/bin/python3"
  chmod +x "$PYT/bin/python3"

  control=$(cd "$PYT/tree" && sh scripts/docs-lint.sh 2>&1) && control_rc=0 || control_rc=$?
  shimmed=$(cd "$PYT/tree" && PATH="$PYT/bin:$PATH" sh scripts/docs-lint.sh 2>&1) && shim_rc=0 || shim_rc=$?
  rm -rf "$PYT"
  py_fail=0
  [ "$control_rc" -eq 0 ] || { echo "FAIL  self-test: the control tree is not clean with a working python3."; py_fail=1; }
  [ "$shim_rc" -ne 0 ] || { echo "FAIL  self-test: a python3 that cannot run left docs-lint.sh exiting 0."; py_fail=1; }
  case "$shimmed" in
    *"did not run — python3 is missing or the check failed"*) ;;
    *) echo "FAIL  self-test: a broken python3 did not produce check 15's did-not-run report."; py_fail=1 ;;
  esac
  if [ "$py_fail" -ne 0 ]; then
    echo "----"
    echo "docs-lint-test: check 15 can stop running without saying so. A gate that goes quiet when"
    echo "its interpreter is absent is indistinguishable from a gate that found nothing."
    exit 1
  fi
  echo "docs-lint-test: check 15 reports a broken interpreter rather than skipping."

  # The same rule one level down: a FILE the check could not read is a file it did not examine.
  # This cannot be a .case — the `@@@ file` splitter writes text through awk and the input needed
  # here is a byte sequence that is not UTF-8 — so it sits beside the interpreter test, and it is
  # run BOTH ways for the same reason: a red run has to be attributable to this file.
  BAD="$WORK.badbytes"
  rm -rf "$BAD"; mkdir -p "$BAD/tree/scripts" "$BAD/tree/docs/decisions"
  cp "$ROOT/scripts/docs-lint.sh" "$BAD/tree/scripts/docs-lint.sh"
  printf '# Project\n\n## Key Decisions\n\nIntro prose.\n\n| Area | Fences |\n|---|---|\n| Area one | `docs/decisions/area-one.md#fences` |\n' > "$BAD/tree/CLAUDE.md"
  printf '# Register\n\n## Areas\n\n| Area | Fences | Governs |\n|---|---|---|\n| Area one | `docs/decisions/area-one.md#fences` | none |\n' > "$BAD/tree/docs/decisions/INDEX.md"
  printf '# Area one — decisions\n\nintro\n\n## Contents\n\n- [Fences](#fences)\n- [Alpha rule](#alpha-rule)\n\n## Fences\n\n- **Alpha rule** — short claim.\n\n---\n\n### Alpha rule\n\nReasoning.\n' > "$BAD/tree/docs/decisions/area-one.md"
  control=$(cd "$BAD/tree" && sh scripts/docs-lint.sh 2>&1) && control_rc=0 || control_rc=$?
  printf '# Notes\n\n\377\376 not utf-8.\n' > "$BAD/tree/docs/notes.md"
  dirty=$(cd "$BAD/tree" && sh scripts/docs-lint.sh 2>&1) && dirty_rc=0 || dirty_rc=$?
  rm -rf "$BAD"
  bad_fail=0
  [ "$control_rc" -eq 0 ] || { echo "FAIL  self-test: the control tree is not clean before the bad file lands."; bad_fail=1; }
  [ "$dirty_rc" -ne 0 ] || { echo "FAIL  self-test: a file check 11 cannot read left docs-lint.sh exiting 0."; bad_fail=1; }
  case "$dirty" in
    *"docs/notes.md could not be read (UnicodeDecodeError), so this check did NOT examine it"*) ;;
    *) echo "FAIL  self-test: an unreadable file was skipped instead of reported."; bad_fail=1 ;;
  esac
  if [ "$bad_fail" -ne 0 ]; then
    echo "----"
    echo "docs-lint-test: check 15 can skip a file without saying so. A file the gate could not"
    echo "open is a file it did not examine, and silence there reads as a clean result."
    exit 1
  fi
  echo "docs-lint-test: check 15 reports a file it cannot read rather than skipping it."
fi
