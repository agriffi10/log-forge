#!/bin/sh
# docstring-lint-test.sh — the fixture corpus for scripts/docstring-lint.py.
#
# Why this exists. Running the checker over `src/` proves `src/` passes. It proves nothing
# about whether any check still fires, and a gate whose checks have gone quiet is
# indistinguishable from a healthy repo — which is how four rounds of regressions reached
# main in `docs-lint.sh` before it got a corpus of its own.
#
# Each case asserts the specific FAIL TEXT, not just the exit code. A check that fails for
# the wrong reason gets "fixed" by changing the wrong thing, and the empty-summary-line case
# is exactly that hazard: a naive implementation reports it as "does not end in '.'".
#
# Cases named `*-ok.case` assert the checker stays SILENT. Half of a linter's regressions
# are false positives, and a corpus of only-failures cannot see one. The silence cases are
# where the EXEMPTIONS live — `@overload`, each directive prefix, `Yields:` for `Returns:`,
# the class exemption, the exempt module-docstring paths — and where `SENTENCE_SPLIT` is
# defended: drop its `\s+` and 39 real summary lines in `src/` become false positives, a
# mutant no failing case can see.
#
# THE `cp` BELOW IS LOAD-BEARING. The checker resolves its own root from `__file__`, so it
# must be run from a COPY inside $WORK. Invoke it at its real path instead and it lints the
# real `src/log_foundry`, ignores the fixture entirely, and every `-ok` case passes with no
# relationship to what it contains — half the corpus silently vacuous while failing cases
# still red loudly. Measured while writing this.
#
# Directives are `@@@ `-prefixed, matching `docs-lint-test.sh`; a fixture whose own content
# began `--- ` was once silently truncated there. Python fixtures make that likelier, not
# less: `# ---` is a legal comment.
#
# Usage: sh scripts/docstring-lint-test.sh [case-name-substring]
# POSIX sh — no dependencies beyond python3.

set -eu
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CASES="$ROOT/tests/docstring-lint"
FILTER="${1:-}"
WORK="${TMPDIR:-/tmp}/docstring-lint-test.$$"
trap 'rm -rf "$WORK"' EXIT INT TERM

# Parse the checker before exercising it. A SyntaxError partway through would otherwise
# surface as every case failing for the same wrong reason, which reads as a broken corpus
# rather than a broken checker.
if ! python3 -c "import ast,sys; ast.parse(open(sys.argv[1]).read())" "$ROOT/scripts/docstring-lint.py"; then
  echo "FAIL  scripts/docstring-lint.py does not parse."
  exit 1
fi

# The corpus must run on the interpreter the gate runs on. Every fixture here parses on 3.9,
# so on an older `python3` the cases pass while the gate SyntaxErrors on the real `src/` —
# a corpus reporting green for a gate that examines nothing. Checked once, loudly.
if ! python3 -c "import sys; sys.exit(0 if sys.version_info >= (3, 12) else 1)"; then
  echo "FAIL  python3 is $(python3 -V 2>&1), but the gate needs >= 3.12 to parse src/."
  echo "      Run: poetry run sh scripts/docstring-lint-test.sh"
  exit 1
fi

pass=0; fail=0
for case_file in "$CASES"/*.case; do
  [ -f "$case_file" ] || continue
  name=$(basename "$case_file" .case)
  case "$name" in *"$FILTER"*) ;; *) continue ;; esac

  rm -rf "$WORK"; mkdir -p "$WORK/scripts" "$WORK/src/log_foundry/sinks"
  cp "$ROOT/scripts/docstring-lint.py" "$WORK/scripts/docstring-lint.py"

  # `harness-reject` is an inverted expectation: the case passes when the harness REFUSES
  # to run it. Without it an empty fixture could only be tested by shipping one, which
  # would make this script exit 1 and contradict its own contract. The flag is read with
  # grep -q, not `sed -n s///p` — that substitution yields an EMPTY string on a match, so a
  # truth test on its output never fires and the guard ships inert.
  want_reject=0
  grep -q '^@@@ expect harness-reject$' "$case_file" && want_reject=1
  want_exit=$(sed -n 's/^@@@ expect exit=//p' "$case_file")

  awk -v work="$WORK" '
    /^@@@ file / { path = work "/" substr($0, 10); system("mkdir -p $(dirname \"" path "\")"); out = path; next }
    /^@@@ / { out = ""; next }
    out { print >> out }
  ' "$case_file"

  # Refuse a case that wrote no fixture, or wrote only empty ones. A case whose fixture
  # never reached disk passes vacuously otherwise — the construct it exists to test is not
  # there, so nothing fires and the expectation "exit=0" is met for the wrong reason.
  written=$(find "$WORK/src" -name '*.py' -size +0c 2>/dev/null | wc -l | tr -d ' ')
  if [ "$written" = "0" ]; then
    if [ "$want_reject" = "1" ]; then
      pass=$((pass + 1)); echo "ok    $name"
    else
      fail=$((fail + 1)); echo "FAIL  $name: wrote no non-empty fixture — the case cannot test anything"
    fi
    continue
  fi
  if [ "$want_reject" = "1" ]; then
    fail=$((fail + 1)); echo "FAIL  $name: expected the harness to reject it, but a fixture was written"
    continue
  fi

  got=$(cd "$WORK" && python3 scripts/docstring-lint.py 2>&1) && rc=0 || rc=$?
  ok=1
  [ "$rc" = "${want_exit:-0}" ] || { ok=0; why="exit $rc, wanted ${want_exit:-0}"; }
  if [ "$ok" = 1 ]; then
    sed -n 's/^@@@ match //p' "$case_file" | while IFS= read -r m; do
      [ -n "$m" ] || continue
      case "$got" in *"$m"*) ;; *) echo "MISSING|$m" ;; esac
    done > "$WORK/.miss"
    if [ -s "$WORK/.miss" ]; then ok=0; why="output lacked: $(sed 's/^MISSING|//' "$WORK/.miss" | head -1)"; fi
  fi
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

# The version floor, exercised directly. No fixture can reach it — the corpus runs on the
# interpreter the gate needs, so the guard never fires and its mutant is equivalent there.
# Forcing the version is machine-independent; relying on an older python3 being installed
# would make this pass or skip depending on the box.
if [ -z "$FILTER" ] || case version-floor in *"$FILTER"*) true ;; *) false ;; esac; then
  got=$(python3 -c '
import runpy, sys
sys.version_info = (3, 9, 6, "final", 0)
try:
    runpy.run_path(sys.argv[1], run_name="__main__")
except SystemExit as exc:
    sys.exit(exc.code)
' "$ROOT/scripts/docstring-lint.py" 2>&1) && rc=0 || rc=$?
  case "$rc:$got" in
    1:*"needs Python >= 3.12"*) pass=$((pass + 1)); echo "ok    version-floor" ;;
    *) fail=$((fail + 1)); echo "FAIL  version-floor: exit $rc, output: $got" ;;
  esac
fi

echo "----"
echo "docstring-lint-test: $pass passed, $fail failed."
[ "$fail" -eq 0 ] || exit 1
