#!/bin/sh
# install-test.sh — the fixture corpus for install.sh's enforcement summary.
#
# Why this exists. That summary is the only thing standing between a mismatched
# `enforce-branches` and an install that looks healthy and refuses nothing, and it shipped with
# two escapes of exactly that kind: an EMPTY pattern counted as matching every branch (an empty
# ERE matches every line) and so reported total coverage for the one setting that has none, and
# an INVALID ERE left the count empty and compared it as an integer. Both were found by review
# rather than by running it. A summary nobody tests is a summary that reports what it likes.
#
# Each case asserts the specific TEXT, not the exit code: the four pattern states differ only in
# what they say, and three of them exit 0.
#
# SAFETY: every case builds its own throwaway repo under $TMPDIR and installs into it.
# `PR_QUEUE_DIR` relocates the queue but NOT the hook, so running install.sh anywhere near a real
# checkout rewrites that checkout's .git/hooks/pre-push — which is why nothing here runs in the
# repo this script lives in.
#
# Usage: sh scripts/pr-queue/install-test.sh
# POSIX sh, no dependencies beyond git.

set -eu
SRC="$(cd "$(dirname "$0")" && pwd)"
WORK="${TMPDIR:-/tmp}/pr-queue-install-test.$$"
trap 'rm -rf "$WORK"' EXIT INT TERM
pass=0; fail=0

sh -n "$SRC/install.sh" || { echo "FAIL  install.sh does not parse"; exit 1; }

# A repo with a main branch, one commit, and whatever extra branches the case wants.
scratch() {
  d="$WORK/$1"; shift
  mkdir -p "$d" && git -C "$d" init -q -b main
  git -C "$d" -c user.email=t@t -c user.name=t commit -q --allow-empty -m init
  mkdir -p "$d/scripts/pr-queue"
  cp "$SRC/install.sh" "$SRC/queue.sh" "$SRC/pre-push" "$SRC/PROTOCOL.md" "$d/scripts/pr-queue/"
  for b in "$@"; do git -C "$d" branch -q "$b" main; done
  printf '%s' "$d"
}

# run <name> <repo> <expect-exit> <@none|@empty|pattern> -- <substring>...
#
# The argument mode is a TOKEN, not an optional string: `${pat:+"$pat"}` drops an empty argument,
# so "install with no pattern" and "install with an explicit empty pattern" were the same call —
# and the empty case silently tested the default until this corpus said so on its first run.
run() {
  name="$1"; d="$2"; want="$3"; mode="$4"; shift 4
  shift   # the --
  case "$mode" in
    @none)  out=$(cd "$d" && PR_QUEUE_DIR="$WORK/q-$name" sh scripts/pr-queue/install.sh 2>/dev/null) && rc=0 || rc=$? ;;
    @empty) out=$(cd "$d" && PR_QUEUE_DIR="$WORK/q-$name" sh scripts/pr-queue/install.sh '' 2>/dev/null) && rc=0 || rc=$? ;;
    *)      out=$(cd "$d" && PR_QUEUE_DIR="$WORK/q-$name" sh scripts/pr-queue/install.sh "$mode" 2>/dev/null) && rc=0 || rc=$? ;;
  esac
  ok=1; why=""
  [ "$rc" = "$want" ] || { ok=0; why="exit $rc, wanted $want"; }
  for want_text in "$@"; do
    case "$out" in *"$want_text"*) ;; *) ok=0; why="${why:+$why; }stdout lacked: $want_text" ;; esac
  done
  if [ "$ok" = 1 ]; then pass=$((pass + 1)); echo "ok    $name"
  else fail=$((fail + 1)); echo "FAIL  $name: $why"; printf '%s\n' "$out" | sed 's/^/        /'; fi
}

# 1 — the default matches the branch shapes this family of repos uses.
d=$(scratch default spec/056-x docs/thing ci/pin test/x fix/y feat/a chore/b perf/c refactor/d)
run default "$d" 0 @none -- "(9 of 9 local branches besides main)"
# The POSITIVE hook case. Case 6 asserts a foreign hook is not overwritten; nothing asserted that
# ours is written, so deleting the wrapper heredoc left this corpus at 8 passed while the summary
# went on printing the `hook:` line the script header tells a reader to trust.
#
# Matched on the queue directory's TAIL, not its full path: install.sh absolutises $Q with
# `cd && pwd`, which on macOS resolves $TMPDIR's /var to /private/var, so a full-path comparison
# fails for a reason that has nothing to do with the hook.
if grep -q PR_QUEUE_WRAPPER "$d/.git/hooks/pre-push" 2>/dev/null && [ -x "$d/.git/hooks/pre-push" ] &&
   grep -q "q-default/pre-push" "$d/.git/hooks/pre-push"; then
  pass=$((pass + 1)); echo "ok    default-hook-written"
else
  fail=$((fail + 1)); echo "FAIL  default-hook-written: no executable wrapper pointing at the queue"
fi

# 2 — a pattern that matches nothing, with branches present to match.
d=$(scratch nomatch spec/056-x docs/thing)
run nomatch "$d" 0 '^nope-' -- "(0 of 2 local branches besides main)" "matches none of them"

# 3 — EMPTY: pre-push exits before testing any ref, and an empty ERE would otherwise count as
#     matching everything. The summary must name the state, not print a count.
d=$(scratch empty spec/056-x docs/thing)
run empty "$d" 0 @empty -- "enforcing: (empty)" "NOTHING is enforced" "set a pattern"

# 4 — INVALID: grep exits 2, the count is empty, and the old summary compared "" as an integer.
d=$(scratch invalid spec/056-x)
run invalid "$d" 0 '^spec[' -- "NOT A VALID ERE" "fix the expression"

# 5 — a fresh clone has no branches of its own; silence, not a warning.
d=$(scratch fresh)
run fresh "$d" 0 @none -- "(0 of 0 local branches besides main)"
out=$(cd "$d" && PR_QUEUE_DIR="$WORK/q-fresh2" sh scripts/pr-queue/install.sh 2>/dev/null)
# The SPECIFIC text, not the bare word: install.sh also warns when `gh` is absent, and matching
# on WARNING failed this case for that instead — a false accusation about the branch-count logic.
case "$out" in *"matches none of them"*) fail=$((fail + 1)); echo "FAIL  fresh: warned about a repo with no branches to match" ;;
                                      *) pass=$((pass + 1)); echo "ok    fresh-no-warning" ;; esac

# 6 — a foreign pre-push is left alone, and the summary must SAY it was left alone rather than
#     printing the install line: that line is what the header tells a reader to check.
d=$(scratch blocked spec/056-x)
printf '#!/bin/sh\nexit 0\n' > "$d/.git/hooks/pre-push"; chmod +x "$d/.git/hooks/pre-push"
run blocked "$d" 3 @none -- "LEFT ALONE" "nothing is enforced"
grep -q 'PR_QUEUE_WRAPPER' "$d/.git/hooks/pre-push" && { fail=$((fail + 1)); echo "FAIL  blocked: the foreign hook was overwritten"; } || { pass=$((pass + 1)); echo "ok    blocked-hook-intact"; }

echo "----"
echo "install-test: $pass passed, $fail failed."
[ "$fail" -eq 0 ]
