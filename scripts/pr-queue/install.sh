#!/bin/sh
# install.sh — put the PR queue where several agent sessions can share it, and make it
# enforceable. Run once per checkout, before launching the agents.
#
#   sh scripts/pr-queue/install.sh [branch-regex]
#
# The queue must live OUTSIDE the repo and outside every worktree: a lock inside a worktree is
# invisible to peers, and a lock inside the repo is a file that itself conflicts. So this copies
# queue.sh, pre-push and PROTOCOL.md to a directory under $HOME and points a
# .git/hooks/pre-push wrapper at the copy there.
#
# The default location is keyed on the ORIGIN URL, not the directory name, because the invariant
# is about the remote: two checkouts of the same repo must share one queue, and two different
# repos that happen to share a directory name must not. Override with PR_QUEUE_DIR.
#
# THERE IS NO DRY RUN. PR_QUEUE_DIR relocates the queue but not the hook: this still writes
# .git/hooks/pre-push in the checkout it is run from, pointed at whichever queue it just built. A
# scratch run therefore takes the live hook with it — measured by doing exactly that to this
# repo — so try it in a throwaway clone, and read the `hook:` line below to see what was touched.
#
# [branch-regex] is the ERE for branches the hook enforces against. The default is
# '^(spec|docs|ci|test|fix|feat|chore|perf|refactor)[-/]', covering both the `spec-207` and
# `spec/207-name` conventions and the other prefixes that open PRs in these repos. Branches
# outside it push freely — see the fail-open note in pre-push. AN EXISTING SETTING IS KEPT when no
# argument is given, so widening this default does not reach a queue that already has one: pass
# the ERE to re-point it. The summary at the end reports how many of this repo's
# branches the pattern actually matches, because a default that matches none of them installs
# cleanly and enforces nothing.
#
# EXIT: 0 installed · 1 could not install · 3 queue installed but the hook was NOT (something
#       else already occupies .git/hooks/pre-push — the message says what to add by hand).

set -eu

SRC="$(cd "$(dirname "$0")" && pwd)"

git rev-parse --git-dir >/dev/null 2>&1 || { echo "error: run this from inside the checkout" >&2; exit 1; }

# The MAIN worktree, not whichever linked worktree this was run from: the queue records one
# checkout for its remote checks, and it has to be the one that will still be there tomorrow.
REPO="$(git worktree list --porcelain | sed -n '1s/^worktree //p')"
[ -n "$REPO" ] && [ -d "$REPO" ] || { echo "error: could not locate the main worktree" >&2; exit 1; }

if [ -n "${PR_QUEUE_DIR:-}" ]; then
  Q="$PR_QUEUE_DIR"
else
  url="$(git -C "$REPO" remote get-url origin 2>/dev/null || echo '')"
  if [ -n "$url" ]; then
    slug="$(printf '%s' "$url" | sed -e 's#^[a-zA-Z+]*://##' -e 's#^[^@/]*@##' -e 's#\.git$##' \
                                     -e 's#[^A-Za-z0-9._-]#-#g')"
  else
    slug="$(basename "$REPO")"
  fi
  Q="$HOME/.claude/pr-queue/$slug"
fi

# Absolutise before anything is compared or written: the guard below is a prefix test, which a
# relative path silently passes, and the same string goes into the hook wrapper — where a
# relative path resolves against whatever directory git happens to run the hook from, leaving a
# healthy-looking queue with no enforcement at all.
mkdir -p "$Q" || { echo "error: could not create $Q" >&2; exit 1; }
Q="$(cd "$Q" && pwd)"

# Outside the repo AND outside every worktree, not just the main one: a queue inside a linked
# worktree is invisible to that worktree's peers, which is the whole failure this avoids.
worktrees="$(git -C "$REPO" worktree list --porcelain | sed -n 's/^worktree //p')"
oldifs="$IFS"; IFS='
'
for wt in $worktrees; do
  case "$Q/" in
    "$wt"/*) echo "error: the queue may not live inside a worktree ($wt)" >&2
             rmdir "$Q" 2>/dev/null || true
             exit 1 ;;
  esac
done
IFS="$oldifs"

MAIN="$(git -C "$REPO" symbolic-ref --short refs/remotes/origin/HEAD 2>/dev/null | sed 's#^origin/##')"
[ -n "$MAIN" ] || MAIN=main

cp "$SRC/queue.sh" "$SRC/pre-push" "$SRC/PROTOCOL.md" "$Q/"
chmod +x "$Q/queue.sh" "$Q/pre-push"
printf '%s\n' "$REPO" > "$Q/repo"
printf '%s\n' "$MAIN" > "$Q/main-branch"

if [ "$#" -ge 1 ]; then
  printf '%s\n' "$1" > "$Q/enforce-branches"
elif [ ! -s "$Q/enforce-branches" ]; then
  # Both conventions, and the docs branches beside them. The previous default, '^spec-', matched
  # neither `spec/…` nor `docs/…`: in a repo using those it installed cleanly, printed its pattern,
  # and enforced nothing on any branch anyone would push — the silent half of the failure the
  # empty-pattern warning in PROTOCOL.md already covers.
  printf '%s\n' '^(spec|docs|ci|test|fix|feat|chore|perf|refactor)[-/]' > "$Q/enforce-branches"
fi

# --- the hook wrapper -------------------------------------------------------------------------
# Linked worktrees share the common git dir's hooks, so installing here covers every session on
# the checkout — which is the point, and also why the hook it execs fails open.
HOOKS="$(git -C "$REPO" config --get core.hooksPath || true)"
[ -n "$HOOKS" ] || HOOKS="$(git -C "$REPO" rev-parse --git-common-dir)/hooks"
case "$HOOKS" in /*) ;; *) HOOKS="$REPO/$HOOKS" ;; esac
mkdir -p "$HOOKS"
HOOK="$HOOKS/pre-push"

hook_status=installed
if [ -e "$HOOK" ] && ! grep -q PR_QUEUE_WRAPPER "$HOOK" 2>/dev/null; then
  hook_status=blocked
else
  cat > "$HOOK" <<WRAPPER
#!/bin/sh
# PR_QUEUE_WRAPPER — written by scripts/pr-queue/install.sh. Execs the real hook from the queue
# directory, so enforcement disappears with the queue rather than outliving it.
[ -x "$Q/pre-push" ] && exec "$Q/pre-push" "\$@"
exit 0
WRAPPER
  chmod +x "$HOOK"
fi

echo "PR queue installed."
echo "  queue:     $Q"
echo "  checkout:  $REPO"
echo "  main:      $MAIN"
# A pattern is only enforcement if it matches the branches this repo actually uses, so say so
# here rather than leaving it to be discovered by a push that should have been refused and was
# not. Counted over local branches other than the default one; a fresh clone has none of its own,
# which is why the warning is gated on there being some to match.
pat="$(cat "$Q/enforce-branches")"
branches="$(git -C "$REPO" for-each-ref --format='%(refname:short)' refs/heads | grep -vxF "$MAIN" || true)"
total=$(printf '%s' "$branches" | grep -c . || true)
# Three states, and only one of them is a count. An EMPTY pattern is what pre-push tests for
# first and exits on, so it enforces nothing — but an empty ERE matches every line, so counting
# it would report total coverage for the one setting that has none. An INVALID ERE makes grep
# exit 2 with empty output, which would print "( of 12)" and then compare an empty string as an
# integer. Both were live escapes in the first version of this summary.
# grep exits 2 on an invalid ERE and 1 on a valid one that simply did not match; `|| pat_rc=$?`
# is what keeps `set -e` from aborting on the ordinary no-match case.
pat_rc=0
printf 'x\n' | grep -qE "$pat" >/dev/null 2>&1 || pat_rc=$?
if [ -z "$pat" ]; then
  echo "  enforcing: (empty) — the hook exits before testing any ref, so NOTHING is enforced."
  echo "  WARNING: set a pattern: sh scripts/pr-queue/install.sh '^(spec|docs|ci|test|fix|feat|chore|perf|refactor)[-/]'"
elif [ "$pat_rc" -gt 1 ]; then
  echo "  enforcing: $pat — NOT A VALID ERE, so the hook matches nothing and refuses nothing."
  echo "  WARNING: fix the expression and re-run install.sh."
else
  matched=$(printf '%s' "$branches" | grep -cE "$pat" || true)
  echo "  enforcing: $pat  ($matched of $total local branches besides $MAIN)"
  [ "$matched" -eq "$total" ] || echo "             (agent worktree and scratch branches are expected not to match)"
  if [ "$total" -gt 0 ] && [ "$matched" -eq 0 ]; then
    echo "  WARNING: that pattern matches none of them, so the hook will refuse nothing. Re-run with
           the ERE your branches use, e.g. sh scripts/pr-queue/install.sh '^(spec|docs|ci|test|fix|feat|chore|perf|refactor)[-/]'"
  fi
fi
case "$hook_status" in
  installed) echo "  hook:      $HOOK -> $Q/pre-push" ;;
  blocked)   echo "  hook:      $HOOK — LEFT ALONE, it is not ours; nothing is enforced until the
             line below is added to it by hand" ;;
esac
command -v gh >/dev/null 2>&1 || echo "  WARNING: 'gh' not found — the built-in remote checks need it, or install
           your own 'open-prs', 'all-prs' and 'main-green' executables in $Q (see PROTOCOL.md)."
[ -x "$Q/open-prs" ] && echo "  NOTE: an existing 'open-prs' override is in place. It must now exclude DRAFT
        PRs; one written before drafts were exempted will reinstate draft-blocking silently."
echo
echo "Brief each agent with these four commands:"
echo "  $Q/queue.sh ticket  SPEC-XXX"
echo "  $Q/queue.sh turn    SPEC-XXX"
echo "  $Q/queue.sh acquire SPEC-XXX"
echo "  $Q/queue.sh release SPEC-XXX"
echo "Protocol: $Q/PROTOCOL.md"

if [ "$hook_status" = blocked ]; then
  echo
  echo "⚠️  THE HOOK WAS NOT INSTALLED — $HOOK already exists and is not ours." >&2
  echo "   The queue works, but nothing enforces it. Add this to that hook by hand:" >&2
  echo >&2
  echo "     [ -x \"$Q/pre-push\" ] && exec \"$Q/pre-push\" \"\$@\"   # PR_QUEUE_WRAPPER" >&2
  echo >&2
  exit 3
fi
