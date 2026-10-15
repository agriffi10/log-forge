#!/usr/bin/env bash
# Where parallel sessions' work meets: each live worktree's branch and touched files, the files two
# worktrees share, and textual conflicts between committed branches and with origin/main.
#
# Read-only toward every peer tree: GIT_OPTIONAL_LOCKS=0 stops git taking index.lock in a tree a
# session is committing in, and nothing here stages, stashes or checks out. bash 3.2-safe, because
# that is the bash a Mac ships.
#
# A worktree whose branch has a merged PR is skipped: squash merges leave its commits unmerged by
# ancestry, so it would otherwise report conflicts that no longer exist. A pair conflict whose files
# are all already one side's conflicts with main is printed as CONFLICT-ALSO-MAIN rather than
# CONFLICT: usually it is main's conflict seen through a peer that rebased, but a file can conflict
# with main in one hunk and with the peer in another, so it is flagged for a look, never dropped.
#
# Usage: scripts/supervisor/overlap.sh
#   lines start WT, SHARED, CONFLICT, CONFLICT-ALSO-MAIN or CONFLICT-WITH-MAIN
set -u
export GIT_OPTIONAL_LOCKS=0 LC_ALL=C
common=$(git -C "$(dirname "$0")" rev-parse --path-format=absolute --git-common-dir) || exit 3
REPO=$(dirname "$common")
cd "$REPO" || exit 3
git fetch -q origin 2>/dev/null
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

gh pr list --state merged --limit 100 --json headRefName --jq '.[].headRefName' > "$TMP/merged" 2>/dev/null
git worktree list --porcelain | awk '/^worktree /{print substr($0,10)}' \
  | grep -E '\.claude/worktrees/[a-z]+-[a-z]+-[0-9a-f]{6}$|/scratchpad/wt-' > "$TMP/wts"

while IFS= read -r wt; do
  [ -d "$wt" ] || continue
  br=$(git -C "$wt" branch --show-current 2>/dev/null); [ -n "$br" ] || continue
  grep -qxF "$br" "$TMP/merged" && continue
  mb=$(git -C "$wt" merge-base HEAD origin/main 2>/dev/null) || continue
  ahead=$(git -C "$wt" rev-list --count origin/main..HEAD)
  files=$( { git -C "$wt" diff --name-only "$mb" 2>/dev/null; git -C "$wt" ls-files --others --exclude-standard 2>/dev/null; } | sort -u)
  [ -z "$files" ] && continue
  name=$(basename "$wt")
  case "$wt" in *scratchpad/wt-*)
    owner=$(echo "$wt" | sed -nE 's#.*claude-worktrees-([a-z]+-[a-z]+-[0-9a-f]{6})/.*#\1#p')
    session=$(echo "$wt" | sed -nE 's#.*/([0-9a-f]{8})[0-9a-f-]*/scratchpad/.*#\1#p')
    name="${owner:-main-$session}:$name";;
  esac
  echo "$br" > "$TMP/$name.branch"; echo "$ahead" > "$TMP/$name.ahead"
  printf '%s\n' "$files" > "$TMP/$name.files"
  dirty=$(git -C "$wt" status --porcelain 2>/dev/null | wc -l | tr -d ' ')
  echo "WT $name [$br] ahead=$ahead dirty=$dirty files=$(printf '%s\n' "$files" | grep -c .)"
done < "$TMP/wts"

cat "$TMP"/*.files 2>/dev/null | sort | uniq -d > "$TMP/shared"
while IFS= read -r f; do
  who=$(grep -lxF "$f" "$TMP"/*.files | xargs -n1 basename | sed 's/\.files$//' | tr '\n' ' ')
  echo "SHARED $f :: $who"
done < "$TMP/shared"

ls "$TMP"/*.ahead 2>/dev/null | while IFS= read -r f; do [ "$(cat "$f")" -gt 0 ] && basename "$f" .ahead; done > "$TMP/committed"
while IFS= read -r a; do
  : > "$TMP/$a.mainconf"
  out=$(git merge-tree --write-tree --name-only --no-messages origin/main "$(cat "$TMP/$a.branch")" 2>&1) || {
    echo "$out" | tail -n +2 | grep . > "$TMP/$a.mainconf"
    echo "CONFLICT-WITH-MAIN $a: $(tr '\n' ' ' < "$TMP/$a.mainconf")"
  }
done < "$TMP/committed"
while IFS= read -r a; do
  while IFS= read -r b; do
    [ "$a" \< "$b" ] || continue
    out=$(git merge-tree --write-tree --name-only --no-messages "$(cat "$TMP/$a.branch")" "$(cat "$TMP/$b.branch")" 2>&1) && continue
    files=$(echo "$out" | tail -n +2 | grep .)
    novel=$(printf '%s\n' "$files" | grep -vxF -f <(cat "$TMP/$a.mainconf" "$TMP/$b.mainconf"; echo '/nonexistent/'))
    if [ -n "$novel" ]; then
      echo "CONFLICT $a x $b: $(printf '%s\n' "$files" | tr '\n' ' ')"
    else
      echo "CONFLICT-ALSO-MAIN $a x $b: $(printf '%s\n' "$files" | tr '\n' ' ')"
    fi
  done < "$TMP/committed"
done < "$TMP/committed"
exit 0
