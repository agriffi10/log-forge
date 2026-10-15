#!/usr/bin/env bash
# The supervisor's watch: a normalised snapshot of the PR queue, open PRs, main's push-run verdict,
# any other run that failed on main's head, where sessions' work meets, and sessions that look ready
# to archive, printed as the lines that appeared (+) or disappeared (-) since the previous snapshot.
#
# Two modes, because the harness offers two ways to be woken:
#   --until-change  (default) exit on the first change. Run it with Bash run_in_background and
#                   relaunch after handling each event; a quiet night costs no wake-ups. With no
#                   state file yet, it prints the baseline as = lines and exits at once.
#   --stream        print every change and never exit, for the Monitor tool (which it outlives
#                   by expiring every 30 minutes and needing a re-arm).
#
# The state file persists between runs, so a relaunch never replays the baseline; delete it to see
# the whole snapshot once. The queue is read from the Q= line of the main checkout's pre-push hook,
# which scripts/pr-queue/install.sh writes when it points the hook at the queue it built.
#
# Usage: scripts/supervisor/watch.sh --self <this session's id> [--until-change|--stream]
#   SUPERVISOR_STATE_DIR  where watch.state lives (default: $TMPDIR/supervisor-<checkout name>)
#   SUPERVISOR_INTERVAL   seconds between snapshots (default 150)
set -u
export LC_ALL=C GIT_OPTIONAL_LOCKS=0
HERE=$(cd "$(dirname "$0")" && pwd)
common=$(git -C "$HERE" rev-parse --path-format=absolute --git-common-dir) || exit 3
REPO=$(dirname "$common")
Q=$(sed -n "s/^Q=['\"]\{0,1\}\([^'\"]*\)['\"]\{0,1\}$/\1/p" "$common/hooks/pre-push" 2>/dev/null | head -1)
MODE=until-change SELF=""
while [ $# -gt 0 ]; do
  case "$1" in
    --self) [ $# -ge 2 ] || { echo "--self needs a session id" >&2; exit 2; }; SELF=$2; shift 2;;
    --until-change) MODE=until-change; shift;;
    --stream) MODE=stream; shift;;
    *) echo "unknown argument: $1" >&2; exit 2;;
  esac
done
[ -n "$SELF" ] || { echo "--self <session id> is required, so this session is never offered for archive" >&2; exit 2; }
STATE_DIR=${SUPERVISOR_STATE_DIR:-${TMPDIR:-/tmp}/supervisor-$(basename "$REPO")}
mkdir -p "$STATE_DIR"
STATE="$STATE_DIR/watch.state"
INTERVAL=${SUPERVISOR_INTERVAL:-150}

snapshot() {
  cd "$REPO" || return
  if [ -z "$Q" ]; then
    echo "ALERT no Q= line in $common/hooks/pre-push; queue not watched"
  elif [ ! -d "$Q" ]; then
    echo "ALERT the queue named by the pre-push hook does not exist: $Q"
  elif [ -f "$Q/lock/holder" ]; then
    spec=$(awk '$1=="spec"{print $2}' "$Q/lock/holder"); br=$(awk '$1=="branch"{print $2}' "$Q/lock/holder")
    since=$(awk '$1=="since"{print $2}' "$Q/lock/holder")
    echo "QUEUE lock held by $spec on $br since $since"
    if t0=$(date -ju -f %Y-%m-%dT%H:%M:%SZ "$since" +%s 2>/dev/null); then
      [ $(( $(date -u +%s) - t0 )) -gt 4800 ] && echo "ALERT lock held by $spec for over 80 minutes"
    else
      echo "ALERT cannot read the lock's age from since=$since; the 80-minute check is off"
    fi
  else
    echo "QUEUE lock free"
  fi
  for t in "${Q:-/nonexistent}"/tickets/*; do [ -d "$t" ] || continue
    echo "QUEUE waiting $(basename "$t") $(cat "$t/spec" 2>/dev/null)"
  done
  if prs=$(gh pr list --state open --json number,headRefName,isDraft --jq '.[]|"PR open #\(.number) \(.headRefName)\(if .isDraft then " DRAFT" else "" end)"' 2>/dev/null); then
    [ -n "$prs" ] && echo "$prs"
  else echo "ALERT gh pr list failed"; fi
  # main is judged at the remote's head, not at the newest run: a push GitHub dropped leaves the
  # newest run on the PREVIOUS commit, green, while the merge was never deployed. A list that does
  # not parse prints an ALERT rather than no line, because a vanished `MAIN … RED` reads as recovered.
  head=$(git ls-remote origin refs/heads/main 2>/dev/null | awk '{print $1}')
  if [ -z "$head" ]; then
    echo "ALERT could not read main's head from origin"
  elif runs=$(gh run list --branch main --event push --limit 20 --json headSha,name,status,conclusion 2>/dev/null); then
    echo "$runs" | HEAD_SHA="$head" python3 -c '
import json, os, sys
sha = os.environ["HEAD_SHA"]
try:
    mine = [r for r in json.load(sys.stdin) if r["headSha"] == sha]
    bad = [r["name"] for r in mine if r["status"] == "completed" and r["conclusion"] not in ("success", "skipped", "neutral")]
    busy = [r for r in mine if r["status"] != "completed"]
except (ValueError, KeyError, TypeError) as e:
    print("ALERT could not parse the push runs on main (%s); MAIN unknown" % type(e).__name__)
    sys.exit(0)
verdict = "no push run yet" if not mine else "RED: " + ", ".join(bad) if bad else "building" if busy else "green"
print("MAIN %s %s" % (sha[:8], verdict))'
  else echo "ALERT gh run list failed"; fi
  # A failed run of any OTHER event on main's head never turns MAIN red, because it is not main's
  # build: the push-triggered Release run is, and its first job is ci.yml's gate. It is still news.
  # A scheduled run (Integration, pip-audit, Scorecard, Zizmor) catches drift no merge caused, and
  # this repo's queue.sh judges main by every event on its head, so the same failure that leaves
  # MAIN green makes `turn` report the trunk RED and stops the line. The limit is queue.sh's own, so
  # an ALERT never names a failure `turn` can no longer see.
  if [ -n "$head" ]; then
    if other=$(gh run list --branch main --limit 30 --json headSha,name,event,status,conclusion 2>/dev/null); then
      echo "$other" | HEAD_SHA="$head" python3 -c '
import json, os, sys
sha = os.environ["HEAD_SHA"]
try:
    for r in json.load(sys.stdin):
        if r["headSha"] == sha and r["event"] != "push" and r["status"] == "completed" and r["conclusion"] not in ("success", "skipped", "neutral"):
            print("ALERT main %s %s run failed: %s" % (sha[:8], r["event"], r["name"]))
except (ValueError, KeyError, TypeError) as e:
    print("ALERT could not parse the other runs on main (%s)" % type(e).__name__)'
    else echo "ALERT gh run list of main's other runs failed"; fi
  fi
  "$HERE/overlap.sh" 2>/dev/null | grep -E '^(SHARED|CONFLICT)'
  if done=$(python3 "$HERE/done_check.py" --no-fetch --self "$SELF" 2>&1); then
    printf '%s\n' "$done" | awk -F'\t' '$4=="ARCHIVABLE"{print "ARCHIVABLE " $1 " " $2}'
  else
    echo "ALERT done_check.py failed, so no session is offered for archive: $(printf '%s' "$done" | tail -1)"
  fi
}

while true; do
  cur=$(snapshot | sort -u)
  if [ ! -f "$STATE" ]; then
    printf '%s\n' "$cur" | sed 's/^/= /'
    printf '%s\n' "$cur" > "$STATE"
    [ "$MODE" = until-change ] && exit 0
  elif [ "$cur" != "$(cat "$STATE")" ]; then
    diff <(cat "$STATE") <(printf '%s\n' "$cur") | sed -n 's/^> /+ /p; s/^< /- /p'
    printf '%s\n' "$cur" > "$STATE"
    [ "$MODE" = until-change ] && exit 0
  fi
  sleep "$INTERVAL"
done
