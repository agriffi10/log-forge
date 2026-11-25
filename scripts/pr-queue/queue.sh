#!/bin/bash
# queue.sh — the PR queue: FIFO ordering over one repo shared by several agent sessions.
#
# One PR open on the remote at a time, taken in the order agents asked for it, with `main`
# settled and green before the next goes up. Every claim here is atomic (mkdir) or idempotent.
#
#   queue.sh ticket  SPEC-207   # get in line, when ready to push (idempotent — keeps your place)
#   queue.sh turn    SPEC-207   # exit 0 when it is your turn AND the remote is clear
#   queue.sh acquire SPEC-207   # take the lock; 0 on ACQUIRED, 1 on BUSY. Run it from your worktree
#   queue.sh release SPEC-207   # drop the lock and the ticket; always exit 0
#   queue.sh status             # who holds it, who is waiting, what is on the remote
#   queue.sh reap               # drop dead waiters and a dead holder (also runs automatically)
#   queue.sh main-state         # print and exit with main's state: 0 green · 1 red · 2 building
#                               #   · 3 could not read · 4 the nightly soak's verdict blocks
#   queue.sh soak-verdict       # the newest scheduled soak night: "0 none", or
#                               #   "<state> <run id>-<attempt> <time> [filed]"; exits 0, 3 or 4
#   queue.sh clearing-range A B # 0 when A..B changes only the soak's two clearing files
#
# THIS SCRIPT LIVES OUTSIDE THE REPO. Its own directory is the queue, so every session on the
# checkout sees one lock and one line. A copy inside a worktree would be invisible to peers, and
# a copy inside the repo would be a file that itself conflicts. `install.sh` puts it in place;
# `PROTOCOL.md` beside it is what the agents read.

set -uo pipefail

Q="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TICKETS="$Q/tickets"
LOCK="$Q/lock"
LOG="$Q/log"
STALE_TICKET=1800   # 30 min with no heartbeat — a waiter that stopped
STALE_LOCK=5400     # 90 min — a holder that stopped
HALF_BUILT=60       # grace for a ticket still being created

# --- configuration: single-value files beside this script, all written by install.sh ---------
REPO="${PR_QUEUE_REPO:-$(cat "$Q/repo" 2>/dev/null)}"
MAIN="$(cat "$Q/main-branch" 2>/dev/null)"; [ -n "$MAIN" ] || MAIN=main

die() { echo "queue.sh: $*" >&2; exit 2; }
[ -n "$REPO" ] || die "no checkout configured — write its path to $Q/repo (install.sh does this)"
git -C "$REPO" rev-parse --git-dir >/dev/null 2>&1 || die "$REPO is not a git checkout"

# `stat` differs between BSD and GNU, and handing GNU the BSD spelling does not fail quietly: it
# reads `-f` as --file-system and PRINTS A REPORT on stdout, which a `||` fallback then appends
# to. So probe the spelling once, and refuse any answer that is not a number.
if stat -c %Y "$Q" >/dev/null 2>&1; then STAT_MTIME=(stat -c %Y)   # GNU / busybox
else                                     STAT_MTIME=(stat -f %m)   # BSD / macOS
fi

now()   { date -u +%FT%TZ; }
mtime() { local m; m="$("${STAT_MTIME[@]}" "$1" 2>/dev/null)"
          case "$m" in ''|*[!0-9]*) echo 0 ;; *) echo "$m" ;; esac; }
age()   { echo $(( $(date +%s) - $(mtime "$1") )); }
note()  { printf '%s %s\n' "$(now)" "$*" >> "$LOG"; }

# A ticket mid-creation has no heartbeat file yet, and reaping on a missing file would delete a
# place someone is still taking. Fall back to the directory's own age, so a half-created ticket
# is young now and still reapable later if its session died between the mkdir and the touch.
ticket_age() { if [ -f "$1/alive" ]; then age "$1/alive"; else age "$1"; fi; }

# --- the two remote checks -------------------------------------------------------------------
# All three are overridable: drop an executable named `open-prs`, `all-prs` or `main-green` in the
# queue directory and it is used instead of the gh default below. That is the seam for a project
# whose CI is not GitHub Actions, or whose repo is not on GitHub at all. NOTE that `open-prs` must
# exclude drafts — an override written before drafts were exempted reinstates draft-blocking, and
# nothing here can detect that, because an override is opaque by design.
#
# open_prs   prints the open NON-DRAFT PRs (empty output = none). Non-zero exit means THE
#            CHECK ITSELF failed — an unreadable remote is never "the remote is clear", so
#            callers fail closed.
#
# A DRAFT DOES NOT BLOCK. The remote check stands in for "another agent is mid-turn", and a
# draft is the one open PR explicitly *not* ready to merge — it can sit for hours by design.
# Counting one starves every agent that obeys this queue while agents that never took a
# ticket push straight past it. Measured in s3-upload-portal, whose queue log records it: ticket 0017 took its place at 21:40:09Z and did not acquire until 00:21:55Z
# — two hours forty behind a draft titled "DO NOT MERGE YET", as the only waiter, with the
# lock free throughout. A non-draft PR still blocks, bot-authored ones included: those are
# intended to merge, so waiting for them is the point.
open_prs() {
  local out
  if [ -x "$Q/open-prs" ]; then
    out="$("$Q/open-prs")" || return 1
  else
    out="$( cd "$REPO" && gh pr list --state open --limit 20 \
              --json number,headRefName,title,isDraft \
              --jq '.[] | select(.isDraft | not) | "#\(.number)  \(.headRefName)  \(.title)"' )" || return 1
  fi
  [ -n "$out" ] && printf '%s\n' "$out"
  return 0   # empty output with a clean exit is the "no PRs open" answer
}

# all_prs    every open PR INCLUDING drafts, for `status` only — never for gating. Without
#            this a clear `turn` next to a visibly open draft reads as a bug in the queue.
all_prs() {
  local out
  if [ -x "$Q/all-prs" ]; then
    out="$("$Q/all-prs")" || return 1
  elif [ -x "$Q/open-prs" ]; then
    # A gh-less install that predates this function wrote only `open-prs`. Fall back to it
    # rather than reaching for a `gh` that is not there: drafts go unlisted in `status`, which
    # is a smaller loss than a permanently broken `status` and a stale-lock check that cannot
    # read the remote at all.
    out="$("$Q/open-prs")" || return 1
  else
    out="$( cd "$REPO" && gh pr list --state open --limit 20 \
              --json number,headRefName,title,isDraft \
              --jq '.[] | "#\(.number)  \(.headRefName)  \(.title)\(if .isDraft then "   DRAFT (ignored by the queue)" else "" end)"' )" || return 1
  fi
  [ -n "$out" ] && printf '%s\n' "$out"
  return 0
}

# main_green  0 green · 1 RED · 2 building or unsettled · 3 the check could not run · 4 the
#             nightly soak's newest scheduled night failed and is not filed (SPEC-099). Only 0
#             lets the queue move, so a check that could not run is never read as evidence that
#             it passed. The head's runs are read first; only when they are clear is the soak's
#             verdict read, and a verdict that cannot be read is 3.
main_green() {
  [ -x "$Q/main-green" ] && { "$Q/main-green"; return $?; }
  local hs
  head_green; hs=$?
  [ "$hs" -eq 0 ] || return "$hs"
  soak_read
  case "$SOAK_RC" in 0) return 0 ;; 4) return 4 ;; *) return 3 ;; esac
}

# head_green  main_green's reading of the runs on main's head, as it was before SPEC-099, with the
#             nightly soak's runs left out: a soak run on the head is a dispatch's or a night's, and
#             only soak_verdict reads those. A red dispatch is someone's experiment, not a red main.
head_green() {
  local sha runs rsha status conclusion wf any=0 building=0 red=0
  # ls-remote rather than a fetch: it reads the remote's head without touching local refs, which
  # several worktrees hitting FETCH_HEAD at once would race on.
  sha="$( cd "$REPO" && git ls-remote origin "refs/heads/$MAIN" 2>/dev/null | cut -f1 )" || return 3
  [ -n "$sha" ] || return 3
  runs="$( cd "$REPO" && gh run list --branch "$MAIN" --limit 30 \
             --json headSha,status,conclusion,workflowName \
             --jq '.[] | "\(.headSha) \(.status) \(.conclusion) \(.workflowName)"' )" || return 3
  while read -r rsha status conclusion wf; do
    [ "$rsha" = "$sha" ] || continue
    [ "$wf" = "$SOAK_WORKFLOW" ] && continue
    any=1
    [ "$status" = "completed" ] || { building=1; continue; }
    case "$conclusion" in success|skipped|neutral) ;; *) red=1 ;; esac
  done <<< "$runs"
  if [ "$any" -eq 0 ]; then
    # No run yet for main's head. Either CI has not started, or this repo has none — and those
    # need opposite answers, so ask which it is rather than guessing. One `ls` per extension:
    # `ls a b` exits non-zero when EITHER operand is missing, so a single call testing both
    # would answer "no workflows" for the repo that has only one of them.
    ls "$REPO"/.github/workflows/*.yml  >/dev/null 2>&1 && return 2
    ls "$REPO"/.github/workflows/*.yaml >/dev/null 2>&1 && return 2
    return 0
  fi
  [ "$red" -eq 1 ] && return 1
  [ "$building" -eq 1 ] && return 2
  return 0
}

# --- the nightly soak's verdict (SPEC-099) ----------------------------------------------------
# `.github/workflows/model-soak.yml` soaks the model test every night. A failed SCHEDULED night
# stops the queue as a durable fact until a pull request files that night; tests/
# test_model_soak_workflow.py holds the workflow's `name:` and file name to these two values.
SOAK_WORKFLOW="Model soak"
SOAK_FILE="model-soak.yml"
SOAK_LOOKBACK=30        # scheduled runs read back through nights with no verdict: a month
SOAK_QUIET_HOURS=48     # a verdict older than this is named in turn's READY answer
SOAK_REF="refs/pr-queue/$MAIN"
SOAK_LINE=""; SOAK_RC=""

# soak_read  reads soak_verdict once per queue.sh process into SOAK_LINE and SOAK_RC, so turn,
#            acquire, the note and status all decide on one reading of the night.
soak_read() {
  [ -n "$SOAK_RC" ] && return 0
  SOAK_LINE="$(soak_verdict)"; SOAK_RC=$?
}

# soak_verdict  0 nothing blocks · 4 the newest scheduled night with a verdict failed and its
#               failing attempt is not filed · 3 the verdict could not be read. Prints "0 none"
#               when no night has a verdict, "0 off" under a main-green override (a project whose
#               CI is not GitHub Actions has no nightly soak), "3 unreadable", and otherwise
#               "<state> <run id>-<attempt> <time>", with "filed" after it when that attempt is.
#
# Only SCHEDULED runs carry a verdict: a dispatch or a pull request's run is someone's experiment.
# The newest run with a verdict is read, on any sha, so a merge made while the night ran cannot
# step past it. `gh run list` reports only a run's latest attempt, so every attempt is read
# through the API: one that concluded failure or timed_out blocks, whatever a re-run did later,
# and a run is listed whatever its status, so a re-run in progress hides none of the attempts
# before it. A night whose every attempt has no verdict (in progress, cancelled, startup_failure,
# action_required, skipped, neutral, stale) is passed over for the one before it. The attempt a
# blocking night names is its last failing one.
soak_verdict() {
  [ -x "$Q/main-green" ] && { echo "0 off"; return 0; }
  local runs id attempts n a c t blocked bad when good goodwhen
  runs="$( cd "$REPO" && gh run list --workflow "$SOAK_FILE" --event schedule \
             --limit "$SOAK_LOOKBACK" --json databaseId,attempt \
             --jq '.[] | "\(.databaseId) \(.attempt)"' </dev/null )" || { echo "3 unreadable"; return 3; }
  while read -r id attempts; do
    [ -n "$id" ] || continue
    case "$id/$attempts" in *[!0-9/]*|/*|*/) echo "3 unreadable"; return 3 ;; esac
    blocked=0; bad=""; when=""; good=""; goodwhen=""
    n=1
    while [ "$n" -le "$attempts" ]; do
      a="$( cd "$REPO" && gh api "repos/{owner}/{repo}/actions/runs/$id/attempts/$n" \
              --jq '"\(.conclusion) \(.updated_at)"' </dev/null )" || { echo "3 unreadable"; return 3; }
      c="${a%% *}"; t="${a#* }"
      case "$c" in
        failure|timed_out) blocked=1; bad="$n"; when="$t" ;;
        success)           good="$n"; goodwhen="$t" ;;
      esac
      n=$((n + 1))
    done
    if [ "$blocked" -eq 1 ]; then
      filed "$id-$bad"
      case $? in
        0) echo "0 $id-$bad $when filed"; return 0 ;;
        1) echo "4 $id-$bad $when"; return 4 ;;
        *) echo "3 unreadable"; return 3 ;;
      esac
    fi
    [ -n "$good" ] && { echo "0 $id-$good $goodwhen"; return 0; }
  done <<< "$runs"
  echo "0 none"; return 0
}

# fetch_queue_ref  fetches the trunk into SOAK_REF, a ref no session works on. `--refmap=` is what
#             keeps it there: without it git also updates refs/remotes/origin/<trunk> from the
#             same fetch (measured, git 2.54.0), and moving origin/main under a session in the
#             middle of a rebase is a known hazard. --no-write-fetch-head, because several
#             worktrees hitting FETCH_HEAD at once would race. Every soak fetch is this one.
fetch_queue_ref() {
  git -C "$REPO" fetch -q --no-write-fetch-head --refmap= origin \
      "+refs/heads/$MAIN:$SOAK_REF" </dev/null 2>/dev/null
}

# filed <run id>-<attempt>  0 when tests/soak_known.py at SOAK_REF files that attempt in
#             FILED_NIGHTS, one entry a line as soak_known.py writes it; 1 when it does not; 2
#             when the trunk could not be read. The filing is the night's own key, so no clock is
#             compared, and a filing cannot be carried by the tree the night tested.
filed() {
  local text
  fetch_queue_ref || return 2
  text="$(git -C "$REPO" show "$SOAK_REF:tests/soak_known.py" 2>/dev/null)" || return 2
  printf '%s\n' "$text" | grep -qE "^    \"$1\": \".+\",\$" && return 0
  return 1
}

# clearing_range <from> <to>  0 when the commits from..to in $PWD change something, and only
#             tests/soak_known.py and docs/architecture.md, each left a regular file (100644).
#             Names are read NUL-delimited and compared whole; a rename reads as a deletion and an
#             addition; no diff.* or color.* setting can change what is read; an empty range, or
#             one whose changes revert each other, is not clearing.
clearing_range() {
  local meta path mode n=0
  while IFS= read -r -d '' meta && IFS= read -r -d '' path; do
    n=$((n + 1))
    read -r _ mode _ <<< "${meta#:}"
    [ "$mode" = 100644 ] || return 1
    [ "$path" = tests/soak_known.py ] || [ "$path" = docs/architecture.md ] || return 1
  done < <(git -C "$PWD" -c core.quotepath=false diff --raw -z --no-abbrev --no-renames \
             --no-ext-diff --no-relative --no-color --no-textconv "$1" "$2" -- 2>/dev/null)
  [ "$n" -gt 0 ]
}

# clearing_branch  0 when the branch in $PWD is a clearing range from its merge base with
#             SOAK_REF, fetched first. Never origin/main: a session's origin/main can be stale, and
#             a stale base counts a peer's merged commits as the branch's own.
clearing_branch() {
  local base
  fetch_queue_ref || return 1
  base="$(git -C "$PWD" merge-base HEAD "$SOAK_REF" 2>/dev/null)" || return 1
  clearing_range "$base" HEAD
}

# soak_note  turn's and status's line about a soak that is checking nothing: a verdict older than
#            SOAK_QUIET_HOURS, no verdict in the last SOAK_LOOKBACK nights, a verdict that could
#            not be read, or a main-green override under which it is not read. Silent otherwise.
soak_note() {
  local t cut
  soak_read
  case "$SOAK_LINE" in
    "0 off"*)  echo "  note: the nightly soak is not read: a main-green override is installed"; return 0 ;;
    "0 none"*) echo "  note: the nightly soak has no verdict in its last $SOAK_LOOKBACK scheduled runs"; return 0 ;;
    3*)        echo "  note: the nightly soak's verdict could not be read"; return 0 ;;
  esac
  t="$(printf '%s' "$SOAK_LINE" | awk '{print $3}')"
  cut="$(date -u -v-"${SOAK_QUIET_HOURS}"H +%Y-%m-%dT%H:%M:%S 2>/dev/null \
         || date -u -d "$SOAK_QUIET_HOURS hours ago" +%Y-%m-%dT%H:%M:%S 2>/dev/null)"
  if [ -n "$t" ] && [ -n "$cut" ] && [[ "${t:0:19}" < "$cut" ]]; then
    echo "  note: the nightly soak's newest verdict is from $t"
  fi
  return 0
}

# --- the holder file --------------------------------------------------------------------------
# `key value` lines, one fact per line, read by field name. The branch is compared as a STRING,
# never grepped: a substring match would let `spec-207/qu` push under `spec-207/queue`'s lock.
holder_field() { awk -v k="$1" '$1==k { $1=""; sub(/^ /,""); print; exit }' "$LOCK/holder" 2>/dev/null; }
my_branch()    { git -C "$PWD" branch --show-current 2>/dev/null; }
write_holder() { { printf 'spec %s\n'   "$1"
                   printf 'branch %s\n' "$2"
                   printf 'since %s\n'  "$3"; } > "$LOCK/holder"; }

my_ticket() {  # prints the ticket dir for a spec, empty if it has none
  local spec="$1" t
  for t in "$TICKETS"/*/; do
    [ -d "$t" ] || continue
    [ "$(cat "$t/spec" 2>/dev/null)" = "$spec" ] && { echo "${t%/}"; return; }
  done
}

reap() {
  local t spec held lock_age before after prs
  held="$(holder_field spec)"
  for t in "$TICKETS"/*/; do
    [ -d "$t" ] || continue
    t="${t%/}"
    spec="$(cat "$t/spec" 2>/dev/null)"
    if [ -z "$spec" ]; then
      # Creation died between the mkdir and the write. Nobody can claim this ticket and it sits
      # at the head of the line, so clear it — after a grace period, because a ticket being
      # taken at this instant looks exactly the same.
      [ "$(age "$t")" -gt "$HALF_BUILT" ] &&
        { note "cleared ticket $(basename "$t") — created but never named"; rm -rf "${t:?}"; }
      continue
    fi
    # The holder is not a waiter. Acquiring stops the polling, and the lock covers a PR lifecycle
    # far longer than the waiter timeout, so ageing out the holder's own ticket would be wrong.
    [ -n "$held" ] && [ "$spec" = "$held" ] && continue
    if [ "$(ticket_age "$t")" -gt "$STALE_TICKET" ]; then
      note "reaped stale ticket $(basename "$t") ($spec) — no heartbeat"
      rm -rf "${t:?}"
    fi
  done
  [ -d "$LOCK" ] || return 0
  # The LOCK DIRECTORY's age, not the holder file's. `acquire` does mkdir first and writes the
  # holder second, and a holder file that is not there yet reads as mtime 0 — which makes a lock
  # one second old look infinitely old, and evicts live holders under contention.
  lock_age=$(age "$LOCK")
  [ "$lock_age" -gt "$STALE_LOCK" ] || return 0
  # Age alone is not enough to break a lock: a long PR is not a dead one. Only a holder whose
  # work has demonstrably stopped may be broken, and a check that could not run proves nothing.
  #
  # ALL_PRS, NOT OPEN_PRS — the two questions are different and only one of them ignores
  # drafts. Gating a waiter asks "is someone mid-turn", and a draft is explicitly not that.
  # Breaking a lock asks "has this holder stopped", and an open draft is live evidence they
  # have not: a holder who opens one would otherwise lose the protection they had, get their
  # lock broken under them, and leave two PRs open at once — the invariant this queue exists
  # for. Nothing refreshes the lock's mtime after acquire, so this check is the only thing
  # standing between a slow holder and a broken lock.
  before=$(mtime "$LOCK")
  prs="$(all_prs)" || return 0
  [ -z "$prs" ] || return 0
  main_green || return 0
  # Those checks take seconds, and the lock can turn over inside them. Re-read before deleting:
  # a changed mtime means someone has acquired since, and this is no longer a dead lock.
  after=$(mtime "$LOCK")
  [ "$before" = "$after" ] || return 0
  [ "$(age "$LOCK")" -gt "$STALE_LOCK" ] || return 0
  note "BROKE STALE LOCK held by [$(holder_field spec)] — ${lock_age}s old, no open PR of any kind, main green"
  rm -f "$LOCK/holder" && rmdir "$LOCK"
}

cmd_ticket() {
  local spec="$1" t n d b
  mkdir -p "$TICKETS"; reap
  t="$(my_ticket "$spec")"
  if [ -z "$t" ]; then
    # Allocate from the high-water mark, never from zero. Reaping frees a low number, and a new
    # arrival taking it would land ahead of someone who has been waiting longer — which is the
    # starvation the tickets exist to prevent. An unreadable or corrupt mark restarts at zero
    # rather than killing the script mid-ticket, which would leave an unclaimable one behind.
    n=$(cat "$Q/seq" 2>/dev/null)
    case "$n" in ''|*[!0-9]*) n=0 ;; esac
    # The mark is the primary source, but it is one file and files are lost. Floor the number at
    # one past the highest ticket in the line as well, so a lost mark cannot restart at zero and
    # put this arrival ahead of everyone already waiting.
    for d in "$TICKETS"/*/; do
      [ -d "$d" ] || continue
      b="$(basename "$d")"
      case "$b" in ''|*[!0-9]*) continue ;; esac
      [ "$((10#$b + 1))" -gt "$n" ] && n=$((10#$b + 1))
    done
    while ! mkdir "$TICKETS/$(printf %04d "$n")" 2>/dev/null; do n=$((n+1)); done
    t="$TICKETS/$(printf %04d "$n")"
    touch "$t/alive"          # heartbeat first: an unheartbeaten ticket is a reap candidate
    echo "$spec" > "$t/spec"
    my_branch > "$t/branch"
    # Written through a temp file: `> "$Q/seq"` truncates before it writes, and a peer reading
    # in that window gets an empty mark and starts again from zero.
    printf '%s\n' "$((n+1))" > "$Q/seq.$$" && mv -f "$Q/seq.$$" "$Q/seq"
    note "$spec took ticket $(basename "$t")"
  fi
  touch "$t/alive"
  echo "ticket $(basename "$t") for $spec"
}

cmd_turn() {
  local spec="$1" t lowest rc pr
  mkdir -p "$TICKETS"; reap
  t="$(my_ticket "$spec")"
  [ -n "$t" ] || { echo "NO_TICKET — run: $0 ticket $spec"; return 3; }
  touch "$t/alive"   # the heartbeat: a live waiter keeps its place, a dead one loses it
  if [ -d "$LOCK" ]; then
    [ "$(holder_field spec)" = "$spec" ] && { echo "READY (you already hold the lock)"; return 0; }
    echo "WAIT — lock held by [$(holder_field spec) on $(holder_field branch), since $(holder_field since)]"
    return 1
  fi
  lowest="$(ls "$TICKETS" 2>/dev/null | sort | head -1)"
  [ "$lowest" = "$(basename "$t")" ] || {
    echo "WAIT — ahead of you: $(cat "$TICKETS/$lowest/spec" 2>/dev/null) (ticket $lowest)"; return 1; }
  # The lock only orders the agents that take it. Sessions outside the set push without one, so
  # ask the remote itself as well.
  pr="$(open_prs)" || { echo "WAIT — could not read the remote (the check failed). Not assuming it is clear."; return 1; }
  [ -z "$pr" ] || { echo "WAIT — a PR is open on the remote:"; echo "$pr" | head -3; return 1; }
  main_green; rc=$?
  case $rc in
    0) echo "READY — your turn, no PR open, $MAIN green"; soak_note; return 0 ;;
    1) echo "WAIT — $MAIN IS RED. Stop and escalate; a red $MAIN is fixed before anything merges."; return 1 ;;
    4) if clearing_branch; then
         echo "READY — the nightly soak's verdict blocks ($(printf '%s' "$SOAK_LINE" | cut -d' ' -f2,3)), and this branch only files a night"
         return 0
       fi
       echo "WAIT — the nightly soak's verdict blocks ($(printf '%s' "$SOAK_LINE" | cut -d' ' -f2,3)). Escalate the night to Andrew, naming it; once he has answered, only a branch that files it may land ($Q/PROTOCOL.md)."
       return 1 ;;
    3) echo "WAIT — could not read $MAIN's status (the check failed). Not assuming it is green."; return 1 ;;
    *) echo "WAIT — $MAIN is building or unsettled"; return 1 ;;
  esac
}

cmd_acquire() {
  local spec="$1" out branch pattern
  # The lock records the branch it covers and pre-push compares against it, so a lock taken from
  # outside a worktree would name no branch and would refuse its own holder's push.
  branch="$(my_branch)"
  [ -n "$branch" ] || {
    echo "REFUSED — run this from your worktree, on the branch you are about to push."; return 1; }
  # The trunk is the one branch pre-push never enforces, so a lock recorded on it can authorise no
  # push at all — it would hand out a lock that silently does nothing. Being on it here almost
  # always means the shared checkout rather than a build worktree.
  [ "$branch" = "$MAIN" ] && {
    cat <<MSG
REFUSED — you are on $MAIN, which is the shared checkout rather than a build worktree.

  cwd: $PWD

pre-push never enforces $MAIN, so a lock stamped with it covers nothing. cd into the worktree you
built in and acquire from there, in the SAME command — a tool that resets cwd between calls will
otherwise put you back here.
MSG
    return 1; }
  # The lock and the hook only line up while `enforce-branches` covers the branch being locked.
  # At the installed default it covers every branch and this never fires; narrow the pattern and
  # a lock can once again be held on a branch nothing enforces, which is the original defect
  # wearing a different hat. A WARNING rather than a refusal: a project may narrow it deliberately,
  # and a queue that is advisory on purpose is not a queue that is broken.
  pattern="$(cat "$Q/enforce-branches" 2>/dev/null)"
  if [ -z "$pattern" ]; then
    # The TOTAL form of the same problem, and the one that used to say nothing at all: with no
    # pattern the hook enforces no branch whatsoever, so every lock this queue hands out is
    # advisory. That deserves a louder note than a merely narrow pattern, not a quieter one.
    echo "  WARNING: $Q/enforce-branches is empty or missing, so pre-push enforces NOTHING." >&2
    echo "           This lock orders the agents that take one and stops nobody at all." >&2
  elif ! echo "$branch" | grep -qE "$pattern"; then
    echo "  note: pre-push does not enforce '$branch' — it is outside enforce-branches, so this" >&2
    echo "        lock orders the agents that take one and stops nobody else pushing it." >&2
  fi
  # turn runs in this shell, not a $( ) subshell, so the soak's one reading (soak_read) is shared
  # with the check below rather than read twice and possibly disagreeing.
  local tmp rc
  tmp="$(mktemp "${TMPDIR:-/tmp}/queue-turn.XXXXXX")" || { echo "BUSY — could not make a temp file"; return 1; }
  cmd_turn "$spec" > "$tmp"; rc=$?
  out="$(cat "$tmp")"; rm -f "$tmp"
  [ "$rc" -eq 0 ] || { echo "BUSY — $out"; return 1; }
  # A holder's turn answers READY before it reads the trunk, so the soak's verdict is read here
  # too: while it blocks, the lock is never stamped onto a branch that does more than file a night.
  soak_read   # the reading turn made, when it made one
  if [ "$SOAK_RC" = 4 ] && ! clearing_branch; then
    echo "REFUSED — the nightly soak's verdict blocks ($(printf '%s' "$SOAK_LINE" | cut -d' ' -f2,3)), and '$branch' changes more than tests/soak_known.py and docs/architecture.md."
    return 1
  fi
  # mkdir IS the atomicity: it either creates or fails, with no window between the two. A
  # test-then-create has a gap, and agents polling on similar cadences land in it.
  if ! mkdir "$LOCK" 2>/dev/null; then
    # Already ours means this is a retry, not a race — `turn` reports READY to the holder, so a
    # turn-then-acquire loop would otherwise spin forever against its own lock.
    #
    # It also RE-STAMPS the branch, keeping the original `since` so the lock does not get younger
    # by being re-acquired. That is the recovery for a lock taken from the wrong directory: it
    # names a branch pre-push will never match, so the holder is refused its own push, and
    # re-acquiring from the right worktree is the way back that does not cost a place in the line.
    if [ "$(holder_field spec)" = "$spec" ]; then
      write_holder "$spec" "$branch" "$(holder_field since)"
      note "$spec re-stamped its lock onto $branch"
      echo "ACQUIRED (already yours, on $branch)"; return 0
    fi
    echo "BUSY — lost the race to [$(holder_field spec)]"; return 1
  fi
  write_holder "$spec" "$branch" "$(now)"
  note "$spec ACQUIRED the lock on $branch"
  echo "ACQUIRED — release it when the PR is merged and $MAIN is green, on every exit path"
}

cmd_release() {
  local spec="$1" t
  if [ -d "$LOCK" ] && [ "$(holder_field spec)" = "$spec" ]; then
    rm -f "$LOCK/holder" && rmdir "$LOCK" && note "$spec released the lock" && echo "RELEASED"
  else
    echo "NOT_HELD by $spec — lock left alone"
  fi
  t="$(my_ticket "$spec")"; [ -n "$t" ] && rm -rf "${t:?}" && note "$spec dropped its ticket"
  return 0
}

cmd_status() {
  local n prs
  mkdir -p "$TICKETS"
  if [ -d "$LOCK" ]; then
    echo "LOCK: [$(holder_field spec) on $(holder_field branch)] — held $(age "$LOCK")s"
  else
    echo "LOCK: free"
  fi
  echo "WAITING:"
  ls "$TICKETS" 2>/dev/null | sort | while read -r n; do
    printf '  %s  %s  (%ss since heartbeat)\n' \
      "$n" "$(cat "$TICKETS/$n/spec" 2>/dev/null)" "$(ticket_age "$TICKETS/$n")"
  done
  soak_read
  echo "SOAK: $SOAK_LINE"
  soak_note
  echo "OPEN PRs:"
  # Assigned first, then printed: in a pipeline `||` would test sed's exit status, not the
  # check's, and the "could not read" branch would never fire.
  if prs="$(all_prs)"; then
    if [ -n "$prs" ]; then printf '%s\n' "$prs" | sed 's/^/  /'; else echo "  (none)"; fi
  else
    echo "  (COULD NOT READ THE REMOTE — this is not evidence that it is clear)"
  fi
  return 0
}

case "${1:-}" in
  ticket)  cmd_ticket  "${2:?spec id required, e.g. SPEC-207}" ;;
  turn)    cmd_turn    "${2:?spec id required}" ;;
  acquire) cmd_acquire "${2:?spec id required}" ;;
  release) cmd_release "${2:?spec id required}" ;;
  status)  cmd_status ;;
  main-state)     main_green; rc=$?; echo "$rc"; exit "$rc" ;;
  soak-verdict)   soak_read; printf '%s\n' "$SOAK_LINE"; exit "$SOAK_RC" ;;
  clearing-range) clearing_range "${2:?from}" "${3:?to}"; exit $? ;;
  reap)    mkdir -p "$TICKETS"; reap; echo "reaped" ;;
  *) sed -n '2,20p' "${BASH_SOURCE[0]}"; exit 64 ;;
esac
