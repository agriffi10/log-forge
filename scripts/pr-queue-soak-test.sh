#!/bin/sh
# pr-queue-soak-test.sh — the fixture corpus for the PR queue's reading of the nightly model soak
# (SPEC-099 FR-006): the night's verdict, its filing, the clearing branch, turn, acquire, the note,
# the hook and the reaping, followed by a mutation sweep of every check.
#
# A STUB `gh` comes first on PATH. It holds canned JSON in the shape GitHub returns — run lists in
# GH_RUNS_JSON (the head's runs) and GH_SCHED_JSON (the soak's runs, filtered by the caller's
# --event, --status and --limit), attempts in GH_ATT_JSON keyed "<run id>/<attempt>" — refuses a
# --json field the canned objects lack, and applies the caller's own --jq to the JSON with `jq`, so
# a broken filter fails a case rather than reading a canned answer. It also renders a missing
# conclusion exactly as the real client does, and refuses canned data in the other shape:
# `gh run list --json` gives "" (measured, gh 2.93.0) and the API gives null, and a stub that canned
# null for a run list once hid a read that an empty field broke. "HEAD" in a canned headSha is
# the trunk's head. GH_FAIL fails every call, GH_API_FAIL only `gh api`. Every call is logged.
#
# Every case asserts where it lands AND why: the state or exit, and a line of what the queue said,
# including the cases that must stay silent. Sections build their own fixture, so the sweep runs
# only the sections a plant names. Needs git, bash and jq; no network and no GitHub.
#
# Usage: sh scripts/pr-queue-soak-test.sh
#   PR_QUEUE_SOAK_TEST_SRC=<dir>      take queue.sh and pre-push from <dir> (the sweep uses this)
#   PR_QUEUE_SOAK_TEST_SECTIONS="a b" run only those sections
#   PR_QUEUE_SOAK_TEST_NO_SWEEP=1     run the cases only

set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="${PR_QUEUE_SOAK_TEST_SRC:-$ROOT/scripts/pr-queue}"
SECTIONS="${PR_QUEUE_SOAK_TEST_SECTIONS:-}"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/pr-queue-soak-test.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT
trap 'exit 1' HUP INT PIPE TERM

# HERMETIC: nothing inherited points gh, the queue or git anywhere, and no user git config is read.
unset GH_REPO GH_TOKEN GH_HOST GH_ENTERPRISE_TOKEN GITHUB_TOKEN PR_QUEUE_REPO PR_QUEUE_BYPASS PR_QUEUE_DIR
GIT_CONFIG_GLOBAL=/dev/null; GIT_CONFIG_NOSYSTEM=1; export GIT_CONFIG_GLOBAL GIT_CONFIG_NOSYSTEM
JQ="$(command -v jq || echo /usr/bin/jq)"
[ -x "$JQ" ] || { echo "FAIL  jq is needed: the stub applies the caller's --jq with it"; exit 1; }
for f in queue.sh pre-push; do
  bash -n "$SRC/$f" || { echo "FAIL  $SRC/$f does not parse"; exit 1; }
done

fails=0; oks=0
check() { if [ "$2" = "$3" ]; then oks=$((oks+1)); echo "ok    $1"; else fails=$((fails+1)); echo "FAIL  $1: got [$2] want [$3]"; fi; }
has()   { case "$2" in *"$3"*) oks=$((oks+1)); echo "ok    $1";; *) fails=$((fails+1)); echo "FAIL  $1: [$3] not in [$(printf '%s' "$2" | tr '\n' '|' | cut -c1-240)]";; esac; }
hasnt() { case "$2" in *"$3"*) fails=$((fails+1)); echo "FAIL  $1: [$3] in [$(printf '%s' "$2" | tr '\n' '|' | cut -c1-240)]";; *) oks=$((oks+1)); echo "ok    $1";; esac; }
want()  { [ -z "$SECTIONS" ] && return 0; case " $SECTIONS " in *" $1 "*) return 0;; esac; return 1; }

STUB="$WORK/bin"; mkdir -p "$STUB"
cat > "$STUB/gh" <<'GH'
#!/bin/sh
printf '%s\n' "$*" >> "$GH_LOG"
[ -n "${GH_FAIL:-}" ] && { echo "gh: stub failure" >&2; exit 1; }
cmd="$1"; shift
sub=""; [ "$cmd" = run ] || [ "$cmd" = pr ] && { sub="$1"; shift; }
json=""; jqx="."; event=""; status=""; limit=20; workflow=""; path=""
while [ $# -gt 0 ]; do
  case "$1" in
    --json) json="$2"; shift 2 ;;
    --jq) jqx="$2"; shift 2 ;;
    --event) event="$2"; shift 2 ;;
    --status) status="$2"; shift 2 ;;
    --limit) limit="$2"; shift 2 ;;
    --workflow) workflow="$2"; shift 2 ;;
    --branch|--state) shift 2 ;;
    *) path="$1"; shift ;;
  esac
done
head="$(git -C "$STUB_REPO" ls-remote origin refs/heads/main 2>/dev/null | cut -f1)"
fields() {  # every --json field must be in every canned object
  for f in $(printf '%s' "$json" | tr ',' ' '); do
    printf '%s' "$1" | "$JQ" -e --arg f "$f" 'all(.[]; has($f))' >/dev/null \
      || { echo "stub: field $f not canned" >&2; exit 2; }
  done
}
case "$cmd/$sub" in
  run/list)
    if [ -n "$workflow" ]; then data="${GH_SCHED_JSON:-[]}"; else data="${GH_RUNS_JSON:-[]}"; fi
    data="$(printf '%s' "$data" | sed "s/\"HEAD\"/\"$head\"/g")"
    data="$(printf '%s' "$data" | "$JQ" -c --arg ev "$event" --arg st "$status" --argjson lim "$limit" \
      '[.[] | select(($ev == "") or (.event == $ev)) | select(($st == "") or (.status == $st))] | .[:$lim]')"
    fields "$data"
    printf '%s' "$data" | "$JQ" -e 'all(.[]; .conclusion != null)' >/dev/null \
      || { echo "stub: gh run list renders a missing conclusion as \"\", never null" >&2; exit 2; }
    printf '%s' "$data" | "$JQ" -r "$jqx" ;;
  api/)
    [ -n "${GH_API_FAIL:-}" ] && { echo "gh: api failure" >&2; exit 1; }
    key="$(printf '%s' "$path" | sed -n 's#^repos/{owner}/{repo}/actions/runs/\([0-9]*\)/attempts/\([0-9]*\)$#\1/\2#p')"
    obj="$(printf '%s' "${GH_ATT_JSON:-"{}"}" | "$JQ" -c --arg k "$key" '.[$k] // empty')"
    [ -n "$key" ] && [ -n "$obj" ] || { echo "HTTP 404: Not Found" >&2; exit 1; }
    printf '%s' "$obj" | "$JQ" -e '.conclusion != ""' >/dev/null \
      || { echo "stub: the API renders a missing conclusion as null, never \"\"" >&2; exit 2; }
    printf '%s' "$obj" | "$JQ" -r "$jqx" ;;
  pr/list) printf '[]' | "$JQ" -r "$jqx" ;;
  *) echo "stub: unknown call $cmd $sub" >&2; exit 1 ;;
esac
GH
chmod +x "$STUB/gh"
PATH="$STUB:$PATH"; export PATH JQ
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
OLD="2020-01-01T00:00:00Z"

# fixture <name>: a bare origin whose main holds the clearing files and a src file, the session's
# clone (also the queue's checkout), a peer's clone, and the queue at $Q enforcing every branch.
fixture() {
  F="$WORK/$1"; mkdir -p "$F"
  git init -q --bare "$F/origin.git"; git -C "$F/origin.git" symbolic-ref HEAD refs/heads/main
  git init -q "$F/repo"; R="$F/repo"
  git -C "$R" symbolic-ref HEAD refs/heads/main
  git -C "$R" config user.email t@example.com; git -C "$R" config user.name Tester
  git -C "$R" remote add origin "$F/origin.git"
  mkdir -p "$R/tests" "$R/docs" "$R/src"
  printf 'KNOWN = ()\nFILED_NIGHTS: dict[str, str] = {\n}\n' > "$R/tests/soak_known.py"
  echo a > "$R/docs/architecture.md"; echo a > "$R/src/x.py"
  git -C "$R" add -A; git -C "$R" commit -qm base; git -C "$R" push -q origin main
  git -C "$R" fetch -q origin
  git clone -q "$F/origin.git" "$F/peer" 2>/dev/null
  git -C "$F/peer" config user.email p@example.com; git -C "$F/peer" config user.name Peer
  Q="$F/q"; mkdir -p "$Q/tickets"
  cp "$SRC/queue.sh" "$SRC/pre-push" "$Q/"; chmod +x "$Q/queue.sh" "$Q/pre-push"
  echo "$R" > "$Q/repo"; echo main > "$Q/main-branch"; echo '.' > "$Q/enforce-branches"
  git -C "$R" config core.hooksPath "$Q"
  GH_LOG="$F/gh.log"; : > "$GH_LOG"; STUB_REPO="$R"
  GH_RUNS_JSON='[{"headSha":"HEAD","status":"completed","conclusion":"success","workflowName":"CI"}]'
  GH_SCHED_JSON='[]'; GH_ATT_JSON='{}'
  export GH_LOG STUB_REPO GH_RUNS_JSON GH_SCHED_JSON GH_ATT_JSON
  unset GH_FAIL GH_API_FAIL
  cd "$R" || exit 1
}
Qs() { bash "$Q/queue.sh" "$@"; }
vstate() { Qs soak-verdict >/dev/null 2>&1; echo $?; }
verdict() { Qs soak-verdict 2>/dev/null; }
mstate() { Qs main-state >/dev/null 2>&1; echo $?; }

# run_json <id> <sha> <status> <event> — one canned soak run.
run_json() { printf '{"databaseId":%s,"attempt":%s,"headSha":"%s","status":"%s","conclusion":"","workflowName":"Model soak","event":"%s"}' "$1" "$2" "$3" "$4" "$5"; }
att_json() { printf '"%s/%s":{"conclusion":%s,"updated_at":"%s"}' "$1" "$2" "$3" "$4"; }
q() { case "$1" in null) printf 'null' ;; *) printf '"%s"' "$1" ;; esac; }
# night C...: scheduled run 101 on HEAD whose attempts concluded C (null for one still running).
night() {
  n=0; atts=""; last=""
  for c in "$@"; do n=$((n+1)); atts="$atts${atts:+,}$(att_json 101 $n "$(q "$c")" "$NOW")"; last="$c"; done
  st=completed; [ "$last" = null ] && st=in_progress
  GH_SCHED_JSON="[$(run_json 101 $n HEAD $st schedule)]"; GH_ATT_JSON="{$atts}"
  export GH_SCHED_JSON GH_ATT_JSON
}
# nights C1 C2: run 102 (one attempt, C1) newest, then run 101 (one attempt, C2).
nights() {
  s1=completed; [ "$1" = null ] && s1=in_progress
  GH_SCHED_JSON="[$(run_json 102 1 HEAD $s1 schedule),$(run_json 101 1 HEAD completed schedule)]"
  GH_ATT_JSON="{$(att_json 102 1 "$(q "$1")" "$NOW"),$(att_json 101 1 "$(q "$2")" "$NOW")}"
  export GH_SCHED_JSON GH_ATT_JSON
}
# file <id-n> [indent] [prefix]: a peer merges a line into main's tests/soak_known.py.
file() {
  ( cd "$F/peer" && git pull -q origin main 2>/dev/null \
    && printf '%s%s"%s": "a lead",\n' "${3:-}" "${2:-    }" "$1" >> tests/soak_known.py \
    && git commit -qam "file $1" && git push -q origin main )
}
peer_src() { ( cd "$F/peer" && git pull -q origin main 2>/dev/null && echo "$1" >> src/x.py && git commit -qam "src $1" && git push -q origin main ); }
newbranch() { git -C "$R" fetch -q origin; git -C "$R" checkout -q -B "$1" origin/main; }
turnout() { Qs ticket "$1" >/dev/null; o="$(Qs turn "$1")"; r=$?; Qs release "$1" >/dev/null; echo "$r $o"; }

if want verdict; then
  echo "== verdict: which night is read, and what it says"
  fixture verdict
  night failure;           check "a failed night blocks" "$(vstate)" 4
                           has   "  ...naming its run, attempt and time" "$(verdict)" "4 101-1 $NOW"
  night success;           check "a passed night does not block" "$(vstate)" 0
                           check "  ...and prints <state> <run id>-<attempt> <time>" "$(verdict)" "0 101-1 $NOW"
  night timed_out;         check "a timed-out night blocks" "$(vstate)" 4
  night failure success;   check "a green re-run does not clear a red attempt" "$(vstate)" 4
                           has   "  ...naming the failing attempt" "$(verdict)" "4 101-1 "
  night success failure;   check "a red re-run of a green night blocks" "$(vstate)" 4
                           has   "  ...naming attempt 2" "$(verdict)" "4 101-2 "
  night failure failure;   has   "two red attempts name the last" "$(verdict)" "4 101-2 "
  for c in cancelled startup_failure action_required skipped neutral stale null; do
    nights "$c" failure;   check "a $c night falls back to a failed one" "$(vstate)" 4
    nights "$c" success;   check "a $c night falls back to a passed one" "$(vstate)" 0
  done
  night failure null;      check "a red night being re-run still blocks" "$(vstate)" 4
  GH_SCHED_JSON="[$(run_json 101 1 0123456789012345678901234567890123456789 completed schedule)]"
  GH_ATT_JSON="{$(att_json 101 1 '"failure"' "$NOW")}"; export GH_SCHED_JSON GH_ATT_JSON
                           check "a red night on an older sha still blocks" "$(vstate)" 4
  GH_SCHED_JSON='[]'; export GH_SCHED_JSON
                           check "no scheduled night does not block" "$(vstate)" 0
                           check "  ...and prints 0 none" "$(verdict)" "0 none"
  nights cancelled cancelled; check "only verdict-less nights do not block" "$(vstate)" 0
                           check "  ...and print 0 none" "$(verdict)" "0 none"
  GH_SCHED_JSON="[$(run_json 103 1 HEAD completed workflow_dispatch),$(run_json 101 1 HEAD completed schedule)]"
  GH_ATT_JSON="{$(att_json 103 1 '"failure"' "$NOW"),$(att_json 101 1 '"success"' "$NOW")}"; export GH_SCHED_JSON GH_ATT_JSON
                           check "a red dispatch carries no verdict" "$(vstate)" 0
fi

if want head; then
  echo "== head: main-state reads the head without the soak, then the night"
  fixture head
  night success
  GH_RUNS_JSON='[{"headSha":"HEAD","status":"completed","conclusion":"success","workflowName":"CI"},{"headSha":"HEAD","status":"completed","conclusion":"failure","workflowName":"Model soak"}]'; export GH_RUNS_JSON
                           check "a red dispatch on the head does not block" "$(mstate)" 0
  GH_RUNS_JSON='[{"headSha":"HEAD","status":"completed","conclusion":"success","workflowName":"CI"},{"headSha":"HEAD","status":"in_progress","conclusion":"","workflowName":"Model soak"}]'; export GH_RUNS_JSON
                           check "a soak running on the head is not unsettled" "$(mstate)" 0
  GH_RUNS_JSON='[{"headSha":"HEAD","status":"completed","conclusion":"success","workflowName":"CI"},{"headSha":"HEAD","status":"queued","conclusion":"","workflowName":"Model soak"}]'; export GH_RUNS_JSON
                           check "a soak queued on the head, its conclusion empty, is not unsettled" "$(mstate)" 0
  GH_RUNS_JSON='[{"headSha":"HEAD","status":"completed","conclusion":"success","workflowName":"CI"},{"headSha":"HEAD","status":"in_progress","conclusion":null,"workflowName":"Model soak"}]'; export GH_RUNS_JSON
                           check "the stub refuses a run list's null conclusion, which gh never gives" "$(mstate)" 3
  night failure
  GH_RUNS_JSON='[{"headSha":"HEAD","status":"completed","conclusion":"failure","workflowName":"CI"}]'; export GH_RUNS_JSON
                           check "another red on the head answers 1" "$(mstate)" 1
                           check "  ...while the verdict still blocks" "$(vstate)" 4
  GH_RUNS_JSON='[{"headSha":"HEAD","status":"in_progress","conclusion":"","workflowName":"Release"}]'; export GH_RUNS_JSON
                           check "another run in progress answers 2" "$(mstate)" 2
  GH_RUNS_JSON='[{"headSha":"HEAD","status":"completed","conclusion":"success","workflowName":"CI"}]'; export GH_RUNS_JSON
                           check "the verdict alone answers 4" "$(mstate)" 4
                           has   "  ...and main-state prints it" "$(Qs main-state 2>/dev/null)" "4"
fi

if want unreadable; then
  echo "== unreadable: a verdict that cannot be read is 3, never clear"
  fixture unreadable
  night failure
  check "gh failing: main-state 3" "$(GH_FAIL=1 Qs main-state >/dev/null 2>&1; echo $?)" 3
  check "gh's API failing: soak-verdict 3" "$(GH_API_FAIL=1 Qs soak-verdict >/dev/null 2>&1; echo $?)" 3
  check "  ...and main-state reads it as 3, not clear" "$(GH_API_FAIL=1 Qs main-state >/dev/null 2>&1; echo $?)" 3
  check "  ...printing 3 unreadable" "$(GH_API_FAIL=1 Qs soak-verdict 2>/dev/null)" "3 unreadable"
  GH_SCHED_JSON="[$(run_json 101 2 HEAD completed schedule)]"; GH_ATT_JSON="{$(att_json 101 1 '"failure"' "$NOW")}"; export GH_SCHED_JSON GH_ATT_JSON
  check "an attempt the API cannot show: 3" "$(vstate)" 3
  check "gh absent: 3" "$(PATH=/usr/bin:/bin bash "$Q/queue.sh" main-state >/dev/null 2>&1; echo $?)" 3
fi

if want calls; then
  echo "== calls: the queue asks gh for exactly the fields and filters it reads"
  fixture calls
  night failure success; Qs main-state >/dev/null 2>&1
  calls="$(cat "$GH_LOG")"
  has "the head's runs: headSha,status,conclusion,workflowName" "$calls" \
    'run list --branch main --limit 30 --json headSha,status,conclusion,workflowName --jq .[] | [.headSha, .status, .conclusion, .workflowName] | map(if . == null or . == "" then "-" else tostring end) | join(" ")'
  has "the soak's scheduled runs, any status" "$calls" \
    'run list --workflow model-soak.yml --event schedule --limit 30 --json databaseId,attempt --jq .[] | [.databaseId, .attempt] | map(if . == null or . == "" then "-" else tostring end) | join(" ")'
  has "each attempt through the API" "$calls" \
    'api repos/{owner}/{repo}/actions/runs/101/attempts/2 --jq [.conclusion, .updated_at] | map(if . == null or . == "" then "-" else tostring end) | join(" ")'
  hasnt "no --status filter on the soak's runs" "$calls" "--status"
fi

if want filing; then
  echo "== filing: only that night's own filing on main clears it"
  fixture filing
  night failure failure
  file "101-1";            check "filing an earlier attempt does not clear" "$(vstate)" 4
  file "101-2";            check "filing the blocking attempt clears" "$(vstate)" 0
                           check "  ...and says so" "$(verdict)" "0 101-2 $NOW filed"
  GH_SCHED_JSON="[$(run_json 103 1 HEAD completed schedule)]"; GH_ATT_JSON="{$(att_json 103 1 '"failure"' "$NOW")}"; export GH_SCHED_JSON GH_ATT_JSON
                           check "another run's filing does not clear" "$(vstate)" 4
  file "103-1" "    " "# "; check "a filing in a comment does not clear" "$(vstate)" 4
  file "103-1" "  ";       check "a filing at the wrong indent does not clear" "$(vstate)" 4
  newbranch localonly; printf '    "103-1": "a lead",\n' >> tests/soak_known.py; git commit -qam local
                           check "a filing on a local branch only does not clear" "$(vstate)" 4
  git checkout -q main
  mv "$F/origin.git" "$F/origin.off"
                           check "an unreadable origin: 3" "$(vstate)" 3
  mv "$F/origin.off" "$F/origin.git"
  file "103-1";            check "a peer's filing not yet fetched here clears" "$(vstate)" 0
fi

if want refs; then
  echo "== refs: the verdict and a clearing branch are read without moving origin/main"
  fixture refs
  git -C "$R" fetch -q origin
  peer_src p1
  stale="$(git -C "$R" rev-parse refs/remotes/origin/main)"
  head="$(git -C "$R" ls-remote origin refs/heads/main | cut -f1)"
  before="$(git -C "$R" for-each-ref refs/remotes)"
  check "the premise: the local origin/main is stale" "$([ "$stale" != "$head" ] && echo stale)" stale
  night failure
  check "a failed night read, its filing looked up" "$(vstate)" 4
  check "  ...origin/main unchanged after the read" "$(git -C "$R" rev-parse refs/remotes/origin/main)" "$stale"
  check "  ...and every other refs/remotes ref" "$(git -C "$R" for-each-ref refs/remotes)" "$before"
  check "  ...the queue's own ref is the remote's head" "$(git -C "$R" rev-parse refs/pr-queue/main)" "$head"
  git -C "$R" fetch -q --refmap= origin main:refs/heads/fresh-main
  git -C "$R" checkout -q -B clean fresh-main; echo b >> tests/soak_known.py; git commit -qam clearing
  git -C "$R" update-ref refs/remotes/origin/main "$stale"
  o="$(turnout SPEC-REFS)"
  check "a valid clearing branch reads READY while origin/main is stale" "${o%% *}" 0
  has   "  ...as a branch that only files a night" "$o" "this branch only files a night"
  check "  ...origin/main still unchanged" "$(git -C "$R" rev-parse refs/remotes/origin/main)" "$stale"
fi

if want turn; then
  echo "== turn: while the verdict blocks, only a clearing branch is READY"
  fixture turn
  night failure
  W="the nightly soak's verdict blocks (101-1"; OK="this branch only files a night"
  rt() { o="$(turnout "SPEC-$1")"; check "$2" "${o%% *}" "$3"; has "  ...$2" "$o" "$4"; }
  newbranch both;    echo b >> tests/soak_known.py; echo b >> docs/architecture.md; git commit -qam both
  rt 1 "both clearing files" 0 "$OK"
  newbranch one;     echo b >> tests/soak_known.py; git commit -qam one
  rt 2 "tests/soak_known.py alone" 0 "$OK"
  newbranch src;     echo b >> src/x.py; echo b >> tests/soak_known.py; git commit -qam src
  rt 3 "src beside a clearing file" 1 "$W"
  newbranch empty
  rt 4 "no commits" 1 "$W"
  newbranch revert;  echo b >> tests/soak_known.py; git commit -qam a; git revert --no-edit HEAD >/dev/null
  rt 5 "a commit and its revert" 1 "$W"
  newbranch renin;   git rm -q tests/soak_known.py; git commit -qm rm; mkdir -p tests; git mv src/x.py tests/soak_known.py; git commit -qm renin
  rt 6 "src/x.py renamed onto tests/soak_known.py" 1 "$W"
  newbranch renout;  git mv tests/soak_known.py src/soak_known.py; git commit -qm renout
  rt 7 "tests/soak_known.py renamed out" 1 "$W"
  newbranch del;     git rm -q docs/architecture.md; git commit -qm del
  rt 8 "docs/architecture.md deleted" 1 "$W"
  newbranch modex;   chmod +x tests/soak_known.py; git commit -qam mode
  rt 9 "tests/soak_known.py made executable" 1 "$W"
  newbranch sym;     rm tests/soak_known.py; ln -s ../src/x.py tests/soak_known.py; git add -A; git commit -qm sym
  rt 10 "tests/soak_known.py made a symlink" 1 "$W"
  newbranch link;    git rm -q --cached tests/soak_known.py; rm tests/soak_known.py
  git update-index --add --cacheinfo 160000,"$(git rev-parse HEAD)",tests/soak_known.py; git commit -qm gitlink
  rt 11 "tests/soak_known.py made a gitlink" 1 "$W"
  newbranch space;   mkdir -p "docs/architecture.md tests"; echo x > "docs/architecture.md tests/soak_known.py"; git add -A; git commit -qm space
  rt 12 "a path with a space ending in a clearing name" 1 "$W"
  newbranch space2;  mkdir -p "tests/soak_known.py docs"; echo x > "tests/soak_known.py docs/architecture.md"; git add -A; git commit -qm space2
  rt 13 "a path holding both names, in order" 1 "$W"
  newbranch top;     echo x > soak_known.py; git add -A; git commit -qm top
  rt 14 "a top-level soak_known.py" 1 "$W"
  newbranch glob1;   mkdir -p '[t]ests'; echo x > '[t]ests/soak_known.py'; git add -A; git commit -qm glob1
  rt 15 "a path [t]ests/soak_known.py" 1 "$W"
  newbranch glob2;   echo x > 'tests/soak_known.p[y]'; git add -A; git commit -qm glob2
  rt 16 "a path tests/soak_known.p[y]" 1 "$W"
  newbranch nl;      printf x > "src/a
b.py"; echo b >> tests/soak_known.py; git add -A; git commit -qm nl
  rt 17 "a src path with a newline in it" 1 "$W"
  newbranch ceil;    echo 'CEILINGS = {}' > tests/soak_ceilings.py; echo b >> tests/soak_known.py; git add -A; git commit -qm ceil
  rt 18 "tests/soak_ceilings.py beside a filing" 1 "$W"
  newbranch side;    echo side >> src/x.py; git commit -qam side
  newbranch merged;  echo b >> tests/soak_known.py; git commit -qam m1; git merge -q --no-edit side
  rt 19 "a merge bringing in src" 1 "$W"
  newbranch cfg;     echo b >> src/x.py; echo b >> tests/soak_known.py; git commit -qam cfg
  git config diff.relative true; git config color.ui always; git config diff.external /bin/false; git config diff.noprefix true; git config diff.renames copies
  rt 20 "src under hostile diff settings" 1 "$W"
  newbranch ok2;     echo c >> tests/soak_known.py; git commit -qam ok2
  rt 21 "a clearing branch under hostile diff settings" 0 "$OK"
  newbranch cfg2;    echo c >> src/x.py; git commit -qam cfg2
  o="$(cd tests && turnout SPEC-22)"
  check "src, turn run from tests/" "${o%% *}" 1; has "  ...src, turn run from tests/" "$o" "$W"
  git config --unset diff.relative; git config --unset color.ui; git config --unset diff.external; git config --unset diff.noprefix; git config --unset diff.renames
  newbranch sub;     echo d >> tests/soak_known.py; git add tests/soak_known.py
  git update-index --add --cacheinfo 160000,"$(git rev-parse HEAD)",vendor/evil; git commit -qm sub
  git config diff.ignoreSubmodules all; git config status.submoduleSummary false
  rt 23 "a gitlink added beside a filing, under diff.ignoreSubmodules all" 1 "$W"
  git config --unset diff.ignoreSubmodules; git config --unset status.submoduleSummary
fi

if want acquire; then
  echo "== acquire: the lock is never stamped onto a branch that does more than file the night"
  fixture acquire
  newbranch codefix; echo d >> src/x.py; git commit -qam codefix
  night success; Qs ticket SPEC-900 >/dev/null; o="$(Qs acquire SPEC-900)"
  check "acquire while nothing blocks" "$?" 0
  night failure
  o="$(Qs acquire SPEC-900)"; check "a holder re-stamping onto a non-clearing branch is refused" "$?" 1
  has   "  ...saying why" "$o" "changes more than tests/soak_known.py"
  newbranch filing; echo e >> tests/soak_known.py; git commit -qam filing
  o="$(Qs acquire SPEC-900)"; check "the holder re-stamping onto a clearing branch" "$?" 0
  has   "  ...is stamped" "$o" "ACQUIRED (already yours, on filing)"
  Qs release SPEC-900 >/dev/null
  # One reading per decision: turn's reading is the one acquire checks, even if a second would differ.
  newbranch other; echo f >> src/x.py; git commit -qam other
  night success; GH_SCHED2_JSON="[$(run_json 104 1 HEAD completed schedule)]"
  GH_ATT_JSON="{$(att_json 101 1 '"success"' "$NOW"),$(att_json 104 1 '"failure"' "$NOW")}"; export GH_ATT_JSON
  mv "$STUB/gh" "$STUB/gh.real"
  cat > "$STUB/gh" <<'FLIPGH'
#!/bin/sh
case " $* " in *" --workflow "*)
  if [ -f "$GH_LOG.read" ]; then GH_SCHED_JSON="$GH_SCHED2_JSON"; export GH_SCHED_JSON; fi
  touch "$GH_LOG.read" ;; esac
exec "$(dirname "$0")/gh.real" "$@"
FLIPGH
  chmod +x "$STUB/gh"; export GH_SCHED2_JSON; rm -f "$GH_LOG.read"
  Qs ticket SPEC-901 >/dev/null; o="$(Qs acquire SPEC-901)"
  check "acquire decides on turn's one reading of the night" "$?" 0
  has   "  ...and is ACQUIRED" "$o" "ACQUIRED"
  Qs release SPEC-901 >/dev/null
  mv "$STUB/gh.real" "$STUB/gh"; rm -f "$GH_LOG.read"
fi

if want note; then
  echo "== note: turn says when the soak is checking nothing, and nothing otherwise"
  fixture note
  newbranch any
  night success; o="$(turnout SPEC-1)"
  check "a fresh verdict: READY" "${o%% *}" 0
  hasnt "  ...and names no soak" "$o" "nightly soak"
  GH_SCHED_JSON='[]'; export GH_SCHED_JSON; o="$(turnout SPEC-2)"
  has   "no verdict in 30 nights: turn says so" "$o" "the nightly soak has no verdict in its last 30 scheduled runs"
  GH_SCHED_JSON="[$(run_json 101 1 HEAD completed schedule)]"; GH_ATT_JSON="{$(att_json 101 1 '"success"' "$OLD")}"; export GH_SCHED_JSON GH_ATT_JSON
  o="$(turnout SPEC-3)"
  has   "an old verdict: turn names its date" "$o" "the nightly soak's newest verdict is from $OLD"
  o="$(GH_API_FAIL=1 Qs status)"
  has   "status: an unreadable verdict says so" "$o" "the nightly soak's verdict could not be read"
  has   "  ...and prints the reading" "$o" "SOAK: 3 unreadable"
  night failure; o="$(Qs status)"
  has   "status: a blocking night" "$o" "SOAK: 4 101-1 $NOW"
fi

if want override; then
  echo "== override: under a main-green override the soak is not read, by anything"
  fixture override
  printf '#!/bin/sh\nexit 0\n' > "$Q/main-green"; chmod +x "$Q/main-green"
  night failure
  check "soak-verdict reads it as off" "$(verdict)" "0 off"
  newbranch ov; echo g >> src/x.py; git commit -qam ov
  o="$(turnout SPEC-1)"; check "turn is READY" "${o%% *}" 0
  has   "  ...saying the soak is not read" "$o" "the nightly soak is not read: a main-green override is installed"
  Qs ticket SPEC-2 >/dev/null; o="$(Qs acquire SPEC-2)"; check "acquire is not refused for the soak" "$?" 0
  printf 'spec SPEC-2\nbranch ov\nsince x\n' > "$Q/lock/holder"
  o="$(git push origin ov 2>&1)"; check "the hook passes the push" "$?" 0
  hasnt "  ...saying nothing of the soak" "$o" "nightly soak"
  Qs release SPEC-2 >/dev/null
  has   "status shows it off" "$(Qs status)" "SOAK: 0 off"
fi

if want hook; then
  echo "== hook: while the verdict blocks, only a clearing push goes up"
  fixture hook
  mkdir -p "$Q/lock"
  hold() { printf 'spec %s\nbranch %s\nsince x\n' "$1" "$2" > "$Q/lock/holder"; }
  p() { out="$(git push "$@" 2>&1)"; echo "$?"; printf '%s\n' "$out" > "$F/last"; }
  last() { cat "$F/last"; }
  S="the nightly soak's verdict blocks"
  night failure
  newbranch hf; echo f >> tests/soak_known.py; git commit -qam hf; hold SPEC-1 hf
  check "a clearing push while it blocks" "$(p origin hf)" 0
  hasnt "  ...draws no refusal" "$(last)" "REFUSED"
  echo g >> src/x.py; git commit -qam sneak
  check "the holder adds src to its clearing branch" "$(p origin hf)" 1
  has   "  ...refused for the soak" "$(last)" "$S"
  GH_RUNS_JSON='[{"headSha":"HEAD","status":"completed","conclusion":"failure","workflowName":"CI"}]'; export GH_RUNS_JSON
  check "the same push while another red is on the head" "$(p origin hf)" 1
  has   "  ...still refused for the soak" "$(last)" "$S"
  GH_RUNS_JSON='[{"headSha":"HEAD","status":"completed","conclusion":"success","workflowName":"CI"}]'; export GH_RUNS_JSON
  check "the same push with gh failing (fails open)" "$(GH_FAIL=1 p origin hf)" 0
  hasnt "  ...saying nothing of the soak" "$(last)" "$S"
  git -C "$F/origin.git" update-ref -d refs/heads/hf
  check "the same push with gh absent (fails open)" "$(PATH=/usr/bin:/bin p origin hf)" 0
  hasnt "  ...saying nothing of the soak" "$(last)" "$S"
  git -C "$F/origin.git" update-ref -d refs/heads/hf
  night success
  check "the same push when nothing blocks" "$(p origin hf)" 0
  hasnt "  ...saying nothing of the soak" "$(last)" "$S"
  night failure
  newbranch t1; echo h >> src/x.py; git commit -qam trunk; hold SPEC-2 t1
  check "a trunk push with src while it blocks" "$(p origin t1:main)" 1
  has   "  ...refused for the soak" "$(last)" "$S"
  newbranch st; echo i >> tests/soak_known.py; git commit -qam st; hold SPEC-3 st
  peer_src peer1; git -C "$R" fetch -q origin
  check "a stale clearing branch" "$(p origin st)" 1
  has   "  ...gets the rebase message" "$(last)" "your base is behind"
  hasnt "  ...and not the soak's" "$(last)" "$S"
  newbranch sp; mkdir -p "docs/architecture.md tests"; echo x > "docs/architecture.md tests/soak_known.py"; git add -A; git commit -qm sp; hold SPEC-4 sp
  check "a push of a path with a space" "$(p origin sp)" 1
  has   "  ...refused for the soak" "$(last)" "$S"
fi

if want reap; then
  echo "== reap: a dead holder's lock is kept while the verdict blocks, and broken once it is filed"
  fixture reap
  mkdir -p "$Q/lock"; printf 'spec SPEC-5\nbranch gone\nsince x\n' > "$Q/lock/holder"; touch -t 202001010000 "$Q/lock"
  night failure
  Qs ticket SPEC-906 >/dev/null
  check "a stale lock is kept while it blocks" "$([ -d "$Q/lock" ] && echo kept || echo broken)" kept
  file "101-1"; Qs ticket SPEC-907 >/dev/null
  check "the same lock is broken once the night is filed" "$([ -d "$Q/lock" ] && echo kept || echo broken)" broken
fi

cd "$ROOT" || exit 1
echo "----"; echo "pr-queue-soak-test: $oks passed, $fails failed."
[ "$fails" -eq 0 ] || exit 1

# --- the mutation sweep ---------------------------------------------------------------------
# Each plant is one sed edit to a copy of queue.sh or pre-push that must land on exactly one line
# and still parse; the sections it names are re-run against the mutant, and every case it names
# must then FAIL. A plant whose cases still pass is a check nothing here tests.
[ -z "$SECTIONS" ] && [ -z "${PR_QUEUE_SOAK_TEST_NO_SWEEP:-}" ] || exit 0
splant=0; sfail=0
plant() {  # <name> <file> <sed-expression> <sections> <case>...
  pname="$1"; pfile="$2"; pexpr="$3"; psect="$4"; shift 4
  M="$WORK/mut-$(printf '%s' "$pname" | tr -c 'a-zA-Z0-9' '-')"; mkdir -p "$M"
  cp "$SRC/queue.sh" "$SRC/pre-push" "$M/"
  sed "$pexpr" "$SRC/$pfile" > "$M/$pfile"
  changed=$(diff "$SRC/$pfile" "$M/$pfile" | grep -c '^>' || true)
  if [ "$changed" -ne 1 ]; then sfail=$((sfail+1)); echo "FAIL  plant $pname: the edit changed $changed line(s), wanted 1"; return 0; fi
  bash -n "$M/$pfile" 2>/dev/null || { sfail=$((sfail+1)); echo "FAIL  plant $pname: the mutant does not parse"; return 0; }
  out="$(PR_QUEUE_SOAK_TEST_SRC="$M" PR_QUEUE_SOAK_TEST_SECTIONS="$psect" sh "$ROOT/scripts/pr-queue-soak-test.sh" 2>&1)"
  missed=""
  for c in "$@"; do
    printf '%s\n' "$out" | grep -qF "FAIL  $c" || missed="$missed [$c]"
  done
  if [ -z "$missed" ]; then splant=$((splant+1)); echo "ok    plant $pname"
  else sfail=$((sfail+1)); echo "FAIL  plant $pname: these cases still pass:$missed"; fi
}
plant "a green attempt clears a red one" queue.sh 's#^        success)           good="\$n"; goodwhen="\$t" ;;#        success)           blocked=0; good="$n"; goodwhen="$t" ;;#' verdict "a green re-run does not clear a red attempt"
plant "the hook refuses when the verdict is unreadable" pre-push 's#^  if \[ \$? -eq 4 \]; then#  if [ $? -ne 0 ]; then#' hook "the same push with gh failing (fails open)" "the same push with gh absent (fails open)"
plant "soak runs on the head read as any run" queue.sh 's#^    \[ "\$wf" = "\$SOAK_WORKFLOW" \] && continue#    :#' head "a red dispatch on the head does not block" "a soak running on the head is not unsettled"
plant "only the latest attempt read" queue.sh 's#^    n=1$#    n=$attempts#' verdict "a green re-run does not clear a red attempt" "a red night being re-run still blocks"
plant "timed_out not blocking" queue.sh 's#^        failure|timed_out) #        failure) #' verdict "a timed-out night blocks"
plant "a verdict-less night read as passed" queue.sh 's#^        success)  #        *)  #' verdict "a cancelled night falls back to a failed one" "a null night falls back to a failed one"
plant "a verdict-less night read as failed" queue.sh 's#^        failure|timed_out) #        failure|timed_out|cancelled|startup_failure|action_required|skipped|neutral|stale|null) #' verdict "a cancelled night falls back to a passed one" "a stale night falls back to a passed one"
plant "only completed runs listed" queue.sh 's#--limit "\$SOAK_LOOKBACK" --json databaseId,attempt#--limit "$SOAK_LOOKBACK" --status completed --json databaseId,attempt#' "verdict calls" "a red night being re-run still blocks" "no --status filter on the soak's runs"
plant "the newest night only, with no fallback" queue.sh 's#^    \[ -n "\$good" \] && { echo "0 \$id-\$good \$goodwhen"; return 0; }#    echo "0 $id-$good $goodwhen"; return 0#' verdict "a cancelled night falls back to a failed one"
plant "a filing of any attempt clears" queue.sh 's#^      filed "\$id-\$bad"$#      filed "$id-1"#' filing "filing an earlier attempt does not clear"
plant "the filing's format not held" queue.sh 's#grep -qE "^    \\"\$1\\": \\".+\\",\\\$"#grep -qE "\\"$1\\""#' filing "a filing in a comment does not clear" "a filing at the wrong indent does not clear"
plant "the queue's ref not fetched" queue.sh 's#^  fetch_queue_ref || return 2$#  true || return 2#' filing "a peer's filing not yet fetched here clears" "filing the blocking attempt clears"
plant "--refmap= dropped" queue.sh 's#fetch -q --no-write-fetch-head --refmap= origin#fetch -q --no-write-fetch-head origin#' refs "  ...origin/main unchanged after the read"
plant "the clearing base read from origin/main" queue.sh 's#merge-base HEAD "\$SOAK_REF"#merge-base HEAD "origin/$MAIN"#' refs "a valid clearing branch reads READY while origin/main is stale"
plant "an unreadable verdict read as clear by main-state" queue.sh 's#4) return 4 ;; \*) return 3 ;; esac#4) return 4 ;; *) return 0 ;; esac#' unreadable "  ...and main-state reads it as 3, not clear"
plant "the mode not held" queue.sh 's#^    \[ "\$mode" = 100644 \] || return 1#    true || return 1#' turn "docs/architecture.md deleted" "tests/soak_known.py made executable" "tests/soak_known.py made a symlink" "tests/soak_known.py made a gitlink"
plant "the path not held" queue.sh 's#^    \[ "\$path" = tests/soak_known.py \] || \[ "\$path" = docs/architecture.md \] || return 1#    true#' turn "src beside a clearing file" "a top-level soak_known.py" "tests/soak_ceilings.py beside a filing"
plant "names matched inside a path" queue.sh 's#\[ "\$path" = tests/soak_known.py \] ||#[[ "$path" == *soak_known.py* ]] ||#' turn "a path holding both names, in order" "a top-level soak_known.py"
plant "the head's fields read space-separated, empty conclusion and all" queue.sh 's#--jq ".\[\] | \[.headSha, .status, .conclusion, .workflowName\] | \$GH_FIELDS"#--jq ".[] | \\"\\\\(.headSha) \\\\(.status) \\\\(.conclusion) \\\\(.workflowName)\\""#' head "a soak running on the head is not unsettled" "a soak queued on the head, its conclusion empty, is not unsettled"
plant "the clearing range read through porcelain diff" queue.sh 's#diff-tree -r -z --no-abbrev --no-renames --ignore-submodules=none#diff --raw -z --no-abbrev --no-renames#' turn "a gitlink added beside a filing, under diff.ignoreSubmodules all"
plant "renames detected" queue.sh 's#--no-abbrev --no-renames#--no-abbrev -M#' turn "tests/soak_known.py renamed out"
plant "an empty range accepted" queue.sh 's#^  \[ "\$n" -gt 0 \]$#  true#' turn "no commits" "a commit and its revert"
plant "turn admits any branch" queue.sh 's#^    4) if clearing_branch; then#    4) if true; then#' turn "src beside a clearing file" "no commits"
plant "acquire not reading the verdict" queue.sh 's#if \[ "\$SOAK_RC" = 4 \] && ! clearing_branch; then#if false; then#' acquire "a holder re-stamping onto a non-clearing branch is refused"
plant "acquire reading the verdict a second time" queue.sh 's#^  soak_read   \# the reading turn made, when it made one$#  SOAK_RC=; soak_read#' acquire "acquire decides on turn's one reading of the night"
plant "the quiet note never printed" queue.sh "s#^    echo \"  note: the nightly soak's newest verdict is from \$t\"#    :#" note "an old verdict: turn names its date"
plant "turn on 0 says nothing of a quiet soak" queue.sh 's#green"; soak_note; return 0 ;;#green"; return 0 ;;#' note "no verdict in 30 nights: turn says so" "an old verdict: turn names its date"
plant "the unreadable note dropped" queue.sh "s#^    3\*)        echo \"  note: the nightly soak's verdict could not be read\"; return 0 ;;#    3*) return 0 ;;#" note "status: an unreadable verdict says so"
plant "the override reaching only main_green" queue.sh 's#^  \[ -x "\$Q/main-green" \] && { echo "0 off"; return 0; }#  :#' override "soak-verdict reads it as off" "the hook passes the push"
plant "a --json field changed" queue.sh 's#--json databaseId,attempt #--json databaseId,attempts #' "calls verdict" "the soak's scheduled runs, any status" "a failed night blocks"
plant "the hook not reading the verdict" pre-push 's#^  "\$Q/queue.sh" soak-verdict >/dev/null 2>&1#  false#' hook "the holder adds src to its clearing branch" "a trunk push with src while it blocks" "a push of a path with a space"
plant "the hook reading main-state instead" pre-push 's#^  "\$Q/queue.sh" soak-verdict >/dev/null 2>&1#  "$Q/queue.sh" main-state >/dev/null 2>\&1#' hook "the same push while another red is on the head"
plant "the hook reading stale refs for the soak" pre-push 's#case " \$stale " in \*" \$branch "\*) ;;#case " $stale " in *" none "*) ;;#' hook "  ...and not the soak's"
plant "the hook skipping the trunk" pre-push 's#\*) pushed="\$pushed\$branch \$lsha#*) [ "$branch" = "$MAIN" ] || pushed="$pushed$branch $lsha#' hook "a trunk push with src while it blocks"
plant "reaping not stopped by the verdict" queue.sh 's#^  main_green || return 0$#  main_green; [ $? -eq 1 ] \&\& return 0#' reap "a stale lock is kept while it blocks"
echo "----"; echo "pr-queue-soak-test sweep: $splant plant(s) broke their cases, $sfail failed."
[ "$sfail" -eq 0 ]
