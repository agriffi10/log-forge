---
type: Decision
title: "The model gates its close count only where one caller makes it exact, and soaks it under concurrency every night"
description: "the model test gates a sink's close count (no close at the previous close's write count, no write after the last close returned) only in its sequential half, where one caller makes it exact; under four callers it is a soak that `model-soak.yml` runs every night on `main` and on a pull request that changes it, and a scheduled night's failed or timed-out attempt stops the PR queue until that night's own filing is on `main`, nothing else stopping or clearing it; the soak excuses a repeat close only for a writer's arm, or a log that entered after the first take and wrote before the second, between the takes; a red item of a known shape is known while its §12 item is open and it is under its ceiling; a bad model switch stops the session. (SPEC-086, SPEC-099)"
---

`tests/test_invariants_model.py` holds every call it makes to its own timeout plus `EPSILON` in
both halves, and that bound gates everywhere. The close count is different. In the sequential half
it is asserted after every step, as `check_repeat`, and after every `shutdown` and exit step, as
`check_late_write`, with a write counted at the first statement of the delivery method the library
calls. The concurrent gating runs assert neither rule. Under `LOG_FOUNDRY_MODEL_SOAK=1` the same
count runs over the concurrent half as `test_soak_the_close_count_under_concurrency`, which reads
the library's own arms and takes through wrappers installed by name and judges by
`check_unowed_repeat`. The four wrapped names are `_CloseRegistry.arm` and `.take`,
`api._note_orphan_emit` and `_lifecycle._get_worker`.

**Since SPEC-099 the soak runs every night.** `.github/workflows/model-soak.yml` sets the switch
every night on `main` at 09:37 UTC over a window of 5,000 seeds chosen by the run number, when
dispatched, and on a pull request that changes the soak or anything it imports, so the soak gates
that pull request. Its report holds a floor (exactly three soak items a seed, none skipped, every
expected failure carrying the known property and text, the eight wiring items run and passed) and
each known shape's nightly ceiling, because pytest's exit over fewer items, or none, is 0. A
scheduled night whose attempt concluded `failure` or `timed_out` stops the PR queue
(`scripts/pr-queue/PROTOCOL.md`): only a pull request that files that night, changing
`tests/soak_known.py` and `docs/architecture.md` and nothing else, clears it, and a re-run, a later
green night, a dispatch and a time clear nothing. Andrew decided both: the nightly soak on
2026-11-14, and that a red night blocks merging on 2026-11-17.

**Why the sink alone is exact under one caller and not under four.** A second close at one write
count is owed when a writer armed the sink again after the first close's take and its write entered
before that close ran. Invariant 5 records that as correct: "a sink written to after its close is
owed a second close" (SPEC-045). With one caller nothing can arm a sink between a take and its
close. With four it happens: seed 1653 does it in about one run in a hundred, and at the sink it
looks the same as the double close #339 fixed. Only the library's record of who armed the sink
between the two takes tells them apart. A gate that reads a race at its rate goes red on honest
runs, so the concurrent count is a soak and its red items are findings, read against the known
shapes, not a verdict on the change under test.

**What a writer's arm is.** The orphan path arms the sink it is about to write. The worker
build arms the configured sink for the worker to write. Both are writers in invariant 5's sense.
The second diff review forced a worker-build arm between two takes. With only the orphan path
counted, the soak flagged `repeat@2` on 5 of 5 runs, while the orphan twin of that sequence was
silent. An orphan-path arm counts only for the sink its call was made for. A build's sink is
resolved inside it, so a build's arm is attributed by the thread being inside it.

**A log that entered after the first take owes the second close too (SPEC-099).** When something
else, such as a sink change, re-armed the sink after the first take, a log finds the sink already
owed, takes the fast path, arms nothing and writes. So the soak records a `Note` at the orphan
path's entry, before its owed check, and `owed_between` excuses a `Note` on a thread followed by a
`Write` on that same thread between the two takes. **Not any write:** a write whose log passed its
owed check before the first take is the race of §12's orphan-log item, and excusing it would read
Shape A, a sink change arming a sink a `shutdown()` had already taken and closed, as owed. Nothing
else between the takes owes the second close. Every arm and take also records the library function
that made it, read from the caller's frame, so a red night is filed from a record that says who
armed and who took.

**A known shape is reported as known (SPEC-099).** `tests/soak_known.py` holds a row per soak shape
that an open item of `docs/architecture.md` §12 records, each with its predicate; a red soak item
whose every finding on every sink has a row ends as a non-strict expected failure naming that item.
A row whose item leaves `### Open` fails the default suite, so the spec that closes an item removes
its row. No row may ever explain Shape A's record, which a regression of the swap item SPEC-097
closed and the open `fenced` item both produce, so either stays red. Each row has a nightly ceiling
in `tests/soak_ceilings.py`, outside the clearing files, set so a rate that doubles stays under it
and one that triples passes it; a row a clearing pull request has just filed reads 3 until a
reviewed pull request measures it.

**Why a writer's arm, and the earliest take.** A rule that excused any arm between the takes would
excuse #339 itself, whose second close follows a sink change's arm. At `31dda162` that weaker rule
failed 6 to 9 of the 48 default soak items and this one 28 to 30 (SPEC-086 *Evidence*). Each close
is paired with the earliest take not yet discharged, because a sink change can hand its close to a
closer thread that a `shutdown()`'s inline close overtakes. The latest take would then serve both
closes and hide the writer's arm between them. A close with no take left is `untaken`, a wiring
failure, and is never judged against a wider window.

**Why a bad switch stops the session.** Under the default `-n 12`, an import error in the model
file fails that file alone while every other file runs. A mistyped soak would read as a few errors
among thousands of passes, or as a green run of the defaults if the parse fell back. So
`tests/conftest.py`'s `pytest_configure` refuses the value as a usage error before collection.

**Rejected:** the close count under concurrency in the gating set, which is a rate rather than an
invariant; the sink-only repeat rule there, which cannot tell an owed close from #339; the weaker
any-arm rule, which excuses #339; the any-write rule, which excuses Shape A; latest-take pairing;
SPEC-085's overlap exemption, which a spec review measured hiding the repeat close in 24 of 24 runs
at `31dda162`; a fallback to the defaults on a malformed switch; and a queue that read the soak from
`main`'s head, which a green re-run, a merge made while the night ran, a cancelled night or a
dispatch cleared or dodged (SPEC-099's execution-frame review).

**Do NOT build:** a concurrent close count in the gating set; a soak rule that excuses a
non-writer's arm, or any write between the takes; a known row without an open item; a known row
that explains Shape A's record; a ceiling in a clearing file; a gating assertion excused as known;
a window drawn from the clock; a queue that reads the soak from `main`'s head, clears a night by a
time, a re-run or a later green night, or lets a dispatch's or a pull request's run block it; or a model switch that
falls back on a value it cannot parse.

## History

### 2026-11-25

- **Refined** in SPEC-099's second code round: the queue reads every night in its 30-run window and
  blocks on the newest unfiled failing one, so a later green night clears nothing; a red night
  older than the window falls out of it unfiled, which `scripts/pr-queue/PROTOCOL.md` states.

### 2026-11-24

- **Reversed in part** by SPEC-099, on Andrew's decisions of 2026-11-14 and 2026-11-17. The soak
  was ~~set by no workflow~~, and its *Do NOT build* listed ~~a workflow or default that sets
  `LOG_FOUNDRY_MODEL_SOAK`~~; it now runs every night on `main`. It ~~gates nothing~~, partly
  reversed: it gates a pull request that changes it, and a failed scheduled night stops the PR
  queue until that night is filed. SPEC-099 also widened the repeat rule to a log that entered
  after the first take, and added the known shapes. Title and fence rewritten in place.

### 2026-11-15

- **Refined** after the second diff review: a worker build's arm is a writer's arm, and an
  orphan-path arm counts only for its own sink.
- **Settled** by SPEC-086 at its completion.
