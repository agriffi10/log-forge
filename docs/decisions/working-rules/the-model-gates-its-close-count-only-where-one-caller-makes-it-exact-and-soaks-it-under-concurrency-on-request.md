---
type: Decision
title: "The model gates its close count only where one caller makes it exact, and soaks it under concurrency on request"
description: "the generative model test gates a sink's close count (no close at the previous close's write count, no write after the last close returned) only in its sequential half, where one caller makes the count exact; under four callers the same count is a soak, collected only with `LOG_FOUNDRY_MODEL_SOAK=1` and set by no workflow, that excuses a repeat close only for a writer's arm between the two closes' takes as the library records them; a malformed model switch stops the session, never falls back. (SPEC-086)"
---

`tests/test_invariants_model.py` holds every call it makes to its own timeout plus `EPSILON` in
both halves, and that bound gates everywhere. The close count is different. In the sequential half
it is asserted after every step, as `check_repeat`, and after every `shutdown` and exit step, as
`check_late_write`, with a write counted at the first statement of the delivery method the library
calls. The concurrent gating runs assert neither rule. Under `LOG_FOUNDRY_MODEL_SOAK=1` the same
count runs over the concurrent half as `test_soak_the_close_count_under_concurrency`, which reads
the library's own arms and takes through wrappers installed by name and judges by
`check_unowed_repeat`.

**Why the sink alone is exact under one caller and not under four.** A second close at one write
count is owed when a writer armed the sink again after the first close's take and its write entered
before that close ran. Invariant 5 records that as correct: "a sink written to after its close is
owed a second close" (SPEC-045). With one caller nothing can arm a sink between a take and its
close. With four it happens: seed 1653 does it in about one run in a hundred, and at the sink it
looks the same as the double close #339 fixed. Only the library's record of who armed the sink
between the two takes tells them apart. A gate that reads a race at its rate goes red on honest
runs, so the concurrent count runs on request and its red items are findings, not a merge verdict.

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
any-arm rule, which excuses #339; latest-take pairing; SPEC-085's overlap exemption, which a spec
review measured hiding the repeat close in 24 of 24 runs at `31dda162`; and a fallback to the
defaults on a malformed switch.

**Do NOT build:** a workflow or default that sets `LOG_FOUNDRY_MODEL_SOAK`, a concurrent close
count in the gating set, a soak rule that excuses a non-writer's arm, or a model switch that
falls back on a value it cannot parse.

## History

### 2026-11-15

- **Settled** by SPEC-086 at its completion.
