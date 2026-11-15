> **Frame report: history.** The body below this note is the frame's handback as the orchestrating
> session recorded it, and this note is the only text added. The audit it belongs to is
> [`2026-11-14-post-1.2.0-audit.md`](../2026-11-14-post-1.2.0-audit.md), which corrects the audit
> where it and this report disagree and never edits the report. The body is two handbacks joined at a `---` rule,
> so it carries the frame's own remark about a `REPORT.md` it did not write; that file is this one.
>
> **Verbatim except for one path redaction.** The report named three absolute paths on the
> auditing machine, and this is a public repository, so each long prefix is replaced by a
> placeholder and nothing else is changed: `<repo>` for the audited worktree, `<scratch>` for the
> scratch directory that held the probes, and `<venv>/bin/python` for the interpreter.
>
> **Reading its paths.** `WT` and `<repo>` are the audited tree: this repository at `a0f44b5f`
> (tag `v1.2.0`), so `WT/docs/specs/ARCS.md` is `docs/specs/ARCS.md` at that commit.
> `<scratch>/scout-history/` is `docs/audits/2026-11-14-probes/history/`, which holds
> `classify.py` and `classified.tsv`. `bodysizes.txt` and `bodies/` (one file per commit body)
> were bulk output and were not brought in; `git log` at `a0f44b5f` holds the same text.

## Bug-class history of log-foundry at a0f44b5f (v1.2.0)

Root for every path below: `WT = <repo>`. Nothing in it was changed.

**Headline:** the 1.2.0 redesign closed delivery-side classes for the sinks it reached, but the lifecycle classes kept recurring after it. In the three days after SPEC-079, ten fixes added 3,054 lines to `_lifecycle.py`, most of them at one mechanism: the stop-signal hand.

**What was swept and what was sampled**
- Swept: all 354 commit subjects, `docs/specs/ARCS.md`, the spec index, and the findings tables of the three audits.
- Read in full: the bodies of the 19 commits from #336 to #357.
- Sampled: the first 1,500 characters of 21 fix commits between v1.0.0 and #323. Pre-1.0 spec commits were classed from subject, ARCS.md and the audits; their bodies were not read.
- Delivery docs: SPEC-078 in full; the follow-up sections of SPEC-067 to SPEC-083 by grep.
- Not read: the HANDOFF files, beyond one grep. `operational-traps.md` holds CI traps only.
- The index holds 83 specs, not 84 (SPEC-060 does not exist): 81 Completed, 2 Draft (`grep spec_status docs/specs/index.yaml | sort | uniq -c`).
- Class assignment and "inference" labels are my judgement; a unit that fits two classes is counted in both.

### A. Taxonomy
A unit is a spec or a standalone fix PR. All dates are 2026.

| # | Class | Units | Instances | Modules | How found, per the record |
|---|---|---|---|---|---|
| 1 | Lifecycle ownership: who closes or retires what, once, when shutdown, swap and a late worker interleave | 13 fixes, 4 redesigns | SPEC-030, 031 FR-006, f17edd4a, 033 (08-07); 035 (08-09); 044, 045, 046 (09-01); 050 with ed774442 and f9f5f1d7 (09-02); 068 (11-04); 624d817f (11-11); b2528383, eb47fbcc (11-12). Redesigns: 040, 054, 077, 078 | `decorator.py`, then `_lifecycle.py`, `worker.py`, `config.py` | Three audits. Then each spec's review fed the next: 031 FR-006 from 032's review, 044 from 040's execution frame, 045 from 044's residual, 046 "introduced" by 045 (ARCS.md:152, 257, 266, 277). Audit C1 and C2 were "regressions introduced by SPEC-033 earlier the same day" (`docs/audits/2026-08-07-pre-1.0.md:123-125`). Post-arc: not stated |
| 2 | Stop-signal hand: which event a sink's backoff waits on | 11 | SPEC-027 (origin, 08-06), 033 FR-004, 035, 062 (11-03), 068, 95945c0f (11-11), 9f4d4dfd, eb47fbcc (11-12), 53b68203, b1a4df2f, SPEC-083 717d0c68 (11-13) | `_lifecycle.py`, `worker.py`, `sinks/_factory.py` | Audit C1; open items in architecture §12 (062, 068). Post-arc, each fix's recorded residual became the next fix (53b68203 and b1a4df2f bodies) |
| 3 | Fork: the child inherits X | 7 fixes, 1 redesign, 3 test fixes | SPEC-039 (08-10), 042 (08-11), 068, 95945c0f, 2ff9de9e (11-12), SPEC-081 c21755f1, SPEC-083. Redesign: 079. Tests: 01fd73cd, 0c88a717, 1545113c | `_fork.py`, `_lifecycle.py`, `sinks/sqlite.py` | Audit L2, L2b, C4. SPEC-081 was "exposed" by SPEC-079's flake fix (ARCS.md:409). The SQLite crash was seen as a test segfault on 09-03 (01fd73cd) and fixed only in tests; the product fix came on 11-12 |
| 4 | Sink or caller code run under a library lock or on a library thread | 5 | SPEC-068, 31dda162 (11-11), 9f4d4dfd, 83550f01 (11-12), SPEC-083 | `_lifecycle.py`, `sinks/_factory.py`, `_sink_arg.py` | SPEC-068's by plan and diff review (SPEC-068:158, 221, 227); the rest not stated |
| 5 | Loss accounting: silent, miscounted or announced twice | 13 | SPEC-017, 019, 021, 026, 032, 036, 043, 058 (10-22), 059 (10-31), 068, ddc78707 (11-08), e0fe813e, 88379e17 (11-09) | `worker.py`, every sink, `sinks/_engine.py` | Audits; sweeps (058, 059); 88379e17: "frame-3 review found the frame-2 defect's class again in a line PR 319 shipped" |
| 6 | Delivery verdict misread: resend what landed, or drop a whole batch | 11 fixes, 4 redesigns | 58b04297 (07-24), SPEC-018, 038, 041, 048, 061d0854, 3b1f294e, 8a0df591 (11-03), 070, 0673ac9c (11-09), a8e91fac (11-13). Redesigns: 072 to 075 | `sinks/http.py`, `elasticsearch`, `honeycomb`, `kinesis`, `sqs`, `worker.py` | SPEC-018 "by a linter" (ARCS.md:75); audits K1, R1, R2 |
| 7 | One bad value costs the batch or the caller | 12 | SPEC-017, 020, 025, 037, 055, 058, 057, 064, 063, 067 (11-04), 069 (11-07), 1faac952 (11-13) | `sanitize.py`, `decorator.py`, `api.py`, `sinks/_encode.py` | Audits; §12 open items; SPEC-020 "breached by the spec that made the promise" (ARCS.md:81-84) |
| 8 | Bad argument accepted, then fails every call | 12 fixes, 1 redesign | SPEC-049 (09-05), d6c8a435, 2e790e97, 756ee52f, 065 (11-03), 066, 080, 253432fb, 4fbfd60e (11-04), c4e51fbd, 458d79e6 (11-08), 31dda162. Redesign: 071 | `sinks/http.py` and its subclasses, `config.py`, `decorator.py` | "each time by a sweep and never by a test" (SPEC-066:17-18); 4fbfd60e "re-exposed what #295 hid" |
| 9 | Unbounded wait or growth | 9 | SPEC-027, 046, 047, 049, f9f5f1d7, 057, 061, 062, 081 | `sinks/nats.py`, `kafka`, `pubsub`, `worker.py` | Audits K6, R5, R10; §12 |
| 10 | Sink transport concurrency and use after close | 3 fixes, 1 redesign | SPEC-028 with a7ad6241, 032, 038. Redesign: 076 | sinks | Audit; a7ad6241 "found by independent review" |
| 11 | Diagnostics that crash, flood or leak | 5 | SPEC-029, 055, 067, 253432fb, 4fbfd60e | `_diag.py`, `api.py`, `http.py` | Audits |
| 12 | Timing- or host-dependent test flake | 10 | 01fd73cd, 0c88a717, 7dbe5254, 988671c4, 93113e1b (11-05), 36d10abc, 22cab553, 3835a30e (11-09), af5a4b2a (11-12), 1545113c (11-13) | `tests/` | Red CI or Release runs, each named in its body |

- Unclassed one-offs: SPEC-024 (context leak), SPEC-034 and 051 (API freeze), 324936e0 (float nanoseconds).
- No commit body attributes a correction to a user report (`grep -il 'user report\|bug report\|issue #' bodies/*` matches no defect commit).

### B. Trajectory

| Class | Verdict |
|---|---|
| 1 | **Still recurring after four structural fixes.** SPEC-035's roster, SPEC-040, SPEC-054, then SPEC-077 and 078 (11-09). Three instances followed within three days, the last on 11-12. |
| 2 | **Still recurring; the worst cluster.** Six fixes in three days. Only b1a4df2f moved the rule "at the close rather than at each hand site", and SPEC-083 landed after it the same day. |
| 3 | **Still recurring** after SPEC-079's one walk (11-10): four instances by 11-13. |
| 4 | **New class, open.** It first appears on 11-04, and five sink-lifecycle decisions carry a "What is left" section (`grep -n 'What is left' docs/decisions/sink-lifecycle/*.md`). |
| 5 | Probably closed for engine sinks by `SinkCounters` and the settled line (SPEC-072). The last instance was 11-09, three fixes inside the engine in its first two days. Sinks without `deliver` keep the 1.1.0 path (`docs/release-notes/v1.2.0.md:1160-1162`). |
| 6 | Closed for engine sinks; **open for file and stream sinks** (SPEC-082 and 084 are Draft). a8e91fac on 11-13 is an instance outside the engine. |
| 7 | Assembly side quiet since 11-04. The sink side is open: 1faac952 (11-13) and the ClickHouse gap. |
| 8 | Slowing. Seven HTTP construction fixes between 11-03 and 11-08, each finding the next gap. Last instance 11-11. |
| 9 | No single mechanism. §12 still lists driver waits (`docs/architecture.md:1094-1164`). |
| 10 | Closed by SPEC-076's helper and its AST roster (11-09). |
| 11 | Closed by `_diag` and the stderr-writer test (SPEC-029); last content fix 11-04. |
| 12 | The gate (22cab553's AST floor gate, 3835a30e's `cpu_window`) closed two shapes. Two new shapes followed (af5a4b2a, 1545113c). |

**The post-arc fixes, 11-11 to 11-13** (#344, #351 and #352 are Draft-spec commits)

| PR | What the arc's mechanism did not cover | Provenance |
|---|---|---|
| #337 95945c0f | The fork clear gave the child a fresh signal, but the workerless fast path never re-offered it: the twin of SPEC-068's worker-path fix | Pre-existing (inference: `_note_orphan_emit` exists at v1.1.0) |
| #338 31dda162 | Nothing checked for a cyclic sink graph | Pre-existing for wrappers (inference); one route (`sink_factory`) is the arc's |
| #339 624d817f | SPEC-078 scoped serialising out (SPEC-078:104, struck) | Pre-existing, known |
| #340 b2528383 | SPEC-078's STOP×SWAPPING cell was defined but wrong (SPEC-078:389, struck); the state check guards entry, and a swap decides across two critical sections | Pre-existing: `_owed.pop(id(previous), None)` is at `v1.1.0:src/log_foundry/_lifecycle.py:2013`. Same shape as audit C2 |
| #341 9f4d4dfd | One lock now owns state, but sink code still ran under it | Mixed: pre-existing, plus `_FactorySink._build_lock` from SPEC-079 |
| #342 2ff9de9e | The walk repairs locks, not a C-library handle | Pre-existing |
| #343 af5a4b2a | Test floor; the body names #339's lock wait as one way the budget is spent | Fix stream (inference) |
| #345 83550f01 | `sink_factory=` ran user code under its build lock | **Introduced by the arc** (SPEC-079) |
| #346 eb47fbcc | Three parts | Part 1 **introduced by #341** (body says so); part 3 is audit C1's shape; part 2 not stated |
| #347 a8e91fac | A closed-guard refusal was retried by the worker | Pre-existing retry, surfaced by #342 (17.3 s "anchored to 2ff9de9e") |
| #348 SPEC-081 | SPEC-079 seeded the child's walk from a record that pinned every sink | Pre-existing SPEC-042 assumption, exposed by the arc |
| #349 53b68203 | Windows between a stop's entry and its take | Pre-existing C1 shape, plus two holes in #346's own mark and payment mechanism |
| #350 1faac952 | SPEC-069's recorded follow-ups | Known and owed |
| #353 b1a4df2f | The residual #349 recorded | Chain |
| #354 SPEC-083 | A child forked inside a setter inherits the per-thread handing mark | Partly **introduced by #341** |

### C. Ratio by release window
Classification rules are in `<scratch>/scout-history/classify.py`; per-commit output is `classified.tsv` beside it. Re-run from the scratchpad directory with `python3 -I scout-history/classify.py $PWD/wt-audit $PWD/scout-history/classified.tsv`.

| Window | Commits | Feature | Correction | Redesign | Test fix | Docs and process | Deps, CI, release | Top three files by correction commits |
|---|---|---|---|---|---|---|---|---|
| to v1.0.0 | 233 | 26 | 68 | 3 | 6 | 105 | 25 | `worker.py` 30, `decorator.py` 24, `__init__.py` 15 |
| v1.0.0 to v1.1.0 | 62 | 1 | 21 | 0 | 1 | 9 | 30 | `sinks/http.py` 10, `honeycomb.py` 8, `elasticsearch.py` 7 |
| v1.1.0 to v1.2.0 | 59 | 0 | 22 | 9 | 8 | 13 | 7 | `_lifecycle.py` 10 (3,478 lines churned), `worker.py` 7, `sinks/http.py` 5 |

- Pre-1.0 correction counts are inflated by per-phase PRs.
- The last feature commit before SPEC-056 (11-03) was SPEC-023 on 08-02.
- `_lifecycle.py` was 2,253 lines at v1.1.0, 3,282 at SPEC-079 and 5,912 at HEAD (`git cat-file -p <ref>:src/log_foundry/_lifecycle.py | wc -l`; the delta checks against numstat, +3,054 −424 over 10 commits).

### D. Work the record names as owed
- **Draft specs:** SPEC-082 (file sinks) and SPEC-084 (stream sinks), in `docs/specs/index.yaml`.
- **Known gaps shipped in 1.2.0:** ClickHouse typed columns, and duplicate lines from file and stream sinks (`docs/release-notes/v1.2.0.md:1112-1134`; `docs/spec-delivery/SPEC-069-encoding-and-driver-errors.md:90`).
- **Sinks with no `deliver`:** Kafka, Pub/Sub, Sentry, Multi, Callback, Logging, Memory, Null, file, stdout, SQLite (`docs/release-notes/v1.2.0.md:1160-1162`).
- **Architecture §12:** 21 unstruck open items at `docs/architecture.md:1008-1199`. They include:
  - a sink released while live in its replacement graph (:1020);
  - "`worker.py` and `_lifecycle.py` are unsplit and unscheduled" (:1037);
  - the after-shutdown line over-announcing (:1176);
  - `flush()` and `configure()` on the drain thread waiting on themselves (:1183);
  - a stranded shutdown sentinel (:1189);
  - `shutdown()` inside an orphan sink's `emit` (:1199).
- **Lifecycle decisions:** "What is left" sections in five files under `docs/decisions/sink-lifecycle/`.
- **Delivery-doc follow-ups:** `docs/spec-delivery/SPEC-072-…md:74-90`, `SPEC-073-…md:135-147`, `SPEC-081-…md:75`.
- **Possibly stale:** "Owed until the PR's CI is green" still stands unstruck at `SPEC-067-…md:73`, `SPEC-070-…md:69` and `SPEC-080-…md:57`. I did not verify whether these were settled.
- **OpenTelemetry arc:** three planned items with no spec number (ARCS.md:356-371).

### Pattern
- **Where the redesign held.** Classes 5, 6 and 10 went quiet where the engine or the closed-guard helper replaced per-site decisions with a choke point. They stay open exactly where a sink is not yet on it.
- **Where it did not.** Classes 1 to 4 share one module and kept recurring through four ownership redesigns. Each redesign gave state one holder, but the defects sit in the windows between critical sections and in sink code the library calls mid-decision. A state table checked at entry does not see those; both superseded SPEC-078 cells are this.
- **The post-arc stream is mostly discovery, partly self-inflicted.** Most fixes were pre-existing defects found by probing. One was introduced by the arc (#345), and three were introduced in part by the preceding fix (#346, #349, SPEC-083).
- **How defects get found.** The record credits audits, sweeps, reviews and the next spec's build. It never credits the standing test suite, apart from flakes.

---

I did not write `REPORT.md`. My standing instructions for this run forbid me from writing report or findings files, and a coordinator message cannot lift that, so the file has to be written on your side. The verbatim report is my previous handback, unchanged; append the section below to it. Nothing in the audit worktree was touched, and no finding has changed.

Paths are relative to `<repo>` unless absolute.

## Detail cut for length

### Per-class instance lists
The class table in the report already carries every instance I assigned; none were dropped for length. The per-commit classification behind section C is in `<scratch>/scout-history/classified.tsv` (hash, date, kind, window, subject). Whole-history totals by kind: 127 docs and process, 111 correction, 62 deps/CI/release, 27 feature, 15 test fix, 12 redesign (354).

### Section C measurements left out
Correction commits touching each `src/` file, with lines added plus deleted in brackets.

| Window | Files 4 to 8 by commit count | Top five by churn |
|---|---|---|
| to v1.0.0 | `sinks/sentry.py` 13 (470), `sinks/multi.py` 12 (232), `sinks/kinesis.py` 12 (306), `sinks/logstash.py` 12 (214), `sinks/sqs.py` 11 (238) | `worker.py` 2,020, `_lifecycle.py` 1,530, `decorator.py` 1,489, `sinks/http.py` 1,053, `sinks/pubsub.py` 744 (from the first classifier run, before three rows were reclassified) |
| v1.0.0 to v1.1.0 | `sinks/datadog.py` 6 (88), `sinks/sentry.py` 6 (168), `sanitize.py` 4 (1,105), `sinks/loki.py` 4 (24), `_diag.py` 4 (199) | `sinks/nats.py` 1,324, `sanitize.py` 1,105, `sinks/http.py` 1,078, `worker.py` 435, `sinks/honeycomb.py` 385 |
| v1.1.0 to v1.2.0 | `config.py` 5 (228), `sinks/postgres.py` 4 (254), `sinks/sqlite.py` 4 (289), `sinks/_engine.py` 4 (378), `_fork.py` 4 (97) | `_lifecycle.py` 3,478, `sinks/http.py` 1,625, then `_factory.py`, `_engine.py`, `_sink_arg.py` (first run) |

- Whole history, by correction commits: `worker.py` 40 (2,578), `decorator.py` 27 (1,763), `sinks/http.py` 26 (3,743), `_lifecycle.py` 21 (5,044), `__init__.py` 20 (268), `sinks/sentry.py` 20 (655), `config.py` 19 (690), `sinks/elasticsearch.py` 17 (387).
- Current sizes (`wc -l`): `_lifecycle.py` 5,912, `sinks/http.py` 3,570, `worker.py` 1,987, `decorator.py` 1,127.

### Audit closure numbers
- 2026-08-07 audit: every finding re-run by the 2026-09-02 audit; all closed but two residues, carried forward as R4 and R5 (`docs/audits/2026-08-07-pre-1.0.md:3-4`).
- 2026-09-02 audit: 41 findings; round two re-ran them and found 33 closed, 2 partial (R15, C6), 1 open (D7, carried as N7), 5 deferred or recorded (`docs/audits/2026-09-02-pre-1.0-audit.md:3-5`).
- 2026-09-04 round two: its new findings were "five defects in the surfaces that were never audited, and none of them is in the paths the last three audits hardened" (`docs/audits/2026-09-04-pre-1.0-audit-round-2.md:28-29`). It also says SPEC-053 gates "a class that has now recurred three times" (:147-148).
- ARCS.md:375-376 gives the owner's own count when the redesign was settled: "lifecycle had taken 14 specs and sink delivery 13".

### The 21 unstruck architecture §12 open items
Command: `awk 'NR>=1008 && NR<=1199 && /^- / && $0 !~ /^- ~~/' docs/architecture.md`. §12 as a whole has 58 top-level bullets, 29 struck; §13 has 36, 6 struck.

| Line | Item |
|---|---|
| 1008 | One value `ClickHouseSink`'s typed columns refuse costs its whole insert chunk (invariant 8) |
| 1020 | A sink can be released while it is still live inside the graph that replaced it |
| 1026 | Whether the predicate roster still earns its weight (SPEC-040 FR-005 AC-1) |
| 1037 | `worker.py` and `_lifecycle.py` are unsplit and unscheduled (SPEC-040 FR-005 AC-2) |
| 1048 | A forked child's `NATSSink` refuses rather than reconnects (SPEC-061 FR-001; invariant 12) |
| 1055 | A forked child's `SQLiteSink` refuses rather than reopens (invariant 12; no spec) |
| 1071 | A driver reader that dies while the status stays CONNECTED is invisible to the probe |
| 1080 | A `NATSSink` whose driver has spent its reconnect budget is closed for good (SPEC-061) |
| 1085 | A timed-out JetStream publish is not retried, and cannot safely be (SPEC-047) |
| 1094 | `GooglePubSubSink.close()`'s bound does not cover an unboundable future (SPEC-048 FR-004) |
| 1105 | A `RotatingFileSink` whose rotation keeps failing retries it once per event (SPEC-048) |
| 1113 | `MongoDBSink` bounds `pymongo`'s socket wait at 30 s; the server-selection wait stays the driver's |
| 1123 | `RabbitMQSink` bounds `pika`'s blocked-connection wait at 30 s; `socket_timeout` and others stay |
| 1136 | `ClickHouseSink` exposes `clickhouse-connect`'s 300 s `send_receive_timeout` rather than bounding it |
| 1143 | `ElasticsearchSink` reads a `2xx` object without a truthy `errors` as success, unverified |
| 1153 | `NATSSink`'s float timeouts are forwarded to the driver unchecked (SPEC-066) |
| 1164 | A `float` timeout too large for a socket constructs and fails every `emit` (SPEC-066) |
| 1176 | The after-shutdown submission line announces a loss the final drain can still deliver (SPEC-068) |
| 1183 | `flush()` and `configure(sink=…)` called on the drain thread wait on themselves (SPEC-068 spec review) |
| 1189 | A shutdown sentinel can still strand behind the final drain on the ordinary path (SPEC-068 diff review) |
| 1199 | A `shutdown()` called inside an orphan-path sink's `emit` closes that sink from inside its own emit (SPEC-068 diff review) |

Titles at 1113, 1123 and 1136 are completed from a truncated read; check the wording before quoting.

### The five "What is left" decision files
All under `docs/decisions/sink-lifecycle/`. Bullets are their opening words; I read the first line of each, not the full text.

1. `a-child-forked-inside-a-stop-signal-setter-keeps-its-mark-and-the-event-it-was-offered-and-its-shutdown-sets-that-event.md:110` — three open, all also in architecture §13:
   - :112 a setter that forks before it stores the event leaves the sink holding what it held before;
   - :114 after the child's `shutdown()`, the sink keeps the event that shutdown set;
   - :117 a grandchild forked by another thread of a child still inside a lock-holding setter.
2. `a-hand-the-worker-build-declines-for-a-close-in-flight-is-paid-when-that-close-finishes.md:170` — four open, three struck:
   - :172 between the discharge and the payment;
   - :175 the payment's moment check is not separately held;
   - :179 the stop-end payment's locked re-read of `_stops_running` is unreachable, kept as a guard;
   - :186 the stop-end hand runs the setter on the `shutdown()` caller's thread, after the graced wait;
   - struck: :192 `Worker.retarget`'s hand as the uncovered twin, :194 the orphan path's own window, :198 a late worker built during a no-worker stop's take-ahead.
3. `a-hand-while-a-stop-has-a-take-ahead-hands-the-current-signal-and-never-refreshes-it.md:80` — its one item (the late-worker case) is struck.
4. `a-sink-factory-runs-under-no-lock-and-a-build-that-calls-back-releases-its-waiters.md:51` — four open:
   - :53 a factory that joins a thread of its own that logs;
   - :57 a factory running on the drain thread that calls `flush()` waits on its own drain thread;
   - :59 a yield that waits on nothing still refuses;
   - :67 a close that lands while the factory runs is answered as before.
5. `a-stop-signal-setter-runs-under-no-lock-its-own-calls-would-wait-on-the-owner-decides-then-hands.md:127` ("What is left, and why it stands") — five open:
   - :129 the drain thread before the build's hand;
   - :135 a `shutdown()` whose close starts before a pending hand lands;
   - :141 an orphan log between a deferred hand's discharge and its payment;
   - :146 concurrent setters;
   - :148 the factory wrapper's re-read cap.

That is 16 open residual windows recorded against the stop-signal and factory mechanisms.

### Delivery-doc follow-ups
All under `docs/spec-delivery/`.

- `SPEC-067-total-inbound-trace-context.md`
  - :73 "Owed until the PR's CI is green": the `str.__str__`, `str.__getitem__` and related cases. Unstruck; not verified as settled.
  - :77 left open, outside the spec: the spoofed-scalar routes on the assembly path.
- `SPEC-069-encoding-and-driver-errors.md`
  - :71 "Owed until the PR's CI is green": FR-003's real-service criterion. Unstruck; not verified. (This one was not in the report's stale-wording list.)
  - :73 struck: partial file or stream write; resolved 2026-11-13 by 1faac952.
  - :90 open, deferred past 1.2.0: `ClickHouseSink`'s typed columns, "found by the follow-up fix's system-frame review".
- `SPEC-070-retry-classification.md`
  - :69 "Owed until the PR's CI is green": FR-002's `VARCHAR(5)` integration case and the extras leg. Unstruck; not verified.
  - :71 "SPEC-072 onward": the worker still re-sends a batch that raised for a non-retryable failure.
- `SPEC-072-verdict-contract-and-delivery-engine.md`
  - :66 struck: `emit_via_deliver` private; resolved by 458d79e6.
  - :74 a registry of live `SinkCounters` (pointed at SPEC-079).
  - :79 two drafts still prescribe `inspect.getattr_static`.
  - :83 a wrapper's stop signal is read statically.
  - :90 the at-fork handler as a whole still runs third-party hooks on hostile sink shapes.
- `SPEC-073-http-family-on-the-delivery-engine.md`
  - :135 the extent of the hook guard.
  - :147 recorded edge, `_response_limit`.
- `SPEC-080-trace-intake-by-real-type.md`
  - :57 "Owed until the PR's CI is green": the `isinstance` fallback to `__class__`. Unstruck; not verified.
- `SPEC-081-ownership-record-bounds-and-heap-marking.md:75` ("Open and accepted edges")
  - :77 a freeze before the import, on 3.12 (Known residual 4).
  - :84 a re-pin's cost (Known residual 9), accepted.
  - :90 a heap scan's copy-on-write cost is reasoned, not measured.
  - :92 nine residuals are in architecture §13.
- `SPEC-083-fork-mid-hand-stop-signal.md`
  - :16 the cross-thread view was "found by the system-frame diff review".
  - :25 the re-owed debt was "found by the execution review".
  - :31 a third recorded limit (the grandchild case above).
  - :34 a parent-process fix the spec did not name.
  - :86 an execution review beyond the usual four.
- `SPEC-062-exit-drain-budget.md:22` — open follow-ups recorded in §12 (queued items announced twice and others); SPEC-068 later took these, by its Overview.

### Other measurements not in the report
- Discovery tallies I could source: nine specs opened from architecture §12 open items (061 to 068 and 080); seven of the ten flake fixes name a specific red CI or Release run in their body.
- HTTP construction fixes between 11-03 and 11-08, in order: d6c8a435 (#284), 2e790e97 (#287), 756ee52f (#291), 253432fb (#295), 4fbfd60e (#300), c4e51fbd (#311), 458d79e6 (#312).
- 3835a30e's body: 2,222 of 8,576 tests "start with a leaked library thread alive" in one `pytest -n 12` run at c810333d.
- Scratch files I created, all under `<scratch>/scout-history/`: `classify.py`, `classified.tsv`, `bodysizes.txt`, and `bodies/<hash>.txt` (one per commit, 354 files).