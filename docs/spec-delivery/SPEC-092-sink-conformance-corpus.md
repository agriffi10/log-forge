# Completed Spec — SPEC-092: A Sink Conformance Corpus Keyed on Acknowledgement Class

## What was completed?

- **The classes as data** (FR-001), in `tests/conformance_classes.py`: ten acknowledgement classes,
  53 variants over 39 sink classes, `ABSTRACT` (`_RedisSink`), `PARAMETERS` (170 names, by AST)
  and `NEUTRAL`, with the pure gates `roster_gap`, `parameter_faults` and `coverage_faults`.
- **The backend, the doubles and the driver** (FR-002): `tests/conformance_backend.py` (the fake
  `Backend`, the fault scripts, the public-path driver and the network guard) and
  `tests/conformance_doubles.py` (a client double per sink, the driver stubs `DRIVERS`, and
  `BUILDERS`, keyed by each variant's `build` name, most made by the `_injected` or `_alone`
  factories). 256 rows, none skipped, no extra and no Docker. The driver refuses, with a
  `HarnessError` rather than a verdict, a row it cannot judge: one that reached the network
  (`connect`, `sendto`, `sendmsg` or `getaddrinfo` on a real socket), built another class, reached
  the sink in another number of worker batches than it submitted, left an action `Backend.decide`
  answered unapplied by its double (each double calls `Backend.apply` in the branch that carries
  one out; no variant and fault pair was found where an action does not apply), or read one batch
  per event and another per batch. 41 of the 53 variants are judged event by event and the 12
  whose judged sink offers no `deliver` per batch, on the worker path only.
- **One assertion** (FR-003): `fate_of`, `judge` over seven checks, the per-event reading proved
  against each run's `Delivery`, and the `ledger` check against SPEC-088's terms.
- **The register** (FR-004), in `tests/conformance_pins.py`: 12 pins over 74 rows, 7 departures
  over 12 rows, `register_faults`, and `settle`, the pure decision of a row's test: a pinned row
  raises `PinnedVerdict` only at its pin's exact verdict, a departure row passes only at its exact
  verdict, and any other row only at `()`. `tests/test_conformance.py` runs one test per row and
  hands its verdict to `settle`. `tests/test_conformance_gate.py` holds the literals, the roster,
  the parameters, the builders, a probe on each network seam (`getaddrinfo`, `connect` and `sendto`), a fixture for each other refusal above (a submission split by a `flush()` inside the span, a double that ignores its action, a mixed reading) and a test that every row's decided actions were applied, the
  standing fixtures of `judge`, `roster_gap`, `register_faults` and `lines_of`, the fixtures of
  `settle` (a pinned and a departure row each at its exact verdict, at `()`, at another verdict
  and at two wider ones, and an unlisted row at `()` and at one check), and a test that exactly the 74 pinned rows carry the strict
  `xfail`.
- **The record**: the decision *Every shipped sink is judged by one conformance corpus keyed on its
  acknowledgement class* (`docs/decisions/sink-delivery/`); `architecture.md` §12 and §13 and
  `invariants.md` as FR-004 words them; four inventory rows.
- **The runner** `docs/audits/2026-11-14-probes/sinks/spec092_build_mutants.py`, with its output
  beside it in `spec092_build_results.txt`, from which every number below is taken.

**Departures from the spec's literal text**, each recorded in its *Revision history* (2026-11-18):

- `Report`'s ledger is three fields, `ledger_moves`, `ledger_expected` and `unaccounted`. The
  expected moves are computed from the worker's own path, because `MultiSink` and `_FactorySink`
  absorb a child's failure: the judged sink is the engine child while the worker's ledger follows
  its own `emit` loop. No verdict moved.
- The finding-12 item: SPEC-089 had already struck the `FlushResult` closure, so the build struck
  the live **Closed by** sentence and wrote the decided text after it, leaving SPEC-089's struck
  text as it stood.
- The Evidence's driver-branch sentence is corrected in place: the `PostgresSink` rows that move
  are `[owned]-reply-lost` and `[autocommit]-autocommit`. The count, 21, was right.

## What changed from earlier specs?

- No file under `src/` changed. `architecture.md` §12: finding 5 gains the `MongoDBSink` and
  borrowed-connection sentences; findings 14 and 12 have their **Closed by** rewritten with
  Andrew's decisions of 2026-11-15 and 2026-11-16; finding 22 is struck and moved under
  *Resolved*. §13 gains the UDP and the raising-callable bullets. `invariants.md`'s *Guarded*
  lines of invariants 2 and 7 name `tests/test_conformance.py`.
- The corpus reuses `model_pins.PinnedVerdict` (SPEC-085) for its strict expected failures and
  `test_model_oracles.skip_offenders` for its no-skip gate.

## Verification

Every run is the runner's, at `f8549fcd` (whose `src/` and `tests/` equal this branch's head), on
macOS with Python 3.13.1 and no extra installed, each tree a `git archive` export under one
temporary root, `$T` below, with the branch's corpus first on the path.

**The verdict set at head** (FR-004): 256 rows, 86 red: 74 pinned under 12 pins, 12 departure rows
under 7 departures, 170 that pass. No row's verdict differed from the spec's, so no pin or
departure was added, changed or removed and none needed reporting to the owner.
`git diff --exit-code origin/main -- src` exits 0 (FR-001).

**Runtime** (FR-002), `pytest tests/test_conformance.py -n 0`, three runs, each exit 0 with 182
passed and 74 expected failures: 1.38, 1.32 and 1.53 s (1.82, 1.80 and 1.96 s wall) at one-minute
loads of 17.21, 17.21 and 16.79. Every verdict and moved-row list below reads as the runner's first
run at `877f8b32`, before the two review rounds' fixes; only the runtime differs, measured at a
higher load.

**The suite** at `f611e32b`, Python 3.13.1 and 3.12.12: `test_conformance_gate.py` collects 83
tests, and with `test_conformance.py` gives 265 passed and 74 expected failures under `-n 0`; the
whole suite, at the default `-n 12`, gives 12,243 passed, 39 skipped and 87 expected failures, exit
0, on both.

**The driver branch** (FR-002): with `driver_classes` answering `()`, 21 rows move, all to
`lost-but-held`: `taken-then-raised` and `reply-lost` of `SQSSink[standard]`, `SQSSink[fifo]`
(from green), `SNSSink`, `KinesisSink`, `FirehoseSink`, `ClickHouseSink`, `AzureEventHubsSink`,
`RedisStreamsSink` and `RedisListSink`, and `PostgresSink[owned]-reply-lost`,
`PostgresSink[autocommit]-autocommit` and `RabbitMQSink[owned]-taken-then-raised`; all but the
two FIFO rows from `duplicate`.

**Each check removed from `judge`** (FR-003), in process at head; no row moved was green before:

- `partition`, 4: `never-acknowledged` of `KafkaSink` and `GooglePubSubSink` to `silent-loss`,
  their `report-indefinite` to `lost-but-held`.
- `silent-loss`, 21, to green unless named: `no-verdict-shape` of `SQSSink[standard]`,
  `SQSSink[fifo]`, `SNSSink`, `KinesisSink`, `FirehoseSink`, `ElasticsearchSink` and
  `OpenSearchSink`; `peer-closed` of `SyslogSink` and `LogstashSink` `tcp` and `udp` and both
  `RabbitMQSink` variants; `LoggingSink` `refused-once` and `refused-always`; `NullSink-clean`;
  `NATSSink[core-injected]-never-acknowledged`; `SentrySink[sdk]` `never-acknowledged` and
  `report-indefinite`; and `never-acknowledged` of `KafkaSink` and `GooglePubSubSink`, to
  `partition`.
- `duplicate`, 55: the 50 `reply-resent` rows, the three `callable-raises-after-keeping` rows and
  `PostgresSink[autocommit]-autocommit` to green, and `SQLiteSink[autocommit]-autocommit` to
  `lost-but-held`.
- `lost-but-held`, 11: the four `reply-counted-lost` rows, the two `jetstream-ack-lost` rows and
  the two `sqlite-failed-before-raise` rows to green; `report-indefinite` of `KafkaSink` and
  `GooglePubSubSink` to `partition`; `SQLiteSink[autocommit]-autocommit` to `duplicate`.
- `unannounced`, `ambiguous-when-knowable` and `ledger`: none, so each was run against a planted
  mutant below.

The four named rows read as FR-003 requires: `KafkaSink-never-acknowledged` at `silent-loss`, and
`SyslogSink[tcp]-peer-closed`, `HTTPSink-reply-lost` and `MongoDBSink-reply-lost` green.

**Source mutants** (FR-003), each in an export of head; every move adds the named check:

- `_diag.lost` returning at once (`unannounced`), 89 rows, 75 of them green before, among them
  `SQSSink[standard]-refused-always`. Green before: the `refused-always` row of every variant
  that runs it except `LoggingSink` (48); every `partial` row (9); `refused-once` of
  `KafkaSink`, `GooglePubSubSink`, the four `NATSSink` variants, `RabbitMQSink[borrowed]`,
  `SentrySink[sdk]`, `StdoutSink` and `StderrSink` (10); `taken-then-raised` of
  `PostgresSink[borrowed]`, `StdoutSink` and `StderrSink`; `no-verdict-shape` of `HoneycombSink`
  and `MongoDBSink`; `caller-code-raises` of `FilteringSink[over-callback]` and
  `TransformSink[over-callback]`; `NATSSink[core]-never-acknowledged`. Red before (14):
  `never-acknowledged` and `report-indefinite` of `KafkaSink` and `GooglePubSubSink`;
  `taken-then-raised` of `MongoDBSink`, `RabbitMQSink[borrowed]` and `SQLiteSink`; `reply-lost` of
  `MongoDBSink`, `PostgresSink[borrowed]` and both JetStream variants; `SQLiteSink-refused-once`;
  `RabbitMQSink[borrowed]-peer-closed`; `SQLiteSink[autocommit]-autocommit`.
- `_RedisSink._round` answering `UNKNOWN`, 8 rows: `refused-once` and `refused-always` of both
  Redis sinks from green to `ambiguous-when-knowable`, and their `taken-then-raised` and
  `reply-lost` from `duplicate` to green, the trade that check exists to refuse.
- The settled run's `sink_lost` increment in `Worker._emit` removed (`ledger`), 26 rows, 22 green
  before, among them `SQSSink[standard]-partial`. Green before: every `partial` row (9);
  `no-verdict-shape` of `HoneycombSink` and `MongoDBSink`; `refused-once` of the four `NATSSink`
  variants, `StdoutSink` and `StderrSink`; `refused-always` and `taken-then-raised` of `StdoutSink`
  and `StderrSink`; `NATSSink[core]-never-acknowledged`. Red before: `MongoDBSink-taken-then-raised`,
  `reply-lost` of both JetStream variants, `RabbitMQSink[borrowed]-peer-closed`.

**Harness mutants** (FR-003), in process at head:

- `fate_of` reading `REJECTED` as landed, 16 rows, 13 green before, each now including
  `partition`, which only the comparison with `Delivery` gives: `partial` of `SQSSink[standard]`,
  `SQSSink[fifo]`, `SNSSink`, `MongoDBSink`, `ElasticsearchSink`, `OpenSearchSink` and
  `HoneycombSink`, `refused-once` of the four `NATSSink` variants and `refused-always` of
  `StdoutSink` and `StderrSink`, each to `partition`, `silent-loss`, `ledger`; and
  `MongoDBSink-taken-then-raised` and `reply-lost` of both JetStream variants, from
  `lost-but-held` to `partition`, `ledger`.
- `duplicate` firing at one copy, 143 rows, 119 green before, each now including `duplicate`:
  every `clean` row but `NullSink`'s departure (50); every `refused-once` row but
  `RabbitMQSink[borrowed]`'s (48, `SQLiteSink`'s and `LoggingSink`'s red before); every
  `partial`, `no-verdict-shape`, `peer-closed` and `caller-code-raises` row (9, 9, 6 and 4);
  `report-indefinite` of `KafkaSink` and `GooglePubSubSink`; `reply-lost` of `SQSSink[fifo]`,
  `MongoDBSink`, `PostgresSink[borrowed]` and both JetStream variants; `taken-then-raised` of
  `FileSink`, `RotatingFileSink`, `LoggingSink`, `PostgresSink[owned]`, `SQSSink[fifo]`,
  `StdoutSink`, `StderrSink`, `MongoDBSink`, `RabbitMQSink[borrowed]` and `SQLiteSink`. So every
  green `clean` row reads `duplicate`, as the criterion asks. **143 here against the Evidence's
  148:** the prototype's mutant changed the per-batch count of copies held more than once, which
  also moved the "none held" count that per-batch `silent-loss` and `lost-but-held` read; the
  build changes only `duplicate`'s condition, as FR-003 words it. Both give 119 rows green before.

**Older trees** (FR-005), ledger off, against an export of `6bd0b66e` at 86 red. Head with the
ledger off moves 0 rows against it, so no dependency moved a row and the comparison is direct.

| Tree | `log_foundry.__file__` | Rows moved, all green before |
|---|---|---|
| `6bd0b66e` | `$T/t6bd0b66e/src/log_foundry/__init__.py` | the baseline |
| head | `$T/head/src/log_foundry/__init__.py` | none |
| `a2cd4b36^` | `$T/a2cd4b36_/src/log_foundry/__init__.py` | `StdoutSink` and `StderrSink` `taken-then-raised` to `duplicate` |
| `9b54b370^` | `$T/9b54b370_/src/log_foundry/__init__.py` | those two, and `FileSink` and `RotatingFileSink` `refused-once` to `lost-but-held` |
| `a0f44b5f` | `$T/a0f44b5f/src/log_foundry/__init__.py` | the same four |

Every source mutant's `log_foundry.__file__` was under its own export the same way
(`$T/diag-lost-silent/`, `$T/redis-round-unknown/`, `$T/worker-sink-lost-dropped/`).

**Owed until the PR's CI:** the corpus's verdicts in `integration.yml`'s unit-suite step with
every extra installed, triggered by `workflow_dispatch` on the PR's branch (FR-002), and the
ordinary `CI / test` job on Python 3.12 and 3.13.
