---
type: Spec
id: SPEC-106
title: "One Home for the Round Settlement Shapes"
description: "Gives the three settlements the shipped sinks write by hand in their rounds (a batch call that raised, a reply nobody could adjudicate, and an entry still failing) one function each in the rounds module, for the library's maintainers, with no line, counter, verdict or resend mark a caller can observe changed."
spec_status: Draft
depends_on: [SPEC-073, SPEC-074, SPEC-093, SPEC-095]
arc: one-home-per-shape
arc_order: 1
---

## Overview

When a sink's batch call fails, the sink has to say three things about the events it carried:
what becomes of each one, whether a resend might put a second copy at the destination, and what
line to write if they are lost. Nine places in the library say this by hand for a call that
raised, each in a slightly different order and spelling, and two smaller settlements are written
twice and four times over. Nothing is wrong with what they say today. The cost is to whoever
changes the rule next, who has nine places to change and no test that says they changed them all.
This spec gives each of the three settlements one function, moves every in-scope site onto it,
and adds a roster that refuses the hand-written form if it comes back. It is a pure refactor:
nothing a caller can see on stderr, in a counter, in a verdict or in a resend count changes, and
a test whose expected values were recorded before any code moved proves it.

## Scope

### In Scope

- `settle_raised` in `src/log_foundry/sinks/_rounds.py`, and the nine sites that settle a raised
  batch call calling it (FR-001).
- `lose_unadjudicated` in the same module, and the four sites that count and announce a reply
  nobody could adjudicate calling it: three in `_rounds.py` and the body of
  `HTTPSink._unadjudicated` (FR-002).
- `still_failing_line` beside `raised_line`, and the two sites that write that line calling it
  (FR-003).
- A test that calls the three helpers directly, for the cases no shipped sink reaches (FR-001 to
  FR-003).
- A roster with a fixture corpus that refuses either hand-written shape outside its home (FR-004).
- A differential test, recorded from `35b2f55d` before any source file changes, and the existing
  suite passing with one named roster restated (FR-005).

### Out of Scope

- **Any change a caller can observe.** No stderr line's text or order, no counter, no verdict, no
  `resent` count and no public name changes. The one spelling that does change is the grouping
  key inside `Lost`, which is a dictionary key and is never written anywhere.
- **`HTTPSink._unadjudicated` as a method.** It stays on `HTTPSink` with its name, its signature
  and its return value, because `ElasticsearchSink`, `HoneycombSink`, `OTLPSink` and `http.py`
  itself call it. Only the three statements of its body move.
- **`HTTPSink._exchange`**, examined and excluded. It makes the same decision for a request that
  raised: it asks `is_retryable`, then answers `RETRYABLE` with `When.LAST` or `REJECTED` with
  `When.ALWAYS`, naming the type. But it adds to no `Lost`: it counts the request on `failed`
  and registers its line through `_give_up`, hands the mark to its caller in an `_Answer`, and
  has a third branch that hands a refused exception back uncounted. Neither roster rule sees it.
- **The rest of the HTTP family's counting**: the general "count N and announce one line for N"
  pairing on `Deferral` and `_Drops` (the HTTP family's drops, the wrappers, `sinks/_encode.py`
  and `AzureEventHubsSink`'s oversize drop), and `_announce_unlined`, which chooses `When.LAST`,
  else `When.SETTLED`, and counts nothing.
- **The file and stream sinks' lose tail**, `_Writes.lose` (`sinks/file.py`) and
  `StdoutSink._lose` (`sinks/stdout.py`). Each writes FR-001's shape beside a second group for a
  torn or handed line that shares its `Lost` and its `whole`. The roster exempts both by name.
- **A refused hand-off** in `KafkaSink._round` and `GooglePubSubSink._round`. It has no exception
  object left to classify, is always `RETRYABLE` and `When.LAST`, and makes no mark, so it is not
  this shape. Its `key=kind` keeps its spelling.
- **`MongoDBSink._settle_bulk` and `NATSSink._register`.** Each chooses `When.LAST` or
  `When.ALWAYS` from `resends_unknown` and announces, and neither counts
  `dropped_unadjudicated`, so neither is FR-002's shape.
- **The `failed_events` whole-batch count** that `_RedisSink._round` and `PostgresSink._round`
  make before they build their `Lost`. Both keep it, and keep building and registering their own
  `Lost`, as `MongoDBSink._round` does.
- `SNSSink`'s per-event oversize count, and every other `NATSSink` settlement.
- `encode_round` and its `encode` argument, which `tests/test_encode.py` resolves from the
  calling class. No call of it moves.
- The `except` handler in each sink that asks `is_retryable(self._retryable_error, err)`. It
  stays in the class, as `tests/test_classify.py` requires; the helper is called after it.
- `SinkCounters`, `RunContext`, `Lost` and `mark_may_have_landed`: none changes.

---

## Functional Requirements

### FR-001: One function settles a batch call that raised

#### Description:

At `35b2f55d` nine sites settle a raised call with the same three steps: each carried position
becomes `RETRYABLE` or `REJECTED` as the sink's predicate answered; the positions are marked as
possibly landed; and the carried events join the round's `Lost` under `When.LAST` for a retryable
raise and `When.ALWAYS` otherwise, with the line `raised_line` builds. `settle_raised` does those
three steps and nothing else. It builds no `Lost`, calls no `register()` and counts nothing; a
site that does any of those keeps doing it around the call.

| Site at `35b2f55d` | `unit` | `name` | Mark | Also passes | Other keys in its `Lost` |
|---|---|---|---|---|---|
| `_rounds.settle_keyed`, 442 to 453 | `message` | its `name` | `reached=True` | | `held` (`_fifo.py:662`), `entry` |
| `_rounds.settle_positional`, 572 to 583 | `record` | its `name` | `reached=True` | | `entry` |
| `AzureEventHubsSink._settle` | `event` | `AzureEventHubsSink, one batch` | `reached=True` | | none |
| `ClickHouseSink._round` | `row` | `ClickHouseSink` | `reached=True` | | none |
| `_RedisSink._round` | `event` | `_diag.type_name(self)` | `reached=True` | `whole` | none |
| `PostgresSink._round` | `event` | `PostgresSink` | `reached=called.reached` | `suffix`, `whole` | none |
| `RabbitMQSink._round` | `message` | `RabbitMQSink` | `reached=called.reached` | | none |
| `SocketTransport._round` | `message` | `SocketTransport` | `reached=called.reached` | `kind`, with its errno | none |
| `MongoDBSink._round` | `document` | `MongoDBSink` | `mark=False` | | none |

Decided, with the reason for each:

- **`MongoDBSink` passes `mark=False`.** It never marks, and that is settled: *A resend after a
  send that may have landed is counted, and its key is the event's `log_id`*
  (`docs/decisions/sink-delivery/`) says "`MongoDBSink`, the file sinks, the stream sinks and
  `NATSSink` never mark". `mark=False` says that; `reached=False` would make the same call and
  claim something else, that the insert never left.
- **`reached` defaults to `None`,** the default of `mark_may_have_landed` and of `Called.reached`,
  which asks the exception's class. Every site passes it today and keeps passing what it passed,
  so a site added later that says nothing gets the answer it would get from
  `mark_may_have_landed` itself, and does not mark every raise by omission. The five explicit
  `True`s are held by FR-005's `ConnectionRefusedError` row: that class says the request never
  left, so a site that lost its `reached=True` would stop marking it.
- **The grouping key is `f"raised {kind}{suffix}"` at every site.** A `Lost` groups by
  `(when, key, attempts)`. The two sites inside `_rounds` already spell it this way, which is what
  keeps an exception class named `entry` or `held` apart from the `entry` and `held` keys that
  share their `Lost`. At the other seven sites the raised call is the only thing that adds to its
  `Lost` (the table's last column), and putting one prefix on every key maps different keys to
  different keys, so no two groups merge, none splits, the first-seen order is the same and
  `whole` attaches to the same group. The key is never written anywhere a caller can read; what a
  caller can read is whether two error types in one round get two lines, and FR-005 holds that.
- **Both copies inside `_rounds` go through `raised_line`** by calling `settle_raised`. Their
  inline lambdas format the same text `raised_line` formats with an empty suffix.
- **The order of the three steps inside the helper is free.** They write to three separate
  stores, the round's `fates`, the run context's marks and the `Lost`, and none reads another's;
  `_diag.type_name` raises nothing and runs no caller code. A site still computes its own
  arguments, such as Postgres's suffix, before the call.

The criteria below that begin "Called directly" are held by `tests/test_settlement_helpers.py`,
which calls the helpers with a `Lost` and a run of its own. No shipped sink reaches those cases.

#### Acceptance Criteria:

- [ ] `settle_raised` exists in `_rounds.py` with the signature in *API / Interface Contract*,
      and each of the nine sites in the table calls it with the arguments its row gives.
- [ ] Called directly, a retryable error leaves every given position `RETRYABLE` and its line is
      written only when that round is the run's last; any other error leaves every position
      `REJECTED`, marks nothing, and its line is written however the run ends.
- [ ] Called directly with a retryable error, it marks the positions when `reached` is `True`,
      none when it is `False`, and as the error's class answers when it is `None`; with
      `mark=False` it marks none, whatever `reached` holds.
- [ ] Called directly, two errors of different types added to one `Lost` are written as two
      lines and two of one type as one line with their summed count.
- [ ] Called directly, the line reads `<name>, N attempt(s), <kind><suffix>`, where `<kind>` is
      the error's type name unless `kind` is given, and a `whole` is added to the count when the
      run fails on that round.
- [ ] Called directly with empty `positions` and `carried`, it changes no fate, marks nothing and
      writes no line.
- [ ] `settle_raised` builds no `Lost`, calls no `register()` and no `RunContext.count`; the
      Redis and Postgres sites still count `failed_events` with `When.FAILED`, and the Redis,
      Postgres and MongoDB sites still build and register their own `Lost`.
- [ ] No in-scope site still writes `When.LAST if … else When.ALWAYS`, calls
      `mark_may_have_landed` or calls `raised_line` itself, and no import is left unused.
- [ ] This FR keeps invariants 2, 7 and 11: each lost event is counted and announced once, a
      resend after a possibly landed send is counted, and the line names the exception's type.

### FR-002: One function counts and announces a reply nobody could adjudicate

#### Description:

Four places at `35b2f55d` write the same three statements: choose `When.LAST` when the run
resends an `UNKNOWN` event and `When.ALWAYS` otherwise, count the events on
`dropped_unadjudicated` under that rule, and announce one line under it. Three are in
`_rounds.py` (lines 458 to 468 and 486 to 496 in `settle_keyed`, and 605 to 615 in
`settle_positional`) and the fourth is the body of `HTTPSink._unadjudicated`
(`http.py`, 2697 to 2704). `lose_unadjudicated` writes them once, and all four call it.

Decided, with the reason for each:

- **`HTTPSink._unadjudicated` is a site, not an exemption.** It is the same
  three statements on the run context, so an identical copy kept behind an exemption row would be
  the opposite of what this arc is for. It is not the deferral-ledger pairing, which stays out of
  scope. The method keeps its name, signature and return value; `http.py` imports
  `lose_unadjudicated`; and the function is public in `_rounds` because another module calls it.
- **The import makes no cycle.** `_rounds.py` imports `_diag`, `_batch`, `_classify`, `_engine`
  and `verdict`, and nothing those import in turn, the package's top-level modules and
  `_refusal` included, imports `http`. Measured at `35b2f55d`: importing
  `log_foundry.sinks._rounds` in a fresh interpreter leaves `log_foundry.sinks.http` unloaded.
  No test reads `http.py`'s import list or patches `run_context`, and
  `tests/test_http_round_guards.py`, which overrides and calls `_unadjudicated`, reaches it as
  a method, which stays.
- **The function takes the whole line as a zero-argument callable, not its detail.** Each site
  keeps its own `lambda: _diag.lost(...)`. Two things depend on that. First, the line's text is
  formatted when the engine calls the line, after the run's counts are committed
  (`_engine._commit`), not when the round registers it; formatting earlier would move a fault in
  formatting from there into the sink's round, where the engine reads it as a round that raised
  and counts differently. Second, `tests/test_diag.py` reads the arguments of every
  `_diag.<writer>(...)` call for a `repr`, and a detail built outside such a call would leave
  that roster's view with the suite still green. So the rule of this refactor is: **no line text
  is formatted earlier than it is at `35b2f55d`, and every detail stays inside a `_diag` call.**
- **The counter's name is the literal `"dropped_unadjudicated"` inside the function,** never a
  parameter. `tests/test_deferral.py` refuses a run-context count whose counter is not a string
  literal, because it cannot tell which counter it is.
- **One existing roster is restated, in `tests/test_http_rounds.py`.** Its
  `_unadjudicated_writers` names every function in the HTTP family's modules that counts or
  stores `dropped_unadjudicated`, and `test_only_unadjudicated_counts_dropped_unadjudicated`
  holds the list to `HTTPSink.__init__` and `HTTPSink._unadjudicated`. It recognises a count by a
  `count` call naming the literal, so once the method calls `lose_unadjudicated` it drops out of
  the derived list and the test goes red. The roster is restated to read a call of
  `lose_unadjudicated` as a count too. Its expected list stays the same two names, and a planted
  `lose_unadjudicated(...)` call joins the two cases
  `test_the_unadjudicated_roster_names_a_planted_writer` already plants.

The criteria below that begin "Called directly" are held by `tests/test_settlement_helpers.py`.

#### Acceptance Criteria:

- [ ] `lose_unadjudicated(owner, count, line)` exists in `_rounds.py` and is in its `__all__`,
      and the four places named above call it.
- [ ] A `count` call naming `"dropped_unadjudicated"` appears under `src/log_foundry/sinks/` only
      inside `lose_unadjudicated`, and neither `_rounds.py` nor `HTTPSink._unadjudicated` calls
      `resends_unknown` anywhere else.
- [ ] Called directly in a run that resends an `UNKNOWN`, the count and the line are committed
      only when that round is the run's last; in any other run both are committed however the
      run ends.
- [ ] Called directly, a spy on `RunContext.resends_unknown` sees one call.
- [ ] Called directly with a count of zero, it moves no counter and the line it was given is
      still written.
- [ ] The function's body calls no `_diag` writer and builds no string; each of the four sites
      passes a `lambda` whose body is its `_diag.lost(...)` call, with the text it has at
      `35b2f55d`.
- [ ] `HTTPSink._unadjudicated` keeps its name, its parameters `(items, detail, noun="event")`
      and its return of one `UNKNOWN` per item, and no caller of it changes.
- [ ] In `tests/test_http_rounds.py`, `_unadjudicated_writers` reads both
      `lose_unadjudicated(...)` and `_rounds.lose_unadjudicated(...)` as a count, and the
      expected list is still `http.HTTPSink.__init__` and `http.HTTPSink._unadjudicated`.
- [ ] A `lose_unadjudicated(...)` call planted in `HoneycombSink._outcomes` is named by that
      roster.
- [ ] `tests/test_deferral.py` and `tests/test_diag.py` pass unedited.
- [ ] This FR keeps invariants 2, 7 and 11: an unadjudicated reply's events are counted once per
      run and announced as events that may have landed, a `2xx` nobody could read still settles
      its chunk without failing the run, and each detail stays inside a `_diag` call.

### FR-003: One function builds the line for an entry still failing

#### Description:

`settle_keyed` and `settle_positional` each end by adding the entries the reply left retryable
under `key="entry"` and `When.LAST`, with a line that differs only in its unit word:
`f"{name}, still failing after {n} attempt(s); abandoned"`. `still_failing_line(unit, name)`
returns that line, as `raised_line` returns its own, formatting the text only when the line is
called. The `lost.add` call, its key and its rule stay at the two sites. The criterion that
begins "Called directly" is held by `tests/test_settlement_helpers.py`.

#### Acceptance Criteria:

- [ ] `still_failing_line(unit, name)` exists beside `raised_line` and is in `_rounds.__all__`.
- [ ] Called directly, `still_failing_line("message", "X")(3, 2)` writes the one line
      `log-foundry: lost 2 message(s); X, still failing after 3 attempt(s); abandoned`.
- [ ] `settle_keyed` passes `still_failing_line("message", name)` and `settle_positional`
      `still_failing_line("record", name)`, each still with `key="entry"` and `when=When.LAST`.
- [ ] The words `still failing after` appear in executable code in `src/log_foundry/sinks/` only
      inside `still_failing_line`.
- [ ] This FR keeps invariants 2 and 11: an entry abandoned still failing is announced once, with
      the number of calls that carried it.

### FR-004: A roster refuses either shape written by hand outside its home

#### Description:

`tests/test_settlement_shape_roster.py` reads the AST of every module under
`src/log_foundry/sinks/`, as `tests/test_chunk_helper_roster.py` does, and names each function,
by module and qualified name, that holds one of two shapes:

- **A raised call settled by hand:** the function holds a conditional expression whose two
  results are `When.LAST` and `When.ALWAYS`, in either order, and also holds a call of
  `raised_line` or an attribute call named `add` that passes `line=`. The second form is the one
  the two copies in `_rounds` use at `35b2f55d`, an inline lambda that bypasses `raised_line`.
- **An unadjudicated reply settled by hand:** the function holds a call of an attribute named
  `resends_unknown` and a call of an attribute named `count` that names the literal
  `"dropped_unadjudicated"`.

Each shape has one home, `settle_raised` and `lose_unadjudicated` in `_rounds.py`. The first
shape has a table of exemptions that gives each row's reason: `_Writes.lose` in `file.py` and
`StdoutSink._lose` in `stdout.py`. The second has none. The functions the roster finds are
compared exactly with the homes and the exemptions, so a home that loses its shape and an
exemption that no longer applies are both findings, and a walker that matches nothing cannot
pass. Measured over `35b2f55d`, the first rule finds eleven functions (the nine sites of FR-001
and the two exempt ones), none of them in the inverted order, and the second finds three
(`settle_keyed`, `settle_positional` and `HTTPSink._unadjudicated`); `KafkaSink._round` and
`GooglePubSubSink._round` call `raised_line` with an unconditional `When.LAST` and are found by
neither.

**What the roster does not see.** It reads one spelling of each shape. A function that chooses
between the two rules with an `if`/`else` statement, or that settles a raise with no `Lost` as
`HTTPSink._exchange` does, is not found. The roster guards against the hand-written form coming
back as it was written; that every site still behaves as it did is held by FR-005, not by this
roster.

#### Acceptance Criteria:

- [ ] Over the finished tree the roster reports no finding: the first shape is held by
      `settle_raised` and its two exemptions and by nothing else, and the second by
      `lose_unadjudicated` alone.
- [ ] A fixture that plants the first shape in a sink is refused, with a reason naming the
      module, the function and `settle_raised`, in each of four forms: with `raised_line`; with
      an inline lambda; with `when` bound to a name before the `add`; and in the inverted order.
- [ ] A fixture that plants the second shape in a sink is refused, with a reason naming the
      module, the function and `lose_unadjudicated`.
- [ ] A fixture with an exemption row whose function no longer holds the shape, and one whose
      home function no longer holds it, are each refused with a reason of their own.
- [ ] These fixtures stay silent: an unconditional `when=When.LAST` with `raised_line`; a `when`
      chosen from `resends_unknown` with an announcement and no `dropped_unadjudicated` count;
      and a conditional choosing `When.LAST`, else `When.SETTLED`.
- [ ] Each of the roster's checks, removed in turn, turns at least one corpus case red.
- [ ] This FR serves no invariant: it is a structural guard on where code sits, and the
      behaviour it protects is held by FR-005 and the tests of each sink.

### FR-005: Nothing a caller can observe changes, and a recorded test proves it

#### Description:

The existing suite already asserts the lines, counters, verdicts and resend marks of every sink
this spec touches, and it must pass without a single expectation edited. That alone would not
show the sites still agree with what they did before, because a test edited beside the code
agrees with the code. So `tests/test_settlement_differential.py` is written first, against the
unmodified tree: it drives each site through real sinks and compares what happened with values
written into the test as literals from that run. It imports nothing from `_rounds`, so it cannot
compute an expectation from the helper it checks; `tests/test_settlement_helpers.py` is the only
new file that does.

**How the rows are driven.** `tests/sink_rigs.py` supplies the sinks and their doubles and is not
edited. Every rig builder that takes a retry bound passes `max_retries=0`, so a two-round row
runs the sink through `_engine._run(..., sink_retries=1)`, the pattern
`tests/test_resend_marks.py::run_once` uses. The AWS doubles always answer success, so the reply
rows (unread, named in neither list, unadjudicated, still failing) use scripted clients the new
file defines itself. The HTTP rows use the scripted opener in `tests/test_http_rounds.py`.

The rows, each recorded at `35b2f55d`:

| Row | Driven at |
|---|---|
| Every call of a two-round run raises a retryable error | each of the nine sites; SQS and SNS for `settle_keyed`, Kinesis and Firehose for `settle_positional` |
| The call raises an error the sink's predicate refuses | the same |
| Every call of a two-round run raises a builtin `ConnectionRefusedError` | the five sites whose row in FR-001 says `reached=True` |
| A retryable raise the site reports as not having reached the destination | Postgres, RabbitMQ and the socket transport, one row each |
| Two exception types in the calls of one round | `ClickHouseSink` with one row per chunk, and one keyed AWS sink |
| The borrowed-connection suffix | Postgres |
| The errno in the line | the socket transport |
| A batch holding an unencodable event, in a run that fails | Redis and Postgres |
| A retryable raise resent with no mark | MongoDB |
| An unread keyed reply, and a keyed entry named in neither list | SQS and SNS |
| An unadjudicated positional response | Kinesis and Firehose |
| A `2xx` the sink cannot read | `ElasticsearchSink`, `HoneycombSink` and `OTLPSink` |
| Each of the three rows above, in a run whose sink declares `log_foundry_idempotent = True` | the same sinks |
| An entry still failing when the rounds run out | SQS, SNS, Kinesis and Firehose |

Two of those rows exist because nothing else holds what they hold; the spec review built the
refactor and ran both mutants. With `reached=True` deleted from one call at a time, the whole
suite stays green at four of the five sites (`tests/test_sink_post_close.py` catches
ClickHouse's), and the
`ConnectionRefusedError` row is what tells them apart: as shipped the events are counted in
`resent`, and without the argument they are not. With the key reduced to `"raised"`, the suite
stays green because every existing run raises one error type; the two-type row writes two lines
as shipped and one line for the mutant.

What the build must leave as it found it, confirmed at `35b2f55d`:

- No test imports, calls or patches `settle_keyed`, `settle_positional`, `raised_line` or
  `mark_may_have_landed`, and none asserts `_rounds.__all__`. The one roster that moves with this
  change is FR-002's, in `tests/test_http_rounds.py`, and it is the only existing test file this
  spec edits.
- `tests/test_resend_marks.py` holds each sink's marking by behaviour, over a roster derived from
  the tree, and is the test for the mark.
- `tests/test_classify.py` requires each class that defines `deliver` to reach an `except`
  handler asking `is_retryable(self._retryable_error, err)` through its own methods. The handler
  is in each sink's own `_round` or send method and is not moved.
- `tests/conformance_pins.py` quotes one sentence each from `_rounds.py`, `eventhubs.py`,
  `clickhouse.py` and `redis.py`, all in docstrings of functions this spec edits
  (`settle_keyed`, `AzureEventHubsSink._settle`, `ClickHouseSink._round`, `_RedisSink._round`),
  and `tests/test_retry_safe_drop_counts_prose.py` pins a phrase in `SocketTransport._round`'s.
  A docstring changes only where the code it describes moved, and each quoted sentence stays in
  its file word for word.
- *A 2xx this sink cannot read is counted, and the `_bulk` reading is argued, never inherited*
  (`docs/decisions/sink-delivery/`) says the chunk is counted "through
  `HTTPSink._unadjudicated`". It still is: the method stays and is the only caller of
  `lose_unadjudicated` in the HTTP family.

#### Acceptance Criteria:

- [ ] `tests/test_settlement_differential.py`, copied onto `35b2f55d` with no other change,
      passes, and it passes unchanged on the finished branch.
- [ ] It has every row of the table in the Description, at every site or sink the row names.
- [ ] Every row compares, with literals, the stderr lines in order, the run's `Delivery`, the
      sink's `SinkCounters`, its `losses()`, and the sink's own `dropped_unadjudicated` where it
      has one.
- [ ] The file imports no name from `log_foundry.sinks._rounds`, and `tests/sink_rigs.py` is
      not edited.
- [ ] Each of these mutants of the finished tree turns it red and is then reverted:
      `settle_raised` makes no mark; it chooses `When.ALWAYS` for a retryable raise; its key
      drops `kind`; `lose_unadjudicated` skips its count.
- [ ] So does each of these, at a site: one `reached=True` call loses the argument, tried at
      each of the five; MongoDB's call loses `mark=False`; Postgres's call loses `suffix`.
- [ ] No file under `tests/` that exists at `35b2f55d` is modified, with one exception:
      `tests/test_http_rounds.py`, where only `_unadjudicated_writers` and the planted cases of
      `test_the_unadjudicated_roster_names_a_planted_writer` change, as FR-002 says.
- [ ] The repo's six gates are green.
- [ ] `_rounds.__all__` gains `settle_raised`, `lose_unadjudicated` and `still_failing_line` and
      loses nothing, and `log_foundry.__all__` and `log_foundry.sinks.__all__` are unchanged.
- [ ] This FR keeps invariants 2, 7 and 11 on every site it drives: the ledger's sink counts,
      the `resent` count and each line's text are what they were at `35b2f55d`.

---

## API / Interface Contract

```python
# src/log_foundry/sinks/_rounds.py

def settle_raised(
    lost: Lost,
    positions: list[int],
    carried: list[dict[str, object]],
    fates: list[Outcome],
    error: Exception,
    retryable: bool,
    *,
    unit: str,
    name: str,
    reached: bool | None = None,
    mark: bool = True,
    kind: str | None = None,
    suffix: str = "",
    whole: int = 0,
) -> None: ...

def lose_unadjudicated(owner: object, count: int, line: Callable[[], None]) -> None: ...

def still_failing_line(unit: str, name: str) -> Callable[[int, int], None]: ...

# Example calls, one per kind of site.

# _rounds.settle_keyed
settle_raised(lost, positions, carried, fates, error, retryable,
              unit="message", name=name, reached=True)

# PostgresSink._round, after its own failed_events count and Lost(self)
settle_raised(lost, positions, carried, fates, called.error, called.retryable,
              unit="event", name="PostgresSink", reached=called.reached,
              suffix=suffix, whole=whole)
lost.register()

# SocketTransport._round
settle_raised(lost, [position], [batch[position]], fates, called.error, called.retryable,
              unit="message", name="SocketTransport", reached=called.reached, kind=kind)

# MongoDBSink._round
settle_raised(lost, positions, carried, fates, err, retryable,
              unit="document", name="MongoDBSink", mark=False)

# _rounds.settle_keyed, the unread reply
lose_unadjudicated(owner, unread, lambda: _diag.lost("message", unread, f"..."))

# HTTPSink._unadjudicated, whose signature and return are unchanged
sent = len(items)
name = _diag.type_name(self)
lose_unadjudicated(
    self, sent,
    lambda: _diag.lost(noun, sent, f"{name} {detail}; they may have landed; not retried"),
)
return (Outcome.UNKNOWN,) * sent
```

Each new function carries a Google-style docstring with `Args:`, `Returns:` and `Raises:`, no
inline comment, and passes `mypy --strict`. `__all__` in `_rounds.py` gains `settle_raised`,
`lose_unadjudicated` and `still_failing_line`.

## File & Folder Structure

```
src/log_foundry/sinks/
├── _rounds.py        settle_raised, lose_unadjudicated, still_failing_line; seven call sites moved
├── http.py           HTTPSink._unadjudicated's body moved; imports lose_unadjudicated
├── eventhubs.py      one site moved
├── clickhouse.py     one site moved
├── redis.py          one site moved
├── postgres.py       one site moved
├── rabbitmq.py       one site moved
├── _socket.py        one site moved
└── mongodb.py        one site moved
tests/
├── test_settlement_differential.py    new; imports nothing from _rounds
├── test_settlement_helpers.py         new; direct calls of the three helpers
├── test_settlement_shape_roster.py    new
└── test_http_rounds.py                _unadjudicated_writers restated; the one existing file edited
```

## Implementation Phases

### Phase 1: The recording, the helpers and the copies inside `_rounds` (FR-005, FR-001, FR-002, FR-003)

- Write `tests/test_settlement_differential.py` against the unmodified tree and commit it alone,
  before any file under `src/` changes, so its literals are a recording.
- Add `settle_raised`, `lose_unadjudicated` and `still_failing_line` to `_rounds.py`, with
  `tests/test_settlement_helpers.py` calling each directly.
- Move the seven call sites inside `settle_keyed` and `settle_positional` onto them: two raised
  calls, three unadjudicated replies and two still-failing lines.

### Phase 2: The sink sites (FR-001, FR-002)

- Move the sites in `eventhubs.py`, `clickhouse.py`, `redis.py`, `postgres.py`, `rabbitmq.py`,
  `_socket.py` and `mongodb.py` onto `settle_raised`, each with its row's arguments.
- Move `HTTPSink._unadjudicated`'s body onto `lose_unadjudicated`, and restate
  `_unadjudicated_writers` in `tests/test_http_rounds.py` in the same commit.
- Remove the imports each module no longer uses, and keep each pinned docstring sentence.

### Phase 3: The roster and the mutants (FR-004, FR-005)

- Write `tests/test_settlement_shape_roster.py` with its exemption table and fixture corpus.
- Run FR-005's mutants and FR-004's check removals, reverting each.
