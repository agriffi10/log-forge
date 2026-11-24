---
type: Spec
id: SPEC-100
title: "SNSSink Writes an SNS FIFO Topic: Each Message Carries Its Trace's Group and Its Event's `log_id`, Resent Unchanged"
description: "Gives SNSSink the FIFO arguments SQSSink has, so a FIFO topic receives every event with a MessageGroupId that defaults to its trace_id and a MessageDeduplicationId that defaults to its log_id, both resolved once per delivery run, resent unchanged, costed in the message limit and held in group order through the mechanism SQSSink already uses; refuses on both sinks, at construction, a fifo that is not a bool, fifo=True on a name not ending in .fifo and an id argument on a sink that is not FIFO; sends an over-long deduplication id on either sink as its digest rather than a prefix two events can share; and records why a FIFO SNSSink still may not declare itself idempotent."
spec_status: Completed
depends_on: [SPEC-010, SPEC-016, SPEC-058, SPEC-071, SPEC-074, SPEC-092, SPEC-093, SPEC-095, SPEC-096]
arc: audit-2026-11-14
arc_order: 16
---

## Overview

An SNS FIFO topic refuses any publish that carries no message group, and `SNSSink` sends none. So
today a caller who points `SNSSink` at a FIFO topic loses every event: against moto's FIFO topic,
an `emit` of three events raised `SinkDeliveryError`, counted all three in `failed` and wrote
`lost 3 message(s); SNSSink, 1 attempt(s), InvalidParameterException`. The loss is counted and
announced, but nothing the caller can configure avoids it. SPEC-095 recorded this as an open item
and Andrew chose to close it in its own spec. This is that spec. `SNSSink` gains the three
arguments `SQSSink` has had since SPEC-016. A `.fifo` topic ARN selects FIFO behaviour. Each
message is then grouped by its event's `trace_id`, so a trace stays in order while separate traces
travel in parallel, and is de-duplicated by its event's `log_id`, which SPEC-095 settled as the
library's idempotency key. Both ids are worked out once per delivery and reused on every resend,
so the topic can discard a copy that a lost reply caused, for as long as its de-duplication
interval lasts. A message is held behind an earlier one of its group exactly as `SQSSink` holds
one, through the same code. Both sinks now judge the three arguments alike at construction:
`fifo` must be a `bool` or `None`, `fifo=True` needs a name ending in `.fifo`, and an id argument
given to a sink that is not FIFO is refused rather than ignored. For `SQSSink` that refuses three
calls 1.x accepted. Writing this spec also found a silent loss that `SQSSink` has today.
A de-duplication id longer than 128 characters is cut to its first 128, so two events whose ids
share that prefix go out under one id, and the queue then discards the second: moto's FIFO queue
held one of two such events, and the sink counted nothing. Both sinks now send such an id as its
digest instead. Planning the build found a second defect, counted rather than silent: an
`SQSSink` id callable that answers `bytes`, or a `str` holding a lone surrogate, loses its whole
round rather than its one event, and both sinks now drop only that event. Last, the spec records
why a FIFO `SNSSink` still may not declare itself idempotent.

## Scope

### In Scope

- `SNSSink(fifo=, message_group_id=, message_deduplication_id=)`, keyword-only and appended after
  SPEC-096's `max_message_bytes`, and one judgement of the three that both sinks call, which
  refuses a non-`bool` `fifo`, `fifo=True` on a name not ending in `.fifo` and an id argument on a
  sink that is not FIFO (FR-001).
- On a FIFO topic, each entry carries both ids, resolved once per run, reused on every resend and
  counted in the entry's cost against the message limit, through one private module the two sinks
  share, and on both sinks an id answer that cannot be costed costs only its own event (FR-002).
- On both sinks, a de-duplication id longer than 128 characters goes out as its SHA-256 hex digest
  (FR-003).
- A FIFO `SNSSink` holds a later message of a group behind an earlier one, as `SQSSink` does
  (FR-004).
- Every way a FIFO publish can be refused is read by the rules SPEC-093 and SPEC-094 already set,
  and each is pinned by a test (FR-005).
- SPEC-092's corpus gains an `SNSSink[fifo]` variant beside `SQSSink[fifo]` (FR-006).
- The record: `docs/invariants.md`, `architecture.md` §12, five decisions and one new one, the
  README, the pending release notes, the component inventory and the docstrings (FR-007).

**Twin paths (invariant 6).** `SQSSink` and `SNSSink` become twins for FIFO behaviour, and FR-002
makes them one mechanism rather than two copies. The worker, the orphan path and
`emit_via_deliver` all run the engine over `deliver`, so the ids, the hold and the costing are the
same on all three. A `FilteringSink` or `TransformSink` around a FIFO `SNSSink` offers `deliver`
because the sink does (SPEC-075). `MultiSink` calls a child's `emit`, and each `emit` is a new run,
so a whole-batch resend around a `MultiSink` is not covered by FR-002's "resolved once" (FR-002
says what it does get).

### Out of Scope

- **Fair queues, and the `MessageGroupId` a standard topic accepts.** AWS forwards a group id from a
  standard topic only to SQS standard subscriptions that use fair queues. A standard-topic
  `SNSSink` sends no group id, as today, and `message_group_id` passed to a sink that is not FIFO is
  refused at construction on both sinks (FR-001). Fair-queue support could later lift that refusal
  for `message_group_id`; nothing here builds it.
- **Managing the topic.** The sink does not read or set `FifoTopic`, `ContentBasedDeduplication`,
  `FifoThroughputScope` or `MaximumMessageSize`. `fifo` is taken from the ARN or the argument, as
  `SQSSink` takes it from the URL. An explicit de-duplication id overrides content-based
  de-duplication, so `ContentBasedDeduplication` changes nothing the sink sends.
- **Message attributes.** No `MessageAttributes` are sent, on either topic type. SPEC-095 settled
  that no new key goes on the wire, and the body already carries `log_id`.
- **Declaring `log_foundry_idempotent` on `SNSSink`.** FR-007 records why a FIFO topic fails
  SPEC-095's conditions 1 and 3. A resend stays counted in `resent`, as SPEC-095 counts it.
- **Holding a group across the worker's whole budget, or ordering inside one call.** The hold is
  SPEC-074's, unchanged: it keeps a later *call* from overtaking an earlier message of its group,
  for `max_retries + 1` sends. What the service does with a group's later messages in a call where
  an earlier one failed is the service's. `spec100_ordering.py` case A shows the worst case, in
  which they land first.
- ~~**Judging `fifo`'s type.**~~ Struck: FR-001 judges it on both sinks (*Revision history*).
- **A group id outside AWS's character set.** A constant or a callable that yields one is sent as
  given. The service refuses it, and FR-005 reads that refusal. Only the de-duplication id is
  digested (FR-003), because only its truncation loses an event. A truncated group id merges two
  groups, which costs ordering, never delivery.
- **Any change to `KinesisSink`, `FirehoseSink`, the engine, the verdicts, the stderr line formats or
  the counters.**

---

## Functional Requirements

**Bases.** The spec was authored at `03dbf502`, where SPEC-093, SPEC-095 and SPEC-096 were Draft.
It was re-verified at the build's start against `5d0e7f68`, the `main` that carries SPEC-093,
SPEC-094, SPEC-095 and SPEC-096, and every `file:line` in the requirements, the Data Model and the
Interface Contract cites that tree. SPEC-096 was built before this spec landed (`dd77b3fb`). At
`1e9ab67e`, which carries it, `spec100_ids_once.py`, `spec100_costing.py`, `spec100_ordering.py`,
`spec100_refusals.py` and `spec100_moto_shapes.py` read the same as at `03dbf502`, apart from
moto's random message ids. `spec100_prototype.patch` applies to `03dbf502` only, and the build
ports it onto the current code (*Evidence → The prototype* says what that changes). Where a
criterion names `max_message_bytes`, `resent` or `dropped_unadjudicated`, it means what those
specs define. *Evidence* gives each measurement and the tree it was taken on.

**What this spec builds on.**

- *A FIFO message group is a trace, not the process* (SPEC-016, refined by SPEC-074).
- *A sink round resends only what failed, and an SQS FIFO group keeps its order* (SPEC-074).
- *Caller code that fails for one event costs only that event; a batch of nothing but failures
  re-raises* (SPEC-058), which is how `SQSSink` treats a raising id callable.
- *A bad argument is floored if it works and refused if it is already broken; a new one is
  refused*, and *An argument door asks the type, never the value* (SPEC-071).
- SPEC-095's key (`log_id`), its `resent` count, and its three conditions for declaring
  `log_foundry_idempotent`.
- SPEC-094's line between a refusal that no attempt could have escaped, which is *definitive
  non-delivery*, and a report after which the event *may have landed*. The fence *A client exception
  costs its chunk, and is provable non-delivery for it* makes a raised call `RETRYABLE` or
  `REJECTED` by its class.

### FR-001: `SNSSink` takes `fifo`, `message_group_id` and `message_deduplication_id`, and both sinks judge the three alike

#### Description:

`SNSSink.__init__` gains three keyword-only parameters, appended in this order after
`max_message_bytes` (SPEC-096), so every existing call binds as before (invariant 10):

- `fifo: bool | None = None`. `None` means `topic_arn.endswith(".fifo")`. AWS requires the name of
  every FIFO topic to end in `.fifo` (`CreateTopic`). It does not say that a standard topic's name
  cannot, and `fifo=False` is the override for that case, as on `SQSSink`.
- `message_group_id: GroupIdSource = None`: a constant `str`, a callable of the event, or `None`
  for the event's `trace_id`.
- `message_deduplication_id: DedupIdSource = None`: a callable of the event, or `None` for the
  event's `log_id`.

Each is stored as a public attribute of the same name, as on `SQSSink`. `GroupIdSource` and
`DedupIdSource` are exported from `log_foundry.sinks.sns` as well as from `log_foundry.sinks.sqs`.
They are the same objects, defined once in the shared module (FR-002).

**The judgement** is one helper, `_fifo.judge`. On both sinks every refusal the constructor makes
today stays where it is, in its order, and `judge` runs after all of them and before any client is
built, so a call refused at the base keeps its exact error on both:

- On `SNSSink` the base refuses `max_retries` (`sns.py:147`) and then `max_message_bytes`
  (:148-154), and builds the client at :155. `judge` runs between the two.
- On `SQSSink` the base refuses the two id types through `require_callable` (`sqs.py:278-279`),
  then `max_retries` (:280), then a blank constant group (:281-287), then `max_message_bytes`
  (:288-294), and builds the client at :295. `judge` runs after `max_message_bytes`. There it
  re-asks the three refusals the base has already made, which therefore cannot fire on `SQSSink`,
  and adds the new ones.

It judges the types first and then the values:

- `message_group_id` that is not a `str` subclass, not callable and not `None` is refused with
  `TypeError` `<Class> message_group_id must be a str, a callable or None, not <type>`, and
  `message_deduplication_id` that is neither callable nor `None` with `TypeError` `<Class>
  message_deduplication_id must be callable or None, not <type>`, as on `SQSSink` today.
- `fifo` that is neither a `bool` nor `None` is refused with `TypeError` `<Class> fifo must be a
  bool or None, not <type>`, asking the type, never the value (SPEC-071 FR-005). `bool` cannot be
  subclassed, so the test admits `True` and `False` alone, and no subclass case exists to read as
  SPEC-066 reads an `int` subclass. `SQSSink` has read `fifo` by truthiness since SPEC-016, so
  `fifo=1` stops constructing there.
- A constant group id that is blank after `str.strip` is refused with `ValueError` `message_group_id
  must be a non-empty string; pass None to group by trace_id`, the base's text, which names no
  class, on both sinks.
- `fifo=True` on a queue URL or topic ARN that does not end in `.fifo` is refused with
  `ValueError` `<Class> fifo=True needs a <queue_url|topic_arn> ending in .fifo: AWS requires every
  FIFO <queue|topic> name to end so`. That sink can never deliver, so it is the bad-argument rule's
  "refused if it is already broken".
- `message_group_id` or `message_deduplication_id` passed, not `None`, to a sink that is not FIFO,
  by its name or by `fifo=False`, is refused with `ValueError` `<Class> cannot apply <names> to a
  standard <queue|topic>; drop it, or use a <queue_url|topic_arn> ending in .fifo`, naming each
  argument passed. A standard destination never reads either, so accepting one ignores an argument
  because another was given (invariant 13), the fence's "an argument the chosen backend cannot
  use". On `SQSSink` this refuses a call that delivered, with the ids ignored.

`fifo=False` on a name ending in `.fifo` stays accepted, on both sinks, because AWS does not say
a standard name cannot end so, and SPEC-016 FR-006 tests that case on purpose
(`tests/test_sinks_sqs_fifo.py::test_fifo_misconfiguration_costs_one_call_per_chunk_not_four`). On
a real FIFO destination its every send fails, counted and announced (FR-005). FR-007 records it as
an exception to invariant 13.

**The rosters and tests that name these arguments.**

- `tests/test_type_doors.py`: `_REFUSED`, the subclass test, the accepted-values test and the
  blank-group test gain `SNSSink` beside `SQSSink`. `_REFUSED` gains `fifo=1`, `fifo="yes"` and a
  value whose `__class__` claims `bool`, on both sinks. `_REFUSED`'s two standard-URL cases
  (`tests/test_type_doors.py:306` and `:316`) still raise their `TypeError`, not the new
  `ValueError`, because types are judged first. The accepted-values test moves to a `.fifo` URL,
  because it builds `SQSSink("https://q", message_group_id=value, ...)`, which is now refused.
- `tests/test_type_door_roster.py`: `EXPECTED_DOORS` gains `sinks._fifo.judge`, fourteen doors,
  and both places the file states the count follow: check 3 of the module docstring
  (`tests/test_type_door_roster.py:20`) and the `EXPECTED_DOORS` docstring (:64), each reading
  thirteen at the base. `judge` carries both an `issubclass(type(fifo), bool)`
  and a stored-name read, so the roster's mutation sweep plants in it. With only that line
  changed, the file passes on the prototype.
- `tests/test_sinks_message_limit.py::test_each_signature_ends_with_max_message_bytes_and_keeps_the_1x_parameters[SNSSink]`
  asserts that the signature ends at `max_message_bytes` (:159-160). For `SNSSink` it changes to
  assert the 1.x parameters, then SPEC-096's `max_message_bytes`, then the three new parameters in
  FR-001's order. The `[SQSSink]` case is unchanged, because `SQSSink`'s three arguments are among
  its 1.x parameters.
- `tests/test_public_surface.py::test_the_aliases_a_public_signature_names_are_exported`.
- `tests/typed_consumer/accepts.py`, the frozen-surface consumer (SPEC-051). Its line 321 builds
  `SQSSink("https://sqs.invalid/q", message_group_id=_GROUP, message_deduplication_id=_DEDUP)`,
  ids on a standard URL, which FR-001 refuses. The file is type-checked and never run, but it
  documents a call a caller may write, so the build moves that line to a `.fifo` URL.
- `tests/test_sinks_encode.py::test_the_public_surface_grows_only_by_five_losses`, which gains the
  three new public attributes for `SNSSink`.
- `tests/test_sinks_sqs_chunks.py`'s `_sink` helper builds `SQSSink("https://q", fifo=fifo, ...)`.
  Six tests that pass `fifo=True` through it fail on the prototype, and the helper takes a `.fifo`
  URL when `fifo` is true. With that change alone they pass.
- `tests/test_sinks_sqs_fifo.py::test_explicit_fifo_true_forces_it_on_a_plain_url` and
  `tests/test_sinks_sqs_id_failures.py::test_a_standard_queue_never_consults_the_callables` become
  refusal tests: the configurations they build are refused.

`tests/test_numeric_doors.py`, `tests/test_numeric_door_roster.py` and
`tests/test_injected_client_roster.py` need no change: none of the three arguments is numeric or a
client build option, and the prototype passes all three unchanged.

#### Acceptance Criteria:

- [ ] `inspect.signature(SNSSink.__init__)` ends with `max_message_bytes`, `fifo`,
      `message_group_id` and `message_deduplication_id`, all keyword-only with default `None`, and
      `SNSSink("arn", client=c, max_retries=2)` still constructs (invariant 10).
- [ ] `SNSSink("arn:aws:sns:r:1:t.fifo", client=c).fifo` is true and `SNSSink("arn:aws:sns:r:1:t",
      client=c).fifo` is false. On both sinks, `fifo=True` on a `.fifo` name constructs FIFO, and
      `fifo=False` on a `.fifo` name constructs with `.fifo` false (invariant 13).
- [ ] On both sinks, `fifo=True` on a name not ending in `.fifo` raises `ValueError` naming `fifo`
      and the destination's keyword, and builds no client (invariants 13 and 6).
- [ ] On both sinks, `fifo=1`, `fifo="yes"` and a value whose `__class__` claims `bool` each raise
      `TypeError` `<Class> fifo must be a bool or None, not <type>` naming the real type, before
      any client is built, and `True`, `False` and `None` are accepted (invariants 13 and 6).
- [ ] On both sinks, a constant or callable `message_group_id`, or a callable
      `message_deduplication_id`, given to a sink that is not FIFO, by its name or by `fifo=False`,
      raises `ValueError` naming that argument and builds no client, and both given name both
      (invariants 13 and 6).
- [ ] Each of the four `_REFUSED` cases written for `SQSSink` (a group of `5` and a dedup of `"x"`,
      each on a FIFO and a standard name) raises the same `TypeError` text on `SNSSink`, naming
      `SNSSink` or a subclass's own name, and builds no client (invariants 13 and 6).
- [ ] Every call refused at the base keeps its exact error on both sinks: `SQSSink("https://q",
      max_retries=2.5, fifo="x")` raises the `max_retries` `TypeError`; `SQSSink("https://q",
      max_message_bytes=5, fifo="x")` and `SNSSink("arn:aws:sns:r:1:t", max_message_bytes=5,
      fifo="x")` each raise the `max_message_bytes` `ValueError`; and `SQSSink("https://q",
      fifo="x", message_group_id=5)` and its `SNSSink` twin raise the `message_group_id`
      `TypeError` (invariants 13 and 6).
- [ ] On both sinks, `message_group_id="  "` raises `ValueError` `message_group_id must be a
      non-empty string; pass None to group by trace_id` before any client is built, and a `str`
      subclass constant is accepted and sent as the `str` it holds, without its own `strip`
      running (invariant 13).
- [ ] `log_foundry.sinks.sns.GroupIdSource is log_foundry.sinks.sqs.GroupIdSource`, and the same
      holds for `DedupIdSource`. Both are in `sns.__all__`, and the typed consumer type-checks under
      `mypy --strict` (invariant 10).

### FR-002: A FIFO `SNSSink` sends each message with its group and its event's `log_id`, resolved once per run, resent unchanged and costed

#### Description:

**One mechanism.** A new private module, `sinks/_fifo.py`, holds what makes an entry FIFO:

- the constants `DEFAULT_GROUP_ID` and `MAX_ID_LEN` and the two type aliases;
- the argument judgement, `judge` (FR-001);
- `bounded`, SQS's `_bounded`, which the group id keeps (FR-003);
- `group_id` and `dedup_id`, which resolve one event's ids;
- the per-run entry cache: SQS's `_Entry`, `_Prepared`, `_Plan` and the identity-keyed
  preparation in `SQSSink._chunks` and `SQSSink._prepare`;
- `hold` and `block` (FR-004).

`SQSSink` keeps its constants and its methods: `sqs.DEFAULT_GROUP_ID`, `sqs.MAX_ID_LEN`,
`SQSSink._group_id`, `SQSSink._dedup_id`, `SQSSink._chunks`, `SQSSink._prepare` and
`SQSSink._send` still resolve, and the two constants are the shared objects. Its module-level
`_Prepared` (`sqs.py:77`), `_Plan` (:103), `_Entry` (:124), `_bounded` (:146), `_hold` (:901) and
`_block` (:937) move to `_fifo.py` without the underscore. All six are runtime names of `sqs`
today, since only `threading` and `SinkLosses` sit under `TYPE_CHECKING` (:41-44), and
`tests/test_sinks_sqs_chunks.py:25,90,111-122` imports `sqs._Plan` and reads its `chunks`,
`dropped` and `refused`. So `sqs.py` keeps each old private name bound to the shared object
(`sqs._Plan is _fifo.Plan`, `sqs._Prepared is _fifo.Prepared`, and so on for all six), and every
test that reads one resolves it unchanged.

`SQSSink._group_id` and `SQSSink._dedup_id` delegate to `_fifo.group_id` and `_fifo.dedup_id`,
but nothing in the sink calls them any more: `SQSSink._prepare` hands the two sources to
`_fifo.prepare`, which resolves the ids itself. A subclass that overrides either therefore changes
nothing the sink sends. Both are private, so no compatibility promise covers them, and their
docstrings say so (FR-007).

**The cache keeps its key and its owner.** `SQSSink._chunks` keeps the run's entries in
`run_context().notes(self)` under `"prepared"`, the round's under `"prepared_round"` and the run's
limit under `"limit"` (`sqs.py:710-716`). The shared preparation keeps those three keys in the
notes of the sink that calls it. `tests/test_sinks_queue_engine.py:622-631`
(`test_the_per_round_id_mutant_is_caught`) disables the cache by popping
`notes(self)["prepared"]`, and it passes unchanged.

Each sink calls `encode_round` in its own module and hands the bodies to the shared preparation. That keeps `tests/test_encode.py`'s roster deriving both sinks: the prototype
first moved the call into `_fifo.py`, and the roster lost both. `SNSSink._bodies` is replaced by
the shared preparation and an `SNSSink._register_drops`. That method keeps SNS's oversize line,
`SNSSink, exceeds the <limit>-byte message limit`, and adds the id-failure line `SQSSink` already
writes, `SNSSink, a FIFO id callable raised <Type>`. `tests/test_retry_safe_drop_counts_prose.py`'s
roster names it in place of `_bodies`.

`plan_round` builds the round's chunks with `chunk_items` (the Interface Contract), so neither
sink calls the helper itself any more: `SQSSink._chunks` loses its call (`sqs.py:747`) and
`SNSSink._round` loses its own (`sns.py:335`), each reading `Plan.chunks`. That breaks two tests
of `tests/test_chunk_helper_roster.py`, which derives the set of modules calling a chunk helper:
`EXPECTED` (:24) becomes `_fifo`, `clickhouse`, `firehose`, `kinesis` and `postgres`, five where
it held six, and the test name `test_the_chunking_modules_are_the_six` (:64) and the module
docstring's "the six on SPEC-071's head" follow. Its premise test,
`test_a_sink_that_inlines_its_chunker_back_is_named` (:68-72), takes `_fifo` in place of `sqs`,
since `sqs` no longer calls `chunk_items`.

**The ids.** On a FIFO topic each entry carries `MessageGroupId` and `MessageDeduplicationId`, as
`SQSSink`'s does on a FIFO queue:

- The group is the constant, the callable's answer, or the event's `trace_id`. A blank answer
  becomes `DEFAULT_GROUP_ID`, and an answer longer than 128 characters is cut to 128 (SPEC-016).
- The de-duplication id is the callable's answer or the event's `log_id`. A blank answer becomes
  a fresh UUID, and an over-long one is FR-003's.
- An id callable that raises, or that returns something other than a `str`, costs its own event,
  dropped and announced. When every event's callable raised, the round is refused with the first
  exception (SPEC-058 FR-004).
- So does an answer that resolves but cannot be costed. At the base, `SQSSink._prepare` costs each
  id with `.encode("utf-8")` outside the `try` that guards the callables (`sqs.py:781-786`). An
  answer that is not a `str` but has a working `strip`, such as `bytes`, and a `str` holding a
  lone surrogate, such as `"ok\ud800"`, each pass the `try` and raise at the costing, and the whole
  round is lost: for one such answer among two events, `emit` raised `SinkDeliveryError ...
  raised 2 after 1 delivery round(s)` with `failed=2` and no call made. `_fifo.prepare` costs the
  ids inside the `try`, so such an answer becomes an id failure for its own event, with the
  id-failure line.

On a standard topic no id is resolved and the entry is `{"Id", "Message"}`, byte for byte as today.

**The default key is `log_id`, on both sinks.** `SQSSink._dedup_id` already defaults to it
(`sqs.py:600-624`, the `log_id` default at :619, SPEC-016 FR-003). `spec100_ids_once.py` case A reads `L0` and `L1` on the wire
for events whose `log_id`s they are, so nothing about `SQSSink` changes here. That is also what the
SPEC-092 double's de-duplication relies on.

**Resolved once per run.** The cache keeps each event occurrence's prepared entry for the run, so a
resend in any later round carries the identical ids. A callable that answers differently on every
call is asked once per event per run: `spec100_ids_once.py` case B counted four calls (two ids for
each of two events) over two rounds, and the resend carried the first round's `d1` and `g0`. That is
SPEC-095's second condition, a key identical on every resend the engine makes. It holds on
`SQSSink` today, measured, and the shared cache gives `SNSSink` the same.

**Across runs it holds only for a stable source.** A second `emit` of the same event objects is a
new run. The default `log_id` and `trace_id` are the same there (case C, `L0` in all three
calls), but a callable that answers differently, or the minted fallback for an event without a
`log_id`, gives a new id (cases C and D). That matters wherever a caller resends whole batches
around the engine, which `MultiSink`'s worker loop does. FR-007 records it in the README beside
the arguments.

**Costed.** An entry's cost is its body's UTF-8 length plus, on a FIFO topic, both ids' UTF-8
lengths, exactly as `SQSSink` costs it, and it is judged against the run's limit: `max_message_bytes`
when it is set, else `MAX_BYTES`, at most 1 MiB, read once per run and kept under `"limit"`, as
`SQSSink._prepare(event, body, *, limit)` (`sqs.py:757`) is handed it today. That is SPEC-016
FR-005, and SPEC-096 kept it when it set the limit. SNS's pages say a message's body and its attributes count
toward `MaximumMessageSize` and say nothing about the two ids. Counting them over-counts by at most
256 bytes, which drops an event at the edge, counted and announced, rather than sending one the
service may refuse together with its chunk. One rule on both twins also keeps FR-006's
`SNSSink[fifo]` rows reading as `SQSSink[fifo]`'s. `spec100_costing.py` case A drops a 262,104-byte
body on a FIFO topic, which costs 262,168 with its two 32-character ids, and sends the same body to
a standard topic (case B). moto takes the same message, but moto counts characters, not bytes, and
no id (`spec100_moto_shapes.py` case D), so it says nothing about AWS.

**What the topic's de-duplication does with a resend.** AWS says a message published with a
de-duplication id it has seen in the interval is accepted but not delivered. The interval is "5
minute minimum". When `FifoThroughputScope` is `MessageGroup`, de-duplication applies within each
group, so a resend is absorbed only if its group id is identical too, which the cache gives within a
run. moto cannot show any of this: its `publish_batch` docstring says "The
MessageDeduplicationId-parameter has not yet been implemented". `spec100_moto_shapes.py` case C
published one id twice to a topic with two subscribers. The FIFO SQS queue held one copy, because
SQS de-duplicates, and the standard queue held two, because moto's SNS forwarded both. The suite
therefore proves what the sink controls, which is identical ids on every resend, against a stand-in
that de-duplicates. The moto probe is re-run at the build against a FIFO subscriber that
de-duplicates. The topic's own absorption rests on AWS's word, and FR-007 says so.

#### Acceptance Criteria:

- [ ] On a FIFO topic, an `emit` of two events sends entries whose `MessageGroupId` is each event's
      `trace_id` and whose `MessageDeduplicationId` is each event's `log_id`. On a standard topic
      the same `emit` sends entries whose only keys are `Id` and `Message` (invariants 2 and 6).
- [ ] A recording client that answers the first call's first entry `{"SenderFault": false}` gets a
      second call whose entry carries the first call's two ids for that event, on `SNSSink` and on
      `SQSSink`. With callables that return a new string per call, each callable is called once per
      event across both rounds (invariants 2 and 6).
- [ ] With the cache disabled, so ids are resolved on every round, the second criterion fails on
      both sinks. That is the mutant it exists to kill, and the build runs it once (invariant 2).
- [ ] An id callable that raises for one event of two drops that event, counts it in
      `losses().dropped`, sends the other and writes one line naming `SNSSink` and the exception's
      type. One that raises for both makes `emit` raise that exception by identity, with nothing
      counted. A batch of one event whose callable raises and one oversized event makes no call,
      returns, counts both in `losses().dropped` and writes one line for each (invariants 2 and
      7).
- [ ] On both sinks, an id callable answering `bytes` for one event of two, and one answering a
      `str` holding a lone surrogate for one event of two, each drop that event as an id failure
      with the id-failure line and send the other (invariant 2).
- [ ] A blank group answer sends `log-foundry`, and a blank de-duplication answer sends a
      32-character hex id that is the same on that event's resend (invariant 2).
- [ ] With `max_message_bytes` at its default, a FIFO `SNSSink` drops an event whose body is 40
      bytes under the limit and whose two ids cost 64, counts it in `dropped_oversized` and writes
      `SNSSink, exceeds the 262144-byte message limit`. A standard-topic `SNSSink` sends the same
      event (invariants 2 and 6).
- [ ] Over a stand-in topic that keeps one copy per de-duplication id, as the SPEC-092 double does,
      and whose first `publish_batch` takes every entry and then raises `ReadTimeoutError`, each of
      three events is held once with group and de-duplication callables that return a new string
      on every call, as in the second criterion, and every call carried the same de-duplication id
      for each event. With the cache disabled, the stand-in holds more than three (invariant 2).
- [ ] `spec100_moto_shapes.py` case E, re-run at the end of Phase 2 against the built tree, with
      FR-004's hold in place, in a scratch environment holding moto, reads 3 messages for 3
      events at the FIFO subscriber, and the delivery doc records the reading. moto never becomes
      a repository dependency, so no suite test imports it (invariant 2).
- [ ] Every test in `tests/test_sinks_sqs*.py` that exists at the build's base passes unchanged,
      except the nine FR-001 and FR-003 name: the six through `test_sinks_sqs_chunks.py`'s
      `_sink`, which change only its URL, the two FR-001 makes refusal tests, and the one FR-003
      rewrites (invariant 6).

### FR-003: A de-duplication id too long to send goes out as its digest, never as a prefix

#### Description:

**The defect, on `SQSSink` today.** `_bounded` cuts an id longer than `MAX_ID_LEN` (128,
`sqs.py:51`) to its first 128 characters (`sqs.py:146-162`). For a group id that merges two groups, which costs
ordering. For a de-duplication id it makes two different events share one id, and a FIFO queue or
topic accepts the second and does not deliver it. `spec100_moto_shapes.py` case F sent two events
whose callable answered 200 characters sharing the first 128 to moto's FIFO queue. The queue held
one, and the sink counted `SinkLosses(dropped=0, failed=0)` and wrote no line. That is a silent
loss under invariant 2. The default `log_id` is 32 characters, so only a caller's callable reaches
it.

**The rule.** `_fifo.dedup_id` strips the answer. If the stripped answer is longer than
`MAX_ID_LEN`, the id sent is the SHA-256 digest of the stripped answer,
`hashlib.sha256(stripped.encode("utf-8", "surrogatepass")).hexdigest()`: 64 characters drawn from
the set AWS accepts, the same for the same stripped answer and different for different ones. So
two answers that differ only in surrounding whitespace share a digest, as they share an id today.
A blank answer keeps the minted fallback, and a stripped answer of up to 128 characters is sent as
it is. The
group id keeps SPEC-016's truncation (*Out of Scope*). The rule lives in the shared module, so it
binds both sinks. `tests/test_sinks_sqs_fifo.py::test_over_long_dedup_id_is_truncated` is
rewritten to assert the digest.

Andrew chose this on 2026-11-17, "Send its SHA-256 digest (Recommended)" (*Revision history*). The
alternatives were to keep the truncation and record the loss in §12, or to drop an event with an
over-long id, counted and announced.

#### Acceptance Criteria:

- [ ] On both sinks, two events whose de-duplication callable answers 200 characters sharing their
      first 128 are sent with two different 64-character ids, each equal to the SHA-256 hex digest
      of its stripped answer (invariants 2 and 6).
- [ ] `spec100_moto_shapes.py` case F, re-run with case E at the end of Phase 2 as FR-002 says,
      reads 2 of 2 events held, and the delivery doc records it (invariant 2).
- [ ] An answer of exactly 128 characters is sent unchanged, one of 128 characters between a
      leading and a trailing space is sent as those 128, one of 129 characters followed by a
      space is sent as the digest of those 129, a blank one is sent as a fresh 32-character
      hex id, and the resend of a digested id carries the same digest (invariant 2).
- [ ] With the digest replaced by truncation, the first criterion fails on both sinks, and the
      build runs that mutant once (invariant 2).

### FR-004: A FIFO `SNSSink` holds a group's later message behind an earlier one, as `SQSSink` does

#### Description:

`SNSSink._round` decides holds with the shared `hold` and `block`, by the rule *A sink round
resends only what failed, and an SQS FIFO group keeps its order* states. A message is held,
`RETRYABLE` and not sent, while an earlier message of its group, sent in an earlier call, has
neither landed nor used its `max_retries + 1` sends. In the run's last round nothing is held. A
message the worker leaves held is counted `failed` with the line `SNSSink, <n> attempt(s), held
behind an earlier message of its FIFO group`.

**What the hold guarantees, measured on the prototype** (`spec100_ordering.py`):

- **Across calls, for `max_retries + 1` sends.** Twelve events of one group need two calls. When
  the first call's first entry failed once, the hold kept the second call's two events until the
  failed one had landed. With the hold switched off they landed first (case D).
- **Not inside one call.** The fake service accepted the nine later entries of the call in which
  the first failed, so they landed before it, with and without the hold (cases A and D). SNS
  documents ordering within a group and within a batch, and does not say what it does with a
  batch's later entries when an earlier one fails. This is the same for `SQSSink`, and FR-007
  corrects the README sentence that reads stronger than this.
- **What it costs.** When a whole first call failed, the hold resent one message per group per
  round until the last round: four calls instead of two (case C), each round after a backoff wait
  that `shutdown()` cuts short.

Andrew chose this on 2026-11-17, "Hold, as SQS (Recommended)" (*Revision history*). It refines the
decision's "do NOT let any sink but `SQSSink` FIFO hold an event back for order" to name both
FIFO sinks. The alternative was no hold, which leaves a FIFO topic's order to the service and to
the order of calls.

#### Acceptance Criteria:

- [ ] Twelve events of one group whose first entry fails once land with the two events of the
      second call after the failed one, on `SNSSink` and on `SQSSink`, and `losses()` reads
      nothing failed (invariants 2 and 6).
- [ ] With `hold` returning `False` for every entry, the first criterion fails on `SNSSink`, and
      the build runs that mutant once (invariant 2).
- [ ] In the twelve-event batch, a first entry that fails on every send holds the second call's two
      events until the run's last round, where they land in the same call as its final send. It is
      counted once in `failed` with the abandoned line naming `SNSSink` (invariants 2 and 11).
- [ ] A worker that declines the round after the first leaves a held message counted in `failed`
      with one line ending `held behind an earlier message of its FIFO group` (invariants 2 and 11).
- [ ] A standard-topic `SNSSink` holds nothing: the twelve-event batch with the same failure lands
      the second call's events in the first round (invariant 6).

### FR-005: Each FIFO refusal is read by the rules already in place

#### Description:

This FR adds no classification. It pins, test by test, how the existing keyed rule and
`aws_retryable` read every way a FIFO publish can be refused, in SPEC-094's terms. **The AWS
pages name no error code for a missing or invalid id.** moto is the only evidence of a refusal's
shape, and each test's stand-in reproduces moto's shape without importing moto.

| Refusal | How it reaches the sink | Shape (moto 5.2.5) | Outcome | Kind |
|---|---|---|---|---|
| No group id | only `fifo=False` on a FIFO topic, which FR-001 accepts as invariant 13's recorded exception: the sink sends both ids or neither, and a blank answer falls back (case F of `spec100_ids_once.py`) | the whole call raises `InvalidParameter`, HTTP 400 | `REJECTED` at the first attempt, the chunk counted in `failed`, one line `SNSSink, 1 attempt(s), InvalidParameterException` | definitive non-delivery: it judges a parameter every attempt carries unchanged, so no attempt could have written it |
| No de-duplication id, with content-based de-duplication off | unreachable: the sink never sends a group id without one | the entry in `Failed`, `SenderFault: true`, HTTP 200 | `REJECTED` and a final rejection (SPEC-070 FR-003) | definitive non-delivery, for the same reason |
| A de-duplication id on a standard topic | only a standard topic whose name ends in `.fifo`, with `fifo` left `None`; `fifo=True` on any other name is refused at construction (FR-001) | the entry in `Failed`, `SenderFault: true` | `REJECTED`, final | definitive non-delivery |
| An id over 128 characters | unreachable: the group is cut to 128 and the de-duplication id digested (FR-003) | moto accepts it and checks nothing | none | none |
| An id with a character AWS does not accept | a constant or a callable's answer | moto accepts it | whatever the service answers: a sender fault or a whole-call 400 is `REJECTED`, definitive; anything else as today | definitive, if refused |
| A throttle | any call; SNS FIFO allows 300 messages per second per group | `ThrottlingException`, HTTP 400, raised; or an entry in `Failed` without `SenderFault` | `RETRYABLE` and resent. A raise is marked as one that may have landed, because botocore retries a throttle inside the call (SPEC-095 FR-002), and a `Failed` entry is not | may have landed, for a raise; not for a named entry |

An empty group id is unreachable too, because a blank answer becomes `DEFAULT_GROUP_ID`. moto
answers one with neither `Successful` nor `Failed` naming the entry (case B). After SPEC-093 that
reads `UNKNOWN`, counted in `dropped_unadjudicated`, and before it, delivered. This is a moto
artifact the sink never meets, recorded so that nobody reads it as AWS's behaviour.

#### Acceptance Criteria:

- [ ] `SNSSink(fifo_arn, fifo=False)` over a stand-in that raises a `ClientError` with code
      `InvalidParameter` and status 400 for a call missing `MessageGroupId` makes one call, raises
      `SinkDeliveryError`, counts every event in `failed`, writes one line ending `1 attempt(s),
      <the raised exception's type name>`, and counts no `resent` (invariants 2, 7 and 13).
- [ ] `SNSSink(standard_arn, fifo=True)` raises `ValueError` at construction (FR-001). An
      `SNSSink` on a `.fifo`-named ARN over a stand-in that answers as a standard topic, each entry
      carrying a de-duplication id refused with a `SenderFault: true` `Failed` item, counts each
      event once in `failed`, never resends it, and returns rather than raising when no event
      landed, as a batch lost only to sender faults does (invariants 2, 7 and 13).
- [ ] No sequence of id-callable answers (blank, `None`, a non-`str`, 200 characters) produces an
      entry carrying one FIFO id without the other, on either sink. A test asserts this over those
      answers (invariant 2).
- [ ] A FIFO `SNSSink` whose first call raises a throttling `ClientError` resends the chunk in the
      next round with identical ids and counts `resent` for it. One whose first reply names an entry
      `Failed` with `SenderFault: false` resends that entry and counts no `resent` (invariants 2 and
      7).

### FR-006: SPEC-092's corpus gains `SNSSink[fifo]` beside `SQSSink[fifo]`

#### Description:

A second constructor variant changes what the corpus calls the first one. SPEC-092 names a variant
only where a class has more than one, so:

- **`SNSSink`'s variant becomes `SNSSink[standard]`.** Every pin and departure row naming
  `SNSSink-<fault>` is renamed `SNSSink[standard]-<fault>`, with its verdict unchanged.
- **`SNSSink[fifo]` is added**, class `keyed-reply`. It is built on a `.fifo` topic ARN with
  `picks={"fifo": True}`, over an SNS double that, like the `SQSSink[fifo]` double, does not take a
  second copy of a `MessageDeduplicationId` it has taken. It runs the seven `keyed-reply` faults.
- **`PARAMETERS`** gains `fifo`, `message_group_id` and `message_deduplication_id` for `SNSSink`
  (`tests/conformance_classes.py:553`). `fifo` is the variant's pick. The other two go in
  `NEUTRAL["SNSSink"]` (:700), as `SQSSink`'s two id arguments sit in `NEUTRAL["SQSSink"]`
  (:702-711).

**Where its rows sit.** On `03dbf502` with this spec's prototype, its seven rows read exactly as
`SQSSink[fifo]`'s, fault by fault: six green, and `no-verdict-shape` red at `silent-loss`
(`spec100_corpus_rows.py`). Its rows therefore join whatever `SQSSink[fifo]`'s rows are in on the
base the build starts from. With SPEC-093 and SPEC-095 built, that means:

- `no-verdict-shape`: green, as SPEC-093's decision A turns `SQSSink[fifo]`'s;
- `refused-once` and `refused-always`: in SPEC-095's `aws-client-retries` departure at
  `resent-when-knowable`, as `SQSSink[fifo]`'s are;
- `clean`, `partial`, `taken-then-raised` and `reply-lost`: green. The double absorbs the resend,
  and SPEC-095 counts it, which `duplicate` accepts.

**The totals.** The variant renamed changes no count, and the seven new rows add no pin and no
departure. Against the base, rows grow by 7, departure rows by 2 (the two `aws-client-retries`
rows, which go from 10 to 12) and must-pass rows by 5, and pins, pinned rows and the number of
departures are unchanged. `arc_order` puts SPEC-093, SPEC-094 and SPEC-095 before this spec, and
SPEC-096 moves no row (`spec096_corpus_rows.py`), so the base is SPEC-095's "after SPEC-094" row:

| Base | Rows | Pinned | Departure rows | Must pass |
|---|---|---|---|---|
| SPEC-095's "after SPEC-094" | 268 | 6, under 4 pins | 34, under 14 departures | 228 |
| with this spec | 275 | 6, under 4 pins | 36, under 14 departures | 233 |

`5d0e7f68` reads the base row exactly: 268 rows, 6 pinned under 4 pins, 34 departure rows under 14
departures, 228 that must pass, and `aws-client-retries` at 10 rows.

**The literals that move**, in `tests/test_conformance_gate.py` at `5d0e7f68`:

- The test name `test_the_variants_are_the_55_of_fr_001_and_spec_093_each_in_one_of_the_ten_classes`
  (:70) and its docstring's count, `len(cc.VARIANTS) == 55` (:72) and `len(built) == 55` (:230)
  become 56.
- The summed `PARAMETERS` length, 173 (:147), becomes 176. The class count, 39 (:146), stays.
- `len(table) == 268` and the unique-id count (:249-250) become 275, and `"keyed-reply": 28`
  (:253) becomes 35. The rows test's name,
  `test_the_corpus_generates_the_268_rows_of_fr_002_and_spec_093` (:241), carries 268 too and
  takes 275, and its docstring, which counts SPEC-093 FR-005's 12 rows, gains SPEC-100's 7.
- `checked == 52` (:292) counts every `clean` row but `NullSink-clean`, and `len(green_clean) ==
  52` (:1017) counts every `clean` row with no pin and no departure. `NullSink-clean` is the only
  `clean` row with either, so both count the same 52 rows. `SNSSink[fifo]-clean` is green, so both
  become 53.
- In `test_the_register_holds_the_totals_of_fr_004`, `len(departing) == 34` (:1113) becomes 36
  and the must-pass 228 (:1114) becomes 233, and its docstring gains SPEC-100's step. The pins, 4
  (:1108), the departures, 14 (:1109), and the pinned rows, 6 (:1112), stay.

#### Acceptance Criteria:

- [ ] `VARIANTS` holds `SNSSink[standard]` and `SNSSink[fifo]` and no bare `SNSSink`, and its count
      literal is one more than the base's (invariants 2 and 7).
- [ ] Each `SNSSink[fifo]-<fault>` row has the same verdict as `SQSSink[fifo]-<fault>`, and a test
      asserts this for all seven faults rather than listing seven literals (invariants 2, 6 and 7).
- [ ] Every row that was `SNSSink-<fault>` before reads the same verdict as
      `SNSSink[standard]-<fault>`, and no pin or departure names a bare `SNSSink` row (invariants 2
      and 7). The suite cannot assert the first half against a name it no longer has, so it is
      settled by the seven `SNSSink-<fault>` verdicts read through `cb.run_row` on the base at the
      build's start, compared with the verdicts `tests/test_conformance.py` holds the renamed rows
      to after it, and the delivery doc records both readings.
- [ ] The corpus's literals read the base's totals plus 7 rows, 2 departure rows and 5 must-pass
      rows, with pins, pinned rows and departures unchanged. `aws-client-retries` names
      `SNSSink[standard]` where it named `SNSSink` and gains `SNSSink[fifo]`'s `refused-once` and
      `refused-always`, 12 rows. The two counts of `clean` rows read 53 (invariants 2 and 7).
- [ ] The parameter roster test passes with `SNSSink`'s three new parameters (invariant 7).

### FR-007: The record says what a FIFO topic gets, what it still does not, and what moved

#### Description:

These are the completion ritual's edits.

- **`docs/invariants.md`.**
  - Invariant 13's narrative gains SPEC-100: on `SQSSink` and `SNSSink`, a `fifo` that is not a
    `bool` or `None` is refused with `TypeError`, and `fifo=True` on a queue URL or topic ARN not
    ending in `.fifo`, or an id argument on a sink that is not FIFO, with `ValueError`, before any
    client is built. Its *Recorded exceptions* gain `fifo=False` on a name ending in `.fifo`,
    accepted because AWS does not say a standard name cannot end so, whose every send to a real
    FIFO destination fails, counted and announced (SPEC-016 FR-006). Its *Guarded* line names
    `tests/test_sinks_sns_fifo.py` and `tests/test_sinks_sqs_fifo.py` for these.
  - Invariant 2's "an SQS id callable's per-event failure when its siblings were delivered"
    becomes "an SQS or SNS FIFO id callable's", naming SPEC-100.
  - Invariant 7's "an SQS batch of an id failure and an oversized or unencodable event" becomes
    "an SQS or SNS FIFO batch", naming SPEC-100. A FIFO `SNSSink` given one event whose callable
    raises and one oversized event returns with both counted and nothing sent (FR-002).
- **§12.**
  - SPEC-095's item *`SNSSink` cannot write an SNS FIFO topic* is struck, dated and naming
    SPEC-100.
  - Its closure says that FR-003 found and closed the de-duplication id truncation on `SQSSink`,
    with case F's numbers.
- **Decisions in `docs/decisions/sink-delivery/`.**
  - **A new decision, with fence and entry: *A de-duplication id too long to send is sent as its
    digest, never as a prefix*.** It records FR-003, case F, and the two rejected alternatives.
  - ***A FIFO message group is a trace, not the process*** gains a dated History entry: the rule
    now covers `SNSSink`, through the shared module.
  - ***A sink round resends only what failed, and an SQS FIFO group keeps its order*** is refined
    in place, with a dated History entry. The hold covers both FIFO sinks, so the "do NOT" names
    `SQSSink` and `SNSSink` FIFO. The guarantee is stated as FR-004 measured it: across calls, and
    not inside one.
  - ***A bad argument is floored if it works and refused if it is already broken; a new one is
    refused*** gains a dated History entry, Andrew's call: FR-001's three refusals on both sinks,
    `fifo`'s type for SPEC-065's reason, `fifo=True` on a name not ending in `.fifo` as already
    broken, and an id argument on a sink that is not FIFO as an argument the chosen backend cannot
    use, which on `SQSSink` refuses a call that delivered; and `fifo=False` on a `.fifo` name as
    the accepted exception. Its fence names the `fifo` refusal.
  - ***An argument door asks the type, never the value*** says in its fence that "A guard holds the
    doors as an exact set of ten", and in its body (line 59) that the derived set "must equal ten
    written out by name", while `tests/test_type_door_roster.py` holds thirteen and FR-001 makes it
    fourteen. Both sentences take a count-free wording, the set written out by name in that test,
    and the decision gains a dated History entry naming `_fifo.judge`.
  - ***No shipped sink declares itself idempotent***: its line 15, "(SNS documents de-duplication
    for FIFO topics only, which `SNSSink` cannot write)", is rewritten to say that a FIFO
    `SNSSink` now exists and fails the conditions below. The decision also gains a dated History
    entry naming a FIFO `SNSSink` against SPEC-095's three conditions:
    1. It fails the first. The topic absorbs a copy only inside its interval, and
       `spec095_fifo_window.py` shows that the engine's second round can start more than 300 s
       after the first when each call ends after botocore's five attempts at its 60-second read
       timeout. SNSSink uses the same client stack.
    2. It meets the second within a run (FR-002), and across runs only with a stable source.
    3. It fails the third: `tests/integration/docker-compose.yml` runs no SNS service, and moto's
       SNS does not de-duplicate (FR-002).
- **README.**
  - The `SNSSink` row of the AWS table gains the three arguments, and loses "a FIFO topic is not
    supported yet".
  - Three sentences that name `SQSSink` alone name both FIFO sinks: "An SQS **FIFO** message group
    keeps its order" (README :949-950), "an `SQSSink` FIFO message held behind an earlier one is
    never released" (:1391), and "a FIFO `SQSSink` … an id callable" (:1502).
  - A *FIFO topics* paragraph follows *FIFO queues*: the `.fifo` ARN, the defaults, that a
    resend's copy is absorbed only within the topic's 5-minute interval and, under
    `FifoThroughputScope=MessageGroup`, only with the same group, and that a callable id is stable
    across the library's resends but not across a caller's own whole-batch resend.
  - *Ordering holds for the sink's own retries* is corrected to say "a later call", per FR-004,
    for both sinks.
  - The over-long de-duplication id is described as FR-003 has it.
  - *Pass `fifo=True` or `fifo=False` to override the URL-based detection* is rewritten for both
    sinks: `fifo=False` overrides a `.fifo` name, `fifo=True` needs one, and an id argument on a
    standard queue or topic is refused.
- **The release notes**, in the pending `docs/release-notes/v1.2.1.md` if it is on `main` when
  this spec completes, and otherwise under a "Release notes owed" heading in the delivery doc:
  - *Added*: `SNSSink` writes FIFO topics, and the three arguments.
  - *Fixed*: an `SQSSink` de-duplication id over 128 characters no longer makes two events share
    one id.
  - *Fixed*: an `SQSSink` id callable answering a non-`str` with a `strip`, or a lone surrogate,
    no longer loses its whole round.
  - The compatibility list gains:
    - **`SQSSink` refuses at construction three calls that 1.x accepted.** A `fifo` that is not a
      `bool` or `None` raises `TypeError`, where 1.x read it by truthiness. `fifo=True` on a queue
      URL not ending in `.fifo` raises `ValueError`, where 1.x built a sink that could never
      deliver, since AWS requires every FIFO queue name to end in `.fifo`. `message_group_id` or
      `message_deduplication_id` on a queue that is not FIFO raises `ValueError`, where 1.x
      ignored it and delivered. `fifo=False` on a `.fifo` URL is still accepted;
    - code that reads an over-long `MessageDeduplicationId` downstream, which now sees a digest;
    - code that points a 1.x `SNSSink` at a `.fifo` ARN and relies on every publish failing;
    - code that matches the text of an `SNSSink` loss line, which can now be the held or the
      id-callable line.
  - The three sites in `docs/release-notes/v1.2.1.md` that SPEC-100 makes false:
    - :16-18, "Five sinks gain one keyword-only argument each … `SQSSink` and `SNSSink` take
      `max_message_bytes`", is rewritten to say that `SNSSink` also takes the three FIFO
      arguments;
    - :987-989, "`SQSSink` and `SNSSink` each gain one keyword-only argument", is rewritten the
      same way;
    - the section *`SNSSink` cannot write an SNS FIFO topic* (:932-935), "… unchanged from 1.2.0",
      is removed, since the *Added* entry replaces it.
- **`docs/component-inventory.md`**: a row for `sinks/_fifo.py`, and the `SQSSink` and AWS-sinks
  rows name it. The *chunk and transport helpers* row, which says `chunk_items` is "used by the
  ClickHouse, Firehose, Kinesis, Postgres, SNS and, since SPEC-071, SQS sinks", names `_fifo` in
  place of the SNS and SQS sinks, as `tests/test_chunk_helper_roster.py` then holds it (FR-002).
- **The docstrings** of `SNSSink`, its `__init__`, `_round`, `_send` and `_register_drops`, and of
  `SQSSink`'s methods that now delegate, say what they share and with whom. Those of
  `SQSSink._group_id` and `SQSSink._dedup_id` say that nothing in the sink calls them, that
  overriding either changes nothing sent, and that as private methods they carry no
  compatibility promise (FR-002). The
  `GroupIdSource` and `DedupIdSource` docstrings, wrong today, are corrected where they move to
  `_fifo.py`: `GroupIdSource`'s (`sqs.py:71`) calls a `str` "an event key to read", but a constant
  is the group itself (:592-593), and `DedupIdSource`'s (:74) says `None` means "a minted UUID",
  but it means the event's `log_id` (:619).

#### Acceptance Criteria:

- [ ] `docs/invariants.md` carries the three edits above: the thirteenth entry names SPEC-100's
      three refusals in its narrative and `fifo=False` on a `.fifo` name among its recorded
      exceptions, the second's exception names an SQS or SNS FIFO id callable, and the seventh's
      an SQS or SNS FIFO batch.
- [ ] §12 strikes *`SNSSink` cannot write an SNS FIFO topic*, dated and naming SPEC-100, and its
      closure carries FR-003's finding with case F's numbers.
- [ ] The new decision file exists with fence and entry and a dated History. The five refined
      decisions carry dated History entries, the hold decision's "do NOT" names both FIFO sinks,
      the bad-argument decision's fence names the `fifo` refusal, the type-door decision states no
      count of doors in its fence or its body, and the idempotency decision no longer says
      `SNSSink` cannot write a FIFO topic.
- [ ] The README's `SNSSink` row and *FIFO topics* paragraph exist, and the row no longer says a
      FIFO topic is not supported. No README sentence says a FIFO group's order holds inside one
      call, that the de-duplication id is truncated, or that `fifo=True` overrides a name without
      `.fifo`, and the three sentences at :949-950, :1391 and :1502 name both FIFO sinks.
- [ ] The release notes carry the *Added* entry, the two *Fixed* entries and the four
      compatibility entries, the first naming each of `SQSSink`'s three refusals. No sentence there says that
      `SNSSink` gains only `max_message_bytes`, that `SQSSink` and `SNSSink` each gain one
      argument, or that `SNSSink` cannot write a FIFO topic.
- [ ] `_fifo.GroupIdSource`'s docstring says a `str` is the group itself, and
      `_fifo.DedupIdSource`'s says `None` means the event's `log_id`.
- [ ] This FR serves no invariant: it is the record of FR-001 to FR-006, and the promises it
      describes are theirs.

---

## Data Model

```python
# src/log_foundry/sinks/_fifo.py — new, private
DEFAULT_GROUP_ID = "log-foundry"           # sqs.DEFAULT_GROUP_ID is this object
MAX_ID_LEN = 128                            # sqs.MAX_ID_LEN is this object
GroupIdSource = str | Callable[[dict[str, object]], str] | None
DedupIdSource = Callable[[dict[str, object]], str] | None

class Prepared(NamedTuple):           # was sqs._Prepared
    position: int
    body: str
    group_id: str | None
    dedup_id: str | None
    size: int                         # the UTF-8 body and, on FIFO, both ids

@dataclass(slots=True)
class Entry:                          # was sqs._Entry; one per event occurrence per run
    body: str = ""
    group_id: str | None = None
    dedup_id: str | None = None
    size: int = 0
    drop: str | None = None           # "oversize" | "id failure" | "unencodable"
    error: Exception | None = None
    landed: bool = False

class Plan(NamedTuple):               # was sqs._Plan (sqs.py:103-120)
    chunks: list[list[Prepared]]      # each valid for one request, built with chunk_items
    dropped: dict[int, str]           # position -> "oversize" | "id failure" | "unencodable"
    refused: Exception | None         # the first id exception, when every event's raised
    limit: int                        # the run's limit; the oversize lines name it (SPEC-096)

# sqs.py keeps _Prepared, _Plan, _Entry, _bounded, _hold and _block bound to these objects.
# The per-run cache lives in run_context().notes(<the sink>) under "prepared", "prepared_round"
# and "limit", as SQSSink._chunks keeps it today (sqs.py:710-716).

# src/log_foundry/sinks/sns.py
class SNSSink:
    fifo: bool                        # new public attribute, as SQSSink.fifo
    message_group_id: GroupIdSource   # new
    message_deduplication_id: DedupIdSource   # new
__all__ = ["DedupIdSource", "GroupIdSource", "SNSSink"]
```

## API / Interface Contract

```python
# src/log_foundry/sinks/_fifo.py
def judge(owner: str, kind: str, name: str, destination: str, fifo: object,
          group: object, dedup: object) -> bool: ...   # FR-001; returns the sink's fifo
def bounded(raw: str, fallback: str) -> str: ...      # was sqs._bounded; the group id's cut
def group_id(source: GroupIdSource, event: dict[str, object]) -> str: ...
def dedup_id(source: DedupIdSource, event: dict[str, object]) -> str: ...   # digest past 128
def prepare(body: str | None, event: dict[str, object], *, fifo: bool,
            group: GroupIdSource, dedup: DedupIdSource, limit: int) -> Entry: ...
class PrepareOne(Protocol):
    def __call__(self, event: dict[str, object], body: str | None, *, limit: int) -> Entry: ...
def plan_round(owner: object, batch: list[dict[str, object]], bodies: dict[int, str],
               prepare_one: PrepareOne, *, max_count: int, limit: int) -> Plan: ...
    # plan_round calls prepare_one(event, body, limit=limit): the event, its body or None, and
    # the run's limit by keyword. SQSSink._prepare(event, body, *, limit) (sqs.py:757) keeps that
    # signature unchanged, and SNSSink._prepare has the same one. limit is the per-run value the
    # caller read once into notes["limit"] (max_message_bytes, else MAX_BYTES, at most 1 MiB),
    # and it bounds each entry's cost, each chunk's summed cost and Plan.limit. plan_round builds
    # Plan.chunks with chunk_items, so neither sink calls the helper itself (FR-002).
def hold(chunk: list[Prepared], blocked: set[str], sends: list[int], limit: int) -> list[bool]: ...
def block(sent: list[Prepared], fates: list[Outcome], sends: list[int],
          blocked: set[str], limit: int) -> None: ...

# src/log_foundry/sinks/sns.py
SNSSink(
    topic_arn: str,
    *,
    client: Any = None,
    max_retries: int = 3,
    max_message_bytes: int | None = None,          # SPEC-096
    fifo: bool | None = None,                      # new
    message_group_id: GroupIdSource = None,        # new
    message_deduplication_id: DedupIdSource = None,  # new
) -> None

# Example: a FIFO topic, grouped by trace, de-duplicated by log_id
SNSSink("arn:aws:sns:us-east-1:123456789012:logs.fifo")
# A tenant group
SNSSink(FIFO_ARN, message_group_id=lambda e: str(e["fields"].get("tenant_id") or e["trace_id"]))
```

## File & Folder Structure

```
src/log_foundry/sinks/
├── _fifo.py                       # new: the judgement, ids, the per-run entry cache, hold and block
├── sqs.py                         # delegates to _fifo; its constants and methods stay
└── sns.py                         # FR-001 to FR-004
tests/
├── test_sinks_sns_fifo.py         # FR-001 to FR-005
├── test_sinks_sqs_fifo.py         # FR-001 and FR-003 rewrite one test each
├── test_sinks_sqs_chunks.py, test_sinks_sqs_id_failures.py   # FR-001: a URL, a refusal
├── test_type_doors.py, test_type_door_roster.py, test_public_surface.py,
│   test_sinks_encode.py, test_retry_safe_drop_counts_prose.py,
│   test_sinks_message_limit.py, test_chunk_helper_roster.py,
│   typed_consumer/accepts.py      # rosters (FR-001, FR-002)
└── conformance*.py                # FR-006, SPEC-092's files
```

## Implementation Phases

### Phase 1: The shared module and the arguments (FR-001, FR-002, FR-003)

- Move SQS's FIFO pieces into `_fifo.py` with `SQSSink`'s names kept and its suite green; add
  `judge` and its refusals on both sinks, with the tests FR-001 names; add the digest and rewrite
  its one test; give `SNSSink` the arguments, the cache, the ids and the
  costing; update the rosters.

### Phase 2: Order and refusals (FR-004, FR-005)

- `SNSSink._round` holds through `hold` and `block`; the ordering, refusal and throttle tests; the
  three mutants FR-002 to FR-004 name; then, with the hold in place, re-run
  `spec100_moto_shapes.py` cases E and F against the built tree in a scratch environment for the
  delivery doc.

### Phase 3: The corpus and the record (FR-006, FR-007)

- The `SNSSink[standard]` rename, the `SNSSink[fifo]` variant and double, `PARAMETERS`; then the
  record.

## Evidence

**The prototype.** `docs/audits/2026-11-14-probes/sinks/spec100_prototype.patch` applies with
`patch -p1` to `git archive 03dbf502 src tests docs/audits/2026-11-14-probes`. It covers FR-001's
constructor and `judge` on both sinks, FR-002's shared module and costing, FR-003's digest and
FR-004's hold. It leaves out the build's tests and rosters and SPEC-093's and SPEC-095's
counters, and it predates SPEC-096's `max_message_bytes` and the `limit` field of `_Plan`: its
`Plan` carries `prepared` rather than `chunks` and `limit`, its `prepare_one` takes no limit, and
it costs `SNSSink`'s entries against `self.MAX_BYTES` in place of a two-argument
`self._bodies(pairs, context.drops(self))`. None of that matches the current tree, where
`SNSSink._bodies(encoded, pending, limit)` takes three arguments and is handed
`context.drops(self)` and the run's limit (`sns.py:332`, :392), and `_Plan` carries `chunks` and
`limit`. The build ports the patch onto that code, as the Data Model and the Interface Contract
state it. Measured on macOS with Python 3.13.1; the moto probes used boto3 1.44.4 and moto 5.2.5
in a scratch environment.

- **The suite on the prototype.** `ruff check`, `mypy --strict` over `src/log_foundry/sinks/` and
  `docstring-lint.py` are clean. The suite, run over a whole-tree export of `2db83022`, whose `src`
  and `tests` are `03dbf502`'s, fails exactly sixteen tests, all of which pass on the same export
  unpatched. Each is a roster, a pin or a fixture this spec names:
  - `test_sinks_sqs_fifo.py::test_over_long_dedup_id_is_truncated` (FR-003 rewrites it);
  - `test_sinks_encode.py::test_the_public_surface_grows_only_by_five_losses` (FR-001's three
    attributes);
  - `test_retry_safe_drop_counts_prose.py`'s two `SNSSink._bodies` cases (FR-002's rename);
  - `test_type_door_roster.py::test_the_real_tree_is_clean_and_holds_the_pinned_doors` and
    `::test_a_transform_result_raise_reverted_to_the_raw_read_is_reported`, each reporting the
    unexpected door `sinks._fifo.judge` (FR-001);
  - `test_type_doors.py::test_a_callable_or_none_is_still_accepted_where_optional`, its `lambda`
    and `class` cases, refused on a standard URL (FR-001);
  - six in `test_sinks_sqs_chunks.py`, `fifo=True` on `https://q` through `_sink`:
    `test_fifo_ids_push_a_pair_over_the_byte_limit`,
    `test_fifo_ids_push_a_single_event_over_and_it_is_dropped`,
    `test_every_id_callable_raising_reraises_the_first_unchanged_and_counts_nothing`,
    `test_id_failures_beside_an_unencodable_event_settle_counted`,
    `test_an_id_failure_beside_an_oversize_event_settles_counted` and
    `test_an_unencodable_event_and_an_id_failure_through_emit_settle_counted` (FR-001);
  - `test_sinks_sqs_fifo.py::test_explicit_fifo_true_forces_it_on_a_plain_url` and
    `test_sinks_sqs_id_failures.py::test_a_standard_queue_never_consults_the_callables` (FR-001).

  With only `_sink`'s URL, the accepted-values test's URL and `EXPECTED_DOORS` changed,
  `test_sinks_sqs_chunks.py`, `test_type_doors.py` and `test_type_door_roster.py` pass on the
  prototype, 138 tests.

**Today, on moto** (`spec100_moto_shapes.py`, on `03dbf502`):

| Case | Reading |
|---|---|
| A, `SNSSink` on a FIFO topic, content-based de-duplication off and on | `emit` of 3 raised `SinkDeliveryError` ("delivered none of 3 event(s): rejected 3"); `SinkLosses(dropped=0, failed=3)`; one line, `lost 3 message(s); SNSSink, 1 attempt(s), InvalidParameterException` |
| B, no group id | the call raised `InvalidParameter`, HTTP 400 |
| B, no de-duplication id | `Failed`: `InvalidParameter`, `SenderFault: true`; `Successful` empty |
| B, de-duplication id on a standard topic | `Failed`, `SenderFault: true` |
| B, group id only on a standard topic | `Successful` |
| B, empty group id | neither list names the entry |
| B, 129-character ids, an id with a space and `é` | `Successful`: moto checks neither length nor characters |
| C, one de-duplication id published twice | both `Successful`; the FIFO SQS subscriber held 1, the standard SQS subscriber 2 |
| D, a 262,144-character body with two 128-character ids; 262,145 characters; 131,073 × `é` (262,146 bytes) | taken; refused as a sender fault, "Message too long"; taken |
| F, `SQSSink` to a FIFO queue, two 200-character de-duplication answers sharing 128 | the queue held 1 of 2; `SinkLosses(dropped=0, failed=0)`; no line |

**On the prototype**, the same probe:

- A: both `emit`s returned, with nothing lost.
- E: after a lost first reply there were four calls, each carrying the same de-duplication id for
  each event. The FIFO subscriber held 3 messages for 3 events, and the standard subscriber held 6.
- F: the queue held 2 of 2.

**The refusals** (`spec100_refusals.py`, on `03dbf502` for `SQSSink` and on the prototype for
both):

- On `03dbf502`, `SQSSink` accepts `fifo=1`, `fifo="yes"` and a value claiming `bool`, storing each
  as given; `fifo=True` on a standard URL; and an id argument on a standard URL or with
  `fifo=False`.
- On the prototype, both sinks refuse each of those, the `fifo` types with `TypeError` naming the
  real type and the rest with `ValueError` naming the arguments, and accept `fifo=True` and
  `fifo=False` on a `.fifo` name.
- A call with two faults reads the same on both trees: `max_retries=2.5` with `fifo="x"` raises the
  `max_retries` refusal, and `fifo="x"` with a group of `5` the `message_group_id` one. The four
  `_REFUSED` shapes and the blank group raise the base's text on both sinks.
- Case H, a FIFO batch of one event whose callable raises and one oversized event: on both trees
  and both sinks `emit` returned with no call made, `SinkLosses(dropped=2, failed=0)`, and one
  line for each.

**The ids** (`spec100_ids_once.py`, on `03dbf502` for `SQSSink` and on the prototype for both):

- A: the de-duplication ids `L0` and `L1` and the groups `T0` and `T1` on the wire, and the
  resend's the same.
- B: the callables ran four times for two events over two rounds, and the resend carried the first
  round's `('d1', 'g0')`.
- C: a callable gave `d0` in both rounds of the first run and `d1` in a second run; the default
  gave `L0` in all three calls.
- D: the minted fallback was the same within a run and new in a second run.
- E: on `03dbf502` both events went out with the same 128-character id; on the prototype, with
  two different 64-character digests.
- F: a blank group sent `log-foundry`, and a blank de-duplication answer sent a 32-character id. A
  `None` answer for one event dropped it with `lost 1 event(s); <Sink>, a FIFO id callable raised
  AttributeError`, and for every event made `emit` raise `AttributeError` with nothing sent. No
  entry ever carried one id without the other.

**The order** (`spec100_ordering.py`, on the prototype; `SQSSink`'s readings are identical on
`03dbf502`):

| Case | `SQSSink` and `SNSSink` | `SNSSink`, hold off |
|---|---|---|
| A, `a0` fails once, one call | 2 calls; `a1` to `a4` land before `a0` | the same |
| B, `a0` fails on every send | 4 calls; `a0` counted once in `failed`, with the abandoned line | the same |
| C, every entry of the first call fails once | 4 calls; group A lands in order, one per round | 2 calls; group A in order |
| D, twelve events of one group, `a00` fails once | 3 calls; `a01` to `a09` (same call) land first, then `a00`, then `a10` and `a11` | 3 calls; `a10` and `a11` land before `a00` |
| E, the same twelve, `a00` fails on every send | 4 calls; `a10` and `a11` held until the last round, then sent in `a00`'s final call; `a00` counted once in `failed` | 5 calls; `a10` and `a11` land in the first round |

**The cost** (`spec100_costing.py`): on a FIFO destination a 262,104-byte body costs 262,168 with
its two 32-character ids. `SQSSink` on `03dbf502` and both sinks on the prototype drop it with one
line, and both send it to a standard destination. With a limit of 1,000 set on the instance, a
960-byte body costs 1,024 and is dropped on both.

**The corpus** (`spec100_corpus_rows.py`). SPEC-092's prototype predates SPEC-090's `exit_batch`
keyword, so on `03dbf502` every row reads `harness-error:batch-count`. The probe forwards that one
keyword where the prototype does not already (SPEC-096's build made it forward the keyword at
`dd77b3fb`), and changes nothing else. On `03dbf502`, `SQSSink` and `SNSSink[standard]` read as SPEC-092
recorded them. On the prototype, `SNSSink[fifo]` reads as `SQSSink[fifo]` on all seven faults.

**Read live on 2026-11-17** by the session that started this spec, recorded in its hand-over note.
This session did not re-read the pages, and the quotation marks are the note's:

- `PublishBatchRequestEntry`, `MessageGroupId` on a FIFO topic: "If you do not provide a
  MessageGroupId, the action fails."
  (https://docs.aws.amazon.com/sns/latest/api/API_PublishBatchRequestEntry.html). On a standard
  topic it is optional, and is forwarded only to SQS standard subscriptions that use fair queues.
  Each id is up to 128 characters, alphanumeric and ASCII punctuation.
- `MessageDeduplicationId`
  (https://docs.aws.amazon.com/sns/latest/api/API_PublishBatchRequestEntry.html and
  https://docs.aws.amazon.com/sns/latest/api/API_Publish.html):
  - The id is required unless content-based de-duplication is on, which hashes the body only, and
    an explicit id overrides it.
  - The interval is a "5 minute minimum deduplication interval".
  - Duplicates "are accepted successfully but aren't delivered".
  - For a resend after the interval, "Amazon SNS can't detect duplicate messages".
  - Its scope follows `FifoThroughputScope`: `Topic`, the default, or `MessageGroup`, which
    de-duplicates within each group.
- `PublishBatch` (https://docs.aws.amazon.com/sns/latest/api/API_PublishBatch.html):
  - entries are published in order within a batch, in FIFO order within a group, and in no order
    across groups;
  - the size counted is the body and the attributes, with no mention of the ids;
  - the batch's sum is bounded by `MaximumMessageSize`, and `BatchRequestTooLong` refuses it.
- `CreateTopic` (https://docs.aws.amazon.com/sns/latest/api/API_CreateTopic.html): a FIFO topic's
  name must end in `.fifo`, and the converse is not stated. `FifoThroughputScope` `Topic` allows
  3,000 messages per second or 20 MB per second.
- SNS quotas (https://docs.aws.amazon.com/general/latest/gr/sns.html): 300 messages per second
  per message group.
- Common errors (https://docs.aws.amazon.com/sns/latest/api/CommonErrors.html): `ThrottlingException`,
  HTTP 400, not tied in the docs to the FIFO limits.
- None of these pages names an error code for a missing or invalid group or de-duplication id.

SPEC-095's *Evidence* quotes the same `PublishBatchRequestEntry` sentences, read on 2026-11-16.

## Revision history

- **Authored on 2026-11-17 against `03dbf502`.** An earlier session paused before writing
  anything. Its measurements were re-run here. It had named a confound it had not isolated: the
  FIFO SQS subscriber, not SNS, might be what kept one copy. Case C settles it. The subscriber
  de-duplicated, and moto's SNS forwarded both copies.
- **Andrew's answers to two product calls, 2026-11-17.** Each was put to him with the measurements
  and the alternatives:
  - **The hold on SNS FIFO** (FR-004): "Hold, as SQS (Recommended)". The alternative was no hold.
  - **The over-long de-duplication id** (FR-003): "Send its SHA-256 digest (Recommended)", on both
    sinks. The alternatives were to keep the truncation and file the loss in §12, or to drop the
    event as an id failure.
- **Revised after the spec review, 2026-11-17, with Andrew's answers to two more product calls.**
  - **`fifo=`** (FR-001): "Refuse both in SPEC-100 (Recommended)". On both sinks, `fifo=True`
    with a name not ending in `.fifo` raises `ValueError`, a `fifo` that is not a `bool` raises
    `TypeError`, and `fifo=False` on a `.fifo` name stays accepted as a recorded exception to
    invariant 13. This replaces the Out of Scope bullet that left `fifo`'s type unjudged.
  - **FIFO id arguments on a standard destination** (FR-001): "Refuse at construction
    (Recommended)", on both sinks. This replaces the Out of Scope sentence that said
    `message_group_id` has no effect on a standard topic.
  - The review's findings fixed in place: FR-001 and FR-005 cite invariant 13 and FR-005's
    `fifo=True`-on-a-standard-ARN criterion became a construction refusal; FR-007 edits invariants
    2, 7 and 13 and the bad-argument decision, and states `SQSSink`'s 1.x change in the release
    notes; FR-006 states the corpus totals; FR-003 says the digest is of the stripped answer;
    FR-002's kept-names sentence names only the names that stay, and its de-duplication stand-in
    criterion runs with callables that answer differently on every call; the Interface Contract
    names `judge` and `bounded`, and the Data Model drops the `Final` the prototype does not
    have. The prototype was revised to match, and *Evidence* re-measured.
- **Decided in session, by the authoring session, not by Andrew.**
  - **The ids count toward the message limit on SNS too**, as SPEC-096 kept them on SQS.
  - **One shared module rather than a second copy**, so the twin cannot diverge (invariant 6),
    with `encode_round` left in each sink's module for `tests/test_encode.py`'s roster.
  - ~~**`fifo`'s type is not judged**~~ Struck: Andrew's answer above.
  - **The refusals live in one helper, `_fifo.judge`, which judges types before values**, so a
    call refused at the base keeps its exact error and the type-door roster gains one door rather
    than two.
  - **No `log_foundry_idempotent`**, by SPEC-095's own conditions (FR-007).
- **Re-verified against `5d0e7f68` at the build's start, 2026-11-24.** That `main` carries
  SPEC-093 to SPEC-096, and every `file:line` now cites it. The corrections bring the spec's
  claims to the code, and none widens its scope:
  - FR-002 said SQS's FIFO types resolve on `sqs` only under `TYPE_CHECKING`. All six are runtime
    names and a test imports `sqs._Plan`, so `sqs.py` keeps each old name bound to the shared
    object, and the cache keeps its `"prepared"` key and owner for the per-round mutant test.
  - The Data Model's `Plan` is today's `_Plan`, with `chunks` and SPEC-096's `limit`, and
    `plan_round` and its `prepare_one` take the run's limit.
  - FR-001 states each sink's refusal order at the base, with `judge` after all of it, adds the
    `max_message_bytes` example and keeps the blank group's text as the base's.
  - FR-006 names each corpus literal that moves, including the two counts of `clean` rows, and
    where `SNSSink`'s id arguments go in `NEUTRAL`.
  - FR-007 replaces a release-notes bullet that targeted no sentence with the three sites the spec
    makes false, and adds the type-door decision's count, the idempotency decision's line 15,
    three SQS-only README sentences and the two alias docstrings.
  - FR-001 moves the typed consumer's standard-URL id call to a `.fifo` URL, and *Evidence* says
    what the prototype predates.
  - **Planning the build against `5d0e7f68` found eight more.** The first is the orchestrator's
    in-session call under the triage rule, an obvious bug fix inside the spec's scope, because
    FR-002 already claimed the behaviour. None of the other seven widens the scope.
    1. FR-002 said a non-`str` id answer costs only its event. An answer with a working `strip`,
       such as `bytes`, and a `str` holding a lone surrogate were costed outside the `try` and
       lost the whole round. `_fifo.prepare` costs the ids inside it; FR-002 gains a criterion,
       and FR-007 a *Fixed* entry, and the Overview names the defect.
    2. Two rosters were missing. `test_sinks_message_limit.py`'s signature test asserts that
       `SNSSink`'s signature ends at `max_message_bytes` (FR-001). `test_chunk_helper_roster.py`
       derives the modules that call `chunk_items`, which moves into `_fifo.plan_round`, so
       `_fifo` replaces both `sqs` and `sns` there and in the inventory's chunk-helper row
       (FR-002, FR-007).
    3. `test_type_door_roster.py` states the door count twice, not once (FR-001).
    4. The Interface Contract typed `prepare_one`'s limit as positional, where `SQSSink._prepare`
       takes it by keyword; `plan_round` now calls it by keyword through a `PrepareOne` protocol.
    5. FR-006 missed the rows test's name, which carries 268.
    6. FR-006's renamed-row criterion could not be asserted inside the suite; it is settled by
       the base readings against the verdicts after, both recorded in the delivery doc.
    7. `SQSSink._group_id` and `_dedup_id` become uncalled delegates, and FR-002 and their
       docstrings say so.
    8. The moto re-run of cases E and F moves to the end of Phase 2, so case E is read with the
       hold in place.
