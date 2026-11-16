---
type: Spec
id: SPEC-096
title: "SQSSink and SNSSink Take Their Message Limit as an Argument, and Never Send Past the Service's"
description: "Gives SQSSink and SNSSink a keyword-only max_message_bytes argument, judged at construction against the 1,024 to 1,048,576 range a queue's or topic's MaximumMessageSize takes, so a caller can match the destination's own limit; the default stays 1.x's 256 KiB through MAX_BYTES, and no message or request is ever sized past the service's 1 MiB."
spec_status: Draft
depends_on: [SPEC-005, SPEC-010, SPEC-016, SPEC-049, SPEC-051, SPEC-066, SPEC-071, SPEC-074]
arc: audit-2026-11-14
arc_order: 12
---

## Overview

`SQSSink` and `SNSSink` drop any event whose message is larger than 256 KiB, count it in
`dropped_oversized` and say so on stderr (invariant 2). That limit was the service's when the
sinks were written. On 2025-08-04 AWS raised SQS's to 1 MiB, and a queue's own limit, its
`MaximumMessageSize`, now runs from 1,024 bytes to 1 MiB and defaults to 1 MiB on a new queue. An
SNS topic's limit runs over the same range but still defaults to 256 KiB. So today a caller whose
queue accepts 1 MiB loses every event between 256 KiB and 1 MiB, counted, and a caller whose queue
or topic is set below 256 KiB has events sent that the service refuses. When the service refuses
the whole call, the events that would have fit go with them. Neither caller can do anything about
it, because the limit is a class attribute that no argument reaches. This spec gives both sinks a
keyword-only `max_message_bytes` argument. A caller copies their destination's
`MaximumMessageSize` into it, and the sink applies it to each message and to each request. The
default stays 256 KiB, so a 1.x caller sees no change. Whatever the limit, no message and no
request is ever sized past the service's 1 MiB.

## Scope

### In Scope

- `max_message_bytes: int | None = None`, keyword-only and appended last, on `SQSSink` and
  `SNSSink`, judged at construction under the bad-argument decision (FR-001).
- `SQSSink` applies the limit to each message, FIFO ids included as today, and to each request's
  sum, capped at 1,048,576 bytes (FR-002).
- `SNSSink` applies it the same way (FR-003).
- `MAX_BYTES` keeps its value and its 1.x reach as the default limit (FR-004).
- The record: the README, the docstrings, `docs/release-notes/v1.2.1.md`, `docs/architecture.md`
  §8 and §12, one decision file, and SPEC-092's parameter roster if it has been built (FR-005).

### Out of Scope

- **Changing the default.** The limit stays 262,144 bytes unless a caller passes
  `max_message_bytes`. Andrew chose this on 2026-11-16, asked whether `SQSSink`'s default should
  stay 256 KiB or become 1 MiB: "Keep 256 KiB (Recommended)". *Evidence* gives the case for
  1,048,576, the service's default for a new queue, and the measured cost of that default against a
  queue still at 262,144.
- **Reading the destination's limit from AWS.** Asking `GetQueueAttributes` or
  `GetTopicAttributes` for `MaximumMessageSize` needs a permission a send-only role does not have,
  adds a network call to construction, and reads a value that "You can increase or decrease ... at
  any time" (SNS *Publishing large messages*). The caller says the limit; the sink holds it.
- **`KinesisSink`'s limits.** Kinesis now takes records of up to 10 MiB on a stream configured for
  them, but a stream's default is still 1 MiB, and its request cap is a second limit with its own
  question. `docs/architecture.md` §12 carries it as an open item, filed with this spec.
  `FirehoseSink`'s limits were read too and still match the service, which says they "cannot be
  changed".
- **Validating `MAX_BYTES`, or `max_message_bytes` assigned after construction.** `MAX_BYTES` is
  read per round when `max_message_bytes` is `None`, as in 1.x, and `sink.max_message_bytes` is a
  public attribute a caller can reassign, as `sink.max_retries` is. An assignment made after
  construction can be judged only on the drain thread, which invariant 13 forbids. Only the upper
  side of either is held, by the 1,048,576 cap (FR-002, FR-003).
- **Not counting the FIFO ids.** `SQSSink` costs a FIFO entry as its body plus its
  `MessageGroupId` and `MessageDeduplicationId` (SPEC-016 FR-005). The AWS pages say message
  attributes count toward the limit and do not say whether these two parameters do, so the sink
  keeps over-counting, by at most 256 bytes for ids AWS accepts (each is at most 128 characters of
  ASCII alphanumerics and punctuation), which drops rather than sends a message at the edge.
  `_bounded` truncates a derived id to 128 *characters*, so an id AWS would refuse anyway can cost
  more.
- **A separate request limit.** A request's sum is held to the same limit as a message, as today.
  SQS's batch total is 1 MiB whatever the queue's limit; holding it to the message limit costs
  extra requests below 1 MiB and keeps the default's requests byte for byte what 1.x sends.
- **A message the service refuses anyway.** If the caller's limit is above the destination's, the
  service's refusal is handled as today: a whole-call refusal costs its chunk and a `SenderFault`
  entry costs itself, counted in `failed` (SPEC-016, SPEC-070). That is the reason to pass the
  destination's limit.
- **Any change to the stderr line formats**, the counters, `MAX_BATCH`, the verdicts or the
  retry classification. SPEC-093 (its FR-003 gives both sinks `dropped_unadjudicated`, a sub-count
  of `failed`) and SPEC-095 (a count of resends), both Draft on `main`, touch these two sinks'
  constructor bodies, rounds and public-attribute rosters. This spec reads no counter either adds
  and adds none, so it does not depend on either, and no requirement here contradicts theirs.
  Whichever is built second rebases over the other's lines.

---

## Functional Requirements

### FR-001: Both sinks take `max_message_bytes`, judged where it is written

#### Description:

**The argument.** `SQSSink` and `SNSSink` each gain `max_message_bytes: int | None = None`,
keyword-only and appended after the last existing parameter, so every 1.x call binds as before
(invariant 10). `None` means the class's `MAX_BYTES`, read as FR-004 says. An `int` is the most
bytes one message may cost, and the most a request's messages may cost together.

**The name.** The house spelling of a byte bound is `max_<what>_bytes`: `HTTPSink(max_batch_bytes=)`,
`SocketTransport(max_datagram_bytes=)`, `configure(max_value_bytes=)`. The decision *A sink's
constructor keeps the vendor's own spelling* governs a keyword the sink forwards to its client.
This value is never forwarded; the sink applies it. The unit in the name also matters for a value
copied from AWS. The console shows *Maximum message size* in KiB, and the API's
`MaximumMessageSize` is in bytes.

**The judgement** follows *A bad argument is floored if it works and refused if it is already
broken; a new one is refused*. This is a new argument, with no working configuration of its own
to protect, so a value outside the range is refused, never floored. It is judged in each
constructor, after every refusal the constructor makes today, so a call refused today keeps its
exact error, and before `boto3` is imported or any client is built:

- `None` is accepted and stored.
- A value that is not an `int` (`float`, `str`, `Decimal`) is refused with the `TypeError` that
  `_retry.exact_int` raises, which names the class, `max_message_bytes` and the type.
- An `int`, `bool` or other `int` subclass is read through `exact_int`, so the exact builtin is
  what is compared and stored. An exact value below 1,024 or above 1,048,576 is refused with a
  `ValueError` naming the class, `max_message_bytes`, the value and both bounds.
- Anything else is stored as the exact `int`.

**The message renders the value through `sanitize._int_text`** (SPEC-066 FR-003), on the exact
`int`, never `f"got {max_message_bytes}"`: CPython refuses to turn an integer of more than 4,300
digits into text, so that f-string raised "Exceeds the limit (4300 digits) for integer string
conversion" out of the refusal itself for `max_message_bytes=10**5000`, naming neither the class
nor the argument. `_int_text` names such a value as `<int: ~5001 digits>`, the form every other
door's refusal uses. And it renders the *exact* value, so a subclass whose `__repr__` or
`__format__` lies cannot choose the text.

The range is the one AWS gives `MaximumMessageSize` for a queue ("An integer from 1,024 bytes (1
KiB) to 1,048,576 bytes (1 MiB)", `CreateQueue`) and for a topic ("Valid values are 1024 to
1048576", `SetTopicAttributes`). So every limit a destination can have is accepted, and nothing
it cannot have is. `True` is `1` and is refused by the range. `max_message_bytes` is not an option
a client is built from, so it is accepted beside `client=` (SPEC-071's injected-client rule does
not reach it).

**The twin.** The two constructors give one verdict on one value (invariant 6). The judgement is
written in each `__init__` (`exact_int` and then the range comparison) rather than in a helper in
another module. That keeps it visible to `tests/test_numeric_door_roster.py`, which follows one
call level within a module only.

#### Acceptance Criteria:

- [ ] `inspect.signature` of each class's `__init__` ends with `max_message_bytes`, keyword-only,
      default `None`, and every parameter before it keeps its 1.x name, kind and default
      (invariant 10).
- [ ] For each class, `max_message_bytes=1024`, `=1048576`, `=262144` and `=None` construct, and
      `sink.max_message_bytes` reads `1024`, `1048576`, `262144` and `None`. An `int` subclass
      holding `2048` whose `__lt__`, `__gt__`, `__le__` and `__ge__` lie is stored as the exact
      `int` 2048, `type(...) is int` (invariant 13).
- [ ] For each class, `int` subclasses holding `512` and `1048577`, whose `__lt__`, `__gt__`,
      `__le__` and `__ge__` all answer that the value is in range and whose `__repr__` and
      `__format__` return `"262144"`, are refused with `ValueError` whose text names `512` and
      `1048577` respectively. A build that range-checks the caller's object and stores the
      `exact_int` copy constructs here, and fails this criterion (invariant 13).
- [ ] For each class, `1023`, `1048577`, `0`, `-1` and `True` raise `ValueError` whose text names
      the class, `max_message_bytes`, the value, `1024` and `1048576`. `1.5e6`, `262144.0`,
      `"262144"` and `Decimal(262144)` raise `TypeError` naming `max_message_bytes` and the type.
      Each is raised before the client is built: with `client=None` and the counted `boto3`
      stand-in of `tests/test_numeric_doors.py` (`_boto3`, the `SQSSink` and `SNSSink` rows of
      `_BUILDS`), the stand-in records one build for a good value and none for each refused one
      (invariant 13).
- [ ] For each class, `max_message_bytes=10**5000` and `=-10**5000` raise exactly `ValueError`
      whose text starts with `<Class> max_message_bytes must be from 1024 to 1048576 bytes, got
      <int: ~5001 digits>`, as two rows each of `_HUGE_DOORS` in `tests/test_numeric_doors.py`.
      Re-planting `f"got {max_message_bytes}"` in either constructor fails its rows with CPython's
      digit-limit error (invariant 13).
- [ ] `SQSSink(url, max_retries=2.5, max_message_bytes=1.5)` raises the `max_retries` error
      unchanged, and `SNSSink` the same. `SQSSink(url, message_group_id=5, max_message_bytes=1.5)`
      raises the `message_group_id` error unchanged (invariant 13).
- [ ] `max_message_bytes=262144` beside an injected `client=` constructs on both classes, and
      `tests/test_injected_client_roster.py` passes unedited (invariant 13).
- [ ] `tests/test_numeric_door_roster.py`'s judged set includes `max_message_bytes` for both
      classes, and its derived sweep, rewriting each new `exact_int` call to the bare parameter,
      reports both sites. `tests/test_type_door_roster.py` passes unedited, since no door is added
      (invariants 6 and 13).
- [ ] `tests/typed_consumer/accepts.py` builds each class with `max_message_bytes=262144` and
      with `max_message_bytes=None`, and `mypy` over it passes (invariant 10).

### FR-002: `SQSSink` holds each message and each request to the limit, never past 1 MiB

#### Description:

**The effective limit.** Each round reads one value, used for every decision that round makes:
`max_message_bytes` when it is not `None`, otherwise `self.MAX_BYTES` (FR-004), and then the
smaller of that and 1,048,576. The cap is a private module constant, `_SERVICE_MAX_BYTES`,
whose docstring quotes the service: "The maximum allowed individual message size and the maximum
total payload size (the sum of the individual lengths of all of the batched messages) are both 1
MiB 1,048,576 bytes" (`SendMessageBatch`). For a judged argument the cap changes nothing, since
FR-001's range ends there. It matters only for a `MAX_BYTES` raised above 1 MiB on a subclass or
an instance. Today such a sink sends a request the service refuses whole. With the limit at 2
MiB, six 300 KiB events were all lost in one refused call, counted in `failed`
(`spec096_destination_below_limit.py`, S5). With the cap, each message over 1 MiB is dropped
alone, and each request is split under 1 MiB.

**Each message.** `_prepare` (`sinks/sqs.py:665`) costs an entry as today: the body's UTF-8
length, plus on a FIFO queue the UTF-8 lengths of `MessageGroupId` and `MessageDeduplicationId`.
An entry whose cost exceeds the effective limit is dropped, counted in `dropped_oversized` and
announced. The line is today's, `SQSSink, <size> bytes exceeds the <limit>-byte message limit`,
with `<limit>` the effective limit. An entry is still prepared once per run (SPEC-074 FR-002), so
the limit an entry was judged against is the one read in the round that prepared it.

**Each request.** `chunk_items` (`:655-660`) is given the effective limit as `max_bytes` where
it is given `self.MAX_BYTES` today. A request therefore holds at most 10 entries whose costs sum
to at most the limit. Today's sum is enforced by the same constant. It is not reached by the
arithmetic of the per-message limit: 10 × 256 KiB is 2,621,440 bytes, past the service's 1 MiB,
and `spec096_limits_today.py` case B packs eight 30 KiB entries into one 246,448-byte request and
the other two into a second. Holding the sum to the message limit keeps that true at every
limit: at 1,048,576 the sum is the service's batch ceiling, and below it the sum is smaller than
the service requires, which costs requests and never a refusal.

#### Acceptance Criteria:

- [ ] With `max_message_bytes=None` and `MAX_BYTES` untouched, the requests a recording client
      receives for each batch of `spec096_limits_today.py` cases A, B, C and E equal, entry for
      entry and byte for byte apart from each entry's `Id`, those recorded at `10ba21ee`, and
      `dropped_oversized`, `failed` and the stderr lines are the same (invariants 2 and 7).
- [ ] With `max_message_bytes=1048576` on a standard queue, an event whose body is 1,048,576 bytes
      is sent alone and lands, and one of 1,048,577 bytes is dropped, `dropped_oversized == 1`,
      with one line reading `SQSSink, 1048577 bytes exceeds the 1048576-byte message limit`
      (invariant 2).
- [ ] With `max_message_bytes=1048576` on a FIFO queue, an entry whose body plus ids costs
      1,048,576 bytes is sent, and one costing 1,048,577 is dropped and counted, although its body
      alone is under the limit (invariant 2).
- [ ] With `max_message_bytes=65536`, a batch of a 100-byte event, a 100 KiB event and a 100-byte
      event sends one request holding the two small events, counts `dropped_oversized == 1`, and
      writes one line naming `65536` (invariant 2).
- [ ] With `max_message_bytes=1048576`, ten 300 KiB events go out in requests whose costed sums
      are each at most 1,048,576 bytes, three entries each and one for the last, and all ten land
      (invariants 2 and 7).
- [ ] On a sink whose `MAX_BYTES` is set to 2,097,152 on the instance, and on a subclass that sets
      it, with `max_message_bytes=None`: a 1.5 MiB event is dropped and counted in
      `dropped_oversized`, its line naming `1048576`. Six 300 KiB events go out in requests each
      summing to at most 1,048,576 bytes, and nothing is counted in `failed` (invariants 2 and 7).
- [ ] A batch whose every event exceeds the limit makes no request, `emit` returns, and each event
      is counted and announced once, as an all-oversized batch is today (invariants 2 and 7).

### FR-003: `SNSSink` holds each message and each request to the limit, never past 1 MiB

#### Description:

FR-002's rule on SNS's twin path (invariant 6). Each round reads one effective limit:
`max_message_bytes`, else `self.MAX_BYTES`, then the smaller of that and `_SERVICE_MAX_BYTES`,
1,048,576, the most any topic accepts ("supports values up to 1 MiB (1,048,576 bytes)",
`PublishBatch`). `_bodies` (`sinks/sns.py:321`) drops a message whose UTF-8 body exceeds it,
counted in `dropped_oversized` with today's line, `SNSSink, exceeds the <limit>-byte message
limit`. `chunk_items` (`:239-241`) is given the effective limit as `max_bytes`.

**A request is costed in bytes.** Today `chunk_items` measures each SNS body with
`size_of=lambda p: len(p[1])`, which counts characters, while `_bodies` checks each message in
UTF-8 bytes. The two agree only because `json.dumps` escapes every non-ASCII character by default,
so a body is ASCII. A subclass whose `_encode` writes UTF-8 (`ensure_ascii=False`) gets requests
up to four times the limit in bytes. `size_of` becomes the body's UTF-8 length, the unit
`_bodies` and the service use. For an ASCII body the two are equal, so no default request changes.

On SNS the request's sum is the topic's own rule, not a choice: "The combined size of all
messages in the batch, including each message's body and attributes, must not exceed the topic's
MaximumMessageSize" (`PublishBatch`). So a caller who raises the limit for a topic at 1 MiB gets
requests that fill it. A caller who raises it above the topic's own limit gets requests the topic
refuses whole: the docs settle that this is a refusal of the call, `BatchRequestTooLong`, "The
length of all the batch messages put together is more than the limit. HTTP Status Code: 400"
(`PublishBatch`). And a topic cannot always be raised: "To set MaximumMessageSize above 256 KiB,
the topic must have 100 or fewer total subscriptions, all of which must be supported endpoint
types (Amazon Data Firehose, Amazon SQS, or AWS Lambda)" (*Publishing large messages*). So a topic
that fans out to HTTP, email or SMS subscribers stays at 256 KiB or below whatever the caller
passes here. With the sink at 1 MiB and the topic at 262,144, two 200 KiB messages were both lost
in one refused call (`spec096_destination_below_limit.py`, T1), where today's sink sends
them in two calls and both land (T2). That is why the default stays at SNS's own default, and why
the README tells the caller to pass the topic's value and nothing larger.

#### Acceptance Criteria:

- [ ] With `max_message_bytes=None` and `MAX_BYTES` untouched, the requests a recording client
      receives for `spec096_limits_today.py` cases F1 and F2 equal, entry for entry and byte for
      byte apart from each `Id`, those recorded at `10ba21ee`, with the same `dropped_oversized`,
      `failed` and lines (invariants 2 and 6).
- [ ] With `max_message_bytes=1024`, on a subclass whose `_encode` is
      `json.dumps(event, ensure_ascii=False)`, an event whose body holds 600 `é` (over 1,024 bytes,
      under 1,024 characters) is dropped and counted. Two events whose bodies are each 700 to
      1,000 bytes and under 512 characters go out in two requests, where counting characters
      would put both in one request of more than 1,024 bytes (invariants 2 and 6).
- [ ] With `max_message_bytes=1048576`, a 600 KiB message is published and lands, where at the
      default it is dropped (`spec096_destination_below_limit.py` T3 and T4). A message of
      1,048,577 bytes is dropped, counted, and announced naming `1048576` (invariants 2 and 6).
- [ ] With `max_message_bytes=262144`, two 200 KiB messages go out in two requests, each summing
      to at most 262,144 bytes (invariants 2 and 7).
- [ ] On an `SNSSink` whose `MAX_BYTES` is set to 2,097,152 on the instance, with
      `max_message_bytes=None`, a 1.5 MiB message is dropped and counted in `dropped_oversized`
      and no request sums past 1,048,576 bytes (invariants 2 and 6).
- [ ] The same inputs give the same `dropped_oversized`, the same request boundaries by event,
      and the same named limit on `SQSSink` (standard queue) and `SNSSink`, for limits 1,024,
      65,536, 262,144 and 1,048,576 (invariant 6).

### FR-004: `MAX_BYTES` stays, holds 262,144, and is the default limit as in 1.x

#### Description:

`MAX_BYTES` is a public class attribute on both sinks. The frozen surface lets it grow and never
shrink (invariant 10), and the `1.x` rule is no breaking change, so it is not removed, renamed or
re-valued. After this spec it means one thing on each class: **the limit a sink uses when
`max_message_bytes` is `None`.** It is read through `self` once per round, as today. So the three
ways 1.x code sets a limit keep working: an assignment on the instance after construction, a
subclass that overrides it, and the class's own value. The suite relies on the first in three
places (`tests/test_sink_concurrency.py:484`, `tests/test_sinks_sqs_id_failures.py:129` and
`:203`), and a caller can rely on it equally. A `max_message_bytes` the caller passes wins over
`MAX_BYTES`, whatever `MAX_BYTES` holds. The class docstring's attribute list says so.

This is why the argument's default is `None` and not `262144`. With an `int` default, the
constructor cannot tell a caller who left the limit alone from one who passed 256 KiB, and the
three ways above would stop working without a word. `PostgresSink.connect_timeout` defaults to
`None` for the same reason (the bad-argument decision, SPEC-071).

#### Acceptance Criteria:

- [ ] `SQSSink.MAX_BYTES == 262144` and `SNSSink.MAX_BYTES == 262144`, and both are still class
      attributes that `type(sink).MAX_BYTES` reads (invariant 10).
- [ ] The three tests above pass unedited (invariant 10).
- [ ] With `max_message_bytes=None`, `sink.MAX_BYTES = 2048` on the instance, and a subclass
      setting `MAX_BYTES = 2048`, each drops a 3 KiB event and keeps a small one, naming `2048` on
      both classes, as `spec096_limits_today.py` cases D1 and D2 (SQS) and F3 and F4 (SNS)
      measured (invariant 10).
- [ ] With `max_message_bytes=65536` and `sink.MAX_BYTES = 2048` on the instance, a 3 KiB event
      lands and the effective limit named for a 100 KiB event is `65536` (invariant 10).

### FR-005: The record says what the limit is, how to match a destination, and what moved

#### Description:

What a caller and the next session read:

- **README**, *AWS — the durable-buffer path*. The `SQSSink` and `SNSSink` rows show
  `max_message_bytes=None`. One paragraph says that the default is 256 KiB on both sinks. It says
  a caller passes their queue's or topic's `MaximumMessageSize`, in bytes, which for a queue
  created at the service default is `1048576`. It says that a value above the destination's own
  costs whole requests when the service refuses them, which is why the sink does not default to
  1 MiB. The paragraph that says each sink "drops any single event too large to ever fit" names
  the limit as the sink's.
- **Docstrings.** Each class's `Args:` and `Raises:` for `max_message_bytes`, its `Attributes:`
  for `MAX_BYTES` and `max_message_bytes`, and `_prepare`/`_bodies`/`_chunks`/`_round`, where
  they name the limit. `sanitize.py`'s `_MAX_RENDERED_BYTES` docstring says "``SQSSink`` sends at
  most 256 KiB", which becomes "256 KiB by default".
- **`docs/release-notes/v1.2.1.md`**, the pending notes on `main`, as SPEC-093 and SPEC-095 also
  edit them. An *Added* section, "`SQSSink` and `SNSSink` take `max_message_bytes` (SPEC-096)",
  gives the argument, its range, its default and the cap. A *Changed* bullet covers a `MAX_BYTES`
  set above 1,048,576 on an instance or a subclass: a message over 1 MiB is now counted in
  `dropped_oversized` with the oversize line, where it was sent and counted in `failed` when the
  service refused it, and requests are split under 1 MiB. The "`>=1,<2` pin" list gains that case
  as an entry named by its content, "code that sets `SQSSink.MAX_BYTES` or `SNSSink.MAX_BYTES`
  above 1,048,576", and its lead-in's count ("with six exceptions" at `ead40798`) is recounted
  against the list as it stands when this spec completes, since SPEC-093 and SPEC-095 each add
  entries. Every sentence saying no constructor signature changed is rewritten to say that the two
  sinks each gain one keyword-only argument, appended, and that no default changed. The sentence
  that says *Added* lists one new public name counts the new ones. The opening list of carried
  specs names SPEC-096.
- **`docs/architecture.md`.** §8's "≤ 256 KB apiece" becomes the sink's message limit, 256 KiB by
  default and at most 1 MiB. §13's "past SQS's 256 KB" (the payload-ceilings bullet) becomes "past
  `SQSSink`'s default 256 KiB, and SQS's own 1 MiB". §12's item for `SQSSink.MAX_BYTES` moves to
  *Resolved*, closed by this spec. Its Kinesis item stays open.
- **`docs/decisions/event-assembly/each-coercion-pass-is-bounded-by-a-node-and-a-byte-budget.md`**
  says "(SQS takes 256 KB)"; it becomes "(`SQSSink` sends 256 KiB by default, and SQS takes
  1 MiB)", with a dated History line saying the fact changed and the decision did not.
  `docs/implementation-guide.md` and the delivery docs keep their 256 KB: they are history.
- **A decision file** in `docs/decisions/sink-delivery/`: *A destination limit the caller can
  configure is a constructor argument, defaulting to the 1.x value, under the service's ceiling*.
  It records the `None` default and FR-004's reason, the cap, the rejected `GetQueueAttributes`
  read, and the evidence on the default.
- **The public-attribute roster.** `tests/test_sinks_encode.py::test_the_public_surface_grows_only_by_five_losses`
  compares each sink's public names with the frozen `BASE_PUBLIC` literal plus the names later
  specs added. A new map beside `ENGINE_GAINED`, `LIMIT_GAINED = {"SQSSink":
  frozenset({"max_message_bytes"}), "SNSSink": frozenset({"max_message_bytes"})}`, joins that
  union. `BASE_PUBLIC` is not edited, since it records the surface as one spec found it.
- **SPEC-092's roster.** SPEC-092's text is amended alongside this spec, before either is built: its
  `PARAMETERS` paragraph names the two parameters and the count they make, and its criterion
  requires `max_message_bytes` in each class's `NEUTRAL` wherever the tree has it, since a limit
  does not change what a sink can know about whether its backend took an event. If
  `tests/conformance_classes.py` exists when this spec is built, the build adds the two names to
  `PARAMETERS` and `NEUTRAL`. If it does not, SPEC-092's build reads them from the tree.

#### Acceptance Criteria:

- [ ] `README.md`'s rows for `SQSSink` and `SNSSink` contain `max_message_bytes=None`, and the
      AWS section contains `MaximumMessageSize`, `1048576` and the words "256 KiB" (serves no
      invariant: documentation, judged by `docs/process/completion-ritual.md`).
- [ ] `docs/release-notes/v1.2.1.md` contains `max_message_bytes`, `1,024`, `1,048,576`,
      `SPEC-096` and the pin entry naming `MAX_BYTES` above 1,048,576. Read with every run of
      whitespace collapsed to one space, so a sentence wrapped across lines is still found, it
      contains neither "No constructor signature or default changed" nor any other sentence
      saying a constructor signature is unchanged, and the lead-in's count equals the number of
      entries in its list (serves no invariant: the release notes, which no gate holds to the
      code).
- [ ] `docs/architecture.md` §12 *Open* has no item headed by `SQSSink.MAX_BYTES`, *Resolved*
      has one naming SPEC-096, and the `KinesisSink` item is still under *Open*. Neither §13 nor
      the coercion-budget decision file states SQS's limit as 256 KB (serves no invariant: the
      open-items register and the record).
- [ ] `test_the_public_surface_grows_only_by_five_losses` passes with `LIMIT_GAINED` in its
      union, and this spec's diff does not touch the `BASE_PUBLIC` literal (serves no invariant:
      the roster's own hygiene; FR-001's signature and typed-consumer criteria hold the surface).
- [ ] The decision file exists under `docs/decisions/sink-delivery/`, `sh scripts/gen-index.sh`
      lists it, and `sh scripts/docs-lint.sh` exits 0 (serves no invariant: the register's
      shape).
- [ ] If `tests/conformance_classes.py` exists, its parameter test passes with `max_message_bytes`
      in `PARAMETERS` and `NEUTRAL` for both classes. Either way, `spec096_corpus_rows.py`, re-run
      on the branch, still reports 0 rows moved at 1,048,576 and at 1,024 (serves no invariant:
      SPEC-092's roster hygiene; no row's verdict depends on the limit).

---

## Data Model

```python
# src/log_foundry/sinks/sqs.py
_SERVICE_MAX_BYTES: Final = 1_048_576   # SendMessageBatch: one message and one request's sum
_LIMIT_FLOOR: Final = 1_024             # CreateQueue/SetQueueAttributes: MaximumMessageSize min

class SQSSink:
    MAX_BATCH = 10
    MAX_BYTES = 256 * 1024              # unchanged; the limit when max_message_bytes is None
    max_message_bytes: int | None       # new public attribute, the judged argument

    def _limit(self) -> int: ...        # min(MAX_BYTES if max_message_bytes is None
                                        #     else max_message_bytes, _SERVICE_MAX_BYTES)

# src/log_foundry/sinks/sns.py
_SERVICE_MAX_BYTES: Final = 1_048_576   # PublishBatch: the most any topic's MaximumMessageSize is
_LIMIT_FLOOR: Final = 1_024             # SetTopicAttributes: MaximumMessageSize min

class SNSSink:
    MAX_BATCH = 10
    MAX_BYTES = 256 * 1024              # unchanged; the limit when max_message_bytes is None
    max_message_bytes: int | None

    def _limit(self) -> int: ...
```

`_limit` tests `is None`, never truthiness. It is called once per round, and its value is passed
down to `_prepare`/`_bodies`, to `chunk_items` and to the oversize line.

---

## API / Interface Contract

```python
SQSSink(
    queue_url: str,
    *,
    client: Any = None,
    max_retries: int = 3,
    fifo: bool | None = None,
    message_group_id: GroupIdSource = None,
    message_deduplication_id: DedupIdSource = None,
    max_message_bytes: int | None = None,     # new, last
) -> None

SNSSink(
    topic_arn: str,
    *,
    client: Any = None,
    max_retries: int = 3,
    max_message_bytes: int | None = None,     # new, last
) -> None

# Refusals, raised before any client is built:
#   TypeError:  "SQSSink max_message_bytes must be an integer, not float"   (exact_int's text)
#   ValueError: "SQSSink max_message_bytes must be from 1024 to 1048576 bytes, got 512"
#               the value rendered by sanitize._int_text(exact), never f"got {max_message_bytes}"

# A queue created at the service default:
SQSSink(queue_url=URL, max_message_bytes=1_048_576)
# A topic raised to 1 MiB with SetTopicAttributes:
SNSSink(topic_arn=ARN, max_message_bytes=1_048_576)
```

---

## File & Folder Structure

```
src/log_foundry/sinks/sqs.py        # FR-001, FR-002, FR-004: argument, _limit, docstrings
src/log_foundry/sinks/sns.py        # FR-001, FR-003, FR-004
src/log_foundry/sanitize.py         # FR-005: one docstring phrase
tests/test_sinks_message_limit.py   # new: FR-001 to FR-004 for both sinks
tests/typed_consumer/accepts.py     # FR-001
tests/test_sinks_encode.py          # FR-005: LIMIT_GAINED joins the public-surface union
tests/test_numeric_doors.py         # FR-001: _BUILDS refusals and four _HUGE_DOORS rows
tests/conformance_classes.py        # FR-005, only if SPEC-092 is built
README.md
docs/release-notes/v1.2.1.md
docs/architecture.md                # §8, §12, §13
docs/decisions/sink-delivery/a-destination-limit-the-caller-can-configure-is-a-constructor-argument-defaulting-to-the-1x-value-under-the-services-ceiling.md
docs/decisions/event-assembly/each-coercion-pass-is-bounded-by-a-node-and-a-byte-budget.md
```

## Implementation Phases

### Phase 1: The argument (FR-001, FR-004)

- Add `max_message_bytes` to both constructors, judged after every existing refusal and before
  any client is built; store the exact `int` or `None`.
- Add `_limit` and the two constants to each module; keep `MAX_BYTES` and its value.
- Tests for every value FR-001 names on both classes, the lying subclasses, the `_BUILDS` and
  `_HUGE_DOORS` rows, the precedence of existing refusals, the two rosters, the typed consumer,
  and FR-004's `MAX_BYTES` reach.

### Phase 2: Applying it (FR-002, FR-003)

- Record, at `10ba21ee`, the requests a recording client receives for `spec096_limits_today.py`
  cases A, B, C, E, F1 and F2, as the fixture FR-002's and FR-003's first criteria compare with.
- Route the effective limit through `_chunks`/`_prepare`/`_announce_oversized` on SQS and
  `_round`/`_bodies` on SNS, read once per round, and cost an SNS request in UTF-8 bytes.
- Tests for each criterion of FR-002 and FR-003, the twin-path table included.

### Phase 3: The record (FR-005)

- README, docstrings, release notes, §8, §12 and §13, the two decision files, `LIMIT_GAINED`, and
  SPEC-092's roster if built.

---

## Evidence

Read and measured on 2026-11-16 at `10ba21ee`, on macOS with CPython 3.13.1, with no `aws` extra
installed, and every probe re-run at `ead40798` after the rebase with identical output: nothing
under `src/` or `tests/` differs between the two. Case F4 was added and first run at `ead40798`. Every AWS page was fetched live that day; the SQS notes the orchestrating session took
on 2026-11-15 agree with each. The three probes are committed under
`docs/audits/2026-11-14-probes/sinks/` and run against the tree with `PYTHONPATH=src` (the third
also needs `tests` and the probes directory on the path, as its header says).

**The service, quoted.**

- *SQS `SendMessageBatch`*: "The maximum allowed individual message size and the maximum total
  payload size (the sum of the individual lengths of all of the batched messages) are both 1 MiB
  1,048,576 bytes." Its error `BatchRequestTooLong`: "The length of all the messages put together
  is more than the limit. HTTP Status Code: 400".
- *SQS `SendMessage`*: `MessageBody` "The maximum size is 1 MiB or 1,048,576 bytes".
- *SQS message quotas*: "The minimum message size is 1 byte (1 character). The maximum is
  1,048,576 bytes (1 MiB)."
- *SQS `CreateQueue` and `SetQueueAttributes`*: "`MaximumMessageSize` – The limit of how many
  bytes a message can contain before Amazon SQS rejects it. Valid values: An integer from 1,024
  bytes (1 KiB) to 1,048,576 bytes (1 MiB). Default: 1,048,576 bytes (1 MiB)."
- *SQS message metadata*: "All components of a message attribute are included in the 1 MiB
  message size restriction."
- *SQS batch actions*: "The total size of all messages sent in a single SendMessageBatch call
  cannot exceed 1,048,576 bytes (1 MiB)".
- *What's New, posted Aug 4, 2025*: "Amazon Simple Queue Service (Amazon SQS) has increased the
  maximum message payload size from 256 KiB to 1 MiB", and "Customers can begin sending payloads
  up to 1 MiB today, across all AWS commercial Regions and AWS GovCloud (US)."
- *SQS's Java extended-client page* still says "more than 256 KB (the standard maximum message
  size)". AWS's own pages have not all caught up.
- *SNS `PublishBatch`*: "By default, the maximum allowed individual message size and the maximum
  total payload size (the sum of the individual lengths of all of the batched messages) are both
  256 KiB (262,144 bytes). To publish larger batches, set the topic's MaximumMessageSize
  attribute, which supports values up to 1 MiB (1,048,576 bytes). The combined size of all
  messages in the batch, including each message's body and attributes, must not exceed the
  topic's MaximumMessageSize."
- *SNS `SetTopicAttributes`*: "Valid values are 1024 to 1048576 (1 MiB). The default is 262144
  (256 KiB)." *Publishing large messages*: "By default, all existing and newly created topics
  have a MaximumMessageSize of 262,144 bytes (256 KiB)", and "If the combined size exceeds the
  limit, Amazon SNS returns an InvalidParameter error on publish."
- *Kinesis `PutRecords`*: "Each record in the request can be as large as 10 MiB, up to a limit of
  10 MiB for the entire request, including partition keys." *Handle large records*: "The default
  maximum record size for existing, and newly created streams is set to 1 MiB."
- *Firehose `PutRecordBatch`*: "Each record in the request can be as large as 1,000 KB (before
  base64 encoding), up to a limit of 4 MB for the entire request. These limits cannot be
  changed."
- No EventBridge sink ships, so its limit was not read for a sink.

**What the pages do not say.** They do not say what `SendMessageBatch` does with an entry larger
than the queue's own `MaximumMessageSize` but under 1 MiB: whether it refuses the call, as moto's
emulation does (`moto/sqs/models.py`, `send_message_batch`, which validates every entry before
sending any and raises `InvalidParameterValue`), or names the entry in `Failed`. They also do not
say whether a queue created before 2025-08-04 at the old default now holds 262,144 or 1,048,576,
where the SNS page answers that question for topics. And they say nothing about partitions other
than the commercial Regions and GovCloud. None of these was measured against AWS.

**Today's sinks, `spec096_limits_today.py`.** A recording client that takes everything:

| Case | Requests | Costed sum per request | Held | `dropped_oversized` |
|---|---|---|---|---|
| A, SQS, ten 100 KiB events | 5 of 2 | 204,972 each | 10 | 0 |
| B, SQS, ten 30 KiB events | 8 and 2 | 246,448 and 61,612 | 10 | 0 |
| C, SQS, one 300 KiB event beside two small | 1 of 2 | 192 | 2 | 1, line `307286 bytes exceeds the 262144-byte message limit` |
| D1, SQS, instance `MAX_BYTES = 2048`, 3 KiB event | 1 of 1 | 96 | 1 | 1, naming `2048` |
| D2, SQS, subclass `MAX_BYTES = 2048` | 1 of 1 | 96 | 1 | 1, naming `2048` |
| E, SQS FIFO, body 262,134 bytes, body plus ids 262,171 | 0 | none | 0 | 1 |
| F1, SNS, ten 100 KiB events | 5 of 2 | 204,972 each | 10 | 0 |
| F2, SNS, one 300 KiB event beside two small | 1 of 2 | 192 | 2 | 1 |
| F3, SNS, instance `MAX_BYTES = 2048` | 1 of 1 | 96 | 1 | 1, naming `2048` |
| F4, SNS, subclass `MAX_BYTES = 2048` | 1 of 1 | 96 | 1 | 1, naming `2048` |

**A destination below the sink, `spec096_destination_below_limit.py`.** The fake enforces a
destination limit and the 1 MiB batch total, refusing an over-limit entry either by failing the
whole call with a 400 (`request`) or by naming it in `Failed` with `SenderFault: true` (`entry`).
A sink limit above 256 KiB is simulated by assigning `MAX_BYTES` on the instance, which is what
the argument does. The batch is nine ~100-byte events and one 400 KiB event.

| Case | `request` mode | `entry` mode |
|---|---|---|
| S1, sink 256 KiB, queue 262,144 | 9 held, `dropped_oversized=1` | the same |
| S2, sink 1 MiB, queue 262,144 | 0 held, `failed=10`, `emit` raised `SinkDeliveryError` | 9 held, `failed=1` |
| S3, sink 1 MiB, queue 1,048,576 | 10 held | 10 held |
| S4, sink 256 KiB, queue 65,536, a 100 KiB event between two small | 0 held of 3, `failed=3`, raised | 2 held, `failed=1` |
| S5, sink `MAX_BYTES` 2 MiB, six 300 KiB events, queue 1 MiB | 0 held of 6, `failed=6`, raised | not run |
| T1, SNS sink 1 MiB, topic 262,144, two 200 KiB | not run | 0 held, `failed=2`, raised (`BatchRequestTooLong`) |
| T2, SNS sink 256 KiB, topic 262,144, the same two | not run | 2 held in 2 calls |
| T3, SNS sink 1 MiB, topic 1 MiB, one 600 KiB | not run | 1 held |
| T4, SNS sink 256 KiB, topic 1 MiB, the same | not run | 0 held, `dropped_oversized=1` |

`failed` in `request` mode is today's handling of a non-retryable `ClientError`. A 400 is not in
`_classify._AWS_RETRY_STATUSES` and `InvalidParameterValue` is not in `_AWS_RETRY_CODES`, so the
chunk is `REJECTED` at its first attempt and every event in it is lost. The line names
`ClientError` and not the code.

**The default.** Andrew chose on 2026-11-16: "Keep 256 KiB (Recommended)". The evidence he
was given follows. For keeping 262,144:

- 1.x callers see nothing change, which is the `1.x` rule.
- A caller whose queue is still at 262,144 keeps S1's outcome, one event dropped and counted with
  a line naming the limit. Under a 1 MiB default that caller gets S2's: in `request` mode the
  whole call's events, nine of which fit, counted in `failed` under a line that names no size.
  Whether real SQS behaves like `request` or `entry`, and how many queues still hold 262,144, are
  both unmeasured. The second depends on whether pre-August-2025 queues were migrated, which the
  SQS pages do not say.
- An event over 256 KiB needs a caller to pass large fields. Assembly bounds each value at
  `max_value_bytes`, 8,192 by default, and `docs/architecture.md` §13 measured 256 keys × 8,192
  bytes as roughly 2 MB, so such an event is possible but takes many large fields.

For 1,048,576:

- It is the service's own default for a queue created today.
- A caller who never reads the README gets S3 rather than S1: the tenth event lands instead of
  being dropped.
- It needs no configuration to use a limit AWS has offered since 2025-08-04.

On `SNSSink` the default is not in question. Every topic defaults to 262,144, and T1 shows a
larger default refusing whole calls on every default topic.

**SPEC-092's rows, `spec096_corpus_rows.py`.** The prototype's 256 rows, 86 red at `10ba21ee`, were
run with both classes' `MAX_BYTES` at 262,144, at 1,048,576 and at 1,024: no row's verdict moved.
At the 16-byte control, seven rows moved, all of them SQS or SNS rows that turned green because
nothing was sent. That proves the patch reached the rows. So no row moves under this spec at any
judged limit. The constructor roster moves: `init_parameters` reads 168 names over 39 classes
today, and this spec adds two.

## Revision history

- **2026-11-16, written.** The §12 item said "Nothing here has been measured or read". The AWS
  pages were read, the limits quoted above, and the item rewritten to name this spec. The Kinesis
  item was filed beside it the same day.
- **2026-11-16, revised after the spec review**, rebased onto `ead40798`, where SPEC-093,
  SPEC-094 and SPEC-095 landed as Draft specs. Andrew answered the default ("Keep 256 KiB
  (Recommended)"), recorded under *Out of Scope* and *Evidence*. Every finding was fixed:
  - FR-001's refusal renders the value through `sanitize._int_text`, with `±10**5000` rows in
    `_HUGE_DOORS`. ~~`f"got {max_message_bytes}"`~~ raised CPython's digit-limit error out of
    the refusal itself.
  - FR-001 gains lying subclasses holding 512 and 1,048,577 that say "in range". The 2,048
    subclass alone passed a build that range-checked the raw value and stored the `exact_int`
    copy.
  - The no-build criterion uses the counted `_boto3` stand-in. ~~"no `aws` extra"~~ cannot hold
    in `integration.yml`, which installs every extra.
  - FR-005 names §13's and the coercion-budget decision's "256 KB". It also names
    `LIMIT_GAINED`, and does not edit `BASE_PUBLIC`.
  - SPEC-092's `PARAMETERS` paragraph and its `NEUTRAL` criterion are amended in this commit.
    They do not say a bare "170": SPEC-092 builds before this spec in the arc, so its count is
    168 at `6bd0b66e` plus two once this spec lands, plus whatever SPEC-093's `ack_wait=` and
    `confirm=` add.
  - FR-003 costs an SNS request in UTF-8 bytes. ~~Characters~~ agreed only because `json.dumps`
    escapes non-ASCII.
  - The FIFO over-count is "at most 256 bytes for ids AWS accepts".
  - Assigning `max_message_bytes` after construction is named in *Out of Scope*.
  - The Data Model no longer uses `or`. FR-004 cites the measured SNS subclass case F4.
  - The release-notes exception is named by its content, and the lead-in count is recounted at
    completion.
  - The release-notes criterion reads across line wraps.
  - SPEC-093's FR-003 and SPEC-095 are cited as Draft on `main`. Neither contradicts a
    requirement here.
  - SNS's whole-call `BatchRequestTooLong` and the 100-subscription precondition are quoted.
