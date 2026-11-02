# Spec: OTLP Log Export — a Private OpenTelemetry Mapping and an OTLP/HTTP JSON Sink

**ID:** SPEC-056
**Status:** Draft
**Last Updated:** 2026-11-02
**Depends On:** SPEC-009, SPEC-017, SPEC-026, SPEC-027, SPEC-028, SPEC-029, SPEC-032, SPEC-036, SPEC-038, SPEC-041, SPEC-048, SPEC-049, SPEC-051, SPEC-055, SPEC-057, SPEC-058, SPEC-059

## Overview

An application that already runs an OpenTelemetry Collector, or ships to Datadog, New Relic,
Honeycomb, Grafana, Splunk or Elastic — all of which ingest OTLP — has to translate log-foundry's
native JSON before any of them will take it. This spec adds a sink that speaks OTLP directly: one
`@trace`d application points `OTLPSink` at any OTLP/HTTP endpoint and its logs arrive with the
OpenTelemetry names those backends already index — severity, trace and span ids, `service.name`,
`exception.type` — and with the library's own fields alongside under a `log_foundry.` prefix.

It is the first slice of a direction Andrew decided on 2026-10-05 (the entry in
`docs/decisions/public-api.md`): what goes outward standardizes on the OpenTelemetry data model and
semantic-convention names, while the transports stay plural — OTLP is one more sink, not the only
way out, because Astra ingests through an OpenSearch-compatible `_bulk` API and has no OTLP
ingestion at all. OTLP is produced natively; no OpenTelemetry SDK or API enters the runtime. The
native JSON field names do not change. The mapping this spec builds is **private** until a second
consumer has proven its shape, and the arc recorded in `docs/specs/INDEX.md` lists what is built
on it after this.

## Scope

### In Scope

- A private mapping from one assembled event to one OTLP/JSON `LogRecord`, total over every value
  assembly can produce and over the ones it cannot (FR-001).
- The request envelope — resources, scope, schema URL — and a proof, against the generated OTLP
  classes, that what is built is OTLP rather than JSON that looks like it (FR-002).
- `OTLPSink`, an `HTTPSink` subclass at `log_foundry.sinks.otlp`: OTLP's endpoint, retry set, byte
  budget and response bound, plus the two private hooks on `HTTPSink` that let a subclass name its
  retryable statuses and bound the response it reads (FR-003).
- Partial success: a `2xx` that rejects some or all of the records, or that this sink cannot read
  (FR-004).
- Registration of the new public path — README, frozen-path baseline, typed-consumer probe, every
  roster the suite lists or derives for the HTTP family, and the register's `Governs` for the
  private module (FR-005).
- One dev-only dependency, `opentelemetry-proto`, with its note in `CLAUDE.md`'s Tech Stack Test row,
  a regenerated `poetry.lock`, and the `mypy` accommodation that dependency forces (FR-002).

### Out of Scope

- **Protobuf and gRPC transports.** JSON over HTTP only. The binary encodings need the generated
  classes at runtime, which is the runtime dependency the core refuses.
- **`OTEL_*` environment variables.** Configuration is explicit constructor arguments; nothing is
  read from the environment, and a base URL is not given `/v1/logs` appended (the endpoint is used
  exactly as written).
- **OTLP traces and spans** built from `span.end`, and an `otel_traces` layout. A later item of the
  arc, last because it carries the most design risk; nothing here reads a span's lifetime.
- **Metrics.** In-library metrics stay out; they are derived downstream.
- **The ClickHouse `otel_logs` layout and the Astra `_bulk` profile.** Planned items of the arc,
  recorded in `INDEX.md` without numbers; this spec only keeps the mapping reusable by the first.
- **A public mapping API.** `_otel` is private: no name in it is in any `__all__`, documented as an
  API, or promised to a caller. Its *output* is another matter — the attribute names the sink sends
  are documented in the README (FR-005) and are what a consumer's queries rest on.
- **Any rename of a native JSON field**, and any rename of a user's `fields` key. A rename to
  semantic-convention names is a 2.0 question.
- **Extra resource attributes** — host, process, runtime, SDK telemetry. Three resource attributes
  are sent: `service.name`, `service.version`, `deployment.environment.name`.
- **A Collector in CI.** The integration roster records `otlp` as unverified against a real service
  with its reason; the generated-class round trip (FR-002) is the verification this spec carries.
- **Per-child retry in `MultiSink`.** OTLP's multi-destination guidance asks for retry per
  destination; an `OTLPSink` beside another child is isolated and is not re-sent when another child
  took the batch (SPEC-006, SPEC-017 FR-004). The fan-out counts the batch's events as absorbed
  `failed` when it swallows the raise, a reporting child's too, and the child's own `failed` counts
  as well — an accepted over-count (SPEC-058 FR-007). That trade is the library's and is unchanged.
- **The worker's batch-level retry.** OTLP forbids retrying a status outside its set, and
  `OTLPSink`'s own request loop obeys that (FR-003); but `Worker._emit` re-sends a batch whenever
  `emit` raises, whatever the status, so a `400` is still offered `max_retries + 1` times by the
  pipeline above the sink. Teaching the worker a per-sink "do not retry" is a change to every sink's
  contract, not to this one.
- **A protobuf- or gzip-encoded response body.** A `2xx` whose body is not JSON is read as full
  success (FR-004). A server answering JSON with protobuf has broken OTLP's same-`Content-Type`
  rule, and `urllib` asks for `Accept-Encoding: identity` (verified against a local server), so a
  gzipped response arises only from a caller's own `headers=` and is read as non-JSON too.
- **A client-side cap on the request body at OTLP's 64 MiB.** `max_batch_bytes` is the cap; the
  5,000,000 default is well under it, and a caller who raises it past 64 MiB has chosen to.
- **Parsing an HTTP-date `Retry-After`.** `HTTPSink` reads the seconds form and falls back to its
  exponential backoff for a date, unchanged.
- **Shared-subtree cost in `sanitize`, and a byte budget in `_otel`.** The build review found the
  same exponential expansion of a shared subtree in assembly, on the caller's own thread. SPEC-057
  (`e8cbd5d`) closes it there, with a per-pass node budget `sanitize._MAX_NODES = 131_072`, a
  16 MiB text budget and the placeholder `<node limit>`; nothing in this spec changes `sanitize`,
  and FR-001's node budget bounds only this mapping. The two budgets are equal in number,
  placeholder text and stop rule on purpose (FR-001 rule 7), and a test pins that equality. Two
  differences are deliberate. The scope of the count differs: `sanitize` counts the values of one
  pass over `fields`, and `_otel` counts the values of one record, body and library attributes
  included. And a transform's coercion runs outside SPEC-057's budgets (`_Coercer(budgeted=False)`,
  so SPEC-058's promise not to clip holds), which makes `_otel`'s node budget the only bound on a
  transform-built record before it reaches the wire. `_otel` has **no byte budget**: SPEC-057
  charges an assembled event at 16 MiB per pass upstream, and a transform's output is the caller's own
  code choosing the size; a record whose rendering is larger than the request budget is dropped by
  `_item_size` after it was rendered, counted in `losses().dropped`, and that rendering is the
  work this spec does not bound.
- **A module-qualified `code.function.name`.** The attribute carries the span's name as the library
  recorded it — `__qualname__`, a type name, or the caller's `name=` — which is not the
  fully-qualified name the convention describes; no module is added.

---

## Functional Requirements

### FR-001: A private mapping turns one assembled event into one OTLP/JSON LogRecord, and is total

#### Description:

`src/log_foundry/_otel.py` holds `log_record(event)`, a pure function — no clock, no I/O, no
mutation of its argument — from the dict `build_event` and `end_event` assemble (arch §6) to the
dict that serializes as an OTLP/JSON `LogRecord`. Keys are lowerCamelCase, `traceId` and `spanId`
are lowercase **hex** (OTLP/JSON's one deviation from proto3 JSON), enums are integers, and a
`fixed64` is a decimal string.

| Event | LogRecord |
|---|---|
| `timestamp` | `timeUnixNano`, and `observedTimeUnixNano` equal to it — the data model describes the two as typically equal for an event that originates in the producing SDK, which is what this library is |
| `level` | `severityNumber` DEBUG 5, INFO 9, WARNING 13, ERROR 17, CRITICAL 21; `severityText` is `level` verbatim, and absent when `level` is empty |
| `message` | `body`, through the value encoder below |
| `trace_id`, `span_id` | `traceId`, `spanId` — hex of the right length in either case, written lowercase; anything else, and the all-zero W3C sentinel, is omitted — and `flags` 1 when `traceId` is written (the W3C sampled bit; `format_traceparent` always sets it) |
| `message` of `span.start` or `span.end` | `eventName`, the same string |
| `function`, `log_id` | attributes `code.function.name`, `log.record.uid` |
| `parent_span_id`, `truncated`, `duration_ms`, `status` | attributes `log_foundry.parent_span_id`, `log_foundry.truncated`, `log_foundry.duration_ms` (through the value encoder: an `int` is an `intValue`, a `float` a `doubleValue`), `log_foundry.status` |
| `error` | attributes `exception.type`, `exception.message`, `exception.stacktrace` |
| `fields` | one attribute per key, through the value encoder below |

**Totality is owed because a raising mapping costs its event, and an unguarded call costs the
batch.** At `68caacf` `HTTPSink._items` called `_render` with no guard, so a raising mapping failed
the whole `emit`, the worker retried it, and every innocent neighbour was lost after four attempts —
the shape SPEC-055 fixed for `SQLiteSink`. SPEC-058 FR-005 changed that: `_items` now guards
`_render` and `_item_size` together for each event, catching `Exception`, and a raise drops that one
event alone, counted in `losses().dropped` and announced once as `lost N event(s)` naming the class
and the exception's type (read at `1130461`, `HTTPSink._items`). Totality is still owed, for three
reasons the guard does not remove. A dropped event is a lost event, and a mapping that can raise
loses data a total one delivers. `HTTPSink._known_refused` and `_deliver` call `_body` outside any
guard, and `_body` calls `resource_key` and `request_body`, so a raise there still fails the whole
`emit`. And invariant 8 asks that what leaves the library parses. An event can reach this sink past
assembly: a `TransformSink` checks its function's result with invariant 8's observable
(`json.dumps(result, allow_nan=False, ensure_ascii=False).encode("utf-8")`) and coerces a failure for
type only (SPEC-058 FR-001), so what it passes through is what that check accepts. Probed at
`1130461`, that still includes a `str` subclass whose `__hash__` raises, an `int` subclass whose
`__str__` lies, and `int` keys in `fields`; it no longer includes a lone surrogate, a non-finite
float, an integer beyond `sys.get_int_max_str_digits()` or a `Mapping` that is not a `dict`, which
the transform replaces with a marked placeholder or coerces to a `dict`. Those reach the mapping
only from a caller that drives the sink directly. The mapping is total over the values assembly
produces **and over the ones it cannot**, and the rules below exist because each was a way to
raise, to loop, or to write something a parser refuses.

1. **Every library key is read at the type its target needs, or omitted.** A `str` for every
   string target (`timestamp`, `level`, `function`, `log_id`, `parent_span_id`, `status`, the two
   ids, and each of `error`'s four), a `float` or non-`bool` `int` for `duration_ms`, `True` for
   `truncated`, a `dict` for `error` and `fields`. Anything else is omitted, so a non-string never
   lands in `severityText` or a `stringValue`. `message` alone takes any value, through the value
   encoder: `message: {"a": 1}` is a `kvlistValue` body and `message: 5` an `intValue` one. An
   event that is not a `dict` maps to `{}`, which is still one record per event.
2. **A value cannot steer its own mapping.** Dispatch is on the exact type, or for a subclass on
   its builtin base through that base's unbound methods — `str.__str__`, `int.__repr__`,
   `float.__float__`, `dict.items`, `list.__iter__`, `tuple.__iter__` — the rule `sanitize` already
   follows with `_INT_LT` and `_FLOAT_REPR`. A subclass whose `__str__`, `__repr__`, `__eq__`,
   `__float__` or `items` lies, or raises, therefore cannot change what is written. A `Mapping` or
   sequence that is not a `dict`, `list` or `tuple` is outside the domain (rule 9). **An object can
   pass `isinstance` and still not be the type.** `class Claim: __class__ = property(lambda s:
   str)` makes `isinstance(Claim(), str)` true, and `str.__str__(Claim())` raises `TypeError`
   (probed at `e8cbd5d`, and the same for `int.__repr__` with a claimed `int`), which is the case
   `sanitize.key` names. So the `isinstance` test is not the dispatch: the unbound read is, and it
   runs inside the guard, so an object that fails it is rule 9 as a value, rule 8 as a key, and
   `None` as a resource stamp.
3. **Every string written is valid UTF-8.** A lone surrogate, in a value, a key, a message or any
   `error` string, is replaced by U+FFFD and the record is marked `log_foundry.truncated`, the
   SPEC-055 rule. `json.dumps` escapes a lone surrogate happily, and `json_format.Parse` then
   refuses the document — measured against the pinned versions in FR-002.
4. **Values.** `None` is an empty `AnyValue`; `bool` is tested before `int`; an `int` within int64
   is `intValue` as a decimal string; a finite `float` is `doubleValue`; `str` is `stringValue`; a
   `dict` is `kvlistValue` and a `list` or `tuple` is `arrayValue`, an empty one with no `values`
   key, which is the canonical form; a non-finite float (assembly replaces them and a `TransformSink` coerces them, so only a direct
   `emit` brings one) is `doubleValue` spelled `"NaN"`, `"Infinity"` or `"-Infinity"`.
5. **Integers beyond int64** — which OTLP cannot carry and a parser refuses — are the
   `stringValue` of their digits, and an integer too long for `sys.get_int_max_str_digits()` is
   `sanitize`'s `<int: ~N digits>` placeholder with the record marked (SPEC-020), decided by
   `bit_length()` and never by converting it, because converting is what raises.
6. **Nesting is bounded, and a cycle is found by path.** Counting from the attribute value, which
   is level 1, **24 nested containers render and the 25th is cut**: it becomes the `stringValue`
   `<depth limit>` and the record is marked. That is `sanitize`'s indexing — a field value is depth
   0 and a value at depth `max_depth` is cut — applied to containers, so a scalar inside the 24th
   container still renders. 24 and not more because `json_format.Parse` refuses a document past 100
   nested messages, and the deepest shape — a `kvlistValue` chain, three messages a level — is
   accepted at 31 levels and refused at 32 (measured against `protobuf` 7.36.2); the cap leaves
   seven levels of margin, envelope included. It is reachable: `configure(max_depth=)` has no upper
   bound, so an event assembled under a deeper ceiling is cut here. A container that is its own
   ancestor is written `<circular>`, as `sanitize` does, by comparing it with its ancestors' ids; a
   depth cap alone does not do this, since a list holding itself twice is two branches a level,
   measured at 2,097,151 visits and 5.6 seconds at a cap of 20 and doubling with every level.
7. **A record's work is bounded.** Path detection does not catch a shared subtree: `y = [y, y]`
   repeated is a DAG, not a cycle, and expands to 2^n nodes — about a second at 18 levels and
   doubling per level (measured on a prototype of this mapping in the build review, and not
   re-measured at `1130461`, where the mapping does not exist). A record therefore has a budget of
   `MAX_NODES = 131_072` values written, counting every value, scalar or container, in the body,
   the attributes and everything nested in them; a mapping key is not a value and is not counted,
   and a value that becomes `<depth limit>` or `<circular>` spends a slot. The first value that
   would exceed it becomes the `stringValue` `<node limit>`, nothing after it is visited, the
   containers around it end there, and the record is marked. The placeholder is the one value
   written past the budget, as in `sanitize`, so a cut record holds `MAX_NODES` values plus
   `<node limit>`. The budget is checked before `MAX_NEST`, so a value past both is `<node limit>`.
   **The walk order is fixed: the library attributes are charged first, then `body`, then the
   user's `fields` in event order.** A hostile `message` can spend the whole budget and cut itself
   and every field after it, but it cannot remove `code.function.name`, `log.record.uid`,
   `log_foundry.status`, `exception.type`, `exception.message`, `exception.stacktrace` or any other
   library attribute, which cost fewer than a dozen values and are charged before it is read. The
   values of an attribute that is dropped (a collision or a reserved-prefix key, rule 8) are never
   walked and cost nothing. **The `log_foundry.truncated` mark is not a counted value.** It is
   decided after the walk, written in its place in the table's order, and written even when the
   budget is already spent, so a cut record is always marked and the count of `MAX_NODES` values
   plus the placeholder excludes it.
   **The number, the placeholder text, the stop and the order of checks are SPEC-057's**, which
   gives `sanitize` a private `_MAX_NODES = 131_072`, a `<node limit>` placeholder and the same
   stop rule for an assembly pass (`e8cbd5d`), and they are equal on purpose: one hostile shape
   should cost the same wherever it is walked. `_otel` stays a leaf and imports nothing from the
   package, so the equality is pinned by a test that compares `_otel.MAX_NODES` with
   `sanitize._MAX_NODES` and the placeholder text with `sanitize._NODE_LIMIT`; it pins the constants
   and not the scope of the count, which differs (a record here, one pass over `fields` there). The
   budget is still needed with SPEC-057 in place: an assembled event is bounded per pass, and a
   boundary event takes two (`build_event`'s and `backfill_baggage`'s), while a transform's output is
   coerced outside SPEC-057's budgets and a direct `emit` is coerced by nothing. The number is
   twice a fully populated two-level structure at the
   default `max_keys` (256 × 256 = 65,536 values), so a wide but ordinary event is not cut, and it
   stays small enough that the worst record costs about a tenth of a second — 131,071 shared nodes
   took 0.11 s through the prototype mapping and `json.dumps` on the authoring machine, and 262,143
   took 0.58 s; not re-measured at `1130461` — which is what a hostile record may hold the drain
   thread for, or on the orphan path the caller's, invariant 4. The constant's docstring says this beside the number. A smaller budget
   would cut legitimate events; a larger one makes the hostile one cost seconds.
8. **Keys are rendered as `sanitize` renders them, and are non-empty and unique.** Assembly has
   already made every key a `str`, so this rule is reached by a `TransformSink`, which passes the
   `int`, `float`, `bool` and `None` keys `json.dumps` accepts (probed at `e8cbd5d`), and by a
   direct `emit`. The trigger is `sanitize._Coercer.key`'s: a key is replaced by a placeholder
   only when it cannot be rendered, because invariant 8 says "a key that cannot be rendered", and
   a key that renders is not a substitution. A `str` key is used as it is, read through
   `str.__str__` (rule 3 applies); a `bool` is `"True"` or `"False"`, tested before `int`; any
   other `int` is its digits through `int.__repr__`, and one too long for
   `sys.get_int_max_str_digits()` is `<int: ~N digits>` and marks (rule 5); a finite `float` is
   `float.__repr__`; a non-finite float is `<float: nan>`, `<float: inf>` or `<float: -inf>` and
   marks, as in `sanitize`; `None` is `"None"`. Anything else — a `tuple`, a `Decimal`, any object —
   and any key whose unbound read raises, an object that claims `str` or `int` only through
   `__class__` included, becomes `<unserializable key: T>`, `T` being the type name as rule 9
   reads it, and marks the record, the one placeholder invariant 8 says marks. Two differences from
   `sanitize` are deliberate. `sanitize` renders those other types with the key's own `str()`
   (probed at `e8cbd5d`: a `tuple` key becomes `"(1, 2)"`, a `Decimal("1.5")` key `"1.5"`, an `int`
   subclass whose `__str__` returns `"evil"` becomes `"evil"`), and `_otel` calls no code of a key
   it was handed (rule 2), so those are a placeholder here, and an `int` subclass is its digits;
   only a direct `emit` reaches either, since `json.dumps` refuses a `tuple` or `Decimal` key
   before a transform returns. And where two keys render to one string `sanitize` keeps the later
   silently, while here the first stands and the drop is counted. A key that is **empty**, or that
   renders to one already written — `1` and `"1"`, or two unrenderable keys of one type, which
   share one `<unserializable key: T>` — is dropped, at any level of nesting, and its record's
   `droppedAttributesCount` counts it. OTLP requires unique keys and OpenTelemetry non-empty ones,
   `json_format.Parse` accepts duplicates and `MessageToDict` writes an empty key back as no key at
   all, so neither would say so. Assembly does produce an empty key: `lf.info("m", fields={"": 1})`
   carries one.
9. **Anything else** — a value outside the domain assembly produces — becomes the `stringValue`
   `<unserializable: T>` naming only its type, never a `repr` (arch §6); a key outside it is rule 8.
   `T` is read the way `sanitize._type_name` reads it: `str(type(value).__name__)`, which goes
   through rule 3 like any string written, and `"?"` when that raises, as it does for a metaclass
   whose `__name__` is a raising property. `type()` ignores `__class__`, so `Claim` above is
   `Claim`.

**The timestamp is converted with integer arithmetic, and `_otel` does not reuse
`sinks/_time.epoch_nanos`.** At `68caacf` that function went through a float and was inexact — it
gave `1791203696788999936` for `"2026-10-05T12:34:56.789Z"` — and that was this spec's reason for
its own conversion. The `epoch_nanos` fix (#255) made it exact: at `1130461` it gives
`1791203696789000000` for that string, by counting whole microseconds. The reason is therefore gone,
and three others stand. It **reads a clock** for anything it cannot parse: `"2026-02-30T00:00:00.000Z"`
and `"x"` both return the emit-time `time.time_ns()` (probed at `1130461`), where OTLP defines `0` as
"unknown or missing" and this mapping omits both fields rather than invent a time. It **accepts any
`fromisoformat` shape**, a naive timestamp read as local time and one before 1970 as a negative
number, where the mapping accepts one shape and a `uint64` range. And it lives in `log_foundry.sinks`,
while `_otel` is a leaf that imports nothing from the package at runtime. The mapping accepts exactly
the library's own shape: UTC, three fractional digits and a trailing `Z`, as in
`2026-10-05T12:34:56.789Z`. Anything else is unusable, and an unusable timestamp omits both time
fields rather than reading a clock. **A timestamp at or before the epoch is unusable too.**
`"1970-01-01T00:00:00.000Z"` would give `timeUnixNano "0"`, which OTLP defines as unknown, and a
zero `fixed64` is a proto3 default that `MessageToDict` omits, so the round trip in FR-002 would
fail on it (probed at `e8cbd5d` with `opentelemetry-proto` 1.45.0). `epoch_nanos` is the independent
oracle for the shape both accept after the epoch: a test asserts the mapping's `timeUnixNano`
equals `str(epoch_nanos(ts))` for the library's own shape, so two exact conversions are held to
agree.

**`exception.type` is module-qualified**: `module.Type`, except that a `builtins` exception, and one
with no recorded module, uses the bare name — so an `OSError` reads `OSError`, as the convention's
own examples do. `error.type` and `error.module` are each already bounded at assembly; the joined
string is not re-clipped.

**Order, collisions and the reserved prefix.** Attributes are the library's, in the order of the
table above, then the user's `fields` in event order. A user field whose key is one the library set
*on this record* is displaced by it; a user field whose key starts with `log_foundry.` is dropped
whether or not the library set that key, so it can never pose as a library attribute; and
`droppedAttributesCount` counts every attribute dropped, by either rule or by rule 8 — a
substitution nobody can see is a silent change to the data, which `sanitize` already refuses. A
user field named `exception.type` on a record where the library set no exception attribute passes
through untouched.

**What is marked and what is counted.** `log_foundry.truncated` is set by: the event's own
`truncated: True`; a lone surrogate replaced (rule 3); an integer too long to render (rule 5); the
depth cut and the node cut (rules 6 and 7); and a key written as a placeholder, a non-finite
`float` key or an over-long `int` key (rule 8). A key that renders — an `int`, `bool`, finite
`float` or `None` — marks nothing. It is **not** set by `<circular>` or by a value `<unserializable: T>`, which like any type-name
placeholder are visible on their own (invariant 8); by an integer beyond int64 written as its
digits, which loses nothing; or by a non-finite float spelled `"NaN"`, which is how OTLP/JSON
writes one. Everything *dropped* — a collision, a reserved-prefix key, a repeated or empty key — is
counted in `droppedAttributesCount` and is not marked.

#### Acceptance Criteria:

- [ ] `"2026-10-05T12:34:56.789Z"` maps to `timeUnixNano == "1791203696789000000"`, with
      `observedTimeUnixNano` the same string; `"1970-01-01T00:00:00.001Z"` maps to `"1000000"`; and
      for each of those and three more library-shaped timestamps `timeUnixNano` equals
      `str(sinks._time.epoch_nanos(ts))`.
- [ ] A timestamp that is absent, not a `str`, lacks the `Z`, is at or before the epoch (`"1970-01-01T00:00:00.000Z"`
      and `"1969-12-31T23:59:59.999Z"` both), is past the largest `uint64` nanosecond, or names an impossible date (`"2026-02-30T00:00:00.000Z"`) omits both
      time fields, and the call returns; no clock is read, asserted by patching `time.time_ns` and
      `time.time` to raise, which `epoch_nanos` fails on the impossible date.
- [ ] The five levels map to 5, 9, 13, 17 and 21; `"TRACE"` and `"info"` carry no `severityNumber`
      and a `severityText` equal to the string; an empty `level`, an absent one, and one of `5`,
      `None` or `{"a": 1}` give neither field.
- [ ] A 32-hex `trace_id` and 16-hex `span_id` pass through with `flags == 1`, and the same ids in
      uppercase are written lowercase; a malformed or all-zero `trace_id` omits `traceId` and
      `flags`, and a malformed or all-zero `span_id` omits `spanId` only.
- [ ] `eventName` is `"span.start"` or `"span.end"` for those two messages and absent otherwise; a
      caller who logs those exact words gets the label too, the limit `model.backfill_baggage`
      already has.
- [ ] A `span.end` event carrying a parent, `truncated` and an error yields exactly these attribute
      keys in this order: `code.function.name`, `log.record.uid`, `log_foundry.parent_span_id`,
      `log_foundry.truncated`, `log_foundry.duration_ms`, `log_foundry.status`, `exception.type`,
      `exception.message`, `exception.stacktrace`; `parent_span_id: None` and an absent `truncated`
      each omit their key.
- [ ] `duration_ms` of `12` gives `{"intValue": "12"}` and of `1.5` gives `{"doubleValue": 1.5}`; a
      `bool`, a string and `None` omit the attribute.
- [ ] `error` of `{"type": "OSError", "module": "builtins"}` gives `exception.type == "OSError"`;
      `{"type": "Boom", "module": "mypkg.errors"}` gives `"mypkg.errors.Boom"`; an empty or absent
      module gives the bare name; an `error` that is not a mapping adds no exception attribute.
- [ ] Field values map as follows: `None` to `{}`; `True` to `{"boolValue": true}` and not
      `intValue "1"`; `5` to `{"intValue": "5"}`; `2**63 - 1` and `-2**63` to `intValue` decimal
      strings; `2**63`, `-2**63 - 1` and `10**100` to `{"stringValue": "<digits>"}`; `1.5` to
      `{"doubleValue": 1.5}`; `float("nan")` to `{"doubleValue": "NaN"}`; `"s"` to
      `{"stringValue": "s"}`; `{}` to `{"kvlistValue": {}}`; `[]` to `{"arrayValue": {}}`;
      `(1, (2,))` and a `namedtuple` to the nested `arrayValue`; and `{"a": [1, {"b": None}]}` to
      the nested `kvlistValue`/`arrayValue` with `{}` for the `None`.
- [ ] Under the default `sys.get_int_max_str_digits()`, `10**5000` and `-10**5000` map to
      `<int: ~N digits>` and the record carries `log_foundry.truncated == true`; neither raises.
- [ ] Library keys of the wrong type — `function: 5`, `log_id: {}`, `parent_span_id: 5`,
      `status: 5`, `duration_ms: "x"`, `truncated: "yes"`, `error: {"type": 5, "message": {"a": 1}}`
      — each omit their attribute; `message: {"a": 1}` gives a `kvlistValue` body, `message: 5` an
      `intValue` body, and an absent `message` no `body`; no `stringValue` or `severityText` in any
      of these records holds a non-string.
- [ ] A lone surrogate (`"a\ud800b"`) as a message, a field value, a field key, an
      `error.message` and a `function` is written `"a\ufffdb"` in each place, and the record
      carries `log_foundry.truncated == true`.
- [ ] A `class Lying(int)` whose `__str__` and `__repr__` return `"evil"`, a `str` subclass whose
      `__str__`, `__eq__` and `__hash__` lie, a `float` subclass whose `__float__` returns `99.0`
      for a value of `1.5`, a `dict` subclass whose `items` raises, and a `tuple` subclass whose
      `__iter__` lies, each map exactly as their base value would, and `"evil"` and `99.0` appear
      nowhere in the output; a `Mapping` that is not a `dict` maps to `<unserializable: T>` both
      when its `items` raises and when it does not (`types.MappingProxyType({"a": 1})` gives
      `<unserializable: mappingproxy>`). These call `log_record` directly: assembly coerces all of
      them first.
- [ ] A `Claim` object (`__class__` returns `str`, so `isinstance(Claim(), str)` is true and
      `str.__str__` raises) as a message, a field value and an `error.message` gives
      `<unserializable: Claim>` (or omits the `error` string), an `IntClaim` likewise, and neither
      raises; as a key each gives `<unserializable key: Claim>`, marked. An instance of a class
      whose metaclass has a raising `__name__` property gives `<unserializable: ?>` as a value and
      `<unserializable key: ?>` as a key, marked, and neither raises.
- [ ] Nested lists around a scalar `1`: 23 and 24 deep render whole, the innermost leaf an
      `{"intValue": "1"}`, with no mark; 25 deep renders 24 containers and the 25th is the
      `stringValue` `<depth limit>`, marked. A list nested 2,000 deep is cut the same way at the
      25th.
- [ ] `x = []; x.append(x); x.append(x)` as a field value returns in under a second, written as an
      `arrayValue` of two `<circular>` strings, unmarked.
- [ ] `y = []` rebuilt 24 times as `y = [y, y]` — shared subtrees, not a cycle — returns within ten
      seconds, holds exactly `MAX_NODES` values plus the one `<node limit>` placeholder, counting
      every value the record holds (library attributes, body and fields) and not the
      `log_foundry.truncated` mark, and is marked; a field holding 256 lists of 256 integers (65,793
      values counting the lists and the field) is written whole and unmarked.
- [ ] The walk order holds: on a `span.end` event with a function, a log id, a status, an error and
      `fields={"k": 1}`, a `message` that is the 24-level shared `[y, y]` structure returns within
      ten seconds, its body contains `<node limit>`, every library attribute is present with its own
      value, `k` is absent with no `droppedAttributesCount`, and `log_foundry.truncated` is `true`
      although the budget was spent before it was written.
- [ ] `_otel.MAX_NODES == sanitize._MAX_NODES` and the `<node limit>` text written equals
      `sanitize._NODE_LIMIT`, so SPEC-057's budget and this one cannot drift apart unseen.
- [ ] Keys, through `log_record` called directly, since assembly has already made every key a
      `str`: `fields={1: "a", 2: "b"}` gives attributes `"1"` holding `"a"` and `"2"` holding
      `"b"`, with no `droppedAttributesCount` and no mark, and the same `fields` returned by a
      `TransformSink` ahead of an `OTLPSink` arrive as both attributes; `{True: 1, None: 2, 1.5: 3}`
      gives `"True"`, `"None"` and `"1.5"` unmarked; `{float("nan"): 1}` gives `"<float: nan>"` and
      `{10**5000: 1}` gives `"<int: ~5001 digits>"`, each marked; an `int` subclass key whose
      `__str__` returns `"evil"` gives its digits; `{(1, 2): 1}`, a `Decimal` key, a key whose
      `__str__` raises, a `Claim` and a `?`-named type give `<unserializable key: T>` with the
      right `T`, each marked. `{1: "a", "1": "b"}` gives one attribute `"1"` holding `"a"`,
      `droppedAttributesCount == 1` and no mark, and two keys that raise on render give the first's
      value under one placeholder, `droppedAttributesCount == 1` and a marked record.
      `fields={"": 1}` gives no attribute, `droppedAttributesCount == 1` and no mark; `{"a": {"": 1,
      "b": 2}}` and each case above nested in a field value are cleaned the same way and counted in
      the record's `droppedAttributesCount`.
- [ ] On a `span.end` event with an error, user `fields` named `log_foundry.status`,
      `code.function.name` and `exception.type` are all absent from the attributes, the library's
      values stand, and `droppedAttributesCount == 3`. On an ordinary `info` event the same three
      give `code.function.name` displaced and `log_foundry.status` dropped for its prefix, while
      `exception.type` passes through, so the count is 2; a field named `log_foundry.custom` is
      dropped on either event; with no collision `droppedAttributesCount` is absent.
- [ ] The mark is exactly the one the Description lists. A record built from `<circular>`, a value
      `<unserializable: object>`, `2**63` written as digits, `float("nan")`, and each drop that
      leaves nothing else behind — a collision, the reserved prefix, an empty key — carries no
      `log_foundry.truncated`; a record built from each of `<depth limit>`, `<node limit>`,
      `10**5000`, a lone surrogate, a key that cannot be rendered and an event with
      `truncated: True` carries it, one record per case; an `int` key marks nothing.
- [ ] Every record produced by the criteria above, wrapped by FR-002's envelope, passes FR-002's
      round-trip helper — parsed, re-serialized equal, ids intact — and has no repeated key in its
      attributes or in any `kvlistValue`, asserted separately because `Parse` accepts duplicates;
      `json.dumps(record, allow_nan=False)` also succeeds on each but is not the oracle, and no
      sentinel string planted in a `__repr__` appears in any output (invariants 1 and 8). This
      criterion can close only in the phase that adds the helper and its dependency.
- [ ] `log_record` run over five events, one of them hostile, returns five records, and the other
      four are equal to their solo mappings; no event dict differs after the call (invariant 2: a
      mutation would change what a `MultiSink` sibling delivers).
- [ ] An event with `truncated: True` carries `log_foundry.truncated == true`, so a cut the
      assembly marked is still marked (invariant 8).
- [ ] Serves invariants 1, 2 and 8.

### FR-002: The request envelope groups by resource, and a generated-class round trip proves it is OTLP

#### Description:

`_otel.request_body(records, scope_version)` takes `(resource_key, rendered_record)` pairs in event
order, where `resource_key(event)` is `(service, version, env)` read from the event's own stamps,
and returns the `ExportLogsServiceRequest` JSON as a string. There is **one `resourceLogs` per
distinct key, in first-seen order**, because `configure()` mid-run changes `service`, `version` or
`env` for events not yet built and one batch can hold both. Within a group the records keep event
order; across groups they do not, which OTLP does not require.

**`resource_key` obeys FR-001's totality rules, because `_body` calls it unguarded.** `_item_size`
and `_body` both call it from inside `emit`. `_items` guards `_item_size` per event since SPEC-058
FR-005, so a raise there costs that one event, but `_body` is called from `_deliver` and
`_known_refused` outside any guard, so a raise there fails the batch. Each stamp is read
as FR-001 reads any string: a `str`, or a subclass of one through `str.__str__`, with a lone
surrogate replaced by U+FFFD (rule 3); anything else is `None`, an object that claims to be a `str`
only through `__class__` included (rule 2), and so is every stamp of an event
that is not a `dict`, which is read through `dict.get`, unbound. The key is therefore always a
tuple of exact `str`s and `None`s — hashable, and valid UTF-8. The build review's repro, at `68caacf`, was a `str`
subclass whose `__hash__` raises, set on a stamp through a `TransformSink`: a `RuntimeError`
escaped `emit` and the four innocent events beside it were lost. At `1130461` that subclass still
passes a `TransformSink` unchanged, and `_items` now drops only the one event it was set on, so
totality is what delivers that event too. A lone surrogate in a stamp makes `json_format.Parse`
refuse the whole document; a `TransformSink` now replaces it with U+FFFD and marks the event
(probed at `1130461`), so only a direct `emit` brings one here. The repair is not marked: a mark
belongs to a record, a stamp is configuration rather than record data, and `configure()` already
refuses a stamp that cannot encode (SPEC-055), so only an event rewritten past assembly reaches it.

Each `resourceLogs` carries `resource.attributes` `service.name`, `service.version` and
`deployment.environment.name` from the key, one `scopeLogs` whose `scope` is `{"name":
"log_foundry", "version": <library version>}` with the records under `logRecords`, and the pinned
schema URL at both `resourceLogs` and `scopeLogs` level. The semantic-conventions version is one
constant, `SEMCONV_VERSION = "1.44.0"` — the release current on 2026-10-05 — and the URL is derived
from it **at call time**, with no module-level URL constant: a URL computed at import would not
follow the version, and moving the pin is one edit and a release-noted change. The form is
canonical throughout: a field at its proto3 default is omitted and an empty list is never written,
so an empty `scope_version` leaves `version` out and an event with no usable stamp leaves `resource`
an empty object — which `MessageToDict` writes back identically, and the comparison below depends
on it. `_otel.framing(key, scope_version)` is what one group costs around its records,
`len(request_body([(key, "")], v).encode("utf-8")) - len(request_body([], v).encode("utf-8"))`, in
UTF-8 bytes, the unit `_item_size` charges; whether `request_body` escapes non-ASCII or writes it
raw, the encoded length is the one that counts, since `ensure_ascii=False` makes a stamp such as
`café` longer in bytes than in characters (probed: 22 characters, 27 bytes).

**The proof is a round trip through the generated classes.** Everything above is JSON that could be
shaped wrongly and still look right, and a dev-only `opentelemetry-proto` is what can say so. The
test converts `traceId` and `spanId` from hex to base64 first, because OTLP/JSON deviates from
proto3 JSON there, parses the document into `ExportLogsServiceRequest` with
`google.protobuf.json_format.Parse` (unknown fields refused, which is its default), and compares
`MessageToDict(parsed, use_integers_for_enums=True)` with the converted input. Properties of that
parser decide the shape of the comparison, each measured against `opentelemetry-proto` 1.45.0 and
`protobuf` 7.36.2: `Parse` **accepts snake_case keys** (so parsing alone cannot prove the spelling
is lowerCamelCase, and the re-serialization comparison can); it **accepts a hex id as base64**
without complaint, decoding 32 hex characters into 24 wrong bytes (so the parsed `trace_id.hex()`
must equal the id that was sent); it **refuses an `int64` out of range**, **refuses a lone
surrogate**, and **refuses a document past 100 nested messages** (the reasons for FR-001's rules 5,
3 and 6); it **accepts duplicate keys** (so uniqueness is asserted by the helper itself); and
`MessageToDict` **writes an attribute with an empty key back without its `key`**, which the
comparison catches and which is why FR-001 rule 8 drops such an attribute first.

**The dev dependency costs `mypy` a line, and the build owes it.** `protobuf` creates a `google/`
namespace directory, and with it present `mypy` stops reading
`src/log_foundry/sinks/pubsub.py:140` (at `1130461`) — `from google.cloud import pubsub_v1  # type:
ignore[import-not-found]` — as a missing module and reads it as installed but untyped: an
`unused-ignore` and an `import-untyped` error. `ci.yml` runs `poetry install --with dev` and then
`poetry run mypy`, so this is red in CI the commit the dependency lands. Reproduced in the spec
review, in the build review, and again at `1130461` on the locked `mypy` 2.4.0 (`poetry.lock`) with
`protobuf` 7.36.2 installed: exactly the two errors, at `pubsub.py:140`. The fix, verified by
running it, and re-run at `1130461` (`mypy` exits 0 over the 55 source files): the inline comment becomes `# type: ignore[import-not-found, unused-ignore]`, and
`pyproject.toml` gains `[[tool.mypy.overrides]]` with `module = "google.cloud.*"` and
`ignore_missing_imports = true`. Two alternatives were measured and fail, and the builder should not
retry them: `[import, unused-ignore]` removes the `import-not-found` text that
`tests/test_sink_integration_roster.py` reads to derive `pubsub` into its population, and a third
inline code makes `ruff` rewrap the import (`I001`). `CLAUDE.md`'s "After installing extras
locally" paragraph rests on `google/` being absent in CI, which stops being true, so it is
rewritten in place in the same change, no longer than it is now — the always-loaded set has little
headroom left — keeping `azure/` and naming `google/`, the `unused-ignore` code and the override.

#### Acceptance Criteria:

- [ ] One resource: the document has exactly one `resourceLogs`, whose resource attributes are
      `service.name`, `service.version`, `deployment.environment.name` in that order with the
      event's three stamps, one `scopeLogs` with `scope == {"name": "log_foundry", "version":
      log_foundry.__version__}`, the records in event order, and `schemaUrl ==
      "https://opentelemetry.io/schemas/1.44.0"` at both levels.
- [ ] Events A, B, A where B was built after a `configure(service=...)` give two `resourceLogs`, A
      first holding records 1 and 3 in order and B holding record 2, every record present exactly
      once (invariant 2).
- [ ] A stamp that is absent or not a `str` omits its resource attribute, and events lacking it
      share a group; two events differing only in `version` do not.
- [ ] `resource_key` never raises: a stamp that is a `str` subclass whose `__hash__` raises, and
      one whose `__eq__` and `__str__` lie, are read as their plain text; a stamp holding a lone
      surrogate is read with U+FFFD in its place; a `Claim` stamp (`isinstance` true,
      `str.__str__` raises), a stamp of any other type, and every stamp of an event that is not a
      `dict`, are `None`. `request_body` over five pairs, one keyed by such a
      stamp, returns one document holding all five records, each in its own key's group, and it
      passes the round-trip helper (invariants 1 and 2).
- [ ] An empty `scope_version` leaves the `version` key out, and an event with all three stamps
      unusable gives a `resource` that is `{}`; both round-trip.
- [ ] `framing(key, v)` equals `len(request_body([(key, "")], v).encode("utf-8")) -
      len(request_body([], v).encode("utf-8"))` for three different keys, one of them with a
      non-ASCII stamp (`service="café-世界"`), and a document holding `n` records of one key is
      exactly `len(request_body([], v).encode("utf-8")) + framing(key, v) + n * record_length +
      (n - 1)` bytes long, `record_length` being the record's UTF-8 length.
- [ ] Patching `_otel.SEMCONV_VERSION` to `"9.9.9"` changes both schema URLs in the next
      `request_body` and nothing else, and the module has no URL constant of its own.
- [ ] The round trip over a corpus — every level, `span.start`, `span.end` with and without an
      error, a null parent, a truncated event, nested `fields` including `{}`, `[]`, `None`, `True`,
      `2**63 - 1`, `2**63` and a finite float, `fields={"": 1}`, an event with no usable timestamp,
      two resources, a `kvlistValue` chain and an `arrayValue` chain each cut at FR-001's 24-level
      cap, a record cut by its node budget, and the hostile records of FR-001 — parses at `Parse`'s
      default recursion limit, re-serializes to the converted input, and each parsed
      `trace_id.hex()` and `span_id.hex()` equals the id sent (invariant 8).
- [ ] The comparison is shown able to fail: a snake_case key, an unconverted hex id, an unknown key,
      an explicit proto3 default (`"severityNumber": 0`), a lone surrogate, a chain 32 `kvlistValue`
      levels deep, a repeated attribute key and an attribute with an empty key are each refused by
      `Parse` or by the helper's own checks, asserted by reason, so a round trip that passes
      everything is not mistaken for one that passes this.
- [ ] The test imports the generated classes unconditionally, so a missing dev dependency fails the
      suite rather than skipping the proof.
- [ ] `opentelemetry-proto` sits in `[tool.poetry.group.dev.dependencies]` only: the runtime
      `dependencies` are unchanged, `poetry.lock` is regenerated, and a fresh interpreter importing
      `log_foundry` and `log_foundry.sinks.otlp` leaves neither `google.protobuf` nor
      `opentelemetry` in `sys.modules`.
- [ ] `poetry run mypy` exits 0 in a fresh environment built as `ci.yml` builds it — `poetry install
      --with dev`, no extras — with `opentelemetry-proto` installed; `pubsub.py:140` carries
      `# type: ignore[import-not-found, unused-ignore]`, `pyproject.toml` carries the one
      `google.cloud.*` override, `ruff check` passes on the file, and
      `tests/test_sink_integration_roster.py` still derives `pubsub` into its population.
- [ ] `CLAUDE.md`'s "After installing extras locally" paragraph is rewritten in the same change and
      is no longer, in bytes, than the paragraph it replaces — a rewrite of 472 bytes against the
      476 it replaces was measured to fit, and the paragraph is unchanged at `1130461` — and the
      always-loaded set has room for all three of the build's `CLAUDE.md` edits together: `1130461`
      alone is 37,489 of 38,000 bytes, and this branch's own Out of Scope edit to `CLAUDE.md` adds
      127, so the branch is 37,616 and the Tech Stack Test-row note and the `_otel` Layout mention
      of Phase 5 must add under 384 bytes between them. `sh scripts/docs-lint.sh` exits 0 after the paragraph and the note, and
      again after the Layout mention.
- [ ] Serves invariants 2 and 8.

### FR-003: `OTLPSink` posts OTLP, retries OTLP's statuses, bounds its response, and keeps every body inside the budget

#### Description:

`log_foundry.sinks.otlp.OTLPSink(HTTPSink)` takes `endpoint` (default
`http://localhost:4318/v1/logs`, OTLP/HTTP's default port and logs path) and
`**http_kwargs: Unpack[HTTPPlatformKwargs]` — every `HTTPSink` keyword but `body_format`, which it
pins. `endpoint` is OTLP's own word for it, which the vendor-spelling fence in
`docs/decisions/sink-delivery.md` (SPEC-051) keeps rather than normalizing to the family's `url`,
and it means what the OTLP exporter specification's per-signal endpoint means: a URL used
**exactly as given**, with no path appended. It overrides `_render` (one `LogRecord` as JSON, the
mapping of FR-001), `_body` (`_otel.request_body`, content type `application/json`), `_item_size`,
`_body_reserve`, `_handle_response` (FR-004) and `losses()`; it does not override `emit`, which a
roster lint holds every `HTTPSink` subclass to.

**Retry set.** `HTTPSink._send` retries `status == 429 or 500 <= status < 600`. That condition moves
into a private hook, `HTTPSink._retryable_status(status) -> bool`, whose default is the same
expression; `OTLPSink` overrides it to `429`, `502`, `503`, `504` — OTLP's exact retryable set, so
a `500` is abandoned on the first request, counted, announced and raised like any other
non-retryable status. `Retry-After` is honoured as for every sink. The hook is **read at one site**,
`_send`, so every other HTTP sink is unchanged by construction, and a roster derived from the AST
holds `OTLPSink` as the only subclass that defines it. One inherited behaviour is a **deliberate
exception** to OTLP's rule that nothing outside the set is retried: on a `413` the base class
halves the byte budget and re-sends the same records in smaller requests (SPEC-038). It is kept
because nothing landed — a `413` precedes ingestion — and because OTLP's own server rule for an
oversized body is to answer `413`, which is exactly the answer that search exists to act on.

**The response is read under a bound.** OTLP says a client MUST limit the response body it parses
(RECOMMENDED 4 MiB) and treat an oversized one as a non-retryable error. A second private hook,
`HTTPSink._response_limit() -> int | None`, defaults to `None` — the unbounded read every HTTP sink
has today, with `_attempt` still calling a bare `read()` — and `OTLPSink` returns 4 MiB
(4,194,304). With a limit set, `_attempt` reads at most one byte past it. A non-`2xx` response
longer than the limit is **abandoned on the spot as non-retryable**, whatever its status: counted,
announced, raised. A `2xx` response reaches FR-004 as it was read, and **`_handle_response` itself
tests `len(payload) > self._response_limit()`** — a body of exactly the limit is adjudicated, one
byte more is not. There is no instance flag set in `_attempt` for it to read: the orphan path emits
from application threads into one shared sink (invariant 4), so a flag would carry one thread's
response into another's chunk. `urllib` asks for `Accept-Encoding: identity`, so the body is read
as sent and there is no decompression to bound. The request side of the same specification — a
server MUST bound the request it parses (RECOMMENDED 64 MiB) and answer `413`, and a client SHOULD
bound the request it sends, likewise — is met by the byte budget below, whose 5,000,000 default is
far under it.

**The budget includes the envelope.** A chunk's body is not the sum of its records: each distinct
resource adds a `resource`, a `scopeLogs` wrapper and two schema URLs, and a `configure()` mid-run
can put several resources in one chunk. `_item_size` therefore charges each record
`_otel.framing(key, version)` — the framing its own resource's group costs — plus the separator that
joins it to its neighbour, and `_body_reserve` withholds the `{"resourceLogs":[…]}` wrapper
(`len(request_body([], v).encode("utf-8"))`) — the `LokiSink` method, over-counting when events share a resource,
which is the safe direction: it makes more requests than strictly needed and never one the
destination rejects. The cost is real and stated: a chunk of one resource is charged an envelope per
record, so it carries fewer records than the budget alone would allow. `max_batch_bytes` is measured
on the uncompressed body. `MAX_BATCH_COUNT` and `MAX_BATCH_BYTES` are the inherited generic defaults
(1,000 and 5,000,000).

**Drops are counted through `emit`'s ledger, and `OTLPSink` adds none of its own.** SPEC-058 and
SPEC-059 settled one rule for every sink: a raising `emit` counts nothing, a per-event drop is
counted and announced only when the `emit` that decided it returns, and the worker, the orphan path
or `MultiSink` owns whatever a raise leaves. `HTTPSink.emit` enters a `Deferral` and hands it to
`_items(batch, pending)` and `_drop_refused(chunk, pending)`; `OTLPSink` inherits all three by not
overriding `emit`, `_items` or `_drop_refused`, so the oversize drop, the `413` drop and an
unrenderable event are retry-safe without a line of its own. It owes the rule two things. Its
`losses()` reports `dropped = dropped_oversized + _unrenderable`, as `HTTPSink.losses` and
`ElasticsearchSink.losses` do, because `tests/test_sinks_http_render.py` holds every `HTTPSink`
subclass to folding `_unrenderable` in (FR-005 says how that sweep must change to reach a total
mapping). And it writes no counter that any `losses()` reports as
`dropped` outside the `Deferral` commit, which `tests/test_deferral.py` derives from the tree and
refuses; FR-004's two counters feed `failed`, the upper bound that gate leaves immediate, exactly
as `ElasticsearchSink`'s `item_errors` and `dropped_unadjudicated` do, and are never summed into
`dropped`. `_handle_response(payload, items)` has no ledger parameter and needs none for that.

**Construction.** `endpoint` that is not a `str` raises `TypeError`, and one `HTTPSink`'s URL check
refuses — no `http`/`https` scheme, no host, empty — raises `ValueError`, both naming `endpoint`
rather than `url`. `method` is in `HTTPPlatformKwargs`, but OTLP/HTTP is `POST` only, so any
`method` other than exactly `"POST"` raises `ValueError` naming `method`: accepting it would send
every batch to a server that refuses it, for the life of the process (invariant 13). The check
reads `method` from `http_kwargs` and does not pass a `method=` keyword to `HTTPSink.__init__`:
`tests/test_http_kwargs_roster.py` derives each forwarding sink's shape from the keywords its
forwarding call sets, and a `method=` there would withdraw `method` from `OTLPSink`'s typed
callers. Every other
`HTTPSink` refusal is inherited.

The class docstring records the three lifecycle answers every HTTP subclass records — no transport
lock (SPEC-028), no post-close guard (SPEC-032), no client buffer (SPEC-036) — in the words the
three roster lints in `test_sink_concurrency.py` look for, so the sink is never left outside them
by an answer that exists only in an ancestor's docstring.

#### Acceptance Criteria:

- [ ] `OTLPSink(opener=fake).emit([event])` makes one `POST` to `http://localhost:4318/v1/logs` with
      `Content-Type: application/json`, and the body parses as an `ExportLogsServiceRequest`
      (FR-002's helper).
- [ ] The endpoint is used as written: `"https://c.example:4318/otlp/v1/logs"` is requested as that
      string, and `"http://h:4318"` is requested as `http://h:4318` with no path appended.
- [ ] `headers`, `auth` (a bearer token and a pair), `gzip`, `timeout`, `max_retries`,
      `max_retry_after`, `max_batch_count`, `max_batch_bytes` and `opener` each reach the request or
      the attribute `HTTPSink` stores; with `gzip=True` the body gunzips to the same document and
      `Content-Encoding: gzip` is sent.
- [ ] `OTLPSink(body_format="ndjson")` raises `TypeError` naming `body_format`, and
      `tests/typed_consumer/rejects.py` carries the matching `mypy --strict` error.
- [ ] `OTLPSink(method="PUT")` and `OTLPSink(method="post")` raise `ValueError` naming `method`;
      `OTLPSink(method="POST")` is accepted (invariant 13).
- [ ] With `max_retries=2` and a fake answering each of `429`, `502`, `503` and `504` once and then
      `200`, every one is retried and delivered with `losses().failed == 0`; each of `500`, `501`,
      `400`, `401` and `404` makes exactly one request, raises `SinkDeliveryError` and leaves
      `losses().failed == 1` (invariant 7).
- [ ] `503` on every attempt with `max_retries=2` makes three requests and then raises; the backoff
      waits through `sinks/_retry.wait` on the sink's stop signal, so with the signal set a `503`
      backoff does not sleep (invariant 3).
- [ ] A `413` to a ten-record chunk halves the budget and re-sends the same records in smaller
      requests that are delivered, with `losses().failed == 0` — the inherited exception, asserted
      so it is a documented behaviour and not an accident.
- [ ] Regression guard, not a twin path: `HTTPSink`, `DatadogSink`, `NewRelicSink`, `HoneycombSink`,
      `LokiSink`, `SplunkHECSink` and `ElasticsearchSink` each still retry a `500` and a `501`,
      their existing tests pass unchanged, each still reads a 5 MiB response body whole, a response
      object whose `read` takes no argument still works with every one of them (the base keeps
      calling a bare `read()`), and the AST roster names `OTLPSink` as the only subclass defining
      `_retryable_status` or `_response_limit`. `test_sinks_http_refused.py`'s
      `_IncompleteResponse.read` takes the same optional size, so that suite's sweep over `OTLPSink`
      fails on `IncompleteRead` and not on a `TypeError` from a size argument it cannot take.
- [ ] A `503` whose body is 4 MiB plus one byte makes exactly one request, raises
      `SinkDeliveryError` and leaves `losses().failed == 1`, where the same `503` with a one-byte
      body is retried and a `503` whose body is exactly 4 MiB is retried too; the fake is asked for
      no more than 4 MiB plus one byte of the response.
- [ ] At a `max_batch_bytes` that holds about ten records of the corpus, a chunk of events from
      one resource, and one of events alternating across three resources so that every request
      spans all three, each yield requests whose every body is no longer than `max_batch_bytes` and
      each parses as an `ExportLogsServiceRequest`. An `_item_size` that charges no framing, planted
      as a mutant, fails the **three-resource** half on the length assertion; the one-resource half
      is not asked to catch it, because there the slack left by greedy filling can absorb the single
      uncharged envelope (measured in the build review).
- [ ] A batch of five events, one carrying a stamp that is a `str` subclass whose `__hash__`
      raises and another a stamp with a lone surrogate, both passed to `emit` directly, makes one
      request holding all five records, parseable, with `losses().dropped == 0` and no exception
      out of `emit`. Separately, a batch of five whose only hostile event carries the `__hash__`
      stamp, returned by a `TransformSink` ahead of an `OTLPSink`, also delivers all five in one
      request, since that stamp passes the transform unchanged; the surrogate stamp cannot take
      that route, which replaces it before the sink sees it (invariants 1 and 2).
- [ ] An event whose record plus framing exceeds the budget is dropped and counted in
      `losses().dropped`, with one stderr line naming `OTLPSink` and a byte count and no event
      content, and the events beside it are delivered (invariants 2 and 11).
- [ ] The drop is retry-safe: with that event in a batch whose request answers `503` on every
      attempt, `emit` raises and `losses().dropped == 0` with no ceiling line written; with an
      opener answering `503` once and then `200` through a `Worker`, `dropped == 1` and exactly one
      ceiling line. An `OTLPSink` whose `losses()` leaves out `_unrenderable` fails
      `tests/test_sinks_http_render.py`'s subclass sweep as FR-005 amends it (invariants 2 and 11).
- [ ] `OTLPSink("ftp://h/v1/logs")`, `OTLPSink("http:///v1/logs")` and `OTLPSink("")` raise
      `ValueError` naming `endpoint`; `OTLPSink(5)` raises `TypeError` naming `endpoint`;
      `timeout=-1` and a header containing a newline still raise `ValueError` naming their
      argument (invariant 13).
- [ ] The same three events delivered once from inside a `@trace` span (the worker path) and once
      from `lf.info` outside one (the orphan path) each reach the fake opener as parseable bodies
      carrying all three records, and `health().sink` reads the same ledger (invariant 6).
- [ ] No class in `sinks/otlp.py` defines `emit`; `close()` is the inherited idempotent no-op and a
      batch emitted after it is still posted (SPEC-032); the class docstring carries the three
      lifecycle answers, and `test_sink_concurrency.py`'s concurrency, client-buffer and
      post-close lints each include `otlp.OTLPSink` in their population and pass for it.
- [ ] Serves invariants 1, 2, 3, 6, 7, 11 and 13.

### FR-004: A `2xx` that rejects records is counted and never retried, one that rejects them all raises, and one that cannot be read is counted too

#### Description:

OTLP answers `200` for full success, and for partial success too: the body carries
`partialSuccess` with `rejectedLogRecords` (an `int64`, which OTLP/JSON writes as a decimal string
and accepts as a number) and an `errorMessage`. **`OTLPSink._handle_response` never raises**, on any
bytes: an exception escaping it escapes `emit`, and the worker then re-sends chunks that already
landed. It follows `ElasticsearchSink._parse_bulk_response` in shape and sorts a body into three
outcomes, **but the builder must not copy that method's `except`**: it catches only
`(ValueError, TypeError)`, so a `RecursionError` from a deeply nested body escapes it (probed at
`e8cbd5d`: `json.loads(b"[" * 1_000_000)` raises `RecursionError`, which is not a `ValueError`),
and this method's never-raises criterion demands more. The Elasticsearch sink is not changed.

- **Full success.** A body that is empty, not JSON, not an object, or without a `partialSuccess`
  (absent or `null`) — which is what a server MUST send on success — and a `partialSuccess` whose
  count is `0`, absent or `null`, which OTLP uses to carry a warning in `errorMessage`. The chunk is
  settled; nothing is counted or written. "Not JSON" is a body `json.loads` rejects as malformed or
  too deep, a `RecursionError` included: a proxy's page or a protobuf body is not this sink's to
  adjudicate (Out of Scope), and the request succeeded. **The one parse failure that is not "no
  content" is an over-long bare integer.** A bare 5,000-digit `rejectedLogRecords` makes
  `json.loads` raise `ValueError` ("Exceeds the limit (4300 digits)"), which would read as not-JSON
  and settle as full success while its string twin, `"1" * 5000`, is unadjudicated. The body is
  therefore parsed with `parse_int=str`, which hands every bare integer over as its digit string
  and never converts it (probed at `e8cbd5d`: a 5,000-digit integer arrives as a 5,000-character
  `str`, `-1` as `"-1"`, `true` and `1.5` unchanged), so a bare integer takes the same digit rule as
  a string: `2` and `"2"` are two rejected, and the over-long one is unadjudicated.
- **Rejected.** A `partialSuccess` object whose `rejectedLogRecords` is an integer, or a string of
  ASCII decimal digits, in `1 … 2**63 - 1`. It is added to a new `rejected_records` counter, clamped
  to the chunk's size so a hostile or confused server cannot inflate `failed` past what was sent.
  `losses()` sums it into `failed` beside the abandoned requests, and one `_diag.lost` line names
  the class and the count; its noun is the literal `"log record"`, so it reads `lost 2 log
  record(s)`. **The line never carries `errorMessage`**: it is the server's text, free to repeat an
  attribute the caller logged, and invariant 11 names a type and never a value.
- **Unadjudicated.** Anything present that this sink cannot read: a `partialSuccess` that is not an
  object (`5`, `[]`, `"x"`), a count that is negative, not an integer, a `bool`, a float, an
  out-of-range or over-long digit string, and a `2xx` body over FR-003's response bound. It is
  counted, with the chunk's size as the upper bound, in a new `dropped_unadjudicated` counter that
  `losses()` also sums into `failed`, with one `_diag.lost` line in the same noun naming the class,
  and **not retried**: the request succeeded, so re-sending could duplicate what landed (SPEC-018).
  The chunk is settled.

**Both counters are immediate, like `failed`, and feed `failed` only.** They are written under
`_counter_lock` as `_handle_response` decides them, not through `emit`'s `Deferral`, exactly as
`ElasticsearchSink._parse_bulk_response` writes `item_errors` and `dropped_unadjudicated`: SPEC-058
and SPEC-059 leave a sink's own `failed` an upper bound, and `tests/test_deferral.py` refuses an
immediate store only to a counter some `losses()` reports as `dropped`, which neither is. The
consequence is the one invariant 2 records for `failed`: when every record of a chunk is rejected,
`emit` raises, the worker re-sends the batch, and each attempt adds the chunk's size again; a
rejected-then-delivered second chunk that a later raise re-sends counts the first again. `dropped`
is never touched by them.

A chunk with some records accepted returns delivered and is **not retried** — OTLP forbids
retrying a populated partial success, and a retry would re-send what landed. A chunk whose every
record was rejected returns not-delivered, so if no chunk of the `emit` was delivered it raises
`SinkDeliveryError` (invariant 7) and the worker's retry engages. That retry re-sends records the
server has already refused, which OTLP's no-retry rule does not permit; it cannot duplicate
anything, because nothing landed, and it is bounded by the worker's `max_retries`. Invariant 7
outranks the OTLP rule here, and `failed_batches` moving is how an operator sees permanent
rejection at all.

#### Acceptance Criteria:

- [ ] A `200` whose body is `b""`, `b"{}"`, `b"not json"`, `b"[]"`, `b"[" * 1_000_000` (which
      `json.loads` answers with `RecursionError`), `{"partialSuccess": null}`,
      `{"partialSuccess": {}}`, `{"partialSuccess": {"rejectedLogRecords": null}}` or
      `{"partialSuccess": {"rejectedLogRecords": "0", "errorMessage": "sampled"}}` settles the chunk
      with no counter moved and nothing written to stderr, and `emit` returns.
- [ ] `{"partialSuccess": {"rejectedLogRecords": "2", "errorMessage": "SECRET-123"}}` against five
      records leaves `rejected_records == 2` and `losses().failed == 2`, makes exactly one request,
      and `emit` returns normally; the number `2` in place of the string `"2"` behaves identically.
- [ ] That response writes one stderr line reading `lost 2 log record(s)` and naming `OTLPSink`,
      and `SECRET-123` appears nowhere in stderr, in `losses()`, or in any exception message
      (invariant 11).
- [ ] `rejectedLogRecords` of `"9"` against a five-record chunk counts `5`, never `9` (and, as that
      rejects the whole chunk, raises as in the criterion below).
- [ ] Each of `"-1"`, `"x"`, `true`, `1.5`, `[]`, `"1" * 5000`, `10**30`, the bare number `-1`
      and 5,000 digits written as a bare number (which a default `json.loads` rejects, so a parse
      that read it as not-JSON would settle it as full success), as the count, and each of
      `5`, `[]` and `"x"` as the whole `partialSuccess`, against a five-record chunk makes exactly
      one request, raises nothing, leaves `dropped_unadjudicated == 5` and `losses().failed == 5`,
      and writes one stderr line naming `OTLPSink` and the noun `log record(s)` and carrying no
      response text (invariants 2 and 11).
- [ ] A `200` whose body is 4 MiB plus one byte is read no further than that, is counted
      unadjudicated for the chunk's size, makes one request, and raises nothing; a `200` whose body
      is exactly 4 MiB — a valid `partialSuccess` of two rejected records padded with whitespace —
      is adjudicated normally, leaving `rejected_records == 2` and `dropped_unadjudicated == 0`.
- [ ] Five of five rejected, in one chunk, raises `SinkDeliveryError`, leaves `failed` raised by
      five, and driven through a worker leaves `flush()` falsy and `health().failed_batches`
      non-zero; `rejected_records` there equals five times the attempts the worker made and
      `losses().dropped` is `0` (invariants 2 and 7).
- [ ] Two chunks, the first wholly rejected and the second delivered, return normally and leave
      `failed` raised by the first chunk's size — partial failure does not raise, so nothing that
      landed is re-sent (invariant 2).
- [ ] Eight threads each making 25 `emit`s whose response rejects one record leave
      `rejected_records == 200` and `losses().failed` equal to it, and `losses()` does not block
      behind an in-flight `emit` (invariant 4: the orphan path reaches this sink from application
      threads).
- [ ] A `2xx` whose body is protobuf bytes, or gzip bytes, is read as full success, asserted so the
      limit in Out of Scope is a tested fact rather than a hope.
- [ ] Serves invariants 2, 4, 7 and 11.

### FR-005: The new path is registered everywhere a public sink must be

#### Description:

`log_foundry.sinks.otlp.OTLPSink` is a public import path, and invariant 10 and the `1.0.0` fence
(`docs/decisions/public-api.md`, the import-path entry) make a documented path part of the `1.x`
promise. Registering it is the half of the work a green suite does not volunteer, since several
rosters are hand-listed and one is derived from the source tree, and an unlisted module fails the
derived one. Six existing tests go red the moment `sinks/otlp.py` exists, measured at `1130461`
by adding a stub `HTTPSink` subclass of that name and running the suites that name a roster: the
two HTTP-subclass rosters (`test_the_http_subclass_roster_holds_every_shipped_platform_sink`,
`test_every_shipped_http_subclass_inherits_the_refusal`), the README table row
`test_every_frozen_sink_import_path_still_resolves` reads, the integration roster
(`test_every_sink_that_talks_to_something_real_is_verified_or_says_why_not`), and the two SPEC-058
and SPEC-059 subclass sweeps, `test_every_http_subclass_folds_unrenderable_into_losses` and
`test_every_http_subclass_counts_no_413_drop_when_down`, each of which fails for `OTLPSink` with "has
no recipe". The first four are registrations. The last two need more than a recipe, because both
build their events on the premise that an unknown top-level key reaches the wire, and this mapping
does not carry one. The fold sweep sends `_ev(0, raw=_Opaque())` and expects `dropped == 1`, but a
total mapping ignores `raw` and delivers; with a recipe added it fails on a real request to
`localhost:4318`, and with an opener it fails on `dropped == 0`. The refusal sweep's `_probe_d`
varies the size of its two events through `big=`, which the mapping also ignores, so both bodies are
the same length, `_known_refused` discards the second without offering it, `emit` returns and
`not delivered` fails (probed at `1130461` with a stub whose `_render` is total). So those registrations
land with the sink, in the phase that creates it, the two sweeps are amended there, and the rest
follow.

The README carries the table rows, the amended retry sentence — "bounded `429`/`5xx` retry" is
untrue of this one sink — and a short subsection stating what the sink sends: the attribute names,
the pinned semantic-conventions version, the `log_foundry.` prefix and that it is reserved, the
int64 rule, the `code.function.name` limit and the partial-success behaviour. Those names are what a
consumer's queries will rest on once released, which is the reason they are documented, not left to
the code.

`src/log_foundry/_otel.py` is a new file under the public-API area's tree. `docs/decisions/INDEX.md`
lists it in that area's *Governs* column and `.claude/rules/decisions-public-api.md` carries the
glob, so the private-mapping fence fires when the file is opened — in the **same change** that
creates the file, because `docs-lint` refuses a rule glob that matches no file.

#### Acceptance Criteria:

- [ ] `tests/data/frozen_sink_paths.txt` has the line `OTLPSink log_foundry.sinks.otlp`, and
      `test_every_frozen_sink_import_path_still_resolves` passes with `OTLPSink` resolving as a
      class.
- [ ] The README sink table (the one with an *Import from* column) has an `OTLPSink` row with its
      import path and signature, the batch-limits table has its row stating that it inherits the
      generic defaults, and the retry sentence names `OTLPSink`'s own set; the tests that read the
      README (`test_the_readme_and_all_cannot_drift`, the sink-table parser, the relative-link
      scanner) pass.
- [ ] The README subsection lists every attribute key the sink can emit, and an exact-set test
      asserts that list equals the keys `log_record` emits for a maximal event.
- [ ] Each hand-listed or derived roster names `otlp`: the HTTP-subclass roster in
      `tests/test_public_surface.py` and its twin in `tests/test_sinks_http.py` with its builder
      list, `_FORWARDING_SINKS` in `tests/test_http_kwargs_roster.py` with its exact-count
      assertion (`len(_FORWARDING_SINKS) == 7`, an equality and not a floor) raised to 8, and
      `UNVERIFIED["otlp"]` in `tests/test_sink_integration_roster.py` with the reason a Collector is
      not run. Removing any one of these entries fails its roster, checked once per entry.
- [ ] `tests/test_sinks_http_render.py`'s `_RECIPES` has an `OTLPSink` entry that forwards
      `opener=`, and the two sweeps that read it are amended so they can reach a total mapping.
      `test_every_http_subclass_folds_unrenderable_into_losses` plants a raising `_render` on the
      instance instead of relying on a value `json.dumps` cannot encode, and passes an opener, so it
      holds every subclass, `OTLPSink` included, to `losses().dropped == 1`.
      `test_every_http_subclass_counts_no_413_drop_when_down` carries its size through `fields`,
      the shape assembly produces, so the two events of `_probe_d` differ in length for `OTLPSink`
      and its premise of eight requests holds. The amended fold sweep is run against a subclass whose
      `losses()` leaves out `_unrenderable` and fails on it, asserted by reason.
- [ ] The builder lists in `test_sink_losses.py` and `test_sink_retry.py` gain an `OTLPSink` entry
      and an assertion tying each list, less `HTTPSink` itself, to the derived HTTP-subclass
      population — neither had one, so a sink missing from them failed nothing. That population
      includes `elasticsearch.OpenSearchSink`, which neither list builds at `1130461`
      (`tests/test_public_surface.py:263-271`, `test_sink_losses.py:609-617`,
      `test_sink_retry.py:113-120`), so the tie forces adding it to both as well; removing an entry
      from either list fails it.
- [ ] `tests/typed_consumer/accepts.py` constructs an `OTLPSink` with the platform keyword shape,
      `rejects.py` carries the `body_format` line FR-003 names, and `tests/test_typed_consumer.py`
      passes.
- [ ] No name from `_otel` appears in any `__all__`; `log_foundry.__all__` and `sinks/__init__.py`
      are unchanged, so the only new public surface is the one documented import path.
- [ ] `docs/decisions/INDEX.md`'s *Governs* for the Public API surface area and
      `.claude/rules/decisions-public-api.md` both list `src/log_foundry/_otel.py`, and
      `sh scripts/docs-lint.sh` exits 0 with the file present.
- [ ] Serves invariant 10.

---

## Data Model

```python
# src/log_foundry/_otel.py — private leaf: nothing here is exported, in any __all__, or promised,
# and it imports nothing from the package at runtime (the library version arrives as an argument)
SEMCONV_VERSION: str = "1.44.0"        # the schema URL is derived from it at call time; no constant
SCOPE_NAME: str = "log_foundry"
MAX_NEST: int = 24                      # nested containers that render; the 25th is cut (rule 6)
MAX_NODES: int = 131_072                # values written per record; its docstring says why (rule 7)
ResourceKey = tuple[str | None, str | None, str | None]       # (service, version, env)

def log_record(event: Mapping[str, object]) -> dict[str, object]
    # one OTLP/JSON LogRecord; pure, total, never mutates `event`
def resource_key(event: Mapping[str, object]) -> ResourceKey
    # total: a str (or subclass, via str.__str__) with surrogates repaired, else None; never raises
def request_body(records: Sequence[tuple[ResourceKey, str]], scope_version: str) -> str
    # ExportLogsServiceRequest JSON; one resourceLogs per distinct key, first-seen order
def framing(key: ResourceKey, scope_version: str) -> int
    # the same difference in UTF-8 bytes of request_body([(key, "")], v) and request_body([], v)

# src/log_foundry/sinks/http.py
class HTTPSink:
    def _retryable_status(self, status: int) -> bool      # default: status == 429 or 500 <= status < 600
    def _response_limit(self) -> int | None                # default: None, the unbounded read

# src/log_foundry/sinks/otlp.py — public path, frozen for 1.x at this import path
class OTLPSink(HTTPSink):
    MAX_BATCH_COUNT = 1000                                 # inherited generic defaults
    MAX_BATCH_BYTES = 5_000_000
    MAX_RESPONSE_BYTES = 4 * 1024 * 1024                   # what _response_limit returns
    rejected_records: int                                  # server-rejected records, clamped per chunk
    dropped_unadjudicated: int                             # records in a response it could not read
```

---

## API / Interface Contract

```python
OTLPSink(
    endpoint: str = "http://localhost:4318/v1/logs",
    **http_kwargs: Unpack[HTTPPlatformKwargs],   # headers, auth, gzip, timeout, max_retries, ...
)                                                # TypeError / ValueError naming `endpoint` or `method`

OTLPSink.emit(batch) -> None                     # inherited chunk loop; SinkDeliveryError on total failure
OTLPSink.losses() -> SinkLosses                  # dropped = oversized + unrenderable; failed = abandoned + rejected + unadjudicated

# Example
import log_foundry as lf
from log_foundry.sinks.otlp import OTLPSink

lf.configure(
    service="payments", version="2.14", env="prod",
    sink=OTLPSink("https://collector.internal:4318/v1/logs",
                  headers={"x-api-key": "..."}, gzip=True),
)
```

## Configuration / Environment

No `OTEL_*` variable is read. Configuration is the constructor's arguments, and the three resource
attributes come from `configure(service=, version=, env=)`, which every event already carries.

One dev-only dependency: `opentelemetry-proto`, which brings `protobuf`. Neither is a runtime
dependency and neither is imported by the library. `CLAUDE.md`'s Tech Stack Test row names it, per
that file's rule that a dependency is noted there first. Its `google/` namespace directory reaches
`mypy`'s environment in CI, which is the one-line `pubsub.py` change and the `pyproject.toml`
override FR-002 describes.

## File & Folder Structure

```
src/log_foundry/
├── _otel.py                  # FR-001, FR-002: the mapping, the envelope, the pin
└── sinks/
    ├── http.py               # FR-003: _retryable_status, _response_limit, read in _send / _attempt
    ├── pubsub.py             # FR-002: one comment, `unused-ignore` added to the ignore codes
    └── otlp.py               # FR-003, FR-004: OTLPSink
tests/
├── test_otel_mapping.py      # FR-001
├── test_otel_envelope.py     # FR-002: the round-trip helper other tests import
├── test_sinks_otlp.py        # FR-003, FR-004
├── test_sinks_http.py        # FR-003: FakeResponse.read(self, n=-1); the roster and builders (FR-005)
├── test_sinks_http_render.py # FR-005: the OTLPSink recipe; the fold sweep plants a raising _render
├── test_sinks_http_refused.py # FR-003, FR-005: _IncompleteResponse.read(self, n=-1); the probe-D sweep
├── data/frozen_sink_paths.txt   # FR-005
└── typed_consumer/{accepts,rejects}.py   # FR-003, FR-005
README.md                     # FR-005
pyproject.toml, poetry.lock   # the dev dependency; the `google.cloud.*` mypy override
CLAUDE.md                     # Tech Stack Test row; the "After installing extras locally" paragraph
docs/decisions/INDEX.md       # FR-005: Governs for the Public API area
.claude/rules/decisions-public-api.md   # FR-005: the matching glob
docs/decisions/sink-delivery.md         # Phase 5: the sink rules this spec settles
```

## Implementation Phases

Every phase leaves `main` and the whole suite green: a phase carries whatever registration its own
new module makes a derived test demand, and nothing that test does not demand waits for a later one.

### Phase 1: The mapping

- `_otel.log_record` with the value rules, the integer timestamp, the id handling, the cut, drop and
  node accounting; `tests/test_otel_mapping.py` including the hostile corpus. Every FR-001 criterion
  closes here **except** the one that runs each corpus record through the round-trip helper, which
  cannot close until Phase 2 adds the helper and its dependency; if Phase 1 lands as a PR of its
  own it is named there as owed (`session-rhythm.md`) and settled in Phase 2.
- In the same change as the new file: `src/log_foundry/_otel.py` in the Public API area's *Governs*
  column in `docs/decisions/INDEX.md` and in `.claude/rules/decisions-public-api.md`, since
  `docs-lint` refuses a glob that matches no file, and a run of `sh scripts/docs-lint.sh`.

### Phase 2: The envelope and its proof

- Add `opentelemetry-proto` to the dev group, regenerate `poetry.lock`, and note it in `CLAUDE.md`'s
  Tech Stack Test row; confirm `dependency-review` and `pip-audit` (whose environment installs the
  non-optional `dev` group) are green with the new pair.
- **In the same change, the `mypy` accommodation**: `pubsub.py:140` to
  `# type: ignore[import-not-found, unused-ignore]`, the `google.cloud.*` override in
  `pyproject.toml`, and `CLAUDE.md`'s "After installing extras locally" paragraph rewritten in
  place, no longer than it is now, with `sh scripts/docs-lint.sh` run against the paragraph and the
  Tech Stack note together. Run `poetry run mypy` and `ruff check` in an environment built as
  `ci.yml` builds it, and `tests/test_sink_integration_roster.py`. Do not try `[import,
  unused-ignore]` or a third inline code (FR-002 says why).
- `_otel.resource_key`, `request_body`, `framing`, the pin; the round trip and its must-fail
  fixtures; and the FR-001 criterion that Phase 1 left owed, settled now against the same helper.

### Phase 3: The sink

- `HTTPSink._retryable_status` and `_response_limit` and their reads in `_send` and `_attempt`.
  **`tests/test_sinks_http.py:33`'s `FakeResponse.read(self)` takes no size, and seven other suites
  import it at `1130461`** (`test_sink_losses`, `test_sink_retry`, `test_sinks_elasticsearch`,
  `test_sinks_saas`, `test_sinks_http_refused`, `test_sinks_http_render`, `test_wrapper_failures`),
  so it becomes `read(self, n=-1)`, returning at most `n` bytes when `n` is not negative.
  `test_sinks_http_refused.py:37`'s `_IncompleteResponse.read(self)` overrides it and takes the same
  `n=-1`, or the probe-D sweep reaches `OTLPSink` as a `TypeError` that happens to raise too. The
  base class keeps calling a bare `read()` while `_response_limit()` is `None`, which is what keeps
  every other sink's behaviour unchanged.
- `OTLPSink` with `_render`, `_body`, `_item_size`, `_body_reserve`; the budget, response-bound and
  `method` criteria across one and three resources.
- `_handle_response`, `rejected_records`, `dropped_unadjudicated`, `losses()`; the partial-success
  criteria, every body shape in them driven through `emit`.
- **In the same phase, the registrations six existing tests demand the moment `sinks/otlp.py`
  exists**, each verified red without it (FR-005): `otlp.OTLPSink` in the HTTP-subclass roster of
  `tests/test_public_surface.py` (`test_the_http_subclass_roster_holds_every_shipped_platform_sink`);
  the README sink-table row, which `test_every_frozen_sink_import_path_still_resolves` reads, with
  the batch-limits row and the `OTLPSink log_foundry.sinks.otlp` line in
  `tests/data/frozen_sink_paths.txt` beside it; `otlp.OTLPSink` in the derived-population assertion
  and the builder list of `tests/test_sinks_http.py`
  (`test_every_shipped_http_subclass_inherits_the_refusal`); and `UNVERIFIED["otlp"]` in
  `tests/test_sink_integration_roster.py`
  (`test_every_sink_that_talks_to_something_real_is_verified_or_says_why_not`); and the `OTLPSink`
  recipe in `tests/test_sinks_http_render.py` with the two amended sweeps of FR-005
  (`test_every_http_subclass_folds_unrenderable_into_losses`,
  `test_every_http_subclass_counts_no_413_drop_when_down`), without which both fail for it.

### Phase 4: The rest of the registration

- The README subsection and the amended retry sentence, the exact-set attribute test,
  `_FORWARDING_SINKS` with its exact count raised to 8, the two builder lists with their tie assertions and
  `OpenSearchSink`'s entries, the typed-consumer probe; each roster entry removed once to see it
  fail.

### Phase 5: The ritual

- The seven steps of `docs/process/completion-ritual.md`. **Step 5 is owed, not skipped:** the sink
  rules this spec settles get a `###` entry in `docs/decisions/sink-delivery.md`, written before its
  Contents row and before its fence — OTLP's retry set through a private predicate hook, a partial
  success never retried, every record rejected raising because invariant 7 outranks OTLP's no-retry
  rule, the response read under OTLP's 4 MiB bound, and the envelope charged per record. The
  direction itself is already in `docs/decisions/public-api.md`.
- The other steps: the header status and the INDEX row, the delivery doc, the
  `component-inventory.md` row, `_otel` added to `CLAUDE.md`'s Layout leaf-helper sentence, the
  `CLAUDE.md` Specs prune, and the pending release notes if `docs/release-notes/<next tag>.md`
  exists.

---

## Revision history

- **2026-10-05 — first draft, and the spec-gate and build-frame reviews answered in place.** Written
  and reviewed against `68caacf`.
- **2026-11-02 — refreshed against `1130461`, after SPEC-058, SPEC-059, the `epoch_nanos` fix (#255)
  and the supervisor kit (#272) landed.** Every claim the spec makes about the tree was re-read at
  `1130461`, and the behavioural ones were run. What changed: `HTTPSink._items` now guards
  `_render` and `_item_size` per event (SPEC-058 FR-005), so FR-001's "no guard" premise is restated
  and totality's remaining reasons are named; a `TransformSink` now coerces a lone surrogate, a
  non-finite float and an oversized integer, so the criteria that set them through one now use a
  direct `emit`; `epoch_nanos` is exact, so the inexactness reason is struck and replaced by the
  three that remain (it reads a clock, accepts any ISO shape, lives outside the leaf), with
  `epoch_nanos` kept as the test oracle; FR-003 and FR-004 state how drops and the two new counters
  comply with `Deferral` and the derived gate in `tests/test_deferral.py`; FR-005 corrects the
  roster line numbers, the `FakeResponse` importer count and the `== 7` assertion, and adds the two
  SPEC-058 and SPEC-059 sweeps that go red for any `HTTPSink` subclass and cannot be satisfied by a
  recipe alone; `pubsub.py:137` is now `:140`; `Depends On` gains SPEC-058 and SPEC-059; Out of Scope
  now says what SPEC-057 (`e8cbd5d`) does about the shared-subtree cost and why `_otel` keeps its own
  node budget and pins it equal. Not re-measured, because they were taken on a prototype of a
  mapping that does not exist at `1130461`: the shared-subtree timings in FR-001 rule 7 and the
  cap-20 cycle figures in rule 6. Re-measured at `1130461` against `opentelemetry-proto` 1.45.0 and
  `protobuf` 7.36.2: the `Parse` properties and the 31-versus-32 `kvlistValue` depth in FR-002, and
  the `mypy` failure and its fix on `mypy` 2.4.0.
- **2026-11-02 — pre-push review, nine findings accepted, at `e8cbd5d`.** Rule 8 now renders keys
  as `sanitize` does and replaces only a key that cannot be rendered; rule 7 fixes the walk order
  (library attributes, body, fields) and leaves the mark uncounted; the FR-003 batch of five no
  longer describes a route no transform can take; the `__class__` impostor and the raising
  `__name__` metaclass are criteria; `framing` is in UTF-8 bytes; a timestamp at or before the
  epoch is unusable; a bare over-long `rejectedLogRecords` is unadjudicated, by `parse_int=str`; the
  `_FORWARDING_SINKS` and `INDEX.md` wording is corrected; and FR-004 warns the builder off
  `ElasticsearchSink._parse_bulk_response`'s narrow `except`.
