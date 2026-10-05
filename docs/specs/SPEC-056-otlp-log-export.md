# Spec: OTLP Log Export — a Private OpenTelemetry Mapping and an OTLP/HTTP JSON Sink

**ID:** SPEC-056
**Status:** Draft
**Last Updated:** 2026-10-05
**Depends On:** SPEC-009, SPEC-017, SPEC-026, SPEC-027, SPEC-028, SPEC-029, SPEC-032, SPEC-036, SPEC-038, SPEC-041, SPEC-048, SPEC-049, SPEC-051, SPEC-055

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
  destination; an `OTLPSink` beside another child is isolated and counted on `MultiSink.failed` and
  is not re-sent when another child took the batch (SPEC-006, SPEC-017 FR-004). That trade is the
  library's and is unchanged.
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
| `level` | `severityNumber` DEBUG 5, INFO 9, WARNING 13, ERROR 17, CRITICAL 21; `severityText` is `level` verbatim |
| `message` | `body`, through the value encoder below |
| `trace_id`, `span_id` | `traceId`, `spanId`, and `flags` 1 (the W3C sampled bit; `format_traceparent` always sets it) |
| `message` of `span.start` or `span.end` | `eventName`, the same string |
| `function`, `log_id` | attributes `code.function.name`, `log.record.uid` |
| `parent_span_id`, `truncated`, `duration_ms`, `status` | attributes `log_foundry.parent_span_id`, `log_foundry.truncated`, `log_foundry.duration_ms`, `log_foundry.status` |
| `error` | attributes `exception.type`, `exception.message`, `exception.stacktrace` |
| `fields` | one attribute per key, through the value encoder below |

**Totality is owed because a batch is the unit of failure.** `HTTPSink._items` calls `_render` with
no guard, so a raising mapping fails the whole `emit`, the worker retries it, and every innocent
neighbour is lost after four attempts — the shape SPEC-055 fixed for `SQLiteSink`. An event can
reach this sink past assembly — a `TransformSink` upstream rewrites events freely — so the mapping
is total over the values assembly produces **and over the ones it cannot**, and the rules below
exist because each was a way to raise, to loop, or to write something a parser refuses.

1. **Every library key is read at the type its target needs, or omitted.** A `str` for every
   string target (`timestamp`, `level`, `function`, `log_id`, `parent_span_id`, `status`, the two
   ids, and each of `error`'s four), a `float` or non-`bool` `int` for `duration_ms`, `True` for
   `truncated`, a `dict` for `error` and `fields`. Anything else is omitted, so a non-string never
   lands in `severityText` or a `stringValue`. `message` alone takes any value, through the value
   encoder: `message: {"a": 1}` is a `kvlistValue` body and `message: 5` an `intValue` one. An
   event that is not a `dict` maps to `{}`, which is still one record per event.
2. **A value cannot steer its own mapping.** Dispatch is on the exact type, or for a subclass on
   its builtin base through that base's unbound methods — `str.__str__`, `int.__repr__`,
   `float.__float__`, `dict.items`, `list.__iter__`, `tuple.__iter__` — the rule `sanitize` already follows with
   `_INT_LT` and `_FLOAT_REPR`. A subclass whose `__str__`, `__repr__`, `__eq__` or `items` lies,
   or raises, therefore cannot change what is written. A `Mapping` or sequence that is not a `dict`
   or `list` is outside the domain (rule 8).
3. **Every string written is valid UTF-8.** A lone surrogate, in a value, a key, a message or any
   `error` string, is replaced by U+FFFD and the record is marked `log_foundry.truncated`, the
   SPEC-055 rule. `json.dumps` escapes a lone surrogate happily, and `json_format.Parse` then
   refuses the document — measured against the pinned versions in FR-002.
4. **Values.** `None` is an empty `AnyValue`; `bool` is tested before `int`; an `int` within int64
   is `intValue` as a decimal string; a finite `float` is `doubleValue`; `str` is `stringValue`; a
   `dict` is `kvlistValue` and a `list` or `tuple` is `arrayValue`, an empty one with no `values`
   key, which is the canonical form; a non-finite float (assembly replaces them, a `TransformSink`
   can add one) is `doubleValue` spelled `"NaN"`, `"Infinity"` or `"-Infinity"`.
5. **Integers beyond int64** — which OTLP cannot carry and a parser refuses — are the
   `stringValue` of their digits, and an integer too long for `sys.get_int_max_str_digits()` is
   `sanitize`'s `<int: ~N digits>` placeholder with the record marked (SPEC-020), decided by
   `bit_length()` and never by converting it, because converting is what raises.
6. **Nesting is bounded in two ways.** A container is cut at 24 levels of nesting: its value
   becomes the `stringValue` `<depth limit>` (`sanitize`'s placeholder) and the record is marked
   `log_foundry.truncated`. 24 and not more because `json_format.Parse` refuses a document past 100
   nested messages, and the deepest shape — a `kvlistValue` chain, three messages a level — is
   accepted at 31 levels and refused at 32 (measured against `protobuf` 7.36.2); the cap leaves
   seven levels of margin, envelope included. It is reachable: `configure(max_depth=)` has no upper
   bound, so an event assembled under a deeper ceiling is cut here. Separately, a container that is
   its own ancestor is detected **by path** and written `<circular>`, as `sanitize` does; like a
   type-name placeholder it is visible on its own and does not set the mark. A depth cap alone does
   not do this: a list holding itself twice is two branches a level, measured at 2,097,151 visits
   and 5.6 seconds at a cap of 20 and doubling with every level beyond it.
7. **Keys are unique.** A key that renders to one already written — two non-`str` keys of one type
   both become `<unserializable: int>` — is dropped, its record's `droppedAttributesCount` counts
   it, and the first stands. OTLP requires unique keys and `json_format.Parse` accepts duplicates, so
   no parser would say so.
8. **Anything else** — a value or key outside the domain assembly produces — becomes the
   `stringValue` `<unserializable: T>` naming only its type, never a `repr` (arch §6).

**The timestamp is converted with integer arithmetic.** `sinks/_time.epoch_nanos` goes through a
float and is inexact at nanosecond scale — measured at `68caacf`, `"2026-10-05T12:34:56.789Z"`
gives `1791203696788999936` — so the nanosecond field would carry a value the event never had. The
mapping accepts exactly the library's own shape: UTC, three fractional digits and a trailing `Z`,
as in `2026-10-05T12:34:56.789Z`. Anything else is unusable, and an unusable timestamp omits both
time fields rather than reading a clock, since OTLP defines `0` as "unknown or missing".

**`exception.type` is module-qualified**: `module.Type`, except that a `builtins` exception, and one
with no recorded module, uses the bare name — so an `OSError` reads `OSError`, as the convention's
own examples do. `error.type` and `error.module` are each already bounded at assembly; the joined
string is not re-clipped.

**Order, collisions and the reserved prefix.** Attributes are the library's, in the order of the
table above, then the user's `fields` in event order. A user field whose key is one the library set
*on this record* is displaced by it; a user field whose key starts with `log_foundry.` is dropped
whether or not the library set that key, so it can never pose as a library attribute; and
`droppedAttributesCount` counts every attribute dropped, by either rule or by rule 7 — a
substitution nobody can see is a silent change to the data, which `sanitize` already refuses.
Everything the mapping *cuts* (rules 3, 5 and 6) sets `log_foundry.truncated`; everything it
*drops* is counted. A user field named `exception.type` on a record where the library set no
exception attribute passes through untouched.

#### Acceptance Criteria:

- [ ] `"2026-10-05T12:34:56.789Z"` maps to `timeUnixNano == "1791203696789000000"`, with
      `observedTimeUnixNano` the same string; `"1970-01-01T00:00:00.001Z"` maps to `"1000000"`.
- [ ] A timestamp that is absent, not a `str`, lacks the `Z`, is before 1970, or is past the largest
      `uint64` nanosecond omits both time fields, and the call returns.
- [ ] The five levels map to 5, 9, 13, 17 and 21; `"TRACE"`, `"info"` and an absent level carry no
      `severityNumber`, and `severityText` is the string verbatim; a `level` of `5`, `None` or
      `{"a": 1}` gives neither field.
- [ ] A 32-hex `trace_id` and 16-hex `span_id` pass through unchanged with `flags == 1`; a malformed
      or all-zero `trace_id` omits `traceId` and `flags`, and a malformed `span_id` omits `spanId`
      only.
- [ ] `eventName` is `"span.start"` or `"span.end"` for those two messages and absent otherwise; a
      caller who logs those exact words gets the label too, the limit `model.backfill_baggage`
      already has.
- [ ] A `span.end` event carrying a parent, `truncated` and an error yields exactly these attribute
      keys in this order: `code.function.name`, `log.record.uid`, `log_foundry.parent_span_id`,
      `log_foundry.truncated`, `log_foundry.duration_ms`, `log_foundry.status`, `exception.type`,
      `exception.message`, `exception.stacktrace`; `parent_span_id: None` and an absent `truncated`
      each omit their key.
- [ ] `error` of `{"type": "OSError", "module": "builtins"}` gives `exception.type == "OSError"`;
      `{"type": "Boom", "module": "mypkg.errors"}` gives `"mypkg.errors.Boom"`; an empty or absent
      module gives the bare name; an `error` that is not a mapping adds no exception attribute.
- [ ] Field values map as follows: `None` to `{}`; `True` to `{"boolValue": true}` and not
      `intValue "1"`; `5` to `{"intValue": "5"}`; `2**63 - 1` and `-2**63` to `intValue` decimal
      strings; `2**63`, `-2**63 - 1` and `10**100` to `{"stringValue": "<digits>"}`; `1.5` to
      `{"doubleValue": 1.5}`; `float("nan")` to `{"doubleValue": "NaN"}`; `"s"` to
      `{"stringValue": "s"}`; `{}` to `{"kvlistValue": {}}`; `[]` to `{"arrayValue": {}}`; and
      `{"a": [1, {"b": None}]}` to the nested `kvlistValue`/`arrayValue` with `{}` for the `None`.
- [ ] Under the default `sys.get_int_max_str_digits()`, `10**5000` and `-10**5000` map to
      `<int: ~N digits>` and the record carries `log_foundry.truncated == true`; neither raises.
- [ ] Library keys of the wrong type — `function: 5`, `log_id: {}`, `parent_span_id: 5`,
      `status: 5`, `duration_ms: "x"`, `truncated: "yes"`, `error: {"type": 5, "message": {"a": 1}}`
      — each omit their attribute; `message: {"a": 1}` gives a `kvlistValue` body, `message: 5` an
      `intValue` body, and an absent `message` no `body`; no `stringValue` or `severityText` in any
      of these records holds a non-string.
- [ ] A lone surrogate (`"a\ud800b"`) as a message, a field value, a field key, an
      `error.message` and a `function` is written `"a�b"` in each place, and the record
      carries `log_foundry.truncated == true`.
- [ ] A `class Lying(int)` whose `__str__` and `__repr__` return `"evil"`, a `str` subclass whose
      `__str__`, `__eq__` and `__hash__` lie, a `float` subclass likewise, and a `dict` subclass
      whose `items` raises, each map exactly as their base value would, and `"evil"` appears nowhere
      in the output; a non-`dict` `Mapping` whose `items` raises maps to `<unserializable: T>`.
- [ ] `x = []; x.append(x); x.append(x)` as a field value returns in under a second, written as an
      `arrayValue` of two `<circular>` strings, unmarked; a list nested 2,000 deep is cut at 24
      levels with `<depth limit>`, and the record carries `log_foundry.truncated == true`.
- [ ] `fields={1: "a", 2: "b"}` gives one `<unserializable: int>` attribute holding `"a"` and
      `droppedAttributesCount == 1`; the same pair nested in a field value is deduplicated the same
      way and counted in the record's `droppedAttributesCount`.
- [ ] On a `span.end` event with an error, user `fields` named `log_foundry.status`,
      `code.function.name` and `exception.type` are all absent from the attributes, the library's
      values stand, and `droppedAttributesCount == 3`. On an ordinary `info` event the same three
      give `code.function.name` displaced and `log_foundry.status` dropped for its prefix, while
      `exception.type` passes through, so the count is 2; a field named `log_foundry.custom` is
      dropped on either event; with no collision `droppedAttributesCount` is absent.
- [ ] Every record produced by the criteria above, wrapped by FR-002's envelope, passes FR-002's
      round-trip helper — parsed, re-serialized equal, ids intact — and has no repeated key in its
      attributes or in any `kvlistValue`, asserted separately because `Parse` accepts duplicates;
      `json.dumps(record, allow_nan=False)` also succeeds on each but is not the oracle, and no
      sentinel string planted in a `__repr__` appears in any output (invariants 1 and 8).
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

Each `resourceLogs` carries `resource.attributes` `service.name`, `service.version` and
`deployment.environment.name` from the key, one `scopeLogs` whose `scope` is `{"name":
"log_foundry", "version": <library version>}` with the records under `logRecords`, and the pinned
schema URL at both `resourceLogs` and `scopeLogs` level. The semantic-conventions version is one
constant, `SEMCONV_VERSION = "1.44.0"` — the release current on 2026-10-05 — and the URL is derived
from it **at call time**, with no module-level URL constant: a URL computed at import would not
follow the version, and moving the pin is one edit and a release-noted change.

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
3 and 6); and it **accepts duplicate keys** (so uniqueness is asserted by the helper itself).

**The dev dependency costs `mypy` a line, and the build owes it.** `protobuf` creates a `google/`
namespace directory, and with it present `mypy` stops reading
`src/log_foundry/sinks/pubsub.py:137` — `from google.cloud import pubsub_v1  # type:
ignore[import-not-found]` — as a missing module and reads it as installed but untyped: an
`unused-ignore` and an `import-untyped` error. `ci.yml` runs `poetry install --with dev` and then
`poetry run mypy`, so this is red in CI the commit the dependency lands. Reproduced in the spec
review and again at authoring with `mypy` 2.4.0 and `protobuf` 7.36.2. The fix, verified by running
it: the inline comment becomes `# type: ignore[import-not-found, unused-ignore]`, and
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
- [ ] Patching `_otel.SEMCONV_VERSION` to `"9.9.9"` changes both schema URLs in the next
      `request_body` and nothing else, and the module has no URL constant of its own.
- [ ] The round trip over a corpus — every level, `span.start`, `span.end` with and without an
      error, a null parent, a truncated event, nested `fields` including `{}`, `[]`, `None`, `True`,
      `2**63 - 1`, `2**63` and a finite float, an event with no usable timestamp, two resources, a
      `kvlistValue` chain and an `arrayValue` chain each cut at FR-001's 24-level cap, and the
      hostile records of FR-001 — parses at `Parse`'s default recursion limit, re-serializes to the
      converted input, and each parsed `trace_id.hex()` and `span_id.hex()` equals the id sent
      (invariant 8).
- [ ] The comparison is shown able to fail: a snake_case key, an unconverted hex id, an unknown key,
      an explicit proto3 default (`"severityNumber": 0`), a lone surrogate, a chain 32 `kvlistValue`
      levels deep and a repeated attribute key are each refused by `Parse` or by the helper's own
      checks, asserted by reason, so a round trip that passes everything is not mistaken for one
      that passes this.
- [ ] The test imports the generated classes unconditionally, so a missing dev dependency fails the
      suite rather than skipping the proof.
- [ ] `opentelemetry-proto` sits in `[tool.poetry.group.dev.dependencies]` only: the runtime
      `dependencies` are unchanged, `poetry.lock` is regenerated, and a fresh interpreter importing
      `log_foundry` and `log_foundry.sinks.otlp` leaves neither `google.protobuf` nor
      `opentelemetry` in `sys.modules`.
- [ ] `poetry run mypy` exits 0 in a fresh environment built as `ci.yml` builds it — `poetry install
      --with dev`, no extras — with `opentelemetry-proto` installed; `pubsub.py:137` carries
      `# type: ignore[import-not-found, unused-ignore]`, `pyproject.toml` carries the one
      `google.cloud.*` override, `ruff check` passes on the file, and
      `tests/test_sink_integration_roster.py` still derives `pubsub` into its population.
- [ ] `CLAUDE.md`'s "After installing extras locally" paragraph is rewritten in the same change and
      is no longer, in bytes, than the paragraph it replaces; `sh scripts/docs-lint.sh` exits 0.
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
has today — and `OTLPSink` returns 4 MiB (4,194,304). `_attempt` then reads at most one byte past
the limit. A non-`2xx` response over it is **abandoned on the spot as non-retryable**, whatever its
status: counted, announced, raised. A `2xx` response over it is handed to FR-004 as unadjudicated.
`urllib` asks for `Accept-Encoding: identity`, so the body is read as sent and there is no
decompression to bound. The request side of the
same specification — a server MUST bound the request it parses (RECOMMENDED 64 MiB) and answer
`413`, and a client SHOULD bound the request it sends, likewise — is met by the byte budget below,
whose 5,000,000 default is far under it.

**The budget includes the envelope.** A chunk's body is not the sum of its records: each distinct
resource adds a `resource`, a `scopeLogs` wrapper and two schema URLs, and a `configure()` mid-run
can put several resources in one chunk. `_item_size` therefore charges each record the framing its
own resource's group costs, and `_body_reserve` withholds the `{"resourceLogs":[…]}` wrapper — the
`LokiSink` method, over-counting when events share a resource, which is the safe direction: it
makes more requests than strictly needed and never one the destination rejects. The cost is real
and stated: a chunk of one resource is charged an envelope per record, so it carries fewer records
than the budget alone would allow. `max_batch_bytes` is measured on the uncompressed body.
`MAX_BATCH_COUNT` and `MAX_BATCH_BYTES` are the inherited generic defaults (1,000 and 5,000,000).

**Construction.** `endpoint` that is not a `str` raises `TypeError`, and one `HTTPSink`'s URL check
refuses — no `http`/`https` scheme, no host, empty — raises `ValueError`, both naming `endpoint`
rather than `url`. `method` is in `HTTPPlatformKwargs`, but OTLP/HTTP is `POST` only, so any
`method` other than exactly `"POST"` raises `ValueError` naming `method`: accepting it would send
every batch to a server that refuses it, for the life of the process (invariant 13). Every other
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
      `LokiSink`, `SplunkHECSink` and `ElasticsearchSink` each still retry a `500` and a `501`, their
      existing tests pass unchanged, each still reads a 5 MiB response body whole, and the AST roster
      names `OTLPSink` as the only subclass defining `_retryable_status` or `_response_limit`.
- [ ] A `503` whose body is 4 MiB plus one byte makes exactly one request, raises
      `SinkDeliveryError` and leaves `losses().failed == 1`, where the same `503` with a one-byte
      body is retried; the fake is asked for no more than 4 MiB plus one byte of the response.
- [ ] At a `max_batch_bytes` that holds about ten records of the corpus, a chunk of events from
      three resources yields requests whose every body is no longer than `max_batch_bytes` and each
      parses as an `ExportLogsServiceRequest`; the same holds for one resource. An `_item_size` that
      charges no framing, planted as a mutant, fails this criterion on the length assertion.
- [ ] An event whose record plus framing exceeds the budget is dropped and counted in
      `losses().dropped`, with one stderr line naming `OTLPSink` and a byte count and no event
      content, and the events beside it are delivered (invariants 2 and 11).
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
- [ ] Serves invariants 2, 3, 6, 7, 11 and 13.

### FR-004: A `2xx` that rejects records is counted and never retried, one that rejects them all raises, and one that cannot be read is counted too

#### Description:

OTLP answers `200` for full success, and for partial success too: the body carries
`partialSuccess` with `rejectedLogRecords` (an `int64`, which OTLP/JSON writes as a decimal string
and accepts as a number) and an `errorMessage`. **`OTLPSink._handle_response` never raises**, on any
bytes: an exception escaping it escapes `emit`, and the worker then re-sends chunks that already
landed. It follows `ElasticsearchSink._parse_bulk_response` and sorts a body into three outcomes.

- **Full success.** A body that is empty, not JSON, not an object, or without a `partialSuccess`
  (absent or `null`) — which is what a server MUST send on success — and a `partialSuccess` whose
  count is `0`, absent or `null`, which OTLP uses to carry a warning in `errorMessage`. The chunk is
  settled; nothing is counted or written.
- **Rejected.** A `partialSuccess` object whose `rejectedLogRecords` is an integer, or a string of
  ASCII decimal digits, in `1 … 2**63 - 1`. It is added to a new `rejected_records` counter, clamped
  to the chunk's size so a hostile or confused server cannot inflate `failed` past what was sent.
  `losses()` sums it into `failed` beside the abandoned requests, and one `_diag.lost` line names the
  class and the count. **The line never carries `errorMessage`**: it is the server's text, free to
  repeat an attribute the caller logged, and invariant 11 names a type and never a value.
- **Unadjudicated.** Anything present that this sink cannot read: a `partialSuccess` that is not an
  object (`5`, `[]`, `"x"`), a count that is negative, not an integer, a `bool`, a float, an
  out-of-range or over-long digit string, and a `2xx` body over FR-003's response bound. It is
  counted, with the chunk's size as the upper bound, in a new `dropped_unadjudicated` counter that
  `losses()` also sums into `failed`, with one `_diag.lost` line naming the class, and **not
  retried**: the request succeeded, so re-sending could duplicate what landed (SPEC-018). The chunk
  is settled.

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
- [ ] That response writes one stderr line naming `OTLPSink` and `2`, and `SECRET-123` appears
      nowhere in stderr, in `losses()`, or in any exception message (invariant 11).
- [ ] `rejectedLogRecords` of `"9"` against a five-record chunk counts `5`, never `9` (and, as that
      rejects the whole chunk, raises as in the criterion below).
- [ ] Each of `"-1"`, `"x"`, `true`, `1.5`, `[]`, `"1" * 5000` and `10**30` as the count, and each of
      `5`, `[]` and `"x"` as the whole `partialSuccess`, against a five-record chunk makes exactly
      one request, raises nothing, leaves `dropped_unadjudicated == 5` and `losses().failed == 5`,
      and writes one stderr line naming `OTLPSink` and carrying no response text (invariants 2
      and 11).
- [ ] A `200` whose body is 4 MiB plus one byte is read no further than that, is counted
      unadjudicated for the chunk's size, makes one request, and raises nothing.
- [ ] Five of five rejected, in one chunk, raises `SinkDeliveryError`, leaves `failed` raised by
      five, and driven through a worker leaves `flush()` falsy and `health().failed_batches`
      non-zero (invariant 7).
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
derived one.

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
      list, `_FORWARDING_SINKS` in `tests/test_http_kwargs_roster.py` with its floor raised by one,
      and `UNVERIFIED["otlp"]` in `tests/test_sink_integration_roster.py` with the reason a
      Collector is not run. Removing any one of these entries fails its roster, checked once per
      entry.
- [ ] The builder lists in `test_sink_losses.py` and `test_sink_retry.py` gain an `OTLPSink` entry
      and an assertion tying each list, less `HTTPSink` itself, to the derived HTTP-subclass
      population — neither had one, so a sink missing from them failed nothing. That population
      includes `elasticsearch.OpenSearchSink`, which neither list builds today
      (`tests/test_public_surface.py:262-270`, `test_sink_losses.py:596-604`,
      `test_sink_retry.py:113-118`), so the tie forces adding it to both as well; removing an entry
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
MAX_NEST: int = 24                      # container levels before a value is cut (FR-001 rule 6)
ResourceKey = tuple[str | None, str | None, str | None]       # (service, version, env)

def log_record(event: Mapping[str, object]) -> dict[str, object]
    # one OTLP/JSON LogRecord; pure, total, never mutates `event`
def resource_key(event: Mapping[str, object]) -> ResourceKey
    # a stamp that is absent or not a str is None
def request_body(records: Sequence[tuple[ResourceKey, str]], scope_version: str) -> str
    # ExportLogsServiceRequest JSON; one resourceLogs per distinct key, first-seen order

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
OTLPSink.losses() -> SinkLosses                  # dropped = oversized; failed = abandoned + rejected + unadjudicated

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

### Phase 1: The mapping

- `_otel.log_record` with the value rules, the integer timestamp, the id handling, the cut and
  collision accounting; `tests/test_otel_mapping.py` including the hostile corpus.
- In the same change as the new file: `src/log_foundry/_otel.py` in the Public API area's *Governs*
  column in `docs/decisions/INDEX.md` and in `.claude/rules/decisions-public-api.md`, since
  `docs-lint` refuses a glob that matches no file, and a run of `sh scripts/docs-lint.sh`.

### Phase 2: The envelope and its proof

- Add `opentelemetry-proto` to the dev group, regenerate `poetry.lock`, and note it in `CLAUDE.md`'s
  Tech Stack Test row; confirm `dependency-review` and `pip-audit` (whose environment installs the
  non-optional `dev` group) are green with the new pair.
- **In the same change, the `mypy` accommodation**: `pubsub.py:137` to
  `# type: ignore[import-not-found, unused-ignore]`, the `google.cloud.*` override in
  `pyproject.toml`, and `CLAUDE.md`'s "After installing extras locally" paragraph rewritten in
  place, no longer than it is now. Run `poetry run mypy` and `ruff check` in an environment built as
  `ci.yml` builds it, and `tests/test_sink_integration_roster.py`. Do not try `[import,
  unused-ignore]` or a third inline code (FR-002 says why).
- `_otel.resource_key`, `request_body`, the pin; the round trip and its must-fail fixtures.

### Phase 3: The sink

- `HTTPSink._retryable_status` and `_response_limit` and their reads in `_send` and `_attempt`;
  `OTLPSink` with `_render`, `_body`, `_item_size`, `_body_reserve`; the budget, response-bound and
  `method` criteria across one and three resources.
- `_handle_response`, `rejected_records`, `dropped_unadjudicated`, `losses()`; the partial-success
  criteria, every body shape in them driven through `emit`.

### Phase 4: Registration

- Frozen-path line, README rows and subsection, every roster including the two new tie assertions
  and `OpenSearchSink`'s entries, the typed-consumer probe; each roster entry removed once to see it
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
