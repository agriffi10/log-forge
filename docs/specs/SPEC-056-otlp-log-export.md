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
  assembly can produce (FR-001).
- The request envelope — resources, scope, schema URL — and a proof, against the generated OTLP
  classes, that what is built is OTLP rather than JSON that looks like it (FR-002).
- `OTLPSink`, an `HTTPSink` subclass at `log_foundry.sinks.otlp`: OTLP's endpoint, retry set and
  byte budget, plus the one private predicate hook on `HTTPSink` that lets a subclass name its
  retryable statuses (FR-003).
- Partial success: a `2xx` that rejects some or all of the records (FR-004).
- Registration of the new public path — README, frozen-path baseline, typed-consumer probe and every
  roster the suite lists or derives for the HTTP family (FR-005).
- One dev-only dependency, `opentelemetry-proto`, with its note in `CLAUDE.md`'s Tech Stack Test row
  and a regenerated `poetry.lock`.

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
- **A protobuf-encoded response body.** A `2xx` whose body is not JSON is read as full success
  (FR-004); a server that answers JSON with protobuf has broken OTLP's same-`Content-Type` rule.
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
| `message` | `body` as a `stringValue` |
| `trace_id`, `span_id` | `traceId`, `spanId`, and `flags` 1 (the W3C sampled bit; `format_traceparent` always sets it) |
| `message` of `span.start` or `span.end` | `eventName`, the same string |
| `function`, `log_id` | attributes `code.function.name`, `log.record.uid` |
| `parent_span_id`, `truncated`, `duration_ms`, `status` | attributes `log_foundry.parent_span_id`, `log_foundry.truncated`, `log_foundry.duration_ms`, `log_foundry.status` |
| `error` | attributes `exception.type`, `exception.message`, `exception.stacktrace` |
| `fields` | one attribute per key, typed as below |

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

**Values.** `None` is an empty `AnyValue`; `bool` is tested before `int`; an `int` within int64 is
`intValue` as a decimal string, and one outside it — which OTLP cannot carry and a parser refuses —
is the `stringValue` of its digits; a finite `float` is `doubleValue`; `str` is `stringValue`; a
`dict` is `kvlistValue` and a `list` or `tuple` is `arrayValue`, an empty one with no `values` key,
which is the canonical form; a non-finite float (reachable only past assembly, which replaces them)
is `doubleValue` spelled `"NaN"`, `"Infinity"` or `"-Infinity"`. Anything else — a value or key
outside the domain assembly produces, which a `TransformSink` upstream can introduce — becomes the
`stringValue` `<unserializable: T>` naming only its type, never a `repr` (arch §6). Nesting is cut
at 64 levels, where the value becomes the `stringValue` `<depth limit>` (`sanitize`'s own
placeholder) and the record is marked `log_foundry.truncated`, because unlike a type-name
placeholder this cut discards structure and invariant 8 asks that every cut be marked. The cap is
what keeps the mapping, which recurses, from reaching Python's own recursion limit on a deep
structure and from never returning on a circular one; assembly's `max_depth` makes it unreachable
for an event `build_event` produced.

**Order and collisions.** Attributes are the library's, in the order of the table above, then the
user's `fields` in event order. A user field whose key is one the library set *on this record* is
displaced by it, and `droppedAttributesCount` counts the displaced — a substitution nobody can see
is a silent change to the data, which `sanitize` already refuses. A user field named
`exception.type` on a record where the library set no exception attribute passes through untouched.

**Totality is owed because a batch is the unit of failure.** `HTTPSink._items` calls `_render` with
no guard, so a raising mapping fails the whole `emit`, the worker retries it, and every innocent
neighbour is lost after four attempts — the shape SPEC-055 fixed for `SQLiteSink`.

#### Acceptance Criteria:

- [ ] `"2026-10-05T12:34:56.789Z"` maps to `timeUnixNano == "1791203696789000000"`, with
      `observedTimeUnixNano` the same string; `"1970-01-01T00:00:00.001Z"` maps to `"1000000"`.
- [ ] A timestamp that is absent, not a `str`, lacks the `Z`, is before 1970, or is past the largest
      `uint64` nanosecond omits both time fields, and the call returns.
- [ ] The five levels map to 5, 9, 13, 17 and 21; `"TRACE"`, `"info"` and an absent level carry no
      `severityNumber`, and `severityText` is the string verbatim (absent when the level is).
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
      strings; `2**63` and `-2**63 - 1` to `{"stringValue": "<digits>"}`; `1.5` to
      `{"doubleValue": 1.5}`; `float("nan")` to `{"doubleValue": "NaN"}`; `"s"` to
      `{"stringValue": "s"}`; `{}` to `{"kvlistValue": {}}`; `[]` to `{"arrayValue": {}}`; and
      `{"a": [1, {"b": None}]}` to the nested `kvlistValue`/`arrayValue` with `{}` for the `None`.
- [ ] On a `span.end` event, user `fields` named `log_foundry.status` and `code.function.name` are
      absent from the attributes, the library's values stand, and `droppedAttributesCount == 2`.
      On an ordinary `info` event the same two fields give `code.function.name` displaced and
      `log_foundry.status` kept (the library set no status there), so the count is 1; with no
      collision `droppedAttributesCount` is absent.
- [ ] The corpus — `bytes`, a `set`, `object()`, a class whose `__repr__` raises and one whose
      `__repr__` returns a sentinel, a non-`str` key, `fields` or `error` that is not a mapping, the
      empty event `{}`, a list nested 2,000 deep and a list containing itself — each returns,
      `json.dumps(record, allow_nan=False)` succeeds on it, the unmappable value is
      `<unserializable: T>`, and the sentinel appears nowhere in the output (invariants 1 and 8).
- [ ] The 2,000-deep list and the self-containing list are each cut at 64 levels with
      `<depth limit>` and the record carries `log_foundry.truncated == true`; a type-name
      placeholder alone does not set it.
- [ ] `log_record` run over five events, one of them from the corpus above, returns five records,
      and the other four are equal to their solo mappings; no event dict differs after the call
      (invariant 2: a mutation would change what a `MultiSink` sibling delivers).
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
constant, `1.44.0` — the release current on 2026-10-05 — and the URL derives from it, so moving
the pin is one edit and a release-noted change.

**The proof is a round trip through the generated classes.** Everything above is JSON that could be
shaped wrongly and still look right, and a dev-only `opentelemetry-proto` is what can say so. The
test converts `traceId` and `spanId` from hex to base64 first, because OTLP/JSON deviates from
proto3 JSON there, parses the document into `ExportLogsServiceRequest` with
`google.protobuf.json_format.Parse` (unknown fields refused, which is its default), and compares
`MessageToDict(parsed, use_integers_for_enums=True)` with the converted input. Three properties of
that parser decide the shape of the comparison, each measured against `opentelemetry-proto` 1.45.0
and `protobuf` 7.36.2: `Parse` **accepts snake_case keys** (so parsing alone cannot prove the
spelling is lowerCamelCase, and the re-serialization comparison can); it **accepts a hex id as
base64** without complaint, decoding 32 hex characters into 24 wrong bytes (so the parsed
`trace_id.hex()` must equal the id that was sent); and it **refuses an `int64` out of range** (the
reason FR-001 retypes one).

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
- [ ] The pin is one constant: patching it to `"9.9.9"` changes both schema URLs and nothing else.
- [ ] The round trip over a corpus — every level, `span.start`, `span.end` with and without an
      error, a null parent, a truncated event, nested `fields` including `{}`, `[]`, `None`, `True`,
      `2**63 - 1`, `2**63` and a finite float, an event with no usable timestamp, and two resources
      — parses, re-serializes to the converted input, and each parsed `trace_id.hex()` and
      `span_id.hex()` equals the id sent (invariant 8).
- [ ] The comparison is shown able to fail: a snake_case key, an unconverted hex id, an unknown key
      and an explicit proto3 default (`"severityNumber": 0`) are each refused by `Parse` or fail the
      equality, asserted by reason, so a round trip that passes everything is not mistaken for one
      that passes this.
- [ ] The test imports the generated classes unconditionally, so a missing dev dependency fails the
      suite rather than skipping the proof.
- [ ] `opentelemetry-proto` sits in `[tool.poetry.group.dev.dependencies]` only: the runtime
      `dependencies` are unchanged, `poetry.lock` is regenerated, and a fresh interpreter importing
      `log_foundry` and `log_foundry.sinks.otlp` leaves neither `google.protobuf` nor
      `opentelemetry` in `sys.modules`.
- [ ] Serves invariants 2 and 8.

### FR-003: `OTLPSink` posts OTLP, retries OTLP's statuses, and keeps every body inside the budget

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
holds `OTLPSink` as the only subclass that defines it.

**The budget includes the envelope.** A chunk's body is not the sum of its records: each distinct
resource adds a `resource`, a `scopeLogs` wrapper and two schema URLs, and a `configure()` mid-run
can put several resources in one chunk. `_item_size` therefore charges each record the framing its
own resource's group costs, and `_body_reserve` withholds the `{"resourceLogs":[…]}` wrapper — the
`LokiSink` method, over-counting when events share a resource, which is the safe direction: it
makes more requests than strictly needed and never one the destination rejects. The cost is real
and stated: a chunk of one resource is charged an envelope per record, so it carries fewer records
than the budget alone would allow. `max_batch_bytes` is measured on the uncompressed body.
`MAX_BATCH_COUNT` and `MAX_BATCH_BYTES` are the inherited generic defaults (1,000 and 5,000,000):
the OTLP/HTTP section of the specification names no request-size limit, and a destination that has
one answers `413`, which the inherited budget search already handles.

**Construction.** `endpoint` that is not a `str` raises `TypeError`, and one `HTTPSink`'s URL check
refuses — no `http`/`https` scheme, no host, empty — raises `ValueError`, both naming `endpoint`
rather than `url`. Every other `HTTPSink` refusal is inherited.

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
- [ ] With `max_retries=2` and a fake answering each of `429`, `502`, `503` and `504` once and then
      `200`, every one is retried and delivered with `losses().failed == 0`; each of `500`, `501`,
      `400`, `401` and `404` makes exactly one request, raises `SinkDeliveryError` and leaves
      `losses().failed == 1` (invariant 7).
- [ ] `503` on every attempt with `max_retries=2` makes three requests and then raises; the backoff
      waits through `sinks/_retry.wait` on the sink's stop signal, so with the signal set a `503`
      backoff does not sleep (invariant 3).
- [ ] `HTTPSink`, `DatadogSink`, `NewRelicSink`, `HoneycombSink`, `LokiSink`, `SplunkHECSink` and
      `ElasticsearchSink` each still retry a `500` and a `501`, their existing tests pass unchanged,
      and the AST roster names `OTLPSink` as the only subclass defining `_retryable_status`
      (invariant 6).
- [ ] A chunk of events from three resources, at a `max_batch_bytes` where the three envelopes are a
      material share of it, yields requests whose every body is no longer than `max_batch_bytes`
      and each parses as an `ExportLogsServiceRequest`; the same holds for one resource.
- [ ] An event whose record plus framing exceeds the budget is dropped and counted in
      `losses().dropped`, with one stderr line, and the events beside it are delivered.
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
- [ ] Serves invariants 3, 6, 7 and 13.

### FR-004: A `2xx` that rejects records is counted and never retried, and one that rejects them all raises

#### Description:

OTLP answers `200` for full success, and for partial success too: the body carries
`partialSuccess` with `rejectedLogRecords` (an `int64`, which OTLP/JSON writes as a decimal string
and accepts as a number) and an `errorMessage`. `OTLPSink._handle_response` reads the body as JSON.
A body that is empty, not JSON, not an object, or carries no `partialSuccess` — which is what a
server MUST send on success — is full success; so is a `partialSuccess` whose count is `0`, which
OTLP uses to carry a warning in `errorMessage`.

A positive count is added to a new `rejected_records` counter, which `losses()` sums into `failed`
beside the abandoned requests, and one `_diag.lost` line names the class and the count. **The line
never carries `errorMessage`**: it is the server's text, free to repeat an attribute the caller
logged, and invariant 11 names a type and never a value. The count is clamped to the chunk's size,
so a hostile or confused server cannot inflate `failed` past what was sent; a count that is
negative, not an integer, a `bool` or a float is unreadable and read as `0`.

A chunk with some records accepted returns delivered and is **not retried** — OTLP forbids
retrying a populated partial success, and a retry would re-send what landed. A chunk whose every
record was rejected returns not-delivered, so if no chunk of the `emit` was delivered it raises
`SinkDeliveryError` (invariant 7) and the worker's retry engages. That retry re-sends records the
server has already refused, which OTLP's no-retry rule does not permit; it cannot duplicate
anything, because nothing landed, and it is bounded by the worker's `max_retries`. Invariant 7
outranks the OTLP rule here, and `failed_batches` moving is how an operator sees permanent
rejection at all.

#### Acceptance Criteria:

- [ ] A `200` whose body is `b""`, `b"{}"`, `b"not json"`, `b"[]"`, `{"partialSuccess": {}}` or
      `{"partialSuccess": {"rejectedLogRecords": "0", "errorMessage": "sampled"}}` settles the chunk
      with `rejected_records == 0`, no counter moved and nothing written to stderr.
- [ ] `{"partialSuccess": {"rejectedLogRecords": "2", "errorMessage": "SECRET-123"}}` against five
      records leaves `rejected_records == 2` and `losses().failed == 2`, makes exactly one request,
      and `emit` returns normally; the number `2` in place of the string `"2"` behaves identically.
- [ ] That response writes one stderr line naming `OTLPSink` and `2`, and `SECRET-123` appears
      nowhere in stderr, in `losses()`, or in any exception message (invariant 11).
- [ ] `rejectedLogRecords` of `"9"` against a five-record chunk counts `5`, never `9` (and, as that
      rejects the whole chunk, raises as in the criterion below); of `"-1"`, `"x"`, `true`, `1.5`
      and `null` it counts nothing and settles the chunk.
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
- [ ] A `2xx` whose body is protobuf bytes is read as full success, asserted so the limit in Out of
      Scope is a tested fact rather than a hope.
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
the pinned semantic-conventions version, the `log_foundry.` prefix, the int64 rule, the
`code.function.name` limit and the partial-success behaviour. Those names are what a consumer's
queries will rest on once released, which is the reason they are documented, not left to the code.

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
      and an assertion tying each list to the derived HTTP-subclass population — neither had one,
      so a sink missing from them failed nothing — and removing the new entry from either fails it.
- [ ] `tests/typed_consumer/accepts.py` constructs an `OTLPSink` with the platform keyword shape,
      `rejects.py` carries the `body_format` line FR-003 names, and `tests/test_typed_consumer.py`
      passes.
- [ ] No name from `_otel` appears in any `__all__`; `log_foundry.__all__` and `sinks/__init__.py`
      are unchanged, so the only new public surface is the one documented import path.
- [ ] Serves invariant 10.

---

## Data Model

```python
# src/log_foundry/_otel.py — private leaf: nothing here is exported, in any __all__, or promised,
# and it imports nothing from the package at runtime (the library version arrives as an argument)
SEMCONV_VERSION: str = "1.44.0"
SCHEMA_URL: str = f"https://opentelemetry.io/schemas/{SEMCONV_VERSION}"
SCOPE_NAME: str = "log_foundry"
MAX_NEST: int = 64                                            # levels before a value is cut
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

# src/log_foundry/sinks/otlp.py — public path, frozen for 1.x at this import path
class OTLPSink(HTTPSink):
    MAX_BATCH_COUNT = 1000                                 # inherited generic defaults
    MAX_BATCH_BYTES = 5_000_000
    rejected_records: int                                  # server-rejected records, clamped per chunk
```

---

## API / Interface Contract

```python
OTLPSink(
    endpoint: str = "http://localhost:4318/v1/logs",
    *,
    **http_kwargs: Unpack[HTTPPlatformKwargs],   # headers, auth, gzip, timeout, max_retries, ...
)                                                # TypeError / ValueError naming `endpoint`

OTLPSink.emit(batch) -> None                     # inherited chunk loop; SinkDeliveryError on total failure
OTLPSink.losses() -> SinkLosses                  # dropped = oversized; failed = abandoned + rejected

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
that file's rule that a dependency is noted there first.

## File & Folder Structure

```
src/log_foundry/
├── _otel.py                  # FR-001, FR-002: the mapping, the envelope, the pin
└── sinks/
    ├── http.py               # FR-003: _retryable_status, read in _send
    └── otlp.py               # FR-003, FR-004: OTLPSink
tests/
├── test_otel_mapping.py      # FR-001
├── test_otel_envelope.py     # FR-002: the round-trip helper other tests import
├── test_sinks_otlp.py        # FR-003, FR-004
├── data/frozen_sink_paths.txt   # FR-005
└── typed_consumer/{accepts,rejects}.py   # FR-003, FR-005
README.md                     # FR-005
pyproject.toml, poetry.lock   # the dev dependency
CLAUDE.md                     # Tech Stack Test row
```

## Implementation Phases

### Phase 1: The mapping

- `_otel.log_record` with the value rules, the integer timestamp, the id handling and collision
  accounting; `tests/test_otel_mapping.py` including the hostile corpus.

### Phase 2: The envelope and its proof

- Add `opentelemetry-proto` to the dev group, regenerate `poetry.lock`, and note it in `CLAUDE.md`'s
  Tech Stack Test row; confirm `dependency-review` and `pip-audit` (whose environment installs the
  non-optional `dev` group) are green with the new pair.
- `_otel.resource_key`, `request_body`, the pin; the round trip and its must-fail fixtures.

### Phase 3: The sink

- `HTTPSink._retryable_status` and its read in `_send`; `OTLPSink` with `_render`, `_body`,
  `_item_size`, `_body_reserve`; the budget criteria across one and three resources.
- `_handle_response`, `rejected_records`, `losses()`; the partial-success criteria.

### Phase 4: Registration

- Frozen-path line, README rows and subsection, every roster, the typed-consumer probe; each
  roster entry removed once to see it fail.

### Phase 5: The ritual

- The seven steps of `docs/process/completion-ritual.md`: the header status and the INDEX row, the
  delivery doc, the `component-inventory.md` row, `_otel` added to `CLAUDE.md`'s Layout leaf-helper
  sentence, the `CLAUDE.md` Specs prune, and the pending release notes if
  `docs/release-notes/<next tag>.md` exists. The register entry and the sweep of the old
  "logs-only" claim were written with this spec and are not repeated.
