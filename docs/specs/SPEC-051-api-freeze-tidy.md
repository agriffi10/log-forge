# Spec: API Freeze Tidy

**ID:** SPEC-051  
**Status:** Draft  
**Last Updated:** 2026-09-02  
**Depends On:** SPEC-034, SPEC-036, SPEC-040, SPEC-042

## Overview

`1.0.0` freezes this library's public surface, and after the tag every shape in it is a
compatibility obligation. The 2026-09-02 pre-1.0 audit found nothing broken in that surface — 28
top-level names, every `__all__` internally consistent, `py.typed` shipping, a `mypy --strict`
consumer clean against it — but it found six shapes that are **free to change today and breaking
to change after the tag**: dataclasses whose field order a third party can bind to positionally,
an internal helper advertised as public, a parameter a typed caller cannot satisfy, a name a
public signature uses and no module exports, and a `**kwargs: object` that accepts anything at
all. This spec closes those before the tag rather than after it. It changes no runtime behaviour:
every event that is delivered today is delivered identically afterwards.

## Scope

### In Scope

- Keyword-only construction for the four public dataclasses, so field order stops being a
  contract.
- `Mapping` instead of `dict` on the two `defaults=` parameters, so a typed caller can pass one.
- Trimming `context.__all__` to the names the package actually re-exports.
- Exporting the names a public signature already uses, and `DEFAULT_SWAP_TIMEOUT`.
- Typing the keywords the seven HTTP platform sinks forward to `HTTPSink`.
- Correcting two public docstrings that describe a shape the code no longer has.
- A typed-consumer probe in the suite: a module type-checked by `mypy --strict` in a subprocess,
  because a test that only exercises the library from inside the library cannot see an invariance
  defect or a keyword-only signature at all.

### Out of Scope

- **`configure(batch_size=…, flush_interval=…, max_queue=…, max_retries=…)`.** The worker's
  tunables stay unreachable from the public API. Adding keyword-only parameters to `configure()`
  is fully additive after `1.0`, so the tag does not force the decision, and the semantics need
  designing rather than bolting on: the worker is built lazily at the first span, so a call before
  that would apply and a call after would silently not, contradicting `configure()`'s documented
  "repeated calls compose rather than reset". Deferred deliberately, recorded in the register.
- **Normalising the sink family's parameter names.** Frozen as they are; the decision and its
  reasoning are recorded in the register by this spec, so the next audit reads a decision rather
  than re-finding an omission.
- **The README.** Every README defect the audit recorded belongs to the release-surface work,
  including the row that names `batch_size`/`flush_interval` as the remedy for `health().dropped`.
- **Console echo.** Still reachable only through `api._console`; additive whenever it is wanted.
- **`Config.defaults` the field.** Widening the parameter is what a caller needs; the field stays
  a `dict` because `get_config()` hands the caller their own copy to do as they like with, and
  annotating it `Mapping` would take that away to fix nothing.

---

## Functional Requirements

### FR-001: The public dataclasses are constructed by keyword

#### Description:

`Health`, `Config`, `SinkLosses` and the `FlushResult`/`ContinueResult` pair are exported types a
third party constructs — `SinkLosses` is required of any sink implementing the optional
`losses()`. All four are `@dataclass(frozen=True)` today and therefore accept positional
arguments, which makes **field order** part of the frozen contract: `Health` alone has twelve
fields, and every one of the last nine was appended by a spec that could only append because
nothing had bound to the order yet. Making them `kw_only=True` ends that. Nothing in `src/` or
`tests/` constructs any of them positionally, so the change is invisible inside the library and
is the last moment it can be made outside it.

#### Acceptance Criteria:

- [ ] `Health(0, 0, 0)`, `SinkLosses(0, 0)`, `FlushResult(True)`, `ContinueResult(True)` and
      `Config("svc")` each raise `TypeError`, and the message names the positional arguments —
      asserted on the message, not on the exception type alone, because a misspelled field name
      raises `TypeError` too.
- [ ] Every keyword construction the library already performs still works: `health()` returns a
      populated `Health` on both the worker and the no-worker path, and `dataclasses.replace` on a
      `Health` still returns one.
- [ ] The typed-consumer probe (FR-006) constructs all five by keyword and type-checks clean.
- [ ] No source file, docstring or document shows a positional construction of any of them; the
      two `losses()` docstrings that print `SinkLosses(0, 0)` say `SinkLosses(dropped=0, failed=0)`.

### FR-002: `defaults=` accepts any mapping

#### Description:

`configure(defaults=)` and `trace(defaults=)` are annotated `dict[str, object] | None`. `dict` is
invariant in its value type, so a caller holding a `dict[str, str]` — the ordinary shape of a
bag of tenant/region labels — is refused by `mypy` and has to widen or cast. This is the identical
defect SPEC-034 FR-004 already fixed on the five level calls' `fields=`, and the fix is the same
one: `Mapping[str, object] | None`. Both call sites already copy what they are given, so nothing
downstream sees a non-`dict`.

#### Acceptance Criteria:

- [ ] A `dict[str, str]` passed to `configure(defaults=…)` and to `trace(defaults=…)` type-checks
      under `mypy --strict` (FR-006's probe is where this is asserted).
- [ ] `configure(defaults=…)` still stores a copy: mutating the caller's mapping after the call
      does not change what `get_config().defaults` reports.
- [ ] `trace(defaults=…)` stores a copy taken once at decoration time, not once per call: mutating
      the caller's mapping after decoration does not change the fields stamped on later spans.
- [ ] Passing a non-`dict` `Mapping` to either works at runtime and produces the same event fields
      a `dict` produces.

### FR-003: `context.__all__` names only what the package re-exports

#### Description:

`log_foundry.context.__all__` lists eleven names, five of which are internal: `current_span`,
`push_span`, `pop_span`, `push_baggage_scope` and `pop_baggage_scope`. `current_span` is the one
that matters — it hands back a **mutable** `Span`, and the module's own `current_trace_context`
docstring says it exists "so nobody is pushed into `current_span`, which is internal". A name in
`__all__` at `1.0` is a name that cannot be withdrawn afterwards. The six that stay are exactly
those `log_foundry` re-exports: `current_baggage_header`, `current_trace_context`,
`current_traceparent`, `get_baggage`, `reset_context` and `set_baggage`.

#### Acceptance Criteria:

- [ ] `log_foundry.context.__all__` is exactly those six names.
- [ ] Every name in it is reachable as an attribute of `log_foundry` itself — the property that
      makes "these six and no others" derived rather than hand-written.
- [ ] `from log_foundry.context import current_span` still works, and the decorator still opens
      and closes spans: removing a name from `__all__` withdraws the claim, not the symbol.

### FR-004: A name a public signature uses is a name a module exports

#### Description:

Three type aliases appear in public constructor signatures and no module exports them, so a caller
annotating a variable of that type has to spell the alias out or reach for a private name:
`GroupIdSource` and `DedupIdSource` (`SQSSink`'s `message_group_id`/`message_deduplication_id`)
and `Backend` (`SentrySink`'s `backend`). Two further names are exported one level down and not at
the top: `flush_sink`, which sits in `sinks.base.__all__` beside `read_losses` and is the probe a
third-party **wrapper** sink needs to forward a flush to its children exactly as `read_losses` is
the one it needs to aggregate their losses; and `DEFAULT_SWAP_TIMEOUT`, which names the bound
`configure(sink=…)` applies and is `DEFAULT_SHUTDOWN_TIMEOUT`'s sibling.

#### Acceptance Criteria:

- [ ] `GroupIdSource` and `DedupIdSource` are in `log_foundry.sinks.sqs.__all__`; `Backend` is in
      `log_foundry.sinks.sentry.__all__`, and the `Backend` docstring no longer says it is not
      exported.
- [ ] `flush_sink` and `DEFAULT_SWAP_TIMEOUT` are in `log_foundry.__all__` and importable from
      `log_foundry`.
- [ ] `log_foundry.DEFAULT_SWAP_TIMEOUT is log_foundry.worker.DEFAULT_SWAP_TIMEOUT` and
      `log_foundry.flush_sink is log_foundry.sinks.base.flush_sink` — an export, not a copy of the
      value under the same name.
- [ ] Every name in every `__all__` this spec touches is importable, and every one is exercised by
      FR-006's probe.

### FR-005: The keywords an HTTP platform sink forwards are typed

#### Description:

Seven sinks — Datadog, Elasticsearch, Honeycomb, Loki, Logstash, New Relic and Splunk HEC — take
`**http_kwargs: object` and forward it to `HTTPSink`, each carrying a
`# type: ignore[arg-type]` on the forwarding call because `object` is not what `HTTPSink` accepts.
The suppression is total: `DatadogSink("k", timeout="not-a-float")` passes `mypy --strict` **and**
constructs, and fails later at the first request. `Unpack[TypedDict]` replaces it. Each sink can
forward only the keywords it does not set or shadow itself, and that set differs by sink — `mypy`
refuses a `**kwargs` TypedDict key that collides with a named parameter — so the shapes are
declared once each in `sinks/http.py` and composed by inheritance rather than restated per module.

#### Acceptance Criteria:

- [ ] `DatadogSink("k", timeout="not-a-float")` is a `mypy --strict` error, and so is an unknown
      keyword; the valid keywords each sink documents still type-check.
- [ ] No call that succeeds at runtime today becomes a type error: `SplunkHECSink(…,
      body_format=…)` and `LogstashSink(…, auth=…)` still check. A call that is a runtime
      `TypeError` today — `DatadogSink("k", body_format=…)` — becoming a type error is the point,
      not a regression, and is asserted as such.
- [ ] No `# type: ignore[arg-type]` remains on any of the seven forwarding calls. Where one is
      still owed it is `[misc]` and covers only the popped `headers` key.
- [ ] A derived test asserts the TypedDicts cannot drift from `HTTPSink`: the widest one's keys are
      exactly `HTTPSink.__init__`'s keyword-only parameters, and every narrower one's keys are a
      subset. A keyword added to `HTTPSink` and to no TypedDict fails it.
- [ ] Every one of the seven sinks constructs and delivers as it does today, with the same
      keywords, in the existing suite.

### FR-006: A typed consumer, checked as a consumer

#### Description:

Every acceptance criterion above about *types* is invisible to a test that imports the library and
calls it: `mypy` checks `src` only, and the invariance in FR-002 was live for the whole life of the
project underneath a green gate. The probe is a module written as a third party writes one —
importing every name in `log_foundry.__all__`, constructing every public dataclass by keyword,
passing a `dict[str, str]` to both `defaults=` parameters, and calling each of the seven HTTP
sinks — run through `mypy --strict` in a subprocess from the suite. It is paired with a **negative**
module that must be *rejected*, asserting the specific error codes, because a probe that has
stopped resolving the library's types passes silently and a corpus of only-passes cannot see that.

#### Acceptance Criteria:

- [ ] The positive probe type-checks clean under `mypy --strict` at the repo's floor
      (`--python-version 3.12`), and the test fails with mypy's own output when it does not.
- [ ] The negative probe is rejected, and the test asserts the error **codes** it expects —
      `call-arg` for a positional dataclass construction, `arg-type` for a wrongly-typed forwarded
      HTTP keyword — not merely a non-zero exit, since an unresolved import exits non-zero too.
- [ ] Both probes run `mypy` through the interpreter running the suite, so the library they check
      is the one under test rather than whatever another environment has installed.
- [ ] The pair is proved to bite: each of the two is defeated in turn — the positive by reverting
      one annotation, the negative by removing one deliberate error — and observed to redden.

---

## Data Model

```python
# src/log_foundry/worker.py, config.py, sinks/base.py, results.py
@dataclass(frozen=True, kw_only=True)
class Health: ...      # 12 fields; order stops being a contract
@dataclass(frozen=True, kw_only=True)
class Config: ...
@dataclass(frozen=True, kw_only=True)
class SinkLosses: ...
@dataclass(frozen=True, kw_only=True)
class _Result: ...     # FlushResult / ContinueResult inherit kw_only per field

# src/log_foundry/sinks/http.py — the forwardable keyword shapes, composed by inheritance
# so each key is declared exactly once.
class HTTPForwardKwargs(TypedDict, total=False):
    method: str
    headers: dict[str, str]
    gzip: bool
    max_retry_after: float
    max_batch_count: int | None
    max_batch_bytes: int | None
    opener: Callable[..., Any] | None

class HTTPRetryKwargs(HTTPForwardKwargs, total=False):
    timeout: float
    max_retries: int

class HTTPAuthKwargs(HTTPForwardKwargs, total=False):
    auth: str | tuple[str, str] | None

class HTTPPlatformKwargs(HTTPRetryKwargs, HTTPAuthKwargs, total=False): ...

class HTTPKwargs(HTTPPlatformKwargs, total=False):
    body_format: str
```

Which sink takes which, and why it cannot take a wider one:

| Sink | Shape | Owns (so cannot forward) |
|---|---|---|
| `SplunkHECSink` | `HTTPKwargs` | nothing but `headers`, which it pops |
| `DatadogSink`, `HoneycombSink`, `NewRelicSink`, `LokiSink` | `HTTPPlatformKwargs` | `body_format` |
| `ElasticsearchSink` | `HTTPRetryKwargs` | `body_format`, and `auth` as its own parameter |
| `LogstashSink` | `HTTPAuthKwargs` | `body_format`, `timeout`, `max_retries` as its own |

---

## API / Interface Contract

```python
# unchanged names, changed annotations
def configure(*, ..., defaults: Mapping[str, object] | None = None, ...) -> None
def trace(func: F | None = None, *, name: str | None = None,
          defaults: Mapping[str, object] | None = None) -> F | Callable[[F], F]

# log_foundry.__all__ gains exactly two names
DEFAULT_SWAP_TIMEOUT: float
def flush_sink(sink: object) -> bool

# log_foundry.context.__all__ shrinks to the six the package re-exports
__all__ = ["current_baggage_header", "current_trace_context", "current_traceparent",
           "get_baggage", "reset_context", "set_baggage"]

# a platform sink, as a consumer sees it
class DatadogSink(HTTPSink):
    def __init__(self, api_key: str, *, site: str = "datadoghq.com", service: str | None = None,
                 ddtags: str | None = None, **http_kwargs: Unpack[HTTPPlatformKwargs]) -> None
```

## Configuration / Environment

None. No new settings, env vars or dependencies; `TypedDict` and `Unpack` are `typing` members
available on the 3.12 floor.

## File & Folder Structure

```
src/log_foundry/
├── __init__.py            # + DEFAULT_SWAP_TIMEOUT, flush_sink in __all__
├── config.py              # Config kw_only; configure(defaults=) Mapping
├── context.py             # __all__ trimmed to six
├── decorator.py           # trace(defaults=) Mapping, copied once at decoration
├── results.py             # _Result kw_only
├── worker.py              # Health kw_only; docstring corrections
└── sinks/
    ├── base.py            # SinkLosses kw_only
    ├── http.py            # the five TypedDicts; merge_headers accepts one
    ├── datadog.py  honeycomb.py  newrelic.py  loki.py
    ├── elasticsearch.py  logstash.py  splunk.py
    ├── sqs.py             # + GroupIdSource, DedupIdSource in __all__
    ├── sentry.py          # + Backend in __all__
    └── filtering.py  transform.py   # SinkLosses(0, 0) -> keyword form in two docstrings
tests/
├── test_public_surface.py     # FR-001..FR-004 assertions
├── test_typed_consumer.py     # FR-006 runner
└── typed_consumer/
    ├── accepts.py             # must type-check clean
    └── rejects.py             # must be rejected, by code
```

## Implementation Phases

### Phase 1: Keyword-only dataclasses and the mapping parameters

- `kw_only=True` on `Health`, `Config`, `SinkLosses`, `_Result`.
- `Mapping[str, object] | None` on `configure(defaults=)` and `trace(defaults=)`, with `trace`
  taking its copy once at decoration time.
- Correct the two `SinkLosses(0, 0)` docstrings.
- Run the suite: this phase is where a positional construction anywhere in the repo surfaces.

### Phase 2: The exports and the docstring corrections

- Trim `context.__all__`; add `GroupIdSource`/`DedupIdSource`, `Backend`, `flush_sink`,
  `DEFAULT_SWAP_TIMEOUT`, and correct the `Backend` docstring.
- Rewrite the `Health` class docstring's opening paragraph: keyword-only is now why appending a
  field is safe, and "index access" was never true of a dataclass.
- Add `inherited_sink`, `orphan_lost` and `in_span_lost` to `health()`'s `Returns:`.
- Assertions in `test_public_surface.py` for FR-001, FR-003 and FR-004.

### Phase 3: The forwarded HTTP keywords

- The five `TypedDict`s in `sinks/http.py`; `merge_headers` accepts one.
- `Unpack[…]` on all seven sinks; drop each `# type: ignore[arg-type]`, leaving `[misc]` only
  where the popped `headers` key demands it.
- The derived anti-drift test against `HTTPSink.__init__`'s signature.

### Phase 4: The typed-consumer probe

- `tests/typed_consumer/accepts.py` and `rejects.py`, and the subprocess runner.
- Defeat each half in turn and confirm it reddens.
- Completion ritual: status, `INDEX.md`, delivery doc, and the register entries — the API-freeze
  decision, the sink-naming freeze, and the deferred worker tunables.
