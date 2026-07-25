# Spec: Payload and Failure Safety

**ID:** SPEC-017
**Status:** Draft
**Last Updated:** 2026-07-24
**Depends On:** SPEC-001 (`build_event`, `model`), SPEC-004 (background worker), SPEC-006 (`MultiSink`)

## Overview

The library promises that logging never breaks the calling application and that a broken destination
degrades logging and nothing more. Today it breaks that promise in three ways, all reachable from
ordinary use. A field the caller passes that JSON cannot serialize — a `datetime`, a `UUID`, a
`Decimal`, any domain object — raises `TypeError`; on the orphan path (a level call with no active
span) that exception lands *synchronously in the caller's own stack frame*, and inside a span it
instead destroys the whole flushed batch, taking every co-batched event from unrelated spans with
it. A single oversized value has no ceiling anywhere in the pipeline, so one large field can push an
otherwise-fine event past a sink's hard limit and get the entire event discarded — including the
error events, whose stack traces are the largest and the most valuable thing the library carries. And
a `MultiSink` whose destinations are all down reports success to the worker on every batch, so the
retry path never engages and the loss is invisible.

This spec makes an event **safe by construction**: by the time any sink sees an event dict, every
value in it is JSON-serializable and bounded in size. It adds the exception's message as a queryable
field instead of leaving it buried in a free-text traceback, makes total `MultiSink` failure visible
to the worker's retry, and gives operators a supported way to read the library's own health counters
— which the README already tells them to check but which no public API exposes.

## Scope

### In Scope

- **Coercion of non-JSON values at event-construction time**, so every sink's `json.dumps` is safe
  without touching any of the 40-plus call sites that make one.
- **Bounded event payloads** — a per-value byte ceiling, a larger dedicated ceiling for
  `error.stack`, a per-mapping key cap, and a nesting-depth cap, each configurable.
- **A queryable `error.message`** (and `error.module`), so filtering an exception no longer means
  substring-matching the formatted traceback.
- **`MultiSink` raising when every child fails**, so the worker's bounded retry sees total loss —
  while keeping today's isolation when at least one child succeeded.
- **A public health snapshot** exposing the worker's `dropped` / `failed_batches` counters.
- **Making queue-overflow drops audible.** They are currently the only failure path in the library
  that writes nothing to stderr.

### Out of Scope

- **Redaction, PII scrubbing, or a secret deny-list.** `TransformSink` remains the seam for this
  (SPEC-006), and a built-in field-name deny-list is a product decision about defaults, not a safety
  fix — a library that silently drops a field named `token` surprises as much as it protects. The
  coercion fallback in FR-001 is deliberately chosen *not* to widen this exposure.
- **Dead-letter queues, error callbacks, and `on_error` hooks.** Failure *routing* is a separate
  design with its own delivery guarantees; this spec only makes existing failures visible.
- **Retryable-vs-permanent classification in the worker.** After FR-001 the dominant permanent
  failure (a serialization `TypeError`) stops occurring, which removes the pressure for it. The
  worker keeps retrying every failure uniformly.
- **Per-event isolation inside a batch.** The worker's retry granularity stays the flattened batch.
  Narrowing it changes the `Sink` protocol's contract, and FR-001 removes the failure mode that made
  batch-level granularity painful.
- **A byte-based bound on the worker queue.** `max_queue` stays a count of submissions. Bounding by
  bytes needs a size estimate per submission on the hot path; FR-002's per-value ceilings cap the
  practical worst case instead.
- **Changing `error.type`.** It stays the bare `type(exc).__name__` that consumers already index.
  FR-003 adds `error.module` alongside it rather than qualifying `type` in place, which would break
  every existing dashboard and query.
- **An `exc_info=`/`exception()` helper on the level emitters.** `log_foundry.error()` still records
  no exception object; only `@trace` produces an `error` field. Worth doing, but it is an API
  addition rather than a safety fix.
- **Retrofitting the individual sinks' `json.dumps` calls.** Explicitly unnecessary: FR-001 holds the
  invariant at construction, so each sink's bare `json.dumps` becomes correct by consequence. A sink
  that adds its own `default=` handler would be duplicating a guarantee it already has.

---

## Functional Requirements

### FR-001: Every event value is JSON-serializable by construction

#### Description:

An event dict must never contain a value `json.dumps` would reject. Coercion happens once, where the
event is assembled, rather than in each sink — one pass per event instead of one per destination
(which matters under `MultiSink`), and it makes the guarantee hold for the non-JSON sinks
(`postgres`, `mongo`, `sqlite`) too.

Coercion applies to the merged `fields` mapping in `build_event`, and to the values
`backfill_baggage` writes after the fact. The coercion table is fixed:

| Input | Result |
|---|---|
| `str`, `int`, `float`, `bool`, `None` | unchanged |
| `datetime`, `date`, `time` | `.isoformat()` |
| `UUID` | `str(value)` |
| `Decimal` | `str(value)` — a string, so precision survives the round-trip |
| `bytes`, `bytearray` | `.decode("utf-8", errors="replace")` |
| `Enum` | `.value` when itself JSON-safe, else `str(value)` |
| `set`, `frozenset`, `tuple` | a `list` of coerced members |
| `Mapping` | a `dict` with `str`-coerced keys and coerced values |
| `Sequence` (non-`str`) | a `list` of coerced members |
| anything else | `f"<unserializable: {type(value).__name__}>"` |

The fallback is a type-name placeholder rather than `repr(value)` on purpose. Architecture §6 refuses
to auto-capture argument and return values precisely so the library cannot leak secrets or PII, and
`repr()` of an arbitrary object routinely prints attribute values — a credential held on a client
object would land in the log. The placeholder identifies what was dropped without disclosing it.

Non-`float` special values (`float("nan")`, `float("inf")`) are left to `json.dumps`, which emits
them as `NaN`/`Infinity`; that is existing behaviour and not changed here.

#### Acceptance Criteria:

- [ ] `json.dumps(event)` succeeds for every event built by `build_event`, `start_event`, and
      `end_event`, for any `fields` mapping the caller supplies.
- [ ] `log_foundry.info("m", when=datetime(2026, 1, 1))` with **no active span** returns normally and
      does not raise into the caller; the emitted event's `fields["when"]` is `"2026-01-01T00:00:00"`.
- [ ] `log_foundry.info("m", oid=UUID(...))`, `amount=Decimal("1.10")`, `raw=b"\xff"`,
      `tags={"b", "a"}` each produce the coercion the table above specifies.
- [ ] A field holding an object with no coercion rule produces exactly
      `"<unserializable: MyClass>"` and the event is still emitted with all its other fields intact.
- [ ] A field holding an object whose `repr()` contains the string `"s3cret"` produces an event whose
      serialized form does not contain `"s3cret"`.
- [ ] A `dict` field with non-string keys (`{1: "a"}`) serializes with the key coerced to `"1"`.
- [ ] A self-referencing container (`d = {}; d["self"] = d`) produces `"<circular>"` at the point of
      recursion, does not raise, and does not recurse without bound.
- [ ] Values passed through `set_baggage` and applied by `backfill_baggage` are coerced by the same
      rules, verified on a `span.end` event.
- [ ] A batch containing one event with a previously-poisonous field is emitted in full by
      `StdoutSink`, and the worker's `failed_batches` stays `0`.

### FR-002: Event payloads are bounded

#### Description:

No single value may be unbounded. Four ceilings, all configurable (FR-006), applied after coercion:

- `max_value_bytes` (default `8192`) — any `str` value, measured in UTF-8 bytes.
- `max_stack_bytes` (default `32768`) — `error.stack` only, which is legitimately long and is the
  field most worth keeping.
- `max_keys` (default `256`) — per mapping; excess keys are dropped in iteration order.
- `max_depth` (default `8`) — nesting levels; deeper structures are replaced.

Truncation of a `str` cuts on a UTF-8 character boundary (never splitting a multi-byte sequence) and
appends the marker `…[truncated]`. `error.stack` is truncated by **keeping its tail**, because
`traceback.format_exception` puts the exception type and the innermost frames last — the head of an
over-long traceback is the least useful part of it. Every other value keeps its head.

Any event to which a ceiling was applied carries top-level `truncated: true`, so a consumer can tell
a complete payload from a clipped one, and an operator can find clipping without diffing sizes.

#### Acceptance Criteria:

- [ ] A field value of 20,000 ASCII characters is emitted at `max_value_bytes` UTF-8 bytes plus the
      `…[truncated]` marker, and the event carries `truncated: true`.
- [ ] A field value whose truncation point falls mid-character (a string of `"é"`) emits a value that
      decodes as valid UTF-8 and is no longer than `max_value_bytes` bytes.
- [ ] A `RecursionError` traceback longer than `max_stack_bytes` produces an `error.stack` of at most
      `max_stack_bytes` bytes that **ends** with the original traceback's final line.
- [ ] `error.stack` is permitted to exceed `max_value_bytes` — a 20,000-byte stack is not clipped to
      8,192.
- [ ] A mapping field with 300 keys emits exactly `max_keys` of them and sets `truncated: true`.
- [ ] A field nested 12 levels deep is replaced at level `max_depth` with `"<depth limit>"` and sets
      `truncated: true`.
- [ ] An event with every value inside all four ceilings does **not** carry a `truncated` key at all
      (absent, not `false`).
- [ ] The 12 base fields (`timestamp`, `trace_id`, `span_id`, … ) are never truncated, and
      `truncated: true` never displaces one.

### FR-003: The exception message is a queryable field

#### Description:

`end_event` records `error.type` and `error.stack`. The exception's message exists only inside the
`stack` blob, so filtering on it means substring-matching free text — the exact anti-pattern the
structured schema exists to avoid. Add `error.message`, and add `error.module` so two same-named
exception classes from different packages are distinguishable.

#### Acceptance Criteria:

- [ ] A `@trace` function raising `ValueError("bad input")` produces `error.message == "bad input"`
      and `error.type == "ValueError"`.
- [ ] The same event carries `error.module` equal to `type(exc).__module__` (`"builtins"` for a
      `ValueError`, the defining module for a custom exception).
- [ ] An exception raised with no arguments (`raise ValueError`) produces `error.message == ""`, not
      a missing key and not `None`.
- [ ] An exception whose `str()` is longer than `max_value_bytes` has its `message` truncated per
      FR-002 while `stack` keeps its own larger ceiling.
- [ ] An exception whose `__str__` itself raises produces
      `error.message == "<unprintable message>"` and the event is still emitted.
- [ ] `error.type` and `error.stack` keep their existing values byte-for-byte for an exception that
      triggers no truncation — a regression guard on existing consumers.
- [ ] The decorator still re-raises the original exception unchanged (arch §4), verified by identity.

### FR-004: `MultiSink` surfaces total failure to the worker

#### Description:

`MultiSink.emit` catches every child exception and returns normally, so the worker records a
successful emit and never retries. When *some* children succeeded that is correct — retrying would
re-deliver duplicates to the healthy ones. When *every* child failed, nothing was delivered, there
are no duplicates to create, and the batch deserves the worker's retry. So: attempt all children
always, then re-raise if none succeeded.

The re-raised exception is the **first** child's, unchanged, preserving its type and traceback; the
others are already on stderr from the per-child warning. An empty `MultiSink` delivers nowhere but
has no failure to report, and must not raise — otherwise a misconfigured empty sink would retry every
batch to exhaustion.

#### Acceptance Criteria:

- [ ] Two children, one raising: `emit` returns normally, the healthy child received the batch, and
      `failed == 1`.
- [ ] Two children, both raising: `emit` raises the **first** child's exception object (verified by
      identity), and `failed == 2`.
- [ ] Both children are attempted before the raise — the second child's `emit` was called even
      though the first raised.
- [ ] Under the real `Worker`, a `MultiSink` whose only child always raises causes
      `failed_batches == 1` after the retry budget is spent (previously `0`).
- [ ] `MultiSink()` with no children accepts a batch, returns normally, and does not raise.
- [ ] `close()` keeps its existing isolate-and-continue behaviour and does **not** gain the re-raise.

### FR-005: Worker health is readable, and overflow drops are audible

#### Description:

The worker counts `dropped` (submissions discarded because the queue was full) and `failed_batches`
(batches abandoned after the retry budget). Both live on a module-private global with no public
accessor, while README §579 tells users to read `worker.dropped`. Expose a snapshot.

Separately, the queue-overflow drop is the only failure path in the library that writes nothing to
stderr — it silently increments a counter nobody can reach. It should warn, but not once per dropped
event: overflow is by nature a high-rate condition, and an unthrottled line per drop is its own
outage. Warn on the first drop, then on every 1000th.

#### Acceptance Criteria:

- [ ] `log_foundry.health()` returns a `Health` snapshot whose `dropped`, `failed_batches`, and
      `queued` reflect the live worker.
- [ ] `log_foundry.health()` called before any worker exists returns a zeroed snapshot and does
      **not** start a worker (verified: no new thread).
- [ ] `Health` is exported from `log_foundry` and included in `__all__`.
- [ ] Filling the queue past `max_queue` increments `dropped` and writes exactly one stderr line for
      the first drop.
- [ ] 2,500 consecutive drops write exactly 3 stderr lines (drops 1, 1000, 2000).
- [ ] The stderr line names the count of drops so far and is prefixed `log-foundry:` like every other
      diagnostic.
- [ ] `shutdown()` followed by `health()` returns the final counters rather than raising.

### FR-006: The ceilings are configurable

#### Description:

The four FR-002 ceilings are `Config` fields settable through `configure()`, so a caller shipping to a
destination with tighter or looser limits can adjust without patching. Defaults are the FR-002
values, chosen so that the overwhelming majority of events are unaffected.

#### Acceptance Criteria:

- [ ] `configure(max_value_bytes=64)` causes a 100-character field to be truncated at 64 bytes.
- [ ] `configure(max_stack_bytes=128)`, `max_keys=2`, and `max_depth=2` each take effect on the next
      event built.
- [ ] Omitting all four from `configure()` yields the FR-002 defaults.
- [ ] `get_config()` reports the four values.
- [ ] A ceiling of `0` or a negative value raises `ValueError` at `configure()` time rather than
      producing empty events.

---

## Data Model

```python
# src/log_foundry/config.py — four new fields on the existing Config
@dataclass
class Config:
    service: str = "unknown"
    version: str = "0.0.0"
    env: str = "dev"
    sink: Sink | None = None
    defaults: dict[str, object] = field(default_factory=dict)
    max_value_bytes: int = 8192      # new — per str value, UTF-8 bytes
    max_stack_bytes: int = 32768     # new — error.stack only
    max_keys: int = 256              # new — per mapping
    max_depth: int = 8               # new — nesting levels

# src/log_foundry/worker.py — the public snapshot
class Health(NamedTuple):
    queued: int           # submissions currently buffered
    dropped: int          # submissions discarded on a full queue
    failed_batches: int   # batches abandoned after the retry budget

# The event gains one optional top-level key and two nested error keys:
#   truncated: bool        # present and True only when a ceiling was applied
#   error: {
#     type: str            # unchanged — bare class name
#     module: str          # new
#     message: str         # new
#     stack: str           # unchanged, now bounded by max_stack_bytes
#   }
```

---

## API / Interface Contract

```python
# src/log_foundry/sanitize.py — new module, the whole surface
def coerce(value: object, *, cfg: Config, _depth: int = 0) -> object:
    """Return a JSON-serializable, size-bounded equivalent of `value`. Never raises."""

def truncate_str(value: str, max_bytes: int) -> tuple[str, bool]:
    """Clip to `max_bytes` on a UTF-8 boundary, keeping the head. Returns (value, was_truncated)."""

def truncate_tail(value: str, max_bytes: int) -> tuple[str, bool]:
    """As above but keeping the tail — for error.stack."""

def sanitize_fields(fields: Mapping[str, object], *, cfg: Config) -> tuple[dict[str, object], bool]:
    """Coerce and bound a whole mapping. Returns (fields, any_truncation_occurred)."""

# src/log_foundry/__init__.py — one new public function
def health() -> Health: ...

# Example
import log_foundry
log_foundry.configure(service="checkout", max_value_bytes=4096)
log_foundry.info("charged", amount=Decimal("10.50"), at=datetime.now())  # no raise
h = log_foundry.health()
if h.dropped:
    print(f"log-foundry dropped {h.dropped} submissions")
```

## Configuration / Environment

Four new `Config` keys, all optional with defaults: `max_value_bytes` (8192), `max_stack_bytes`
(32768), `max_keys` (256), `max_depth` (8). No new environment variables. No new dependencies — the
coercion table uses only `datetime`, `decimal`, `enum`, `uuid`, and `collections.abc` from the
standard library, keeping the core dependency-free.

## File & Folder Structure

```
src/log_foundry/
├── sanitize.py          # new — coercion + truncation, no imports from model/decorator
├── config.py            # + four ceiling fields, + validation
├── model.py             # build_event/backfill_baggage call sanitize; end_event adds message/module
├── worker.py            # + Health, + snapshot method, + throttled overflow warning
├── __init__.py          # + health, Health in __all__
└── sinks/
    └── multi.py         # + re-raise when every child failed

tests/
├── test_sanitize.py            # new — the coercion table, ceilings, circular refs
├── test_model.py               # + error.message/module, truncated marker
├── test_worker.py              # + health snapshot, throttled overflow warning
├── test_sinks_composition.py   # + MultiSink total-failure re-raise
└── test_config.py              # + ceiling defaults, validation
```

## Implementation Phases

### Phase 1: The sanitizer and its configuration

- Add `sanitize.py` with `coerce`, `truncate_str`, `truncate_tail`, `sanitize_fields`.
- Add the four ceilings to `Config` with defaults and `configure()` validation (FR-006).
- Write `tests/test_sanitize.py` covering the full coercion table, all four ceilings, UTF-8 boundary
  cutting, circular references, and depth limits.
- No wiring into the event path yet — this phase leaves the library's behaviour unchanged.

### Phase 2: Wire the event path

- Call `sanitize_fields` in `build_event` for the merged mapping, and in `backfill_baggage` for the
  values it writes post-build (FR-001).
- Set the `truncated` marker when any ceiling fired (FR-002).
- Add `error.message` and `error.module` in `end_event`, applying `max_stack_bytes` to `stack` with
  tail-keeping truncation and `max_value_bytes` to `message` (FR-003).
- Extend `tests/test_model.py`; confirm the orphan path in `api.py` no longer raises.

### Phase 3: `MultiSink` total-failure semantics

- Attempt every child, then re-raise the first exception when none succeeded; keep isolation when any
  succeeded; keep `close()` as-is; no-op for an empty sink list (FR-004).
- Extend `tests/test_sinks_composition.py`, including the end-to-end check that the real `Worker`
  now records `failed_batches` for an all-children-down `MultiSink`.

### Phase 4: Health snapshot and audible overflow

- Add `Health` and a worker snapshot accessor; add `log_foundry.health()` and export both (FR-005).
- Add the throttled first-then-every-1000th overflow warning.
- Extend `tests/test_worker.py`; update the README passage that points at the unreachable
  `worker.dropped` to use `health()` instead.
