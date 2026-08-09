# Spec: The Public API Freeze

**ID:** SPEC-034  
**Status:** Draft  
**Last Updated:** 2026-08-07  
**Depends On:** SPEC-026, SPEC-030, SPEC-033

## Overview

`1.0.0` puts the public API under semantic versioning. Every inconsistency still in a shipped
signature at that moment stops being a wart and becomes a promise: fixing it afterwards costs a
**major version**. This spec is the list of things worth changing while it is still free.

An earlier draft of this spec carried four unrelated corrections that a README review happened to
trip over. The 2026-08-07 audit then examined the public surface deliberately
(`docs/audits/2026-08-07-pre-1.0.md`, P1–P9), and the result is a different spec: the two
signature fixes from that draft survive as FR-001 and FR-002, and the rest of it moved to
SPEC-036 (loss visibility) and SPEC-038 (sinks).

**FR-003 is the one that is not cosmetic.** `get_config()` returns the live mutable config
singleton, so the sink swap SPEC-030 exists to provide has a public back door beside it:

```
configure(sink=A) → work() → get_config().sink = B → work()
  A got 4    B got 0    config claims B    incomplete_swaps=0    A never closed
  get_config().max_value_bytes = 0   accepted (configure() rejects it)
```

That is SPEC-030's defect exactly, reachable with no underscore, plus a bypass of the ceiling
validation `configure()` performs. Freezing "the config object is yours to mutate" would make the
swap machinery advisory for the whole of `1.x`.

## Scope

### In Scope

- `SQSSink`'s positional injected client.
- `SentrySink`'s `sdk=` kwarg.
- `get_config()` handing out the live singleton.
- `echo` and `message` as reserved words on the emitters.
- The extension points missing from `__all__`.
- `stop_signal` as an undeclared attribute assigned onto third-party objects.
- `flush()` and `continue_trace()` returning one bit.
- `Health` and `SinkLosses` freezing their tuple shape.

### Out of Scope

- **Renaming `producer=` or `connection=` to `client=`.** A survey of every sink shows the
  injection kwarg follows the **service's own vocabulary**, consistently: `client` (10 sinks),
  `producer` (Kafka, Azure Event Hubs — both genuinely producers), `connection` (Postgres,
  RabbitMQ, SQLite — all connections), `logger` (`LoggingSink`), `opener` (`HTTPSink`,
  `SentrySink`). That is a convention, not drift. `sdk` is the only name with no family.
- **Aligning `KinesisSink(partition_key_field=)` with `KafkaSink(key_field=)`.** Cut after review
  and it stays cut: SQS names the same concept a third way (`message_group_id`), so two names
  survive either way and the rename buys nothing. A real alignment across all three is a `1.x`
  discussion with an argument behind it.
- **Making the worker's tunables configurable** (audit P8). Real — a user watching
  `health().dropped` climb has no remedy but to log less — but it is a *feature*, additive, and
  therefore free to add in `1.x`. Only things that cost a major version belong here.
- **Re-homing `MemorySink`/`NullSink`/`StderrSink` out of `sinks.util`** (audit P10). Also a
  major-version cost, and genuinely worth doing — but it is a pure move with no design content,
  and it can ride with SPEC-038, which is already touching the sink package.
- **`**http_kwargs: object` → `Unpack[HTTPOptions]`** (audit P10). Type-level only; it changes no
  runtime behaviour and can land any time.

---

## Functional Requirements

### FR-001: `SQSSink`'s injected client is keyword-only

#### Description:

`SQSSink(queue_url, client=None, *, ...)` accepts a positional client. The rule every other sink
follows, and this one breaks:

> **A sink's positional parameters identify its destination. An injected client or transport
> object is keyword-only.**

Measured across every sink class, five take more than one positional parameter —
`HoneycombSink(api_key, dataset)`, `SplunkHECSink(url, token)`, `SyslogSink(host, port)`,
`TransformSink(inner, fn)` and `SQSSink(queue_url, client)`. In the first four both parameters
*are* the destination, or the wrapper's two subjects. `SQSSink` is the only one whose second
positional is an injected transport beside an identifier that already names the destination.
`StdoutSink(stream)` and `LoggingSink(logger)` are not violations: there the stream or logger
**is** the destination identity.

Breaking, taken now precisely because `1.0.0` has not shipped. Nothing in the repository or the
README uses the positional form.

#### Acceptance Criteria:

- [ ] AC-1: `SQSSink(url, client=fake)` works; `SQSSink(url, fake)` raises `TypeError`.
- [ ] AC-2: A test asserts that **no sink class in `sinks/` takes a parameter named `client`,
      `sdk`, `producer`, `connection` or `opener` positionally**, derived from parameter names
      across the package rather than a sink list, so a later sink is covered without anyone
      remembering. It is a **blacklist of five names, not a rule derived from syntax**, and says
      so: role is not inferable from a name alone — `stream` is a positional *transport* in
      `StdoutSink` and a positional *destination* in `RedisStreamsSink`. Verified today: across
      all 39 sink classes with an `emit`, exactly one such parameter is positional.
- [ ] AC-3: No call site in `src/`, `tests/` or `README.md` passes it positionally.

### FR-002: `SentrySink` injects through `client=`

#### Description:

`SentrySink(dsn=None, *, sdk=None, ...)` is the only sink whose injection kwarg has no family.
`sdk` also names the *module* rather than the thing injected, which is what the other names get
right.

#### Acceptance Criteria:

- [ ] AC-1: `SentrySink(client=fake_sdk)` works and `sdk=` raises `TypeError` — no alias. An
      alias would have to live for the whole of `1.x`, which is the cost this spec exists to
      avoid.
- [ ] AC-2: `sdk` appears nowhere in `src/`, `tests/` or `README.md`. FR-001's roster test cannot
      observe this rename — `SentrySink.sdk` is *already* keyword-only — which is why this is a
      grep rather than a reuse.
- [ ] AC-3: The attribute is renamed with the parameter, so `sink.client` reads as it does on the
      other ten.

### FR-003: `get_config()` cannot be used to mutate the live config

#### Description:

`get_config()` returns `_config` itself. Two consequences, both measured: assigning `.sink`
retargets what the config *reports* while every event continues to the sink the worker captured —
SPEC-030's defect, reachable publicly — and assigning a ceiling bypasses `_require_positive`, so
`max_value_bytes = 0` is accepted and would empty every event it touched.

**The design decision is what `get_config()` returns instead.** A frozen copy is the
straightforward answer: `Config` becomes `frozen=True` and `get_config()` returns
`dataclasses.replace(_config)`, so reads keep working unchanged and writes raise
`FrozenInstanceError`. The cost is one object per call, on a function that is not on the hot path
— the library reads `_config` directly internally.

The alternative, returning the singleton and documenting "do not mutate", is what exists today
and is what this FR exists to end.

#### Acceptance Criteria:

- [ ] AC-1: `get_config().sink = X` raises rather than silently retargeting the config.
- [ ] AC-2: `get_config().max_value_bytes = 0` raises; the only route to a ceiling is
      `configure()`, which validates.
- [ ] AC-3: Every documented read still works — `get_config().service`, `.sink`, `.defaults` and
      the four ceilings — and the README's examples are unchanged.
- [ ] AC-4: Mutating the returned object cannot affect the library's behaviour even if a caller
      defeats the freeze (e.g. `object.__setattr__`): the returned object is a **copy**, so the
      internal `_config` is unreachable through it. A test asserts identity is *not* shared.
- [ ] AC-5: `defaults` — a mutable `dict` on the dataclass — is copied too, or the freeze is
      cosmetic. A test mutates the returned `defaults` and asserts the library's is unchanged.
- [ ] AC-6: The library's own internals do not go through `get_config()` on any hot path, so this
      adds no per-event allocation. A test or a benchmark shows the event path is unchanged.

### FR-004: `echo` and `message` stop being reserved words

#### Description:

`def info(message: str, *, echo: bool = False, **fields: object)` steals two ordinary words from
the caller's field namespace.

```
lf.info("x", echo="incoming payload echoed back")   → field silently dropped, console echo turned on
lf.info("x", message="y")                           → TypeError: multiple values for 'message'
```

The first is the bad one: a real field is discarded with no error, *and* an unwanted stderr line
appears. `echo` is an ordinary domain word for anything proxying or replaying.

The escape hatch is an explicit `fields=` parameter — `info("x", fields={"echo": ...})` — which
also gives a caller a way to pass a key that is not a Python identifier at all. Adding it is
additive; what is *not* additive is the decision to keep `**fields` alongside it, which is why
this belongs here.

#### Acceptance Criteria:

- [ ] AC-1: `info("x", fields={"echo": "v", "message": "w", "not-an-identifier": 1})` puts all
      three in the event's `fields`.
- [ ] AC-2: `**fields` still works for every non-reserved name, unchanged.
- [ ] AC-3: A key given in both `fields=` and `**fields` resolves deterministically, and the
      docstring says which wins.
- [ ] AC-4: `echo=` still controls the console echo — this FR adds a route to the field, it does
      not move the flag.
- [ ] AC-5: The two reserved names are documented as reserved, with `fields=` named as the way
      round them.
- [ ] AC-6: The same treatment reaches all five emitters, derived rather than hand-applied.

### FR-005: The extension points are exported

#### Description:

`Sink` — the extension point the README documents over 40 lines — is not importable from
`log_foundry`, nor from `log_foundry.sinks` (whose `__init__.py` is empty). Nor are `Config` (the
return type of a public function), `read_losses`, or `get_baggage`. Baggage is publicly settable
and readable only as a serialized header the caller must re-parse.

None of this is a *behaviour* change, but each is a public-surface decision that reads as
deliberate once `1.0.0` freezes: a name absent from `__all__` at 1.0 says "not part of the API".

#### Acceptance Criteria:

- [ ] AC-1: `from log_foundry import Sink` works, and `Sink` is in `__all__`.
- [ ] AC-2: `Config`, `read_losses` and `get_baggage` likewise.
- [ ] AC-3: `get_baggage()` returns the current baggage as a `dict`, and a test asserts it
      round-trips with `set_baggage`.
- [ ] AC-4: The README's "writing your own sink" section imports `Sink` from the top level, and
      its `SinkDeliveryError`/`SinkLosses` example uses the public path rather than
      `log_foundry.sinks.base`.
- [ ] AC-5: A test asserts every name in `__all__` is importable and every name the README
      documents as public is in `__all__` — derived from the README, so the two cannot drift.

### FR-006: `stop_signal` is a declared, namespaced part of the sink contract

#### Description:

`_lifecycle.offer_stop_signal` does `if hasattr(sink, "stop_signal"): sink.stop_signal = stop`.
`sinks/base.py` — the file SPEC-026 designates as the third-party sink contract — documents the
optional `losses()` and never mentions this. Two costs freeze at 1.0: every third-party retrying
sink is uninterruptible, reintroducing SPEC-027's global pause for anyone who writes their own;
and a sink that already owns an attribute of that name has it silently overwritten with a
`threading.Event` (reproduced).

#### Acceptance Criteria:

- [ ] AC-1: `sinks/base.py` documents it beside `losses()` — what it is, when it is assigned, and
      that honouring it is how a retrying sink stays interruptible.
- [ ] AC-2: The attribute is namespaced so it cannot collide with a name a third-party object
      already uses. A rename is breaking for the shipped sinks and free now.
- [ ] AC-3: Every shipped retrying sink is updated, derived from the roster.
- [ ] AC-4: The README's custom-sink section shows it.

### FR-007: `flush()` and `continue_trace()` can grow a reason

#### Description:

`flush() -> bool` is one bit for five distinct outcomes: timed out, worker retired, drain thread
died, queue too full for the marker, or a batch abandoned. Measured, `flush()` after `shutdown()`
and `flush(0)` are indistinguishable, and a Lambda handler needs "the worker is retired, my code
is wrong" separated from "the sink is slow". `continue_trace() -> bool` has the same shape:
nothing supplied and malformed input both read `False`.

This is here rather than in `1.x` because of how it must be done: a `NamedTuple` cannot be
retrofitted (a non-empty tuple is always truthy, so `if flush():` would silently change meaning).
A small result object with `__bool__` and a `reason: str | None` keeps every existing call site
working and leaves room to add reasons later — but only if the return type changes **now**.

#### Acceptance Criteria:

- [ ] AC-1: `if flush():` and `assert flush() is True`-style checks keep working. The second is
      the risk: `is True` breaks on an object. A test covers both idioms and the README/docs are
      updated wherever they use identity.
- [ ] AC-2: `flush().reason` distinguishes at least retired-worker from timed-out from
      batch-abandoned.
- [ ] AC-3: The same treatment for `continue_trace()`, distinguishing "nothing supplied" from
      "supplied and rejected".
- [ ] AC-4: `mypy --strict` is clean, and the return types are exported so a caller can annotate.
- [ ] AC-5: Adding a new reason later is additive — the type is documented as growing by new
      `reason` values, never by changing `__bool__`.

### FR-008: `Health` and `SinkLosses` do not freeze their tuple shape

#### Description:

Both are `NamedTuple`s, so length and positional unpacking are part of the contract at 1.0.
`Health` has grown in six consecutive specs and grows again in SPEC-036; `d, f = sink.losses()`
works today and breaks the moment `SinkLosses` gains a third counter, which SPEC-018's
`dropped_unadjudicated` vocabulary makes likely.

Two ways out, and the FR must pick: convert both to frozen dataclasses (attribute access
unchanged, unpacking and `len()` gone — itself breaking, and free only now), or keep the
`NamedTuple` and state in the docstring that only attribute access is supported.

The recommendation is **convert**. "Documented as unsupported" is what the tuple shape already
effectively is, and it has not stopped anything: the shape is still real, still relied upon by
`len(h) == 9` in this repo's own test suite until SPEC-036 changes it.

#### Acceptance Criteria:

- [ ] AC-1: Attribute access is unchanged everywhere — `h.dropped`, `h.sink.failed`.
- [ ] AC-2: Unpacking and `len()` no longer work, and the change is in the release notes as
      breaking.
- [ ] AC-3: Every construction site in `src/` and `tests/` uses keywords, so the conversion is
      mechanical. Verified before the change rather than discovered during it.
- [ ] AC-4: `_replace`-style updates keep working, or their call sites are converted with the
      type.
- [ ] AC-5: `SinkLosses` converts with it — a third-party sink returning a plain object with the
      right attributes still aggregates.

---

## Data Model

```python
@dataclass(frozen=True)
class Config: ...        # FR-003; get_config() returns a copy

@dataclass(frozen=True)
class Health: ...        # FR-008, was NamedTuple
@dataclass(frozen=True)
class SinkLosses: ...    # FR-008, was NamedTuple

class FlushResult:       # FR-007
    def __bool__(self) -> bool: ...
    reason: str | None
```

## Implementation Phases

### Phase 1: The cheap signature fixes (FR-001, FR-002, FR-005, FR-006)

Mechanical, independently reviewable, no design content beyond what is written.

### Phase 2: `get_config()` (FR-003)

### Phase 3: The emitters' field namespace (FR-004)

### Phase 4: The return and container types (FR-007, FR-008)

Last, and largest blast radius — both touch every call site in the repo and both are the kind of
change where the tests are the deliverable.
