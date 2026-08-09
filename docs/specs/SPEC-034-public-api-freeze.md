# Spec: The Public API Freeze

**ID:** SPEC-034  
**Status:** Draft  
**Last Updated:** 2026-08-09  
**Depends On:** SPEC-026, SPEC-030, SPEC-033, SPEC-036, SPEC-037 — FR-008 converts `Health` to a
dataclass, and both 036 (FR-003, `orphan_lost`) and 037 (FR-001 AC-5, `in_span_lost`) append a
field to it as a `NamedTuple` first; building this before either would make that spec's criteria
unsatisfiable, and AC-2b's "tenth and eleventh indices" needs both to have landed

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
- **Re-homing `MemorySink`/`NullSink`/`StderrSink` out of `sinks.util`** (audit P9). Also a
  major-version cost, and genuinely worth doing — but it is a pure move with no design content,
  so it rides with SPEC-038, which is already touching the sink package. **SPEC-038 FR-013 is
  that item**; a first draft of this bullet handed it over without anything on the other side to
  catch it.
- **`**http_kwargs: object` → `Unpack[HTTPOptions]`** (audit P9). Type-level only; it changes no
  runtime behaviour and can land any time.
- **A public way to redirect or disable the console echo** (audit P7). `ConsoleWriter` is not
  exported and `configure()` has no `echo` argument, so the only route is assigning
  `log_foundry.api._console`. Genuinely missing — but adding `configure(echo_stream=…)` or
  exporting `ConsoleWriter` is **additive**, and therefore free in `1.x`. Only things that cost a
  major version belong in this spec.
- **The destination-name positional policy** (audit P9). `PostgresSink(table)` and
  `KafkaSink(topic)` take theirs positionally where `ElasticsearchSink(*, index)` and
  `MongoDBSink(*, database, collection)` require a keyword. This *does* freeze, and it is
  deliberately left: FR-001's rule governs injected transports, where a wrong answer causes a
  double-close or a leaked client, whereas this is ergonomics with no correctness content. A
  reader will trip over it; nothing will break. Recorded so the next audit does not re-find it as
  new.

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
      all sink classes **that define an `emit`** — 34 of them, which is SPEC-032's lint scope and
      not the 39 classes merely named `*Sink` — exactly one such parameter is positional. The two
      rosters differ and the test states which it uses.
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
`FrozenInstanceError`. The cost is **not** free, and an earlier draft of this FR claimed it was: `model.build_event`
calls `get_config()` at `model.py:96`, and again at `:239` and `:272` — one to three calls **per
event**, on the hot path. A copy per call, plus AC-5's `dict(defaults)`, would allocate per event.
So the internal call sites must read `_config` directly (they are inside the package and the
freeze is a guarantee to *callers*, not to the library itself), and AC-6 is what proves it.

`configure()` also mutates `_config` in place, so `frozen=True` means rebinding the module global
instead — safe only because no module does `from config import _config`, which is verified today
and worth a test.

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
      adds no per-event allocation. `model.build_event`'s three call sites are converted, and a
      benchmark shows the per-event path is unchanged rather than an assertion that it is.
- [ ] AC-7: No module imports `_config` by value (`from config import _config`), so rebinding it
      in `configure()` is safe. A test asserts it, since the freeze depends on it.

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
- [ ] AC-3: Every shipped retrying sink is updated, derived from the roster. This lands **after**
      SPEC-035, whose new tests assert `sink.stop_signal is worker._stop`; the rename sweeps them
      too, and a grep for the old name across `tests/` is part of the AC.
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

- [ ] AC-1: `if flush():` keeps working. `assert flush() is True` **cannot** — an object with
      `__bool__` is never `True` — and a draft of this AC claimed both would, which is the
      contradiction to avoid. Counted on this branch: **26** identity assertions on `flush()` and
      **18** on `continue_trace()` across the test suite. All are converted and the count is
      stated in the PR. Those figures are a **floor, recounted at build time**: SPEC-036 and
      SPEC-037 both add call sites before this spec builds, so a count taken on this branch and
      trusted at build time is the kind of stale roster FR-002's lesson is about.
- [ ] AC-1b: The FR states whether `Worker.flush` changes type too, or only the public
      `log_foundry.flush` — they are different call sites with different callers.
- [ ] AC-2: `flush().reason` distinguishes at least retired-worker, timed-out and
      batch-abandoned — **plus the two SPEC-036 adds**, a failed span sweep and a failed
      `sink.flush()`, since this lands after it.
- [ ] AC-3: The same treatment for `continue_trace()`, distinguishing "nothing supplied" from
      "supplied and rejected" — **plus the third reason SPEC-036 FR-001 AC-11a adds**, supplied
      and well-formed but refused because the span had been swept, since this lands after it.
      Stated explicitly for the reason AC-2 states its two: a reason invented in 036 with nothing
      on this side to carry it is a reason that quietly does not survive the freeze.
- [ ] AC-4: `mypy --strict` is clean, and the return types are exported so a caller can annotate.
- [ ] AC-5: Adding a new reason later is additive — the type is documented as growing by new
      `reason` values, never by changing `__bool__`.

### FR-008: `Health` and `SinkLosses` do not freeze their tuple shape

#### Description:

Both are `NamedTuple`s, so length and positional unpacking are part of the contract at 1.0.
`Health` has grown in six consecutive specs and grows twice more in this arc — `orphan_lost`
(SPEC-036 FR-003) and `in_span_lost` (SPEC-037 FR-001 AC-5); `d, f = sink.losses()`
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
- [ ] AC-2b: **Two** tests become impossible under a dataclass and both are updated here —
      `tests/test_orphan_sink_handoff.py::test_health_gains_no_field`, which by then pins
      `Health._fields[:9]` plus the tenth and eleventh names and carries the `len(h)` assertion
      relocated into it by SPEC-036 FR-003 AC-10; and
      `tests/test_worker.py::test_existing_health_fields_keep_their_positions`, whose **whole
      body** is positional (`h[0]`, `h[3]`, `h[4]`, `h[5..7]`, `h[8]`, plus `h[10]` after
      SPEC-037 AC-5a) and which 036 and 037 both leave in place. A draft of this AC named only
      the first and called itself the catcher, which is how the second would have arrived as a
      red build rather than a criterion. **Both** appended fields are in scope because the build
      order is 035 → 036 → 037 → 034: this spec is the last to see `Health` as a tuple.
- [ ] AC-2c: The `Health` field set is reviewed **as a whole**, once, before the type freezes —
      eleven fields arrived across nine specs, each appended on its own merits and none of them
      ever looked at together. The freeze is the last moment that review is free. It is a review
      with a recorded outcome, not a licence to rename: anything it does change is a change made
      here rather than in `1.x`.
- [ ] AC-3: Every construction site is converted to keywords **first, as its own commit** — two
      positional sites exist today (`tests/test_sink_losses.py:213`, `:228`), so a draft claiming
      this was already true was wrong. Verified by grep before the type changes.
- [ ] AC-4: `_replace`-style updates keep working, or their call sites are converted with the
      type.
- [ ] AC-5: `SinkLosses` converts with it. It does **not** start accepting a plain object with
      the right attributes: `read_losses` gates on `isinstance(losses, SinkLosses)`, which
      SPEC-026 FR-002 settled deliberately, and a draft of this AC would have re-opened that
      decision by accident. Duck-typed loss reporting is out of scope; if it is wanted it is its
      own FR against SPEC-026's reasoning.

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
