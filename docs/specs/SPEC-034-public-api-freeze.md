# Spec: The Public API Freeze

**ID:** SPEC-034  
**Status:** In Progress  
**Last Updated:** 2026-08-09  
**Depends On:** SPEC-026, SPEC-030, SPEC-033

~~**Depends On:** … SPEC-036, SPEC-037 — FR-008 converts `Health` to a dataclass, and both 036
(FR-003, `orphan_lost`) and 037 (FR-001 AC-5, `in_span_lost`) append a field to it as a
`NamedTuple` first; building this before either would make that spec's criteria unsatisfiable,
and AC-2b's "tenth and eleventh indices" needs both to have landed~~

**Struck (SPEC-021), and the arc's build order reversed with it.** The dependency was real but
self-inflicted, and it ran the wrong way. Scheduling FR-008's conversion *last* is precisely what
forced 036 and 037 each to append a field **as a tuple** and prove indices 0..8 unchanged, and
then forced this spec to undo both. Nine acceptance criteria and two test rewrites existed only
to serve that ordering: 036 FR-003 AC-3/AC-10/AC-11, 037 FR-001 AC-5a, and this spec's AC-2b,
AC-2c and AC-3.

Converted **first**, a `Health` field is plainly additive — no index proof, no `len(h)`
migration, no `test_health_gains_no_field` rewrite, and no NamedTuple→dataclass churn for a field
that only ever existed as a tuple member to satisfy an ordering. It also moves AC-2c's
whole-field-set review to before two more fields land, which is when it is useful rather than
merely possible.

The second consequence is larger than the paperwork: with `Health` a dataclass and the `Sink`
members probed by name, most of what remains in this arc becomes **additive and free in `1.x`** —
so it no longer has to precede the tag. See the cut line in `docs/specs/INDEX.md`.

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

- [x] AC-1: `SQSSink(url, client=fake)` works; `SQSSink(url, fake)` raises `TypeError`.
- [x] AC-2: A test asserts that **no sink class in `sinks/` takes a parameter named `client`,
      `sdk`, `producer`, `connection` or `opener` positionally**, derived from parameter names
      across the package rather than a sink list, so a later sink is covered without anyone
      remembering. It is a **blacklist of five names, not a rule derived from syntax**, and says
      so: role is not inferable from a name alone — `stream` is a positional *transport* in
      `StdoutSink` and a positional *destination* in `RedisStreamsSink`. Verified today: across
      all sink classes **that define an `emit`** — 34 of them, which is SPEC-032's lint scope and
      not the 39 classes merely named `*Sink` — exactly one such parameter is positional. The two
      rosters differ and the test states which it uses.
- [x] AC-3: No call site in `src/`, `tests/` or `README.md` passes it positionally.

### FR-002: `SentrySink` injects through `client=`

#### Description:

`SentrySink(dsn=None, *, sdk=None, ...)` is the only sink whose injection kwarg has no family.
`sdk` also names the *module* rather than the thing injected, which is what the other names get
right.

#### Acceptance Criteria:

- [x] AC-1: `SentrySink(client=fake_sdk)` works and `sdk=` raises `TypeError` — no alias. An
      alias would have to live for the whole of `1.x`, which is the cost this spec exists to
      avoid.
- [x] AC-2: `sdk` appears nowhere in `src/`, `tests/` or `README.md`, **narrowed at build time to
      the injection name**: no `sdk=` keyword argument and no `.sdk` attribute access. Taken
      literally it also forbids `sentry_sdk`, `_import_sdk` and a local holding a fake SDK, none
      of which is the rename and one of which is a third-party module name. The two test locals
      that *were* called `sdk` are renamed anyway, so the check stays strict rather than
      tolerant. FR-001's roster test cannot observe this rename — `SentrySink.sdk` is *already*
      keyword-only — which is why this is a grep rather than a reuse.
- [x] AC-3: The attribute is renamed with the parameter, so `sink.client` reads as it does on the
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

**Three things this Description got wrong or left out, all found by reading `config.py` before
writing any of it and then measured** (recorded here rather than fixed silently, per SPEC-021):

1. ~~`get_config()` returns `dataclasses.replace(_config)`~~ — that **shares the `defaults`
   dict**, so it hands back a frozen shell around the live mapping and fails this FR's own AC-5.
   It must be `replace(_config, defaults=dict(_config.defaults))`. The Description proposed the
   failing call as "the straightforward answer" while AC-5 separately forbade its consequence.
2. **`_ensure_sink()` mutates too**, and is not named anywhere above. It assigns
   `_config.sink = StdoutSink()` when nothing was configured — the zero-config path. Under
   `frozen=True` without a rebind it raises `FrozenInstanceError` into `api._log`'s orphan guard,
   so **every log in a process that never called `configure(sink=...)` is absorbed and lost**:
   measured, `log-foundry: absorbed a failure while emitting an orphan log
   (FrozenInstanceError); the event was lost`. A list of mutating call sites that names only
   `configure()` is the kind of hand-roster this arc has repeatedly paid for.
3. **`configure()` writes nine fields one at a time.** Rebinding per field would allocate nine
   configs and, worse, leave a window in which another thread reads a half-applied one — a
   `service` from this call beside a `sink` from the last, stamped onto real events. It is one
   `replace(**changed)` and one rebind, counted at **runtime**: a first version counted call
   *nodes* in the source and passed against `for k, v in changed.items(): _rebind(**{k: v})` —
   one syntactic call, nine executions, nine windows, whole suite green.
4. **Freezing turns every write into a read-modify-write, and one of the writers is on the
   logging path.** The worst finding in this FR, and a regression it introduced rather than a
   defect it inherited: `_ensure_sink()` runs on the orphan path on arbitrary application
   threads, so a stale snapshot there puts back the pre-`configure()` `service`, `version`,
   `env`, `defaults` **and** `sink`, permanently. Measured on the unlocked version: **268 of
   2000** trials shipped every later event with `service="unknown"` after one concurrent
   `info()`, against **0** before the freeze — wrong data in the log stream for the life of the
   process, which is SPEC-024's category. `configure()` being documented "not thread-safe" does
   not cover it: the racing party is `info()`. A dedicated `_config_lock` serializes the two
   writers; `_live_config()` stays lock-free, so AC-6 is untouched. Lock ordering is one-way —
   nothing takes `decorator._worker_lock` underneath it.

The alternative, returning the singleton and documenting "do not mutate", is what exists today
and is what this FR exists to end.

#### Acceptance Criteria:

- [x] AC-1: `get_config().sink = X` raises rather than silently retargeting the config.
- [x] AC-2: `get_config().max_value_bytes = 0` raises; the only route to a ceiling is
      `configure()`, which validates.
- [x] AC-3: Every documented read still works — `get_config().service`, `.sink`, `.defaults` and
      the four ceilings — and the README's examples are unchanged.
- [x] AC-4: Mutating the returned object cannot affect the library's behaviour even if a caller
      defeats the freeze (e.g. `object.__setattr__`): the returned object is a **copy**, so the
      internal `_config` is unreachable through it. A test asserts identity is *not* shared.
- [x] AC-5: `defaults` — a mutable `dict` on the dataclass — is copied too, or the freeze is
      cosmetic. A test mutates the returned `defaults` and asserts the library's is unchanged.
- [x] AC-6: The library's own internals do not go through `get_config()` on any hot path, so this
      adds no per-event allocation. `model.build_event`'s three call sites are converted, and a
      benchmark shows the per-event path is unchanged rather than an assertion that it is.
- [x] AC-7: No module imports `_config` by value (`from config import _config`), so rebinding it
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

**Two things this Description leaves out, found by reading `api.py` before writing any of it:**

- **`fields=` is itself breaking, in the direction the FR does not mention.** The var-keyword is
  literally named `**fields` today, so `info("x", fields={"a": 1})` *already works* and produces
  a field called `fields` holding that dict. Promoting it to a real parameter silently changes
  that call's meaning. That strengthens the case for taking it before 1.0 rather than weakening
  it — but it also makes `fields` the **third** reserved word, which the FR does not say.
- **The escape hatch must reach its own name**, or "reserved" has a hole: `fields={"fields": …}`
  puts a field called `fields` in the event. AC-1 named only `echo`, `message` and a
  non-identifier; `fields` is added to it. That self-hosting property is what makes reserving
  anything tolerable — every reserved name has exactly one route through.
- **The merge runs *before* `api._log`, so it is outside that function's orphan guard**, and an
  unguarded `{**fields, **kv}` therefore raised a `TypeError` into the application on all four
  entry paths — including the orphan one, where SPEC-025's promise holds and
  `tests/test_promises.py` does not xfail it. Inside a span the decorator then recorded
  `status=error` with an `error.type` the caller never raised: the SPEC-025 shape verbatim, in a
  new place, and a regression on `info("m", **payload)` where a dynamic payload carries a
  `fields` key. It was also asymmetric in a way neither reading defends — `fields=[]` silently
  ignored, `fields=["x"]` fatal. The library coerces rather than validates (SPEC-017), so the
  merge absorbs and announces by type. Found by review; the promise matrix carries the case now.
- **`dict[str, object]` is invariant, so the annotation rejected the callers it was for.**
  `mypy --strict` refused `dict[str, str]` — a headers dict, a counter map — and refused
  `Mapping[str, object]`, which is the natural type for "a mapping built somewhere else", the
  exact use the docstring cites. The parameter is `Mapping[str, object] | None`; runtime already
  accepted any mapping, so widening cost nothing. This one freezes at 1.0, which is why it is
  here rather than noted.

#### Acceptance Criteria:

- [x] AC-1: `info("x", fields={"echo": "v", "message": "w", "not-an-identifier": 1,
      "fields": "self"})` puts all **four** in the event's `fields` — `fields` added at build
      time, per the Description above.
- [x] AC-2: `**fields` still works for every non-reserved name, unchanged.
- [x] AC-3: A key given in both `fields=` and the keyword form resolves deterministically, and
      the docstring says which wins: **the keyword form**. `fields=` is the bulk route, usually a
      mapping built somewhere else; `**kv` is what the caller wrote at this call site, and a
      literal overriding a base is what `{**base, **overrides}` already means in the language.
- [x] AC-4: `echo=` still controls the console echo — this FR adds a route to the field, it does
      not move the flag.
- [x] AC-5: The two reserved names are documented as reserved, with `fields=` named as the way
      round them.
- [x] AC-6: The same treatment reaches all five emitters, derived rather than hand-applied.

### FR-005: The extension points are exported

#### Description:

`Sink` — the extension point the README documents over 40 lines — is not importable from
`log_foundry`, nor from `log_foundry.sinks` (whose `__init__.py` is empty). Nor are `Config` (the
return type of a public function), `read_losses`, or `get_baggage`. Baggage is publicly settable
and readable only as a serialized header the caller must re-parse.

None of this is a *behaviour* change, but each is a public-surface decision that reads as
deliberate once `1.0.0` freezes: a name absent from `__all__` at 1.0 says "not part of the API".

#### Acceptance Criteria:

- [x] AC-1: `from log_foundry import Sink` works, and `Sink` is in `__all__`.
- [x] AC-2: `Config`, `read_losses` and `get_baggage` likewise.
- [x] AC-3: `get_baggage()` returns the current baggage as a `dict`, and a test asserts it
      round-trips with `set_baggage`.
- [x] AC-3b: **The name is `get_baggage`, not `current_baggage`, and that was a decision.**
      Review raised it: the context readers are `current_traceparent()`,
      `current_trace_context()` and `current_baggage_header()`, so the new name sits beside a
      family it does not join, and a rename costs a major version after 1.0. It pairs with
      `set_baggage()` instead, which is the right pairing — the `current_*` family reads the
      **trace context**, which the caller never sets directly, while baggage is the one piece of
      context a caller does set, and a getter whose name does not mirror its setter is the worse
      surprise. Recorded rather than renamed.
- [x] AC-3a: **It returns a copy, and the library's own hot path does not pay for it.** Added at
      build time: `context.get_baggage()` returned the live mapping with a docstring saying "must
      not be mutated in place", and *exporting* that is FR-003's defect under another name — a
      public accessor handing out live internal state, where the caller's slip is silent and
      edits the context every later event reads. It is the same rule FR-003 applies to
      `get_config()`, so it is applied here rather than left because this FR is what makes the
      accessor public. The cost is the same too: `api._log` reads baggage **once per event**, so
      the copy would allocate per event — `context._live_baggage()` is the internal read, and the
      three internal call sites use it, exactly as AC-6 requires of `_config`.
- [x] AC-4: The README's "writing your own sink" section imports `Sink` from the top level, and
      its `SinkDeliveryError`/`SinkLosses` example uses the public path rather than
      `log_foundry.sinks.base`.
- [x] AC-5: A test asserts every name in `__all__` is importable and every name the README
      documents as public is in `__all__` — derived from the README, so the two cannot drift.
      **Both halves are built.** The first build shipped only the first and ticked the AC;
      review caught it, and the anti-drift half then caught two of that review's own findings.
      "Documents as public" is read as the `from log_foundry import ...` form only: `import
      log_foundry as lf` then `lf.info(...)` says the *module* is public and says nothing about
      `__all__`, so treating every attribute reached that way as a claim would let the README's
      prose examples define the API surface.

### FR-006: `stop_signal` is a declared, namespaced part of the sink contract

#### Description:

`_lifecycle.offer_stop_signal` does `if hasattr(sink, "stop_signal"): sink.stop_signal = stop`.
`sinks/base.py` — the file SPEC-026 designates as the third-party sink contract — documents the
optional `losses()` and never mentions this. Two costs freeze at 1.0: every third-party retrying
sink is uninterruptible, reintroducing SPEC-027's global pause for anyone who writes their own;
and a sink that already owns an attribute of that name has it silently overwritten with a
`threading.Event` (reproduced).

#### Acceptance Criteria:

- [x] AC-1: `sinks/base.py` documents it beside `losses()` — what it is, when it is assigned, and
      that honouring it is how a retrying sink stays interruptible.
- [x] AC-2: The attribute is namespaced so it cannot collide with a name a third-party object
      already uses. A rename is breaking for the shipped sinks and free now.
- [x] AC-3: Every shipped retrying sink is updated, derived from the roster. This lands **after**
      SPEC-035, whose new tests assert `sink.stop_signal is worker._stop`; the rename sweeps them
      too, and a grep for the old name across `tests/` is part of the AC.
- [x] AC-4: The README's custom-sink section shows it.

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
      stated in the PR. Those figures are **recounted at build time regardless** — a count taken
      on one branch and trusted on another is the kind of stale roster FR-002's lesson is about —
      though with the order reversed they are now a ceiling rather than a floor: this spec runs
      before 036 and 037 add their call sites, which is the cheaper end to convert from.
- [ ] AC-1b: The FR states whether `Worker.flush` changes type too, or only the public
      `log_foundry.flush` — they are different call sites with different callers.
- [ ] AC-2: `flush().reason` distinguishes at least retired-worker, timed-out and
      batch-abandoned. ~~plus the two SPEC-036 adds, a failed span sweep and a failed
      `sink.flush()`, since this lands after it~~ — struck with the reversed order: 036 now lands
      *after* this and adds its own two reasons, which AC-5 is what makes possible. The
      obligation does not disappear, it moves — SPEC-036 FR-001 and FR-002 each carry an AC
      requiring the reason they invent to be surfaced here.
- [ ] AC-3: The same treatment for `continue_trace()`, distinguishing "nothing supplied" from
      "supplied and rejected". Its third reason — supplied, well-formed, and refused because the
      span had been swept — likewise moves to SPEC-036 FR-001 AC-11a, which is where it is
      invented. What must **not** move is AC-5's guarantee: a reason added later is additive, so
      a reason invented in 036 with nothing on this side to carry it is the failure this AC was
      written to prevent, and reversing the order makes AC-5 the thing that prevents it.
- [ ] AC-4: `mypy --strict` is clean, and the return types are exported so a caller can annotate.
- [ ] AC-5: Adding a new reason later is additive — the type is documented as growing by new
      `reason` values, never by changing `__bool__`.

### FR-008: `Health` and `SinkLosses` do not freeze their tuple shape

#### Description:

Both are `NamedTuple`s, so length and positional unpacking are part of the contract at 1.0.
`Health` has grown in six consecutive specs and is due to grow twice more in this arc —
`orphan_lost` (SPEC-036 FR-003) and `in_span_lost` (SPEC-037 FR-001 AC-5); `d, f = sink.losses()`
works today and breaks the moment `SinkLosses` gains a third counter, which SPEC-018's
`dropped_unadjudicated` vocabulary makes likely.

**Those two fields are the reason this FR moved to the front of the arc rather than the back.**
Appending to a `NamedTuple` costs an index proof and a `len()` migration in the spec that appends
it, and then costs this spec the undoing of both; appending to a frozen dataclass costs nothing.
Converting first is not merely cheaper — it is what makes the remaining `Health` work additive,
and therefore what takes it off the critical path to `1.0.0` (see the header).

Two ways out, and the FR must pick: convert both to frozen dataclasses (attribute access
unchanged, unpacking and `len()` gone — itself breaking, and free only now), or keep the
`NamedTuple` and state in the docstring that only attribute access is supported.

The recommendation is **convert**. "Documented as unsupported" is what the tuple shape already
effectively is, and it has not stopped anything: the shape is still real, and still relied upon by
`len(h) == 9` in this repo's own test suite.

#### Acceptance Criteria:

- [ ] AC-1: Attribute access is unchanged everywhere — `h.dropped`, `h.sink.failed`.
- [ ] AC-2: Unpacking and `len()` no longer work, and the change is in the release notes as
      breaking.
- [ ] AC-2b: **Two** tests become impossible under a dataclass and both are updated here —
      `tests/test_orphan_sink_handoff.py::test_health_gains_no_field` and
      `tests/test_worker.py::test_existing_health_fields_keep_their_positions`, whose **whole
      body** is positional (`h[0]`, `h[3]`, `h[4]`, `h[5..7]`, `h[8]`). A draft of this AC named
      only the first and called itself the catcher, which is how the second would have arrived as
      a red build rather than a criterion. Both pin **nine** fields, not eleven: with the order
      reversed this spec is the **first** to touch `Health`, not the last, so `orphan_lost` and
      `in_span_lost` are appended to a dataclass afterwards and neither test has to be written
      twice. ~~Both appended fields are in scope because the build order is 035 → 036 → 037 →
      034~~ — struck with the ordering it depended on.
- [ ] AC-2c: The `Health` field set is reviewed **as a whole**, once, before the type freezes —
      nine fields arrived across seven specs, each appended on its own merits and none of them
      ever looked at together, with two more due. The freeze is the last moment that review is
      free, and running it **before** those two land is the point of the reversal: a review that
      arrives after them can only ratify them. It is a review with a recorded outcome, not a
      licence to rename — anything it does change is changed here rather than in `1.x` — and its
      outcome is an input to SPEC-036 FR-003 and SPEC-037 FR-001 AC-5, which name the two fields
      it will be asked about.
- [ ] AC-3: Every construction site is converted to keywords **first, as its own commit** — two
      positional sites exist today (`tests/test_sink_losses.py:213`, `:228`), so a draft claiming
      this was already true was wrong. Re-grepped at build time rather than trusted from here.
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
