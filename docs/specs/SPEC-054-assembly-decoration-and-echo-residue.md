# Spec: Assembly, Decoration and Echo Residue — a Surrogate, a Nameless Callable, a Hostile Key and a Broken Pipe

**ID:** SPEC-054
**Status:** Draft
**Last Updated:** 2026-09-04
**Depends On:** SPEC-017, SPEC-020, SPEC-025, SPEC-029, SPEC-037

## Overview

The second round of the pre-1.0 surface audit found five defects at the two edges the library
promises most about — what leaves `build_event`, and what `@trace` accepts — and none of them
is visible to `health()`. Every one was reproduced at `456e9b7` and again at `98c7e78`, the tree
this spec is written on. The audit itself is landed by `02a8ac5` as
`docs/audits/2026-09-04-pre-1.0-audit-round-2.md`, which also files the first and fourth findings
below in `architecture.md` §12; this spec is self-contained without it.

- **A lone surrogate survives assembly.** `sanitize._measured` encodes with `errors="replace"`
  to *measure* a string, never to replace it, so the `surrogateescape` case its own docstring
  cites — `os.fsdecode(b"file-\xff.txt")` — leaves `build_event` intact. The JSON sinks are
  fine (`ensure_ascii` escapes it, and no sink turns it off), but `SQLiteSink`, `PostgresSink`
  and `ClickHouseSink` project `function` into a column as a raw `str`, the driver raises
  `UnicodeEncodeError`, and the worker abandons the whole batch after four attempts — innocent
  neighbours included. Measured on `SQLiteSink`: `lost 3 event(s); batch abandoned after 4 emit
  attempts`, `failed_batches=1`.
- **`@trace` on a callable without `__qualname__` fails the caller at call time.** The wrapper
  reads `fn.__qualname__` per call, so `trace(functools.partial(f, 1))()` and a callable
  instance raise `AttributeError` into the application — invariant 1 broken from inside the
  decorator's own setup. A misordered `@classmethod` under `@trace` is accepted at decoration
  and fails at the first call with `'classmethod' object is not callable`; a misordered
  `@staticmethod` is accepted and passes `self` to a function that takes none.
- **`@trace` on a generator or async generator traces the *call*, not the iteration.** Only
  `iscoroutinefunction` dispatches, so the span opens and closes before the body runs, and
  every event logged inside the body is an orphan on a fresh trace. Measured:
  `['span.start', 'span.end']` before the first `next()`, then `('inside', parent_span_id=None)`
  on a different `trace_id`; the async generator behaves identically.
- **A mapping key whose `__str__` raises replaces the whole sibling mapping.** `key()`'s
  fallback branch is guarded only by `value()`, so `fields={"boom": {"sib": 1, KeyBoom(): 2}}`
  becomes `'boom': '<unserializable: dict>'` with `truncated` unset — while `key()`'s own
  docstring says the integer path exists so that "one hostile key would not take every sibling
  with it, unmarked".
- **Echo to a broken pipe writes one stderr diagnostic per event, forever.** `api._log` absorbs
  the `BrokenPipeError` per call without the throttled idiom the queue-full site uses. Measured:
  200,000 echoed events piped into `head -1` produced 199,970 identical stderr lines.

**The generator finding needs a product decision, and it is Andrew's, not this spec's.** Two
options, both complete:

- **Option A — refuse at decoration.** `@trace` raises `TypeError` on a generator function or an
  async generator function, naming it and saying what to do instead (trace the consumer, or open
  the span around the loop). This is invariant 13's shape, it is a few lines, and it is
  **forward-compatible**: a later spec can lift the refusal into a wrap without breaking anyone,
  whereas a wrap shipped first freezes its semantics at 1.0.
- **Option B — wrap the iteration.** Open the span on the first `__next__`, push it as current
  around every resumption — `__next__`, `send` and `throw` — and pop it at every `yield`, close
  it on exhaustion, `close()` or an exception, and mirror all of that for `__anext__`, `asend`,
  `athrow` and `aclose`. This is a real feature with semantics no other span has: `duration_ms`
  counts suspended time; a generator finalised by the collector runs its close in whatever
  context happens to be current, where the span-stack token belongs to another context; and the
  decorator gains two more twin paths for invariant 6 to police. Chosen, it is a spec of its own
  — SPEC-055, beside this one as an arc — and FR-003 below is replaced by a refusal-free dispatch
  to it.

**Recommendation: Option A, now.** It closes the wrong-data defect before the tag at the cost of
a `TypeError` a consumer meets at import, and it leaves Option B open at zero cost. FR-003 is
written as Option A; it is built only once the decision is made, and the other five FRs are
built regardless.

## Scope

### In Scope

- Making every string that leaves assembly encodable as UTF-8, and refusing at `configure()` a
  stamp that is not (FR-001).
- Resolving a span name once, at decoration, from any callable, and refusing a misordered
  descriptor there (FR-002).
- Refusing a generator function at decoration (FR-003, pending the decision above).
- Confining a hostile mapping key to its own placeholder, and bounding every placeholder like
  any other string (FR-004).
- Disabling echo on a stream that cannot come back, and throttling the diagnostic for one that
  might (FR-005).
- An adversarial corpus over assembly, asserting invariant 8's observable value by value (FR-006).

### Out of Scope

- **Wrapping a generator's iteration** — Option B above. If chosen, it is SPEC-055.
- **Detecting a plain function that *returns* a generator object.** FR-003 tests the code flags
  at decoration, which is where invariant 13 refuses; a wrapper whose own body is not a generator
  is indistinguishable from any other function there, and a call-time check would be a refusal
  on the drain-thread side of the line that invariant draws.
- **`StdoutSink`/`StderrSink` against a broken pipe.** A sink's `emit` raising reaches the
  worker's retry, which already announces once per abandoned *batch*; that is a different site
  with a different existing throttle, and it is not the per-event flood FR-005 fixes.
- **Bounding `service`, `version` and `env` by `max_value_bytes`.** They bypass assembly's
  ceilings today, and FR-001 refuses only what *cannot be encoded*; an over-long stamp is a
  configuration the caller wrote once and can see. Recorded as an exception to invariant 8 on
  its own page (Phase 5), so the gap is a decision a reader of that page can see.
- **Validating `Config(...)` constructed directly.** The ceilings are validated in `configure()`
  and not in the dataclass, and FR-001's refusal follows the same precedent rather than moving
  both.
- **A `Health` term for suppressed echoes.** An echo is a second audience for an event that still
  rides the pipeline; nothing is lost that invariant 2 counts.
- **Any change to `_diag`'s three writers.** FR-005 moves one constant into it and adds no line
  shape.

---

## Functional Requirements

### FR-001: No string leaves assembly carrying a lone surrogate, and a stamp that cannot encode is refused

#### Description:

`truncate_str` and `truncate_tail` return an exact `str` that encodes as UTF-8 strictly — never
the caller's own object, so a `str` subclass instance cannot reach an event either. The
measurement takes the string through `str.__str__`, unbound, which is the identity on an exact
`str` and a plain copy of a subclass, so a subclass whose `encode` or `__str__` raises cannot
divert it — the same reasoning as `_INT_LT` and `_FLOAT_REPR` beside it. Today
`info(BadEncode("x"))` costs the event; after this it costs nothing. It then tries a strict
encode first, so the common case costs what it costs today; only on `UnicodeEncodeError` is
every surrogate code point (U+D800–U+DFFF) replaced by U+FFFD — one replacement character per
surrogate, never the `?` that `errors="replace"` writes on encode and never the three U+FFFD a
`surrogatepass` round trip produces — and the replaced string is what is measured, clipped and
returned. The second element of the returned pair widens from "the ceiling fired" to "the string
was altered", so `_Coercer.text()` sets `truncated` for a replacement as it does for a clip: a
substitution nobody can see is a silent change to the data, which is the rule `real()` already
applies to a non-finite float. The same rule reaches `bytes`: a `bytes`, `bytearray` or
`memoryview` value is decoded strictly first, and only on `UnicodeDecodeError` decoded with
`errors="replace"` and marked — today the same undecodable byte is marked when it arrives as a
`str` and silent when it arrives as `bytes`.

Every string `build_event` writes reaches one of the two clippers — `message`, `function`, every
field key and value, every baggage value, and the four `error.*` strings — except the three
config stamps `service`, `version` and `env`, which are copied raw on the hottest path. Those are
refused at `configure()` instead: a value that is not a `str` is a `TypeError` and one that does
not encode strictly is a `ValueError`, each naming the argument, checked beside the ceiling
checks — before `_lifecycle.stamp(sink)` and before anything is assigned — so a rejected call
leaves the config and the ownership record exactly as it found them. What is stored is
`str.__str__(value)`, an exact `str`, so a `StrEnum` member stays an acceptable `env` and a
`str` subclass instance never reaches an event through the config either. That is invariant
13's door, and it keeps the per-event path at zero extra encodes.

Three existing tests change meaning under this and are rewritten rather than left passing:
`tests/test_api.py::test_a_bad_value_is_absorbed_inside_a_span` and
`::test_the_absorbed_failure_is_announced_once_by_type_only` assert the library's own fault on
`info(ValueError(...))` is an `AttributeError`, and it becomes a `TypeError` from `str.__str__`
— still absorbed, still announced by type, a non-`str` message is still a slip and not a
coercion; and `tests/test_sanitize.py::test_lone_surrogate_does_not_raise`, whose second
assertion cannot fail once the value is replaced, is replaced by one that pins the replacement.

#### Acceptance Criteria:

- [ ] `truncate_str(os.fsdecode(b"file-\xff.txt"), 8192)` returns `("file-�.txt", True)`,
      and the result `.encode("utf-8")` strictly without raising.
- [ ] A string of ten lone surrogates against a ceiling of 4 returns the marker alone, and the
      flag is `True`; a lone surrogate inside an over-budget string is replaced before the clip,
      so the clipped result still encodes strictly.
- [ ] `truncate_tail` gives the same answers at the tail, and a lone surrogate in an
      `error.stack` is replaced in the `span.end` event.
- [ ] `@trace(name=bad)` and `lf.info(bad, path=bad)` on `SQLiteSink`, where `bad` is the
      `fsdecode` value above and the connection was opened `check_same_thread=False` (the worker
      inserts from its own thread), deliver all three events with `failed_batches == 0`, and the
      stored `function`, `message` and `fields.path` carry U+FFFD.
- [ ] `b"\xff\xfe\x80"` as a field value coerces to three U+FFFD with `truncated == True`;
      `b"plain"` coerces with it absent.
- [ ] A `str` subclass whose `encode` raises `RuntimeError`, passed as the message, is delivered
      as its plain text with `type(event["message"]) is str`, and costs nothing in
      `in_span_lost` or `orphan_lost`; the same object as a mapping key renders as its plain
      text too.
- [ ] The event's `truncated` is `True` when a surrogate was replaced anywhere in it, and absent
      when the same event carries only ordinary text (invariant 8).
- [ ] `configure(service="\udcff")` raises `ValueError` naming `service`; `configure(env=7)`
      raises `TypeError` naming `env`; `version` behaves the same; `get_config()` is unchanged
      after each refusal; `configure(service="\udcff", sink=s)` leaves `s` unstamped; and
      `configure(env=SomeStrEnum.PROD)` is accepted with `type(get_config().env) is str`
      (invariant 13).
- [ ] The `sinks/kinesis.py` docstring claiming `sanitize.coerce` passes a lone surrogate through
      is corrected, and its test's docstring with it; the guard in `_partition_key` stays,
      because that key is derived from an event field a `TransformSink` may have rewritten. The
      same claim in `docs/specs/SPEC-048-*.md` (FR-003's description) and
      `docs/spec-delivery/SPEC-048-*.md` is struck in place with this spec's number, per
      SPEC-021's rule.
- [ ] The three rewritten tests named above are the only existing tests that change, checked
      by diffing `pytest --collect-only -q` before and after.
- [ ] Reverting the replacement while keeping the strict-encode fast path reddens the SQLite
      criterion and the corpus (FR-006); reverting `str.__str__` reddens the subclass criterion.
- [ ] Serves invariant 8 on both delivery paths (invariant 6: the in-span build and the orphan
      build), and invariant 13 for the stamps.

### FR-002: `@trace` names a callable once, at decoration, and refuses a misordered descriptor there

#### Description:

The span name is resolved when `decorate` runs, not on every call: the explicit `name` if one
was given, else `fn.__qualname__` when the callable has one, else the callable's type name. A
`functools.partial` therefore traces as `partial` and a callable instance as its class, and
neither raises. A `__qualname__` that exists but is not a `str` needs no clause of its own:
`functools.wraps` copies the attribute onto the wrapper and Python refuses a non-`str` there
with `TypeError` before any wrapper exists, which is a refusal at decoration already. Both
wrappers read the resolved name from their closure, which also removes an attribute lookup from
the per-call path.

Accepting a callable instance opens one door the old `AttributeError` kept shut: an instance
whose `__call__` is `async def` reads as synchronous to `asyncio.iscoroutinefunction`, so it
would take the sync wrapper and close the span before the coroutine ran — the generator
finding's shape again. The dispatch therefore consults the bound `__call__` too, on any callable
that is not a plain function: `iscoroutinefunction(fn) or iscoroutinefunction(fn.__call__)`.
FR-003's generator test consults `__call__` the same way.

A `classmethod` or `staticmethod` object handed to `@trace` is a decorator applied in the wrong
order, and it is refused at decoration with a `TypeError` that names the underlying function and
says which way round to put them. `classmethod` is refused because it is not callable and the
wrapper would fail every call; `staticmethod` is refused even though it *is* callable, because
the wrapper replaces the descriptor and an instance call would then hand `self` to a function
declared without one. Anything else that is not callable is refused by the same check, naming
its type — and when it is a `str`, which is the slip `@trace("checkout")`, the message says
`name=` is the keyword that was meant. Invariant 13: refused where it is written, never on the
first call.

#### Acceptance Criteria:

- [ ] `trace(functools.partial(f, 1))()` returns `f(1)` and emits a span named `partial`;
      `trace(C())()` on a callable instance returns its result and emits a span named `C`
      (invariant 1).
- [ ] An explicit `name=` still wins over both, and an instance carrying a non-`str`
      `__qualname__` is refused at decoration by `functools.wraps` itself, with `TypeError`.
- [ ] A callable instance whose `__call__` is `async def` takes the async wrapper: the span
      closes after the coroutine's body ran, and an event logged inside it carries the span's
      `span_id`.
- [ ] `@trace` above `@classmethod` and above `@staticmethod` each raise `TypeError` at class
      body execution, the message naming the function and the correct order; `@classmethod` above
      `@trace` keeps working, as does `@staticmethod` above `@trace` (invariant 13).
- [ ] `trace(object())` raises `TypeError` naming the type; `trace("checkout")` raises
      `TypeError` whose message contains `name=`.
- [ ] `iscoroutinefunction` dispatch is unchanged: a partial of an `async def` still takes the
      async wrapper.
- [ ] Removing the `__call__` consultation reddens the async-instance criterion; removing the
      `staticmethod` clause reddens the misorder criterion while the `classmethod` half stays
      red on its own.
- [ ] Serves invariants 1 and 13 on both wrappers (invariant 6: sync and async).

### FR-003: `@trace` refuses a generator function at decoration

#### Description:

*Option A above; built only once the decision is made.* `decorate` tests
`inspect.isgeneratorfunction` and `inspect.isasyncgenfunction` — both see through a
`functools.partial`, and both are also asked of the bound `__call__` of a callable instance, as
FR-002's dispatch is — and raises `TypeError` naming the function and saying that a generator's
body runs after the wrapper has returned, so the span would close before it starts; the message
points at tracing the consumer or opening the span around the loop. Nothing else changes:
today's behaviour for a generator is a span of the *call*, which is wrong data on a fresh trace
for every event the body logs, and no consumer can be relying on it knowingly.

#### Acceptance Criteria:

- [ ] `@trace` on a `def` containing `yield` raises `TypeError` at decoration, naming the
      function; the same for `async def` with `yield`, for a `partial` of either, and for an
      instance whose `__call__` is a generator function (invariant 13).
- [ ] A function that returns a generator object without being one is accepted, and the
      limitation is stated in the docstring rather than closed.
- [ ] The README's `@trace` section says generators are refused and what to do instead.
- [ ] Removing the async-generator half of the check reddens its criterion while the sync half
      stays red on its own.
- [ ] Serves invariant 13 on both dispatch branches (invariant 6).

### FR-004: A hostile mapping key costs itself, not its siblings, and every placeholder is bounded

#### Description:

`_Coercer.key()` is total. Whatever a key's `__str__`, `bit_length` or `__lt__`
raises is caught inside `key()` itself, the key becomes `<unserializable key: T>` where `T` is
the key's type name, and `truncated` is set — a key that could not be rendered is a substitution
the reader must be able to see, unlike a value placeholder which is visible on its own. A `str`
subclass key never reaches the guard: FR-001's `str.__str__` renders it as plain text first,
which is the same answer the message gets. The guard sits in `key()` rather than in `mapping()`'s
loop because the loop's other work — the `max_keys` cap and the value coercion — already has
the right owners, and a second `try` in the loop would hide which of the three failed.

Two hostile keys of one type collide on one placeholder and the later wins; that is accepted, and
said in the docstring, because the alternative is a key that carries the value's identity, which
is what arch §6 forbids. Both placeholders — the key form here and `_placeholder`'s value form —
go through `text()` on their way out, so a type whose `__name__` was set to something enormous
still honours `max_value_bytes` exactly.

#### Acceptance Criteria:

- [ ] `fields={"boom": {"sib": 1, KeyBoom(): 2}}` delivers `fields.boom == {"sib": 1,
      "<unserializable key: KeyBoom>": 2}` with `truncated == True` (invariant 8).
- [ ] An `int` subclass key whose `__str__` raises gets the same placeholder and costs no
      sibling; a `str` subclass key whose `encode` raises renders as its plain text (FR-001).
- [ ] A hostile key at the top level of `fields=` behaves the same as one nested three deep.
- [ ] A type whose `__name__` exceeds `max_value_bytes` produces a placeholder that fits the
      ceiling, marker included, on both the key and the value form.
- [ ] Removing the guard in `key()` reddens the first criterion with the whole-mapping
      placeholder the audit measured, not with an exception.
- [ ] Serves invariant 8 on both delivery paths (invariant 6).

### FR-005: A broken echo stream is announced once and disabled; a faulting one is throttled

#### Description:

`ConsoleWriter.write` owns its stream's failures. A `BrokenPipeError`, or the `ValueError` a
closed file raises, is a stream that will not come back: the writer announces it once through
`_diag.absorbed` with a detail saying echo is disabled for the life of the writer, latches a
flag, and every later `write` returns without touching the stream. Any other `Exception` from
the stream may be transient — `EAGAIN` on a non-blocking terminal, `ENOSPC` on a file — so the
writer keeps trying, counts each failure, and announces the first and then every thousandth with
the running total, which is the queue-full site's idiom (SPEC-017 FR-005). The period is one
definition, `_diag.WARN_EVERY`, read by all three throttle sites — the two in `worker.py` and
this one — because two constants stating one number disagree eventually. `api._log`'s outer
guard stays as the total guard for anything else, a missing key above all; it is no longer the
thing that writes the line for a stream fault.

The writer is process-global and `_log` reaches it from arbitrary application threads
(invariant 4), so the counter is incremented under a lock taken on the failure path only —
the success path takes nothing, as today — and the disable flag is a set-only latch read without
one, which is the shape `Worker.submit` uses for `_shutdown_done`. The state lives on the writer
rather than the module so a `ConsoleWriter(stream=…)` built for a test starts clean, and so the
process-global `api._console` is the only one that can be disabled for the process. The stream is
still bound at construction (SPEC-031 FR-003); nothing here re-resolves it.

#### Acceptance Criteria:

- [ ] 200,000 echoed events into a stream raising `BrokenPipeError` produce exactly one stderr
      line, it names `BrokenPipeError` and says echo is disabled, and the stream is written
      exactly once (invariant 11).
- [ ] A stream raising `ValueError` behaves identically; a stream raising `OSError` is written
      on every call, and 2,500 calls produce exactly three lines whose totals read 1, 1000 and
      2000.
- [ ] Eight threads each making 1,000 echo calls against an `OSError` stream leave the failure
      count at exactly 8,000 and produce exactly nine lines.
- [ ] Every echoed event still reaches the sink on both paths, and the caller's function returns
      normally (invariants 1 and 2).
- [ ] A `KeyboardInterrupt` from the stream still reaches the caller.
- [ ] `test_api.py`'s existing echo tests pass unchanged, including the one asserting the
      `OSError` line's text.
- [ ] `worker.py` reads its period from `_diag.WARN_EVERY`: with `WARN_EVERY` patched to 5,
      both worker sites write at totals 1, 5 and 10, and `worker._DROP_WARN_EVERY` no longer
      exists. The two existing throttle criteria in `test_worker.py` pass unchanged.
- [ ] Removing the disable flag reddens the first criterion by line count; removing the modulo
      reddens the `OSError` criterion; removing the lock is the one mutant no criterion above is
      promised to catch — a lost increment shifts a line, and the threaded criterion is
      evidence, not proof.
- [ ] Serves invariant 11, and invariant 1 on both delivery paths (invariant 6).

### FR-006: An adversarial corpus asserts invariant 8's observable, value by value

#### Description:

`tests/test_sanitize_corpus.py` carries the audit's adversarial values as a parametrised table
and drives each through **both delivery paths** with one assertion function: `lf.info` inside a
`@trace` (the in-span build, delivered by the worker) and `lf.info` with no span open (the
orphan build, emitted synchronously), both against a `MemorySink`. For every row it asserts the
whole of invariant 8's observable, not the one clause the row is about: the call returned; a
walker over every key and value finds no string that fails a strict `.encode("utf-8")`, and no
`str` subclass instance; `json.dumps(event, allow_nan=False)` succeeds; every string is within
`max_value_bytes` and `error.stack` within `max_stack_bytes`; no top-level key was overwritten;
and `truncated` is `True` when the table says so and **absent** when it says not — the silence
cases are what keep the walker honest. Each row also pins the coerced value or placeholder, so a
regression cannot pass as a different placeholder.

The values, so a builder need not recover them from the audit: a self-referencing `dict` and
`list`; a `dict` subclass whose `__iter__` and `items` raise; one whose `__len__` lies by
10⁹; a `Mapping` whose `__iter__` raises; an object whose `__str__` and `__repr__` raise; a
`str` subclass whose `encode` raises; `b"\xff\xfe\x80"`; a lone surrogate as a value and as a
key; a 10 MB string; a 100,000-key `dict`; a 1,000-deep nesting; `Decimal("0.1")`; an aware
and a naive `datetime`; a `str`-valued `Enum`, an `Enum` whose value is a bare `object()`; a
dataclass; `set`, `frozenset`, `tuple`; `[True, 1, False, 0]`; `-0.0`; `1e400`; `nan`;
`10**100_000` and its negative; a mapping with `int`, `tuple`, `None`, `float`, `bool` and
hostile keys; NFC and NFD `é`; `bytearray`; `memoryview`; `10**5000` and `10**4999`; a
20,000-character key. Added by this spec: the `fsdecode` surrogate as message, span name, key,
value and inside `bytes`; the hostile `str` subclass as message and as key (both plain text);
a hostile key beside a sibling; a type whose `__name__` is longer than `max_value_bytes`, as a
key and as a value; and a control row of ordinary text expecting `truncated` absent.

The walker is a test helper, not library code; a library function to "check an event" would be
a second implementation of the invariant for a reviewer to reconcile with the first. Its failure
messages render the offending value through `ascii()`: a failing assertion whose message carries
a lone surrogate crashes the xdist worker that tries to report it, so a plain `repr` would make
the FR-001 mutant readable only under `-n 0`.

#### Acceptance Criteria:

- [ ] The table has at least 45 rows, every row names its expectation for `truncated`, and at
      least ten rows expect it absent.
- [ ] Every row passes on both routes, and the routes share one assertion function.
- [ ] The walker reaches keys as well as values, and at least one row fails the strict-encode
      check when FR-001's replacement is reverted while every other check still passes — and
      that failure is reported, not an `INTERNALERROR`, under the default `-n 12`.
- [ ] Reverting FR-004's guard fails at least one row on its pinned value, not on an exception.
- [ ] Serves invariant 8 on both delivery paths (invariant 6).

---

## Data Model

```python
# src/log_foundry/sanitize.py — new module state, private
_SURROGATES: re.Pattern[str]      # "[\ud800-\udfff]"
_REPLACEMENT = "�"
_STR_STR = str.__str__            # unbound, so a subclass cannot divert the measurement

def _measured(value: str) -> tuple[str, bytes, bool]:
    # the exact str as measured (never the caller's subclass instance), its strict bytes,
    # and whether a surrogate was replaced
def truncate_str(value: str, max_bytes: int) -> tuple[str, bool]   # bool now means "altered"
def truncate_tail(value: str, max_bytes: int) -> tuple[str, bool]  # same widening; exact str out
# _Coercer._dispatch: bytes/bytearray/memoryview decode strictly, then with "replace" + truncated

# src/log_foundry/config.py — configure() only; Config unchanged
def _require_text(name: str, value: object) -> str | None   # TypeError / ValueError; str.__str__ out

# src/log_foundry/console.py — ConsoleWriter, new private state
_disabled: bool                   # latched on BrokenPipeError / ValueError; write() returns early
_failures: int                    # every stream fault, under _lock, for the throttle
_lock: threading.Lock             # taken on the failure path only

# src/log_foundry/_diag.py
WARN_EVERY: int = 1000            # the period every per-event throttle in the library uses

# src/log_foundry/decorator.py — decorate(), decoration-time only
span_name: str                    # name or fn.__qualname__ (if a str) or type(fn).__name__
```

`Config`, `Health`, `FlushResult` and every public signature are unchanged.

---

## API / Interface Contract

```
truncate_str(value, max_bytes) -> (str, bool)   # an exact str that encodes strictly; bool: altered
truncate_tail(value, max_bytes) -> (str, bool)  # same
configure(service=..., version=..., env=...)    # TypeError (not a str) / ValueError (no UTF-8)
trace(fn)                                       # TypeError: classmethod/staticmethod/non-callable,
                                                #            generator function (FR-003)
ConsoleWriter.write(event)                      # total on a stream fault; KeyError still escapes
```

## Configuration / Environment

None. No new knobs, no new extras.

## File & Folder Structure

```
src/log_foundry/
├── sanitize.py        # FR-001 (assembly half), FR-004
├── config.py          # FR-001 (the stamps)
├── decorator.py       # FR-002, FR-003
├── console.py         # FR-005
├── _diag.py           # FR-005: WARN_EVERY
├── worker.py          # FR-005: reads WARN_EVERY at both throttle sites
└── sinks/kinesis.py   # FR-001: one docstring
tests/
├── test_sanitize.py           # FR-001, FR-004 unit criteria
├── test_sanitize_corpus.py    # FR-006
├── test_config.py             # FR-001 stamps
├── test_decorator_sync.py     # FR-002, FR-003
├── test_decorator_async.py    # FR-002, FR-003 (async twin)
├── test_console_echo.py       # FR-005
├── test_sinks_sqlite.py       # FR-001 end-to-end
└── test_sinks_kinesis.py      # FR-001: one docstring
docs/
├── invariants.md              # the Guarded lines for 8, 11, 13; a Recorded exception on 8 for the stamps
├── decisions.md               # event-assembly area extended; one trace-model entry; one diag entry
├── architecture.md            # §12: the two entries 02a8ac5 files, struck in place; §6 auto-capture line
├── specs/SPEC-048-*.md        # FR-001: the surrogate claim, struck in place
├── spec-delivery/SPEC-048-*.md  # same
└── specs/INDEX.md
README.md                      # the @trace section: name fallback, refusals; the echo bullet
```

## Implementation Phases

### Phase 1: Assembly

- FR-001's assembly half: `_measured` returns the exact, replaced string; `str.__str__`; both
  clippers; the strict-then-replace `bytes` decode.
- The three rewritten tests, and the `--collect-only` diff that proves they are the only ones.
- FR-004: `key()` total, the key placeholder, both placeholders through `text()`.
- Correct `_measured`'s and `key()`'s docstrings, `test_sanitize.py`'s surrogate test, the
  `kinesis.py` claim, and the two SPEC-048 prose sites.
- FR-006: the corpus, both routes, the walker.

### Phase 2: The stamps

- FR-001's `configure()` half: `_require_text` beside `_require_positive`, before the
  ownership stamp, storing `str.__str__(value)`; tests in `test_config.py`.
- The SQLite end-to-end criterion.

### Phase 3: Decoration

- FR-002: resolve the name once; refuse `classmethod`, `staticmethod` and non-callables, with
  the `name=` hint for a `str`; consult `__call__` in the async dispatch.
- FR-003, once decided: the generator refusal, both flags, `__call__` consulted.
- README `@trace` section; `architecture.md` §6's auto-capture line.

### Phase 4: Echo

- FR-005: `_diag.WARN_EVERY`, `worker.py` reads it at both sites, `ConsoleWriter.write` owns
  stream faults with the lock and the latch.
- README echo bullet.

### Phase 5: The ritual

- `invariants.md`: Guarded lines for 8, 11 and 13; the stamp exemption on invariant 8's Recorded
  exceptions.
- `decisions.md` entries and the CLAUDE.md digest.
- `architecture.md` §12: if `02a8ac5` has landed, strike its two entries in place with this
  spec; if it has not, its author strikes them on rebase, and the PR body says so.
- Delivery doc; INDEX row.
