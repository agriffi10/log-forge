# Spec: Assembly, Decoration and Echo Residue — a Surrogate, a Nameless Callable, a Hostile Key and a Broken Pipe

**ID:** SPEC-054
**Status:** Draft
**Last Updated:** 2026-09-04
**Depends On:** SPEC-017, SPEC-020, SPEC-025, SPEC-029, SPEC-037

## Overview

The second round of the pre-1.0 surface audit found five defects at the two edges the library
promises most about — what leaves `build_event`, and what `@trace` accepts — and none of them
is visible to `health()`. Every one was reproduced at `456e9b7` and again at `98c7e78`, the tree
this spec is written on.

- **A lone surrogate survives assembly.** `sanitize._measured` encodes with `errors="replace"`
  to *measure* a string, never to replace it, so the `surrogateescape` case its own docstring
  cites — `os.fsdecode(b"file-\xff.txt")` — leaves `build_event` intact. The JSON sinks are
  fine (`ensure_ascii` escapes it), but `SQLiteSink`, `PostgresSink` and `ClickHouseSink` bind
  `function` and `level` as raw `str`, the driver raises `UnicodeEncodeError`, and the worker
  abandons the whole batch after four attempts — innocent neighbours included. Measured on
  `SQLiteSink`: `lost 3 event(s); batch abandoned after 4 emit attempts`, `failed_batches=1`.
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
  around every resumption and pop it at every `yield`, close it on exhaustion, `close()` or an
  exception, and mirror all of that for `asend`/`athrow`/`aclose`. This is a real feature with
  semantics no other span has: `duration_ms` counts suspended time; a generator finalised by the
  collector runs its close in whatever context happens to be current, where the span-stack
  token belongs to another context; and the decorator gains two more twin paths for invariant 6
  to police. Chosen, it is a spec of its own — SPEC-055, beside this one as an arc — and
  FR-003 below is replaced by a refusal-free dispatch to it.

**Recommendation: Option A, now.** It closes the wrong-data defect before the tag at the cost of
a `TypeError` a consumer meets at import, and it leaves Option B open at zero cost. FR-003 is
written as Option A; it is built only once the decision is made, and the other four FRs are
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
  configuration the caller wrote once and can see. Recorded here so the gap is a decision.
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

`truncate_str` and `truncate_tail` return a string that encodes as UTF-8 strictly. The
measurement tries a strict encode first, so the common case costs what it costs today; only on
`UnicodeEncodeError` is every surrogate code point (U+D800–U+DFFF) replaced by U+FFFD — one
replacement character per surrogate, never the `?` that `errors="replace"` writes on encode and
never the three U+FFFD a `surrogatepass` round trip produces — and the replaced string is what is
then measured, clipped and returned. The second element of the returned pair widens from "the
ceiling fired" to "the string was altered", so `_Coercer.text()` sets `truncated` for a
replacement as it does for a clip: a substitution nobody can see is a silent change to the data,
which is the rule `real()` already applies to a non-finite float.

The measurement is taken through `str.__str__`, unbound, so a `str` subclass whose `encode` or
`__str__` raises cannot divert it — the same reasoning as `_INT_LT` and `_FLOAT_REPR` beside it.
Today `info(BadEncode("x"))` costs the event; after this it costs nothing.

Every string `build_event` writes reaches one of the two clippers — `message`, `function`, every
field key and value, every baggage value, and the four `error.*` strings — except the three
config stamps `service`, `version` and `env`, which are copied raw on the hottest path. Those are
refused at `configure()` instead: a value that is not a `str` is a `TypeError` and one that does
not encode strictly is a `ValueError`, each naming the argument, checked before anything is
assigned so a rejected call leaves the config as it found it. That is invariant 13's door, and it
keeps the per-event path at zero extra encodes.

#### Acceptance Criteria:

- [ ] `truncate_str(os.fsdecode(b"file-\xff.txt"), 8192)` returns `("file-�.txt", True)`,
      and the result `.encode("utf-8")` strictly without raising.
- [ ] A string of ten lone surrogates against a ceiling of 4 returns the marker alone, and the
      flag is `True`; a lone surrogate inside an over-budget string is replaced before the clip,
      so the clipped result still encodes strictly.
- [ ] `truncate_tail` gives the same answers at the tail, and a lone surrogate in an
      `error.stack` is replaced in the `span.end` event.
- [ ] `@trace(name=bad)` and `lf.info(bad, path=bad)` on `SQLiteSink`, where `bad` is the
      `fsdecode` value above, deliver all three events with `failed_batches == 0`, and the stored
      `function`, `message` and `fields.path` carry U+FFFD.
- [ ] A `str` subclass whose `encode` raises `RuntimeError`, passed as the message, is delivered
      as its plain text and costs nothing in `in_span_lost` or `orphan_lost`.
- [ ] The event's `truncated` is `True` when a surrogate was replaced anywhere in it, and absent
      when the same event carries only ordinary text (invariant 8).
- [ ] `configure(service="\udcff")` raises `ValueError` naming `service`; `configure(env=7)`
      raises `TypeError` naming `env`; `version` behaves the same; and `get_config()` is
      unchanged after each refusal (invariant 13).
- [ ] The `sinks/kinesis.py` docstring claiming `sanitize.coerce` passes a lone surrogate through
      is corrected, and its test's docstring with it; the guard in `_partition_key` stays,
      because that key is derived from an event field a `TransformSink` may have rewritten.
- [ ] Reverting the replacement while keeping the strict-encode fast path reddens the SQLite
      criterion and the corpus (FR-006); reverting `str.__str__` reddens the subclass criterion.
- [ ] Serves invariant 8 on both delivery paths (invariant 6: the in-span build and the orphan
      build), and invariant 13 for the stamps.

### FR-002: `@trace` names a callable once, at decoration, and refuses a misordered descriptor there

#### Description:

The span name is resolved when `decorate` runs, not on every call: the explicit `name` if one
was given, else `fn.__qualname__` when that attribute exists and is a `str`, else the callable's
type name. A `functools.partial` therefore traces as `partial` and a callable instance as its
class, and neither raises. Both wrappers read the resolved name from their closure, which also
removes an attribute lookup from the per-call path.

A `classmethod` or `staticmethod` object handed to `@trace` is a decorator applied in the wrong
order, and it is refused at decoration with a `TypeError` that names the underlying function and
says which way round to put them. `classmethod` is refused because it is not callable and the
wrapper would fail every call; `staticmethod` is refused even though it *is* callable, because
the wrapper replaces the descriptor and an instance call would then hand `self` to a function
declared without one. Anything else that is not callable is refused by the same check, naming its
type. Invariant 13: refused where it is written, never on the first call.

#### Acceptance Criteria:

- [ ] `trace(functools.partial(f, 1))()` returns `f(1)` and emits a span named `partial`;
      `trace(C())()` on a callable instance returns its result and emits a span named `C`
      (invariant 1).
- [ ] An explicit `name=` still wins over both, and a `__qualname__` that is not a `str` falls
      through to the type name rather than reaching `Span.name`.
- [ ] `@trace` above `@classmethod` and above `@staticmethod` each raise `TypeError` at class
      body execution, the message naming the function and the correct order; `@classmethod` above
      `@trace` keeps working, as does `@staticmethod` above `@trace` (invariant 13).
- [ ] `trace(object())` raises `TypeError` naming the type.
- [ ] `iscoroutinefunction` dispatch is unchanged: a partial of an `async def` still takes the
      async wrapper.
- [ ] Removing the `isinstance(__qualname__, str)` check reddens its criterion; removing the
      `staticmethod` clause reddens the misorder criterion while the `classmethod` half stays
      red on its own.
- [ ] Serves invariants 1 and 13 on both wrappers (invariant 6: sync and async).

### FR-003: `@trace` refuses a generator function at decoration

#### Description:

*Option A above; built only once the decision is made.* `decorate` tests
`inspect.isgeneratorfunction` and `inspect.isasyncgenfunction` — both see through a
`functools.partial` — and raises `TypeError` naming the function and saying that a generator's
body runs after the wrapper has returned, so the span would close before it starts; the message
points at tracing the consumer or opening the span around the loop. Nothing else changes:
today's behaviour for a generator is a span of the *call*, which is wrong data on a fresh trace
for every event the body logs, and no consumer can be relying on it knowingly.

#### Acceptance Criteria:

- [ ] `@trace` on a `def` containing `yield` raises `TypeError` at decoration, naming the
      function; the same for `async def` with `yield`, and for a `partial` of either
      (invariant 13).
- [ ] A function that returns a generator object without being one is accepted, and the
      limitation is stated in the docstring rather than closed.
- [ ] The README's `@trace` section says generators are refused and what to do instead.
- [ ] Removing the async-generator half of the check reddens its criterion while the sync half
      stays red on its own.
- [ ] Serves invariant 13 on both dispatch branches (invariant 6).

### FR-004: A hostile mapping key costs itself, not its siblings, and every placeholder is bounded

#### Description:

`_Coercer.key()` is total. Whatever a key's `__str__`, `bit_length` or `encode` raises is caught
inside `key()` itself, the key becomes `<unserializable key: T>` where `T` is the key's type name,
and `truncated` is set — a key that could not be rendered is a substitution the reader must be
able to see, unlike a value placeholder which is visible on its own. The guard sits in `key()`
rather than in `mapping()`'s loop because the loop's other work — the `max_keys` cap and the
value coercion — already has the right owners, and a second `try` in the loop would hide which of
the three failed.

Two hostile keys of one type collide on one placeholder and the later wins; that is accepted, and
said in the docstring, because the alternative is a key that carries the value's identity, which
is what arch §6 forbids. Both placeholders — the key form here and `_placeholder`'s value form —
go through `text()` on their way out, so a type whose `__name__` was set to something enormous
still honours `max_value_bytes` exactly.

#### Acceptance Criteria:

- [ ] `fields={"boom": {"sib": 1, KeyBoom(): 2}}` delivers `fields.boom == {"sib": 1,
      "<unserializable key: KeyBoom>": 2}` with `truncated == True` (invariant 8).
- [ ] A `str` subclass key whose `encode` raises, and an `int` subclass key whose `__str__`
      raises, each get the same placeholder and cost no sibling.
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
`_diag.absorbed` with a detail saying echo is disabled for the life of the writer, sets a flag,
and every later `write` returns without touching the stream. Any other `Exception` from the
stream may be transient — `EAGAIN` on a non-blocking terminal, `ENOSPC` on a file — so the writer
keeps trying, counts each failure, and announces the first and then every thousandth with the
running total, which is the queue-full site's idiom (SPEC-017 FR-005). The period is one
definition, `_diag.WARN_EVERY`, read by both sites, because two constants stating one number
disagree eventually. `api._log`'s outer guard stays as the total guard for anything else, a
missing key above all; it is no longer the thing that writes the line for a stream fault.

The state lives on the writer rather than the module so a `ConsoleWriter(stream=…)` built for a
test starts clean, and so the process-global `api._console` is the only one that can be disabled
for the process. The stream is still bound at construction (SPEC-031 FR-003); nothing here
re-resolves it.

#### Acceptance Criteria:

- [ ] 200,000 echoed events into a stream raising `BrokenPipeError` produce exactly one stderr
      line, it names `BrokenPipeError` and says echo is disabled, and the stream is written
      exactly once (invariant 11).
- [ ] A stream raising `ValueError` behaves identically; a stream raising `OSError` is written
      on every call, and 2,500 calls produce exactly three lines whose totals read 1, 1000 and
      2000.
- [ ] Every echoed event still reaches the sink on both paths, and the caller's function returns
      normally (invariants 1 and 2).
- [ ] A `KeyboardInterrupt` from the stream still reaches the caller.
- [ ] `test_api.py`'s existing echo tests pass unchanged, including the one asserting the
      `OSError` line's text.
- [ ] `worker.py` reads its period from `_diag.WARN_EVERY` and the two throttle criteria in
      `test_worker.py` still pass.
- [ ] Removing the disable flag reddens the first criterion by line count; removing the modulo
      reddens the `OSError` criterion.
- [ ] Serves invariant 11, and invariant 1 on both delivery paths (invariant 6).

### FR-006: An adversarial corpus asserts invariant 8's observable, value by value

#### Description:

`tests/test_sanitize_corpus.py` carries the round-two audit's 37-value probe as a parametrised
table, plus the cases this spec adds — a lone surrogate as message, span name, key, value and
inside bytes; a hostile `str` subclass as message and as key; a hostile key beside a sibling; a
placeholder-length type name — and drives each through the real path twice: `build_event`
directly, and `lf.info` inside a `@trace` against a `MemorySink`, so both twins are covered. For
every case it asserts the whole of invariant 8's observable, not the one clause the case is
about: the call returned; a walker over every key and value finds no string that fails a strict
`.encode("utf-8")`; `json.dumps(event, allow_nan=False)` succeeds; every string is within
`max_value_bytes` and `error.stack` within `max_stack_bytes`; no top-level key was overwritten;
and `truncated` is `True` when the table says so and **absent** when it says not — the silence
cases are what keep the walker honest. Each row also pins the coerced value or placeholder, so a
regression cannot pass as a different placeholder.

The walker is a test helper, not library code; a library function to "check an event" would be
a second implementation of the invariant for a reviewer to reconcile with the first.

#### Acceptance Criteria:

- [ ] The table has at least 45 rows, every row names its expectation for `truncated`, and at
      least ten rows expect it absent.
- [ ] Every row passes on both routes, and the routes share one assertion function.
- [ ] The walker reaches keys as well as values, and at least one row fails the strict-encode
      check when FR-001's replacement is reverted while every other check still passes.
- [ ] Reverting FR-004's guard fails at least one row on its pinned value, not on an exception.
- [ ] Serves invariant 8 on both delivery paths (invariant 6).

---

## Data Model

```python
# src/log_foundry/sanitize.py — new module state, private
_SURROGATES: re.Pattern[str]      # "[\ud800-\udfff]"
_REPLACEMENT = "�"
_STR_STR = str.__str__            # unbound, so a subclass cannot divert the measurement

def _measured(value: str) -> tuple[str, bytes]:   # the string as measured, and its strict bytes
def truncate_str(value: str, max_bytes: int) -> tuple[str, bool]   # bool now means "altered"
def truncate_tail(value: str, max_bytes: int) -> tuple[str, bool]  # same widening

# src/log_foundry/console.py — ConsoleWriter, new private state
_disabled: bool                   # set on BrokenPipeError / ValueError; write() returns early
_failures: int                    # every stream fault, for the throttle

# src/log_foundry/_diag.py
WARN_EVERY: int = 1000            # the period every per-event throttle in the library uses

# src/log_foundry/decorator.py — decorate(), decoration-time only
span_name: str                    # name or fn.__qualname__ (if a str) or type(fn).__name__
```

`Config`, `Health`, `FlushResult` and every public signature are unchanged.

---

## API / Interface Contract

```
truncate_str(value, max_bytes) -> (str, bool)   # the str encodes strictly; bool: altered
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
├── worker.py          # FR-005: reads WARN_EVERY
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
├── invariants.md              # the Guarded lines for 8, 11, 13
├── decisions.md               # event-assembly area extended; one trace-model entry; one diag entry
├── architecture.md            # §12 if a sibling filed N1/N4 there; §6 auto-capture line
└── specs/INDEX.md
README.md                      # the @trace section: name fallback, refusals; the echo bullet
```

## Implementation Phases

### Phase 1: Assembly

- FR-001's assembly half: `_measured` returns the replaced string; `str.__str__`; both clippers.
- FR-004: `key()` total, the key placeholder, both placeholders through `text()`.
- Correct `_measured`'s and `key()`'s docstrings, `test_sanitize.py`'s surrogate test, and the
  `kinesis.py` claim.
- FR-006: the corpus, both routes, the walker.

### Phase 2: The stamps

- FR-001's `configure()` half: type and encode checks before assignment, tests in
  `test_config.py`.
- The SQLite end-to-end criterion.

### Phase 3: Decoration

- FR-002: resolve the name once; refuse `classmethod`, `staticmethod` and non-callables.
- FR-003, once decided: the generator refusal, both flags.
- README `@trace` section; `architecture.md` §6's auto-capture line.

### Phase 4: Echo

- FR-005: `_diag.WARN_EVERY`, `worker.py` reads it, `ConsoleWriter.write` owns stream faults.
- README echo bullet.

### Phase 5: The ritual

- `invariants.md` Guarded lines; `decisions.md` entries and the CLAUDE.md digest; `architecture.md`
  §12 supersessions if any sibling filed these; delivery doc; INDEX row.
