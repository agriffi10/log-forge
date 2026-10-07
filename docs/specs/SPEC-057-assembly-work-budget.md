# Spec: Assembly Work Budget — Node and Byte Budgets per Pass, and Reads Bounded by the Ceiling

**ID:** SPEC-057  
**Status:** Draft  
**Last Updated:** 2026-10-07  
**Depends On:** SPEC-017, SPEC-020, SPEC-055

## Overview

`build_event` runs on the caller's thread on both delivery paths: inside a span the event is built
where `info()` is called and only the sink I/O moves to the drain thread, and outside one the whole
emit is synchronous (invariant 4). The coercion pass it runs bounds how deep a value nests
(`max_depth`), how wide a container is (`max_keys`) and how long a string is (`max_value_bytes`),
and it finds a cycle by ancestor path. It bounds nothing about the pass's *total* work, so a
pathological but legal argument can hold the application's thread, or inflate the event without
limit, and invariant 1 does not survive it. Three effects, each readable from `sanitize.py` as it
stands at `324936e`:

- **A shared sublist is walked once per path.** `y = 0`, then `y = [y] * 16` seven times, then
  `info("m", fields={"a": y})` is a DAG, not a cycle, so the ancestor check never fires. At the
  default `max_depth` of 8 and `max_keys` of 256 nothing cuts it either, and the pass makes
  16^7 = 268,435,456 visits to the leaves. It was still running after 15 s at `68caacf`, where
  `sanitize.py` is byte-identical to `324936e`. Memoising by `id()` would not help: the output
  must be a JSON tree, so a DAG's output is itself exponential and `json.dumps` in the sink would
  blow up instead.
- **The work to read one value is proportional to its length, not to its ceiling.**
  `truncate_str` and `truncate_tail` encode the whole string before they clip it, and `_dispatch`
  copies and decodes a whole `bytes`, `bytearray` or `memoryview` first; `str()` of a `Decimal`, or
  of a non-`str` key, costs its digits or its contents. Even once the walk is bounded to 131,072
  values (FR-001), a 1 MiB string shared across all of them is 131,072 full encodes, 128 GiB of
  work, from one small argument.
- **The event has no size bound.** Even once the walk is bounded to 131,072 values, an 8 KiB string
  shared across all of them is 131,072 × 8,192 bytes, 1 GiB of strings. `json.dumps` escapes — a
  control character becomes six bytes — so the serialised event is up to 6 GiB, and on the orphan
  path that serialisation is on the caller's thread.

This spec bounds every coercion pass three ways: by the number of values it visits, by the bytes
it charges, and — so that neither budget is spent by reading rather than by writing — by making a
read cost the ceiling instead of the input's length. It adds no configuration key.

## Scope

### In Scope

- A node budget per coercion pass: `sanitize.MAX_NODES = 131_072` values, past which the walk
  stops at a `<node limit>` placeholder (FR-001).
- A byte budget per coercion pass: `sanitize.MAX_RENDERED_BYTES = 16 * 1024 * 1024`, past which the
  walk stops at a `<size limit>` placeholder (FR-002).
- Reads bounded by the ceiling: `truncate_str`, `truncate_tail` and the bytes-like branch of
  `_dispatch` read a prefix of `max_bytes + 1` characters (items, for a bytes-like) of a longer
  input (FR-003).
- The promise restated at the four sites outside the register that state the old claim (FR-004).
- **Sequencing with SPEC-056 (OTLP log export).** That spec bounds its own OTLP mapping with a record
  budget of the same number, the same placeholder text and the same stop rule, and leaves `sanitize`
  out of its own scope. The constant has one home, `sanitize`, which imports nothing from the
  package at runtime, so a module that imports it from there cannot close a cycle. Whichever of the
  two builds second shares that constant instead of defining another: if SPEC-056 has built first,
  FR-001 replaces the mapping's definition with an import of `sanitize.MAX_NODES` and leaves the
  mapping's placeholder text and stop rule as they are.

### Out of Scope

- **A `configure()` knob for either budget.** Both are module constants. A constant can become a
  knob later by appending a keyword, while a knob that ships cannot be withdrawn (invariant 10, and
  the deferral of the worker tunables in `docs/decisions/public-api.md`). SPEC-056's budget is
  likewise a constant, not a key.
- **Changing the defaults** of `max_value_bytes`, `max_stack_bytes`, `max_keys` or `max_depth`.
- **The cost of a caller's own `__str__`, `__repr__`, `__iter__` or `__hash__`.** That is user code.
  The budgets bound how often the library calls it, not what one call costs.
- **A `TransformSink` that adds a DAG after assembly.** It runs on the drain thread, and SPEC-056's
  mapping carries its own budget for what it serialises.
- **Sink-side size limits.** A destination's hard limit is still met by lowering `max_value_bytes`
  or `max_keys`, and `dropped_oversized` still counts what a sink refuses. `MAX_RENDERED_BYTES` is a
  work budget on the caller's thread, not a limit tuned to any destination.
- **The three config stamps** (`service`, `version`, `env`), which bypass assembly and are recorded
  as an exception on invariant 8 already.

### Accepted Limits

Stated here rather than left to be found.

- **The budgets are per pass, not per event.** Each `sanitize_fields` or `coerce` call is one pass
  with its own counters. `build_event` makes one pass over the merged fields (defaults, span
  defaults, baggage, per-call fields). `model.backfill_baggage` makes a second pass over the final
  baggage and merges the result into `span.start` and `span.end`, so a boundary event can hold up
  to twice each budget. The work on the caller's thread is still bounded, per pass.
- **A pathological `defaults` is walked on every event.** The budget bounds each pass, not their
  sum over a process's life; a `configure(defaults=…)` that reaches the budget pays it per event.
- **The serialised worst case is about six times the byte budget**, 96 MiB, because `json.dumps`
  escapes a control character as six bytes, plus the uncharged scalars and punctuation, which the
  node count bounds at a few tens of bytes per node.
- **The new placeholders are written as they stand**, 12 bytes each, like `<depth limit>` and
  `<circular>`. A `max_value_bytes` below a placeholder's length is already exceeded by
  `<depth limit>`, by `<circular>` and by the 14-byte truncation marker; the two new placeholders
  share that gap, and this spec does not close it.
- **Neither budget adds module-level mutable state.** The counters live on the pass object, so
  nothing is stranded across a fork (invariant 12).

---

## Functional Requirements

### FR-001: A node budget per coercion pass

#### Description:

The pass counts every value it visits, scalar or container, including one that becomes
`<depth limit>` or `<circular>`: every entry to `_Coercer.value`. The top-level `fields` mapping
(entered at depth −1, without passing through `value`) and mapping keys (rendered by `key()`) are
not values and are not counted. The value that would be the `MAX_NODES + 1`th is not dispatched: it
becomes the placeholder `<node limit>`, the event is marked `truncated`, and nothing after it is
visited. Every container still open ends after the member it was writing, so the siblings that come
later in walk order are dropped. That is the stated cost: walk order decides what is sacrificed, and
the first `MAX_NODES` values in walk order survive. A `max_keys` raised past `MAX_NODES` cannot
widen a container past the budget.

The order of checks on one value is: node budget, then `max_depth`, then the type's own rule. A
value past the budget is `<node limit>` even where it also sits past `max_depth`, and a value that
becomes `<depth limit>` still spends a slot. The number, the placeholder text, the stop and the
counting are deliberately those of SPEC-056's record budget (see *Scope*).

**Why 131,072.** It is twice a fully populated two-level structure at the default `max_keys`
(256 × 256 = 65,536 values), so a wide but ordinary event is not cut, and no larger, so the worst
pass stays small. The constant's docstring says this beside the number.

**Rejected.** *Memoising by `id()`*: the output must be a JSON tree, so a DAG's output is still
exponential. *A global "visited" set*: it breaks the rule `_Coercer.__init__` documents, that two
siblings referencing one object both render. *Replacing every value past the budget with a
placeholder while containers keep iterating* (the shape of `<depth limit>`): it writes up to
(`max_depth` + 1) × `max_keys` placeholders after the breach and diverges from SPEC-056.

#### Acceptance Criteria:

- [ ] `y = 0`, then `y = [y] * 16` seven times, passed as `fields={"a": y}`, returns on both routes
      (a level call inside a `@trace`, and one with no span open) before a 5-second alarm.
      `fields["a"]` holds exactly `MAX_NODES + 1` values counting itself — `MAX_NODES` visited plus
      `<node limit>`, which is last in walk order — and the event is marked `truncated`.
- [ ] The alarm is `signal.setitimer(signal.ITIMER_REAL, …)` with a handler that raises a
      `BaseException` subclass, because `_Coercer.value`'s guard catches `Exception` and would turn
      an `Exception` into a placeholder; the test first asserts that it runs on the main thread, the
      only one signals reach.
- [ ] The test is run once against `main`'s `sanitize.py` (checked out into a scratch worktree,
      never by switching this branch) and fails by the alarm; the PR body records that run.
- [ ] With `max_keys` raised to `MAX_NODES`, a field holding a list of `MAX_NODES − 1` integers
      (`MAX_NODES` values counting the list) is written whole with `truncated` absent; a list of
      `MAX_NODES` integers keeps its first `MAX_NODES − 1` intact, ends in `<node limit>`, and is
      marked. `coerce` on the same two lists gives the same two answers.
- [ ] A field holding 256 lists of 256 integers (65,793 values) is written whole, with `truncated`
      absent.
- [ ] With `max_keys` raised, `fields={"a": [[0] * (MAX_NODES − 2), [1, 2, 3]], "b": 1}` yields
      `a == [[0] * (MAX_NODES − 2), "<node limit>"]`, no key `b`, and a marked event: the
      placeholder replaced a container unread, and every open container ended.
- [ ] With `max_depth=1` and `max_keys` raised, `{"a": [0] * MAX_NODES}` writes `<depth limit>` for
      its first `MAX_NODES − 1` members and `<node limit>` for the last: the node check precedes the
      depth check, and a value that becomes `<depth limit>` still counts.
- [ ] A `@trace`d function that sets a baggage value shaped like the first input has `<node limit>`
      in the `fields` of both `span.start` and `span.end`, each marked: the `backfill_baggage` pass
      honours the budget on the same terms.
- [ ] `tests/test_sanitize_corpus.py` gains a row for a three-level 256-wide shared nest, expecting
      `<node limit>` and `truncated`, and a silence row for a 256 × 256 structure, expecting neither.
- [ ] Planted out once each — the node check, each stop `break` in `mapping` and `members`, and the
      order of the node check ahead of the depth check, and likewise every new guard of FR-002 and
      FR-003 (the byte check, the produced-text charge, each bounded read) — a named test
      reddens; the PR body records each guard and the test that caught it.
- [ ] Serves invariants 1, 6 and 8: the criteria that build an event run on both delivery paths,
      the baggage criterion covers the boundary-event pass, and `truncated` marks the cut.

### FR-002: A byte budget per coercion pass

#### Description:

The pass keeps a running charge against `MAX_RENDERED_BYTES` (16,777,216). What is charged is the
table under *Data Model*, in three classes:

- **Text from the caller** — a `str` value or key, the text decoded from a bytes-like, an enum
  member's `str` value — is charged at its UTF-8 length *after* clipping.
- **Text the pass produces** by calling `str()` or `isoformat()` itself — a `Decimal`, `UUID`,
  date or time, an enum's fallback `str(value)`, a key that is not a `str` (an integer or float key
  included, and whatever placeholder it becomes), and a type-name placeholder
  (`<unserializable: T>`, `<unserializable key: T>`) — is charged at the *larger* of its length in
  characters before clipping and its UTF-8 bytes after, because producing it was the work.
- **An integer written as a value** is charged at the rendered length `integer()` estimates, digits
  plus one for a sign (the estimate can overcount by one, which errs toward the cut). A key is
  charged once, as the string it becomes, so an integer key is never charged both by `integer()` and
  as a string.

Fixed placeholders (`<depth limit>`, `<circular>`, `<node limit>`, `<size limit>`, `<int: …>`,
`<float: …>`), floats, bools and `None` are not charged; the node count bounds them.

The value whose charge would push the total past the budget becomes `<size limit>` — whole, never
partially clipped — and the event is marked. Nothing after it is visited and every open container
ends after the member it was writing: FR-001's breach rule exactly, and the pass stops when *either*
budget is spent. Where the item that crosses is a mapping key, the entry is written
`"<size limit>": "<size limit>"`, its value unvisited, and the mapping ends.

**Why two classes of text.** A caller's own `str` already exists, FR-003 reads only its ceiling, and
clipped it costs the event at most `max_value_bytes`; charging it at its full length would turn
today's clipped 20 MB string field into `<size limit>`, a regression for a legitimate input. Produced
text did not exist until the pass made it, so its length *is* the work: one `Decimal` of 10,000,000
digits referenced 256 times is 2,560,000,000 characters of `str()`, which clipping afterwards does
not undo.

**Why 16 MiB.** No sink here accepts an event near it (SQS: 256 KB), and reaching it legitimately
takes more than 2,048 strings at the default 8 KiB ceiling, so in practice it only catches
amplification. The serialised worst case follows from it: six times the budget (*Accepted Limits*).

**Behaviour that changes.** A single value of more than 16 MiB of produced text — a `Decimal` of more
than about 16.7 million digits — becomes `<size limit>` where today it is clipped, and so does a
caller's string longer than the budget under a `max_value_bytes` configured above it. That agrees with
the fence "a value too large to *render* is replaced, never clipped"
(`docs/decisions/event-assembly.md`, SPEC-020).

**A deferral reversed.** `architecture.md` §13 says a per-event byte ceiling "was deferred by
SPEC-017, again by SPEC-020, and deliberately again by SPEC-021", because it has real design surface:
what happens on breach, and which fields are sacrificed first. It is added here, and FR-001's breach
rule answers both: a breach is a placeholder and a stop, and walk order decides what is sacrificed.
It is a *work* budget, not a sink-size limit.

**Rejected.** *A `__sizeof__` gate for `Decimal`*: on CPython's C implementation it reports an
allocation, not a digit count — flat up to 76 digits, then one 8-byte word per 19 digits — so it gates
falsely at a small `max_value_bytes`. *A smaller node budget in place of this one*: 2,048 nodes would
bound the bytes and cut a 256 × 256 structure, and would diverge from SPEC-056. *Charging a caller's
`str` at its pre-clip length*: the regression above.

#### Acceptance Criteria:

- [ ] A structure whose charged total is exactly `MAX_RENDERED_BYTES` is written whole with
      `truncated` absent: key `a` (1 byte), 2,047 strings of 8,192 bytes and one of 8,191, in one
      flat list with `max_keys` raised. Replacing the 8,191-byte string with one of 8,192 makes
      that string `<size limit>` and marks the event; a sibling `"z"` after it in the list, and a
      second field `b`, are absent.
- [ ] The exact structure also holds an integer key, a float, a bool, a `None`, an over-long integer
      (which becomes `<int: …>`) and a value at `max_depth` (`<depth limit>`), adjusted so the total
      is still exact: the last five charge nothing and the key charges once, so a charge-rule error
      moves the total by at least a byte and reddens the test.
- [ ] 256 references to one `str` of 10,000,000 characters are each clipped, the event is marked by
      the clips, and none is `<size limit>`; a list of two references to one `Decimal` of
      10,000,000 digits is `[<clipped Decimal>, "<size limit>"]` and marked.
- [ ] A mapping key that crosses: after a first field `a` charging 16,777,215 bytes with its key, a
      second field `bb` yields `"<size limit>": "<size limit>"` and no `bb`.
- [ ] With `10**3999` (4,000 digits, charged exactly 4,000) in lists of at most 256 under one field,
      4,194 are written whole and the 4,195th is `<size limit>`. The test asserts as its premise
      that `sys.get_int_max_str_digits()` is 0 or at least 4,000.
- [ ] With `max_value_bytes` configured to 32 MiB, a `str` of `MAX_RENDERED_BYTES` bytes is
      `<size limit>`, marked and not clipped.
- [ ] Each of these returns on both routes before the 5-second alarm and is marked: a `Decimal` of
      10,000,000 digits referenced 256 times from one list, which is `[<clipped>, "<size limit>"]`;
      the fan-out-16, depth-7 shape with a shared 1,048,576-character non-ASCII string as its leaf,
      which contains `<size limit>` and not `<node limit>`; and a three-deep nest of 256-key
      mappings whose keys are `(s, i)`, `s` a shared 1,048,576-character string and each value the
      next level's one shared mapping, which contains `<size limit>`. Each is run once against
      `main`'s `sanitize.py`, and the PR body names whether it failed by the alarm or by its
      assertion.
- [ ] `tests/test_sanitize_corpus.py` gains a row for a list of two references to one 9,000,000-digit
      `Decimal`, expecting `<size limit>` second and `truncated`, and a silence row for 100 references
      to a clipped 10,000,000-character `str`, expecting no `<size limit>`.
- [ ] Serves invariants 1, 6 and 8: the byte budget is a work bound on the caller's thread and holds
      on both delivery paths and the boundary-event pass, and `truncated` marks the cut.

### FR-003: Reading a value costs work bounded by its ceiling, not its length

#### Description:

A string longer than `max_bytes` characters cannot fit: every character is at least one byte.
`truncate_str` therefore reads only the first `max_bytes + 1` characters of one, and `truncate_tail`
only the last `max_bytes + 1`, through the unbound `str.__len__` and `str.__getitem__`, so a subclass
cannot divert them — the module's existing rule (`_STR_STR`, `_INT_LT`, `_FLOAT_REPR`). It is one more
than the ceiling because the clip fires when the encoding *exceeds* `max_bytes`: a prefix of exactly
`max_bytes` ASCII characters encodes to exactly `max_bytes` bytes and would be returned whole where
today's is clipped. A string of `max_bytes` characters or fewer is read as it is today (and a subclass
is copied by `_STR_STR`, which costs at most the ceiling). A `bytes`, `bytearray` or `memoryview` is
read through its buffer at every size, never through a method a subclass can override, and its size
through the type's own unbound accessor (`bytes.__len__`, `bytearray.__len__`,
`memoryview.nbytes`); one longer than `max_value_bytes` bytes is read only through its first
`max_value_bytes + 1` bytes. A memoryview is sliced where its layout allows — one-dimensional at any
stride, where the slice counts items and so yields at least that many bytes, or C-contiguous after a
cast to bytes.

The rendering is **byte-identical to today's unbounded computation, flag included**; it is a pure
work reduction. The prefix's UTF-8 is a prefix of the whole string's, and the clip keeps at most
`max_bytes − 14` bytes of head or tail (the marker is 14 bytes). A lone surrogate is replaced by
U+FFFD wherever it sits, so one before or after the cut gives one result. For bytes the decoder works
left to right, a sequence split by the cut starts no earlier than three bytes before it, and a decoded
unit is never shorter than its source, so everything the clip keeps was decoded identically. The
criteria hold this to a reference copy of today's algorithm rather than to this argument.

**Two recorded exceptions**, both hostile-only or unreachable from the standard library. A `bytes` or
`bytearray` subclass that overrides `__bytes__` is rendered from its buffer, as `_STR_STR` already
does for a `str` subclass; today `bytes(value)` honours the override. A memoryview longer than the
ceiling that is neither one-dimensional nor C-contiguous — constructible only by an extension type
such as a strided NumPy slice — cannot be prefix-read, and falls to the `<unserializable: memoryview>`
placeholder through `value()`'s guard where today it is clipped and marked. No criterion asserts the
second, because the standard library cannot build one; the guard carries it.

This bounds *reading*. Producing text — `str(Decimal)`, a key's `str()` — costs what it costs, and
FR-002 bounds how often the pass does it. The `model.py` callers (`message`, `function`, the four
`error` strings, the stack through `truncate_tail`) benefit without an edit.

#### Acceptance Criteria:

- [ ] Against a reference copy of today's `truncate_str`, `truncate_tail` and bytes-like rendering,
      the new code returns an identical `(text, flag)` over a corpus of: ASCII; 2-, 3- and 4-byte
      characters at every offset around the cut; a lone surrogate before the cut and after it;
      invalid and truncated UTF-8 at and after the cut; ceilings from 1 to 40 bytes; strings of
      exactly `max_bytes` characters (whole, unmarked) and of `max_bytes + 1` (clipped, marked); and
      a `bytes`, `bytearray` and `memoryview` — contiguous, strided one-dimensional, itemsize
      above 1, zero-dimensional, two-dimensional C-contiguous — of each such content.
- [ ] A `str` subclass whose `__len__`, `__getitem__`, `encode` and `__str__` each lie or raise,
      longer and shorter than the ceiling, gives the answer for its plain text as an exact `str`,
      consulting none of them.
- [ ] A `bytes` subclass whose `__bytes__` returns other content, longer and shorter than the
      ceiling, is rendered from its buffer (the first recorded exception).
- [ ] The work is bounded by allocation, not by time: with the input allocated before tracing
      starts, the traced peak of `truncate_str`, `truncate_tail` and `coerce` is under 1 MiB for a
      32 MiB exact `str` and a 32 MiB `str` subclass, and of `coerce` for a 32 MiB `bytes`,
      `bytearray` and `memoryview`; and of `build_event` for a 32 MiB `message` and a 32 MiB span
      name. Each is run once against `main`'s module, where it exceeds the bound.
- [ ] The existing criteria in `tests/test_sanitize.py` and every row of `tests/test_sanitize_corpus.py`
      pass unchanged on both routes.
- [ ] Serves invariants 1 and 8: no input makes the call return late for want of a bound on the read,
      and every ceiling still holds exactly.

### FR-004: The promise restated where it is recorded

#### Description:

"The ceilings bound each *value*, not the event" is the old claim, and after FR-001 to FR-003 it is
no longer the whole of it: the four ceilings still bound a value, each pass is now bounded as a whole
in work, and the event's size is still not bounded to any destination's limit. It is stated at five
sites. This FR amends the four that are not the register; the register is written at completion.

- `docs/invariants.md` §8: the statement, the *Observable* and *Guarded* gain the two budgets, the two
  placeholders and the new tests. The observable is kept exact — a pass visits at most `MAX_NODES`
  values and charges at most `MAX_RENDERED_BYTES`, and the value that crosses either is a marked
  placeholder after which nothing is visited. A *Recorded exceptions* line gains the two-pass boundary
  event. This FR edits invariant 8's own text, so it cites invariant 8.
- `docs/architecture.md` §13, the bullet "The payload ceilings bound each *value*, not the event as a
  whole": amended in place. It keeps the 2,060,506-byte measurement with its `456e9b7` anchor, states
  the 16 MiB work budget, the node budget and the serialised worst case at six times the byte budget,
  and says that a destination's limit is still met by lowering the ceilings. The sentence deferring a
  per-event byte ceiling is struck through, not deleted.
- `src/log_foundry/config.py`, the `Config` docstring: "They bound each *value*, not the event as a
  whole" is amended to say what now bounds the pass.
- `README.md`, *Field values are coerced and bounded*: "all four ceilings bound each *value*" is
  amended the same way, with the link to the known constraints kept absolute — the README is the PyPI
  long description.

**Written at completion, not by this FR** (`docs/process/completion-ritual.md`): the register entry
and its Contents row, then the amended fence "Ceilings bound per *value*" in
`docs/decisions/event-assembly.md`; the `value sanitizer` row of `docs/component-inventory.md`; the
pending release notes, when a tag's file exists, since a pass now ends at `<node limit>` or
`<size limit>` where it did not; and the prune of `CLAUDE.md`'s `## Specs`. The delivery docs of
SPEC-017 and SPEC-020 state the claim too, and are left alone: they are frozen records of what those
specs shipped, and their statement stays true of the four ceilings.

#### Acceptance Criteria:

- [ ] `docs/invariants.md` §8 names `MAX_NODES`, `MAX_RENDERED_BYTES`, `<node limit>` and
      `<size limit>`, records the two-pass boundary event as an exception, and lists
      `tests/test_assembly_budget.py` under *Guarded*; `tests/test_invariants_model.py`'s
      `test_the_page_cites_tests_that_exist_and_this_file_cites_invariants_that_do` passes.
- [ ] The `architecture.md` §13 bullet states the budgets and the six-times serialised bound, keeps
      the `456e9b7` anchor on its measurement, and shows the deferral sentence struck through with
      `~~`, not removed.
- [ ] The `Config` docstring and the README paragraph each name the two budgets, and
      `poetry run python scripts/docstring-lint.py` passes.
- [ ] A search of `README.md`, `docs/architecture.md`, `docs/invariants.md` and `src/` for "not the
      event" and for "each *value*" finds each hit only in a sentence that also names the budgets;
      `docs/decisions/`, `docs/spec-delivery/` and `docs/specs/` are excluded, for the reasons above.
- [ ] `sh scripts/docs-lint.sh` and `sh scripts/spec-lint.sh` pass.
- [ ] Serves invariant 8: the page that states what assembly promises is amended in the same change
      that alters what it promises.

---

## Data Model

```
# src/log_foundry/sanitize.py            (no module-level mutable state; the counters are per pass)
MAX_NODES: int = 131_072                 # values visited per pass; its docstring says why (FR-001)
MAX_RENDERED_BYTES: int = 16 * 1024 * 1024   # 16_777_216 bytes charged per pass (FR-002)
_NODE_LIMIT = "<node limit>"             # beside _DEPTH_LIMIT and _CIRCULAR
_SIZE_LIMIT = "<size limit>"
__all__ += ["MAX_NODES", "MAX_RENDERED_BYTES"]

class _Coercer:                          # __slots__ gains, per pass:
    #   values visited so far, bytes charged so far, and which limit (if any) has stopped the walk
```

What is charged against `MAX_RENDERED_BYTES`:

| Item written | Charge |
|---|---|
| `str` value or `str` key; decoded bytes-like; enum member's `str` value | UTF-8 bytes of the result, after clipping |
| `Decimal`, `UUID`, date/time, enum fallback `str(value)`, non-`str` key (an integer or float key included, and whatever placeholder it becomes), `<unserializable: T>` and `<unserializable key: T>` | the larger of characters before clipping and UTF-8 bytes after |
| `int` written as a value | `integer()`'s rendered-length estimate: digits, plus one if negative |
| `<depth limit>`, `<circular>`, `<node limit>`, `<size limit>`, `<int: …>`, `<float: …>`, finite float, `bool`, `None` | nothing |

Order of checks on one value: node budget, `max_depth`, the type's rule (whose text or integer
charge is the byte check).

## API / Interface Contract

No signature changes: `truncate_str(value: str, max_bytes: int) -> tuple[str, bool]`,
`truncate_tail`, `coerce(value, *, cfg)` and `sanitize_fields(fields, *, cfg)` keep their shapes and
return types. The only additions are the two module constants, listed in `sanitize.__all__`; neither
is re-exported from `log_foundry`.

```
# a pass that reaches either budget
sanitize_fields({"a": y}, cfg=cfg)  ->  ({"a": [..., "<node limit>"]}, True)
sanitize_fields({"a": big}, cfg=cfg) -> ({"a": [<clipped>, "<size limit>"]}, True)
```

## Configuration / Environment

None. The budgets are constants, not `configure()` keys (*Out of Scope*).

## File & Folder Structure

```
src/log_foundry/
├── sanitize.py        # the constants, the bounded reads, the two budgets
└── config.py          # the Config docstring only
tests/
├── test_assembly_budget.py    # new: the alarm tests, exactness, the boundary pass, the reference
│                              #      comparison and the allocation bound
└── test_sanitize_corpus.py    # new rows, silence rows included
docs/
├── invariants.md      # §8
└── architecture.md    # §13
README.md              # Field values are coerced and bounded
```

## Implementation Phases

### Phase 1: Bounded reads (FR-003)

- Write the reference-comparison test and the allocation-bound test first; run the allocation test
  against `main`'s module to watch it redden.
- Bound `truncate_str`, `truncate_tail` and the bytes-like branch of `_dispatch`; carry the reasoning
  in their docstrings, which are the only prose in `src/`.

### Phase 2: The two budgets (FR-001, FR-002)

- Write the four hostile-input tests with the alarm guard; run them once against `main`'s
  `sanitize.py` from a scratch worktree, and keep the reasons for the PR body.
- Add the constants, the placeholders and the per-pass counters to `_Coercer`; the node check ahead of
  the depth check in `value()`; the charge rules in `text()` and `integer()`; the stop in `mapping()`
  and `members()`; the additions to `__all__`. If SPEC-056 has built, replace its definition with an
  import of `MAX_NODES`.

### Phase 3: The remaining tests and the docs (FR-001, FR-002, FR-004)

- The exactness tests, the boundary-event pass, the corpus rows with their silence rows, and the
  mutation run FR-001 names.
- The four documents of FR-004, then every gate: `ruff`, `mypy`, `pytest`, `spec-lint`, `docs-lint`
  and `docstring-lint`.
