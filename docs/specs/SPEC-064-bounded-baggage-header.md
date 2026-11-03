---
type: Spec
id: SPEC-064
title: "Bounded, Total Baggage Header — Rendered Through Assembly's Coercion and Capped by Whole Members"
description: "Renders the outbound baggage header through assembly's bounded coercion and caps it at 8192 bytes by whole members, so current_baggage_header() always returns and its output always parses."
spec_status: In Progress
depends_on: [SPEC-014, SPEC-057]
---

## Overview

`current_baggage_header()` is what a caller puts on an outbound request so the next process continues
the trace. It is public and its docstring says `Raises: None.`, but it renders every baggage value
with `str()` and percent-encodes the result, which is neither bounded nor total. Four failures,
each reproduced on the tree at `71e42dd` (`context.py` is byte-identical to `b282c1d`, where the
first two were first measured, `docs/architecture.md` §12):

- **(a) It never returns.** A value that is a shared-sublist DAG (`y = 0`, then `y = [y] * 16` seven
  times) is walked once per path by `str`, 16^7 times.
- **(b) It raises.** A value whose `__str__` raises makes the call raise into the caller.
- **(c) It raises on text.** `set_baggage(k="\ud800")` makes `quote` raise `UnicodeEncodeError`. The
  inbound route reaches the same state: `continue_trace(baggage="k=\ud800")` stores the parsed value,
  because `parse_baggage_header` passes non-`%` characters through `unquote`, so a raw lone surrogate
  survives into baggage.
- **(d) It raises on a large integer.** `set_baggage(i=10**4300)` makes `str` raise `ValueError`,
  because CPython refuses to render an integer of more than 4,300 digits and the coercer's default
  digit ceiling is the same number, so every integer assembly replaces by a placeholder made the
  unfixed formatter raise.

Event assembly handles all four values without hanging or raising, so the library's two text
producers for one baggage disagree (invariant 6) and the promise that a public call returns
(invariant 1) is broken. A further defect sits beside them: the formatter has no size bound, and one
value at the default 8 KiB ceiling already makes a header over `BAGGAGE_MAX_BYTES` (8192), which the
next log-foundry process's `parse_baggage_header` refuses whole. Formatter and parser are twins.

This spec renders the header through assembly's own coercion, under its own budgets, and caps the
result at the size the parser accepts, dropping whole members. It adds no configuration key and no
public name.

## Scope

### In Scope

- Rendering the baggage header from `sanitize.sanitize_fields(baggage, cfg=...)`, one pass over the
  whole mapping, in both `context.format_baggage_header` and `context.current_baggage_header`
  (FR-001).
- A guard that makes the render total, including the lone-surrogate case reached by both intake
  routes (FR-002).
- A cap of `BAGGAGE_MAX_BYTES` on the emitted header, kept by whole members in mapping order, a
  skip of any member the parser would refuse for its key or whose key cannot be rendered, and a
  throttled `_diag` announcement of what was dropped (FR-003).
- **The guard binds at render, not at intake.** `set_baggage` keeps storing arbitrary objects,
  because the event coerces them at assembly (invariant 8: once, at assembly); coercing at intake
  would change what events carry and would coerce twice. The inbound route (`continue_trace`, then
  `parse_baggage_header`) already yields only `str` values and its total size is capped at
  `BAGGAGE_MAX_BYTES`, so intake needs no new guard. The fence "inbound context is untrusted" is
  honoured at render too, because case (c) arrives that way.

### Out of Scope

- **Editing `sanitize.py`.** The render calls `sanitize_fields` as it stands. A sibling spec,
  SPEC-063, owns the module.
- **The spoofed-scalar routes** (an object that claims `int`, `float` or `str` through `__class__`
  and survives coercion; `docs/architecture.md` §12). FR-002's guard makes the render total against
  an exception from such an object; it does not close the route. An object that spoofs a scalar type
  can still reach `str()` here until SPEC-063 lands, with the same exposure assembly has, and a
  `__str__` that hangs rather than raises is not bounded by this spec.
- **A W3C 64-member cap.** `max_keys` already bounds the member count at 256 by default, and the byte
  cap (FR-003) is what the parser enforces.
- **Changing `set_baggage` or `parse_baggage_header`**, and any new check at intake.
- **A `health()` counter for dropped members.** `health()` is the ledger of events (invariant 2), and a
  baggage member is not an event; adding a counter widens the frozen public surface (invariant 10)
  for a loss the announcement already makes visible. The diagnostic is the announcement.
- **Announcing a member dropped by `max_keys`, or a value clipped by a ceiling.** Both are assembly's
  coercion doing what it does for an event, where the event carries the `truncated` marker and the
  header has no slot for one. FR-003 announces only what its own rules drop: the size cap and an
  empty key.

---

## Functional Requirements

### FR-001: The header is rendered through assembly's bounded coercion

#### Description:

`format_baggage_header(baggage)` makes one call, `sanitize.sanitize_fields(baggage, cfg=...)`, over
the whole mapping, under the node and byte budgets of SPEC-057 and the configured ceilings assembly
uses (`max_keys`, `max_depth`, `max_value_bytes`). The config is read through the live config
(`config._live_config()`, as `model.backfill_baggage` reads it), which holds the same ceilings
`get_config()` returns without copying them. `sanitize.py` is not edited. It is one pass, not one per
member, so the budgets bound the header as a whole as they bound an event.

Each coerced value is then rendered as text: an exact `str` is used as it is, and any other coerced
value is rendered with `str()`. The text is percent-encoded with `quote(..., safe='')` as today. A
plain dict therefore still renders as it does now (`{'a': 1}`), and today's docstring already
promises "arrives as its repr". The keys are the coerced keys.

**The consequence a caller can see: the header renders the same *coerced value* the event carries,
as text with `str()`.** The event carries that value as JSON, so `True` is `true` there and `True`
here; what changes is which value is rendered. Measured against `coerce` on the tree at `71e42dd`:

| Baggage value | Header text before | Header text now |
|---|---|---|
| `datetime(2026, 1, 1)` | `2026-01-01 00:00:00` | `2026-01-01T00:00:00` |
| an `Enum` member `E.A` with value `"a"` | `E.A` | `a` |
| a `Path` | its path | `<unserializable: PosixPath>` |
| `(1, 2)`, `{1, 2}` | `(1, 2)`, `{1, 2}` | `[1, 2]` |
| `b"ab"` | `b'ab'` | `ab` |
| `float("nan")` | `nan` | `<float: nan>` |
| an int too large to render | raises `ValueError` (case d) | `<int: ~N digits>` |
| a string past `max_value_bytes` | the whole string | clipped, ending `…[truncated]`; observable only under a small ceiling, since at the default a clipped value plus its key exceeds FR-003's cap |
| `True`, `None`, `["x"]` | `True`, `None`, `['x']` | unchanged: `True`, `None`, `['x']` (the event carries `true`, `null`, `["x"]`) |
| a lone surrogate | raises (case c) | U+FFFD |
| members past `max_keys` (256) | all emitted | dropped, as assembly drops them |

`sanitize_fields({"a": DAG})` returns in about 0.03 s of CPU on the same tree. The twins this FR
touches: **header render vs event assembly** (both coerce one baggage with one coercer), and
**`current_baggage_header` vs `format_baggage_header`** (the first stays a call of the second on the
live baggage).

**Test harness.** The tests reproduce (a), (b), (c) and (d) and are run once against the unfixed
`context.py` (a scratch worktree off `main`, never by switching this branch, with
`log_foundry.context.__file__` asserted to lie under it) to show each fails for its reason. A test
whose old behaviour is "never returns" must not hang the suite. Measured at `71e42dd`, a
`signal.setitimer` alarm whose handler raises a `BaseException` subclass interrupts `str` of the DAG,
so SPEC-057's alarm harness is usable; the handler must not raise an `Exception`, because FR-002's
guard catches `Exception`. A child process with a wall-clock kill is the alternative, and the
implementation plan chooses between them. Timing bounds are asserted on CPU time
(`time.thread_time()` or `time.process_time()`), and a budget stays generous while the gap between
the budget and the fixed operation stays tight relative to the unfixed cost.

#### Acceptance Criteria:

- [ ] `set_baggage(a=y)` with `y` the shared-sublist DAG, then `current_baggage_header()`, returns
      with CPU time under 1 s, once inside an open span and once with no span open. Under the
      default budgets the coerced DAG renders to roughly 385 KB, so FR-003's cap makes the header
      `""`; with `sanitize._MAX_NODES` patched to 64 the unquoted header is under the cap and contains
      `<node limit>`. Against the unfixed `context.py` the same test is killed by the harness, not by
      an assertion.
- [ ] Each row of the table above is a test with the literal expected header text, not an expression
      computed from `coerce`: for example `set_baggage(d=datetime(2026, 1, 1))` gives
      `d=2026-01-01T00%3A00%3A00`, and `set_baggage(m={"a": 1})` gives `m=%7B%27a%27%3A%201%7D`;
      `True` gives `t=True` and `None` gives `n=None`. A baggage of 300 integer members emits 256 of
      them with the default `max_keys`.
- [ ] The clipped-string row: with `configure(max_value_bytes=16)` and `set_baggage(s="x" * 40)`, the
      header is `s=xx%E2%80%A6%5Btruncated%5D`. The test restores the config afterwards, and against
      the unfixed `context.py` the header carries all 40 characters.
- [ ] (d) `set_baggage(i=10**4300)` gives `i=%3Cint%3A%20~4301%20digits%3E`; against the unfixed
      `context.py` the test fails with `ValueError`.
- [ ] One pass: with `sanitize._MAX_NODES` patched to 4 and a baggage of `a=[1, 2, 3]` then
      `b=[4]`, the header is `a=%5B1%2C%202%2C%203%5D,b=%3Cnode%20limit%3E`, so the second member's
      value is `<node limit>` because the first spent the budget. A per-member pass would render
      `b` whole.
- [ ] Twin: for the DAG baggage with `sanitize._MAX_NODES` patched to 64, the event assembled by
      `info()` in the same context carries `<node limit>` in `fields` and the header carries it too,
      and both calls return.
- [ ] The existing baggage tests pass unchanged, and `set_baggage(request_id="req-123",
      tenant="acme")` still gives `request_id=req-123,tenant=acme` and a comma-bearing string still
      round-trips through `parse_baggage_header`.
- [ ] `context.__all__`, `log_foundry.__all__` and both functions' signatures are unchanged.
- [ ] Serves invariants 1 and 6: the call returns on a hostile value, and the header renders the same
      coerced value the event carries.

### FR-002: The render is total, including a lone surrogate from either intake route

#### Description:

Rendering one member's coerced value can still raise. After coercion a value is a scalar, a list or a
dict, so `str()` of it cannot raise except for an object that spoofs a scalar type and survives
coercion, which is SPEC-063's open item. The render therefore guards the **whole per-member render**:
`str()` and both `quote` calls, key and value. A spoof whose `__str__` returns a `str` subclass with a
raising `encode` gets past `str()` and then makes `quote` raise, so a guard around `str()` alone is
not enough. If rendering one member's value raises an `Exception`, that value becomes
`<unserializable: TypeName>`, and the other members are unaffected. The type is named by
`sanitize._type_name(value)`, which returns `"?"` when even the name cannot be read, never by
`type(value).__name__` (a spoofed-`__class__` object whose metaclass has a raising `__name__` property
would raise inside the handler) and never by `repr`. If the key cannot be rendered, or the
placeholder itself cannot be encoded, the member is skipped and counted in FR-003's drop total, like
an empty key. A coerced key is always a `str`, so the key route is defensive. The guard catches
`Exception`, never `BaseException`, so a `KeyboardInterrupt` or the test harness's alarm reaches the
caller (invariant 1).
`current_baggage_header()` keeps `Raises: None.` truthfully. The residual is stated under Out of
Scope: it is not closed here.

Cases (b) and (c) need no guard of their own: an object whose `__str__` raises is not a type
coercion recognises, so it becomes `<unserializable: T>` before any `str()` runs, and a lone
surrogate becomes U+FFFD in coercion, so `quote` never sees it. The surrogate arrives by two
routes, the twins **`set_baggage` intake vs `continue_trace` intake**, and both are exercised. Keys
are coerced the same way, so a surrogate in a key is covered by the same pass. Neither intake is
changed: `get_baggage()` still returns the stored value, surrogate included.

#### Acceptance Criteria:

- [ ] (b) `set_baggage(b=Boom())` with a `__str__` that raises `RuntimeError("secret")`, then
      `current_baggage_header()`, returns `b=%3Cunserializable%3A%20Boom%3E`, and the text `secret`
      appears nowhere in it. Against the unfixed `context.py` the same test fails with that
      `RuntimeError`.
- [ ] (c) `set_baggage(k="\ud800")` gives `k=%EF%BF%BD`; against the unfixed `context.py` the test
      fails with `UnicodeEncodeError`. A lone surrogate in a key (`set_baggage(**{"\ud800": "v"})`)
      gives `%EF%BF%BD=v`.
- [ ] (c), inbound: `continue_trace(baggage="k=\ud800")` followed by `current_baggage_header()`
      gives `k=%EF%BF%BD` and does not raise, and `get_baggage()["k"]` is still `"\ud800"`.
- [ ] The guard itself: a value that survives coercion and raises from `str()` (an object that
      returns itself from coercion through a spoofed `__class__`, built the way SPEC-063's routes
      are) yields `<unserializable: TypeName>` for that member while a sibling member renders
      normally. The test is mutation-checked once by removing the guard, which reddens it.
- [ ] Measured escape 1: an object that survives coercion and whose `__str__` returns a `str`
      subclass with a raising `encode` yields `<unserializable: TypeName>`, a sibling member renders,
      and nothing raises. A guard around `str()` alone fails it.
- [ ] Measured escape 2: an object with a spoofed `__class__` whose metaclass has a raising
      `__name__` property and whose `__str__` raises yields `v=%3Cunserializable%3A%20%3F%3E`, a
      sibling member renders, and nothing raises.
- [ ] A key that cannot be rendered (`quote` patched to raise for one key, since a coerced key is
      always a `str`) skips that member, the siblings on either side render, the header parses, and
      the skip is in FR-003's total.
- [ ] A `BaseException` subclass raised from that `__str__` propagates out of
      `current_baggage_header()`; an `Exception` does not.
- [ ] The placeholder in every case above names the type and contains neither the exception message
      nor a `repr` of the value.
- [ ] Serves invariants 1, 6 and 11: the public call is total on both intake routes, the render and
      the event agree on the same surrogate, and nothing but a type name leaves the library about a
      value.

### FR-003: The header is capped at `BAGGAGE_MAX_BYTES` by whole members, an empty key is skipped, and a drop is announced

#### Description:

The emitted header is at most `BAGGAGE_MAX_BYTES` (8192) bytes. After FR-001, members are rendered
in the coerced mapping's order and joined with `,`; a member is kept while the header, comma
included, still fits, and the first member that does not fit ends the header, so it and every member
after it are dropped even where a later one is small. A partial member is never emitted (W3C Baggage
permits dropping whole list-members past 8192 bytes). One member that alone exceeds the cap yields
`""`. The length is that of the percent-encoded text, which is ASCII, so characters are bytes.

A member whose percent-encoded key is empty is **skipped**: it is not emitted, and unlike a member
past the cap it does not end the header, so the members after it are still considered. Today
`set_baggage(**{"": 1})` renders `=1`, which `parse_baggage_header` refuses whole because it requires
a non-empty key, so one such member costs the next process every other member. The emitted keys are
therefore the coerced mapping's keys, in order, minus the skipped empty key, taken up to the first
member that does not fit.

Why: today a single value at the default `max_value_bytes` (8192) already makes a header over the
cap, and the next process's `parse_baggage_header` returns `None` for the whole header, losing
correlating fields it could have kept; an empty key does the same. Formatter and parser are twins
(invariant 6): everything the formatter emits must be something the parser accepts.

A drop is announced through `_diag.lost("baggage member", total, detail)` on stderr, written the first
time and then at every multiple of `_diag.WARN_EVERY`, like the library's other per-event sites. The
members counted are those dropped by the cap, a skipped empty-key member and a member skipped for a
key that cannot be rendered. The total is drawn from a module-level `itertools.count(1)`: each
dropped member takes one `next()`, which is atomic under the GIL, so it needs no lock. A bare `+=`
could lose an increment, and then a line's count would not reproduce from the ledger (invariant 11);
a new lock in `context.py` would be a fork hazard (invariant 12). A call writes the line if any
value it drew is 1 or a multiple of `WARN_EVERY`, and carries the last value it drew as the total,
which the detail says is a running total. Each drawn value is unique, so each crossing is announced
by exactly one call, even under concurrent calls. The line names a count only, never a key or a
value (invariant 11). The counter is process-level diagnostic state: after a fork a child holds its
own copy of the count, which only shifts which line it writes, so nothing is stranded (invariant 12).
The write is `_diag.lost`, which cannot raise on a stream fault. `tests/test_diag.py` walks every module under
`src/` for bare stderr writes and for a `repr` in a `_diag` call, so `context.py`'s new call is
covered by it without a hand-kept entry; that file also asserts the call-site floor, which the new
site only raises.

#### Acceptance Criteria:

- [ ] Whole members, in order: a baggage of members that together exceed the cap emits a header of
      at most 8192 bytes, every member of which is whole, and the emitted keys are exactly the coerced
      mapping's keys in order, minus any empty key, up to the first member that does not fit.
- [ ] Exact boundary: baggage whose header is exactly 8192 bytes is emitted whole with no
      diagnostic; the same baggage with one byte added to its last value drops that member and
      writes the diagnostic.
- [ ] Prefix, not packing: with members `a` (fits), `b` (does not fit) and `c` (small enough to
      fit), the header holds `a` only.
- [ ] `set_baggage(k="x" * 8192)` gives `""` and writes the diagnostic. A value of 2,000 `€`
      (6,000 UTF-8 bytes, under the default ceiling, but 18,000 characters once percent-encoded) is
      dropped because the cap is on the encoded length.
- [ ] Round-trip, over a corpus covering the cases above and a baggage of mixed members: every
      non-empty header emitted parses with `parse_baggage_header` to a non-`None` mapping whose keys
      are the coerced mapping's keys, in order, minus any empty key, up to the first member that
      does not fit. Against the unfixed `context.py`, the 8192-byte value yields a header the
      parser refuses.
- [ ] Empty key: `set_baggage(a="1", **{"": "x"}, b="2")` gives `a=1,b=2`, which parses to
      `{"a": "1", "b": "2"}`, and the diagnostic is written with count 1. An empty-key member ahead of
      an over-cap one still lets the members before the over-cap one through, and both skips are in
      the running total. Against the unfixed `context.py` the header is `a=1,=x,b=2` and the parser
      refuses it.
- [ ] The diagnostic: the first drop writes exactly one line beginning `log-foundry: lost 1 baggage
      member(s)`; over 1,001 calls that each drop one member, lines are written at running totals 1
      and 1,000 only, and each carries its total. A baggage whose dropped member has key
      `secret-key` and value `secret-value` produces a stderr line containing neither. Planting the
      key into the detail reddens the second test.
- [ ] The concurrent total: N threads (8) each make 500 calls that drop one member, released
      together. The next value drawn afterwards is the number of drops plus one, the lines written
      number exactly the crossings of the sequence (totals 1, 1000, 2000, 3000 and 4000, five lines),
      and no two lines carry the same total.
- [ ] Twin: the inbound route into the outbound cap. With `h = "a=1,b=" + "(" * 8186`, which is exactly
      8192 bytes and so accepted by `parse_baggage_header`, `continue_trace(baggage=h)` followed by
      `current_baggage_header()` gives exactly `a=1`: `b` is dropped whole because `quote` encodes
      each `(` as three bytes. The unfixed `context.py` emits about 24 KB there, which the parser
      refuses.
- [ ] Serves invariants 6 and 11: the formatter never emits what the parser refuses, for size or for
      an empty key, and the announcement carries a count and no caller data.

---

## Data Model

```
# src/log_foundry/context.py            (BAGGAGE_MAX_BYTES = 8192 is unchanged)
_dropped_members: itertools.count[int]  # itertools.count(1): one next() per member dropped by the cap
                                        # or skipped (empty or unrenderable key); no lock, atomic under
                                        # the GIL; diagnostic throttle state only (FR-003)

# src/log_foundry/sanitize.py           (not edited)
sanitize_fields(fields: Mapping[str, object], *, cfg: Config) -> tuple[dict[str, object], bool]
```

## API / Interface Contract

No signature changes and no public name added.

```
current_baggage_header() -> str                      # Raises: None. now true on every input
format_baggage_header(baggage: dict[str, object]) -> str

# the render, per member of sanitize_fields(baggage, cfg=...)[0]
k = quote(key, safe='')                               # Exception or k == "" -> member skipped, counted
try:                                                  # the guard wraps str() AND quote()
    text = value if type(value) is str else str(value)
    v = quote(text, safe='')
except Exception:
    v = quote(f"<unserializable: {sanitize._type_name(value)}>", safe='')   # "?" if unreadable
member = f"{k}={v}"
# members joined by ",", kept while len(header) <= BAGGAGE_MAX_BYTES; first misfit ends the header
```

The docstrings of both functions change: `current_baggage_header`'s no longer says non-string values
are serialized with `str()` and says instead that they are coerced as an event's fields are, and
both say the header is capped by whole members.

## Configuration / Environment

None. The render reads `max_keys`, `max_depth` and `max_value_bytes` from the live config and the
private budgets in `sanitize`; no key is added.

## File & Folder Structure

```
src/log_foundry/
└── context.py                  # the render, the guard, the cap, the diagnostic
tests/
├── test_baggage_header.py      # new: (a)-(d) on both intake routes, the cap, the diagnostic
└── test_diag.py                # unchanged unless the plan finds a hand-kept site list to extend
docs/
├── architecture.md             # §12 item moved to Resolved
├── decisions/trace-model/      # one new decision file, with its dated History
└── release-notes/v1.1.0.md     # one bullet
```

## Implementation Phases

### Phase 1: Tests that reproduce (a), (b), (c) and (d) (FR-001, FR-002)

- Write the four reproductions with the harness the plan chooses; run them once against the unfixed
  `context.py` from a scratch worktree and keep the reasons each fails for the PR body.

### Phase 2: The render, the guard and the cap (FR-001, FR-002, FR-003)

- Route `format_baggage_header` through `sanitize_fields`, add the per-member guard, the cap by
  whole members and the throttled `_diag.lost`; settle the import placement so `context` imports no
  cycle. Change the two docstrings.
- Write the table, boundary, prefix, round-trip and diagnostic tests; mutation-check the guard's
  `try` and the key-in-detail plant once each.

### Phase 3: Completion (docs)

- Completion-ritual work (`docs/process/completion-ritual.md`), not requirements: the §12 item in
  `docs/architecture.md` moves to *Resolved*; one decision file in `docs/decisions/trace-model/`
  records that the baggage header renders through assembly's coercion, bound at render not intake,
  capped at `BAGGAGE_MAX_BYTES` by whole members; one bullet in `docs/release-notes/v1.1.0.md` states
  the visible change (header values render the coerced value the event carries, and an over-cap header
  drops whole members and announces it); regenerate the indexes; prune `CLAUDE.md`'s `## Specs`.
- Then every gate: `ruff`, `mypy`, `pytest`, `spec-lint`, `docs-lint` and `docstring-lint`.
