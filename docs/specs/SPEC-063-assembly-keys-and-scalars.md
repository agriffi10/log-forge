---
type: Spec
id: SPEC-063
title: "Assembly Keys and Scalars — Container Keys Under the Pass Budgets, and No Type Routing the Caller's Object Controls"
description: "Renders a container mapping key from its budgeted coercion, stops routing on a type test a caller's object can spoof, and closes three sibling faults that cost an event or a span's events."
spec_status: In Progress
depends_on: [SPEC-055, SPEC-057]
arc: coercer-and-invariant-8
arc_order: 3
---

## Overview

`build_event` runs on the caller's thread on both delivery paths. SPEC-057 bounded what a coercion
pass does to a *value* — nodes visited, bytes charged, reads held to the ceiling — and recorded two
things it left open in `architecture.md` §12, both pre-existing and not regressed by it. This spec
closes both and the siblings found by sweeping `api.py`, `model.py`, `_diag.py` and `sanitize.py`
for the same shape: a caller's object or argument consulted outside every guard. Everything below was
measured at `71e42dd` with Python 3.13.

- **A container key is rendered before any budget applies.** `_Coercer._key` renders a key that is
  not a `str`, `int` or `float` with `str(key)`, the builtin `repr`, which recurses through a shared
  member once per path. With `f = frozenset({f, (f, i)})` applied 14, 16, 18 and 20 times as the
  single key of `sanitize_fields({f: 1})`, one call costs 0.008, 0.034, 0.142 and 0.572 s of CPU,
  doubling per level (§12 records 2.61 s at level 22 at `b282c1d`), so forty levels never return. A
  node count alone would not close it: a tuple key of 64 references to one 1 MiB string renders
  64 MiB (0.079 s of CPU) before the byte budget turns it into `<size limit>`, and 64 references to
  that tuple would be 4 GiB. A `bytes` key is the same shape without sharing: a 64 MiB `b"\x00"` key
  is rendered to 256 MiB (a `tracemalloc` peak, 0.1 to 0.2 s of CPU) and only then becomes
  `<size limit>`.
- **A caller's object can pick the type rule that returns it.** `isinstance` consults `__class__`
  and `kind in _PLAIN_SCALARS` consults the type's `__hash__` and `__eq__`, i.e. the metaclass. Each
  of these, as the value of a field, reaches the assembled event as the object itself, and
  `json.dumps(event, allow_nan=False)` then raises `TypeError` in a sink: an object whose
  `__class__` property returns `bool`; one whose `__class__` returns `float` with `__float__`
  returning `1.0` (it decides `isfinite()` in `real()`); an instance of a class whose metaclass
  defines `__hash__` as `hash(int)` and `__eq__` as true; and an `Enum` member whose `.value` is the
  `float` impostor, a route §12 did not name (the Enum branch's `isinstance(member, float)` and
  `type(member) in _PLAIN_SCALARS`). A genuine `float` or `int` subclass value also reaches the
  event as the subclass instance: harmless to `json`, which uses the base repr, but the event holds
  a caller's type.
- **A caller's argument's truthiness can raise into the application.** `info("m", fields=X())`
  where `X` is a `Mapping` whose `__bool__` or `__len__` raises propagates `RuntimeError` to the
  caller, on the orphan path and inside a `@trace`d function, where the decorator then records
  `status=error` for an exception the caller never raised: `_merge`'s `if not fields:` sits outside
  its own `try`. `info("m", echo=Y())` with a raising `Y.__bool__` propagates likewise: `_log`'s
  `if echo and event is not None:` sits outside the echo guard.
- **A hostile exception class loses its span's events and its announcement, and a colliding key
  loses its event.** A `@trace`d function raising an exception whose class's metaclass defines
  `__name__` as a property that raises, or that returns the integer `42`, or whose `__module__`
  property raises, has its span's events lost ("absorbed a failure while closing a span
  (RuntimeError|TypeError); the span's events were lost"), because `_error_fields` reads
  `type(exc).__name__` and `__module__` through the metaclass and `truncate_str` refuses a non-`str`.
  The user's exception is still re-raised unchanged, so the first invariant holds, but the eighth's
  "for any value… the call returns" is not kept for the event. `_diag.absorbed` reads
  `type(exc).__name__` through the metaclass too, inside its own `try`, so with a raising `__name__`
  the announcement is swallowed: measured, an orphan loss is counted with an empty stderr, which
  breaks the second invariant's "counted and announced", and the `fields=` and `echo` absorbs FR-003
  adds receive caller exceptions. And a hostile key loses its event. With `K.__hash__` returning
  `hash("svc")` and `K.__eq__` raising, `build_event`'s
  `{**cfg.defaults, **span.defaults, **baggage, **fields}` compares the keys, against SPEC-055's
  promise that a hostile key costs only itself. Two shapes are reachable, each probed at `71e42dd`: a
  hostile key in an earlier source, `configure(defaults={K(): 1})` or `@trace(defaults={K(): 1})`,
  followed by `info("m", svc=3)`; and a hostile per-call key, `info("m", fields={K(): 2})`, against
  `configure(defaults={"svc": 1})`. Each loses the event on the orphan path ("emitting an orphan
  log") and the in-span path ("building an in-span log"), while the span's own boundary events
  survive. The shape `fields={K(): 2, "svc": 3}` cannot be reached: a dict literal raises at
  construction, a dict cannot be built holding both (the second insertion raises), and a caller
  `Mapping` yielding both is dropped whole by `_merge`, announced "merging the fields= argument".

The remedy is one rule per class. A container key is coerced by the pass exactly as a value would
be, and its text is the JSON of the coerced form; a `bytes` key is priced before it is rendered. A route that returns the caller's object, or lets
its method decide a measurement, admits it only by an exact-type identity test or by first
converting it through an unbound builtin accessor, and a type's name is read from where the type
stores it. A truthiness test on a caller's argument sits inside the guard that already absorbs its
siblings, and a merge that cannot compare two keys drops the lower-precedence one. No configuration
key and no public name is added.

## Scope

### In Scope

- A container mapping key coerced through the pass at its value depth, so it spends nodes and bytes,
  obeys the depth, key-count and cycle rules, and is written as the JSON text of its coerced form;
  a `bytes` or `bytearray` key priced before it is rendered (FR-001).
- Every `_dispatch`, Enum and key route that returns the caller's object, or lets its method decide
  a measurement, admitting it by exact type or an unbound conversion; `_type_name` reading the
  stored type name as an exact `str` (FR-002).
- The `fields=` truthiness test inside `_merge`'s absorb and the `echo` test inside the echo guard
  (FR-003).
- The `error` sub-document, the stack fallback and `_diag.absorbed` reading a type's stored name
  (and the `error` sub-document its stored module), and the defaults/baggage/fields merge dropping
  only the lower-precedence key of a pair that cannot be compared (FR-004).
- The record: both §12 items resolved, the eighth invariant's text, the superseded "keys are not
  counted" claim at every site that states it, a decision, and the pending release notes (FR-005).

### Out of Scope

- **A `configure()` knob** for any of this, and any public name.
- **The cost of a caller's own `__str__`, `__repr__`, `__iter__` or `__hash__`.** SPEC-057's rule,
  unchanged: the budgets bound how often the pass calls them, not what one call costs. An `int`
  subclass key's own `__str__` is the example that stays.
- **`context.py`'s baggage header** (`format_baggage_header` renders with `str()` and is neither
  bounded nor total): SPEC-064 owns it.
- **`config.py`'s ceilings** (`configure()` accepting a ceiling that is not an `int`): SPEC-065.
- **A non-`str` `message`.** Already absorbed and announced (SPEC-055 FR-001) and pinned by
  `tests/test_api.py`.
- **A `fields=` argument whose keys cannot hash**, or whose key collides with a keyword in `_merge`'s
  own `{**fields, **kv}` (measured at `71e42dd`: `fields={K(): 2}` with `svc=3` loses the argument,
  announced "merging the fields= argument", and keeps the event), or a caller `Mapping` that yields
  two keys which cannot be compared. `_merge` already drops that argument whole and says so; this
  spec leaves it.
- **`@trace(name=X())` where `X`'s `__class__` claims `str`.** It passes the `isinstance` test at
  `decorator.py:849`, is not refused at decoration, and the calls then run untraced, announced. That
  is `decorator.py` and the thirteenth invariant's decoration-time refusal, not this spec's file.
- **`_otel.py`.** It maps an already assembled event, which FR-002 now guarantees holds only exact
  types, and keeps its own `_type_name` (`tests/test_otel_mapping.py` pins its `?`).
- **Keys of any other type** (`Decimal`, `None`, `bool`, an object, a `memoryview`): they keep
  today's rendering. A `bytes` or `bytearray` key keeps it too, once priced (FR-001).

### Accepted Limits

Stated here rather than left to be found.

- **A container key's text is the JSON of its coerced form, not Python's `repr`.** A tuple and a
  list key with equal members give one text. A `list` cannot be a dict key, so only a caller
  `Mapping` can yield the collision; the later key wins, as any two keys that render alike do.
- **A container key's members spend the pass's node and byte budgets.** A huge key therefore
  sacrifices the entries after it in walk order, exactly as a huge value does (SPEC-057).
- **A container key that is not a builtin subclass** — a custom hashable `Sequence` — keeps `str()`,
  which is the caller's code (*Out of Scope*).
- **The rebuilt merge changes key order.** On the marked slow path, entered only when the unpack
  raised, the merged mapping is in highest-source-first order (per-call fields, baggage, span
  defaults, config defaults) rather than the unpack's first-insertion order, and each source's keys
  keep that source's own order. Precedence is exactly the unpack's: the higher-precedence key of a
  colliding pair is always present first, so the key that cannot be tested, and is dropped, is
  always the lower-precedence one, and a per-call field is never dropped by a hostile default.
  Precedence holds for values, not for the key *object* kept when keys are equal but distinct, such
  as `1`, `1.0` and `True`: the unpack keeps the lowest source's key object with the highest
  source's value, while the rebuild keeps the highest source's key object, so on the marked slow
  path such a key can render `"1.0"` where the unpack wrote `"1"`.
- **An object key keeps `str()`, which for a default object is its `repr`.** The no-repr rule holds
  inside a container key and for a value; a key that is an arbitrary object renders as its own
  `str()`, the caller's code (*Out of Scope*), and the eighth invariant's page records that
  exception.
- **A transform's types-only coercion** (`_coerce_unbounded`, outside both budgets, SPEC-058) renders
  a container key by the same rule, and its accepted cost for a DAG value extends to a DAG key.
- **The key text is not charged a second time.** The members are charged as values are; the JSON
  punctuation and escapes are bounded by the node count and by six times the charged text, like the
  serialised event (SPEC-057, *Accepted Limits*), and the text is clipped to `max_value_bytes`.

---

## Functional Requirements

### FR-001: A container key is rendered from its coerced form, and a bytes key is priced first

#### Description:

A mapping key whose **real** type — `type(key)`, never `__class__` — is a subclass of `tuple`,
`list`, `dict`, `set` or `frozenset` (a namedtuple included) and not a subclass of `Enum` is coerced
through the pass exactly as a
value at the entry's value depth (`depth + 1`) would be. It therefore spends one node for itself and
one per member per path from the node budget; its members' text is charged to the byte budget as
values' is; and `max_depth`, `max_keys`, the cycle check and the per-type rules all apply. Its text
is `json.dumps(coerced, ensure_ascii=False)` with the default separators, clipped to
`max_value_bytes` with the truncation marker, and `truncated` is set when it is clipped. So `(1, 2)`
becomes the key `"[1, 2]"`, `("a",)` becomes `'["a"]'`, `frozenset({1})` becomes `"[1]"`, a nested
container the same way, and a member past `max_depth` appears as `"<depth limit>"` inside the text.
A key whose coerced form is itself a string — `<depth limit>` or `<circular>` — reads as that
string in quotes, so it cannot collide with a plain `str` key of the same text. Only the stop and
failure placeholders — `<node limit>`, `<size limit>` and `<unserializable key: T>` — are written
bare, because they replace the key whole.

The key is charged where the members are: the JSON punctuation and escapes are not charged again.
This changes the text of every container key, which is a caller-visible change and deliberate:
distinct keys stay distinct, and a member object's `repr` is never written, so arch §6's no-repr
rule now holds inside container keys too (today `str(key)` of a tuple writes each member's `repr`).
An object that is itself the key keeps `str()`, which for a default object is its `repr`
(*Accepted Limits*). It changes
SPEC-057 FR-001's statement that "mapping keys are not values and are not counted": a container key
and its members are counted.

**A stop inside a key.** A container key whose coercion stops the pass — the node budget, or the byte
budget — leaves its entry's value unread, and the entry is written as the stop placeholder for both
key and value: `"<node limit>": "<node limit>"` or `"<size limit>": "<size limit>"`. SPEC-057 FR-002
already does this for a key that crosses the byte budget, with `mapping()` writing `_SIZE_LIMIT`
there by name; this FR writes whichever placeholder stopped the pass. A key that completes with the
budget spent exactly leaves the value to be read: it becomes `<node limit>` or `<size limit>` as any
next value would.

**A failure in the key.** A container key whose own iteration raises becomes the
`<unserializable key: T>` placeholder, marked, costing no sibling (SPEC-055 FR-004). The key path
must see that exception, which `_Coercer.value`'s guard would turn into the value placeholder
`<unserializable: T>`. The nodes and bytes the key spent before it failed stay spent. An element of
the key that cannot be coerced is its `<unserializable: T>` placeholder inside the text, as it would
be as a value.

**A bytes-like key is priced before it is rendered.** A key whose real type is a subclass of
`bytes` or `bytearray` is rendered by `str(key)`, whose repr is up to four characters per byte, so
it costs its repr before the budget sees it (*Overview*). Its length is read
through the unbound `bytes.__len__` or `bytearray.__len__`, a lower bound of its repr, which the
caller's `__len__` cannot divert. When the pass's charge plus that length would cross the byte
budget, the key becomes `<size limit>` unrendered and the pass halts, as for an integer key
(SPEC-057 FR-002); otherwise it is rendered and charged as today.

**An `Enum` member is not a container key.** A key whose real type subclasses `Enum` keeps `str()`,
like `Color(int, Enum)`: a `(tuple, Enum)` mixin member `TE.A` stays `"TE.A"`, as the value route
sends an `Enum` to its own branch ahead of the sequence branch.

**What does not change.** Keys of any other type keep today's rendering, an object that only claims
through `__class__` to be a tuple is not a container key, and the per-key order SPEC-057 fixed —
key rendered and charged before the value is visited — holds. `_coerce_unbounded` (SPEC-058) renders
container keys by the same rule, outside both budgets.

**Tests that move.** Four sites pin the old `repr` text and are re-derived under the new rule.
Three are in `tests/test_assembly_budget.py`:

- The tuple-key probe of `test_produced_text_and_integers_are_charged_and_placeholders_are_not`
  (around lines 679–693). At its `max_depth=2` the tuple key sits at value depth 2 and is
  `"<depth limit>"` whole, one node and no bytes, so the probe no longer exercises a produced-text
  charge; the test is re-derived with another key whose `str()` is long, and this spec claims no
  number for it.
- The three-deep `(s, i)`-keyed nest (around line 857). Each key now charges its clipped member
  (8,192) plus its integer's estimate and every key's text clips to
  `'["' + "x" * 8176 + TRUNCATION_MARKER` and collapses to one, so the shape
  `{K: {K: {K: 0, "<size limit>": "<size limit>"}}}` is unchanged; by the charge rules the crossing
  key is the 247th (`k = 246`) of the innermost mapping on its 8th visit.
- The `_PRODUCED_SITES` rows `tuple-key` and `non-ascii-key-unclipped` (around lines 901–971). Their
  charge becomes the clipped member's (14 and 80), their written text becomes the marker and
  `'["' + "😀" * 20 + '"]'`, and their `str(...)` premise rows go.

The fourth is the `mixed_keys` row of `tests/test_sanitize_corpus.py` (around lines 193–205), whose
`"(1, 2)": "b"` becomes `"[1, 2]": "b"`. The implementer searches `tests/` for any other pin on a
container key's old text and re-derives it the same way.

#### Acceptance Criteria:

- [ ] `sanitize_fields` writes these keys, each unmarked: `(1, 2)` as `"[1, 2]"`, `("a",)` as
      `'["a"]'`, `frozenset({1})` as `"[1]"`, `((1, 2), (3,))` as `"[[1, 2], [3]]"`, a namedtuple
      `P(x=1, y=2)` as `"[1, 2]"`, `(None, True, 1.5, "é")` as `'[null, true, 1.5, "é"]'`, `()` as
      `"[]"`, and a `dict` subclass key yielded by a caller `Mapping` as `'{"a": 1}'`. `(1, "2")`
      and `(1, 2)` give distinct keys. A `(tuple, Enum)` mixin member `TE.A` as a key stays
      `"TE.A"` (silence: passes at `71e42dd`).
- [ ] `("x" * 100,)` at `max_value_bytes=40` gives the key `'["' + "x" * 24 + TRUNCATION_MARKER`
      with `truncated` set; `tuple(range(300))` at the default `max_keys` gives a key whose
      `json.loads` is `list(range(256))`, marked.
- [ ] A tuple key holding an object whose `__repr__` returns `"SECRET"` is written
      `'["<unserializable: T>"]'` naming the object's type, and `"SECRET"` appears nowhere in the
      event; at `71e42dd` the key is its `repr` text.
- [ ] At `max_depth=2` the key `(1,)` of a mapping at depth 0 is `'["<depth limit>"]'`, marked; at
      `max_depth=1` it is `'"<depth limit>"'`, marked. A caller `Mapping` yielding a `list`-subclass
      key that holds the mapping itself gives `'["<circular>"]'`.
- [ ] With `_MAX_NODES` patched and `{"a": {(1, 2): 0}}` (the inner dict is node 1, the key tuple 2,
      its members 3 and 4, the value 5): at 5 the result is `{"a": {"[1, 2]": 0}}` unmarked; at 4
      it is `{"a": {"[1, 2]": "<node limit>"}}`, marked; at 3 and at 1 it is
      `{"a": {"<node limit>": "<node limit>"}}`, marked.
- [ ] With `_MAX_RENDERED_BYTES` patched to 10: `{("x" * 9,): "y"}` is written whole and unmarked
      (the 13-character key text is not charged again); `{("x" * 10,): "y"}` gives
      `{'["xxxxxxxxxx"]': "<size limit>"}`, marked; and `{("x" * 11,): None}` gives
      `{"<size limit>": "<size limit>"}` alone, marked, its value unread.
- [ ] A caller `Mapping` yielding a `list` subclass whose `__iter__` raises, then a sibling, gives
      `<unserializable key: T>` for the key with the sibling kept, marked, on both delivery paths.
- [ ] A key object whose `__class__` property returns `tuple`, with `__str__` returning `"S"` and no
      usable `__iter__`, is written `"S"` (silence: passes at `71e42dd`). `b"ab"`, `None`, `True`, `Decimal`, a float and
      an `int` key render exactly as at `71e42dd` (silence).
- [ ] With `_MAX_RENDERED_BYTES` patched to 100, a `bytes` subclass key of 101 zero bytes whose
      `__str__` raises and whose `__len__` returns 0 gives `{"<size limit>": "<size limit>"}`,
      marked: it was priced through the unbound length and never rendered (a rendered key is
      `<unserializable key: T>` at `71e42dd`). It is `__str__` that raises, not `__repr__`, because
      `str()` of a `bytes` or `bytearray` subclass calls the C repr directly and never the
      subclass's `__repr__`; a raising `__repr__` row gives the same result with or without the
      pricing and catches nothing. A plain `b"\x00" * (64 << 20)` key (allocated before tracing)
      returns in under 2 s of `process_time()` with a `tracemalloc` peak under 8 MiB and the same
      result; at `71e42dd` its peak is 256 MiB. A `bytearray` key is held to the same two results,
      as a `bytearray` subclass that defines `__hash__` (a plain `bytearray` is unhashable and
      cannot be a dict key) or as a key yielded by a caller `Mapping`.
- [ ] `_coerce_unbounded` renders `{"fields": {(1, 2): 3}}` with the key `"[1, 2]"`, and a key of
      `("x" * 100_000,)` whole, unclipped.
- [ ] `f = frozenset()`, then `f = frozenset({f, (f, i)})` applied 40 times, as the single key of a
      field, returns on both delivery paths (inside a `@trace` and with no span open) in under 2 s
      of `time.process_time()`, with `truncated` set; the test's failure messages never render the
      key. It is alarm-guarded on SPEC-057 FR-001's terms and run once against `71e42dd`'s
      `sanitize.py` from a scratch worktree, where it fails by the alarm. At the default
      `max_depth` of 8 the depth limit cuts this key before the node budget does, so the row
      exercises the depth rule through a key, not the budget.
- [ ] The same key with `configure(max_depth=64)`, which is deep enough that the node budget is what
      stops the walk, returns on both delivery paths in under 2 s of `process_time()` as
      `{"<node limit>": "<node limit>"}`, marked, and is alarm-guarded and run against `71e42dd`'s
      `sanitize.py` on the same terms.
- [ ] A tuple key of 64 references to one 1 MiB string (allocated before tracing starts) returns in
      under 2 s of `process_time()`, with a `tracemalloc` peak under 8 MiB, and its text is
      `'["' + "x" * 8176 + TRUNCATION_MARKER`, marked; at `71e42dd` the peak is above 64 MiB. A tuple
      key of 64 references to that tuple, driven through `_Coercer.mapping` directly, returns in
      under 2 s with `{"<size limit>": "<size limit>"}`, marked, and the pass's charge equals
      `_MAX_RENDERED_BYTES` (2,048 leaves of 8,192 bytes); this shape is not run against `71e42dd`,
      where it allocates 4 GiB.
- [ ] The four sites named under *Tests that move* are re-derived and pass, and the search of
      `tests/` for any other pin on a container key's old text finds none left.
- [ ] Planted out once each — the real-type test, the `Enum` exclusion, the key's node and byte
      spend, the stop placeholder generalisation, the clip, the key-specific catch, and the
      bytes-like pricing — a named test reddens,
      each built on a shape just over a budget or alarm-guarded; the PR body records each guard and
      the test that caught it.
- [ ] Serves invariants 1, 6 and 8: the criteria that build an event run on both delivery paths,
      the hang is a call that fails to return, and `truncated` marks every cut.

### FR-002: No route returns a caller's object that a spoofable test admitted

#### Description:

`isinstance` consults `__class__`; `kind in _PLAIN_SCALARS`, a `frozenset` of types, consults the
type's `__hash__` and `__eq__`. A caller's object controls both. **The rule:** a route that returns
the caller's object, or lets the object's own method decide a measurement, admits it only by an
exact-type identity test (`type(v) is T`) or by first converting it through an unbound builtin
accessor — `int.__int__` or `float.__float__`, which return an exact `int` or `float` for a genuine
subclass and raise `TypeError` for an impostor (verified at `71e42dd`). An impostor then becomes the
`<unserializable: T>` placeholder naming the type of the node being coerced, its **real** type.

Concretely: `kind in _PLAIN_SCALARS` becomes `kind is bool` (`int` is already handled exactly above
it, and `_PLAIN_SCALARS` goes); the `isinstance(value, bool)` branch is removed, since `bool` cannot
be subclassed and only impostors reach it; a genuine `int` subclass is converted to an exact `int`
before it is bounded and charged; and every route into the float rule — the subclass branch, the
Enum member branch and the float key branch — converts through `float.__float__` unless the type is
exactly `float`. The Enum member routes follow the same rules: `type(member) is bool` replaces
`type(member) in _PLAIN_SCALARS`. A member that claims a type and is none falls to the enum's own
fallback text (`str(value)`, library-produced), unless it claims `float` or `str`: the conversion
or `text()`'s unbound read raises, and the placeholder names the Enum class, because the Enum member
is the node. The `str` claim is existing behaviour. A genuine
`int` subclass *key* keeps its own `str()`, so `class Color(int, Enum)` as a key still reads
`Color.RED`; a genuine `float` subclass key renders as its exact float.

**Where `isinstance` stays, on purpose.** The routes that render through the object's own methods
into library-produced text — `mapping()` and `members()` iteration, `isoformat()`, `str()` of a
`UUID` or `Decimal`, the enum fallback `str()` — keep `isinstance`. What they write is the library's
own text, always an exact `str`, `dict` or `list`, and transparent proxies (`wrapt.ObjectProxy`,
Django's `SimpleLazyObject`) set `__class__` legitimately and render correctly through them today;
switching those to `type()` would turn a proxied `dict` or `datetime` into a placeholder. A proxy of
a scalar never worked: a proxied `int` is already a placeholder (SPEC-057's unbound reads), and a
proxied `float` or `bool` reached the event as the proxy.

**The stored name.** `_type_name` reads the stored type name through
`_STR_STR(type.__dict__["__name__"].__get__(type(value)))`, so a metaclass `__name__` property can
neither raise nor rename the placeholder (verified at `71e42dd` on a class whose metaclass property
raises: the descriptor returns the stored name). The unbound `str.__str__` is needed because
`type.__name__`'s setter accepts a `str` subclass, so the descriptor can return an object whose
`__str__` raises or whose `__format__` hijacks the f-string that embeds it. Measured by the review
with the read as `type.__dict__["__name__"].__get__(type(value))` alone: `{"sib": 1, "v": C(), ...}`
lost all of `fields`, and a hijacking `__format__` renamed the placeholder; `_STR_STR` returns an
exact `str`, and was verified on such a class (it returns `Stored`). The fallback `"?"` stays as a
guard.

The observable is stated on every row of the hostile corpus (`tests/test_sanitize_corpus.py`, which
this spec extends): on both delivery paths every leaf of the assembled event is of type exactly
`str`, `int`, `float`, `bool` or `NoneType`, every container exactly `dict` or `list`, and
`json.dumps(event, allow_nan=False)` succeeds.

#### Acceptance Criteria:

- [ ] Each of these as a field value is, on both delivery paths, `<unserializable: T>` naming its
      real class, with `json.dumps(event, allow_nan=False)` succeeding: an object whose `__class__`
      returns `bool`; one whose `__class__` returns `float` with `__float__` returning `1.0`; an
      instance of a class whose metaclass defines `__hash__` as `hash(int)` and `__eq__` as true;
      each fails at `71e42dd` with `TypeError` from the dump. A type-name placeholder does not set
      `truncated`.
- [ ] An `Enum` member whose `.value` is the `float` impostor is `<unserializable: E>` naming the
      Enum; one whose `.value` is the metaclass impostor is the enum's own text (`E.M`); each fails at
      `71e42dd` with `TypeError`. One whose `.value` is the `bool` impostor is also `E.M` (silence:
      its real type was never admitted), and one whose `.value` claims `str` is `<unserializable: E>`
      (silence: existing behaviour).
- [ ] A `float` subclass and an `int` subclass value, with `__float__`, `__int__`, `__repr__` and
      `__str__` each lying or raising, are written as an exact `float` and an exact `int` equal to
      the stored value, unmarked; a `float` subclass holding `nan` is `<float: nan>`, marked; an
      Enum whose `.value` is a `float` subclass is written as an exact `float`. Each asserts
      `type(leaf) is float` or `int`, and fails at `71e42dd`.
- [ ] A `float` impostor key is `<unserializable key: T>`, marked, with its sibling kept; a `float`
      subclass key whose `__str__` lies is the key `"1.5"` for `1.5`; an `int` subclass key keeps
      its own `str()` (`Color.RED`, and a subclass whose `__str__` returns `"P"` gives `"P"`); an
      object whose `__class__` returns `bool` as a key is its own `str()`. The first two fail at
      `71e42dd`; the rest are silence rows.
- [ ] A class whose metaclass defines `__name__` as a property that raises, or returns `42`, or
      returns `"renamed"`, gives `<unserializable: Stored>` as a value and
      `<unserializable key: Stored>` as a key, `Stored` being its stored name; each fails at
      `71e42dd` (`?`, `42`, `renamed`).
- [ ] A class whose stored `__name__` is a `str` subclass with a raising `__str__` and a
      `__format__` returning `"HIJACK"` gives `<unserializable: Stored>` (the stored text) for a
      value and `<unserializable key: Stored>` for a key, with every sibling in `fields` kept; at
      `71e42dd` the name is `?`.
- [ ] A proxy whose `__class__` is `dict` and which implements `items()` renders as the dict, and a
      proxy whose `__class__` is `datetime` renders through its `isoformat()`, exactly as at
      `71e42dd` (silence); a proxied `int` is still a placeholder.
- [ ] `tests/test_sanitize_corpus.py` holds a row for each case above and a walker asserting the
      leaf and container types over every row of the corpus on both paths, so the existing rows are
      held to the new observable too.
- [ ] An exact `True` and `False` as a field value and as an `Enum` member's `.value` stay exact
      `bool`s (`type(leaf) is bool`, not `1` and `0`). It is a separate row because planting out
      `kind is bool` is otherwise silent: `int.__int__(True)` is `1`, `1 == True`, and the leaf walker
      admits an `int`.
- [ ] A test parses `sanitize.py` and finds no `in` or `not in` comparison whose left side is a
      call to `type` or the local name `kind`, nor the name `_PLAIN_SCALARS`; it is run against a
      fixture holding the old shape and reddens.
- [ ] Planted out once each — `kind is bool`, the int conversion, each of the three float
      conversions (subclass branch, Enum branch, key branch), the stored-name read, and the
      `_STR_STR` around it (dropping it reddens the `str`-subclass criterion) — a named test
      reddens; the PR body records each.
- [ ] Serves invariants 1, 6 and 8: no input makes a call fail, the corpus rows run on both
      delivery paths, and the event is safe for a bare `json.dumps`.

### FR-003: A caller's argument's truthiness cannot raise into the application

#### Description:

`_merge` is outside `_log`'s guards, and `_log` tests `echo` outside the echo guard. Both consult a
caller's object's `__bool__` or `__len__` where nothing catches. The `fields=` truthiness test moves
inside `_merge`'s absorb: a `fields=` whose truth cannot be read is ignored and announced "merging
the fields= argument", as for a non-mapping, and the event is delivered with the keyword fields. The
`echo` test moves inside the echo guard: the event is delivered, nothing is echoed, and the fault is
announced "echoing to the console". `fields=[]`, `None`, `{}` and `echo=True`, `False`, `1`, `0`
behave exactly as today. The announcements name the exception's type, never a value (a diagnostic
names a type).

#### Acceptance Criteria:

- [ ] `info("m", fields=X(), a=1)` with `X` a `Mapping` whose `__bool__` raises, and one whose
      `__len__` raises, returns normally with no span open, delivering an event whose `fields` is
      `{"a": 1}`; stderr names "merging the fields= argument". It fails at `71e42dd` with the
      caller's `RuntimeError`. The same holds for each of the five level functions.
- [ ] The same call inside a `@trace`d function returns the function's own value, its `span.end` has
      `status` `ok`, and the span's events include the event with `{"a": 1}`; at `71e42dd` the
      function raises and the span records `status=error`.
- [ ] `info("m", echo=Y())` with a raising `Y.__bool__` returns normally with no span open and inside
      a `@trace`d function; the event is delivered once, nothing is written to the console stream
      (read through `capsys`), and stderr names "echoing to the console". Each fails at `71e42dd`.
- [ ] When the exception `X.__bool__` raises is of a class whose metaclass `__name__` property
      raises, the call still returns and stderr still names "merging the fields= argument", with the
      stored class name (the announcement's read is FR-004's `_diag` change).
- [ ] `fields=[]`, `fields=None`, `fields={}` and `fields={"x": 1}` with `x=2`, and `echo=True`,
      `False`, `1` and `0`, behave exactly as at `71e42dd` (silence rows): the keyword wins a
      collision, `echo=1` writes the console line and `echo=0` does not.
- [ ] Planted out once each — the `fields=` test back outside the `try`, and the `echo` test back
      outside the guard — a named test reddens.
- [ ] Serves invariants 1 and 6: the call returns on a hostile argument, on both delivery paths.

### FR-004: A hostile exception class or a colliding key costs only itself

#### Description:

**The exception class.** `_error_fields` reads `type(exc).__name__` and `__module__` through the
metaclass, `_exception_stack`'s fallback reads `type(exc).__name__` unguarded, and
`_diag.absorbed` reads it inside its own `try`, so with a raising `__name__` the line is swallowed
and a counted loss goes unannounced. All three read the type's stored name through
`_STR_STR(type.__dict__["__name__"].__get__(type(exc)))`, FR-002's read and for FR-002's reason: the
unbound `str.__str__` returns an exact `str` where a `str` subclass name could raise or hijack the
f-string. `_diag` is a leaf that imports nothing from the package, so it writes that expression
itself; no other module's reading of a name changes. The `error` sub-document reads the stored
`__module__` through `type.__dict__["__module__"].__get__(type(exc))`, taken as `str()` of it under a
guard with `"?"` as the fallback (a class whose own `__module__` is an object whose `__str__` raises,
or whose lookup fails). The stack's fallback placeholder `<unformattable traceback: T>` uses the
stored name. `traceback.format_exception` still reads the metaclass's `__module__`, so for that shape
it raises (measured at `71e42dd`) and the fallback is what the stack holds; the span's events survive.

**The merge.** `build_event`'s `{**cfg.defaults, **span.defaults, **baggage, **fields}` stays on the
common path, at C speed. Only when it raises is the merged mapping rebuilt by walking the sources
**highest precedence first** — per-call fields, baggage, span defaults, config defaults — inserting a
key only if it is not already present (`if k not in out: out[k] = v`). A key whose test or insertion
raises is dropped, and the event is marked `truncated`. Because the higher-precedence key of a
colliding pair is always present first, the one dropped is always the lower-precedence one: precedence
is exactly that of the unpack, and a per-call field can never be dropped by a hostile default. The
cost is stated under *Accepted Limits*: on this marked slow path the merged key order is
highest-source-first. Nothing is announced beyond the mark, as for any key placeholder (SPEC-055).

**How the shapes were checked.** Each reachable shape below was run at `71e42dd` and loses its event
there: `configure(defaults={K(): 1})` then `info("m", svc=3)` (orphan, and inside a `@trace`d
function); `@trace(defaults={K(): 1})` then `info("m", svc=3)`; and `configure(defaults={"svc": 1})`
then `info("m", fields={K(): 2})` (orphan and in-span). The unreachable shape was checked three ways:
`{K(): 2, "svc": 3}` raises at construction; `d[K()] = 1` then `d["svc"] = 3` raises; and a `Mapping`
yielding both is dropped whole by `_merge`, announced and leaving the event.

#### Acceptance Criteria:

- [ ] A `@trace`d function raising an exception whose class's metaclass defines `__name__` as a
      property that raises, or one returning `42`, or one returning `"Renamed"`: the original
      exception is re-raised, `is`-identical; an `lf.info` made earlier in the span is delivered;
      `span.end` is delivered with `status` `error` and `error.type` equal to the class's stored
      name; and no "span's events were lost" line is written. The first two fail at `71e42dd` with
      the events lost; the `"Renamed"` row loses nothing there and fails on `error.type`, which
      reads `Renamed`.
- [ ] A class whose stored `__name__` is a `str` subclass with a raising `__str__` and a hijacking
      `__format__` gives `error.type == "Stored"`, an exact `str`, with the span's events
      delivered (silence: it passes at `71e42dd`, because `truncate_str` already applies
      `_STR_STR`, and must keep passing). It does not discriminate the `_STR_STR` read; the next two
      criteria do.
- [ ] The same class with a metaclass `__module__` property that raises, so `format_exception` fails
      and the f-string fallback is reached, has `error.stack == "<unformattable traceback: Stored>"`;
      at `71e42dd` the hijacking `__format__` renames it. Dropping the `_STR_STR` in `model.py`'s
      read fails it.
- [ ] The same with a metaclass `__module__` property that raises: `error.type` and `error.module`
      are the stored name and module, and `error.stack` is `<unformattable traceback: Boom>` naming
      the stored name.
- [ ] An exception class with `__module__ = 5` has `error.module == "5"`; one whose `__module__` is
      an object with a raising `__str__` has `error.module == "?"`.
- [ ] `_diag.absorbed("x", exc)` for an exception of a class whose stored `__name__` is the
      hijacking `str` subclass writes a line ending `(Stored)`, where `71e42dd` writes `(HIJACK)`.
      For an exception of a class whose metaclass `__name__` property raises it writes a line ending
      `(Stored)` instead of nothing; on the orphan path a sink whose
      `emit` raises such an exception is counted in `orphan_lost` and announced, with stderr naming
      `Stored`. At `71e42dd` the count is made and stderr is empty.
- [ ] With `configure(defaults={K(): 1})`, `info("m", svc=3)` delivers, with no span open and
      inside a `@trace`d function, an event whose `fields["svc"]` is `3` and which holds no entry of
      `K`'s, with `truncated` set; the same holds for `@trace(defaults={K(): 1})`. Each is lost at
      `71e42dd`, announced "emitting an orphan log" or "building an in-span log".
- [ ] With `configure(defaults={"svc": 1})`, `info("m", fields={K(): 2})` delivers, on both paths,
      an event that holds `K`'s entry and not the default `svc`, with `truncated` set: the
      lower-precedence key is the one dropped. It is lost at `71e42dd`.
- [ ] Precedence and order: with `defaults={K(): 1, "a": 1, "x": 1}`, a span default `y`, baggage `y`
      and `fields={"svc": 3, "x": 2}`, the delivered `fields` equal, as a mapping, those of the same
      call without `K()` (`x == 2`, baggage's `y` over the span default's), and their key order is
      per-call fields first, then baggage, span defaults and config defaults, which pins the
      stated cost.
- [ ] An event with no colliding key is identical to `71e42dd`'s, truncation flag and key order
      included (silence).
- [ ] Planted out once each — the stored-name read, the stored-module read, the stack fallback, the
      `_STR_STR` in `model.py`'s read (the stack-fallback criterion reddens, not the `error.type`
      one), the `_diag` read and its `_STR_STR`, and the rebuild's highest-first order (walking
      lowest first reddens the `svc=3` criterion) — a named test reddens.
- [ ] Serves invariants 1, 2, 8 and 11: the user's exception is untouched, the events are delivered
      rather than lost, the loss is announced naming a type, and the event is built for a hostile
      class and key.

### FR-005: The record

#### Description:

This FR states the promise where it is recorded and is documentation, so it serves no invariant on
its own; it is what keeps the page that numbers the promises equal to the code. Both `architecture.md`
§12 items are moved from Open to `### Resolved` naming SPEC-063, struck through in that section's
form. The eighth invariant's text and *Observable* gain the exact-type sentence and the container-key
sentence. SPEC-057 FR-001's claim that keys "are not counted" is superseded at every site outside the
register that states it; the implementer greps for it, and the known sites are `sanitize.py`'s
`sanitize_fields` docstring ("the top-level mapping and the keys are not values, so they spend no
node"), `_Coercer.value`'s docstring (which says only that every value counts, and gains the key
clause), `mapping()`'s docstring (its paragraph ending "since a key visits no value", false once a
container key visits values), the `key`, `_key` and `produced` docstrings that call every key that
is not a `str` produced text, and SPEC-057 itself at its FR-001 description and its *Accepted
Limits* entry, and its charge-table row (the one near line 495 that says a "non-`str` key" is
charged as produced text), each struck through with a one-line pointer to this spec rather than
deleted. One
further docstring is corrected in passing: `_TypesOnly`'s says "a ``Config`` validates its
ceilings", which is false (`Config` is a plain frozen dataclass and `configure()` validates), and
becomes "``configure()`` validates the ceilings; a hand-built ``Config`` does not".

A decision file in `docs/decisions/event-assembly/` records the rule, fence and entry together,
ending in its dated History. Its fence is that a route trusting the caller's object admits it by
exact type or an unbound accessor; `isinstance` stays for render-through routes, with the proxy
reason; and a container key is rendered from its coerced JSON. It records the rejected alternatives:
keeping the `repr` behind a pre-pricing walk (a second implementation of the pass's traversal that
must agree with it), and a type-name placeholder for the key (distinct keys collapse and a
hostile-looking key replaces an ordinary one). The existing decision "Each coercion pass is bounded
by a node and a byte budget" says these two items are "Not closed here, and recorded in
architecture.md §12": that line gets a superseded pointer to SPEC-063, and so does its "What is
charged" clause naming a non-`str` key.

`docs/release-notes/v1.1.0.md` (pending and untagged) gains a `### Keys, impostor scalars and
hostile classes (SPEC-063)` subsection under a `## Fixed` section placed after
`## New: OTLPSink (SPEC-056)` and before `## What did not change`; the first spec to land creates
`## Fixed`, and this one appends beneath it if it exists. It lists the caller-visible changes:
container keys read as JSON (`(1, 2)` as `[1, 2]`) and no member's `repr` is written inside one; a
huge container key now spends the pass budgets and a huge `bytes` key is priced before it is
rendered; an object that claims `bool`, `float` or `int` through `__class__` or a metaclass is
`<unserializable: T>` where it reached a sink as itself; an `int` or `float` subclass is written as an
exact number and a `float` subclass key no longer uses its own `__str__`; a raising `fields=` or
`echo=` no longer reaches the caller; a hostile exception class no longer costs its span's events
or silences its announcement; and a colliding key no longer costs its event, the lower-precedence
key being the one dropped. `CHANGELOG.md`, `docs/spec-delivery/RELEASES.md` and the notes' title are
not touched, and any sentence of the file that the change makes false is corrected. The build
re-judges the file's headline sentence that a `>=1,<2` pin takes the release "without a code
change": container keys now read as JSON, so a downstream query or alert on the old key text would
notice, and the sentence is either qualified or followed by a note saying so. (At `0005dd4`, before
this change was specified, no other sentence of the file was false, since it describes the OTLP
sink, which maps an already assembled event.)

**Written at completion, not by this FR** (`docs/process/completion-ritual.md`): the delivery doc,
the `value sanitizer` row of `docs/component-inventory.md`, the arc narrative in
`docs/specs/ARCS.md`, and the prune of `CLAUDE.md`'s `## Specs`.

#### Acceptance Criteria:

- [ ] `docs/architecture.md` §12 has neither item as an open bullet; each stands struck through
      under `### Resolved` with "fixed in SPEC-063" and what closed it.
- [ ] The page `docs/invariants.md` states, in its section 8, the exact-type rule for a route that
      trusts a caller's object and the container-key rule, records that an object key keeps its
      own `str()` as an exception to the no-`repr` rule, and lists the new test module under
      *Guarded*; `tests/test_invariants_model.py`'s
      `test_the_page_cites_tests_that_exist_and_this_file_cites_invariants_that_do` passes.
- [ ] A whitespace-collapsed search of `src/`, `docs/architecture.md` and `docs/specs/` (this spec
      excepted, which quotes the phrases) for "keys
      are not values", "spend no node", "are not counted" and "a key visits no value" finds none
      stating the old claim outside a hit struck through with a pointer to SPEC-063, and
      `_TypesOnly`'s docstring no longer says a `Config` validates its ceilings. The search is
      collapsed because "a key visits no value" wraps across two lines in `mapping()`'s docstring,
      so a line-based search passes before any edit is made; it is piped over `git ls-files`
      (`tr -s ' \n' '  ' < "$f" | grep -oE "..."`), never `for f in $VAR`, which zsh does not
      word-split. `docs/spec-delivery/` is excluded: those are frozen
      records of what their specs shipped.
- [ ] The decision file exists in `docs/decisions/event-assembly/` with its fence, entry and a dated
      History, names both rejected alternatives, and `sh scripts/gen-index.sh` leaves
      `docs/decisions/index.yaml` unchanged on a second run.
- [ ] The "Each coercion pass is bounded…" decision's line "Not closed here, and recorded in
      `architecture.md` §12" and its "What is charged" clause naming a non-`str` key each carry a
      pointer to SPEC-063.
- [ ] `docs/release-notes/v1.1.0.md` has the `### ... (SPEC-063)` subsection under `## Fixed`,
      between `## New: OTLPSink (SPEC-056)` and `## What did not change`, listing each
      caller-visible change named above, and `git diff` shows `CHANGELOG.md` and
      `docs/spec-delivery/RELEASES.md` untouched. The headline sentence "without a code change"
      is qualified, or a note follows it, to say that container-key text changed.
- [ ] `sh scripts/docs-lint.sh`, `sh scripts/spec-lint.sh` and
      `poetry run python scripts/docstring-lint.py` pass.
- [ ] Serves no invariant on its own: it records the promises the other four FRs keep, in the
      documents that state them, and the lints above are the gate that holds those documents.

---

## Data Model

```
# src/log_foundry/sanitize.py            (private; __all__ is unchanged; no module-level mutable state)
_PLAIN_SCALARS                           # removed; `kind is bool` replaces its one use
_CONTAINER_KEYS = (tuple, list, dict, set, frozenset)
    # a container key: issubclass(type(key), _CONTAINER_KEYS) and not issubclass(type(key), Enum)

_Coercer._key(key, depth) -> str         # gains the entry's value depth; mapping() passes depth + 1
    container key  -> coerced = dispatch at depth + 1 (one node; the pass's guard is NOT applied,
                      so a failure surfaces to key() and becomes <unserializable key: T>)
                      text    = json.dumps(coerced, ensure_ascii=False)   # not charged again
                      return clip(text, max_value_bytes)                  # truncated when clipped
    bytes-like key -> n = bytes.__len__(key) / bytearray.__len__(key)       # unbound
                      if self._charged + n > self._max_bytes: halt(_SIZE_LIMIT), return it unrendered
_Coercer.mapping:  a stop after the key writes {self._stopped: self._stopped}   # was _SIZE_LIMIT

_type_name(value) -> str                 # _STR_STR(type.__dict__["__name__"].__get__(type(value)))
# src/log_foundry/model.py
_stored_module(cls) -> str               # type.__dict__["__module__"].__get__(cls), str() under a guard, "?"
_merged(fields, baggage, span_defaults, defaults) -> tuple[dict[str, object], bool]
                                         # {**d, **s, **b, **f}; if that raises, walk f, b, s, d with
                                         # `if k not in out: out[k] = v`, dropping a key that raises;
                                         # bool = a key was dropped
# src/log_foundry/_diag.py
absorbed(...)                            # names the type by the same stored-name read, written inline:
                                         # the leaf imports nothing from the package
```

What is charged is SPEC-057's table, with one row changed: a container key is charged as the
members it contains, each as the value it is, and its text is charged nothing further.

## API / Interface Contract

No signature changes and no public name added; `sanitize.__all__` and `log_foundry.__all__` are
unchanged, and `build_event`, `_error_fields`, `_exception_stack`, `_log` and `_merge` keep their
shapes.

```
sanitize_fields({(1, 2): 3, frozenset({1}): 4}, cfg=cfg)  ->  ({"[1, 2]": 3, "[1]": 4}, False)
sanitize_fields({"a": object_claiming_float}, cfg=cfg)    ->  ({"a": "<unserializable: Impostor>"}, False)
info("m", fields=hostile_truthiness_mapping, a=1)          ->  event with fields == {"a": 1}
```

## Configuration / Environment

None.

## File & Folder Structure

```
src/log_foundry/
├── sanitize.py        # the container-key rule, the exact-type routes, the stored name
├── model.py           # the stored name and module, the merge rebuild
├── _diag.py           # the stored-name read in `absorbed`
└── api.py             # the two truthiness tests moved inside their guards
tests/
├── test_assembly_keys_and_scalars.py   # new: the CPU-bounded and alarm tests, the key texts, the stops
├── test_assembly_budget.py             # the three sites under FR-001 re-derived
├── test_sanitize.py                    # the exact-type routes
├── test_sanitize_corpus.py             # new rows and the leaf-type walker
├── test_api.py                         # FR-003
├── test_diag.py                        # FR-004: the announcement for a hostile class
└── test_model.py                       # FR-004
docs/
├── architecture.md                     # §12
├── invariants.md                       # the eighth invariant (and the object-key exception)
├── decisions/event-assembly/           # the new decision; the existing one's pointers
├── release-notes/v1.1.0.md             # `## Fixed` > `### ... (SPEC-063)`
└── specs/SPEC-057-assembly-work-budget.md   # superseded pointers
```

## Implementation Phases

### Phase 1: The exact-type routes (FR-002)

- Write the corpus rows and the leaf-type walker first; run them once against `71e42dd`'s module to
  watch each named route redden.
- Replace `kind in _PLAIN_SCALARS`, the `isinstance(value, bool)` branch, and the int, float, Enum
  and float-key conversions; read the stored name, through `_STR_STR`, in `_type_name`. Carry the reasoning in the
  docstrings, the only prose in `src/`.

### Phase 2: Container keys (FR-001)

- Write the CPU-bounded, allocation and key-text tests with the alarm guard; run the frozenset and
  one-level tuple shapes once against `71e42dd`'s module from a scratch worktree and keep the
  failure reasons for the PR body.
- Add the container-key rule (with the `Enum` exclusion) and the bytes-like pricing to `_key`, pass
  the value depth from `mapping()`, generalise the stop placeholder, and re-derive the four sites
  under FR-001.

### Phase 3: The siblings (FR-003, FR-004)

- Move the two truthiness tests inside their guards; add the stored name and module reads (in
  `model.py` and `_diag.py`) and the highest-first merge rebuild; write the tests and the mutation run
  each FR names.

### Phase 4: The record (FR-005)

- The architecture, invariants, decision, release-notes (`## Fixed`) and superseded-pointer edits,
  the `_TypesOnly` docstring included; regenerate
  the indexes; then every gate: `ruff`, `mypy`, `pytest`, `spec-lint`, `docs-lint` and
  `docstring-lint`.
