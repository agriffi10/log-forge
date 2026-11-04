---
type: Spec
id: SPEC-067
title: "Total Inbound Trace Context — a Bounded Echo and Parsers That Run No Code of the Value"
description: "Makes continue_trace total over any argument it is handed: the rejection echo is bounded and runs no code of the value, and the four parsers accept only an exact str."
spec_status: Completed
depends_on: [SPEC-014, SPEC-029, SPEC-064]
---

## Overview

`continue_trace` is the library's one inbound path. It takes a caller's `traceparent`, `trace_id`,
`parent_span_id` and `baggage`: values that come off the wire and are typed `str` only by
annotation, which a caller not running mypy is free to ignore. Two things make it neither bounded
nor total today, and a third route hands a hostile value on instead of refusing it. The rejection echo, `_diag.rejected`, calls `repr` on the value and only then cuts
the result to 64 characters, so a hostile value can hang the caller's thread, or make the
announcement fail silently while `reason="rejected"` says it was made. And the parsers call the
value's own methods, so a `str` subclass or an object with a hostile `__class__` makes
`continue_trace` raise into the caller. This spec closes both halves in one change, because the
second is the same promise as the first: **no code of an inbound value runs inside the library,
and a refusal is always announced, in bounded form.** A well-behaved `str` subclass carrying a
valid header is still adopted, as today, and from now on is adopted as the exact `str` it holds.

Probes A to F were measured at `2e790e9`, the tree this spec was drafted on, by the orchestrating
session through `continue_trace`, each under a 3 s `setitimer` whose handler raises a
`BaseException`. Probe G was measured at `7dc881f` by the drafting session. In this spec "base" is
`7dc881f`, the tree the branch is rebased onto: `_diag.py`, `ids.py` and `decorator.py` are identical
at the two commits, and `context.py` differs only by SPEC-064's baggage render, which leaves
`parse_baggage_header` as it was. The deadline must be a `BaseException`: an alarm that raises an
`Exception` subclass is swallowed by `rejected`'s own `except Exception`, and the call "returns"
`reason="rejected"` with no line, a false pass.

- **Probe A: a shared-sublist DAG does not return.** `y = 0`, then `y = [y] * 16` seven times
  (16^7 paths to one leaf) passed as `traceparent` did not return in 3 s (2.97 s of CPU, no line
  written). The same value as `baggage` did not return either. `repr` walks the paths, and
  `rejected` runs it before it slices.
- **Probe B: a `__repr__` that raises makes the verdict false.** `continue_trace` returned
  `ContinueResult(ok=False, reason="rejected")` with no line written, because `rejected`'s `try`
  covers the whole write. `results.py` and `decorator.py` document `"rejected"` as meaning exactly
  that a rejection was announced (invariant 11: what the line says happened is true).
- **Probe C: the echo's cost grows with the value.** A 50,000,000-character `str` costs 0.10 to
  0.11 s of CPU inside `rejected`, before the slice.
- **Probe D: `bytes` is echoed as its `repr`.** `b"00-bad"` is written as `b'00-bad'`.
- **Probe E: a `str` subclass with one raising method escapes.** With a single overridden method
  raising `RuntimeError`: `strip` raises out of `continue_trace` for `traceparent` (valid or not)
  and for `baggage`; `__len__`, `__iter__` and `__ne__` raise for a valid-looking `trace_id`;
  `split` and `encode` raise for `baggage`.
- **Probe F: an object whose `__class__` is a property escapes.** `isinstance(v, str)` reads
  `__class__` (CPython falls back to it when the real type check fails, and propagates anything
  but `AttributeError`). A raising property raises out of `continue_trace` for `traceparent`,
  `trace_id` and `baggage`. One returning `str` passes `isinstance`, and the parser then calls
  `.strip()` or `len()` on a non-`str`, so an `AttributeError` or `TypeError` escapes.
- **Probe G: an adopted `str` subclass is stored as it is, and detonates later.** At `7dc881f`,
  `continue_trace(trace_id=S(valid), parent_span_id=S(valid))`, where `S` is a `str` subclass
  whose `__format__`, `__str__`, `encode` and `__repr__` raise, returns `ok=True` and adopts the
  two objects themselves. The next span's events then carry a `trace_id` of type `S`, and the
  public `current_traceparent()`, called inside that span, raises `S`'s `RuntimeError` into the
  caller from `format_traceparent`'s f-string (invariant 1). Validation does not stop it, because
  a valid subclass is valid.
- `parent_span_id` alone was total on the Probe E and F values at `2e790e9`; this spec still tests
  it, because its totality was an accident of which methods it happens to call.

## Scope

### In Scope

- `_diag.rejected` echoes without running any code of the value, its type or its metaclass, and in
  bounded form, whatever the value's size or shape (FR-001, FR-002).
- The four parsers (`parse_traceparent`, `is_valid_trace_id`, `is_valid_span_id`,
  `context.parse_baggage_header`) accept only an exact `str` (FR-003).
- `continue_trace` normalises each of its four arguments once, on entry, returns normally on any
  input and keeps its verdict true (FR-004).
- The prose that stated the old behaviour, and the release notes (FR-005). The change is
  caller-visible on stderr (a `bytes` or any other non-`str` value is now echoed by type name, and a
  long `str`'s quoting can differ, and an adopted `str` subclass is stored as an exact `str`), so it
  goes in `docs/release-notes/v1.1.0.md`.
  `architecture.md` §12 carries an item SPEC-064 added, "`_diag.rejected` echoes an inbound value
  through an unbounded `repr()`", which this spec closes; striking it is the completion ritual's
  step, not an FR.

### Out of Scope

- **Bounding `reason`.** It is a literal supplied by the library, escaped but not cut, and stays
  so.
- **What `continue_trace` adopts for a valid exact `str`.** Unchanged, for every argument and
  every combination.
- **The parsers' own linear cost on a very long exact `str`.** They run no code of the value and
  return; this spec bounds the echo, not the parse. No figure is claimed for it.
- **`ids.format_traceparent`'s own code.** It formats whatever ids it is handed, and at base that
  includes an adopted inbound `str` subclass (Probe G). What changes is the id it is handed,
  because `continue_trace` stores an exact `str` (FR-004), not the formatter. The outbound baggage
  render SPEC-064 changed is likewise untouched.
- **Every other `_diag` writer** (`absorbed`, `lost`). Their inputs are an exception's type and a
  library-controlled detail, not an inbound value.
- **Assembly's coercion of event fields** (`sanitize.py`, `model.py`), which has its own work
  budget (SPEC-057).
- **Public surface.** `ids` is not exported from `log_foundry`; no name or `configure()` key
  changes (invariant 10).

### Twin paths (invariant 6)

- **The four inbound arguments of `continue_trace`.** Each is normalised by the same step and each
  reaches `rejected` by the same route. The rejection table cannot guard the normalisation: once
  FR-001 and FR-003 land, an argument that skips it is merely refused, which the table accepts
  (a mutation removing it for `parent_span_id` and `baggage` left every cell green). The guard is
  FR-004's **adoption** criterion, which states a `str` subclass for each of the four arguments
  separately and asserts it was adopted.
- **The four parsers.** The two id validators are what `parse_traceparent` calls on its fields, and
  `parse_baggage_header` is the baggage twin of `parse_traceparent`. FR-003 tests all four from one
  table.
- **`rejected`'s `str` route and its non-`str` route.** Both end in the same escape and the same
  bound, and FR-001 tests the two against the one limit.
- **The swept-span refusal in `continue_trace`** echoes `traceparent` or, otherwise, the trace id
  through `str(trace_id)`, a call on the value. FR-004 covers it, because it is a fifth place a value
  meets library code.

---

## Functional Requirements

### FR-001: `rejected` echoes without running any code of the value, and the line is always written

#### Description:

`rejected(reason, value)` decides what the value is from its real type and reads it only through
`str`'s and `type`'s own slots, so no `__repr__`, `__class__`, `__getattribute__`, `__len__`,
`__getitem__`, `__name__` property or metaclass method of the value is ever called. The echo is
computed by a private helper, `_echo(value)`, inside the guard that already covers the write.

- **Is it a `str`.** `issubclass(type(value), str)`, a check over the real type's MRO. Never
  `isinstance`, which reads `__class__`.
- **A `str`, exact or subclass.** Its first `_MAX_REJECTED_ECHO` characters are taken with
  `str.__getitem__(value, slice(None, _MAX_REJECTED_ECHO))`, which returns an exact `str`, rendered
  with `str.__repr__`, escaped with the existing `_escape`, and cut to `_MAX_REJECTED_ECHO` plus
  `"…"` exactly as today.
- **Anything else.** Echoed as `<TypeName>`. The name is read with
  `type.__dict__["__name__"].__get__(type(value))`, which bypasses a metaclass `__name__` property.
  It goes through the same `str` path, except that the slice is the first **65** characters (then
  escaped, the escaped text cut to 64 characters plus `"…"` when longer, no quotes), because a
  class's `__name__` may be set to a `str` subclass instance, may be any length and may contain a
  newline. The result is wrapped in `<` and `>`. The slice is 65, not 64, because a printable name
  longer than 64 characters escapes to exactly 64 from a 64-character slice and would be cut with
  no `…`; with 65 every name over 64 characters is cut and marked, and the output is unchanged for
  every name of 64 or fewer. The `str` route keeps its 64-character slice, because `repr`'s quotes
  always push a 64-character slice past the cut.

So the echo is at most `_MAX_REJECTED_ECHO + 3` characters whatever the value. For an exact `str`
of at most 64 characters the line is byte-identical to today's. For a longer one the quoting may
differ, because today's choice of quote, and so of which quote is escaped, depends on characters
past the cut. `bytes` is now `<bytes>`, which names the caller's actual mistake.

If computing the echo raises an `Exception` (a `MemoryError`, or a C type whose name will not
render), the line is still written, with the constant echo `<?>`. So the line is written on every
input bar a fault of the stream itself, which is still swallowed, and a `BaseException` from
computing the echo or from the write still propagates (SPEC-029).

#### Acceptance Criteria:

Tests are in `tests/test_diag.py`. Every criterion marked *fails at base* fails against `7dc881f`.
A "spy" is a value whose every method, property and metaclass hook appends to a list, then asserts
the list is empty after the call. A hook that raises is armed only around the `rejected` call, as
`_NamelessMeta` is in `tests/test_baggage_header.py`, because pytest's `saferepr` reads
`cls.__name__` and an always-raising metaclass turns any failure into an INTERNALERROR.

- [ ] A value whose `__repr__` raises writes one line ending `: <_Hostile>` and returns. *Fails at
      base: nothing is written (Probe B).*
- [ ] A value whose `__repr__` returns a raw newline writes exactly one line, and a spy on
      `__repr__` records no call. *Fails at base.*
- [ ] An instance of a class whose metaclass raises from `__getattribute__` and a `__name__`
      property, and which has a raising `__class__` property, is echoed `<C>` using its real name,
      with the spy empty. *Fails at base.* (`__subclasscheck__` is not a hook here:
      `issubclass(type(v), str)` consults `str`'s metaclass, never the value's.)
- [ ] An instance of a class whose `__name__` was set to a `str` subclass instance, 500 characters
      long, beginning `"a\nlog-foundry: forged"` and with every method of the subclass raising,
      writes one line, and the part after `: ` equals `<`, the first 65 characters of the name
      escaped and then cut to 64 characters, `…` and `>`, written out as a literal. The spy on the
      subclass is empty. *Fails at base, which echoes the default object `repr` and not the name.*
- [ ] A class named by 64 printable characters is echoed `<` and that name and `>`, and one named by
      65 printable characters is echoed `<`, its first 64 characters, `…` and `>`. *Both fail at
      base, which echoes the default object `repr` of the instance, not its type's name.*
- [ ] `b"00-bad"` is echoed `<bytes>`, an `int` `<int>` and a `list` `<list>`. *Fails at base for
      all three (`b'00-bad'`, `123` and the `repr` of the list).*
- [ ] A `str` subclass with `__repr__`, `__getitem__`, `__len__`, `__str__`, `__iter__` and `__eq__`
      all raising is echoed as a `str` (quoted, escaped, bounded) and the spy is empty. *Fails at
      base.*
- [ ] Output for an exact `str` of at most 64 characters equals these literals, written out in the
      test and not computed by the production expression: `'nope'`, `''`, `"it's"`, `'say "hi"'`,
      `'a\'b"c'` (both quotes), `'a\nb'`, `'a\tb'`, `'\x85'`, `'\u2028'`, and a 64-character string.
      *Passes at base by design: it pins the unchanged half.*
- [ ] `"0" * 500` is echoed as a quote, 63 zeros and `…`. `"x" * 100 + "'"` opens with `'` (today
      `repr` of the whole string opens with `"`), and `"it's" + "x" * 100 + '"'` opens with `"it's`
      (today `'it\'s`, because the apostrophe is escaped too). *The last two fail at base.*
- [ ] A raw newline in `reason` is still escaped and `reason` is not cut (unchanged).
- [ ] With `_diag._echo` replaced by a function raising `MemoryError`, `rejected` writes one line
      ending `: <?>`; with it raising `KeyboardInterrupt`, that propagates and nothing is written.
      *Fails at base, which has no such helper and so cannot pass for the right reason.*
- [ ] A stream whose `write` raises `OSError` makes `rejected` return normally, and one raising
      `KeyboardInterrupt` propagates it (the existing SPEC-029 tests stay green, unchanged).
- [ ] `test_a_hostile_value_cannot_break_rejected` asserts a line is written (it asserts the
      opposite at base), and `test_rejected_escapes_a_repr_that_lies` asserts the `__repr__` was not
      called and the output is one line, since its value is now echoed `<_Liar>`.
- [ ] Serves invariants 1 and 11.

### FR-002: `rejected`'s cost does not grow with the value

#### Description:

FR-001's route takes 64 characters and a type name, so the work done is independent of the size or
shape of the value. This FR is the measurable half of that promise, for the shapes that made the
cost follow the value (Probes A and C, and the `bytes` analogue). Two instruments, because neither
sees all of it. CPU time against `repr` of the same value in the same test catches a pass over the
value without pinning an absolute number, so a slow or loaded machine moves both; but it passes a
full O(n) copy followed by a slice, which costs 0.02 to 0.03 of a `repr`. Peak allocation under
`tracemalloc` is deterministic and catches that copy: measured at `7dc881f` on a 10^8-character
`str` subclass, the slot slice peaks at 105 bytes and a full copy at 100,000,041 (`str.__str__` of it alone).

#### Acceptance Criteria:

Tests are in `tests/test_inbound_totality.py`, which carries a `_deadline` context manager modelled
on SPEC-064's in `tests/test_baggage_header.py` (a `SIGALRM`/`setitimer` alarm raising a
`BaseException`, asserted to run on the main thread) and reads stderr through `capsys`.

- [ ] For a `"0" * 10**8` string, `time.process_time` across `rejected` is under a tenth of the
      `time.process_time` across `repr` of the same string in the same test. *Fails at base: the
      ratio is about one.*
- [ ] The same ratio holds for `b"0" * 10**8`, echoed `<bytes>`. *Fails at base.*
- [ ] With the value built before tracing starts and `tracemalloc.reset_peak()` called just before,
      the peak traced memory across `rejected` is under 65,536 bytes for a `"0" * 10**8` exact
      `str`, for a `str` subclass of the same length that overrides nothing (one whose `__repr__` raises would
      make `repr` fail early at base and pass vacuously), and for `b"0" * 10**8`. The bound is 64 KiB
      because it sits some 600 times above the measured 105 bytes, which leaves room for the
      write, the capture buffer and interpreter noise, and some 1,500 times below a copy of the
      value, so only an O(n) copy can fail it. *Fails at base, where `repr` of the value peaks at
      about its size.*
- [ ] The shared-sublist DAG (Probe A) passed to `rejected` returns inside a 5 s deadline and writes
      `<list>`. *Fails at base: the deadline fires.*
- [ ] The measurement is taken with the stream accepting writes, and each ratio test asserts the
      line was written, so a `rejected` that returned early without writing cannot pass.
- [ ] Serves invariant 1.

### FR-003: The four parsers accept only an exact `str` and run no code of the value

#### Description:

`parse_traceparent`, `is_valid_trace_id`, `is_valid_span_id` and `context.parse_baggage_header` test
`type(value) is str` and return `None` or `False` for anything else, so on their own they run no
code of the value, whatever they are handed. This replaces `isinstance(value, str)`, which reads the
value's `__class__` and admits a subclass whose methods then run. A `str` subclass is therefore
refused by a parser called directly; it is `continue_trace` (FR-004) that normalises a subclass to
an exact `str` first, so a valid one is still adopted.

#### Acceptance Criteria:

Tests are in `tests/test_ids.py`, one parametrised table over the four functions, each row with a
valid exact `str` and its expected result.

- [ ] A valid exact `str` gives each function's existing result (the unchanged half; passes at
      base).
- [ ] A well-behaved `str` subclass holding the same valid text gives `None` or `False`. *Fails at
      base, where it is accepted.*
- [ ] A `str` subclass with exactly one of `strip`, `__len__`, `__iter__`, `__ne__`, `__eq__`,
      `split`, `encode` raising returns `None` or `False` and raises nothing, and a spy on every
      method records no call. *Fails at base for each of the Probe E methods its function reaches.*
- [ ] An object with a raising `__class__` property, and one whose `__class__` returns `str`, each
      return `None` or `False`, with the spy empty. *Fails at base (Probe F).*
- [ ] `None`, `b"..."`, an `int`, and the DAG each return `None` or `False` (unchanged; passes at
      base).
- [ ] Serves invariants 1 and 6.

### FR-004: `continue_trace` normalises its arguments once and is total

#### Description:

On entry `continue_trace` replaces each of `traceparent`, `trace_id`, `parent_span_id` and
`baggage`: a value whose real type is a `str` subclass (tested as in FR-001) becomes an exact
`str` through `str.__str__(value)`, a C-level copy that runs no method of the subclass; any other
value is passed through unchanged. The implementation either binds new local names or assigns back
through `cast("str | None", ...)`, and either is fine; a plain assignment of an `object` back to the
`str | None` parameters is what `mypy --strict` refuses. The rest of the
function, the parsers and `_diag.rejected` therefore see only an exact `str` or a non-`str` of
which no code is ever run. So a `str` subclass carrying a valid header is still adopted as today,
and what is adopted, stored and later formatted by `current_traceparent()` is the exact `str` it
holds (Probe G), while an argument of any other type is refused and announced. After this change no
code of any inbound value runs: the `str(trace_id)` fallback in the swept-span refusal now sees only
an exact `str`, because a `trace_id` that reaches it has been validated.

The verdict holds: on any input `continue_trace` returns normally, bar a `BaseException` raised by
the stderr write (SPEC-029), and when the result is falsy `reason == "rejected"` exactly when a
rejection was announced, which is to say `_diag.rejected` ran. The verdict follows the
announcement, not its delivery, because the write is best-effort: a stream that faults still gives
`"rejected"`, which is what the documentation says and what the code does today.

#### Acceptance Criteria:

Tests are in `tests/test_inbound_totality.py`. The rejection table crosses every probe with every
argument (`traceparent`, `trace_id`, `parent_span_id` alone, `parent_span_id` beside a valid
`trace_id`, `baggage`), each call under `_deadline(5, ...)`. The probes are the DAG, an object whose
`__repr__` raises, a `"0" * 50_000_000` string, `b"00-bad"`, a `str` subclass holding `"00-bad"`
(`"k"` for `baggage`) with one of `strip`, `__len__`, `__iter__`, `__ne__`, `__eq__`, `split`,
`encode`, `__repr__`, `__str__`, `__getitem__` raising, an object with a raising `__class__`
property, and one whose `__class__` returns `str`.

- [ ] Every cell returns a `ContinueResult` without raising and inside the deadline. A spy on each
      probe that is a user-defined object records no call, and the overridden method of each
      subclass probe was never called. The DAG and `bytes` are builtins and cannot carry a spy, so
      those cells rely on the deadline and the echo literal (`<list>`, `<bytes>`) instead. *Fails at base in the cells Probes A, E and F name.*
- [ ] In every cell where the result is falsy, `reason == "rejected"` and at least one line
      beginning `log-foundry: ignoring inbound trace context` is on stderr; and the raising
      `__repr__` cell writes a line ending `: <_Hostile>`. *The second half fails at base (Probe B).*
- [ ] `parent_span_id` beside a valid `trace_id` adopts that trace as a root (`ok` is true) and
      writes one `invalid parent_span_id; joining as a root` line, for every probe.
- [ ] **Adoption, one argument at a time.** Let `S` be a `str` subclass with every method it
      overrides raising (`strip`, `__len__`, `__iter__`, `__ne__`, `__eq__`, `split`, `encode`,
      `__repr__`, `__str__`, `__format__`, `__getitem__`), and every other argument an exact `str`
      or absent. Each of these is its own test, and each fails if normalisation is removed for that
      argument alone, which is what the rejection table cannot do. *Each fails at base.*
      - `traceparent=S(valid)`: `ok` is true, and the next root span's events carry the header's
        trace id and parent span id, each `type(...) is str`.
      - `trace_id=S(valid)` with an exact valid `parent_span_id`: the same, with those ids.
      - `trace_id=` an exact valid id and `parent_span_id=S(valid)`: `ok` is true, no line is
        written, and the root span's events carry that `parent_span_id`. An unnormalised one is
        refused as invalid, joins as a root and still reports `ok=True`, so `ok` alone cannot show
        it.
      - `baggage=S("k=v")`: `context.get_baggage()` equals `{"k": "v"}`, and `ok` is false with
        `reason == "nothing-supplied"` (the verdict describes the trace context only).
- [ ] **Probe G.** `trace_id=S(valid)` and `parent_span_id=S(valid)`, where `S` raises from
      `__format__`, `__str__`, `encode` and `__repr__`, are adopted; inside the next span
      `current_traceparent()` returns an exact `str` that matches
      `00-<that trace id>-<16 lowercase hex>-01`, raises nothing, and the span's events carry a
      `trace_id` with `type(...) is str`. *Fails at base: `current_traceparent()` raises `S`'s
      `RuntimeError`.* Serves invariants 1, 8 and 9 (the id type is the assembly half; what is
      adopted stays a correlation id and confers nothing).
- [ ] A valid exact-`str` `traceparent`, `trace_id`+`parent_span_id` and `baggage` are adopted
      exactly as before, and a call with nothing supplied gives `reason == "nothing-supplied"` with
      nothing on stderr (unchanged; passes at base).
- [ ] End to end through `continue_trace(traceparent=...)`, a `str` subclass of 100 `"x"` characters
      whose `__repr__` raises, and an exact `str` of the same characters, each write one line
      whose echo equals the literal quote, 63 `x` characters and `…`. *The subclass case fails at
      base: no line is written.*
- [ ] With the span already swept as `tests/test_span_sweep.py` arranges it, a valid `trace_id`
      that is a `str` subclass with `__str__` raising is refused with one line and `reason ==
      "rejected"`, and nothing raises. *Fails at base: `str(trace_id)` runs the subclass.*
- [ ] With `sys.stderr.write` raising `OSError`, a rejection returns normally with
      `reason == "rejected"`; with it raising `KeyboardInterrupt`, that propagates.
- [ ] Serves invariants 1 and 11.

### FR-005: The prose and release notes state the new behaviour

#### Description:

Several documents state what this spec replaces. `rejected`'s docstring calls it "the one place in
the library where a `repr` is correct" and says the value is echoed as a bounded `repr`. The
failure-paths decision says "The one bounded `repr` is `rejected`". `SPEC-029`'s spec states the same
claim at three sites (lines 57 to 59, where `_warn_rejected` "already bounds and `repr`-escapes",
lines 107 to 109, wrapped across lines, and line 165, a docstring sketch), its delivery doc says "SPEC-014's bounded `repr`", and
`tests/test_diag.py:195`'s section comment says `rejected` "keeps SPEC-014's bounded repr exactly".
A completed spec and a delivery doc are history, so those two get a superseded marker (the
completion ritual's step for every other site that states the old claim, and `SPEC-036` and
`SPEC-005` already carry such markers) and are not rewritten. The four parsers' docstrings say
their argument is "of any type" without saying a subclass is refused. `continue_trace`'s docstring
documents the verdict but not the normalisation. The caller-visible stderr change has no release
note. None of this has behaviour to test, which is why this FR keeps no invariant.

#### Acceptance Criteria:

- [ ] `rejected`'s docstring states the echo rule (the `str` route, the `<TypeName>` route, no code
      of the value runs) and no longer says a `repr` is correct there; `_diag`'s module docstring
      states that rule among its rules; `poetry run python scripts/docstring-lint.py` is clean.
- [ ] The four parsers' docstrings say a non-exact-`str` is refused, and `continue_trace`'s says each
      argument is normalised on entry and what its verdict means for a stream fault.
- [ ] The decision file under `docs/decisions/failure-paths/` named
      `one-module-writes-every-diagnostic-...` has its "one bounded `repr`" sentence rewritten in
      place with a dated History entry. `docs/specs/SPEC-029-diagnostic-output-safety.md` carries a
      short superseded blockquote naming SPEC-067 at each of its three sites (the out-of-scope
      bullet near line 57, FR-002's last criterion near line 107 and the docstring sketch near
      line 165), `docs/spec-delivery/SPEC-029-diagnostic-output-safety.md` carries one where it
      says "SPEC-014's bounded `repr`", and the comment at
      `tests/test_diag.py:195` is reworded.
- [ ] A sweep over `docs/`, `src/` and `tests/` that **reassembles wrapped sentences** (it joins
      each paragraph or list item before matching, because the SPEC-029 claim is split across three
      lines and a line-by-line `grep` cannot see it) finds no statement that `rejected` (or `_warn_rejected`) echoes a
      bounded or `repr`-escaped `repr`, that a `repr` is correct there, or that the one bounded
      `repr` is `rejected`, except under a superseded marker.
- [ ] `docs/release-notes/v1.1.0.md` has a new subsection at the end of `## Fixed`, after the
      Honeycomb, SPEC-064 and SPEC-065 entries, that names the caller-visible changes: a non-`str`
      value is echoed by type name, a long `str`'s quoting may differ, the parsers refuse a `str`
      subclass called directly while `continue_trace` still adopts a valid one, and an adopted
      `str` subclass is now stored as the exact `str` it holds, which stops
      `current_traceparent()` raising the subclass's own exception (Probe G). It ends with a
      "Who must change code" line saying no caller must, as SPEC-065's does.
- [ ] The SPEC-064 entry in that file says "`set_baggage`, `get_baggage`, `continue_trace` and
      `parse_baggage_header` are unchanged", which this spec makes false in the same release for
      the last two. The sentence is qualified in place to read: `set_baggage` and `get_baggage` are
      unchanged, and so are `continue_trace` and `parse_baggage_header` for an exact `str` header; what
      they do with a value that is not an exact `str` is in the SPEC-067 entry below. The rest of
      the sentence (`get_baggage()` still returns what you stored, surrogate included) stays.
- [ ] The lead paragraph of that file is unchanged. Its claim that a `>=1,<2` pin takes the release
      "without a code change" unless a caller passes a `float` ceiling stays true: this change is
      on stderr and in a module the frozen surface does not list, and a stderr format change is
      not a code change. `CHANGELOG.md`, `docs/spec-delivery/RELEASES.md` and the notes' title are
      not touched.
- [ ] `sh scripts/docs-lint.sh` and `sh scripts/spec-lint.sh` are clean.
- [ ] Serves no invariant: prose and release-notes hygiene, which `docs/process/completion-ritual.md`
      judges by its own rules.

---

## API / Interface Contract

No signature changes and no new public name.

```
# src/log_foundry/decorator.py: the normalisation, a private helper applied once on entry
def _exact(value: object) -> object:
    # str subclass -> str.__str__(value); anything else -> value, untouched
    # callers bind new names, or assign back through cast("str | None", ...); a plain assignment
    # back to the `str | None` parameters does not type-check

# src/log_foundry/_diag.py: rejected(reason: str, value: object) -> None   (unchanged signature)
def _echo(value: object) -> str:
    # str route:      first 64 chars via str.__getitem__, str.__repr__, _escape, cut to 64 + "…"
    # non-str route:  "<" + name (first 65 chars, escaped, cut to 64 + "…", no quotes) + ">",
    #                 name = type.__dict__["__name__"].__get__(type(value))
# rejected calls _echo inside its guard; an Exception from it writes the line with "<?>"

```

## File & Folder Structure

```
src/log_foundry/_diag.py        # rejected
src/log_foundry/ids.py          # parse_traceparent, is_valid_trace_id, is_valid_span_id
src/log_foundry/context.py      # parse_baggage_header
src/log_foundry/decorator.py    # continue_trace
tests/test_diag.py              # FR-001 (extends)
tests/test_ids.py               # FR-003 (extends)
tests/test_inbound_totality.py  # FR-002, FR-004 (new)
```

## Implementation Phases

### Phase 1: The echo

- Rewrite `rejected`'s echo (FR-001) and invert the two tests named there.
- Add the timing tests (FR-002) to the new file.

### Phase 2: The parsers and the entry point

- Switch the four parsers to `type(value) is str` (FR-003).
- Normalise in `continue_trace` and add the argument-by-probe table (FR-004).

### Phase 3: The prose

- Docstrings, the decision file, the superseded marker and the release note (FR-005).
