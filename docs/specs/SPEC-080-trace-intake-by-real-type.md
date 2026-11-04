---
type: Spec
id: SPEC-080
title: "@trace Intake by Real Type — a Lying Argument Is Refused or Traced at Decoration, Never Absorbed on Every Call"
description: "Makes @trace route its name= argument, the span-name fallback and its refusal messages on each value's real type, keeping exact str, so no argument decorates cleanly and then fails every call."
spec_status: Draft
depends_on: [SPEC-055, SPEC-063, SPEC-067]
---

## Overview

`@trace` promises to refuse an argument it cannot use at decoration, where the caller wrote it,
with a `TypeError` that names the argument (invariant 13). Four of its checks decide what a value
is by asking the value: `isinstance` reads the value's own `__class__`, and `type(x).__name__` reads
a metaclass `__name__` property. So an argument that misreports its type either decorates cleanly
and then runs every call untraced, announcing `log-foundry: absorbed a failure while opening a span
(TypeError); this call runs untraced` each time, or raises its own exception at decoration in place
of the `TypeError` that names the mistake. A callable that merely claims to be a `str` is refused
with advice that makes no sense for it. This spec routes each of those checks on the value's real
type, keeps a `str` subclass as the exact `str` it holds, and reads type names through `type`'s
own descriptor, as `continue_trace` and `_diag.rejected` already do since SPEC-067. A well-behaved
`str` name, a plain function and every refusal a caller can hit by accident behave exactly as
today.

Every probe below was measured by the drafting session at `253432f`, through the public
`log_foundry.trace` with a configured sink, in `src/log_foundry/decorator.py` at that commit. A
"spy" is a property or metaclass hook that appends to a list before it returns or raises.

- **Probe A: `name=` claiming `str` decorates, then absorbs every call.** `decorate`'s check
  (line 901) is `isinstance(name, str)`. A `name=` object whose `__class__` property returns `str`
  decorates; two calls return their values, write the absorbed line twice and emit no
  `span.start`. A raising `__class__` property raises its own `RuntimeError` at decoration. A
  metaclass `__name__` property returning `"str"` turns the refusal into `@trace name= must be a
  str, got str`.
- **Probe B: a `str` subclass `name=` runs its own code at decoration.** It is kept as the
  subclass, and `span_name = name or _span_name(fn)` (line 903) tests its truthiness, calling its
  `__bool__` (else `__len__`): one whose `__bool__` raises raises that out of decoration, sync and
  async alike; one without a `__bool__` has its `__len__` run once. Per-call assembly already
  writes an exact `str`: a subclass spying on every `str` dunder recorded no call across two
  calls, and the events carried an exact `str`.
- **Probe C: the span-name fallback reads a metaclass `__name__` property.** `_span_name`
  (line 812) returns `qualname if isinstance(qualname, str) else type(fn).__name__`. A callable
  instance whose metaclass `__name__` property returns `1` decorates, and two calls write the
  absorbed line twice and emit no `span.start`. One whose property raises raises its
  `RuntimeError` at decoration, for a sync, an async and a generator `__call__` (the generator
  refusal's message calls `_span_name`, line 736). A class whose stored `__name__` is a `str`
  subclass makes `_span_name` return that subclass. A `__qualname__` whose `__class__` property
  returns `str` is refused by `functools.wraps`, correctly, but only after `isinstance` ran the
  property.
- **Probe D: the `str` slip runs the value's `repr`, unbounded, and catches a callable.**
  `_refuse_unusable` (line 728) tests `isinstance(fn, str)` and renders the hint as `{fn!r}`. A
  `str` subclass whose `__repr__` raises raises its `RuntimeError` at decoration; a
  10,000-character `str` produces a 10,069-character message. A callable whose `__class__`
  property returns `str` is refused with `got str; pass a span name as
  @trace(name=<__main__.CallableClaimsStr object at 0x…>)`.
- **Probe E: the misordered-descriptor message reads attributes it has not checked.** Line 721
  tests `isinstance(fn, (classmethod, staticmethod))`, then reads `type(fn).__name__` for the
  decorator to move and `fn.__func__` unguarded. An object claiming `classmethod` with no
  `__func__` raises `AttributeError: 'LiarCM' object has no attribute '__func__'`; one claiming
  `staticmethod` is told `put @LiarSM above @trace`; a `__func__.__qualname__` of `5` is written
  as `around 5`; a real `classmethod` around a function whose `__qualname__` is a `str` subclass
  with a raising `__format__` raises that `RuntimeError`. The non-callable refusal (line 733)
  writes `type(fn).__name__`, so a raising metaclass `__name__` property raises its
  `RuntimeError` there instead of the `TypeError`.

The fix shape was applied temporarily to `decorator.py` at `253432f`, the probes re-run, and the
file restored:
every row above then refuses with the `TypeError` this spec names, or traces under the name it
names, with each spy empty bar the callable's own `__class__` reads noted in FR-003.

## Scope

### In Scope

- `name=` routes on `issubclass(type(name), str)` and is kept as an exact `str` (FR-001).
- `_span_name` routes `__qualname__` on its real type and falls back to the type's stored name, as
  an exact `str` (FR-002).
- The `str` slip is detected by real type and its hint rendered bounded, through `str`'s own slots;
  a callable that claims `str` is traced (FR-003).
- The misordered-descriptor and non-callable messages are built without reading anything unchecked
  (FR-004). The `isinstance` test at that site is **kept**, deliberately (FR-004's Description).
- The docstrings of `trace`, `decorate`, `_refuse_unusable` and `_span_name` state the new routing
  (a criterion in each FR that changes them).
- **Completion deliverables** (the completion ritual's steps, not FRs):
  - `docs/architecture.md` §12's open item "**`@trace(name=...)` tests its argument with
    `isinstance(name, str)`**" moves to *Resolved*, closed by this spec, naming the sibling sites
    this spec also closed.
  - One new decision file in `docs/decisions/failure-paths/`. Its fence states: *`@trace` routes
    the values it is handed (`name=`, a `__qualname__`, a type name, the `str` it refuses) on their
    real type, keeps a `str` as the exact `str` it holds and reads type names through `type`'s own
    descriptor; the callable itself may still run its own code at decoration, and the
    misordered-descriptor test keeps `isinstance` on purpose.* Its entry carries the reasoning in
    FR-004's Description for that exception, and a dated History.
  - `docs/release-notes/v1.1.0.md` gains a subsection at the end of `## Fixed`, after the existing
    entries, naming the caller-visible changes: a `name=` that misreports its type is refused at
    decoration rather than running every call untraced; a callable that misreports itself as `str`
    is traced rather than refused; a misordered-descriptor proxy is told the decorator by its
    claimed kind; and the refusal messages differ for these exotic arguments only. It ends with a
    "Who must change code" line saying no caller must.

### Out of Scope

- **`defaults=` intake.** `dict(defaults)` runs the mapping's own code at decoration, which is
  invariant 13's moment, so a hostile mapping is already refused where it is written; its values
  are assembly's, bounded by SPEC-063.
- **The callable's own code at decoration.** `callable`, `inspect`, `asyncio.iscoroutinefunction`,
  `functools.wraps`, `getattr(fn, "__qualname__")`, `getattr(fn, "__func__")` and the kept
  `isinstance` read the callable's attributes, and the callable is the caller's code by
  definition; the existing docstrings already state that these raise at decoration, in the
  caller's frame. That is a refusal where the argument is written, not a per-call failure.
- **The inbound path** (`continue_trace`, the parsers, `_diag.rejected`). Settled by SPEC-067.
- **Bounding the type name in a decoration-time `TypeError`.** It is the caller's own class name,
  raised into the caller's own frame, never written to stderr by the library (invariant 11 governs
  diagnostics); it is read through `type`'s descriptor so no code runs, and is not cut.
- **Per-call span-name assembly.** Already writes an exact `str` (Probe B); unchanged.
- **Public surface.** No signature, name or `configure()` key changes (invariant 10).

### Twin paths (invariant 6)

- **The sync and async wrappers.** Both read the one `span_name` computed in `decorate` before the
  branch, so FR-001 and FR-002 each test an `async def` beside a plain function.
- **`_span_name`'s two callers.** `decorate` and the generator refusal's message (line 736); FR-002
  tests both.
- **`decorator._exact`** is the sibling rule on the inbound path. It is not reused for `name=`,
  because it swallows a failed copy so that `continue_trace` stays total, where decoration is
  allowed to raise (FR-001).

---

## Functional Requirements

Tests extend `tests/test_decorator_sync.py`, beside the SPEC-055 refusal tests invariant 13 already
names. A hook that raises from a metaclass `__name__` property is armed only around the call under
test, as `_NamelessMeta` is in `tests/test_baggage_header.py`, because pytest's `saferepr` reads
`cls.__name__` and an always-raising one turns a failure into an INTERNALERROR. Every criterion
marked *fails at base* fails against `253432f`.

### FR-001: `name=` routes on its real type and is kept as an exact `str`

#### Description:

In `decorate`, a `name=` whose real type is a `str` subclass (`issubclass(type(name), str)`)
becomes the exact `str` it holds through `str.__str__(name)`, which runs no method of the subclass;
`None` stays `None`; anything else raises `TypeError(f"@trace name= must be a str, got {N}")`,
where `N` is the type's stored name read through `_diag._STORED_NAME` and made an exact `str`
through `_diag._STR_STR`. The truthiness test `name or _span_name(fn)` then runs on the exact
`str`, so an empty name still falls back to the qualified name. The copy is not guarded: a
`MemoryError` from copying an enormous subclass propagates at decoration, which invariant 13
allows, where `_exact` swallows it only because `continue_trace` must be total.

#### Acceptance Criteria:

- [ ] A `name=` whose `__class__` property returns `str` raises `TypeError` at decoration matching
      `name= must be a str, got <its real class name>`, the `__class__` spy is empty, and stderr is
      empty: there is no decorated function to write the absorbed line. *Fails at base (Probe A).*
- [ ] A `name=` whose `__class__` property raises gives the same `TypeError`, spy empty. *Fails at
      base: `RuntimeError`.*
- [ ] A `name=` whose class's metaclass `__name__` property returns `"str"` is refused naming the
      class's stored name, and the metaclass spy is empty. *Fails at base: `got str`.*
- [ ] A `str` subclass `name=` whose `__bool__`, `__len__`, `__str__`, `__repr__`, `__format__` and
      `__eq__` all raise decorates a plain function and an `async def`; two calls of each emit two
      `span.start` events whose `function` equals the held text with `type(...) is str`, stderr
      is empty, and the spy is empty. *Fails at base: `__bool__`'s `RuntimeError` at decoration.*
- [ ] An empty `str` subclass `name=` with a raising `__bool__` traces under the function's
      qualified name. *Fails at base.*
- [ ] An exact `str` `name=`, `name=None` and `name=1` behave as today: the given name, the
      qualified name, and `name= must be a str, got int` (passes at base).
- [ ] `trace`'s and `decorate`'s docstrings say `name=` is routed by its real type and a subclass
      kept as its exact `str`, and `poetry run python scripts/docstring-lint.py` is clean.
- [ ] Serves invariants 13 and 6.

### FR-002: The span-name fallback routes on real type and reads the stored type name

#### Description:

`_span_name(fn)` reads `getattr(fn, "__qualname__", None)` as today. If that value's real type is
a `str` subclass it returns `str.__str__(qualname)`; otherwise it returns
`_diag._STR_STR(_diag._STORED_NAME(type(fn)))`, the type's stored name as an exact `str`, which a
metaclass `__name__` property or `__getattribute__` cannot divert. A `__qualname__` that is not a
`str` by real type is still refused by `functools.wraps` at decoration, as today, but the route no
longer runs its `__class__` on the way.

#### Acceptance Criteria:

- [ ] A callable instance whose class's metaclass `__name__` property returns `1` decorates; two
      calls emit two `span.start` events with `function` equal to the class's stored name, and
      stderr carries no `absorbed a failure while opening a span` line. *Fails at base (Probe C).*
- [ ] With that metaclass property raising instead, a sync and an `async` callable instance each
      decorate and trace under the stored name, and the spy is empty. *Fails at base:
      `RuntimeError` at decoration.*
- [ ] A callable instance whose `__call__` is a generator and whose metaclass `__name__` property
      raises is refused with the generator `TypeError` naming the stored class name. *Fails at
      base: `RuntimeError`.*
- [ ] `_span_name` of an instance whose class's stored `__name__` is a `str` subclass, and of a
      function whose `__qualname__` is one, returns a value with `type(...) is str` equal to the
      held text, and no method of the subclass runs. *Both fail at base, which returns the
      subclass itself.*
- [ ] An instance whose `__qualname__` has a `__class__` property returning `str` is still refused
      with a `TypeError` matching `__qualname__`, and the `__class__` spy is empty. *The spy half
      fails at base.*
- [ ] A plain function, a `functools.partial` (`partial`) and an ordinary callable instance (its
      class name) trace under the same names as today; the existing SPEC-055 tests pass unchanged.
- [ ] `_span_name`'s docstring states the real-type route and the stored-name fallback;
      `docstring-lint.py` is clean.
- [ ] Serves invariants 13 and 6.

### FR-003: The `str` slip is refused by real type, with a bounded hint, and a callable claiming `str` is traced

#### Description:

`_refuse_unusable` tests `issubclass(type(fn), str)` in place of `isinstance(fn, str)`, and renders
the hint with `_diag._echo(fn)`, which for a `str` takes its first 64 characters through
`str.__getitem__`, quotes them with `str.__repr__`, escapes and cuts with `…`, running no method of
a subclass. For `"checkout"` the message is unchanged. A callable whose `__class__` claims `str` no
longer matches and is traced like any callable instance; its own `__class__` property still runs at
decoration through the other checks (Out of Scope), measured at 25 reads at decoration and none
per call with the fix shape applied.

#### Acceptance Criteria:

- [ ] `@trace("checkout")` raises `TypeError` whose message ends `pass a span name as
      @trace(name='checkout')`, written out as a literal (passes at base).
- [ ] A `str` subclass holding `"checkout"` whose `__repr__`, `__getitem__` and `__len__` raise is
      refused with that same message, and the spy is empty. *Fails at base: `RuntimeError`.*
- [ ] `"x" * 10_000` is refused with a message ending `@trace(name='` followed by 63 `x`, `…` and
      `)`, written out as a literal. *Fails at base: the message is 10,069 characters.*
- [ ] A callable instance whose `__class__` property returns `str` decorates; two calls return its
      value, emit two `span.start` events with `function` equal to its class's stored name, and
      stderr is empty. *Fails at base: refused with `got str`.*
- [ ] `_refuse_unusable`'s docstring states the real-type test and the bounded hint;
      `docstring-lint.py` is clean.
- [ ] Serves invariant 13.

### FR-004: The misordered-descriptor and non-callable refusals read nothing they have not checked

#### Description:

The test `isinstance(fn, (classmethod, staticmethod))` **stays `isinstance`**. A proxy that forwards
`__class__`, such as a `wrapt.FunctionWrapper` around a `classmethod`, is a descriptor whose binding
a plain-function wrapper would lose, so refusing it is right; and every outcome at that test is
already a refusal at decoration or the callable's own `__class__` raising there, so no lying shape
can reach a per-call failure through it. What changes is the message:

- **`kind`**: the type's stored name, through `_diag._STR_STR(_diag._STORED_NAME(type(fn)))`, when
  `issubclass(type(fn), (classmethod, staticmethod))`, so a real subclass is told its own decorator
  as today; otherwise `"classmethod"` if `isinstance(fn, classmethod)`, else `"staticmethod"`, the
  kind the proxy claims.
- **`inner`**: `q = getattr(getattr(fn, "__func__", None), "__qualname__", None)`, then
  `str.__str__(q)` if `issubclass(type(q), str)`, else `"the function"`.
- **The non-callable refusal** writes `got {N}` with `N` the stored type name, as FR-001 reads it.

#### Acceptance Criteria:

- [ ] An object whose `__class__` returns `classmethod` and that has no `__func__` raises `TypeError`
      matching `cannot wrap the classmethod object around the function; put @classmethod above
      @trace`, and stderr is empty. *Fails at base: `AttributeError`.*
- [ ] One whose `__class__` returns `staticmethod`, with `__func__` a function `g`, is told
      `cannot wrap the staticmethod object around g; put @staticmethod above @trace`. *Fails at
      base: it names the proxy's class.*
- [ ] One claiming `classmethod` whose `__func__.__qualname__` is `5` is told `around the function`.
      *Fails at base: `around 5`.*
- [ ] A real `classmethod` around a function whose `__qualname__` is a `str` subclass with a raising
      `__format__` and `__str__` is refused naming the held text, spy empty. *Fails at base:
      `RuntimeError`.*
- [ ] A real subclass `MyCM(classmethod)` is told `put @MyCM above @trace`, and a plain
      `classmethod` and `staticmethod` keep today's messages (the existing tests pass unchanged).
- [ ] A non-callable instance whose metaclass `__name__` property raises is refused with `TypeError`
      matching `needs a callable, got <its stored class name>`, spy empty. *Fails at base:
      `RuntimeError`.* `object()` still gives `got object`.
- [ ] `_refuse_unusable`'s docstring says why the descriptor test keeps `isinstance`;
      `docstring-lint.py` is clean.
- [ ] Serves invariant 13.

---

## API / Interface Contract

No signature changes and no new public name. The four sites in `src/log_foundry/decorator.py`:

```python
# decorate (FR-001)
if issubclass(type(name), str):
    exact_name: str | None = str.__str__(name)
elif name is not None:
    raise TypeError(f"@trace name= must be a str, got {_stored(type(name))}")
else:
    exact_name = None
span_name = exact_name or _span_name(fn)

# _span_name (FR-002)
qualname = getattr(fn, "__qualname__", None)
return str.__str__(qualname) if issubclass(type(qualname), str) else _stored(type(fn))

# _refuse_unusable (FR-003, FR-004)
#   classmethod/staticmethod: isinstance kept; kind and inner as FR-004 states
#   issubclass(type(fn), str) -> hint is _diag._echo(fn)
#   not callable              -> f"got {_stored(type(fn))}"

# _stored(t) = _diag._STR_STR(_diag._STORED_NAME(t)); a local helper or inline, the builder's call
```

## File & Folder Structure

```
src/log_foundry/decorator.py     # decorate, _span_name, _refuse_unusable, trace's docstring
tests/test_decorator_sync.py     # FR-001 to FR-004 (extends)
```

## Implementation Phases

### Phase 1: Real-type routing

- FR-001 and FR-002 with their tests, sync and async.
- FR-003 and FR-004 with their tests; the existing SPEC-055 refusal tests stay green unchanged.
- The docstrings each FR names.
