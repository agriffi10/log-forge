---
type: Decision
title: "An argument door asks the type, never the value"
description: "an argument door — `configure()`'s stamps and ceilings, the `_retry` validators, `OTLPSink`'s endpoint, `_args.require_callable` — tests `issubclass(type(v), T)`, never `isinstance`, and names the type as `_diag._STR_STR(_diag._STORED_NAME(type(v)))`, never `type(v).__name__`, so no `__class__` or metaclass `__name__` the caller wrote runs and the message names the argument and the real class. The only `isinstance` on a door's parameter is `decorator._refuse_unusable`'s two kept proxy tests, held as an exact multiset; `@trace`'s half is SPEC-080's decision in `trace-model`. A guard holds the doors as an exact set of ten. (SPEC-071)"
---

**A door that refuses an argument by its type decides the type by asking `type`, never the
value.** `isinstance(v, T)` falls back to reading `v`'s own `__class__` when the real type check
fails, and `type(v).__name__` consults a metaclass `__name__` property, so both run code the caller
supplied and both can lie. Invariant 13 promises a bad argument is refused where it is written,
with the argument named. Probed at `4fbfd60`, a lying value broke the "named" half at every one of
the seven doors this decision covers: `configure(service=x)` with `x`'s `__class__` property
returning `str` raised `descriptor '__str__' requires a 'str' object but received a 'Claims'`,
which does not name `service`; a raising `__class__` raised its own exception out of every door;
and a metaclass `__name__` property returning `"str"` gave `service must be a str, got str`. Every
lying value was still stopped at the call, so only the message broke the promise.

**What every door does.** The test is `issubclass(type(v), T)` with `T` unchanged (`str`, `int` or
`int | float`); `type()` reads the real type and `issubclass` walks its MRO through `T`'s own
metaclass, so neither runs the value's `__class__`. The name is
`_diag._STR_STR(_diag._STORED_NAME(type(v)))`, the expression `decorator._stored_name` returns:
`type`'s own `__name__` descriptor ignores a metaclass property, and `str.__str__` copies a
`str`-subclass name without running its methods. For a type with no metaclass `__name__` property
the stored name *is* `type(v).__name__`, so an honest wrong type gets byte-identical text and the
existing message tests passed unedited. The doors are `config._require_text` and
`_require_positive`, `sinks/_retry.py`'s `exact_seconds`, `require_positive`, `exact_int` and
`wait_budget`, `OTLPSink.__init__`'s endpoint check, and `sinks/_args.require_callable`, whose
`callable(v)` reads the type's call slot and runs nothing of the value. `_retry._exact_number`
picks `int.__index__` or `float.__float__` on the real type too, so a real `float` subclass whose
`__class__` claims `int` is read as the float it holds, as SPEC-065 meant for every member of the
declared type, where it used to raise `int.__index__`'s descriptor error.

**The twin paths hold one verdict.** `flush()` and `shutdown()` both judge `timeout` through
`wait_budget`, and `config._require_positive` and `_retry.require_positive` give one verdict on one
value, so each lying shape gets the same refusal from both of each pair. `@trace`, invariant 13's
third entry point, was routed this way by SPEC-080; its decision, *`@trace` routes what it is
handed on its real type, and only a callable runs its own code at decoration*, in
`docs/decisions/trace-model/`, is this rule's other half, and the guard below holds its two doors
too.

**Sites routed the same way that are not doors.** The HTTP sinks' `auth` (`http._valid_auth`,
`http._authorization`) now tests `issubclass(type(auth), str)`, so a value that is not really a
`str` takes the pair route without its `__class__` or `__contains__` running. That route already
turned a value that cannot be unpacked as a pair into `ValueError("<owner> auth cannot be sent
over HTTP (TypeError)")`, naming `auth`, and it accepts anything that unpacks into two items,
running its `__iter__`: a value claiming `str` that also defines `__iter__` is accepted and sent as
Basic auth. A real `str` and a real pair are judged as before. A one-shot iterable `auth`, which
this spec left passing construction and failing every `emit`, is now unpacked once at construction
(*`auth` beside an `Authorization` header is refused, never resolved by precedence*). `SQSSink`'s constant group id is told apart by real type and read
through `str.__str__`, at the constructor and per event, so a callable claiming `str` is still
asked and a `str` subclass's own `strip` never runs.

**The guard.** `tests/test_type_door_roster.py` derives from the AST of `src/log_foundry/**/*.py`.
A door is a function raising a `TypeError` that names, through the stored name, the type of one of
its parameters other than `self` or `cls`; the derived set must equal ten written out by name, so a
door removed, renamed or inlined back reddens by name and a new one is a visible edit. It reports a
`raise TypeError` formatting `type(x).__name__` or `x.__class__.__name__` for a non-`self` `x`,
outside two exempt raises held exactly (`sanitize._coerce_unbounded` and `TransformSink._apply`,
which judge a transform's *result* once per event, where invariants 2 and 8 govern and 13 does
not), and any `isinstance` on a door's parameter, or on the parameter of a same-module function the
door passes it to. Its one exemption is `decorator._refuse_unusable`'s kept proxy test, SPEC-080's
on purpose, held as an exact multiset of its two expressions after the function's `not callable(fn)`
refusal, so a rewritten earlier test or a second copy of an exempt one is still reported. A mutation
sweep rewrites every door back to `isinstance` and `type(p).__name__` in memory and requires a
report for each. **What it does not see**, each a deliberate edge its corpus asserts silence on: a
type named by a helper (`raise TypeError(_type_name(value))`), a message held in a local and raised
later, a `ValueError` door such as `auth`'s, a `TypeError` naming no type, and an argument rebound
to a local before it is tested (`v = value; isinstance(v, int)`), since the guard follows a door's
parameters by name.

**Rejected.**

- *Rewriting SPEC-080's decision to cover both.* Its title and its area are about `@trace`; a
  reader working on a sink or on `configure()` would not look in `trace-model`.
- *Importing `decorator._stored_name`.* `decorator` imports `model`, which imports `config`, so
  `config` cannot import it; the doors use `_diag`, a leaf, directly.
- *A `TypeError` door for `auth`.* The pair route already refuses a value that cannot be unpacked
  as a pair with a `ValueError` naming `auth`, and a new door would change the message an honest
  wrong type gets today.
- *A count of doors rather than a set.* A door removed and another added cancel in a count.
- *Taking `usable_timeout`'s comparison.* It is floor-side by the bad-argument decision, and a
  non-number still raises the comparison's own `TypeError` there, now for a lying value too.

**Do NOT** call `isinstance`, read `type(v).__name__` or `v.__class__`, or call anything that
dispatches through the value's own type on a door's parameter before its real type is known; add a
door without adding it to the guard's pinned set; or extend the guard's exemptions beyond the two
named raises and the one multiset. (SPEC-071, invariants 13 and 6)

## History

### 2026-11-07

- **Updated (HTTP auth fix, no spec).** The one-shot `auth` this entry recorded as open is closed:
  the pair is copied once at construction. The sentence saying so now points to that decision.
- **Settled (SPEC-071 FR-005).** Closed the `architecture.md` §12 entry SPEC-080's frame-2 review
  recorded, at Andrew's direction, deferred from 1.1.0. Written as a sibling of SPEC-080's decision
  rather than a rewrite of it, since that one is about `@trace`.
