---
type: Decision
title: "`@trace` routes what it is handed on its real type, and only a callable runs its own code at decoration"
description: "`@trace` routes the values it is handed (`name=`, a `__qualname__`, a type name, the `str` it refuses) on their real type, keeps a `str` as the exact `str` it holds and reads type names through `type`'s own descriptor; of the decorated object only a callable runs its own code at decoration, bar the `__func__` and `__qualname__` read for a real `classmethod` or `staticmethod`'s message, and the misordered-descriptor test keeps `isinstance`, last, for callables, on purpose. (SPEC-080)"
---

**`@trace` decides what a value is by asking `type`, never the value.** `isinstance(x, T)` falls
back to reading `x`'s own `__class__` when the real type check fails, and `type(x).__name__`
consults a metaclass `__name__` property, so both run code the caller supplied and both can lie.
Invariant 13 promises that `@trace` refuses what it cannot use where the caller wrote it, with a
`TypeError` naming the argument. A lying value broke that promise in two ways: it decorated
cleanly and then ran every call untraced, announcing "absorbed a failure while opening a span"
each time, or it raised its own exception at decoration in place of the `TypeError` that names
the mistake. Every site that reads a value `@trace` is handed therefore routes on
`issubclass(type(x), …)`, copies a `str` it keeps with `str.__str__`, which runs no method of a
subclass, and reads a type's name through `type`'s own `__name__` descriptor
(`decorator._stored_name`, over `_diag._STORED_NAME` and `_diag._STR_STR`). These are the rules
`continue_trace` and `_diag.rejected` follow on the inbound path since SPEC-067 (the sibling
decision *An inbound value runs no code of its own anywhere on the inbound path*, in
`failure-paths`), applied to decoration.

- **`name=` is judged at the `trace(...)` call, in `trace`'s own body**, before `decorate` exists.
  A `str` subclass becomes the exact `str` it holds; `None` stays `None`; anything else raises
  `TypeError("@trace name= must be a str, got <stored type name>")` there, which is invariant
  13's "raises at that call", rather than when the decorator it returns is applied. `decorate`
  reads the exact name from the closure, so the truthiness test that sends an empty name to the
  default runs on an exact `str`, never on a subclass's `__bool__` or `__len__`. Because the
  judgement moved ahead of `decorate`, a call with two bad arguments reports `name=` first:
  `trace("checkout", name=1)` and `trace(fn, name=1)` both say `got int`.
- **The span-name fallback** (`_span_name`) keeps a `__qualname__` only if its real type is a
  `str`, as an exact `str`, and otherwise names the callable by its type's stored name, so a
  metaclass `__name__` property can neither divert nor raise out of it.
- **`_refuse_unusable` runs its checks in a fixed order**, so that of the decorated object only a
  callable runs its own code: (1) a `str` by real type, refused with the `name=` hint echoed
  through `_diag._echo` (its first 64 characters through `str`'s own slots, quoted, escaped and
  cut), first so that no other check reads a `str` subclass; (2) a `classmethod` or
  `staticmethod` by real type, the message naming the type's stored name, so a subclass is told
  its own decorator; (3) `not callable(fn)`, which reads the type's slot and runs nothing of the
  value, the message naming the stored type name; (4) only then `isinstance(fn, (classmethod,
  staticmethod))`, for a callable proxy, told the kind it claims. Both descriptor messages are
  built by `_misordered`, which reads `__func__` and its `__qualname__` through `getattr` and keeps
  the latter only if its real type is a `str`, else writes "the function". Those two reads are the
  one exception, since step 2 runs before `callable` is asked: a real `classmethod` or
  `staticmethod`'s `__func__`, and that object's `__qualname__`, are the caller's code and may
  raise at decoration.
- **`_echo` has two callers that treat a failure differently.** `_diag.rejected` catches an
  `Exception` from computing the echo and writes `<?>`, because its line must be written;
  `_refuse_unusable` lets it propagate into the caller's frame at decoration, which invariant 13
  allows. It is reused rather than copied, so the bound has one definition.

**Why step 4 keeps `isinstance`.** A callable proxy that forwards `__class__`, such as a
`wrapt.FunctionWrapper` around a `classmethod`, is a descriptor whose binding a plain-function
wrapper would lose, so refusing it is right, and only `isinstance` sees it. By step 4 the value is
a callable, and a callable is the caller's code by definition: its `__class__` raising there is a
refusal at decoration, in the caller's frame, never a per-call failure, so no lying shape reaches
the failure this decision exists to prevent through it. **The order's one trade-off, accepted:** a
*non-callable* proxy forwarding `__class__` to a `classmethod` now reaches step 3 and is told
`needs a callable, got <its type>` rather than the descriptor advice. That is still a refusal at
decoration naming the argument, and a callable proxy keeps the descriptor message.

**What is left on purpose.** Once `callable(fn)` holds, the kept `isinstance`, `inspect`,
`asyncio.iscoroutinefunction`, `functools.wraps` and the `__qualname__` read run the callable's
own attributes; each can raise only at decoration. `defaults=` is copied with `dict(defaults)` at
the `trace(...)` call, which runs the mapping's own code at invariant 13's moment, and its values
are assembly's, bounded by SPEC-063. The type name in a decoration-time `TypeError` is read
without running code but is not cut: it is the caller's own class name, raised into the caller's
own frame and never written to stderr by the library, so invariant 11 does not reach it.

**Rejected.**

- *Routing step 4 on real type too.* It would let a callable proxy around a `classmethod` through
  to be wrapped as a plain function, losing the binding: the misordered decorator the refusal
  exists to catch, wrapped silently.
- *Keeping the descriptor test first, where it was.* Run on every argument, it read a `str`
  subclass's or a non-callable's own `__class__` before either was refused: one whose
  `__class__` raised raised that exception, and a `str` subclass whose `__class__` answered
  `classmethod` raised `AttributeError` for its missing `__func__`.
- *Judging `name=` in `decorate`, as before.* The bad argument is written at the `trace(...)`
  call, and a decorator that is built but not yet applied would carry it unrefused.
- *Refusing a `str` subclass `name=`.* Its characters can be read without running any of its
  code, and a subclass that behaves traced before this change; refusing it would break a valid
  caller to stop an invalid one.
- *Reusing `decorator._exact` for `name=`, or `sanitize._type_name` for the type name.* Each
  swallows a failure because its path must be total — `continue_trace` and assembly — where
  decoration is allowed to raise; and `_type_name`'s `"?"` would be a wrong span name.

**Do NOT** call `isinstance`, read `type(x).__name__`, or call anything that dispatches through
the value's own type (`str()`, `repr()`, `bool()`, `len()`, a method) on a value `@trace` is
handed before its real type is known; route on `issubclass(type(x), …)` and read through `str`'s
and `type`'s own slots. Do not move a check ahead of `issubclass(type(fn), str)`, nor the kept
`isinstance` ahead of `callable(fn)`, and do not replace that `isinstance` with a real-type test.
(SPEC-080, invariant 13)

## History

### 2026-11-04

- **Settled.**
