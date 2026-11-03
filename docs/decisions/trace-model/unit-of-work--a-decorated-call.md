---
type: Decision
title: "Unit of work = a decorated call"
description: "`@log_foundry.trace`; the outermost call starts a trace, every call is a span within it. Named once at decoration, where a misordered descriptor, a non-callable or a generator function is refused. (arch §4, SPEC-055)"
---

**Unit of work = a decorated call** — (`@log_foundry.trace`); outermost call starts a trace, every call is a span within it. (arch §4) **The span name is resolved once, at decoration, and what `@trace` cannot use is refused there** (SPEC-055 FR-002, invariant 13): `__qualname__` where the callable has one, its type's name otherwise, so a `functools.partial` or a callable instance traces rather than raising `AttributeError` into the caller on every call. A `classmethod` or `staticmethod` object is a decorator applied in the wrong order and is refused with a `TypeError` naming the function — `staticmethod` even though it is callable, because the wrapper replaces the descriptor and an instance call would hand `self` to a function declared without one; a `str` is refused with the `name=` hint; anything else not callable by type. The async dispatch consults the type's `__call__`, because accepting instances opened a door the old `AttributeError` kept shut: an instance whose `__call__` is `async def` reads as synchronous to `asyncio.iscoroutinefunction` and would have closed its span before the coroutine ran. **A generator function is refused at decoration, not wrapped** (SPEC-055 FR-003): its body runs after the wrapper has returned the generator object, so the span closed before a line of it ran and every event inside was an orphan on a fresh trace. Wrapping the iteration — a span pushed around every resumption, closed on exhaustion or `close()` — is a feature with semantics no other span has (`duration_ms` counting suspended time, a collector-finalised generator closing in a foreign context, two more twin paths) and was deferred rather than rejected: a refusal can be lifted into a wrap later without breaking anyone, while a wrap shipped first freezes its semantics at 1.0. The check is the code flags, on `fn`, on what it advertises through `__wrapped__` (so `@contextmanager`, `@lru_cache` and a `wraps` wrapper of a generator are refused too) and on the type's `__call__`; a plain function that returns a generator object without advertising it is a stated limit, not detected, because invariant 13 refuses at decoration and not at call time.

## History

### 2026-06-24

- **Settled.**
