# Trace model and context — decisions

The settled decisions for the trace model — what a unit of work is, how IDs and context are
carried, and what crosses a process boundary. Read the fences; pull an entry only when you need
the reasoning.

## Contents

- [Fences](#fences)
- [Unit of work = a decorated call](#unit-of-work--a-decorated-call)
- [IDs are W3C Trace Context compatible](#ids-are-w3c-trace-context-compatible)
- [Context via `contextvars`](#context-via-contextvars)
- [Cross-process traces are adopted explicitly, never auto-instrumented](#cross-process-traces-are-adopted-explicitly-never-auto-instrumented)
- [Boundary events take the span's *final* baggage; mid-span events keep the moment's](#boundary-events-take-the-spans-final-baggage-mid-span-events-keep-the-moments)
- [Per-request context is released at the root span — baggage restored, an adopted trace context cleared](#per-request-context-is-released-at-the-root-span--baggage-restored-an-adopted-trace-context-cleared)

## Fences

- **Unit of work = a decorated call** — `@log_foundry.trace`; the outermost call starts a trace, every call is a span within it. Named once at decoration, where a misordered descriptor, a non-callable or a generator function is refused. (arch §4, SPEC-055)
- **IDs are W3C Trace Context compatible** — `trace_id` 16B/32hex, `span_id` 8B/16hex, `log_id` UUID, so adopting tracing later stays cheap. (arch §3.1)
- **Context via `contextvars`** — not thread-locals — correct under threads and asyncio; holds a span stack plus baggage. (arch §5)
- **Cross-process traces are adopted explicitly, never auto-instrumented** — no client patching or middleware, which would need the deps the core refuses. Inbound context is untrusted and confers no authority. (SPEC-014)
- **Boundary events take the span's *final* baggage; mid-span events keep the moment's** — backfill only at close. Backfilling everything inverts `build_event`'s precedence and lets baggage beat a per-call field. (SPEC-015)
- **Per-request context is released at the root span — baggage restored, an adopted trace context cleared** — the asymmetry is deliberate: restoring an inbound context puts back an adoption made *before* the span, leaving a warm container joining the first caller's trace forever. (SPEC-024)

---

### Unit of work = a decorated call

**Unit of work = a decorated call** — (`@log_foundry.trace`); outermost call starts a trace, every call is a span within it. (arch §4) **The span name is resolved once, at decoration, and what `@trace` cannot use is refused there** (SPEC-055 FR-002, invariant 13): `__qualname__` where the callable has one, its type's name otherwise, so a `functools.partial` or a callable instance traces rather than raising `AttributeError` into the caller on every call. A `classmethod` or `staticmethod` object is a decorator applied in the wrong order and is refused with a `TypeError` naming the function — `staticmethod` even though it is callable, because the wrapper replaces the descriptor and an instance call would hand `self` to a function declared without one; a `str` is refused with the `name=` hint; anything else not callable by type. The async dispatch consults the type's `__call__`, because accepting instances opened a door the old `AttributeError` kept shut: an instance whose `__call__` is `async def` reads as synchronous to `asyncio.iscoroutinefunction` and would have closed its span before the coroutine ran. **A generator function is refused at decoration, not wrapped** (SPEC-055 FR-003): its body runs after the wrapper has returned the generator object, so the span closed before a line of it ran and every event inside was an orphan on a fresh trace. Wrapping the iteration — a span pushed around every resumption, closed on exhaustion or `close()` — is a feature with semantics no other span has (`duration_ms` counting suspended time, a collector-finalised generator closing in a foreign context, two more twin paths) and was deferred rather than rejected: a refusal can be lifted into a wrap later without breaking anyone, while a wrap shipped first freezes its semantics at 1.0. The check is the code flags, on `fn`, on what it advertises through `__wrapped__` (so `@contextmanager`, `@lru_cache` and a `wraps` wrapper of a generator are refused too) and on the type's `__call__`; a plain function that returns a generator object without advertising it is a stated limit, not detected, because invariant 13 refuses at decoration and not at call time.


### IDs are W3C Trace Context compatible

**IDs are W3C Trace Context compatible** — `trace_id` 16B/32hex, `span_id` 8B/16hex, `log_id` UUID; makes future trace adoption cheap. (arch §3.1)


### Context via `contextvars`

**Context via `contextvars`**, not thread-locals — correct under threads and asyncio; holds a span stack + baggage. (arch §5)


### Cross-process traces are adopted explicitly, never auto-instrumented

**Cross-process traces are adopted explicitly, never auto-instrumented** — `continue_trace()` takes a W3C `traceparent`/baggage the *caller* moved; no client patching or middleware, which would need the deps the core refuses. Inbound context is untrusted and confers no authority. (SPEC-014, arch §12)


### Boundary events take the span's *final* baggage; mid-span events keep the moment's

**Boundary events take the span's *final* baggage; mid-span events keep the moment's** — one backfill at close completes `span.start`/`span.end` (which describe the whole span and carry the outcome), while an `info` is left exactly as it was emitted. Backfilling everything would also invert `build_event`'s precedence by letting baggage beat a per-call field. (SPEC-015)


### Per-request context is released at the root span — baggage restored, an adopted trace context cleared

**Per-request context is released at the root span — baggage restored, an adopted trace context cleared** — the asymmetry is deliberate. Baggage set before any span is a process-level default, so it is restored *to*; an inbound context is a one-shot handoff to the trace it names, and restoring it would put back an adoption made *before* the span, leaving a warm container joining the first caller's trace forever. Consequences: one `continue_trace()` serves one root span (a batch needs one per record, or one `@trace` entry point), and the release lands in the context the span's `finally` runs in — so adopting outside a span and dispatching into an `asyncio.Task` needs `reset_context()`, recorded as a constraint in arch §13. Nested spans never reset: "at or below" is where baggage starts, the root span's close is where it stops. (SPEC-024, arch §5.1)


