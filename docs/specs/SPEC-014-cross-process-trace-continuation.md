# Spec: Cross-Process Trace Continuation (W3C `traceparent` + baggage propagation)

**ID:** SPEC-014
**Status:** Draft
**Last Updated:** 2026-07-22
**Depends On:** SPEC-001 (span model, ids, context), SPEC-002 (baggage), SPEC-013 (the serverless consumer that motivates it)

## Overview

A trace stops at the process boundary. `@trace` mints a fresh `trace_id` whenever there is no
span already open in the current context, so two processes cooperating on one logical operation —
an HTTP client and its server, a queue producer and its consumer, nine Lambdas in one state
machine — produce N unrelated traces with no way to join them but a field the user remembered to
set by hand.

This is the deferral `architecture.md` §12 recorded as *resolved-but-later*: "Cross-service trace
continuation (adopting an inbound `trace_id` from a `traceparent` request header, plus
cross-process baggage — the full Correlation ID Journey) → deferred to a later version. ID formats
are already W3C-compatible (§3.1) to make this cheap." `ids.py`'s own docstring makes the same
promise — the wire formats were chosen so this feature "is just a header parse."

This spec cashes that in, in both directions: **`current_traceparent()`** so a process can publish
where it is, and **`continue_trace()`** so the next one can pick it up. Shipping only the consuming
half would be a feature nobody can use — a caller cannot propagate a context it has no public way
to read.

The immediate consumer is the `s3-upload-portal` data-quality control plane (its SPEC-128), where
one run is nine Lambda invocations and today produces nine traces correlated only by a hand-set
`request_id` baggage field. But nothing here is serverless-specific: the same two calls join an
HTTP client to a server, or a Celery caller to its worker.

## Scope

### In Scope

- `continue_trace()` — adopt an inbound trace context from a W3C `traceparent` string, or from
  explicit `trace_id` / `parent_span_id` arguments.
- Re-parenting an **already-open root span**, so the ergonomic `@trace`-decorated entry point with
  a first-line `continue_trace()` works correctly.
- `current_traceparent()` and `current_trace_context()` — the producer side.
- Cross-process **baggage** propagation via the W3C `baggage` header format, completing the
  "Correlation ID Journey" §12 names.
- Strict validation of inbound context, which is untrusted input by definition.

### Out of Scope

- **Sampling.** `traceparent`'s flags byte carries a sampled bit; this library records everything
  and has no sampling decision to make or honour. FR-002 defines how the byte is read and written,
  and nothing acts on it.
- **`tracestate`.** The vendor-extension header is a separate W3C concept with its own mutation
  rules, and no consumer needs it. Not parsed, not emitted, not round-tripped.
- **Automatic propagation.** No HTTP client patching, no framework middleware, no boto3 hooks. The
  caller moves the header; the library only reads and writes it. Auto-instrumentation is a
  different product and would drag in the dependencies the core deliberately does not have.
- **"Follows-from" span relationships** for fire-and-forget work (architecture.md §12's other
  deferral). Still deferred; unaffected by this spec.
- **Changing how a root span is minted when there is no inbound context.** Absent a
  `continue_trace()` call, behaviour is byte-for-byte what it is today.

---

## Functional Requirements

### FR-001: `continue_trace()` adopts an inbound context

#### Description

One entry point, two input shapes, because callers arrive with either a header string (HTTP, or a
value passed verbatim through a queue) or the two ids already in hand (a state-machine payload
field).

#### Acceptance Criteria:

- [ ] `continue_trace(traceparent: str | None = None, *, trace_id: str | None = None,
      parent_span_id: str | None = None) -> bool` is public, exported from
      `log_foundry.__init__`, in `__all__`, and passes `mypy --strict`.
- [ ] Called with a valid `traceparent`, subsequent root spans use its `trace_id` and take its
      span id as their `parent_span_id`. A test asserts a span opened after the call carries both.
- [ ] Called with explicit `trace_id` / `parent_span_id`, the same. Passing `traceparent`
      **and** explicit ids is a programming error: `traceparent` wins and a warning is logged
      (never raises — FR-004).
- [ ] `parent_span_id` may be omitted with `trace_id` given: the adopting span joins the trace as
      **another root** (`parent_span_id=None`). A comment records that this is legitimate — a
      consumer that knows the trace but not the specific parent span is better off in the right
      trace than in a fresh one.
- [ ] Returns `True` when a context was adopted and `False` when it was not (nothing supplied, or
      validation failed and a fresh trace was used instead), so a caller can assert on propagation
      in its own tests.
- [ ] The adopted context is stored in a `contextvars.ContextVar`, following `context.py`'s
      existing rules: never mutate a default mutable, use `.set()`. It is therefore correct under
      threads and `asyncio` for free, and a task that adopts a context does not leak it to siblings.
- [ ] `_open_span` consults the adopted context **only when there is no current span** — a nested
      call still inherits from its in-process parent. A test asserts a nested span under an adopted
      root takes the root's span id as parent, not the inbound one.

### FR-002: The `traceparent` format, parsed strictly and written back correctly

#### Description

`traceparent` is `version-trace_id-span_id-flags`, e.g.
`00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`. The W3C spec is specific about what is
invalid, and about forward compatibility — getting this wrong produces ids that are silently
incompatible with every other tracing tool, which is the entire reason `ids.py` chose these formats.

#### Acceptance Criteria:

- [ ] A `trace_id` of exactly 32 lowercase hex characters and a `span_id` of exactly 16 are
      required. **All-zero ids are invalid** per the W3C spec and are rejected. Tests cover
      wrong length, uppercase hex, non-hex characters, and both all-zero cases.
- [ ] Version `00` is parsed as the 4-field form. A **higher** version is not rejected: per the
      W3C forward-compatibility rule, the first three fields are parsed and any extra fields are
      ignored. A comment records that rule, because "reject anything that isn't 00" is the
      intuitive-and-wrong reading. A test covers a `01-…` header with a trailing field.
- [ ] An **unparseable or invalid** `traceparent` is not adopted, and a fresh trace is minted as
      though nothing had been passed (FR-004 governs the reporting).
- [ ] `current_traceparent()` emits version `00` and flags `01`. A comment records why flags are
      hard-coded: `01` is the sampled bit, this library records every span, so "sampled" is always
      true — and an inbound flags byte is deliberately **not** round-tripped, because honouring
      another system's sampling decision would mean dropping spans, which this library does not do.
- [ ] Parsing and formatting live in `ids.py` alongside the generators whose formats they mirror —
      not in `context.py` or `api.py` — keeping the one-concept-per-module rule intact.

### FR-003: Re-parenting an already-open root span

#### Description

The ergonomic pattern is a decorated entry point whose first line adopts the caller's context:

```python
@lf.trace
def handler(event, context):
    lf.continue_trace(event["traceparent"])
```

By that first line the root span is **already open** — `@trace` opened it before the body ran — so
setting a contextvar for "the next span" would leave the handler's own span in a fresh, unrelated
trace. `continue_trace()` therefore also re-parents the current span when that span is a root.

**There is a trap here, and it is the reason this is its own FR.** `decorator._open_span` appends
`start_event(span)` at open time (`decorator.py:53`), and `model.build_event` **snapshots**
`trace_id` / `span_id` / `parent_span_id` into the event dict (`model.py:70-72`). Re-parenting only
the `Span` dataclass would leave the buffered start event carrying the **old** trace id — so one
span would emit its start on trace A and its end on trace B. A split trace is worse than no
continuation at all, because it looks like data rather than a bug.

#### Acceptance Criteria:

- [ ] When `continue_trace()` is called with a current span whose `parent_span_id is None` (a
      root), that span's `trace_id` and `parent_span_id` are updated in place.
- [ ] **Every event already buffered in that span is rewritten** to the new `trace_id` and, where
      it recorded the span's parent, the new `parent_span_id`. A test asserts the emitted batch
      for such a span has **one** distinct `trace_id` across the start and end events — written so
      it fails against an implementation that updates only the dataclass.
- [ ] When the current span is **not** a root (it has a parent), the call does **not** re-parent
      it. A comment records why: a nested span already belongs to an in-process trace, and moving
      it would sever it from its own parent. The adopted context still applies to the next root
      span opened in that context, and the call returns `True`.
- [ ] Called with **no** span open, nothing is re-parented and the context applies to the next root
      span. A test covers this, since it is the non-decorated-handler pattern.
- [ ] The `continue_trace()` docstring states exactly when re-parenting happens — current span
      exists **and** is a root — rather than describing it as "adopts the trace". The behaviour is
      deliberately convenient and therefore has to be documented precisely.
- [ ] `span_id` is **never** overwritten. The adopting span keeps its own identity and takes the
      inbound span as its parent; overwriting would give two processes the same span id and break
      any parent/child reconstruction downstream.

### FR-004: Inbound context is untrusted, and the library still never raises

#### Description

A `traceparent` arrives from outside the process — an HTTP header, a queue body, an event payload.
It may be absent, truncated, the wrong type entirely, or attacker-supplied. Two rules apply, and
they are the library's existing posture rather than anything new: **never raise** (a logging call
must not be why a caller's function fails) and **never emit a malformed id** (a non-hex `trace_id`
written into the event stream corrupts the field for every downstream consumer, not just this span).

#### Acceptance Criteria:

- [ ] Invalid input is never fatal. A test passes `None`, `""`, a truncated header, a non-hex
      header, an all-zero id, an `int`, and a `dict` — each returns `False`, mints a fresh valid
      trace, and raises nothing.
- [ ] A rejected context writes **one** warning to stderr naming the reason, consistent with how
      `worker` and `SQSSink` report their own failures. The rejected value is **not** echoed into
      the warning verbatim beyond a bounded prefix — an unbounded attacker-controlled string in an
      operator's log is a log-injection surface. A comment records that.
- [ ] It is documented that adopting a context grants **nothing** — it selects a correlation id and
      confers no authority — so validation is about output integrity, not authorization. A comment
      records this so nobody later mistakes `continue_trace()` for a trust boundary.
- [ ] After any rejection the emitted `trace_id` still matches `^[0-9a-f]{32}$` and is non-zero. A
      test asserts the invariant directly on the emitted event, not on the return value.

### FR-005: Baggage crosses the boundary too

#### Description

Adopting a trace but losing its baggage half-solves the problem: the events join up, and every
correlating field the caller had set disappears at the hop. `architecture.md` §12 names
"cross-process baggage" as part of the same deferred item, and the immediate consumer's whole
current workaround *is* a hand-threaded baggage field.

#### Acceptance Criteria:

- [ ] `current_baggage_header() -> str` serializes the current baggage in the W3C `baggage` format
      (`key1=value1,key2=value2`, percent-encoded), and `continue_trace(..., baggage=<header>)`
      parses and merges it into the current context via the existing `set_baggage` path.
- [ ] Keys and values are percent-encoded on write and decoded on read, so a value containing
      `,`, `=`, or a non-ASCII character round-trips. A test asserts round-tripping through
      both functions is lossy for nothing.
- [ ] Non-string baggage values (the store is `dict[str, object]`) are serialized with `str()` and
      the docstring says so plainly — the wire format is text, and a caller putting a dict in
      baggage should know it arrives as its repr, not silently as something else.
- [ ] An invalid or oversized `baggage` header is skipped with a warning while the **trace context
      is still adopted**. A comment records why they fail independently: losing correlating fields
      is bad, and losing the trace join because a field was malformed is worse.
- [ ] A bounded cap on the parsed header is enforced (W3C suggests 8192 bytes) and documented, so
      a hostile header cannot inflate every subsequent event in the process.

### FR-006: The producer side, and the docs that make the pair usable

#### Acceptance Criteria:

- [ ] `current_traceparent() -> str | None` returns the current span's context as a `traceparent`
      string, or `None` when no span is active. A test asserts the value it returns is accepted by
      `continue_trace()` — the round-trip is the contract.
- [ ] `current_trace_context() -> tuple[str, str] | None` returns `(trace_id, span_id)` for callers
      that would rather move two fields than a formatted string (a Step Functions payload, a queue
      message attribute). A comment records that this exists so callers are never pushed into
      `context.current_span()`, which is internal.
- [ ] The README gains a **"Continuing a trace across processes"** section showing both halves —
      producer emits, consumer adopts — with the Lambda/state-machine case as the worked example,
      since that is the motivating consumer.
- [ ] `architecture.md` §12's "Resolved" entry is updated from *deferred* to *shipped in SPEC-014*,
      and the Known Constraints entry SPEC-013 FR-006 added (trace context does not cross a process
      boundary) is amended to say it now can, and how.
- [ ] `docs/component-inventory.md` gains rows for `continue_trace`, `current_traceparent`,
      `current_trace_context`, and `current_baggage_header`.
- [ ] The version bump is **minor**: every addition is additive and no existing behaviour changes
      when `continue_trace()` is never called.

---

## API / Interface Contract

```python
def continue_trace(
    traceparent: str | None = None,
    *,
    trace_id: str | None = None,
    parent_span_id: str | None = None,
    baggage: str | None = None,
) -> bool:
    """Adopt an inbound trace context so this process's spans join the caller's trace.

    Accepts a W3C ``traceparent`` string or the ids directly. If a root span is already
    open (the ``@trace``-decorated entry point calling this on its first line), that span
    is re-parented in place and its buffered events are rewritten to match.

    Returns ``True`` when a context was adopted, ``False`` when nothing valid was supplied
    and a fresh trace is in use. Never raises.
    """

def current_traceparent() -> str | None:
    """Return the current span as a W3C ``traceparent``, or ``None`` if no span is active."""

def current_trace_context() -> tuple[str, str] | None:
    """Return ``(trace_id, span_id)`` for the current span, or ``None`` if none is active."""

def current_baggage_header() -> str:
    """Return the current baggage in W3C ``baggage`` header format (``""`` when empty)."""
```

```python
# Producer — hand the context to whatever crosses the boundary.
@lf.trace
def enqueue_check(location: str) -> None:
    sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps({
            "location": location,
            "traceparent": lf.current_traceparent(),
            "baggage": lf.current_baggage_header(),
        }),
    )

# Consumer — one line, first line.
@lf.trace
def handler(event, context):
    lf.continue_trace(event.get("traceparent"), baggage=event.get("baggage"))
    lf.info("inspecting")          # same trace_id as the producer; parent is its span
    try:
        return inspect(event)
    finally:
        lf.flush()                 # SPEC-013
```

## File & Folder Structure

```
src/log_foundry/ids.py           # traceparent parse + format, beside the generators
src/log_foundry/context.py       # the adopted-context ContextVar; baggage header codec
src/log_foundry/decorator.py     # _open_span consults the adopted context; re-parent + rewrite
src/log_foundry/__init__.py      # export the four new names
README.md                        # "Continuing a trace across processes"
docs/architecture.md             # §12 Resolved → shipped; Known Constraints amendment
docs/component-inventory.md      # four new rows
tests/test_trace_continuation.py # FR-001..FR-005
```

## Implementation Phases

### Phase 1: Format

- `traceparent` parse/format in `ids.py`: length, lowercase-hex, all-zero rejection, the
  higher-version forward-compatibility rule, fixed `01` flags on write.
- The `baggage` header codec with percent-encoding and the size cap.

### Phase 2: Adopt

- The adopted-context ContextVar; `_open_span` consulting it only for root spans.
- `continue_trace()` including the re-parent path **and the buffered-event rewrite** — the
  split-trace failure in FR-003 is the one to write a failing test for first.
- Total, non-raising handling of every malformed input.

### Phase 3: Publish and document

- `current_traceparent()` / `current_trace_context()` / `current_baggage_header()`; the
  round-trip test that ties producer to consumer.
- README section, architecture.md §12 update and Known Constraints amendment,
  component-inventory rows.
- `sh scripts/spec-lint.sh`, the completion ritual, and a minor version tag.
