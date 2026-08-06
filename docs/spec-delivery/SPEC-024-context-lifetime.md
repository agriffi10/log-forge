# Completed Spec — SPEC-024: Context Lifetime

## What was completed?

Baggage and the adopted inbound trace context were written into `contextvars` and never taken back
out, so both outlived the request that set them. On a thread that serves requests sequentially — the
main thread, a pooled worker, a warm Lambda container — one request's `user_id` appeared on the next
request's events, and a handler that adopted a `traceparent` once kept joining that caller's trace on
every later invocation that supplied no header, parented to a span in a process that had exited. The
first arc finding that puts **wrong data** in the log stream rather than losing it, and both were
already documented as not happening.

- **Baggage is scoped to the root span** (FR-001). `@trace` brackets the root — the span opened when
  no other was active — with `push_baggage_scope` / `pop_baggage_scope`, in the same `finally` as the
  `pop_span` it already ran, so success, error and async paths are covered identically and
  `_close_span` still runs first (SPEC-015's boundary backfill is untouched). Nested spans do not
  reset: baggage set three calls deep stays visible to its parent and later siblings.
- **The adopted context is released at the same point but *cleared*** (FR-002) — see the deviation
  below.
- **`reset_context()`** (FR-003) clears both for a caller who opens no span. One function, because
  the two values have the same lifetime and the same failure mode.
- **The docs describe the delivered scope** (FR-004): each variable's lifetime in `context.py`,
  when baggage is discarded on `get_baggage`/`set_baggage`, the root-span boundary in
  `architecture.md` §5.1, the one-shot rule and the task-boundary constraint on `continue_trace`,
  and a README section for `reset_context()`.

**Deliberate deviations.** (1) FR-002 specified a token restore for the adopted context, matching
baggage. That defeats its own acceptance criterion: a token restores whatever was current at
root-span *entry*, so an adoption made before the span — a framework middleware, and the spec's own
API-contract example — survives it and leaves invocation 2 joined to invocation 1's trace. It is
**cleared** instead, and `push_baggage_scope` returns one token rather than two. FR-002 and the Data
Model were amended in the Phase 1 PR. (2) `pop_baggage_scope` guards broadly rather than on
`ValueError` alone: a reused token raises `RuntimeError`, and a raise from a `finally` would replace
the caller's own exception (arch §4).

## What changed from earlier specs?

- **One adoption is now consumed by one root span.** A batch that calls `continue_trace()` once and
  then opens a root span per record joins only the *first* record to the inbound trace, where before
  all of them joined. This follows necessarily from the clear — a batch loop and a warm container are
  indistinguishable from inside the library — so it is pinned by a test and stated in the docstring.
  The remedy is one call per record, or one `@trace` entry point so the records are nested spans.
- **Baggage set inside a trace no longer outlives it.** Code that relied on the leak to carry keys
  between traces must set them before any span (a process-level default, which is restored to) or use
  `configure(defaults=...)`.
- **A constraint, recorded in `architecture.md` §13:** the release lands in whichever context the
  root span's `finally` runs in, so adopting *outside* a span and dispatching into an `asyncio.Task`
  clears the copy while the parent keeps the adoption. `contextvars` cannot write to a parent
  context, so it is closed by documentation, a test that pins it, and `reset_context()`.
- **Two false statements in `architecture.md` §5.1 were corrected** while the section was open:
  baggage does *not* propagate into a new thread (a thread gets a fresh context; measured), and it
  has crossed process boundaries since SPEC-014.

## Verification

Local: 599 tests pass (31 new), `ruff` clean, `mypy --strict` clean over 48 source files, `spec-lint`
clean. CI green on 3.12 and 3.13 across PRs
[#95](https://github.com/agriffi10/log-forge/pull/95),
[#96](https://github.com/agriffi10/log-forge/pull/96) and
[#97](https://github.com/agriffi10/log-forge/pull/97). Three fresh-context reviews found: the
asyncio task-boundary hole and the sibling-root-span narrowing (both reproduced before being
accepted, both now pinned); `pop_baggage_scope`'s totality hole; a **vacuous** test whose docstring
claimed to reject an in-place `.clear()` implementation that in fact passed every assertion; FR-003's
"inside an open span" criterion being covered only synthetically, which surfaced that a mid-span
reset empties the boundary events; and the two false `architecture.md` claims. Phase 1's fix was
verified by stashing `src/` and re-running — 15 of 18 new tests failed without it, the other three
being guardrails against resetting too much. The fixture removal (FR-002 AC-5) was verified by
running all 39 tests in that module in isolation and inspecting the contextvars each left behind,
then independently by the reviewer with a suite-wide teardown probe and an 8-seed shuffle.
