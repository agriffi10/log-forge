# Completed Spec — SPEC-025: The Library Must Not Fail the Caller

## What was completed?

Architecture §4's first promise is that logging never breaks the application. SPEC-017 closed the
places that promise leaked which its audit found; three remained, and each was worse than the ones
already fixed, because in each **the exception the caller received was one the library invented**.

- **A successful call stays successful** (FR-001, FR-002). `_close_span` sat inside the decorator's
  `try`, so a failing close made a function that had already returned raise — and the
  `except BaseException` handler re-entered the close, emitting both a `status=ok` and a
  `status=error` end event for the same call. Measured: a function returning `42` raised, and the
  second close attempt carried `['span.start', 'span.end', 'span.end']`. Both wrappers now delegate
  to `_begin`/`_end`, the body runs in a bare `try`, and the outcome is recorded in the `except` and
  read in the `finally` — one close, knowing what actually happened.
- **The orphan path cannot raise** (FR-003). A bare `log_foundry.info(...)` outside any span called
  `_ensure_sink().emit(...)` unguarded and handed the application a `ConnectionError` from a
  destination it never chose to talk to. SPEC-017's delivery doc records "the orphan-path crash is
  gone" — true of the *sanitize* crash it fixed, not of the sink crash on the next line.
- **`shutdown()` is total** (FR-004). A failing `sink.close()` escaped, and the once-only flag meant
  the retry was a no-op, so the sink was never closed. On the `atexit` path CPython printed
  "Exception ignored in atexit callback" with a traceback carrying the exception's message.
- **`_diag.absorbed`** is the shared reporter behind all three: one stderr line, exception **type**
  only, and total in itself since it is called from the guards that exist to stop an exception
  reaching the caller. SPEC-029 takes ownership of the module.

**Deliberate deviations.** (1) `_open_span` was **brought into scope** during the build, on
instruction — the spec had excluded it as "not reachable in practice", which is the same argument
that left the three defects unguarded, and a fault *before* the body is worse because the library
stops the application working rather than merely losing a log. The pre-body setup is guarded as one
unit and the call proceeds untraced; `pop_span` became total alongside it, closing SPEC-024's
deferred note. The Out of Scope entry is struck through in place with the reason. (2) The baggage
scope is opened **first** of the three setup steps, so no partial state can keep a span while losing
its scope — that state would be SPEC-024's leak reappearing through a failure path.

## What changed from earlier specs?

- **A decorated function no longer fails because logging did**, on any path — including before the
  body runs. Code that (accidentally) relied on a sink error surfacing through `@trace` will now see
  a stderr line instead.
- **One `span.end` per span, always.** Anything counting end events, or reading `status`, previously
  had to tolerate a contradicting pair after a close failure.
- **`shutdown()` and the `atexit` drain never raise**, and the traceback they used to print at
  interpreter shutdown — message included — is now one type-only line.
- **A `KeyboardInterrupt` or `SystemExit` still reaches the caller everywhere.** Every guard catches
  `Exception`, never `BaseException` — the same line SPEC-019 drew in the opposite direction for the
  worker thread, where the *absence* of a handler was the defect.
- **Not changed, deliberately:** the in-span emitter path (FR-003 requires no behaviour change, so
  a `build_event` failure there still propagates — the orphan and in-span sides now differ, resting
  on SPEC-017's totality claim, which SPEC-020 already had to patch once); and the absorbed losses
  are neither retried nor counted in `health()`, which are SPEC-027's and SPEC-026's. One
  unthrottled stderr line per call, with no dedup, is what SPEC-029 inherits.
- **`_diag` must import nothing from its own package** — three modules reach it at module scope
  while the package is partially initialised, and it resolves only because it is a leaf. Recorded in
  its docstring.

## Verification

Local: 646 tests pass (39 new), `ruff` clean, `mypy --strict` clean over 49 source files,
`spec-lint` clean. CI green on 3.12 and 3.13 across PRs
[#99](https://github.com/agriffi10/log-forge/pull/99),
[#100](https://github.com/agriffi10/log-forge/pull/100) and
[#101](https://github.com/agriffi10/log-forge/pull/101). Each phase was reproduced before being
fixed and mutation-tested after; three fresh-context reviews found, between them: a **reference
cycle this arc introduced** (holding the outcome past the `except` rebuilt the cycle
`except ... as` deletes — 6500 objects retained per 500 raising calls, pinning the *caller's*
frames), a **reintroduced SPEC-024 baggage leak** through a failure path, a test that **corrupted
the process-global console writer** for the rest of the session, `_begin` telling the operator the
opposite of what it did, and four spec statements no test pinned. One claim of mine was wrong and is
corrected in the spec and the tests: the `atexit` guard does **not** rescue the process exit status
— CPython never lost it — it removes the traceback and the arch §6 message leak.
