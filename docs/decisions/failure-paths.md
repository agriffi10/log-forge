# Failure paths and diagnostics — decisions

The settled decisions about the paths a caller stands on and how a swallowed fault is announced.
Read the fences; pull an entry only when you need the reasoning.

## Contents

- [Fences](#fences)
- [A dead worker is reported, not restarted — and as a *reason*, not a liveness flag](#a-dead-worker-is-reported-not-restarted--and-as-a-reason-not-a-liveness-flag)
- [Every path the caller stands on is total, and a swallowed fault is announced by *type*](#every-path-the-caller-stands-on-is-total-and-a-swallowed-fault-is-announced-by-type)
- [One module writes every diagnostic, so the rules are applied once rather than remembered twenty-eight times](#one-module-writes-every-diagnostic-so-the-rules-are-applied-once-rather-than-remembered-twenty-eight-times)

## Fences

- **A dead worker is reported, not restarted — and as a *reason*, not a liveness flag** — `Health.stopped_reason` is `None` for a live worker, a never-created one, **and** a cleanly shut-down one, so it extends the alert idiom by a term. No auto-restart: a thread that resurrects itself fights a process trying to exit. (SPEC-019)
- **Every path the caller stands on is total, and a swallowed fault is announced by *type*** — never `BaseException` — a `KeyboardInterrupt` or `SystemExit` is the operator's or the runtime's intent and must reach the caller. (SPEC-025)
- **One module writes every diagnostic, so the rules are applied once rather than remembered twenty-eight times** — `_diag` owns `absorbed`/`lost`/`rejected`, and an exception is named by `type(exc).__name__`, never `repr(exception)`. Twelve sites printed the repr and two were unguarded before the rules had one home. Per-event lines share one throttle period; a dead echo stream is announced once, then disabled. (SPEC-029, SPEC-055)

---

### A dead worker is reported, not restarted — and as a *reason*, not a liveness flag

**A dead worker is reported, not restarted — and as a *reason*, not a liveness flag** — the drain loop is guarded end to end and records the exception type that ended it (`Health.stopped_reason`), because `dropped` climbing already means backpressure and must not double as "the thread is gone". A reason string is `None` for a live worker, a never-created one, **and** a cleanly shut-down one, so it extends the alert idiom by a term; an `alive` flag would read `False` on every process that has not logged yet. No auto-restart: a thread that resurrects itself fights a process trying to exit. Type name only, never the exception message (arch §6). (SPEC-019)


### Every path the caller stands on is total, and a swallowed fault is announced by *type*

**Every path the caller stands on is total, and a swallowed fault is announced by *type*** — the decorator (setup, body, close, teardown), the orphan emitter and its echo, and `shutdown()` with its `atexit` drain all absorb an `Exception` and report one `_diag.absorbed` line rather than raising. Never `BaseException`: a `KeyboardInterrupt` or `SystemExit` is the operator's or the runtime's intent and must reach the caller — the same line SPEC-019 drew in the opposite direction for the worker thread, where the *absence* of a handler was the defect. A pre-body fault degrades to an **untraced call**, never a failed one; a failed close is announced, not retried (the once-only flag stays ahead of it, because a second `close()` on a partially released sink is worse than an unclosed one). Only the type is written, never the message (arch §6). `_diag` must import nothing from its own package. (SPEC-025)


### One module writes every diagnostic, so the rules are applied once rather than remembered twenty-eight times

**One module writes every diagnostic, so the rules are applied once rather than remembered twenty-eight times** — which is exactly how twelve sites came to print `repr(exception)` while the other eight printed a type name, and how two came to be unguarded. `_diag` owns `absorbed`/`lost`/`rejected`: an exception is named by `type(exc).__name__`, and where that is not diagnosable (an `OSError` is not "refused" vs "host unknown") the caller passes a detail built from values the *library* controls — an `errno`, an HTTP status, an attempt count — never from the exception's text. Any detail is escaped **then** bounded, so the bound governs what is written, and `isprintable()` is the escape test rather than a C0 table: `splitlines()` breaks on three separators such a table misses, so a newline count would call a forged line safe. The one bounded `repr` is `rejected`, whose input is an inbound *header* rather than an exception — and it is escaped afterwards anyway, because `repr` escaping newlines is a property of the built-ins, not of `repr`. A test forbids any other module writing to stderr; it is a lint on the idiom (`stderr.write`, `print(file=…)`, `traceback.print_*`), not a sandbox. (SPEC-029, arch §6) **A per-event diagnostic is throttled at its site from one period, and a stream that cannot come back is disabled after one line** (SPEC-055 FR-005, invariant 11): `_diag.WARN_EVERY` is the one definition the worker's two sites and the console writer read, because two constants stating one number disagree eventually. `ConsoleWriter` owns its stream's faults — a `BrokenPipeError` or a closed file's `ValueError` is announced once and latches echo off for the life of the writer (measured before: 200,000 echoed events into `head -1` wrote 199,970 identical lines), while any other `Exception` may be transient and is counted under a lock on the failure path and announced on the throttle. Echo loss earns no `Health` term: the event still rides the pipeline, so nothing invariant 2 counts is lost.


