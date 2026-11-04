---
type: Decision
title: "One module writes every diagnostic, so the rules are applied once rather than remembered twenty-eight times"
description: "`_diag` owns `absorbed`/`lost`/`rejected`, and an exception is named by `type(exc).__name__`, never `repr(exception)`. Twelve sites printed the repr and two were unguarded before the rules had one home. Per-event lines share one throttle period; a dead echo stream is announced once, then disabled. (SPEC-029, SPEC-055)"
---

**One module writes every diagnostic, so the rules are applied once rather than remembered twenty-eight times** — which is exactly how twelve sites came to print `repr(exception)` while the other eight printed a type name, and how two came to be unguarded. `_diag` owns `absorbed`/`lost`/`rejected`: an exception is named by `type(exc).__name__`, and where that is not diagnosable (an `OSError` is not "refused" vs "host unknown") the caller passes a detail built from values the *library* controls — an `errno`, an HTTP status, an attempt count — never from the exception's text. Any detail is escaped **then** bounded, so the bound governs what is written, and `isprintable()` is the escape test rather than a C0 table: `splitlines()` breaks on three separators such a table misses, so a newline count would call a forged line safe. `rejected`, whose input is an inbound *header* rather than an exception, echoes it without running any code of the value: a `str`, exact or a subclass, is its first 64 characters read through `str`'s own slot, quoted by `str.__repr__`, escaped and cut; anything else is `<TypeName>`, read through `type`'s own `__name__` descriptor; and when even that cannot be computed the line is still written with `<?>`, because `"rejected"` means the refusal was announced (SPEC-067). Why, and the rule's other half on the parsers and `continue_trace`, is the sibling decision *An inbound value runs no code of its own anywhere on the inbound path*. A test forbids any other module writing to stderr; it is a lint on the idiom (`stderr.write`, `print(file=…)`, `traceback.print_*`), not a sandbox. (SPEC-029, arch §6) **A per-event diagnostic is throttled at its site from one period, and a stream that cannot come back is disabled after one line** (SPEC-055 FR-005, invariant 11): `_diag.WARN_EVERY` is the one definition the worker's two sites and the console writer read, because two constants stating one number disagree eventually. `ConsoleWriter` owns its stream's faults — a `BrokenPipeError` or a closed file's `ValueError` is announced once and latches echo off for the life of the writer (measured before: 200,000 echoed events into `head -1` wrote 199,970 identical lines), while any other `Exception` may be transient and is counted under a lock on the failure path and announced on the throttle. Echo loss earns no `Health` term: the event still rides the pipeline, so nothing invariant 2 counts is lost.

## History

### 2026-11-04

- **Refined (SPEC-067).** `rejected` stopped taking a `repr` of the inbound value. It was taken of
  the whole value before the cut, so a shared-sublist list passed to `continue_trace` never returned,
  and a raising `__repr__` was swallowed with the write, so `continue_trace` reported `"rejected"`
  with no line written. The echo now runs no code of the value, its type or its metaclass, and the
  line is always written. The inbound path's reasoning, with its rejected alternatives, is the
  sibling decision *An inbound value runs no code of its own anywhere on the inbound path*.

### 2026-09-02

- **Settled.**
