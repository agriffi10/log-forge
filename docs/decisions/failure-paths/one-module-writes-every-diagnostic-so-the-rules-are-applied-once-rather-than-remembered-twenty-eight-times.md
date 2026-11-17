---
type: Decision
title: "One module writes every diagnostic, so the rules are applied once rather than remembered twenty-eight times"
description: "`_diag` owns `absorbed`/`lost`/`rejected`, and an exception is named by its type's stored name (`_diag.type_name`), never `repr(exception)`. Twelve sites printed the repr and two were unguarded before the rules had one home. Per-event lines share one throttle period; a dead echo stream is announced once, then disabled. (SPEC-029, SPEC-055)"
---

**One module writes every diagnostic, so the rules are applied once rather than remembered twenty-eight times** — which is exactly how twelve sites came to print `repr(exception)` while the other eight printed a type name, and how two came to be unguarded. `_diag` owns `absorbed`/`lost`/`rejected`: an exception is named by its type's stored name (`_diag.type_name`), and where that is not diagnosable (an `OSError` is not "refused" vs "host unknown") the caller passes a detail built from values the *library* controls — an `errno`, an HTTP status, an attempt count — never from the exception's text. Any detail is escaped **then** bounded, so the bound governs what is written, and `isprintable()` is the escape test rather than a C0 table: `splitlines()` breaks on three separators such a table misses, so a newline count would call a forged line safe. `rejected`, whose input is an inbound *header* rather than an exception, echoes it without running any code of the value: a `str`, exact or a subclass, is its first 64 characters read through `str`'s own slot, quoted by `str.__repr__`, escaped and cut; anything else is `<TypeName>`, read through `type`'s own `__name__` descriptor; and when even that cannot be computed the line is still written with `<?>`, because `"rejected"` means the refusal was announced (SPEC-067). Why, and the rule's other half on the parsers and `continue_trace`, is the sibling decision *An inbound value runs no code of its own anywhere on the inbound path*. A test forbids any other module writing to stderr; it is a lint on the idiom (`stderr.write`, `print(file=…)`, `traceback.print_*`), not a sandbox. (SPEC-029, arch §6) **A per-event diagnostic is throttled at its site from one period, and a stream that cannot come back is disabled after one line** (SPEC-055 FR-005, invariant 11): `_diag.WARN_EVERY` is the one definition the worker's two sites and the console writer read, because two constants stating one number disagree eventually. `ConsoleWriter` owns its stream's faults — a `BrokenPipeError` or a closed file's `ValueError` is announced once and latches echo off for the life of the writer (measured before: 200,000 echoed events into `head -1` wrote 199,970 identical lines), while any other `Exception` may be transient and is counted under a lock on the failure path and announced on the throttle. Echo loss earns no `Health` term: the event still rides the pipeline, so nothing invariant 2 counts is lost.

**At exit, a dead sink's repeated lines are folded, still by `_diag` alone** (SPEC-103, invariant
11). From the first abandoned exit batch on, the worker opens a fold window around each later exit
batch's sink call: the engine run, or each 1.x `emit`, and nothing else. Inside it, `lost`,
`absorbed` and `pending` record one row per distinct line text rather than writing it, and
`take_folded`, `_diag`'s fifth stderr write, writes each row once with its summed count and the
tail `; <k> line(s) folded at exit`. That happens where SPEC-090's exit tallies are written, before
them, or before `stop()`'s expiry line. So a line inside the window is written later than its
counter moved, which the record-first rule allows. Its count is never lost, and its line is
lost only in one case:

- an expired join closes the fold, so a wedged call's later lines are written at once;
- a recorder that cannot take `_FOLD_LOCK` at once writes at once, and no take waits on it
  without bound: `stop()`'s closing take waits 0.1 s, and the drain's take gives way once the
  table is closed;
- a forked child withdraws the window as `_fork`'s first step;
- the one case: rows a timed-out closing take leaves are written only if the drain's take
  reaches the lock, so at interpreter exit after an expired join, where the drain is a daemon
  thread, their lines are not written. `health()` still counts their losses.

The worker's own lines are never folded. Neither is a count that is not an increment, a running
total or a snapshot, which `lost` marks with `running_total=True`, because summing either
overstates a loss. The fold check is
inline, so a line outside a window calls nothing the writer did not call before. Why the fold
replaced growing a dead sink's exit batches is *A backlog drains in larger batches, and capacity
is not a setting* (`docs/decisions/pipeline/`).

## History

### 2026-11-17

- **Refined (SPEC-103).** `_diag` gains the exit fold and its fifth write, `take_folded`, and
  `lost` gains `running_total`. A refusing `HTTPSink` with 10,000 spans at exit wrote 1,004 lines
  at `4599a0c5` and 6 at `f28f17d4`, with its 16,016 calls unchanged. The fence is unchanged:
  every line is still `_diag`'s.

### 2026-11-15

- **Refined (stored-name sweep, no spec).** An exception is named by its type's stored name,
  `_diag.type_name`, where this said `type(exc).__name__`, which runs a metaclass `__name__`
  property; the rule against `repr` is unchanged. The `failure-paths` decision *A type is named by
  the name it stores* holds the spelling, and `tests/test_stored_name_roster.py` enforces it.

### 2026-11-04

- **Refined (SPEC-067).** `rejected` stopped taking a `repr` of the inbound value. It was taken of
  the whole value before the cut, so a shared-sublist list passed to `continue_trace` never returned,
  and a raising `__repr__` was swallowed with the write, so `continue_trace` reported `"rejected"`
  with no line written. The echo now runs no code of the value, its type or its metaclass, and the
  line is always written. The inbound path's reasoning, with its rejected alternatives, is the
  sibling decision *An inbound value runs no code of its own anywhere on the inbound path*.

### 2026-09-02

- **Settled.**
