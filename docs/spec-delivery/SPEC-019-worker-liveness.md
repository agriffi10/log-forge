# Completed Spec — SPEC-019: Worker Liveness and Terminal-Failure Reporting

## What was completed?

Every event the library delivers passes through one background drain thread, and that thread had no
terminal-failure path. Anything escaping `Worker._run` stopped delivery with nothing recorded —
`SystemExit` above all, which CPython's thread bootstrap discards without even a traceback. The app
kept logging, submissions kept landing in a queue no one was draining, and `health()` kept returning
a healthy-looking snapshot.

The reading did change eventually, but into the **wrong signal**: once the bounded queue filled,
`dropped` climbed, and `dropped` already means backpressure — a different fault with a different
remedy (tune the sink vs. restart the process). The two were indistinguishable, and the wrong one
arrived late.

- **`Worker._run` is guarded end to end** (FR-001). It records the exception type, announces it, and
  exits. Not swallow-and-continue: looping past a `KeyboardInterrupt` would be worse than the bug.
  `_emit`'s `except Exception` + retry + `failed_batches` is the *non*-terminal path and is
  untouched — an ordinary sink error is still absorbed and the worker still survives it.
- **`_terminal_failure`** records under `_lock` **before** writing to stderr (FR-002). Unlike the
  overflow warning this line is written exactly once and cannot be re-emitted, so the record must
  not be able to ride on a closed console. The **type name only** is reported, never the exception's
  message — a sink's error text can carry event data, and arch §6 keeps caller data out of places it
  was not asked for (the rule behind `sanitize`'s type-name placeholder).
- **`Health.stopped_reason: str | None`** (FR-003), appended and defaulted.
- **README table** contrasting all three fields and the response each one wants (FR-004).

**Deliberate deviation:** none of substance. One file outside the spec's File & Folder Structure
(`decorator.py`) took a docstring-only edit, since `_worker_health`'s "three zeros" wording no longer
described the snapshot it returns.

## What changed from earlier specs?

- **`Health` gained a fourth field, which breaks whole-tuple unpacking.** `h.queued` / `h.dropped` /
  `h.failed_batches` and index access are unchanged, but `queued, dropped, failed = health()` now
  raises `ValueError`. Accepted and stated in the spec rather than discovered later; the README has
  only ever advertised attribute access. The repo's own suite caught it — one SPEC-017 test compared
  `h == (0, 0, 0)` and now compares field-wise.
- **SPEC-004's drain loop moved from `_run` into `_drain(pending)`**, so `_run` owns `pending` and
  can report its length. The loop `clear()`s it in place instead of rebinding, which is why the
  count is meaningful. Behaviour-preserving: `_emit` builds its own flattened list, so no sink ever
  holds an alias to `pending`.
- **SPEC-017's `health()` contract widens** from "counters you should alert on" to that plus one
  categorical failure. The alert idiom gains a term rather than changing shape, which is the whole
  reason the field is a reason string and not an `alive` flag.

## Notes for the next spec

*Reconciled by SPEC-021 — two of these were the defects it was written to fix.*

- **`stopped_reason` is a one-shot terminal fact, not a counter.** It records the *first* thing to
  kill the thread, and there is no second — nothing runs afterwards to overwrite it.
  → **Settled** (SPEC-021). A property of the design, documented on the field itself.
- ~~**The stderr line reports only what the thread was holding.** Event-lists still in the bounded
  queue are equally undelivered and are not in that number; `health().queued` still shows them.
  Spec-compliant, but an operator reading "1 undrained event-list(s)" may under-read the loss.~~
  → **Fixed by SPEC-021 (FR-002).** The line reports what was held *and* what was still queued.
  The queued figure is labelled "items" because it counts internal markers alongside real
  submissions, and it is a floor: a producer can add to the queue between the death and the read.
- **The field's default commits future `Health` fields to being defaulted too**, since a
  non-defaulted field cannot follow a defaulted one in a `NamedTuple`.
  → **Settled** (SPEC-021). A live constraint on future work, not an outstanding question. It has
  already held once: SPEC-021 added no `Health` field.
- ~~**`flush()` returns `True` for a marker whose emit died** — the `finally` sets the waiter's
  event so it isn't stranded. Pre-existing (SPEC-013), documented in place, and untouched here,
  but it means `flush() is True` does not prove delivery.~~
  → **Fixed by SPEC-021 (FR-001).** The marker carries the drain's outcome back, so `True` means
  delivered. This note is why SPEC-021 exists: it was the one genuine defect in the four specs'
  worth of notes, and a false success in the serverless path `flush()` was built for.
- ~~**Non-`str` scalars in `sanitize.py` are still unbounded** — named in this spec's Out of Scope
  as payload-safety territory. Still unclaimed by any spec.~~
  → **Fixed by SPEC-020**, which claimed it. See the same note on SPEC-017's delivery doc.

## Verification

Local: 530 tests pass, `ruff check` clean, `mypy --strict` clean over 48 source files, `spec-lint`
clean. CI green on 3.12 and 3.13 (PR [#63](https://github.com/agriffi10/log-forge/pull/63)). A
fresh-context review checked the diff against all acceptance criteria and returned no correctness
defects, verifying the `pending.clear()` aliasing question and the lock ordering specifically, and
running the worker tests 12× for flakiness. Its two substantive findings were fixed before merge:
two stderr assertions polled the record rather than thread exit and could pass vacuously (the
reviewer reproduced it with a slow write), and FR-001's decorated-function clause had no test. Its
version nit was declined — see the release note below.
