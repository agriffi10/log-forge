# Completed Spec — SPEC-029: Diagnostic Output Safety

## What was completed?

The library wrote to stderr in twenty-eight places, every one announcing loss, and two settled rules
governed them — both already violated. Every line now goes through one module, so the rules are
applied once rather than remembered per call site, which is how twelve of the sites came to disagree
with the other eight.

- **`_diag.py` owns every diagnostic** (FR-001). SPEC-025 shipped `absorbed`; this spec added
  `lost(what, count, detail)`, `rejected(reason, value)` (SPEC-014's bounded `repr`, lifted out of
  `decorator._warn_rejected` unchanged for a `str`) and `errno_of(exc)`. All 28 sites converted
  across `worker`, `decorator` and 17 sink modules. `api` needed nothing — SPEC-025 had already
  converted it.
- **An exception is named by type, never by `repr`** (FR-002). Twelve sites interpolated `{err!r}`.
  `PostgresSink` was the sharpest: `_row` binds the whole `json.dumps(event)` as a statement
  parameter and a psycopg error repr reprints the failing statement *and its bound parameters*, so
  the diagnostic for a failed insert reprinted the event, PII included. Where a bare type is not
  diagnosable the caller passes a library-controlled `detail` — an `errno` (`SocketTransport`,
  `HTTPSink`), an HTTP status, an attempt count, librdkafka's numeric code. Details are
  control-character-escaped **then** truncated, so the bound governs what is written.
- **A diagnostic can never be the failure** (FR-003). `Worker._emit` and `SocketTransport` held the
  two unguarded writes, both on the worker thread: a broken stderr rose into `_run`'s handler and
  ended delivery for good, with `health()` then reporting `stopped_reason='ValueError'` for a fault
  that had nothing to do with the sink. Every write is guarded inside the module; a `BaseException`
  still propagates.
- **The rules are written down** (FR-004): `_diag`'s docstring, and `architecture.md` §6.
- **Enforcement**: a test asserts no other module calls `stderr.write`, `print(file=…stderr)` or
  `traceback.print_*`; another asserts no `_diag` argument contains `!r`, `repr()` or `.args`. Both
  match on the AST — `sinks/_socket.py` names `sys.stderr.write` in a comment, and a text search
  would read that as a violation.

**Deliberate deviations.** (1) `Worker._terminal_failure` uses `absorbed`, not a fourth writer — the
thread's death is an exception it caught and did not propagate. (2) The spec's example
`lost("batch", len(batch), …)` would print "lost 12 batch(s)" for 12 events; the site uses
`lost("event", …)`. (3) `MultiSink`'s two lines are `absorbed`, not `lost` — no count is known.
(4) `KafkaSink`'s `err` is a `KafkaError`, not an exception, so it uses `lost` plus a `_code` helper.

## What changed from earlier specs?

- **Every diagnostic line's wording changed**; counts, sink names, attempt numbers and the
  `_DROP_WARN_EVERY` throttle are unchanged. Anything grepping for the old text — including
  SPEC-019's `worker thread stopped on X`, now `absorbed a failure while draining the log queue (X)`
  — needs updating. Nine test assertions moved.
- **`decorator._warn_rejected` and its `_MAX_REJECTED_ECHO` are gone**, replaced by `_diag.rejected`.
  Byte-identical for a `str`; additionally hardened against a `__repr__` that returns a raw newline.
- **`_diag.lost`'s count is the increment**, except at a throttled site, which passes its running
  total and says so in `detail`. `Worker.submit` is the only one.
- **Unblocks SPEC-026**, which needs the same writers for the counters it exposes.

## Verification

Local: 811 tests pass (130 new), `ruff` and `mypy --strict` clean over 49 source files, `spec-lint`
clean. CI green on 3.12 and 3.13 across PRs
[#103](https://github.com/agriffi10/log-forge/pull/103),
[#104](https://github.com/agriffi10/log-forge/pull/104),
[#105](https://github.com/agriffi10/log-forge/pull/105) and
[#106](https://github.com/agriffi10/log-forge/pull/106).

Every phase was mutation-tested, and four fresh-context reviews found nine real defects between
them — three in `_diag` itself, each defeating the guarantee the module exists to provide:
`errno_of` interpolating an `int` **subclass** whose `__str__` is arbitrary user code; an escape
table over `range(0x20)` that missed U+0085/U+2028/U+2029 and the C1 block, so a newline count said
one line while `splitlines()` saw two; and `rejected` trusting that `repr` escapes, which is a
property of the built-ins and not of `repr`. Also: `kafka._code` not total despite saying so,
`MultiSink` putting a runtime value in the one field that is not bounded, a leaked adoption in a new
test, and — the one that mattered most — the Phase 4 enforcement test missing
`traceback.print_exc()`, the form most likely to be added by accident inside the very `except`
blocks this spec converted, and the one that reopens FR-002's leak as well as FR-003's.

Two of my own testing mistakes are worth recording. A per-site mutation harness (zeroing each
`_diag.lost` count in turn) found **6 unasserted counts** after spot-checking three had found none —
sampling does not scale to a mechanical change across 28 sites. And a `monkeypatch.setattr(sys,
"stderr", …)` applied in a *fixture* is silently undone by pytest's capture-resume between the setup
and call phases, which would have made every "the secret is absent from stderr" assertion pass
vacuously; the tests read through `capsys` instead. Two CI-only traps are recorded in
`docs/process/operational-traps.md`: `OSError`'s concrete type is per-platform (`OSError(111, …)` is
`ConnectionRefusedError` on Linux, plain `OSError` on macOS), and `ruff format` is not a gate here
and rewrites files a change never touched.
