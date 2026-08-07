# Completed Spec — SPEC-031: Audit Small Corrections

Shipped in two PRs, as the spec's Phase 4 asks: FR-001..FR-005 (#121), FR-006 alone (#122).

## What was completed?

- **FR-001 — `RotatingFileSink` rotates on a monotonic clock.** `_schedule_next` and
  `_should_rotate` both read `time.monotonic()`; a backward wall-clock step no longer defers
  rotation. AC-4 (preserve wall-clock filename labels) is **vacuous here** and recorded as such:
  backups are numbered, never timestamped, so no filename derives from a clock.
- **FR-002 — UDP reaches IPv6.** `_make_udp` gained a `host` parameter and resolves the address
  family via `getaddrinfo`, once per socket creation. 14 test seam sites updated; no default was
  given to the parameter, which would have hidden it from every real reader. **IPv4 wins wherever
  the host offers it** — see the review round below for why the obvious `[0][0]` was a regression.
- **FR-003 — three false doc claims corrected.** `architecture.md` §12 and §6.1 both said console
  echo goes to stdout (it has been stderr since SPEC-002); the construction-time stream binding is
  now documented on `ConsoleWriter` and `StdoutSink`. **A fourth was corrected alongside them**:
  §6.1's "Configurable" bullet described a global echo destination/format/`echo_level` surface that
  was never built and that §12 had already declined. Same false-claim shape, same paragraph. The
  `api.py:53` comment the spec names was **already gone** — SPEC-025 rewrote that docstring — so the
  settled decision is now stated there with a pointer to §12 instead of a comment being removed.
- **FR-004 — two micro-inconsistencies.** `_Coercer` caches the integer ceiling in `__init__`
  (`__slots__` gained `_int_ceiling`); `model.py`'s function-local `get_config`/`new_log_id` moved
  to module scope in `build_event`, `end_event` and `backfill_baggage` — there was no cycle to dodge.
- **FR-005 — recorded, not changed.** `Worker._release_waiters`'s use of `queue.Queue`'s private
  `.mutex`/`.queue` is now an `architecture.md` §13 constraint, with a test against a mixed queue so
  a CPython change fails loudly instead of silently timing out flush waiters.
- **FR-006 — a process that never created a worker closes its sink.** New in `decorator.py`:
  `_note_orphan_emit` (armed from `api._log`'s orphan branch, **after** the emit returns),
  `_close_orphan_sink`, `_register_exit_handler`, and three module flags. `health().retired` is
  synthesized without a worker. `conftest._reset_worker` clears the flags.

**The design choice worth knowing:** the spec's Data Model suggested avoiding `_atexit_registered`.
What ships instead **unifies the handler** — `_shutdown_worker` covers both paths, so one
registration under the existing flag is correct. That dodges both traps the FR names at once: no
LIFO double-close, and a mixed process keeps its worker drain. The once-only property lives on the
*close*, as the FR requires, not on the registration. A live worker still owns the close.

**Out of scope, and still open:** `configure(sink=A)` → `info()` → `configure(sink=B)` → `info()`
leaves **A** unclosed with `incomplete_swaps` at zero, because `_swap_sink` returns early on a null
worker. Recorded in `architecture.md` §13 beside the struck-through FR-006 entry. It needs its own
spec.

## What changed from earlier specs?

- `_make_udp()` → `_make_udp(host)`. Module-private test seam; both callers are in this repo.
- `Health.retired` is no longer purely a `Worker` field — `decorator._worker_health` synthesizes it
  when there is no worker (SPEC-030 FR-001's meaning is unchanged; only its reach widened).
  `submitted_after_shutdown` is deliberately **not** synthesized: SPEC-030 defines it as queued
  where nothing drains, and this path is refused-and-announced.
- `architecture.md` §13's "a process that only ever used the orphan path never closes its sink"
  is struck through and marked closed (SPEC-021's rule).
- `api` now imports `decorator`. New edge, no cycle — `decorator` does not import `api`.

## Review rounds

Both PRs were reviewed in a fresh context before merge. Both reviews found real defects; neither
was caught by CI, which was green throughout.

**PR #121 — one HIGH.** FR-002's first cut took `getaddrinfo(host)[0][0]`. RFC 6724 sorts AAAA
first, so a **dual-stack hostname moved from IPv4 to IPv6** — and against a collector bound to
`0.0.0.0:514`, the common rsyslog/logstash deployment, the datagram is discarded with no signal:
UDP is unconnected, so `sendto` succeeds locally, `emit` returns, and `losses()` stays at zero.
Measured. A spec written to remove one instance of silent loss had introduced another, and FR-002's
own AC-2 ("delivery to a hostname is unchanged") forbids it. IPv4 now wins where offered, so AC-1
and AC-2 hold together — which taking `[0]` cannot do. This is a fixed preference, not the
happy-eyeballs / caching / preference-*setting* redesign Out of Scope bars.

The AC-2 test **could not have caught it**: it bound its receiver with the same
`getaddrinfo(...)[0][0]` expression production used, so a family mismatch was unrepresentable. The
tell is that it went *red* when `_make_udp` was reverted to pre-PR `AF_INET` — a failure caused by
the regression, readable as evidence of the fix. Deleted, not patched; three tests replace it, one
binding an IPv4-only receiver so the sink must come to it.

Also: the FR-005 test was racy (`_stop.set()` does not wake the drain thread out of
`get(timeout=flush_interval)`, so it consumed queued items; flaked as `deque mutated during
iteration` under a tightened switch interval), §6.1 still called the echo format a "default", the
new §13 entry mis-enumerated `Queue`'s public API, and `StderrSink.__init__` had missed FR-003's
binding note.

**PR #122 — one HIGH, plus three smaller.** An orphan-only `shutdown()` leaves `_worker` unset, so
a later `@trace` builds a **fresh, non-retired worker** and `health().retired` reverted to `False`
— contradicting what `health()` reported one call earlier, and making three shipped documents
false. Fixed by making the synthesis an `or` rather than a fallback.

The reviewer rated it as silent total loss on the strength of `KafkaSink`/`GooglePubSubSink` being
unguarded; **measured, that part does not hold** — SPEC-032 made exactly those three sinks refuse,
so the loss surfaces as `failed_batches=1` plus a `_diag` line, and a sink with no guard
(`StdoutSink`) releases nothing on `close()` and genuinely still delivers. So the residual is a
*signal shape*, recorded here: after an orphan-only shutdown, a later span's loss is detected by
`failed_batches`, not by SPEC-030's `retired` + `submitted_after_shutdown` pair. That pair keeps its
narrower meaning, per AC-5.

Smaller, all fixed: `_close_orphan_sink` read `_worker` outside the lock that `_get_worker`
publishes it under (a `shutdown()` racing a first `@trace` could close the sink the new worker had
just captured — reproduced with an injected preemption point); arming *after* the emit meant a sink
that **raised** never armed the close, leaving the socket behind a dead destination open forever,
which is the leak FR-006 exists to stop in the case most likely to be leaking (arming now precedes
the emit); and the AC-6 assertion did not name `SinkDeliveryError`, so any unrelated fault in that
branch satisfied it.

**Two of FR-003's premises were already stale** when the branch was cut, and are recorded rather
than presented as corrections: the `api.py:53` comment had been removed by SPEC-025's docstring
pass, and `console.py`'s module docstring no longer carried the "Lambda's stdout → CloudWatch" text
the spec quotes. The surviving false claims were in `architecture.md`.

## Verification

1082 tests pass on 3.12 and 3.13; `ruff`, `mypy --strict`, `spec-lint` clean. 33 tests added, none
removed (verified by diffing `pytest --collect-only` names, not pass counts).

Every new assertion was mutation-checked against reverted source. FR-006's battery ran four
mutants: the full revert (kills 8), **trap A verbatim** — arming via `_atexit_registered` so
`_get_worker` skips registration — which the whole 1082-test suite catches in exactly **one** test
(`test_a_mixed_process_at_interpreter_exit_closes_once_and_still_drains[orphan-then-span]`,
the criterion the spec said the rest of the set does not imply), **trap B** (a second `atexit`
handler, caught only by the subprocess tests that assert the close *count*), and arming from
`configure()` instead of a landed emit (caught only by the AC-8 test). Two FR-002 tests survive
their mutation by design — they are AC-2/AC-3 regression guards, not proof of the fix.

The IPv6 delivery tests bind a real `::1` loopback receiver and skip where the host has no IPv6
stack. Interpreter-exit behaviour is exercised in subprocesses, since `atexit` does not run under
pytest and `atexit._run_exitfuncs()` would fire the whole registry.
