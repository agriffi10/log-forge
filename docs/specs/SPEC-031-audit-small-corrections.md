# Spec: Audit Small Corrections

**ID:** SPEC-031  
**Status:** Draft  
**Last Updated:** 2026-08-07  
**Depends On:** SPEC-004, SPEC-008, SPEC-009, SPEC-020, SPEC-030

## Overview

The 2026-08-05 audit turned up six items too small to justify a spec each and too real to leave
unrecorded. SPEC-021 established how this repository handles that: an open item is closed by being
fixed, settled, or recorded as a constraint — never deleted, because a note that is merely removed
takes its reasoning with it.

They are unrelated to one another except in size. One is a latent correctness bug (`RotatingFileSink`
measures its rotation interval on the wall clock, so a backward clock step defers rotation
indefinitely — the same hazard `model.py:39-41` deliberately avoids one module over). One is a
capability gap (`SocketTransport` hardcodes `AF_INET`, so UDP syslog to an IPv6 host cannot work).
Three are documentation that contradicts the code, including `architecture.md` and a module docstring
that both name the wrong output stream for console echo. One is a pair of micro-inconsistencies in
`sanitize` and `model` that sit oddly beside the hot-path care taken elsewhere in the same files.

Grouping them keeps six trivial branches from becoming six specs, and gives each a recorded
resolution.

**FR-006 is the exception and should be read as one.** It was added on 2026-08-07, found while
reviewing SPEC-032, and it is neither small nor a documentation correction: a process that only ever
logs outside a span never closes its sink, loses every event, and reports a clean `health()`. It is
here by decision rather than because it fits the theme — the alternative was a spec of its own, and
a one-FR spec for a fix this contained is worse paperwork than an FR in the wrong-shaped home. It is
the only item here that changes runtime behaviour a user can observe, and the only one that should
be built with its own review. A reader who came to this spec expecting residue should skip to it
first, not last.

## Scope

### In Scope

- `RotatingFileSink`'s time trigger.
- IPv6 for UDP socket transport.
- Three documentation corrections where a claim is false.
- Two micro-inconsistencies in `sanitize.py` and `model.py`.
- Recording the one item deliberately *not* changed, per SPEC-021's rule.
- **FR-006:** closing the sink in a process that never created a worker.

### Out of Scope

- **Anything from SPEC-024 through SPEC-030.** Those are the audit's substantive findings and each
  has its own spec. This one is otherwise the residue — FR-006 is the single deliberate exception,
  and the Overview says why.
- **Restarting or recreating the worker in `shutdown()`.** FR-006 closes a sink that no worker ever
  owned; it does not build a worker to do it. Standing up a thread at exit to prove there is nothing
  to drain is pure cost, and SPEC-030's `_swap_sink` already declines to do the same thing for the
  same reason.
- **Any new `Health` field.** FR-006 makes `retired` truthful in a case where it is currently
  vacuous; it does not add a counter. SPEC-030 settled that vocabulary.
- **Changing `RotatingFileSink`'s rotation *policy*.** Interval and size semantics, naming, and the
  retention count are unchanged; only the clock it reads changes.
- **A dual-stack or hostname-resolution redesign for the socket transport.** FR-002 makes UDP work
  against an IPv6 destination; it does not add happy-eyeballs, address caching, or a preference
  setting.
- **Making the sinks' stream binding reconfigurable at runtime.** FR-003 corrects the docs and the
  one demonstrable defect; a `set_stream()` API is not built.

---

## Functional Requirements

### FR-001: `RotatingFileSink` rotates on a monotonic clock

#### Description:

`_schedule_next` (`file.py:136-140`) returns `time.time() + interval` and `file.py:150` compares it
against `time.time()`, while the docstring calls it "Absolute **monotonic**-wallclock time". A
backward clock step — an NTP correction, a container clock sync — larger than the interval defers
every time-based rotation until wall-clock catches up, silently defeating the "bounds on-disk
growth" promise in the class docstring.

`model.py:39-41` handles the identical hazard correctly and says why: `start_ts` is monotonic "so
`duration_ms` can never go negative across a clock change". The same reasoning applies to a deadline.

The size trigger is unaffected and stays as the backstop it already is.

#### Acceptance Criteria:

- [ ] The rotation deadline is computed and compared with `time.monotonic()`.
- [ ] With the wall clock stepped backwards by more than the interval, a rotation still occurs
      within one interval of real elapsed time.
- [ ] With the wall clock stepped forwards, rotation is not triggered early by more than the step —
      i.e. the trigger no longer tracks wall-clock at all.
- [ ] Filenames derived from wall-clock time (the rotated file's timestamp suffix) keep using
      `time.time()`: the *deadline* is monotonic, the *label* is not, and a monotonic value is
      meaningless in a filename.
- [ ] The docstring matches what the code does.
- [ ] Existing size-based rotation tests pass unchanged.

### FR-002: UDP socket transport reaches an IPv6 destination

#### Description:

`_make_udp` (`_socket.py:30`) constructs `socket.socket(socket.AF_INET, socket.SOCK_DGRAM)`
unconditionally, so `sendto` to an IPv6 address fails every time and the message is abandoned after
the retry bound. TCP is unaffected — `socket.create_connection` is family-agnostic — so the defect
is UDP-only and silent, which is why it went unnoticed.

The address family is resolved from the configured host rather than assumed.

#### Acceptance Criteria:

- [ ] UDP `SyslogSink` and `LogstashSink` deliver to an IPv6 destination (`::1` in test).
- [ ] UDP delivery to an IPv4 destination and to a hostname is unchanged.
- [ ] A host that resolves to neither family fails as it does today — counted and announced, not
      raised (subject to SPEC-026 FR-001, whichever lands first).
- [ ] Resolution happens once per socket creation, not per message.
- [ ] The `_make_udp` test seam still allows a fake socket to be substituted with no network access.
- [ ] `SyslogSink`'s and `LogstashSink`'s docstrings state that both families are supported.

### FR-003: Three false documentation claims are corrected

#### Description:

Each of these states something the code does not do, and each was found by reading the code against
the doc rather than by any failure.

1. `console.py:5` says echo goes "to a terminal user or a Lambda's stdout → CloudWatch", and
   `architecture.md` §12 Resolved states "`console.py` echoes to **stdout**". It defaults to
   **stderr** (`console.py:23`). Two documents, one wrong stream.
2. `ConsoleWriter` and `StdoutSink` capture `sys.stderr`/`sys.stdout` at construction, and
   `api._console` is built at import (`api.py:33`), so a later `redirect_stdout` or test capture is
   not honoured. This is defensible behaviour but is documented nowhere, and it surprises anyone
   writing a test against echo.
3. `api.py:53`'s comment — "SPEC-004's worker will later own this direct handoff" — describes work
   that shipped in SPEC-004 and was then deliberately left as-is (`architecture.md` §12 Resolved).
   The comment reads as a pending TODO for a settled decision.

#### Acceptance Criteria:

- [ ] `console.py`'s docstring names stderr, and explains why (the twelve-factor convention
      `StderrSink` already cites: logs on stderr, app output on stdout).
- [ ] `architecture.md` §12's stdout claim is corrected in place and marked as corrected, per
      SPEC-021's rule that a superseded note is struck through rather than deleted.
- [ ] The construction-time stream binding is documented on `ConsoleWriter` and `StdoutSink`, with
      the note that an explicit `stream=` argument is how a test captures output.
- [ ] `api.py:53`'s comment is replaced by one stating the settled decision and pointing at
      `architecture.md` §12, so it no longer reads as pending work.
- [ ] No behaviour changes under this FR.

### FR-004: Two micro-inconsistencies are resolved

#### Description:

Both sit next to code that took the opposite care, which is the only reason they are worth touching.

1. `sanitize.py:315` calls `_int_digit_ceiling` — and through it `sys.get_int_max_str_digits()` —
   for **every** integer coerced, four lines below the binding of `int.__lt__` whose comment
   justifies itself by "a per-value hot path" (`sanitize.py:58-61`). The interpreter limit cannot
   change during a coercion pass, so it is resolved once per pass rather than once per value.
2. `model.build_event` imports `get_config` and `new_log_id` inside the function
   (`model.py:81-82`), repeated at `model.py:176` and `model.py:216` — on the hottest path in the
   library. The imports exist to avoid a cycle, but `sanitize.py:38-41` solves the identical problem
   with a `TYPE_CHECKING` import plus a one-time binding, and says so.

Neither is a measurable problem. Both are resolved for consistency of standard, and the resolution
is recorded so the next reader does not re-litigate them.

#### Acceptance Criteria:

- [ ] The interpreter's integer limit is read once per coercion pass, not once per integer.
- [ ] SPEC-020's behaviour is unchanged: the same integers are replaced and the same
      `<int: ~N digits>` placeholders produced, including at the ceiling boundary and for negative
      values (SPEC-021 FR-003).
- [ ] A test asserts a coercion pass over many integers reads the interpreter limit at most once.
- [ ] `build_event`'s per-call imports are resolved once, with no import cycle introduced
      (`poetry run python -c "import log_foundry"` and the full suite both pass).
- [ ] `end_event` and `backfill_baggage` use the same resolution rather than keeping their own
      function-local imports.

### FR-005: The item not changed is recorded

#### Description:

`Worker._release_waiters` reads `queue.Queue`'s private `.mutex` and `.queue`
(`worker.py:296-297`). The audit flagged it; it is **not** changed here.

The docstring already justifies it: a snapshot under the queue's own mutex is what makes it
impossible to miss a marker mid-iteration, and the alternative — draining and re-enqueueing — would
destroy the queued event-lists that `health().queued` and the terminal-failure line report as
evidence. There is no public API for "inspect without consuming". The cost of the private access is
that a future CPython change could break it, which a test would catch.

Per SPEC-021, this is closed by being recorded as a constraint rather than fixed or deleted.

#### Acceptance Criteria:

- [ ] `architecture.md` §13 Known Constraints records the reliance on `queue.Queue` internals, why
      no public alternative exists, and what would break if CPython changed it.
- [ ] `_release_waiters`'s docstring points at that entry.
- [ ] A test exercises `_release_waiters` against a queue containing a mix of markers and
      event-lists, so a CPython change surfaces as a test failure rather than a silent behaviour
      change.

### FR-006: A process that never created a worker still closes its sink

#### Description:

A level call made with no active span emits synchronously on the caller's thread, straight into the
configured sink — it never touches the worker. So a process that only ever logs that way creates no
worker, and two things follow from `decorator.py`:

- `atexit.register(_shutdown_worker)` sits **inside** `_get_worker` (`decorator.py:209-211`), so it
  is never registered.
- `_shutdown_worker` returns early when `_worker is None` (`decorator.py:232`), so an explicit
  `shutdown()` does nothing either.

The sink is therefore never closed, by any path. Measured against a `KafkaSink` with a recording
producer — `configure(sink=…)` → `info()` → `shutdown()` → `info()`, no span anywhere:

```
produced=2  flushes=0  sink._closed=False  delivered=0
health: retired=False, submitted_after_shutdown=0, stopped_reason=None, sink=None, failed_batches=0
```

Both events sit in the producer's local batch and die with the process. **Every** event is lost, not
only the one after `shutdown()`.

What makes this worth building rather than accepting: `health()` reads **all-clear**. Every field
SPEC-030 added describes the worker, and there is no worker — so `retired` is `False` after a
completed `shutdown()`, and SPEC-019's alert idiom (`stopped_reason` plus `dropped`) is structurally
blind. That is the silent-loss shape SPEC-026, SPEC-030 and SPEC-032 each exist to end, reached
through the one path none of them covers. SPEC-032's post-close guard is invisible here precisely
because `close()` never happens.

A process that opens even one span is unaffected: it builds a worker, registers `atexit`, and every
existing guarantee applies.

The fix is to make sink closure a property of *having a configured sink*, not of having built a
worker — the `atexit` registration and `shutdown()`'s work both need to reach the orphan-only case.
Note that `_ensure_sink()` is what the orphan path already resolves, so the sink to close is
knowable without a worker.

#### Acceptance Criteria:

- [ ] After `configure(sink=…)` → `info()` → `shutdown()` with no span ever opened, the sink's
      `close()` has been called exactly once.
- [ ] The same holds at interpreter exit with no explicit `shutdown()`, so a process that just ends
      still flushes. Demonstrated in a subprocess, since `atexit` cannot be exercised in-process.
- [ ] `health().retired` reads `True` after that `shutdown()`, and a subsequent `info()` is counted
      the way SPEC-030 counts one — the pair `retired` + `submitted_after_shutdown` must become
      readable in this case, not stay vacuous.
- [ ] With SPEC-032's guard in place, that subsequent `info()` reaches a **closed** sink, is refused,
      and produces one `_diag` line via the orphan path's SPEC-025 guard rather than being silently
      buffered.
- [ ] No worker thread is created by any of this. A test asserts the thread count is unchanged
      across `configure` → `info` → `shutdown` with no span.
- [ ] `shutdown()` stays idempotent and still never raises, including when the sink's `close()`
      raises (SPEC-025: `atexit` must not fail the process).
- [ ] A process that *does* open a span behaves exactly as it does today — one `close()`, not two.
      The test asserts the count, since a second `close()` on a partially released sink is what
      SPEC-025's once-only flag exists to prevent.
- [ ] `configure()` called and then never logged at all closes nothing and creates nothing:
      registering an `atexit` handler for a sink no event ever reached is cost with no benefit, and
      the sink was never opened by this library.
- [ ] The `architecture.md` §13 entry recording this is struck through in place and marked closed by
      SPEC-031 FR-006, per SPEC-021's rule.

---

## Data Model

No new types, no signature changes to any public function.

```python
# sanitize.py — resolved once per pass, on the existing __slots__-ed pass object:
class _Coercer:
    __slots__ = ("_cfg", "_int_ceiling", "_parents", "truncated")

    def __init__(self, cfg: Config) -> None:
        ...
        self._int_ceiling = _int_digit_ceiling(cfg.max_value_bytes)
```

---

## API / Interface Contract

```python
# file.py — the deadline moves to the monotonic clock; the label does not:
self._next_rotation = time.monotonic() + self._interval    # deadline
suffix = time.strftime("%Y%m%d-%H%M%S", time.localtime())  # filename label, unchanged


# _socket.py — the family is resolved from the host:
def _make_udp(host: str) -> socket.socket:
    family = socket.getaddrinfo(host, None, type=socket.SOCK_DGRAM)[0][0]
    return socket.socket(family, socket.SOCK_DGRAM)
```

`_make_udp` gains a parameter; it is a module-private test seam, not public API, and both callers
are in this repository.

## Configuration / Environment

None.

## File & Folder Structure

```
src/log_foundry/
├── decorator.py    # modified — close the sink with no worker (FR-006)
├── model.py        # modified — resolve the per-call imports once
├── sanitize.py     # modified — integer ceiling once per pass
├── worker.py       # modified — docstring pointer only (FR-005)
├── console.py      # modified — docstring (stream, binding)
├── api.py          # modified — stale comment
└── sinks/
    ├── file.py     # modified — monotonic rotation deadline
    ├── stdout.py   # modified — docstring (binding)
    ├── _socket.py  # modified — UDP address family
    ├── syslog.py   # modified — docstring (IPv6)
    └── logstash.py # modified — docstring (IPv6)

tests/
├── test_sanitize.py      # modified — ceiling read once; SPEC-020 boundaries unchanged
├── test_sinks_file.py    # modified — clock-step rotation
├── test_sinks_syslog.py  # modified — IPv6 UDP
├── test_worker.py        # modified — _release_waiters mixed-queue case
└── test_api.py           # modified — orphan-only shutdown closes the sink (FR-006)

docs/architecture.md      # modified — §12 correction, §13 constraint
```

## Implementation Phases

### Phase 1: The two behaviour fixes

- `RotatingFileSink`'s monotonic deadline; UDP address-family resolution.
- Tests: backward and forward clock steps; IPv6 and IPv4 UDP; unresolvable host; the `_make_udp`
  seam still works.

### Phase 2: The micro-inconsistencies

- Integer ceiling once per pass; resolve `model.py`'s per-call imports.
- Tests: limit read at most once; SPEC-020 boundary behaviour identical; no import cycle.

### Phase 3: Documentation and the recorded constraint

- The three FR-003 corrections, including the struck-through `architecture.md` §12 note.
- The FR-005 §13 constraint entry, the docstring pointer, and the `_release_waiters` test.

### Phase 4: The orphan-only sink close (FR-006)

- Last and separate, because it is the only phase here that changes observable behaviour, and it
  should not be reviewed alongside four documentation edits.
- Close the sink on the no-worker path, from both `shutdown()` and `atexit`, without creating a
  worker.
- Tests: explicit shutdown; interpreter exit in a subprocess; `retired` becoming truthful; the
  refused follow-up log; thread count unchanged; exactly one `close()` on the span path; nothing
  registered when nothing was ever logged.
- Strike through the `architecture.md` §13 entry and mark it closed.
