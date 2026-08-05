# Spec: Audit Small Corrections

**ID:** SPEC-031  
**Status:** Draft  
**Last Updated:** 2026-08-05  
**Depends On:** SPEC-008, SPEC-009, SPEC-020

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

None of them changes a public contract. Grouping them keeps six trivial branches from becoming six
specs, and gives each a recorded resolution.

## Scope

### In Scope

- `RotatingFileSink`'s time trigger.
- IPv6 for UDP socket transport.
- Three documentation corrections where a claim is false.
- Two micro-inconsistencies in `sanitize.py` and `model.py`.
- Recording the one item deliberately *not* changed, per SPEC-021's rule.

### Out of Scope

- **Anything from SPEC-024 through SPEC-030.** Those are the audit's substantive findings and each
  has its own spec. This one is strictly the residue.
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
└── test_worker.py        # modified — _release_waiters mixed-queue case

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
