# Spec: Sentry Backend Selection

**ID:** SPEC-043  
**Status:** Draft  
**Last Updated:** 2026-08-31  
**Depends On:** SPEC-026, SPEC-032, SPEC-041

## Overview

`SentrySink` has two backends — the `sentry-sdk` when the optional extra is installed, and an
HTTP-envelope fallback that POSTs to the DSN's ingest URL otherwise. Which one a caller gets is
decided by whether the SDK *imports*, and nothing else. That is the wrong question, and it costs
in both directions: a caller who explicitly asks for the fallback is silently given the SDK, and a
caller whose SDK is installed but never initialised gets a sink that reports every event as sent
while delivering nothing at all.

Both were found by SPEC-041's job, which ran the existing suite with the extras installed for the
first time. Four tests were green only because CI never installs the `sentry` extra; pinning them
made the suite honest but left the behaviour they had been hiding.

## Scope

### In Scope

- Choosing the backend on whether the SDK can actually deliver, not on whether it imports.
- An explicit way for a caller to select a backend.
- The loss-visibility rule (SPEC-026 FR-001) applied to the case where neither backend can deliver.

### Out of Scope

- **Any other sink's backend selection.** `LogstashSink` picks HTTP-vs-socket from its arguments,
  which is explicit already.
- **Initialising the SDK on the caller's behalf.** `sentry_sdk.init()` is process-global
  configuration with its own DSN, sampling and integrations; a logging sink calling it would take
  over configuration the application owns.
- **Changing the HTTP fallback's envelope format or transport.** Only which backend is selected.
- **Auto-detecting a *later* `init()` for a sink already constructed** beyond what FR-001's
  per-emit check gives for free.

---

## Functional Requirements

### FR-001: An inactive SDK is not a usable backend

#### Description:

`__init__` calls `_import_sdk()` whenever no `client=` was passed, and keeps whatever it returns.
`sentry_sdk.get_client()` on an uninitialised process returns a non-recording client whose
`capture_event()` is a no-op that returns without raising — so `_capture` returns `True` and the
sink counts a delivery that never happened.

Measured against `sentry-sdk` 2.68.1 with no `init()`: two `emit`s reported `sent=2`,
`transport_errors=0`, `losses=SinkLosses(dropped=0, failed=0)`, and nothing left the process.
This is SPEC-026 FR-001's shape — a sink the worker believes, so its retry never engages and
`health().failed_batches` never moves.

The SDK's own `is_active()` distinguishes the two states. It is probed by name, as
`NATSSink._is_connected` probes `is_connected` (SPEC-041 FR-004 AC-5), because an injected
`client=` need not be `sentry_sdk`.

#### Acceptance Criteria:

- [ ] AC-1: With the SDK installed but not initialised and a DSN given, events are delivered
      through the HTTP fallback rather than counted as sent by a no-op.
- [ ] AC-2: A client that does not publish `is_active` is treated as usable, so an injected
      double is not broken by this check and a pre-SPEC-043 client still works.
- [ ] AC-3: The check runs per emit rather than once at construction, so an application that
      calls `sentry_sdk.init()` *after* building the sink starts using the SDK without rebuilding
      it. Ordering between `init()` and `configure()` is not something this library can impose.
- [ ] AC-4: A client whose `is_active` raises is treated as usable — a probe may never be the
      reason a batch fails (SPEC-025).
- [ ] AC-5: Verified with the real `sentry-sdk` installed, in the job SPEC-041 built, since an
      inactive client is a property of the real SDK that a fake asserts rather than demonstrates.

### FR-002: A caller can select the backend explicitly

#### Description:

`opener=` is documented as the fallback's injection point, and passing it is an unambiguous
request for the fallback — but it is silently ignored whenever the SDK imports. Measured: with
`sentry-sdk` installed, `SentrySink(dsn, opener=…)` gave `_http is None` and made **zero** calls
to the opener.

That also has a real deployment case behind it: `sentry-sdk` is a common transitive dependency, so
a project can acquire it without asking for it and have this sink silently change backend.

#### Acceptance Criteria:

- [ ] AC-1: A keyword argument selects the backend explicitly, with a default that preserves
      today's behaviour where the SDK is active.
- [ ] AC-2: Asking for a backend that cannot be built is an error at construction, not a silent
      substitution — the fallback without a DSN already raises `ValueError`, and asking for the
      SDK without one installed must raise rather than quietly POST.
- [ ] AC-3: Passing `opener=` no longer selects a backend by itself; the argument that selects is
      the one named for it. Whether `opener=` alone should *imply* the fallback is decided in this
      FR rather than left to the implementer — it does not, because a single argument that both
      injects a test double and switches production behaviour is how this defect arose.
- [ ] AC-4: The class docstring states which backend is chosen under each combination, and the
      README row matches.

### FR-003: Neither backend able to deliver is reported, not absorbed

#### Description:

With no DSN and an inactive SDK there is no way to deliver, and the current sink returns normally.
SPEC-026 FR-001: a sink that delivered none of a batch raises, so the worker's retry engages and
`failed_batches` moves.

#### Acceptance Criteria:

- [ ] AC-1: When no backend can deliver, `emit` raises `SinkDeliveryError` naming the batch size,
      as every other sink's total-failure path does.
- [ ] AC-2: It moves no `losses()` counter — a refusal is a failure *reported* to the worker, not
      one absorbed, and SPEC-032 settled that counting both reports one loss twice.
- [ ] AC-3: Events below `min_level` are still skipped rather than refused: a batch of nothing but
      skipped events has nothing to deliver and is a successful emit, which is existing behaviour
      this FR must not change.
- [ ] AC-4: A construction that can never deliver — no DSN and no active SDK — is diagnosed at
      construction where it is detectable, so the failure is not deferred to the first event.
      Whether it raises or degrades is decided here: it **raises**, matching the existing
      `ValueError` for a fallback with no DSN.

---

## Implementation Phases

### Phase 1: FR-002

The selection argument and its errors, which FR-001 and FR-003 both build on.

### Phase 2: FR-001 and FR-003

The activity check and the total-failure path, plus the integration coverage AC-5 requires.
