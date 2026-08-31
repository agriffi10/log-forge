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
caller whose SDK is installed but cannot deliver gets a sink that reports every event as sent
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
- **Judging whether Sentry accepted an event the SDK's client took.** The predicate answers
  whether the client has somewhere to send to, never whether the send succeeded — the SDK's
  transport is asynchronous and reports nothing back, which `flush()`'s docstring already records.

---

## Functional Requirements

### FR-001: An SDK that cannot deliver is not a usable backend

#### Description:

`__init__` calls `_import_sdk()` whenever no `client=` was passed, and keeps whatever it returns.
`sentry_sdk.get_client()` on an uninitialised process returns a `NonRecordingClient` whose
`capture_event()` is a no-op that returns without raising — so `_capture` returns `True` and the
sink counts a delivery that never happened.

Measured against `sentry-sdk` 2.68.1 with no `init()`: two `emit`s reported `sent=2`,
`transport_errors=0`, `losses=SinkLosses(dropped=0, failed=0)`, and nothing left the process.
This is SPEC-026 FR-001's shape — a sink the worker believes, so its retry never engages and
`health().failed_batches` never moves.

**The predicate is "has somewhere to send", and it takes two members, not one.** `is_active()` is
the SDK's documented answer and it is a *class* discriminator rather than a capability one:
`NonRecordingClient.is_active` returns a hardcoded `False` and `_Client.is_active` a hardcoded
`True`. So the common `sentry_sdk.init()` with `SENTRY_DSN` unset lands on the truthy side —
measured on 2.68.1, `is_active() == True` with `transport is None` and `capture_event` returning
`None` having sent nothing. That is the same defect this FR exists to fix, so a client is usable
only when it reports itself active **and** publishes a transport that is not `None`.

**The probe descends before it reads.** `__init__` holds whatever `_import_sdk()` returned, which
is the `sentry_sdk` **module**, and neither member lives there — measured,
`hasattr(sentry_sdk, "is_active")` is `False` while `hasattr(sentry_sdk.get_client(), "is_active")`
is `True`. So the probe calls `get_client()` when the held object publishes one and reads the
result, and reads the held object directly otherwise, which is what keeps an injected `client=`
working whether it is a real `Client` or a double.

Each member is probed by name and **absence means usable**, as `NATSSink._is_connected` probes
`is_connected` (SPEC-041 FR-004 AC-5) — an injected double publishing only `capture_event` must
keep working, and so must a pre-2.0 `sentry-sdk`, where `get_client` and `is_active` do not exist
at all. Unlike `is_connected`, `is_active` is a **method**: it is called, because reading it as an
attribute yields a truthy bound method and a guard that can never fail.

**Both backends are built in `__init__`; only the choice is per emit.** Building the fallback
lazily on first emit would miss the worker's one-shot `log_foundry_stop_signal` offer — the setter
forwards to `self._http` at set time, and the worker offers once at ownership — so the fallback's
backoff would stop being interruptible and `shutdown()` could no longer cut it short, which is the
defect SPEC-027 exists to prevent. It would also rebind transport state inside `emit`, falsifying
the class docstring's SPEC-028 exemption.

#### Acceptance Criteria:

- [ ] AC-1: With the SDK installed but unable to deliver and a DSN given, events are delivered
      through the HTTP fallback rather than counted as sent by a no-op.
- [ ] AC-2: Both the uninitialised case (`NonRecordingClient`) and the initialised-without-a-DSN
      case (`is_active()` true, `transport` `None`) are treated as unusable. A test covering only
      the first would pass against a predicate that reads `is_active()` alone.
- [ ] AC-3: A client publishing neither member is treated as usable, so an injected double, a
      pre-SPEC-043 client and a pre-2.0 SDK are not broken by this check.
- [ ] AC-4: The check runs once per `emit` rather than once at construction, so an application
      that calls `sentry_sdk.init()` *after* building the sink starts using the SDK without
      rebuilding it. This holds for a sink holding the module; a caller who injects a *client*
      object is pinned to that object, which is what injecting one asks for.
- [ ] AC-5: A probe that raises leaves the client usable — a probe may never be the reason a batch
      fails (SPEC-025).
- [ ] AC-6: `self._http` is non-`None` after construction whenever a DSN was given and the
      selected backend can be `http`, including when the SDK imported. A lazily-built fallback
      passes AC-1 and fails this.
- [ ] AC-7: Verified with the real `sentry-sdk` installed, in the **extras leg of the unit
      suite** — `integration.yml`'s `poetry run pytest tests` step — since an inactive client is a
      property of the real SDK that a fake asserts rather than demonstrates. Not
      `tests/integration/`: that directory's derived roster
      (`tests/test_sink_integration_roster.py`) records `sentry` as unverified for want of a local
      ingest, and a module added there fails three roster assertions.
- [ ] AC-8: That verification **fails rather than skips** when the extras are expected. A bare
      `importorskip` skips silently in the gating leg by design and would skip just as silently in
      the extras leg if that install regressed — which is the Overview's own failure, recurring.

### FR-002: A caller can select the backend explicitly

#### Description:

`opener=` is documented as the fallback's injection point, and passing it is an unambiguous
request for the fallback — but it is silently ignored whenever the SDK imports. Measured: with
`sentry-sdk` installed, `SentrySink(dsn, opener=…)` gave `_http is None` and made **zero** calls
to the opener.

That also has a real deployment case behind it: `sentry-sdk` is a common transitive dependency, so
a project can acquire it without asking for it and have this sink silently change backend.

**An explicit selection is honoured, and FR-001's check only chooses for the default.** Under
`backend="sdk"` a client that cannot deliver is FR-003's refusal, not a silent diversion to HTTP:
the caller named a backend, and quietly substituting another is this defect in a new place. Under
`backend="http"` the SDK is not consulted, held, or flushed.

**An argument that cannot be honoured under the selected backend is an error, not a silent
ignore** — that is the whole thesis of this FR, applied to its own new argument.

#### Acceptance Criteria:

- [ ] AC-1: A `backend` keyword selects between the SDK and the HTTP fallback, defaulting to a
      value that keeps today's behaviour for a caller who passes nothing and whose SDK can
      deliver.
- [ ] AC-2: A selection that cannot be built raises `ValueError` at construction: `"sdk"` with no
      client available, `"http"` with no DSN (which already raises), and any value outside the
      three the Data Model names.
- [ ] AC-3: An argument the selected backend cannot use raises `ValueError` rather than being
      ignored — `opener=` under `"sdk"`, `client=` under `"http"`.
- [ ] AC-4: `opener=` does not select a backend. It is a transport injection point under any
      backend that can be `http`, and under `"sdk"` it is AC-3's error; a single argument that
      both injects a test double and switches production behaviour is how this defect arose.
- [ ] AC-5: The **default** selection is never refused at construction on the grounds that the SDK
      is currently unable to deliver. FR-001 AC-4 exists because `init()` may follow, and the
      README documents `SentrySink()` with no DSN followed by the caller's own
      `sentry_sdk.init(...)` — refusing there would forbid the ordering the README teaches. The
      existing refusal for no DSN *and* no importable SDK is unchanged.
- [ ] AC-6: The class docstring states which backend is chosen under each combination, `flush()`'s
      docstring stops claiming the client is consulted under a backend that does not hold one, and
      the README rows match. The lint-asserted strings `**adds no post-close guard**` /
      `SPEC-032 FR-003` and `**no** transport lock` / `SPEC-028 FR-002` survive the rewrite, and
      remain true — neither backend releases anything on `close()` or rebinds transport state.

### FR-003: Neither backend able to deliver is reported, not absorbed

#### Description:

With no usable SDK and no HTTP fallback there is no way to deliver, and the current sink returns
normally. SPEC-026 FR-001: a sink that delivered none of a batch raises, so the worker's retry
engages and `failed_batches` moves.

#### Acceptance Criteria:

- [ ] AC-1: When no backend can deliver, `emit` raises `SinkDeliveryError` naming the number of
      **qualifying** events, as the existing total-failure path does.
- [ ] AC-2: The raise happens after the level filter has run, so a batch with no qualifying
      events returns normally and `skipped` still counts. `emit([])` remains the no-op
      `sinks/base.py` documents. A guard at the top of `emit` passes AC-1 and fails this.
- [ ] AC-3: Events below `min_level` are still skipped rather than refused: a batch of nothing but
      skipped events has nothing to deliver and is a successful emit, which is existing behaviour
      this FR must not change. Asserted with **no** backend available, since the existing test for
      it builds a working fallback and so never enters this case.
- [ ] AC-4: It moves no `losses()` counter — a refusal is a failure *reported* to the worker, not
      one absorbed, and SPEC-032 settled that counting both reports one loss twice.

### FR-004: The suite selects the backend the way production does

#### Description:

`tests/conftest.py::sentry_http_fallback` pins the backend by monkeypatching `_import_sdk`, and
`tests/test_sinks_saas.py` does the same inline at two more sites. That fixture exists because
there was no other way to say which backend a test wanted. FR-002 gives it one, and a suite that
pins a backend by a different mechanism from the one production uses is a suite that stops
testing the mechanism.

#### Acceptance Criteria:

- [ ] AC-1: No test selects a `SentrySink` backend by patching `_import_sdk`. The fixture and the
      two inline pins either use the new argument or are removed.
- [ ] AC-2: The tests that used the fixture assert the same behaviour as before and pass in both
      environments — the no-extras gating leg and the extras leg.
- [ ] AC-3: A test that asserts the fallback is in use derives that from the selection, not from
      the absence of an extra, so it cannot silently become vacuous in either leg.

---

## Data Model / Interface Contract

`src/log_foundry/sinks/sentry.py`:

```python
Backend = Literal["auto", "sdk", "http"]

class SentrySink:
    def __init__(
        self,
        dsn: str | None = None,
        *,
        min_level: str = "ERROR",
        backend: Backend = "auto",
        client: Any = None,
        opener: Any = None,
        max_retries: int = 3,
    ) -> None: ...
```

`backend` is spelled that way rather than `sdk=`/`use_sdk=` because
`tests/test_public_surface.py` lints `src/`, `tests/` and `README.md` for a `sdk=` keyword and a
`.sdk` attribute (SPEC-034 FR-002); a selector spelled `sdk=` fails that lint repo-wide.

What is held after construction, and what each `emit` then does:

| `backend` | `client` available | DSN | `self.client` | `self._http` | Per emit |
|---|---|---|---|---|---|
| `auto` | yes | yes | the client | built | SDK if it can deliver, else HTTP |
| `auto` | yes | no | the client | `None` | SDK if it can deliver, else FR-003 refusal |
| `auto` | no | yes | `None` | built | HTTP |
| `auto` | no | no | — | — | `ValueError` at construction (unchanged) |
| `sdk` | yes | either | the client | `None` | SDK if it can deliver, else FR-003 refusal |
| `sdk` | no | either | — | — | `ValueError` at construction |
| `http` | ignored | yes | `None` | built | HTTP |
| `http` | ignored | no | — | — | `ValueError` at construction (unchanged message) |

"Available" means a `client=` was injected or `_import_sdk()` returned the module. "Can deliver"
is FR-001's predicate, evaluated once per `emit`. `client=` under `"http"` and `opener=` under
`"sdk"` are FR-002 AC-3 errors rather than the "ignored" the table would otherwise imply.

---

## Implementation Phases

### Phase 1: FR-001 and FR-002

The selection argument, its construction errors, and the per-emit capability predicate. They are
one change to `__init__` and `_capture` and cannot be sequenced apart: FR-002's default is defined
in terms of FR-001's predicate, so landing the argument first would ship an intermediate default
("SDK if importable") that FR-001 then changes.

### Phase 2: FR-003 and FR-004

The total-failure refusal, the suite's conversion off `_import_sdk` patching, and the real-SDK
verification FR-001 AC-7 and AC-8 require.

---

## Revision history

The draft reviewed on 2026-08-31 was amended before build:

- FR-001 named `is_active()` as the discriminator, probed on the object `__init__` holds. That
  object is the `sentry_sdk` **module**, which publishes no `is_active`, so the probe would have
  classified the defective path as usable and left the defect in place with the criteria green.
  The descent, and the requirement to *call* rather than read, are now stated.
- `is_active()` alone was measured insufficient: `sentry_sdk.init()` with no DSN reports active
  with a `None` transport and drops events silently. The predicate now takes the transport too.
- ~~FR-003 AC-4 required a construction-time refusal when no DSN and no active SDK.~~ Struck: it
  contradicted FR-001's per-emit check and would have raised on the exact ordering the README
  documents (`SentrySink()` before the caller's `sentry_sdk.init()`). Construction-time refusal is
  now FR-002 AC-2's, and covers only a selection that can never be built.
- FR-002 gained the precedence rule (an explicit selection is honoured, never diverted) and the
  Data Model table, neither of which the draft stated.
- FR-004 is new: the draft acknowledged that SPEC-041 pinned four tests via `_import_sdk` but did
  not say what became of that pinning.
