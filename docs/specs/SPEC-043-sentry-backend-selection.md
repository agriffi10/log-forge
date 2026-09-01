# Spec: Sentry Backend Selection

**ID:** SPEC-043  
**Status:** Completed  
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
`True`. Measured on 2.68.1, three client states cannot deliver and only one of them is inactive:

| State | `is_active()` | `transport` |
|---|---|---|
| Never initialised (`NonRecordingClient`) | `False` | `None` |
| `init()` with no DSN — the `SENTRY_DSN`-unset case | `True` | `None` |
| `init(dsn=…)` then `client.close()` | `True` | `None` |

So a client is usable only when it reports itself active **and** publishes a transport that is not
`None`. **The transport member is the one that binds**: on 2.68.1 there is no client where
`is_active()` is `False` while a transport is present, so no acceptance criterion below can tell a
two-member implementation from `transport` alone. `is_active()` is kept because it is the SDK's
documented answer and a future client may diverge — not because a criterion protects it, and this
paragraph is here so that nobody ticks AC-2 believing one does.

**The probe descends before it reads.** `__init__` holds whatever `_import_sdk()` returned, which
is the `sentry_sdk` **module**, and neither member lives there — measured,
`hasattr(sentry_sdk, "is_active")` is `False` while `hasattr(sentry_sdk.get_client(), "is_active")`
is `True`. So the probe calls `get_client()` when the held object publishes a *callable* one and
reads the result; where there is no callable `get_client`, or it returns `None`, the held object is
itself the probe target, which is what keeps an injected `client=` working whether it is a real
`Client` or a double.

Each member is probed by name and **absence means usable**, as `NATSSink._is_connected` probes
`is_connected` (SPEC-041 FR-004 AC-5) — an injected double publishing only `capture_event` must
keep working, and so must a pre-2.0 `sentry-sdk`, where `get_client` and `is_active` do not exist
at all. Three details the probe cannot get right by accident: `is_active` is a **method** and must
be *called*, because reading it as an attribute yields a truthy bound method and a guard that can
never fail; a non-callable `is_active` is treated as absent rather than as its own truth value;
and `transport` needs a sentinel, because "no `transport` member" and "`transport is None`" are
opposite answers and `None` cannot encode both.

**Both backends are built in `__init__`; only the choice is per emit.** Building the fallback
lazily on first emit would miss the worker's one-shot `log_foundry_stop_signal` offer — the setter
forwards to `self._http` at set time, and the worker offers once at ownership — so the fallback's
backoff would stop being interruptible and `shutdown()` could no longer cut it short, which is the
defect SPEC-027 exists to prevent. It would also rebind transport state inside `emit`, falsifying
the class docstring's SPEC-028 exemption.

#### Acceptance Criteria:

- [ ] AC-1: With the SDK installed but unable to deliver and a DSN given, events are delivered
      through the HTTP fallback rather than counted as sent by a no-op.
- [ ] AC-2: All three unusable states in the table above are treated as unusable. A test covering
      only the uninitialised one passes against a predicate that reads `is_active()` alone.
- [ ] AC-3: A client publishing neither member is treated as usable, so an injected double, a
      pre-SPEC-043 client and a pre-2.0 SDK are not broken by this check.
- [ ] AC-4: The check runs once per `emit` rather than once at construction, so an application
      that calls `sentry_sdk.init()` *after* building the sink starts using the SDK without
      rebuilding it. This holds for a sink holding the module; a caller who injects a *client*
      object is pinned to that object, which is what injecting one asks for.
- [ ] AC-5: A probe that raises leaves the client usable — a probe may never be the reason a batch
      fails (SPEC-025).
- [ ] AC-6: `self._http` is non-`None` after construction whenever a DSN was given and the
      selected backend can be `http`, including when the SDK imported — and a test covers the
      stop-signal forward reaching it on that path, not only where the SDK is absent. A lazily
      built fallback passes AC-1 and fails this.
- [ ] AC-7: Verified with the real `sentry-sdk` installed, in the **extras leg of the unit
      suite** — `integration.yml`'s `poetry run pytest tests` step — since an inactive client is a
      property of the real SDK that a fake asserts rather than demonstrates. Not
      `tests/integration/`: `tests/test_sink_integration_roster.py` records `sentry` as unverified
      for want of a local ingest, and a module added there fails that file's
      `test_the_service_rosters_agree_with_each_other`.
- [ ] AC-8: That verification **fails rather than skips** when the extras are expected, keyed on
      `LOG_FOUNDRY_EXTRAS=1` added to `integration.yml`'s unit-suite step — a signal the repo does
      not have today, and not `LOG_FOUNDRY_INTEGRATION`, whose name would then lie. A bare
      `importorskip` skips silently in the gating leg by design and would skip just as silently in
      the extras leg if that install regressed, which is the Overview's own failure recurring.

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

**An argument whose only consumer is a backend this construction will never select is an error,
not a silent ignore** — the thesis of this FR, applied to its own new argument. The rule is
deliberately limited to the two arguments defaulting to `None`: `max_retries` defaults to `3` and
an explicit `3` is indistinguishable from the default, so no honest check exists for it.

#### Acceptance Criteria:

- [ ] AC-1: A `backend` keyword selects between the SDK and the HTTP fallback, defaulting to a
      value that keeps today's behaviour for a caller who passes nothing and whose SDK can
      deliver.
- [ ] AC-2: A selection that cannot be built raises `ValueError` at construction: `"sdk"` with no
      client available, `"http"` with no DSN, and any value outside the three the Data Model
      names. The no-DSN message names the *selection* rather than the environment — the existing
      wording blames a missing `sentry-sdk` and is false when one is installed.
- [ ] AC-3: `opener=` raises `ValueError` under any construction that builds no HTTP fallback, and
      `client=` raises under `"http"`. Both are silently ignored today, which is the defect.
- [ ] AC-4: `opener=` does not select a backend. It is a transport injection point under a
      construction that has one; a single argument that both injects a test double and switches
      production behaviour is how this defect arose.
- [ ] AC-5: The **default** selection is never refused at construction on the grounds that the SDK
      is currently unable to deliver. FR-001 AC-4 exists because `init()` may follow, and the
      README's signature defaults `dsn=None` while telling the caller to run `sentry_sdk.init(...)`
      themselves — so refusing there would forbid an ordering the README permits. The existing
      refusal for no DSN *and* no importable SDK is unchanged.
- [ ] AC-6: The class docstring states which backend is chosen under each combination, `flush()`'s
      docstring stops claiming the client is consulted under a backend that holds none, and the
      README rows match. The two lint-asserted exemption claims — `adds no post-close guard` /
      `SPEC-032 FR-003` and `no transport lock` / `SPEC-028 FR-002` — survive the rewrite **as the
      lints read them**, whitespace-normalised per `_decision_doc`, and remain true: neither
      backend releases anything on `close()` or rebinds transport state.

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
- [ ] AC-4: It moves no `losses()` counter and writes no `_diag` line — a refusal is a failure
      *reported* to the worker, which announces it, and SPEC-032 settled that counting both
      reports one loss twice.

### FR-004: The suite selects the backend the way production does

#### Description:

`tests/conftest.py::sentry_http_fallback` pins the backend by monkeypatching `_import_sdk`, and
`tests/test_sinks_saas.py` does the same inline at two more sites. That fixture exists because
there was no other way to say which backend a test wanted. FR-002 gives it one, and a suite that
pins a backend by a different mechanism from the one production uses is a suite that stops
testing the mechanism.

Two of FR-002 AC-2's refusals — `"sdk"` with no client, and the unchanged no-DSN-no-SDK case — are
reachable only where the extra is genuinely absent, and removing the pin removes the only way to
manufacture that. Asking `_import_sdk()` which environment the test is running in is not pinning
it: it is a question put to the production function, and the answer decides which half of the
assertion applies.

#### Acceptance Criteria:

- [ ] AC-1: No test selects a `SentrySink` backend by monkeypatching `_import_sdk` — the check is
      on a `setattr` of it, not on the name, which appears in prose in two files. The fixture and
      the two inline pins either use the new argument or are removed.
- [ ] AC-2: A test whose expectation depends on whether the extra is installed derives it from
      `_import_sdk()` and asserts the complement in the other leg, so it is vacuous in neither.
      `test_sentry_without_sdk_or_dsn_raises` is the one test AC-1 would otherwise delete outright,
      since converting it changes which Data Model row it covers.
- [ ] AC-3: Every converted test asserts the same behaviour as before and passes in both
      environments — the no-extras gating leg and the extras leg.

---

## Data Model / Interface Contract

`src/log_foundry/sinks/sentry.py`:

```python
Backend = Literal["auto", "sdk", "http"]

class SentrySink:
    client: Any          # the SDK object or None; None whenever backend == "http"
    _http: HTTPSink | None

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

`Backend` is **not** added to `log_foundry.__all__`; callers pass the string literals, which the
class docstring and the README name. Adding a public symbol is a SPEC-034 decision and this spec
does not need one.

The selector is spelled `backend` rather than the two obvious alternatives because
`tests/test_public_surface.py` lints `src/`, `tests/` and `README.md` for a keyword of that other
name and for a `.sdk` attribute (SPEC-034 FR-002). The same lint is a regex over source *lines*, so
this paragraph must be paraphrased rather than quoted where the reasoning is recorded in a
docstring.

`self.client` stays declared `Any`. Once it can be `None`, the emit branch must key on the
**selection** rather than on `client is not None`: the old condition still type-checks and still
runs, and under `auto` with an unusable client it takes the SDK branch anyway — the defect,
preserved through the fix.

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
| `http` | ignored | no | — | — | `ValueError` at construction |

"Available" means a `client=` was injected or `_import_sdk()` returned the module. "Can deliver"
is FR-001's predicate, evaluated once per `emit`. A row building no `_http` rejects `opener=`, and
the two `http` rows reject `client=`, per FR-002 AC-3.

`flush()` still calls a held client's `flush` even when the predicate currently reads it as
unusable: the client may become usable before the next emit, and flushing one that is not is a
no-op. Under `"http"` no client is held and it stays the documented no-op.

---

## Implementation Phases

### Phase 1: FR-001, FR-002 and FR-003

The whole of the sink's behaviour — selection argument, construction errors, per-emit predicate
and the total-failure refusal — with unit coverage built on doubles. They land together because
FR-002's default is defined in terms of FR-001's predicate, and because a Phase 1 without FR-003
routes the no-backend case into `_post_envelope`'s assertion: still refused, but via
`transport_errors` and a `_diag` line that FR-003 AC-4 then forbids, so splitting it means shipping
a counter that moves in one PR and stops in the next.

FR-001 AC-7 and AC-8 are the exception and land in Phase 2 — they need CI wiring, so Phase 1 does
not close FR-001.

### Phase 2: FR-004, and FR-001 AC-7 + AC-8

The suite's conversion off `_import_sdk` patching, the `LOG_FOUNDRY_EXTRAS` signal in
`integration.yml`, and the real-SDK verification keyed on it.

---

## Revision history

The draft reviewed on 2026-08-31 was amended twice before build.

**After the first review** (spec frame):

- FR-001 named `is_active()` as the discriminator, probed on the object `__init__` holds. That
  object is the `sentry_sdk` **module**, which publishes no `is_active`, so the probe would have
  classified the defective path as usable and left the defect in place with the criteria green.
- `is_active()` alone was measured insufficient; the predicate now takes the transport too.
- ~~FR-003 AC-4 required a construction-time refusal when no DSN and no active SDK.~~ Struck: it
  contradicted FR-001's per-emit check and would have raised on the ordering the README permits.
  Construction-time refusal is now FR-002 AC-2's, and covers only a selection that can never be
  built.
- FR-002 gained the precedence rule and the Data Model table; FR-004 is new.

**After the second review** (implementer frame, which built the spec cold and ran both suites
green):

- A third unusable client state was measured — `init(dsn=…)` then `client.close()` leaves
  `is_active()` true with a `None` transport — and AC-2 now names all three.
- The `is_active()` member was shown to be unfalsifiable against 2.68.1. Rather than drop it or
  leave a criterion that cannot fail, FR-001 now says so in writing.
- The probe's undecided edges are settled: a non-callable `is_active`, a `get_client` returning
  `None`, and the sentinel that distinguishes an absent `transport` from a `None` one.
- FR-002 AC-3's rule was stated as a rule rather than a two-item list, and bounded to the
  arguments that default to `None`.
- FR-004 AC-2 is new: FR-002 AC-2's `"sdk"`-with-no-client refusal is unreachable in the extras
  leg once the pin is gone, and the obvious test for it goes red there.
- ~~"a module added there fails three roster assertions"~~ — struck: it fails one test. The claim
  was carried over from a review report without being run.
- AC-6's lint-asserted strings are matched whitespace-normalised; the literal spellings are split
  across lines by the 100-column limit and are not substrings of the docstring.
- Phase 1 absorbed FR-003, which removes an intermediate `transport_errors` behaviour the earlier
  split would have shipped and then reversed.
