# Completed Spec — SPEC-043: Sentry Backend Selection

## What was completed?

- **`SentrySink` chooses its backend on whether one can *deliver*, not on whether `sentry-sdk`
  imports** (`src/log_foundry/sinks/sentry.py`). `_client_can_deliver()` runs **once per `emit`**,
  not once at construction, so a `sentry_sdk.init()` that follows the sink's construction takes
  effect without rebuilding it. The predicate reads **two** members, because `is_active()` is a
  class discriminator: of the three undeliverable states only one is inactive. `transport` is the
  member that actually binds, and FR-001 records that no acceptance criterion can distinguish the
  two-member check from reading `transport` alone — so a green AC-2 is not evidence for
  `is_active()`. A client publishing neither member counts as usable, so injected doubles and
  pre-2.0 SDKs are unaffected.
- **A `backend` keyword selects explicitly** — `Literal["auto", "sdk", "http"]`, default `"auto"`,
  which preserves prior behaviour for a caller who passes nothing and whose SDK can deliver. Under
  `"sdk"` an undeliverable client is a refusal, never a silent diversion to HTTP; under `"http"`
  the SDK is not consulted, held or flushed. `Backend` is deliberately **not** exported.
- **An argument whose only consumer is a backend this construction cannot select is a
  `ValueError`** — the spec's thesis applied to its own new argument. `opener=` was ignored
  outright whenever the SDK imported (measured: `_http is None`, zero opener calls). The rule
  covers the two *injection* arguments, `client=` and `opener=`; `max_retries=3` is excluded
  because an explicit `3` is indistinguishable from the default.
- **The no-backend case now reaches the existing total-failure raise.** `emit` already raised
  `SinkDeliveryError` naming the count of *qualifying* events (SPEC-026 FR-001); what changed is
  which events reach it. Before, a held-but-undeliverable client always took the SDK branch — the
  old `_capture` keyed on `client is not None`, and `_http` was built only when `client is None` —
  so a no-op `capture_event` counted the event as sent. The raise still happens **after** the level
  filter, so `emit([])` and an all-sub-`min_level` batch stay successful no-ops.
- **The suite now selects the backend the way production does** (FR-004). The
  `sentry_http_fallback` fixture monkeypatched `_import_sdk` — the mechanism under test — and was
  **removed**; its five users now pass `backend="http"`. Of the two inline pins, one does the
  same and the other became a *question* put to `_import_sdk()`, asserting the complement in the
  other leg so it is vacuous in neither.
- **The SDK path is verified against the real `sentry-sdk`** in SPEC-041's extras leg of the unit
  suite, now gated on a new `LOG_FOUNDRY_EXTRAS=1` in `integration.yml`. It **fails rather than
  skips**: a bare `importorskip` would skip just as silently in the extras leg if that install
  regressed, which is this spec's originating failure recurring.

**Deviation:** none in behaviour, but FR-002's rule is enforced for two arguments rather than for
every argument it describes. `dsn=` under `backend="sdk"` is also "an argument whose only consumer
is a backend this construction cannot select", and it is silently accepted and never read
(measured: constructed with a DSN and `backend="sdk"`, `_http is None`). FR-002 bounds the rule to
the arguments defaulting to `None` and then names only two; `dsn` defaults to `None` as well.
Recorded rather than fixed — enforcing it is a constructor change, not a completion — so a later
spec can close it deliberately.

## What changed from earlier specs?

- **`SentrySink.__init__` gained `backend=` and now rejects arguments it previously ignored** —
  `opener=` under `"sdk"` or no DSN, `client=` under `"http"`. A *new* no-DSN refusal names the
  selection (`backend='http'`); the pre-existing `auto` refusal, which fires only when no SDK is
  available at all, keeps its wording unchanged per FR-002 AC-5.
- **`self.client` can now be `None`**, so the emit branch keys on the resolved selection rather
  than `client is not None` — the old condition still type-checks and, under `auto` with an
  unusable client, still takes the SDK branch: the defect preserved through the fix.
- **`tests/conftest.py`'s `sentry_http_fallback` fixture is deleted.** It was shared test
  infrastructure; a later spec looking for it will not find it.
- Completes SPEC-041, which pinned the four `SentrySink` tests that were green only because CI
  never installs that extra, and whose FR-001 built the extras leg this spec gates.
- `docs/component-inventory.md`'s SaaS-sinks row no longer describes the backend as import-picked.

## Verification

Four gates green locally by exit code, and PR #174 merged green on both legs — the gating
no-extras matrix (3.12 and 3.13) and the job carrying the extras leg. The defect was **measured
before being fixed**: against `sentry-sdk` 2.68.1 with no `init()`, two `emit`s reported `sent=2`,
`transport_errors=0`, `losses=SinkLosses(dropped=0, failed=0)` and nothing left the process — a
sink the worker believes, so its retry never engages and `failed_batches` never moves. The build's
own hardening then found three assertions passing against the defect they named and two criteria
carrying no evidence.

**Completion was recorded late.** The implementation merged as
[#174](https://github.com/agriffi10/log-forge/pull/174); the header, index row and this document
were not written then, so the spec read `In Progress` through twelve later merges. Nothing was
outstanding in the code — every FR's artifacts were confirmed present and byte-identical on `main`
before this was written.
