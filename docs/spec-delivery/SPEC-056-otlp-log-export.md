# Completed Spec — SPEC-056: OTLP Log Export

## What was completed?

Five FRs, built as `d7fc36a..` on `spec/056-build` from `ba3bd7d`, one commit per phase. The first
slice of the OpenTelemetry-shaped direction (`docs/decisions/public-api/`): OTLP produced natively,
over a private mapping, with no OpenTelemetry SDK in the runtime.

- **The mapping** (FR-001, `src/log_foundry/_otel.py`): `log_record` turns one assembled event
  into one OTLP/JSON `LogRecord`, total over every value assembly produces and the ones it cannot —
  unbound-method dispatch, surrogate repair, int64 and digit-limit rules, a 24-container depth
  cap, cycle detection by path, and a per-record node budget pinned equal to `sanitize._MAX_NODES`
  by test; keys render as `sanitize` renders them and a dropped attribute is counted. A leaf.
- **The envelope and its proof** (FR-002): `resource_key`, `request_body` (one `resourceLogs` per
  distinct stamp triple, first-seen order), `framing`, and the `SEMCONV_VERSION = "1.44.0"` pin
  with no URL constant. `tests/test_otel_envelope.py::roundtrip` parses every document with the
  generated classes (`opentelemetry-proto`, dev-only) and compares, with eight must-fail cases, and
  runs every record the mapping tests build through the same helper — 147 at `cb75551`.
- **The sink** (FR-003, FR-004, `src/log_foundry/sinks/otlp.py`): `OTLPSink(endpoint=…,
  **HTTPPlatformKwargs)`; two private hooks on `HTTPSink`, `_retryable_status` and
  `_response_limit`, defaulting to the family's behaviour; partial success on `rejected_records`
  and `dropped_unadjudicated`, never retried, all-rejected raising. `_valid_url` gained `name=`.
- **Registration** (FR-005): README rows, retry sentence and the *what it sends* subsection with an
  exact-set test; the frozen-path line; every HTTP roster, with the losses and retry builder lists
  now tied to the derived population (which brought `OpenSearchSink` into both); the two
  SPEC-058/059 sweeps amended to reach a total mapping; the typed consumer; `governs` and the rule.

Deviations, one line each: the pre-push review's additions (an AST leaf test instead of a
`sys.modules` one; `scope.version` asserted through `emit`; a lone-surrogate `level` in the corpus;
the counter lock observed under an injected preemption point) were built in; `docs-lint`'s
byte budget took three `CLAUDE.md` edits at 37,755/38,000 with the paragraph at 470 bytes; the
release notes went to a new `docs/release-notes/v1.1.0.md` — a step the ritual words as "top up the
pending notes", which `v1.0.2.md` cannot be since it says there is no OTLP sink in it — so `v1.0.2`
must be tagged before this lands.

## What changed from earlier specs?

- `HTTPSink._send` reads the retry predicate from a hook and `_attempt`'s read takes a bound when a
  subclass names one; with the default `None` every existing sink runs the literal same call. The
  base class's behaviour on a failed `2xx` body read (an `IncompleteRead` escaping `emit`, an
  `OSError` mid-body re-sent) is **unchanged** and filed separately; the probe-D sweep holds every
  subclass to it. What the bounded read changes on `OTLPSink` alone is recorded in the
  sink-delivery decision *A 2xx this sink cannot read is counted*: measured at `7641de1`, a `200`
  with `Content-Length: 100` and 20 bytes on the wire gives `read()` → `IncompleteRead` and
  `read(4194305)` → the 20 bytes, so a truncated body settles as full success there.
- `tests/test_sinks_http.py::FakeResponse.read` and `_IncompleteResponse.read` take an optional
  size; `test_sinks_http_refused.py`'s events carry their size under `fields`.
- `sinks/pubsub.py:140` carries `# type: ignore[import-not-found, unused-ignore]` and
  `pyproject.toml` a `google.cloud.*` mypy override, because the dev group now installs `protobuf`.
- SPEC-049's `_valid_url` message names the argument it is given (`url` by default).

## Follow-ups and open limits

- **The frozen-path baseline cannot see a missing line.** Removing `OTLPSink log_foundry.sinks.otlp`
  from `tests/data/frozen_sink_paths.txt` leaves `test_every_frozen_sink_import_path_still_resolves`
  green: it checks frozen → resolves and frozen → documented, not shipped → frozen. By the file's
  own design ("ADD a line when a new sink ships"); recorded, not chased.
- **The typed-consumer corpus is the file itself**, so removing a planted `rejects.py` line removes
  its expectation; the floor is 13 and the count is otherwise exact.
- **A Collector is not run in CI**; `UNVERIFIED["otlp"]` says so and the round trip is the proof.
- **Owed to CI**: `dependency-review` and `pip-audit` on the lock diff that adds
  `opentelemetry-proto` 1.45.2 and makes `protobuf` 7.35.1 non-optional.

## Verification

At `7641de1`: `pytest` 3421 passed, 10 skipped (exit 0) with `opentelemetry-proto` installed;
`ruff check`, `mypy` (cold cache, no extras), `docstring-lint.py`, `spec-lint.sh` and
`docs-lint.sh` exit 0. 31 mutants planted over the plan's silent-failure guards: 30 killed, one
confounded on its first run (the spec's 25-container-deep shared subtree let the depth cap supply
the mark the node budget should have) and killed after a within-depth case was added; the
predicted survivor — the separator byte in `_item_size` — was killed by a direct unit test. Each
Phase 3 and Phase 4 roster entry was removed once and its roster went red, except the two above.
`pytest --collect-only` names at `ba3bd7d` are all present at the head. Not run: `tests/integration`
(no Docker daemon here); owed on CI.
