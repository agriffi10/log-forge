# Completed Spec — SPEC-073: The HTTP Family on the Delivery Engine

## What was completed?

- `HTTPSink` and its eight subclasses offer `deliver`. One call is one **round** (`HTTPSink._round`):
  every chunk in one request, classified by one private method, `_exchange`, which `_send` shares,
  and each event's fate returned by index. The engine resends only the `RETRYABLE` events within
  the 1.x total, with one wait per round. `emit` is the private
  `_engine.emit_via_deliver(..., deliver=self._round)`, the adapter PR 312 made public as
  `log_foundry.emit_via_deliver`; this spec adds no public name.
  `_send` keeps its 1.x contract on one-request engine runs counted into a throwaway
  `SinkCounters`. Gone: `_BACKOFF_BASE`, `_sleep_backoff`, `_deliver`, `_abandon` and the loop.
- `sinks/_engine.py` gains the family-neutral **run context** (`When`, `RunContext`, `_Drops`,
  `run_context`, `detached`, `one_round`, `_commit`), which keeps every family counter's 1.x name and
  unit, and the "no engine settled line beside a sink line" refinement SPEC-072 FR-003 states.
- `ElasticsearchSink`, `HoneycombSink` and `OTLPSink` override `_outcomes` and give up through the
  one `HTTPSink._unadjudicated`; each `_handle_response` is a 1.x shim. `HTTPSink.__init_subclass__`
  keeps a third-party `emit` override or bool `_handle_response` on its 1.x path.
- Tests: `tests/test_http_rounds.py` and `tests/test_run_context.py` (new), and an `http` form in
  `tests/test_invariants_model.py`.
- The frame-1 diff review (`9892c6a6`): items map back to events by identity, then by unclaimed
  position, so an `_items` override returning rebuilt events maps exactly, and an event no request
  carries is `DROPPED` only when that many were dropped and counted, else `REJECTED`; only a
  raising reader gets the `raised <Type>` line. `_abandon_unreduced` keeps its 1.x signature
  (`181745b5`).
- Deviations, each in the spec's 2026-11-08 revision-history entry: FR-004's mechanism and the base
  `_outcomes` built in Phase 2, beside the first `deliver`; the plan review's two narrowings of
  eligibility; N1 to N3 below; the shim's non-empty clause; per-round aggregation of
  request-building faults; criterion 2.9 measured over the five twin-path responses. The Phase 2
  commit (`8663f868` as rebased) was knowingly red on four ids of
  `test_every_http_subclass_counts_no_413_drop_when_down`, green from Phase 3 (`2e074d70`).

## What changed from earlier specs?

- Counts that move (FR-002): a total failure counts once per batch; a lone `413` beside a failed
  batch is the catcher's; a request-building fault is `dropped`; a raising `2xx` reader is
  `dropped_unadjudicated`. Counts and lines land when the run ends, and a `BaseException` mid-run
  discards them. `emit`'s total-failure message is the adapter's, counted in events.
- Tests edited, each with the mechanism it pinned, from `git diff 26fad7ab..9892c6a6 --stat --
  tests/` and the commits:
  - `log_foundry.sinks.http.wait` (gone with `_sleep_backoff`), re-pointed at `_retry.wait`: the
    `_no_sleep` fixtures of `test_sinks_http`, `_render`, `_honeycomb`, `_otlp`, `_http_body_read`,
    `test_wrapper_failures`, `test_sinks_http_auth`, the four `_header_*` files, and PR 312's
    `test_sinks_http_body_hook`, `_credentials` and `_header_values`; the module tuple in
    `test_sink_losses.py`; OTLP's stop-signal spy, `test_sinks_http_body_read.py`'s Retry-After
    recorder and `test_numeric_doors.py`'s `_retry_after_waits` with its
    `_BACKOFF_BASE` reads (now `_engine.BACKOFF_BASE`): sixteen files in all.
  - `_sleep_backoff` on the instance: `test_sinks_http_url.py`'s two sites (a recording stop signal
    and a set one), `test_classify.py`'s `_LOOPS` entry and `_sentry` helper,
    `test_sink_retry.py`'s branch, and a corpus row in `test_sinks_http_header_collisions.py`.
  - `_send`'s loop: `test_classify.py`'s floor (12 to 11) and its corpus ids; `test_sink_retry.py`
    pins the base module set less `http`.
  - `_abandon`'s and `_abandon_unreduced`'s immediate stores and `HTTPSink.emit`'s `Deferral`:
    `test_deferral.py`'s `HTTPSink` site and `IMMEDIATE` entries, now `_engine._commit` and a
    `SETTLED` shape check.
  - The worker's whole-batch resend of a raised HTTP batch: Probe D's six functions (14 ids) in
    `test_sinks_http_refused.py`, which also lose the `_arm` instance spy; the Honeycomb and OTLP
    worker tests (1 POST, counted once; the OTLP one renamed).
  - The instance `_handle_response` spy: OTLP's truncated-`2xx` test, now on `_outcomes`.
  - `HTTPSink.emit`'s own `delivered none of N chunk(s)` raise (N1, N2): one `match=` each in
    `test_sinks_http.py` and `test_sinks_elasticsearch.py`.
  - `HTTPSink.emit` reaching `_items` directly (N3): `test_encode.py`'s `derive` follows `deliver=`.
  - Readers found only through `_handle_response`: `test_declared_exceptions.py`'s reader set and
    `test_sinks_http_body_read.py`'s `_BODY_READERS`, each asserted equal to the `_outcomes` set.
  - New after review, in `test_http_rounds.py`: the per-run search state (two adjacent `503`s
    resent with no `413`, the smallest refused size, 27 requests for spent reductions), the
    unreduced chunk on the base and a 1.x-signature override, the item mapping (an enriching and
    an omitting `_items`), a retryable `413`, `_send` re-raising a `Request` build failure, the
    first-exception line, malformed reader results, a shim inside a round, an engine-native
    parent with its own `emit`, and a bool reader's engine line; `test_invariants_model.py` drops
    its HTTP recorder records at teardown (`8ae95369`, the fork flake).
- Decisions: one new, *The HTTP family delivers in rounds on the engine, and a run context keeps
  its counters' units*; nine rewritten in place and *A sink may report per event* applied, each with a
  2026-11-08 History entry (FR-005 lists them).
  `docs/invariants.md` entries 2 and 7, `architecture.md`'s drop-deferral paragraph (a superseded
  marker) and its `timeout` open item, the README, the inventory's `http` and engine rows, and the
  1.2.0 release notes.

## Verification

- Local, Python 3.13.1, at `9892c6a6` plus this revision: ruff, mypy, pytest, spec-lint,
  docs-lint and docstring-lint, each exit 0.
- Criterion 5.3, rerun after the rebase: `git worktree add --detach <dir> 26fad7ab`, HEAD's
  `test_sinks_http_refused.py`, `test_sinks_honeycomb.py` and `test_sinks_otlp.py` copied in, and
  `PYTHONPATH=<dir>/src pytest -n 0` over the 14 Probe D ids and the two worker tests: 16 failed,
  and the two all-`413` tests that must pass unedited passed.
- Criterion 5.7, rerun after the rebase in a detached worktree at `9892c6a6`: expecting `http` back in
  `test_sink_retry.py`'s pinned set reddens `test_the_retry_loop_modules_are_the_base_set_less_http`;
  a `for attempt in range(self.max_retries + 1)` loop planted in `_send` reddens
  `test_every_retry_loop_asks_its_sinks_predicate`,
  `test_the_sweep_covers_every_retry_loop_the_roster_finds` and two `test_sink_retry.py` checks.
- FR-005's grep, run from the repo root at `9892c6a6` plus this revision. It reads README.md and the live docs (not
  Completed specs, delivery docs, audits, shipped release notes or this spec) a paragraph at a
  time, split into sentences so a wrapped claim is seen, and prints `<file>:<paragraph>.<sentence>`
  for each sentence naming `HTTPSink._send` or `` `_send` `` with retry or loop wording, or a worker
  resend beside an HTTP-family name:

  ```sh
  { echo README.md; ls docs/invariants.md docs/architecture.md docs/component-inventory.md \
    docs/implementation-guide.md docs/release-notes/v1.2.0.md docs/specs/ARCS.md
    find docs/decisions docs/process docs/best-practices -name '*.md' | LC_ALL=C sort
    grep -L 'spec_status: Completed' docs/specs/SPEC-*.md | grep -v SPEC-073; } |
  while read -r f; do awk -v F="$f" 'BEGIN { RS = "" } { p = tolower($0); gsub(/\n/, " ", p)
    n = split(p, s, /[.;] /); for (i = 1; i <= n; i++) {
    a = (s[i] ~ /httpsink\._send[^a-z_]/ && s[i] ~ /retr|loop|attempt/) || s[i] ~ /`_send`[^.]*(retr|loop)/
    w = s[i] ~ /worker[^.]*(re-?sends?|re-?sent|retr(y|ies|ied)|re-?post)/ || s[i] ~ /re-?sends? (a|the) (raised|raising|whole) batch/
    h = s[i] ~ /httpsink|http family|http sink|elasticsearch|opensearch|honeycomb|otlp|datadog|splunk|newrelic|new relic|lokisink|_bulk/
    if (a || (w && h)) print F ":" NR "." i } }' "$f"; done
  ```

  Output, 25 sentences, grouped (`sink-delivery/` is `docs/decisions/sink-delivery/`):
  `README.md` 101.24; `docs/architecture.md` 74.1, 138.10, 142.38;
  `docs/decisions/event-assembly/a-transforms-output-is-checked-…` 2.2; in `sink-delivery/`:
  `a-2xx-bulk-response-…` 2.1, `a-client-exception-…` 2.3 and 2.11, `a-dropped-count-…` 2.3,
  `a-populated-partial-success-…` 2.13, 2.14 and 5.1, `a-protocols-retry-set-…` 2.2 and 5.1,
  `a-raising-emit-…` 3.2, `a-sink-declares-…` 1.3, 3.1 and 16.1, `after-a-status-line-…` 2.4,
  `the-http-family-delivers-in-rounds-…` 2.2 and 8.1, `the-sinks-keep-four-…` 5.1 and 12.1;
  `docs/release-notes/v1.2.0.md` 79.1; `docs/specs/SPEC-074-…` 38.8. Every one was read: each
  states the new mechanism, is marked as before SPEC-073, is a measurement anchored to a commit,
  says the worker does **not** resend, is the general rule for a sink not on the engine, is about
  the AWS sinks' `_send`, or is unrelated (a `max_retries` floor). None says `_send` retries the
  chunk path or the worker resends an HTTP batch that delivered nothing.
- **Owed on CI**: the Python 3.12 leg of `ci.yml`, and `integration.yml` (real services and the
  suite with every extra), neither run locally.

## Follow-ups

- The hook guard covers the request-building hooks and the `2xx` readers only. A third-party
  override of `_retryable_status`, `_response_limit`, `_items`, `_take`, `_event_ceiling`,
  `_chunk_budget`, `_drop_refused` or `_abandon_unreduced` that raises escapes the round, as it
  escaped 1.x `emit`, and the engine fails that round's events. `_abandon_unreduced` keeps its
  1.x signature `(chunk)` and reads the run's context through `run_context()`, so a third-party
  override written against 1.x is still called.
