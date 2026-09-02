# Tests

The unit suite runs against the library with **no optional extras installed** — the contract the
package ships under, and the leg that gates a merge: every module of `log_foundry` must import
with nothing but the standard library present. It is not the *only* environment CI uses; the
integration workflow installs `--all-extras` and runs this same suite a second time with
`LOG_FOUNDRY_EXTRAS=1`, under which a test needing an extra **fails** rather than skipping. Third-party sinks are exercised through fakes
in `sys.modules`; `tests/integration/` is the separate suite that talks to real services.

## Running

```bash
poetry install --with dev       # first time
poetry run pytest               # everything, in parallel (-n 12 by default)
poetry run pytest -n 0          # ...serially — REQUIRED for -s and --pdb, which xdist discards
poetry run pytest -v            # per-test names
poetry run pytest --cov=log_foundry
```

The parallel run is several times faster than the serial one, and the gap is mostly overlapped
waiting rather than cores — a good deal of this suite is about timeouts, drains and backoff.

## A skip is a signal, not the normal state

This suite predates most of the library, and every test once guarded on the module it needed so
the suite could be green before that code existed. That scaffolding is gone (SPEC-052): a
missing or broken `log_foundry` module is now an **error**, not a skipped file. If you see a
collection error, something is genuinely wrong — do not reach for a guard to quiet it.

What may still legitimately skip:

- **An optional extra is absent** — `pytest.importorskip("nats", …)` and the `sentry` probe. These
  can fire and must stay; removing them would red the no-extras run.
- **A parametrisation the case does not apply to** — a roster test that walks every module and
  excludes the one module that is itself the subject, with the exclusion named in the reason.
- **The host cannot provide something the test needs** — no IPv6 loopback, `localhost` not
  resolving to IPv4, or the distribution not installed (a bare `PYTHONPATH=src` run). Three sites,
  each naming the host condition in its reason. These do not fire on a normal developer machine or
  in CI, so if you see one, it is telling you something about your box.

Nothing else. In particular, **`tests/integration/` skips for nothing at all** — an unreachable
service there must *fail*, because a fixture that skips on an absent service exits 0 and a silently
dropped module reads as a smaller pass count (SPEC-041). The rule is stated in
`tests/integration/conftest.py`.

## Conventions

- **`FakeSink`** (`conftest.py`) is the standard double: it records emitted batches, so assertions
  are on event dicts, never on stdout or the network.
- **The `lf` fixture** gives you `log_foundry` configured with a `FakeSink` and flushing
  *synchronously*, because the background worker means `fake_sink.events` is otherwise empty when a
  pipeline test asserts. Worker behaviour itself — batching, retry, backpressure, shutdown — is
  covered directly in `test_worker.py`, not through this fixture.
- **Context tests run inside `contextvars.copy_context().run(...)`** so their span and baggage
  mutations stay isolated from the rest of the suite.
- **Assert on contract fields** (`status`, `trace_id`, `parent_span_id`), never on the exact text of
  an auto-generated span-boundary message — those are free to be reworded.
- **`run_concurrently`** (`conftest.py`) is the harness for races: a spec touching lifecycle or
  concurrency gets an execution harness rather than a review, because a race is not findable by
  reading.

## Docstrings

`src/` is held to the docstring rule by `poetry run python scripts/docstring-lint.py`, a local
pre-push gate. **`tests/` is deliberately outside its scope** — the rule `CLAUDE.md` states is
scoped to `src/`, and the checker takes no argument that would widen it. A test module needs a
docstring only when there is something extra to say.
