# Tests

These tests are written **ahead of the implementation** and track the phases in
[`../docs/implementation-guide.md`](../docs/implementation-guide.md). Each test guards on
the feature it needs, so the suite is green from day one and lights up phase by phase as
you build.

| File | Phase | Skips until |
|------|-------|-------------|
| `test_ids.py` | 2 | `log_foundry.ids` exists |
| `test_model.py` | 3 | `log_foundry.model` exists + public API |
| `test_context.py` | 4 | `log_foundry.context` exists |
| `test_decorator.py` | 6-7 | `log_foundry.configure/trace/info` exist |
| `test_decorator_async.py` | 8 | same, plus `pytest-asyncio` installed |

## Running

```bash
poetry install --with dev       # first time: gets pytest, pytest-asyncio, etc.
poetry run pytest               # run everything (parallel by default: -n 12, ~35 s)
poetry run pytest -n 0          # ...serially; required for -s and --pdb
poetry run pytest -v            # see which tests run vs. skip
poetry run pytest --cov=log_foundry
```

Skipped tests are expected early on — `-v` shows `SKIPPED (… not implemented yet)`.

## Conventions

- **`FakeSink`** (in `conftest.py`) is the standard test double: it records emitted batches
  so you assert on event dicts, never on stdout or the network.
- **Context tests run inside `contextvars.copy_context().run(...)`** so their span/baggage
  mutations stay isolated.
- Tests assert on **contract fields** (`status`, `trace_id`, `parent_span_id`), not on the
  exact text of auto-generated span boundary messages — rename those freely.
- **Once the async worker lands (Phase 9):** `fake_sink.events` won't be populated until
  the worker drains. Add a synchronous-flush test mode or drain via `log_foundry.shutdown()`
  before asserting. See the note in `conftest.py`.
