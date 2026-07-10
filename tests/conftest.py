"""Shared fixtures for the log-forge test suite.

These tests are written *ahead* of the implementation (see docs/implementation-guide.md).
Every test guards on the feature it needs — `pytest.importorskip("log_forge.<module>")`
for whole modules, and an attribute check in the `lf` fixture for the public API — so the
suite stays green and simply *skips* anything not built yet. As you complete each phase,
the matching tests light up on their own.
"""

import pytest


@pytest.fixture(autouse=True)
def _reset_config():
    """Restore a fresh global ``Config`` before each test.

    ``log_forge.config._config`` is a process-wide singleton mutated by ``configure()``;
    without this, tests would leak identity/sink/defaults into one another and only pass by
    ordering. No-ops until the module exists.
    """
    try:
        from log_forge import config
    except ImportError:
        yield
        return
    config._config = config.Config()
    _reset_worker()
    yield
    # Tear down any process worker a test created so its daemon thread / sink don't leak.
    _reset_worker()


def _reset_worker() -> None:
    """Drain and clear the lazily-created process worker (SPEC-004), if present.

    Without this the first test's sink/thread would leak into later tests through the
    module-global ``decorator._worker``. No-ops until the worker module exists.
    """
    try:
        from log_forge import decorator
    except ImportError:
        return
    worker = getattr(decorator, "_worker", None)
    if worker is not None:
        worker.shutdown()
        decorator._worker = None


class FakeSink:
    """A Sink that records emitted batches so tests can assert on the event dicts.

    This is the test double from the guide: it exercises the part you wrote (span
    lifecycle, IDs, schema, context) without the part you didn't (the network).
    """

    def __init__(self) -> None:
        self.batches: list[list[dict]] = []

    def emit(self, batch: list[dict]) -> None:
        self.batches.append(list(batch))

    def close(self) -> None:
        pass

    @property
    def events(self) -> list[dict]:
        """All emitted events, flattened across batches."""
        return [event for batch in self.batches for event in batch]


@pytest.fixture
def fake_sink() -> FakeSink:
    return FakeSink()


@pytest.fixture
def lf(fake_sink: FakeSink, monkeypatch):
    """`log_forge` configured with a FakeSink, flushing synchronously.

    SPEC-004 rewired the decorator to hand finished spans to a background worker, so
    `fake_sink.events` would no longer be populated by the time a pipeline test asserts.
    These tests care about span/event/context semantics, not delivery timing, so we keep
    flushing inline here (the synchronous-flush test mode this fixture always anticipated);
    the worker's own batching/retry/backpressure/shutdown behavior is covered directly in
    `test_worker.py`.
    """
    log_forge = pytest.importorskip("log_forge")
    for attr in ("configure", "trace", "info"):
        if not hasattr(log_forge, attr):
            pytest.skip(f"log_forge.{attr} not implemented yet")
    log_forge.configure(service="test", version="0.0.0", env="test", sink=fake_sink)

    decorator = pytest.importorskip("log_forge.decorator")
    from log_forge.config import _ensure_sink

    def _sync_flush(span) -> None:
        _ensure_sink().emit(span.events)

    monkeypatch.setattr(decorator, "_flush", _sync_flush)
    return log_forge
