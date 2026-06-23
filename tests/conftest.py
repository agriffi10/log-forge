"""Shared fixtures for the log-forge test suite.

These tests are written *ahead* of the implementation (see docs/implementation-guide.md).
Every test guards on the feature it needs — `pytest.importorskip("log_forge.<module>")`
for whole modules, and an attribute check in the `lf` fixture for the public API — so the
suite stays green and simply *skips* anything not built yet. As you complete each phase,
the matching tests light up on their own.
"""

import pytest


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
def lf(fake_sink: FakeSink):
    """`log_forge` configured with a FakeSink.

    Works as-is while flushing is synchronous (guide Phases 1-7). Once the async worker
    lands (Phase 9), `fake_sink.events` won't be populated until the worker drains, so
    add a synchronous-flush test mode or call `log_forge.shutdown()` before asserting.
    """
    log_forge = pytest.importorskip("log_forge")
    for attr in ("configure", "trace", "info"):
        if not hasattr(log_forge, attr):
            pytest.skip(f"log_forge.{attr} not implemented yet")
    log_forge.configure(service="test", version="0.0.0", env="test", sink=fake_sink)
    return log_forge
