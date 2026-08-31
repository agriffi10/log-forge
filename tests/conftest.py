"""Shared fixtures for the log-foundry test suite.

These tests are written *ahead* of the implementation (see docs/implementation-guide.md).
Every test guards on the feature it needs — `pytest.importorskip("log_foundry.<module>")`
for whole modules, and an attribute check in the `lf` fixture for the public API — so the
suite stays green and simply *skips* anything not built yet. As you complete each phase,
the matching tests light up on their own.
"""

import sys
import threading

import pytest


def run_concurrently(work, threads: int, *, per_thread: int = 1) -> list[BaseException]:
    """Run ``work`` on N threads that start together, and return whatever it raised.

    The shared concurrent-emitter helper (SPEC-028 FR-004). Threads rendezvous on a
    ``threading.Barrier`` so they enter ``work`` at the same instant — a barrier is a real
    synchronization primitive, unlike the ``sleep`` FR-004 forbids, so the overlap does not
    depend on how fast the machine is. The switch interval is tightened for the duration to
    widen the window in which CPython preempts a read-modify-write; without it a lost increment
    is real but rare enough that a race test would pass against the bug it exists to catch.

    Args:
      work: Called as ``work(thread_index, iteration)``. Exceptions are captured, not raised in
        the worker thread where they would be discarded.
      threads: How many threads to run.
      per_thread: How many times each thread calls ``work``.

    Returns:
      Every exception raised across all threads, in completion order. Empty means all calls
      returned normally. Assert on this rather than trusting the threads were silent.

    Raises:
      None.
    """
    barrier = threading.Barrier(threads)
    errors: list[BaseException] = []
    errors_lock = threading.Lock()

    def runner(index: int) -> None:
        barrier.wait()
        for iteration in range(per_thread):
            try:
                work(index, iteration)
            except BaseException as exc:
                with errors_lock:
                    errors.append(exc)

    pool = [threading.Thread(target=runner, args=(i,)) for i in range(threads)]
    original_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        for thread in pool:
            thread.start()
        for thread in pool:
            thread.join(timeout=30)
    finally:
        sys.setswitchinterval(original_interval)
    assert not any(thread.is_alive() for thread in pool), "a worker thread did not finish"
    return errors


@pytest.fixture(autouse=True)
def _reset_config():
    """Restore a fresh global ``Config`` before each test.

    ``log_foundry.config._config`` is a process-wide singleton mutated by ``configure()``;
    without this, tests would leak identity/sink/defaults into one another and only pass by
    ordering. No-ops until the module exists.

    It also clears the ``contextvars`` state, which is a *different* singleton and was leaking
    the same way. Baggage set with no span open is a process-level default and nothing releases
    it (SPEC-024, arch §5.1) — so one test's ``set_baggage`` reached later tests' events, and
    two `test_trace_continuation` cases failed only when run after a new file that set baggage,
    passing in isolation. That is SPEC-024's own finding, reproduced between two tests.
    """
    try:
        from log_foundry import config, context
    except ImportError:
        yield
        return
    config._config = config.Config()
    context.reset_context()
    _reset_worker()
    yield
    # Tear down any process worker a test created so its daemon thread / sink don't leak.
    _reset_worker()
    context.reset_context()


def _reset_worker() -> None:
    """Drain and clear the lazily-created process worker (SPEC-004), if present.

    Without this the first test's sink/thread would leak into later tests through the
    module-global ``decorator._worker``. No-ops until the worker module exists.

    SPEC-031 FR-006's three flags are cleared alongside it, for the same reason and one more:
    ``_orphan_retired`` is what ``health()`` synthesizes when there is no worker, so a test
    that calls ``shutdown()`` would otherwise make every later test read ``retired=True``, and
    ``_orphan_sink`` would make "nothing was ever logged, so nothing is closed"
    untestable in-process. ``_atexit_registered`` is deliberately **not** cleared — the
    handler really is registered for the life of the interpreter, and re-arming the flag would
    have the next worker register a second one.

    SPEC-033 adds three more. ``_orphan_stop`` is replaced rather than cleared, since an
    ``Event`` cannot be un-set and a test that called ``shutdown()`` would otherwise leave every
    later test's sink backing off not at all. ``_lifecycle._closers`` is now process-global, so a
    hung closer from one test — the capped-grace tests create them deliberately — would
    otherwise leak a non-zero ``closing_sinks`` into the next.

    SPEC-036 FR-003 adds the two loss counters. They are cumulative for the life of the process
    and ``health()`` synthesizes them whether or not a worker exists, so a single test that loses
    an event would otherwise leave every later test reading a non-zero ``orphan_lost`` — the exact
    failure this fixture's first paragraph describes, on a field whose whole purpose is to be
    believed.
    """
    try:
        from log_foundry import decorator
    except ImportError:
        return
    worker = getattr(decorator, "_worker", None)
    if worker is not None:
        worker.shutdown()
        decorator._worker = None
    for flag in ("_orphan_sink", "_orphan_closed_sink"):
        if hasattr(decorator, flag):
            setattr(decorator, flag, None)
    if hasattr(decorator, "_orphan_retired"):
        decorator._orphan_retired = False
    if hasattr(decorator, "_orphan_stop"):
        decorator._orphan_stop = threading.Event()
    for counter in ("_orphan_lost", "_in_span_lost"):
        if hasattr(decorator, counter):
            setattr(decorator, counter, 0)
    try:
        from log_foundry import _lifecycle
    except ImportError:
        return
    with _lifecycle._closers_lock:
        _lifecycle._closers.clear()


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
    """`log_foundry` configured with a FakeSink, flushing synchronously.

    SPEC-004 rewired the decorator to hand finished spans to a background worker, so
    `fake_sink.events` would no longer be populated by the time a pipeline test asserts.
    These tests care about span/event/context semantics, not delivery timing, so we keep
    flushing inline here (the synchronous-flush test mode this fixture always anticipated);
    the worker's own batching/retry/backpressure/shutdown behavior is covered directly in
    `test_worker.py`.
    """
    log_foundry = pytest.importorskip("log_foundry")
    for attr in ("configure", "trace", "info"):
        if not hasattr(log_foundry, attr):
            pytest.skip(f"log_foundry.{attr} not implemented yet")
    log_foundry.configure(service="test", version="0.0.0", env="test", sink=fake_sink)

    decorator = pytest.importorskip("log_foundry.decorator")
    from log_foundry.config import _ensure_sink

    def _sync_flush(span) -> None:
        _ensure_sink().emit(span.events)

    monkeypatch.setattr(decorator, "_flush", _sync_flush)
    return log_foundry


@pytest.fixture
def sentry_http_fallback(monkeypatch):
    """Forces `SentrySink` onto its HTTP-envelope backend regardless of what is installed.

    `SentrySink.__init__` calls `_import_sdk()` whenever no `client=` is given, so which backend
    a test gets is decided by whether the optional `sentry` extra happens to be present. Every
    test that passes `opener=` wants the fallback -- and `opener` is silently ignored on the SDK
    branch, so with the extra installed those tests assert against a sink they did not build.

    Four of them were green in CI and red under `--all-extras`, found by SPEC-041 FR-001's job
    running the existing suite in the extras environment for the first time: `sink._http is None`,
    `DID NOT RAISE SinkDeliveryError`, `assert 2 == 1`, `(5, 0) == (4, 1)`. They were passing only
    because CI never installs that extra.

    A plain import rather than `importorskip`: this module needs no extra, so the guard could
    never fire -- and if it somehow did it would silently delete the four tests in exactly the
    environment they exist for, which is the failure this fixture was written to end.
    """
    from log_foundry.sinks import sentry

    monkeypatch.setattr(sentry, "_import_sdk", lambda: None)
    return sentry
