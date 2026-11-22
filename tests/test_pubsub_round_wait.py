"""SPEC-094 FR-002 — ``GooglePubSubSink`` waits for this round's publish futures, on the engine.

Each round publishes every remaining event, then waits on the round's futures under one
deadline, ``overflow_timeout``, in slices, until the stop signal once the first slice has passed.
A result is ``ACCEPTED``; ``InvalidArgument`` or ``FlowControlLimitError`` is ``REJECTED``; any
other exception, no result in time, or a ``result`` that takes no ``timeout`` is ``UNKNOWN``,
counted once with a may-have-landed line and never resent, and ``close()`` waits, counting
nothing, on the ones left ``UNKNOWN``.
Serves invariants 2, 3, 7, 10, 11 and 13.
"""

from __future__ import annotations

import inspect
import subprocess
import sys
import textwrap
import threading
import time
import types
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry
from conftest import cpu_window, install_worker
from log_foundry.sinks import pubsub
from log_foundry.sinks.base import SinkLosses
from log_foundry.sinks.pubsub import GooglePubSubSink
from log_foundry.sinks.verdict import Outcome
from log_foundry.worker import Worker
from test_fork_lifecycle import run_in_child

if TYPE_CHECKING:
    import pathlib

_API_CORE = {
    "GoogleAPIError": (),
    "GoogleAPICallError": ("GoogleAPIError",),
    "ClientError": ("GoogleAPICallError",),
    "BadRequest": ("ClientError",),
    "InvalidArgument": ("BadRequest",),
    "Forbidden": ("ClientError",),
    "PermissionDenied": ("Forbidden",),
    "NotFound": ("ClientError",),
    "ServerError": ("GoogleAPICallError",),
    "GatewayTimeout": ("ServerError",),
    "DeadlineExceeded": ("GatewayTimeout",),
}
"""``google.api_core.exceptions``' classes these tests raise, with google-api-core's bases."""


@pytest.fixture
def api_core(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    """Installs a stub ``google.api_core.exceptions``, which ``driver_classes`` reads.

    Args:
      monkeypatch: The test's patch, which restores the module.

    Returns:
      The stub.

    Raises:
      None.
    """
    module = types.ModuleType("google.api_core.exceptions")
    for name, bases in _API_CORE.items():
        parents = tuple(getattr(module, b) for b in bases) or (Exception,)
        setattr(module, name, type(name, parents, {"__module__": module.__name__}))
    monkeypatch.setitem(sys.modules, module.__name__, module)
    return module


class _Client:
    """A publisher whose futures come from a factory, one per publish."""

    def __init__(self, factory: Any) -> None:
        """Keeps the factory.

        Args:
          factory: ``index -> future``.

        Returns:
          None.

        Raises:
          None.
        """
        self.factory = factory
        self.futures: list[Any] = []

    def publish(self, topic: str, data: bytes | None = None, **attrs: Any) -> Any:
        """Publishes one event.

        Args:
          topic: Ignored.
          data: Ignored.
          **attrs: Ignored.

        Returns:
          The future.

        Raises:
          None.
        """
        future = self.factory(len(self.futures))
        self.futures.append(future)
        return future


def _failed(error: BaseException) -> Future[str]:
    """A future already resolved with ``error``.

    Args:
      error: The exception.

    Returns:
      The future.

    Raises:
      None.
    """
    future: Future[str] = Future()
    future.set_exception(error)
    return future


def _sink(factory: Any, **kwargs: Any) -> GooglePubSubSink:
    """A sink over a client whose futures ``factory`` makes.

    Args:
      factory: ``index -> future``.
      **kwargs: More constructor arguments.

    Returns:
      The sink.

    Raises:
      None.
    """
    return GooglePubSubSink("projects/p/topics/t", client=_Client(factory), **kwargs)


def _batch(n: int) -> list[dict[str, object]]:
    """``n`` events.

    Args:
      n: How many.

    Returns:
      The events.

    Raises:
      None.
    """
    return [{"message": f"m{i}"} for i in range(n)]


def _lost(err: str) -> list[str]:
    """The ``lost`` lines of a stderr capture.

    Args:
      err: The capture.

    Returns:
      The lines.

    Raises:
      None.
    """
    return [line for line in err.splitlines() if "lost" in line]


def _through_the_worker(sink: GooglePubSubSink, n: int) -> tuple[bool, int]:
    """One batch through a real ``Worker`` with ``max_retries=3``.

    Args:
      sink: The sink.
      n: Events.

    Returns:
      The flush verdict and the worker's failed batches.

    Raises:
      None.
    """
    worker = Worker(sink, batch_size=1000, flush_interval=60.0, max_retries=3)
    install_worker(worker)
    worker.submit(_batch(n))
    ok = bool(worker.flush(timeout=10.0))
    return ok, log_foundry.health().failed_batches


def test_an_invalid_argument_fails_the_batch(api_core: types.ModuleType) -> None:
    """Every future refused with ``InvalidArgument``: ``flush()`` false and one failed batch."""
    client_futures = lambda i: _failed(api_core.InvalidArgument("bad"))  # noqa: E731
    ok, failed_batches = _through_the_worker(_sink(client_futures), 3)
    assert (ok, failed_batches) == (False, 1)


@pytest.mark.parametrize("name", ["PermissionDenied", "NotFound", "DeadlineExceeded"])
def test_a_refusal_that_may_follow_a_kept_attempt_settles_counted_once(
    name: str, api_core: types.ModuleType, capsys: pytest.CaptureFixture[str]
) -> None:
    """The publish retry can end in these after a ``DeadlineExceeded`` the server kept: ``UNKNOWN``."""
    error = getattr(api_core, name)
    sink = _sink(lambda i: _failed(error("nope")))
    ok, failed_batches = _through_the_worker(sink, 3)
    assert (ok, failed_batches) == (True, 0)
    assert sink.losses() == SinkLosses(dropped=0, failed=3)
    lines = _lost(capsys.readouterr().err)
    assert len(lines) == 1 and f"GooglePubSubSink publish unconfirmed ({name})" in lines[0], lines


def test_futures_that_never_resolve_are_waited_on_for_the_bound() -> None:
    """``overflow_timeout=0.5``: one ``emit`` takes 0.5 s to 1.5 s, its events ``UNKNOWN``."""
    sink = _sink(lambda i: Future(), overflow_timeout=0.5)
    began = time.monotonic()
    verdict = sink.deliver(_batch(3))
    took = time.monotonic() - began
    assert 0.5 <= took <= 1.5, took
    assert verdict.outcomes == (Outcome.UNKNOWN,) * 3


def test_a_stop_set_during_the_wait_ends_it() -> None:
    """The stop set 0.3 s in, ``overflow_timeout=5``: the round returns in 0.3 s to 1.3 s."""
    sink = _sink(lambda i: Future(), overflow_timeout=5)
    stop = threading.Event()
    sink.log_foundry_stop_signal = stop
    began = time.monotonic()
    timer = threading.Timer(0.3, stop.set)
    timer.start()
    verdict = sink.deliver(_batch(2))
    took = time.monotonic() - began
    timer.cancel()
    assert 0.3 <= took <= 1.3, took
    assert verdict.outcomes == (Outcome.UNKNOWN, Outcome.UNKNOWN)


class _Clock:
    """A clock the futures advance, so the first-slice test reads no wall time."""

    def __init__(self) -> None:
        """Starts at zero.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.now = 0.0

    def monotonic(self) -> float:
        """The fake time.

        Args:
          None.

        Returns:
          Seconds.

        Raises:
          None.
        """
        return self.now


class _Due:
    """A future that resolves at a fake time; ``result`` advances the clock up to its timeout."""

    def __init__(self, clock: _Clock, due: float) -> None:
        """Keeps the clock and when it resolves.

        Args:
          clock: The fake clock.
          due: The fake time it resolves at.

        Returns:
          None.

        Raises:
          None.
        """
        self.clock = clock
        self.due = due

    def result(self, timeout: float | None = None) -> str:
        """Waits, on the fake clock, until it resolves or its timeout passes.

        Args:
          timeout: Seconds.

        Returns:
          The message id.

        Raises:
          TimeoutError: When the timeout passed first.
        """
        if self.clock.now < self.due:
            self.clock.now = min(self.due, self.clock.now + (timeout or 0))
        if self.clock.now >= self.due:
            return "id"
        raise TimeoutError

    def done(self) -> bool:
        """Whether it has resolved.

        Args:
          None.

        Returns:
          The answer.

        Raises:
          None.
        """
        return self.clock.now >= self.due


def test_under_a_set_stop_the_first_slice_takes_every_future_resolving_inside_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """1,000 futures resolving 10 to 30 ms after the publish, stop set first: all ``ACCEPTED``.

    The first slice is a deadline at the wait's start plus ``_POLL_INTERVAL``; the clock is the
    futures' own, so a loaded runner cannot move the result. With the first slice's end set to
    the wait's start, the re-plant reads events ``UNKNOWN``.
    """
    clock = _Clock()
    monkeypatch.setattr(pubsub, "time", clock)
    sink = _sink(lambda i: _Due(clock, 0.010 + 0.00002 * i), overflow_timeout=5)
    stop = threading.Event()
    stop.set()
    sink.log_foundry_stop_signal = stop
    verdict = sink.deliver(_batch(1000))
    assert sum(o is Outcome.UNKNOWN for o in verdict.outcomes) == 0
    assert clock.now == pytest.approx(0.010 + 0.00002 * 999)


class _Spinning:
    """A future whose ``result`` raises ``TimeoutError`` at once, and which is never done."""

    def __init__(self) -> None:
        """Counts nothing yet.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.calls = 0

    def result(self, timeout: float | None = None) -> str:
        """Raises at once.

        Args:
          timeout: Ignored.

        Returns:
          Never.

        Raises:
          TimeoutError: Always.
        """
        self.calls += 1
        raise TimeoutError

    def done(self) -> bool:
        """Never.

        Args:
          None.

        Returns:
          False.

        Raises:
          None.
        """
        return False


@pytest.mark.parametrize("stop", [False, True], ids=["stop-unset", "stop-set"])
def test_a_result_that_returns_at_once_does_not_spin(stop: bool) -> None:
    """Over a 0.5 s wait the round uses under 0.1 s of CPU, stop or no stop."""
    spinning = _Spinning()
    sink = _sink(lambda i: spinning, overflow_timeout=0.5)
    if stop:
        event = threading.Event()
        event.set()
        sink.log_foundry_stop_signal = event
    with cpu_window() as window:
        sink.deliver(_batch(1))
    assert window.spent < 0.1, window.spent
    assert spinning.calls < 100, spinning.calls


def test_a_future_resolved_with_a_timeout_error_is_unknown_at_once(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Told from a pending future by ``done()``: ``UNKNOWN`` at once, its line naming it.

    The spec's 0.1 s is measured by the build's runner; a CI test may not hold a ceiling under
    the stall allowance, so this one asserts the round never waited on the 5 s deadline.
    """
    sink = _sink(lambda i: _failed(TimeoutError("resolved")), overflow_timeout=5)
    began = time.monotonic()
    sink.emit(_batch(1))
    assert time.monotonic() - began < 1.0
    [line] = _lost(capsys.readouterr().err)
    assert "GooglePubSubSink publish unconfirmed (TimeoutError); may have landed" in line


def test_the_signature_is_unchanged_and_max_pending_still_refuses_a_non_int() -> None:
    """Invariants 10 and 13: the same parameters; ``max_pending`` validated, though inert."""
    assert list(inspect.signature(GooglePubSubSink).parameters) == [
        "topic",
        "client",
        "max_pending",
        "overflow_timeout",
    ]
    with pytest.raises(TypeError, match="max_pending"):
        GooglePubSubSink("t", client=_Client(lambda i: Future()), max_pending=2.5)  # type: ignore[arg-type]


_EXIT_DRAIN_CHILD = textwrap.dedent(
    """
    import concurrent.futures, sys, threading, time
    import log_foundry
    from log_foundry.sinks.pubsub import GooglePubSubSink

    landed_path, report_path = sys.argv[1], sys.argv[2]

    class Client:
        def publish(self, topic, data=None, **attrs):
            future = concurrent.futures.Future()
            def send():
                time.sleep(0.2)
                with open(landed_path, "a") as fh:
                    fh.write("landed\\n")
                future.set_result("id")
            threading.Thread(target=send, daemon=True).start()
            return future

    class Watched(GooglePubSubSink):
        def close(self):
            before = self.losses().failed
            super().close()
            with open(report_path, "w") as fh:
                fh.write(f"{before} {self.losses().failed}\\n")

    sink = Watched("t", client=Client(), overflow_timeout=5.0)
    log_foundry.configure(service="s", version="0", env="test", sink=sink)

    @log_foundry.trace
    def work():
        for i in range(5):
            log_foundry.info("event", i=i)

    work()
    """
)


def test_a_normal_exit_lands_every_publish_the_exit_drain_left_unconfirmed(
    tmp_path: pathlib.Path,
) -> None:
    """Andrew's "Wait at close (Recommended)": seven events, a 0.2 s send, all seven land.

    Five events and the span's two inside one ``@trace`` reach the worker, which delivers them at
    the exit drain with the stop signal set: the round reads each ``UNKNOWN`` after its first
    slice, while the stand-in client is still sending them from daemon threads, as
    google-cloud-pubsub's commit thread is. ``close()`` then waits, so every send finishes before
    the interpreter ends its daemons, and it counts nothing: the seven the round counted are the
    seven after it. Flag-only, the process exits on the verdict and nothing lands.
    """
    landed, report = tmp_path / "landed.txt", tmp_path / "report.txt"
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", _EXIT_DRAIN_CHILD, str(landed), str(report)],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    before, after = report.read_text().split()
    assert before == "7", f"the premise: the exit drain's round read all seven UNKNOWN: {before}"
    assert after == before, f"close() counted {after} where its round had counted {before}"
    count = landed.read_text().count("landed\n") if landed.exists() else 0
    assert count == 7, f"{count} of 7 publishes landed before the process exited"


def test_a_close_over_futures_that_never_resolve_is_bounded_and_counts_nothing(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``close()`` waits ``overflow_timeout`` at most, counts nothing, writes nothing, waits once.

    A set stop signal ends the round after its first slice with all three ``UNKNOWN``. The close
    then waits on the three under one 1.0 s deadline, not cut short by the stop, and returns
    inside that plus the suite's 0.5 s stall allowance; a second ``close()`` has nothing left to
    wait on.
    """
    sink = _sink(lambda i: Future(), overflow_timeout=1.0)
    stop = threading.Event()
    stop.set()
    sink.log_foundry_stop_signal = stop
    assert sink.deliver(_batch(3)).outcomes == (Outcome.UNKNOWN,) * 3
    counted = sink.losses()
    capsys.readouterr()
    began = time.monotonic()
    sink.close()
    took = time.monotonic() - began
    assert 1.0 <= took <= 1.5, took
    assert sink.losses() == counted
    assert _lost(capsys.readouterr().err) == []
    again = time.monotonic()
    sink.close()
    assert time.monotonic() - again < 0.5


def _kept_after_a_stopped_round(sink: GooglePubSubSink, n: int) -> None:
    """Runs one round under a set stop signal, so its ``n`` pending futures read ``UNKNOWN``.

    Args:
      sink: The sink, over a client whose futures stay pending until a test resolves them.
      n: Events.

    Returns:
      None.

    Raises:
      None.
    """
    stop = threading.Event()
    stop.set()
    sink.log_foundry_stop_signal = stop
    assert sink.deliver(_batch(n)).outcomes == (Outcome.UNKNOWN,) * n


def test_a_forked_childs_own_close_waits_on_nothing_its_parent_kept() -> None:
    """Invariant 12: a caller's direct ``close()`` in a child returns at once; the parent keeps its two.

    No thread in the child resolves the parent's futures, so a child that waited on them would
    spend the whole 5 s ``overflow_timeout``.
    """
    sink = _sink(lambda i: Future(), overflow_timeout=5)
    _kept_after_a_stopped_round(sink, 2)

    def close_in_the_child() -> str:
        began = time.monotonic()
        sink.close()
        return f"{time.monotonic() - began:.3f}"

    child = run_in_child(close_in_the_child, timeout=15)
    assert child.finished, f"the child {child.ending}, having written {child.output!r}"
    assert float(child.output) < 1.0, f"the child's close took {child.output} s"
    assert len(sink._unconfirmed) == 2


def test_a_round_settling_after_the_close_keeps_nothing() -> None:
    """A close landing between a round's wait and its keep leaves no list for a later close."""
    sink = _sink(lambda i: Future(), overflow_timeout=5)
    keep = sink._keep_for_close

    def close_first(futures: list[Any]) -> None:
        sink.close()
        keep(futures)

    sink._keep_for_close = close_first  # type: ignore[method-assign]
    _kept_after_a_stopped_round(sink, 3)
    assert sink._unconfirmed == []


def test_each_keep_drops_the_futures_already_kept_that_have_resolved() -> None:
    """Two rounds of two; the first two resolve before the second keeps, so two are kept, not four."""
    client = _Client(lambda i: Future())
    sink = GooglePubSubSink("projects/p/topics/t", client=client, overflow_timeout=5)
    _kept_after_a_stopped_round(sink, 2)
    for future in client.futures:
        future.set_result("id")
    _kept_after_a_stopped_round(sink, 2)
    assert sink._unconfirmed == client.futures[2:]
