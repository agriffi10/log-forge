"""The private wrapper behind ``configure(sink_factory=…)``: one sink per process (SPEC-079)."""

from __future__ import annotations

import os
import threading
from typing import TYPE_CHECKING

from log_foundry import _diag, _lifecycle, _sink_arg
from log_foundry.sinks._wrapper import ForwardedStopSignal
from log_foundry.sinks.base import Sink, SinkDeliveryError, SinkLosses, flush_sink, read_losses

if TYPE_CHECKING:
    from collections.abc import Callable


class _FactorySink:
    """Builds this process's sink on its first emit, and delivers to it after (SPEC-079 FR-006).

    ``configure(sink_factory=f)`` installs one of these as the configured sink and never calls
    ``f``; the first ``emit`` in each process does, on whichever thread emits — the drain thread
    for span events, the caller's thread on the orphan path — so the caller's thread inside a
    span carries no sink construction (invariant 4), and a prefork master that never logs never
    builds one. Lazy in every process is Andrew's decision of 2026-11-04. A forked child is given
    a new one by ``_lifecycle._adopt_factory_after_fork``, so the child builds its own.

    **The build.** Under :attr:`_build_lock` the factory is called once, its result checked with
    ``isinstance(result, Sink)``, stamped for this process (``_lifecycle.stamp``), handed the stop
    signal this wrapper was offered, and stored. Concurrent first emits wait on the lock and the
    factory runs once between them. Later emits read :attr:`_inner` without the lock. The lock is
    a **build** lock, not a transport lock: it is held across the one factory call, which is the
    caller's code and may do I/O, and never across a delivery.

    **A failed build** — the factory raising ``Exception``, or returning something that is not a
    ``Sink`` — is announced here through ``_diag.absorbed``, naming the exception's type or the
    returned object's type and never a value (invariant 11), on the first failure and every
    ``WARN_EVERY``-th after it with the running total, because a factory that keeps failing fails
    once per emit. **A built sink that contains itself is a failed build too**: one leading back
    to this wrapper — the factory wrapped ``get_config().sink`` — or holding a wrapper that leads
    back to itself, found by ``_sink_arg.loop_in``, because storing it would make every emit
    recurse. It then raises ``SinkDeliveryError`` chained from the cause, which the path
    that made the call counts and announces as any sink's failure: the worker retries under its
    ``max_retries``, the orphan path counts ``orphan_lost``. The next emit calls the factory
    again. A non-sink result is dropped unclosed, since it has no ``close`` the library can
    trust, and so is a looped one, since the library cannot tell which of its sinks the caller
    still holds elsewhere. A ``BaseException`` from the factory propagates unchanged (SPEC-025).

    **An emit from inside the factory**, on the same thread, raises ``SinkDeliveryError`` at once
    rather than calling the factory again or waiting on the lock its own thread holds. The thread
    is keyed by process and thread ident, because a wrapper a forked child inherited mid-build
    carries the parent's mark, and a child thread reusing that ident would otherwise be refused
    forever.

    It takes **no** transport lock (SPEC-028 FR-002): it holds no transport of its own, and the
    built sink serialises its own delivery. It **adds no post-close guard** (SPEC-032 FR-003):
    after a close, an emit with a built sink is delegated to it and the built sink decides
    (invariant 7), and an emit with nothing built builds one, since nothing was released.

    It defines no ``deliver`` and no ``reacquire_after_fork``: the built sink's own engine runs
    inside its ``emit`` (SPEC-072, SPEC-075), and the built sink's own hook is reached by the fork
    walk through :attr:`_inner`.

    Attributes:
      log_foundry_stop_signal: The worker's shutdown event, stored where the engine reads it and
        forwarded to the built sink once there is one.
    """

    log_foundry_stop_signal = ForwardedStopSignal()

    def __init__(self, factory: Callable[[], Sink]) -> None:
        """Holds the factory; builds nothing.

        Args:
          factory: Called with no arguments, at most once per successful build, to build this
            process's sink.

        Returns:
          None.

        Raises:
          None.
        """
        self._factory = factory
        self._build_lock = threading.Lock()
        self._inner: Sink | None = None
        self._building: tuple[int, int] | None = None
        self._failures = 0
        vars(self)["log_foundry_stop_signal"] = None

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Delivers the batch to this process's sink, building it first if there is none yet.

        Args:
          batch: The events to deliver.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When the build fails, or is re-entered from inside the factory.
          Exception: Whatever the built sink's ``emit`` raises, unchanged.
        """
        inner = self._inner
        if inner is None:
            inner = self._build()
        inner.emit(batch)

    def _build(self) -> Sink:
        """Builds and stores this process's sink, once, under the build lock.

        Args:
          None.

        Returns:
          The built sink, or the one a concurrent first emit built while this one waited.

        Raises:
          SinkDeliveryError: When this thread is already inside the factory, or the factory
            raises ``Exception``, returns something that is not a ``Sink``, or returns a sink
            that leads back to this wrapper or to a wrapper of its own
            (``_sink_arg.loop_in``).
        """
        me = (os.getpid(), threading.get_ident())
        if self._building == me:
            raise SinkDeliveryError("sink_factory emitted from inside itself; the event is lost")
        with self._build_lock:
            built = self._inner
            if built is not None:
                return built
            self._building = me
            try:
                result = self._factory()
            except Exception as exc:
                self._announce(exc, "")
                raise SinkDeliveryError("sink_factory raised; the next emit retries") from exc
            finally:
                self._building = None
            if not isinstance(result, Sink):
                stored = _diag._STR_STR(_diag._STORED_NAME(type(result)))
                refusal = SinkDeliveryError("sink_factory did not return a sink")
                self._announce(refusal, f"it returned {stored}, not a sink")
                raise refusal
            looped = _sink_arg.loop_in(result, back_to=self)
            if looped is not None:
                stored = _diag._STR_STR(_diag._STORED_NAME(type(looped)))
                where = (
                    "leads back to the sink_factory= wrapper"
                    if looped is self
                    else f"holds a {stored} that leads back to itself"
                )
                refusal = SinkDeliveryError("sink_factory returned a sink that contains itself")
                self._announce(refusal, f"what it returned {where}")
                raise refusal
            _lifecycle.stamp(result)
            signal = vars(self).get("log_foundry_stop_signal")
            if signal is not None:
                _lifecycle.offer_stop_signal(result, signal)
            self._inner = result
            return result

    def _announce(self, exc: BaseException, what: str) -> None:
        """Counts one failed build; writes the line on the first and every ``WARN_EVERY``-th.

        Args:
          exc: The factory's exception, or the refusal of what it returned.
          what: What the factory returned, by type, when it returned a non-sink; else empty.

        Returns:
          None.

        Raises:
          None.
        """
        self._failures += 1
        if self._failures == 1 or self._failures % _diag.WARN_EVERY == 0:
            lead = f"{what}; " if what else ""
            _diag.absorbed(
                "building this process's sink with sink_factory",
                exc,
                f"{lead}{self._failures} build(s) failed so far; the next emit retries",
            )

    def flush(self) -> None:
        """Forwards the flush to the built sink, and does nothing when none is built.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the built sink's flush raises, so the failure reaches the caller as
            a ``FlushResult`` reason.
        """
        inner = self._inner
        if inner is not None:
            flush_sink(inner)

    def losses(self) -> SinkLosses | None:
        """Reports the built sink's losses, and zeros when none is built; counts nothing itself.

        Args:
          None.

        Returns:
          The built sink's losses (``None`` when it reports none), or zeros.

        Raises:
          None.
        """
        inner = self._inner
        return SinkLosses(dropped=0, failed=0) if inner is None else read_losses(inner)

    def close(self) -> None:
        """Releases the built sink as the shipped wrappers release theirs; nothing when none.

        Routed through ``_lifecycle.release`` with this wrapper as the owner, so a sink this
        process may not release is refused here as at the lifecycle's own closers (SPEC-042).
        After a close, an emit with a built sink is delegated to it and the built sink decides
        (invariant 7); an emit with nothing built builds one, since nothing was released
        (SPEC-032).

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the built sink's ``close`` raises.
        """
        inner = self._inner
        if inner is not None:
            _lifecycle.release(inner, owner=self)

    def _forward_stop_signal(self, signal: threading.Event | None) -> None:
        """Hands the worker's stop signal to the built sink, when there is one.

        Called by :class:`~log_foundry.sinks._wrapper.ForwardedStopSignal` after it stored the
        signal in this wrapper's ``__dict__``; a sink built later is handed it at its build.

        Args:
          signal: The worker's shutdown event, or ``None``.

        Returns:
          None.

        Raises:
          None.
        """
        inner = self._inner
        if inner is not None and signal is not None:
            _lifecycle.offer_stop_signal(inner, signal)
