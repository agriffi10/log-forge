"""Scripted and hostile test sinks for SPEC-072's verdict contract and delivery engine.

Not a test module (no ``test_`` prefix), in the manner of ``sink_rigs.py``: the engine, path and
fork tests share these so each criterion is driven by one shape of sink rather than a local copy.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Sequence
from typing import Any

from log_foundry.sinks._engine import emit_via_deliver
from log_foundry.sinks.base import SinkLosses, read_losses
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict

_UNSET: Any = object()


class Raw:
    """A script step returned from ``deliver`` exactly as held, for a malformed verdict."""

    def __init__(self, value: object) -> None:
        self.value = value


Step = Outcome | Sequence[Outcome] | Verdict | BaseException | Raw | Callable[[list[Any]], Any]


def events(n: int, tag: str = "e") -> list[dict[str, object]]:
    """``n`` distinct event dicts, each carrying its index, for identity assertions."""
    return [{"message": f"{tag}{i}", "i": i} for i in range(n)]


class Scripted:
    """A sink offering ``deliver``, answering each call with the next step of its script.

    A step is an ``Outcome`` (the same for every event of the round), a sequence of outcomes, a
    ``Verdict``, an exception instance (raised), a :class:`Raw` value (returned as is) or a
    callable given the round's batch. The last step repeats once the script runs out. Each call
    records a copy of the list it was handed, the list object itself, and the calling thread's
    name, so a test can assert identity, order and thread without a shared counter. ``emit`` is
    the inline adapter, as a shipped engine sink's would be; ``max_retries`` is set only when
    given, so "declares none" is the default.
    """

    log_foundry_stop_signal: threading.Event | None = None

    def __init__(
        self,
        script: Sequence[Step] = (Outcome.ACCEPTED,),
        *,
        max_retries: Any = _UNSET,
        seconds: float = 0.0,
    ) -> None:
        self.log_foundry_counters = SinkCounters()
        if max_retries is not _UNSET:
            self.max_retries = max_retries
        self.script = list(script)
        self.seconds = seconds
        self.rounds: list[list[dict[str, object]]] = []
        self.handed: list[list[dict[str, object]]] = []
        self.threads: list[str] = []
        self.closes = 0
        self._lock = threading.Lock()

    @property
    def calls(self) -> int:
        """How many times ``deliver`` was called."""
        with self._lock:
            return len(self.rounds)

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        """Records the round, waits ``seconds``, then answers with the script's next step."""
        with self._lock:
            index = len(self.rounds)
            self.rounds.append(list(batch))
            self.handed.append(batch)
            self.threads.append(threading.current_thread().name)
            step = self.script[min(index, len(self.script) - 1)]
        if self.seconds:
            threading.Event().wait(self.seconds)
        return _answer(step, batch)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Runs the engine inline, as a shipped engine sink's ``emit`` does."""
        emit_via_deliver(self, batch)

    def losses(self) -> SinkLosses:
        """The engine's counters."""
        return self.log_foundry_counters.losses()

    @property
    def failed(self) -> int:
        """``failed_events``, the shape a shipped sink's ``failed`` counter has."""
        return self.log_foundry_counters.losses().failed

    def close(self) -> None:
        """Counts the close."""
        with self._lock:
            self.closes += 1


def _answer(step: Step, batch: list[dict[str, object]]) -> Any:
    """What one script step makes ``deliver`` do for this round's batch."""
    if isinstance(step, Outcome):
        return Verdict.uniform(step, len(batch))
    if isinstance(step, Verdict):
        return step
    if isinstance(step, BaseException):
        raise step
    if isinstance(step, Raw):
        return step.value
    if callable(step):
        return step(batch)
    return Verdict(outcomes=tuple(step))


class WaitRecorder(threading.Event):
    """A stop event that records every ``wait`` timeout and returns at once unless told not to."""

    def __init__(self, *, real: bool = False) -> None:
        super().__init__()
        self.waits: list[float | None] = []
        self.real = real

    def wait(self, timeout: float | None = None) -> bool:
        """Records the timeout; waits for real only when built with ``real=True``."""
        self.waits.append(timeout)
        if self.real:
            return super().wait(timeout)
        return self.is_set()


class ThirdPartyWrapper:
    """A wrapper a third party would write: ``emit`` delegates, ``losses`` reads the inner."""

    def __init__(self, inner: Any) -> None:
        self.inner = inner

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Delegates to the inner sink's ``emit``."""
        self.inner.emit(batch)

    def losses(self) -> SinkLosses | None:
        """What the inner sink reports."""
        return read_losses(self.inner)

    def close(self) -> None:
        """Closes the inner sink."""
        self.inner.close()


class Proxy:
    """A proxy whose ``__getattr__`` delegates every missing attribute to an inner sink."""

    def __init__(self, inner: Any) -> None:
        object.__setattr__(self, "_inner", inner)
        object.__setattr__(self, "entered", [])

    def __getattr__(self, name: str) -> Any:
        self.entered.append(name)
        return getattr(self._inner, name)


class RaisingGetattr(Scripted):
    """An engine sink whose ``__getattr__`` raises, recording that it was entered."""

    entered: list[str]

    def __getattr__(self, name: str) -> Any:
        type(self).entered.append(name)
        raise RuntimeError(f"__getattr__-secret {name}")


RaisingGetattr.entered = []


class OptOut(Scripted):
    """A subclass of an engine sink that opts out with ``deliver = None`` and its own ``emit``."""

    deliver = None  # type: ignore[assignment]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.emitted: list[dict[str, object]] = []

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Keeps every event."""
        with self._lock:
            self.emitted.extend(batch)


class InheritsAdapter(Scripted):
    """A subclass that sets ``deliver = None`` and inherits the adapter ``emit``."""

    deliver = None  # type: ignore[assignment]


class Stamping(Scripted):
    """A subclass whose ``emit`` stamps a copy of every event and calls ``super().emit()``."""

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Stamps copies, then runs the inherited adapter over them."""
        super().emit([{**event, "stamped": True} for event in batch])


class MisspeltCounters:
    """An engine sink whose counters attribute is misspelt, so the adapter must refuse."""

    def __init__(self) -> None:
        self.log_foundry_counter = SinkCounters()
        self.calls = 0

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        """Accepts everything, counting the call."""
        self.calls += 1
        return Verdict.uniform(Outcome.ACCEPTED, len(batch))

    def emit(self, batch: list[dict[str, object]]) -> None:
        """The adapter."""
        emit_via_deliver(self, batch)

    def close(self) -> None:
        """Nothing to release."""


def metaclass_named(claimed: str, calls: list[str]) -> type:
    """A metaclass whose ``__name__`` property records its call and claims ``claimed``."""

    def name(cls: type) -> str:
        calls.append("__name__")
        return claimed

    return type("_ClaimingMeta", (type,), {"__name__": property(name)})
