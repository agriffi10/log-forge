"""Sinks and helpers the SPEC-088 ledger tests share. A helper module, not collected."""

import threading

from log_foundry import Outcome, SinkCounters, Verdict, emit_via_deliver


class GatedSink:
    """An ``emit``-only sink that records every event, and can stall or raise on a call.

    Attributes:
      events: Every event a returned ``emit`` received.
      calls: ``emit`` calls made.
      gate: Cleared to stall ``emit`` until it is set again.
      entered: Set the first time ``emit`` is entered.
      raise_on: A callable ``(call_number, batch) -> BaseException | None``.
    """

    def __init__(self) -> None:
        self.events: list[dict] = []
        self.calls = 0
        self.gate = threading.Event()
        self.gate.set()
        self.entered = threading.Event()
        self.raise_on = None
        self._lock = threading.Lock()

    def emit(self, batch: list[dict]) -> None:
        with self._lock:
            self.calls += 1
            call = self.calls
        self.entered.set()
        self.gate.wait()
        if self.raise_on is not None:
            exc = self.raise_on(call, batch)
            if exc is not None:
                raise exc
        with self._lock:
            self.events.extend(batch)

    def close(self) -> None:
        pass

    def messages(self) -> list[object]:
        with self._lock:
            return [event["message"] for event in self.events]


class PlanSink:
    """A ``deliver`` sink whose ``plan(batch)`` decides each event's fate, or raises.

    ``plan`` returns a list of ``Outcome`` for a readable verdict, the string ``"unreadable"``
    for a verdict the engine cannot read, or raises.

    Attributes:
      accepted: Events answered ``ACCEPTED``.
      calls: ``deliver`` calls made.
    """

    max_retries = 0

    def __init__(self, plan) -> None:
        self.log_foundry_counters = SinkCounters()
        self.plan = plan
        self.accepted: list[dict] = []
        self.calls = 0

    def deliver(self, batch: list[dict]) -> Verdict:
        self.calls += 1
        outcomes = self.plan(batch)
        if outcomes == "unreadable":
            return None  # type: ignore[return-value]
        self.accepted += [e for e, o in zip(batch, outcomes, strict=True) if o is Outcome.ACCEPTED]
        return Verdict(outcomes=tuple(outcomes))

    def emit(self, batch: list[dict]) -> None:
        emit_via_deliver(self, batch)

    def losses(self):
        return self.log_foundry_counters.losses()

    def close(self) -> None:
        pass


def every(outcome: Outcome):
    """A plan answering ``outcome`` for every event."""
    return lambda batch: [outcome] * len(batch)


def reading():
    """One ledger reading, with its residuals."""
    from log_foundry import _lifecycle

    return _lifecycle._ledger_reading()


def ledger():
    """The published ledger, read now."""
    from log_foundry import _lifecycle

    return _lifecycle._ledger_snapshot()


def queued_events(worker) -> int:
    """The events in a worker's queue, counted under its mutex."""
    with worker._queue.mutex:
        return sum(len(item) for item in worker._queue.queue if isinstance(item, list))
