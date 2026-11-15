"""SPEC-082 — the binary handle doubles and round recorders the file-sink tests share.

A file sink writes through ``sink._stream``, an unbuffered ``io.FileIO``, and decides every
outcome from the counts its write calls return. The doubles here wrap the sink's real handle, so
what a test reads back is what reached the file, and they fail the way a kernel does: a call
takes some of what it was handed and returns that count, and a later call raises.

Nothing here computes an expectation. A test states the bytes or the outcome tuple it expects as
a literal and uses these only to stage the failure and to read back what happened.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from log_foundry.sinks import _engine
from log_foundry.sinks.verdict import Outcome
from verdict_sinks import WaitRecorder

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

A, R, J, U, D = (
    Outcome.ACCEPTED,
    Outcome.RETRYABLE,
    Outcome.REJECTED,
    Outcome.UNKNOWN,
    Outcome.DROPPED,
)

EVENTS: list[dict[str, object]] = [
    {"n": 1, "pad": "aaaa"},
    {"n": 2, "pad": "bbbb"},
    {"n": 3, "pad": "cccc"},
    {"n": 4, "pad": "dddd"},
    {"n": 5, "pad": "eeee"},
]
"""Five events whose NDJSON lines are each 24 bytes, written out in :data:`LINES`."""

LINES: list[bytes] = [
    b'{"n": 1, "pad": "aaaa"}\n',
    b'{"n": 2, "pad": "bbbb"}\n',
    b'{"n": 3, "pad": "cccc"}\n',
    b'{"n": 4, "pad": "dddd"}\n',
    b'{"n": 5, "pad": "eeee"}\n',
]
"""The bytes 1.x wrote for :data:`EVENTS` in UTF-8, as literals, 24 bytes a line."""

LINE = 24
"""The length of each of :data:`LINES`, stated rather than measured."""


class Budgeted:
    """Wraps a real handle: its writes take ``budget`` bytes in all, then every call raises.

    ``budget=None`` never fails. ``once=True`` lifts the fault after the first raise, as a
    cleared disk or a raised limit would. ``calls`` holds what each write call was handed and
    ``took`` what each returned, a raise recorded as ``None``.
    """

    def __init__(
        self,
        real: Any,
        budget: int | None,
        error: Callable[[], BaseException] | None = None,
        *,
        once: bool = False,
    ) -> None:
        self.real = real
        self.budget = budget
        self.error = error or (lambda: OSError(28, "No space left on device"))
        self.once = once
        self.calls: list[bytes] = []
        self.took: list[int | None] = []
        self.raised = 0

    def write(self, data: bytes) -> int:
        self.calls.append(bytes(data))
        if self.budget is None:
            count = int(self.real.write(data))
            self.took.append(count)
            return count
        if self.budget <= 0:
            self.took.append(None)
            self.raised += 1
            if self.once:
                self.budget = None
            raise self.error()
        part = bytes(data)[: self.budget]
        self.budget -= len(part)
        count = int(self.real.write(part))
        self.took.append(count)
        return count

    def lift(self) -> None:
        """Removes the fault, so every later write goes through whole."""
        self.budget = None

    def refill(self, budget: int) -> None:
        """Arms the fault again with a new byte budget."""
        self.budget = budget

    def close(self) -> None:
        self.real.close()

    @property
    def closed(self) -> bool:
        return bool(self.real.closed)

    def fileno(self) -> int:
        return int(self.real.fileno())

    def seekable(self) -> bool:
        return bool(self.real.seekable())

    def tell(self) -> int:
        return int(self.real.tell())


def budget(sink: Any, taken: int | None, error: Any = None, *, once: bool = False) -> Budgeted:
    """Installs a :class:`Budgeted` handle on ``sink`` over its real one, and returns it."""
    handle = Budgeted(sink._stream, taken, error, once=once)
    sink._stream = handle
    return handle


class Rounds:
    """Records each round's outcome tuple on one sink, and can act between rounds.

    It wraps the instance's ``_round``, which ``deliver`` reads through ``self``, so the class's
    own ``deliver`` and ``emit`` still run and the engine's probe still offers them. ``after`` is
    called with the 1-based number of the round that just returned, outside the sink's lock. A
    round the sink runs *while* ``after`` is running, another section a test stages between two
    rounds, passes through unrecorded, so the record is the one run's rounds.
    """

    def __init__(self, sink: Any, after: Callable[[int], None] | None = None) -> None:
        self.outcomes: list[tuple[Outcome, ...]] = []
        self.batches: list[list[dict[str, object]]] = []
        self._between = False
        real = sink._round

        def recorded(batch: list[dict[str, object]]) -> Any:
            if self._between:
                return real(batch)
            verdict = real(batch)
            self.batches.append(list(batch))
            self.outcomes.append(verdict.outcomes)
            if after is not None:
                self._between = True
                try:
                    after(len(self.outcomes))
                finally:
                    self._between = False
            return verdict

        sink._round = recorded


def run(sink: Any, batch: list[dict[str, object]], *, retries: int = 0) -> _engine.Delivery:
    """Runs one engine run of ``retries + 1`` rounds as the worker's would, without waiting."""
    stop = WaitRecorder()
    stop.set()
    delivery = _engine.run_delivery(sink, batch, worker_retries=retries, stop=stop)
    assert delivery is not None, "the premise: the sink offers deliver"
    return delivery


def lines_of(path: Path | str) -> list[bytes]:
    """Every newline-terminated line of the file, without its terminator, and any cut remainder."""
    with open(path, "rb") as stream:
        raw = stream.read()
    return [line for line in raw.split(b"\n") if line]


def ids_of(path: Path | str) -> list[int]:
    """The ``n`` of every line in the file, each line parsed as JSON; raises if one does not."""
    return [json.loads(line)["n"] for line in lines_of(path)]
