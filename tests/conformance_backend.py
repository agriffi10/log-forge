"""SPEC-092's fake backend, its fault scripts, the row driver and the one assertion every row meets.

A helper, not collected. It imports `log_foundry`, the standard library and `pytest` for its
`MonkeyPatch`; from `tests/`, only `conftest` for `_reset_worker` and, inside `drive`, the
corpus's own `conformance_doubles`, which imports `conformance_classes`. So a probe can put the
corpus's modules in front of an older tree's `src` and `tests` and run the same rows there
(SPEC-092 FR-005).

**A row** is one variant under one fault. `run_row` drives it through the public path the
worker serves, reads what the library reported for each event, and `judge` compares that with
what the row's `Backend` durably holds. The reading of the library's report is the engine's own:
every engine run is captured at `_engine._run` by wrapping the `deliver` callable it is handed,
and each event's fate is its outcome in the last round of that run that carried it, read by one
function, `fate_of`. A sink with no `deliver` is read per batch from `Worker._emit`.
"""

from __future__ import annotations

import collections
import contextlib
import dataclasses
import gzip
import inspect
import io
import json
import re
import socket
from collections.abc import Callable, Iterator, Mapping
from typing import Any, Literal

import pytest

N = 8
"""Events per batch: six level calls inside one traced call, with `span.start` and `span.end`.
Halves and quarters are whole, so `partial` splits a call 4, 2 and 2."""

TOKEN = re.compile(r"c92e\d{4}")
"""Every event's `log_id` in a row; the backend counts what it holds by these, never by position."""

Action = Literal["ok", "refuse", "partial", "taken-raise", "reply-lost", "noshape", "nack"]
Fate = Literal["landed", "lost", "ambiguous", "skipped"]
Check = Literal[
    "partition",
    "silent-loss",
    "duplicate",
    "lost-but-held",
    "unannounced",
    "ambiguous-when-knowable",
    "ledger",
]

UNKNOWABLE: frozenset[str] = frozenset(
    {"taken-then-raised", "reply-lost", "peer-closed", "no-verdict-shape"}
)
"""Faults after whose faulted call the sink cannot tell what its backend kept, unless the double
says otherwise (a file's `write` returns what it took; a rolled-back transaction kept nothing)."""


class HarnessError(Exception):
    """A row that could not be judged.

    It built the wrong class, lost track of its events, or reached the network. It is never folded
    into a verdict, so a broken harness cannot read green.
    """


def tokens(payload: object) -> list[str]:
    """The `log_id`s a payload carries, each once per payload, in first-seen order.

    Args:
      payload: Bytes (gzip-compressed or not), text, or anything `json.dumps` can render.

    Returns:
      The tokens.

    Raises:
      None.
    """
    if isinstance(payload, bytes | bytearray | memoryview):
        raw = bytes(payload)
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        text = raw.decode("utf-8", "replace")
    elif isinstance(payload, str):
        text = payload
    else:
        text = json.dumps(payload, default=repr)
    return list(dict.fromkeys(TOKEN.findall(text)))


@dataclasses.dataclass
class Call:
    """One client call the backend saw.

    Attributes:
      carried: The tokens the call carried.
      knowable: Whether the sink could know what the backend kept of them.
      action: The action `Backend.decide` answered for it.
      applied: Whether the double carried the action out; an `ok` call needs nothing applied.
    """

    carried: list[str]
    knowable: bool
    action: str = "ok"
    applied: bool = True


class Backend:
    """The stateful fake every double of a row writes through: what it durably holds, by `log_id`.

    `decide` is the fault script. It is asked once per client call with the call's tokens and
    answers one action; the double then takes the tokens the action says and answers in its
    client's own shape. What "taken" means is the backend's alone, and a double never decides it.
    A fault acts on the first call of its batch unless it says otherwise. A double that carries
    out an action other than `ok` says so through `apply`, in the branch that does it, and a row
    whose double left one unapplied is a harness error, not a verdict: a double with no branch
    for an action would otherwise answer as if the call were clean and the row pass vacuously.

    Attributes:
      fault: The row's fault script.
      held: Copies durably held, by `log_id`.
      calls: Every client call, in order.
      peer_closed: Set when `peer-closed`'s second batch begins.
      on_close: Hooks a double defers to after `shutdown()`: a client buffer's late report, a
        database read back, a temporary directory removed.
    """

    def __init__(self, fault: str) -> None:
        """Starts with nothing held and no call seen.

        Args:
          fault: The row's fault script.

        Returns:
          None.

        Raises:
          None.
        """
        self.fault = fault
        self.held: collections.Counter[str] = collections.Counter()
        self.calls: list[Call] = []
        self.batch = 0
        self.in_batch = 0
        self.faulted_batch = 1 if fault == "peer-closed" else 0
        self.peer_closed = False
        self.on_close: list[Callable[[], None]] = []

    def begin_batch(self, number: int) -> None:
        """Numbers the calls of a new batch from zero.

        Args:
          number: The batch, from zero.

        Returns:
          None.

        Raises:
          None.
        """
        self.batch = number
        self.in_batch = 0
        if self.fault == "peer-closed" and number == self.faulted_batch:
            self.peer_closed = True

    def after_close(self) -> None:
        """Runs whatever a double deferred to `close()`.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        for hook in self.on_close:
            hook()

    def decide(self, carried: list[str], *, knowable: bool | None = None) -> Action:
        """Records one call and answers its action.

        Args:
          carried: The tokens the call carries.
          knowable: Overrides whether the call's outcome is knowable; by default a call is
            unknowable when its action leaves the backend's keeping unsaid.

        Returns:
          `ok`, `refuse`, `partial`, `taken-raise`, `reply-lost`, `noshape` or `nack`.

        Raises:
          None.
        """
        first = self.batch == self.faulted_batch and self.in_batch == 0
        self.in_batch += 1
        action: Action = "ok"
        if self.fault == "refused-always":
            action = "refuse"
        elif first:
            action = _FIRST_CALL.get(self.fault, "ok")
        if self.fault == "never-acknowledged":
            action = "nack"
        if knowable is None:
            knowable = action not in ("taken-raise", "reply-lost", "noshape")
        self.calls.append(Call(list(carried), knowable, action, applied=action == "ok"))
        return action

    def apply(self) -> None:
        """Marks the last call's action as carried out by its double.

        Args:
          None.

        Returns:
          None.

        Raises:
          AssertionError: When no call has been decided.
        """
        assert self.calls, "apply() before any decide()"
        self.calls[-1].applied = True

    def unapplied(self) -> list[str]:
        """Each call whose action other than `ok` its double never applied.

        Args:
          None.

        Returns:
          One line per such call, naming its position and its action.

        Raises:
          None.
        """
        return [
            f"call {index} was decided {call.action!r} and never applied"
            for index, call in enumerate(self.calls)
            if not call.applied
        ]

    def take(self, carried: list[str]) -> None:
        """Holds one more copy of each token.

        Args:
          carried: The tokens taken.

        Returns:
          None.

        Raises:
          None.
        """
        for token in carried:
            self.held[token] += 1

    def partial_split(self, carried: list[str]) -> tuple[list[str], list[str], list[str]]:
        """`partial`'s split of one call: landed, refused once (retryable), refused for good.

        Args:
          carried: The call's tokens.

        Returns:
          The first half, the next quarter, and the last quarter.

        Raises:
          None.
        """
        half = len(carried) // 2
        quarter = max(len(carried) // 4, 1)
        return carried[:half], carried[half : half + quarter], carried[half + quarter :]

    def permanent(self) -> set[str]:
        """Tokens `partial` refuses on every call, retryable code or not.

        Args:
          None.

        Returns:
          The last quarter of the faulted batch's first call, or nothing under another fault.

        Raises:
          None.
        """
        if self.fault != "partial" or not self.calls:
            return set()
        return set(self.partial_split(self.calls[0].carried)[2])

    def unknowable(self) -> set[str]:
        """Tokens carried by at least one call whose outcome the sink could not know.

        Args:
          None.

        Returns:
          The tokens.

        Raises:
          None.
        """
        return {t for call in self.calls if not call.knowable for t in call.carried}


_FIRST_CALL: Mapping[str, Action] = {
    "refused-once": "refuse",
    "partial": "partial",
    "taken-then-raised": "taken-raise",
    "reply-lost": "reply-lost",
    "no-verdict-shape": "noshape",
}


@dataclasses.dataclass(frozen=True, kw_only=True)
class Report:
    """What the sink and its catcher reported for a row, per event where the path allows.

    Attributes:
      fates: Each event's fate, or `None` for a row read per batch (a sink with no `deliver`).
      landed: Events reported landed.
      lost: Events reported lost or skipped.
      ambiguous: Events reported ambiguous.
      late: Counts that moved after a batch's verdict was in: at a later batch or at `close()`.
      batch_losses: Per batch, the events it lost or left ambiguous and the loss lines written
        inside its own `Worker._emit`.
      late_lines: Loss lines written after the last batch's `_emit` returned.
      disagreement: Set when a run's fates disagree with that run's `Delivery`.
      ledger_moves: The row's moves of `delivered`, `skipped`, `sink_lost` and `batch_lost`, or
        `None` with the ledger off.
      ledger_expected: The same four terms as the worker's own path and the fates set them.
      unaccounted: `health().events.unaccounted` at the row's end, or `None` with the ledger off.
    """

    fates: Mapping[str, Fate] | None
    landed: int
    lost: int
    ambiguous: int
    late: int
    batch_losses: tuple[tuple[int, int], ...]
    late_lines: int
    disagreement: str | None = None
    ledger_moves: Mapping[str, int] | None = None
    ledger_expected: Mapping[str, int] | None = None
    unaccounted: int | None = None


LEDGER_TERMS = ("delivered", "skipped", "sink_lost", "batch_lost")
"""The ledger terms a row's events can end in (SPEC-088)."""

Predicate = Callable[[list[str], Backend, Report], bool]


def _counts(events: list[str], backend: Backend) -> tuple[int, int, int]:
    """How many events the backend holds once, more than once, and not at all.

    Args:
      events: The row's events.
      backend: The row's backend.

    Returns:
      `(once, many, none)`.

    Raises:
      None.
    """
    once = sum(1 for t in events if backend.held[t] == 1)
    many = sum(1 for t in events if backend.held[t] > 1)
    return once, many, len(events) - once - many


def _partition(events: list[str], backend: Backend, report: Report) -> bool:
    """The fates do not cover the events once, disagree with a `Delivery`, or a count is late.

    Per batch: what was reported landed, lost and late does not add up to the events, or a count is
    late.

    Args:
      events: The row's events.
      backend: What the row's backend holds and saw.
      report: What the library reported.

    Returns:
      True when the check fails.

    Raises:
      None.
    """
    if report.fates is None:
        return report.landed + report.lost + report.late != len(events) or bool(report.late)
    return (
        sorted(report.fates) != sorted(events)
        or bool(report.late)
        or report.disagreement is not None
    )


def _silent_loss(events: list[str], backend: Backend, report: Report) -> bool:
    """An event reported landed is not held; per batch, more reported landed than held.

    Args:
      events: The row's events.
      backend: What the row's backend holds and saw.
      report: What the library reported.

    Returns:
      True when the check fails.

    Raises:
      None.
    """
    if report.fates is None:
        once, many, _ = _counts(events, backend)
        return report.landed > once + many
    return any(report.fates.get(t) == "landed" and backend.held[t] == 0 for t in events)


def _duplicate(events: list[str], backend: Backend, report: Report) -> bool:
    """The backend holds an event more than once.

    Args:
      events: The row's events.
      backend: What the row's backend holds and saw.
      report: What the library reported.

    Returns:
      True when the check fails.

    Raises:
      None.
    """
    return any(backend.held[t] > 1 for t in events)


def _lost_but_held(events: list[str], backend: Backend, report: Report) -> bool:
    """An event reported lost or skipped is held; per batch, more counted than not held.

    Args:
      events: The row's events.
      backend: What the row's backend holds and saw.
      report: What the library reported.

    Returns:
      True when the check fails.

    Raises:
      None.
    """
    if report.fates is None:
        _, _, none = _counts(events, backend)
        return report.lost + report.late > none
    return any(report.fates.get(t) in ("lost", "skipped") and backend.held[t] > 0 for t in events)


def _unannounced(events: list[str], backend: Backend, report: Report) -> bool:
    """A batch's loss or ambiguous event, or a late count, has no loss line of its own.

    A batch that lost or left ambiguous an event must write a loss line inside its own
    `Worker._emit`, and a late count one after the last batch.

    Args:
      events: The row's events.
      backend: What the row's backend holds and saw.
      report: What the library reported.

    Returns:
      True when the check fails.

    Raises:
      None.
    """
    if any(lost and not lines for lost, lines in report.batch_losses):
        return True
    return bool(report.late) and not report.late_lines


def _ambiguous_when_knowable(events: list[str], backend: Backend, report: Report) -> bool:
    """An event reported ambiguous was carried only by knowable calls.

    A row read per batch reports no ambiguous event, so it never fails this.

    Args:
      events: The row's events.
      backend: What the row's backend holds and saw.
      report: What the library reported.

    Returns:
      True when the check fails.

    Raises:
      None.
    """
    if report.fates is None:
        return False
    unknowable = backend.unknowable()
    return any(report.fates.get(t) == "ambiguous" and t not in unknowable for t in events)


def _ledger(events: list[str], backend: Backend, report: Report) -> bool:
    """`unaccounted` is not zero, or the row's ledger moves differ from what its fates set.

    A row read with the ledger off never fails this.

    Args:
      events: The row's events.
      backend: What the row's backend holds and saw.
      report: What the library reported.

    Returns:
      True when the check fails.

    Raises:
      None.
    """
    if report.unaccounted is None:
        return False
    return report.unaccounted != 0 or report.ledger_moves != report.ledger_expected


CHECKS: tuple[tuple[Check, Predicate], ...] = (
    ("partition", _partition),
    ("silent-loss", _silent_loss),
    ("duplicate", _duplicate),
    ("lost-but-held", _lost_but_held),
    ("unannounced", _unannounced),
    ("ambiguous-when-knowable", _ambiguous_when_knowable),
    ("ledger", _ledger),
)
"""The assertion's checks, in the order a verdict lists them, each with the condition that fails
it. `judge` reads this tuple and nothing else, so removing or changing a check for a mutation run
is a patch of this one name."""


def judge(events: list[str], backend: Backend, report: Report) -> tuple[Check, ...]:
    """Every check the row fails, in `CHECKS` order; an empty tuple is a pass.

    Args:
      events: The row's events, by `log_id`.
      backend: What the row's fake backend holds and every call it saw.
      report: What the library reported.

    Returns:
      The failed checks.

    Raises:
      None.
    """
    return tuple(name for name, failed in CHECKS if failed(events, backend, report))


def _fates() -> dict[object, Fate]:
    """`FATES`'s initial table, read from the installed library's `Outcome`.

    Args:
      None.

    Returns:
      Each outcome's fate.

    Raises:
      None.
    """
    from log_foundry.sinks.verdict import Outcome

    return {
        Outcome.ACCEPTED: "landed",
        Outcome.RETRYABLE: "lost",
        Outcome.REJECTED: "lost",
        Outcome.DROPPED: "lost",
        Outcome.UNKNOWN: "ambiguous",
        Outcome.SKIPPED: "skipped",
    }


FATES: dict[object, Fate] = _fates()
"""Each engine outcome's fate. `fate_of` reads it, so a mutation run patches this table."""


def fate_of(outcome: object) -> Fate:
    """The one reading of a round's outcome as a fate; anything unreadable is ambiguous.

    Args:
      outcome: One outcome of a round's verdict.

    Returns:
      Its fate.

    Raises:
      None.
    """
    return FATES.get(outcome, "ambiguous")


@dataclasses.dataclass(frozen=True)
class Round:
    """One `deliver` call an engine run made.

    Attributes:
      sink: The sink the run delivered to.
      carried: The `log_id`s the round carried, in order.
      result: Its verdict, or the exception it raised.
      run_id: The run that made it.
    """

    sink: object
    carried: list[str]
    result: object
    run_id: int


@dataclasses.dataclass(frozen=True)
class Run:
    """One engine run, with the `Delivery` it committed.

    Attributes:
      sink: The sink the run delivered to.
      delivery: Its `Delivery`.
      run_id: Its number in the row.
    """

    sink: object
    delivery: Any
    run_id: int


class Recorder:
    """Every engine run and every round of a row, captured at `_engine._run`.

    `_engine._run` is the one function the worker's `run_delivery` and the inline adapter
    `emit_via_deliver` both call, so a run is seen whichever path made it. Nothing the engine
    decides is reimplemented: each round is recorded by wrapping the `deliver` callable the run
    was handed.

    Attributes:
      runs: The runs, in the order they committed.
      rounds: The rounds, in the order they returned.
    """

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Wraps `_engine._run` for the life of the patch.

        Args:
          monkeypatch: The row's patch.

        Returns:
          None.

        Raises:
          AttributeError: When `_engine._run` is gone.
        """
        from log_foundry.sinks import _engine

        self.runs: list[Run] = []
        self.rounds: list[Round] = []
        ids = iter(range(10**9))
        original = _engine._run

        def wrapper(sink: object, deliver: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
            """Runs one engine run with its `deliver` recorded, and records the run.

            Args:
              sink: The run's sink.
              deliver: The callable the run was handed.
              *args: `_run`'s other positional arguments.
              **kwargs: Its keyword arguments.

            Returns:
              The run's `Delivery`.

            Raises:
              BaseException: Whatever the run raises.
            """
            run_id = next(ids)

            def recorded(batch: list[dict[str, object]]) -> Any:
                """Calls `deliver` and records the round, its verdict or its raise.

                Args:
                  batch: The round's events.

                Returns:
                  The round's verdict.

                Raises:
                  BaseException: Whatever `deliver` raises, after recording it.
                """
                carried = [str(e.get("log_id")) for e in batch]
                try:
                    result = deliver(batch)
                except BaseException as err:
                    self.rounds.append(Round(sink, carried, err, run_id))
                    raise
                self.rounds.append(Round(sink, carried, result, run_id))
                return result

            delivery = original(sink, recorded, *args, **kwargs)
            self.runs.append(Run(sink, delivery, run_id))
            return delivery

        monkeypatch.setattr(_engine, "_run", wrapper)


def fates_from(rounds: list[Round], owner: object, run_id: int) -> dict[str, Fate]:
    """Each event's fate in one run: its outcome in that run's last round that carried it.

    A round that raised loses what it carried; a verdict that is not one, or whose outcomes do
    not match what the round carried, leaves it ambiguous.

    Args:
      rounds: The row's rounds.
      owner: The run's sink.
      run_id: The run.

    Returns:
      The fates, by `log_id`.

    Raises:
      None.
    """
    from log_foundry.sinks.verdict import Verdict

    fates: dict[str, Fate] = {}
    for one in rounds:
        if one.sink is not owner or one.run_id != run_id:
            continue
        if isinstance(one.result, BaseException):
            fates.update(dict.fromkeys(one.carried, "lost"))
            continue
        outcomes = (
            getattr(one.result, "outcomes", None) if isinstance(one.result, Verdict) else None
        )
        if outcomes is None or len(outcomes) != len(one.carried):
            fates.update(dict.fromkeys(one.carried, "ambiguous"))
            continue
        for token, outcome in zip(one.carried, outcomes, strict=True):
            fates[token] = fate_of(outcome)
    return fates


def agrees(fates: Mapping[str, Fate], delivery: Any) -> str | None:
    """The differential oracle: one run's fates against the engine's own `Delivery`.

    Args:
      fates: The run's fates.
      delivery: The run's `Delivery`.

    Returns:
      `None` when the counts agree or the run failed; otherwise what differs.

    Raises:
      None.
    """
    if delivery.failed:
        return None
    counts = collections.Counter(fates.values())
    lost = sum(getattr(delivery, name) for name in ("rejected", "dropped", "unresolved", "raised"))
    want = {
        "landed": delivery.accepted,
        "ambiguous": delivery.unknown,
        "skipped": delivery.skipped,
        "lost": lost,
    }
    got = {k: counts.get(k, 0) for k in want}
    return None if got == want else f"reconstructed {got} against Delivery {want}"


def losses_total(sink: object) -> tuple[int, int]:
    """`(dropped, failed)` from a sink's `losses()`, or zeros when it reports none.

    Args:
      sink: Any sink.

    Returns:
      The two counts.

    Raises:
      None.
    """
    reader = getattr(sink, "losses", None)
    value = reader() if callable(reader) else None
    if value is None:
        return (0, 0)
    return (value.dropped, value.failed)


_LOST = re.compile(r"^log-foundry: lost \d+ ")


def lines_of(text: str) -> int:
    """How many loss lines a stretch of stderr holds: lines starting `log-foundry: lost N `.

    Counted as lines rather than summed as events: the HTTP family names its losses in requests, its
    recorded unit, so a `MultiSink` child that failed every request names `lost 1 request(s)` for
    eight events (SPEC-092 FR-003).

    Args:
      text: A stretch of stderr.

    Returns:
      The number of loss lines.

    Raises:
      None.
    """
    return sum(1 for line in text.splitlines() if _LOST.match(line))


@dataclasses.dataclass
class Batch:
    """One batch as the worker's `_emit` handled it.

    Attributes:
      tokens: The `log_id`s it carried.
      worker_sink: The worker's sink while it ran.
      runs: The slice of the row's runs made inside it.
      failed_batches: How far it moved `failed_batches`.
      moved: How far it moved the sink's `losses()`.
      losses_after: The sink's `losses()` when it returned.
      lines: Loss lines written inside it.
      text_end: Where stderr stood when it returned.
    """

    tokens: list[str]
    worker_sink: object
    runs: tuple[int, int]
    failed_batches: int
    moved: int
    losses_after: tuple[int, int]
    lines: int
    text_end: int


@dataclasses.dataclass(frozen=True)
class RowResult:
    """A driven and judged row.

    Attributes:
      verdict: The failed checks.
      events: The row's events.
      backend: Its backend.
      report: Its report.
      built: The class of the sink the worker was given.
    """

    verdict: tuple[Check, ...]
    events: list[str]
    backend: Backend
    report: Report
    built: type


def _fresh_process_state() -> None:
    """What `tests/conftest.py`'s autouse `_reset_config` does before each test, per row.

    Args:
      None.

    Returns:
      None.

    Raises:
      None.
    """
    import conftest
    from log_foundry import config, context

    config._config = config.Config()
    context.reset_context()
    conftest._reset_worker()


@contextlib.contextmanager
def _network_guard(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[str]]:
    """Makes a real socket's `connect`, `sendto`, `sendmsg` and `getaddrinfo` raise, and records each.

    The record is what fails the row: a sink's catcher would absorb the raise and the row could
    read green over events that reached nothing.

    Args:
      monkeypatch: The row's patch.

    Returns:
      The attempts, as the guard records them, yielded for the life of the context.

    Raises:
      None.
    """
    attempts: list[str] = []

    def no_connect(sock: socket.socket, address: object) -> None:
        """Records a connect and refuses it.

        Args:
          sock: The socket.
          address: Where it would have connected.

        Returns:
          None.

        Raises:
          RuntimeError: Always.
        """
        attempts.append(f"connect {address!r}"[:160])
        raise RuntimeError("the conformance corpus opened a network connection")

    def no_resolve(*args: Any, **kwargs: Any) -> Any:
        """Records a name resolution and refuses it.

        Args:
          *args: `getaddrinfo`'s positional arguments.
          **kwargs: Its keyword arguments.

        Returns:
          Nothing; it raises.

        Raises:
          RuntimeError: Always.
        """
        attempts.append(f"getaddrinfo {args!r} {kwargs!r}"[:160])
        raise RuntimeError("the conformance corpus resolved a network name")

    def no_send(sock: socket.socket, *args: Any) -> int:
        """Records a datagram sent on a real socket and refuses it.

        Args:
          sock: The socket.
          *args: `sendto`'s or `sendmsg`'s arguments.

        Returns:
          Nothing; it raises.

        Raises:
          RuntimeError: Always.
        """
        attempts.append(f"send {args[1:]!r}"[:160])
        raise RuntimeError("the conformance corpus sent on a real socket")

    monkeypatch.setattr(socket.socket, "connect", no_connect)
    monkeypatch.setattr(socket.socket, "sendto", no_send)
    monkeypatch.setattr(socket.socket, "sendmsg", no_send)
    monkeypatch.setattr(socket, "getaddrinfo", no_resolve)
    yield attempts


def _set_flush_interval(monkeypatch: pytest.MonkeyPatch, seconds: float) -> None:
    """Raises the worker's `flush_interval` default, on the unwrapped `Worker.__init__`.

    `tests/ledger_check.py` wraps `Worker.__init__` with `functools.wraps`, whose wrapper has no
    keyword defaults of its own; the defaults live on the function it wraps. The key is asserted
    first because `setitem` would add a missing key silently.

    Args:
      monkeypatch: The row's patch.
      seconds: The new default.

    Returns:
      None.

    Raises:
      AssertionError: When `Worker.__init__` no longer has a `flush_interval` default.
    """
    from log_foundry.worker import Worker

    defaults = inspect.unwrap(Worker.__init__).__kwdefaults__
    assert defaults is not None and "flush_interval" in defaults, (
        "Worker.__init__ no longer takes a flush_interval keyword default"
    )
    monkeypatch.setitem(defaults, "flush_interval", seconds)


def drive(row: Any, monkeypatch: pytest.MonkeyPatch, *, ledger: bool = True) -> RowResult:
    """Runs one row through the public path under `monkeypatch`, and judges it.

    `configure(sink=…)`, one `@trace`d call per batch logging `N - 2` events inside its span
    (with `span.start` and `span.end`, `N` events in one submission), a `flush()` between the two
    batches of `peer-closed`, and `shutdown(timeout=None)`. The exit drain delivers with the stop
    event set, so every backoff returns at once and no retry is declined, which shortens a row
    and changes no verdict; `shutdown()` then closes the sink, which is where a client buffer's
    late report lands. The worker's `flush_interval` default is raised to an hour, so SPEC-091's
    sweep of an open span does not split a row's one submission across two batches; one that
    splits anyway is a harness error.

    Args:
      row: A `conformance_classes.Row`.
      monkeypatch: Where every seam is set; the caller restores it.
      ledger: Whether to read SPEC-088's ledger (off for a tree that predates it).

    Returns:
      The judged row.

    Raises:
      HarnessError: When the row built another class, reached the network, reached the sink in
        another number of worker batches than it submitted, handled other events than its own,
        left a decided action unapplied, or was read per event in one batch and per batch in
        another.
    """
    import conformance_doubles as doubles
    import log_foundry
    from log_foundry import model
    from log_foundry.worker import Worker

    _fresh_process_state()
    before = log_foundry.health().events if ledger else None
    backend = Backend(row.fault)
    stderr = io.StringIO()
    counter = [iter(range(10**6))]
    current = [0]
    handled: list[Batch] = []

    doubles.install_drivers(monkeypatch)
    _set_flush_interval(monkeypatch, 3600.0)
    monkeypatch.setattr(
        model, "new_log_id", lambda: f"c92e{current[0] * 100 + next(counter[0]) % 100:04d}"
    )
    recorder = Recorder(monkeypatch)
    original_emit = Worker._emit
    after_ledger = None
    with _network_guard(monkeypatch) as attempts, contextlib.redirect_stderr(stderr):
        sink, target = doubles.build(row, backend, monkeypatch)

        def emit(self: Any, event_lists: list[list[dict[str, object]]], **kw: Any) -> None:
            """`Worker._emit`, recording the batch it handled and what it moved.

            Args:
              self: The worker.
              event_lists: The batch's event lists.
              **kw: `_emit`'s keywords, such as SPEC-090's `exit_batch`.

            Returns:
              None.

            Raises:
              BaseException: Whatever `Worker._emit` raises.
            """
            carried = [str(e.get("log_id")) for events in event_lists for e in events]
            mark = len(recorder.runs)
            failed = self.failed_batches
            worker_sink = self.sink
            start = losses_total(sink)
            text_at = len(stderr.getvalue())
            try:
                original_emit(self, event_lists, **kw)
            finally:
                after = losses_total(sink)
                if carried:
                    handled.append(
                        Batch(
                            carried,
                            worker_sink,
                            (mark, len(recorder.runs)),
                            self.failed_batches - failed,
                            (after[0] - start[0]) + (after[1] - start[1]),
                            after,
                            lines_of(stderr.getvalue()[text_at:]),
                            len(stderr.getvalue()),
                        )
                    )

        monkeypatch.setattr(Worker, "_emit", emit)
        log_foundry.configure(service="spec092", sink=sink)

        @log_foundry.trace
        def work() -> None:
            """One traced call that logs `N - 2` events inside its span.

            Args:
              None.

            Returns:
              None.

            Raises:
              None.
            """
            for _ in range(N - 2):
                log_foundry.error("m")

        for number in range(row.batches):
            current[0] = number
            counter[0] = iter(range(10**6))
            backend.begin_batch(number)
            work()
            if number < row.batches - 1:
                log_foundry.flush(timeout=10.0)
        log_foundry.shutdown(timeout=None)
        backend.after_close()
        if ledger:
            after_ledger = log_foundry.health().events
    if attempts:
        raise HarnessError(f"{row.id} reached the network: {attempts}")
    if type(sink).__name__ != row.variant.sink:
        raise HarnessError(f"{row.id} built {type(sink).__name__}")
    if len(handled) != row.batches:
        raise HarnessError(
            f"{row.id}'s submissions reached the sink in {len(handled)} worker batch(es), not "
            f"{row.batches}: a split submission is read as two batches, so the row is not judged"
        )
    events = [t for b in handled for t in b.tokens]
    if len(events) != row.batches * N:
        raise HarnessError(f"{row.id} handled {len(events)} events, not {row.batches * N}")
    unapplied = backend.unapplied()
    if unapplied:
        raise HarnessError(f"{row.id}'s double ignored its fault: {unapplied}")
    report = _report(handled, recorder, sink, target, stderr.getvalue())
    if before is not None and after_ledger is not None:
        report = dataclasses.replace(
            report,
            ledger_moves={t: getattr(after_ledger, t) - getattr(before, t) for t in LEDGER_TERMS},
            ledger_expected=_ledger_expected(handled, recorder),
            unaccounted=after_ledger.unaccounted,
        )
    return RowResult(judge(events, backend, report), events, backend, report, type(sink))


def run_row(row: Any, *, ledger: bool = True) -> RowResult:
    """Drives one row under a `MonkeyPatch` of its own, so every seam is restored when it returns.

    Args:
      row: A `conformance_classes.Row`.
      ledger: Whether to read SPEC-088's ledger.

    Returns:
      The judged row.

    Raises:
      HarnessError: As `drive` does.
    """
    with pytest.MonkeyPatch.context() as monkeypatch:
        return drive(row, monkeypatch, ledger=ledger)


def _report(
    handled: list[Batch], recorder: Recorder, sink: object, target: object, text: str
) -> Report:
    """Reads what the library reported, per event where the judged sink ran the engine.

    Args:
      handled: The batches `Worker._emit` handled.
      recorder: The row's runs and rounds.
      sink: The sink the worker was given.
      target: The sink whose runs are judged per event.
      text: Everything the row wrote to stderr.

    Returns:
      The report, its ledger fields unset.

    Raises:
      HarnessError: When one batch is read per event and another per batch, which would mix two
        readings into one report.
    """
    after_close = losses_total(sink)
    fates: dict[str, Fate] | None = {}
    landed = lost = 0
    disagreement: str | None = None
    batch_losses: list[tuple[int, int]] = []
    per_event = [
        any(run.sink is target for run in recorder.runs[b.runs[0] : b.runs[1]]) for b in handled
    ]
    if len(set(per_event)) > 1:
        raise HarnessError(
            f"the judged sink ran the engine in some batches and not others: {per_event}"
        )
    for batch in handled:
        judged = [run for run in recorder.runs[batch.runs[0] : batch.runs[1]] if run.sink is target]
        mine = set(batch.tokens)
        if judged and fates is not None:
            found_all: dict[str, Fate] = {}
            for run in judged:
                found = {
                    t: f
                    for t, f in fates_from(recorder.rounds, target, run.run_id).items()
                    if t in mine
                }
                if run.delivery.failed:
                    found = dict.fromkeys(mine, "lost")
                else:
                    disagreement = disagreement or agrees(found, run.delivery)
                found_all.update(found)
            fates.update(found_all)
            batch_lost = sum(1 for f in found_all.values() if f in ("lost", "ambiguous"))
        else:
            fates = None
            if batch.failed_batches:
                batch_lost = len(batch.tokens)
                lost += batch_lost
            else:
                batch_lost = min(batch.moved, len(batch.tokens))
                landed += len(batch.tokens) - batch_lost
                lost += batch_lost
        batch_losses.append((batch_lost, batch.lines))
    last = handled[-1].losses_after if handled else (0, 0)
    late = (after_close[0] - last[0]) + (after_close[1] - last[1])
    ambiguous = 0
    if fates is not None:
        landed = sum(1 for f in fates.values() if f == "landed")
        lost = sum(1 for f in fates.values() if f in ("lost", "skipped"))
        ambiguous = sum(1 for f in fates.values() if f == "ambiguous")
    return Report(
        fates=fates,
        landed=landed,
        lost=lost,
        ambiguous=ambiguous,
        late=late,
        batch_losses=tuple(batch_losses),
        late_lines=lines_of(text[handled[-1].text_end :]) if handled else 0,
        disagreement=disagreement,
    )


def _ledger_expected(handled: list[Batch], recorder: Recorder) -> dict[str, int]:
    """The four ledger terms as the worker's own path and the fates set them.

    The worker's own path is read from `_engine._run`'s `sink` argument against the worker's
    sink seen inside its `_emit`, never from the ledger. An engine run on the worker's own sink
    puts a settled run's landed events in `delivered`, its skipped ones in `skipped` and the
    rest in `sink_lost`, and a failed run's batch in `batch_lost`. Otherwise the worker's `emit`
    loop ran, and SPEC-088's ledger ends where `emit` returns: a batch whose `emit` returned is
    all `delivered`, an abandoned one all `batch_lost`. A wrapper with no `deliver`, such as
    `MultiSink`, takes the second route although its child's fates are read per event.

    Args:
      handled: The batches `Worker._emit` handled.
      recorder: The row's runs and rounds.

    Returns:
      `delivered`, `skipped`, `sink_lost` and `batch_lost`.

    Raises:
      None.
    """
    want = dict.fromkeys(LEDGER_TERMS, 0)
    for batch in handled:
        own = [
            run
            for run in recorder.runs[batch.runs[0] : batch.runs[1]]
            if run.sink is batch.worker_sink
        ]
        size = len(batch.tokens)
        if not own:
            want["batch_lost" if batch.failed_batches else "delivered"] += size
            continue
        run = own[-1]
        if run.delivery.failed:
            want["batch_lost"] += size
            continue
        mine = set(batch.tokens)
        fates = {
            t: f for t, f in fates_from(recorder.rounds, run.sink, run.run_id).items() if t in mine
        }
        want["delivered"] += sum(1 for f in fates.values() if f == "landed")
        want["skipped"] += sum(1 for f in fates.values() if f == "skipped")
        want["sink_lost"] += sum(1 for f in fates.values() if f in ("lost", "ambiguous"))
    return want
