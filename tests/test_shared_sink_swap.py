"""SPEC-087: a swap keeps the sinks the graph it installs still holds, and closes each sink once.

A caller who logs to a sink and later adds a destination beside it writes
`configure(sink=MultiSink(sink, other))`; one who drops it again writes `configure(sink=sink)`.
Until SPEC-087 either call closed the shared sink while it was still the live destination, and a
sink that has released its transport refuses every later event. The doubles here refuse after
`close()`, as a shipped sink does, so a close at the swap shows as refused events and a second
close; the table at the top is run against a shipped `FileSink` too, and was run red against
`5cb0f449` before the change. Serves invariants 2, 3, 5, 6 and 12.

The table names nothing the tree before SPEC-087 lacks, so this whole file collects there; a test
that reaches a seam SPEC-087 added names it inside its body.
"""

from __future__ import annotations

import gc
import importlib
import os
import pathlib
import random
import re
import subprocess
import sys
import threading
import time
from functools import partial
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry
from conftest import join_graced
from log_foundry import _fork, _lifecycle, api
from log_foundry import worker as worker_mod
from log_foundry.sinks.base import SinkDeliveryError
from log_foundry.sinks.file import FileSink
from log_foundry.sinks.filtering import FilteringSink
from log_foundry.sinks.multi import MultiSink
from log_foundry.sinks.transform import TransformSink
from test_owed_closes import BufferingSink

if TYPE_CHECKING:
    from collections.abc import Callable

GATE = 10.0
"""How long any parked call waits for the test to release it; never reached on a green run."""

ROOT = pathlib.Path(__file__).resolve().parents[1]
PROBES = ROOT / "docs" / "audits" / "2026-11-14-probes" / "lifecycle"


class Shared:
    """A sink that refuses after `close()`, counts its closes and sees one overlap another.

    `park_emit`, when set, holds the drain thread inside `emit` until the test sets it, and
    `parked` says it is there; `serial` makes `close()` take the lock `emit` holds, so a close
    under a parked drain thread blocks rather than passing unseen. `close_gate`, when set, holds
    every close open until the test sets it.
    """

    log_foundry_stop_signal: threading.Event | None = None

    def __init__(self, name: str = "A", *, serial: bool = False) -> None:
        self.name = name
        self.serial = serial
        self.closes = 0
        self.events = 0
        self.refused = 0
        self.closed = False
        self.inside = 0
        self.closing = 0
        self.overlaps = 0
        self.closed_while_inside = 0
        self.events_at_close: list[int] = []
        self.close_threads: list[str] = []
        self.park_emit: threading.Event | None = None
        self.parked = threading.Event()
        self.close_gate: threading.Event | None = None
        self.slow_emit = 0.0
        self._lock = threading.Lock()
        self._emit_lock = threading.Lock()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Takes the batch, or refuses it once closed; parks the drain thread when asked."""
        with self._lock:
            if self.closed:
                self.refused += len(batch)
                raise SinkDeliveryError(f"{self.name} is closed")
            self.inside += 1
        try:
            if self.serial:
                self._emit_lock.acquire()
            try:
                gate = self.park_emit
                if gate is not None and threading.current_thread().name == "log-foundry-worker":
                    self.parked.set()
                    gate.wait(GATE)
                if self.slow_emit:
                    time.sleep(self.slow_emit)
                with self._lock:
                    self.events += len(batch)
            finally:
                if self.serial:
                    self._emit_lock.release()
        finally:
            with self._lock:
                self.inside -= 1

    def close(self) -> None:
        """Counts the close, and whether it overlapped another or a thread still inside."""
        with self._lock:
            if self.inside:
                self.closed_while_inside += 1
            self.closing += 1
            if self.closing > 1:
                self.overlaps += 1
        try:
            if self.serial:
                self._emit_lock.acquire()
                self._emit_lock.release()
            gate = self.close_gate
            if gate is not None:
                gate.wait(GATE)
            time.sleep(0.01)
        finally:
            with self._lock:
                self.closing -= 1
                self.closes += 1
                self.closed = True
                self.events_at_close.append(self.events)
                self.close_threads.append(threading.current_thread().name)

    def __repr__(self) -> str:
        """Names the sink and its counts, so a failure reads without a debugger."""
        return (
            f"<{self.name} closes={self.closes} events={self.events} refused={self.refused} "
            f"overlaps={self.overlaps} closed_while_inside={self.closed_while_inside}>"
        )


class CountingFileSink(FileSink):
    """A shipped `FileSink` that counts its closes."""

    def __init__(self, path: str) -> None:
        super().__init__(path)
        self.closes = 0

    def close(self) -> None:
        """Counts, then closes as the shipped sink does."""
        self.closes += 1
        super().close()


@log_foundry.trace
def _span() -> None:
    """One span: a start and an end event on the worker path."""


def _write(path: str, n: int = 1) -> None:
    """Writes `n` events with no span, or `n` spans and a flush."""
    for _ in range(n):
        if path == "spans":
            _span()
        else:
            log_foundry.info("x")
    if path == "spans":
        log_foundry.flush(timeout=5.0)


def _per(path: str) -> int:
    """Events one write delivers: a span is a start and an end event."""
    return 2 if path == "spans" else 1


def _losses() -> tuple[int, ...]:
    """Every loss term `health()` reports."""
    h = log_foundry.health()
    return (h.dropped, h.failed_batches, h.orphan_lost, h.in_span_lost, h.submitted_after_shutdown)


def _wait(predicate: Callable[[], bool], timeout: float = GATE) -> bool:
    """Polls `predicate` until it holds or `timeout` passes."""
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


def _owed() -> list[object]:
    """The owed record's sinks, in arming order."""
    return list(_lifecycle._state._closes.owed.values())


def _lines(path: pathlib.Path) -> int:
    """How many lines a `FileSink` wrote."""
    return len(path.read_text().splitlines()) if path.exists() else 0


# -- the wrapper table (FR-005): every shipped wrapper, each direction, both paths ----------------

BUILDERS: dict[tuple[str, str], Callable[[Any, Any], Any]] = {
    ("sinks.multi", "MultiSink"): lambda a, b: MultiSink(a, b),
    ("sinks.filtering", "FilteringSink"): lambda a, b: FilteringSink(a),
    ("sinks.transform", "TransformSink"): lambda a, b: TransformSink(a, lambda event: event),
}
"""How each shipped wrapper is built around the shared sink `a` (and a second member `b`)."""

FACTORY = ("sinks._factory", "_FactorySink")
"""The `sink_factory=` wrapper, configured through `configure(sink_factory=...)`."""

EXEMPT = {
    ("sinks.logstash", "LogstashSink"): "builds its own inner sink; a caller cannot share it",
    ("sinks.sentry", "SentrySink"): "builds its own inner sink; a caller cannot share it",
}
"""Wrappers in the slot table that hold no sink a caller handed them."""

REVERSE_ONLY = {FACTORY: "its forward direction is FR-004's first shape: a factory not yet built"}

NOT_SINKS = {("config", "Config"), ("worker", "Worker"), ("worker", "SwapOutcome")}
"""Slot-table entries that hold a sink but are not sinks themselves."""


def _slot_classes(table: dict[tuple[str, str], dict[str, str]]) -> dict[tuple[str, str], type]:
    """Each slot-table key resolved to its class."""
    out: dict[tuple[str, str], type] = {}
    for module, qualname in table:
        found: Any = importlib.import_module(f"log_foundry.{module}")
        for part in qualname.split("."):
            found = getattr(found, part)
        out[(module, qualname)] = found
    return out


UNCOVERED = "a sink wrapper in _fork._CHILD_SINK_SLOTS has neither a row nor a named exemption"
NOT_A_SINK = "a slot-table entry that is not a sink is not named in NOT_SINKS"


def coverage_findings(table: dict[tuple[str, str], dict[str, str]]) -> list[str]:
    """Why the table below does not cover a slot table, one reason per key."""
    findings: list[str] = []
    for key, cls in sorted(_slot_classes(table).items()):
        if not (hasattr(cls, "emit") and hasattr(cls, "close")):
            if key not in NOT_SINKS:
                findings.append(f"{key}: {NOT_A_SINK}")
            continue
        if key not in BUILDERS and key != FACTORY and key not in EXEMPT:
            findings.append(f"{key}: {UNCOVERED}")
    return findings


def test_every_sink_wrapper_in_the_slot_table_has_a_row_or_an_exemption() -> None:
    """FR-005 AC2: a wrapper added to the slot table later fails here rather than going unchecked."""
    assert coverage_findings(_fork._CHILD_SINK_SLOTS) == []


def test_the_coverage_check_names_an_unrowed_wrapper_and_an_unnamed_holder() -> None:
    """The check above refuses both shapes, by reason, and is silent on the shipped table."""
    table = dict(_fork._CHILD_SINK_SLOTS)
    table[("sinks.memory", "MemorySink")] = {"_events": "one"}
    table[("_lifecycle", "Runtime")] = {"_worker": "one"}
    assert coverage_findings(table) == [
        f"{('_lifecycle', 'Runtime')}: {NOT_A_SINK}",
        f"{('sinks.memory', 'MemorySink')}: {UNCOVERED}",
    ]


def _table_cases() -> list[Any]:
    """Every row of the table: wrapper, direction, path and double."""
    cases = []
    for key in [*BUILDERS, FACTORY]:
        for direction in ("forward", "reverse"):
            if key in REVERSE_ONLY and direction == "forward":
                continue
            for path in ("orphan", "spans"):
                for double in ("refusing", "file"):
                    name = f"{key[1]}-{direction}-{path}-{double}"
                    cases.append(pytest.param(key, direction, path, double, id=name))
    return cases


@pytest.mark.parametrize(("key", "direction", "path", "double"), _table_cases())
def test_a_shared_sink_is_kept_open_and_closed_once(
    key: tuple[str, str], direction: str, path: str, double: str, tmp_path: pathlib.Path
) -> None:
    """FR-005 AC1: every event written reaches the shared sink, and every sink closes once.

    One write before the swap and two after, then `shutdown()`. The shared sink is either a double
    that refuses after `close()` or a shipped `FileSink`; at `5cb0f449` every row lost events, a
    second close, or both.
    """
    target = tmp_path / "shared.jsonl"
    shared: Any = Shared("A") if double == "refusing" else CountingFileSink(str(target))
    other = Shared("B")
    if key == FACTORY:
        first: dict[str, Any] = {"sink_factory": lambda: shared}
    else:
        first = {"sink": BUILDERS[key](shared, other)}
    second: dict[str, Any] = {"sink": shared}
    if direction == "forward":
        first, second = second, first
    log_foundry.configure(service="table", **first)
    _write(path)
    log_foundry.configure(**second)
    _write(path, 2)
    log_foundry.shutdown(timeout=5.0)
    join_graced()

    expected = 3 * _per(path)
    delivered = shared.events if double == "refusing" else _lines(target)
    counts = f"delivered={delivered}/{expected} closes={shared.closes} losses={_losses()}"
    assert delivered == expected, f"every event reached the shared sink: {counts}"
    assert shared.closes == 1, f"the shared sink was closed once: {counts}"
    if key == ("sinks.multi", "MultiSink"):
        assert other.closes == 1, f"and the other member once: {other!r}"
    assert _losses() == (0, 0, 0, 0, 0), counts


# -- FR-001: the forward direction, and the read ------------------------------------------------


def _forward_wrappers() -> list[Any]:
    """The four `W`s of FR-001's first criterion."""
    return [
        pytest.param(lambda a, b: MultiSink(a, b), id="multi"),
        pytest.param(lambda a, b: FilteringSink(a), id="filtering"),
        pytest.param(lambda a, b: TransformSink(a, lambda event: event), id="transform"),
        pytest.param(lambda a, b: MultiSink(FilteringSink(a), b), id="nested"),
    ]


@pytest.mark.parametrize("path", ["orphan", "spans"])
@pytest.mark.parametrize("build", _forward_wrappers())
def test_forward_keeps_the_shared_sink_open(
    build: Callable[[Any, Any], Any], path: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-001 AC1: `configure(A)`, a write, `configure(W)`, two writes, then `shutdown()`."""
    a, b = Shared("A"), Shared("B")
    log_foundry.configure(service="fwd", sink=a)
    _write(path)
    log_foundry.configure(sink=build(a, b))
    _write(path, 2)
    join_graced()
    assert a.closes == 0, f"the swap closed the sink the new graph holds: {a!r}"
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert a.closes == 1 and a.refused == 0 and a.events == 3 * _per(path), a
    assert _losses() == (0, 0, 0, 0, 0)
    assert capsys.readouterr().err == ""


def _run_probe(script: str, tmp_path: pathlib.Path) -> str:
    """Runs one audit probe with the documented command, against this tree's `src`."""
    env = dict(os.environ, PYTHONPATH=str(ROOT / "src"), TMPDIR=str(tmp_path))
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-B", str(PROBES / script)],
        capture_output=True,
        text=True,
        timeout=180,
        env=env,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    return result.stdout


def test_the_p4b_forward_probe_prints_full_files(tmp_path: pathlib.Path) -> None:
    """FR-001 AC2: the audit's forward probe, whose shared sink is a shipped `FileSink`."""
    out = _run_probe("p4b_forward.py", tmp_path)
    assert re.search(r"^orphan .*caller=4 lines in the file=4 ", out, re.MULTILINE), out
    assert re.search(r"^worker .*caller=8 lines in the file=8 ", out, re.MULTILINE), out


@pytest.mark.parametrize("path", ["orphan", "spans"])
def test_after_the_forward_swap_the_record_holds_the_wrapper_only(path: str) -> None:
    """FR-001 AC3: the kept sink left the record and joined the graph that holds it."""
    a, b = Shared("A"), Shared("B")
    log_foundry.configure(service="fwd", sink=a)
    _write(path)
    wrapper = MultiSink(a, b)
    log_foundry.configure(sink=wrapper)
    owed = _owed()
    assert len(owed) == 1 and owed[0] is wrapper, owed
    assert log_foundry.health().closing_sinks == 0


class EqualShared(Shared):
    """A `Shared` equal to every other one, so only identity tells two apart."""

    def __eq__(self, other: object) -> bool:
        """Equal to any `EqualShared`."""
        return isinstance(other, EqualShared)

    def __hash__(self) -> int:
        """One hash for all, consistent with `__eq__`."""
        return 0


def test_an_equal_but_distinct_sink_is_not_kept() -> None:
    """FR-001 AC4: membership is by identity, so `A1 == A2` does not keep `A1`."""
    a1, a2, b = EqualShared("A1"), EqualShared("A2"), Shared("B")
    log_foundry.configure(service="eq", sink=a1)
    _write("orphan")
    log_foundry.configure(sink=MultiSink(a2, b))
    assert _wait(lambda: a1.closes == 1), a1
    assert a2.closes == 0, a2
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert a1.closes == 1 and a2.closes == 1, (a1, a2)


def test_nothing_written_closes_nothing_then_shutdown_closes_each_once() -> None:
    """FR-001 AC5: a swap with nothing owed closes nothing; a write then owes the new graph."""
    a, b = Shared("A"), Shared("B")
    log_foundry.configure(service="cold", sink=a)
    log_foundry.configure(sink=MultiSink(a, b))
    assert (a.closes, b.closes) == (0, 0)
    _write("orphan")
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert (a.closes, b.closes) == (1, 1), (a, b)


def _raising(holder: object, found: list[object]) -> list[object]:
    """A per-holder reader that raises, carrying a value no line may print."""
    raise RuntimeError("secret-value-087")


@pytest.mark.parametrize("fault", ["raise", "trip"])
def test_a_read_that_raises_or_trips_is_absorbed_and_the_swap_goes_on(
    fault: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-001 AC7: the read keeps what it found, writes its line, and `configure()` goes on."""
    real = _lifecycle._installed_graph
    faulted = partial(real, read=_raising) if fault == "raise" else partial(real, ceiling=1)
    a = Shared("A")
    log_foundry.configure(service="fault", sink=a)
    _write("orphan")
    new = MultiSink(Shared("C"), Shared("D"))
    monkeypatch.setattr(_lifecycle, "_installed_graph", faulted)
    log_foundry.configure(sink=new)
    monkeypatch.setattr(_lifecycle, "_installed_graph", real)
    log_foundry.info("after")
    assert new._sinks[0].events == 1, "the new sink receives the next event"
    err = capsys.readouterr().err
    if fault == "raise":
        assert "absorbed a failure while reading the sinks beneath a sink (RuntimeError)" in err
        assert "secret-value-087" not in err, err
    else:
        assert "reading the sinks beneath a sink stopped short" in err, err
    assert "a sink it shares with another may be closed while still in use" in err


# -- FR-002: the reverse direction, and one close per decision ----------------------------------


def _counted(cls: type) -> type:
    """A subclass of a shipped wrapper that counts its own `close()` and inherits the rest."""

    def close(self: Any) -> None:
        self.wrapper_closes = getattr(self, "wrapper_closes", 0) + 1
        cls.close(self)

    return type(f"Counted{cls.__name__}", (cls,), {"close": close})


def _reverse_wrappers() -> list[Any]:
    """FR-001's four `W`s, and a kept sink after a nested sibling, each counting its close."""
    multi, filtering = _counted(MultiSink), _counted(FilteringSink)
    transform = _counted(TransformSink)
    return [
        pytest.param(lambda a, b: multi(a, b), id="multi"),
        pytest.param(lambda a, b: filtering(a), id="filtering"),
        pytest.param(lambda a, b: transform(a, lambda event: event), id="transform"),
        pytest.param(lambda a, b: multi(FilteringSink(a), b), id="nested"),
        pytest.param(lambda a, b: multi(FilteringSink(b), a), id="kept-after-nested"),
    ]


@pytest.mark.parametrize("path", ["orphan", "spans"])
@pytest.mark.parametrize("build", _reverse_wrappers())
def test_reverse_closes_only_the_unshared_members(
    build: Callable[[Any, Any], Any], path: str
) -> None:
    """FR-002 AC1: `configure(W)`, a write, `configure(A)`, two writes, then `shutdown()`."""
    a, b = Shared("A"), Shared("B")
    wrapper = build(a, b)
    log_foundry.configure(service="rev", sink=wrapper)
    _write(path)
    log_foundry.configure(sink=a)
    if isinstance(wrapper, MultiSink):
        assert b.closes == 1, f"the unshared member closed by the time configure returned: {b!r}"
    assert _wait(lambda: getattr(wrapper, "wrapper_closes", 0) == 1)
    _write(path, 2)
    assert a.closes == 0, f"the swap closed the live sink through the wrapper: {a!r}"
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert a.closes == 1 and a.refused == 0 and a.events == 3 * _per(path), a
    assert wrapper.wrapper_closes == 1
    assert _losses() == (0, 0, 0, 0, 0)


@pytest.mark.parametrize("script", ["p4b_shared_child_shipped_sink.py", "p4_shared_child_swap.py"])
def test_the_reverse_probes_hold(script: str, tmp_path: pathlib.Path) -> None:
    """FR-002 AC2: the audit's two reverse probes on the built tree."""
    out = _run_probe(script, tmp_path)
    if script.startswith("p4b"):
        assert re.search(r"^orphan .*caller=4 lines in the file=4 ", out, re.MULTILINE), out
        assert re.search(r"^worker .*caller=8 lines in the file=8 ", out, re.MULTILINE), out
    else:
        rows = [line for line in out.splitlines() if "X.closes=" in line]
        assert len(rows) == 4, out
        assert all("X.closes=1 " in r and "X.events_after_its_close=0 " in r for r in rows), out


@pytest.mark.parametrize("path", ["orphan", "spans"])
def test_two_wrappers_sharing_a_member_swap_closes_only_the_dropped_one(path: str) -> None:
    """FR-002 AC3: `MultiSink(A, B)` to `MultiSink(A, C)`: B by the swap, A and C at shutdown."""
    a, b, c = Shared("A"), Shared("B"), Shared("C")
    log_foundry.configure(service="two", sink=MultiSink(a, b))
    _write(path)
    log_foundry.configure(sink=MultiSink(a, c))
    assert b.closes == 1 and a.closes == 0 and c.closes == 0, (a, b, c)
    _write(path, 2)
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert (a.closes, b.closes, c.closes) == (1, 1, 1), (a, b, c)
    assert a.refused == 0 and a.events == 3 * _per(path), a


def test_a_factory_wrapper_then_its_sink_closes_the_sink_once_at_shutdown() -> None:
    """FR-002 AC4: the `sink_factory` wrapper's forward goes through the same keep."""
    a = Shared("A")
    log_foundry.configure(service="factory", sink_factory=lambda: a)
    _write("orphan")
    log_foundry.configure(sink=a)
    _write("orphan", 2)
    assert a.closes == 0, a
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert a.closes == 1 and a.refused == 0 and a.events == 3, a


def test_a_member_held_twice_then_a_filter_over_it() -> None:
    """FR-002 AC5: `MultiSink(A, A, B)` then `FilteringSink(A)`: B by the swap, A at shutdown."""
    a, b = Shared("A"), Shared("B")
    log_foundry.configure(service="twice", sink=MultiSink(a, a, b))
    _write("orphan")
    log_foundry.configure(sink=FilteringSink(a))
    _write("orphan")
    assert (a.closes, b.closes) == (0, 1), (a, b)
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert (a.closes, b.closes) == (1, 1), (a, b)


def test_a_chain_of_three_swaps_closes_each_shared_sink_once_without_overlap() -> None:
    """FR-002 AC6: with spans, `A`, `MultiSink(A, B)`, `C`: the last swap closes A and B once."""
    a, b, c = Shared("A"), Shared("B"), Shared("C")
    log_foundry.configure(service="chain", sink=a)
    _write("spans")
    log_foundry.configure(sink=MultiSink(a, b))
    _write("spans")
    log_foundry.configure(sink=c)
    _write("spans")
    assert _wait(lambda: a.closes == 1 and b.closes == 1), (a, b)
    join_graced()
    assert (a.closes, b.closes, a.overlaps) == (1, 1, 0), (a, b)
    assert a.events == 4 and a.refused == 0, a


def _unconfirmed(monkeypatch: pytest.MonkeyPatch, slow: Shared, new: object) -> None:
    """Swaps to `new` with a budget the drain into `slow` outlasts, so the fence is unconfirmed."""
    slow.slow_emit = 0.5
    _span()
    monkeypatch.setattr(worker_mod, "DEFAULT_SWAP_TIMEOUT", 0.1)
    log_foundry.configure(sink=new)
    monkeypatch.setattr(worker_mod, "DEFAULT_SWAP_TIMEOUT", 5.0)
    assert log_foundry.health().incomplete_swaps == 1, "the premise: the swap is unconfirmed"
    assert _wait(lambda: slow.inside == 0)
    slow.slow_emit = 0.0


def test_closed__two_owed_wrappers_sharing_a_member_are_closed_once_each(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-002 AC7: an unconfirmed swap leaves both wrappers owed; one take closes A once."""
    a, b, c = Shared("A"), Shared("B"), Shared("C")
    log_foundry.configure(service="closed", sink=MultiSink(a, b))
    _span()
    log_foundry.flush(timeout=5.0)
    _unconfirmed(monkeypatch, a, MultiSink(a, c))
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert (a.closes, b.closes, c.closes) == (1, 1, 1), (a, b, c)
    assert a.overlaps == 0, a


def _retire_inside(a: Shared, holder: object) -> None:
    """Parks the drain thread inside `a.emit` through `holder`, then expires a `shutdown()`."""
    log_foundry.configure(service="retired", sink=holder)
    _write("spans")
    a.park_emit = threading.Event()
    _span()
    assert a.parked.wait(GATE), "the drain thread is inside A"
    log_foundry.shutdown(timeout=0.2)


def _leave(a: Shared) -> None:
    """Releases the parked drain thread and waits until it has left `a.emit`."""
    assert a.park_emit is not None
    a.park_emit.set()
    assert _wait(lambda: a.inside == 0)
    a.park_emit = None


def test_remaining__a_member_inside_a_retired_drain_is_not_closed_under_it() -> None:
    """FR-002 AC8: a take of `MultiSink(A, C)` skips A, which the retired drain is still inside."""
    a, b, c = Shared("A"), Shared("B"), Shared("C")
    _retire_inside(a, MultiSink(a, b))
    log_foundry.configure(sink=MultiSink(a, c))
    log_foundry.info("orphan")
    log_foundry.shutdown(timeout=0.3)
    assert a.closes == 0 and a.closed_while_inside == 0, a
    assert c.closes == 1, c
    _leave(a)
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert a.closes == 1 and a.closed_while_inside == 0 and a.refused == 0, a


def test_the_stale_take_guard_reads_an_unread_graph_as_unknown() -> None:
    """FR-003: a sink in use whose graph was not read makes every stale sink pass over.

    A sink armed after the swap's unlocked read has no graph, or a graph read from another
    object that held its ``id``; what lies beneath it is unknown, so nothing stale is taken.
    The controls: a known graph holding the stale sink passes it over, and one that does not
    takes it.
    """
    from log_foundry._lifecycle import _lies_beneath

    stale, holder, other = Shared("S"), Shared("H"), Shared("O")
    assert _lies_beneath(stale, [holder], {}) is True
    assert _lies_beneath(stale, [holder], {id(holder): (other, {})}) is True
    assert _lies_beneath(stale, [holder], {id(holder): (holder, {id(stale): stale})}) is True
    assert _lies_beneath(stale, [holder], {id(holder): (holder, {id(other): other})}) is False
    assert _lies_beneath(stale, [], {}) is False


def test_a_callers_own_wrapper_close_still_closes_every_member() -> None:
    """FR-002 AC9: a forward outside any release the library started carries no keep."""
    a, b = Shared("A"), Shared("B")
    log_foundry.configure(service="own", sink=a)
    _write("orphan")
    MultiSink(a, b).close()
    assert (a.closes, b.closes) == (1, 1), (a, b)


# -- one decision takes a sink and forwards it: the take's claim -------------------------------


def _held_arm(monkeypatch: pytest.MonkeyPatch) -> tuple[threading.Event, threading.Event]:
    """Holds the next orphan log between reading the configured sink and arming it."""
    real = api._note_orphan_emit
    reached, go = threading.Event(), threading.Event()

    def held(sink: Any) -> None:
        reached.set()
        go.wait(GATE)
        real(sink)

    monkeypatch.setattr(api, "_note_orphan_emit", held)
    return reached, go


def _straddle(monkeypatch: pytest.MonkeyPatch, a: object, holder: object) -> None:
    """A log with no span reads `a`, a swap to `holder` keeps it, then the log arms it again."""
    reached, go = _held_arm(monkeypatch)
    straddler = threading.Thread(target=lambda: log_foundry.info("straddle"), name="straddler")
    straddler.start()
    assert reached.wait(GATE), "the straddling log read the configured sink"
    monkeypatch.undo()
    log_foundry.configure(sink=holder)
    go.set()
    straddler.join(GATE)
    assert not straddler.is_alive()
    assert _lifecycle._state._closes.owes(a), "the premise: the kept sink is a root again"


@pytest.mark.parametrize("decision", ["swap", "shutdown"])
def test_a_sink_one_decision_takes_and_forwards_is_closed_once(
    decision: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FR-002's closed part at a take: `MultiSink(Y, Z)` and `Y` owed together, both taken.

    A forward swap with no worker keeps Y; the straddling log then arms it again as a root, so
    the record owes the wrapper and Y. One decision — the next swap's stale take, or the
    closer's take — takes both: Y's own take closes it, and the wrapper's forward of Y is
    skipped because the decision recorded that take before any of its closes started.
    """
    y, z, c = Shared("Y"), Shared("Z"), Shared("C")
    log_foundry.configure(service="took", sink=y)
    _write("orphan")
    wrapper = MultiSink(y, z)
    _straddle(monkeypatch, y, wrapper)
    assert {id(s) for s in _owed()} == {id(wrapper), id(y)}
    if decision == "swap":
        log_foundry.configure(sink=c)
        assert _wait(lambda: y.closes >= 1 and z.closes >= 1)
        join_graced()
        assert (y.closes, z.closes, y.overlaps) == (1, 1, 0), (y, z)
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert (y.closes, z.closes, y.overlaps) == (1, 1, 0), (y, z)


# -- FR-003: the twin paths ---------------------------------------------------------------------


def test_an_expired_shutdown_leaves_the_kept_sink_open(capsys: pytest.CaptureFixture[str]) -> None:
    """FR-003 AC1: forward with spans, the drain parked inside A, A's close sharing emit's lock."""
    a, b = Shared("A", serial=True), Shared("B")
    log_foundry.configure(service="expired", sink=a)
    _write("spans")
    log_foundry.configure(sink=MultiSink(a, b))
    a.park_emit = threading.Event()
    _span()
    assert a.parked.wait(GATE)
    start = time.monotonic()
    log_foundry.shutdown(timeout=0.3)
    took = time.monotonic() - start
    assert took < 1.0, f"the shutdown waited on a close of the sink its drain is inside: {took}"
    assert a.closes == 0, a
    assert "left open" in capsys.readouterr().err
    _leave(a)
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert a.closes == 1 and a.closed_while_inside == 0, a


def test_events_queued_at_a_later_swap_reach_the_shared_sink_before_its_close() -> None:
    """FR-003 AC2: three spans queued behind a slow `A.emit`, then `configure(C)`."""
    a, b, c = Shared("A"), Shared("B"), Shared("C")
    log_foundry.configure(service="queued", sink=a)
    _write("spans")
    log_foundry.configure(sink=MultiSink(a, b))
    a.slow_emit = 0.05
    for _ in range(3):
        _span()
    log_foundry.configure(sink=c)
    join_graced()
    assert a.events == 8 and a.refused == 0, a
    assert a.closes == 1 and a.events_at_close == [8], a


def _straddle_with_worker(monkeypatch: pytest.MonkeyPatch, a: Shared, b: Shared) -> MultiSink:
    """`configure(A)`, a span, then the straddle across a forward swap to `MultiSink(A, B)`."""
    log_foundry.configure(service="straddle", sink=a)
    _write("spans")
    wrapper = MultiSink(a, b)
    _straddle(monkeypatch, a, wrapper)
    return wrapper


def test_the_straddling_log_at_an_expired_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-003 AC3: the kept sink armed again as a root is not closed under the drain thread."""
    a, b = Shared("A", serial=True), Shared("B")
    _straddle_with_worker(monkeypatch, a, b)
    a.park_emit = threading.Event()
    _span()
    assert a.parked.wait(GATE)
    start = time.monotonic()
    log_foundry.shutdown(timeout=0.3)
    took = time.monotonic() - start
    assert took < 1.0, f"the closer took the root its drain thread is inside: {took}"
    assert a.closes == 0 and a.closed_while_inside == 0, a
    _leave(a)
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert a.closes == 1 and a.closed_while_inside == 0 and a.refused == 0, a


def test_a_passed_over_sink_is_owed_and_not_in_flight_when_shutdown_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-003 AC5: the guard's pass-over ends in the put-back that already existed."""
    a, b = Shared("A"), Shared("B")
    _straddle_with_worker(monkeypatch, a, b)
    a.park_emit = threading.Event()
    _span()
    assert a.parked.wait(GATE)
    log_foundry.shutdown(timeout=0.3)
    assert _lifecycle._state._closes.owes(a), "the passed-over sink is back in the record"
    assert not _lifecycle._state._closes.in_flight(a), "with no close in flight for it"
    _leave(a)
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert a.closes == 1, a


def test_the_straddling_log_at_a_later_swap(monkeypatch: pytest.MonkeyPatch) -> None:
    """FR-003 AC4: the swap's stale take passes over the root its drain thread is inside."""
    a, b, c = Shared("A"), Shared("B"), Shared("C")
    _straddle_with_worker(monkeypatch, a, b)
    a.park_emit = threading.Event()
    for _ in range(3):
        _span()
    assert a.parked.wait(GATE)
    swap = threading.Thread(target=lambda: log_foundry.configure(sink=c), name="swapper")
    swap.start()
    time.sleep(0.2)
    assert a.closed_while_inside == 0 and a.closes == 0, a
    _leave(a)
    swap.join(GATE)
    assert not swap.is_alive()
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert a.closes == 1 and a.closed_while_inside == 0 and a.refused == 0, a
    assert a.events == 2 + 1 + 6, a


@pytest.mark.parametrize("worker", ["live", "retired"])
def test_a_stale_sink_both_kept_and_beneath_a_sink_in_use_is_ceded(
    worker: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FR-003: kept is tested before the guard, so both of the sink's holders account for it.

    The straddle leaves A owed as a root beneath `MultiSink(A, X)`, which something may be inside:
    the worker's current sink, or, after an expired `shutdown()`, a retired drain thread parked in
    A through it. The next swap, to `MultiSink(A, Y)`, finds A both kept and beneath a sink in use:
    it cedes A into the graph it installs rather than leaving it owed, and the displaced wrapper's
    forward of A is skipped. With no live worker the cede is the first section's, which is where
    the order of the two tests decides it.
    """
    a, x, y = Shared("A"), Shared("X"), Shared("Y")
    holder = _straddle_with_worker(monkeypatch, a, x)
    if worker == "retired":
        a.park_emit = threading.Event()
        _span()
        assert a.parked.wait(GATE)
        log_foundry.shutdown(timeout=0.2)
    installed = MultiSink(a, y)
    log_foundry.configure(sink=installed)
    assert not _lifecycle._state._closes.owes(a), "A was left owed rather than ceded"
    assert _owed()[-1] is installed
    if worker == "retired":
        assert _lifecycle._state._closes.owes(holder)
        _leave(a)
    else:
        _write("spans")
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert (a.closes, x.closes, y.closes) == (1, 1, 1), (a, x, y)
    assert a.refused == 0 and a.overlaps == 0 and a.closed_while_inside == 0, a


@pytest.mark.parametrize("direction", ["forward", "reverse"])
def test_an_unconfirmed_swap_keeps_the_outcome(
    direction: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-003 AC6: one incomplete swap and its line, and A still closed once, after its events."""
    a, b = Shared("A"), Shared("B")
    wrapper = MultiSink(a, b)
    first, second = (a, wrapper) if direction == "forward" else (wrapper, a)
    log_foundry.configure(service="unconfirmed", sink=first)
    _write("spans")
    _unconfirmed(monkeypatch, a, second)
    assert "could not be confirmed drained" in capsys.readouterr().err
    _write("spans", 2)
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert a.closes == 1 and a.refused == 0 and a.overlaps == 0, a
    assert a.events_at_close == [a.events], a


class Reconfigures(Shared):
    """A member whose first `emit` calls `configure(sink=target)` from inside the sink's code."""

    def __init__(self, name: str, target: object) -> None:
        super().__init__(name)
        self.target = target
        self.fired = False

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Takes the batch, swapping the configured sink the first time."""
        super().emit(batch)
        if not self.fired:
            self.fired = True
            log_foundry.configure(sink=self.target)


class SettingReconfigures(Shared):
    """A member whose stop-signal setter calls `configure(sink=target)`, once."""

    def __init__(self, name: str, target: object) -> None:
        super().__init__(name)
        self.target = target
        self.fired = False
        self._signal: threading.Event | None = None

    @property  # type: ignore[override]
    def log_foundry_stop_signal(self) -> threading.Event | None:
        """The signal the library handed it."""
        return self._signal

    @log_foundry_stop_signal.setter
    def log_foundry_stop_signal(self, signal: threading.Event | None) -> None:
        self._signal = signal
        if not self.fired:
            self.fired = True
            log_foundry.configure(sink=self.target)


@pytest.mark.parametrize("route", ["emit-orphan", "emit-spans", "setter-orphan", "setter-spans"])
def test_a_swap_from_sink_code(route: str) -> None:
    """FR-003 AC7: `configure(sink=A)` from a member's emit or stop-signal setter."""
    a = Shared("A")
    path = "spans" if route.endswith("spans") else "orphan"
    member: Shared = (
        Reconfigures("B", a) if route.startswith("emit") else SettingReconfigures("S", a)
    )
    log_foundry.configure(service="reentrant", sink=a)
    _write(path)
    log_foundry.configure(sink=MultiSink(a, member))
    _write(path)
    _write(path)
    assert member.fired, "the premise: the member's code swapped the sink"
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert a.closes == 1 and a.refused == 0 and a.overlaps == 0, a
    assert a.events_at_close == [a.events], a


@pytest.mark.parametrize("direction", ["forward", "reverse"])
def test_a_shutdown_during_a_swap(direction: str) -> None:
    """FR-003 AC8: the swap parked in its first drain, a `shutdown()` on another thread."""
    a, b = Shared("A"), Shared("B")
    wrapper = MultiSink(a, b)
    first, second = (a, wrapper) if direction == "forward" else (wrapper, a)
    log_foundry.configure(service="mid-swap", sink=first)
    _write("spans")
    a.park_emit = threading.Event()
    _span()
    assert a.parked.wait(GATE)
    swap = threading.Thread(target=lambda: log_foundry.configure(sink=second), name="swapper")
    swap.start()
    time.sleep(0.1)
    stop = threading.Thread(target=lambda: log_foundry.shutdown(timeout=5.0), name="stopper")
    stop.start()
    time.sleep(0.1)
    _leave(a)
    swap.join(GATE)
    stop.join(GATE)
    assert not swap.is_alive() and not stop.is_alive()
    join_graced()
    assert a.closes == 1 and a.refused == 0 and a.overlaps == 0, a
    assert _owed() == [], _owed()


@pytest.mark.parametrize("direction", ["forward", "reverse"])
def test_a_swap_during_a_shutdown(direction: str) -> None:
    """FR-003 AC9: a `shutdown()` parked in another sink's close while the swap runs."""
    a, b, d = Shared("A"), Shared("B"), Shared("D")
    wrapper = MultiSink(a, b)
    first, second = (a, wrapper) if direction == "forward" else (wrapper, a)
    log_foundry.configure(service="mid-stop", sink=first)
    _write("orphan")
    _lifecycle._note_orphan_emit(d)
    d.close_gate = threading.Event()
    stop = threading.Thread(target=lambda: log_foundry.shutdown(timeout=5.0), name="stopper")
    stop.start()
    assert _wait(lambda: d.closing == 1), "the shutdown is inside D's close"
    log_foundry.configure(sink=second)
    d.close_gate.set()
    stop.join(GATE)
    assert not stop.is_alive()
    join_graced()
    assert a.closes == 1 and a.refused == 0 and a.overlaps == 0, a
    assert a.close_threads[0] in {"stopper", "log-foundry-owed-close"}, a.close_threads


EXIT_SCRIPT = """
import atexit, sys, threading
counts = {}
atexit.register(lambda: print(f"A.closes={A.closes} A.events={A.events} A.refused={A.refused} "
                              f"B.closes={B.closes} b_at_swap={counts.get('b')}", flush=True))
import log_foundry
from log_foundry.sinks.multi import MultiSink
class S:
    def __init__(self):
        self.closes = self.events = self.refused = 0
        self.closed = False
    def emit(self, batch):
        if self.closed:
            self.refused += len(batch)
            raise RuntimeError("closed")
        self.events += len(batch)
    def close(self):
        self.closes += 1
        self.closed = True
A, B = S(), S()
W = MultiSink(A, B)
first, second = (A, W) if sys.argv[1] == "forward" else (W, A)
@log_foundry.trace
def work():
    pass
def write():
    if sys.argv[2] == "spans":
        work()
        log_foundry.flush(timeout=5)
    else:
        log_foundry.info("x")
log_foundry.configure(service="exit", sink=first)
write()
log_foundry.configure(sink=second)
counts["b"] = B.closes
write()
write()
"""


@pytest.mark.parametrize("path", ["orphan", "spans"])
@pytest.mark.parametrize("direction", ["forward", "reverse"])
def test_the_exit_drain(direction: str, path: str, tmp_path: pathlib.Path) -> None:
    """FR-003 AC10: no `shutdown()`; the exit drain closes A once, and in reverse B at the swap.

    The probe's `atexit` hook is registered before the library is imported, so it runs after the
    library's own handler (`atexit` is last in, first out).
    """
    script = tmp_path / "exit_drain.py"
    script.write_text(EXIT_SCRIPT)
    env = dict(os.environ, PYTHONPATH=str(ROOT / "src"))
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-B", str(script), direction, path],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    per = _per(path)
    assert f"A.closes=1 A.events={3 * per} A.refused=0" in result.stdout, result.stdout
    if direction == "reverse":
        assert "B.closes=1 b_at_swap=1" in result.stdout, result.stdout


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork")
@pytest.mark.parametrize("direction", ["forward", "reverse"])
def test_a_child_forked_after_the_swap(direction: str, tmp_path: pathlib.Path) -> None:
    """FR-003 AC11: a child's three events reach the shared `FileSink`; each process closes it.

    The parent closes A once. The child closes A once in reverse, where A is the configured sink
    it reacquired, and not at all forward, where A sits beneath the inherited wrapper that a child
    never releases.
    """
    target = tmp_path / "shared.jsonl"
    a = CountingFileSink(str(target))
    b = Shared("B")
    wrapper = MultiSink(a, b)
    first, second = (a, wrapper) if direction == "forward" else (wrapper, a)
    log_foundry.configure(service="fork", sink=first)
    log_foundry.info("parent-before")
    log_foundry.configure(sink=second)
    read, write = os.pipe()
    gc.collect()
    pid = os.fork()
    if pid == 0:
        code = 1
        try:
            os.close(read)
            for index in range(3):
                log_foundry.info("child", index=index)
            log_foundry.shutdown(timeout=5.0)
            os.write(write, str(a.closes).encode())
            code = 0
        finally:
            os._exit(code)
    os.close(write)
    _, status = os.waitpid(pid, 0)
    child_closes = int(os.read(read, 64).decode() or "-1")
    os.close(read)
    assert os.waitstatus_to_exitcode(status) == 0
    log_foundry.info("parent-after")
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    text = target.read_text()
    assert text.count('"message": "child"') == 3, text
    assert "parent-after" in text and "parent-before" in text, text
    assert a.closes == 1, a.closes
    assert child_closes == (1 if direction == "reverse" else 0), child_closes


def test_reconfiguration_200_rounds() -> None:
    """FR-003 AC12: the record does not grow, and every dropped member closes before shutdown."""
    a = Shared("A")
    log_foundry.configure(service="rounds", sink=a)
    members: list[Shared] = []
    sizes: list[int] = []
    for index in range(200):
        member = Shared(f"B{index}")
        members.append(member)
        log_foundry.configure(sink=MultiSink(a, member))
        log_foundry.info("wrapped")
        log_foundry.configure(sink=a)
        log_foundry.info("direct")
        sizes.append(len(_owed()))
    join_graced()
    assert all(member.closes == 1 for member in members), [m for m in members if m.closes != 1]
    assert sizes[-1] <= sizes[0], sizes[:3] + sizes[-3:]
    assert a.closes == 0, a
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert a.closes == 1 and a.events == 400 and a.refused == 0, a


@pytest.mark.parametrize("seed", range(16))
def test_concurrent_callers_over_seeds(seed: int, monkeypatch: pytest.MonkeyPatch) -> None:
    """FR-003 AC13: logging and flushing beside 150 swaps among graphs that all hold A.

    A holds every graph, so it is never closed by a swap: once, at the end, never overlapping,
    and refusing nothing. Every other member is closed once per arming of the wrapper holding
    it, never two at once. A wrapper is armed again when a log with no span read it as the
    configured sink before a swap took it and arms it after, which invariant 5 records as owing a
    second close (SPEC-045, `p8_second_close_after_a_rearm.py`); under four logging threads that
    happens on some seeds, so the count is held to the arms rather than to one.
    """
    rng = random.Random(seed)  # noqa: S311
    a = Shared("A")
    holders: list[tuple[Shared, object]] = []
    arms: dict[int, int] = {}
    registry = _lifecycle._state._closes
    real_arm = registry.arm

    def counting_arm(sink: Any) -> None:
        arms[id(sink)] = arms.get(id(sink), 0) + 1
        real_arm(sink)

    monkeypatch.setattr(registry, "arm", counting_arm)

    def graph() -> object:
        other = Shared(f"O{len(holders)}")
        built = rng.choice(
            [
                lambda: a,
                lambda: MultiSink(a, other),
                lambda: FilteringSink(a),
                lambda: MultiSink(FilteringSink(a), other),
                lambda: TransformSink(a, lambda event: event),
            ]
        )()
        holders.append((other, built))
        return built

    log_foundry.configure(service="seeds", sink=a)
    _write("spans")
    stop = threading.Event()
    errors: list[BaseException] = []

    def logger(index: int) -> None:
        draws = random.Random(seed * 100 + index)  # noqa: S311
        try:
            while not stop.is_set():
                if index % 2 == 0:
                    _span()
                else:
                    log_foundry.info("x")
                if draws.random() < 0.1:
                    log_foundry.flush(timeout=5.0)
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=logger, args=(i,)) for i in range(4)]
    for thread in threads:
        thread.start()
    for _ in range(150):
        log_foundry.configure(sink=graph())
    stop.set()
    for thread in threads:
        thread.join(GATE)
    last = threading.Thread(target=lambda: log_foundry.configure(sink=graph()))
    last.start()
    log_foundry.shutdown(timeout=5.0)
    last.join(GATE)
    join_graced()
    assert not errors, errors
    assert a.closes == 1 and a.overlaps == 0 and a.refused == 0, a
    over = [
        (other, arms.get(id(holder), 0))
        for other, holder in holders
        if other.overlaps or other.closes > max(1, arms.get(id(holder), 0))
    ]
    assert not over, over


@pytest.mark.parametrize("direction", ["forward", "reverse"])
def test_a_sink_whose_close_delivers(direction: str) -> None:
    """FR-003 AC14: a sink that delivers on `close()` and keeps accepting is closed once."""
    a, b = BufferingSink("A"), BufferingSink("B")
    wrapper = MultiSink(a, b)
    first, second = (a, wrapper) if direction == "forward" else (wrapper, a)
    log_foundry.configure(service="buffering", sink=first)
    log_foundry.info("one")
    log_foundry.configure(sink=second)
    log_foundry.info("two")
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert a.closes == 1 and a.buffered == 0 and a.delivered == a.events == 2, a


# -- FR-004: what the read does not reach, and what the library cannot spare --------------------


class MineSink(Shared):
    """A caller's wrapper, built on a sink-shaped base, that closes its member itself."""

    def __init__(self, inner: Shared) -> None:
        super().__init__("Mine")
        self.inner = inner

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Forwards the batch."""
        self.inner.emit(batch)

    def close(self) -> None:
        """Closes its member with its own code."""
        self.inner.close()


class MinePlain:
    """A caller's wrapper that subclasses nothing and closes its member itself."""

    def __init__(self, inner: Shared) -> None:
        self.inner = inner

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Forwards the batch."""
        self.inner.emit(batch)

    def close(self) -> None:
        """Closes its member with its own code."""
        self.inner.close()


class Hoarder:
    """A caller's wrapper that holds its events and writes them to its member at `close()`."""

    def __init__(self, inner: Shared) -> None:
        self.inner = inner
        self.held: list[dict[str, object]] = []

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Holds the batch."""
        self.held.extend(batch)

    def close(self) -> None:
        """Writes what it held, then closes its member."""
        held, self.held = self.held, []
        if held:
            self.inner.emit(held)
        self.inner.close()


class Leaves:
    """A caller's wrapper whose `close()` leaves its member alone."""

    def __init__(self, inner: Shared) -> None:
        self.inner = inner

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Forwards the batch."""
        self.inner.emit(batch)

    def close(self) -> None:
        """Closes nothing."""


class MineT(TransformSink):
    """A caller's wrapper built on a shipped one, inheriting its `close()`."""


@pytest.mark.parametrize("path", ["orphan", "spans"])
@pytest.mark.parametrize("mine", [MineSink, MinePlain], ids=["sink-shaped", "plain"])
def test_a_callers_wrapper_forward_on_a_fenced_swap_and_with_no_worker(
    mine: type, path: str
) -> None:
    """FR-004 AC1: the read reaches into a caller's wrapper, so A joins it and closes once."""
    a = Shared("A")
    log_foundry.configure(service="mine", sink=a)
    _write(path)
    log_foundry.configure(sink=mine(a))
    _write(path, 2)
    assert a.closes == 0, a
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert a.closes == 1 and a.overlaps == 0 and a.refused == 0 and a.events == 3 * _per(path), a


@pytest.mark.parametrize("route", ["unconfirmed", "straddle"])
def test_a_callers_wrapper_after_an_unconfirmed_swap_and_after_the_straddle(
    route: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FR-004 AC2: A stays open until `shutdown()` and refuses nothing; two closes may remain."""
    a = Shared("A")
    log_foundry.configure(service="mine-owed", sink=a)
    _write("spans")
    mine = MinePlain(a)
    if route == "unconfirmed":
        _unconfirmed(monkeypatch, a, mine)
    else:
        _straddle(monkeypatch, a, mine)
    _write("spans", 2)
    assert a.closes == 0, a
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert a.refused == 0 and a.closes >= 1, a


@pytest.mark.parametrize("path", ["orphan", "spans"])
def test_a_hoarding_wrapper_with_every_close_held_open(path: str) -> None:
    """FR-004 AC3: a wrapper that writes to A from its own `close()` finds A open."""
    a = Shared("A")
    log_foundry.configure(service="hoard", sink=a)
    _write(path)
    log_foundry.configure(sink=Hoarder(a))
    _write(path, 2)
    a.close_gate = threading.Event()
    threading.Timer(0.3, a.close_gate.set).start()
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert a.events == 3 * _per(path) and a.refused == 0 and a.closes == 1, a


@pytest.mark.parametrize("path", ["orphan", "spans"])
def test_a_displaced_callers_wrapper_closes_the_live_sink_counted_and_announced(
    path: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-004 AC4: the third shape, displaced: every event A then refuses is counted and said."""
    a = Shared("A")
    log_foundry.configure(service="displaced", sink=MinePlain(a))
    _write(path)
    log_foundry.configure(sink=a)
    _write(path, 2)
    assert a.refused > 0, "the premise: the caller's wrapper closed the live sink"
    health = log_foundry.health()
    if path == "orphan":
        assert health.orphan_lost == a.refused, (health, a)
    else:
        assert health.failed_batches >= 1, health
    assert "lost" in capsys.readouterr().err


@pytest.mark.parametrize("path", ["orphan", "spans"])
def test_an_unbuilt_factory_releases_the_sink_counted_and_announced(
    path: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-004 AC5: the first shape: a factory that has not built holds nothing to read."""
    a = Shared("A")
    log_foundry.configure(service="unbuilt", sink=a)
    _write(path)
    log_foundry.configure(sink_factory=lambda: a)
    _write(path, 2)
    assert a.refused > 0, "the premise: the swap released A, which the factory then returned"
    health = log_foundry.health()
    if path == "orphan":
        assert health.orphan_lost == a.refused, (health, a)
    else:
        assert health.failed_batches >= 1, health
    assert "lost" in capsys.readouterr().err


@pytest.mark.parametrize("first", ["multi", "direct"])
def test_a_wrapper_that_leaves_its_member_installed_over_a_kept_sink(first: str) -> None:
    """FR-004 AC6: the fourth shape, installed: A is never closed, and nothing is lost."""
    a, b = Shared("A"), Shared("B")
    log_foundry.configure(service="leaves", sink=MultiSink(a, b) if first == "multi" else a)
    _write("orphan")
    log_foundry.configure(sink=Leaves(a))
    _write("orphan", 2)
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert a.closes == 0 and a.refused == 0 and a.events == 3, a
    assert _losses() == (0, 0, 0, 0, 0)


def test_a_wrapper_that_leaves_its_member_as_a_remaining_holder() -> None:
    """FR-004 AC7: the fourth shape, remaining: A is not closed under the drain thread."""
    a, b = Shared("A"), Shared("B")
    _retire_inside(a, Leaves(a))
    log_foundry.configure(sink=MultiSink(a, b))
    log_foundry.info("orphan")
    log_foundry.shutdown(timeout=0.3)
    _leave(a)
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert a.closed_while_inside == 0 and a.refused == 0, a


@pytest.mark.parametrize("path", ["orphan", "spans"])
@pytest.mark.parametrize("direction", ["forward", "reverse"])
def test_a_wrapper_built_on_a_shipped_one_inherits_the_rule(direction: str, path: str) -> None:
    """FR-004 AC8: `class Mine(TransformSink)` sits in a declared slot and inherits the forward."""
    a = Shared("A")
    mine = MineT(a, lambda event: event)
    first, second = (a, mine) if direction == "forward" else (mine, a)
    log_foundry.configure(service="mine-t", sink=first)
    _write(path)
    log_foundry.configure(sink=second)
    _write(path, 2)
    assert a.closes == 0, a
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert a.closes == 1 and a.refused == 0 and a.events == 3 * _per(path), a
