"""SPEC-067 — `continue_trace` is total over any argument, and a refusal is always announced.

`continue_trace` is the library's one inbound path, and its four arguments are `str` only by
annotation. The rejection echo used to `repr` the whole value before cutting it, so a shared-sublist
list hung the caller's thread and a raising `__repr__` lost the line while `reason="rejected"` said
it was written; and the parsers called the value's own methods, so a `str` subclass or a spoofed
`__class__` raised into the caller. These tests hand each argument every such value.

Two instruments bound the echo's cost (FR-002), because neither sees all of it: CPU time against
`repr` of the same value in the same test catches a pass over the value on any machine, and the
`tracemalloc` peak catches a full copy followed by a slice, which CPU time is too coarse to see.
The deadline raises a `BaseException`, because an alarm raising an `Exception` would be swallowed
by `rejected`'s own guard and read as a pass.

Every expected echo is a literal, never computed from `_diag`.
"""

from __future__ import annotations

import contextlib
import re
import signal
import sys
import threading
import time
import tracemalloc
from typing import TYPE_CHECKING

import pytest

import log_foundry
from conftest import armed, fan_out, object_probe, str_probe
from log_foundry import _diag

if TYPE_CHECKING:
    from collections.abc import Iterator

_PREFIX = "log-foundry: ignoring inbound trace context"


# -- the deadline (SPEC-064's alarm harness, copied: test modules here do not import each other) --


class _Deadline(BaseException):
    """What the alarm raises: a `BaseException`, because `rejected`'s guard catches `Exception`."""


@contextlib.contextmanager
def _deadline(seconds: float, what: str) -> Iterator[None]:
    """Fails the test, instead of hanging the run, when the body has not returned in time."""
    assert threading.current_thread() is threading.main_thread(), (
        "SIGALRM reaches only the main thread, so the deadline cannot fire here"
    )

    def _ring(signum: int, frame: object) -> None:
        raise _Deadline

    previous = signal.signal(signal.SIGALRM, _ring)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    except _Deadline:
        pytest.fail(f"{what} did not return within {seconds} s", pytrace=False)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


class _Plain(str):
    """A `str` subclass that overrides nothing, so `repr` walks it at base rather than failing early."""

    __slots__ = ()


# -- FR-002: the echo's cost does not grow with the value -----------------------------------------


def test_rejected_on_a_long_str_costs_a_fraction_of_its_repr(
    capsys: pytest.CaptureFixture[str],
) -> None:
    value = "0" * 10**8

    started = time.process_time()
    _diag.rejected("x", value)
    echo_cost = time.process_time() - started

    started = time.process_time()
    repr(value)
    repr_cost = time.process_time() - started

    assert echo_cost < repr_cost / 10, f"rejected {echo_cost:.4f} s against repr {repr_cost:.4f} s"
    assert capsys.readouterr().err == (
        f"{_PREFIX} (x): '000000000000000000000000000000000000000000000000000000000000000…\n"
    ), "the line must be written, or an early return would pass the ratio"


def test_rejected_on_long_bytes_costs_a_fraction_of_its_repr(
    capsys: pytest.CaptureFixture[str],
) -> None:
    value = b"0" * 10**8

    started = time.process_time()
    _diag.rejected("x", value)
    echo_cost = time.process_time() - started

    started = time.process_time()
    repr(value)
    repr_cost = time.process_time() - started

    assert echo_cost < repr_cost / 10, f"rejected {echo_cost:.4f} s against repr {repr_cost:.4f} s"
    assert capsys.readouterr().err == f"{_PREFIX} (x): <bytes>\n", "the line must be written"


@pytest.mark.parametrize(
    ("kind", "echo"),
    [
        ("str", "'000000000000000000000000000000000000000000000000000000000000000…"),
        ("str_subclass", "'000000000000000000000000000000000000000000000000000000000000000…"),
        ("bytes", "<bytes>"),
    ],
    ids=["str", "str_subclass", "bytes"],
)
def test_rejected_allocates_nothing_proportional_to_the_value(
    capsys: pytest.CaptureFixture[str], kind: str, echo: str
) -> None:
    """64 KiB sits some 600 times above the slot slice's measured peak and 1,500 below a copy."""
    value: object = {
        "str": lambda: "0" * 10**8,
        "str_subclass": lambda: _Plain("0" * 10**8),
        "bytes": lambda: b"0" * 10**8,
    }[kind]()

    tracemalloc.start()
    try:
        tracemalloc.reset_peak()
        _diag.rejected("x", value)
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()

    assert peak < 65_536, f"rejected peaked at {peak:,} bytes on a {kind} of 10^8"
    assert capsys.readouterr().err == f"{_PREFIX} (x): {echo}\n", "the line must be written"


def test_rejected_returns_on_a_shared_sublist_dag(capsys: pytest.CaptureFixture[str]) -> None:
    value = fan_out()

    with _deadline(5, "rejected on the DAG"):
        _diag.rejected("x", value)

    assert capsys.readouterr().err == f"{_PREFIX} (x): <list>\n"


# -- FR-004: continue_trace normalises once, and is total -----------------------------------------

_TRACE = "4bf92f3577b34da6a3ce929d0e0e4736"
_SPAN = "00f067aa0ba902b7"
_TRACEPARENT = f"00-{_TRACE}-{_SPAN}-01"

_EVERY_PROBED_METHOD = (
    "strip",
    "__len__",
    "__iter__",
    "__ne__",
    "__eq__",
    "split",
    "encode",
    "__repr__",
    "__str__",
    "__getitem__",
)
_PROBES = (
    "dag",
    "raising_repr",
    "long_str",
    "bytes",
    *(f"subclass{method}" for method in _EVERY_PROBED_METHOD),
    "class_raises",
    "class_is_str",
)
_REFUSING_COLUMNS = ("traceparent", "trace_id", "parent_span_id", "baggage")
_ADOPTION_RAISERS = frozenset({*_EVERY_PROBED_METHOD, "__format__"})


def _probe(kind: str, column: str) -> object:
    """Builds one probe inside the test, so no 50 MB value becomes a test id or outlives it."""
    if kind == "dag":
        return fan_out()
    if kind == "raising_repr":
        return object_probe("_Hostile", raising={"__repr__"})
    if kind == "long_str":
        return "0" * 50_000_000
    if kind == "bytes":
        return b"00-bad"
    if kind == "class_raises":
        return object_probe("_Spoof", class_property="raise")
    if kind == "class_is_str":
        return object_probe("_Spoof", class_property="str")
    text = "k" if column == "baggage" else "00-bad"
    return str_probe(text, raising={kind.removeprefix("subclass")})


def _continue(column: str, value: object) -> object:
    """Hands ``value`` to `continue_trace` as the one argument ``column`` names."""
    if column == "traceparent":
        return log_foundry.continue_trace(value)
    if column == "parent_beside_trace":
        return log_foundry.continue_trace(trace_id=_TRACE, parent_span_id=value)
    return log_foundry.continue_trace(**{column: value})


@pytest.mark.parametrize("kind", _PROBES)
@pytest.mark.parametrize("column", _REFUSING_COLUMNS)
def test_continue_trace_refuses_every_probe_and_announces_it(
    capsys: pytest.CaptureFixture[str], column: str, kind: str
) -> None:
    """Invariants 1 and 11: it returns on any input, and `"rejected"` means a line was written."""
    value = _probe(kind, column)

    with _deadline(5, f"continue_trace({column}=<{kind}>)"), armed() as calls:
        result = _continue(column, value)

    lines = capsys.readouterr().err.splitlines()
    assert type(result) is log_foundry.ContinueResult
    assert calls == [], f"the value's own code ran: {calls}"
    assert not result
    assert result.reason == "rejected"
    assert any(line.startswith(_PREFIX) for line in lines), lines
    if kind == "raising_repr":
        assert any(line.endswith(": <_Hostile>") for line in lines), lines


@pytest.mark.parametrize("kind", _PROBES)
def test_an_invalid_parent_beside_a_valid_trace_joins_as_a_root(
    capsys: pytest.CaptureFixture[str], kind: str
) -> None:
    value = _probe(kind, "parent_beside_trace")

    with _deadline(5, f"continue_trace(trace_id=..., parent_span_id=<{kind}>)"), armed() as calls:
        result = _continue("parent_beside_trace", value)

    lines = capsys.readouterr().err.splitlines()
    assert calls == [], f"the value's own code ran: {calls}"
    assert result, "an invalid parent drops just the parent, never the trace"
    assert len(lines) == 1, lines
    assert "(invalid parent_span_id; joining as a root)" in lines[0]


def _root_events(lf, fake_sink) -> list[dict]:
    """Runs one root span and returns the events it emitted."""

    @lf.trace
    def work() -> None:
        lf.info("inside")

    work()
    return fake_sink.batches[-1]


def test_a_str_subclass_traceparent_is_adopted_as_an_exact_str(lf, fake_sink) -> None:
    value = str_probe(_TRACEPARENT, raising=_ADOPTION_RAISERS)

    with armed() as calls:
        result = lf.continue_trace(value)
        events = _root_events(lf, fake_sink)

    assert calls == [], f"the subclass's own code ran: {calls}"
    assert result
    root = events[0]
    assert type(root["trace_id"]) is str
    assert type(root["parent_span_id"]) is str
    assert (root["trace_id"], root["parent_span_id"]) == (_TRACE, _SPAN)


def test_a_str_subclass_trace_id_is_adopted_as_an_exact_str(lf, fake_sink) -> None:
    value = str_probe(_TRACE, raising=_ADOPTION_RAISERS)

    with armed() as calls:
        result = lf.continue_trace(trace_id=value, parent_span_id=_SPAN)
        events = _root_events(lf, fake_sink)

    assert calls == [], f"the subclass's own code ran: {calls}"
    assert result
    root = events[0]
    assert type(root["trace_id"]) is str
    assert type(root["parent_span_id"]) is str
    assert (root["trace_id"], root["parent_span_id"]) == (_TRACE, _SPAN)


def test_a_str_subclass_parent_span_id_is_adopted_as_an_exact_str(
    lf, fake_sink, capsys: pytest.CaptureFixture[str]
) -> None:
    """`ok` alone cannot show it: an unnormalised parent is refused, joins as a root, and is `ok`."""
    value = str_probe(_SPAN, raising=_ADOPTION_RAISERS)

    with armed() as calls:
        result = lf.continue_trace(trace_id=_TRACE, parent_span_id=value)
        events = _root_events(lf, fake_sink)

    assert calls == [], f"the subclass's own code ran: {calls}"
    assert result
    assert capsys.readouterr().err == "", "a valid parent is not refused"
    assert type(events[0]["parent_span_id"]) is str
    assert events[0]["parent_span_id"] == _SPAN


def test_a_str_subclass_baggage_is_applied(lf) -> None:
    """The verdict describes the trace context only, so a valid baggage alone is not `ok`."""
    value = str_probe("k=v", raising=_ADOPTION_RAISERS)

    with armed() as calls:
        result = lf.continue_trace(baggage=value)

    assert calls == [], f"the subclass's own code ran: {calls}"
    assert lf.get_baggage() == {"k": "v"}
    assert not result
    assert result.reason == "nothing-supplied"


def test_an_adopted_str_subclass_cannot_break_current_traceparent(lf, fake_sink) -> None:
    """Probe G. A valid subclass is valid, so only storing the exact `str` stops it detonating.

    Armed through the span body, because that is where the stored subclass used to raise: from
    `format_traceparent`'s f-string, inside the public `current_traceparent()`.
    """
    raising = {"__format__", "__str__", "encode", "__repr__"}
    trace_id = str_probe(_TRACE, raising=raising)
    parent = str_probe(_SPAN, raising=raising)
    seen: dict[str, object] = {}

    @lf.trace
    def work() -> None:
        seen["traceparent"] = lf.current_traceparent()

    with armed() as calls:
        result = lf.continue_trace(trace_id=trace_id, parent_span_id=parent)
        work()

    assert calls == [], f"the subclass's own code ran: {calls}"
    assert result
    header = seen["traceparent"]
    assert type(header) is str
    assert re.fullmatch(rf"00-{_TRACE}-[0-9a-f]{{16}}-01", header), header
    assert type(fake_sink.batches[-1][0]["trace_id"]) is str


def test_a_valid_exact_str_context_is_adopted_as_before(lf, fake_sink) -> None:
    assert lf.continue_trace(_TRACEPARENT)
    root = _root_events(lf, fake_sink)[0]
    assert (root["trace_id"], root["parent_span_id"]) == (_TRACE, _SPAN)

    assert lf.continue_trace(trace_id=_TRACE, parent_span_id=_SPAN)
    root = _root_events(lf, fake_sink)[0]
    assert (root["trace_id"], root["parent_span_id"]) == (_TRACE, _SPAN)

    lf.continue_trace(baggage="team=core")
    assert lf.get_baggage() == {"team": "core"}


def test_nothing_supplied_is_silent(lf, capsys: pytest.CaptureFixture[str]) -> None:
    result = lf.continue_trace()

    assert result.reason == "nothing-supplied"
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize("kind", ["exact", "subclass"])
def test_a_long_inbound_str_echoes_its_first_64_characters(
    lf, capsys: pytest.CaptureFixture[str], kind: str
) -> None:
    value = "x" * 100 if kind == "exact" else str_probe("x" * 100, raising={"__repr__"})

    with armed():
        lf.continue_trace(traceparent=value)

    assert capsys.readouterr().err == (
        f"{_PREFIX} (unparseable traceparent): "
        "'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx…\n"
    )


class _Recorder:
    """Records what reached the sink, as `tests/test_span_sweep.py`'s `Recorder` does."""

    def __init__(self) -> None:
        self.got: list[dict[str, object]] = []
        self._lock = threading.Lock()

    def emit(self, batch: list[dict[str, object]]) -> None:
        with self._lock:
            self.got.extend(batch)

    def close(self) -> None:
        return None


def test_the_swept_span_refusal_runs_no_code_of_the_trace_id(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The fifth place a value meets library code: the refusal echoed `str(trace_id)`."""
    log_foundry.configure(service="t", sink=_Recorder())
    value = str_probe(_TRACE, raising={"__str__"})
    seen: dict[str, object] = {}

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("before-sweep")
        log_foundry.flush(timeout=5.0)
        with armed() as calls:
            seen["result"] = log_foundry.continue_trace(trace_id=value)
        seen["calls"] = list(calls)

    work()
    log_foundry.flush(timeout=5.0)

    result = seen["result"]
    lines = capsys.readouterr().err.splitlines()
    assert seen["calls"] == [], f"the subclass's own code ran: {seen['calls']}"
    assert not result
    assert result.reason == "rejected"
    assert len(lines) == 1, lines
    assert "the current span has already been flushed" in lines[0]


class _RaisingStream:
    """A `sys.stderr` whose `write` fails."""

    def __init__(self, exc: BaseException) -> None:
        self._exc = exc

    def write(self, text: str) -> int:
        raise self._exc

    def flush(self) -> None:
        return None


def test_a_stream_fault_still_reports_rejected(lf, monkeypatch: pytest.MonkeyPatch) -> None:
    """The verdict follows the announcement, not its delivery."""
    monkeypatch.setattr(sys, "stderr", _RaisingStream(OSError(32, "Broken pipe")))

    result = lf.continue_trace("00-bad")

    assert not result
    assert result.reason == "rejected"


def test_a_baseexception_from_the_write_propagates_out_of_continue_trace(
    lf, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "stderr", _RaisingStream(KeyboardInterrupt()))

    with pytest.raises(KeyboardInterrupt):
        lf.continue_trace("00-bad")
