"""A refusal no retry can cure costs one ``emit`` call, not ``max_retries + 1``.

A closed or closing shipped sink, and a forked child's ``SQLiteSink``, raise the error
``sinks/_refusal.final_refusal`` builds. The worker abandons such a batch at the attempt that
raised it, moving ``failed_batches`` once and writing the abandoned line with the attempts it
made, where before it resent the batch ``max_retries`` times with a backoff wait between, about
85 ms a batch. Any other raise keeps the resend: a third-party sink's ``SinkDeliveryError``, a
dual-typed driver error, and a shipped sink's final refusal that a sink of the caller's own caught
and re-raised. ``MultiSink`` re-raises a final refusal only when every child's raise was one.

``emit`` calls are counted through ``sys.monitoring`` rather than by wrapping ``emit``, because a
wrapper defined here would put a frame of this module into every refusal's traceback, and a
foreign frame there is exactly what turns a final refusal back into a resent one. Serves
invariants 2, 3, 6 and 7.
"""

from __future__ import annotations

import ast
import sqlite3
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry
import sink_rigs
from conftest import install_worker
from log_foundry.sinks import _engine
from log_foundry.sinks._driver_error import delivery_error
from log_foundry.sinks._engine import emit_via_deliver
from log_foundry.sinks._refusal import final_refusal, is_final_refusal
from log_foundry.sinks.base import SinkDeliveryError
from log_foundry.sinks.callback import CallbackSink
from log_foundry.sinks.file import FileSink
from log_foundry.sinks.memory import MemorySink
from log_foundry.sinks.multi import MultiSink
from log_foundry.sinks.nats import NATSSink, _LoopWedgedError
from log_foundry.sinks.transform import TransformSink
from log_foundry.worker import Worker
from test_sink_post_close import EMIT_ONLY_CLOSED, ENGINE_CLOSED, Built, _hold_a_section

if TYPE_CHECKING:
    import threading
    from collections.abc import Callable, Iterator
    from types import CodeType

LIMIT = 5.0
RETRIES = 3

SINKS = Path(__file__).resolve().parent.parent / "src" / "log_foundry" / "sinks"

EMIT_ONLY = (*sorted(EMIT_ONLY_CLOSED), "GooglePubSubSink")
"""Every shipped sink with no ``deliver`` that refuses after ``close()``."""


def _batch(n: int = 2) -> list[dict[str, object]]:
    return [sink_rigs.ok(f"e{i}") for i in range(n)]


@pytest.fixture
def calls_to() -> Iterator[Callable[[Callable[..., Any]], Callable[[], int]]]:
    """Counts the calls one function receives, from any thread, adding no frame to its stack.

    Yields a ``watch(function)`` that starts counting ``function``'s calls and returns a reader.
    """
    monitoring = sys.monitoring
    tool = next(t for t in range(6) if monitoring.get_tool(t) is None)
    monitoring.use_tool_id(tool, "test_final_refusal")
    counts: dict[CodeType, int] = {}

    def started(code: CodeType, offset: int) -> None:
        if code in counts:
            counts[code] += 1

    monitoring.register_callback(tool, monitoring.events.PY_START, started)

    def watch(function: Callable[..., Any]) -> Callable[[], int]:
        code = function.__code__
        counts[code] = 0
        monitoring.set_local_events(tool, code, monitoring.events.PY_START)
        return lambda: counts[code]

    try:
        yield watch
    finally:
        for code in counts:
            monitoring.set_local_events(tool, code, 0)
        monitoring.register_callback(tool, monitoring.events.PY_START, None)
        monitoring.free_tool_id(tool)


def _drain(sink: Any) -> tuple[bool, int]:
    """Runs one batch through a real ``Worker`` with ``max_retries=3``; flush verdict, failures."""
    worker = Worker(sink, batch_size=1000, flush_interval=60.0, max_retries=RETRIES)
    install_worker(worker)
    worker.submit(_batch(2))
    ok = bool(worker.flush(timeout=LIMIT))
    return ok, log_foundry.health().failed_batches


def _abandoned(err: str) -> list[str]:
    return [line for line in err.splitlines() if "batch abandoned after" in line]


def _no_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_engine, "cycle_backoff", lambda attempt: 0.0)


@pytest.mark.parametrize(
    ("name", "state"), [*((name, "closed") for name in EMIT_ONLY), ("SQLiteSink", "forked")]
)
def test_one_refused_batch_costs_one_emit_call_not_four(
    name: str,
    state: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    calls_to: Callable[[Callable[..., Any]], Callable[[], int]],
) -> None:
    """The regression: ``max_retries=3`` once cost 4 calls and 3 backoffs; now one call.

    ``forked`` applies to ``SQLiteSink`` alone, whose pid check runs before anything else in
    ``emit``; the pid is put back before teardown so the borrowed connection is closed as usual.
    """
    rig = sink_rigs.RIGS[name](monkeypatch, tmp_path)
    sink = rig.sink
    if state == "closed":
        sink.close()
    else:
        own_pid = sink._pid
        sink._pid = -1
    emits = calls_to(type(sink).emit)
    before = rig.calls()
    capsys.readouterr()
    try:
        ok, failed = _drain(sink)
    finally:
        if state == "forked":
            sink._pid = own_pid
    assert emits() == 1, f"{name} {state}: {emits()} emit call(s) for one refused batch"
    assert not ok, "a refused batch reported a flush success"
    assert failed == 1
    assert rig.calls() == before, f"{name} reached its driver while {state}"
    assert _abandoned(capsys.readouterr().err) == [
        "log-foundry: lost 2 event(s); batch abandoned after 1 emit attempts"
    ]


class _Raises:
    """A third-party sink whose ``emit`` raises a fixed exception every time."""

    def __init__(self, error: Exception) -> None:
        self.error = error

    def emit(self, batch: list[dict[str, object]]) -> None:
        raise self.error


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(
            SinkDeliveryError("ThirdPartySink delivered none: the sink is closed"), id="unmarked"
        ),
        pytest.param(
            delivery_error(sqlite3.OperationalError("database is locked")), id="dual-typed"
        ),
        pytest.param(ConnectionError("refused"), id="transient"),
        pytest.param(
            final_refusal(object(), "Borrowed: the sink is closed"), id="marked-but-foreign"
        ),
    ],
)
def test_any_other_raise_keeps_the_resend(
    error: Exception,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    calls_to: Callable[[Callable[..., Any]], Callable[[], int]],
) -> None:
    """The silent twin: what a shipped sink did not raise as final is resent, as in 1.x."""
    _no_backoff(monkeypatch)
    emits = calls_to(_Raises.emit)
    capsys.readouterr()
    ok, failed = _drain(_Raises(error))
    assert emits() == RETRIES + 1
    assert not ok and failed == 1
    assert _abandoned(capsys.readouterr().err) == [
        f"log-foundry: lost 2 event(s); batch abandoned after {RETRIES + 1} emit attempts"
    ]


class _Replacing:
    """A sink of the caller's own that swaps in a fresh ``FileSink`` when its inner one refuses."""

    def __init__(self, inner: FileSink, path: Path) -> None:
        self.inner = inner
        self.path = path
        self.replacements: list[FileSink] = []

    def emit(self, batch: list[dict[str, object]]) -> None:
        try:
            self.inner.emit(batch)
        except SinkDeliveryError:
            self.inner = FileSink(str(self.path))
            self.replacements.append(self.inner)
            raise

    def close(self) -> None:
        self.inner.close()


def test_a_callers_sink_re_raising_a_shipped_refusal_keeps_the_resend_it_arranged(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    calls_to: Callable[[Callable[..., Any]], Callable[[], int]],
) -> None:
    """The caller's frame is in the traceback, so the refusal is not final and the resend lands.

    The wrapper re-raises the closed ``FileSink``'s own error by identity, mark and all, after
    replacing it; the worker's second attempt reaches the fresh sink and is delivered.
    """
    _no_backoff(monkeypatch)
    closed = FileSink(str(tmp_path / "old.ndjson"))
    closed.close()
    wrapper = _Replacing(closed, tmp_path / "new.ndjson")
    emits = calls_to(_Replacing.emit)
    try:
        ok, failed = _drain(wrapper)
    finally:
        for sink in wrapper.replacements:
            sink.close()
    assert emits() == 2
    assert ok and failed == 0
    assert len((tmp_path / "new.ndjson").read_text().splitlines()) == 2


class _FailsOnce:
    """A stream whose first ``write`` raises ``EIO``, for a transient file failure."""

    def __init__(self) -> None:
        self.writes = 0

    def write(self, line: str) -> int:
        self.writes += 1
        raise OSError(5, "Input/output error")

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass


def test_a_refusal_after_a_transient_failure_stops_the_resend_where_it_lands(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    calls_to: Callable[[Callable[..., Any]], Callable[[], int]],
) -> None:
    """A write fails (retried), the sink closes in the backoff, and the close's refusal is final.

    The line counts the attempts actually made: two.
    """
    sink = FileSink(str(tmp_path / "f.ndjson"))
    sink._stream.close()
    sink._stream = _FailsOnce()

    def close_in_the_backoff(attempt: int) -> float:
        sink.close()
        return 0.0

    monkeypatch.setattr(_engine, "cycle_backoff", close_in_the_backoff)
    emits = calls_to(FileSink.emit)
    capsys.readouterr()
    ok, failed = _drain(sink)
    assert emits() == 2
    assert not ok and failed == 1
    assert _abandoned(capsys.readouterr().err) == [
        "log-foundry: lost 2 event(s); batch abandoned after 2 emit attempts"
    ]


@pytest.mark.parametrize("name", EMIT_ONLY)
def test_every_emit_only_closed_refusal_is_final_and_still_exactly_sink_delivery_error(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The mark changes nothing a caller sees: the type is exact and the text is today's."""
    rig = sink_rigs.RIGS[name](monkeypatch, tmp_path)
    rig.sink.close()
    with pytest.raises(SinkDeliveryError) as raised:
        rig.sink.emit(_batch(2))
    assert type(raised.value) is SinkDeliveryError
    assert is_final_refusal(raised.value, rig.sink)
    if name in EMIT_ONLY_CLOSED:
        assert str(raised.value) == EMIT_ONLY_CLOSED[name](2)


@pytest.mark.parametrize("name", ["KafkaSink", "GooglePubSubSink"])
def test_a_closed_buffering_sinks_flush_refusal_is_final(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The two buffering emit-only sinks refuse a flush after close the same way."""
    rig = sink_rigs.RIGS[name](monkeypatch, tmp_path)
    rig.sink.close()
    with pytest.raises(SinkDeliveryError) as raised:
        rig.sink.flush()
    assert is_final_refusal(raised.value, rig.sink)
    assert str(raised.value) == f"{name} cannot flush: the sink is closed"


def test_a_forked_sqlite_sinks_refusal_is_final(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The forked-child refusal, raised before the guard, carries the mark too."""
    rig = sink_rigs.RIGS["SQLiteSink"](monkeypatch, tmp_path)
    own_pid = rig.sink._pid
    rig.sink._pid = -1
    try:
        with pytest.raises(SinkDeliveryError) as raised:
            rig.sink.emit(_batch(1))
    finally:
        rig.sink._pid = own_pid
    assert is_final_refusal(raised.value, rig.sink)
    assert "a forked child acquired no connection" in str(raised.value)


@pytest.mark.parametrize("state", ["closed", "closing"])
@pytest.mark.parametrize("name", sorted(ENGINE_CLOSED))
def test_a_closed_or_closing_engine_sinks_emit_raises_a_final_refusal(
    name: str, state: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Through the inline adapter, as a ``MultiSink`` or third-party wrapper reaches it."""
    built = Built(name, monkeypatch, tmp_path)
    closer: threading.Thread | None = None
    leave: threading.Event | None = None
    if state == "closed":
        built.target.close()
    else:
        closer, leave = _hold_a_section(built)
    try:
        with pytest.raises(SinkDeliveryError) as raised:
            if name == "SocketTransport":
                built.outer.emit(_batch(1))
            else:
                emit_via_deliver(built.outer, _batch(1))
    finally:
        if leave is not None and closer is not None:
            leave.set()
            closer.join(LIMIT)
    assert type(raised.value) is SinkDeliveryError
    assert is_final_refusal(raised.value, built.outer)


def test_a_forked_nats_sinks_emit_raises_a_final_refusal(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """``NATSSink``'s forked-child refusal, the precedent for ``SQLiteSink``'s, is final too."""
    built = Built("NATSSink", monkeypatch, tmp_path)
    own_pid = built.target._pid
    built.target._pid = -1
    try:
        with pytest.raises(SinkDeliveryError) as raised:
            emit_via_deliver(built.target, _batch(1))
    finally:
        built.target._pid = own_pid
    assert is_final_refusal(raised.value, built.target)
    assert "a forked child acquired no connection" in str(raised.value)


@pytest.mark.parametrize("call", ["emit", "flush"])
def test_a_wedged_nats_loops_refusal_is_final(
    call: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The backstop closes the sink, so its refusal on either call is final."""
    built = Built("NATSSink", monkeypatch, tmp_path)
    sink: NATSSink = built.target

    def wedged(self: NATSSink, coroutine: Any, timeout: float) -> Any:
        coroutine.close()
        raise _LoopWedgedError("wedged")

    monkeypatch.setattr(NATSSink, "_run", wedged)
    with pytest.raises(SinkDeliveryError) as raised:
        if call == "emit":
            emit_via_deliver(sink, _batch(1))
        else:
            sink.flush()
    assert is_final_refusal(raised.value, sink)
    assert "the loop is wedged and the sink is now closed" in str(raised.value)


def test_a_fan_out_of_closed_children_costs_one_attempt(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    calls_to: Callable[[Callable[..., Any]], Callable[[], int]],
) -> None:
    """Every child refused for good, an engine child among them: one fan-out call."""
    file_rig = sink_rigs.RIGS["FileSink"](monkeypatch, tmp_path)
    engine = Built("ClickHouseSink", monkeypatch, tmp_path)
    file_rig.sink.close()
    engine.target.close()
    fan_out = MultiSink(file_rig.sink, engine.outer)
    emits = calls_to(MultiSink.emit)
    ok, failed = _drain(fan_out)
    assert emits() == 1
    assert not ok and failed == 1


@pytest.mark.parametrize("closed_first", [True, False], ids=["closed-first", "closed-last"])
def test_a_fan_out_with_one_curable_child_keeps_the_resend(
    closed_first: bool,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    calls_to: Callable[[Callable[..., Any]], Callable[[], int]],
) -> None:
    """A closed child must not stop the resend a sibling's transient failure is owed."""
    _no_backoff(monkeypatch)
    file_rig = sink_rigs.RIGS["FileSink"](monkeypatch, tmp_path)
    file_rig.sink.close()
    transient = ConnectionError("refused")
    children = [file_rig.sink, _Raises(transient)]
    fan_out = MultiSink(*(children if closed_first else children[::-1]))
    with pytest.raises(ConnectionError) as raised:
        fan_out.emit(_batch(1))
    assert raised.value is transient
    emits = calls_to(MultiSink.emit)
    ok, failed = _drain(fan_out)
    assert emits() == RETRIES + 1
    assert not ok and failed == 1


def test_a_fan_out_of_final_refusals_raises_the_first_childs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """When every raise is final the first child's is re-raised, as before."""
    first = sink_rigs.RIGS["FileSink"](monkeypatch, tmp_path).sink
    (tmp_path / "second").mkdir()
    second = sink_rigs.RIGS["RotatingFileSink"](monkeypatch, tmp_path / "second").sink
    first.close()
    second.close()
    fan_out = MultiSink(first, second)
    with pytest.raises(SinkDeliveryError) as raised:
        fan_out.emit(_batch(1))
    assert str(raised.value) == EMIT_ONLY_CLOSED["FileSink"](1)
    assert is_final_refusal(raised.value, fan_out)


class _Hostile(Exception):
    """An exception whose every attribute read raises."""

    def __getattribute__(self, name: str) -> Any:
        raise RuntimeError(name)


def _raise(error: Exception) -> None:
    raise error


def _caught(error: Exception) -> Exception:
    """Raises ``error`` through a frame of this module and returns it as its catcher holds it."""
    try:
        _raise(error)
    except Exception as err:
        return err
    raise AssertionError("not raised")


def test_the_mark_is_read_only_from_an_exact_marked_sink_delivery_error() -> None:
    """Total, and narrow: a subclass, an unmarked instance and a hostile class are not final."""
    refuser = MemorySink()
    marked = final_refusal(refuser, "X: the sink is closed")
    assert type(marked) is SinkDeliveryError and str(marked) == "X: the sink is closed"
    assert is_final_refusal(marked, refuser), "an error never raised has no foreign frame"
    assert is_final_refusal(marked, MultiSink(MemorySink(), refuser))
    assert not is_final_refusal(marked, MemorySink()), "a refuser outside the called graph"

    class Sub(SinkDeliveryError):
        pass

    sub = Sub("x")
    vars(sub).update(vars(marked))
    assert not is_final_refusal(sub, refuser)
    assert not is_final_refusal(SinkDeliveryError("X: the sink is closed"), refuser)
    assert not is_final_refusal(_Hostile(), refuser)
    assert not is_final_refusal(delivery_error(sqlite3.OperationalError("x")), refuser)


def test_a_foreign_frame_below_the_catcher_makes_a_marked_error_resent() -> None:
    """The catcher's own frame is skipped; any other frame outside the package is not."""
    refuser = MemorySink()
    assert not is_final_refusal(_caught(final_refusal(refuser, "X: closed")), refuser)
    try:
        raise final_refusal(refuser, "X: the sink is closed")
    except SinkDeliveryError as err:
        assert is_final_refusal(err, refuser), "the catcher's own frame counted as a carrier"


class _Proxy:
    """Forwards every attribute to the current inner sink: ``emit`` leaves no frame of its own."""

    def __init__(self, inner: FileSink) -> None:
        self.inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)


class _Rebinding:
    """Holds the inner sink's bound ``emit`` as its own, rebinding it on a swap."""

    def __init__(self, inner: FileSink) -> None:
        self.inner = inner
        self.emit = inner.emit

    def swap(self, inner: FileSink) -> None:
        self.inner = inner
        self.emit = inner.emit

    def close(self) -> None:
        self.inner.close()


class _Property:
    """Answers ``emit`` through a property that returns the current inner sink's bound method."""

    def __init__(self, inner: FileSink) -> None:
        self.inner = inner

    @property
    def emit(self) -> Callable[[list[dict[str, object]]], None]:
        return self.inner.emit

    def close(self) -> None:
        self.inner.close()


class _Reopening(FileSink):
    """A ``FileSink`` of the caller's own that re-initialises itself, inheriting ``emit``."""

    def reopen(self, path: str) -> None:
        self.close()
        FileSink.__init__(self, path)


class _Router:
    """A ``CallbackSink`` callable that dispatches through a property, so it leaves no frame."""

    def __init__(self, inner: FileSink) -> None:
        self.inner = inner

    @property
    def __call__(self) -> Callable[[list[dict[str, object]]], None]:  # type: ignore[override]
        return self.inner.emit


class _Spoofed:
    """A caller's class that declares this package's module and routes ``emit`` by property."""

    __module__ = "log_foundry.sinks.file"

    def __init__(self, inner: FileSink) -> None:
        self.inner = inner

    @property
    def emit(self) -> Callable[[list[dict[str, object]]], None]:
        return self.inner.emit

    def close(self) -> None:
        self.inner.close()


class _EmitKey(str):
    """A ``str`` subclass equal to ``"emit"``, planting an instance ``emit`` a plain read skips."""

    __slots__ = ()

    def __eq__(self, other: object) -> bool:
        return str.__eq__(self, other)

    def __hash__(self) -> int:
        return str.__hash__(self)


SHAPES = [
    "getattr",
    "rebound",
    "property",
    "transform-over-getattr",
    "subclass",
    "callback-property",
    "module-spoof",
    "class-patched",
    "str-subclass-key",
]
"""Every frameless route a review found from a caller's object to a closed shipped sink."""


@pytest.mark.parametrize("shape", SHAPES)
def test_a_callers_sink_that_routes_emit_without_a_frame_keeps_the_resend(
    shape: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The receiver half: a caller's object chose where ``emit`` went, so the refusal is resent.

    The routed-to ``FileSink`` is closed; the first backoff swaps a fresh one in, as a caller's
    self-healing sink would, and the worker's second attempt delivers both events. On 1.x every
    shape delivered this way.
    """
    old, new = tmp_path / "old.ndjson", tmp_path / "new.ndjson"
    closed = _Reopening(str(old)) if shape == "subclass" else FileSink(str(old))
    closed.close()
    opened: list[FileSink] = []
    state: dict[str, Any] = {"target": closed, "swapped": False}
    sink: Any
    if shape == "subclass":
        sink = closed
    elif shape == "rebound":
        sink = _Rebinding(closed)
    elif shape == "property":
        sink = _Property(closed)
    elif shape == "callback-property":
        router = _Router(closed)
        sink = TransformSink(CallbackSink(router), lambda event: event)
    elif shape == "module-spoof":
        sink = _Spoofed(closed)
    elif shape == "class-patched":
        original = FileSink.emit
        sink = FileSink(str(tmp_path / "unused.ndjson"))
        opened.append(sink)
        monkeypatch.setattr(
            FileSink, "emit", property(lambda self: original.__get__(state["target"]))
        )
    elif shape == "str-subclass-key":
        sink = MemorySink()
        vars(sink)[_EmitKey("emit")] = closed.emit
    else:
        sink = _Proxy(closed)
    if shape == "transform-over-getattr":
        sink = TransformSink(sink, lambda event: event)

    def swap_in_the_backoff(attempt: int) -> float:
        if state["swapped"]:
            return 0.0
        state["swapped"] = True
        if shape == "subclass":
            closed.reopen(str(new))
            state["target"] = closed
            opened.append(closed)
            return 0.0
        fresh = FileSink(str(new))
        opened.append(fresh)
        state["target"] = fresh
        if shape == "rebound":
            sink.swap(fresh)
        elif shape == "callback-property":
            router.inner = fresh
        elif shape == "str-subclass-key":
            vars(sink)[_EmitKey("emit")] = fresh.emit
        elif shape == "transform-over-getattr":
            sink._inner.inner = fresh
        elif shape != "class-patched":
            sink.inner = fresh
        return 0.0

    monkeypatch.setattr(_engine, "cycle_backoff", swap_in_the_backoff)
    try:
        ok, failed = _drain(sink)
    finally:
        monkeypatch.undo()
        for each in opened:
            each.close()
    assert ok and failed == 0, f"{shape}: the resend the caller arranged was not made"
    assert len(new.read_text().splitlines()) == 2


def test_package_graph_reads_the_graph_without_running_it(tmp_path: Path) -> None:
    """The receiver test's own corpus: package graphs list their sinks; anything foreign is None."""
    from log_foundry._sink_arg import package_graph
    from log_foundry.sinks.syslog import SyslogSink

    plain = FileSink(str(tmp_path / "a.ndjson"))
    memory = MemorySink()
    try:
        graph = package_graph(plain)
        assert graph is not None and id(plain) in graph
        graph = package_graph(TransformSink(MultiSink(plain, memory), lambda event: event))
        assert graph is not None and {id(plain), id(memory)} <= graph
        syslog = SyslogSink("localhost", 514, transport="udp")
        graph = package_graph(syslog)
        assert graph is not None and id(syslog._socket) in graph, "a held transport is a refuser"
        syslog.close()
        callback = CallbackSink(plain.emit)
        graph = package_graph(callback)
        assert graph is not None and id(plain) not in graph, "a callable's target is not forwarded to"
        assert package_graph(_Proxy(plain)) is None
        assert package_graph(MultiSink(memory, _Proxy(plain))) is None
        assert package_graph(TransformSink(_Proxy(plain), lambda event: event)) is None
        assert package_graph(_Spoofed(plain)) is None
        subclass = _Reopening(str(tmp_path / "b.ndjson"))
        assert package_graph(subclass) is None
        subclass.close()
        patched = MemorySink()
        patched.emit = plain.emit  # type: ignore[method-assign]
        assert package_graph(patched) is None
        assert package_graph(MultiSink(patched)) is None
        keyed = MemorySink()
        vars(keyed)[_EmitKey("other")] = None
        assert package_graph(keyed) is None, "a key that is not exactly str is doubt"
    finally:
        plain.close()


def section_problems(source: str) -> list[str]:
    """Every closed-guard section in ``source`` whose refusal is not a ``final_refusal``.

    A section is ``with <...>.enter() as <name>:``; its refusal is the body of the
    ``if not <name>:`` inside it, which must call ``final_refusal`` and must not build a
    ``SinkDeliveryError`` itself. A section with no such ``if`` is reported too, since it would
    refuse nothing at all.
    """
    problems: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.With):
            continue
        for item in node.items:
            call = item.context_expr
            if not (
                isinstance(call, ast.Call)
                and isinstance(call.func, ast.Attribute)
                and call.func.attr == "enter"
                and isinstance(item.optional_vars, ast.Name)
            ):
                continue
            name = item.optional_vars.id
            refusals = [
                inner
                for inner in ast.walk(node)
                if isinstance(inner, ast.If)
                and isinstance(inner.test, ast.UnaryOp)
                and isinstance(inner.test.op, ast.Not)
                and isinstance(inner.test.operand, ast.Name)
                and inner.test.operand.id == name
            ]
            if not refusals:
                problems.append(f"line {node.lineno}: section {name!r} has no `if not {name}:`")
            for refusal in refusals:
                called = {
                    c.func.id
                    for stmt in refusal.body
                    for c in ast.walk(stmt)
                    if isinstance(c, ast.Call) and isinstance(c.func, ast.Name)
                }
                if "SinkDeliveryError" in called:
                    problems.append(
                        f"line {refusal.lineno}: builds SinkDeliveryError, not final_refusal"
                    )
                elif "final_refusal" not in called:
                    problems.append(f"line {refusal.lineno}: refuses without final_refusal")
    return problems


SECTION_CORPUS = [
    (
        (
            "with self._guard.enter() as section:\n"
            "    if not section:\n"
            "        raise final_refusal('X: the sink is closed')\n"
        ),
        [],
    ),
    (
        (
            "with self._guard.enter() as section:\n"
            "    if not section:\n"
            "        run_context().refuse(final_refusal('X: the sink is closed'))\n"
            "        return None\n"
        ),
        [],
    ),
    (
        (
            "with self._guard.enter() as section:\n"
            "    if not section:\n"
            "        raise SinkDeliveryError('X: the sink is closed')\n"
        ),
        ["line 2: builds SinkDeliveryError, not final_refusal"],
    ),
    (
        (
            "with self._guard.enter() as section:\n"
            "    if not section:\n"
            "        return None\n"
        ),
        ["line 2: refuses without final_refusal"],
    ),
    (
        (
            "with self._guard.enter() as admitted:\n"
            "    send()\n"
        ),
        ["line 1: section 'admitted' has no `if not admitted:`"],
    ),
    (
        (
            "with open(path) as handle:\n"
            "    if not handle:\n"
            "        raise SinkDeliveryError('x')\n"
        ),
        [],
    ),
]
"""The checker's own corpus: two compliant shapes, each failure reason, and a silent case."""


@pytest.mark.parametrize(("source", "expected"), SECTION_CORPUS)
def test_the_section_checker_reports_each_reason(source: str, expected: list[str]) -> None:
    assert section_problems(source) == expected


def test_every_closed_guard_section_refuses_with_a_final_refusal() -> None:
    """Derived from the tree, with a floor: the twelve sections at the time of writing."""
    sections = 0
    problems: list[str] = []
    for path in sorted(SINKS.glob("*.py")):
        source = path.read_text()
        sections += source.count(".enter() as ")
        problems += [f"{path.name}: {p}" for p in section_problems(source)]
    assert sections >= 12, f"only {sections} closed-guard section(s) found; the roster went quiet"
    assert problems == []
