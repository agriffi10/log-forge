"""SPEC-058 FR-002 and FR-003 — caller code that fails for one event costs only that event.

``TransformSink``'s function and ``FilteringSink``'s predicate run once per event, and a raise
from either used to cost the whole batch: the siblings were never forwarded and the worker's
abandon line reported them lost. Now one failure costs one event, under two rules:

* **The ledger rule.** If every event of the batch failed — nothing forwarded and nothing
  deliberately declined — the first exception is re-raised unchanged and nothing is counted, so
  the catcher above (the worker, ``orphan_lost``, or ``MultiSink``) owns the batch. Otherwise the
  failures are set aside and the rest is forwarded.
* **The deferral rule.** The failures are counted, in the wrapper's own ``losses()``, and announced
  once per batch, only after the inner ``emit`` returned — a raise there is retried whole, and
  counting first would count the same event once per attempt.

Also here, because they need a wrapper that defers: FR-001's one-of-several hostile mapping, and
FR-007's models (b) and (c) and its third accepted cost.
"""

from __future__ import annotations

import io
import threading
from typing import TYPE_CHECKING

import pytest

import log_foundry
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.filtering import FilteringSink
from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.memory import MemorySink
from log_foundry.sinks.multi import MultiSink
from log_foundry.sinks.stdout import StdoutSink
from log_foundry.sinks.transform import TransformSink
from log_foundry.worker import Worker
from test_multi_absorbed import ParkingCounter, SpyLock
from test_sinks_http import FakeOpener, FakeResponse

if TYPE_CHECKING:
    from collections.abc import Callable

SENTINEL = "s3cr3t-sentinel"

KINDS = ["transform", "filtering"]


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Neutralize the HTTP backoff, so the fan-out models through ``HTTPSink`` run instantly."""
    monkeypatch.setattr("log_foundry.sinks.http.wait", lambda _delay, _stop=None: None)


class Recording:
    """An inner sink that records every call and reports nothing."""

    def __init__(self) -> None:
        self.calls: list[list[dict[str, object]]] = []

    def emit(self, batch: list[dict[str, object]]) -> None:
        self.calls.append(list(batch))

    def close(self) -> None:
        pass


class Reporting(Recording):
    """An inner sink that records every call and reports a fixed loss."""

    def __init__(self, dropped: int, failed: int) -> None:
        super().__init__()
        self._losses = SinkLosses(dropped=dropped, failed=failed)

    def losses(self) -> SinkLosses:
        return self._losses


class Down(Recording):
    """An inner sink that records every call, raises on each, and reports nothing."""

    def emit(self, batch: list[dict[str, object]]) -> None:
        super().emit(batch)
        raise SinkDeliveryError("inner down")


class DownOnce(Recording):
    """An inner sink whose first ``emit`` raises and every later one delivers."""

    def emit(self, batch: list[dict[str, object]]) -> None:
        super().emit(batch)
        if len(self.calls) == 1:
            raise SinkDeliveryError("first attempt")


def _ev(i: int, level: str = "INFO", **extra: object) -> dict[str, object]:
    return {"i": i, "level": level, "message": "m", **extra}


def _wrapper(
    kind: str,
    inner: object,
    *,
    fails: Callable[[dict[str, object]], bool] = lambda event: False,
    declines: Callable[[dict[str, object]], bool] = lambda event: False,
    raised: list[BaseException] | None = None,
    error: type[BaseException] = ValueError,
    message: str = "callable failed",
    min_level: str | None = None,
):
    """One of the two wrappers around ``inner``, whose callable fails or declines by predicate."""

    def decide(event: dict[str, object]) -> bool:
        if fails(event):
            exc = error(message)
            if raised is not None:
                raised.append(exc)
            raise exc
        return not declines(event)

    if kind == "transform":
        return TransformSink(inner, lambda event: event if decide(event) else None)  # type: ignore[arg-type]
    return FilteringSink(inner, predicate=decide, min_level=min_level)  # type: ignore[arg-type]


def _lost_lines(err: str) -> list[str]:
    return [line for line in err.splitlines() if " lost " in line]


# --- FR-002 AC: one failure through the worker, and its twin for the predicate -------------


def test_a_transform_failing_for_one_event_costs_only_that_event(capsys) -> None:
    """Ten spans through a ``StdoutSink``: 29 lines, one counted drop, one line naming the type."""
    stream = io.StringIO()

    def fn(event: dict[str, object]) -> dict[str, object]:
        fields = event.get("fields")
        if isinstance(fields, dict) and fields.get("i") == 5:
            raise ValueError(SENTINEL)
        return event

    log_foundry.configure(service="t", sink=TransformSink(StdoutSink(stream=stream), fn))

    @log_foundry.trace
    def work(i: int) -> None:
        log_foundry.info("hello", i=i)

    for i in range(10):
        work(i)
    assert log_foundry.flush().ok

    lines = stream.getvalue().splitlines()
    assert len(lines) == 29
    assert len(set(lines)) == 29, "each distinct event once"
    health = log_foundry.health()
    assert health.failed_batches == 0
    assert health.sink == SinkLosses(dropped=1, failed=0)
    assert _lost_lines(capsys.readouterr().err) == [
        "log-foundry: lost 1 event(s); TransformSink, the function raised ValueError"
    ]


def test_a_predicate_failing_for_one_event_costs_only_that_event(capsys) -> None:
    inner = Recording()
    sink = _wrapper("filtering", inner, fails=lambda event: event["i"] == 2)

    sink.emit([_ev(0), _ev(1), _ev(2), _ev(3)])

    assert inner.calls == [[_ev(0), _ev(1), _ev(3)]], "the others forwarded, in order"
    assert sink.losses() == SinkLosses(dropped=1, failed=0)
    assert _lost_lines(capsys.readouterr().err) == [
        "log-foundry: lost 1 event(s); FilteringSink, the predicate raised ValueError"
    ]


def test_a_below_floor_event_whose_predicate_raises_is_a_failure(capsys) -> None:
    """The predicate runs before the level floor, so a raise there is counted, not declined."""
    inner = Recording()
    sink = _wrapper("filtering", inner, fails=lambda event: event["i"] == 0, min_level="warning")

    sink.emit([_ev(0, "DEBUG"), _ev(1, "ERROR")])

    assert inner.calls == [[_ev(1, "ERROR")]]
    assert sink.losses() == SinkLosses(dropped=1, failed=0)
    assert len(_lost_lines(capsys.readouterr().err)) == 1


# --- FR-002 AC: the ledger rule's two halves ------------------------------------------------


@pytest.mark.parametrize("kind", KINDS)
def test_a_callable_failing_for_every_event_reraises_its_own_exception(kind: str, capsys) -> None:
    """Every event failed: the callable's own first exception, nothing counted, the worker owns it."""
    inner = Recording()
    raised: list[BaseException] = []
    sink = _wrapper(kind, inner, fails=lambda event: True, raised=raised)

    with pytest.raises(ValueError) as caught:
        sink.emit([_ev(0), _ev(1)])

    assert caught.value is raised[0], "the first failure's own exception, by identity"
    assert inner.calls == []
    assert sink.losses() is None
    assert capsys.readouterr().err == "", "the wrapper writes no line"

    worker = Worker(sink, batch_size=1, max_retries=0)
    try:
        worker.submit([_ev(0), _ev(1), _ev(2)])
        assert not worker.flush(timeout=5.0)
        assert worker.health().failed_batches == 1
    finally:
        worker.stop()
    assert sink.losses() is None
    assert _lost_lines(capsys.readouterr().err) == [
        "log-foundry: lost 3 event(s); batch abandoned after 1 emit attempts"
    ]


@pytest.mark.parametrize("shape", ["none", "false", "floor"])
def test_one_failure_and_the_rest_declined_settles_without_the_inner(shape: str, capsys) -> None:
    """A failure beside deliberate drops is not the all-failed case: counted once, no raise."""
    inner = Recording()
    if shape == "none":
        sink = _wrapper(
            "transform", inner, fails=lambda e: e["i"] == 0, declines=lambda e: e["i"] > 0
        )
        batch = [_ev(0), _ev(1), _ev(2)]
    elif shape == "false":
        sink = _wrapper(
            "filtering", inner, fails=lambda e: e["i"] == 0, declines=lambda e: e["i"] > 0
        )
        batch = [_ev(0), _ev(1), _ev(2)]
    else:
        sink = _wrapper("filtering", inner, fails=lambda e: e["i"] == 0, min_level="error")
        batch = [_ev(0, "ERROR"), _ev(1, "DEBUG"), _ev(2, "INFO")]

    sink.emit(batch)

    assert inner.calls == [], "everything else was declined, so the inner sink is not called"
    assert sink.losses() == SinkLosses(dropped=1, failed=0)
    assert len(_lost_lines(capsys.readouterr().err)) == 1


# --- FR-002 AC: retries ---------------------------------------------------------------------


@pytest.mark.parametrize("kind", KINDS)
def test_a_retried_batch_counts_the_failure_only_on_the_attempt_that_returned(
    kind: str, capsys
) -> None:
    """The worker's retry, by hand: attempt 1 raises at the inner sink, attempt 2 delivers."""
    inner = DownOnce()
    sink = _wrapper(kind, inner, fails=lambda event: event["i"] == 1)
    batch = [_ev(0), _ev(1), _ev(2)]

    with pytest.raises(SinkDeliveryError):
        sink.emit(batch)
    assert sink.losses() is None, "a raising emit counts nothing"
    assert capsys.readouterr().err == "", "and announces nothing"

    sink.emit(batch)
    assert sink.losses() == SinkLosses(dropped=1, failed=0), "counted once, on the return"
    assert len(_lost_lines(capsys.readouterr().err)) == 1
    assert inner.calls == [[_ev(0), _ev(2)], [_ev(0), _ev(2)]]


@pytest.mark.parametrize("kind", KINDS)
def test_an_abandoned_batch_leaves_the_wrapper_count_at_zero(kind: str, capsys) -> None:
    sink = _wrapper(kind, Down(), fails=lambda event: event["i"] == 1)
    worker = Worker(sink, batch_size=1, max_retries=3)
    try:
        worker.submit([_ev(0), _ev(1), _ev(2)])
        assert not worker.flush(timeout=5.0)
        assert worker.health().failed_batches == 1
    finally:
        worker.stop()

    assert sink.losses() is None
    assert _lost_lines(capsys.readouterr().err) == [
        "log-foundry: lost 3 event(s); batch abandoned after 4 emit attempts"
    ], "the worker's abandon line is the only count of the batch"


# --- FR-002 AC: a non-dict result, and what reaches stderr ----------------------------------


def test_a_non_dict_result_is_a_failure_named_by_type(capsys) -> None:
    inner = Recording()
    sink = TransformSink(inner, lambda event: f"text {SENTINEL}" if event["i"] == 1 else event)  # type: ignore[arg-type,return-value]

    sink.emit([_ev(0), _ev(1), _ev(2)])

    assert inner.calls == [[_ev(0), _ev(2)]]
    assert sink.losses() == SinkLosses(dropped=1, failed=0)
    err = capsys.readouterr().err
    assert SENTINEL not in err
    assert _lost_lines(err) == [
        "log-foundry: lost 1 event(s); TransformSink, the function raised TypeError"
    ]

    every = TransformSink(Recording(), lambda event: f"text {SENTINEL}")  # type: ignore[arg-type,return-value]
    with pytest.raises(TypeError) as caught:
        every.emit([_ev(0), _ev(1)])
    assert "TransformSink" in str(caught.value) and "str" in str(caught.value)
    assert SENTINEL not in str(caught.value), "the type is named, never the value"
    assert every.losses() is None
    assert every._inner.calls == []  # type: ignore[attr-defined]


@pytest.mark.parametrize("kind", KINDS)
def test_no_sentinel_reaches_stderr(kind: str, capsys) -> None:
    sink = _wrapper(kind, Recording(), fails=lambda e: e["i"] == 0, message=f"bad {SENTINEL}")

    sink.emit([_ev(0, secret=SENTINEL), _ev(1, secret=SENTINEL)])

    err = capsys.readouterr().err
    assert SENTINEL not in err
    assert "ValueError" in err


@pytest.mark.parametrize("kind", KINDS)
def test_an_empty_batch_calls_nothing(kind: str) -> None:
    inner = Recording()
    calls: list[object] = []
    sink = _wrapper(kind, inner, fails=lambda event: calls.append(event) or False)

    sink.emit([])

    assert calls == [] and inner.calls == []
    assert sink.losses() is None


@pytest.mark.parametrize("kind", KINDS)
def test_a_keyboard_interrupt_propagates_uncounted(kind: str, capsys) -> None:
    sink = _wrapper(kind, Recording(), fails=lambda e: e["i"] == 0, error=KeyboardInterrupt)

    with pytest.raises(KeyboardInterrupt):
        sink.emit([_ev(0), _ev(1)])

    assert sink.losses() is None
    assert capsys.readouterr().err == ""


# --- FR-002 AC: concurrency, the orphan path, and a fan-out --------------------------------


class _Parks(Recording):
    """Parks a batch carrying ``park`` until released, then raises; delivers every other batch."""

    def __init__(self) -> None:
        super().__init__()
        self.parked = threading.Event()
        self.release = threading.Event()

    def emit(self, batch: list[dict[str, object]]) -> None:
        if any(event.get("park") for event in batch):
            self.parked.set()
            self.release.wait(5.0)
            raise SinkDeliveryError("A's inner raised")
        super().emit(batch)


def test_two_concurrent_emits_keep_separate_pending_drops() -> None:
    inner = _Parks()
    sink = _wrapper("transform", inner, fails=lambda event: event["i"] < 0)
    a_errors: list[BaseException] = []

    def call_a() -> None:
        try:
            sink.emit([_ev(-1), _ev(1, park=True)])
        except SinkDeliveryError as exc:
            a_errors.append(exc)

    a = threading.Thread(target=call_a)
    a.start()
    try:
        assert inner.parked.wait(5.0), "the premise: A is parked inside its inner emit"
        sink.emit([_ev(-2), _ev(2)])
        assert sink.losses() == SinkLosses(dropped=1, failed=0), "B's one, not A's as well"
    finally:
        inner.release.set()
        a.join(5.0)

    assert len(a_errors) == 1, "A's inner raised"
    assert sink.losses() == SinkLosses(dropped=1, failed=0), "A raised, so A counted nothing"


@pytest.mark.parametrize("kind", KINDS)
def test_an_orphan_failure_is_absorbed_once(kind: str, capsys) -> None:
    """Outside a span the batch is one event, so a failure is the all-failed case."""
    sink = _wrapper(kind, MemorySink(), fails=lambda event: True)
    log_foundry.configure(service="t", sink=sink)

    log_foundry.info("outside a span")

    assert log_foundry.health().orphan_lost == 1
    assert sink.losses() is None
    assert "Sink, the" not in capsys.readouterr().err, "no line from the wrapper"


def test_under_a_fan_out_the_wrapper_and_the_aggregate_never_share_an_event() -> None:
    """A wrapper whose inner raised counts nothing and the fan-out absorbs all four; else 1 and 0."""
    raising = TransformSink(Down(), lambda e: _fail_for(e, 0))
    multi = MultiSink(raising, MemorySink())
    multi.emit([_ev(0), _ev(1), _ev(2), _ev(3)])
    assert raising.losses() is None
    assert multi.losses() == SinkLosses(dropped=0, failed=4)

    delivering = TransformSink(Recording(), lambda e: _fail_for(e, 0))
    multi = MultiSink(delivering, MemorySink())
    multi.emit([_ev(0), _ev(1), _ev(2), _ev(3)])
    assert delivering.losses() == SinkLosses(dropped=1, failed=0)
    assert multi.losses() == SinkLosses(dropped=1, failed=0)


def _fail_for(event: dict[str, object], i: int) -> dict[str, object]:
    if event["i"] == i:
        raise ValueError("fails for one")
    return event


# --- FR-001 AC: a hostile mapping as one event of several ----------------------------------


class _HostileItems(dict):
    def items(self):  # type: ignore[override]
        raise KeyError("hostile")


def test_a_hostile_mapping_among_several_is_dropped_and_named_by_its_own_type(capsys) -> None:
    inner = Recording()
    sink = TransformSink(inner, lambda e: _HostileItems(e) if e["i"] == 1 else e)

    sink.emit([_ev(0), _ev(1), _ev(2)])

    assert inner.calls == [[_ev(0), _ev(2)]]
    assert sink.losses() == SinkLosses(dropped=1, failed=0)
    assert _lost_lines(capsys.readouterr().err) == [
        "log-foundry: lost 1 event(s); TransformSink, the function raised KeyError"
    ]


# --- FR-003 AC: each wrapper reports its own drops ------------------------------------------


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize(
    ("inner_losses", "own", "expected"),
    [
        (None, 0, None),
        (None, 2, SinkLosses(dropped=2, failed=0)),
        ((3, 4), 2, SinkLosses(dropped=5, failed=4)),
        ((0, 0), 0, SinkLosses(dropped=0, failed=0)),
    ],
    ids=["none-0", "none-n", "ab-n", "zero-0"],
)
def test_wrapper_losses_shape(kind: str, inner_losses, own: int, expected) -> None:
    inner = Recording() if inner_losses is None else Reporting(*inner_losses)
    sink = _wrapper(kind, inner, fails=lambda event: event["i"] < 0)
    for n in range(own):
        sink.emit([_ev(-1 - n), _ev(n)])

    assert sink.losses() == expected


@pytest.mark.parametrize("shape", ["none", "false", "floor"])
def test_a_declined_event_never_moves_the_count(shape: str) -> None:
    if shape == "none":
        sink = _wrapper("transform", Recording(), declines=lambda event: True)
    elif shape == "false":
        sink = _wrapper("filtering", Recording(), declines=lambda event: True)
    else:
        sink = FilteringSink(Recording(), min_level="critical")

    for _ in range(3):
        sink.emit([_ev(0, "DEBUG"), _ev(1, "INFO"), _ev(2, "ERROR")])

    assert sink.losses() is None


def test_health_and_a_fan_out_carry_the_wrapper_count_once() -> None:
    """Through the worker ``health().sink`` is the wrapper's sum; a fan-out adds it once."""

    def fn(event: dict[str, object]) -> dict[str, object]:
        if event.get("message") == "bad":
            raise ValueError("fails for one")
        return event

    wrapper = TransformSink(Reporting(0, 0), fn)
    log_foundry.configure(service="t", sink=wrapper)

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("bad")
        log_foundry.info("good")

    work()
    assert log_foundry.flush().ok
    assert log_foundry.health().sink == SinkLosses(dropped=1, failed=0)

    sibling = Reporting(dropped=5, failed=2)
    multi = MultiSink(wrapper, sibling)
    assert multi.losses() == SinkLosses(dropped=1 + 5, failed=0 + 2)


def test_nested_wrappers_each_count_their_own() -> None:
    """``TransformSink(FilteringSink(inner))``: each counts its own failure, the outer the sum."""
    inner_wrapper = _wrapper("filtering", Recording(), fails=lambda e: e["i"] == 1)
    outer = TransformSink(inner_wrapper, lambda e: _fail_for(e, 0))

    outer.emit([_ev(0), _ev(1), _ev(2)])

    assert inner_wrapper.losses() == SinkLosses(dropped=1, failed=0)
    assert outer.losses() == SinkLosses(dropped=2, failed=0)


def test_a_parked_commit_loses_no_increment(monkeypatch: pytest.MonkeyPatch) -> None:
    """A second commit while the first is parked between its read and its write: both land."""
    counter = ParkingCounter("_dropped")
    monkeypatch.setattr(TransformSink, "_dropped", counter, raising=False)
    sink = TransformSink(MemorySink(), lambda e: _fail_for(e, 0))
    sink._counter_lock = SpyLock()  # type: ignore[assignment]

    counter.armed = True
    a = threading.Thread(target=sink.emit, args=([_ev(0), _ev(1)],))
    a.start()
    assert counter.parked.wait(5.0), "the premise: A reached its commit and parked there"
    b = threading.Thread(target=sink.emit, args=([_ev(0), _ev(2)],))
    b.start()
    for _ in range(500):
        if sink._counter_lock.contended.is_set() or not b.is_alive():  # type: ignore[attr-defined]
            break
        b.join(0.01)
    counter.release.set()
    a.join(5.0)
    b.join(5.0)

    assert not a.is_alive() and not b.is_alive()
    assert sink._dropped == 2, "both commits landed"


class _BreaksAfter(io.StringIO):
    """A stream that accepts ``writes`` writes and raises on every one after."""

    def __init__(self, writes: int) -> None:
        super().__init__()
        self.left = writes

    def write(self, text: str) -> int:
        if self.left <= 0:
            raise OSError("the stream broke")
        self.left -= 1
        return super().write(text)


def test_model_b_a_wrapper_turning_reporting_loses_nothing() -> None:
    """FR-003 and FR-007's model (b), over four direct three-event ``MultiSink.emit`` calls.

    The first batch: ``fn`` fails for its first event and the stream takes the other two, so the
    wrapper counts one and becomes a reporting child. Every later batch: the stream raises, the
    wrapper raises having counted nothing, and the fan-out absorbs all three. Under SPEC-036's
    silent-only rule the nine would be counted nowhere, reading ``(1, 0)``.
    """
    stream = _BreaksAfter(2)
    wrapper = TransformSink(StdoutSink(stream=stream), lambda e: _fail_for(e, 0))
    multi = MultiSink(wrapper, MemorySink())

    for start in range(0, 12, 3):
        multi.emit([_ev(i) for i in range(start, start + 3)])
    log_foundry.configure(service="t", sink=multi)

    assert len(stream.getvalue().splitlines()) == 2, "the premise: the stream took two lines"
    assert log_foundry.health().sink == SinkLosses(dropped=1, failed=9)


def _http_500(opener: FakeOpener | None = None) -> HTTPSink:
    return HTTPSink(
        "http://h", max_retries=0, opener=opener or FakeOpener([FakeResponse(500, b"")])
    )


def test_model_c_a_wrapper_whose_inner_raised_is_absorbed_whole() -> None:
    """FR-007's model (c): one failure among 30, and the inner HTTP sink raising on the rest."""
    http = _http_500()
    multi = MultiSink(TransformSink(http, lambda e: _fail_for(e, 7)), MemorySink())

    multi.emit([_ev(i) for i in range(30)])

    assert http.failed >= 1, "the premise: the HTTP sink counted its own abandoned request"
    assert multi.losses() == SinkLosses(dropped=0, failed=http.failed + 30)


def test_a_raising_wrapper_is_charged_its_whole_batch_including_what_it_filtered() -> None:
    """FR-007's third accepted cost: a raising wrapper child is charged ``len(batch)``.

    The fan-out cannot see which events the wrapper chose not to forward, so the nine ``info``
    events below the floor are in ``failed`` beside the one ``error`` the HTTP sink lost — an
    upper bound that grows with the filter ratio, which ``SinkLosses`` documents ``failed`` as.
    """
    http = _http_500()
    multi = MultiSink(FilteringSink(http, min_level="error"), MemorySink())

    multi.emit([_ev(i, "INFO") for i in range(9)] + [_ev(9, "ERROR")])

    assert http.failed == 1
    assert multi.losses() == SinkLosses(dropped=0, failed=http.failed + 10)
