"""SPEC-006 — composition & adapter sinks: Callback, Multi, Filtering, Transform.

All tests are pure in-process (no network, no I/O): child sinks are small recording doubles,
so the assertions are about routing/isolation/close semantics, not delivery.
"""

from __future__ import annotations

import pytest

from log_foundry.sinks.base import Sink
from log_foundry.sinks.callback import CallbackSink
from log_foundry.sinks.filtering import FilteringSink
from log_foundry.sinks.multi import MultiSink
from log_foundry.sinks.transform import TransformSink


class RecordingSink:
    """A Sink double that records emitted batches and counts ``close()`` calls."""

    def __init__(self) -> None:
        self.batches: list[list[dict[str, object]]] = []
        self.closed = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        self.batches.append(list(batch))

    def close(self) -> None:
        self.closed += 1

    @property
    def events(self) -> list[dict[str, object]]:
        return [event for batch in self.batches for event in batch]


class BoomSink:
    """A Sink double that always raises — for isolation tests."""

    def emit(self, batch: list[dict[str, object]]) -> None:
        raise RuntimeError("boom emit")

    def close(self) -> None:
        raise RuntimeError("boom close")


def ev(level: str = "INFO", **fields: object) -> dict[str, object]:
    """Build a minimal event dict; sinks only read ``level`` and whatever a predicate/fn reads."""
    return {"level": level, "message": "m", "fields": dict(fields)}


# --- CallbackSink (FR-001) --------------------------------------------------------------


def test_callback_delegates_batch_unchanged() -> None:
    seen: list[list[dict[str, object]]] = []
    batch = [ev(), ev()]
    CallbackSink(seen.append).emit(batch)
    assert seen == [batch]
    assert seen[0] is batch  # handed over unchanged, not copied


def test_callback_exception_propagates() -> None:
    def boom(batch: list[dict[str, object]]) -> None:
        raise RuntimeError("nope")

    with pytest.raises(RuntimeError, match="nope"):
        CallbackSink(boom).emit([ev()])


def test_callback_close_invokes_on_close_once() -> None:
    calls = []
    sink = CallbackSink(lambda b: None, on_close=lambda: calls.append(1))
    sink.close()
    assert calls == [1]


def test_callback_close_without_on_close_is_noop() -> None:
    CallbackSink(lambda b: None).close()  # must not raise


def test_callback_is_sink_instance() -> None:
    assert isinstance(CallbackSink(lambda b: None), Sink)


# --- MultiSink (FR-002) -----------------------------------------------------------------


def test_multi_fans_out_in_construction_order() -> None:
    order: list[str] = []
    sinks = [CallbackSink(lambda b, name=name: order.append(name)) for name in ("a", "b", "c")]
    MultiSink(*sinks).emit([ev()])
    assert order == ["a", "b", "c"]


def test_multi_same_batch_reaches_every_child() -> None:
    a, b = RecordingSink(), RecordingSink()
    batch = [ev(), ev()]
    MultiSink(a, b).emit(batch)
    assert a.events == batch
    assert b.events == batch


def test_multi_isolates_failing_child() -> None:
    a, b = RecordingSink(), RecordingSink()
    multi = MultiSink(a, BoomSink(), b)
    multi.emit([ev()])  # must not raise
    assert len(a.batches) == 1 and len(b.batches) == 1  # siblings still received it
    assert multi.failed == 1


def test_multi_close_closes_all_even_when_one_raises() -> None:
    a, b = RecordingSink(), RecordingSink()
    multi = MultiSink(a, BoomSink(), b)
    multi.close()  # must not raise
    assert a.closed == 1 and b.closed == 1
    assert multi.failed == 1


def test_multi_logs_child_failure_to_stderr(capsys: pytest.CaptureFixture[str]) -> None:
    # A lone failing child is now also a *total* failure, so this re-raises (SPEC-017 FR-004).
    # The stderr line is still written first, which is what this test guards.
    with pytest.raises(RuntimeError):
        MultiSink(BoomSink()).emit([ev()])
    err = capsys.readouterr().err
    assert "MultiSink" in err and "BoomSink" in err  # FR-002: failure logged to stderr


def test_multi_empty_is_noop() -> None:
    multi = MultiSink()
    multi.emit([ev()])
    multi.close()
    assert multi.failed == 0


def test_multi_is_sink_instance() -> None:
    assert isinstance(MultiSink(), Sink)


# --- FilteringSink (FR-003) -------------------------------------------------------------


def test_filter_predicate_keeps_truthy_preserving_order() -> None:
    inner = RecordingSink()
    batch = [ev("INFO"), ev("DEBUG"), ev("WARNING")]
    FilteringSink(inner, predicate=lambda e: e["level"] != "DEBUG").emit(batch)
    assert [e["level"] for e in inner.events] == ["INFO", "WARNING"]


def test_filter_min_level_forwards_at_or_above() -> None:
    inner = RecordingSink()
    batch = [ev("DEBUG"), ev("INFO"), ev("WARNING"), ev("ERROR")]
    FilteringSink(inner, min_level="WARNING").emit(batch)
    assert [e["level"] for e in inner.events] == ["WARNING", "ERROR"]


def test_filter_min_level_is_case_insensitive() -> None:
    inner = RecordingSink()
    FilteringSink(inner, min_level="warning").emit([ev("Warning"), ev("info")])
    assert [e["level"] for e in inner.events] == ["Warning"]


def test_filter_unknown_or_missing_level_fails_open() -> None:
    inner = RecordingSink()
    FilteringSink(inner, min_level="ERROR").emit([ev("TRACE"), {"message": "no level"}])
    assert len(inner.events) == 2  # both forwarded rather than dropped


def test_filter_predicate_and_min_level_both_required() -> None:
    inner = RecordingSink()
    batch = [
        {"level": "ERROR", "keep": True},
        {"level": "ERROR", "keep": False},
        {"level": "INFO", "keep": True},
    ]
    FilteringSink(inner, predicate=lambda e: e["keep"] is True, min_level="WARNING").emit(batch)
    assert len(inner.events) == 1 and inner.events[0]["level"] == "ERROR"


def test_filter_no_pass_skips_inner_emit() -> None:
    inner = RecordingSink()
    FilteringSink(inner, min_level="ERROR").emit([ev("INFO"), ev("DEBUG")])
    assert inner.batches == []  # no empty-batch emit


def test_filter_invalid_min_level_raises() -> None:
    with pytest.raises(ValueError, match="min_level"):
        FilteringSink(RecordingSink(), min_level="WARN")


def test_filter_close_delegates_and_is_sink_instance() -> None:
    inner = RecordingSink()
    sink = FilteringSink(inner)
    sink.close()
    assert inner.closed == 1
    assert isinstance(sink, Sink)


# --- TransformSink (FR-004) -------------------------------------------------------------


def test_transform_applies_fn_as_new_list() -> None:
    inner = RecordingSink()
    TransformSink(inner, lambda e: {**e, "tagged": True}).emit([ev(), ev()])
    assert all(e["tagged"] is True for e in inner.events)


def test_transform_none_drops_event() -> None:
    inner = RecordingSink()
    TransformSink(inner, lambda e: None if e["level"] == "DEBUG" else e).emit(
        [ev("DEBUG"), ev("INFO")]
    )
    assert [e["level"] for e in inner.events] == ["INFO"]


def test_transform_does_not_mutate_caller() -> None:
    inner = RecordingSink()

    def redact(event: dict[str, object]) -> dict[str, object]:
        event = dict(event)
        fields = dict(event.get("fields", {}))  # type: ignore[arg-type]
        fields.pop("password", None)
        event["fields"] = fields
        return event

    original = ev(password="secret")
    batch = [original]
    TransformSink(inner, redact).emit(batch)
    assert original["fields"] == {"password": "secret"}  # caller's dict untouched
    assert inner.events[0]["fields"] == {}  # forwarded copy redacted


def test_transform_all_dropped_skips_inner_emit() -> None:
    inner = RecordingSink()
    TransformSink(inner, lambda e: None).emit([ev(), ev()])
    assert inner.batches == []


def test_transform_close_delegates_and_is_sink_instance() -> None:
    inner = RecordingSink()
    sink = TransformSink(inner, lambda e: e)
    sink.close()
    assert inner.closed == 1
    assert isinstance(sink, Sink)


# --- Nesting & close-once cascade (FR-005) ----------------------------------------------


def test_nested_composition_routes_correctly() -> None:
    dev, prod = RecordingSink(), RecordingSink()

    def redact(event: dict[str, object]) -> dict[str, object]:
        event = dict(event)
        fields = dict(event.get("fields", {}))  # type: ignore[arg-type]
        fields.pop("password", None)
        event["fields"] = fields
        return event

    sink = MultiSink(dev, FilteringSink(TransformSink(prod, redact), min_level="ERROR"))
    sink.emit([ev("INFO", password="p"), ev("ERROR", password="p")])

    assert [e["level"] for e in dev.events] == ["INFO", "ERROR"]  # dev tee sees everything
    assert [e["level"] for e in prod.events] == ["ERROR"]  # prod: only errors, redacted
    assert prod.events[0]["fields"] == {}


def test_close_reaches_each_leaf_once() -> None:
    a, b, c = RecordingSink(), RecordingSink(), RecordingSink()
    sink = MultiSink(a, FilteringSink(TransformSink(b, lambda e: e), min_level="ERROR"), c)
    sink.close()
    assert (a.closed, b.closed, c.closed) == (1, 1, 1)


# -- SPEC-017 FR-004: total failure reaches the worker's retry ----------------------------


def test_multi_raises_when_every_child_fails() -> None:
    """Nothing was delivered, so a retry creates no duplicates — the one case worth retrying."""
    multi = MultiSink(BoomSink(), BoomSink())
    with pytest.raises(RuntimeError):
        multi.emit([ev()])
    assert multi.failed == 2


def test_multi_raises_the_first_childs_exception_by_identity() -> None:
    first = RuntimeError("first")
    second = RuntimeError("second")

    class Raiser:
        def __init__(self, err: Exception) -> None:
            self.err = err
            self.calls = 0

        def emit(self, batch: list[dict[str, object]]) -> None:
            self.calls += 1
            raise self.err

        def close(self) -> None: ...

    a, b = Raiser(first), Raiser(second)
    multi = MultiSink(a, b)
    with pytest.raises(RuntimeError) as caught:
        multi.emit([ev()])

    assert caught.value is first  # identity, not just type/message
    assert b.calls == 1, "every child is attempted before the raise"


def test_multi_still_isolates_when_one_child_succeeds() -> None:
    """Partial success must not raise — retrying would duplicate into the healthy child."""
    good = RecordingSink()
    multi = MultiSink(BoomSink(), good)
    multi.emit([ev()])  # must not raise
    assert len(good.batches) == 1
    assert multi.failed == 1


def test_multi_empty_does_not_raise_on_emit() -> None:
    """A fan-out with no children delivered nothing but has no failure to report — a raise here
    would make a misconfigured empty MultiSink retry every batch to exhaustion."""
    MultiSink().emit([ev()])  # must not raise


def test_worker_records_a_failed_batch_when_every_child_is_down() -> None:
    """End-to-end: the whole point of FR-004 — this used to leave failed_batches at 0."""
    worker_mod = pytest.importorskip("log_foundry.worker")

    worker = worker_mod.Worker(MultiSink(BoomSink()), batch_size=1, max_retries=1)
    try:
        worker.submit([ev()])
        worker.flush(timeout=5.0)
    finally:
        worker.shutdown()

    assert worker.failed_batches == 1


def test_a_child_class_name_is_bounded(capsys: pytest.CaptureFixture[str]) -> None:
    """A child's ``__name__`` is a runtime value, so it belongs in the bounded ``detail``.

    ``_diag.absorbed`` bounds ``detail`` and not ``where`` — ``where`` is documented as a literal.
    Put the class name in ``where`` and a generated or dynamically renamed class writes an
    unbounded line (SPEC-029 FR-002).
    """
    huge = type("X" * 5000, (BoomSink,), {})

    with pytest.raises(RuntimeError):
        MultiSink(huge()).emit([ev()])

    line = capsys.readouterr().err
    assert len(line) < 400, "the class name went through the detail's bound"
    assert line.endswith("…\n")
    assert "XXXX" in line, "bounded, not dropped — the name is still the diagnosis"
