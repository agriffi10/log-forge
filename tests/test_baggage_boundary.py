"""SPEC-015 — baggage reaches the span boundary events.

`span.start` and `span.end` were built with `baggage={}` hardcoded, so the two events carrying
`duration_ms`, `status` and `error` carried none of the correlation context. A consumer filtering
on a baggage key got a span's narration and lost its outcome.

These use the `lf` fixture (synchronous flush into `fake_sink`) so a span's emitted batch can be
asserted on whole — the FR-004 regression test needs to see all three events together.
"""

import asyncio

import pytest

context_mod = pytest.importorskip("log_foundry.context")
model = pytest.importorskip("log_foundry.model")


@pytest.fixture(autouse=True)
def _reset_baggage():
    """Clear baggage around each test — it is a ContextVar shared across the whole session."""
    context_mod._baggage.set({})
    yield
    context_mod._baggage.set({})


def _by_message(fake_sink, message: str) -> dict:
    for event in fake_sink.events:
        if event.get("message") == message:
            return event
    raise AssertionError(f"no {message!r} event in {[e.get('message') for e in fake_sink.events]}")


# -- FR-001: both boundary events carry the span's final baggage ------------------------


def test_span_end_carries_baggage_alongside_duration_and_status(lf, fake_sink) -> None:
    """The whole point: the record with the outcome is findable by the correlation key."""

    @lf.trace
    def work():
        lf.set_baggage(request_id="r1")
        return "done"

    work()

    end = _by_message(fake_sink, "span.end")
    assert end["fields"]["request_id"] == "r1"
    assert end["status"] == "ok"
    assert "duration_ms" in end


def test_span_start_carries_baggage_set_inside_the_body(lf, fake_sink) -> None:
    """`span.start` is buffered before the body runs — it is completed at close."""

    @lf.trace
    def work():
        lf.set_baggage(request_id="r1")

    work()

    assert _by_message(fake_sink, "span.start")["fields"]["request_id"] == "r1"


def test_error_path_carries_baggage(lf, fake_sink) -> None:
    """The invocation most worth correlating is the one that failed."""

    @lf.trace
    def boom():
        lf.set_baggage(request_id="r1")
        raise ValueError("nope")

    with pytest.raises(ValueError):
        boom()

    end = _by_message(fake_sink, "span.end")
    assert end["fields"]["request_id"] == "r1"
    assert end["status"] == "error"
    assert end["error"]["type"] == "ValueError"
    assert _by_message(fake_sink, "span.start")["fields"]["request_id"] == "r1"


def test_async_path_carries_baggage(lf, fake_sink) -> None:
    @lf.trace
    async def work():
        lf.set_baggage(request_id="r1")

    asyncio.run(work())

    assert _by_message(fake_sink, "span.start")["fields"]["request_id"] == "r1"
    assert _by_message(fake_sink, "span.end")["fields"]["request_id"] == "r1"


def test_inner_span_does_not_gain_baggage_set_after_it_closed(lf, fake_sink) -> None:
    """Each span backfills at its own close, from the baggage live at that moment."""

    @lf.trace
    def inner():
        pass

    @lf.trace
    def outer():
        inner()
        lf.set_baggage(request_id="late")

    outer()

    inner_batch = next(b for b in fake_sink.batches if b[0]["function"].endswith("inner"))
    assert all("request_id" not in e["fields"] for e in inner_batch)

    outer_batch = next(b for b in fake_sink.batches if b[0]["function"].endswith("outer"))
    assert _by_message_in(outer_batch, "span.end")["fields"]["request_id"] == "late"


def _by_message_in(batch: list[dict], message: str) -> dict:
    return next(e for e in batch if e.get("message") == message)


# -- FR-002: which events, and which keys win -------------------------------------------


def test_baggage_beats_span_defaults_on_a_boundary_event(lf, fake_sink) -> None:
    """`build_event` precedence is span.defaults → baggage; the backfill must not invert it."""

    @lf.trace(defaults={"tenant": "from-defaults"})
    def work():
        lf.set_baggage(tenant="from-baggage")

    work()

    assert _by_message(fake_sink, "span.start")["fields"]["tenant"] == "from-baggage"
    assert _by_message(fake_sink, "span.end")["fields"]["tenant"] == "from-baggage"


def test_no_baggage_leaves_boundary_fields_untouched(lf, fake_sink) -> None:
    """The backfill returns early on empty baggage — no empty key, no None value."""

    @lf.trace(defaults={"verb": "check"})
    def work():
        pass

    work()

    assert _by_message(fake_sink, "span.start")["fields"] == {"verb": "check"}
    assert _by_message(fake_sink, "span.end")["fields"] == {"verb": "check"}


def test_model_does_not_import_context(lf) -> None:
    """arch §6: `model` builds records and does not know where the current span lives.

    The baggage is a parameter for exactly this reason, and a future refactor that "simplifies"
    `backfill_baggage` by reading the ContextVar directly would create the import this pins.
    """
    source = (model.__file__ or "").replace("__init__.py", "model.py")
    with open(source) as fh:
        text = fh.read()
    assert "import context" not in text
    assert "from log_foundry.context" not in text


# -- FR-003: mid-span events record a moment and are not rewritten ----------------------


def test_info_before_set_baggage_is_not_rewritten(lf, fake_sink) -> None:
    @lf.trace
    def work():
        lf.info("before")
        lf.set_baggage(request_id="r1")
        lf.info("after")

    work()

    assert "request_id" not in _by_message(fake_sink, "before")["fields"]
    assert _by_message(fake_sink, "after")["fields"]["request_id"] == "r1"


def test_per_call_field_still_beats_baggage(lf, fake_sink) -> None:
    """A naive backfill over *all* events is exactly the change that would break this."""

    @lf.trace
    def work():
        lf.set_baggage(stage="from-baggage")
        lf.info("event", stage="from-call")

    work()

    assert _by_message(fake_sink, "event")["fields"]["stage"] == "from-call"


# -- FR-004: the regression cannot come back --------------------------------------------


def test_every_event_in_the_span_carries_the_correlation_key(lf, fake_sink) -> None:
    """One assertion over the whole batch, so fixing one event and dropping another fails."""

    @lf.trace
    def work():
        lf.set_baggage(request_id="r1")
        lf.info("midpoint", rows=42)

    work()

    batch = fake_sink.batches[0]
    assert [e["message"] for e in batch] == ["span.start", "midpoint", "span.end"]
    assert all(e["fields"].get("request_id") == "r1" for e in batch)
