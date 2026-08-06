"""SPEC-014 — cross-process trace continuation: `traceparent` codec, adoption, baggage.

These use the `lf` fixture (synchronous flush into `fake_sink`) so a span's emitted batch can be
asserted on directly — which is the only way to catch the FR-003 split-trace failure, where the
dataclass is re-parented but the already-buffered `span.start` event is not.
"""

import asyncio
import re

import pytest

ids = pytest.importorskip("log_foundry.ids")
context_mod = pytest.importorskip("log_foundry.context")

# The example header from the W3C Trace Context spec.
INBOUND_TRACE = "4bf92f3577b34da6a3ce929d0e0e4736"
INBOUND_SPAN = "00f067aa0ba902b7"
TRACEPARENT = f"00-{INBOUND_TRACE}-{INBOUND_SPAN}-01"

HEX32 = re.compile(r"^[0-9a-f]{32}$")


@pytest.fixture(autouse=True)
def _reset_propagation_context():
    """Clear the adopted context and baggage around each test.

    Both are ContextVars living in the module, and pytest runs every test in the same context —
    without this an adoption in one test would silently decide the trace in the next.
    """
    context_mod._adopted.set(None)
    context_mod._baggage.set({})
    yield
    context_mod._adopted.set(None)
    context_mod._baggage.set({})


def _span_batch(fake_sink, name: str) -> list[dict]:
    """The emitted batch belonging to the span named ``name``."""
    for batch in fake_sink.batches:
        if batch and batch[0].get("function") == name:
            return batch
    raise AssertionError(f"no emitted batch for span {name!r}")


# -- FR-002: the traceparent format, parsed strictly ------------------------------------


def test_parse_traceparent_returns_the_ids() -> None:
    assert ids.parse_traceparent(TRACEPARENT) == (INBOUND_TRACE, INBOUND_SPAN)


@pytest.mark.parametrize(
    ("header", "why"),
    [
        (f"00-{'0' * 32}-{INBOUND_SPAN}-01", "all-zero trace_id is invalid per W3C"),
        (f"00-{INBOUND_TRACE}-{'0' * 16}-01", "all-zero span_id is invalid per W3C"),
        (f"00-{INBOUND_TRACE.upper()}-{INBOUND_SPAN}-01", "uppercase hex"),
        (f"00-{INBOUND_TRACE[:31]}-{INBOUND_SPAN}-01", "trace_id too short"),
        (f"00-{INBOUND_TRACE}a-{INBOUND_SPAN}-01", "trace_id too long"),
        (f"00-{INBOUND_TRACE}-{INBOUND_SPAN[:15]}-01", "span_id too short"),
        (f"00-{'z' * 32}-{INBOUND_SPAN}-01", "non-hex characters"),
        (f"00-{INBOUND_TRACE}-{INBOUND_SPAN}", "truncated: missing flags"),
        (f"00-{INBOUND_TRACE}-{INBOUND_SPAN}-01-extra", "version 00 is exactly four fields"),
        (f"ff-{INBOUND_TRACE}-{INBOUND_SPAN}-01", "version ff is reserved and invalid"),
        (f"0-{INBOUND_TRACE}-{INBOUND_SPAN}-01", "version must be two hex chars"),
        (f"00-{INBOUND_TRACE}-{INBOUND_SPAN}-0", "flags must be two hex chars"),
        ("", "empty"),
        ("not-a-traceparent", "nonsense"),
    ],
)
def test_parse_traceparent_rejects_invalid(header: str, why: str) -> None:
    assert ids.parse_traceparent(header) is None, why


def test_parse_traceparent_is_forward_compatible_with_higher_versions() -> None:
    """W3C says parse the first three fields of a higher version and ignore the rest.

    "Reject anything that isn't 00" is the intuitive-and-wrong reading, and would strand this
    library the moment version 01 ships.
    """
    header = f"01-{INBOUND_TRACE}-{INBOUND_SPAN}-01-anextrafield"
    assert ids.parse_traceparent(header) == (INBOUND_TRACE, INBOUND_SPAN)


def test_parse_traceparent_never_raises_on_non_strings() -> None:
    for value in (None, 42, {"traceparent": TRACEPARENT}, [TRACEPARENT], object()):
        assert ids.parse_traceparent(value) is None


def test_format_traceparent_uses_version_00_and_sampled_flags() -> None:
    formatted = ids.format_traceparent(INBOUND_TRACE, INBOUND_SPAN)
    assert formatted == f"00-{INBOUND_TRACE}-{INBOUND_SPAN}-01"
    # The round-trip is the contract.
    assert ids.parse_traceparent(formatted) == (INBOUND_TRACE, INBOUND_SPAN)


def test_inbound_flags_are_not_round_tripped(lf, fake_sink) -> None:
    """An unsampled inbound header still produces flags=01 outbound: we never drop spans."""

    @lf.trace
    def handler() -> str | None:
        lf.continue_trace(f"00-{INBOUND_TRACE}-{INBOUND_SPAN}-00")  # flags 00 = not sampled
        return lf.current_traceparent()

    assert handler().endswith("-01")


# -- FR-001: continue_trace() adopts an inbound context ----------------------------------


def test_adopts_a_traceparent_for_the_next_root_span(lf, fake_sink) -> None:
    assert lf.continue_trace(TRACEPARENT) is True

    @lf.trace
    def work() -> None:
        pass

    work()
    batch = _span_batch(fake_sink, "test_adopts_a_traceparent_for_the_next_root_span.<locals>.work")
    assert batch[0]["trace_id"] == INBOUND_TRACE
    assert batch[0]["parent_span_id"] == INBOUND_SPAN


def test_adopts_explicit_ids(lf, fake_sink) -> None:
    assert lf.continue_trace(trace_id=INBOUND_TRACE, parent_span_id=INBOUND_SPAN) is True

    @lf.trace
    def work() -> tuple[str, str] | None:
        return lf.current_trace_context()

    trace_id, _ = work()
    assert trace_id == INBOUND_TRACE


def test_traceparent_wins_over_explicit_ids_and_warns(lf, capsys) -> None:
    other = "1" * 32
    assert lf.continue_trace(TRACEPARENT, trace_id=other, parent_span_id="2" * 16) is True

    @lf.trace
    def work() -> tuple[str, str] | None:
        return lf.current_trace_context()

    assert work()[0] == INBOUND_TRACE, "traceparent wins"
    assert "ignoring inbound trace context" in capsys.readouterr().err


def test_trace_id_without_a_parent_joins_as_another_root(lf, fake_sink) -> None:
    """Legitimate: knowing the trace but not the parent span beats being in a fresh trace."""
    assert lf.continue_trace(trace_id=INBOUND_TRACE) is True

    @lf.trace
    def work() -> None:
        pass

    work()
    batch = _span_batch(
        fake_sink, "test_trace_id_without_a_parent_joins_as_another_root.<locals>.work"
    )
    assert batch[0]["trace_id"] == INBOUND_TRACE
    assert batch[0]["parent_span_id"] is None


def test_returns_false_when_nothing_was_supplied(lf) -> None:
    assert lf.continue_trace() is False


def test_nested_span_inherits_its_in_process_parent_not_the_inbound_one(lf, fake_sink) -> None:
    @lf.trace
    def inner() -> None:
        pass

    @lf.trace
    def outer() -> None:
        lf.continue_trace(TRACEPARENT)
        inner()

    outer()
    prefix = "test_nested_span_inherits_its_in_process_parent_not_the_inbound_one.<locals>."
    outer_batch = _span_batch(fake_sink, prefix + "outer")
    inner_batch = _span_batch(fake_sink, prefix + "inner")

    assert inner_batch[0]["trace_id"] == INBOUND_TRACE, "the nested span joined the adopted trace"
    assert inner_batch[0]["parent_span_id"] == outer_batch[0]["span_id"], (
        "a nested span parents to its in-process parent, never to the inbound span"
    )
    assert inner_batch[0]["parent_span_id"] != INBOUND_SPAN


async def test_adopted_context_does_not_leak_between_sibling_tasks(lf) -> None:
    """It is a ContextVar, so this is free — but free only until someone uses a global."""
    seen: dict[str, tuple[str, str] | None] = {}
    adopted = asyncio.Event()

    @lf.trace
    async def adopting() -> None:
        lf.continue_trace(TRACEPARENT)
        seen["adopting"] = lf.current_trace_context()
        adopted.set()

    @lf.trace
    async def sibling() -> None:
        await adopted.wait()  # deliberately reads *after* the other task adopted
        seen["sibling"] = lf.current_trace_context()

    await asyncio.gather(adopting(), sibling())

    assert seen["adopting"][0] == INBOUND_TRACE
    assert seen["sibling"][0] != INBOUND_TRACE, "a sibling task must not inherit the adoption"


# -- FR-003: re-parenting an already-open root span --------------------------------------


def test_reparenting_rewrites_events_already_buffered(lf, fake_sink) -> None:
    """The trap: `build_event` snapshots the ids, so the buffered `span.start` needs rewriting.

    Written to fail against an implementation that re-parents only the `Span` dataclass — that
    leaves one span emitting its start on trace A and its end on trace B, and a split trace is
    worse than no continuation at all because it looks like data rather than a bug.
    """

    @lf.trace
    def handler() -> None:
        lf.continue_trace(TRACEPARENT)  # the ergonomic pattern: first line, span already open
        lf.info("working")

    handler()
    batch = _span_batch(
        fake_sink, "test_reparenting_rewrites_events_already_buffered.<locals>.handler"
    )

    assert len(batch) >= 3, "start + info + end"
    assert {e["trace_id"] for e in batch} == {INBOUND_TRACE}, (
        "every buffered event — start included — must be on the adopted trace"
    )
    assert {e["parent_span_id"] for e in batch} == {INBOUND_SPAN}
    assert len({e["span_id"] for e in batch}) == 1, "one span, one span_id"


def test_reparenting_never_overwrites_span_id(lf, fake_sink) -> None:
    @lf.trace
    def handler() -> None:
        lf.continue_trace(TRACEPARENT)

    handler()
    batch = _span_batch(fake_sink, "test_reparenting_never_overwrites_span_id.<locals>.handler")
    # Taking the inbound span *id* would give two processes the same span and break any
    # parent/child reconstruction downstream.
    assert batch[0]["span_id"] != INBOUND_SPAN


def test_a_non_root_span_is_not_reparented(lf, fake_sink) -> None:
    @lf.trace
    def inner() -> None:
        assert lf.continue_trace(TRACEPARENT) is True, "adopted, for the next root span opened"

    @lf.trace
    def outer() -> None:
        inner()

    outer()
    prefix = "test_a_non_root_span_is_not_reparented.<locals>."
    inner_batch = _span_batch(fake_sink, prefix + "inner")
    outer_batch = _span_batch(fake_sink, prefix + "outer")

    assert inner_batch[0]["trace_id"] != INBOUND_TRACE, "moving it would sever it from its parent"
    assert inner_batch[0]["trace_id"] == outer_batch[0]["trace_id"]
    assert inner_batch[0]["parent_span_id"] == outer_batch[0]["span_id"]


def test_with_no_span_open_nothing_is_reparented(lf, fake_sink) -> None:
    """The non-decorated-handler pattern: adopt first, open the span afterwards."""
    assert lf.continue_trace(TRACEPARENT) is True

    @lf.trace
    def later() -> None:
        pass

    later()
    batch = _span_batch(fake_sink, "test_with_no_span_open_nothing_is_reparented.<locals>.later")
    assert batch[0]["trace_id"] == INBOUND_TRACE


# -- FR-004: inbound context is untrusted, and the library still never raises -------------


@pytest.mark.parametrize(
    "hostile",
    [
        None,
        "",
        f"00-{INBOUND_TRACE}",
        f"00-{'z' * 32}-{INBOUND_SPAN}-01",
        f"00-{'0' * 32}-{INBOUND_SPAN}-01",
        42,
        {"traceparent": TRACEPARENT},
        ["00", INBOUND_TRACE],
    ],
)
def test_invalid_input_is_never_fatal_and_still_emits_a_valid_trace(
    lf, fake_sink, hostile: object
) -> None:
    assert lf.continue_trace(hostile) is False

    @lf.trace
    def work() -> None:
        pass

    work()
    batch = _span_batch(
        fake_sink,
        "test_invalid_input_is_never_fatal_and_still_emits_a_valid_trace.<locals>.work",
    )
    emitted = batch[0]["trace_id"]
    # Asserted on the emitted event, not the return value: a non-hex trace_id in the stream
    # corrupts the field for every downstream consumer, not just this span.
    assert HEX32.match(emitted), f"emitted a malformed trace_id: {emitted!r}"
    assert emitted != "0" * 32


def test_rejection_writes_one_bounded_warning(lf, capsys) -> None:
    hostile = "00-" + "q" * 4000 + "-x-01"
    assert lf.continue_trace(hostile) is False

    err = capsys.readouterr().err
    assert err.count("ignoring inbound trace context") == 1, "exactly one warning"
    assert len(err) < 300, "the attacker-controlled value must not be echoed unbounded"


def test_rejection_warning_cannot_forge_a_log_line(lf, capsys) -> None:
    """`repr` escapes newlines, so a hostile value cannot inject a second line."""
    assert lf.continue_trace("00-bad\nlog-foundry: everything is fine-01") is False
    assert capsys.readouterr().err.count("\n") == 1


def test_a_silently_absent_traceparent_writes_no_warning(lf, capsys) -> None:
    """`event.get("traceparent")` yields None whenever the caller did not propagate one."""
    assert lf.continue_trace(None) is False
    assert capsys.readouterr().err == "", "an absent header is a no-op, not a rejection"


# -- FR-005: baggage crosses the boundary too --------------------------------------------


def test_baggage_round_trips_through_the_header(lf) -> None:
    awkward = {"request_id": "req,123", "expr": "a=b", "who": "ünïcode ✓", "plain": "x"}

    @lf.trace
    def producer() -> str:
        lf.set_baggage(**awkward)
        return lf.current_baggage_header()

    header = producer()
    context_mod._baggage.set({})  # a fresh process, conceptually

    @lf.trace
    def consumer() -> dict:
        lf.continue_trace(TRACEPARENT, baggage=header)
        return dict(context_mod.get_baggage())

    assert consumer() == awkward, "separators and non-ASCII must survive the hop"


def test_non_string_baggage_values_are_str_serialized(lf) -> None:
    @lf.trace
    def producer() -> str:
        lf.set_baggage(count=7, flag=True, nested={"a": 1})
        return lf.current_baggage_header()

    header = producer()
    parsed = context_mod.parse_baggage_header(header)
    assert parsed["count"] == "7"
    assert parsed["flag"] == "True"
    assert parsed["nested"] == str({"a": 1}), "documented: a dict arrives as its repr"


def test_invalid_baggage_is_skipped_but_the_trace_is_still_adopted(lf, fake_sink, capsys) -> None:
    @lf.trace
    def handler() -> None:
        # They fail independently: losing correlating fields is bad, losing the trace join
        # because one field was malformed is worse.
        assert lf.continue_trace(TRACEPARENT, baggage="this is not a baggage header") is True

    handler()
    batch = _span_batch(
        fake_sink, "test_invalid_baggage_is_skipped_but_the_trace_is_still_adopted.<locals>.handler"
    )
    assert batch[0]["trace_id"] == INBOUND_TRACE, "the trace join survived the bad baggage"
    assert "unusable baggage header" in capsys.readouterr().err


def test_oversized_baggage_header_is_rejected(lf) -> None:
    oversized = ",".join(f"k{i}=" + "v" * 40 for i in range(400))
    assert len(oversized.encode()) > context_mod.BAGGAGE_MAX_BYTES
    assert context_mod.parse_baggage_header(oversized) is None


def test_baggage_parser_is_total() -> None:
    for value in (None, "", "   ", 42, {"k": "v"}, "novaluehere", "=novalue"):
        assert context_mod.parse_baggage_header(value) is None


def test_baggage_header_is_empty_when_no_baggage_is_set(lf) -> None:
    assert lf.current_baggage_header() == ""


# -- FR-006: the producer side ------------------------------------------------------------


def test_current_traceparent_round_trips_into_continue_trace(lf, fake_sink) -> None:
    """Producer to consumer, which is the whole contract."""
    published: dict[str, str] = {}

    @lf.trace
    def producer() -> None:
        published["traceparent"] = lf.current_traceparent()

    producer()
    producer_batch = _span_batch(
        fake_sink, "test_current_traceparent_round_trips_into_continue_trace.<locals>.producer"
    )

    assert lf.continue_trace(published["traceparent"]) is True, "our own output must be accepted"

    @lf.trace
    def consumer() -> None:
        pass

    consumer()
    consumer_batch = _span_batch(
        fake_sink, "test_current_traceparent_round_trips_into_continue_trace.<locals>.consumer"
    )
    assert consumer_batch[0]["trace_id"] == producer_batch[0]["trace_id"]
    assert consumer_batch[0]["parent_span_id"] == producer_batch[0]["span_id"]


def test_current_trace_context_returns_the_two_ids(lf) -> None:
    @lf.trace
    def work() -> tuple[str, str] | None:
        return lf.current_trace_context()

    trace_id, span_id = work()
    assert ids.is_valid_trace_id(trace_id)
    assert ids.is_valid_span_id(span_id)


def test_producer_side_returns_none_with_no_span_active(lf) -> None:
    assert lf.current_traceparent() is None
    assert lf.current_trace_context() is None


def test_new_names_are_exported(lf) -> None:
    for name in (
        "continue_trace",
        "current_traceparent",
        "current_trace_context",
        "current_baggage_header",
    ):
        assert name in lf.__all__
        assert callable(getattr(lf, name))


# -- SPEC-024 FR-002: the adopted context does not outlive its trace ----------------------

SECOND_TRACE = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4"
SECOND_SPAN = "a1b2c3d4e5f6a1b2"
SECOND_TRACEPARENT = f"00-{SECOND_TRACE}-{SECOND_SPAN}-01"


def _handler_batches(fake_sink) -> list[list[dict]]:
    """Every emitted batch for a span named ``handler``, in order."""
    return [b for b in fake_sink.batches if b and b[0].get("function") == "handler"]


def test_an_adopted_context_does_not_survive_into_the_next_root_span(lf, fake_sink) -> None:
    """The contract example: adopted *outside* the span, on a warm container."""

    @lf.trace(name="handler")
    def handler() -> None:
        lf.info("working")

    lf.continue_trace(TRACEPARENT)  # invocation 1 supplied a header
    handler()
    handler()  # invocation 2 supplied none

    first, second = _handler_batches(fake_sink)
    assert all(e["trace_id"] == INBOUND_TRACE for e in first)
    assert all(e["trace_id"] != INBOUND_TRACE for e in second), (
        "invocation 2 must not still be joining invocation 1's caller's trace"
    )
    assert HEX32.match(second[0]["trace_id"])
    assert all(e["parent_span_id"] is None for e in second)


def test_an_adopted_context_adopted_inside_the_span_also_does_not_survive(lf, fake_sink) -> None:
    """The documented placement — first line of the decorated entry point."""
    headers = [TRACEPARENT, None]

    @lf.trace(name="handler")
    def handler(header) -> None:
        lf.continue_trace(header)
        lf.info("working")

    for header in headers:
        handler(header)

    first, second = _handler_batches(fake_sink)
    assert all(e["trace_id"] == INBOUND_TRACE for e in first)
    assert all(e["trace_id"] != INBOUND_TRACE for e in second)
    assert HEX32.match(second[0]["trace_id"])
    assert all(e["parent_span_id"] is None for e in second)


def test_sequential_invocations_land_in_their_own_adopted_traces(lf, fake_sink) -> None:
    @lf.trace(name="handler")
    def handler(header: str) -> None:
        lf.continue_trace(header)
        lf.info("working")

    handler(TRACEPARENT)
    handler(SECOND_TRACEPARENT)

    first, second = _handler_batches(fake_sink)
    assert all(e["trace_id"] == INBOUND_TRACE for e in first)
    assert all(e["trace_id"] == SECOND_TRACE for e in second)
    assert second[0]["parent_span_id"] == SECOND_SPAN


def test_an_invalid_continue_trace_does_not_clear_a_context_adopted_in_the_same_trace(
    lf, fake_sink, capsys
) -> None:
    @lf.trace(name="handler")
    def handler() -> None:
        assert lf.continue_trace(TRACEPARENT) is True
        capsys.readouterr()  # drop anything written so far; the next call must add nothing
        assert lf.continue_trace(None) is False
        assert capsys.readouterr().err == "", "nothing supplied at all is a *silent* no-op"
        assert lf.continue_trace("garbage") is False
        assert context_mod.get_adopted_context() == (INBOUND_TRACE, INBOUND_SPAN)
        lf.info("working")

    handler()

    (batch,) = _handler_batches(fake_sink)
    assert all(e["trace_id"] == INBOUND_TRACE for e in batch)


def test_baggage_adopted_from_a_header_does_not_leak_into_the_next_invocation(
    lf, fake_sink
) -> None:
    @lf.trace(name="handler")
    def handler(header) -> None:
        lf.continue_trace(header, baggage="tenant=acme" if header else None)
        lf.info("working")

    handler(TRACEPARENT)
    handler(None)

    _, second = _handler_batches(fake_sink)
    assert all("tenant" not in e["fields"] for e in second)


def test_an_adoption_is_consumed_by_one_root_span_not_by_every_later_sibling(
    lf, fake_sink
) -> None:
    """Pins the narrowing that "consumed by the trace it names" implies (SPEC-024 FR-002).

    A batch loop that adopts once and then opens a root span per record joins only the *first*
    record to the inbound trace. This is the same one-shot rule that stops a warm container from
    logging every later invocation into the first caller's trace — the two shapes are
    indistinguishable from inside the library. The remedy is one call per record, or a single
    ``@trace`` entry point so the records are nested spans of one trace.
    """

    @lf.trace(name="handler")
    def handler() -> None:
        lf.info("working")

    lf.continue_trace(TRACEPARENT)
    for _ in range(3):
        handler()

    first, *rest = _handler_batches(fake_sink)
    assert all(e["trace_id"] == INBOUND_TRACE for e in first)
    assert [b[0]["trace_id"] for b in rest] != [INBOUND_TRACE, INBOUND_TRACE]
    assert len({b[0]["trace_id"] for b in rest}) == 2, "each later sibling gets its own trace"


async def test_an_adoption_outside_a_task_boundary_is_not_cleared(lf, fake_sink) -> None:
    """Pins the one shape SPEC-024 FR-002 cannot close — a documented constraint, not a bug.

    ``pop_baggage_scope`` clears the adopted context in whichever context the root span's
    ``finally`` runs in. Adopt *outside* a span and then run the span inside a child context —
    any ``asyncio.Task``, including the one ``asyncio.run`` creates — and the clear lands in the
    copy while the parent keeps the adoption. ``contextvars`` offers no way to write to a parent
    context, so the remedy is documentary: adopt on the entry point's first line (inside the
    span, which works — see the test above), or call ``reset_context()`` in the outer context.
    """

    @lf.trace(name="handler")
    def handler() -> None:
        lf.info("working")

    async def dispatch() -> None:
        handler()

    lf.continue_trace(TRACEPARENT)
    for _ in range(2):
        await asyncio.create_task(dispatch())  # Task copies the context; the clear lands there

    first, second = _handler_batches(fake_sink)
    assert all(e["trace_id"] == INBOUND_TRACE for e in first)
    assert all(e["trace_id"] == INBOUND_TRACE for e in second), (
        "the adoption survives a task boundary — the constraint this test exists to record"
    )
    assert context_mod.get_adopted_context() == (INBOUND_TRACE, INBOUND_SPAN)
