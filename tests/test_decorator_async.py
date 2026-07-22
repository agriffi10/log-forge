"""Phase 8 — async @trace (arch §5).

Requires pytest-asyncio (in the dev group); asyncio_mode="auto" in pyproject means async
test functions run without an explicit marker.
"""

import asyncio

import pytest


def _async_trace_supported() -> bool:
    """True once ``@trace`` returns a coroutine function for async defs (SPEC-003 FR-001).

    Until the async wrapper lands, ``trace`` yields the SPEC-001 sync wrapper, which is not a
    coroutine function — so this probe (and the module skip below) lifts automatically when
    SPEC-003 is implemented, matching the "guard on the feature you need" suite convention.
    """
    lf = pytest.importorskip("log_foundry")
    if not hasattr(lf, "trace"):
        return False

    async def _probe() -> None: ...

    return asyncio.iscoroutinefunction(lf.trace(_probe))


pytestmark = pytest.mark.skipif(
    not _async_trace_supported(),
    reason="async @trace not implemented yet (SPEC-003)",
)


async def test_async_trace_emits_span_and_status(lf, fake_sink) -> None:
    @lf.trace(name="afetch")
    async def afetch() -> str:
        lf.info("awaiting")
        return "done"

    assert await afetch() == "done"

    events = fake_sink.events
    assert any(e["function"] == "afetch" for e in events)
    assert any(e["message"] == "awaiting" for e in events)
    assert sum(1 for e in events if e.get("status") == "ok") == 1


async def test_async_exception_is_recorded_then_reraised(lf, fake_sink) -> None:
    @lf.trace(name="aboom")
    async def aboom() -> None:
        raise RuntimeError("async nope")

    with pytest.raises(RuntimeError, match="async nope"):
        await aboom()

    assert any(e.get("status") == "error" for e in fake_sink.events)


def test_dispatch_by_coroutine_function(lf) -> None:
    # FR-001: async def -> coroutine-function wrapper; sync def -> sync wrapper.
    @lf.trace
    async def af() -> None: ...

    @lf.trace
    def sf() -> None: ...

    assert asyncio.iscoroutinefunction(af)
    assert not asyncio.iscoroutinefunction(sf)


async def test_parameterized_trace_on_async(lf, fake_sink) -> None:
    @lf.trace(name="named", defaults={"k": "v"})
    async def af() -> int:
        lf.info("hi")
        return 1

    assert await af() == 1

    events = fake_sink.events
    assert any(e["function"] == "named" for e in events)
    assert any(e["fields"].get("k") == "v" for e in events)


async def test_async_nesting_links_parent(lf, fake_sink) -> None:
    @lf.trace(name="load")
    async def load() -> None:
        lf.info("loading")

    @lf.trace(name="fetch")
    async def fetch() -> None:
        await load()

    await fetch()

    events = fake_sink.events
    assert len({e["trace_id"] for e in events}) == 1, "await chain shares one trace"
    parent_span_id = next(e["span_id"] for e in events if e["function"] == "fetch")
    load_events = [e for e in events if e["function"] == "load"]
    assert load_events
    assert all(e["parent_span_id"] == parent_span_id for e in load_events)


async def test_parent_baggage_visible_to_awaited_child(lf, fake_sink) -> None:
    # FR-003: baggage set in a parent async span is visible to child async spans it awaits.
    @lf.trace(name="load")
    async def load() -> None:
        lf.info("loading")

    @lf.trace(name="fetch")
    async def fetch() -> None:
        lf.set_baggage(request_id="req-async")
        await load()

    await fetch()

    child_log = next(e for e in fake_sink.events if e["message"] == "loading")
    assert child_log["fields"].get("request_id") == "req-async"


async def test_concurrent_gather_children_share_trace_distinct_spans(lf, fake_sink) -> None:
    @lf.trace(name="child")
    async def child(i: int) -> int:
        lf.info("child", i=i)
        return i

    @lf.trace(name="parent")
    async def parent() -> list[int]:
        return await asyncio.gather(*(child(i) for i in range(3)))

    assert await parent() == [0, 1, 2]

    events = fake_sink.events
    assert len({e["trace_id"] for e in events}) == 1
    parent_span_id = next(e["span_id"] for e in events if e["function"] == "parent")
    child_events = [e for e in events if e["function"] == "child"]
    assert all(e["parent_span_id"] == parent_span_id for e in child_events)
    assert len({e["span_id"] for e in child_events}) == 3, "each child gets a distinct span"


async def test_sibling_baggage_isolation(lf, fake_sink) -> None:
    @lf.trace(name="child")
    async def child(i: int) -> None:
        lf.set_baggage(**{f"key{i}": i})
        await asyncio.sleep(0)  # yield so the two children genuinely interleave
        lf.info("done", i=i)

    @lf.trace(name="parent")
    async def parent() -> None:
        await asyncio.gather(child(0), child(1))

    await parent()

    done = {e["fields"]["i"]: e for e in fake_sink.events if e["message"] == "done"}
    assert done[0]["fields"].get("key0") == 0
    assert "key1" not in done[0]["fields"], "sibling baggage must not leak in"
    assert done[1]["fields"].get("key1") == 1
    assert "key0" not in done[1]["fields"], "sibling baggage must not leak in"


async def test_cancellation_records_error_end_event(lf, fake_sink) -> None:
    started = asyncio.Event()

    @lf.trace(name="acancel")
    async def acancel() -> None:
        started.set()
        await asyncio.sleep(10)

    task = asyncio.create_task(acancel())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    end_events = [e for e in fake_sink.events if e.get("status") == "error"]
    assert end_events, "a cancelled coroutine records an error end event, not an unclosed span"
    assert any(e.get("error", {}).get("type") == "CancelledError" for e in end_events)
