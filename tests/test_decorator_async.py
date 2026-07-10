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
    lf = pytest.importorskip("log_forge")
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
