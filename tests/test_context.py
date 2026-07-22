"""Phase 4 — Context (arch §5). Span stack + baggage via contextvars.

Each test body runs inside `contextvars.copy_context().run(...)` so its mutations are
isolated and never leak into other tests — the same isolation technique you'll lean on
in real async code.
"""

import contextvars

import pytest


def test_span_stack_push_and_pop() -> None:
    context = pytest.importorskip("log_foundry.context")
    model = pytest.importorskip("log_foundry.model")

    def body() -> None:
        assert context.current_span() is None
        span = model.Span(trace_id="a" * 32, span_id="b" * 16,
                          parent_span_id=None, name="x", start_ts=0.0)
        token = context.push_span(span)
        assert context.current_span() is span
        context.pop_span(token)
        assert context.current_span() is None

    contextvars.copy_context().run(body)


def test_baggage_merges_within_context() -> None:
    context = pytest.importorskip("log_foundry.context")

    def body() -> dict:
        context.set_baggage(tenant="acme")
        context.set_baggage(request_id="r1")
        return context.get_baggage()

    assert contextvars.copy_context().run(body) == {"tenant": "acme", "request_id": "r1"}


def test_baggage_does_not_leak_across_contexts() -> None:
    context = pytest.importorskip("log_foundry.context")

    contextvars.copy_context().run(lambda: context.set_baggage(secret="leak"))
    # a fresh context sees nothing the previous one set
    assert contextvars.copy_context().run(context.get_baggage) == {}
