"""Phase 4 — Context (arch §5). Span stack + baggage via contextvars.

Each test body runs inside `contextvars.copy_context().run(...)` so its mutations are
isolated and never leak into other tests — the same isolation technique you'll lean on
in real async code.
"""

import contextvars

import pytest


def test_span_stack_push_and_pop() -> None:
    from log_foundry import context, model

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
    from log_foundry import context

    def body() -> dict:
        context.set_baggage(tenant="acme")
        context.set_baggage(request_id="r1")
        return context.get_baggage()

    assert contextvars.copy_context().run(body) == {"tenant": "acme", "request_id": "r1"}


def test_baggage_does_not_leak_across_contexts() -> None:
    from log_foundry import context

    contextvars.copy_context().run(lambda: context.set_baggage(secret="leak"))
    # a fresh context sees nothing the previous one set
    assert contextvars.copy_context().run(context.get_baggage) == {}


# -- SPEC-024: the root-span scope ------------------------------------------------------


def test_baggage_scope_restores_the_prior_value() -> None:
    from log_foundry import context

    def body() -> None:
        context.set_baggage(process="default")
        scope = context.push_baggage_scope()
        context.set_baggage(request="r1")
        assert context.get_baggage() == {"process": "default", "request": "r1"}
        context.pop_baggage_scope(scope)
        # restored to the pre-scope value, not cleared (FR-001)
        assert context.get_baggage() == {"process": "default"}

    contextvars.copy_context().run(body)


def test_baggage_scope_restores_to_empty_when_nothing_was_set() -> None:
    from log_foundry import context

    def body() -> None:
        scope = context.push_baggage_scope()
        context.set_baggage(request="r1")
        context.pop_baggage_scope(scope)
        assert context.get_baggage() == {}

    contextvars.copy_context().run(body)


def test_baggage_scope_clears_the_adopted_context() -> None:
    from log_foundry import context

    def body() -> None:
        scope = context.push_baggage_scope()
        context.set_adopted_context("a" * 32, "b" * 16)
        context.pop_baggage_scope(scope)
        # cleared, not restored — a one-shot handoff to the trace it named (FR-002)
        assert context.get_adopted_context() is None

    contextvars.copy_context().run(body)


def test_baggage_scope_clears_an_adopted_context_that_predates_it() -> None:
    from log_foundry import context

    def body() -> None:
        context.set_adopted_context("a" * 32, "b" * 16)
        scope = context.push_baggage_scope()
        context.pop_baggage_scope(scope)
        # the case a token restore would get wrong: adopted *before* the span opened
        assert context.get_adopted_context() is None

    contextvars.copy_context().run(body)


def test_pop_baggage_scope_tolerates_a_token_from_another_context() -> None:
    """A span body that hands work to another thread can make `reset` raise ValueError."""
    from log_foundry import context
    minted: dict[str, object] = {}

    def mint() -> None:
        minted["scope"] = context.push_baggage_scope()
        context.set_baggage(request="r1")

    def body() -> None:
        context.set_baggage(process="default")
        contextvars.copy_context().run(mint)
        # Precondition, asserted so this test cannot quietly stop covering the fallback:
        # the raw reset is exactly what `pop_baggage_scope` has to survive.
        with pytest.raises(ValueError):
            context._baggage.reset(minted["scope"])
        context.pop_baggage_scope(minted["scope"])  # must not raise
        assert context.get_baggage() == {"process": "default"}

    contextvars.copy_context().run(body)


def test_pop_baggage_scope_from_another_context_falls_back_to_empty() -> None:
    """The same fallback when the foreign token captured an unset variable."""
    from log_foundry import context
    minted: dict[str, object] = {}

    # An empty Context has never had the baggage var set, so the token's old value is MISSING.
    contextvars.Context().run(lambda: minted.__setitem__("scope", context.push_baggage_scope()))

    def body() -> None:
        context.set_baggage(leftover="x")
        context.pop_baggage_scope(minted["scope"])
        assert context.get_baggage() == {}

    contextvars.copy_context().run(body)


# -- SPEC-024 FR-003: the explicit reset -------------------------------------------------


def test_reset_context_clears_baggage() -> None:
    from log_foundry import context

    def body() -> None:
        context.set_baggage(tenant="acme", request="r1")
        context.reset_context()
        assert context.get_baggage() == {}

    contextvars.copy_context().run(body)


def test_reset_context_clears_the_adopted_context() -> None:
    from log_foundry import context

    def body() -> None:
        context.set_adopted_context("a" * 32, "b" * 16)
        context.reset_context()
        assert context.get_adopted_context() is None

    contextvars.copy_context().run(body)


def test_reset_context_clears_a_process_level_baggage_default() -> None:
    """Unlike the scope release, an explicit reset erases rather than restores."""
    from log_foundry import context

    def body() -> None:
        context.set_baggage(process="p1")  # set before any span — a deliberate default
        context.reset_context()
        assert context.get_baggage() == {}

    contextvars.copy_context().run(body)


def test_reset_context_is_safe_when_nothing_was_ever_set() -> None:
    from log_foundry import context

    def body() -> None:
        context.reset_context()
        context.reset_context()  # and idempotent
        assert context.get_baggage() == {}
        assert context.get_adopted_context() is None

    contextvars.copy_context().run(body)


def test_reset_context_inside_a_span_clears_for_the_remainder_then_the_scope_restores() -> None:
    from log_foundry import context

    def body() -> None:
        context.set_baggage(process="p1")
        scope = context.push_baggage_scope()  # what @trace's root span does
        context.set_baggage(request="r1")
        context.reset_context()
        assert context.get_baggage() == {}, "cleared for the rest of the span"
        context.pop_baggage_scope(scope)
        assert context.get_baggage() == {"process": "p1"}, "the scope still restores"

    contextvars.copy_context().run(body)


def test_reset_context_replaces_the_baggage_dict_rather_than_emptying_it() -> None:
    """The module's never-mutate rule, pinned where it actually bites.

    A `reset_context` written as ``_baggage.get().clear()`` looks equivalent and passes every
    other test here. It is not: the dict a child context reads is the *same object* its parent
    holds, so emptying it in place reaches back and wipes the parent's baggage. Only `.set()` of
    a new dict is confined to the context that calls it.
    """
    from log_foundry import context

    def body() -> None:
        context.set_baggage(tenant="acme")
        contextvars.copy_context().run(context.reset_context)  # a child resets
        assert context.get_baggage() == {"tenant": "acme"}, "the parent keeps its own baggage"

    contextvars.copy_context().run(body)


def test_reset_context_does_not_hand_back_the_shared_contextvar_default() -> None:
    """The other half: with nothing ever set, `.clear()` would be a no-op on the shared default.

    Reads through ``_live_baggage`` rather than ``get_baggage``. This is an **identity**
    assertion, and SPEC-034 FR-005 made the public accessor return a copy — so against
    ``get_baggage`` it became trivially true and stopped catching anything: with the
    ``reset_context`` bug reinstated (``_baggage.get().clear()``) it passed. Found by review, by
    running that exact mutant against both branches.
    """
    from log_foundry import context
    shared_default = contextvars.Context().run(context._live_baggage)

    def body() -> None:
        context.reset_context()
        assert context._live_baggage() is not shared_default

    contextvars.copy_context().run(body)
    assert shared_default == {}, "the shared default must never have been mutated"


def test_reset_context_is_exported_from_the_package() -> None:
    import log_foundry as lf
    assert "reset_context" in lf.__all__
    assert callable(lf.reset_context)


# -- SPEC-025: `pop_span` is total, on the same terms as `pop_baggage_scope` --------------


def test_pop_span_tolerates_a_token_from_another_context() -> None:
    from log_foundry import context, model
    minted: dict[str, object] = {}

    def span(name: str):
        return model.Span(
            trace_id="a" * 32, span_id="b" * 16, parent_span_id=None, name=name, start_ts=0.0
        )

    def mint() -> None:
        minted["token"] = context.push_span(span("inner"))

    def body() -> None:
        outer = span("outer")
        context.push_span(outer)
        contextvars.copy_context().run(mint)
        # Precondition, asserted so this cannot quietly stop covering the fallback.
        with pytest.raises(ValueError):
            context._span_stack.reset(minted["token"])
        context.pop_span(minted["token"])  # must not raise
        assert context.current_span() is outer, "the captured stack is restored, not discarded"

    contextvars.copy_context().run(body)


def test_pop_span_from_another_context_falls_back_to_empty() -> None:
    from log_foundry import context, model
    minted: dict[str, object] = {}

    def mint() -> None:
        minted["token"] = context.push_span(
            model.Span(
                trace_id="a" * 32, span_id="b" * 16, parent_span_id=None, name="x", start_ts=0.0
            )
        )

    # An empty Context has never had the stack set, so the token's old value is MISSING.
    contextvars.Context().run(mint)

    def body() -> None:
        context.pop_span(minted["token"])
        assert context.current_span() is None

    contextvars.copy_context().run(body)
