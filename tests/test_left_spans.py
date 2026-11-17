"""The fixture corpus for `_left_spans`, the teardown guard against a span left on the stack.

An interrupted close leaves its root span on the context's span stack by design, and pytest runs
every sync test in one context, so the span stays current for every later test and
``continue_trace`` refuses each adoption. Each case runs in a copied context, so a guard that
stopped repairing could not leak its planted span into the rest of the suite. The last cases drive
the ``_reset_config`` fixture by hand, so a teardown that stops calling the guard, or stops failing
on its verdict, reddens here rather than going quiet.
"""

from __future__ import annotations

import contextvars
import dataclasses
from typing import Any

import pytest

import conftest
import log_foundry as lf
from log_foundry import config, context, decorator


@lf.trace
def _closed_root() -> Any:
    """Returns its own span, which is closed once the call returns."""
    return context.current_span()


def _isolated(case: Any) -> Any:
    """Runs ``case`` in a copy of the test's context and returns what it returns."""
    return contextvars.copy_context().run(case)


def test_a_closed_span_left_on_the_stack_is_reported_and_removed() -> None:
    """The defect's shape: a closed root span still current once the test body ends."""

    def case() -> tuple[str | None, tuple[Any, ...]]:
        context.push_span(_closed_root())
        return conftest._left_spans(), context._live_span_stack()

    message, after = _isolated(case)
    assert message is not None and "1 closed span(s)" in message, message
    assert "_closed_root" in message and "copied context" in message, message
    assert after == (), "the repair did not empty the stack"


def test_an_open_span_left_on_the_stack_is_removed_but_left_to_the_ledger_check() -> None:
    """An open span is ``ledger_check``'s failure to report; the guard only empties the stack."""

    def case() -> tuple[str | None, tuple[Any, ...]]:
        context.push_span(dataclasses.replace(_closed_root(), closed=False))
        return conftest._left_spans(), context._live_span_stack()

    message, after = _isolated(case)
    assert message is None, message
    assert after == (), "the repair did not empty the stack of an open span"


def test_the_interrupted_close_itself_is_reported() -> None:
    """A real interrupted close, not a planted span: the case the guard exists for."""

    def interrupt(*args: object, **kwargs: object) -> dict[str, Any]:
        raise KeyboardInterrupt

    def case() -> str | None:
        with pytest.MonkeyPatch.context() as patch:

            @lf.trace
            def work() -> None:
                patch.setattr(decorator, "end_event", interrupt)

            try:
                work()
            except KeyboardInterrupt:
                pass
        return conftest._left_spans()

    message = _isolated(case)
    assert message is not None and ".work" in message, message


def test_an_empty_stack_is_silent() -> None:
    """A test that opened and closed its spans normally leaves nothing to report."""

    def case() -> str | None:
        _closed_root()
        return conftest._left_spans()

    assert _isolated(case) is None


def _reset_config(request: pytest.FixtureRequest) -> Any:
    """The conftest's ``_reset_config`` generator function, unwrapped from its fixture."""
    defs = request._fixturemanager.getfixturedefs("_reset_config", request.node)
    assert defs, "the conftest no longer defines _reset_config"
    return defs[-1].func


def test_the_reset_fixture_fails_the_test_that_left_a_span(request: pytest.FixtureRequest) -> None:
    """Driven by hand: a span left by the body fails the teardown, after the rest of the reset.

    The failure is raised last, so the config and the context are reset on this path too: what
    ``shadow_check`` walks afterwards must not reach the failing test's sink.
    """
    fixture = _reset_config(request)

    def case() -> tuple[str, tuple[Any, ...], bool, dict[str, object]]:
        teardown = fixture()
        next(teardown)
        context.push_span(_closed_root())
        context.set_baggage(left="behind")
        during = config._config
        with pytest.raises(pytest.fail.Exception) as failed:
            teardown.send(None)
        replaced = config._config is not during
        return str(failed.value), context._live_span_stack(), replaced, context.get_baggage()

    message, after, replaced, baggage = _isolated(case)
    assert "1 closed span(s)" in message and "_closed_root" in message, message
    assert after == ()
    assert replaced, "the failure skipped the teardown's config reset"
    assert baggage == {}, "the failure skipped the teardown's context reset"


def test_the_reset_fixture_is_silent_when_no_span_is_left(request: pytest.FixtureRequest) -> None:
    """The same fixture driven over a body that leaves the stack empty ends without a failure."""
    fixture = _reset_config(request)

    def case() -> None:
        teardown = fixture()
        next(teardown)
        _closed_root()
        with pytest.raises(StopIteration):
            teardown.send(None)

    _isolated(case)
