"""The fixture corpus for `shadow_check`, the teardown guard against a method shadowed by a patch.

Each case plants the residue of a real ``MonkeyPatch`` round trip on a library singleton and
asserts the guard's verdict by the name it reports, and the silent cases assert ``None``: a
class-level patch and a data-attribute patch both undo cleanly and must not be reported. The
last three cases drive the conftest hook itself, so a hook that stops failing, stops being a
teardown wrapper, or stops repairing under a teardown that raised, reddens here rather than
going quiet.
"""

from __future__ import annotations

from typing import Any

import pytest

import shadow_check
from log_foundry import _lifecycle


def _round_trip(owner: object, name: str, value: object) -> None:
    """Patches ``owner.name`` with ``value`` and undoes it, as a test's monkeypatch would."""
    patch = pytest.MonkeyPatch()
    patch.setattr(owner, name, value)
    patch.undo()


def test_an_undone_instance_patch_one_level_down_is_reported_and_repaired() -> None:
    """The defect itself: ``_state._closes.arm`` patched on the instance, then undone."""
    closes = _lifecycle._state._closes
    _round_trip(closes, "arm", lambda sink: None)
    assert "arm" in vars(closes), "the precondition: the undo left the bound method behind"
    message = shadow_check.check_and_repair()
    assert message is not None and "log_foundry._lifecycle._state._closes.arm" in message, message
    assert "arm" not in vars(closes), "the repair did not remove the shadow"
    assert shadow_check.check_and_repair() is None


def test_an_undone_instance_patch_on_a_module_global_is_reported() -> None:
    """A method of a root singleton, reached straight from its module's globals."""
    _round_trip(_lifecycle._state, "live_worker", lambda: None)
    message = shadow_check.check_and_repair()
    assert message is not None and "log_foundry._lifecycle._state.live_worker" in message, message
    assert "live_worker" not in vars(_lifecycle._state)


def test_an_undone_class_patch_is_silent() -> None:
    """Patching the class, the fix's shape, leaves nothing on any instance."""
    _round_trip(_lifecycle._CloseRegistry, "arm", lambda self, sink: None)
    assert shadow_check.check_and_repair() is None


def test_an_undone_data_attribute_patch_is_silent() -> None:
    """A data attribute already lives in the instance dict, and its undo restores it."""
    _round_trip(_lifecycle._state, "_swaps_running", [])
    assert shadow_check.check_and_repair() is None


class _Base:
    """A library-typed base whose method a subclass inherits, for the MRO walk."""

    def inherited(self) -> int:
        """Returns a constant; only its place on the base class matters."""
        return 0


class _Probe(_Base):
    """A library-typed singleton stand-in with a staticmethod and a classmethod."""

    @staticmethod
    def stat() -> int:
        """Returns a constant; only its kind matters."""
        return 1

    @classmethod
    def klass(cls) -> int:
        """Returns a constant; only its kind matters."""
        return 2


_Base.__module__ = _Probe.__module__ = "log_foundry._shadow_probe"


@pytest.mark.parametrize("name", ["stat", "klass", "inherited"])
def test_every_method_kind_and_an_inherited_method_are_reported(
    name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each method kind, and a method found on a base, is held before any library class has one."""
    probe = _Probe()
    monkeypatch.setattr(_lifecycle, "_shadow_probe", probe, raising=False)
    _round_trip(probe, name, lambda *args: None)
    assert name in vars(probe), "the precondition: the undo left the method behind"
    message = shadow_check.check_and_repair()
    assert message is not None and f"log_foundry._lifecycle._shadow_probe.{name}" in message, (
        message
    )
    assert name not in vars(probe)


def test_a_process_with_no_patch_is_silent() -> None:
    """No library singleton shadows a method of its own class at import or after a reset."""
    assert shadow_check.shadows() == []


def _hook(request: pytest.FixtureRequest) -> Any:
    """The conftest's ``pytest_runtest_teardown`` implementation, from the plugin manager."""
    impls = [
        impl
        for impl in request.config.pluginmanager.hook.pytest_runtest_teardown.get_hookimpls()
        if getattr(impl.function, "__module__", "") == "conftest"
    ]
    assert len(impls) == 1, impls
    return impls[0]


def test_the_hook_is_a_teardown_wrapper(request: pytest.FixtureRequest) -> None:
    """Registered as a wrapper, so it reads after every fixture's finaliser, the undo included."""
    assert _hook(request).wrapper is True


def test_the_hook_fails_the_test_that_left_a_shadow(request: pytest.FixtureRequest) -> None:
    """Driven by hand: a shadow planted inside the teardown fails it, and is repaired first."""
    closes = _lifecycle._state._closes
    teardown = _hook(request).function(item=None, nextitem=None)
    next(teardown)
    _round_trip(closes, "arm", lambda sink: None)
    with pytest.raises(pytest.fail.Exception, match=r"_state\._closes\.arm"):
        teardown.send(None)
    assert "arm" not in vars(closes)


def test_the_hook_repairs_a_shadow_under_a_teardown_that_raised(
    request: pytest.FixtureRequest,
) -> None:
    """A teardown that raised keeps its own error, and the shadow is still removed under it."""
    closes = _lifecycle._state._closes
    teardown = _hook(request).function(item=None, nextitem=None)
    next(teardown)
    _round_trip(closes, "arm", lambda sink: None)
    with pytest.raises(RuntimeError, match="the teardown's own error"):
        teardown.throw(RuntimeError("the teardown's own error"))
    assert "arm" not in vars(closes), "the repair did not run on the raising path"
