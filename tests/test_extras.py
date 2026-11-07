"""SPEC-071 FR-002 — `import_extra` converts only an absent module, and leaves the rest alone.

The absence of a module is made by `test_extras_sites.make_absent`'s finder, which records the
exception object it raised, so each case can assert the identity the requirement states: the
interpreter's own exception is the `__cause__` of a conversion, and is what propagates otherwise.

Serves invariants 10, 11 and 13: the error type is private, its message names an extra and a
module, never a value, and a missing extra is refused where the sink is constructed.
"""

from __future__ import annotations

import importlib
import pkgutil
import sys
import types

import pytest

import log_foundry
from log_foundry.sinks import _extras
from log_foundry.sinks._extras import MissingExtraError, import_extra
from test_extras_sites import make_absent


def test_a_module_already_in_sys_modules_is_returned_as_is(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = types.ModuleType("lf_extra_fake")
    monkeypatch.setitem(sys.modules, "lf_extra_fake", fake)
    assert import_extra("lf_extra_fake", "aws") is fake
    stand_in = types.SimpleNamespace(client=object())
    monkeypatch.setitem(sys.modules, "lf_extra_stand_in", stand_in)
    assert import_extra("lf_extra_stand_in", "aws") is stand_in


def test_an_absent_module_is_refused_naming_the_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    finder = make_absent(monkeypatch, "boto3")
    with pytest.raises(MissingExtraError) as info:
        import_extra("boto3", "aws")
    error = info.value
    assert isinstance(error, ModuleNotFoundError)
    assert isinstance(error, ImportError)
    assert str(error) == (
        "The \"aws\" extra is not installed (No module named 'boto3'): "
        'pip install "log-foundry[aws]"'
    )
    assert error.name == "boto3"
    assert error.path is None
    assert error.__cause__ is finder.raised[0]


def test_the_original_path_is_kept(monkeypatch: pytest.MonkeyPatch) -> None:
    make_absent(
        monkeypatch,
        "lf_extra_pathed",
        lambda full: ModuleNotFoundError(f"No module named {full!r}", name=full, path="/x/y"),
    )
    with pytest.raises(MissingExtraError) as info:
        import_extra("lf_extra_pathed", "kafka")
    assert info.value.path == "/x/y"


def test_an_absent_parent_package_is_refused_naming_the_extra(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    make_absent(monkeypatch, "lf_extra_parent")
    with pytest.raises(MissingExtraError) as info:
        import_extra("lf_extra_parent.child.leaf", "gcp-pubsub")
    assert info.value.name == "lf_extra_parent"
    assert 'pip install "log-foundry[gcp-pubsub]"' in str(info.value)


def test_a_module_mapped_to_none_is_refused_naming_the_extra(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "lf_extra_none", None)
    with pytest.raises(MissingExtraError) as info:
        import_extra("lf_extra_none", "redis")
    assert info.value.name == "lf_extra_none"
    assert "None in sys.modules" in str(info.value)


@pytest.mark.parametrize(
    ("module", "make"),
    [
        (
            "lf_extra_dep",
            lambda full: ModuleNotFoundError("No module named 'botocore'", name="botocore"),
        ),
        ("lf_extra_dep", lambda full: ModuleNotFoundError("no name")),
        ("lf_extra_dep", lambda full: ImportError("broken binary", name=full)),
        ("lf_extra_dep", lambda full: RuntimeError("broken import")),
        (
            "lf_extra_dep",
            lambda full: ModuleNotFoundError("prefix, not a parent", name="lf_extra_de"),
        ),
        (
            "lf_extra_dep.sub",
            lambda full: ModuleNotFoundError("a submodule of it", name="lf_extra_dep.sub.deeper"),
        ),
    ],
    ids=[
        "another-module",
        "no-name",
        "not-a-module-not-found",
        "not-an-import-error",
        "a-name-prefix-that-is-not-a-parent",
        "a-submodule-of-the-module",
    ],
)
def test_anything_but_the_module_being_absent_propagates_as_the_same_object(
    monkeypatch: pytest.MonkeyPatch, module: str, make: object
) -> None:
    finder = make_absent(monkeypatch, "lf_extra_dep", make)  # type: ignore[arg-type]
    with pytest.raises(BaseException) as info:
        import_extra(module, "aws")
    assert info.value is finder.raised[0]
    assert not isinstance(info.value, MissingExtraError)


def test_the_error_type_and_helper_are_in_no_public_name() -> None:
    """Invariant 10: private, in no ``__all__``, not re-exported."""
    assert not hasattr(_extras, "__all__")
    for info in pkgutil.walk_packages(log_foundry.__path__, "log_foundry."):
        exported = getattr(importlib.import_module(info.name), "__all__", ())
        assert "MissingExtraError" not in exported, info.name
        assert "import_extra" not in exported, info.name
