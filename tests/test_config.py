"""Phase 1 — Config (arch §7). Global settings, set once at startup."""

import pytest

config = pytest.importorskip("log_forge.config")


def test_configure_sets_identity_fields() -> None:
    config.configure(service="payments", version="2.14", env="prod")
    cfg = config.get_config()
    assert (cfg.service, cfg.version, cfg.env) == ("payments", "2.14", "prod")


def test_configure_patches_only_provided_fields() -> None:
    config.configure(service="payments", version="2.14", env="prod")
    config.configure(env="staging")  # a second call composes rather than resetting
    cfg = config.get_config()
    assert cfg.env == "staging"
    assert cfg.service == "payments"  # untouched by the second call


def test_configured_sink_is_stored() -> None:
    class FakeSink:
        def emit(self, batch: list[dict[str, object]]) -> None: ...
        def close(self) -> None: ...

    sink = FakeSink()
    config.configure(sink=sink)
    assert config.get_config().sink is sink


def test_defaults_default_to_empty_dict() -> None:
    # A fresh interpreter starts with no user defaults; setting them replaces the dict.
    config.configure(defaults={"team": "checkout"})
    assert config.get_config().defaults == {"team": "checkout"}
