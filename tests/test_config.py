"""Phase 1 — Config (arch §7). Global settings, set once at startup."""

import pytest

config = pytest.importorskip("log_foundry.config")


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


# -- SPEC-017 FR-006: payload ceilings ---------------------------------------------------


def test_ceilings_have_documented_defaults() -> None:
    cfg = config.get_config()
    assert cfg.max_value_bytes == 8192
    assert cfg.max_stack_bytes == 32768
    assert cfg.max_keys == 256
    assert cfg.max_depth == 8


def test_ceilings_are_configurable() -> None:
    config.configure(max_value_bytes=64, max_stack_bytes=128, max_keys=2, max_depth=2)
    cfg = config.get_config()
    assert (cfg.max_value_bytes, cfg.max_stack_bytes, cfg.max_keys, cfg.max_depth) == (
        64,
        128,
        2,
        2,
    )


@pytest.mark.parametrize(
    "name", ["max_value_bytes", "max_stack_bytes", "max_keys", "max_depth"]
)
@pytest.mark.parametrize("bad", [0, -1])
def test_a_non_positive_ceiling_is_rejected(name: str, bad: int) -> None:
    with pytest.raises(ValueError, match=name):
        config.configure(**{name: bad})


def test_a_rejected_call_leaves_the_config_untouched() -> None:
    """Validation runs before any assignment, so a bad ceiling cannot half-apply a call."""
    config.configure(service="before")
    with pytest.raises(ValueError):
        config.configure(service="after", max_keys=0)
    assert config.get_config().service == "before"


def test_max_keys_configured_through_configure_takes_effect() -> None:
    model = pytest.importorskip("log_foundry.model")
    config.configure(max_keys=2)
    span = model.Span(
        trace_id="a" * 32, span_id="b" * 16, parent_span_id=None, name="fn", start_ts=0.0
    )
    event = model.build_event(
        span, "INFO", "m", fields={str(i): i for i in range(10)}, baggage={}
    )
    assert len(event["fields"]) == 2
    assert event["truncated"] is True


def test_max_depth_configured_through_configure_takes_effect() -> None:
    model = pytest.importorskip("log_foundry.model")
    config.configure(max_depth=2)
    span = model.Span(
        trace_id="a" * 32, span_id="b" * 16, parent_span_id=None, name="fn", start_ts=0.0
    )
    event = model.build_event(
        span, "INFO", "m", fields={"a": {"b": {"c": {"d": 1}}}}, baggage={}
    )
    assert "<depth limit>" in str(event["fields"])
    assert event["truncated"] is True


def test_max_depth_of_one_still_keeps_scalar_field_values() -> None:
    """`fields` is the payload container, not a nesting level the caller chose — so the
    smallest legal max_depth must still emit scalar values rather than an empty event."""
    model = pytest.importorskip("log_foundry.model")
    config.configure(max_depth=1)
    span = model.Span(
        trace_id="a" * 32, span_id="b" * 16, parent_span_id=None, name="fn", start_ts=0.0
    )
    event = model.build_event(span, "INFO", "m", fields={"a": 1, "b": "two"}, baggage={})
    assert event["fields"] == {"a": 1, "b": "two"}
