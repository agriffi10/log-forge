"""Phase 3 — Data model (arch §6). Schema shape and field precedence."""


BASE_FIELDS = (
    "timestamp", "level", "message", "trace_id", "span_id", "parent_span_id",
    "log_id", "function", "service", "version", "env", "fields",
)


def _make_span(model):
    return model.Span(
        trace_id="a" * 32,
        span_id="b" * 16,
        parent_span_id=None,
        name="fn",
        start_ts=0.0,
    )


def test_build_event_matches_schema(lf) -> None:
    from log_foundry import model
    event = model.build_event(
        _make_span(model), "INFO", "hello",
        fields={"user_id": 7}, baggage={"tenant": "acme"},
    )
    for key in BASE_FIELDS:
        assert key in event, f"event is missing base field: {key}"
    assert event["level"] == "INFO"
    assert event["message"] == "hello"
    assert event["function"] == "fn"
    assert event["service"] == "test"          # from the configured Config (arch §7)
    # baggage and per-call fields both land in `fields`
    assert event["fields"]["user_id"] == 7
    assert event["fields"]["tenant"] == "acme"


def test_field_precedence_explicit_fields_win(lf) -> None:
    """arch §5.1: defaults < span.defaults < baggage < per-call fields."""
    from log_foundry import model
    event = model.build_event(
        _make_span(model), "INFO", "m",
        fields={"k": "from_fields"}, baggage={"k": "from_baggage"},
    )
    assert event["fields"]["k"] == "from_fields"


# -- SPEC-017 FR-001: pipeline-level serialization safety ---------------------------------


def test_a_poisonous_field_does_not_break_the_batch(lf, fake_sink) -> None:
    """One unserializable field used to take every co-batched event down with it."""
    import json

    class MyClass:
        pass

    @lf.trace(name="work")
    def work() -> None:
        lf.info("first", ok=1)
        lf.info("second", bad=MyClass())
        lf.info("third", ok=3)

    work()

    messages = [e["message"] for e in fake_sink.events]
    assert "first" in messages and "second" in messages and "third" in messages
    json.dumps(fake_sink.events)  # the whole batch serializes
