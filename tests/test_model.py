"""Phase 3 — Data model (arch §6). Schema shape and field precedence."""

import contextvars

import pytest

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


# -- SPEC-063 FR-004: a hostile exception class or a colliding key costs only itself -----------


def _raises(cls: type) -> object:
    raise RuntimeError("hostile metaclass")


def _exception_class(name: str = "Boom", **metaclass_attrs: object) -> type[BaseException]:
    """An exception class stored as ``name`` whose metaclass carries the given attributes."""
    meta = type("_Meta", (type,), dict(metaclass_attrs))
    return meta(name, (Exception,), {})  # type: ignore[no-any-return]


class _HijackName(str):
    """A ``str`` subclass for a stored ``__name__``: ``str()`` raises and formatting renames."""

    def __str__(self) -> str:
        raise RuntimeError("str")

    def __format__(self, spec: str) -> str:
        return "HIJACK"


def _traced_failure(lf, exc_cls: type[BaseException]):
    """Runs a traced function that logs once and raises an ``exc_cls``; returns both exceptions.

    The first is the instance the function raised and the second what reached the caller, so a
    test can hold the re-raise to identity, not only to type.
    """
    instance = exc_cls()

    @lf.trace(name="work")
    def work() -> None:
        lf.info("before")
        raise instance

    try:
        contextvars.copy_context().run(work)
    except BaseException as exc:
        return instance, exc
    raise AssertionError("the traced function did not raise")


def _end(fake_sink) -> dict:
    ends = [e for e in fake_sink.events if e["message"] == "span.end"]
    assert len(ends) == 1, f"{len(ends)} span.end events"
    return ends[0]


@pytest.mark.parametrize(
    "metaclass_name",
    [property(_raises), property(lambda cls: 42), property(lambda cls: "Renamed")],
    ids=["raises", "42", "Renamed"],
)
def test_a_hostile_exception_class_keeps_its_spans_events(
    lf, fake_sink, capsys, metaclass_name: property
) -> None:
    """FR-004 AC1: re-raised ``is``-identical, events delivered, ``error.type`` the stored name."""
    exc_cls = _exception_class(__name__=metaclass_name)
    instance, raised = _traced_failure(lf, exc_cls)
    assert raised is instance, "the user's exception was not re-raised unchanged"
    assert [e["message"] for e in fake_sink.events] == ["span.start", "before", "span.end"]
    end = _end(fake_sink)
    assert end["status"] == "error"
    assert end["error"]["type"] == "Boom"
    assert "span's events were lost" not in capsys.readouterr().err


def test_a_str_subclass_stored_name_is_exact_text_in_the_error(lf, fake_sink) -> None:
    """FR-004 AC2 (silence): ``truncate_str`` already returns the stored text, an exact ``str``."""
    exc_cls = _exception_class()
    exc_cls.__name__ = _HijackName("Stored")
    _traced_failure(lf, exc_cls)
    error = _end(fake_sink)["error"]
    assert (type(error["type"]), error["type"]) == (str, "Stored")


def test_a_str_subclass_name_is_not_hijacked_on_the_stack_fallback(lf, fake_sink) -> None:
    """FR-004 AC3: with ``format_exception`` failing, the fallback names the stored text.

    The metaclass's raising ``__module__`` makes ``format_exception`` raise, so the f-string
    fallback is what the stack holds, and there the ``str`` subclass's ``__format__`` would rename
    the type unless the name is read through ``str.__str__`` first.
    """
    exc_cls = _exception_class(__module__=property(_raises))
    exc_cls.__name__ = _HijackName("Stored")
    _traced_failure(lf, exc_cls)
    assert _end(fake_sink)["error"]["stack"] == "<unformattable traceback: Stored>"


def test_a_raising_metaclass_module_reads_the_stored_module_and_falls_back_on_the_stack(
    lf, fake_sink
) -> None:
    """FR-004 AC4: ``error.type`` and ``error.module`` are stored; the stack is the fallback.

    The metaclass's ``__name__`` raises too, so the fallback must itself read the stored name.
    """
    exc_cls = _exception_class(__module__=property(_raises), __name__=property(_raises))
    _traced_failure(lf, exc_cls)
    error = _end(fake_sink)["error"]
    assert error["type"] == "Boom"
    assert error["module"] == __name__
    assert error["stack"] == "<unformattable traceback: Boom>"


class _StrRaises:
    def __str__(self) -> str:
        raise RuntimeError("str")


@pytest.mark.parametrize(
    ("module", "expected"), [(5, "5"), (_StrRaises(), "?")], ids=["5", "raising-str"]
)
def test_error_module_is_str_of_the_stored_module_or_a_question_mark(
    lf, fake_sink, module: object, expected: str
) -> None:
    """FR-004 AC5: ``str()`` of what the class stores as ``__module__``, or ``?`` when it raises."""
    exc_cls = type("Boom", (Exception,), {"__module__": module})
    _traced_failure(lf, exc_cls)
    assert _end(fake_sink)["error"]["module"] == expected


class _K:
    """A key that hashes as ``"svc"`` and cannot be compared, so a merge colliding on it raises."""

    def __hash__(self) -> int:
        return hash("svc")

    def __eq__(self, other: object) -> bool:
        raise RuntimeError("cannot compare")

    def __str__(self) -> str:
        return "K"


def _one_m(fake_sink) -> dict:
    events = [e for e in fake_sink.events if e["message"] == "m"]
    assert len(events) == 1, f"{len(events)} events for the level call: the event was lost"
    return events[0]


def _run(lf, route: str, call, *, span_defaults: dict | None = None) -> None:
    if route == "orphan":
        call()
        return
    contextvars.copy_context().run(lf.trace(name="work", defaults=span_defaults or {})(call))


@pytest.mark.parametrize(
    ("route", "source"),
    [("orphan", "configure"), ("in_span", "configure"), ("in_span", "trace")],
    ids=["orphan-configure", "in_span-configure", "in_span-trace"],
)
def test_a_hostile_default_key_costs_only_itself(lf, fake_sink, route: str, source: str) -> None:
    """FR-004 AC6: the per-call ``svc`` survives, ``K``'s entry is dropped, and the event is marked.

    A span default exists only inside a span, so the ``@trace(defaults=…)`` shape has no orphan
    route.
    """
    if source == "configure":
        lf.configure(defaults={_K(): 1})
    _run(
        lf,
        route,
        lambda: lf.info("m", svc=3),
        span_defaults={_K(): 1} if source == "trace" else None,
    )
    event = _one_m(fake_sink)
    assert event["fields"] == {"svc": 3}
    assert event.get("truncated") is True


@pytest.mark.parametrize("route", ["orphan", "in_span"])
def test_a_hostile_per_call_key_drops_the_lower_precedence_default(
    lf, fake_sink, route: str
) -> None:
    """FR-004 AC7: the per-call ``K`` is kept and the default ``svc`` is the one dropped."""
    lf.configure(defaults={"svc": 1})
    _run(lf, route, lambda: lf.info("m", fields={_K(): 2}))
    event = _one_m(fake_sink)
    assert event["fields"] == {"K": 2}
    assert event.get("truncated") is True


def test_the_rebuilt_merge_keeps_precedence_and_orders_highest_source_first(lf, fake_sink) -> None:
    """FR-004 AC8: the same values as the unpack, in the stated highest-source-first order."""

    def call() -> None:
        lf.set_baggage(y="baggage")
        lf.info("m", fields={"svc": 3, "x": 2})

    lf.configure(defaults={"a": 1, "x": 1})
    _run(lf, "in_span", call, span_defaults={"y": "span"})
    plain = _one_m(fake_sink)
    fake_sink.batches.clear()
    lf.configure(defaults={_K(): 1, "a": 1, "x": 1})
    _run(lf, "in_span", call, span_defaults={"y": "span"})
    rebuilt = _one_m(fake_sink)
    assert rebuilt["fields"] == plain["fields"] == {"a": 1, "x": 2, "y": "baggage", "svc": 3}
    assert list(rebuilt["fields"]) == ["svc", "x", "y", "a"]
    assert rebuilt.get("truncated") is True


def test_an_event_with_no_colliding_key_is_unchanged(lf, fake_sink) -> None:
    """FR-004 AC9 (silence): the unpack's order, and no ``truncated``."""

    def call() -> None:
        lf.set_baggage(y="baggage")
        lf.info("m", fields={"svc": 3, "x": 2})

    lf.configure(defaults={"a": 1, "x": 1})
    _run(lf, "in_span", call, span_defaults={"y": "span"})
    event = _one_m(fake_sink)
    assert list(event["fields"]) == ["a", "x", "y", "svc"]
    assert "truncated" not in event
