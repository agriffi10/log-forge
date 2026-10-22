"""SPEC-058 FR-001 — a transform's output reaches the inner sink JSON-safe.

``TransformSink`` runs its function *after* assembly, so the function can put ``bytes``, a
``datetime`` or a NaN into an event that invariant 8 promised every sink could serialize bare.
Every result is checked once with invariant 8's observable; one that fails is replaced by a
types-only coercion — assembly's per-type rules, no configured ceiling, one fixed depth bound.

Most tests here deliver into a ``MemorySink``, which never serializes: what arrives is exactly what
the wrapper forwarded, so a raw ``bytes`` or NaN that slipped through is visible rather than
hidden by an inner sink that would have failed on it anyway.
"""

from __future__ import annotations

import ast
import copy
import io
import json
import math
import subprocess
import sys
import textwrap
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

import log_foundry
from log_foundry import sanitize
from log_foundry.sanitize import _TypesOnly
from log_foundry.sinks.loki import LokiSink
from log_foundry.sinks.memory import MemorySink
from log_foundry.sinks.stdout import StdoutSink
from log_foundry.sinks.transform import TransformSink, _check
from test_sinks_http import FakeOpener

_DEPTH_LIMIT = "<depth limit>"


def _bytes_for_five(event: dict[str, object]) -> dict[str, object]:
    """Probe A and B's function: ``bytes`` into the one event whose ``fields.i == 5``."""
    fields = event.get("fields")
    if isinstance(fields, dict) and fields.get("i") == 5:
        return {**event, "fields": {**fields, "raw": b"\x01\x02"}}
    return event


def _spans(count: int) -> None:
    """``count`` traced calls, each logging one ``info`` carrying its index."""

    @log_foundry.trace
    def work(i: int) -> None:
        log_foundry.info("hello", i=i)

    for i in range(count):
        work(i)


def _deep(levels: int) -> dict[str, object]:
    """A mapping with ``levels`` single-key mappings nested below it, each under ``"k"``."""
    root: dict[str, object] = {}
    node = root
    for _ in range(levels):
        child: dict[str, object] = {}
        node["k"] = child
        node = child
    return root


def _cut(event: dict[str, object]) -> tuple[int, object, object]:
    """Intact levels below ``fields``, the value the walk ends on, and ``truncated``."""
    node = event["fields"]
    steps = 0
    while isinstance(node, dict) and "k" in node:
        node = node["k"]
        steps += 1
    return steps - 1, node, event.get("truncated")


def _transform(fn) -> tuple[TransformSink, MemorySink]:
    inner = MemorySink()
    return TransformSink(inner, fn), inner


def _one(fields: dict[str, object], **top: object) -> dict[str, object]:
    return {"message": "m", "level": "INFO", "fields": fields, **top}


# --- AC: identity, and a new mapping that leaves the returned one alone ------------------


def test_a_result_that_passes_the_check_is_forwarded_as_the_same_object() -> None:
    returned = _one({"a": 1})
    sink, inner = _transform(lambda event: returned)

    sink.emit([_one({})])

    assert inner.events[0] is returned, "a valid result is forwarded untouched, by identity"
    assert "truncated" not in returned


def test_a_failing_result_is_forwarded_as_a_new_mapping_and_the_returned_one_is_untouched() -> None:
    """The delivered event is the coerced form, and what the function returned is not mutated.

    The inner sink is a ``MemorySink``, which never serializes, so a wrapper that forwarded the
    function's result unchanged would deliver the raw ``bytes`` here and fail the equality.
    """
    returned = _one({"raw": b"caf\xc3\xa9", "nested": {"when": datetime(2026, 1, 2, tzinfo=UTC)}})
    before = copy.deepcopy(returned)
    sink, inner = _transform(lambda event: returned)

    sink.emit([_one({})])

    delivered = inner.events[0]
    assert delivered is not returned
    assert delivered == _one({"raw": "café", "nested": {"when": "2026-01-02T00:00:00+00:00"}})
    assert returned == before, "the coercion builds a new mapping and never mutates the input"


# --- AC: Probe A and Probe B, re-run ------------------------------------------------------


def _loki_values(opener: FakeOpener) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    for call in opener.calls:
        for stream in json.loads(call["body"])["streams"]:
            events.extend(json.loads(line) for _ts, line in stream["values"])
    return events


def test_probe_a_rerun_every_event_reaches_loki_and_the_bytes_arrive_decoded() -> None:
    """31 spans of three events: all 93 reach the opener, and the ``bytes`` arrive as text.

    Before FR-001, ``LokiSink._render`` raised ``TypeError`` out of ``HTTPSink._items`` and one
    30-event batch was abandoned after four attempts: 63 of 93 arrived.
    """
    opener = FakeOpener()
    log_foundry.configure(
        service="t", sink=TransformSink(LokiSink("http://loki", opener=opener), _bytes_for_five)
    )

    _spans(31)
    result = log_foundry.flush()

    events = _loki_values(opener)
    assert result.ok, result
    assert log_foundry.health().failed_batches == 0
    assert len(events) == 93
    assert len({event["log_id"] for event in events}) == 93
    five = [event for event in events if event["fields"].get("i") == 5]
    assert [event["fields"]["raw"] for event in five] == ["\x01\x02"]


def test_probe_b_rerun_thirty_distinct_lines_none_twice() -> None:
    """Ten spans through a ``StdoutSink``: 30 lines, 30 distinct ``log_id``s.

    Before FR-001 the sink wrote the events ahead of the bad one and raised, the worker retried
    the whole batch, and 64 lines were written for 16 distinct events.
    """
    stream = io.StringIO()
    log_foundry.configure(
        service="t", sink=TransformSink(StdoutSink(stream=stream), _bytes_for_five)
    )

    _spans(10)
    assert log_foundry.flush().ok

    ids = [json.loads(line)["log_id"] for line in stream.getvalue().splitlines()]
    assert len(ids) == 30
    assert len(set(ids)) == 30, "no event written twice"
    assert log_foundry.health().failed_batches == 0


# --- AC: the per-type literals, and the two faults a bare json.dumps lets through ----------


class _Unknown:
    pass


_KEY = uuid.UUID("12345678-1234-5678-1234-567812345678")

_PER_TYPE = {
    "bytes": (b"caf\xc3\xa9", "café", None),
    "datetime": (datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC), "2026-01-02T03:04:05+00:00", None),
    "decimal": (Decimal("0.1"), "0.1", None),
    "uuid": (_KEY, "12345678-1234-5678-1234-567812345678", None),
    "nan": (math.nan, "<float: nan>", True),
    "inf": (math.inf, "<float: inf>", True),
    "surrogate": ("a\ud800b", "a�b", True),
    "undecodable": (b"a\xffb", "a�b", True),
    "unknown-class": (_Unknown(), "<unserializable: _Unknown>", None),
}


@pytest.mark.parametrize("kind", sorted(_PER_TYPE))
def test_each_type_is_coerced_to_its_literal(kind: str) -> None:
    value, expected, truncated = _PER_TYPE[kind]
    sink, inner = _transform(lambda event: _one({"v": value}))

    sink.emit([_one({})])

    delivered = inner.events[0]
    assert delivered["fields"] == {"v": expected}
    assert delivered.get("truncated") is truncated


@pytest.mark.parametrize("kind", ["nan", "surrogate"])
def test_a_nan_or_a_lone_surrogate_alone_is_coerced(kind: str) -> None:
    """Both pass a bare ``json.dumps``, so a check weaker than invariant 8's would forward them."""
    value, expected, _ = _PER_TYPE[kind]
    json.dumps(_one({"v": value}))
    sink, inner = _transform(lambda event: _one({"v": value}))

    sink.emit([_one({})])

    assert inner.events[0]["fields"] == {"v": expected}
    assert inner.events[0]["truncated"] is True


# --- AC: no configured ceiling --------------------------------------------------------------


def test_no_configured_ceiling_applies_to_a_coerced_result() -> None:
    """A result that fails the check for one reason keeps everything the ceilings would clip.

    The one fault is a ``datetime``, which coerces without marking, so ``truncated`` stays absent
    and any clip below would be the only thing that could set it.
    """
    cfg = log_foundry.config.get_config()
    long_text = "x" * (3 * cfg.max_value_bytes)
    wide = {f"k{i}": i for i in range(300)}
    deep = _deep(cfg.max_depth + 1)
    stack = "s" * 40_000
    returned = _one(
        {"when": datetime(2026, 1, 2, tzinfo=UTC), "long": long_text, "wide": wide, "deep": deep},
        error={"type": "E", "stack": stack},
    )
    sink, inner = _transform(lambda event: returned)

    sink.emit([_one({})])

    delivered = inner.events[0]
    assert delivered["fields"]["long"] == long_text
    assert delivered["fields"]["wide"] == wide
    assert delivered["fields"]["deep"] == deep
    assert delivered["error"]["stack"] == stack
    assert "truncated" not in delivered


# --- AC: the structural bound, the same in every context -----------------------------------


def _deep_for_message(message: str):
    def fn(event: dict[str, object]) -> dict[str, object]:
        if event.get("message") == message:
            return {**event, "fields": _deep(20_000)}
        return event

    return fn


_EXPECTED_CUT = (99, _DEPTH_LIMIT, True)


def test_a_deep_result_is_cut_at_the_fixed_bound_and_marked() -> None:
    result = _one(_deep(20_000))
    with pytest.raises(RecursionError):
        _check(result)
    sink, inner = _transform(lambda event: result)

    sink.emit([_one({})])

    assert _cut(inner.events[0]) == _EXPECTED_CUT


_SMALL_STACK = textwrap.dedent(
    """
    import json, sys, threading
    sys.path.insert(0, sys.argv[1])
    from test_transform_coercion import _deep, _cut, _one
    from log_foundry.sinks.memory import MemorySink
    from log_foundry.sinks.transform import TransformSink, _check

    out = {}

    def body():
        result = _one(_deep(20_000))
        try:
            _check(result)
        except RecursionError:
            out["premise"] = "RecursionError"
        inner = MemorySink()
        TransformSink(inner, lambda event: result).emit([_one({})])
        out["cut"] = list(_cut(inner.events[0]))

    threading.stack_size(4 * 1024 * 1024)
    thread = threading.Thread(target=body)
    thread.start()
    thread.join()
    print(json.dumps(out))
    """
)


def test_the_cut_is_the_same_on_the_drain_thread_the_orphan_path_and_a_small_stack() -> None:
    """The drain thread, the main thread through the orphan path, and a 4 MiB thread agree.

    The small-stack context runs in its own interpreter, so a crash there — C frames per JSON
    level are unmeasured on Linux x86-64 — is a named failure with a return code, rather than a
    dead xdist worker. The child asserts the premise first: that the check raises
    ``RecursionError`` on that stack rather than killing the process.
    """
    inner = MemorySink()
    log_foundry.configure(service="t", sink=TransformSink(inner, _deep_for_message("deep")))

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("deep")

    work()
    assert log_foundry.flush().ok
    log_foundry.info("deep")

    deep = [event for event in inner.events if event["message"] == "deep"]
    assert len(deep) == 2, "one through the drain thread, one through the orphan path"
    assert _cut(deep[0]) == _EXPECTED_CUT, "the drain thread"
    assert _cut(deep[1]) == _EXPECTED_CUT, "the orphan path, on the main thread"
    assert log_foundry.health().orphan_lost == 0

    child = subprocess.run(  # noqa: S603
        [sys.executable, "-c", _SMALL_STACK, str(Path(__file__).parent)],
        capture_output=True,
        timeout=120,
        check=False,
    )
    assert child.returncode == 0, (
        f"the 4 MiB-stack child ended with {child.returncode}: {child.stderr.decode(errors='replace')}"
    )
    report = json.loads(child.stdout)
    assert report.get("premise") == "RecursionError", report
    assert tuple(report["cut"]) == _EXPECTED_CUT, "a 4 MiB thread"


# --- AC: a mapping the coercion cannot enter ----------------------------------------------


class _HostileItems(dict):
    def items(self):  # type: ignore[override]
        raise KeyError("hostile")


def test_a_mapping_whose_items_raises_is_a_failure_not_an_empty_event() -> None:
    """Alone: ``emit`` raises the ``KeyError``, the inner sink is not called, nothing is counted.

    ``sanitize_fields`` answers a top-level mapping it cannot enter with an empty one; delivering
    ``{}`` in place of a hostile event would be a silent change to the data.
    """
    hostile = _HostileItems(message="m")
    sink, inner = _transform(lambda event: hostile)

    with pytest.raises(KeyError):
        sink.emit([_one({})])

    assert inner.events == []
    assert sink.losses() is None


# --- AC: a coercion is not loss -----------------------------------------------------------


def test_a_coercion_is_not_loss(capsys) -> None:
    sink, inner = _transform(_bytes_for_five)
    log_foundry.configure(service="t", sink=sink)
    before = log_foundry.health()

    sink.emit([_one({"i": 5})])

    assert inner.events[0]["fields"]["raw"] == "\x01\x02"
    assert capsys.readouterr().err == ""
    assert sink.losses() is None
    assert log_foundry.health() == before


def test_the_orphan_path_delivers_the_coerced_event() -> None:
    sink, inner = _transform(_bytes_for_five)
    log_foundry.configure(service="t", sink=sink)

    log_foundry.info("outside a span", i=5)

    assert [event["fields"]["raw"] for event in inner.events] == ["\x01\x02"]
    assert log_foundry.health().orphan_lost == 0


# --- AC: nothing public, and the seam between the stand-in and the coercer ----------------


def test_sanitize_exports_nothing_new() -> None:
    assert sanitize.__all__ == [
        "TRUNCATION_MARKER",
        "coerce",
        "sanitize_fields",
        "truncate_str",
        "truncate_tail",
    ]


def test_the_stand_in_carries_every_ceiling_the_coercer_reads() -> None:
    """Every ``cfg.X`` / ``self._cfg.X`` the coercer reads is an attribute of the stand-in.

    The stand-in is passed through a ``cast``, so mypy cannot check it against ``_Coercer``'s
    reads; this derives them from the source instead, so a ceiling added to the coercer fails
    here rather than as an ``AttributeError`` on the first coerced event.
    """
    tree = ast.parse(Path(sanitize.__file__).read_text(encoding="utf-8"))
    coercer = next(
        node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "_Coercer"
    )
    reads = set()
    for node in ast.walk(coercer):
        if not isinstance(node, ast.Attribute):
            continue
        base = node.value
        if isinstance(base, ast.Name) and base.id == "cfg":
            reads.add(node.attr)
        if isinstance(base, ast.Attribute) and base.attr == "_cfg":
            reads.add(node.attr)
    assert len(reads) >= 3, f"the derivation found too little to trust: {reads}"
    missing = {name for name in reads if not hasattr(_TypesOnly, name)}
    assert not missing, f"the stand-in lacks ceilings the coercer reads: {missing}"
