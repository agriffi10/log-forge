"""SPEC-056 FR-002 — the request envelope, and the generated-class round trip that proves it.

``roundtrip`` is the helper every OTLP test imports. The generated classes are imported
unconditionally: a missing dev dependency fails the suite rather than skipping the proof.
"""

from __future__ import annotations

import ast
import base64
import copy
import inspect
import json
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any

import pytest
from google.protobuf import json_format
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest

import log_foundry
from log_foundry import _otel
from log_foundry._otel import ResourceKey, framing, log_record, request_body, resource_key

_ROOT = Path(__file__).resolve().parent.parent
_VERSION = log_foundry.__version__
_URL = "https://opentelemetry.io/schemas/1.44.0"
_KEY: ResourceKey = ("svc", "1.2", "prod")


# --- the helper ---------------------------------------------------------------------------


def _check_keys(attributes: list[dict[str, Any]]) -> None:
    """Refuses a repeated or empty key, at this level and inside every ``kvlistValue``.

    ``Parse`` accepts duplicates and ``MessageToDict`` writes an empty key back as no key, so
    neither would say so; the helper does.
    """
    seen: set[str] = set()
    for pair in attributes:
        key = pair.get("key", "")
        assert key != "", f"empty attribute key in {attributes!r}"
        assert key not in seen, f"repeated attribute key {key!r}"
        seen.add(key)
        _check_value(pair.get("value", {}))


def _check_value(value: dict[str, Any]) -> None:
    if "kvlistValue" in value:
        _check_keys(value["kvlistValue"].get("values", []))
    for item in value.get("arrayValue", {}).get("values", []):
        _check_value(item)


def roundtrip(document: str, *, convert_ids: bool = True) -> ExportLogsServiceRequest:
    """Parses an ``ExportLogsServiceRequest`` document with the generated classes and compares.

    Converts ``traceId``/``spanId`` from hex to base64 first (OTLP/JSON's one deviation from
    proto3 JSON), parses with unknown fields refused, compares ``MessageToDict`` with the
    converted input, checks every parsed id against the hex sent, and refuses a repeated or
    empty attribute key itself. Each failure names its reason.

    Args:
      document: The request JSON as the sink would send it.
      convert_ids: ``False`` sends the ids as written, which is how a must-fail case shows that
        ``Parse`` accepts a hex id as base64 without complaint.

    Returns:
      The parsed message.

    Raises:
      AssertionError: With the reason, on any disagreement.
      google.protobuf.json_format.ParseError: On what the parser itself refuses.
    """
    converted = json.loads(document)
    sent: list[tuple[str | None, str | None]] = []
    for resource_logs in converted.get("resourceLogs", []):
        _check_keys(resource_logs.get("resource", {}).get("attributes", []))
        for scope_logs in resource_logs.get("scopeLogs", []):
            for record in scope_logs.get("logRecords", []):
                _check_keys(record.get("attributes", []))
                _check_value(record.get("body", {}))
                sent.append((record.get("traceId"), record.get("spanId")))
                for field in ("traceId", "spanId"):
                    if field in record and convert_ids:
                        record[field] = base64.b64encode(bytes.fromhex(record[field])).decode()
    message = json_format.Parse(json.dumps(converted), ExportLogsServiceRequest())
    back = json_format.MessageToDict(message, use_integers_for_enums=True)
    assert back == converted, f"re-serialization differs:\n{back!r}\n{converted!r}"
    parsed = [
        record
        for resource_logs in message.resource_logs
        for scope_logs in resource_logs.scope_logs
        for record in scope_logs.log_records
    ]
    for (trace_id, span_id), record in zip(sent, parsed, strict=True):
        if trace_id is not None:
            assert record.trace_id.hex() == trace_id, f"id changed: {record.trace_id.hex()}"
        if span_id is not None:
            assert record.span_id.hex() == span_id, f"id changed: {record.span_id.hex()}"
    return message


def _records(document: str) -> list[dict[str, Any]]:
    return [
        record
        for resource_logs in json.loads(document)["resourceLogs"]
        for scope_logs in resource_logs["scopeLogs"]
        for record in scope_logs["logRecords"]
    ]


def _event(**over: object) -> dict[str, object]:
    event: dict[str, object] = {
        "timestamp": "2026-10-05T12:34:56.789Z",
        "level": "INFO",
        "message": "m",
        "trace_id": "0af7651916cd43dd8448eb211c80319c",
        "span_id": "b7ad6b7169203331",
        "parent_span_id": None,
        "log_id": "log-1",
        "function": "work",
        "service": "svc",
        "version": "1.2",
        "env": "prod",
        "fields": {"k": 1},
    }
    event.update(over)
    return event


def _pair(event: dict[str, object]) -> tuple[ResourceKey, str]:
    return resource_key(event), json.dumps(log_record(event), separators=(",", ":"))


def _document(*events: dict[str, object], version: str = _VERSION) -> str:
    return request_body([_pair(event) for event in events], version)


# --- the envelope -------------------------------------------------------------------------


def test_one_resource_one_scope_the_pinned_schema_url() -> None:
    """AC-1."""
    document = _document(_event(message="a"), _event(message="b"))
    parsed = json.loads(document)
    assert len(parsed["resourceLogs"]) == 1
    resource_logs = parsed["resourceLogs"][0]
    assert resource_logs["resource"]["attributes"] == [
        {"key": "service.name", "value": {"stringValue": "svc"}},
        {"key": "service.version", "value": {"stringValue": "1.2"}},
        {"key": "deployment.environment.name", "value": {"stringValue": "prod"}},
    ]
    assert len(resource_logs["scopeLogs"]) == 1
    scope_logs = resource_logs["scopeLogs"][0]
    assert scope_logs["scope"] == {"name": "log_foundry", "version": _VERSION}
    assert [record["body"] for record in scope_logs["logRecords"]] == [
        {"stringValue": "a"},
        {"stringValue": "b"},
    ]
    assert resource_logs["schemaUrl"] == _URL and scope_logs["schemaUrl"] == _URL
    roundtrip(document)


def test_a_configure_mid_batch_groups_by_resource_in_first_seen_order() -> None:
    """AC-2. Events A, B, A: A first holding records 1 and 3, B holding 2, each once."""
    a1, b2, a3 = _event(message="1"), _event(message="2", service="other"), _event(message="3")
    document = _document(a1, b2, a3)
    parsed = json.loads(document)
    assert len(parsed["resourceLogs"]) == 2
    first, second = parsed["resourceLogs"]
    assert first["resource"]["attributes"][0]["value"] == {"stringValue": "svc"}
    assert [r["body"]["stringValue"] for r in first["scopeLogs"][0]["logRecords"]] == ["1", "3"]
    assert second["resource"]["attributes"][0]["value"] == {"stringValue": "other"}
    assert [r["body"]["stringValue"] for r in second["scopeLogs"][0]["logRecords"]] == ["2"]
    assert sorted(r["body"]["stringValue"] for r in _records(document)) == ["1", "2", "3"]
    roundtrip(document)


def test_an_absent_or_non_string_stamp_omits_its_attribute_and_shares_a_group() -> None:
    """AC-3."""
    absent = _event(message="a")
    del absent["version"]
    document = _document(absent, _event(message="b", version=5), _event(message="c", version="2"))
    parsed = json.loads(document)
    assert len(parsed["resourceLogs"]) == 2
    shared, other = parsed["resourceLogs"]
    assert [a["key"] for a in shared["resource"]["attributes"]] == [
        "service.name",
        "deployment.environment.name",
    ]
    assert [r["body"]["stringValue"] for r in shared["scopeLogs"][0]["logRecords"]] == ["a", "b"]
    assert [r["body"]["stringValue"] for r in other["scopeLogs"][0]["logRecords"]] == ["c"]
    roundtrip(document)


class _HashRaises(str):
    __slots__ = ()

    def __hash__(self) -> int:
        raise RuntimeError("hash")


class _Lies(str):
    __slots__ = ()

    def __eq__(self, other: object) -> bool:
        return False

    def __hash__(self) -> int:
        return 0

    def __str__(self) -> str:
        return "lie"


class _Claim:
    __class__ = property(lambda self: str)  # type: ignore[assignment]


class _DictClaim:
    """Passes ``isinstance(_, dict)`` and fails ``dict.get``: the guard's one reachable case."""

    __class__ = property(lambda self: dict)  # type: ignore[assignment]


def test_resource_key_never_raises_and_five_hostile_pairs_round_trip() -> None:
    """AC-4 (invariants 1 and 2)."""
    assert resource_key(_event(service=_HashRaises("svc"))) == ("svc", "1.2", "prod")
    assert resource_key(_event(service=_Lies("svc"))) == ("svc", "1.2", "prod")
    assert resource_key(_event(service="a\ud800b")) == ("a�b", "1.2", "prod")
    assert resource_key(_event(service=_Claim())) == (None, "1.2", "prod")
    assert resource_key(_event(service=5, version=None, env=["x"])) == (None, None, None)
    assert resource_key("not a dict") == (None, None, None)  # type: ignore[arg-type]
    assert resource_key(None) == (None, None, None)  # type: ignore[arg-type]
    assert resource_key(_DictClaim()) == (None, None, None)  # type: ignore[arg-type]
    for key in (resource_key(_event(service=_HashRaises("svc"))),):
        assert all(type(part) is str or part is None for part in key)
        hash(key)
    events = [
        _event(message="1"),
        _event(message="2", service=_HashRaises("svc")),
        _event(message="3", service="a\ud800b"),
        _event(message="4", service=_Claim()),
        _event(message="5", env="other"),
    ]
    document = _document(*events)
    parsed = json.loads(document)
    assert len(parsed["resourceLogs"]) == 4, "svc shared by 1 and 2; then three distinct keys"
    assert sorted(r["body"]["stringValue"] for r in _records(document)) == list("12345")
    roundtrip(document)


def test_an_empty_scope_version_and_an_empty_resource_are_canonical() -> None:
    """AC-5."""
    document = _document(_event(service=None, version=None, env=None), version="")
    parsed = json.loads(document)
    resource_logs = parsed["resourceLogs"][0]
    assert resource_logs["resource"] == {}
    assert resource_logs["scopeLogs"][0]["scope"] == {"name": "log_foundry"}
    roundtrip(document)


@pytest.mark.parametrize(
    "key",
    [_KEY, (None, None, None), ("café-世界", "v", None)],
    ids=["ascii", "empty", "non-ascii"],
)
def test_framing_is_the_difference_in_utf8_bytes(key: ResourceKey) -> None:
    """AC-6. And a document of ``n`` records costs base + framing + n * length + (n - 1)."""
    base = len(request_body([], _VERSION).encode("utf-8"))
    one = len(request_body([(key, "")], _VERSION).encode("utf-8"))
    assert framing(key, _VERSION) == one - base
    record = json.dumps(log_record(_event(message="café")), separators=(",", ":"))
    length = len(record.encode("utf-8"))
    for n in (1, 2, 7):
        document = request_body([(key, record)] * n, _VERSION)
        assert len(document.encode("utf-8")) == base + framing(key, _VERSION) + n * length + (n - 1)


def test_the_schema_url_follows_the_pin_and_only_the_pin(monkeypatch: pytest.MonkeyPatch) -> None:
    """AC-7. No module-level URL constant: patching the version changes both URLs, nothing else."""
    before = _document(_event())
    monkeypatch.setattr(_otel, "SEMCONV_VERSION", "9.9.9")
    after = _document(_event())
    assert after.count("https://opentelemetry.io/schemas/9.9.9") == 2
    assert "1.44.0" not in after
    assert after.replace("9.9.9", "1.44.0") == before
    for name, value in vars(_otel).items():
        assert not (isinstance(value, str) and "opentelemetry.io/schemas" in value), name


# --- the proof ----------------------------------------------------------------------------


def _chain(levels: int, kind: str) -> object:
    value: object = 1
    for _ in range(levels):
        value = {"k": value} if kind == "kvlist" else [value]
    return value


def _shared(levels: int) -> list[object]:
    y: list[object] = []
    for _ in range(levels):
        y = [y, y]
    return y


def _corpus() -> list[dict[str, object]]:
    """FR-002 AC-8's list, plus the lone-surrogate level the plan review added."""
    events: list[dict[str, object]] = [
        _event(level=level) for level in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
    ]
    events += [
        _event(message="span.start"),
        _event(message="span.end", duration_ms=1.5, status="ok"),
        _event(
            message="span.end",
            duration_ms=2,
            status="error",
            error={"type": "Boom", "module": "m", "message": "x", "stack": "tb"},
        ),
        _event(parent_span_id=None),
        _event(truncated=True),
        _event(
            fields={
                "a": {},
                "b": [],
                "c": None,
                "d": True,
                "e": 2**63 - 1,
                "f": 2**63,
                "g": 1.5,
                "h": {"i": [1, {"j": "k"}]},
            }
        ),
        _event(fields={"": 1}),
        _event(timestamp="not a time"),
        _event(service="other"),
        _event(fields={"deep": _chain(30, "kvlist")}),
        _event(fields={"deep": _chain(30, "array")}),
        _event(fields={"y": _shared(24)}),
        _event(level="ERROR\ud800"),
        _event(level="x\udfffy", message="a\ud800b", fields={"a\udc00": "b\ud800"}),
    ]
    return events


def test_the_round_trip_over_the_corpus() -> None:
    """AC-8. Parses at the default recursion limit, re-serializes equal, ids intact."""
    document = _document(*_corpus())
    assert len(json.loads(document)["resourceLogs"]) == 2
    message = roundtrip(document)
    records = [
        record
        for resource_logs in message.resource_logs
        for scope_logs in resource_logs.scope_logs
        for record in scope_logs.log_records
    ]
    assert len(records) == len(_corpus())
    assert all(record.trace_id.hex() == "0af7651916cd43dd8448eb211c80319c" for record in records)
    assert sum(1 for r in records if r.severity_text == "ERROR�") == 1
    assert not any("\ud800" in r.severity_text or "\udfff" in r.severity_text for r in records)


def _with(record_patch: dict[str, Any]) -> str:
    """One well-formed document with ``record_patch`` applied to its only record."""
    record = log_record(_event())
    record.update(record_patch)
    return request_body([(_KEY, json.dumps(record, separators=(",", ":")))], _VERSION)


def test_the_comparison_is_shown_able_to_fail() -> None:
    """AC-9. Each of the eight refused by ``Parse`` or by the helper, asserted by reason."""
    with pytest.raises(AssertionError, match="re-serialization differs"):
        roundtrip(_with({"time_unix_nano": "5"}))
    with pytest.raises(AssertionError, match="id changed"):
        roundtrip(_with({}), convert_ids=False)
    with pytest.raises(json_format.ParseError, match='no field named "bogus"'):
        roundtrip(_with({"bogus": 1}))
    with pytest.raises(AssertionError, match="re-serialization differs"):
        roundtrip(_with({"severityNumber": 0}))
    with pytest.raises(json_format.ParseError):
        roundtrip(_with({"body": {"stringValue": "a\ud800b"}}))
    with pytest.raises(json_format.ParseError, match=r"too deep|recursion|depth"):
        roundtrip(
            _with({"body": {"kvlistValue": {"values": [{"key": "k", "value": _kvlist_chain(32)}]}}})
        )
    with pytest.raises(AssertionError, match="repeated attribute key"):
        roundtrip(_with({"attributes": [{"key": "a", "value": {}}, {"key": "a", "value": {}}]}))
    with pytest.raises(AssertionError, match="empty attribute key"):
        roundtrip(_with({"attributes": [{"key": "", "value": {}}]}))
    with pytest.raises(AssertionError, match="repeated attribute key"):
        roundtrip(
            _with(
                {
                    "body": {
                        "kvlistValue": {
                            "values": [{"key": "a", "value": {}}, {"key": "a", "value": {}}]
                        }
                    }
                }
            )
        )


def _kvlist_chain(levels: int) -> dict[str, Any]:
    value: dict[str, Any] = {"intValue": "1"}
    for _ in range(levels):
        value = {"kvlistValue": {"values": [{"key": "k", "value": value}]}}
    return value


def test_the_depth_margin_the_cap_rests_on() -> None:
    """FR-001 rule 6's measurement, re-run on the locked versions: 31 kvlist levels in, 32 out."""
    roundtrip(_with({"body": _kvlist_chain(31)}))
    with pytest.raises(json_format.ParseError):
        roundtrip(_with({"body": _kvlist_chain(32)}))


def test_the_generated_classes_are_imported_unconditionally() -> None:
    """AC-10."""
    tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
    top_level = {
        node.module for node in tree.body if isinstance(node, ast.ImportFrom) and node.module
    }
    assert "google.protobuf" in top_level, "the import sits in the module body, under no try"
    assert "opentelemetry.proto.collector.logs.v1.logs_service_pb2" in top_level
    skips = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute) and node.attr == "importorskip"
    ]
    assert skips == [], "a skipped proof is a proof that was not run"


# --- FR-001 AC-23: every record the mapping tests built, through the same helper -------------


def _run_every_mapping_test() -> list[tuple[str, dict[str, object]]]:
    """Calls every test in ``test_otel_mapping`` in this process and returns what they built.

    Called rather than relied on, because ``xdist`` collects per worker and a module-level list
    filled by tests that ran elsewhere is empty here.
    """
    import test_otel_mapping as mapping

    mapping.CORPUS.clear()
    for name, function in inspect.getmembers(mapping, inspect.isfunction):
        if not name.startswith("test_") or function.__module__ != mapping.__name__:
            continue
        parametrize = [m for m in getattr(function, "pytestmark", []) if m.name == "parametrize"]
        wants_monkeypatch = "monkeypatch" in inspect.signature(function).parameters
        calls: list[dict[str, object]] = [{}]
        if parametrize:
            names, values = parametrize[0].args[0], parametrize[0].args[1]
            names = [n.strip() for n in names.split(",")] if isinstance(names, str) else list(names)
            calls = [
                dict(zip(names, (row,) if len(names) == 1 else row, strict=True)) for row in values
            ]
        for kwargs in calls:
            patcher = pytest.MonkeyPatch()
            try:
                function(**kwargs, monkeypatch=patcher) if wants_monkeypatch else function(**kwargs)
            finally:
                patcher.undo()
    return list(mapping.CORPUS)


def test_every_record_the_mapping_criteria_build_round_trips() -> None:
    """FR-001 AC-23, closed here against the helper (invariants 1 and 8)."""
    import test_otel_mapping as mapping

    corpus = _run_every_mapping_test()
    assert len(corpus) >= 100, f"the mapping corpus shrank to {len(corpus)}"
    for name, record in corpus:
        json.dumps(record, allow_nan=False)
        rendered = json.dumps(record, separators=(",", ":"), ensure_ascii=False)
        assert mapping.SENTINEL not in rendered, name
        document = request_body([(_KEY, rendered)], _VERSION)
        roundtrip(document)


# --- the dependency -----------------------------------------------------------------------


def test_the_dependency_is_dev_only_and_the_library_never_imports_it() -> None:
    """AC-11. Importing the library and the sink leaves neither ``google`` nor ``opentelemetry``."""
    with (_ROOT / "pyproject.toml").open("rb") as handle:
        project = tomllib.load(handle)
    dev = project["tool"]["poetry"]["group"]["dev"]["dependencies"]
    assert "opentelemetry-proto" in dev
    runtime = project["project"]["dependencies"]
    extras = project["project"]["optional-dependencies"]
    assert runtime == [], f"the core is dependency-free: {runtime}"
    for name in ("opentelemetry", "protobuf"):
        assert not any(name in requirement for requirement in runtime)
        assert not any(name in req for reqs in extras.values() for req in reqs), name
        assert not any(
            name in group for group in project["tool"]["poetry"]["group"] if group != "dev"
        ), name
    probe = (
        "import sys, log_foundry, log_foundry.sinks.otlp; "
        "print(sorted(m for m in sys.modules if m.startswith(('google.protobuf', 'opentelemetry'))))"
    )
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", probe], capture_output=True, text=True, check=True, cwd=_ROOT
    )
    assert result.stdout.strip() == "[]", result.stdout


def test_the_envelope_parts_are_deep_copies_of_nothing() -> None:
    """``request_body`` never mutates its input pairs."""
    pairs = [(_KEY, "{}"), ((None, None, None), "{}")]
    before = copy.deepcopy(pairs)
    request_body(pairs, _VERSION)
    assert pairs == before
