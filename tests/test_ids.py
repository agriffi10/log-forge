"""Phase 2 — IDs (arch §3.1). W3C Trace Context wire formats."""

import re

import pytest

ids = pytest.importorskip("log_forge.ids")

HEX_32 = re.compile(r"^[0-9a-f]{32}$")  # 16-byte trace_id
HEX_16 = re.compile(r"^[0-9a-f]{16}$")  # 8-byte span_id


def test_trace_id_is_32_lowercase_hex() -> None:
    assert HEX_32.match(ids.new_trace_id())


def test_span_id_is_16_lowercase_hex() -> None:
    assert HEX_16.match(ids.new_span_id())


def test_ids_are_unique_across_calls() -> None:
    assert ids.new_trace_id() != ids.new_trace_id()
    assert ids.new_span_id() != ids.new_span_id()
    assert ids.new_log_id() != ids.new_log_id()
