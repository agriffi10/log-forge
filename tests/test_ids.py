"""Phase 2 — IDs (arch §3.1). W3C Trace Context wire formats.

SPEC-067 FR-003 adds the four inbound parsers' type rule: each accepts only an exact `str`, and
runs no code of anything else, a `str` subclass included. One table drives all four, because the
two id validators are what `parse_traceparent` calls on its fields and `parse_baggage_header` is
its baggage twin.
"""

import re

import pytest

from conftest import armed, fan_out, object_probe, str_probe
from log_foundry import context, ids

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


# -- SPEC-067 FR-003: the four parsers accept only an exact str -----------------------------------

_TRACE = "4bf92f3577b34da6a3ce929d0e0e4736"
_SPAN = "00f067aa0ba902b7"

# (function, a valid exact str, its result, what a refusal returns)
_PARSERS = {
    "parse_traceparent": (ids.parse_traceparent, f"00-{_TRACE}-{_SPAN}-01", (_TRACE, _SPAN), None),
    "is_valid_trace_id": (ids.is_valid_trace_id, _TRACE, True, False),
    "is_valid_span_id": (ids.is_valid_span_id, _SPAN, True, False),
    "parse_baggage_header": (context.parse_baggage_header, "k=v", {"k": "v"}, None),
}

_PROBE_E_METHODS = ("strip", "__len__", "__iter__", "__ne__", "__eq__", "split", "encode")


@pytest.mark.parametrize("parser", list(_PARSERS))
def test_a_valid_exact_str_gives_the_existing_result(parser: str) -> None:
    function, valid, result, _ = _PARSERS[parser]
    assert function(valid) == result


@pytest.mark.parametrize("parser", list(_PARSERS))
def test_a_well_behaved_str_subclass_is_refused(parser: str) -> None:
    """Called directly, a subclass is refused; `continue_trace` is what normalises one first."""
    function, valid, _, refused = _PARSERS[parser]
    value = str_probe(valid)

    with armed() as calls:
        got = function(value)

    assert got is refused
    assert calls == []


@pytest.mark.parametrize("method", _PROBE_E_METHODS)
@pytest.mark.parametrize("parser", list(_PARSERS))
def test_a_str_subclass_with_one_raising_method_is_refused_without_calling_it(
    parser: str, method: str
) -> None:
    """Probe E. The probe holds the valid text, so at base every cell is accepted or raises."""
    function, valid, _, refused = _PARSERS[parser]
    value = str_probe(valid, raising={method})

    with armed() as calls:
        got = function(value)

    assert got is refused
    assert calls == [], f"the subclass's own code ran: {calls}"


@pytest.mark.parametrize("spoof", ["raise", "str"])
@pytest.mark.parametrize("parser", list(_PARSERS))
def test_a_spoofed_class_is_refused_without_reading_it(parser: str, spoof: str) -> None:
    """Probe F: `isinstance` reads `__class__`, which may raise or answer `str`."""
    function, _, _, refused = _PARSERS[parser]
    value = object_probe("_Spoof", class_property=spoof)

    with armed() as calls:
        got = function(value)

    assert got is refused
    assert calls == [], f"the value's own code ran: {calls}"


@pytest.mark.parametrize("kind", ["none", "bytes", "int", "dag"])
@pytest.mark.parametrize("parser", list(_PARSERS))
def test_a_non_str_is_refused(parser: str, kind: str) -> None:
    function, _, _, refused = _PARSERS[parser]
    value = {"none": None, "bytes": b"00-bad", "int": 123, "dag": fan_out()}[kind]

    assert function(value) is refused
