"""SPEC-092's gates on its own corpus: the declaration, the harness, the assertion and the register.

A gate whose checks have no fixtures goes quiet unseen, so each pure gate here (`judge`,
`roster_gap`, the parameter checks, `register_faults`) has a standing corpus of hand-built cases
that asserts its exact failure text or verdict, including cases that assert silence. The harness
is held to failing loudly: a row that reaches the network, a seam that is gone, a seam left
patched. And the checks are held to firing on the rows SPEC-092 FR-002 and FR-003 name when one
is removed, its boundary moved, or the library's driver classes go unrecognised.
"""

from __future__ import annotations

import ast
import collections
import dataclasses
import inspect
import pathlib
import socket
import sys
import textwrap
import types
from typing import TYPE_CHECKING, Any

import pytest

import conformance_backend as cb
import conformance_classes as cc
import conformance_doubles as cd
import conformance_pins as cp
from model_pins import PinnedVerdict
from test_model_oracles import skip_offenders
from test_public_surface import _base_names, _sink_class_nodes, _sink_classes_with_emit

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

_ROOT = pathlib.Path(__file__).resolve().parents[1]

_CORPUS_FILES = (
    "conformance_classes.py",
    "conformance_backend.py",
    "conformance_doubles.py",
    "conformance_pins.py",
    "test_conformance.py",
    "test_conformance_gate.py",
)


def _row(row_id: str) -> cc.Row:
    """The generated row with this id.

    Args:
      row_id: The row's id.

    Returns:
      The row.

    Raises:
      KeyError: When no row has that id.
    """
    for row in cc.rows():
        if row.id == row_id:
            return row
    raise KeyError(row_id)


# The declaration (FR-001).


def test_the_variants_are_the_56_of_fr_001_and_spec_093_each_in_one_of_the_ten_classes() -> None:
    """FR-001: 53 variants, SPEC-093 FR-005's two confirmed RabbitMQ ones, and SPEC-100's FIFO SNS.

    SPEC-100 FR-006 splits ``SNSSink`` into ``SNSSink[standard]`` and ``SNSSink[fifo]``, as
    ``SQSSink`` is split, so it adds one variant; the ids stay unique.
    """
    assert len(cc.VARIANTS) == 56
    assert cc.ACK_CLASSES == (
        "keyed-reply",
        "positional-reply",
        "request-reply",
        "transactional",
        "local-write",
        "socket-stream",
        "per-event-ack",
        "client-buffered",
        "in-process",
        "wrapper",
    )
    assert all(v.ack in cc.ACK_CLASSES for v in cc.VARIANTS)
    assert {v.ack for v in cc.VARIANTS} == set(cc.ACK_CLASSES)
    ids = [v.id for v in cc.VARIANTS]
    assert len(ids) == len(set(ids)), collections.Counter(ids).most_common(3)
    assert len({v.sink for v in cc.VARIANTS}) == 39


def test_the_roster_has_no_gap() -> None:
    """FR-001: every class under `sinks/` with an `emit` is a variant's or `ABSTRACT`'s."""
    derived = {cls.name for _, cls in _sink_classes_with_emit()} - {"Sink"}
    declared = {v.sink for v in cc.VARIANTS} | set(cc.ABSTRACT)
    assert cc.roster_gap(derived, declared) == []
    assert len(derived) == 40, sorted(derived)


@pytest.mark.parametrize(
    ("derived", "declared", "lines"),
    [
        (
            {"HTTPSink", "ExampleSink"},
            {"HTTPSink"},
            ["ExampleSink ships under sinks/ with no entry in VARIANTS or ABSTRACT"],
        ),
        (
            {"HTTPSink"},
            {"HTTPSink", "GhostSink"},
            ["GhostSink has an entry in VARIANTS or ABSTRACT but no class under sinks/ defines it"],
        ),
        ({"HTTPSink", "SQSSink"}, {"SQSSink", "HTTPSink"}, []),
    ],
    ids=["a-class-with-no-entry", "an-entry-with-no-class", "equal-sets"],
)
def test_roster_gap_names_each_side(
    derived: set[str], declared: set[str], lines: list[str]
) -> None:
    """FR-001: `roster_gap`'s failure text, and its silence."""
    assert cc.roster_gap(derived, declared) == lines


def _nodes() -> dict[str, ast.ClassDef]:
    """Every class under `sinks/`, by name, as `test_public_surface` reads them.

    Args:
      None.

    Returns:
      The class nodes.

    Raises:
      None.
    """
    return {name: node for name, (_, node) in _sink_class_nodes().items()}


def test_the_parameters_are_the_sources_and_each_is_a_pick_or_neutral() -> None:
    """FR-001: the AST's parameters equal `PARAMETERS`, which picks and `NEUTRAL` cover exactly."""
    derived = cc.ast_parameters(_nodes(), _base_names, cc.PARAMETERS)
    assert cc.parameter_faults(derived, cc.PARAMETERS) == []
    assert set(cc.PARAMETERS) == {v.sink for v in cc.VARIANTS}
    assert cc.coverage_faults(cc.PARAMETERS, cc.VARIANTS, cc.NEUTRAL) == []
    assert set(cc.NEUTRAL) == set(cc.PARAMETERS)
    assert len(cc.PARAMETERS) == 39
    assert sum(len(names) for names in cc.PARAMETERS.values()) == 176
    if "max_message_bytes" in cc.PARAMETERS["SQSSink"]:
        assert "max_message_bytes" in cc.NEUTRAL["SQSSink"]
    if "max_message_bytes" in cc.PARAMETERS["SNSSink"]:
        assert "max_message_bytes" in cc.NEUTRAL["SNSSink"]


def test_parameter_faults_name_the_class_and_the_parameter() -> None:
    """FR-001: one extra parameter yields one line naming both; agreement yields none."""
    assert cc.parameter_faults({"ExampleSink": ("url", "extra")}, {"ExampleSink": ("url",)}) == [
        "ExampleSink gained the parameter extra"
    ]
    assert cc.parameter_faults({"ExampleSink": ()}, {"ExampleSink": ("url",)}) == [
        "ExampleSink lost the parameter url"
    ]
    assert cc.parameter_faults({"ExampleSink": ("url",)}, {"ExampleSink": ("url",)}) == []


def test_coverage_faults_name_an_undecided_and_a_doubly_decided_parameter() -> None:
    """FR-001: a parameter neither picked nor neutral, or both, is one line naming both."""
    variant = cc.Variant(
        sink="ExampleSink", variant="a", ack="request-reply", build="http", picks={"mode": "a"}
    )
    assert cc.coverage_faults(
        {"ExampleSink": ("mode", "url", "extra")}, [variant], {"ExampleSink": frozenset({"url"})}
    ) == ["ExampleSink: the parameter extra is neither a variant's pick nor in NEUTRAL"]
    assert cc.coverage_faults(
        {"ExampleSink": ("mode", "url")}, [variant], {"ExampleSink": frozenset({"url", "mode"})}
    ) == ["ExampleSink: the parameter mode is both a variant's pick and in NEUTRAL"]
    assert cc.coverage_faults(
        {"ExampleSink": ("mode", "url")},
        [variant],
        {"ExampleSink": frozenset({"url", "gone"})},
    ) == ["ExampleSink: gone is a pick or in NEUTRAL but not a parameter"]
    assert (
        cc.coverage_faults(
            {"ExampleSink": ("mode", "url")}, [variant], {"ExampleSink": frozenset({"url"})}
        )
        == []
    )


def test_ast_parameters_inherits_through_a_qualified_base() -> None:
    """FR-001: a class with no `__init__` takes its base's, however the base is spelled."""
    source = textwrap.dedent(
        """
        class BaseSink:
            def __init__(self, url, *extra, timeout=1.0, **kwargs):
                pass
        class MiddleSink(module.BaseSink):
            pass
        class LeafSink(MiddleSink):
            pass
        class BareSink:
            pass
        """
    )
    nodes = {n.name: n for n in ast.parse(source).body if isinstance(n, ast.ClassDef)}
    found = cc.ast_parameters(nodes, _base_names, ["LeafSink", "BareSink"])
    assert found == {"LeafSink": ("url", "timeout", "*extra", "**kwargs"), "BareSink": ()}


def _first_rows() -> list[cc.Row]:
    """Each variant's first row.

    Args:
      None.

    Returns:
      One row per variant, in table order.

    Raises:
      None.
    """
    seen: dict[str, cc.Row] = {}
    for row in cc.rows():
        seen.setdefault(row.variant.id, row)
    return list(seen.values())


def test_every_builder_builds_the_class_its_entry_names_and_abstract_is_only_a_base() -> None:
    """FR-001: each builder's class has its entry's name; `ABSTRACT` is built only as a base."""
    built = {row.variant.id: cb.run_row(row).built for row in _first_rows()}
    assert len(built) == 56
    wrong = {vid: cls.__name__ for vid, cls in built.items() if cls.__name__ != vid.split("[")[0]}
    assert not wrong, wrong
    for name in cc.ABSTRACT:
        assert name not in {v.sink for v in cc.VARIANTS}
        assert any(name in [c.__name__ for c in cls.__mro__] for cls in built.values()), name


# The rows (FR-002).


def test_the_corpus_generates_the_275_rows_of_fr_002_and_spec_093() -> None:
    """FR-002: 256 rows, by class as the spec's Evidence counts them, SPEC-093 FR-005's 12, and 7.

    The last seven are SPEC-100 FR-006's ``SNSSink[fifo]``, which runs ``keyed-reply``'s seven
    faults beside the renamed ``SNSSink[standard]``.

    Each variant's faults are its class's, then its `adds`, less its skips, and no skip names a
    fault the class does not run. The two confirmed RabbitMQ variants each add two faults to
    `per-event-ack`'s four, which makes `per-event-ack` 20 rows.
    """
    table = cc.rows()
    assert len(table) == 275
    assert len({row.id for row in table}) == 275
    by_class = collections.Counter(row.variant.ack for row in table)
    assert by_class == {
        "keyed-reply": 35,
        "positional-reply": 35,
        "request-reply": 60,
        "transactional": 16,
        "local-write": 16,
        "socket-stream": 28,
        "per-event-ack": 20,
        "client-buffered": 23,
        "in-process": 10,
        "wrapper": 32,
    }
    for variant in cc.VARIANTS:
        if variant.only:
            base = variant.only
            assert set(base) <= set(cc.APPLIES[variant.ack]), variant.id
        elif variant.ack == "wrapper":
            inner = cc.variant_named(variant.wraps or "")
            base = (*cc.APPLIES[inner.ack], *cc.APPLIES["wrapper"])
        else:
            base = cc.APPLIES[variant.ack]
        base = (*base, *variant.adds)
        assert set(variant.skips) <= set(base), f"{variant.id} skips a fault it never runs"
        generated = [row.fault for row in table if row.variant is variant]
        assert generated == [f for f in base if f not in variant.skips], variant.id


def test_every_clean_row_but_nullsinks_holds_each_event_exactly_once() -> None:
    """FR-002: a clean row whose backend did not hold each event once tests nothing."""
    checked = 0
    for row in cc.rows():
        if row.fault != "clean":
            continue
        result = cb.run_row(row)
        held = [result.backend.held[t] for t in result.events]
        if row.id == "NullSink-clean":
            assert held == [0] * len(result.events)
            continue
        assert held == [1] * len(result.events), (row.id, held)
        checked += 1
    assert checked == 53


def _dial_by_name() -> None:
    """Dials a host by name, which `socket.getaddrinfo` resolves first.

    Args:
      None.

    Returns:
      None.

    Raises:
      Exception: Whatever the guard or the dial raises.
    """
    socket.create_connection(("example.invalid", 9), timeout=0.01)


def _dial_by_address() -> None:
    """Connects to a literal address, which reaches `socket.socket.connect` and resolves nothing.

    Args:
      None.

    Returns:
      None.

    Raises:
      Exception: Whatever the guard or the connect raises.
    """
    with socket.socket() as sock:
        sock.connect(("127.0.0.1", 9))


def _dial_datagram() -> None:
    """Sends one datagram on a real UDP socket, which `connect` and `getaddrinfo` never see.

    Args:
      None.

    Returns:
      None.

    Raises:
      Exception: Whatever the guard or the send raises.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.sendto(b"x", ("127.0.0.1", 9))


def _network_probe(dial: Callable[[], None]) -> cd.Builder:
    """A builder of a `CallbackSink` whose callable dials out, absorbs the raise, and keeps it all.

    Args:
      dial: How the callable reaches the network.

    Returns:
      The builder.

    Raises:
      None.
    """

    def build(variant: cc.Variant, backend: cb.Backend, monkeypatch: Any) -> tuple[Any, Any]:
        """Builds the probe's `CallbackSink`.

        Args:
          variant: The probe's variant.
          backend: The row's backend.
          monkeypatch: The row's patch, unused.

        Returns:
          The sink, twice: it is its own judged sink.

        Raises:
          None.
        """
        from log_foundry.sinks.callback import CallbackSink

        def fn(batch: list[dict[str, object]]) -> None:
            """Dials, swallows what the guard raises, and keeps every event.

            Args:
              batch: The events.

            Returns:
              None.

            Raises:
              None.
            """
            try:
                dial()
            except Exception:
                pass
            backend.decide(cb.tokens(batch))
            backend.take(cb.tokens(batch))

        sink = CallbackSink(fn)
        return sink, sink

    return build


@pytest.mark.parametrize(
    ("dial", "seen"),
    [
        (_dial_by_name, "getaddrinfo .*example.invalid"),
        (_dial_by_address, "connect .*127.0.0.1"),
        (_dial_datagram, "send .*127.0.0.1"),
    ],
    ids=["getaddrinfo", "connect", "sendto"],
)
def test_a_row_that_reaches_the_network_is_a_harness_error(
    monkeypatch: pytest.MonkeyPatch, dial: Callable[[], None], seen: str
) -> None:
    """FR-002: the guard's raise is absorbable, so its record, on either seam, fails the row."""
    monkeypatch.setitem(cd.BUILDERS, "network-probe", _network_probe(dial))
    variant = cc.Variant(
        sink="CallbackSink", variant="probe", ack="in-process", build="network-probe"
    )
    with pytest.raises(cb.HarnessError, match=f"reached the network: .*{seen}"):
        cb.run_row(cc.Row(variant, "clean"))


_DRIVER_ROWS = tuple(
    f"{sink}-{fault}"
    for sink in ("SQSSink[standard]", "ClickHouseSink", "RedisListSink")
    for fault in ("taken-then-raised", "reply-lost")
)


def test_with_driver_classes_unrecognised_the_named_rows_change_verdict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-002: the rows reach the sinks' driver-class branch, which `()` would skip.

    `_classify` reads `sys` only for `sys.modules`, so a module-level `sys` with no modules makes
    `driver_classes` answer `()` everywhere, however a sink imported the function.
    """
    from log_foundry.sinks import _classify

    before = {row_id: cb.run_row(_row(row_id)).verdict for row_id in _DRIVER_ROWS}
    monkeypatch.setattr(_classify, "sys", types.SimpleNamespace(modules={}))
    after = {row_id: cb.run_row(_row(row_id)).verdict for row_id in _DRIVER_ROWS}
    unmoved = [row_id for row_id in _DRIVER_ROWS if before[row_id] == after[row_id]]
    assert not unmoved, (unmoved, before, after)


def test_each_wrapper_row_reads_as_its_inner_row() -> None:
    """FR-002: for every fault both run, a wrapper's verdict is its inner variant's."""
    pairs = []
    for variant in cc.VARIANTS:
        if variant.ack != "wrapper":
            continue
        inner = cc.variant_named(variant.wraps or "")
        shared = [f for f in cc.faults_of(variant) if f in cc.faults_of(inner)]
        pairs += [(cc.Row(variant, f), cc.Row(inner, f)) for f in shared]
    assert len(pairs) == 28
    differ = []
    for wrapper, inner in pairs:
        outer, under = cb.run_row(wrapper).verdict, cb.run_row(inner).verdict
        if outer != under:
            differ.append((wrapper.id, outer, inner.id, under))
    assert not differ, differ


def _seams() -> dict[str, object]:
    """Every seam a row patches, as it stands now.

    Args:
      None.

    Returns:
      Each seam's current object, by name.

    Raises:
      None.
    """
    from log_foundry import model
    from log_foundry.sinks import _engine, _socket, eventhubs
    from log_foundry.worker import Worker

    defaults = inspect.unwrap(Worker.__init__).__kwdefaults__ or {}
    found: dict[str, object] = {
        "_engine._run": _engine._run,
        "Worker._emit": Worker.__dict__["_emit"],
        "model.new_log_id": model.new_log_id,
        "socket.socket.connect": socket.socket.__dict__.get("connect"),
        "socket.getaddrinfo": socket.getaddrinfo,
        "flush_interval": defaults.get("flush_interval"),
        "_socket._make_tcp": _socket._make_tcp,
        "_socket._make_udp": _socket._make_udp,
        "eventhubs._event_data_cls": eventhubs._event_data_cls,
    }
    for name in (*cd.DRIVERS, "pika", "psycopg", "nats"):
        found[f"sys.modules[{name}]"] = sys.modules.get(name)
    return found


def test_every_seam_is_restored_when_a_row_returns() -> None:
    """FR-002: `run_row` patches under a `MonkeyPatch` of its own, undone as it returns."""
    before = _seams()
    for row_id in (
        "SyslogSink[tcp]-clean",
        "SyslogSink[udp]-clean",
        "AzureEventHubsSink-clean",
        "NATSSink[core]-clean",
        "PostgresSink[owned]-clean",
        "RabbitMQSink[owned]-clean",
    ):
        cb.run_row(_row(row_id))
    after = _seams()
    moved = {k: (before[k], after[k]) for k in before if before[k] is not after[k]}
    assert not moved, moved
    assert before["flush_interval"] == 1.0


@pytest.mark.parametrize(
    ("module", "name", "row_id"),
    [
        ("log_foundry.sinks._engine", "_run", "HTTPSink-clean"),
        ("log_foundry.model", "new_log_id", "HTTPSink-clean"),
        ("log_foundry.sinks._socket", "_make_tcp", "SyslogSink[tcp]-clean"),
        ("log_foundry.sinks._socket", "_make_udp", "SyslogSink[udp]-clean"),
        ("log_foundry.sinks.eventhubs", "_event_data_cls", "AzureEventHubsSink-clean"),
    ],
)
def test_a_seam_that_is_gone_fails_the_row(
    monkeypatch: pytest.MonkeyPatch, module: str, name: str, row_id: str
) -> None:
    """FR-002: a renamed seam raises rather than leaving the row unpatched."""
    import importlib

    monkeypatch.delattr(importlib.import_module(module), name)
    with pytest.raises(AttributeError, match=name):
        cb.run_row(_row(row_id))


def test_a_flush_interval_default_that_is_gone_fails_the_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-002: the default's key is asserted, since `setitem` would add a missing one silently."""
    from log_foundry.worker import Worker

    defaults = inspect.unwrap(Worker.__init__).__kwdefaults__
    assert defaults is not None
    monkeypatch.delitem(defaults, "flush_interval")
    with pytest.raises(AssertionError, match="flush_interval"):
        cb.run_row(_row("HTTPSink-clean"))


# The assertion (FR-003).


def test_the_checks_are_the_eight_of_fr_003_and_spec_095_in_order() -> None:
    """FR-003, and SPEC-095 FR-004: `resent-when-knowable` follows `ambiguous-when-knowable`."""
    assert tuple(name for name, _ in cb.CHECKS) == (
        "partition",
        "silent-loss",
        "duplicate",
        "lost-but-held",
        "unannounced",
        "ambiguous-when-knowable",
        "resent-when-knowable",
        "ledger",
    )


_E = ["e1", "e2"]
_BALANCED = {"delivered": 2, "skipped": 0, "sink_lost": 0, "batch_lost": 0}


def _backend(held: Mapping[str, int], unknowable: tuple[str, ...] = ()) -> cb.Backend:
    """A backend holding `held`, whose calls carried every event, `unknowable` ones unknowably.

    Args:
      held: Copies held, by event.
      unknowable: Events carried by an unknowable call.

    Returns:
      The backend.

    Raises:
      None.
    """
    backend = cb.Backend("clean")
    backend.held.update(held)
    backend.calls.append(cb.Call([t for t in _E if t not in unknowable], True))
    if unknowable:
        backend.calls.append(cb.Call(list(unknowable), False))
    return backend


def _per_event(fates: Mapping[str, cb.Fate] | None = None, **fields: Any) -> cb.Report:
    """A per-event report, all landed and announced unless a field says otherwise.

    Args:
      fates: The fates, or `None` for all landed.
      **fields: Report fields to override.

    Returns:
      The report.

    Raises:
      None.
    """
    values: dict[str, Any] = {
        "fates": dict(fates) if fates is not None else dict.fromkeys(_E, "landed"),
        "landed": 0,
        "lost": 0,
        "ambiguous": 0,
        "late": 0,
        "batch_losses": ((0, 0),),
        "late_lines": 0,
        "ledger_moves": _BALANCED,
        "ledger_expected": _BALANCED,
        "unaccounted": 0,
    }
    values.update(fields)
    return cb.Report(**values)


def _per_batch(**fields: Any) -> cb.Report:
    """A per-batch report, all landed unless a field says otherwise.

    Args:
      **fields: Report fields to override.

    Returns:
      The report.

    Raises:
      None.
    """
    values: dict[str, Any] = {
        "fates": None,
        "landed": 2,
        "lost": 0,
        "ambiguous": 0,
        "late": 0,
        "batch_losses": ((0, 0),),
        "late_lines": 0,
        "ledger_moves": _BALANCED,
        "ledger_expected": _BALANCED,
        "unaccounted": 0,
    }
    values.update(fields)
    return cb.Report(**values)


_ONCE = {"e1": 1, "e2": 1}
_TWICE = {"e1": 2, "e2": 1}

_PER_EVENT_CASES: list[tuple[str, cb.Backend, cb.Report, tuple[str, ...]]] = [
    ("passes", _backend(_ONCE), _per_event(), ()),
    ("partition-by-coverage", _backend(_ONCE), _per_event({"e1": "landed"}), ("partition",)),
    (
        "partition-by-delivery",
        _backend(_ONCE),
        _per_event(disagreement="reconstructed 2 landed against 1"),
        ("partition",),
    ),
    ("partition-by-late-count", _backend(_ONCE), _per_event(late=1, late_lines=1), ("partition",)),
    ("silent-loss", _backend({"e1": 1}), _per_event(), ("silent-loss",)),
    ("duplicate", _backend({"e1": 2, "e2": 1}), _per_event(), ("duplicate",)),
    (
        "lost-but-held",
        _backend(_ONCE),
        _per_event({"e1": "lost", "e2": "landed"}, batch_losses=((1, 1),)),
        ("lost-but-held",),
    ),
    (
        "skipped-but-held",
        _backend(_ONCE),
        _per_event({"e1": "skipped", "e2": "landed"}),
        ("lost-but-held",),
    ),
    (
        "unannounced-by-a-silent-batch",
        _backend({"e2": 1}),
        _per_event({"e1": "lost", "e2": "landed"}, batch_losses=((1, 0),)),
        ("unannounced",),
    ),
    (
        "unannounced-by-a-silent-late-count",
        _backend(_ONCE),
        _per_event(late=1, late_lines=0),
        ("partition", "unannounced"),
    ),
    (
        "ambiguous-when-knowable",
        _backend({"e2": 1}),
        _per_event({"e1": "ambiguous", "e2": "landed"}, batch_losses=((1, 1),)),
        ("ambiguous-when-knowable",),
    ),
    (
        "ambiguous-when-unknowable-passes",
        _backend({"e2": 1}, unknowable=("e1",)),
        _per_event({"e1": "ambiguous", "e2": "landed"}, batch_losses=((1, 1),)),
        (),
    ),
    ("ledger-by-unaccounted", _backend(_ONCE), _per_event(unaccounted=1), ("ledger",)),
    (
        "ledger-by-a-term",
        _backend(_ONCE),
        _per_event(ledger_moves={**_BALANCED, "delivered": 1, "sink_lost": 1}),
        ("ledger",),
    ),
    (
        "ledger-off-passes",
        _backend(_ONCE),
        _per_event(ledger_moves=None, ledger_expected=None, unaccounted=None),
        (),
    ),
    (
        "two-checks-in-order",
        _backend({"e1": 2}),
        _per_event(),
        ("silent-loss", "duplicate"),
    ),
    (
        "duplicate-counted-passes",
        _backend(_TWICE, unknowable=("e1",)),
        _per_event(resent={"e1": 1}, batch_resends=((1, 1),)),
        (),
    ),
    (
        "duplicate-beyond-its-count",
        _backend({"e1": 3, "e2": 1}, unknowable=("e1",)),
        _per_event(resent={"e1": 1}, batch_resends=((1, 1),)),
        ("duplicate",),
    ),
    (
        "resent-when-knowable",
        _backend(_TWICE),
        _per_event(resent={"e1": 1}, batch_resends=((1, 1),)),
        ("resent-when-knowable",),
    ),
    (
        "unannounced-by-a-silent-resend",
        _backend(_TWICE, unknowable=("e1",)),
        _per_event(resent={"e1": 1}, batch_resends=((1, 0),)),
        ("unannounced",),
    ),
    (
        "partition-by-the-resend-count",
        _backend(_TWICE, unknowable=("e1",)),
        _per_event(
            resent={"e1": 1},
            batch_resends=((1, 1),),
            disagreement="reconstructed 1 resend(s) against Delivery 0",
        ),
        ("partition",),
    ),
]

_PER_BATCH_CASES: list[tuple[str, cb.Backend, cb.Report, tuple[str, ...]]] = [
    ("passes", _backend(_ONCE), _per_batch(), ()),
    ("partition-by-coverage", _backend(_ONCE), _per_batch(landed=1), ("partition",)),
    (
        "partition-by-late-count",
        _backend({"e1": 1}),
        _per_batch(landed=1, late=1, late_lines=1),
        ("partition",),
    ),
    ("silent-loss", _backend({"e1": 1}), _per_batch(), ("silent-loss",)),
    ("duplicate", _backend({"e1": 2, "e2": 1}), _per_batch(), ("duplicate",)),
    (
        "lost-but-held",
        _backend(_ONCE),
        _per_batch(landed=1, lost=1, batch_losses=((1, 1),)),
        ("lost-but-held",),
    ),
    (
        "unannounced-by-a-silent-batch",
        _backend({"e1": 1}),
        _per_batch(landed=1, lost=1, batch_losses=((1, 0),)),
        ("unannounced",),
    ),
    (
        "unannounced-by-a-silent-late-count",
        _backend({"e1": 1}),
        _per_batch(landed=1, late=1, late_lines=0),
        ("partition", "unannounced"),
    ),
    ("ledger-by-unaccounted", _backend(_ONCE), _per_batch(unaccounted=-1), ("ledger",)),
    (
        "duplicate-counted-passes",
        _backend(_TWICE, unknowable=("e1",)),
        _per_batch(resent_counted=1, batch_resends=((1, 1),)),
        (),
    ),
    (
        "duplicate-beyond-its-count",
        _backend({"e1": 3, "e2": 1}, unknowable=("e1",)),
        _per_batch(resent_counted=1, batch_resends=((1, 1),)),
        ("duplicate",),
    ),
    (
        "resent-when-every-call-was-knowable",
        _backend(_TWICE),
        _per_batch(resent_counted=1, batch_resends=((1, 1),)),
        ("resent-when-knowable",),
    ),
    (
        "unannounced-by-a-silent-resend",
        _backend(_TWICE, unknowable=("e1",)),
        _per_batch(resent_counted=1, batch_resends=((1, 0),)),
        ("unannounced",),
    ),
    (
        "ledger-by-a-term",
        _backend(_ONCE),
        _per_batch(ledger_moves={**_BALANCED, "delivered": 0, "batch_lost": 2}),
        ("ledger",),
    ),
]


@pytest.mark.parametrize(
    ("backend", "report", "verdict"),
    [pytest.param(b, r, v, id=name) for name, b, r, v in _PER_EVENT_CASES],
)
def test_judge_per_event(backend: cb.Backend, report: cb.Report, verdict: tuple[str, ...]) -> None:
    """FR-003: each way each check fails per event, and a report that fails none."""
    assert cb.judge(_E, backend, report) == verdict


@pytest.mark.parametrize(
    ("backend", "report", "verdict"),
    [pytest.param(b, r, v, id=name) for name, b, r, v in _PER_BATCH_CASES],
)
def test_judge_per_batch(backend: cb.Backend, report: cb.Report, verdict: tuple[str, ...]) -> None:
    """FR-003: each way each check of the per-batch branch fails, and a report that fails none.

    `ambiguous-when-knowable` has no per-batch branch: a batch reports no ambiguous event.
    """
    assert cb.judge(_E, backend, report) == verdict


@pytest.mark.parametrize(
    ("removed", "row_id", "verdict"),
    [
        ("silent-loss", "SyslogSink[udp]-peer-closed", ()),
        ("duplicate", "PostgresSink[autocommit]-autocommit", ()),
        ("lost-but-held", "PostgresSink[borrowed]-reply-lost", ()),
        ("resent-when-knowable", "ClickHouseSink-refused-once", ()),
    ],
)
def test_a_check_removed_from_judge_frees_the_row_it_holds(
    monkeypatch: pytest.MonkeyPatch, removed: str, row_id: str, verdict: tuple[str, ...]
) -> None:
    """FR-003 and SPEC-095 FR-004: each check today's code needs is the one holding its named row.

    SPEC-092 named four and SPEC-095 added ``resent-when-knowable``. ``partition`` held only the
    four rows of ``KafkaSink`` and ``GooglePubSubSink`` that SPEC-094 turned green, so no shipped
    row reads it any more; it stays held by
    ``test_fate_of_misreading_rejected_is_caught_by_the_delivery_comparison`` and by ``judge``'s
    standing fixtures. ``silent-loss``'s row moved from the TCP ``SyslogSink``, which SPEC-093
    turned green, to the UDP one, a departure that still reads it.
    """
    held = cb.run_row(_row(row_id)).verdict
    assert removed in held, held
    monkeypatch.setattr(cb, "CHECKS", tuple(c for c in cb.CHECKS if c[0] != removed))
    assert cb.run_row(_row(row_id)).verdict == verdict


def _round(
    carried: list[str],
    outcomes: object,
    reached: tuple[str, ...] = (),
    run_id: int = 0,
    sink: object = "s",
) -> cb.Round:
    """A hand-built round: a `Verdict` of `outcomes` when they are a list, else `outcomes` as is.

    Args:
      carried: The tokens it carried.
      outcomes: Its outcomes, or a raw result.
      reached: The tokens it marked.
      run_id: Its run.
      sink: Its sink.

    Returns:
      The round.

    Raises:
      None.
    """
    from log_foundry.sinks.verdict import Verdict

    result = Verdict(outcomes=tuple(outcomes)) if isinstance(outcomes, list) else outcomes
    return cb.Round(sink, carried, result, run_id, frozenset(reached))


def _resend_cases() -> list[tuple[str, list[cb.Round], dict[str, int]]]:
    """`resends_from`'s fixture corpus: rounds and the resends they reconstruct to.

    Args:
      None.

    Returns:
      `(id, rounds, resends)` triples.

    Raises:
      None.
    """
    from log_foundry.sinks.verdict import Outcome

    a, r = Outcome.ACCEPTED, Outcome.RETRYABLE
    return [
        ("marked-then-offered", [_round(_E, [r, r], ("e1",)), _round(_E, [a, a])], {"e1": 1}),
        (
            "marked-twice",
            [_round(_E, [r, r], ("e1",)), _round(_E, [r, r], ("e1",)), _round(_E, [a, a])],
            {"e1": 2},
        ),
        ("unreadable", [_round(_E, None, ("e1", "e2")), _round(_E, [a, a])], {}),
        ("too-short", [_round(_E, [r], ("e1", "e2")), _round(_E, [a, a])], {}),
        ("not-retryable", [_round(_E, [a, r], ("e1",)), _round(_E, [a, a])], {}),
        (
            "raised-after-a-marked-round",
            [
                _round(_E, [r, r], ("e1",)),
                _round(_E, ConnectionError("x"), ("e1",)),
                _round(_E, [a, a]),
            ],
            {"e1": 1},
        ),
        (
            "another-run-and-another-sink",
            [
                _round(_E, [r, r], ("e1",), run_id=1),
                _round(_E, [r, r], ("e1",), sink="other"),
                _round(_E, [a, a]),
            ],
            {},
        ),
        ("never-offered-again", [_round(_E, [r, a], ("e1",))], {}),
    ]


@pytest.mark.parametrize(
    ("rounds", "resends"),
    [pytest.param(rounds, resends, id=name) for name, rounds, resends in _resend_cases()],
)
def test_resends_from_rebuilds_each_offer_after_a_mark(
    rounds: list[cb.Round], resends: dict[str, int]
) -> None:
    """SPEC-095 FR-004: the reconstruction counts an offer only after a readable mark."""
    assert cb.resends_from(rounds, "s", 0) == resends


@pytest.mark.parametrize(
    ("resent", "delivered", "found"),
    [
        ({"e1": 1}, 0, "reconstructed 1 resend(s) against Delivery 0"),
        ({}, 1, "reconstructed 0 resend(s) against Delivery 1"),
        ({"e1": 2, "e2": 1}, 3, None),
        ({}, 0, None),
    ],
    ids=["reconstructed-more", "delivery-more", "agree", "neither"],
)
def test_resends_agree_compares_the_reconstruction_with_the_delivery(
    resent: dict[str, int], delivered: int, found: str | None
) -> None:
    """SPEC-095 FR-004: the oracle, on a real `Delivery`, settled and failed alike."""
    from log_foundry.sinks import _engine

    for failed in (False, True):
        delivery = dataclasses.replace(_engine._SETTLED_EMPTY, resent=delivered, failed=failed)
        assert cb.resends_agree(resent, delivery) == found


def test_resends_agree_reads_a_delivery_without_the_field_as_zero() -> None:
    """SPEC-095 FR-004: the corpus still judges a tree that predates `Delivery.resent`."""
    assert cb.resends_agree({}, object()) is None
    assert cb.resends_agree({"e1": 1}, object()) == "reconstructed 1 resend(s) against Delivery 0"


def test_fate_of_misreading_rejected_is_caught_by_the_delivery_comparison(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-003: only the comparison with the engine's `Delivery` gives this row `partition`."""
    from log_foundry.sinks.verdict import Outcome

    assert cb.run_row(_row("SQSSink[standard]-partial")).verdict == ()
    monkeypatch.setitem(cb.FATES, Outcome.REJECTED, "landed")
    result = cb.run_row(_row("SQSSink[standard]-partial"))
    assert "partition" in result.verdict, result.verdict
    assert result.report.disagreement is not None


def test_duplicate_firing_at_one_copy_turns_every_green_clean_row_red(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-003: the boundary is "more than once"; at "at least once" every green clean row fails."""

    def at_least_once(events: list[str], backend: cb.Backend, report: cb.Report) -> bool:
        """`duplicate`'s condition moved to "held at least once".

        Args:
          events: The row's events.
          backend: The row's backend.
          report: The row's report.

        Returns:
          True when the backend holds any event.

        Raises:
          None.
        """
        return any(backend.held[t] >= 1 for t in events)

    monkeypatch.setattr(
        cb,
        "CHECKS",
        tuple((n, at_least_once if n == "duplicate" else p) for n, p in cb.CHECKS),
    )
    green_clean = [
        row
        for row in cc.rows()
        if row.fault == "clean" and cp.pin_of(row.id) is None and cp.departure_of(row.id) is None
    ]
    assert len(green_clean) == 53
    quiet = [row.id for row in green_clean if "duplicate" not in cb.run_row(row).verdict]
    assert not quiet, quiet


def test_the_corpus_files_hold_no_skip_no_loose_xfail_and_no_ledger_exemption() -> None:
    """FR-002, FR-003 and FR-004: nothing is skipped, every xfail is strict, every row balances."""
    for name in _CORPUS_FILES:
        text = (_ROOT / "tests" / name).read_text(encoding="utf-8")
        assert not skip_offenders(text), (name, skip_offenders(text))
        assert "mark.ledger_exempt" not in text.replace('"mark.ledger_exempt"', ""), name


def _variant(**kwargs: Any) -> cc.Variant:
    """A `per-event-ack` variant for `adds_faults`'s fixtures.

    Args:
      **kwargs: Fields beyond the class and names.

    Returns:
      The variant.

    Raises:
      None.
    """
    fields: dict[str, Any] = {"sink": "S", "variant": "v", "ack": "per-event-ack", "build": "b"}
    fields.update(kwargs)
    return cc.Variant(**fields)


_ADDS_MISUSE = {
    "already-runs": (
        _variant(adds=types.MappingProxyType({"clean": "r"})),
        "S[v] adds clean, which its class already runs",
    ),
    "beside-only": (
        _variant(only=("clean",), adds=types.MappingProxyType({"peer-closed": "r"})),
        "S[v] adds peer-closed beside only, which replaces its faults",
    ),
    "also-skipped": (
        _variant(
            adds=types.MappingProxyType({"peer-closed": "r"}),
            skips=types.MappingProxyType({"peer-closed": "r"}),
        ),
        "S[v] adds peer-closed and also skips it",
    ),
    "not-a-fault": (
        _variant(adds=types.MappingProxyType({"lost-in-the-post": "r"})),
        "S[v] adds 'lost-in-the-post', which is not a fault",
    ),
}


@pytest.mark.parametrize("case", sorted(_ADDS_MISUSE))
def test_adds_faults_names_each_misuse_of_adds(case: str) -> None:
    """SPEC-093 FR-005: each misuse of `adds` is one line naming the variant and the fault."""
    variant, line = _ADDS_MISUSE[case]
    assert cc.adds_faults([variant]) == [line]


def test_adds_faults_is_silent_on_a_well_formed_table() -> None:
    """A fault its class does not run, with no `only` and no `skips` of it, reads as fine."""
    variant = _variant(adds=types.MappingProxyType({"peer-closed": "r", "taken-then-raised": "r"}))
    assert cc.adds_faults([variant]) == []
    assert cc.faults_of(variant, [variant]) == (
        "clean",
        "refused-once",
        "refused-always",
        "reply-lost",
        "peer-closed",
        "taken-then-raised",
    )


def test_the_variant_table_uses_adds_as_specified() -> None:
    """`adds_faults` applied to `VARIANTS` yields nothing."""
    assert cc.adds_faults(cc.VARIANTS) == []


# The register (FR-004).


def test_the_register_holds_the_totals_of_fr_004() -> None:
    """The totals as SPEC-095, then SPEC-093 and SPEC-094, leave them.

    SPEC-092 FR-004 asserted 12 pins over 74 rows, 7 departures over 12 rows and 170 that must
    pass, of 256. SPEC-095 FR-004 brought it to 11 pins over 22 rows, 12 departures over 31 and
    203 that must pass. SPEC-093 FR-005 and SPEC-094 FR-003, landing after it, add twelve rows and
    remove seven pins over 16 rows, and add two departures over 3: 268 rows, 4 pins over 6 rows
    and three items, 14 departures over 34 rows, and 228 that must pass. SPEC-100 FR-006 then adds
    ``SNSSink[fifo]``'s seven rows, no pin and no departure: two of them join
    ``aws-client-retries``, as ``SQSSink[fifo]``'s do, and five must pass, which makes 275 rows,
    36 departure rows and 233 that must pass.
    """
    assert len(cp.PINS) == 4
    assert len(cp.DEPARTURES) == 14
    pinned = [row for pin in cp.PINS for row in pin.rows]
    departing = [row for d in cp.DEPARTURES for row in d.rows]
    assert len(pinned) == 6
    assert len(departing) == 36
    assert len(cc.rows()) - len(pinned) - len(departing) == 233
    assert len({pin.item for pin in cp.PINS}) == 3


def test_each_sns_fifo_row_reads_as_its_sqs_fifo_row() -> None:
    """SPEC-100 FR-006: ``SNSSink[fifo]`` runs ``SQSSink[fifo]``'s faults and reads as it, row by row.

    Derived from the two variants' fault lists rather than seven written verdicts, so a fault one
    variant gains, or a verdict one variant changes, reddens here; the pin and the departure each
    row is held by must agree too.
    """
    sns_fifo = cc.variant_named("SNSSink[fifo]")
    sqs_fifo = cc.variant_named("SQSSink[fifo]")
    faults = cc.faults_of(sns_fifo)
    assert faults == cc.faults_of(sqs_fifo)
    assert len(faults) == 7
    for fault in faults:
        sns_row, sqs_row = cc.Row(sns_fifo, fault), cc.Row(sqs_fifo, fault)
        assert cb.run_row(sns_row).verdict == cb.run_row(sqs_row).verdict, fault
        assert _held_by(sns_row.id) == _held_by(sqs_row.id), fault


def _held_by(row_id: str) -> tuple[str | None, str | None]:
    """The names of the pin and the departure that hold a row, ``None`` for each that does not."""
    pin, departure = cp.pin_of(row_id), cp.departure_of(row_id)
    return (pin.name if pin else None, departure.name if departure else None)


def test_no_pin_or_departure_names_a_bare_sns_row() -> None:
    """SPEC-100 FR-006: the rename leaves no ``SNSSink-<fault>`` in the register, and no variant."""
    assert "SNSSink" not in {v.id for v in cc.VARIANTS}
    assert {"SNSSink[standard]", "SNSSink[fifo]"} <= {v.id for v in cc.VARIANTS}
    named = [row for pin in cp.PINS for row in pin.rows] + [
        row for d in cp.DEPARTURES for row in d.rows
    ]
    assert not [row for row in named if row.startswith("SNSSink-")]
    retries = next(d for d in cp.DEPARTURES if d.name == "aws-client-retries")
    assert len(retries.rows) == 12
    assert {
        "SNSSink[standard]-refused-once",
        "SNSSink[standard]-refused-always",
        "SNSSink[fifo]-refused-once",
        "SNSSink[fifo]-refused-always",
    } <= set(retries.rows)


def _repository_texts() -> dict[str, str]:
    """The register's files, read from the repository.

    Args:
      None.

    Returns:
      Each file's text, by repository-relative path.

    Raises:
      OSError: When a record file is missing.
    """
    paths = {cp.ARCHITECTURE, *(d.record[0] for d in cp.DEPARTURES)}
    return {path: (_ROOT / path).read_text(encoding="utf-8") for path in paths}


def test_the_register_is_sound_over_the_repository() -> None:
    """FR-004: every entry's rows are generated and owned once, items open, phrases present."""
    rows = [row.id for row in cc.rows()]
    assert cp.register_faults(rows, cp.PINS, cp.DEPARTURES, _repository_texts()) == []


_FIXTURE_ARCH = textwrap.dedent(
    """\
    ## 12. Open items

    ### Open

    - **An open item whose bold lead wraps
      across two lines** (a finding). Its text.
    - ~~**A struck item**~~ closed by an earlier spec.

    ### Resolved

    - **A resolved item** long ago.
    """
)
_FIXTURE_RECORD = "docs/record.md"
_FIXTURE_TEXTS = {
    cp.ARCHITECTURE: _FIXTURE_ARCH,
    _FIXTURE_RECORD: "It says a phrase that\nwraps here, in its own words.",
}


def _pin(rows: tuple[str, ...], item: str, name: str = "p") -> cp.Pin:
    """A fixture pin.

    Args:
      rows: Its rows.
      item: Its item.
      name: Its name.

    Returns:
      The pin.

    Raises:
      None.
    """
    return cp.Pin(name=name, rows=rows, verdict=("duplicate",), item=item)


def _departure(rows: tuple[str, ...], phrase: str, name: str = "d") -> cp.Departure:
    """A fixture departure whose record is the fixture record file.

    Args:
      rows: Its rows.
      phrase: The phrase it quotes.
      name: Its name.

    Returns:
      The departure.

    Raises:
      None.
    """
    return cp.Departure(
        name=name, rows=rows, verdict=("silent-loss",), record=(_FIXTURE_RECORD, phrase)
    )


_OPEN = "An open item whose bold lead wraps across two lines"
_PHRASE = "a phrase that wraps here"


@pytest.mark.parametrize(
    ("pins", "departures", "lines"),
    [
        pytest.param(
            [_pin(("A-clean",), _OPEN)],
            [_departure(("A-clean",), _PHRASE)],
            ["A-clean is in 2 entries: p, d"],
            id="a-row-in-two-entries",
        ),
        pytest.param(
            [_pin(("Ghost-clean",), _OPEN)],
            [],
            ["p names Ghost-clean, which the table does not generate"],
            id="a-row-the-table-does-not-generate",
        ),
        pytest.param(
            [_pin(("A-clean",), "An open item whose bold lead wrapps across two lines")],
            [],
            [
                (
                    "p's item is not the lead of an open §12 bullet: "
                    "An open item whose bold lead wrapps across two lines"
                )
            ],
            id="a-misspelled-item",
        ),
        pytest.param(
            [_pin(("A-clean",), "A struck item")],
            [],
            ["p's item is struck through in §12: A struck item"],
            id="a-struck-item",
        ),
        pytest.param(
            [_pin(("A-clean",), "A resolved item")],
            [],
            ["p's item is not the lead of an open §12 bullet: A resolved item"],
            id="a-resolved-item",
        ),
        pytest.param(
            [],
            [_departure(("A-clean",), "a phrase the file lacks")],
            [f"d's phrase is not in {_FIXTURE_RECORD}: a phrase the file lacks"],
            id="a-phrase-the-file-lacks",
        ),
        pytest.param(
            [],
            [
                cp.Departure(
                    name="d",
                    rows=("A-clean",),
                    verdict=("silent-loss",),
                    record=("docs/absent.md", _PHRASE),
                )
            ],
            ["d's record docs/absent.md is not in the texts read"],
            id="a-record-file-not-read",
        ),
        pytest.param(
            [_pin(("A-clean",), _OPEN)],
            [_departure(("B-clean",), _PHRASE)],
            [],
            id="a-wrapped-lead-and-a-differently-wrapped-phrase-are-found",
        ),
    ],
)
def test_register_faults_names_each_problem_and_only_those(
    pins: list[cp.Pin], departures: list[cp.Departure], lines: list[str]
) -> None:
    """FR-004: `register_faults`'s failure text, and its silence."""
    assert cp.register_faults(["A-clean", "B-clean"], pins, departures, _FIXTURE_TEXTS) == lines


def test_register_faults_names_a_missing_section_12() -> None:
    """FR-004: an architecture text without the two headings is a line, not a silent pass."""
    texts = {cp.ARCHITECTURE: "## 12. Open items\n\nnothing here\n"}
    assert cp.register_faults(["A-clean"], [_pin(("A-clean",), _OPEN)], [], texts) == [
        f"{cp.ARCHITECTURE} has no §12 between `### Open` and `### Resolved`"
    ]


# The row's decision (FR-004) and the loss-line count (FR-003).


def test_settle_holds_a_pinned_row_only_at_its_pins_exact_verdict() -> None:
    """FR-004: the pure decision behind the strict mark, at, beside and without the pin's verdict."""
    pin = cp.Pin(name="p", rows=("A-clean",), verdict=("silent-loss",), item="i")
    with pytest.raises(PinnedVerdict):
        cp.settle("A-clean", ("silent-loss",), [pin], [])
    for other in ((), ("duplicate",), ("silent-loss", "duplicate"), ("partition", "silent-loss")):
        with pytest.raises(AssertionError) as caught:
            cp.settle("A-clean", other, [pin], [])
        assert not isinstance(caught.value, PinnedVerdict), other


def test_settle_holds_a_departure_row_only_at_its_departures_exact_verdict() -> None:
    """FR-004: a departure passes at its verdict only, never at another, a wider one or a pass."""
    departure = cp.Departure(
        name="d", rows=("A-clean",), verdict=("silent-loss",), record=("f", "phrase")
    )
    cp.settle("A-clean", ("silent-loss",), [], [departure])
    for other in ((), ("duplicate",), ("silent-loss", "duplicate"), ("partition", "silent-loss")):
        with pytest.raises(AssertionError):
            cp.settle("A-clean", other, [], [departure])


def test_settle_passes_an_unlisted_row_only_when_it_is_green() -> None:
    """FR-004: a row in no entry passes at `()` and fails at any check."""
    cp.settle("A-clean", (), [], [])
    with pytest.raises(AssertionError):
        cp.settle("A-clean", ("ledger",), [], [])


def test_every_pinned_row_and_only_those_carry_the_strict_mark() -> None:
    """FR-004: each pinned row's parameter carries one strict `xfail` expecting `PinnedVerdict`."""
    import test_conformance

    marked = 0
    for param in test_conformance._params():
        (row,) = param.values  # type: ignore[attr-defined]
        marks = list(param.marks)  # type: ignore[attr-defined]
        if cp.pin_of(row.id) is None:
            assert not marks, row.id
            continue
        (mark,) = marks
        assert mark.name == "xfail", row.id
        assert mark.kwargs["strict"] is True, row.id
        assert mark.kwargs["raises"] is PinnedVerdict, row.id
        marked += 1
    assert marked == 6


@pytest.mark.parametrize(
    ("text", "count"),
    [
        ("log-foundry: lost 8 event(s) (HTTPSink, batch abandoned after 4 emit attempts)\n", 1),
        ("log-foundry: lost 1 request(s) (HTTPSink)\nlog-foundry: lost 2 event(s) (x)\n", 2),
        ("log-foundry: retrying a batch (HTTPSink)\n", 0),
        ("a line that says log-foundry: lost 3 event(s) inside other text\n", 0),
        ("log-foundry: nothing was lost 3 times\n", 0),
        ("", 0),
    ],
    ids=[
        "one-loss-line",
        "two-loss-lines",
        "an-ordinary-line",
        "lost-inside-text",
        "lost-later",
        "empty",
    ],
)
def test_lines_of_counts_loss_lines_and_nothing_else(text: str, count: int) -> None:
    """FR-003: `unannounced` reads loss lines, each starting `log-foundry: lost N `."""
    assert cb.lines_of(text) == count


@pytest.mark.parametrize(
    ("payload", "found"),
    [
        ('{"log_id": "c92e0001", "trace_id": "0af7651916cd43dd8448eb211c80319c"}', ["c92e0001"]),
        ('{"log_id": "c92e0001", "trace_id": "0af76519c92e1234d8448eb211c80319"}', ["c92e0001"]),
        ('{"log_id": "c92e0001", "span_id": "c92e12345b7ad6b7"}', ["c92e0001"]),
        ('{"log_id": "c92e0001", "span_id": "00f067aac92e1234"}', ["c92e0001"]),
        ('["c92e0002", "c92e0001", "c92e0002"]', ["c92e0002", "c92e0001"]),
    ],
    ids=["plain", "inside-a-trace-id", "starts-a-span-id", "ends-a-span-id", "first-seen-once"],
)
def test_tokens_are_never_read_inside_a_hex_id(payload: str, found: list[str]) -> None:
    """A `c92e` run inside a random hex trace or span id is not an event's token."""
    assert cb.tokens(payload) == found


# The harness refuses a row it cannot judge (FR-002, FR-003).


def test_a_submission_split_across_worker_batches_is_a_harness_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-002: a split submission would be judged as two batches, so the row is refused instead.

    A `flush()` inside the traced call sweeps the open span's first events into a batch of their
    own (SPEC-091), which splits the row's one submission deterministically.
    """
    import log_foundry

    original = log_foundry.error
    calls = [0]

    def error(*args: Any, **kwargs: Any) -> None:
        """Logs, and after the third event flushes the open span's events so far.

        Args:
          *args: `error`'s arguments.
          **kwargs: Its keyword arguments.

        Returns:
          None.

        Raises:
          None.
        """
        original(*args, **kwargs)
        calls[0] += 1
        if calls[0] == 3:
            log_foundry.flush(timeout=5.0)

    monkeypatch.setattr(log_foundry, "error", error)
    with pytest.raises(cb.HarnessError, match=r"in 2 worker batch\(es\), not 1"):
        cb.run_row(_row("HTTPSink-clean"))


def _ignoring(variant: cc.Variant, backend: cb.Backend, monkeypatch: Any) -> tuple[Any, Any]:
    """A `CallbackSink` whose callable asks for its action and then keeps every event regardless.

    Args:
      variant: The fixture's variant.
      backend: The row's backend.
      monkeypatch: The row's patch, unused.

    Returns:
      The sink, twice.

    Raises:
      None.
    """
    from log_foundry.sinks.callback import CallbackSink

    def fn(batch: list[dict[str, object]]) -> None:
        """Decides, ignores the action, and keeps the batch.

        Args:
          batch: The events.

        Returns:
          None.

        Raises:
          None.
        """
        backend.decide(cb.tokens(batch))
        backend.take(cb.tokens(batch))

    sink = CallbackSink(fn)
    return sink, sink


def test_a_double_that_ignores_its_action_is_a_harness_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-002: a decided action no double applied would let the row pass as a clean one."""
    monkeypatch.setitem(cd.BUILDERS, "ignoring", _ignoring)
    variant = cc.Variant(
        sink="CallbackSink", variant="ignoring", ack="in-process", build="ignoring"
    )
    assert cb.run_row(cc.Row(variant, "clean")).verdict == ()
    with pytest.raises(cb.HarnessError, match="call 0 was decided 'refuse' and never applied"):
        cb.run_row(cc.Row(variant, "refused-once"))


def test_every_rows_decided_actions_were_applied() -> None:
    """FR-002: across the corpus, every action other than `ok` was carried out by its double.

    `run_row` refuses a row with an unapplied action, so this holds by every row passing; it is
    asserted here over the actions themselves, so a corpus that decided none would fail too.
    """
    decided: collections.Counter[str] = collections.Counter()
    for row in cc.rows():
        backend = cb.run_row(row).backend
        assert backend.unapplied() == [], row.id
        decided.update(call.action for call in backend.calls if call.action != "ok")
    assert set(decided) == {"refuse", "partial", "taken-raise", "reply-lost", "noshape", "nack"}


def test_a_row_read_per_event_in_one_batch_and_per_batch_in_another_is_a_harness_error() -> None:
    """FR-003: `_report` refuses to mix the two readings into one report."""
    target = object()
    delivery = types.SimpleNamespace(
        failed=False,
        accepted=1,
        unknown=0,
        skipped=0,
        rejected=0,
        dropped=0,
        unresolved=0,
        raised=0,
    )
    recorder = object.__new__(cb.Recorder)
    recorder.runs = [cb.Run(target, delivery, 0)]
    recorder.rounds = []
    handled = [
        cb.Batch(["e1"], target, (0, 1), 0, 0, (0, 0), 0, 0),
        cb.Batch(["e2"], target, (1, 1), 0, 0, (0, 0), 0, 0),
    ]
    with pytest.raises(cb.HarnessError, match="ran the engine in some batches and not others"):
        cb._report(handled, recorder, object(), target, "")
