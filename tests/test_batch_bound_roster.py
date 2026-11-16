"""SPEC-090 FR-004: every shipped sink takes a capped batch whole, and a per-event sink stays short.

Under SPEC-090 a batch of up to ``_BACKLOG_BATCH_CAP`` (1,000) events becomes frequent under a
backlog. No sink is handed a batch larger than one it already received, since a batch past the
cap holds at most ``batch_size`` submissions, which the count trigger alone also sends; what
changes is how often a sink sees one. So each record-building sink is handed 1,000 events through
its double and must deliver all of them, the HTTP family must split them into the requests its
limits predict, and a JetStream ``NATSSink``, whose one ``publish_timeout`` bounds a whole call,
must keep each call short. The table of how each public sink takes a batch is derived from the
tree in both directions, so a new sink cannot ship without saying how it takes one.
"""

from __future__ import annotations

import asyncio
import json
import math
import time
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry
from log_foundry.sinks.base import read_losses
from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.nats import NATSSink
from sink_rigs import RIGS, ok
from test_sink_concurrency import _sink_classes_with_an_emit
from test_sinks_http import FakeOpener, FakeResponse
from test_sinks_http_render import _RECIPES, _shipped_http_subclasses

if TYPE_CHECKING:
    import pathlib

_CAPPED = 1_000

_HTTP = "requests of at most 1,000 events and the class's byte limit"
_SHARDS = "requests of at most 500 records"
_TEN = "requests of at most 10 entries"
_CHUNKS = "inserts of at most chunk_size, 1,000 by default"
_ROUND = "one insert_many, executemany or pipeline per round"
_PER_EVENT_HANDOFF = "one hand-off or publish per event"
_PER_EVENT_LINE = "one line, record or frame per event"
_AS_GIVEN = "the list as given"
_WRAPPED = "whatever the sinks they wrap do"

BATCH_SHAPES: dict[str, str] = {
    "HTTPSink": _HTTP,
    "ElasticsearchSink": _HTTP,
    "OpenSearchSink": _HTTP,
    "DatadogSink": _HTTP,
    "HoneycombSink": _HTTP,
    "NewRelicSink": _HTTP,
    "LokiSink": _HTTP,
    "SplunkHECSink": _HTTP,
    "OTLPSink": _HTTP,
    "LogstashSink": "as the HTTP family in HTTP mode; one frame per event in socket mode",
    "KinesisSink": _SHARDS,
    "FirehoseSink": _SHARDS,
    "SQSSink": _TEN,
    "SNSSink": _TEN,
    "ClickHouseSink": _CHUNKS,
    "PostgresSink": _CHUNKS,
    "AzureEventHubsSink": "as many EventDataBatch packs as the hub's size limit needs",
    "MongoDBSink": _ROUND,
    "SQLiteSink": _ROUND,
    "RedisStreamsSink": _ROUND,
    "RedisListSink": _ROUND,
    "KafkaSink": _PER_EVENT_HANDOFF,
    "RabbitMQSink": _PER_EVENT_HANDOFF,
    "GooglePubSubSink": _PER_EVENT_HANDOFF,
    "SentrySink": _PER_EVENT_HANDOFF,
    "NATSSink": _PER_EVENT_HANDOFF,
    "FileSink": _PER_EVENT_LINE,
    "RotatingFileSink": _PER_EVENT_LINE,
    "StdoutSink": _PER_EVENT_LINE,
    "StderrSink": _PER_EVENT_LINE,
    "SyslogSink": _PER_EVENT_LINE,
    "LoggingSink": _PER_EVENT_LINE,
    "MemorySink": _AS_GIVEN,
    "NullSink": _AS_GIVEN,
    "CallbackSink": _AS_GIVEN,
    "MultiSink": _WRAPPED,
    "FilteringSink": _WRAPPED,
    "TransformSink": _WRAPPED,
}
"""How one batch reaches the destination, for every public sink: SPEC-090 FR-004's table."""

MISSING = "a public sink class names no batch shape"
STALE = "a batch shape names no public sink class"


def public_sinks(root: pathlib.Path | None = None) -> set[str]:
    """Every public sink class under ``root``: in scope for the roster, with no private name.

    A class whose own name or whose module's name starts with an underscore is a helper, not a
    sink a caller configures, so it names no shape.

    Args:
      root: The directory to scan, the installed ``sinks/`` unless given.

    Returns:
      The class names.
    """
    return {
        cls.name
        for stem, cls in _sink_classes_with_an_emit(root)
        if not stem.startswith("_") and not cls.name.startswith("_")
    }


def batch_table_findings(classes: set[str], table: dict[str, str]) -> list[tuple[str, str]]:
    """Every public sink the table leaves out, and every entry naming no public sink.

    Args:
      classes: The public sink classes the tree defines.
      table: The batch shape of each, by class name.

    Returns:
      One ``(reason, class name)`` pair per finding, sorted.
    """
    found = [(MISSING, name) for name in classes - table.keys()]
    found += [(STALE, name) for name in table.keys() - classes]
    return sorted(found)


def test_every_public_sink_names_how_it_takes_a_batch() -> None:
    """FR-004 AC3: the tree's public sinks and the table are the same set."""
    classes = public_sinks()
    assert len(classes) >= 38, f"the roster shrank: {sorted(classes)}"
    assert batch_table_findings(classes, BATCH_SHAPES) == []


_CORPUS_MODULES = {
    "alpha.py": "class AlphaSink:\n    def emit(self, batch):\n        pass\n",
    "_helper.py": "class HelperTransport:\n    def close(self):\n        pass\n",
    "beta.py": "class _BetaBase:\n    def emit(self, batch):\n        pass\n",
}

_TABLE_CORPUS: dict[str, tuple[dict[str, str], list[tuple[str, str]]]] = {
    "missing": ({}, [(MISSING, "AlphaSink")]),
    "stale": ({"AlphaSink": "x", "GoneSink": "y"}, [(STALE, "GoneSink")]),
    "private names are silent": ({"AlphaSink": "x"}, []),
}


@pytest.mark.parametrize("case", sorted(_TABLE_CORPUS))
def test_the_batch_table_refuses_its_shapes(case: str, tmp_path: pathlib.Path) -> None:
    """Each check fires for its own reason over a real module tree; private names pass silent."""
    for name, text in _CORPUS_MODULES.items():
        (tmp_path / name).write_text(text, encoding="utf-8")
    table, expected = _TABLE_CORPUS[case]
    assert batch_table_findings(public_sinks(tmp_path), table) == expected, case


@pytest.mark.parametrize("name", sorted(RIGS))
def test_every_rigged_sink_takes_a_capped_batch_whole(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    """FR-004 AC1: 1,000 events in one call all arrive, and no loss counter moves."""
    rig = RIGS[name](monkeypatch, tmp_path)
    before = read_losses(rig.sink)
    batch = [ok(str(i)) for i in range(_CAPPED)]
    rig.sink.emit(batch)
    assert sorted(rig.delivered()) == sorted(str(i) for i in range(_CAPPED))
    assert read_losses(rig.sink) == before


class _EveryEntryOpener(FakeOpener):
    """An opener answering Honeycomb's batch API: one accepted status per entry it was sent."""

    def __call__(self, request: Any, timeout: float | None = None) -> FakeResponse:
        """Records the request, then accepts every entry in its body."""
        super().__call__(request, timeout)
        entries = json.loads(request.data)
        return FakeResponse(200, json.dumps([{"status": 202}] * len(entries)).encode())


def _http_family() -> list[type[HTTPSink]]:
    """``HTTPSink`` and every shipped subclass."""
    return [HTTPSink, *_shipped_http_subclasses()]


@pytest.mark.parametrize("cls", _http_family(), ids=lambda cls: cls.__name__)
def test_the_http_family_takes_a_capped_batch_in_predicted_requests(cls: type[HTTPSink]) -> None:
    """FR-004 AC2: 1,000 small events go in the requests the count limit predicts, none lost."""
    opener = _EveryEntryOpener() if cls.__name__ == "HoneycombSink" else FakeOpener()
    if cls is HTTPSink:
        sink = HTTPSink("http://x/logs", opener=opener)
    else:
        sink = _RECIPES[cls.__name__](opener=opener)
    before = read_losses(sink)
    sink.emit([ok(str(i)) for i in range(_CAPPED)])
    assert len(opener.calls) == math.ceil(_CAPPED / sink.max_batch_count), cls.__name__
    assert all(len(call["body"]) <= sink.max_batch_bytes for call in opener.calls)
    assert read_losses(sink) == before


class _JetStream:
    """A JetStream context whose publish waits the ack time."""

    def __init__(self, client: _NatsClient, ack: float) -> None:
        self.client = client
        self.ack = ack

    async def publish(
        self,
        subject: str,
        payload: bytes,
        *,
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> None:
        """Waits the ack, then counts it."""
        await asyncio.sleep(self.ack)
        self.client.acked += 1


class _NatsClient:
    """An injected NATS client that is always connected; its JetStream acks after a delay."""

    def __init__(self, ack: float) -> None:
        self.is_connected = True
        self.pending_data_size = 0
        self.stats = {"reconnects": 0}
        self.acked = 0
        self.ack = ack

    async def publish(self, subject: str, payload: bytes) -> None:
        """Counts a core publish."""
        self.acked += 1

    def jetstream(self) -> _JetStream:
        """The JetStream context."""
        return _JetStream(self, self.ack)

    async def flush(self, timeout: float | None = None) -> None:  # noqa: ASYNC109
        """Nothing is buffered."""

    async def close(self) -> None:
        """Nothing to close."""

    async def drain(self) -> None:
        """Nothing to drain."""


class _TimedNATS(NATSSink):
    """A ``NATSSink`` that records how long each ``deliver`` call took."""

    durations: list[float]

    def deliver(self, batch: list[dict[str, object]]) -> Any:
        """Delivers as the sink does, timing the call."""
        began = time.monotonic()
        try:
            return super().deliver(batch)
        finally:
            self.durations.append(time.monotonic() - began)


@log_foundry.trace
def _work() -> None:
    pass


def test_a_jetstream_nats_sink_behind_a_slow_ack_keeps_its_calls_short() -> None:
    """FR-004 AC4, in the suite: ``spec090_nats_ack_backlog.py 0.002 200 10``.

    A JetStream ``NATSSink`` behind a 2 ms ack, with the default ``publish_timeout``, sent 200
    spans through the library's own worker: all 400 events are acked, the sink counts nothing
    failed, and no ``deliver`` call lasts as long as a second.
    """
    client = _NatsClient(0.002)
    sink = _TimedNATS("s", client=client, jetstream=True)
    sink.durations = []
    log_foundry.configure(sink=sink)
    for _ in range(200):
        _work()
    assert log_foundry.flush(60)
    health = log_foundry.health()
    assert client.acked == 400
    assert health.sink is not None and health.sink.failed == 0
    assert max(sink.durations) < 1.0, sink.durations
