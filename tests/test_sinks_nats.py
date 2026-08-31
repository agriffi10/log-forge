"""SPEC-010 — NATSSink: sync-driven publish, JetStream path, drain-on-close (fake client)."""

from __future__ import annotations

import json

import pytest

from log_foundry.sinks.base import Sink, SinkDeliveryError
from log_foundry.sinks.nats import NATSSink


class FakeJetStream:
    def __init__(self, owner: FakeNATS) -> None:
        self._owner = owner

    async def publish(self, subject, payload) -> None:
        self._owner.js_published.append((subject, payload))


class FakeNATS:
    def __init__(self, fail: bool = False) -> None:
        self.published: list[tuple] = []
        self.js_published: list[tuple] = []
        self.drained = False
        self._fail = fail

    async def publish(self, subject, payload) -> None:
        if self._fail:
            raise RuntimeError("no responders")
        self.published.append((subject, payload))

    def jetstream(self) -> FakeJetStream:
        return FakeJetStream(self)

    async def drain(self) -> None:
        self.drained = True


def test_is_a_sink() -> None:
    sink = NATSSink("subject", client=FakeNATS())
    assert isinstance(sink, Sink)
    sink.close()


def test_publishes_one_message_per_event() -> None:
    client = FakeNATS()
    sink = NATSSink("logs", client=client)
    sink.emit([{"a": 1}, {"a": 2}])
    sink.close()
    assert client.published == [
        ("logs", json.dumps({"a": 1}).encode("utf-8")),
        ("logs", json.dumps({"a": 2}).encode("utf-8")),
    ]


def test_jetstream_path_publishes_via_jetstream() -> None:
    client = FakeNATS()
    sink = NATSSink("logs", client=client, jetstream=True)
    sink.emit([{"a": 1}])
    sink.close()
    assert client.js_published == [("logs", json.dumps({"a": 1}).encode("utf-8"))]
    assert client.published == []


def test_publish_errors_counted(capsys) -> None:
    client = FakeNATS(fail=True)
    sink = NATSSink("logs", client=client)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}, {"a": 2}])  # nothing published (SPEC-026 FR-001)
    sink.close()
    assert sink.failed == 2
    assert capsys.readouterr().err.count("lost 1 event(s)") == 2


def test_close_drains_the_connection() -> None:
    client = FakeNATS()
    sink = NATSSink("logs", client=client)
    sink.close()
    assert client.drained is True
    sink.close()  # idempotent (loop already closed)


# -- SPEC-041 FR-004 AC-5: a disconnected client is reported, not absorbed --------------------


class DisconnectedNATS(FakeNATS):
    """A client that reports itself disconnected, as `nats-py` does while reconnecting."""

    is_connected = False


def test_a_disconnected_client_makes_emit_report_total_non_delivery() -> None:
    client = DisconnectedNATS()
    sink = NATSSink("subject", client=client)

    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}, {"a": 2}])

    # A core publish would have "succeeded" into the client's outbound buffer and been reported
    # as delivered -- measured against a real server as 1 of 6 events arriving with every counter
    # at zero. Nothing must reach the client at all.
    assert client.published == []


def test_refusing_moves_no_loss_counter() -> None:
    # SPEC-032's rule: a refusal is a failure REPORTED to the worker, which records it in
    # health().failed_batches, not one this sink absorbed. Counting it here reports it twice.
    sink = NATSSink("subject", client=DisconnectedNATS())

    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}])

    assert sink.losses().failed == 0
    assert sink.losses().dropped == 0


def test_a_client_that_says_nothing_about_connectedness_is_still_published_to() -> None:
    # The probe is by name because an injected client need not be `nats-py`. Assuming a silent
    # client is disconnected would fail batches that were going to succeed.
    client = FakeNATS()
    assert not hasattr(client, "is_connected")
    sink = NATSSink("subject", client=client)

    sink.emit([{"a": 1}])

    assert len(client.published) == 1


def test_a_client_whose_probe_raises_is_treated_as_connected() -> None:
    class Hostile(FakeNATS):
        @property
        def is_connected(self):
            raise RuntimeError("driver fault")

    client = Hostile()
    sink = NATSSink("subject", client=client)

    sink.emit([{"a": 1}])   # a diagnostic probe must never be the reason a batch fails

    assert len(client.published) == 1
