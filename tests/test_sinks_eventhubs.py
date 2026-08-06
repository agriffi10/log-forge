"""SPEC-010 — AzureEventHubsSink: 1MB batch packing + oversized drop + send retry (fake producer)."""

from __future__ import annotations

import json

import pytest

from log_foundry.sinks.base import Sink
from log_foundry.sinks.eventhubs import AzureEventHubsSink


class FakeBatch:
    """Mimics EventDataBatch: ``add`` raises ValueError when the byte budget is exceeded."""

    def __init__(self, max_bytes: int) -> None:
        self.items: list[bytes] = []
        self._max = max_bytes
        self._bytes = 0

    def add(self, data: bytes) -> None:
        size = len(data)
        if self.items and self._bytes + size > self._max:
            raise ValueError("batch full")
        if not self.items and size > self._max:
            raise ValueError("event too large for an empty batch")
        self.items.append(data)
        self._bytes += size

    def __len__(self) -> int:
        return len(self.items)


class FakeProducer:
    def __init__(self, max_bytes: int = 1024 * 1024, fail: bool = False) -> None:
        self.sent: list[list[bytes]] = []
        self._max = max_bytes
        self._fail = fail
        self.closed = False

    def create_batch(self) -> FakeBatch:
        return FakeBatch(self._max)

    def send_batch(self, batch: FakeBatch) -> None:
        if self._fail:
            raise RuntimeError("send failed")
        self.sent.append(list(batch.items))

    def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def _identity_event_data(monkeypatch):
    """EventData(body) -> body, so the fake batch sees raw bytes (no azure dependency)."""
    monkeypatch.setattr("log_foundry.sinks.eventhubs._event_data_cls", lambda: lambda body: body)


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr("log_foundry.sinks.eventhubs.time.sleep", lambda _s: None)


def test_is_a_sink() -> None:
    assert isinstance(AzureEventHubsSink(producer=FakeProducer()), Sink)


def test_packs_events_across_batches_by_size() -> None:
    # Each event serializes to ~15 bytes; a 40-byte budget forces multiple batches.
    producer = FakeProducer(max_bytes=40)
    sink = AzureEventHubsSink(producer=producer)
    events = [{"i": i} for i in range(6)]
    sink.emit(events)
    sent_events = [json.loads(body) for batch in producer.sent for body in batch]
    assert sent_events == events  # every event was sent, none lost across batch boundaries
    assert len(producer.sent) > 1  # actually split into multiple batches


def test_oversized_event_is_dropped(capsys) -> None:
    producer = FakeProducer(max_bytes=30)
    sink = AzureEventHubsSink(producer=producer)
    sink.emit([{"pad": "x" * 100}, {"ok": 1}])
    assert sink.dropped_oversized == 1
    assert "lost 1 event(s)" in capsys.readouterr().err
    sent_events = [json.loads(body) for batch in producer.sent for body in batch]
    assert sent_events == [{"ok": 1}]


def test_send_failure_is_retried_then_counted(capsys) -> None:
    producer = FakeProducer(fail=True)
    sink = AzureEventHubsSink(producer=producer, max_retries=1)
    sink.emit([{"a": 1}, {"a": 2}])
    assert sink.failed == 2  # whole batch abandoned after the bound
    assert "lost 2 event(s)" in capsys.readouterr().err


def test_close_closes_producer() -> None:
    producer = FakeProducer()
    AzureEventHubsSink(producer=producer).close()
    assert producer.closed is True


def test_requires_connection_str_without_producer() -> None:
    with pytest.raises(ValueError):
        AzureEventHubsSink()
