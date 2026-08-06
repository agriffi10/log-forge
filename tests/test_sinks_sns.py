"""SPEC-010 — SNSSink: 10-entry publish_batch chunks + Failed-list retry (fake client)."""

from __future__ import annotations

import json

from log_foundry.sinks.base import Sink
from log_foundry.sinks.sns import SNSSink


class FakeSNS:
    """Records publish_batch calls; fails entry Ids in ``fail_once``/``always_fail``."""

    def __init__(self, fail_once: set[str] | None = None, always_fail: set[str] | None = None):
        self.calls: list[list[dict]] = []
        self._fail_once = set(fail_once or set())
        self._always_fail = set(always_fail or set())
        self._failed_once: set[str] = set()

    def publish_batch(self, *, TopicArn: str, PublishBatchRequestEntries: list[dict]) -> dict:
        self.calls.append([dict(e) for e in PublishBatchRequestEntries])
        successful, failed = [], []
        for entry in PublishBatchRequestEntries:
            eid = entry["Id"]
            if eid in self._always_fail:
                failed.append({"Id": eid, "Code": "InternalError"})
            elif eid in self._fail_once and eid not in self._failed_once:
                self._failed_once.add(eid)
                failed.append({"Id": eid, "Code": "Throttled"})
            else:
                successful.append({"Id": eid})
        return {"Successful": successful, "Failed": failed}


def test_is_a_sink() -> None:
    assert isinstance(SNSSink("arn", client=FakeSNS()), Sink)


def test_one_message_per_event() -> None:
    client = FakeSNS()
    SNSSink("arn", client=client).emit([{"a": 1}, {"a": 2}])
    entries = client.calls[0]
    assert [e["Id"] for e in entries] == ["0", "1"]
    assert json.loads(entries[0]["Message"]) == {"a": 1}


def test_chunks_by_ten_entries() -> None:
    client = FakeSNS()
    SNSSink("arn", client=client).emit([{"i": i} for i in range(25)])
    assert [len(call) for call in client.calls] == [10, 10, 5]


def test_oversized_message_is_dropped(capsys) -> None:
    client = FakeSNS()
    sink = SNSSink("arn", client=client)
    sink.emit([{"pad": "x" * (256 * 1024 + 50)}, {"ok": 1}])
    assert sink.dropped_oversized == 1
    assert "lost 1 event(s)" in capsys.readouterr().err
    assert len(client.calls[0]) == 1


def test_failed_entries_retried_then_succeed() -> None:
    client = FakeSNS(fail_once={"0"})
    sink = SNSSink("arn", client=client)
    sink.emit([{"a": 1}])
    assert len(client.calls) == 2
    assert sink.failed == 0


def test_persistent_failures_counted(capsys) -> None:
    client = FakeSNS(always_fail={"0"})
    sink = SNSSink("arn", client=client, max_retries=2)
    sink.emit([{"a": 1}])
    assert len(client.calls) == 3
    assert sink.failed == 1
    assert "lost 1 message(s)" in capsys.readouterr().err
