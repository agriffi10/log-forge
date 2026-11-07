"""SPEC-071 FR-004 — `SQSSink._chunks`' boundaries, recorded as literals before the swap.

Recorded in Phase 1 against the unmodified `_chunks` of `2368ade3` (its own accumulator), and not
edited by the commit that moves it onto `chunk_items`. Each case asserts the literal lengths of
the chunks returned and what the call's deferral committed, never a recomputation of the rule, so
the check cannot mirror the code it checks.

The bodies are written to a byte size directly: `_chunks` costs each event as its body's UTF-8
length plus, on a FIFO queue, both ids' lengths. The FIFO cases use a one-byte group id and a
one-byte deduplication id, so every FIFO event costs its body plus two.

Serves invariants 2 and 7: every drop `_chunks` decides is counted once, only when the calling
`emit` returns, and a batch of nothing sendable settles counted without raising.
"""

from __future__ import annotations

from typing import Any

import pytest

from log_foundry.sinks import sqs as sqs_mod
from log_foundry.sinks._deferral import Deferral
from log_foundry.sinks.base import SinkDeliveryError
from log_foundry.sinks.sqs import SQSSink

MAX = SQSSink.MAX_BYTES


class _Client:
    """Accepts every entry, or raises a transient error from every send.

    Attributes:
      calls: The entry lists sent, in order.
    """

    def __init__(self, fail: bool = False) -> None:
        """Arms the client.

        Args:
          fail: Raise ``ConnectionError`` from every send instead of accepting.

        Returns:
          None.

        Raises:
          None.
        """
        self.calls: list[list[dict[str, Any]]] = []
        self._fail = fail

    def send_message_batch(self, *, QueueUrl: str, Entries: list[dict[str, Any]]) -> dict[str, Any]:
        """Records the entries and accepts them all, or raises.

        Args:
          QueueUrl: Unused.
          Entries: The entries sent.

        Returns:
          Every entry as successful.

        Raises:
          ConnectionError: When armed to fail.
        """
        self.calls.append(Entries)
        if self._fail:
            raise ConnectionError("down")
        return {
            "Successful": [{"Id": e["Id"], "MessageId": e["Id"]} for e in Entries],
            "Failed": [],
        }


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sqs_mod, "wait", lambda _delay, _stop=None: None)


def _sink(fifo: bool = False, **kw: Any) -> SQSSink:
    if fifo:
        kw.setdefault("message_group_id", "g")
        kw.setdefault("message_deduplication_id", lambda e: "d")
    return SQSSink("https://q", client=_Client(), fifo=fifo, **kw)


def _encoded(*sizes: int, char: str = "x") -> list[tuple[dict[str, object], str]]:
    return [({"log_id": f"l{i}", "trace_id": "t"}, char * size) for i, size in enumerate(sizes)]


def _run(
    sink: SQSSink, encoded: list[tuple[dict[str, object], str]], unencodable: int = 0
) -> list[int]:
    with Deferral(sink) as pending:
        chunks = sink._chunks(encoded, pending, unencodable=unencodable)
    assert type(chunks) is list, (
        "every drop is decided before the first send: a list, not a generator"
    )
    return [len(chunk) for chunk in chunks]


def _drops(sink: SQSSink) -> tuple[int, int]:
    return (sink.dropped_oversized, sink._id_failed)


def test_exactly_the_count_limit_is_one_chunk() -> None:
    sink = _sink()
    assert _run(sink, _encoded(*[10] * 10)) == [10]
    assert _drops(sink) == (0, 0)


def test_one_past_the_count_limit_is_two_chunks() -> None:
    assert _run(_sink(), _encoded(*[10] * 11)) == [10, 1]


def test_costed_sizes_summing_to_exactly_the_byte_limit_are_one_chunk() -> None:
    assert _run(_sink(), _encoded(MAX // 2, MAX // 2)) == [2]


def test_one_byte_over_the_byte_limit_is_two_chunks() -> None:
    assert _run(_sink(), _encoded(MAX // 2, MAX // 2 + 1)) == [1, 1]


def test_count_and_bytes_together() -> None:
    assert _run(_sink(), _encoded(*[10] * 9, MAX - 50)) == [9, 1]


def test_a_single_event_at_the_limit_fits() -> None:
    sink = _sink()
    assert _run(sink, _encoded(MAX)) == [1]
    assert _drops(sink) == (0, 0)


def test_a_single_event_one_byte_over_is_dropped_counted() -> None:
    sink = _sink()
    assert _run(sink, _encoded(MAX + 1)) == []
    assert _drops(sink) == (1, 0)


def test_an_oversize_event_between_two_that_fit() -> None:
    sink = _sink()
    assert _run(sink, _encoded(10, MAX + 1, 10)) == [2]
    assert _drops(sink) == (1, 0)


def test_a_batch_of_only_oversize_events_settles_counted_without_raising() -> None:
    sink = _sink()
    assert _run(sink, _encoded(MAX + 1, MAX + 5, MAX * 2)) == []
    assert _drops(sink) == (3, 0)


def test_an_empty_batch_is_no_chunks() -> None:
    sink = _sink()
    assert _run(sink, []) == []
    assert _drops(sink) == (0, 0)


def test_the_cost_is_utf8_bytes_not_characters() -> None:
    each = MAX // 4 + 1
    assert _run(_sink(), _encoded(each, each, char="é")) == [1, 1]
    assert _run(_sink(), _encoded(each, each)) == [2], "the premise: as ASCII the pair fits"


def test_fifo_ids_push_a_pair_over_the_byte_limit() -> None:
    body = MAX // 2 - 1
    assert _run(_sink(fifo=True), _encoded(body, body)) == [1, 1]
    assert _run(_sink(), _encoded(body, body)) == [2], "the premise: without ids the pair fits"


def test_fifo_ids_push_a_single_event_over_and_it_is_dropped() -> None:
    sink = _sink(fifo=True)
    assert _run(sink, _encoded(MAX - 1)) == []
    assert _drops(sink) == (1, 0)


def test_every_id_callable_raising_reraises_the_first_unchanged_and_counts_nothing() -> None:
    raised: list[Exception] = []

    def dedup(event: dict[str, object]) -> str:
        error = ValueError(str(event["log_id"]))
        raised.append(error)
        raise error

    sink = _sink(fifo=True, message_deduplication_id=dedup)
    with pytest.raises(ValueError) as info:
        _run(sink, _encoded(10, 10, 10))
    assert info.value is raised[0]
    assert _drops(sink) == (0, 0)


def test_id_failures_beside_an_unencodable_event_settle_counted() -> None:
    sink = _sink(fifo=True, message_deduplication_id=lambda e: str(e["missing"]))
    assert _run(sink, _encoded(10, 10), unencodable=1) == []
    assert _drops(sink) == (0, 2)


def test_an_id_failure_beside_an_oversize_event_settles_counted() -> None:
    sink = _sink(fifo=True, message_deduplication_id=lambda e: str(e["d"]))
    encoded = [({"log_id": "a"}, "x" * 10), ({"log_id": "b", "d": "1"}, "x" * MAX)]
    assert _run(sink, encoded) == []
    assert _drops(sink) == (1, 1)


def test_an_unencodable_event_and_an_id_failure_through_emit_settle_counted() -> None:
    sink = _sink(fifo=True, message_deduplication_id=lambda e: str(e["missing"]))
    sink.emit([{"log_id": "a", "trace_id": "t"}, {"log_id": "b", "bad": b"\x00"}])
    assert (sink._unencodable, sink._id_failed, sink.dropped_oversized) == (1, 1, 0)
    assert sink.client.calls == []


def test_a_fifo_batch_whose_send_raises_counts_its_oversize_drop_nowhere() -> None:
    """FR-004 AC-3: every drop sits in the deferral, which a raising ``emit`` discards."""
    sink = SQSSink(
        "https://q.fifo",
        client=_Client(fail=True),
        message_group_id="g",
        max_retries=0,
    )
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"log_id": "a", "trace_id": "t"}, {"log_id": "b", "pad": "x" * MAX}])
    assert len(sink.client.calls) == 1, "the premise: the one sendable chunk was attempted"
    assert sink.dropped_oversized == 0
