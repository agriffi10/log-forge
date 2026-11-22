"""SPEC-093 FR-003 — a batch reply that names no event adjudicates none of it.

Andrew's decision A (2026-11-15): an AWS batch reply that names no failure does not read as full
delivery. An SQS or SNS reply with neither ``Failed`` nor ``Successful`` is unread, a ``Successful``
list is read with ``adjudicate_keyed``'s discipline and an entry it leaves out is ``UNKNOWN``, and a
``Failed``-only reply reads as 1.x did. A Kinesis or Firehose reply with neither a failed count nor
a record array is unadjudicated through ``adjudicate_positional(absent=True)``. Every ``UNKNOWN``
entry is counted in ``failed`` and in ``dropped_unadjudicated``, its sub-count on these two sinks,
and is never sent again. Serves invariants 2, 7 and 10.
"""

from __future__ import annotations

from typing import Any

import pytest

from log_foundry.sinks._batch import ABSENT, Adjudication, adjudicate_positional
from log_foundry.sinks.base import SinkLosses
from log_foundry.sinks.sns import SNSSink
from log_foundry.sinks.sqs import SQSSink


class _Keyed:
    """An SQS and SNS client double whose reply is built from the ids each call sent."""

    def __init__(self, reply: Any) -> None:
        """Keeps the function that builds a reply from the call's entry ids.

        Args:
          reply: ``ids -> reply``.

        Returns:
          None.

        Raises:
          None.
        """
        self.reply = reply
        self.calls: list[list[str]] = []

    def _answer(self, entries: list[dict[str, Any]]) -> Any:
        """Records the call's ids and answers.

        Args:
          entries: The call's entries.

        Returns:
          The scripted reply.

        Raises:
          None.
        """
        ids = [str(entry["Id"]) for entry in entries]
        self.calls.append(ids)
        return self.reply(ids)

    def send_message_batch(self, *, QueueUrl: str, Entries: list[dict[str, Any]]) -> Any:
        """SQS's batch call.

        Args:
          QueueUrl: Ignored.
          Entries: The entries.

        Returns:
          The scripted reply.

        Raises:
          None.
        """
        return self._answer(Entries)

    def publish_batch(
        self,
        *,
        TopicArn: str,
        PublishBatchRequestEntries: list[dict[str, Any]],
    ) -> Any:
        """SNS's batch call.

        Args:
          TopicArn: Ignored.
          PublishBatchRequestEntries: The entries.

        Returns:
          The scripted reply.

        Raises:
          None.
        """
        return self._answer(PublishBatchRequestEntries)


def _sink(cls: type, client: _Keyed) -> Any:
    """An SQS or SNS sink over the double, retrying nothing.

    Args:
      cls: ``SQSSink`` or ``SNSSink``.
      client: The double.

    Returns:
      The sink.

    Raises:
      None.
    """
    target = "https://q" if cls is SQSSink else "arn"
    return cls(target, client=client, max_retries=0)


def _lost(err: str) -> list[str]:
    """The ``lost`` lines of a stderr capture.

    Args:
      err: The capture.

    Returns:
      The lines naming a loss.

    Raises:
      None.
    """
    return [line for line in err.splitlines() if "lost" in line]


_EVENTS = [{"i": i} for i in range(3)]
_KEYED = [SQSSink, SNSSink]


@pytest.mark.parametrize("cls", _KEYED, ids=lambda c: c.__name__)
@pytest.mark.parametrize(
    "reply",
    [
        lambda ids: {"Failed": []},
        lambda ids: {"Successful": [{"Id": i} for i in ids]},
        lambda ids: {"Successful": [{"Id": i} for i in ids], "Failed": []},
    ],
    ids=["failed-only", "successful-only", "both"],
)
def test_a_reply_naming_every_entry_or_failing_none_reads_all_accepted(
    cls: type, reply: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    """Criterion 3: each of these replies leaves every entry ``ACCEPTED`` and moves no counter."""
    client = _Keyed(reply)
    sink = _sink(cls, client)
    sink.emit(list(_EVENTS))
    assert len(client.calls) == 1
    assert sink.dropped_unadjudicated == 0
    assert sink.losses() == SinkLosses(dropped=0, failed=0)
    assert _lost(capsys.readouterr().err) == []


@pytest.mark.parametrize("cls", _KEYED, ids=lambda c: c.__name__)
@pytest.mark.parametrize(
    "reply",
    [
        lambda ids: {"Successful": [{"Id": ids[0]}], "Failed": [{"Id": ids[0], "Code": "X"}]},
        lambda ids: {"Successful": [*({"Id": i} for i in ids), {"Id": "999"}]},
    ],
    ids=["named-twice", "unknown-id"],
)
def test_a_contradictory_successful_list_makes_the_whole_call_unknown(
    cls: type, reply: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    """Criterion 4: every entry ``UNKNOWN``, counted, and the client called once, as for a read."""
    client = _Keyed(reply)
    sink = _sink(cls, client)
    sink.emit(list(_EVENTS))
    assert len(client.calls) == 1
    assert sink.dropped_unadjudicated == 3
    assert sink.losses().failed == 3
    [line] = _lost(capsys.readouterr().err)
    assert f"{cls.__name__} could not adjudicate a" in line
    assert line.endswith("(3 entry(ies) sent); may have landed, not retried")


@pytest.mark.parametrize("cls", _KEYED, ids=lambda c: c.__name__)
def test_an_empty_reply_is_unread_and_counted_never_resent(
    cls: type, capsys: pytest.CaptureFixture[str]
) -> None:
    """Decision A: ``{}`` names no entry, so the call is unread, even with retries left."""
    client = _Keyed(lambda ids: {})
    target = "https://q" if cls is SQSSink else "arn"
    sink = cls(target, client=client, max_retries=3)
    sink.emit(list(_EVENTS))
    assert len(client.calls) == 1
    assert sink.dropped_unadjudicated == 3
    assert sink.losses().failed == 3
    [line] = _lost(capsys.readouterr().err)
    assert line.endswith("may have landed, not retried")


@pytest.mark.parametrize("cls", _KEYED, ids=lambda c: c.__name__)
def test_an_entry_named_in_neither_list_is_unknown_on_its_own_line(
    cls: type, capsys: pytest.CaptureFixture[str]
) -> None:
    """``Successful`` names two of three and ``Failed`` is absent: the third is ``UNKNOWN``."""
    client = _Keyed(lambda ids: {"Successful": [{"Id": i} for i in ids[:2]]})
    sink = _sink(cls, client)
    sink.emit(list(_EVENTS))
    assert sink.dropped_unadjudicated == 1
    assert sink.losses().failed == 1
    [line] = _lost(capsys.readouterr().err)
    assert (
        f"lost 1 message(s); {cls.__name__}, a" in line
        and "reply named neither success nor failure for 1 entry(ies); may have landed, not retried"
        in line
    )


def test_a_reply_with_neither_a_count_nor_an_array_adjudicates_none_of_the_chunk() -> None:
    """Criterion 5: ``absent`` makes the chunk unadjudicated; left out, SPEC-070's reading holds."""
    a, b = object(), object()
    assert adjudicate_positional([a, b], [], failed_count=ABSENT, absent=True) == Adjudication(
        [], 2
    )
    assert adjudicate_positional([a, b], [], failed_count=ABSENT) == Adjudication([], 0)
    assert adjudicate_positional([a, b], [], failed_count=0, absent=True) == Adjudication([], 0)


@pytest.mark.parametrize("cls", _KEYED, ids=lambda c: c.__name__)
def test_a_new_sink_counts_no_unadjudicated_entry_and_losses_is_unchanged(cls: type) -> None:
    """Criterion 7: the sub-count starts at zero and adds nothing to ``losses()``."""
    sink = _sink(cls, _Keyed(lambda ids: {"Failed": []}))
    assert sink.dropped_unadjudicated == 0
    assert sink.losses() == SinkLosses(dropped=0, failed=0)
