"""SPEC-100 probe: what moto's SNS does with a FIFO topic, and whose de-duplication it shows.

Written on 2026-11-17 while authoring SPEC-100, against 03dbf502; not one of the audit's probes.
It needs `boto3` and `moto` (measured with boto3 1.44.4 and moto 5.2.5). moto is not a repository
dependency and must not become one: install both into a scratch virtualenv. Run from the
repository root:

    PYTHONPATH=src <venv>/bin/python -B docs/audits/2026-11-14-probes/sinks/spec100_moto_shapes.py

moto is not AWS. It is the only executable evidence of the shape SNS gives a FIFO refusal, because
the AWS pages name no error code for a missing id, and each case below says what moto leaves out.

Cases:
  A  today's SNSSink on a FIFO topic, with and without ContentBasedDeduplication: what is lost.
  B  the raw shape of each FIFO refusal: a missing group id, a missing dedup id, a dedup id on a
     standard topic, an empty group id, a 129-character id, an id with a space and a non-ASCII
     character.
  C  whose de-duplication absorbs a resend: one dedup id published twice, read back from a FIFO
     SQS subscriber and from a standard SQS subscriber of the same topic.
  D  what moto counts against the message limit: a 262,144-character body with two 128-character
     ids, a 262,145-character body, and 131,073 two-byte characters.
  E  when the tree's SNSSink takes `fifo` (the SPEC-100 prototype): a FIFO SNSSink whose first
     call lands and then loses its reply, read back from both subscribers.
  F  SQSSink on a moto FIFO queue, two events whose dedup callable answers 200 characters sharing
     the first 128: how many messages the queue holds, and what the sink counted.
"""

from __future__ import annotations

import contextlib
import inspect
import io
import os

os.environ.update(AWS_DEFAULT_REGION="us-east-1", AWS_ACCESS_KEY_ID="x", AWS_SECRET_ACCESS_KEY="x")

import boto3
from moto import mock_aws

from log_foundry.sinks.sns import SNSSink


def shape(call, **kwargs) -> str:
    """The reply's two lists, or the raised error's code and HTTP status."""
    try:
        reply = call(**kwargs)
    except Exception as err:
        response = getattr(err, "response", {})
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        return f"raised {type(err).__name__} {response.get('Error', {}).get('Code')} HTTP {status}"
    return f"Successful={reply.get('Successful')} Failed={reply.get('Failed')}"


def drain(sqs, url: str) -> list[str]:
    """Every body the queue holds, deleted as read."""
    got: list[str] = []
    while True:
        batch = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10).get("Messages", [])
        if not batch:
            return got
        for message in batch:
            got.append(message["Body"])
            sqs.delete_message(QueueUrl=url, ReceiptHandle=message["ReceiptHandle"])


def topic_with_subscribers(sns, sqs, attrs: dict[str, str]) -> tuple[str, dict[str, str]]:
    """A FIFO topic with one FIFO and one standard SQS queue subscribed, raw delivery."""
    arn = sns.create_topic(Name="t.fifo", Attributes={"FifoTopic": "true", **attrs})["TopicArn"]
    queues: dict[str, str] = {}
    for name, queue_attrs in (("fifo.fifo", {"FifoQueue": "true"}), ("standard", {})):
        url = sqs.create_queue(QueueName=name, Attributes=queue_attrs)["QueueUrl"]
        qarn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"][
            "QueueArn"
        ]
        sns.subscribe(
            TopicArn=arn,
            Protocol="sqs",
            Endpoint=qarn,
            Attributes={"RawMessageDelivery": "true"},
        )
        queues[name] = url
    return arn, queues


def event(i: int) -> dict[str, object]:
    return {"log_id": f"{i:032x}", "trace_id": "a" * 32, "n": i}


@mock_aws
def case_a(cbd: bool) -> None:
    sns = boto3.client("sns")
    attrs = {"FifoTopic": "true"}
    if cbd:
        attrs["ContentBasedDeduplication"] = "true"
    arn = sns.create_topic(Name="t.fifo", Attributes=attrs)["TopicArn"]
    sink = SNSSink(arn, client=sns, max_retries=1)
    err = io.StringIO()
    with contextlib.redirect_stderr(err):
        try:
            sink.emit([event(i) for i in range(3)])
            outcome = "returned"
        except Exception as exc:
            outcome = f"raised {type(exc).__name__}: {exc}"
    print(f"A  ContentBasedDeduplication={cbd}: emit of 3 {outcome}")
    print(f"   losses()={sink.losses()}")
    for line in err.getvalue().splitlines():
        print(f"   stderr: {line}")


@mock_aws
def case_b() -> None:
    sns = boto3.client("sns")
    fifo = sns.create_topic(Name="t.fifo", Attributes={"FifoTopic": "true"})["TopicArn"]
    standard = sns.create_topic(Name="s")["TopicArn"]

    def entry(**fields: str) -> list[dict[str, str]]:
        return [{"Id": "0", "Message": "m", **fields}]

    cases = [
        ("no group id", fifo, entry(MessageDeduplicationId="d1")),
        ("no dedup id, CBD off", fifo, entry(MessageGroupId="g")),
        ("dedup id on a standard topic", standard, entry(MessageDeduplicationId="d2")),
        ("group id only on a standard topic", standard, entry(MessageGroupId="g")),
        ("empty group id", fifo, entry(MessageGroupId="", MessageDeduplicationId="d3")),
        ("129-char dedup id", fifo, entry(MessageGroupId="g", MessageDeduplicationId="x" * 129)),
        ("129-char group id", fifo, entry(MessageGroupId="g" * 129, MessageDeduplicationId="d4")),
        ("dedup id 'a b é'", fifo, entry(MessageGroupId="g", MessageDeduplicationId="a b é")),
    ]
    for label, arn, entries in cases:
        print(f"B  {label}: {shape(sns.publish_batch, TopicArn=arn, PublishBatchRequestEntries=entries)}")


@mock_aws
def case_c() -> None:
    sns = boto3.client("sns")
    sqs = boto3.client("sqs")
    arn, queues = topic_with_subscribers(sns, sqs, {})
    entries = [{"Id": "0", "Message": "m0", "MessageGroupId": "g", "MessageDeduplicationId": "A"}]
    first = shape(sns.publish_batch, TopicArn=arn, PublishBatchRequestEntries=entries)
    second = shape(sns.publish_batch, TopicArn=arn, PublishBatchRequestEntries=entries)
    print(f"C  first publish:  {first}")
    print(f"C  second publish: {second}")
    for name, url in queues.items():
        print(f"C  {name} subscriber holds {drain(sqs, url)}")


@mock_aws
def case_d() -> None:
    sns = boto3.client("sns")
    arn = sns.create_topic(Name="t.fifo", Attributes={"FifoTopic": "true"})["TopicArn"]
    cases = [
        ("262,144 chars + two 128-char ids", "x" * 262144, "g" * 128, "D" * 128),
        ("262,145 chars", "x" * 262145, "g", "E"),
        ("131,073 x U+00E9 (262,146 bytes)", "é" * 131073, "g", "F"),
    ]
    for label, body, group, dedup in cases:
        entries = [
            {"Id": "0", "Message": body, "MessageGroupId": group, "MessageDeduplicationId": dedup}
        ]
        print(f"D  {label}: {shape(sns.publish_batch, TopicArn=arn, PublishBatchRequestEntries=entries)}")


class LoseFirstReply:
    """A pass-through SNS client whose first `publish_batch` lands and then raises a read timeout."""

    def __init__(self, inner) -> None:
        self.inner = inner
        self.calls: list[list[dict[str, str]]] = []

    def publish_batch(self, **kwargs):
        self.calls.append(kwargs["PublishBatchRequestEntries"])
        reply = self.inner.publish_batch(**kwargs)
        if len(self.calls) == 1:
            from botocore.exceptions import ReadTimeoutError

            raise ReadTimeoutError(endpoint_url="https://sns")
        return reply


@mock_aws
def case_e() -> None:
    if "fifo" not in inspect.signature(SNSSink.__init__).parameters:
        print("E  skipped: this tree's SNSSink takes no `fifo` (run on the SPEC-100 prototype)")
        return
    sns = boto3.client("sns")
    sqs = boto3.client("sqs")
    arn, queues = topic_with_subscribers(sns, sqs, {})
    client = LoseFirstReply(sns)
    sink = SNSSink(arn, client=client)
    err = io.StringIO()
    with contextlib.redirect_stderr(err):
        sink.emit([event(i) for i in range(3)])
    ids = [[e["MessageDeduplicationId"] for e in call] for call in client.calls]
    print(f"E  calls: {len(client.calls)}; dedup ids per call: {ids}")
    print(f"E  losses()={sink.losses()}")
    for name, url in queues.items():
        print(f"E  {name} subscriber holds {len(drain(sqs, url))} message(s) for 3 events")


@mock_aws
def case_f() -> None:
    from log_foundry.sinks.sqs import SQSSink

    sqs = boto3.client("sqs")
    url = sqs.create_queue(QueueName="q.fifo", Attributes={"FifoQueue": "true"})["QueueUrl"]
    sink = SQSSink(
        url, client=sqs, message_deduplication_id=lambda e: "p" * 128 + str(e["n"]) * 72
    )
    err = io.StringIO()
    with contextlib.redirect_stderr(err):
        sink.emit([{"log_id": "L0", "trace_id": "T", "n": 0}, {"log_id": "L1", "trace_id": "T", "n": 1}])
    held = drain(sqs, url)
    print(
        f"F  queue holds {len(held)} of 2 events; losses()={sink.losses()}; "
        f"stderr lines: {len(err.getvalue().splitlines())}"
    )


if __name__ == "__main__":
    case_a(False)
    case_a(True)
    case_b()
    case_c()
    case_d()
    case_e()
    case_f()
