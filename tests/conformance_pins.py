"""SPEC-092's register: the rows red today, each pinned to its open item or a permitted departure.

A helper, not collected. It imports the standard library and `model_pins`'s `PinnedVerdict`, the
exception SPEC-085's strict marks expect, at run time, and `conformance_backend`'s `Check` for
typing only, so the probe that runs the corpus against an older tree never imports it. A **pin** names rows,
the exact verdict each has, and its item: the bold lead of an open bullet under `### Open` in
`docs/architecture.md` §12. A pinned row is an expected failure only at that verdict, so the
suite turns red the day a sink is fixed or a new defect appears, and a pin moves only after the
owner hears why. A **departure** is a red row the library permits, held at its exact verdict, and
its record is a file and a phrase quoted from it verbatim.
"""

from __future__ import annotations

import collections
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from model_pins import PinnedVerdict

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from conformance_backend import Check

_AWS = ("SQSSink[standard]", "SQSSink[fifo]", "SNSSink", "KinesisSink", "FirehoseSink")


@dataclass(frozen=True, kw_only=True)
class Pin:
    """Rows red today at one verdict, owned by one open §12 item.

    Attributes:
      name: The pin's name.
      rows: Row ids, `<sink>[<variant>]-<fault>`.
      verdict: The exact verdict each row has.
      item: The bold lead of an open bullet under `### Open` in `docs/architecture.md` §12.
    """

    name: str
    rows: tuple[str, ...]
    verdict: tuple[Check, ...]
    item: str


@dataclass(frozen=True, kw_only=True)
class Departure:
    """Rows red at one verdict that the library permits, and the record that permits it.

    Attributes:
      name: The departure's name.
      rows: Row ids.
      verdict: The exact verdict each row has.
      record: The file, and a phrase quoted from it verbatim.
    """

    name: str
    rows: tuple[str, ...]
    verdict: tuple[Check, ...]
    record: tuple[str, str]


def _each(sinks: Iterable[str], *faults: str) -> tuple[str, ...]:
    """Every `<sink>-<fault>` row id, sink by sink.

    Args:
      sinks: Variant ids.
      *faults: Faults.

    Returns:
      The row ids.

    Raises:
      None.
    """
    return tuple(f"{sink}-{fault}" for sink in sinks for fault in faults)


_LANDED_THEN_LOST = "An event a send may have landed is counted as lost when no later send lands it"
_FINDING_12 = (
    "`KafkaSink` and `GooglePubSubSink` give their verdict when the client queues a message, "
    "not when the broker answers"
)
_FINDING_15 = "An injected autocommit connection breaks the database sinks' all-or-nothing write"

PINS: tuple[Pin, ...] = (
    Pin(
        name="reply-counted-lost",
        rows=(
            "PostgresSink[borrowed]-reply-lost",
            "RabbitMQSink[borrowed]-taken-then-raised",
        ),
        verdict=("lost-but-held",),
        item=_LANDED_THEN_LOST,
    ),
    Pin(
        name="reply-without-verdict",
        rows=_each(
            ("SQSSink[standard]", "SQSSink[fifo]", "SNSSink", "KinesisSink", "FirehoseSink"),
            "no-verdict-shape",
        ),
        verdict=("silent-loss",),
        item="An AWS batch reply that names no failure is read as full delivery",
    ),
    Pin(
        name="bulk-without-errors",
        rows=_each(("ElasticsearchSink", "OpenSearchSink"), "no-verdict-shape"),
        verdict=("silent-loss",),
        item=(
            "`ElasticsearchSink` reads a `2xx` object without a truthy `errors` as success, "
            "unverified."
        ),
    ),
    Pin(
        name="peer-closed-tcp",
        rows=_each(("SyslogSink[tcp]", "LogstashSink[tcp]"), "peer-closed"),
        verdict=("silent-loss",),
        item=(
            "A TCP `SyslogSink` or `LogstashSink` counts a frame as accepted when `sendall` "
            "returns, so frames written after the peer closed vanish uncounted"
        ),
    ),
    Pin(
        name="publish-without-confirms",
        rows=_each(("RabbitMQSink[owned]", "RabbitMQSink[borrowed]"), "peer-closed"),
        verdict=("silent-loss",),
        item=(
            "`RabbitMQSink` publishes without confirms, so an unroutable message or one written "
            "to a dead socket reads as accepted"
        ),
    ),
    Pin(
        name="jetstream-ack-lost",
        rows=_each(("NATSSink[jetstream]", "NATSSink[jetstream-injected]"), "reply-lost"),
        verdict=("lost-but-held",),
        item="A timed-out JetStream publish is not retried, and cannot safely be",
    ),
    Pin(
        name="verdict-before-refusal",
        rows=_each(("KafkaSink", "GooglePubSubSink"), "never-acknowledged"),
        verdict=("partition", "silent-loss"),
        item=_FINDING_12,
    ),
    Pin(
        name="verdict-before-indefinite-report",
        rows=_each(("KafkaSink", "GooglePubSubSink"), "report-indefinite"),
        verdict=("partition", "lost-but-held"),
        item=_FINDING_12,
    ),
    Pin(
        name="sdk-discard-counted-sent",
        rows=("SentrySink[sdk]-never-acknowledged",),
        verdict=("silent-loss",),
        item="`SentrySink`'s SDK backend counts an event the SDK discarded as sent",
    ),
    Pin(
        name="autocommit-postgres",
        rows=("PostgresSink[autocommit]-autocommit",),
        verdict=("duplicate",),
        item=_FINDING_15,
    ),
    Pin(
        name="autocommit-sqlite",
        rows=("SQLiteSink[autocommit]-autocommit",),
        verdict=("duplicate", "lost-but-held"),
        item=_FINDING_15,
    ),
)
"""The eleven pins after SPEC-095 FR-004: 22 rows over nine open items.

SPEC-092 FR-004 set twelve pins over 74 rows. SPEC-095 removed `reply-resent`, whose 50 rows read
green once a counted resend may leave a second copy, and moved `MongoDBSink`'s two
`reply-counted-lost` rows to green, its duplicate `_id` now read as delivery.
"""

DEPARTURES: tuple[Departure, ...] = (
    Departure(
        name="sqlite-failed-before-raise",
        rows=_each(("SQLiteSink",), "refused-once", "taken-then-raised"),
        verdict=("lost-but-held",),
        record=(
            "docs/invariants.md",
            (
                "the batch size a `SQLiteSink` adds to `failed` when its driver raises, before it "
                "raises in turn (SPEC-069 FR-004)"
            ),
        ),
    ),
    Departure(
        name="udp-fire-and-forget",
        rows=_each(("SyslogSink[udp]", "LogstashSink[udp]"), "peer-closed"),
        verdict=("silent-loss",),
        record=(
            "docs/architecture.md",
            (
                "A UDP datagram is fire-and-forget: a `sendto` that returned is all `SyslogSink` and "
                "`LogstashSink` can know."
            ),
        ),
    ),
    Departure(
        name="injected-nats-unprobed",
        rows=("NATSSink[core-injected]-never-acknowledged",),
        verdict=("silent-loss",),
        record=(
            "docs/architecture.md",
            (
                "`NATSSink` cannot drive a client it did not connect, so an injected client's "
                "liveness is the caller's."
            ),
        ),
    ),
    Departure(
        name="callable-raises-after-keeping",
        rows=_each(
            ("CallbackSink", "FilteringSink[over-callback]", "TransformSink[over-callback]"),
            "taken-then-raised",
        ),
        verdict=("duplicate",),
        record=(
            "docs/architecture.md",
            "A caller's callable that keeps events and then raises has them resent.",
        ),
    ),
    Departure(
        name="handler-absorbs",
        rows=_each(("LoggingSink",), "refused-once", "refused-always"),
        verdict=("silent-loss",),
        record=(
            "docs/specs/SPEC-049-sink-construction-validation-and-driver-bounds.md",
            "a handler failing in the ordinary way is invisible here",
        ),
    ),
    Departure(
        name="null-discards",
        rows=("NullSink-clean",),
        verdict=("silent-loss",),
        record=(
            "docs/spec-delivery/SPEC-026-sink-loss-visibility.md",
            "`NullSink` deliberately exposes no `losses()`: discarding is what it is *for*",
        ),
    ),
    Departure(
        name="aws-client-retries",
        rows=_each(_AWS, "refused-once", "refused-always"),
        verdict=("resent-when-knowable",),
        record=(
            "src/log_foundry/sinks/_rounds.py",
            (
                "botocore retries a call inside the client, so a raise can follow an attempt that "
                "landed, and every raise an AWS sink's call makes is counted as one that may have "
                "landed"
            ),
        ),
    ),
    Departure(
        name="firehose-coded-may-land",
        rows=("FirehoseSink-partial",),
        verdict=("resent-when-knowable",),
        record=(
            "src/log_foundry/sinks/firehose.py",
            (
                "AWS says a record coded `ServiceUnavailableException` or `InternalFailure` might "
                "have completed, so each is counted as one that may have landed"
            ),
        ),
    ),
    Departure(
        name="eventhubs-client-retries",
        rows=_each(("AzureEventHubsSink",), "refused-once", "refused-always"),
        verdict=("resent-when-knowable",),
        record=(
            "src/log_foundry/sinks/eventhubs.py",
            (
                "the Event Hubs producer retries a send inside the client, so every send it fails "
                "is counted as one that may have landed"
            ),
        ),
    ),
    Departure(
        name="clickhouse-error-unclassed",
        rows=_each(("ClickHouseSink",), "refused-once", "refused-always"),
        verdict=("resent-when-knowable",),
        record=(
            "src/log_foundry/sinks/clickhouse.py",
            (
                "clickhouse-connect retries inside the client and raises `OperationalError` "
                "whether or not the insert reached the server, so every insert it fails is "
                "counted as one that may have landed"
            ),
        ),
    ),
    Departure(
        name="redis-client-retries",
        rows=_each(("RedisStreamsSink", "RedisListSink"), "refused-once", "refused-always"),
        verdict=("resent-when-knowable",),
        record=(
            "src/log_foundry/sinks/redis.py",
            (
                "redis-py retries a command inside the client and raises `ConnectionError` both "
                "for a connection it could not open and for one lost after the pipeline was "
                "written, so every pipeline it fails is counted as one that may have landed"
            ),
        ),
    ),
    Departure(
        name="sdk-transport-drop",
        rows=("SentrySink[sdk]-report-indefinite",),
        verdict=("silent-loss",),
        record=(
            "src/log_foundry/sinks/sentry.py",
            "It cannot report a failure, and that is the SDK's shape rather than a choice here.",
        ),
    ),
)
"""The twelve departures: SPEC-092 FR-004's seven over 12 rows, and SPEC-095 FR-004's five over 19.

SPEC-095's five each hold a sink whose every failed call is counted as one that may have landed,
so a resend after a refusal it could have known is counted too: the upper bound its record states.
"""

ARCHITECTURE = "docs/architecture.md"
"""The file whose §12 holds every pin's item."""


def pin_of(row_id: str, pins: Iterable[Pin] = PINS) -> Pin | None:
    """The pin naming a row, if any.

    Args:
      row_id: The row's id.
      pins: The pins to search.

    Returns:
      The pin, or `None`.

    Raises:
      None.
    """
    return next((pin for pin in pins if row_id in pin.rows), None)


def departure_of(row_id: str, departures: Iterable[Departure] = DEPARTURES) -> Departure | None:
    """The departure naming a row, if any.

    Args:
      row_id: The row's id.
      departures: The departures to search.

    Returns:
      The departure, or `None`.

    Raises:
      None.
    """
    return next((d for d in departures if row_id in d.rows), None)


def settle(
    row_id: str,
    verdict: tuple[str, ...],
    pins: Iterable[Pin] = PINS,
    departures: Iterable[Departure] = DEPARTURES,
) -> None:
    """Decides a row's test from its verdict: the pure decision behind the strict mark.

    A pinned row raises `PinnedVerdict`, which its strict `xfail` expects, only at its pin's exact
    verdict; at any other verdict, a pass included, it fails with an `AssertionError`, so a fixed
    sink's pin cannot keep xfailing silently. A departure row passes only at its departure's exact
    verdict, and every other row only at `()`.

    Args:
      row_id: The row's id.
      verdict: Its verdict.
      pins: The pins.
      departures: The departures.

    Returns:
      None, when the row passes.

    Raises:
      PinnedVerdict: When a pinned row reads its pin's verdict.
      AssertionError: When the row reads anything its entry, or the lack of one, does not allow.
    """
    pin = pin_of(row_id, pins)
    if pin is not None:
        if tuple(verdict) == pin.verdict:
            raise PinnedVerdict(f"{row_id} reads {verdict}, as {pin.name} pins it")
        raise AssertionError(f"{row_id} reads {verdict}, but {pin.name} pins it at {pin.verdict}")
    departure = departure_of(row_id, departures)
    expected = departure.verdict if departure is not None else ()
    assert tuple(verdict) == expected, f"{row_id} reads {verdict}, expected {expected}"


def _collapse(text: str) -> str:
    """Runs of whitespace collapsed to one space.

    Args:
      text: Any text.

    Returns:
      The collapsed text.

    Raises:
      None.
    """
    return " ".join(text.split())


def section_12_leads(text: str) -> tuple[list[str], list[str]] | None:
    """The bold leads of §12's bullets between `### Open` and `### Resolved`: open, then struck.

    Each bullet is read as a paragraph, its lines joined, because a lead wraps. A bullet opening
    `- ~~**` is struck through, and its lead is never an open item's.

    Args:
      text: `docs/architecture.md`.

    Returns:
      `(open leads, struck leads)`, whitespace collapsed; `None` when either heading is missing.

    Raises:
      None.
    """
    if "\n### Open\n" not in text or "\n### Resolved\n" not in text:
        return None
    section = text.split("\n### Open\n", 1)[1].split("\n### Resolved\n", 1)[0]
    open_leads: list[str] = []
    struck: list[str] = []
    for bullet in re.split(r"\n(?=- )", "\n" + section):
        for prefix, into in (("- **", open_leads), ("- ~~**", struck)):
            if bullet.startswith(prefix):
                into.append(_collapse(bullet[len(prefix) :].split("**", 1)[0]))
    return open_leads, struck


def register_faults(
    rows: Iterable[str],
    pins: Iterable[Pin],
    departures: Iterable[Departure],
    texts: Mapping[str, str],
) -> list[str]:
    """One failure line per problem with the register; empty when it is sound.

    Every pinned and departure row is a generated row, in one entry only; every pin's item is the
    bold lead of an open §12 bullet; every departure's phrase is in its file once whitespace is
    collapsed. It reads only `texts`, a mapping of file to content, so a fixture never touches the
    repository.

    Args:
      rows: The generated row ids.
      pins: The pins.
      departures: The departures.
      texts: File contents, by repository-relative path.

    Returns:
      The failure lines.

    Raises:
      None.
    """
    generated = set(rows)
    pin_list, departure_list = list(pins), list(departures)
    lines: list[str] = []
    owners: dict[str, list[str]] = collections.defaultdict(list)
    for entry in [*pin_list, *departure_list]:
        for row in entry.rows:
            owners[row].append(entry.name)
            if row not in generated:
                lines.append(f"{entry.name} names {row}, which the table does not generate")
    for row, names in sorted(owners.items()):
        if len(names) > 1:
            lines.append(f"{row} is in {len(names)} entries: {', '.join(names)}")
    leads = section_12_leads(texts.get(ARCHITECTURE, ""))
    if leads is None:
        lines.append(f"{ARCHITECTURE} has no §12 between `### Open` and `### Resolved`")
    else:
        open_leads, struck = leads
        for pin in pin_list:
            item = _collapse(pin.item)
            if item in open_leads:
                continue
            if item in struck:
                lines.append(f"{pin.name}'s item is struck through in §12: {pin.item}")
            else:
                lines.append(f"{pin.name}'s item is not the lead of an open §12 bullet: {pin.item}")
    for departure in departure_list:
        path, phrase = departure.record
        if path not in texts:
            lines.append(f"{departure.name}'s record {path} is not in the texts read")
        elif _collapse(phrase) not in _collapse(texts[path]):
            lines.append(f"{departure.name}'s phrase is not in {path}: {phrase}")
    return lines
