"""SPEC-041 FR-003 — what a *stock* Logstash makes of what `LogstashSink` currently sends.

**These tests characterize the defect, not the fix.** FR-003 AC-1 requires the analysis to be
verified against a real Logstash *before changing anything*, and this is that evidence: the audit
called K10 the lowest-confidence finding it had, and the one where a wrong analysis would make the
fix worse than the defect. So the assertions below record today's behaviour, and the spec's Phase 3
inverts them once the body format changes.

The observation route matters as much as the assertion. Logstash outputs to Redis
(`tests/integration/logstash.conf`), so a test reads back **what Logstash parsed** rather than
that a POST returned 200 -- which it does either way, and which is exactly why reading the code
could not settle this.
"""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING

import pytest
import redis as redis_mod

from integration.conftest import READINESS_MARKER
from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.logstash import LogstashSink

if TYPE_CHECKING:
    from integration.conftest import Endpoint

PARSED_KEY = "logstash-parsed"


@pytest.fixture
def parsed(services_are_up: dict[str, Endpoint]):
    endpoint = services_are_up["redis"]
    client = redis_mod.Redis.from_url(f"redis://{endpoint.url_host}")
    client.delete(PARSED_KEY)

    def drain(at_least: int, settle: float = 12.0) -> list[dict[str, object]]:
        # Wait for `at_least`, then keep draining briefly: a test asserting "only one event
        # arrived" is worthless if it stops reading the moment the first one lands.
        #
        # The readiness probe POSTs a real request, because binding the port is not the same as
        # serving (see `_logstash_ready`), and every accepted request becomes an event. That
        # event is filtered rather than raced: the pipeline may deliver it after this fixture
        # has cleared the key, and it would otherwise land in a test's count.
        deadline = time.monotonic() + settle
        out: list[dict[str, object]] = []
        while time.monotonic() < deadline:
            item = client.lpop(PARSED_KEY)
            if item is None:
                if len(out) >= at_least:
                    time.sleep(0.5)
                    if client.llen(PARSED_KEY) == 0:
                        break
                time.sleep(0.3)
                continue
            event = json.loads(item)
            if READINESS_MARKER not in event:
                out.append(event)
        return out

    yield drain
    client.delete(PARSED_KEY)


def test_the_current_ndjson_body_arrives_as_one_event(
    services_are_up: dict[str, Endpoint], parsed
) -> None:
    sink = LogstashSink(url=f"http://{services_are_up['logstash'].url_host}")
    sink.emit([{"case": "k10", "n": 1}, {"case": "k10", "n": 2}, {"case": "k10", "n": 3}])

    events = parsed(1)

    # The whole batch collapses into a single Logstash event. `application/x-ndjson` is not in
    # the `http` input's default `additional_codecs` map, so the body falls through to the
    # `plain` codec.
    assert len(events) == 1, f"expected the K10 defect, got {len(events)} events"


def test_the_current_body_loses_every_field_into_message(
    services_are_up: dict[str, Endpoint], parsed
) -> None:
    sink = LogstashSink(url=f"http://{services_are_up['logstash'].url_host}")
    sink.emit([{"case": "k10", "n": 1}, {"case": "k10", "n": 2}])

    events = parsed(1)
    assert len(events) == 1
    only = events[0]

    # This is the half that makes K10 a data defect rather than a cosmetic one: the structured
    # fields do not exist at the destination at all. They are text inside `message`.
    assert "case" not in only
    assert "n" not in only
    assert '"case": "k10"' in str(only.get("message", "")) or '"case":"k10"' in str(
        only.get("message", "")
    )


def test_a_json_array_body_arrives_as_one_event_per_element(
    services_are_up: dict[str, Endpoint], parsed
) -> None:
    # The other half of FR-003 AC-1's measurement, and the reason the finding is actionable
    # rather than merely true: the SAME stock input parses a JSON array correctly. Without this
    # the two tests above establish only that something is wrong, not that anything better
    # exists -- and the audit called K10 the one finding where a wrong analysis would make the
    # fix worse than the defect.
    #
    # It drives `HTTPSink` directly rather than `LogstashSink`, because on this branch the sink
    # still hardcodes `body_format="ndjson"` and forwarding `body_format=` through its
    # `**http_kwargs` raises `TypeError: got multiple values`. That is itself part of what
    # FR-003 has to fix; here it just means the comparison is made one layer down.
    sink = HTTPSink(
        f"http://{services_are_up['logstash'].url_host}", body_format="json_array"
    )
    sink.emit([{"case": "array", "n": 1}, {"case": "array", "n": 2}, {"case": "array", "n": 3}])

    events = parsed(3)

    assert len(events) == 3, f"a JSON array should parse per element, got {len(events)}"
    assert sorted(event["n"] for event in events) == [1, 2, 3]
    assert all(event["case"] == "array" for event in events)
