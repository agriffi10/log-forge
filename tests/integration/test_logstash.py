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
            out.append(json.loads(item))
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
