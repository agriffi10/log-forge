"""SPEC-041 FR-003 — what a *stock* Logstash makes of what `LogstashSink` sends.

The audit called K10 its lowest-confidence finding, and the one where a wrong analysis would make
the fix worse than the defect, so FR-003 AC-1 required verifying it against a real Logstash
*before changing anything*. It was verified, it was right, and these tests now hold **both** ends
of that measurement: the default parses per event, and the old wire form still collapses a whole
batch into one -- which is why the old form remains reachable rather than deleted.

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


def test_the_default_body_arrives_as_one_event_per_log_line(
    services_are_up: dict[str, Endpoint], parsed
) -> None:
    sink = LogstashSink(url=f"http://{services_are_up['logstash'].url_host}")
    sink.emit([{"case": "fixed", "n": 1}, {"case": "fixed", "n": 2}, {"case": "fixed", "n": 3}])

    events = parsed(3)

    # Against a STOCK `http` input -- no `codec`, no `additional_codecs` (see logstash.conf).
    assert len(events) == 3, f"expected one event per log line, got {len(events)}"
    assert sorted(event["n"] for event in events) == [1, 2, 3]
    assert all(event["case"] == "fixed" for event in events)


def test_the_ndjson_escape_hatch_still_produces_the_old_wire_form(
    services_are_up: dict[str, Endpoint], parsed
) -> None:
    # K10 itself, still reproducible on demand. This is not nostalgia: an input configured
    # `additional_codecs => {"application/x-ndjson" => "json_lines"}` -- the documented
    # workaround for the defect -- parses THIS body correctly and the new default incorrectly,
    # because that setting replaces the default map rather than merging with it. So the old form
    # has to stay reachable, and this test is what stops it being quietly dropped.
    sink = LogstashSink(
        url=f"http://{services_are_up['logstash'].url_host}", body_format="ndjson"
    )
    sink.emit([{"case": "k10", "n": 1}, {"case": "k10", "n": 2}, {"case": "k10", "n": 3}])

    events = parsed(1)

    assert len(events) == 1, "the old wire form collapses the batch against a stock input"
    only = events[0]
    assert "case" not in only and "n" not in only, "the fields survive only as text"
    assert '"case": "k10"' in str(only.get("message", ""))
