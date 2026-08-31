"""Every sink that talks to a real destination is verified against one, or says why not.

SPEC-041 FR-001 AC-4: "which sinks remain unverified after this job exists is written down, so
the next audit knows what it is still reading rather than running." This is that record, and it
is a **derived** roster rather than prose, because a hand-listed set is one this repo has twice
watched rot (SPEC-028's roster missed three sinks; SPEC-038's two rosters drifted twice).

**The population is derived from two markers, and the second one is the correction.** The obvious
derivation -- modules carrying a lazy third-party import (`# type: ignore[import-not-found]`) --
is what an optional extra looks like, and it yields fourteen modules behind the eleven optional
extras the spec's Overview counts (`aws` alone covers four of them). It
also **silently excludes `logstash`**, which is one of AC-1's named minimum four *and* the entire
subject of FR-003, because that sink reaches Logstash over stdlib HTTP and imports no third-party
client at all. The same hole hides `syslog`, `elasticsearch`, `loki` and the four SaaS sinks. So
the population is that set **union** the modules that reach a network destination through
`HTTPSink` or `SocketTransport`: "needs an optional extra" and "talks to something real" are
different questions, and AC-4 asks the second.

**What this file certifies is a name, not a run.** It asserts an integration module exists for
each verified sink; whether that module's tests actually executed is enforced by the floor in
`tests/integration/conftest.py`, which fails a run where a module contributed nothing or where
anything skipped. Stated here rather than left for a reader to discover, because the two halves
are only a guarantee together.
"""

from __future__ import annotations

import ast
import pathlib

import log_foundry

_SINKS = pathlib.Path(log_foundry.__file__).parent / "sinks"
_INTEGRATION = pathlib.Path(__file__).parent / "integration"

_NETWORK_CORES = {
    ("sinks.http", "HTTPSink"),
    ("sinks._socket", "SocketTransport"),
}
"""The two things a stdlib-only sink imports when it reaches a network destination.

Kept as (module suffix, name) pairs rather than matched on the module alone: importing something
*else* from `sinks.http` -- `merge_headers`, say -- does not make a sink a network client.
"""

_NETWORK_CORE_NAMES = {name for _, name in _NETWORK_CORES}
"""The same cores by bare name, for the module that *defines* one instead of importing it."""


def _population() -> dict[str, set[str]]:
    """Returns the sink modules that reach a real destination, by how they were detected.

    A module qualifies as `network` by **importing** a network core or by **defining** one that
    is itself a sink. Importing alone was the first version and left a hole: `sinks/http.py`
    defines `HTTPSink` rather than importing it, so the module holding the one directly
    constructible generic HTTP sink -- public, and documented in `README.md` -- was outside the
    population and could not be given a roster answer at all. "Defines a class with an `emit`"
    is what keeps `_socket.SocketTransport` out on the same pass: it is a transport a sink owns,
    not a sink, which is the distinction `test_sink_release_roster.py` had to draw too.
    """
    marker: set[str] = set()
    network: set[str] = set()
    for path in sorted(_SINKS.glob("*.py")):
        text = path.read_text(encoding="utf-8")
        tree = ast.parse(text)
        if "import-not-found" in text or "import-untyped" in text:
            marker.add(path.stem)
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                for alias in node.names:
                    for suffix, name in _NETWORK_CORES:
                        if node.module.endswith(suffix) and alias.name == name:
                            network.add(path.stem)
            if (
                isinstance(node, ast.ClassDef)
                and node.name in _NETWORK_CORE_NAMES
                and any(
                    isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef)
                    and child.name == "emit"
                    for child in node.body
                )
            ):
                network.add(path.stem)
    return {"marker": marker, "network": network}


VERIFIED: dict[str, str] = {
    "clickhouse": "test_clickhouse",
    # `LogstashSink(url=...)` IS an `HTTPSink` -- it builds one and delegates to it -- so
    # `test_logstash` drives this module against a real HTTP destination. Two sink modules
    # legitimately share one integration module; the cross-check below compares sets.
    "http": "test_logstash",
    "kafka": "test_kafka",
    "logstash": "test_logstash",
    "mongodb": "test_mongo",
    "nats": "test_nats",
    "postgres": "test_postgres",
    "pubsub": "test_pubsub",
    "rabbitmq": "test_rabbitmq",
    "redis": "test_redis",
}
"""Sink modules exercised against a real service, and the integration module that does it."""

UNVERIFIED: dict[str, str] = {
    "sqs": "AWS; no emulator is run in the job. Fakes cover chunking and id-keyed adjudication.",
    "sns": "AWS; as sqs.",
    "kinesis": "AWS; as sqs. Positional adjudication is covered by fakes (SPEC-018).",
    "firehose": "AWS; as sqs.",
    "eventhubs": "Azure; no emulator is run in the job.",
    "sentry": "SaaS; no local ingest. The HTTP-envelope fallback is covered by a fake opener.",
    "datadog": "SaaS; no local ingest.",
    "splunk": "SaaS; no local ingest.",
    "newrelic": "SaaS; no local ingest.",
    "honeycomb": "SaaS; no local ingest.",
    "elasticsearch": "An official image exists upstream; none is run here. Not in AC-1's minimum.",
    "loki": "An official image exists upstream; none is run here. Not in AC-1's minimum.",
    "syslog": "No container is run; the socket transport is covered by a local listener fake.",
}
"""Sink modules NOT executed against a real service, each with the reason.

The `elasticsearch`/`loki` entries are deliberately honest rather than flattering: unlike the
AWS and SaaS entries there is no obstacle beyond a decision -- an official image exists for each
-- and a reader deciding what to add next should be able to see that at a glance.
"""


def test_every_sink_that_talks_to_something_real_is_verified_or_says_why_not() -> None:
    population = set().union(*_population().values())
    answered = set(VERIFIED) | set(UNVERIFIED)

    unanswered = population - answered
    assert not unanswered, (
        f"sink module(s) with no integration answer: {sorted(unanswered)}. Add an integration "
        "module and a VERIFIED entry, or an UNVERIFIED entry with a reason."
    )
    stale = answered - population
    assert not stale, f"roster names module(s) that no longer reach a destination: {sorted(stale)}"


def test_logstash_is_in_the_population_via_the_network_marker() -> None:
    # The reason this roster derives from two markers rather than one. `logstash` is AC-1's named
    # minimum and FR-003's subject, and it carries no third-party import marker at all, so a
    # single-marker population would omit exactly the sink this spec exists to fix.
    detected = _population()
    assert "logstash" not in detected["marker"]
    assert "logstash" in detected["network"]


def test_every_verified_sink_names_an_integration_module_that_exists() -> None:
    for sink, module in sorted(VERIFIED.items()):
        assert (_INTEGRATION / f"{module}.py").is_file(), (
            f"{sink} is recorded as verified by {module}.py, which does not exist"
        )


def test_the_service_rosters_agree_with_each_other() -> None:
    # Three hand-written lists describe the same nine services: the compose file's `services:`,
    # the conftest's SERVICES/MODULE_FLOORS, and the integration modules on disk. SPEC-038 FR-001
    # AC-1a/AC-1b records what happens without this check -- two rosters drifted twice, once on
    # the trigger and once on the base spelling.
    from integration.conftest import MODULE_FLOORS, SERVICES

    compose = (_INTEGRATION / "docker-compose.yml").read_text(encoding="utf-8")
    declared = {
        line[2:-1] for line in compose.splitlines() if line.startswith("  ") and line.endswith(":")
        and not line.startswith("    ")
    }
    # BOTH directions. The first version asserted only that every SERVICES key was in the compose
    # file, so a service added with no readiness probe and no module floor went unnoticed --
    # confirmed by appending an `elasticsearch:` entry and watching all five tests stay green.
    assert declared == set(SERVICES), (
        f"compose declares {sorted(declared)} but SERVICES names {sorted(SERVICES)}"
    )
    assert set(MODULE_FLOORS) == set(VERIFIED.values()), (
        "the conftest's per-module floors and the roster's verified modules have drifted"
    )
    on_disk = {path.stem for path in _INTEGRATION.glob("test_*.py")}
    assert on_disk == set(VERIFIED.values()), (
        f"integration modules on disk {sorted(on_disk)} do not match the roster "
        f"{sorted(VERIFIED.values())}"
    )


def test_the_roster_has_not_collapsed() -> None:
    # The floor both sibling rosters carry, and for the reason SPEC-038 records: moving five
    # `emit`s into a base class dropped five classes out of two lints in one commit, 34 to 29,
    # with the suite green, and only the roster that had a floor noticed.
    population = set().union(*_population().values())
    assert len(population) >= 23, f"population collapsed to {len(population)}"
    assert len(VERIFIED) >= 10, f"verified set collapsed to {len(VERIFIED)}"
