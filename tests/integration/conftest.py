"""The integration suite's gate, readiness probes and vacuity floor (SPEC-041 FR-001).

These tests run the extras-backed sinks against real services in containers, which the gating
no-extras CI leg deliberately cannot do (`CLAUDE.md`: that environment is the contract `mypy
--strict`'s ignore comments depend on). Stand the services up with
`tests/integration/docker-compose.yml` and set `LOG_FOUNDRY_INTEGRATION=1`.

**This module imports nothing beyond the standard library at module level, and that is
load-bearing.** `testpaths = ["tests"]` means the *gating* no-extras run imports this file even
though `collect_ignore_glob` below stops it collecting anything here. An extras import at module
level would therefore fail the run this whole design exists to leave untouched.
"""

from __future__ import annotations

import json
import os
import socket
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable

GATE = "LOG_FOUNDRY_INTEGRATION"
"""The environment variable that turns this suite on.

Not a pytest marker: a marker is deselected with `-m`, which leaves a green run behind, and the
point of the gate is that turning the suite off and running it are visibly different outcomes.
"""

if not os.environ.get(GATE):
    collect_ignore_glob = ["test_*.py"]

_READY_DEADLINE = 90.0
"""Seconds allowed for **all** services to come up, shared across every probe.

One deadline for the set, not one per service. A per-service timeout is not a bound at all --
nine unreachable services at 60 s each is nine minutes before the job says anything, which is
SPEC-038 FR-004's measured lesson about `GooglePubSubSink._await_overflow` applied to a test
harness. The probes run concurrently against this single deadline, so an entirely dead
environment is reported in 90 s rather than in 9 minutes.
"""

_PROBE_INTERVAL = 0.5


@dataclass(frozen=True)
class Endpoint:
    """A host and port a service is reachable on.

    Frozen because a fixture hands the same object to every test in the session, and a test that
    reassigned a port would silently redirect its neighbours.

    Attributes:
      host: The host the test process connects to.
      port: The published port on that host.
    """

    host: str
    port: int

    @property
    def url_host(self) -> str:
        """Renders the endpoint as ``host:port`` for a URL or a client's server list.

        Args:
          None.

        Returns:
          The rendered authority.

        Raises:
          None.
        """
        return f"{self.host}:{self.port}"


def _endpoint(name: str, default_port: int) -> Endpoint:
    """Reads one service's endpoint from the environment, defaulting to the compose mapping.

    Args:
      name: The service's name in `docker-compose.yml`, upper-cased for the variable.
      default_port: The port that compose file publishes.

    Returns:
      The endpoint.

    Raises:
      None.
    """
    prefix = f"LOG_FOUNDRY_{name.upper()}"
    return Endpoint(
        host=os.environ.get(f"{prefix}_HOST", "127.0.0.1"),
        port=int(os.environ.get(f"{prefix}_PORT", str(default_port))),
    )


SERVICES: dict[str, int] = {
    "postgres": 55432,
    "redis": 56379,
    "kafka": 59092,
    "logstash": 55044,
    "nats": 54222,
    "pubsub": 58085,
    "mongo": 57017,
    "rabbitmq": 55672,
    "clickhouse": 58123,
}
"""Every service this suite needs, and the port `docker-compose.yml` publishes it on.

`test_sink_integration_roster.py` asserts this dict, the compose file's `services:` and the
module floors below all name the same nine. Three hand-written lists of one set is how the sink
rosters drifted twice (SPEC-038 FR-001 AC-1a/AC-1b); the cross-check is what stops it here.
"""


def _tcp_ready(endpoint: Endpoint) -> bool:
    """Reports whether a TCP connection to the endpoint succeeds.

    Args:
      endpoint: The service to probe.

    Returns:
      True when the connection was accepted.

    Raises:
      None.
    """
    try:
        with socket.create_connection((endpoint.host, endpoint.port), timeout=2.0):
            return True
    except OSError:
        return False


def _kafka_ready(endpoint: Endpoint) -> bool:
    """Reports whether the broker is serving and advertises the **port** this probe dialled.

    Two things a TCP connect cannot tell you, and they are separate.

    The first is protocol readiness: the container accepts connections well before KRaft has
    elected a controller, so a bare connect goes green on a broker that will refuse the next
    produce. A metadata request is the earliest call that proves the broker is actually serving.

    The second is the compose file's stated trap. `KAFKA_ADVERTISED_LISTENERS` names the
    *host-published* port, and a client follows whatever address metadata gives it rather than
    the one it dialled -- so a `ports:` mapping changed without the environment variable leaves
    every connect succeeding and every produce failing. A metadata call alone does **not** catch
    that either: it completes on the bootstrap connection and never dials the advertised address.
    So the probe compares them, which is the only part of this that actually checks the trap.

    **Only the port is compared, and that is a constraint rather than an oversight.** The
    endpoint's host defaults to `127.0.0.1` while the broker advertises `localhost`, so a host
    comparison would fail on every run. Do not "tighten" this to compare the host.

    Args:
      endpoint: The broker to probe.

    Returns:
      True when metadata came back and names the port this probe connected to.

    Raises:
      None.
    """
    try:
        from confluent_kafka.admin import AdminClient

        metadata = AdminClient({"bootstrap.servers": endpoint.url_host}).list_topics(timeout=2.0)
    except Exception:
        return False
    if any(broker.port == endpoint.port for broker in metadata.brokers.values()):
        return True
    advertised = sorted({broker.port for broker in metadata.brokers.values()})
    raise RuntimeError(
        f"kafka is running and answering, but advertises port(s) {advertised} while this suite "
        f"connects on {endpoint.port}. `KAFKA_ADVERTISED_LISTENERS` and `ports:` in "
        "docker-compose.yml have to name the same host-published port."
    )


READINESS_MARKER = "__log_foundry_readiness_probe__"
"""Field marking the event a Logstash readiness probe necessarily injects.

The probe has to POST a real request (see :func:`_logstash_ready`), and every accepted request
becomes an event at the destination. Marking it is what lets a test filter it out rather than
race it: the pipeline may deliver the probe's event *after* a test has cleared the key.
"""


def _logstash_ready(endpoint: Endpoint) -> bool:
    """Reports whether Logstash's ``http`` input is actually serving requests.

    **A TCP connect is not enough, and this was measured in CI rather than reasoned.** The input
    binds its port well before the pipeline is ready, so a connect succeeds and the first real
    POST is met with a reset — three Logstash tests failed on the first run of this job with
    ``ConnectionResetError errno=104`` while every local run passed, because a laptop's
    containers had been up for minutes. It is the same shape as the Kafka probe above: the only
    proof a service is serving is asking it the question a client asks.

    Args:
      endpoint: The HTTP input to probe.

    Returns:
      True when a request was accepted.

    Raises:
      None.
    """
    request = urllib.request.Request(
        f"http://{endpoint.url_host}",
        data=json.dumps({READINESS_MARKER: True}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=2.0) as response:  # noqa: S310
            return 200 <= response.status < 300
    except Exception:
        return False


_PROBES: dict[str, Callable[[Endpoint], bool]] = {
    "kafka": _kafka_ready,
    "logstash": _logstash_ready,
}
"""Per-service readiness probes, where a TCP connect is not the right question.

Everything else answers a connect honestly enough: the client's own first call is the real test,
and a fixture that reached this point has a service listening.
"""


@pytest.fixture(scope="session", autouse=True)
def _not_distributed(request: pytest.FixtureRequest) -> None:
    """Refuses a session that would run this directory's tests across processes.

    **A correctness guard.** `addopts` carries `-n 12 --dist worksteal`, so parallel is the DEFAULT for
    every invocation and this suite must opt out with `-n 0`: the nine modules share nine real
    services, so distributing them has them draining each other's destinations. Measured with the
    services up, on this repo's own documented recipe: `-n 0` gave `19 passed`, and the same
    command without it gave `1 failed, 18 passed` -- a Logstash test reading a destination its
    sibling had already consumed. A requirement stated only in a comment is enforced by nobody.

    Three mechanism choices, each measured rather than assumed. It runs BEFORE
    :func:`services_are_up`, which requests it, because that fixture spends up to 90 s probing
    nine services and a run that is already invalid should say so first -- ordering by
    *dependency* rather than by definition order, which is not guaranteed. It raises from a
    fixture rather than from `pytest_collection_modifyitems`, because a `UsageError` raised during
    a worker's collection surfaces not as a message but as `INTERNALERROR ... assert not
    crashitem`. And the predicate reads `workerinput` and not only `getoption("dist")`, because in
    an xdist **worker** `dist` reads `"no"` -- the controller holds `"load"` -- so a `dist`-only
    test would never fire in the very place this runs.

    It defers to :func:`_gate_is_set` when the gate is unset, returning rather than refusing.
    Naming a file directly with no gate is the invocation form that fixture exists to diagnose,
    and refusing here first told that caller to add `-n 0` -- which then produced the gate error
    they should have seen in the first place, a two-step diagnosis for one mistake.

    Args:
      request: The fixture request, for the session config carrying the xdist state.

    Returns:
      None.

    Raises:
      RuntimeError: If the session is distributing tests across worker processes.
    """
    if not os.environ.get(GATE):
        return
    config = request.config
    if hasattr(config, "workerinput") or config.getoption("dist", "no") != "no":
        raise RuntimeError(
            "the integration suite must not be distributed -- these tests share nine real "
            "services and will drain each other's destinations. `addopts` defaults to "
            "`-n 12 --dist worksteal`, so add `-n 0`: "
            "`LOG_FOUNDRY_INTEGRATION=1 poetry run pytest tests/integration -n 0`"
        )


@pytest.fixture(scope="session", autouse=True)
def services_are_up(_not_distributed: None) -> dict[str, Endpoint]:
    """Waits for every service, concurrently, against one shared deadline.

    **It fails rather than skips**, which is the whole point. A skipped integration test leaves a
    green job that verified nothing, and `pytest` exits 0 on a fully skipped session -- so
    "the service was not there" has to be a failure or the job's premise is unchecked.

    Autouse and session-scoped, so a run cannot reach a test without passing through it.

    Args:
      None.

    Returns:
      Every service's endpoint, keyed by name.

    Raises:
      RuntimeError: If any service was still unreachable at the deadline.
    """
    endpoints = {name: _endpoint(name, port) for name, port in SERVICES.items()}
    deadline = time.monotonic() + _READY_DEADLINE

    def wait(item: tuple[str, Endpoint]) -> tuple[str, str | None]:
        """Returns the service's name and its failure reason, or None once it is ready."""
        name, endpoint = item
        probe = _PROBES.get(name, _tcp_ready)
        while time.monotonic() < deadline:
            try:
                if probe(endpoint):
                    return name, None
            except Exception as err:
                # A probe that RAISES has diagnosed something retrying cannot fix -- a broker
                # that is up but advertising the wrong port, say. Reporting that as "unreachable
                # after 90s, start the containers" would send a developer to fix a container
                # that is already running.
                return name, str(err)
            time.sleep(_PROBE_INTERVAL)
        return name, "not reachable"

    with ThreadPoolExecutor(max_workers=len(endpoints)) as pool:
        results = dict(pool.map(wait, endpoints.items()))
    problems = sorted((name, why) for name, why in results.items() if why is not None)
    if problems:
        detail = "; ".join(f"{name}: {why}" for name, why in problems)
        raise RuntimeError(
            f"integration services not usable after {_READY_DEADLINE:.0f}s -- {detail}. Start "
            "them with `docker compose -f tests/integration/docker-compose.yml up -d`."
        )
    return endpoints


@pytest.fixture(autouse=True)
def _gate_is_set() -> None:
    """Fails any test reached with the gate unset.

    `collect_ignore_glob` is a *directory* filter, not a gate: naming a file directly --
    `pytest tests/integration/test_postgres.py` -- bypasses it entirely and would run against
    whatever happens to be listening. This closes that second invocation form.

    Args:
      None.

    Returns:
      None.

    Raises:
      RuntimeError: If the gate variable is unset.
    """
    if not os.environ.get(GATE):
        raise RuntimeError(
            f"the integration suite requires {GATE}=1 and the services in docker-compose.yml"
        )


MODULE_FLOORS: dict[str, int] = {
    "test_postgres": 4,
    "test_redis": 2,
    "test_kafka": 2,
    "test_logstash": 2,
    "test_nats": 7,
    "test_pubsub": 2,
    "test_mongo": 1,
    "test_rabbitmq": 1,
    "test_clickhouse": 1,
}
"""The least each module may contribute to a full run, keyed by file stem.

A floor rather than an exact count so adding a test is not a two-file edit; a *per-module* floor
rather than one total so a whole service dropping out cannot be masked by another module's
tests. The numbers are what each module ships today, not a target.
"""


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Fails a run that collected work but verified nothing (FR-001).

    **Recorded limit: this does not report from an xdist CONTROLLER.** Measured, gate exported:
    a whole-suite `pytest` (parallel by default since `addopts` carries `-n 12`) printed ZERO
    `INTEGRATION FLOOR` lines, while `pytest tests/integration -n 0` printed 10. The controller
    loads conftests only for the invocation's initial paths, and a worker setting
    `session.exitstatus` cannot change the controller's. No false green follows today: CI runs
    this suite with `-n 0`, where the floor works, and the one invocation that would otherwise
    reach here distributed is refused outright by :func:`_not_distributed` -- the run above still
    exited 1, on the guard's 10 errors rather than on this. Left as a limit rather than fixed
    because the scenario it covers no longer exists.

    **The vacuity guard, and it is not an exit code.** `pytest tests/integration` exits 5 when
    nothing is collected, which catches only a forgotten gate variable -- a typo that surfaces on
    the very first run and never again. Every way an integration suite dies quietly exits **0**:
    a fixture that skips on an unreachable service, a `skipif`, a body-level `pytest.skip`. So
    the guard is this repo's own answer, the floor that `test_sink_concurrency.py` and
    `test_sink_release_roster.py` already carry.

    Two rules, deliberately separate:

    - **No skips, ever, and this always applies.** The only reason to skip here is that a service
      is absent, which is a failure of the job's premise.
    - **Every expected module contributed a pass**, applied only to a full-directory run. A
      developer narrowing to one test with `-k` or a file path would otherwise be told the other
      eight modules are missing, which measured as `2 passed` and a session exit of 1.

    It returns early when the gate is unset, and *that* is load-bearing rather than defensive:
    `testpaths = ["tests"]` means the gating no-extras run imports this module and registers this
    hook, and seven files in that suite carry conditional skips. Without the early return, adding
    this file turns `main`'s own gate red.

    Args:
      session: The finished session.
      exitstatus: The status pytest computed, overridden here on a floor breach.

    Returns:
      None.

    Raises:
      None.
    """
    if not os.environ.get(GATE):
        return
    reporter = session.config.pluginmanager.getplugin("terminalreporter")
    if reporter is None:
        return
    if not _collected_here(reporter):
        return
    problems: list[str] = []
    for outcome in ("skipped", "xfailed", "error"):
        count = len(_ours(reporter, outcome))
        if count:
            problems.append(f"{count} {outcome} -- an absent service must fail, not skip")
    passed = _ours(reporter, "passed")
    if _is_whole_suite(session):
        per_module: dict[str, int] = {}
        for report in passed:
            stem = report.nodeid.split("/")[-1].split("::")[0].removesuffix(".py")
            per_module[stem] = per_module.get(stem, 0) + 1
        for stem, floor in sorted(MODULE_FLOORS.items()):
            actual = per_module.get(stem, 0)
            if actual < floor:
                problems.append(f"{stem} contributed {actual} passing test(s), floor is {floor}")
    if problems:
        reporter.write_line("")
        for problem in problems:
            reporter.write_line(f"INTEGRATION FLOOR: {problem}", red=True)
        session.exitstatus = 1


_HERE = "tests/integration/"


def _ours(reporter: pytest.TerminalReporter, outcome: str) -> list[pytest.TestReport]:
    """Returns only this directory's reports for one outcome.

    **Every term is scoped to this directory, and the alternative was measured wrong.** The rule
    "nothing may skip" is about integration tests, where a skip means an absent service. Applied
    to the whole session it also judges the ordinary suite, which legitimately skips: with the
    gate variable exported in a shell -- which this module's docstring tells a developer to do --
    a whole-suite run in the extras environment reported `1780 passed, 3 skipped` and then failed
    it with "3 skipped -- an absent service must fail".

    A first fix guarded on the session having collected anything here at all, and that was aimed
    at the wrong mechanism: in the case that actually bites, the integration tests *were*
    collected and ran fine, and the skips came from `test_fork_lifecycle.py` next door.

    Args:
      reporter: The terminal reporter holding the session's outcomes.
      outcome: The stats key to read.

    Returns:
      The reports for that outcome whose node id is under this directory. An entry carrying no
      node id at all is **kept**, not dropped: this is a guard against silent vacuity, so the
      unknown case has to fail loudly rather than quietly leave the tally.

    Raises:
      None.
    """
    return [
        report
        for report in reporter.stats.get(outcome, [])
        if getattr(report, "nodeid", _HERE).startswith(_HERE)
    ]


def _collected_here(reporter: pytest.TerminalReporter) -> bool:
    """Reports whether this session ran any integration test at all.

    **This is not the guard that keeps the ordinary suite green** -- `_ours` is, by scoping
    every term to this directory. An earlier revision claimed that job for this function and
    was measured wrong: neutralising it and replaying the case it named (the gate variable
    exported, a whole-suite run, `test_fork_lifecycle.py`'s skips) still exits 0.

    What it does cover is the one shape `_ours` cannot: a whole-suite-shaped invocation that
    collected **no** integration tests, such as `pytest tests --ignore=tests/integration` with
    the gate exported. `_is_whole_suite` reports True there, so without this every one of the
    nine per-module floors fires against a session that was never asked to run them.

    Args:
      reporter: The terminal reporter holding the session's outcomes.

    Returns:
      True when at least one report came from this directory.

    Raises:
      None.
    """
    return any(
        _ours(reporter, outcome)
        for outcome in ("passed", "failed", "skipped", "xfailed", "error")
    )


def _is_whole_suite(session: pytest.Session) -> bool:
    """Reports whether this run was the whole integration directory, unfiltered.

    The per-module floor only means anything about a run that asked for every module. Selection
    by `-k`, by `-m`, or by naming a file makes a missing module the caller's intent rather than
    a service that vanished.

    Args:
      session: The session to inspect.

    Returns:
      True when no selection narrowed the run.

    Raises:
      None.
    """
    option = session.config.option
    if getattr(option, "keyword", "") or getattr(option, "markexpr", ""):
        return False
    return not any(arg.endswith(".py") or "::" in arg for arg in session.config.args)
