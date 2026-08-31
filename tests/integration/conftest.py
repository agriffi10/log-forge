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

import os
import socket
import time
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
    """Reports whether the broker answers a metadata request.

    **Not a TCP connect, and the difference is a real trap.** The broker advertises the
    *host-published* port from `KAFKA_ADVERTISED_LISTENERS`, and a client follows whatever
    address the metadata response names. If that value and the compose port mapping disagree,
    a connect succeeds and every later produce fails on the advertised address -- so the probe
    has to ask the question a client asks.

    Args:
      endpoint: The broker to probe.

    Returns:
      True when metadata came back.

    Raises:
      None.
    """
    try:
        from confluent_kafka.admin import AdminClient

        AdminClient({"bootstrap.servers": endpoint.url_host}).list_topics(timeout=2.0)
    except Exception:
        return False
    return True


_PROBES: dict[str, Callable[[Endpoint], bool]] = {"kafka": _kafka_ready}
"""Per-service readiness probes, where a TCP connect is not the right question.

Everything else answers a connect honestly enough: the client's own first call is the real test,
and a fixture that reached this point has a service listening.
"""


@pytest.fixture(scope="session", autouse=True)
def services_are_up() -> dict[str, Endpoint]:
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

    def wait(item: tuple[str, Endpoint]) -> tuple[str, bool]:
        name, endpoint = item
        probe = _PROBES.get(name, _tcp_ready)
        while time.monotonic() < deadline:
            if probe(endpoint):
                return name, True
            time.sleep(_PROBE_INTERVAL)
        return name, False

    with ThreadPoolExecutor(max_workers=len(endpoints)) as pool:
        results = dict(pool.map(wait, endpoints.items()))
    missing = sorted(name for name, ok in results.items() if not ok)
    if missing:
        raise RuntimeError(
            f"integration services unreachable after {_READY_DEADLINE:.0f}s: {', '.join(missing)}"
            " -- start them with"
            " `docker compose -f tests/integration/docker-compose.yml up -d`"
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
    "test_postgres": 2,
    "test_redis": 2,
    "test_kafka": 2,
    "test_logstash": 2,
    "test_nats": 2,
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
    problems: list[str] = []
    for outcome in ("skipped", "xfailed", "error"):
        count = len(reporter.stats.get(outcome, []))
        if count:
            problems.append(f"{count} {outcome} -- an absent service must fail, not skip")
    passed = reporter.stats.get("passed", [])
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
