"""A third-party consumer of `log_foundry`, written to type-check clean under `mypy --strict`.

This is not run as a test. `tests/test_typed_consumer.py` type-checks it in a subprocess, because
the repo's own gate is `files = ["src"]` and cannot see a consumer at all -- which is how
`configure(defaults=)` stayed unusable from typed code for the whole life of the project under a
green `mypy`. Every name in `log_foundry.__all__` is imported and referenced here, every public
dataclass is constructed by keyword, and the HTTP sinks are called with the keywords SPEC-051
FR-005 says must still check.
"""

from collections.abc import Callable
from typing import Any

from log_foundry import (
    DEFAULT_SHUTDOWN_TIMEOUT,
    DEFAULT_SWAP_TIMEOUT,
    Config,
    ContinueResult,
    FlushResult,
    Health,
    Sink,
    SinkDeliveryError,
    SinkLosses,
    __version__,
    configure,
    continue_trace,
    critical,
    current_baggage_header,
    current_trace_context,
    current_traceparent,
    debug,
    error,
    flush,
    flush_sink,
    get_baggage,
    get_config,
    health,
    info,
    read_losses,
    reset_context,
    set_baggage,
    shutdown,
    trace,
    warning,
)
from log_foundry.sinks.datadog import DatadogSink
from log_foundry.sinks.elasticsearch import ElasticsearchSink
from log_foundry.sinks.honeycomb import HoneycombSink
from log_foundry.sinks.http import (
    HTTPAuthKwargs,
    HTTPForwardKwargs,
    HTTPKwargs,
    HTTPPlatformKwargs,
    HTTPRetryKwargs,
    HTTPSink,
    merge_headers,
)
from log_foundry.sinks.logstash import LogstashSink
from log_foundry.sinks.loki import LokiSink
from log_foundry.sinks.newrelic import NewRelicSink
from log_foundry.sinks.sentry import Backend, SentrySink
from log_foundry.sinks.splunk import SplunkHECSink
from log_foundry.sinks.sqs import DedupIdSource, GroupIdSource, SQSSink

# FR-001: every public dataclass, by keyword. A positional form is planted in `rejects.py`.
_HEALTH = Health(queued=0, dropped=0, failed_batches=0, in_span_lost=0)
_LOSSES = SinkLosses(dropped=0, failed=0)
_FLUSH = FlushResult(ok=True)
_CONTINUE = ContinueResult(ok=False, reason="nothing-supplied")
_CONFIG = Config(service="svc", version="1.0.0", env="prod")

# FR-002: the invariance. A caller's labels are a `dict[str, str]`, and `dict` is invariant, so
# this line is the whole point of the probe -- it was a `mypy --strict` error until SPEC-051.
_LABELS: dict[str, str] = {"tenant": "acme", "region": "eu-west-1"}


class _MySink:
    """A sink a third party would write, satisfying the protocol structurally."""

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Delivers a batch.

        Args:
          batch: The events to deliver.

        Returns:
          None.

        Raises:
          None.
        """

    def close(self) -> None:
        """Releases the transport.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """

    def flush(self) -> None:
        """Empties any client-side buffer.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """

    def losses(self) -> SinkLosses | None:
        """Reports absorbed loss.

        Args:
          None.

        Returns:
          The cumulative losses.

        Raises:
          None.
        """
        return SinkLosses(dropped=0, failed=0)


def _wire() -> None:
    """Exercises the top-level surface the way a consumer's startup module does.

    Args:
      None.

    Returns:
      None.

    Raises:
      None.
    """
    sink: Sink = _MySink()
    configure(service="svc", version="1.0.0", env="prod", sink=sink, defaults=_LABELS)
    set_baggage(request_id="r-1")
    info("started", fields=_LABELS)
    debug("d")
    warning("w")
    error("e")
    critical("c")
    reset_context()
    _: FlushResult = flush(timeout=DEFAULT_SHUTDOWN_TIMEOUT)
    __: ContinueResult = continue_trace(traceparent=current_traceparent())
    shutdown(timeout=DEFAULT_SWAP_TIMEOUT)


@trace(name="work", defaults=_LABELS)
def _work() -> int:
    """A decorated call, with a `dict[str, str]` as its per-decorator defaults.

    Args:
      None.

    Returns:
      A number.

    Raises:
      None.
    """
    return 1


def _sinks() -> list[object] :
    """Constructs the HTTP family with the keywords FR-005 AC-2 says must still check.

    Args:
      None.

    Returns:
      The constructed sinks.

    Raises:
      None.
    """
    forward: HTTPForwardKwargs = {"gzip": True}
    retry: HTTPRetryKwargs = {"timeout": 2.0, "max_retries": 1}
    auth: HTTPAuthKwargs = {"auth": ("user", "password")}
    platform: HTTPPlatformKwargs = {"timeout": 2.0, "auth": "token"}
    everything: HTTPKwargs = {"body_format": "json_array", "max_batch_count": 100}
    return [
        HTTPSink("https://example.invalid/logs", **everything),
        SplunkHECSink("https://example.invalid", "tok", body_format="json_array"),
        LogstashSink("https://example.invalid", **auth),
        # Separately: splatting a shape that MAY carry `headers` beside an explicit
        # `headers=` is a duplicate-argument error, which is the rule working.
        DatadogSink("key", headers=None),
        DatadogSink("key", **platform),
        HoneycombSink("key", "dataset", **forward),
        NewRelicSink("key", opener=None, max_retry_after=1.0),
        LokiSink("https://example.invalid", **forward),
        ElasticsearchSink("https://example.invalid", index="logs", **retry),
        SentrySink("https://public@example.invalid/1", backend=_BACKEND),
        SQSSink("https://sqs.invalid/q", message_group_id=_GROUP, message_deduplication_id=_DEDUP),
    ]


_BACKEND: Backend = "auto"
_GROUP: GroupIdSource = "trace_id"
_DEDUP: DedupIdSource = None

# Every remaining exported name, referenced so nothing is imported and unused. `__all__` is the
# claim; touching each name here is what makes the claim checkable.
_NAMES: tuple[object, ...] = (
    _HEALTH, _LOSSES, _FLUSH, _CONTINUE, _CONFIG, _LABELS, _wire, _work, _sinks,
    __version__, health, get_config, get_baggage, current_baggage_header, current_trace_context,
    read_losses, flush_sink, SinkDeliveryError, merge_headers, _BACKEND, _GROUP, _DEDUP,
)
_READERS: tuple[Callable[..., Any], ...] = (read_losses, flush_sink)
