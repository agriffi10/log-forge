"""Global configuration — set once at startup (arch §7, guide Phase 1)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from log_foundry.sinks.base import Sink


@dataclass
class Config:
    """Process-wide settings stamped onto every event and consulted by the pipeline.

    The four ``max_*`` ceilings bound every event payload (SPEC-017 FR-002), defaulted so
    that the overwhelming majority of events are untouched; they exist to stop one
    pathological value getting a whole event rejected by a sink's hard limit. They bound
    each *value*, not the event as a whole — see arch §13 Known Constraints.

    ``max_value_bytes`` carries two units deliberately (SPEC-020, recorded by SPEC-021): a
    string is measured in UTF-8 bytes, an integer in the decimal length it renders as, sign
    included. An integer is also bounded by ``sys.get_int_max_str_digits()`` whenever that
    is lower, since a longer one cannot be rendered at all.
    """

    service: str = "unknown"
    version: str = "0.0.0"
    env: str = "dev"
    sink: Sink | None = None
    defaults: dict[str, object] = field(default_factory=dict)
    max_value_bytes: int = 8192
    max_stack_bytes: int = 32768
    max_keys: int = 256
    max_depth: int = 8


_config = Config()


def _require_positive(name: str, value: int | None) -> None:
    """Rejects a non-positive ceiling.

    A ceiling of zero would empty every event it touched.

    Args:
      name: The setting's name, used in the error message.
      value: The proposed ceiling, or ``None`` to skip the check.

    Returns:
      None.

    Raises:
      ValueError: If the value is present and less than 1.
    """
    if value is not None and value < 1:
        raise ValueError(f"{name} must be >= 1, got {value}")


def configure(
    *,
    service: str | None = None,
    version: str | None = None,
    env: str | None = None,
    sink: Sink | None = None,
    defaults: dict[str, object] | None = None,
    max_value_bytes: int | None = None,
    max_stack_bytes: int | None = None,
    max_keys: int | None = None,
    max_depth: int | None = None,
) -> None:
    """Patches the global config, and is meant to be called once at startup.

    Only the arguments passed are applied, so repeated calls compose rather than reset. If
    no sink has ever been set, this defaults to :class:`~log_foundry.sinks.stdout.StdoutSink`,
    the zero-dependency dev default (arch §8). Every ceiling is validated before anything is
    assigned, so a rejected call leaves the config exactly as it found it.

    A ``sink=`` passed after logging has already started is the one argument that needs more
    than an assignment, because whatever is delivering captured its sink before the call
    (arch §7). It **swaps the live delivery target**: the previous sink is closed and subsequent
    events go to the new one, so "repeated calls compose rather than reset" holds for the sink
    too, at the cost of one bounded wait. Passing the sink that is already live is a no-op: no
    drain, no close. The previous sink is closed and must not be handed back to a later call —
    doing so closes it twice.

    **What the swap promises differs by delivery path**, because the two have different work to
    do. With a background worker — anything using ``@trace`` — everything submitted so far is
    drained to the previous sink before it is closed, and a drain that could not be confirmed
    leaves that sink open and records ``health().incomplete_swaps`` (SPEC-030 FR-003). A process
    that has only ever logged outside a span has no worker and nothing buffered: those events
    were emitted synchronously and have already returned, so the handoff is the close alone
    (SPEC-033 FR-002). There is no drain to confirm there and ``incomplete_swaps`` stays at zero
    by design; the close is still bounded, and ``health().closing_sinks`` still reports one that
    has not come back.

    The bound covers the whole call, the close included. ``Sink.close`` takes no timeout, so that
    close runs on its own daemon thread and is joined for what is left of the budget: a
    destination that hangs in ``close()`` delays this call by the budget and no more, then carries
    on in the background. An expired join reports nothing, since a slow close and a stuck one
    cannot be told apart at that moment; ``health().closing_sinks`` reports the live fact instead.

    This is still a startup call. It is not thread-safe, and a span finishing on another thread
    during a swap may land on either sink.

    Args:
      service: The service name stamped onto every event.
      version: The service version stamped onto every event.
      env: The deployment environment stamped onto every event.
      sink: The destination every event is delivered to. Passed after the first log, it swaps
        the live target as described above rather than only updating what ``get_config()``
        reports.
      defaults: Fields merged into every event at the lowest precedence.
      max_value_bytes: Per-value ceiling, in UTF-8 bytes or rendered digits.
      max_stack_bytes: Ceiling for ``error.stack`` alone.
      max_keys: Ceiling on the entries of one mapping or sequence.
      max_depth: Ceiling on nesting levels.

    Returns:
      None.

    Raises:
      ValueError: If any ceiling is less than 1.
    """
    _require_positive("max_value_bytes", max_value_bytes)
    _require_positive("max_stack_bytes", max_stack_bytes)
    _require_positive("max_keys", max_keys)
    _require_positive("max_depth", max_depth)

    if service is not None:
        _config.service = service
    if version is not None:
        _config.version = version
    if env is not None:
        _config.env = env
    if sink is not None:
        _config.sink = sink
    if defaults is not None:
        _config.defaults = dict(defaults)
    if max_value_bytes is not None:
        _config.max_value_bytes = max_value_bytes
    if max_stack_bytes is not None:
        _config.max_stack_bytes = max_stack_bytes
    if max_keys is not None:
        _config.max_keys = max_keys
    if max_depth is not None:
        _config.max_depth = max_depth

    _ensure_sink()

    if sink is not None:
        _swap_live_sink(sink)


def _swap_live_sink(sink: Sink) -> None:
    """Points an already-running worker at a newly configured sink (SPEC-030 FR-003).

    The import is local for the reason ``_ensure_sink``'s is: ``decorator`` imports this module
    at module scope, so reaching back the other way at import time would be a cycle. It also
    keeps the dependency to the one call that needs it — ``configure()`` without a ``sink=``
    never touches the worker at all.

    The budget is passed explicitly rather than left to the parameter default, so that the
    bound this call actually applies is resolved when it runs. A default argument is bound at
    definition time, which would leave the end-to-end bound untestable without reaching past
    the function under test.

    Args:
      sink: The sink just written to the config.

    Returns:
      None.

    Raises:
      None.
    """
    from log_foundry.decorator import _swap_sink
    from log_foundry.worker import DEFAULT_SWAP_TIMEOUT

    _swap_sink(sink, DEFAULT_SWAP_TIMEOUT)


def get_config() -> Config:
    """Returns the current global config singleton.

    Args:
      None.

    Returns:
      The process-wide :class:`Config`.

    Raises:
      None.
    """
    return _config


def _ensure_sink() -> Sink:
    """Returns the active sink, applying the ``StdoutSink`` default if none was configured.

    Centralizing the zero-dependency default (arch §8) means ``configure()`` and the flush
    path resolve a sink the same way, so a decorated call never crashes just because the
    user has not called ``configure()`` yet. The local import defers the ``sinks``
    dependency and avoids a top-level import cycle (arch §7).

    Args:
      None.

    Returns:
      The configured sink, or a newly created ``StdoutSink``.

    Raises:
      None.
    """
    if _config.sink is None:
        from log_foundry.sinks.stdout import StdoutSink

        _config.sink = StdoutSink()
    return _config.sink
