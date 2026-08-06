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

    Args:
      service: The service name stamped onto every event.
      version: The service version stamped onto every event.
      env: The deployment environment stamped onto every event.
      sink: The destination every event is delivered to.
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
