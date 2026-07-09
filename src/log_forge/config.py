"""Global configuration — set once at startup (arch §7, guide Phase 1).

Every log event needs ``service`` / ``version`` / ``env`` stamped on it, and the decorator
and worker both need to find the configured sink. A single module-level singleton is the
simplest thing that lets the rest of the code stay decoupled from *how* it was configured.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Type-only import: keeps ``config`` free of any runtime dependency on ``sinks``
    # (no import cycle) while still typing the sink field. ``from __future__ import
    # annotations`` means this name is never evaluated at runtime.
    from log_forge.sinks.base import Sink


@dataclass
class Config:
    """Process-wide settings stamped onto every event / consulted by the pipeline."""

    service: str = "unknown"
    version: str = "0.0.0"
    env: str = "dev"
    sink: Sink | None = None
    defaults: dict[str, object] = field(default_factory=dict)


_config = Config()  # module-level singleton; the whole library reads through get_config()


def configure(
    *,
    service: str | None = None,
    version: str | None = None,
    env: str | None = None,
    sink: Sink | None = None,
    defaults: dict[str, object] | None = None,
) -> None:
    """Patch the global config. Call once at startup.

    Only the arguments you pass are applied, so repeated calls compose rather than reset.
    If no sink has ever been set, defaults to :class:`~log_forge.sinks.stdout.StdoutSink`
    (the zero-dependency dev default, arch §8) once that phase lands.
    """
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

    _ensure_sink()


def get_config() -> Config:
    """Return the current global config singleton."""
    return _config


def _ensure_sink() -> Sink:
    """Return the active sink, applying the ``StdoutSink`` default if none was configured.

    Centralizes the zero-dependency default (arch §8) so both ``configure()`` and the flush
    path resolve a sink the same way — a decorated call never crashes just because the user
    hasn't called ``configure()`` yet; it emits to stdout. The local import defers the
    ``sinks`` dependency and avoids a top-level import cycle (arch §7).
    """
    if _config.sink is None:
        from log_forge.sinks.stdout import StdoutSink

        _config.sink = StdoutSink()
    return _config.sink
