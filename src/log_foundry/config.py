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
    from log_foundry.sinks.base import Sink


@dataclass
class Config:
    """Process-wide settings stamped onto every event / consulted by the pipeline.

    The four ``max_*`` ceilings bound every event payload (SPEC-017 FR-002). Defaults are set so
    that the overwhelming majority of events are untouched; they exist to stop *one* pathological
    value getting a whole event rejected by a sink's hard limit.

    ``max_value_bytes`` carries **two units**, deliberately (SPEC-020, recorded by SPEC-021): a
    string is measured in UTF-8 bytes, an integer in the decimal length it renders as, sign
    included. They coincide for ASCII digits, and one ceiling covering "how big may a single
    value get" beat a second config key for a distinction almost no one configures. An integer is
    also bounded by ``sys.get_int_max_str_digits()`` whenever that is lower, since a longer one
    cannot be rendered at all.

    They bound each *value*, not the event as a whole — see arch §13 Known Constraints.
    """

    service: str = "unknown"
    version: str = "0.0.0"
    env: str = "dev"
    sink: Sink | None = None
    defaults: dict[str, object] = field(default_factory=dict)
    max_value_bytes: int = 8192  # per value: UTF-8 bytes for a str, rendered digits for an int
    max_stack_bytes: int = 32768  # error.stack only — legitimately long, and worth keeping
    max_keys: int = 256  # per mapping / sequence
    max_depth: int = 8  # nesting levels


_config = Config()  # module-level singleton; the whole library reads through get_config()


def _require_positive(name: str, value: int | None) -> None:
    """Reject a non-positive ceiling. A ceiling of zero would empty every event it touched."""
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
    """Patch the global config. Call once at startup.

    Only the arguments you pass are applied, so repeated calls compose rather than reset.
    If no sink has ever been set, defaults to :class:`~log_foundry.sinks.stdout.StdoutSink`
    (the zero-dependency dev default, arch §8) once that phase lands.

    The four ``max_*`` ceilings bound event payloads (SPEC-017 FR-006); each must be >= 1.
    """
    # Validate every ceiling *before* assigning anything: a rejected call must leave the config
    # exactly as it found it, not half-applied with `service` set and the ceiling rejected.
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
        from log_foundry.sinks.stdout import StdoutSink

        _config.sink = StdoutSink()
    return _config.sink
