"""Global configuration — set once at startup (arch §7, guide Phase 1)."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from log_foundry import _lifecycle

if TYPE_CHECKING:
    from collections.abc import Mapping

    from log_foundry.sinks.base import Sink


@dataclass(frozen=True, kw_only=True)
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

    It is **frozen** (SPEC-034 FR-003). :func:`get_config` handed out this object, so assigning
    to it retargeted what the config *reported* while every event continued to the sink the
    worker had already captured — SPEC-030's defect reachable with no underscore in sight — and
    assigning a ceiling bypassed :func:`_require_positive`, so ``max_value_bytes = 0`` was
    accepted and emptied every event it touched. Both measured. :func:`configure` is the only
    supported route to a change, and it rebinds the module global rather than mutating.
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

_config_lock = threading.Lock()
"""Serializes the read-modify-write that replacing a frozen config now is.

Freezing :class:`Config` turned each field assignment into a whole-object
``replace()`` — a read of every field followed by a write of every field — and one of the two
call sites, :func:`_ensure_sink`, runs on the **orphan logging path**, on whatever application
thread called ``info()``. A stale snapshot there puts back the pre-``configure()`` ``service``,
``version``, ``env``, ``defaults`` *and* ``sink``, permanently. Measured on the unlocked version:
268 of 2000 trials shipped every later event with ``service="unknown"`` after one concurrent
``info()``, against 0 before the freeze — a regression, and the SPEC-024 category of wrong data
rather than lost data.

It does **not** cover reads. :func:`_live_config` is one atomic global read and stays lock-free,
so the per-event path pays nothing (SPEC-034 FR-003 AC-6). Lock ordering is one-way and stays
that way: ``_ensure_sink`` is called with ``_lifecycle._state._lock`` held, and ``configure()``
releases this lock before ``_swap_live_sink`` takes that one, so nothing acquires
the lifecycle lock underneath this.
"""


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


def _require_text(name: str, value: object) -> str | None:
    """Rejects a stamp that is not a ``str`` or cannot encode as UTF-8 (SPEC-054 FR-001).

    ``service``, ``version`` and ``env`` are copied from the config into every event without
    passing through ``sanitize`` — the one route by which a string reaches an event unbounded
    and uncoerced, which is deliberate on the hottest path in the library. So the check is at
    the door instead (invariant 13): a lone surrogate here would otherwise cost every batch on
    every sink that binds ``service`` as a column, for the life of the process, and an ``int``
    would land in a ``str`` field. What is stored is ``str.__str__(value)``, an exact ``str``,
    so a ``StrEnum`` member is accepted and a ``str`` subclass instance never reaches an event.

    Args:
      name: The setting's name, used in the error message.
      value: The proposed stamp, or ``None`` to skip the check.

    Returns:
      The stamp as an exact ``str``, or ``None`` when it was not supplied.

    Raises:
      TypeError: If the value is present and not a ``str``.
      ValueError: If the value is present and does not encode as UTF-8.
    """
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str, got {type(value).__name__}")
    plain = str.__str__(value)
    try:
        plain.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError(f"{name} must be encodable as UTF-8; it carries a lone surrogate") from None
    return plain


def configure(
    *,
    service: str | None = None,
    version: str | None = None,
    env: str | None = None,
    sink: Sink | None = None,
    defaults: Mapping[str, object] | None = None,
    max_value_bytes: int | None = None,
    max_stack_bytes: int | None = None,
    max_keys: int | None = None,
    max_depth: int | None = None,
) -> None:
    """Patches the global config, and is meant to be called once at startup.

    Only the arguments passed are applied, so repeated calls compose rather than reset. If
    no sink has ever been set, this defaults to :class:`~log_foundry.sinks.stdout.StdoutSink`,
    the zero-dependency dev default (arch §8). Every ceiling and every stamp is validated before
    anything is assigned — and before the sink is stamped into the ownership record — so a
    rejected call leaves the config and the record exactly as it found them.

    A ``sink=`` passed after logging has already started is the one argument that needs more
    than an assignment, because whatever is delivering captured its sink before the call
    (arch §7). It **swaps the live delivery target**: the previous sink is closed and subsequent
    events go to the new one, so "repeated calls compose rather than reset" holds for the sink
    too, at the cost of one bounded wait. Passing the sink that is already live is a no-op: no
    drain, no close.

    **Every sink this has been given is closed** (SPEC-045), whatever races the call and however
    many are outstanding. Handing the previous sink back to a later call is therefore supported
    rather than forbidden: it is closed again, because it took events again, and what it took
    after the hand-back is flushed by that second close. The record of which sinks are owed a
    close used to be a single slot, so an ordinary ``info()`` on another thread could leave the
    **live** sink closed by nobody and its buffer undelivered.

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

    This is still a startup call, and it is **not thread-safe**. A span finishing on another
    thread during a swap may land on either sink, and two threads calling this at once can leave
    the config naming one sink while delivery goes to another. What is unconditional is that
    every sink involved is closed; where its events went is not.

    Args:
      service: The service name stamped onto every event.
      version: The service version stamped onto every event.
      env: The deployment environment stamped onto every event.
      sink: The destination every event is delivered to. Passed after the first log, it swaps
        the live target as described above rather than only updating what ``get_config()``
        reports.
      defaults: Fields merged into every event at the lowest precedence. Any ``Mapping`` is
        accepted and a copy is stored, so a later edit to the caller's own object does not
        reach the config (SPEC-051 FR-002).
      max_value_bytes: Per-value ceiling, in UTF-8 bytes or rendered digits.
      max_stack_bytes: Ceiling for ``error.stack`` alone.
      max_keys: Ceiling on the entries of one mapping or sequence.
      max_depth: Ceiling on nesting levels.

    Returns:
      None.

    Raises:
      ValueError: If any ceiling is less than 1, or a stamp does not encode as UTF-8.
      TypeError: If ``service``, ``version`` or ``env`` is not a ``str`` (SPEC-054 FR-001).
    """
    _require_positive("max_value_bytes", max_value_bytes)
    _require_positive("max_stack_bytes", max_stack_bytes)
    _require_positive("max_keys", max_keys)
    _require_positive("max_depth", max_depth)
    service = _require_text("service", service)
    version = _require_text("version", version)
    env = _require_text("env", env)

    changed: dict[str, object] = {
        name: value
        for name, value in (
            ("service", service),
            ("version", version),
            ("env", env),
            ("sink", sink),
            ("defaults", None if defaults is None else dict(defaults)),
            ("max_value_bytes", max_value_bytes),
            ("max_stack_bytes", max_stack_bytes),
            ("max_keys", max_keys),
            ("max_depth", max_depth),
        )
        if value is not None
    }
    if sink is not None:
        _lifecycle.stamp(sink)

    _rebind(**changed)

    _ensure_sink()

    if sink is not None:
        _swap_live_sink(sink)


def _swap_live_sink(sink: Sink) -> None:
    """Points an already-running worker at a newly configured sink (SPEC-030 FR-003).

    The import is local for the reason ``_ensure_sink``'s is: ``_lifecycle`` reaches this
    module at call time and this module reaches back, so an import at module scope in either
    direction at import time would be a cycle. It also
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
    from log_foundry._lifecycle import _swap_sink
    from log_foundry.worker import DEFAULT_SWAP_TIMEOUT

    _swap_sink(sink, DEFAULT_SWAP_TIMEOUT)


def get_config() -> Config:
    """Returns the current global config, for reading.

    **Mutating what this returns raises** (SPEC-034 FR-003). It used to hand back the live
    singleton, so assigning to it retargeted what the config *reported* while every event
    continued to the sink the worker had already captured, and assigning a ceiling bypassed the
    validation :func:`configure` performs — ``max_value_bytes = 0`` was accepted and emptied
    every event it touched. Both measured. :func:`configure` is the only route to a change.

    It is a **copy**, not the frozen original, and ``defaults`` is copied with it. A caller who
    defeats the freeze — ``object.__setattr__`` reaches through any frozen dataclass — then
    edits an object the library does not read, rather than the live config; and ``defaults`` is
    a plain mutable ``dict``, so sharing it would leave the freeze cosmetic at the one field
    that is not a scalar. ``dataclasses.replace`` alone does **not** do this: it shares the
    dict, which was measured while building this.

    Args:
      None.

    Returns:
      A copy of the process-wide :class:`Config`, with its own ``defaults``.

    Raises:
      None.
    """
    return replace(_config, defaults=dict(_config.defaults))


def _rebind(**changed: object) -> None:
    """Replaces the module-global config with a copy carrying the changed fields.

    :class:`Config` is frozen (SPEC-034 FR-003), so a change is a new object and a rebinding of
    the global rather than an assignment to a field. It is **one** replacement for the whole
    call, not one per field: nine rebindings would allocate nine configs and, worse, would leave
    a window in which another thread reads a half-applied config — a `service` from the new call
    beside a `sink` from the old one, stamped onto real events.

    Rebinding is safe only because no module imports ``_config`` by value; a
    ``from log_foundry.config import _config`` anywhere would hold the pre-rebind object forever,
    which is why a test asserts the absence rather than a comment claiming it.

    Args:
      **changed: Field names and their new values. Fields not named keep their current value.

    Returns:
      None.

    Raises:
      None.
    """
    global _config
    with _config_lock:
        _config = replace(_config, **changed)  # type: ignore[arg-type]


def _live_config() -> Config:
    """Returns the config object itself, for callers inside the package.

    :func:`get_config` copies, because it is public and a caller must not be able to reach the
    live object through it. ``model.build_event`` reads the config **one to three times per
    event**, so routing that through the public accessor would allocate a ``Config`` and a
    ``defaults`` dict per event. The freeze is a guarantee to the library's *users*, not one the
    library needs against itself, so internal callers read the live object and treat it as
    read-only — the same split :func:`context._live_baggage` makes for baggage.

    Args:
      None.

    Returns:
      The live process-wide :class:`Config`.

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

    It is called on the **orphan logging path**, on arbitrary application threads, so the
    default is resolved under :data:`_config_lock` with a double check and the global is re-read
    inside it (SPEC-034 FR-003). Returning a freshly built local instead handed two racing
    threads two different ``StdoutSink`` objects — measured 996 of 3000 trials — one of which
    then received events and was referenced by nothing, so nothing closed it (SPEC-031 FR-006).
    The unlocked read above it is the fast path: once a sink is configured this is one atomic
    global read and no lock at all.

    Args:
      None.

    Returns:
      The configured sink, or a newly created ``StdoutSink``.

    Raises:
      None.
    """
    global _config
    sink_now = _config.sink
    if sink_now is not None:
        return sink_now

    from log_foundry.sinks.stdout import StdoutSink

    with _config_lock:
        if _config.sink is None:
            default = StdoutSink()
            _lifecycle.stamp(default)
            _config = replace(_config, sink=default)
        resolved = _config.sink
    if resolved is None:  # pragma: no cover - unreachable; the branch above just set it
        raise RuntimeError("the default sink could not be resolved")
    return resolved
