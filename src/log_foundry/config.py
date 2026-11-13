"""Global configuration — set once at startup (arch §7, guide Phase 1)."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, cast

from log_foundry import _diag, _lifecycle
from log_foundry._sink_arg import require_sink
from log_foundry.sanitize import _int_text

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from log_foundry.sinks.base import Sink


@dataclass(frozen=True, kw_only=True)
class Config:
    """Process-wide settings stamped onto every event and consulted by the pipeline.

    The four ``max_*`` ceilings bound every event payload (SPEC-017 FR-002), defaulted so
    that the overwhelming majority of events are untouched; they exist to stop one
    pathological value getting a whole event rejected by a sink's hard limit. They bound
    each *value*; each assembly pass is also bounded as a whole, at 131,072 values visited and
    16 MiB of text charged (SPEC-057), which bounds the work on the caller's thread and not the
    event's size at a destination — see arch §13 Known Constraints.

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

    ``sink_factory`` (SPEC-079 FR-005) is where this process's sink comes from when it is built
    per process: with one configured, ``sink`` is the library's private wrapper that builds the
    factory's sink on its first emit, and ``sink_factory`` is the callable itself.
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
    sink_factory: Callable[[], Sink] | None = None


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

_sink_lock = threading.Lock()
"""Serializes the calls to :func:`configure` that change the sink, from the write to the swap.

Two ``configure(sink=...)`` calls at once with a live worker closed one sink twice: measured
in 10 of 50 runs at ``641dfc86``. Both swaps retarget the worker, which ``Worker.retarget``
serializes, so the worker goes A to C and then C to B. B's swap then closes C as the sink it
displaced, and C's swap, deciding afterwards on what its retarget saw, re-arms C, so the exit
closes C a second time with nothing written to it in between. Each swap's two critical sections
are correct for a swap alone; what breaks is a decision taken in the second on facts the other
swap changed after the first. The same gap has a second shape: B's first critical section finds
A, already displaced by C's retarget and no longer held, owed and releases it as stale, and C's
swap then releases A again as the sink it displaced. Held across the config write, the
default-sink step and the swap, this lock makes two concurrent calls the two sequential ones the
swap already handles, and it also ends the second race a concurrent pair had, the config naming
one sink while delivery went to the other, because the call that writes the config last is now
the one that swaps last.

**The wait is bounded by the call's one swap budget, not added to it** (invariant 3). The budget
is taken once, before the wait, and the swap gets what is left. That wait can run out while the
holder is still finishing, even with two callers and nobody misbehaving: each clock starts before
its own wait and ``threading.Lock`` grants no arrival order, so the holder's budget can end after
this one's, and a holder that spends its whole budget overruns it by its last critical section
and the release. Measured by the frame-2 review, two callers delivering to a sink whose ``emit``
takes 0.1 s against a 0.3 s budget fell back in 75 of 150 rounds, none of them defective, and
with a 0.02 s ``emit`` against 1.0 s in none of 60. A call whose wait ran out proceeds without
the lock, its swap with nothing left, so its drain is unconfirmed and counted in
``health().incomplete_swaps``, and it says so on stderr: a call that waited forever
would break the bound, and one that gave up would lose the configure, the worse outcome by the
measure ``configure()``'s ``Raises:`` takes. Running alongside the holder, it is back to the
unserialized pair, so a sink may be closed twice or the config may name one sink while delivery
goes to the other, and the line says both. A budget above ``threading.TIMEOUT_MAX``, ``inf``
included, waits without bound, as ``None`` does, which is ``sinks._retry.wait_budget``'s rule
(SPEC-066): passed to ``acquire`` it would raise ``OverflowError`` out of ``configure()``.

It is outermost. ``stamp`` takes ``_lifecycle._state._owned_lock`` under it, :func:`_rebind`
and :func:`_ensure_sink` take :data:`_config_lock`, and the swap takes ``_lifecycle._state._lock``
and the worker's own lock, all released before it is; nothing that holds any of them takes this.
A ``configure()`` without ``sink`` or ``sink_factory``, or whose ``sink_factory`` is already the
configured one, does not take it, so the orphan path, a call that only changes a stamp and a
call that changes nothing pay nothing. A forked child re-creates it from
``_fork._GLOBAL_PRIMITIVES``, so a parent thread holding it at the fork leaves no child waiting.
"""

_sink_holder: int | None = None
"""The ``threading.get_ident()`` of the thread holding :data:`_sink_lock`, or ``None``.

Read by :func:`_change_sink` so that a sink's own code re-entering ``configure()`` on the thread
already holding the lock tries it once rather than waiting on itself. Written only by the holder,
right after it acquires and right before it releases, and read without a lock: a reader that is
not the holder can only see another thread's ident or ``None``, and either sends it to the wait.
"""


def _require_positive(name: str, value: object) -> int | None:
    """Returns a ceiling as an exact ``int``, or raises (SPEC-065 FR-001, FR-002).

    A ceiling of zero would empty every event it touched, and one that is not an ``int`` does
    integer arithmetic in ``sanitize`` on every event with whatever it is: measured at
    ``71e42dd``, ``max_value_bytes=2.5`` emptied ``fields``, ``nan`` lost the event and ``inf``
    wrote values unbounded, while ``"5"`` raised the comparison's own ``TypeError`` naming
    neither the ceiling nor the type. So the ``isinstance`` test comes first and names both. A
    value outside ``int | None`` is refused rather than floored, because ``math.ceil(2.5)`` would
    hide the caller's mistake behind a ceiling they never chose, and under invariant 10 a float
    was already a ``mypy`` error for every type-checked caller.

    **The test asks ``type``, never the value** (SPEC-071 FR-005): ``issubclass(type(value), T)``
    reads the real type, so a value whose ``__class__`` property lies or raises is judged by what it
    is and nothing of it runs, and the message names the type through its stored name
    (``_diag._STORED_NAME``), which a metaclass ``__name__`` property cannot answer.

    An ``int`` instance — a ``bool``, an ``IntEnum`` member or any subclass — is of the declared
    type and works as the int it holds, so it is accepted; what is stored is
    ``int.__index__(value)``, which reads the integer from the int's own storage and ignores a
    subclass's ``__index__``, ``__int__``, ``__lt__`` and arithmetic. That is the same move
    :func:`_require_text` makes with ``str.__str__``, and it is what closes the zero ceiling
    SPEC-034 shut: a ``class Spoof(int)`` whose ``__lt__`` returned ``False`` passed the old
    ``value < 1`` test holding ``0``. The ``< 1`` test and the message are both built from the
    exact ``int``, so a subclass whose ``__repr__`` raises still yields the ``ValueError``, and
    the message renders it through ``sanitize._int_text``, so a negative too long for CPython's
    digit limit is named ``<int: ~N digits>`` rather than raising that limit's own ``ValueError``
    out of the message, naming neither the setting nor the bound (SPEC-066).

    Args:
      name: The setting's name, used in the error message.
      value: The proposed ceiling, or ``None`` to skip the check.

    Returns:
      The ceiling as an exact ``int``, or ``None`` when it was not supplied.

    Raises:
      TypeError: If the value is present and not an ``int`` instance.
      ValueError: If the value is present and less than 1.
    """
    if value is None:
        return None
    if not issubclass(type(value), int):
        raise TypeError(
            f"{name} must be an int, got {_diag._STR_STR(_diag._STORED_NAME(type(value)))}"
        )
    exact = int.__index__(cast("int", value))
    if exact < 1:
        raise ValueError(f"{name} must be >= 1, got {_int_text(exact)}")
    return exact


def _require_text(name: str, value: object) -> str | None:
    """Rejects a stamp that is not a ``str`` or cannot encode as UTF-8 (SPEC-055 FR-001).

    ``service``, ``version`` and ``env`` are copied from the config into every event without
    passing through ``sanitize`` — the one route by which a string reaches an event unbounded
    and uncoerced, which is deliberate on the hottest path in the library. So the check is at
    the door instead (invariant 13): a lone surrogate here would otherwise cost every batch on
    every sink that binds ``service`` as a column, for the life of the process, and an ``int``
    would land in a ``str`` field. What is stored is ``str.__str__(value)``, an exact ``str``,
    so a ``StrEnum`` member is accepted and a ``str`` subclass instance never reaches an event.

    The test is ``issubclass(type(value), str)``, the same real-type test
    :func:`_require_positive` makes (SPEC-071 FR-005): a value whose ``__class__`` claims ``str``
    used to reach ``str.__str__`` and raise its descriptor error, which named no setting.

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
    if not issubclass(type(value), str):
        raise TypeError(
            f"{name} must be a str, got {_diag._STR_STR(_diag._STORED_NAME(type(value)))}"
        )
    plain = str.__str__(cast("str", value))
    try:
        plain.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError(
            f"{name} must be encodable as UTF-8; it carries a lone surrogate"
        ) from None
    return plain


def _require_factory(value: object) -> None:
    """Refuses a ``sink_factory`` that is not callable or cannot be called with no arguments.

    Checked with the ceilings and stamps, before anything is assigned, so a refused call leaves
    the config and the ownership record as it found them (SPEC-079 FR-005). ``callable`` reads the
    type's call slot, and the message names the type through its stored name, never the value
    (SPEC-071 FR-005's door shape). A signature that cannot bind zero arguments is refused —
    ``sink_factory=FileSink`` needs a path — and one that ``inspect.signature`` cannot read, a
    ``ValueError``, is accepted, since only calling it would tell. Whatever else the signature read
    raises propagates unchanged. ``inspect`` is imported here rather than at module scope, because
    it is costly to import and only this check needs it.

    What the factory returns can only be known by calling it, which ``configure()`` never does:
    ``_FactorySink`` gives that case its defined answer on the first emit.

    Args:
      value: The ``sink_factory`` argument, not ``None``.

    Returns:
      None.

    Raises:
      TypeError: When the value is not callable, or its signature cannot bind zero arguments.
    """
    if not callable(value):
        raise TypeError(
            "sink_factory must be callable with no arguments, "
            f"got {_diag._STR_STR(_diag._STORED_NAME(type(value)))}"
        )
    import inspect

    try:
        signature = inspect.signature(value)
    except ValueError:
        return
    try:
        signature.bind()
    except TypeError:
        raise TypeError(
            "sink_factory must be callable with no arguments, "
            f"got {_diag._STR_STR(_diag._STORED_NAME(type(value)))}"
        ) from None


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
    sink_factory: Callable[[], Sink] | None = None,
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

    **``sink_factory`` and ``sink`` are one setting: where this process's sink comes from**
    (SPEC-079 FR-005). Passing both raises ``ValueError``, because one would be silently ignored.
    A later ``sink=`` clears ``sink_factory`` and a later ``sink_factory=`` replaces the sink; a
    call passing neither keeps both. ``sink_factory=f`` installs a private wrapper sink that calls
    ``f`` on the **first emit in each process** — never here, so a prefork master that never logs
    never builds one — and when logging has started it is swapped in exactly as ``sink=`` is.
    Passing the factory already configured (``is``) does nothing. A forked child is handed a new
    wrapper by the fork handler and builds its own sink on its own first emit; the parent's stays
    the parent's. A factory that raises or returns no sink is not refused here: each emit until
    one builds fails, counted and announced (``docs/invariants.md`` §13). **The factory must build
    a new sink on each call**: one returning an object that existed before the fork hands every
    child that object, which the child then delivers to as an inherited sink, or claims if
    nothing recorded it.

    ``sink=`` is refused when it is a value that can never be a sink — exactly one of the builtin
    types ``_sink_arg._UNPATCHABLE`` lists, such as ``object()``, a ``str`` or a ``dict`` — since
    every batch would otherwise be lost on the drain thread. A sink with no ``close`` is accepted,
    and so is one that has no ``emit`` yet but could gain one; a user class that never does loses
    its batches counted and announced. A ``sink=`` that contains itself — a wrapper sink whose
    wrapped sinks lead back to it — is refused too, since every call into it would recurse.

    This is still a startup call, but two threads passing ``sink`` or ``sink_factory`` at once
    are safe: the calls run one after the other, whichever runs second wins both the config and
    delivery, and every sink involved is closed once. The second call waits for the first, and
    that wait is spent from its own swap budget rather than added to it; a wait that runs out,
    which a sink slow enough to use most of the budget makes common, runs alongside the first
    instead, with a line on stderr saying the two were not serialized. So does a call made by a
    sink's own code from inside a swap, which does not wait at all. A span finishing on another
    thread during a swap may still land on either sink.

    Called from inside a ``sink_factory`` with a ``sink`` or a different ``sink_factory``, an emit
    on another thread that would wait for that factory's build refuses instead, for the length of
    the sink change: the change can wait on a drain thread that is itself waiting for the factory
    (``_lifecycle.yielding``).

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
      max_value_bytes: Per-value ceiling, in UTF-8 bytes or rendered digits. An ``int``; a
        ``bool`` or other ``int`` subclass is stored as the exact ``int`` it holds, and a
        ``float`` is refused rather than floored (SPEC-065).
      max_stack_bytes: Ceiling for ``error.stack`` alone. An ``int``, as ``max_value_bytes``.
      max_keys: Ceiling on the entries of one mapping or sequence. An ``int``, as
        ``max_value_bytes``.
      max_depth: Ceiling on nesting levels. An ``int``, as ``max_value_bytes``.
      sink_factory: Called with no arguments, on the first emit in each process, to build that
        process's sink. Never called by ``configure()``.

    Returns:
      None.

    Raises:
      ValueError: If any ceiling is less than 1, a stamp does not encode as UTF-8, both
        ``sink`` and ``sink_factory`` are given, or ``sink`` holds a ``MultiSink``,
        ``FilteringSink`` or ``TransformSink`` that leads back to itself, so every call into it
        would recurse (``_sink_arg.require_sink``).
      TypeError: If ``service``, ``version`` or ``env`` is not a ``str`` (SPEC-055 FR-001), a
        ceiling is not an ``int`` (SPEC-065 FR-001), ``sink`` is a value that can never be a sink,
        or ``sink_factory`` is not callable with no arguments (SPEC-079 FR-005).
      BaseException: A ``KeyboardInterrupt`` or ``SystemExit`` raised by a sink's own
        ``log_foundry_stop_signal`` setter while the sink change hands it a stop signal. The
        setter is the sink's code, and an ``Exception`` from it is absorbed and announced.
    """
    if sink is not None and sink_factory is not None:
        raise ValueError("configure() takes sink or sink_factory, not both")
    max_value_bytes = _require_positive("max_value_bytes", max_value_bytes)
    max_stack_bytes = _require_positive("max_stack_bytes", max_stack_bytes)
    max_keys = _require_positive("max_keys", max_keys)
    max_depth = _require_positive("max_depth", max_depth)
    service = _require_text("service", service)
    version = _require_text("version", version)
    env = _require_text("env", env)
    if sink is not None:
        require_sink(sink, "sink")
    if sink_factory is not None:
        _require_factory(sink_factory)

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
    if sink is None and (sink_factory is None or sink_factory is _config.sink_factory):
        _rebind(**changed)
        _ensure_sink()
        return
    with _lifecycle.yielding():
        _change_sink(changed, sink, sink_factory)


def _change_sink(
    changed: dict[str, object], sink: Sink | None, sink_factory: Callable[[], Sink] | None
) -> None:
    """Writes a new sink or factory into the config and swaps delivery to it, under one lock.

    The tail of :func:`configure` for a call that passes ``sink`` or a ``sink_factory`` that is not
    the configured one, held under :data:`_sink_lock` from the factory comparison to the end of the
    swap, so two such calls at once run one after the other and the config names the sink delivery
    goes to. The comparison is made again here, under the lock, because a concurrent call may have
    changed the factory since :func:`configure` read it. The swap budget is resolved here, once,
    when the call runs, and the wait for the lock is spent from it; the swap gets what is left. A
    budget of ``None``, or above ``threading.TIMEOUT_MAX``, waits without bound and reaches the swap
    as ``None``: passed on as ``inf``, the swap's own deadline arithmetic raised ``OverflowError``,
    which it absorbed and announced while the worker stayed on the old sink, as at ``641dfc86``.

    **The lock is bound to a local** and released through it. A forked child re-creates the global
    from ``_fork._GLOBAL_PRIMITIVES``, so a thread that forks from inside this call — a sink's stop
    signal setter, say — continues in the child into a ``finally`` that would otherwise release the
    child's fresh, unlocked lock and raise ``RuntimeError`` out of ``configure()`` (measured by the
    frame-2 review; every other roster lock is taken with ``with``, which binds it the same way).

    **A re-entrant call never waits, and swaps with no budget.** That is a call on the thread
    already holding the lock, or on one of the library's own threads, the drain and the two
    closers, named with the ``log-foundry-`` prefix: a sink's own code re-entering ``configure()``,
    from a stop-signal setter, an ``emit`` or a ``close``. The swap in flight is waiting on that
    very thread — its call stack on the holder, its retarget on the drain, its join on the closer —
    so a wait there stalls the outer call by a whole budget past its own, or hangs with an
    unbounded one, which the frame-2 review measured. It tries the lock once without blocking.
    Its swap then gets no budget, so its drain is never confirmed and it releases nothing it
    displaces: the outer swap is suspended between its retarget and its decision, and a re-entrant
    swap that closed the sink it displaced would have that sink re-armed by the outer decision and
    closed twice (measured at ``641dfc86`` for a re-entering setter, 3 of 3). An unconfirmed swap
    leaves the displaced sink to the exit, which closes it once, and is counted in
    ``health().incomplete_swaps``; one ``pending`` line says the call was re-entrant.

    A wait that outlasts the budget proceeds without the lock too, and writes one ``pending`` line,
    since it cannot know whether the call still holding it will close a sink twice or leave the
    config naming another sink than delivery.

    Args:
      changed: The validated fields :func:`configure` is about to write.
      sink: The sink passed, or ``None`` when only a factory was.
      sink_factory: The factory passed, or ``None``.

    Returns:
      None.

    Raises:
      BaseException: A ``KeyboardInterrupt`` or ``SystemExit`` from a sink's own
        ``log_foundry_stop_signal`` setter while the swap hands it a stop signal
        (``_lifecycle._swap_sink``); an ``Exception`` from it is absorbed.
    """
    global _sink_holder
    from log_foundry.worker import DEFAULT_SWAP_TIMEOUT

    budget = DEFAULT_SWAP_TIMEOUT
    started = time.monotonic()
    unbounded = budget is None or budget > threading.TIMEOUT_MAX
    lock = _sink_lock
    me = threading.get_ident()
    reentrant = _sink_holder == me or threading.current_thread().name.startswith("log-foundry-")
    if reentrant:
        serialized = lock.acquire(blocking=False)
        _diag.pending(
            "configure(sink=...) was called by a sink's own code, from inside the library, and "
            "swapped without waiting or confirming its drain; the sink it displaced is left "
            "open for the exit to close"
        )
    else:
        serialized = lock.acquire(timeout=-1 if unbounded else max(0.0, budget))
        if not serialized:
            _diag.pending(
                "configure(sink=...) could not wait for a concurrent sink change within its swap "
                "budget and proceeded alongside it; a sink both calls touched may be closed "
                "twice, and the config may name another sink than the one delivered to"
            )
    if serialized:
        _sink_holder = me
    try:
        built = False
        if sink is not None:
            changed["sink_factory"] = None
        elif sink_factory is not None and sink_factory is not _config.sink_factory:
            from log_foundry.sinks._factory import _FactorySink

            sink = _FactorySink(sink_factory)
            built = True
            changed["sink"] = sink
            changed["sink_factory"] = sink_factory
        if sink is not None:
            _lifecycle.stamp(sink, built=built)

        _rebind(**changed)

        _ensure_sink()

        if sink is not None:
            if reentrant:
                remaining: float | None = 0.0
            elif unbounded:
                remaining = None
            else:
                remaining = max(0.0, budget - (time.monotonic() - started))
            _swap_live_sink(sink, remaining)
    finally:
        if serialized:
            _sink_holder = None
            lock.release()


def _swap_live_sink(sink: Sink, timeout: float | None) -> None:
    """Points an already-running worker at a newly configured sink (SPEC-030 FR-003).

    The import is local for the reason ``_ensure_sink``'s is: ``_lifecycle`` reaches this
    module at call time and this module reaches back, so an import at module scope in either
    direction at import time would be a cycle. It also
    keeps the dependency to the one call that needs it — ``configure()`` without a ``sink=``
    never touches the worker at all.

    The budget is passed explicitly rather than left to the parameter default, so that the
    bound this call actually applies is resolved when it runs. A default argument is bound at
    definition time, which would leave the end-to-end bound untestable without reaching past
    the function under test. :func:`_change_sink` resolves it, and passes what its wait for
    :data:`_sink_lock` left of it.

    Args:
      sink: The sink just written to the config.
      timeout: Seconds left of the call's swap budget, or ``None``.

    Returns:
      None.

    Raises:
      BaseException: A ``KeyboardInterrupt`` or ``SystemExit`` from a sink's own
        ``log_foundry_stop_signal`` setter while the swap hands it a stop signal
        (``_lifecycle._swap_sink``); an ``Exception`` from it is absorbed.
    """
    from log_foundry._lifecycle import _swap_sink

    _swap_sink(sink, timeout)


def get_config() -> Config:
    """Returns the current global config, for reading.

    **Mutating what this returns raises** (SPEC-034 FR-003). It used to hand back the live
    singleton, so assigning to it retargeted what the config *reported* while every event
    continued to the sink the worker had already captured, and assigning a ceiling bypassed the
    validation :func:`configure` performs — ``max_value_bytes = 0`` was accepted and emptied
    every event it touched. Both measured. :func:`configure` is the only route to a change.

    It is a **copy**, not the frozen original, and ``defaults`` is copied with it — **one level
    deep**: the mapping is the caller's own, its values are shared with the library, so a nested
    mutable value edited through the copy still reaches every later event (2026-09-04 audit,
    N8). Deep-copying was declined because a default can be any object and copying an arbitrary
    object can raise or be wrong; a caller who needs isolation for a nested value copies it. A
    caller who
    defeats the freeze — ``object.__setattr__`` reaches through any frozen dataclass — then
    edits an object the library does not read, rather than the live config; and ``defaults`` is
    a plain mutable ``dict``, so sharing it would leave the freeze cosmetic at the one field
    that is not a scalar. ``dataclasses.replace`` alone does **not** do this: it shares the
    dict, which was measured while building this.

    Args:
      None.

    Returns:
      A copy of the process-wide :class:`Config`, with its own top-level ``defaults`` mapping
      whose values are shared.

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
            _lifecycle.stamp(default, built=True)
            _config = replace(_config, sink=default)
        resolved = _config.sink
    if resolved is None:  # pragma: no cover - unreachable; the branch above just set it
        raise RuntimeError("the default sink could not be resolved")
    return resolved
