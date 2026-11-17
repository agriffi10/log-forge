"""The library's own diagnostic channel — one stderr line for what it lost, absorbed or awaits.

Four rules, all load-bearing (SPEC-029 FR-004, SPEC-067 FR-001):

* The exception's type, never its message. A message can carry a value from the event that
  provoked it, and arch §6 keeps user data out of anything the library emits about itself.
  ``PostgresSink`` is the sharpest case: a psycopg error repr reprints the failing statement
  and its bound parameters, so a naive diagnostic would reprint the event, PII included.
* No stream fault escapes. These writers are called from the guards that exist to stop an
  exception reaching the caller. A ``BaseException`` still passes through, as everywhere else
  in this library: a ``KeyboardInterrupt`` mid-write is the operator's intent.
* Record first, announce second. A counter moves before its line is attempted, at every call
  site, because the write is best-effort and the counter ``health()`` reports is not.
* An inbound value runs no code of its own. ``rejected`` echoes a ``str`` by its first 64
  characters, read through ``str``'s own slot and quoted by ``str.__repr__``, and anything else
  as ``<TypeName>``, read through ``type``'s own descriptor; never a ``repr`` of the value. If
  even that cannot be computed, the line is still written with ``<?>``.

Four writers, one per kind of line (SPEC-068 FR-001): ``absorbed`` for a failure swallowed rather
than propagated, ``lost`` for a counted loss, ``rejected`` for an inbound trace context refused,
and ``pending`` for an outcome not yet decided. The fourth exists because a line written while
the drain thread is still inside the sink cannot know whether what it describes will be
delivered, and a ``lost`` line there announced a loss that another line then announced again.

**Five stderr writes: the four writers and the fold's** (SPEC-103 FR-001). From the first
abandoned exit batch on, the worker opens a fold window around each later exit batch's sink call
(:func:`fold_into`), and inside it ``lost``, ``absorbed`` and ``pending`` record a row keyed on the
text their line would carry, without its count, instead of writing it. :func:`take_folded` writes
one line per key at the end of the exit, or before ``stop()``'s expiry line, each ending in
``line(s) folded at exit``. ``rejected`` never folds. A ``lost`` call that passes
``running_total=True`` is written at once, because summing running totals overstates a loss.
The window is a thread-local, not a ``ContextVar``: a ``ContextVar`` would follow
``asyncio.to_thread`` into a sink's own thread, whose lines must be written at once, and a forked
child's withdrawal would reach only the context current at the fork. ``_FOLD_LOCK`` guards the
rows and **is never waited on by a recorder**: one that cannot take it at once, because a
finalizer the collector ran inside the section re-entered on the same thread or another thread
holds it, writes its line at once, unfolded. It is on ``_fork._GLOBAL_PRIMITIVES``, and
``_fork``'s child handler withdraws the window before anything else, so a child forked inside a
folded call writes its repair's lines at once.

Stderr stays the channel: routing through the ``logging`` module would make the library's own
failure reports depend on machinery that may be the thing failing. This module must import
nothing from its own package — it is reached at module scope while the package is still
partially initialised, and resolves only because it is a leaf.
"""

from __future__ import annotations

import sys
import threading
from typing import TYPE_CHECKING, TypeGuard, cast, overload

if TYPE_CHECKING:
    from typing_extensions import TypeIs

__all__ = [
    "WARN_EVERY",
    "FoldTable",
    "absorbed",
    "errno_of",
    "fold_into",
    "lost",
    "pending",
    "rejected",
    "take_folded",
    "withdraw_fold_in_child",
]

WARN_EVERY = 1000
"""The period of every per-event throttle in the library (SPEC-017 FR-005, SPEC-055 FR-005).

A site that can fail once per event writes its line on the first failure and then on every
``WARN_EVERY``-th, carrying the running total, because a line per failure is its own outage and
silence is how the failure survives to production. One definition, here, because the worker's
queue-full and post-shutdown sites and the console writer's stream-fault site all throttle on
the same period, and two constants stating one number disagree eventually.
"""

_MAX_DETAIL = 200

_MAX_REJECTED_ECHO = 64

_ESCAPES = {"\t": "\\t", "\n": "\\n", "\r": "\\r"}

_STR_STR = str.__str__
"""``str.__str__``, unbound, so a ``str`` subclass name writes its stored text (SPEC-063 FR-004).

This module is a leaf and imports nothing from the package, so it binds the two accessors
``sanitize._type_name`` uses itself rather than importing them. ``_STORED_NAME`` reads the name a
type stores past a metaclass ``__name__`` property, and this returns it as an exact ``str``:
measured at ``71e42dd``, a class whose stored name was a ``str`` subclass with a hijacking
``__format__`` was announced as ``(HIJACK)``, and one whose metaclass ``__name__`` property raised
was not announced at all, the line swallowed by this module's own guard while its loss was counted.
"""
_STORED_NAME = type.__dict__["__name__"].__get__


def type_name(value: object) -> str:
    """Returns the name ``value``'s type stores, as an exact ``str``, without running caller code.

    ``type(value).__name__`` is resolved through the metaclass, so a metaclass whose ``__name__``
    is a raising property made every line that named such a type raise instead, from paths
    documented ``Raises: None`` (invariant 1), and one returning another string renamed the type.
    This reads the stored name through :data:`_STORED_NAME` and copies it with :data:`_STR_STR`,
    so a ``str`` subclass stored as the name can neither raise nor rename it through its own
    ``__str__`` or ``__format__``. It is the spelling for a value's type; a class already in hand
    is read as ``_STR_STR(_STORED_NAME(cls))``, the form older sites also apply to ``type(value)``,
    to the same effect. ``tests/test_stored_name_roster.py`` refuses any read of ``__name__`` or
    ``__qualname__`` through attribute lookup.

    Args:
      value: The value whose type is named, such as a caught exception or a sink.

    Returns:
      The stored name of ``type(value)``.

    Raises:
      None.
    """
    return _STR_STR(_STORED_NAME(type(value)))


_SUBCLASS_CHECK = type.__dict__["__subclasscheck__"]
"""``type.__subclasscheck__``, unbound, so a test of a class reads its MRO and nothing else.

Called as ``_SUBCLASS_CHECK(target, kind)`` it answers whether ``kind`` subclasses ``target`` by
``kind``'s own MRO, past any ``__subclasscheck__`` the target's metaclass defines, so an ABC's
registry, cache and ``__subclasshook__`` are not consulted and no ``__hash__`` or ``__eq__`` of
``kind``'s metaclass runs.
"""


@overload
def is_a[T](value: object, classes: type[T]) -> TypeIs[T]: ...


@overload
def is_a[T](value: object, classes: tuple[type[T], ...]) -> TypeGuard[T]: ...


def is_a(value: object, classes: type | tuple[type, ...]) -> bool:
    """Whether ``value``'s real type subclasses ``classes``, without running any caller code.

    ``isinstance(value, X)`` reads ``value.__class__`` through attribute lookup whenever the real
    type is not ``X`` or a subclass of it, so a ``__class__`` property or ``__getattribute__`` of
    the value ran inside the library: a raising one raised from paths documented ``Raises: None``
    (invariant 1), and one answering ``X`` made an impostor pass. ``issubclass(type(value), X)``
    does not read ``__class__``, but asks ``X``'s metaclass, and an ABC's ``__subclasscheck__``
    hashes ``type(value)`` into its cache, which runs that class's metaclass ``__hash__``. This
    reads ``type(value)``'s MRO through :data:`_SUBCLASS_CHECK`, which is the test an ``except``
    clause makes, so it answers by real type alone: a genuine subclass passes, a transparent proxy
    that only claims ``X`` through ``__class__`` does not, and neither does a class an ABC knows
    only by ``register`` — so ``classes`` must be concrete classes (``dict``, not ``Mapping``), and
    ``tests/test_class_check_roster.py`` refuses an abstract one where it can resolve it. That
    roster also refuses any ``isinstance`` call in ``src/`` outside its exemptions.

    A tuple is walked in a plain loop rather than ``any()`` over a generator, which cost about four
    times as much per call on the SQL sinks' per-value walk. A single class narrows like
    ``isinstance``, both branches (``TypeIs``). A tuple narrows only
    the true branch (``TypeGuard``): ``TypeIs`` over ``tuple[type[T], ...]`` narrowed the false
    branch of ``is_a(err, TRANSIENT_BUILTINS)`` to ``Never``, so ``mypy --strict`` stopped checking
    the rest of five retry predicates, and over ``(list, tuple)`` it dropped ``str`` from the rest.

    Args:
      value: The value to test, such as a caught exception, a sink's result or an event field.
      classes: A class, or a tuple of classes, none of them abstract.

    Returns:
      Whether ``type(value)`` is one of ``classes`` or subclasses one of them.

    Raises:
      None.
    """
    kind = type(value)
    if type(classes) is tuple:
        for target in classes:  # noqa: SIM110
            if _SUBCLASS_CHECK(target, kind):
                return True
        return False
    return bool(_SUBCLASS_CHECK(classes, kind))


_FOLD_TAIL = "line(s) folded at exit"

_CLOSING_TAKE_WAIT = 0.1
"""Seconds ``stop()``'s closing take waits for ``_FOLD_LOCK`` before it leaves the rows (SPEC-103).

The lock is held for a dictionary update, microseconds, so a holder still inside it after this is
stuck, most likely in a finalizer the collector ran inside the section, and waiting longer buys
nothing. ``stop()`` reaches its take only once its join has expired, with the caller's budget
spent, so it must not wait unboundedly before its expiry line (invariant 3). A tenth of a second
is the margin the worker already reserves past a predicted attempt (``worker._JOIN_MARGIN``), so
the overrun it can add is one the budget's own arithmetic tolerates. The rows a timed-out take
leaves are written by the drain thread's own take at the end of the exit, so none is lost.
"""


class FoldTable:
    """The rows folded at one worker's exit: one per distinct line, in first-seen order (SPEC-103).

    One object for the worker's life, cleared in place and never rebound, because an open window
    holds this object and a rebind would drop every row recorded into the old one. ``closed`` is
    set by ``stop()``'s take when its join expires; a record into a closed table is written at
    once, so a sink call the expired join left running announces what it loses before it returns.

    Attributes:
      rows: Each key — the kind, then the rendered parts of the line without its count — mapped
        to ``[summed count, lines folded]``.
      closed: Whether every later record is written at once rather than held.
    """

    def __init__(self) -> None:
        """Starts with no rows, open.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.rows: dict[tuple[str, ...], list[int]] = {}
        self.closed = False

    def reopen(self) -> None:
        """Empties the rows in place and opens the table again, for a forked child's worker.

        Takes no lock: its one caller is ``Worker._reinit_after_fork``, which runs on the child's
        only thread after ``_fork`` has withdrawn the window and replaced ``_FOLD_LOCK``.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.rows.clear()
        self.closed = False


class _Window(threading.local):
    """The fold window of the calling thread: the table its lines are folded into, or none.

    ``table`` defaults to ``None`` on the class, so a thread that never opened a window reads
    ``None`` through a plain attribute read, which calls nothing: a ``getattr`` with a default
    would add a call to every diagnostic the library writes (SPEC-103 FR-001).

    Attributes:
      table: The table this thread's folded lines go into, or ``None`` outside a window.
    """

    table: FoldTable | None = None


_FOLD = _Window()

_FOLD_LOCK = threading.Lock()


def fold_into(table: FoldTable | None) -> None:
    """Opens the calling thread's fold window onto ``table``, or withdraws it with ``None``.

    The worker opens it on the drain thread around an exit batch's sink call once the first
    abandoned exit batch has been counted, and withdraws it in a ``finally`` when the call ends,
    so this module holds no reference to a worker and still imports nothing from the package.

    Args:
      table: The worker's table, or ``None``.

    Returns:
      None.

    Raises:
      None.
    """
    _FOLD.table = table


def withdraw_fold_in_child() -> None:
    """Withdraws the forking thread's fold window in a child, taking no lock (SPEC-103 FR-001).

    ``_fork._reinit_after_fork``'s first step. A sink that forks inside a folded call leaves the
    child's only thread with the window open on the parent's table; the repair's own lines would
    be recorded there and then erased by the worker's reset, never written.

    Args:
      None.

    Returns:
      None.

    Raises:
      None.
    """
    _FOLD.table = None


def take_folded(table: FoldTable, *, close: bool) -> None:
    """Takes every folded row, clearing the table in place, and writes one line per key.

    The fifth stderr write. The rows are copied and cleared under ``_FOLD_LOCK``, then written
    outside it in first-seen order, each line ending in ``; <k> line(s) folded at exit`` after
    the detail is bounded, so the bound never cuts the tail. A ``lost`` line sums the counts, an
    ``absorbed`` line counts the failures, a ``pending`` line names its detail. **Closing never
    waits unboundedly.** With ``close`` the table is marked closed first, by a plain store
    without the lock, so every later record is written at once; the lock is then waited on for
    :data:`_CLOSING_TAKE_WAIT` at most, and on a timeout the rows stay for the drain thread's own
    take at the end of the exit. Without ``close`` — the drain thread's take — the lock is waited
    on normally, and taken even when the table is empty, so a forked child whose inherited lock
    the roster failed to replace blocks visibly rather than skipping the take.

    Args:
      table: The worker's table.
      close: Whether every record after this take is written at once (``stop()``'s take).

    Returns:
      None.

    Raises:
      None on a stream fault; a ``BaseException`` from a write still propagates.
    """
    if close:
        table.closed = True
        if not _FOLD_LOCK.acquire(timeout=_CLOSING_TAKE_WAIT):
            return
    else:
        _FOLD_LOCK.acquire()
    try:
        rows = list(table.rows.items())
        table.rows.clear()
    finally:
        _FOLD_LOCK.release()
    for key, (count, folded) in rows:
        try:
            if key[0] == "lost":
                text = f"lost {count} {key[1]}(s){key[2]}"
            elif key[0] == "absorbed":
                text = f"absorbed {folded} failure(s) while {key[1]} ({key[2]}){key[3]}"
            else:
                text = key[1]
            sys.stderr.write(f"log-foundry: {text}; {folded} {_FOLD_TAIL}\n")
        except Exception:
            pass


def absorbed(where: str, exc: BaseException, detail: str = "") -> None:
    """Reports a failure the library swallowed rather than propagated.

    Only the exception's type is written, never its message: a message can carry a value
    from the event that provoked it, and arch §6 keeps user data out of anything the library
    emits about itself. The type is named by the name it stores, read through
    :data:`_STORED_NAME` and :data:`_STR_STR` rather than ``type(exc).__name__``, so a metaclass
    property can neither silence the line nor rename it (SPEC-063 FR-004). A stream fault is
    swallowed because these writers are called from the ``except`` and ``finally`` blocks that
    exist to stop an exception reaching the caller, though a ``BaseException`` from the write
    still propagates.

    Inside a fold window (SPEC-103) the failure is recorded instead, keyed on the phrase, the
    type's stored name and the rendered detail, and counted once; the check is inline rather than
    a helper, so a line outside a window costs no call the writer did not already make. A record
    that cannot take ``_FOLD_LOCK`` at once, or finds the table closed, writes the line here.

    Args:
      where: What was being attempted, as a participle phrase that reads after "while", such
        as ``"closing a span"``. A literal, never a runtime value.
      exc: The absorbed exception. Only its type's stored name is written.
      detail: Optional consequence for the reader, appended after a semicolon — most usefully
        what was lost, since an absorbed failure is invisible apart from this line. Escaped
        and truncated to ``_MAX_DETAIL``; must not be derived from the exception's text.

    Returns:
      None.

    Raises:
      None on a stream fault. The channel of last resort has no fallback of its own, and
        raising from the guard that protects the caller's exception would be worse (arch §4).
    """
    try:
        table = _FOLD.table
        if table is not None:
            key = (
                "absorbed",
                _STR_STR(_escape(where)),
                _escape(_STR_STR(_STORED_NAME(type(exc)))),
                _suffix(detail),
            )
            if _FOLD_LOCK.acquire(blocking=False):
                try:
                    if not table.closed:
                        row = table.rows.get(key)
                        if row is None:
                            table.rows[key] = [1, 1]
                        else:
                            row[0] += 1
                            row[1] += 1
                        return
                finally:
                    _FOLD_LOCK.release()
        sys.stderr.write(
            f"log-foundry: absorbed a failure while {_escape(where)} "
            f"({_escape(_STR_STR(_STORED_NAME(type(exc))))}){_suffix(detail)}\n"
        )
    except Exception:
        pass


def lost(what: str, count: int, detail: str = "", *, running_total: bool = False) -> None:
    """Reports a counted loss — events, messages, batches or rows that will not be delivered.

    Callers record the counter before calling this, because the write is best-effort by
    construction and the counter ``health()`` reports must not be able to ride on it.

    Inside a fold window (SPEC-103) the loss is recorded instead, keyed on the unit and the
    rendered detail, and its count summed into the row. A running total is never folded: summing
    1 and then 1,000 would read 1,001 against a true 1,000, so ``running_total`` writes at once,
    and ``tests/test_diag.py`` requires it of every ``lost`` call in a function that reads
    :data:`WARN_EVERY`. A count that is not an exact ``int`` is written at once too, so nothing
    but ``int`` arithmetic runs under ``_FOLD_LOCK``. The check is inline, so a line outside a
    window costs no call the writer did not already make.

    Args:
      what: Singular noun for the unit lost, such as ``"event"`` or ``"row"``. Rendered with
        the corpus-wide ``(s)`` suffix, so it must be a literal, never a runtime value.
      count: How many this line reports as lost, normally the increment the caller's counter
        has just taken. A throttled site instead passes its running total, because a line
        written on every thousandth drop saying "lost 1" would read as one loss rather than a
        thousand, and such a site says so in ``detail``.
      detail: Optional circumstances — the sink's class name, the attempt count, an
        ``errno``, an exception type. Escaped and truncated to ``_MAX_DETAIL``.
      running_total: Whether ``count`` is a running total, which a fold window writes at once.

    Returns:
      None.

    Raises:
      None on a stream fault; a ``BaseException`` from the write still propagates.
    """
    try:
        table = _FOLD.table
        if table is not None and not running_total and type(count) is int:
            key = ("lost", _STR_STR(_escape(what)), _suffix(detail))
            if _FOLD_LOCK.acquire(blocking=False):
                try:
                    if not table.closed:
                        row = table.rows.get(key)
                        if row is None:
                            table.rows[key] = [count, 1]
                        else:
                            row[0] += count
                            row[1] += 1
                        return
                finally:
                    _FOLD_LOCK.release()
        sys.stderr.write(f"log-foundry: lost {count} {_escape(what)}(s){_suffix(detail)}\n")
    except Exception:
        pass


def pending(detail: str) -> None:
    """Reports an outcome the library cannot decide yet, without claiming a loss (SPEC-068 FR-001).

    The line is ``log-foundry: <detail>``, with no ``lost N ...`` prefix: what it describes may
    still be delivered, and whichever line settles it later carries the count. That is what keeps
    an event from being announced lost twice — once while its future was open, and again by the
    drain that took it. The detail is escaped and bounded exactly as :func:`lost`'s is, through
    the same helper.

    Inside a fold window (SPEC-103) the line is recorded instead, keyed on the rendered detail
    and counted once, through the same inline check as :func:`lost`.

    Args:
      detail: What is pending and why, built from library-controlled values only. An empty
        detail writes nothing, because a bare prefix says nothing a reader could act on.

    Returns:
      None.

    Raises:
      None on a stream fault; a ``BaseException`` from the write still propagates.
    """
    if not detail:
        return
    try:
        table = _FOLD.table
        if table is not None:
            key = ("pending", _STR_STR(_bounded(detail)))
            if _FOLD_LOCK.acquire(blocking=False):
                try:
                    if not table.closed:
                        row = table.rows.get(key)
                        if row is None:
                            table.rows[key] = [1, 1]
                        else:
                            row[0] += 1
                            row[1] += 1
                        return
                finally:
                    _FOLD_LOCK.release()
        sys.stderr.write(f"log-foundry: {_bounded(detail)}\n")
    except Exception:
        pass


def rejected(reason: str, value: object) -> None:
    """Reports an inbound trace context the library refused to adopt (SPEC-014, SPEC-067).

    The offending value is echoed because its exact shape is what makes a rejection
    diagnosable, and it is echoed without running any code of the value, its type or its
    metaclass (:func:`_echo`). A ``str``, exact or a subclass, is shown as its first
    ``_MAX_REJECTED_ECHO`` characters, quoted, escaped and cut; anything else is shown as
    ``<TypeName>``. The value is only ``str`` by annotation, which a caller not running mypy is
    free to ignore, so the old echo, a ``repr`` of the whole value taken before it was cut,
    could hang the caller's thread on a shared-sublist list or raise from a ``__repr__``. That
    raise was swallowed with the write, so ``continue_trace`` reported a rejection as announced
    while nothing was written.

    The echo is computed inside the guard, and an ``Exception`` from computing it writes the
    line with ``<?>`` instead, so the line is written on every input bar a fault of the stream
    itself. ``"rejected"`` from ``continue_trace`` means this function ran, which is why the
    line must not depend on the value.

    Args:
      reason: Why it was refused, such as ``"unparseable traceparent"``. A literal, never a
        runtime value: unlike the echo, it is escaped but not cut.
      value: The refused value, of any type, echoed in at most ``_MAX_REJECTED_ECHO + 3``
        characters, because unbounded it would be a log-injection surface.

    Returns:
      None.

    Raises:
      None on a stream fault; a ``BaseException`` from computing the echo or from the write
        still propagates.
    """
    try:
        try:
            shown = _echo(value)
        except Exception:
            shown = "<?>"
        sys.stderr.write(
            f"log-foundry: ignoring inbound trace context ({_escape(reason)}): {shown}\n"
        )
    except Exception:
        pass


def _echo(value: object) -> str:
    """Renders a refused value in bounded form without running any of its code.

    It has two callers, which treat a failure differently. :func:`rejected` echoes a refused
    inbound value, catches an ``Exception`` from computing the echo and writes ``<?>`` instead,
    so its line is still written. ``decorator._refuse_unusable`` echoes the ``str`` handed to
    ``@trace`` in place of a callable (SPEC-080 FR-003) and lets that failure propagate, at
    decoration, into the caller's frame, which invariant 13 allows.

    The value's real type decides the route, through ``issubclass(type(value), str)``: an MRO
    check on ``str``'s side, where ``isinstance`` would read the value's own ``__class__`` and a
    property there runs. A ``str``, exact or a subclass, keeps its first ``_MAX_REJECTED_ECHO``
    characters, read with ``str.__getitem__`` so a subclass's ``__getitem__`` and ``__len__`` are
    never consulted and the slice is an exact ``str``. That slice is quoted with
    ``str.__repr__``, so the work is the same whatever the value's length: a ``repr`` of the
    whole value walked all of it before the old echo cut it, and a shared-sublist list never
    returned at all.

    Anything else is named by its type, as ``<TypeName>``, read through ``type``'s own
    ``__name__`` descriptor (:data:`_STORED_NAME`), which a metaclass ``__name__`` property or ``__getattribute__``
    cannot divert. The name goes through the same escape and cut, because a class's
    ``__name__`` may be any length, may contain a newline and may be a ``str`` subclass
    instance. It is sliced one character past the bound, so a printable name longer than the
    bound still escapes to something longer than it and is cut with a ``…``; a slice of exactly
    the bound would escape to exactly the bound and be cut silently. The ``str`` route needs no
    such margin, since its quotes always carry a full slice past the bound.

    Escaping precedes the cut, as in :func:`_suffix`, so the bound applies to what is written.
    The result is at most ``_MAX_REJECTED_ECHO + 3`` characters.

    Args:
      value: The refused value, of any type.

    Returns:
      The bounded, escaped echo: a quoted ``str`` or ``<TypeName>``.

    Raises:
      Exception: Whatever computing the echo raises, such as a ``MemoryError``. :func:`rejected`
        catches it and writes ``<?>`` in its place, so the line is still written;
        ``decorator._refuse_unusable`` lets it propagate at decoration.
    """
    if issubclass(type(value), str):
        text = str.__repr__(str.__getitem__(cast("str", value), slice(None, _MAX_REJECTED_ECHO)))
        opening, closing = "", ""
    else:
        name: str = _STORED_NAME(type(value))
        text = str.__getitem__(name, slice(None, _MAX_REJECTED_ECHO + 1))
        opening, closing = "<", ">"
    shown = _escape(text)
    if len(shown) > _MAX_REJECTED_ECHO:
        shown = shown[:_MAX_REJECTED_ECHO] + "…"
    return f"{opening}{shown}{closing}"


def errno_of(exc: BaseException) -> str:
    """Returns ``"errno=N"`` for an exception carrying one, else an empty string.

    This is the library-controlled detail that makes an ``OSError`` line actionable:
    "connection refused" and "host unknown" share a type name and are told apart only by the
    code, and an integer from the OS is not caller data. ``urllib``'s ``URLError`` carries the
    underlying socket error on ``.reason`` rather than setting its own ``errno``, so that is
    consulted too.

    The value is rendered through ``int()`` rather than interpolated as found, because
    :func:`is_a` admits any ``int`` subclass and a driver's error code is routinely one — an
    ``IntEnum`` or a bespoke class whose ``__str__`` returns whatever its author chose, which
    an f-string would write verbatim. That is the leak this helper exists to be the
    alternative to.

    Args:
      exc: The exception to inspect.

    Returns:
      The rendered ``errno=N``, or an empty string when there is no integer code.

    Raises:
      None. Attribute access on an arbitrary exception can run a property that raises, and a
        helper for a diagnostic must not become one.
    """
    try:
        code = getattr(exc, "errno", None)
        if code is None:
            code = getattr(getattr(exc, "reason", None), "errno", None)
        return f"errno={int(code)}" if is_a(code, int) else ""
    except Exception:
        return ""


def _suffix(detail: str) -> str:
    """Renders a detail as a bounded, escaped ``"; ..."`` suffix.

    The escape and the cut are :func:`_bounded`'s, shared with :func:`pending`.

    Args:
      detail: The caller-supplied detail, possibly empty.

    Returns:
      The suffix, or an empty string when there is no detail or it cannot be rendered.

    Raises:
      None. This runs inside the caller's f-string, so a detail that cannot be rendered would
        otherwise take the whole line with it, including the count an operator cannot
        reconstruct.
    """
    if not detail:
        return ""
    try:
        return f"; {_bounded(detail)}"
    except Exception:
        return ""


def _bounded(detail: str) -> str:
    """Escapes a detail and cuts it to ``_MAX_DETAIL``, marking a cut with ``…``.

    Escaping precedes the cut so the bound applies to what is written, and so the cut can only
    remove characters from an already-safe string rather than split an escape into something
    that is not one. One helper for :func:`_suffix` and :func:`pending`, so the two writers that
    carry a free-form detail cannot drift apart on the rule.

    Args:
      detail: The caller-supplied detail.

    Returns:
      The escaped detail, at most ``_MAX_DETAIL`` characters plus the cut mark.

    Raises:
      Exception: Whatever escaping a detail that is not a ``str`` raises; both callers guard it.
    """
    shown = _escape(detail)
    if len(shown) > _MAX_DETAIL:
        shown = shown[:_MAX_DETAIL] + "…"
    return shown


def _escape(text: str) -> str:
    """Renders every non-printable character visibly, so nothing in the text can forge a line.

    ``str.isprintable()`` is the test rather than a table over ``range(0x20)``: it is
    ``False`` for the C0 block and for DEL, the C1 block, U+0085, U+2028, U+2029, and the bidi
    format characters. Python's own ``splitlines()`` breaks on three of those a C0 table
    misses, so a reader doing the obvious thing would see a forged ``log-foundry:`` line that
    a newline count says is not there. The whole-string fast path keeps the common case to a
    single C-level call, and space is printable, so ordinary text is untouched.

    Args:
      text: The string to render safely.

    Returns:
      The text with every non-printable character escaped.

    Raises:
      None.
    """
    if text.isprintable():
        return text
    return "".join(char if char.isprintable() else _escaped(char) for char in text)


def _escaped(char: str) -> str:
    """Escapes one non-printable character, following Python's own ``repr`` conventions.

    Args:
      char: The single character to escape.

    Returns:
      The escape sequence for that character.

    Raises:
      None.
    """
    escape = _ESCAPES.get(char)
    if escape is not None:
        return escape
    code = ord(char)
    if code <= 0xFF:
        return f"\\x{code:02x}"
    return f"\\u{code:04x}" if code <= 0xFFFF else f"\\U{code:08x}"
