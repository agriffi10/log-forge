"""The closed guard: a transport's lock, its once-only close and its refusal after (SPEC-076)."""

from __future__ import annotations

import contextlib
import enum
import os
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

__all__ = ["CLOSE_OVERLAP_WAIT", "ClosedGuard", "GuardMode", "Section"]

CLOSE_OVERLAP_WAIT: float = 0.5
"""The longest a ``CLOSE_ONLY`` close waits, idle, for the sections still inside (SPEC-076 FR-003).

For ``KafkaSink`` it is measured headroom: ``produce()`` is a local hand-off, which the sink's own
docstring measures at 0.0001 s for three messages. For ``MongoDBSink`` and the Redis sinks it is a
chosen bound, not a measured one: a round against a reachable server returns in the client's round
trip, well inside it, and a round that outlives it is the call the bound exists to stop waiting
for, whose outcome is still counted. It is a quarter of ``DEFAULT_CLOSER_GRACE``, which leaves room
for the release inside a full grace but promises nothing once less than this remains of a
detached close's budget. Private, and unreachable from ``configure()``.
"""


class GuardMode(enum.Enum):
    """Which calls a guard's lock spans.

    Attributes:
      SERIAL: One lock spans every admitted section and the close, for a transport that cannot
        be shared (SPEC-028).
      CLOSE_ONLY: Sections run concurrently and the lock spans the close only, for a client its
        vendor documents as thread-safe.
    """

    SERIAL = "serial"
    CLOSE_ONLY = "close-only"


class _State(enum.Enum):
    """Where a guard is in its one life: open, then closing, then closed."""

    OPEN = "open"
    CLOSING = "closing"
    CLOSED = "closed"


class Section:
    """One call's pass through :meth:`ClosedGuard.enter`: admitted, or refused holding nothing.

    Attributes:
      admitted: Whether the call may use the transport. ``bool(section)`` reads it.
      released_during: Read after the ``with`` block: the guard reached ``CLOSED`` while this
        section was admitted, so the release ran under it. Always False in ``SERIAL`` mode,
        where the release cannot run while a section holds the lock.
    """

    def __init__(self, admitted: bool, pid: int) -> None:
        """Records the admission and the process it was decided in.

        Args:
          admitted: Whether the section was admitted.
          pid: The process the admission was decided in, so a forked child does not count a
            parent's section out of its own tally.

        Returns:
          None.

        Raises:
          None.
        """
        self.admitted = admitted
        self.released_during = False
        self._pid = pid

    def __bool__(self) -> bool:
        """Whether the section was admitted.

        Args:
          None.

        Returns:
          The admission.

        Raises:
          None.
        """
        return self.admitted


class ClosedGuard:
    """A transport's lock, its once-only close and its refusal after close, in one place.

    A context manager rather than a base class, so each adopter decides what its section covers:
    one engine round, a batch's writes, or an insert whose rows were built before it. The state
    is ``OPEN``, then ``CLOSING`` from the moment a :meth:`close` begins, then ``CLOSED`` from the
    moment its release begins. Every :meth:`enter` outside ``OPEN`` is refused, and a refused
    section holds no lock and registers nothing.

    **The closing rule** is SPEC-061's for ``NATSSink``, generalised. ``threading.Lock`` makes no
    promise of FIFO order, so a close sets ``CLOSING`` before it takes ``_lock``, and a ``SERIAL``
    :meth:`enter` reads the state before taking the lock and again after acquiring it. Once a
    close has begun, the only section it can wait for is the one holding the lock at that moment.

    **The overlap rule.** A ``SERIAL`` close waits for that one section and never releases under
    it. A ``CLOSE_ONLY`` close waits, idle, for at most :data:`CLOSE_OVERLAP_WAIT` for the
    sections inside, then releases whether or not one is still inside, and such a section reads
    ``released_during`` afterwards.

    **Locks.** ``_lock`` and ``_state_lock`` are ``threading.Lock``, never an ``RLock``: a section
    that re-enters its own ``SERIAL`` guard deadlocks, which SPEC-028 chose so a sink re-entering
    its own ``emit`` is not hidden. The order is ``_lock`` then ``_state_lock``, never the reverse;
    the state lock is a leaf, and no counter lock is ever taken under it. There is no
    ``threading.Condition``, which the fork repair does not replace.

    **A forked child** (SPEC-076 FR-006). ``_fork``'s walk replaces both locks and the ``_idle``
    event, since the guard is a package type held at ``self._guard``. Its state is adopted lazily:
    every member first compares the pid recorded at construction with the current one under the
    state lock, and on a mismatch the child counts no section inside, sets ``_idle``, and turns a
    ``CLOSING`` it inherited back to ``OPEN``, since no parent closer runs in the child and one
    that had not set ``CLOSED`` had not begun its release. ``CLOSED`` stays ``CLOSED``.

    **An interrupted closer.** Idempotence is keyed on ``CLOSED``, never on ``CLOSING``: a
    ``KeyboardInterrupt`` while a closer waits for the lock or for ``_idle`` leaves the guard
    ``CLOSING``, refusing every section, and the next close runs the release. One residual is
    accepted: an interrupt landing inside a ``CLOSE_ONLY`` :meth:`enter` between registering the
    section and entering its ``try`` leaks one count, so a later close waits the whole
    :data:`CLOSE_OVERLAP_WAIT` before it releases, which is still bounded.

    **What it must never define** is ``emit``: ``_lifecycle`` records any object carrying both
    ``emit`` and ``close`` as a sink, and would then release the guard as one.
    """

    def __init__(self, mode: GuardMode) -> None:
        """Builds an open guard in one mode.

        Args:
          mode: Which calls the lock spans.

        Returns:
          None.

        Raises:
          None.
        """
        self._mode = mode
        self._lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._idle = threading.Event()
        self._idle.set()
        self._state = _State.OPEN
        self._inside = 0
        self._pid = os.getpid()

    def _adopt(self) -> None:
        """Adopts a forked child's state on the first member call in it, the state lock held.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        pid = os.getpid()
        if pid == self._pid:
            return
        self._pid = pid
        self._inside = 0
        self._idle.set()
        if self._state is _State.CLOSING:
            self._state = _State.OPEN

    def _is_open(self) -> bool:
        """Reads whether the guard is ``OPEN``, under the state lock and after adoption.

        Args:
          None.

        Returns:
          Whether the state is ``OPEN``.

        Raises:
          None.
        """
        with self._state_lock:
            self._adopt()
            return self._state is _State.OPEN

    @property
    def closed(self) -> bool:
        """Whether a close has begun in this process: ``CLOSING`` or ``CLOSED``.

        What ``FileSink.reacquire_after_fork`` reads, and what every adopter's read-only
        ``_closed`` returns. In a forked child it is read after adoption, so a close that was only
        ``CLOSING`` in the parent reads False.

        Args:
          None.

        Returns:
          The answer.

        Raises:
          None.
        """
        with self._state_lock:
            self._adopt()
            return self._state is not _State.OPEN

    @contextlib.contextmanager
    def enter(self) -> Iterator[Section]:
        """Admits one section, or refuses it holding no lock and registering nothing.

        A ``SERIAL`` section is admitted only if the state is ``OPEN`` both before it takes
        ``_lock`` and after acquiring it, and holds the lock for its whole block. A caller refused
        at the second read releases the lock **before** its block runs, so a close queued on the
        lock is never held off by a refusal. A ``CLOSE_ONLY`` section takes no lock across its
        block; it is counted in ``_inside`` under the state lock, ``_idle`` is clear while any is
        inside, and on leaving it reads ``released_during``. The caller answers a refused section
        before doing anything else; what a refusal means is the sink's.

        Args:
          None.

        Returns:
          A context manager yielding the section.

        Raises:
          None. What the caller's block raises propagates unchanged.
        """
        if self._mode is GuardMode.SERIAL:
            if self._is_open():
                with self._lock:
                    if self._is_open():
                        yield Section(True, self._pid)
                        return
            yield Section(False, self._pid)
            return
        with self._state_lock:
            self._adopt()
            admitted = self._state is _State.OPEN
            if admitted:
                self._inside += 1
                self._idle.clear()
            section = Section(admitted, self._pid)
        if not admitted:
            yield section
            return
        try:
            yield section
        finally:
            with self._state_lock:
                self._adopt()
                if section._pid == self._pid:
                    self._inside -= 1
                    if self._inside == 0:
                        self._idle.set()
                section.released_during = self._state is _State.CLOSED

    def close(self, release: Callable[[], None]) -> None:
        """Runs ``release`` once in the guard's life, after the sections it waits for.

        It sets ``CLOSING`` before taking ``_lock``, so every later :meth:`enter` is refused, then
        holds ``_lock`` for the rest of its run: a second close that finds one running waits there
        and returns only after that close's release has returned, never through it. Under the
        lock, a guard already ``CLOSED`` returns at once. A ``CLOSE_ONLY`` close with a section
        inside waits on ``_idle`` for at most :data:`CLOSE_OVERLAP_WAIT`. ``CLOSED`` is set
        **before** ``release`` runs, which is the order ``Sink.close`` states.

        Args:
          release: The sink's release of its transport.

        Returns:
          None.

        Raises:
          BaseException: Whatever ``release`` raises, by identity. The guard stays ``CLOSED`` and
            a later close returns without calling it again.
        """
        with self._state_lock:
            self._adopt()
            if self._state is _State.OPEN:
                self._state = _State.CLOSING
        with self._lock:
            with self._state_lock:
                self._adopt()
                if self._state is _State.CLOSED:
                    return
                self._state = _State.CLOSING
                inside = self._inside > 0
            if self._mode is GuardMode.CLOSE_ONLY and inside:
                self._idle.wait(CLOSE_OVERLAP_WAIT)
            with self._state_lock:
                self._state = _State.CLOSED
            release()
