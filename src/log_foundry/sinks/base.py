"""The sink interface (arch §8)."""

from __future__ import annotations

import contextlib
import types
from abc import abstractmethod
from dataclasses import dataclass
from typing import Final, Protocol, cast, runtime_checkable

__all__ = ["Sink", "SinkDeliveryError", "SinkLosses", "flush_sink", "read_losses"]


class SinkDeliveryError(Exception):
    """Raised by a sink whose ``emit`` delivered none of the batch (SPEC-026 FR-001).

    It is a distinct type so an operator reading a ``stopped_reason`` or a diagnostic line can
    tell "the destination refused everything" from "the sink itself has a bug". Sinks that
    already have a natural exception to re-raise — a driver error, a ``MultiSink`` child's —
    re-raise that instead: the contract is that something propagates, not that it must be this
    type.

    A sink may also raise a **subclass that is the driver's type as well** (SPEC-069 FR-004):
    ``SQLiteSink`` and the file sinks raise their ``sqlite3.Error`` or ``OSError`` as an instance
    of both, so ``isinstance(exc, SinkDeliveryError)`` and the driver's own ``except`` clause both
    match. That subclass is built at run time and has no public name; ask with ``isinstance``.
    """


@dataclass(frozen=True, kw_only=True)
class SinkLosses:
    """What a sink discarded or could not confirm, cumulative for its lifetime (FR-002).

    ``failed`` is an upper bound on loss, not a count of it: a sink that also raises on total
    failure counts the attempt here and hands the batch back to the worker, whose retry may
    then deliver it, so a transient outage leaves ``failed`` non-zero with nothing actually
    lost. ``health().failed_batches`` is the worker-level record of a batch given up on for
    good; this is the sink-level record of everything that did not go through first time.

    Construction is **keyword-only** (SPEC-051 FR-001), which is a break for a sink written
    against ``0.x``: passing the two counters positionally now raises, and because
    :func:`read_losses`
    swallows a raising accessor by design, that sink's loss reporting degrades to ``None`` —
    "reports nothing" — rather than to an error anyone sees. One keyword each is the fix. The
    same decision empties ``__match_args__``, so a positional ``case SinkLosses(d, f):`` raises
    ``TypeError`` from the ``match`` statement itself **against a** ``SinkLosses`` **subject**,
    while ``case SinkLosses(dropped=d, failed=f):`` still matches. Against any other subject —
    ``health().sink`` is ``None`` whenever no sink is configured — it falls through as before.
    The three behaviours are pinned by
    ``test_an_empty_match_args_raises_for_its_own_type_and_falls_through_for_others``, which is
    where to check this rather than here.

    Attributes:
      dropped: An event the sink discarded before attempting delivery, usually one the
        destination could never have accepted as built, so the fix is upstream in what the
        application logs. For a sink whose client owns a local buffer it also covers what that
        buffer refused, which is backpressure instead; the stderr line names which.
      failed: Delivery was attempted and the destination did not confirm it, so the fix is the
        destination or the network.
    """

    dropped: int
    failed: int


@runtime_checkable
class Sink(Protocol):
    """The swappable output transport (arch §8).

    A sink receives already-built, batched event dicts from the worker and knows nothing about
    spans or context — that dumbness is what makes sinks trivially interchangeable. Beyond
    "put these somewhere" it carries the two obligations :meth:`emit` documents, because the
    library's whole loss-reporting apparatus is built on them: a sink that absorbs a total
    failure and returns normally is a sink the worker believes, so the retry never engages,
    ``failed_batches`` stays at zero and ``flush()`` returns True while every event is lost.

    A third obligation is optional and deliberately not declared on this Protocol, which is
    structural: a sink may also offer ``losses() -> SinkLosses | None``, reporting the
    cumulative loss it absorbed, never raising and safe to call during an emit. Returning
    ``None`` is the same answer as having no method at all, which is what keeps a third-party
    sink written against the pre-SPEC-026 interface satisfying this one. :func:`read_losses` is
    the probe.

    :meth:`emit` and :meth:`close` are ``@abstractmethod`` (SPEC-034 FR-005). A ``Protocol`` is
    normally satisfied *structurally* and that is still how every shipped sink satisfies this
    one — but making it a public export invites inheritance, and an incomplete subclass of a
    protocol whose members have empty bodies instantiates happily and returns ``None`` from the
    method it failed to define. Reproduced before the decorators were added: one typo
    (``def emmit``) and three events were gone with ``flush()`` reporting ``True`` and every
    counter at zero — the "sink the worker believes" failure this file exists to prevent.
    ``mypy`` already refused it; only the runtime did not. Inheriting is still not required, and
    ``isinstance`` remains structural.

    A fourth is optional in the same way and is an attribute rather than a method:
    ``log_foundry_stop_signal``. A sink that defines it — a plain
    ``threading.Event | None``, initialised to ``None`` — is handed the worker's shutdown event
    whenever the library takes ownership of it, and a sink that does not is simply never
    offered one. **Honouring it is how a retrying sink stays interruptible** (SPEC-027): the
    worker owns a single drain thread, so a sink's backoff is a global pause on log delivery
    held across ``shutdown()``, which joins that thread. Pass it to ``sinks/_retry.wait`` — or
    wait on it directly — rather than calling ``time.sleep``, or a shutdown cannot cut the
    wait short and a slow destination holds process exit for the length of its own backoff.
    A **wrapper** sink must forward it to whatever actually holds the retry loop; set on a
    wrapper and stopped there, the signal reaches nothing.

    The name is namespaced deliberately (SPEC-034 FR-006). The library assigns this attribute
    onto an object it does not own, by ``hasattr`` probe, so a bare ``stop_signal`` — which is
    what shipped before 1.0 — silently overwrote any attribute of that name a third-party sink
    already used, with a ``threading.Event``. Reproduced. A prefixed name cannot collide by
    accident, and the cost is one verbose attribute on the sinks that want interruptibility.

    A fifth is optional in the same way and exists for one event only:
    ``reacquire_after_fork() -> None``. A sink that owns a **buffered** stream — one it
    opened itself, rather than the process's ``sys.stdout`` — inherits the parent's unflushed
    bytes in a forked child, and both processes then write them (SPEC-039 FR-004). It is
    *called* rather than assigned, which is why it carries no ``log_foundry_`` prefix: the
    collision the fourth member's name guards against is an attribute the library writes onto an
    object it does not own.

    **The contract has two halves and the name says the larger one** (SPEC-042 FR-005). Strand
    what you inherited — the parent's pending bytes are not yours to write — *and*, by returning
    normally, **claim the transport as this process's own**. ``FileSink`` satisfies both in one
    step, because reopening the path is what strands the buffer: measured, parent and child then
    hold different descriptors. That second half is what the library acts on, so a sink that
    merely dropped a buffer without re-acquiring would satisfy a name describing only the
    discard — which this member carried until SPEC-042 — while making a destructive close look
    safe. A sink that does **not** implement this keeps whatever it inherited, so a child will
    not release it; one that raises is treated the same way, since a failed re-acquisition is
    not a claim.

    **Inheriting the hook claims the whole object, so a subclass that adds a transport must
    override it.** ``class MySink(FileSink)`` that also holds a socket inherits a hook which
    re-acquires only the *file*, returns normally, and thereby tells the library the child owns
    everything — after which the child's ``close()`` runs against the parent's connection.
    Measured. Returning normally is the claim; the library cannot check what was actually
    re-acquired, which is why the obligation sits here. A subclass that cannot honour it should
    define the member to raise, which is refused and therefore safe.

    **Which sinks are actually asked is narrower than "whichever define it", and the boundary is
    the same one every fork repair has.** The child's repair walks this package's own objects —
    a sink whose class is defined here or **inherits** one that is, ``Sink`` included, and the
    **owned** sinks such an object holds, ``MultiSink``'s children among them. A third-party
    sink satisfying this Protocol *structurally*, which is how every shipped sink satisfies it,
    is outside that walk exactly as its locks are (SPEC-039 FR-005): its hook is never called,
    and a fork mid-batch there can still duplicate — including when a wrapper this library owns
    is holding it. Inheriting from ``Sink`` or from a shipped sink is what brings it inside. A
    **wrapper** sink the walk enters need do nothing itself, because the owned sinks it holds
    are reached directly rather than through it.

    **It runs in a child that has not yet returned from ``fork``, on that child's only thread,
    and must not block.** Nothing else is running there, so there is no holder a lock could be
    waiting for — and no watchdog to end a wait taken anyway, since a pending alarm does not
    survive the fork and this runs before any application code can arm one. It may raise: the
    library absorbs and announces that, and the cost is a child that may write the parent's
    pending bytes a second time.

    "Safe to call during an emit" is a concurrency requirement once :meth:`emit` is (SPEC-028
    FR-003). The shipped sinks keep their loss counters under a **dedicated** lock, separate
    from whatever guards their transport: an increment is a read-modify-write that Python does
    not promise is atomic, and the pair read must come from one instant rather than straddling
    another thread's increments. It is deliberately not the transport lock — ``health()`` is
    the call an operator makes when a destination is already hanging, and sharing one lock
    would make that poll wait for an in-flight emit and its retry backoff. Where a sink holds
    both, the order is always transport then counter, never the reverse.
    A sixth is optional in the same way: ``flush() -> None``, which drains whatever the sink is
    holding in its **client** without closing it (SPEC-036 FR-002). A sink that buffers in a
    driver rather than writing through — ``KafkaSink`` hands to librdkafka, ``GooglePubSubSink``
    appends an unresolved future — is unreachable through ``log_foundry.flush()`` without it:
    measured against a stand-in with that shape, ``flush() -> True``, on the wire 0, in the
    client buffer 3, ``health()`` all zeros. It is called **after** the queue drain, so the
    queue's events have reached the client buffer before it is emptied.

    It is **not a close**, and the difference is the whole point: the sink keeps its transport and
    goes on accepting events afterwards. Like :meth:`emit` it must tolerate being called
    concurrently with an emit (SPEC-028), and like :meth:`emit` it must **raise** when it could
    not deliver what it was holding — that is the only channel by which ``log_foundry.flush()``
    can report ``reason="sink-flush"`` instead of success. :func:`flush_sink` is the probe, and it
    deliberately does **not** behave like :func:`read_losses`: that one swallows a raising
    accessor because a broken reporter must not take ``health()`` down, while this one propagates,
    because a swallowed flush failure is exactly the "sink the worker believes" this file exists
    to prevent.

    A seventh is optional in the same way and changes how a batch is delivered rather than what
    is reported: ``deliver(batch) -> Verdict`` (SPEC-072), which returns one ``Outcome`` per
    event, by index, instead of raising for the whole batch. ``read_deliver`` is the probe, and it
    reads the sink's **type** statically: ``deliver`` must be a plain function defined on the
    class, no class earlier in the MRO than the one defining it may define ``emit``, and no
    ``emit`` may be set on the instance (so a subclass that overrides ``emit`` to stamp or scrub
    keeps running its override, and so does ``sink.emit = scrub``); some class must define
    ``emit``, and none may define ``__getattribute__`` as a plain function, which could hand the
    worker another ``emit``; and the sink
    must also carry ``log_foundry_counters``, exactly a ``SinkCounters``, which the engine is
    the one writer of; ``losses()`` then returns ``log_foundry_counters.losses()``. A class
    attribute ``deliver = None`` declares an opt-out. When ``deliver`` is offered, the worker and
    the orphan path call it through the delivery engine instead of calling ``emit``: the engine
    resends only the events marked ``RETRYABLE``, within the 1.x total of attempts
    (``max_retries`` + 1 times the worker's ``max_retries`` + 1, the sink's own ``max_retries``
    read as an exact ``int``), waits the 1.x backoff on the stop signal, honours a verdict's
    ``retry_after`` under the sink's ``max_retry_after`` (30 seconds when it declares none), and
    counts what did not land into the counters at the end of the run. Two more optional
    attributes refine it: ``log_foundry_idempotent = True`` (exactly ``True``) lets an
    ``UNKNOWN`` outcome, an unreadable verdict or an exception escaping ``deliver`` be resent,
    the last only when the sink's ``_retryable_error`` accepts it. Such a sink's ``emit`` should
    be ``emit_via_deliver(self, batch)`` — public at ``log_foundry`` and
    ``log_foundry.sinks.verdict`` — which runs the same engine inline and raises when the
    run fails, so a wrapper calling ``inner.emit(batch)`` sees what a 1.x ``emit`` showed it.
    **A ``deliver`` must not call its own ``emit``**, and must not mutate the list it is given.

    **A direct ``deliver`` call outside any engine run is a single round with no memory of
    earlier calls** (SPEC-074 handover item 1a, SPEC-075). It is a detached run of its own: the
    run context it counts into is new and ends with the call, so what an earlier call sent or
    held is forgotten, and the round is its own last for every count and line it registers. A
    caller that loops it therefore sees a sink's held line or giving-up line on every call — an
    ``SQSSink`` writes "still failing after 1 attempt(s)" each time — and an ``SQSSink`` FIFO
    hold is not released at the end, because only a run's budgeted last round sends what is
    held, and a detached round is never one. A caller that wants retries should use
    ``emit_via_deliver``, which runs the engine over the sink's own budget, keeps the counts
    across its rounds and sends held events in the run's last round.
    """

    @abstractmethod
    def emit(self, batch: list[dict[str, object]]) -> None:
        """Ships a batch of serialized event dicts.

        **May be called concurrently from more than one thread, and must tolerate it**
        (SPEC-028 FR-001). The background worker drains on its own thread, and a level call
        made with no active span emits synchronously on the *caller's* thread (arch §12), which
        may be any of the application's — an audit observed one sink object entered by two
        application threads and the worker at once. An implementation holding mutable transport
        state, such as a stream it rebinds, a reused socket or a connection with transaction
        scope, must serialize access to it for the whole span of the operation that assumes
        exclusivity. One that holds no such state need do nothing.

        This is a requirement on implementations, not a promise the library serializes on their
        behalf. It cannot: the orphan path runs on a thread the library does not own.

        Raise when the batch delivered nothing and it was non-empty — the worker's bounded
        retry and ``health().failed_batches`` depend on that signal, and a retry there cannot
        duplicate anything (SPEC-026 FR-001). Raise after the sink's own retries are spent, so
        the worker's retry composes on top rather than replacing it; a sink whose own budget
        makes the worker's redundant should say so in its docstring rather than absorb
        silently.

        Do not raise on partial failure. A batch where some records landed would be
        re-delivered wholesale by the worker's retry, and duplicates downstream are worse than
        the counted loss (SPEC-017 FR-004, SPEC-018). Report that through ``losses()`` instead.

        **After ``close()`` has released or invalidated something, raise rather than absorb**
        (SPEC-032 FR-002). This is the same obligation applied to the sink's own lifecycle
        rather than the destination's: an absorbed batch is one the worker believes, so the
        retry never engages, ``failed_batches`` stays at zero and ``flush()`` returns True while
        the events are gone — reached not through a destination that is down but through a
        transport the library itself has already let go. Three shipped sinks failed this rule
        for four specs, in three different ways: a produce into a batch nothing would flush
        again, a future nothing would resolve, and a client that quietly reconnected.

        The converse is equally binding: a sink holding nothing to release **keeps accepting**.
        ``close()`` on a sink that opens a fresh connection per request, or whose client belongs
        to the caller, has invalidated nothing, so a later batch still delivers and refusing it
        would be loss the library invented rather than loss it reported. Which of the two
        applies is a property of the sink, so each shipped sink records its answer in its class
        docstring and a test holds it to it.

        Args:
          batch: The events to ship. **Borrowed, not given** — the list and the dicts inside it
            may be handed to other sinks afterwards, which ``MultiSink`` does to every child in
            turn, so mutating either in place silently changes or empties what a later sink
            receives. Copy before reshaping or redacting. Reproduced: a child that cleared the
            list left the next child with nothing and no error anywhere. ``emit([])`` is a no-op and never raises, since an empty
            batch has not failed to deliver — closed or not.

        Returns:
          None.

        Raises:
          Exception: When the batch delivered nothing.
        """
        ...

    @abstractmethod
    def close(self) -> None:
        """Flushes and releases any resources.

        **May be called while an ``emit`` is in flight on another thread** (SPEC-028 FR-001).
        It must therefore either wait for that emit or become a no-op, and must never release a
        resource a concurrent ``emit`` is about to use — a half-released transport is worse than
        an unreleased one, because the emit then fails against a closed handle instead of
        succeeding. Taking the same lock ``emit`` takes satisfies this; so does an idempotent
        guard checked under that lock.

        The flag this sets is the one a later :meth:`emit` reads, so the two are one decision
        rather than two (SPEC-032 FR-002). Set it *before* releasing anything: an emit arriving
        during a release must be refused rather than handed a half-released transport. Make the
        release idempotent for the same reason a second ``close()`` is expected at all —
        ``atexit`` racing a caller's own cleanup is the documented case.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever releasing the transport raises; the worker absorbs it.
        """
        ...


_ABSENT: Final = object()
"""What :func:`_static_value` answers for an attribute that is missing or cannot be read."""

_MRO = type.__dict__["__mro__"].__get__
"""``type.__mro__``, unbound, so a metaclass ``__mro__`` property is never consulted."""

_NAMESPACE = type.__dict__["__dict__"].__get__
"""``type.__dict__``, unbound, so a class's own namespace is read without any hook running."""

_GETSET_GET = types.GetSetDescriptorType.__get__
"""The builtin ``__get__`` of a getset descriptor: the instance ``__dict__`` slot, read in C."""

_MEMBER_GET = types.MemberDescriptorType.__get__
"""The builtin ``__get__`` of a ``__slots__`` member descriptor, read in C."""


def _class_member(cls: type, name: str, *, skip_none: bool = False) -> tuple[object, int] | None:
    """Finds ``name`` in a class's own MRO namespaces, returning the value and its MRO index.

    The MRO and each namespace are read through the unbound ``type`` accessors, so no metaclass
    hook runs — no ``__mro__`` or ``__dict__`` property, and no metaclass ``__eq__`` or
    ``__hash__``, since nothing here hashes or compares a class. One residue is accepted rather
    than closed, and ordinary ``getattr`` shares it: the name is looked up in each namespace's
    dict (and the instance dict, in :func:`_own_attribute`), so any key there whose hash collides
    with the name has its ``__eq__`` run — a ``str`` subclass with its own ``__eq__``, or any
    object whose ``__hash__`` returns ``hash(name)``, planted with ``setattr`` or
    ``__dict__[key] = value``. With ``skip_none`` a ``None`` found in a
    namespace is passed over and the walk goes on, which is how SPEC-072's adapter reads a
    ``deliver = None`` opt-out: "do not offer", not "do not have".

    Args:
      cls: The class to search.
      name: The member's name, an exact ``str``.
      skip_none: Whether a ``None`` value is passed over rather than returned.

    Returns:
      ``(value, index)`` for the first class in the MRO whose namespace holds ``name``, or
      ``None``.

    Raises:
      Exception: Whatever an unusual class object raises while its MRO is read; callers guard.
    """
    for index, klass in enumerate(_MRO(cls)):
        namespace = _NAMESPACE(klass)
        if name in namespace:
            value = namespace[name]
            if value is None and skip_none:
                continue
            return value, index
    return None


def _instance_dict(obj: object) -> dict[str, object] | None:
    """Returns an object's own ``__dict__`` through a builtin getset descriptor, or ``None``.

    The descriptor is the first exact ``GetSetDescriptorType`` named ``__dict__`` along the MRO
    that applies to the object, called through its builtin ``__get__``, so a ``__dict__``
    property or any other hook never runs. One that does not apply — a getset borrowed from an
    unrelated class — is passed over. ``None`` means the dict cannot be read this way: there is
    none (a slotted or builtin object), or the class shadows ``__dict__`` where it defines it, so
    no builtin descriptor exists for it. The caller treats that as "no instance attribute".

    Args:
      obj: The object.

    Returns:
      The instance dict, or ``None``.

    Raises:
      Exception: Whatever an unusual class object raises while its MRO is read; callers guard.
    """
    cls = type(obj)
    for klass in _MRO(cls):
        holder = _NAMESPACE(klass).get("__dict__")
        if type(holder) is not types.GetSetDescriptorType:
            continue
        with contextlib.suppress(Exception):
            own = _GETSET_GET(holder, obj, cls)
            return cast("dict[str, object]", own) if issubclass(type(own), dict) else None
    return None


_GENERIC_GETATTR = object.__getattribute__
"""CPython's generic attribute lookup, unbound: no ``__getattribute__`` or ``__getattr__`` runs.

Called only where it cannot run a descriptor: when the class holds nothing for the name, or a
plain value whose type defines no ``__get__``, or a plain function, whose ``__get__`` is C. It
reads the real instance dict whatever the class does to ``__dict__`` — a property, a borrowed
getset — which no Python-level accessor can.
"""

_METHOD_FUNC = types.MethodType.__dict__["__func__"].__get__
"""A bound method's ``__func__``, read through its builtin member."""

_METHOD_SELF = types.MethodType.__dict__["__self__"].__get__
"""A bound method's ``__self__``, read through its builtin member."""


def _own_attribute(obj: object, name: str, member: tuple[object, int] | None) -> object:
    """Returns the value ``obj``'s own instance dict holds for ``name``, or :data:`_ABSENT`.

    ``member`` is the class's (from :func:`_class_member`); the caller has already ruled out a
    data descriptor. Three routes, chosen so that none runs a hook:

    - nothing on the class, or a value whose type defines no ``__get__``: CPython's generic
      lookup, which answers from the instance dict when it holds the name, else with the class
      value (read here as "not the instance's own") or ``AttributeError``;
    - a plain function: the generic lookup binds it in C when the instance does not shadow it, so
      a bound method of that very function on this very object means "not the instance's own";
    - any other non-data descriptor, whose ``__get__`` the generic lookup would call: the
      instance dict read through :func:`_instance_dict`. **The one limit:** when the class
      shadows ``__dict__`` where it defines it, an instance value shadowing such a descriptor is
      not seen.

    Args:
      obj: The object.
      name: The attribute's name, an exact ``str``.
      member: ``_class_member(type(obj), name)``.

    Returns:
      The instance's own value, or :data:`_ABSENT`.

    Raises:
      Exception: Whatever an unusual object raises; callers guard.
    """
    held = None if member is None else member[0]
    if member is None or _class_member(type(held), "__get__") is None:
        try:
            value = _GENERIC_GETATTR(obj, name)
        except AttributeError:
            return _ABSENT
        return _ABSENT if member is not None and value is held else value
    if type(held) is types.FunctionType:
        value = _GENERIC_GETATTR(obj, name)
        if type(value) is types.MethodType and _METHOD_FUNC(value) is held:
            return _ABSENT if _METHOD_SELF(value) is obj else value
        return value
    own = _instance_dict(obj)
    return _ABSENT if own is None else dict.get(own, name, _ABSENT)


def _static_value(obj: object, name: str) -> object:
    """Reads one attribute the way ordinary access finds it, running none of the object's code.

    The order is CPython's own for an instance: a **data descriptor** on the class (one whose
    type defines ``__get__`` and also ``__set__`` or ``__delete__``) wins, then the instance's own
    value, then any other class attribute. A value whose type defines ``__set__`` or
    ``__delete__`` without ``__get__`` is not one for reading — CPython reads the instance dict
    past it — so a "validated field" descriptor guarding ``log_foundry_counters`` on assignment
    leaves the instance value visible. The class side reads through ``type``'s unbound
    ``__mro__`` and ``__dict__`` accessors and a ``__slots__`` member through its builtin
    ``__get__``; the instance side is :func:`_own_attribute`'s. So no property, ``__getattr__``,
    ``__getattribute__``, descriptor ``__get__`` or metaclass hook (``__mro__``, ``__name__``,
    ``__eq__``, ``__hash__``) runs; the one residue, the ``__eq__`` of a dict key whose hash
    collides with the name, is :func:`_class_member`'s, and ``getattr`` has it too.
    ``inspect.getattr_static`` was used
    here first and is not this: its shadowed-``__dict__`` check is ``lru_cache``-wrapped, so it
    hashes the class and can compare it, which runs a metaclass's ``__hash__`` and ``__eq__``
    (SPEC-072's first diff review, measured on 3.12 and 3.13). A slot that is unset reads as
    absent; any other descriptor is returned as found, so a property reads as the property object
    and is refused by the caller's type test. Where the instance side raises, the class's own
    member is still returned. ``tests/test_static_read_corpus.py`` holds this to ``getattr`` over
    a corpus of class shapes, the divergences named case by case.

    Args:
      obj: The object to read.
      name: The attribute's name, an exact ``str``.

    Returns:
      The value, or :data:`_ABSENT` when it is missing or could not be read.

    Raises:
      None.
    """
    try:
        cls = type(obj)
        found = _class_member(cls, name)
        if found is not None:
            kind = type(found[0])
            if kind is types.MemberDescriptorType:
                return _MEMBER_GET(cast("types.MemberDescriptorType", found[0]), obj, cls)
            if _class_member(kind, "__get__") and (
                _class_member(kind, "__set__") or _class_member(kind, "__delete__")
            ):
                return found[0]
    except Exception:
        return _ABSENT
    try:
        own = _own_attribute(obj, name, found)
    except Exception:
        own = _ABSENT
    if own is not _ABSENT:
        return own
    return _ABSENT if found is None else found[0]


def read_losses(sink: object) -> SinkLosses | None:
    """Reads a sink's optional ``losses()``.

    This is the single reader for the optional half of the protocol (SPEC-026 FR-002), shared
    by ``Worker.health`` and ``MultiSink.losses`` so the probe and its guarantees are written
    once. Nothing is written to stderr — a poll happens as often as the caller likes, and a
    broken accessor is a sink bug rather than a loss.

    Args:
      sink: The sink to probe, of any type.

    Returns:
      The sink's losses, or ``None`` when it reports nothing. ``None`` covers four cases
      deliberately treated alike: no ``losses`` attribute, one that is not callable, one that
      raised, and one that returned anything other than a ``SinkLosses`` — including ``None``
      itself, which the shipped wrapper sinks return when what they wrap reports nothing and
      they have counted nothing of their own; one that has counted a drop reports it over an
      inner sink that reports nothing (SPEC-058 FR-003).

    Raises:
      None. ``health()`` is documented as never raising and is the call an operator makes when
        things are already going wrong, so a third-party sink with a broken accessor must not
        take the snapshot down with it. The returned shape is checked as well as the call.
    """
    try:
        accessor = getattr(sink, "losses", None)
        losses = accessor() if callable(accessor) else None
    except Exception:
        return None
    return losses if isinstance(losses, SinkLosses) else None


def flush_sink(sink: object) -> bool:
    """Calls a sink's optional ``flush()``, letting any failure propagate (SPEC-036 FR-002).

    The sibling of :func:`read_losses`, written here for the same reason — the probe and its
    guarantees belong in one place — and with the **opposite** failure rule, which is the part
    worth reading twice. ``read_losses`` swallows a raising accessor because a broken reporter
    must not take ``health()`` down with it. This one must not swallow anything: a sink's flush
    failure reaches the caller only through ``log_foundry.flush()``'s result, so absorbing it here
    would produce the exact "sink the worker believes" this module exists to prevent — a
    ``flush()`` reporting success over a client buffer that never went out.

    Args:
      sink: The sink to probe, of any type.

    Returns:
      Whether the sink had a ``flush`` to call. ``False`` means it holds nothing of its own, and
      the queue drain was the whole of the flush.

    Raises:
      Exception: Whatever the sink's ``flush`` raises, deliberately unguarded. The caller turns
        it into a ``FlushResult`` reason; see ``_lifecycle._flush_live_sink``.
    """
    accessor = getattr(sink, "flush", None)
    if not callable(accessor):
        return False
    accessor()
    return True
