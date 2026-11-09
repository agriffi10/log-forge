"""Constructor argument checks the sinks share, each in one place (SPEC-071 FR-003, FR-005)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from log_foundry import _diag

if TYPE_CHECKING:
    from collections.abc import Mapping


def refuse_with_injected(
    owner: str, kind: str, injected: object | None, supplied: Mapping[str, object | None]
) -> None:
    """Refuses every build option passed beside an injected client, producer or connection.

    An option a sink builds its client from cannot reach a client the caller built, so accepting
    it would silently ignore an argument because another was given — invariant 13's observable.
    Each sink's constructor calls this once, before anything is built, with every option it would
    build from whose default is ``None`` (``tests/test_injected_client_roster.py`` derives that
    set from the build path and holds each constructor to it).

    **The test is ``is not None``, never truthiness**, for the injected object and for each value:
    ``KafkaSink(producer=p, producer_config={})`` is refused, and an injected client whose
    ``__bool__`` is false or raises is still an injected client. Nothing of a value runs, and the
    message carries names only (invariant 11).

    Args:
      owner: The constructing class's name, for the message.
      kind: ``"client"``, ``"producer"`` or ``"connection"``: the injected keyword.
      injected: The injected object, or ``None`` when the sink builds its own.
      supplied: Each build option by name, with the value the caller passed.

    Returns:
      None.

    Raises:
      ValueError: When an object is injected and any option is not ``None``; the message begins
        ``<owner> cannot apply <names, sorted> to an injected <kind>`` and names the keyword to
        drop.
    """
    if injected is None:
        return
    names = sorted(name for name, value in supplied.items() if value is not None)
    if names:
        raise ValueError(
            f"{owner} cannot apply {', '.join(names)} to an injected {kind}; pass them where the "
            f"{kind} is built, or drop {kind}="
        )


def require_callable(
    value: object, name: str, owner: str, *, text: bool = False, optional: bool = True
) -> None:
    """Refuses, at the constructor, an argument the sink will call but that is not callable.

    Seven constructor arguments are called later, per event or at ``close()``, and none was tested:
    ``message_group_id=5`` constructed and then raised ``'int' object is not callable`` out of
    every ``emit`` (SPEC-071 FR-005). The reach of a type refusal extends to every callable
    argument a constructor stores, whether or not the selected branch reads it: the value is
    outside the declared type, so no type-checked caller breaks (the bad-argument decision).

    **It asks the type, never the value.** ``callable(value)`` reads the type's call slot, and the
    ``str`` route is ``issubclass(type(value), str)``, so a value whose ``__class__`` claims to be a
    ``str`` or a function is judged by what it is and nothing of it runs. The message names the
    type through its stored name (``_diag._STORED_NAME``), which a metaclass ``__name__`` property
    cannot answer.

    Args:
      value: The argument.
      name: Its keyword, for the message.
      owner: The constructing class's name, for the message.
      text: Also accept a value whose real type is a ``str`` subclass (``message_group_id``).
      optional: Accept ``None``; ``False`` for a required callable (``CallbackSink``'s ``fn``).

    Returns:
      None.

    Raises:
      TypeError: When the value is none of what is accepted, naming the owner, the argument and
        the type: ``must be callable or None``, ``must be callable`` without ``optional``, or
        ``must be a str, a callable or None`` with ``text``.
    """
    if optional and value is None:
        return
    if callable(value):
        return
    if text and issubclass(type(value), str):
        return
    if text:
        wanted = "a str, a callable or None" if optional else "a str or a callable"
    else:
        wanted = "callable or None" if optional else "callable"
    raise TypeError(
        f"{owner} {name} must be {wanted}, not {_diag._STR_STR(_diag._STORED_NAME(type(value)))}"
    )


class ReadOnlyCount[T]:
    """A constructor's exact ``int`` exposed read-only: assignment refuses, reading finds it.

    SPEC-074 FR-006 exposes ``max_retries`` on ``RabbitMQSink``, ``SocketTransport`` and
    ``SyslogSink``, which kept it private in 1.x, because the delivery engine reads it by name to
    size a run. The engine reads it statically (``base._static_value``), so a ``property`` would
    read as the property object and give the sink one round. This descriptor defines ``__set__``
    and no ``__get__``: CPython's lookup — and the static reader, which follows it — passes over
    a descriptor without ``__get__`` to the instance's own ``__dict__``, where
    :func:`store_read_only` writes the value, while an assignment reaches ``__set__`` and is
    refused. The ``__get__`` below exists for the type checker only.

    It is generic in what a read returns, for the type checker alone: ``ReadOnlyCount[int]`` on
    the SPEC-074 sinks, which store the floored constructor value, and ``ReadOnlyCount[int |
    None]`` on SPEC-075's ``FilteringSink``, ``TransformSink`` and ``LogstashSink``, which store
    the ``max_retries`` they copied from the sink they wrap, or ``None`` when they copied none.
    A class that declares one stores a value in every instance it builds, since a read of a name
    with nothing stored finds this descriptor object itself. The refusal's reason says where the
    value comes from: by default the constructor, as on the SPEC-074 sinks, and on SPEC-075's
    classes the sink or backend each one delivers through. ``FilteringSink`` and
    ``TransformSink`` take no ``max_retries`` at all; ``LogstashSink`` takes one, but hands it to
    the backend it builds and exposes the value the engine reads off that backend.

    Attributes:
      None.
    """

    __slots__ = ("_name", "_reason")

    def __init__(self, reason: str = "pass it to the constructor") -> None:
        """Holds the reason the refusal gives, after the attribute's name.

        Args:
          reason: Where the value comes from, as the refusal states it.

        Returns:
          None.

        Raises:
          None.
        """
        self._reason = reason

    if TYPE_CHECKING:

        def __get__(self, obj: object, owner: type | None = None) -> T:
            """Types a read as the stored value; never defined at run time.

            Args:
              obj: The instance.
              owner: Its class.

            Returns:
              The stored value.

            Raises:
              None.
            """
            ...

    def __set_name__(self, owner: type, name: str) -> None:
        """Records the attribute's name, for the refusal's message.

        Args:
          owner: The class the descriptor is assigned in.
          name: The attribute's name.

        Returns:
          None.

        Raises:
          None.
        """
        self._name = name

    def __set__(self, obj: object, value: object) -> None:
        """Refuses an assignment: the value is the constructor's.

        Args:
          obj: The instance.
          value: The value assigned, never read.

        Returns:
          None.

        Raises:
          AttributeError: Always, naming the attribute.
        """
        raise AttributeError(f"{self._name} is read-only; {self._reason}")


class ReadOnlySeconds:
    """A copied ``max_retry_after`` exposed read-only, the sibling of :class:`ReadOnlyCount`.

    SPEC-075 gives ``FilteringSink``, ``TransformSink`` and ``LogstashSink`` the retry ceiling of
    the sink each wraps, copied at construction, because the engine runs its rounds on the
    wrapper and reads the ceiling there statically. A writable copy would let an assignment on
    the wrapper move the engine's ceiling away from the wrapped sink's, and making it read-only
    later would break a 1.x caller, so it is read-only from the start. It works as
    :class:`ReadOnlyCount` does — ``__set__`` refuses, no run-time ``__get__``, the value in the
    instance's own ``__dict__`` through :func:`store_read_only` — and differs only in what a read
    is typed as: seconds, or ``None`` when nothing was copied. Its refusal says where the value
    came from, as :class:`ReadOnlyCount`'s does.

    Attributes:
      None.
    """

    __slots__ = ("_name", "_reason")

    def __init__(self, reason: str = "it is copied from the wrapped sink") -> None:
        """Holds the reason the refusal gives, after the attribute's name.

        Args:
          reason: Where the value comes from, as the refusal states it.

        Returns:
          None.

        Raises:
          None.
        """
        self._reason = reason

    if TYPE_CHECKING:

        def __get__(self, obj: object, owner: type | None = None) -> float | None:
            """Types a read as the stored ceiling; never defined at run time.

            Args:
              obj: The instance.
              owner: Its class.

            Returns:
              The stored value.

            Raises:
              None.
            """
            ...

    def __set_name__(self, owner: type, name: str) -> None:
        """Records the attribute's name, for the refusal's message.

        Args:
          owner: The class the descriptor is assigned in.
          name: The attribute's name.

        Returns:
          None.

        Raises:
          None.
        """
        self._name = name

    def __set__(self, obj: object, value: object) -> None:
        """Refuses an assignment: the value is copied at construction.

        Args:
          obj: The instance.
          value: The value assigned, never read.

        Returns:
          None.

        Raises:
          AttributeError: Always, naming the attribute.
        """
        raise AttributeError(f"{self._name} is read-only; {self._reason}")


def store_read_only(obj: object, name: str, value: object) -> None:
    """Writes a :class:`ReadOnlyCount`'s or :class:`ReadOnlySeconds`' value into ``__dict__``.

    The one write past the refusing ``__set__``, made by the constructor that judged the value.

    Args:
      obj: The instance.
      name: The attribute's name.
      value: The value, already judged or copied by the constructor.

    Returns:
      None.

    Raises:
      None.
    """
    vars(obj)[name] = value
