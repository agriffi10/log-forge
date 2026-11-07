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
