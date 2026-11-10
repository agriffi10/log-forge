"""The one refusal of a sink argument that can never be a sink (SPEC-079 FR-005)."""

from __future__ import annotations

from log_foundry import _diag

_UNPATCHABLE: tuple[type, ...] = (
    object,
    str,
    bytes,
    bytearray,
    int,
    float,
    complex,
    bool,
    list,
    tuple,
    dict,
    set,
    frozenset,
    range,
)
"""The exact builtin types a sink argument is refused for: none can ever gain a callable ``emit``.

Each was probed at ``4fbfd60``: it is not a heap type and its ``__dictoffset__`` is ``0``, so
``setattr(x, "emit", f)`` raises; no class in its MRO has an ``emit``; and configured as
``sink=`` it lost every batch. **A closed list, not a rule.** "Not a heap type, and no instance
``__dict__``" was probed too and over-refuses: it holds for ``weakref.proxy(sink)`` and for
``super(Sub, inst)``, and both deliver, because a C type's own ``tp_getattro`` can forward
attribute lookup and Python cannot tell a forwarding one from the generic one. So another
unpatchable C type — ``memoryview``, an extension type — is accepted and loses its batches counted
and announced, as before; that trade is deliberate (Andrew's decisions of 2026-11-05).
"""


def unpatchable(value: object) -> bool:
    """Whether a value is exactly one of :data:`_UNPATCHABLE`'s types, so it can never be a sink.

    ``type(value) is t`` reads the type pointer and compares identities, so nothing of the value
    runs. Shared by :func:`require_sink` and by the fork walk's positional record, which skips a
    value ``require_sink`` would have refused even when one was assigned into a slot after
    construction.

    Args:
      value: Any object.

    Returns:
      Whether its exact type is one of the unpatchable builtin types.

    Raises:
      None.
    """
    return any(type(value) is kind for kind in _UNPATCHABLE)


def require_sink(value: object, name: str) -> None:
    """Refuses a sink argument that is exactly one of the unpatchable builtin types.

    The test is ``type(value) is t`` for each member of :data:`_UNPATCHABLE`: ``type()`` reads the
    object's type pointer and ``is`` compares identities, so neither a metaclass ``__eq__`` nor
    anything of the value runs. A value that lacks an ``emit`` today but could gain one — a class
    with ``emit = None`` whose ``start()`` binds it, a ``SimpleNamespace``, a module, a function —
    is accepted, since it delivers once it does; one that never does loses its batches counted
    and announced. The message names the type through its stored name, never the value.

    Args:
      value: The sink argument as the caller passed it.
      name: The argument's name for the message: ``sink``, ``FilteringSink inner``,
        ``TransformSink inner`` or ``MultiSink sinks[i]``.

    Returns:
      None.

    Raises:
      TypeError: When ``value`` is exactly one of the unpatchable builtin types.
    """
    if unpatchable(value):
        raise TypeError(
            f"{name} must be a sink with an emit method, "
            f"got {_diag._STR_STR(_diag._STORED_NAME(type(value)))}"
        )
