"""Constructor argument checks the sinks share, each in one place (SPEC-071 FR-003, FR-005)."""

from __future__ import annotations

from typing import TYPE_CHECKING

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
