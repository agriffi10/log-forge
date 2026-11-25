"""SPEC-099's nightly ceilings for the known soak shapes, kept out of the clearing files.

A clearing pull request (FR-006) changes ``tests/soak_known.py`` and ``docs/architecture.md`` only,
so it cannot raise a ceiling here: a shape it has just filed reads :data:`UNMEASURED_CEILING`
until an ordinary reviewed pull request measures it and writes its ceiling below. Each ceiling is
set from its shape's rate in one night of 15,000 items so that a rate that doubles stays under it
and one that triples passes it, for every rate measured. Each entry carries its measurement, the
commit it was measured at and "x2 under, x3 over" in a comment on its own line, which
``test_every_ceiling_names_a_row_and_carries_its_derivation`` holds.
"""

UNMEASURED_CEILING = 3
"""A row ``CEILINGS`` does not name: the night that filed it may recur at a like count, no more."""

CEILINGS: dict[str, int] = {
    "orphan-write-after-close": 21,  # measured 10 a night at eda19f32 (24 in 36,000) and 7.5 at d9f2f097 (6 in 12,000), macOS; x2 under, x3 over
}
"""Each measured ceiling, keyed by a ``KNOWN`` row's name."""


def ceiling_of(name: str) -> int:
    """Returns a known row's nightly ceiling.

    Args:
      name: The row's name.

    Returns:
      Its measured ceiling, or :data:`UNMEASURED_CEILING` when none is written.

    Raises:
      None.
    """
    return CEILINGS.get(name, UNMEASURED_CEILING)
