"""SPEC-099's nightly ceilings for the known soak shapes, kept out of the clearing files.

A clearing pull request (FR-006) changes ``tests/soak_known.py`` and ``docs/architecture.md`` only,
so it cannot raise a ceiling here: a shape it has just filed reads :data:`UNMEASURED_CEILING`
until an ordinary reviewed pull request measures it and writes its ceiling below. Each ceiling is
set from its shape's rate in one night of 15,000 items so that a rate that doubles stays under it
and one that triples passes it. The late-write rate on macOS depends on load, so its ceiling is
derived from the moderate-load macOS rates and applies to the scheduled nights, which run on Linux,
where the stand-in measured about half a night's item and the ceiling is loose; the first nights
on a real runner re-derive it. Each entry carries its measurement, the commit it was measured at
and "x2 under, x3 over" in a comment on its own line, which
``test_every_ceiling_names_a_row_and_carries_its_derivation`` holds.
"""

UNMEASURED_CEILING = 3
"""A row ``CEILINGS`` does not name: the night that filed it may recur at a like count, no more."""

CEILINGS: dict[str, int] = {
    "orphan-write-after-close": 21,  # measured 10 a night at eda19f32 (24 in 36,000, loads to 15) and 7.5 at d9f2f097 (6 in 12,000, load ~10), macOS; x2 under, x3 over at those loads, not at loads 19-34 (21.7 a night, same library); Linux stand-in 0.5 a night, so loose there
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
