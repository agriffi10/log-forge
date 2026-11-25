"""The nightly model soak's seeds and its report (SPEC-099 FR-003, FR-004).

Two subcommands, both run by ``.github/workflows/model-soak.yml``:

    python scripts/model-soak-report.py seeds --event <event> --asked=<input> --run-number <n>
    python scripts/model-soak-report.py report soak.xml <seeds> --summary <path>

``seeds`` prints the seeds a run soaks on one line of digits, commas and hyphens, or refuses with
exit 1 and nothing on stdout. ``report`` reads the soak step's JUnit XML, writes Markdown to the
summary file itself and to stdout, and exits 1 when the run's floor or a ceiling is broken. An
exit code from pytest is not a floor: a narrower ``-k``, a collection that drops the soak, a skip
mark or an ``xfail`` marker each exit 0 over fewer items, or over none that judged anything.
"""

from __future__ import annotations

import argparse
import collections
import pathlib
import sys
import xml.etree.ElementTree as ET

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "tests"))

import soak_known
from model_oracles import FORMS_SOAKED, parse_seeds, rerun_command
from soak_ceilings import ceiling_of
from soak_verdict import KNOWN_PREFIX

# Measured at d9f2f097 on the 4-CPU Linux stand-in: 15,000 soak items took 307.6 s and 284.3 s.
WINDOW = 5000
"""Seeds one night soaks, 15,000 soak items: about five minutes on a 4-CPU runner at ``-n 12``."""

WINDOWS = 30
"""Windows the nights rotate through, so the nights cover seeds 0 to 149,999 every 30 runs."""

# Measured at d9f2f097 on the 4-CPU Linux stand-in: 61.5 ms a seed, so 10,000 seeds is 615 s.
MAX_SEEDS = 10_000
"""The most seeds a dispatch may ask for, so it ends with its report rather than being killed.

The job has 1,800 s. 180 s is allowed for the install and 600 s for one hung item's guard and its
dump, which leaves 1,020 s. At the stand-in's cost per seed, 10,000 seeds takes 615 s, and half as
much again is 923 s, both under 1,020 s.
"""

PULL_REQUEST_SEEDS = "0-99"
"""What a pull request that changes the soak runs: 300 soak items."""

SOAK = "test_soak_the_close_count_under_concurrency["
"""The id prefix of every soak item."""

WIRING = (
    "test_the_soak_is_wired[emit]",
    "test_the_soak_is_wired[deliver]",
    "test_the_soak_is_wired[http]",
    "test_the_soak_records_the_worker_path",
    "test_a_name_the_soak_cannot_wrap_fails_it[_lifecycle._CloseRegistry.arm]",
    "test_a_name_the_soak_cannot_wrap_fails_it[_lifecycle._CloseRegistry.take]",
    "test_a_name_the_soak_cannot_wrap_fails_it[log_foundry.api._note_orphan_emit]",
    "test_a_name_the_soak_cannot_wrap_fails_it[_lifecycle._get_worker]",
)
"""The eight items of the wiring set, each of which must run and pass."""

PROPERTY = "soak_known"
"""The JUnit property a known soak item records its shapes' names under."""


def normalise(seeds: tuple[int, ...]) -> str:
    """Returns sorted seeds as comma-separated inclusive ranges: digits, commas and hyphens only.

    Args:
      seeds: The seeds, sorted, each once.

    Returns:
      Such as ``1-2,4``; empty for no seeds.

    Raises:
      None.
    """
    parts: list[str] = []
    start = prev = -2
    for seed in seeds:
        if seed == prev + 1:
            prev = seed
            continue
        if start >= 0:
            parts.append(str(start) if start == prev else f"{start}-{prev}")
        start = prev = seed
    if start >= 0:
        parts.append(str(start) if start == prev else f"{start}-{prev}")
    return ",".join(parts)


def written_count(asked: str) -> int:
    """Counts the seeds a dispatch's text names, each written range by its ends, without a set.

    An overlap counts twice, so the count is never below the seeds named, and an element that
    does not parse counts nothing here: ``parse_seeds`` refuses it afterwards.

    Args:
      asked: The dispatch's ``seeds`` input.

    Returns:
      The count.

    Raises:
      None.
    """
    written = 0
    for item in asked.split(","):
        low, _, high = item.strip().partition("-")
        if low.isascii() and low.isdigit() and (not high or (high.isascii() and high.isdigit())):
            written += max(int(high or low) - int(low) + 1, 0)
    return written


def seeds(event: str, asked: str, run_number: int) -> str:
    """Returns the seeds a run soaks: a dispatch's, a pull request's, or the run number's window.

    A dispatch's text is counted before any set is built, so ``0-10000000`` is refused without
    building ten million seeds, then parsed by ``parse_seeds`` and printed normalised, so an input
    carrying a newline cannot write a second line into ``$GITHUB_OUTPUT``. The window is taken from
    the run number and nothing else: a re-run keeps its run number, so it soaks the same seeds, and
    nothing in the choice reads a clock.

    Args:
      event: ``github.event_name``.
      asked: The dispatch's ``seeds`` input, empty for none.
      run_number: ``github.run_number``.

    Returns:
      The seeds, normalised.

    Raises:
      ValueError: For a dispatch over ``MAX_SEEDS``, or one ``parse_seeds`` refuses.
    """
    if asked:
        written = written_count(asked)
        if written > MAX_SEEDS:
            raise ValueError(f"seeds={asked!r} names {written} seeds; at most {MAX_SEEDS}")
        return normalise(parse_seeds(asked))
    if event == "pull_request":
        return PULL_REQUEST_SEEDS
    start = (run_number % WINDOWS) * WINDOW
    return f"{start}-{start + WINDOW - 1}"


def _seed_of(name: str) -> int:
    """Returns the seed a soak item's id names.

    Args:
      name: Such as ``test_soak_the_close_count_under_concurrency[emit-seed7]``.

    Returns:
      Its seed.

    Raises:
      ValueError: If the id names none.
    """
    return int(name.rsplit("seed", 1)[1].rstrip("]"))


def report(xml: pathlib.Path, seeds_value: str) -> tuple[str, int]:
    """Returns the run's summary in Markdown and the exit status its floor and ceilings set.

    The floor: exactly ``3 x |seeds|`` soak items ran; none was skipped; every expected failure
    carries both the ``soak_known`` property and a reason starting ``known soak finding:``; and
    every item of the wiring set ran and passed. The ceilings: each known shape's count of items,
    never of sinks, is at most its ceiling, and names a row.

    Args:
      xml: The soak step's JUnit XML.
      seeds_value: The seeds the run was asked for.

    Returns:
      The Markdown, and 0 or 1.

    Raises:
      None.
    """
    lines = [f"## Model soak: seeds {seeds_value or '(none)'}", ""]
    try:
        expected = len(parse_seeds(seeds_value)) * len(FORMS_SOAKED)
    except ValueError as exc:
        return "\n".join([*lines, f"**Floor broken:** no seeds to read: {exc}", ""]), 1
    try:
        cases = list(ET.parse(xml).getroot().iter("testcase"))  # noqa: S314
    except (OSError, ET.ParseError) as exc:
        return "\n".join([*lines, f"**Floor broken:** no readable JUnit XML: {exc}", ""]), 1
    soaked = clean = known_items = new_items = 0
    unexplained: list[str] = []
    known: collections.Counter[str] = collections.Counter()
    new: list[str] = []
    wiring: dict[str, bool] = {}
    for case in cases:
        name = case.get("name", "")
        failure = case.find("failure")
        if failure is None:
            failure = case.find("error")
        skipped = case.find("skipped")
        if name in WIRING:
            wiring[name] = failure is None and skipped is None
        if not name.startswith(SOAK):
            continue
        soaked += 1
        props = {p.get("name"): p.get("value") or "" for p in case.iter("property")}
        if failure is not None:
            first = (failure.get("message") or "").splitlines()[:1]
            node = f"tests/test_invariants_model.py::{name}"
            new_items += 1
            new.append(f"- `{name}`: {' '.join(first)}")
            new.append(f"  re-run: `{rerun_command(node, _seed_of(name), soak=True)}`")
        elif skipped is not None:
            message = skipped.get("message") or ""
            shapes = {shape for shape in props.get(PROPERTY, "").split(",") if shape}
            if (
                skipped.get("type") == "pytest.xfail"
                and message.startswith(KNOWN_PREFIX)
                and shapes
            ):
                known_items += 1
                known.update(shapes)
            else:
                unexplained.append(name)
        else:
            clean += 1
    lines.append(f"- soak items run: {soaked} of {expected}")
    lines.append(f"- clean: {clean}; known: {known_items}; new: {new_items}")
    lines.append(f"- wiring items passed: {sum(wiring.values())} of {len(WIRING)}")
    status = 0
    rows = {row.name: row for row in soak_known.KNOWN}
    if known:
        lines += ["", "### Known, each an open item of `docs/architecture.md` §12", ""]
        for shape, count in sorted(known.items()):
            row = rows.get(shape)
            ceiling = ceiling_of(shape)
            item = row.item if row is not None else "(no such row)"
            lines.append(f"- {count} x {shape}, ceiling {ceiling}: {item}")
            if row is None:
                lines.append(f"  **No such row:** {shape} names no row of tests/soak_known.py.")
                status = 1
            elif count > ceiling:
                lines.append(
                    f"  **Ceiling broken:** {shape} held {count} items, ceiling {ceiling}."
                )
                status = 1
    if new:
        lines += ["", "### New", "", *new]
    if soaked != expected:
        lines += ["", f"**Floor broken:** {soaked} soak items ran where {expected} were asked for."]
        status = 1
    if unexplained:
        lines += [
            "",
            (
                f"**Floor broken:** {len(unexplained)} soak items were skipped or marked "
                f"expected to fail without a known shape: {unexplained[:5]}"
            ),
        ]
        status = 1
    missing = [name for name in WIRING if name not in wiring]
    failed = [name for name, passed in wiring.items() if not passed]
    if missing or failed:
        lines += ["", f"**Wiring broken:** did not run: {missing}; did not pass: {failed}"]
        status = 1
    return "\n".join(lines) + "\n", status


def main(argv: list[str] | None = None) -> int:
    """Runs one subcommand.

    Args:
      argv: The arguments, ``sys.argv[1:]`` when ``None``.

    Returns:
      The exit status.

    Raises:
      None.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    pick = sub.add_parser("seeds")
    pick.add_argument("--event", default="")
    pick.add_argument("--asked", default="")
    pick.add_argument("--run-number", type=int, required=True)
    rep = sub.add_parser("report")
    rep.add_argument("xml", type=pathlib.Path)
    rep.add_argument("seeds")
    rep.add_argument("--summary", type=pathlib.Path)
    args = parser.parse_args(argv)
    if args.command == "seeds":
        try:
            chosen = seeds(args.event, args.asked, args.run_number)
        except ValueError as exc:
            print(f"model-soak-report: refused: {exc}", file=sys.stderr)
            return 1
        print(chosen)
        return 0
    text, status = report(args.xml, args.seeds)
    print(text)
    if args.summary is not None:
        with args.summary.open("a", encoding="utf-8") as out:
            out.write(text)
    return status


if __name__ == "__main__":
    sys.exit(main())
