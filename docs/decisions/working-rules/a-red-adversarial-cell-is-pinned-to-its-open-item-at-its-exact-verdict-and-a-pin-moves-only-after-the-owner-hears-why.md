---
type: Decision
title: "A red adversarial cell is pinned to its open item at its exact verdict, and a pin moves only after the owner hears why"
description: "a red adversarial cell is pinned to the `docs/architecture.md` §12 open item that records its defect, as a strict expected failure at its exact verdict, so a pass or any other verdict fails the suite; what a pin withholds is computed from the pin alone; no pin is added, changed or removed to make the suite pass before the differing verdict is reported to the owner; `EPSILON` changes only by revising the spec. (SPEC-085)"
---

`PINS` in `tests/model_pins.py` is the register of where the adversarial sink cells are red. A
pin names its cells, the exact verdict each has (a set of `Class:key` signatures), and its item:
the bold lead of a bullet under `### Open` in `docs/architecture.md` §12. A pinned cell still
runs, as `xfail(strict=True, raises=PinnedVerdict)`, and `judge` raises `PinnedVerdict` only when
the verdict equals the pin's exactly. A pass fails the suite with the instruction to remove the
pin and move its item to *Resolved*, any other verdict fails it, and a crash is never an expected
failure. Tests hold every pinned cell to the generated table, every item to an *Open* lead, and the
model files to no `skip`, `skipif`, `importorskip` or non-strict `xfail`.

**Why the exact verdict and a strict mark.** A class alone is not a reason: any hang is a
`HangFound`, so a pin at a class would let a second defect in the same cell pass as the first. A
strict mark makes the day a defect is fixed a red run, so neither the pin nor its item outlives
the defect. Its item then leaves *Open* as
`docs/decisions/working-rules/an-open-item-is-closed-by-being-fixed-settled-or-recorded-as-a-constraint--never-deleted.md`
says.

**Why what is withheld comes from the pin alone.** A pin on a one-operation script withholds the
longer scripts holding that operation at the same place, and the scripts holding its sibling: the
pinned defect again, at seconds of waiting each. A withheld cell is not collected at all, so a
hand-kept list of them would leave cells hidden after their pin was deleted, with no defect behind
them and nothing to say so. `withheld()` reads `PINS` at the time of the call, and a test empties
`PINS` and asserts that all 637 cells are scheduled.

**Why a pin moves only after the owner hears why.** Editing the register is the one change that
turns the gate green while leaving it looking intact: pinning a new red as a known defect silences
the signal the cell exists to raise, and unpinning a cell that changed verdict hides what changed.
So a cell whose verdict differs from the register (red and unlisted, listed and green, or listed at
another verdict) is a finding, and it is reported to the owner with its verdict before any pin is
added, changed or removed. A spec that fixes a pinned defect names the pin it removes, which is
how the owner hears of that one.

**Why `EPSILON` changes only by revising the spec.** It is 1.0 s, set by SPEC-085 FR-003: over
twice the slowest return seen under load, and half the 2.0 s closer grace, the shortest self-wait
the cells must see. Above 1.0 s that self-wait passes unseen; below it, load reddens honest calls.
A CI leg whose worst margin over passing cells exceeds `MARGIN_REDERIVE` stops the merge, and the
remedy is a spec revision that re-derives the value, never an edit of the constant to quiet one run.

**Rejected:** pinning at a class or a signature prefix, which hides a second defect; a non-strict
`xfail` or a skip, which outlives the fix; and a hand list of withheld cells, which outlives its
pin.

**Do NOT build:** a pin, a withheld list or an `EPSILON` value edited to make a red run pass
before the differing verdict has reached the owner.

## History

### 2026-11-15

- **Settled** by SPEC-085 at its completion, as the register the `audit-2026-11-14` arc's later
  specs build on.
