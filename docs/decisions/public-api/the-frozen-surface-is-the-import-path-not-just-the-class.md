---
type: Decision
title: "The frozen surface is the import path, not just the class"
description: "no concrete sink is in `__all__`, so a class frozen at a path free to move was pinned to nothing; `1.0.0` itself deleted `sinks/util.py` and moved three classes. The documented dotted path, and the public names the signatures use, are inside the `1.x` promise. (No spec — settled on the README's 1.0.0 pass, `48299e3`.)"
---

**The frozen surface is the import path, not just the class** — the promise published in the `v1.0.0` release body was "everything in
`log_foundry.__all__`, the `Sink` protocol, and every shipped sink class". The third clause pinned
nothing, and the reason is structural rather than a matter of degree: **no concrete sink is
exported from the top level at all** — the only `*Sink` name in `__all__` is the `Sink` protocol,
and `sinks/__init__.py` is a bare docstring that re-exports nothing — so every one of them is
reachable only as `from log_foundry.sinks.sqs import SQSSink`. A class whose sole route can move
is not frozen, and
the same release proved it by deleting `sinks/util.py` and relocating `MemorySink`, `NullSink` and
`StderrSink` — a `ModuleNotFoundError` on upgrade for anyone who had imported them. So the freeze
covers the class **at its documented import path**, plus the public names its signatures use
(`GroupIdSource`, `DedupIdSource`, `Backend`), which the fence *A frozen surface is keyword-first* already treats
as exported but which neither `__all__` nor "sink class" reaches.

**The cost is real and is accepted:** the **documented** `sinks/` module layout is frozen for
`1.x` — the `sinks/_*.py` helpers stay private and free to move — and the split `1.0.0` performed
is one this fence now forbids. Moving a module in `1.x` therefore costs a permanent re-export at
the old path, and that is a genuine cost rather than a loophole: a re-export added in `1.x` has to
live until `2.0.0`, exactly like the keyword alias SPEC-034 refused for that same reason
(`SPEC-034` FR-002 AC-1: "an alias would have to live for the whole of `1.x`"). What the tag removed is
not the cost but the **alternative** — before it, a module could be renamed and the old name
deleted outright, which is what made refusing an alias free. That option is gone, so the trade is
now re-export or wait for `2.0.0`, and the fence is what stops the layout drifting by accident in
the meantime.

**The published notes and this register disagree, deliberately, and this register wins.** The
`v1.0.0` GitHub Release body carries the narrower sentence and cannot be amended: the release is
immutable (`gh api repos/agriffi10/log-forge/releases/tags/v1.0.0 --jq .immutable` → `true`), which
in this repository is usually cited for frozen *assets* and applies to the body just the same. The
divergence is therefore permanent rather than something to fix.

The repository's statement is the authority. `main`'s copy of `docs/release-notes/v1.0.0.md`
carries a marker at the site of the old claim — but note that the **two copies a reader actually
reaches cannot carry one**: the release body, and the `blob/v1.0.0/` blob that `README.md` and
`CHANGELOG.md` deliberately link tag-absolute. So the marker serves the next agent reading `main`,
not the upgrader, and the current form is what `README.md` states in full. `CHANGELOG.md` carries
the import-path half but not the signature-names half; it states a partial new form rather than the
old one, so it is owed no marker. **The promise is gated against a pinned baseline, and the baseline is the point.**
`test_every_frozen_sink_import_path_still_resolves` reads the pairs promised at `v1.0.0` from
`tests/data/frozen_sink_paths.txt` — committed data, not re-derived from the README each run. The
first version of this gate did re-derive it, and was useless for the reason that a *temporal*
promise cannot be checked by a *same-commit* comparison: a contributor who renames a module and
updates the README satisfies both sides at once, so replaying the `sinks/util.py` split against it
came out green. Reading the tag at runtime is not the alternative either, since CI checks out
without tags. The file therefore says in its own header that a move leaves the old line standing
and adds a re-export, and that it is regenerated wholesale only at `2.0.0`.

Three failure modes, three assertions: a frozen path stops resolving — as a **class**, since a
`LokiSink = None` optional-dependency fallback satisfies `hasattr` while `LokiSink(url)` raises;
the README stops documenting a frozen path, so the promise survives but no reader can find it; or
a new sink **class** ships undocumented and is outside the freeze by omission rather than decision.
That third one is class-level rather than module-level because the promise is: a module-level
version missed both a sink shipped in a package's `__init__.py` and a new class added to an
already-documented module. ~~`sinks/base` is the one exempt module~~ — since SPEC-072, `sinks/base`
and `sinks/verdict` are the two exempt modules, each because everything it exports is a top-level
export, so the `__all__` clause of the freeze covers it without a README table row; `sinks/verdict`
(`Outcome`, `Verdict`, `SinkCounters`, `read_deliver`, `emit_via_deliver`) is a documented path the
freeze covers. Each
exemption is derived from the module's own `__all__` rather than a listed set — a listed one stops
describing the module the moment a name is added, which is what a hand-written three-name version
of it did while `base.__all__` already had five.

**A documented contract that names a function puts that function on the surface.** SPEC-072 left
`emit_via_deliver` in the private `sinks/_engine.py` while the public `Sink` docstring told a
third-party author whose sink defines `deliver` to make its `emit` `emit_via_deliver(self, batch)`,
so following the documented contract meant importing a path free to move — the gap this decision
exists to close, one level down from a class. For 1.2.0 it is public, a fifth new name beside
SPEC-072's four, in `log_foundry.__all__` and `sinks.verdict.__all__`, and `sinks.verdict` is its
documented path. It is its own definition there, delegating to the engine through a function-level
import, because `_engine` imports `verdict` at module level; so the engine stays private and free
to move, and the public name does not depend on its name. Its surface is keyword-first, as *A
frozen surface is keyword-first, and says what it will not grow* asks of a dataclass: `sink` and
`batch` are its only positional parameters, `deliver` is keyword-only with a default, anything it
gains in 1.x is keyword-only with a default, and it returns `None` (invariant 10).
`tests/test_public_surface.py` pins the exact set, the defining module, the parameter kinds and the
signature equal to the engine's, and the delegation per argument; `tests/typed_consumer/` imports
and calls it from `log_foundry`, and `rejects.py` holds a positional `deliver` that `mypy` refuses.

## History

### 2026-11-08

- **Refined (the 1.2.0 follow-up fix, no spec), at Andrew's direction.** `emit_via_deliver` is
  public at `log_foundry` and `log_foundry.sinks.verdict`, keyword-first, because the `Sink`
  docstring already pointed third-party authors at it while it lived only in private `_engine`;
  `__all__` goes from 30 names to 35 in 1.2.0.

### 2026-11-07

- **Refined** by SPEC-072: `sinks/verdict` joins `sinks/base` as a public sink module exempt from a README table row, on the same `__all__`-derived rule.

### 2026-09-08

- **Settled.**
