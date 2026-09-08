# Public API surface — decisions

The settled decisions about what is public, what it promises, and what it will not grow. Read the
fences; pull an entry only when you need the reasoning.

## Contents

- [Fences](#fences)
- [Logs-only, send everything for now](#logs-only-send-everything-for-now)
- [An extra's floor is a published contract — moved deliberately, never by a bot](#an-extras-floor-is-a-published-contract--moved-deliberately-never-by-a-bot)
- [A public accessor hands out a copy; the library reads the live object](#a-public-accessor-hands-out-a-copy-the-library-reads-the-live-object)
- [A result that can grow a reason must stop being a `bool` before 1.0, not after](#a-result-that-can-grow-a-reason-must-stop-being-a-bool-before-10-not-after)
- [A protocol that is exported is a protocol that will be inherited](#a-protocol-that-is-exported-is-a-protocol-that-will-be-inherited)
- [A frozen surface is keyword-first, and says what it will not grow](#a-frozen-surface-is-keyword-first-and-says-what-it-will-not-grow)
- [The frozen surface is the import path, not just the class](#the-frozen-surface-is-the-import-path-not-just-the-class)

## Fences

- **Logs-only, send everything for now** — no metrics or OTel-native traces. Sampling is deferred and **unbuilt** — no `should_send` exists in code — and the per-span flush makes the pipeline span-outcome-ready, *not* tail-sampling-ready. (arch §10, §13)
- **An extra's floor is a published contract — moved deliberately, never by a bot** — `versioning-strategy: increase-if-necessary` stays, so floors move only when a human decides they should. A floor raise is a contract change. (No spec — it shipped alongside SPEC-022 in `v0.9.0`.)
- **A public accessor hands out a copy; the library reads the live object** — a public getter documented "do not mutate" is a promise the caller's slip breaks silently; `_live_config()`/`_live_baggage()` are the per-event reads. (SPEC-034)
- **A result that can grow a reason must stop being a `bool` before 1.0, not after** — a `NamedTuple` cannot be retrofitted — a non-empty tuple is always truthy, so every `if flush():` would silently keep passing. `FlushResult`/`ContinueResult` grow by new reason values only. (SPEC-034)
- **A protocol that is exported is a protocol that will be inherited** — `Sink`'s members are `@abstractmethod`: empty bodies let a subclass with one typo instantiate happily and return `None` from `emit`, losing events with every counter at zero. (SPEC-034)
- **A frozen surface is keyword-first, and says what it will not grow** — every public dataclass is `kw_only`, `defaults=` takes a `Mapping` (`dict` is invariant), `context.__all__` names only the six re-exported, and the worker tunables stay **unreachable** from `configure()`. Only a typed consumer probe sees any of it — the gate stops at `src`. (SPEC-051)
- **The frozen surface is the import path, not just the class** — no concrete sink is in `__all__`, so a class frozen at a path free to move was pinned to nothing; `1.0.0` itself deleted `sinks/util.py` and moved three classes. The documented dotted path, and the public names the signatures use, are inside the `1.x` promise. (No spec — settled on the README's 1.0.0 pass, `48299e3`.)

---

### Logs-only, send everything for now

**Logs-only, send everything for now** — no metrics/OTel-native traces; sampling is deferred and **unbuilt** — no `should_send` exists in code, and the per-span flush makes the pipeline span-outcome-ready, *not* tail-sampling-ready. (arch §10, §13)


### An extra's floor is a published contract — moved deliberately, never by a bot

Dependabot's first `pip` PR raised `boto3`/`sentry-sdk`/`pika` past floors that already admitted the new release. Those raises were **kept** (staying near-current on boto3 is worth the narrowing) but `versioning-strategy: increase-if-necessary` stays, so the floors now move only when a human decides they should. A floor raise is a contract change: it cuts a release **minor**, not patch. (`v0.9.0`)


### A public accessor hands out a copy; the library reads the live object

**A public accessor hands out a copy; the library reads the live object** — `get_config()` and `get_baggage()` copy, because a public getter documented "do not mutate" is a promise the caller's slip breaks silently, while `config._live_config()` and `context._live_baggage()` are the per-event reads, since `build_event` runs one to three config reads and one baggage read **per event** and a copy there allocates per event. Both copies are **one level**: deep-copying arbitrary caller objects inside an accessor that must never raise trades a narrow sharing bound for a wide new failure, so the bound is stated and pinned rather than closed. Freezing `Config` also turned every write into a read-modify-write, and one writer (`_ensure_sink`) runs on the orphan logging path — measured, one concurrent `info()` permanently reverted `configure()` in 268 of 2000 trials — so `_config_lock` serializes the writers while reads stay lock-free. (SPEC-034 FR-003, FR-005)


### A result that can grow a reason must stop being a `bool` before 1.0, not after

**A result that can grow a reason must stop being a `bool` before 1.0, not after** — `flush()` answered five outcomes with one bit and `continue_trace()` two. A `NamedTuple` cannot be retrofitted (a non-empty tuple is always truthy, so every `if flush():` would silently keep passing), so `FlushResult`/`ContinueResult` carry `__bool__` plus a `reason`, and grow by new reason values only. `Worker.flush` carries the type too: the five outcomes are distinguishable only there. For the same reason `Health` and `SinkLosses` became frozen dataclasses — six specs had each argued their appended field left the indices undisturbed, and with a dataclass there are no indices. (SPEC-034 FR-007, FR-008)


### A protocol that is exported is a protocol that will be inherited

**A protocol that is exported is a protocol that will be inherited** — `Sink`'s members were empty-bodied and not `@abstractmethod`, so a subclass with one typo instantiated happily and its inherited `emit` returned `None`: three events gone, `flush()` truthy, every counter zero. `mypy` refused it and only the runtime did not. Structural satisfaction is untouched, which matters because no shipped sink inherits it. (SPEC-034 FR-005)


### A frozen surface is keyword-first, and says what it will not grow

**A frozen surface is keyword-first, and says what it will not grow** — every public dataclass is `kw_only=True`, so field **order** is not part of the frozen contract: `Health` reached twelve fields by appending nine, each append safe only because nothing outside the library had bound to a position, and after the tag that stops being true on its own. **Two consequences reach outside the library and are the price, not an oversight:** `SinkLosses` is the one public type a third-party sink must construct, so a `0.x` sink building it positionally now raises inside `losses()` — and since `read_losses` swallows a raising accessor by design, that sink's loss reporting degrades to `None`, which the composites read as "reports nothing" rather than "no loss". And `kw_only` empties `__match_args__`, so a positional `case Health(a, b):` does not merely stop matching — the `match` statement raises `TypeError` against a `Health` subject, falls through against any other, and matches on a keyword pattern; those three are asserted by `test_an_empty_match_args_raises_for_its_own_type_and_falls_through_for_others`, which is the copy to trust. Both are asserted in the suite rather than only documented. `configure(defaults=)` and `trace(defaults=)` take `Mapping[str, object]`, since `dict` is invariant and a caller's `dict[str, str]` was refused — the SPEC-034 FR-004 fix applied to the two parameters it missed; `trace` copies at **decoration**, because it used to bind the caller's object to every span and read it live, and an arbitrary `Mapping`'s `keys()` is user code on the per-event path. `context.__all__` names only the six the package re-exports: `current_span` hands back a mutable `Span` and could not have been withdrawn after `1.0`. A name a public signature uses is exported — `GroupIdSource`, `DedupIdSource`, `Backend`, and `flush_sink`/`DEFAULT_SWAP_TIMEOUT` beside the siblings they belong with. **Do NOT build** `configure(batch_size=…, flush_interval=…, max_queue=…, max_retries=…)`: the worker's tunables stay unreachable, deferred deliberately on 2026-09-02 because keyword-only parameters are fully additive after `1.0` and the semantics are unsettled — the worker is built lazily at the first span, so a call before that would apply and a call after would silently not, contradicting `configure()`'s own "repeated calls compose rather than reset". None of it is checkable from inside `src`, where `mypy`'s `files` stops, which is why a consumer probe runs under `mypy --strict` in the suite. (SPEC-051)

### The frozen surface is the import path, not just the class

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
(`GroupIdSource`, `DedupIdSource`, `Backend`), which the keyword-first fence above already treats
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
already-documented module. `sinks/base` is the one exempt module, and
the exemption is derived from its own `__all__` rather than a listed set — a listed one stops
describing the module the moment a name is added, which is what a hand-written three-name version
of it did while `base.__all__` already had five.
