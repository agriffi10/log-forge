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

## Fences

- **Logs-only, send everything for now** — no metrics or OTel-native traces. Sampling is deferred and **unbuilt** — no `should_send` exists in code — and the per-span flush makes the pipeline span-outcome-ready, *not* tail-sampling-ready. (arch §10, §13)
- **An extra's floor is a published contract — moved deliberately, never by a bot** — `versioning-strategy: increase-if-necessary` stays, so floors move only when a human decides they should. A floor raise is a contract change. (No spec — it shipped alongside SPEC-022 in `v0.9.0`.)
- **A public accessor hands out a copy; the library reads the live object** — a public getter documented "do not mutate" is a promise the caller's slip breaks silently; `_live_config()`/`_live_baggage()` are the per-event reads. (SPEC-034)
- **A result that can grow a reason must stop being a `bool` before 1.0, not after** — a `NamedTuple` cannot be retrofitted — a non-empty tuple is always truthy, so every `if flush():` would silently keep passing. `FlushResult`/`ContinueResult` grow by new reason values only. (SPEC-034)
- **A protocol that is exported is a protocol that will be inherited** — `Sink`'s members are `@abstractmethod`: empty bodies let a subclass with one typo instantiate happily and return `None` from `emit`, losing events with every counter at zero. (SPEC-034)
- **A frozen surface is keyword-first, and says what it will not grow** — every public dataclass is `kw_only`, `defaults=` takes a `Mapping` (`dict` is invariant), `context.__all__` names only the six re-exported, and the worker tunables stay **unreachable** from `configure()`. Only a typed consumer probe sees any of it — the gate stops at `src`. (SPEC-051)

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

**A frozen surface is keyword-first, and says what it will not grow** — every public dataclass is `kw_only=True`, so field **order** is not part of the frozen contract: `Health` reached twelve fields by appending nine, each append safe only because nothing outside the library had bound to a position, and after the tag that stops being true on its own. **Two consequences reach outside the library and are the price, not an oversight:** `SinkLosses` is the one public type a third-party sink must construct, so a `0.x` sink building it positionally now raises inside `losses()` — and since `read_losses` swallows a raising accessor by design, that sink's loss reporting degrades to `None`, which the composites read as "reports nothing" rather than "no loss". And `kw_only` empties `__match_args__`, so a positional `case Health(a, b):` stops matching while a keyword pattern still does. Both are asserted in the suite rather than only documented. `configure(defaults=)` and `trace(defaults=)` take `Mapping[str, object]`, since `dict` is invariant and a caller's `dict[str, str]` was refused — the SPEC-034 FR-004 fix applied to the two parameters it missed; `trace` copies at **decoration**, because it used to bind the caller's object to every span and read it live, and an arbitrary `Mapping`'s `keys()` is user code on the per-event path. `context.__all__` names only the six the package re-exports: `current_span` hands back a mutable `Span` and could not have been withdrawn after `1.0`. A name a public signature uses is exported — `GroupIdSource`, `DedupIdSource`, `Backend`, and `flush_sink`/`DEFAULT_SWAP_TIMEOUT` beside the siblings they belong with. **Do NOT build** `configure(batch_size=…, flush_interval=…, max_queue=…, max_retries=…)`: the worker's tunables stay unreachable, deferred deliberately on 2026-09-02 because keyword-only parameters are fully additive after `1.0` and the semantics are unsettled — the worker is built lazily at the first span, so a call before that would apply and a call after would silently not, contradicting `configure()`'s own "repeated calls compose rather than reset". None of it is checkable from inside `src`, where `mypy`'s `files` stops, which is why a consumer probe runs under `mypy --strict` in the suite. (SPEC-051)


