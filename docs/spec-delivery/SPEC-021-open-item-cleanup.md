# Completed Spec — SPEC-021: Open-Item Cleanup

## What was completed?

Four robustness specs left eighteen "Notes for the next spec" and `architecture.md` §12 had carried
three open items since before the first line of code. A reader could not tell a live defect from a
settled decision, and two notes were outright false. Everything is now fixed, settled, or recorded
as a constraint — and the one real wart among them is gone.

- **`flush()` reports delivery** (FR-001). It returned `True` whenever the drain it forced *ran*,
  including when the emit failed every retry — a false success in the serverless path SPEC-013 built
  it for, where the return value is the caller's only evidence. `_FlushMarker` now stamps
  `failed_batches` at call time and is answered by comparing it after the drain, so the question is
  *"was anything abandoned while this call was outstanding"* — which covers the batch the marker
  forces **and** any batch another flush or a batching trigger lost while it waited. `_emit` no
  longer returns an outcome: the counter already moves exactly once per abandoned batch.
- **The terminal-failure line accounts for the queue** (FR-002) — "N event-list(s) held and M queued
  item(s) undelivered". `qsize()` is read in its own guard, after `stopped_reason` is recorded.
- **The integer ceiling counts the minus sign** (FR-003), via an unbound `int.__lt__` — see the
  deviation below.
- **Every note reconciled** (FR-004): struck through with the spec that closed it, moved to Key
  Decisions, or written into `architecture.md` §13. §12 now carries no open items.

**Deliberate deviations.** (1) `_FlushMarker.delivered` defaults to `False`, not the spec Data
Model's `True`: every answering path assigns it explicitly, so the default is read only when the
drain thread died without computing an answer. (2) The sign test uses an unbound `int.__lt__`
rather than `value < 0`, which on an `int` subclass dispatches to user code that can raise — and a
raise there replaces the whole enclosing mapping, the collateral loss `key()` exists to prevent.
(3) `_emit`'s retry count is floored at zero, closing a negative-`max_retries` path that discarded a
batch with no attempt, no counter and nothing on stderr.

## What changed from earlier specs?

- **`flush()`'s `True` is narrower and now means what it claimed.** Code that treats `True` as
  "logging is fine" will start seeing `False` where a sink is failing — which is the point. It
  answers for its own window: a batch abandoned *before* the call belongs to
  `health().failed_batches`. Reporting a past loss and preventing a past loss from sticking are the
  same property in opposite directions, and FR-001's "an empty drain is a successful one" chooses.
  Two concurrent flushes may therefore disagree if one is called after the abandonment.
- **SPEC-019's stderr line changed wording**, so anything grepping it needs updating.
- **SPEC-020's ceiling is one byte stricter for negatives**, including a negative integer sitting
  exactly on the interpreter's own digit limit.

## Verification

Local: 568 tests pass (14 new), `ruff` clean, `mypy --strict` clean over 48 source files,
`spec-lint` clean. CI green on 3.12 and 3.13 across PRs
[#69](https://github.com/agriffi10/log-forge/pull/69),
[#70](https://github.com/agriffi10/log-forge/pull/70) and
[#71](https://github.com/agriffi10/log-forge/pull/71). Two fresh-context reviews ran three rounds
between them and changed the FR-001 design twice: the first found the false success surviving in
concurrent flushes, the second found the fix both too weak (a later success erased an earlier loss)
and too strong (a past loss stuck forever). They also caught the `int.__lt__` dispatch, a stranded
waiter when the drain thread dies, a vacuous stderr assertion, a GIL-race in a test, and one
untested `False` path. Everything measurable was verified empirically by the reviewers — integer
sweeps against `len(str(n))` on both signs across the interpreter bound, and 300-trial concurrency
probes — not by reading.
