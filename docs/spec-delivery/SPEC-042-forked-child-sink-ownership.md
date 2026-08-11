# Completed Spec — SPEC-042: Forked-Child Sink Ownership

## What was completed?

- **A process releases only a transport it acquired here.** `_lifecycle` gained the ownership
  record — `stamp` (at `configure(sink=…)` and `_ensure_sink()`'s lazy default, over the whole
  reachable sink graph, write-once per object), `releasable`, `reclaim`, `_mark_inherited` — and
  `release`, the one path by which the library ever closes a sink. Eight closers route through
  it: three lifecycle sites and the five shipped wrapper sinks.
- **`reacquire_after_fork()`**, renamed from `discard_buffered_after_fork()` with its contract
  restated: strand what you inherited *and*, by returning, claim the transport as this process's
  own. `_fork` publishes which hooks returned; `_lifecycle` re-stamps those. The old name never
  reached a stable release; SPEC-039's spec and delivery doc keep it, annotated in place.
- **`Health.inherited_sink`** — a state, not an alert term. Answerable with no worker.
- **`_fork._SKIP_ATTRIBUTE`** — a general opt-out a module declares for state that holds objects
  but is not live state to repair. `_lifecycle._owned` and `_fork.reacquired_in_child` use it.

**One deviation, two clarifications and a choice the spec left open** — the distinction matters,
because only the first changes what a criterion asked for:

1. *The deviation.* **FR-001's "no record means refused" became a wrapper-inheritance rule**
   (escalated to the maintainer and approved before implementing). FR-001's Description and Data
   Model still state the original rule; they are left as written and annotated in the spec,
   since what shipped is recorded here and in `architecture.md` §9.
   Every lifecycle path stamps, so the flat default only ever fired where a *user* closed a
   wrapper the library never saw — silently no-op'ing `FilteringSink(inner).close()`, a
   documented public API. An unrecorded sink now inherits the answer from the wrapper releasing
   it: neither recorded → the caller's, so close; a recorded wrapper may not release an
   unrecorded member, which keeps FR-001 AC-6.
2. *Clarification.* **FR-002 AC-1's discriminator needs a defines-or-inherits-`emit` clause.** The roster it
   reuses triggers on `emit`/`send_all`/`close`, so it contains `SocketTransport`; without the
   clause the lint claims two socket closes and the roster is ten, not eight.
3. *Clarification.* **FR-001 AC-12's lock order is three terms**, not two:
   `_worker_lock` → `_config_lock` → `_owned_lock`, because `_get_worker` holds the first across
   `_ensure_sink()`. AC-12's version is true and skips the middle term.
4. *A choice AC-11 left open, settled with the measurement it asked for.* Not a deviation — the
   criterion says the bound "is chosen with the measurement in hand rather than guessed". The
   number was larger than it anticipated: 1,109 ms naive on a 100k-event `MemorySink` against the
   ~202 ms cited, 279 ms with an exact-type builtin pre-filter, 2 ms with the container descent
   bounded to one level. `_mark_inherited` measures separately: 0.0 / 0.3 / 117 ms.

## What changed from earlier specs?

- **SPEC-039's hook is renamed** (`discard_buffered_after_fork` → `reacquire_after_fork`) and
  `_fork._reacquire_transports` now returns the roster of successes. Its AC-5 lint is unchanged
  in scope under the new name, floor intact.
- **SPEC-030's `close_detached` is now `release(..., detached=True)`**; both bounded joins are
  unchanged and now tested behaviourally rather than by AST alone.
- **`Health` gained a tenth field** — a plain append, which is what SPEC-034 FR-008's dataclass
  conversion bought.
- **`config.py` imports `_lifecycle`** and stamps before `_rebind` publishes the sink. The
  `_config_lock` whitelist and `_lifecycle`'s import invariant were both widened deliberately.
- **SPEC-035's predicate roster** gained two rows for `_delivering_to_an_inherited_sink`
  (existence).

## Verification

`ruff`, `mypy --strict`, `sh scripts/spec-lint.sh` and 1697 tests green locally and in CI across
four PRs (#158–#161); no existing test's assertions changed except three invariants widened on
purpose. Every fix was mutation-tested, which mattered: **five separate assertions were caught
passing against the defect they named**, including two that survived the entire suite.

Eight review rounds across the four PRs found 2, 0, 4, 4, 0, 1, 0 and 1 blocking defects. The
one worth recording: after
phase 2 first landed, *unrecorded* was a **claimable** state rather than a terminal one —
`setdefault` defends only a record that already exists, so a child could `configure()` its way
into genuine ownership of a sink the parent never recorded and close the parent's transport
legitimately. Measured on a real socket. `_mark_inherited` exists to make unrecorded unclaimable.

**One route remains and is recorded in §13**: a parent that holds a connection sink only in
application state and never hands it to the library, whose child's `post_fork` is the first
process to `configure()` it. Undecidable inside FR-001's rule. A 17-shape claiming matrix refuses
everywhere else, and eight destructive-close routes against a goodbye-writing socket sink
produced no goodbye at all.
