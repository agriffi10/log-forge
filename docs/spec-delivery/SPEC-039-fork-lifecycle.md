# Completed Spec — SPEC-039: Fork Lifecycle

## What was completed?

`os.fork()` was unhandled anywhere in the tree. A forked child inherited a worker whose drain
thread does not exist (measured: six events never delivered, `health()` clean on every term of the
documented alert idiom) and locks held by threads that do not exist (measured: **19 of 60 children
hung permanently** in `info()`, on the application's own thread). Both are closed, in the child
only.

- **`src/log_foundry/_fork.py`** — new module, the whole mechanism. Registers exactly one
  `os.register_at_fork(after_in_child=…)` handler, guarded for a platform without it. Its order of
  work is the contract: re-initialise every lock and event the library owns → discard inherited
  buffered writes → run registered handlers. `register_child_handler` is an **inverted registry**
  (`decorator` registers the worker rebuild rather than `_fork` reaching for it), which is what
  keeps the module free of any intra-package import but `_diag`.
- **`Worker._reinit_after_fork(resume=)`** — rebuilds **in place** with a replaced queue object and
  zeroed counters, so guards keyed on `_worker.sink is X` survive. A retired parent forks a retired
  child. `stopped_reason` is *not* set to `"Forked"` — SPEC-019 defines that field as "the drain
  thread died", and this child's is running.
- **`discard_buffered_after_fork()`** — a new optional, probed-by-name member of the sink
  protocol, documented in `sinks/base.py` beside `losses()` and implemented by `FileSink` and
  `RotatingFileSink` (`dup2` to `/dev/null`, then reopen in **append** mode).
- **Three lints**, all derived rather than hand-listed: every `Lock`/`RLock`/`Event` in `src/` must
  be assigned where the walk can write it back; no module may build a primitive type the walk
  cannot replace; and every sink that opens a buffered stream of its own must implement the
  discard hook (scoped on the roster `test_sink_concurrency` already derives, imported rather than
  re-derived).

Deviations, each recorded in place: FR-002 AC-2 was **amended during Phase 1** to require the queue
*object* be replaced rather than drained (a `queue.Queue` builds its own mutex, which no lock rule
over this package can see), including on the retired path. FR-005's boundary turned out to have
five occupants rather than the three the spec named — the two extra are a third-party sink's own
locks and hook, and a mixed-base class whose foreign attributes are replaced with its own.

## What changed from earlier specs?

- `sinks/base.py` — the `Sink` protocol gains a **fifth** optional member. No shipped or
  third-party sink stops satisfying it; the member is probed, never required.
- `sinks/file.py` — both file sinks gain the hook. No change to `emit`, `close` or construction.
- `worker.py` / `decorator.py` — a rebuild path and its guards, all new; the three new worker
  questions in `decorator.py` are classified in SPEC-035 FR-002's roster.
- `architecture.md` §9 states the fork behaviour, §13 records the five things beyond the boundary,
  the shared-sink hazard, and the `_thread`-install invariant. `README.md`'s sink conventions gain
  a **Forking** bullet: build a connection-holding sink only in the worker process, never
  before the fork.

## Verification

`pytest -q` 1631 passed / 2 skipped / 2 xfailed, `ruff check .`, `mypy --strict`, `spec-lint`, and
full CI on 3.12 and 3.13. Every new statement was mutation-swept scoped to its own function — 20
mutants in the last implementation phase alone, all killed by their intended test.

Twelve adversarial review rounds across the four phases. **Every blocking finding after the first was
a defect in the previous round's fix**, and the sharpest was evidence rather than behaviour:
`open(path, "a")` → `"w"` passed all 1626 tests, because every test built a file that was *empty on
disk* at fork time and asserted that emptiness as its own precondition — so truncate and append
were the same program, and a prefork child would have destroyed the shared log on every fork. The
documentation phase repeated the lesson in its own register: the remedy it first gave for a shared
connection (reconfigure in the child) was measured *breaking the parent's connection*, because
both swap paths close what they replace. Recorded in §13 and carried as a named finding on
SPEC-040 (FR-005 AC-3), which cannot fix it — that spec forbids behaviour change — but is where
the shape it belongs to is being addressed.
