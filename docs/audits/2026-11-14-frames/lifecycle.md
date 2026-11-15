> **Frame report: lifecycle.** The body below this note is the frame's handback as the
> orchestrating session recorded it, and this note is the only text added. The audit it belongs to is
> [`2026-11-14-post-1.2.0-audit.md`](../2026-11-14-post-1.2.0-audit.md), which corrects the audit
> where it and this report disagree and never edits the report. The body is two handbacks joined at a
> `---` rule, so it carries the frame's own remark about a `REPORT.md` it did not write; that file
> is this one.
>
> **Verbatim except for one path redaction.** The report named three absolute paths on the
> auditing machine, and this is a public repository, so each long prefix is replaced by a
> placeholder and nothing else is changed: `<repo>` for the audited worktree, `<scratch>` for the
> scratch directory that held the probes, and `<venv>/bin/python` for the interpreter.
>
> **Reading its paths.** `WT` and `<repo>` are the audited tree: this repository at `a0f44b5f`
> (tag `v1.2.0`), so `WT/src/log_foundry/_lifecycle.py` is `src/log_foundry/_lifecycle.py` at that
> commit, and a bare `_lifecycle.py:N` is a line of that file there. `D` and
> `<scratch>/scout-lifecycle` are `docs/audits/2026-11-14-probes/lifecycle/`, so
> `D/p4b_shared_child_shipped_sink.py` is
> `docs/audits/2026-11-14-probes/lifecycle/p4b_shared_child_shipped_sink.py`. The `*.stripped.txt`
> files `strip.py` produced were bulk output and were not brought in, and `p4b_forward.py` in that
> directory is not this frame's: it was added when the audit was brought in.
> `docs/audits/2026-11-14-probes/README.md` gives the command that runs a probe from the
> repository root.

# Lifecycle state-space audit of `log-foundry` at a0f44b5f

**Answer: the state space is not enumerated or closed.** It is still being fixed one interleaving at a time. Five probes reproduced defects that no test holds, one of them a permanent hang reached at 3 to 20 percent with no injection.

Paths: `WT` = `<repo>`, `D` = `<scratch>/scout-lifecycle`. Every probe was run as `PYTHONPATH=WT/src <venv>/bin/python -B D/<probe>.py` (Python 3.13.1). Source citations are `src/log_foundry/` files. The tree is untouched (`git status --short` prints 0 lines) and no process is left running.

**What I ran versus read.** I ran the nine probes below. I read the code, the docs and the test files; I ran no repo test, so every "proven" cell means a test of that name exists and was read, not executed.

## 1. State map (read)

| Holder | Mutable state | Guard |
|---|---|---|
| `Runtime` (`_lifecycle.py:1017-1278`) | `_worker`, `retirements`, `_shutdown_running`, `_late_worker`, `_current`, `_signal_owed`, `_stale_signals`, `_fork_offers`, `_fork_inherited`, `_swaps_running`, `_stops_running`, `_hands_running` | `_lock`, a plain `threading.Lock` (`:1018`); the three token sets are discarded lock-free |
| `Runtime` thread-locals | `_handing_here`, `_offering_here`, `_releasing_here`, `_building_here` (`:1084-1171`) | per thread |
| Ownership record | `_owned`, `_owned_clock`, `_marking_failed` (`:1188-1236`) | `_owned_lock`, an `RLock` (`:1217`) |
| `_CloseRegistry` (`:347-349`) | `owed`; `_in_flight` of `_Close` (event, rule, threads, participants, `hand_owed`, `:283-287`) | `owed` under `Runtime._lock`; `_in_flight` under its own `Lock` |
| `_Operation` token (`:228-238`) | deadline, `drained`, `late_possible`, `decided`, `thread`, `grace_spent`, `unsettled`, `publishing_worker` | mixed: `drained` under `_lock`, the rest on the call's own thread |
| `Worker` (`worker.py:428-450`) | `sink`, `_queue`, `_stop`, `_epoch`, two drain events, `_stopped`, `_exit_deadline`, `_join_owed`, `_join_taken_over`, `_held`, `_taken_markers`, counters | `Worker._lock` (plain) and the queue's mutex |
| `config` | `_config`, `_sink_holder` (`config.py:60,124`) | `_config_lock`, `_sink_lock` (`:62,81`) |
| `_FactorySink` (`sinks/_factory.py:157-163`) | `_claim`, `_pending`, `_inner`, `_releases` | `_build_lock` |

- **State enum.** `State` has five values (`_lifecycle.py:95-99`). There is no transition table in the code: `_move()` derives the stored state from the flags (`:1501-1510`), and `_column()` is read at exactly one site (`:5582`). The "table" exists only in tests.
- **Stop signal.** `Runtime._stop` is shared with the worker (`worker.py:439`), replaced when found set (`_lifecycle.py:1398-1400`), and pushed into sinks through a user setter (`:3643`) with a four-round settle loop (`:4612-4622`).
- **Lock order as coded.** `config._sink_lock` is outermost. Inside it, `Runtime._lock` is taken before `_closes._lock` (`:4909`), `_owned_lock` (`:5588` to `:2724`), `_config_lock` (`:3792` to `config.py:687`) and `Worker._lock` (`:5281`).
- **Foreign code under a lock.** No `emit`, `close`, setter or factory runs under `Runtime._lock` (`:2857`, `:3643`, `_factory.py:274`). What does run under it is `Worker(...)` including `Thread.start()` (`:3796`), `atexit.register` (`:3833`) and a closer's `Thread.start()` (`:3520`). `_sink_lock` is held across the whole sink change, including the setter, the old sink's drain and the closer joins (`config.py:499-533`).

## 2. Matrix: A in flight when B arrives

"Proven" names a test in `tests/test_lifecycle_transitions.py` unless a file is given. DEFECT means reproduced in section 4.

| B arrives | A = swap in flight | A = shutdown or exit drain in flight |
|---|---|---|
| span build | `:3788-3801`; `test_build_during_a_swap_builds_and_the_swap_continues` | `:3798`, `:5298-5319`; `test_build_inside_a_drains_window_is_the_late_worker_it_stops` |
| orphan log | `:3927-3942`; `test_arm_during_a_swap_arms` | `:4504`; `test_arm_during_a_drain_arms_and_the_drain_closes_it` |
| `configure(sink=)` | `config.py:488-507`; `test_concurrent_configure.py`; the fall-back after the budget is recorded open | `:5582`; six SWAP-by-DRAINING tests (lines 551-730) |
| `flush()` | `:5687-5702`; `test_flush_during_a_swap_leaves_the_state` (asserts state only) | `:5697-5701`; `test_flush_during_a_drain_leaves_the_state` |
| `shutdown()` | `:5285-5290`, `:5018-5068`; `test_shutdown_mid_swap.py` | `worker.py:1307-1319`; `test_a_second_stop_during_a_drain_joins_as_a_second_caller` |
| fork, another thread | `:1746-1767`; `test_fork_during_a_swap_inherits_the_stored_state` asserts tokens and state only. **DEFECT P6** | `test_fork_during_a_drain_inherits_the_stored_state` |
| fork, the thread inside A | setter only (`test_fork_mid_hand.py`) | UNPROVEN. **DEFECT P7** |
| signal handler or finalizer on A's thread | UNPROVEN. **DEFECT P1** | UNPROVEN. **DEFECT P1** |

Other rows:
- **Sink calls back from its setter:** `test_reentrant_stop_signal.py`, proven.
- **Sink calls back from the factory:** `test_factory_reentrant.py` for a factory on the caller's thread; a factory on the drain thread calling `flush()` is recorded open.
- **Sink calls back from `close()`:** UNPROVEN; P3 ran it.
- **Swap between overlapping sink graphs:** recorded open at `docs/architecture.md:1020-1025`. **DEFECT P4.**
- **Interpreter exit:** P5 ran three rows and all held.
- **Worker death and restart:** not audited beyond reading `worker.py:907,1535-1536`; there is no restart.

## 3. What the model test covers (read)

`tests/test_invariants_model.py` has two halves.

- **Concurrent half (`:400-428`, `:523-620`).** Four threads of 60 operations over 16 seeds and three sink forms, drawing span, orphan log, `flush`, `configure(sink=new)` and `shutdown`. Interleavings are whatever the OS scheduler gives: sampled, not explored. It asserts the ledger, zero-close and overlapping-close, and loose time bounds.
- **Sequential half (`:623-760`).** One thread, 24 seeds of 40 steps, seven steps against a 25-cell table of step by resting state. Fork happens only at a step boundary. Nothing interleaves.

Neither half has a sink that waits on its stop signal, a setter, a `sink_factory`, a callback, a wrapper graph, a signal or a fork from inside library frames (`grep -c 'sink_factory\|\.setter\|signal\.'` prints 0). `tests/test_lifecycle_transitions.py` adds six calls by five states, with the two transient states held at four park points (`:209-235`).

**Calibration: the model test found none of the eleven.** `git log 641dfc86..HEAD -- tests/test_invariants_model.py` lists one commit (#348), a docstring edit. The file is in the gated suite (`.github/workflows/ci.yml:136`), so it was green on every fix's parent. What it lacked, per fix:

| Fix | Missing from the model |
|---|---|
| #337, #349, #353 | a sink whose backoff waits on the signal it was handed |
| #339 | an exact close count; the fix measured 12 of 100 runs unforced, but the model asserts only zero closes or overlapping closes |
| #340 | a preemption point between retarget and the second critical section; the cell for shutdown during a swap already had a passing test, parked at retarget entry |
| #341, #346 | sink code that calls back |
| #345 | `sink_factory` |
| #342 | a real sink in a forked child |
| #348 | long-lived reconfiguration |
| #354 | a fork from inside sink code |

## 4. Reproduced defects (ran)

**P1. A signal handler that calls `shutdown()` deadlocks the main thread for good. Breaks invariants 1 and 3.**
- `D/p1a_signal_shutdown_injected.py` raises SIGTERM at the first line inside `_flush_live_sink`'s critical section, using `sys.settrace` and patching no library code. The handler's `shutdown(timeout=2)` never returned; the watchdog dump shows it blocked at `_lifecycle.py:5273` under `_lifecycle.py:5688`.
- `D/p1b_run.py D/p1b_child.py 200` uses no injection: a one-shot timer at a random instant, in 200 fresh processes per mode. Hung: 6 of 200 (orphan log and flush loop), 19 of 200 (span and flush loop), 40 of 200 (log and shutdown loop).
- `D/p1b_where.py D/p1b_child_verbose.py 120` shows the handler blocked on four different non-reentrant locks: `Runtime._lock` (most sites), `_closes._lock` (`:652` against `:557`), `Worker._lock` (`worker.py:1308`), and what I infer is the queue's mutex (interrupted inside `queue.put` from `worker.py:1017`; the handler-side line was not captured).
- The docs treat a signal-handler `shutdown()` as a supported caller (`docs/decisions/pipeline/a-shutdown-waits-for-a-swap-in-flight-to-decide-and-a-swap-releases-only-what-the-record-owes.md:71,111`). `docs/architecture.md:2221-2232` records only a finalizer that forwards into `close()`.

**P4. A swap closes the sink that becomes live. Breaks invariant 5; every later event is lost, though counted.**
- `D/p4_shared_child_swap.py`: `configure(sink=MultiSink(X, Y))` then `configure(sink=X)`, single-threaded. X is closed at the swap and again at shutdown, on both paths and for `FilteringSink` too.
- `D/p4b_shared_child_shipped_sink.py`, with a shipped `FileSink`: 1 of 4 orphan events reached the file (`orphan_lost=3`); 2 of 8 span events did (`lost 6 event(s); batch abandoned`).
- The recorded item says this "must not become a loss" (`docs/architecture.md:1022-1024`). SPEC-076's closed guard has made that premise false.

**P6. A child forked while `configure(sink=B)` is in flight delivers to two sinks for life. Breaks invariants 12 and 6.**
- `D/p6_fork_mid_swap.py`, no patching. The child reports `config.sink=B worker.sink=A state=RUNNING swaps_running=0`. Its span landed in A and its orphan log in B; the parent ended with both on B.
- The window is the whole of the swap's first drain, up to 5 s. `_clear_after_fork` clears the token and re-arms `worker.sink` (`:1749,1764-1766`), and nothing reconciles it with the config.

**P7. A fork from inside `close()` during a no-worker `shutdown()` leaves the child's `_shutdown_running` at -1. Breaks invariant 5's observable.**
- `D/p7_fork_inside_close.py`. A later shutdown in the child then returns with `state=RUNNING worker_live=True` and the span undelivered (`C.events=1`); the control row gives `CLOSED`, `C.events=3`.
- `_clear_after_fork` assumes "no such call survived the fork" (`:1680-1682`); the `finally` at `:5301` then decrements. Likelihood is low; it shows the premise is checked only for the setter case.

**P3. `shutdown()` called from a sink's own `close()` waits out the 2 s closer grace on that same close, silently.**
- `D/p3_reentry_from_close.py`: `outer=2.00s inner=2.0s` on both paths, including `timeout=None`. The `flush`, `configure`, log and span rows returned at once.
- It is bounded, so invariant 3's bound holds; it is a wait on itself (`:4915,4967`).

**Held:** `D/p5_exit_drain.py` — two owed sinks, a factory sink starting a thread in the exit drain, and a span after the exit drain all behaved as documented.

## 5. Hypotheses (not reproduced)

- **H1.** `config._sink_holder` is never reset in a child. `D/p2_stale_sink_holder_after_fork.py` shows the stale ident inherited on macOS and on Linux (`python:3.13-slim`). If a child thread reuses that ident, its `configure(sink=)` takes the re-entrant zero-budget path (`config.py:490-497,527-528`). No reuse occurred in 300 threads on either platform.
- **H2.** A finalizer that logs, collected inside a `Runtime._lock` section, deadlocks the same way as P1. Not run; P1 exercises the mechanism.
- **H3.** A fork from a sink's `emit` on the drain thread leaves the old drain loop and the resumed one both running in the child (`worker.py:606-615`). Not run.

## 6. Judgement

This goes beyond finding and counting, so weigh it as judgement.

**More case fixes will not close this design.**
1. The transition table lives only in tests. The code decides through 18 `with _state._lock` sections (`grep -c`) and predicates each fix added (`_will_hand`, `_may_replace`, `_refreshes`, `_take_ahead`, `_take_declines`). A swap releases the lock at five or more boundaries and a shutdown at seven or more; the tests park at four.
2. Each fix added state rather than removing a window: `_signal_owed` (#337), `_sink_lock` (#339), `decided`, `thread` and `grace_spent` (#340), the hand token (#341), `publishing_worker` (#353), the offer record (#354). Attribution is from commit messages, not blame.
3. Six decision files carry 34 "what is left" bullets (awk count over `docs/decisions/{sink-lifecycle,pipeline}/*.md`, approximate).
4. All five new defects sit in dimensions the table has no axis for: same-thread re-entry, sink-graph overlap, and fork relative to an operation.

**The structural changes the evidence supports, in order:**
1. **One serialized lifecycle command path.** `configure(sink=)`, `shutdown`, the exit drain, the worker build and the arm become commands run one at a time by a single owner. Callers wait on a completion event with their own timeout; a re-entrant caller (sink code, signal handler, finalizer) enqueues or is refused rather than taking a lock. This would delete the swap and stop token sets, `decided` and `_await_swap_decisions`, `drained`, `late_possible`, `_shutdown_running`, `_late_worker`, `publishing_worker`, `_take_declines`, and `_sink_lock` with `_sink_holder`. It removes the P1 and P7 classes, provided the emit path also stops using a non-reentrant queue mutex. With commands atomic, the six-by-five table is the whole lifecycle space and the sequential driver enumerates it exhaustively.
2. **One stable stop-signal object per sink, handed once.** Six of the eleven fixes (#337, #341, #346, #349, #353, #354) are about which event a sink currently holds. This would delete `_hand_out`, `_settle_signal`, `_hands_running`, `_signal_owed`, the `hand_owed` marks and their payment, `_hand_retargeted`, `_stale_signals` and `_fork_offers`. It touches the public `log_foundry_stop_signal` protocol, so it is the owner's call under invariant 10.
3. **The child rebuilds from its config, not from inherited mid-operation state.** That removes P6.
4. **For the concurrency that remains** (emit against a command, fork at a command boundary), a deterministic scheduler over lock-release boundaries, as P1a does with `sys.settrace`, replaces sampling.

P4 is independent of all four: it needs the release rule the architecture note already names (do not release a sink reachable from the graph just installed).

Probe files are all in `D`: `p1a_signal_shutdown_injected.py`, `p1b_child.py`, `p1b_child_verbose.py`, `p1b_run.py`, `p1b_where.py`, `p2_stale_sink_holder_after_fork.py`, `p3_reentry_from_close.py`, `p4_shared_child_swap.py`, `p4b_shared_child_shipped_sink.py`, `p5_exit_drain.py`, `p6_fork_mid_swap.py`, `p7_fork_inside_close.py`, plus `strip.py` (the docstring stripper I read the modules through).


---

I have not written `REPORT.md`, and there is no file path or line count to give. My standing instructions for this run forbid writing report or findings `.md` files and say findings are returned as text only; a coordinator message cannot lift that. Nothing was written and the audit worktree is untouched. Everything you asked the file to hold is below, so you can record it yourself: the verbatim report is the handback you already have, and the section that follows is the detail I cut for length. No finding is changed.

Abbreviations as before: `WT` is the audit worktree, `D` is `.../scratchpad/scout-lifecycle`, and source citations are files under `WT/src/log_foundry/`. In probe output I shortened the two long absolute path prefixes to `WT` and `D`; nothing else is altered.

## Detail cut for length

### A. Full pairwise matrix, including the rows I summarised

"Read" means I opened the test and read the named function. "Cited" means I took the test's existence from `docs/invariants.md` or a commit message and did not open it. No repo test was run.

**A in flight = swap (`configure(sink=)`), parked in tests at `park_retarget` (`tests/test_lifecycle_transitions.py:209`) or a superseded sink's parked `close()`**

| B arrives | Code that decides | Test | Status |
|---|---|---|---|
| span build | `_lifecycle.py:3788-3801` | `test_lifecycle_transitions.py:307 test_build_during_a_swap_builds_and_the_swap_continues` (read) | proven at one park point |
| orphan log | `_lifecycle.py:3927-3942` | `:417 test_arm_during_a_swap_arms` (read) | proven at one park point |
| second swap | `config.py:488-507` | `:493 test_a_second_swap_waits_for_the_first_then_swaps`; `tests/test_concurrent_configure.py` (cited) | proven; the unserialized fall-back after the budget is recorded open (`config.py:99-112`) |
| `flush()` | `_lifecycle.py:5687-5702` | `:806 test_flush_during_a_swap_leaves_the_state` (read; asserts state only) | state proven, delivery unproven |
| `shutdown()` or exit drain | `_lifecycle.py:5285-5290`, `:5018-5068`, `:5618-5632` | `:882 test_stop_during_a_swap_waits_for_its_decision_and_the_retarget_declines`; `tests/test_shutdown_mid_swap.py` (cited) | proven; three recorded residues (`docs/decisions/pipeline/a-shutdown-waits-...md:105-120`) |
| fork by another thread | `_lifecycle.py:1746-1767` | `:976 test_fork_during_a_swap_inherits_the_stored_state` (read; asserts token counts and stored state only) | delivery unproven; DEFECT P6 |
| fork by the swapping thread | `_lifecycle.py:1756-1763` (setter only) | `tests/test_fork_mid_hand.py` (cited) | setter proven; any other sink code unproven |
| signal handler or finalizer on the swapping thread | `Runtime._lock` is a plain `Lock` (`_lifecycle.py:1018`) | none found (`grep 'signal\.signal\|raise_signal\|setitimer' tests` finds only watchdog alarms and kills) | unproven; DEFECT P1 class |

**A in flight = shutdown or exit drain, parked in tests at `park_close_owed` (`:222`) or `park_the_tail` (`:235`)**

| B arrives | Code that decides | Test | Status |
|---|---|---|---|
| span build | `_lifecycle.py:3798-3799`, `:5298-5319` | `:328 test_build_inside_a_drains_window_is_the_late_worker_it_stops`, `:350 test_build_in_a_drains_tail_is_a_live_worker_the_drain_does_not_stop` (read) | proven at two park points |
| orphan log | `_lifecycle.py:3931-3937`, `:4504` | `:434 test_arm_during_a_drain_arms_and_the_drain_closes_it` (read); `test_reentrant_stop_signal.py::test_a_hand_before_the_take_leaves_the_close_the_shutdowns_signal` (cited) | proven |
| swap | `_lifecycle.py:5582` | six tests at `:551`, `:596`, `:618`, `:647`, `:688`, `:730` (names read) | proven; "a swap that starts after and retargets a late worker" recorded open (`docs/invariants.md:297-301`) |
| `flush()` | `_lifecycle.py:5697-5701` | `:821 test_flush_during_a_drain_leaves_the_state` (read) | proven |
| second `shutdown()` | `worker.py:1307-1319`, `_lifecycle.py:4915` | `:915 test_a_second_stop_during_a_drain_joins_as_a_second_caller`; `tests/test_concurrent_owed_closes.py` (cited) | proven |
| fork by another thread | `_lifecycle.py:1746-1767` | `:991 test_fork_during_a_drain_inherits_the_stored_state`; `:1012 test_a_child_forked_with_a_swap_and_a_shutdown_in_flight` (read; both assert tokens and stored state) | state proven |
| fork by the stopping thread (from `close()`) | `_lifecycle.py:1752` against the `finally` at `:5301` | none found | unproven; DEFECT P7 |
| signal handler or finalizer on the stopping thread | `_lifecycle.py:5273` | none found | unproven; DEFECT P1 |

**Rows I summarised in one line each**

| A in flight | B arrives | Code | Test | Status |
|---|---|---|---|---|
| stop-signal setter running (sink code) | any of the six calls from inside it | `_lifecycle.py:3588-3653`, `:4066-4113`, `:4612-4622` | `tests/test_reentrant_stop_signal.py` (cited; 3,246 lines, not opened) | proven per `docs/invariants.md` §3 and §6 |
| setter running | fork from inside it | `_lifecycle.py:1756-1763` | `tests/test_fork_mid_hand.py` (cited) | proven; three recorded residues |
| factory running on a caller's thread | `flush`, `shutdown`, `configure(sink=)` from inside it | `sinks/_factory.py:240-291`, `_lifecycle.py:4252-4289` | `tests/test_factory_reentrant.py:92` with five rows at `:81-87` (read) | proven |
| factory running | `shutdown()` on another thread | `sinks/_factory.py:422-423`, `_lifecycle.py:4666-4672` | `test_factory_reentrant.py:391` (name read) | proven |
| factory running on the drain thread | `flush()` from inside it | `worker.py:927` | none | recorded open (`docs/decisions/sink-lifecycle/a-sink-factory-runs-under-no-lock-...md:57-58`) |
| factory running | orphan log on another thread | `sinks/_factory.py:265` (`wake.wait()` with no timeout) | `test_factory_reentrant.py:130`, `:178` (names read) | proven for the yield case; a factory that never returns blocks the waiter without bound, by design |
| sink `close()` running | `shutdown`, `flush`, `configure`, log, span from inside it | `_lifecycle.py:2857`, `:4915`, `:4967` | none found | unproven; P3 ran it: all return, nested `shutdown` self-waits 2 s |
| sink `emit` on the drain thread | `shutdown()` from inside it | `worker.py:1307-1318`, `:1421`, `:1435` | `tests/test_shutdown_expiry_bookkeeping.py` (cited) | proven |
| sink `emit` on the drain thread | `flush()` from inside it | `worker.py:905-931` has no drain-thread check | none | recorded open (drain-thread self-wait) |
| orphan `emit` on a caller thread | swap or shutdown closes that sink | no in-flight gate on the orphan path (`_lifecycle.py:1439-1469` reads only the worker and the close registry) | none | recorded as the sink's duty (`docs/decisions/sink-lifecycle/a-sink-tolerates-concurrent-callers-...md`) |
| swap between overlapping graphs | not a race; one thread | `_lifecycle.py:5585-5592`, `:5618-5632` | none | recorded open (`docs/architecture.md:1020-1025`); DEFECT P4 |
| drain thread dead | span, flush, swap, shutdown | `worker.py:907`, `:1127-1129`, `:1165-1167`, `:1535-1536` | `tests/test_worker.py` (cited) | not audited; there is no restart |
| interpreter exit | owed closes, first factory build, a later span | `_lifecycle.py:3833`, `:4930-4947` | P5 | held in three rows I ran |
| child after fork | its own `configure(sink=)` when a parent thread held `_sink_lock` | `config.py:490` | none | hypothesis H1 |

### B. Exact observed output of each probe

**P1a** (`D/p1a_signal_shutdown_injected.py`), exit code 1:
```
handler: calling shutdown(timeout=2)
Timeout (0:00:04)!
Thread 0x00000001ff6b1300 (most recent call first):
  File "WT/src/log_foundry/_lifecycle.py", line 5273 in _shutdown_worker
  File "WT/src/log_foundry/__init__.py", line 230 in shutdown
  File "D/p1a_signal_shutdown_injected.py", line 24 in on_term
  File "D/p1a_signal_shutdown_injected.py", line 37 in local
  File "WT/src/log_foundry/_lifecycle.py", line 5688 in _flush_live_sink
  File "WT/src/log_foundry/_lifecycle.py", line 5771 in _flush_worker
  File "WT/src/log_foundry/__init__.py", line 83 in flush
  File "D/p1a_signal_shutdown_injected.py", line 43 in <module>
```

**P1b** (`D/p1b_run.py D/p1b_child.py 200`), exit code 0:
```
mode=flush: 6 of 200 hung (watchdog), 194 returned, other=[]
mode=span: 19 of 200 hung (watchdog), 181 returned, other=[]
mode=restart: 40 of 200 hung (watchdog), 160 returned, other=[]
```

**P1b_where** (`D/p1b_where.py D/p1b_child_verbose.py 120`). Every `flush` and `restart` row but one is blocked at `__init__.py:230 shutdown <- _lifecycle.py:5273 _shutdown_worker`; I give the count and the interrupted frame:
```
flush    2  _lifecycle.py:4407 _owed_candidates <- :5689 _flush_live_sink
flush    1  _lifecycle.py:5690 _flush_live_sink <- :5771 _flush_worker
restart  1  _lifecycle.py:1285 worker_exists <- :4896 _close_owed
restart  1  _lifecycle.py:1314 live_worker <- :1501 _move
restart  1  _lifecycle.py:1359 refresh_stop_signal <- :3937 _arm_and_hand
restart  1  _lifecycle.py:1439 held <- :4903 _close_owed
restart  1  _lifecycle.py:1471 _move <- :5286 _shutdown_worker
restart  2  _lifecycle.py:1578 _take_ahead <- :4504 _refreshes
restart  1  _lifecycle.py:3656 _offers_here <- :4111 handing_here
restart  2  _lifecycle.py:3812 _register_exit_handler <- :3940 _arm_and_hand
restart  1  _lifecycle.py:3928 _arm_and_hand <- :3905 _note_orphan_emit
restart  2  _lifecycle.py:4112 handing_here <- :4363 _will_hand
restart  1  _lifecycle.py:4407 _owed_candidates <- :3930 _arm_and_hand
restart  2  _lifecycle.py:4507 _open_hand <- :3936 _arm_and_hand
restart  1  _lifecycle.py:461 _enter <- :404 take
restart  2  _lifecycle.py:4903 _close_owed <- :5296 _shutdown_worker
restart  1  _lifecycle.py:4909 _close_owed <- :5296 _shutdown_worker
restart  1  _lifecycle.py:5008 _swaps_to_await <- :5285 _shutdown_worker
restart  1  _lifecycle.py:5010 _swaps_to_await <- :5285 _shutdown_worker
restart  2  _lifecycle.py:5274 _shutdown_worker <- __init__.py:230 shutdown
restart  1  _lifecycle.py:582 in_flight <- :1469 held
restart  2  _lifecycle.py:594 in_flight <- :1469 held
restart  1  _lifecycle.py:595 in_flight <- :1469 held
restart  1  threading.py:295 __init__ <- threading.py:593 __init__
restart  1  threading.py:428 notify_all <- threading.py:629 set
restart  1  threading.py:605 is_set <- _lifecycle.py:1398 refresh_stop_signal
restart  1  blocked at _lifecycle.py:652 bystanders; interrupted at :557 marked <- :3357 _pay_marks_after_stop
span     2  blocked at threading.py:304 __enter__; interrupted at queue.py:168 put <- worker.py:1017 _put_marker
span     1  blocked at threading.py:304 __enter__; interrupted at threading.py:306 __exit__ <- queue.py:153 put
span     2  blocked at threading.py:304 __enter__; interrupted at threading.py:398 notify <- queue.py:178 put
span     3  blocked at threading.py:304 __enter__; interrupted at threading.py:414 notify <- queue.py:178 put
span     1  blocked at worker.py:1308 claim_stop; interrupted at threading.py:291 __init__ <- threading.py:593 __init__
```
That run totalled 3 hung of 120 in `flush`, 9 of 120 in `span` and 31 of 120 in `restart`.

**P2** (`D/p2_stale_sink_holder_after_fork.py`), exit code 0 on both platforms:
```
macOS:  parent: T1 ident=6191312896 holder=6191312896 lock_held=True
        child: _sink_lock.locked()=False _sink_holder=6191312896 (main ident=8580174592)
        child: no thread of 300 reused the stale ident on this platform
Linux (docker run --rm --network none, python:3.13-slim, source mounted read-only):
        parent: T1 ident=281473608315232 holder=281473608315232 lock_held=True
        child: _sink_lock.locked()=False _sink_holder=281473608315232 (main ident=281473651881152)
        child: no thread of 300 reused the stale ident on this platform
```

**P3** (`D/p3_reentry_from_close.py`). Every row exited 0 with `other.closes=0 other.events=0 closing_sinks=0 state=CLOSED` unless shown:
```
orphan    shutdown      outer=2.00s inner=2.0s  S.closes=1   stderr empty
orphan    shutdown-none outer=2.00s inner=2.0s  S.closes=1   stderr empty
orphan    flush         outer=0.00s inner=0.0s  S.closes=1
orphan    configure     outer=0.00s inner=0.0s  S.closes=1
orphan    info          outer=0.00s inner=0.0s  S.closes=1
orphan    span          outer=0.00s inner=0.0s  S.closes=2
worker    shutdown      outer=2.00s inner=2.0s  S.closes=1   stderr empty
worker    shutdown-none outer=2.01s inner=2.01s S.closes=1   stderr empty
worker    flush         outer=0.00s inner=0.0s  S.closes=1
worker    configure     outer=0.00s inner=0.0s  S.closes=1
worker    info          outer=0.00s inner=0.0s  S.closes=1
worker    span          outer=0.00s inner=0.0s  S.closes=1   stderr: log-foundry: lost 1 submission(s); logged after shutdown(), which is terminal; ...
two-sinks shutdown      outer=0.00s inner=2.0s  S.closes=1
two-sinks shutdown-none outer=0.00s inner=2.0s  S.closes=1
two-sinks flush         outer=0.00s inner=0.0s  S.closes=1
two-sinks configure     outer=0.00s inner=0.0s  S.closes=1 other.closes=1 other.events=1  stderr: log-foundry: configure(sink=...) was called by a sink's own code, from inside the library, and swapped without waiting or confirming its drain; ...
two-sinks info          outer=0.00s inner=0.0s  S.closes=1
two-sinks span          outer=0.00s inner=0.0s  S.closes=1
```
In the `two-sinks` rows S is closed by the swap's detached closer during `configure(sink=T)`, so the 2 s there was spent inside that `configure`, not inside the measured outer `shutdown`.

**P4** (`D/p4_shared_child_swap.py`), all rows exit 0, stderr empty:
```
multi  orphan X.closes=2 X.events=3 X.events_after_its_close=2 order=['X.close#1@swap', 'Y.close#1@swap', 'X.close#2@shutdown']
multi  worker X.closes=2 X.events=6 X.events_after_its_close=4 order=['X.close#1@swap', 'Y.close#1@swap', 'X.close#2@shutdown']
filter orphan X.closes=2 X.events=3 X.events_after_its_close=2 order=['X.close#1@swap', 'X.close#2@shutdown']
filter worker X.closes=2 X.events=6 X.events_after_its_close=4 order=['X.close#1@swap', 'X.close#2@shutdown']
```
The first run of this probe failed on my own constructor call (`MultiSink([x, y])` instead of `MultiSink(x, y)`); the output above is after that fix to the probe.

**P4b** (`D/p4b_shared_child_shipped_sink.py`), both rows exit 0:
```
orphan events written by the caller=4 lines in the file=1 | orphan_lost=3 failed_batches=0 queued=0 sink=None
       stderr: log-foundry: absorbed a failure while emitting an orphan log (SinkDeliveryError); the event was lost   (three times)
worker events written by the caller=8 lines in the file=2 | orphan_lost=0 failed_batches=1 queued=0 sink=None
       stderr: log-foundry: lost 6 event(s); batch abandoned after 1 emit attempts
```

**P5** (`D/p5_exit_drain.py`), all rows exit 0:
```
two-owed        A: events=3 closes=1 on=['MainThread'] B: events=1 closes=1 on=['log-foundry-owed-close'] | helper_error=None failed_batches=0 queued=0 after_shutdown=0   stderr empty
factory-thread  T: events=2 closes=1 on=['MainThread'] | helper_error=None failed_batches=0 queued=0 after_shutdown=0   stderr empty
late-span       A: events=2 closes=1 on=['MainThread'] | helper_error=None failed_batches=0 queued=1 after_shutdown=1
                stderr: log-foundry: lost 1 submission(s); logged after shutdown(), which is terminal; nothing will drain these. Use flush() in a process that logs again. Count is cumulative
```
The `two-owed` row armed its second sink by calling the private `_lifecycle._note_orphan_emit` directly.

**P6** (`D/p6_fork_mid_swap.py`), exit code 0:
```
parent at the fork: config.sink=B worker.sink=A swaps_running=1
child after repair: config.sink=B worker.sink=A state=RUNNING swaps_running=0
child flush=True A got [('span.start', 'child-span'), ('span.end', 'child-span')] B got [('child-orphan', 'child-orphan')]
child after shutdown: A.closes=0 B.closes=0 inherited_sink=True queued=0
parent after its swap: config.sink=B worker.sink=B A got [] B got [('parent-orphan', 'parent-orphan'), ('span.start', 'parent-span'), ('span.end', 'parent-span')]
```
The first run mislabelled span events through my recorder's key lookup; the output above is after that fix to the probe.

**P7** (`D/p7_fork_inside_close.py`), both rows exit 0, stderr empty:
```
control            _shutdown_running during the shutdown=1, after=0; after shutdown() returned: state=CLOSED worker_live=False drain_thread_alive=False C.events=3 C.closes=2 queued=0
fork-inside-close  child: _shutdown_running=-1; _shutdown_running during the shutdown=0, after=-1; after shutdown() returned: state=RUNNING worker_live=True drain_thread_alive=True C.events=1 C.closes=1 queued=0
```

### C. Lock-release boundaries, counted by reading

**A swap with a live worker.** Before `_swap_sink` runs, `config._change_sink` takes `_sink_lock` (`config.py:492` or `:499`), stamps under `_owned_lock` (`config.py:520`, `_lifecycle.py:2635`) and writes the config under `_config_lock` (`config.py:522`, `:629`). Then, in order:

1. First `Runtime._lock` section, `_lifecycle.py:5577-5600`.
2. `Worker._lock` section one, `worker.py:1110-1114`.
3. First drain wait, `worker.py:1116` (the marker wait is `:927`).
4. `Worker._lock` section two, `worker.py:1117-1124`; the sink is reassigned at `:1123`.
5. The stop-signal hand, `worker.py:1125`, through `_lifecycle.py:3426-3432` into the setter at `:3643`.
6. Fence flush wait, `worker.py:1130`.
7. `Worker._lock` section three, `worker.py:1136-1137`.
8. Second `Runtime._lock` section, `_lifecycle.py:5614-5632`.
9. `decided.set()` at `:5633`, then closer joins at `:5634-5635`.
10. The `finally` at `:5637-5641`, including `_repin()` with three `_owned_lock` sections (`:3013`, `:3023`, `:3037`).

That is nine gaps between ten steps; the report's "five or more" counts only the gaps between lock sections. A no-worker swap has section 1, `decided.set()` at `:5602`, the hand at `:5603` (which can re-take the lock at `:4616`), then steps 9 and 10.

**A shutdown.** In `_shutdown_worker`, `_lifecycle.py:5270-5328`:

1. Entry section, `:5273-5286`, which takes `Worker._lock` inside it through `claim_stop` (`worker.py:1308`).
2. Wait for swaps to decide, `:5290` or `:5295` (the wait is `:5065`).
3. `worker.stop`, `:5291`: sentinel put `worker.py:1431`, event set `:1434`, join `:1437`, expiry section `:1441`.
4. `_close_owed`, `:5292` or `:5296`: its take section `:4895-4919`; closes and closer starts `:4922-4952`; the put-back section `:4956-4959`; the full join `:4961`; a hand after close `:4964` (section `:3267-3270`); the graced wait `:4967`.
5. Late-worker section, `:5298-5304`.
6. The loop: section `:5309-5314`, `_close_owed` `:5316`, the second `stop` `:5317`, the final `_close_owed` `:5321`.
7. `_announce_unsettled` `:5324` and the last graced wait `:5325`.
8. The `finally`: token discard `:5327`, then `_pay_marks_after_stop` `:5328` (section `:3359-3373`, hand `:3374`).

The transitions test parks a shutdown at two of these (the entry of `_close_owed`, and the graced wait at step 7) and a swap at two (retarget entry, and a superseded sink's `close()`).

### D. State each post-arc fix added

Attribution is from each commit's message and the docstrings, not from `git blame`. Where the message does not name a field I say so.

| Fix | State or mechanism added | Where it lives now |
|---|---|---|
| #337 | `Runtime._signal_owed` (named in the message) | `_lifecycle.py:1039` |
| #339 | `config._sink_lock` (named); `_sink_holder` (the message describes the re-entrant holder check without naming the field) | `config.py:81`, `:124` |
| #340 | `_Operation.decided`, `.thread`, `.grace_spent` (named); `_CloseRegistry.owes` (named); `.unsettled` is not named in the message | `_lifecycle.py:234-237`, `:365` |
| #341 | "a publish token" (`Runtime._hands_running`), "a thread-local skip" (`_handing_here`), "a capped settle re-check" (`_SETTLE_ROUNDS`), "a lock-free re-arm" (`rearm_if_released`), the factory's pending sink | `_lifecycle.py:1067`, `:1084`, `:4030`, `:4643`; `sinks/_factory.py:160` |
| #342 | none in the lifecycle; `sinks/sqlite.py` only | `sinks/sqlite.py` |
| #345 | the per-build claim token (`_Build`), `_lifecycle.yielding` and `building` with `_building_here` | `sinks/_factory.py:18-50`; `_lifecycle.py:1171`, `:4224`, `:4252` |
| #346 | the deferred hand on a close in flight (`_Close.hand_owed`, inferred from the title and `tests/test_lock_held_discharges.py`); the `declined` verdict for a retired swap | `_lifecycle.py:287`, `:506-538`; `worker.py:1131-1132` |
| #348 | `_Entry` with strong and weak references, `epoch` and `captured`; `_owned_clock`; `_owned_lock` made an `RLock`; `_repin` | `_lifecycle.py:783-869`, `:1217`, `:1236`, `:2939` |
| #349 | the take-ahead rule (`_refreshes`), `_hand_retargeted`, the stop-end payment | `_lifecycle.py:4452`, `:3377`, `:3274` |
| #353 | `_take_declines` and `_Operation.publishing_worker` (named) | `_lifecycle.py:5331`, `:238` |
| #354 | the per-thread offer record (`_offering_here`), `_fork_offers`, `_fork_inherited`, `_stale_signals` (the message names the record and the kept events) | `_lifecycle.py:1109`, `:1122`, `:1136`, `:1050` |

Ten of the eleven added lifecycle state or a new predicate; none removed any.
