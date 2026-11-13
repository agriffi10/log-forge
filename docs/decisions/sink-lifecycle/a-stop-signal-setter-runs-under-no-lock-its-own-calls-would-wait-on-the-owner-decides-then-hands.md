---
type: Decision
title: "A stop-signal setter runs under no lock its own calls would wait on: the owner decides, then hands"
description: "the library never runs a sink's `log_foundry_stop_signal` setter, or the `hasattr` probe before it, under a lock a call from inside the setter would wait on: the owner decides under `_state._lock`, opens a hand token before the section publishes an arm, a refreshed `_stop` or a cleared debt, hands after releasing, and re-checks lock-free that the sink holds the current signal. One section publishes with no token: a worker build that, during a stop, defers its hand to a close of its sink in flight, since `_will_hand` refuses that sink to every thread a token would redirect. The factory wrapper hands what it built under no lock. Never hand inside a lifecycle critical section, never publish before the token but there, never open a token for a hand that published nothing. (fix after SPEC-079)"
---

**A setter is the sink's own code, and the library ran it under its own locks.** The orphan log,
the post-fork signal debt, `flush()`, the no-worker swap and the worker build offered the signal
while holding `_lifecycle._state._lock`; a worker built by a `flush()` sweep did it under
`decorator._sweep_lock`; the factory wrapper did it under its build lock. All three are
non-reentrant, so a setter calling `configure()`, `info()`, `flush()` or `shutdown()`, or running a
`@trace`d function, on the same thread hung the process. Measured at `624d817f` in a forked child
with a bounded wait (`tests/test_reentrant_stop_signal.py`'s table): of twenty calls across the
paths, fifteen hung. `Worker.retarget` returned, because it already handed outside every lock but
`config._sink_lock`, and a re-entrant `configure()` never waits on that. A log from a setter
during the no-worker swap returned too, because that swap armed the sink before offering, so the
log took the lock-free fast path. And a `configure()` from the setter of a sink built by
`sink_factory=` returned with that sink written to and closed by nobody. The `hasattr`
probe in `offer_stop_signal` runs a property getter or `__getattr__`, so it is sink code too and
leaves the lock with the assignment.

**Each path decides under the lock and hands after it.** `_will_hand` and `_owed_candidates`
choose the sinks, the section refreshes `_stop` (since 2026-11-13, not while a stop has a take
ahead) and arms as before, and `_hand_out` runs with no lifecycle lock held. The no-worker swap's hand and `Worker.retarget`'s still run inside
`config._sink_lock`, so a setter's `configure()` there is re-entrant: it tries that lock once,
swaps unconfirmed and announces it, which the public-api decision on concurrent sink changes
records. Arming stays in the same section as before, so the swap's stale releases, the
SPEC-078 take-ahead decline and the worker build's arm and late-worker registration keep their
atomicity, and no pinned lock count moves (`tests/test_runtime_state.py`).

**A hand token closes the window that deciding first opens.** The atomic offer meant no other
thread could reach a sink between its arm and its signal. Now the section opens a token in
`Runtime._hands_running` before it publishes, and the orphan fast path reads the token after the
fields it already reads, so a thread that sees the arm or the new `_stop` sees the token too, takes
the locked branch and hands the sink its signal itself before emitting. `flush()` does the same for
every owed sink while a token is open (invariant 6). **A section that publishes nothing opens no
token**: a token per hand made every log that met one hand again under a token of its own, and
`tests/test_reentrant_stop_signal.py`'s concurrency case measured 2399 setter calls for 2400 logs
that way, against 8 or 9 with tokens opened only on a publication.

**One section publishes with no token: the worker build that defers its hand.** During a stop, a
build whose sink has a close in flight marks that close instead of handing, and the close's
discharge pays the hand (`docs/decisions/sink-lifecycle/a-hand-the-worker-build-declines-for-a-close-in-flight-is-paid-when-that-close-finishes.md`).
That section still publishes the worker, the arm and a refreshed `_stop`, and opens no token,
because every thread a token would send to the locked branch would meet `_will_hand`'s refusal of
a sink whose close is in flight and hand it nothing there either. The cost is under *What is left*.

**Two hands to one sink are settled by a lock-free re-check.** One thread decides `E`, a shutdown
sets `E`, another refreshes to `F` and hands it, and the first thread's late hand would leave the
sink holding the set `E` while the fast path, seeing `F` unset, never refreshes it. So after each
offer `_settle_signal` reads `_state._stop` without the lock and hands again only if it moved,
whichever thread wrote last reading after its own write. It asks the same moment test the
decision asks (`_may_replace`), never a bare `in_flight`: bailing out whenever a worker drained
into the sink left a late hand's set event on a sink a fresh worker had just handed the new one,
and a 0.5 s backoff then waited not at all. The rounds stop at `_SETTLE_ROUNDS`, which
only a setter that moves the signal on every call can reach, and the remainder becomes a signal
debt the next log or `flush()` settles, rather than a loop.

**The rule: a settle only ever hands the current signal, and never creates one.** It hands
`_state._stop` as it is, set or not, and never refreshes, so it cannot publish an event the rest
of the runtime has not seen. A round that refreshed did exactly that inside a shutdown: between
`_shutdown_worker` setting the signal and its close taking the sink, no moment predicate can see
the close coming, and a late hand's refresh gave the sink a fresh unset event it then held into
its close — measured, `shutdown(timeout=2)` took 8.00 s against the sink's 8 s backoff, against
0.00 s with no late hand. A set current signal is what the atomic offer left, and the next log or
`flush()` refreshes it, since the fast path declines on a set signal. So a late hand never
delivers an unset event into a shutdown's close (SPEC-044 FR-003): every event a hand delivers is
the one a shutdown sets.

> **Refined 2026-11-13** (fix after SPEC-079): the deciding sections did refresh between a stop
> setting the signal and its take, so the log or `flush()` above handed an unset event into that
> take's close. They now hand the current signal while a take is ahead, and refresh only after it;
> one case remains, a late worker's build publishing its own unset signal. See
> `docs/decisions/sink-lifecycle/a-hand-while-a-stop-has-a-take-ahead-hands-the-current-signal-and-never-refreshes-it.md`.

**The rest of the shape.**

- A thread-local mark (`handing_here`) keeps a setter that logs to its own sink, or builds the
  worker on it, from being handed again inside itself; it is the one test every hand decision asks.
  Since SPEC-083 it is also true for a sink on the thread's offer record (`offer_stop_signal`
  records each offer). That covers `Worker.retarget`'s hand, which runs outside `handing()` and
  whose setter, when it logged, was handed again inside itself. In a forked child, every thread
  also sees the offers the forking thread is still inside (`Runtime._fork_offers`), so no other
  thread of the child hands that sink again. The stop-end payment alone asks the `handing` mark
  (`_in_handing`). A child forked inside a setter keeps both marks (*A child
  forked inside a stop-signal setter keeps its mark…*).
- A sink a live worker drains into is handed the worker's own event when that is the current,
  unset signal, which replaces nothing and lets a log on another thread hand it while the build's
  hand is still running.
- The signal debt is cleared in the deciding section, after the token, so a setter that forks
  leaves the child's debt, set by its own handler, alone.
- A setter that swaps the sink or shuts down can close the sink its log then writes to; the log
  re-arms it on a lock-free miss (invariant 5).
- The sweep builds its worker outside `_sweep_lock`, still before any detach.
- The factory wrapper builds and records the sink as pending (since 2026-11-12 with no lock held
  across the factory call: *A sink factory runs under no lock…*), hands it outside the
  lock, publishes it only after its hand, and re-reads its own signal after publishing; any
  thread that finds the built sink pending hands it the signal itself before using it; its
  `close()` releases a pending sink, and after its hand it re-arms itself if something closed it
  meanwhile, but only when the library owed it a close as the emit began: a wrapper held inside a
  caller's `MultiSink` is closed through that wrapper, and arming it too closed the built sink
  twice. That also closes a leak older than this fix: a `configure(sink=...)` on another thread
  while the factory was still running closed the wrapper with nothing built, and the sink built a
  moment later was written to and closed by nobody.

**Rejected: handing the no-worker swap's sink before its section.** Measured by planting it: the
setter's re-entrant `configure(sink=B)` then ran a whole swap first, and the outer section found B
owed and released it as stale under a config naming B, so the next log wrote to a closed B and B
ended closed twice. Avoiding that needs a "superseded?" re-check, the guard shape
`concurrent-sink-changes-are-serialized-by-one-lock-whose-wait-is-spent-from-the-swap-budget`
rejects. Handed after the section, the re-entrant call sees the state of the two calls made in
sequence.

**Rejected: arming in a second section after the hand.** It closes the cross-thread window too, but
a first orphan log then takes the lock twice, which the pinned lock counts refuse; the token does it
in the one section the call already has.

**Rejected: an `RLock`.** A re-entrant lock would let the setter's call run inside the outer
section, midway through state that section has not finished deciding; the closed-guard decision in
this area refuses an `RLock` for a sink's transport lock on the same ground.

**Rejected for the factory: holding the hand under the build lock.** A drain thread blocked on that
lock, plus a setter calling `shutdown()` or `flush()` with no timeout, joins forever. Publishing the
sink before its hand instead lets another thread's run start with no signal; refusing the setter's
own emit loses its event.

**What is left, and why it stands.**

- **The drain thread before the build's hand.** Another thread can submit to the newly published
  worker and the drain thread can emit a batch before the sink holds its signal. Engine sinks are
  unaffected, since the drain passes the worker's event to the engine explicitly, so only a 1.x
  sink with a backoff of its own waits once uninterruptibly. `Worker.retarget` ships with the same
  window. Starting the thread after the hand would publish a worker whose `Thread.start()` can
  still fail.
- **A `shutdown()` whose close starts before a pending hand lands.** Its close runs against a sink
  that does not hold the signal yet, so a backoff the sink began before the hand landed is not cut
  short. One begun after it is: the hand delivers the very event the shutdown set. Since the
  no-worker swap publishes its decision before its hand, a `shutdown()` on another thread reaches
  this window on purpose rather than waiting out the setter
  (`docs/decisions/pipeline/a-shutdown-waits-for-a-swap-in-flight-to-decide-and-a-swap-releases-only-what-the-record-owes.md`).
- **An orphan log between a deferred hand's discharge and its payment.** The discharge deletes the
  close's entry before the payment's section reads the moment, and the deferred build opened no
  token, so in that interval an orphan log on another thread takes the fast path (the sink is
  owed, the signal is the late worker's and unset) and emits with the sink still on the
  shutdown's set signal: any backoff in that one emit is not a wait.
- **Concurrent setters.** A sink's setter can run on two threads at once, as duplicate hands of one
  signal; a sink tolerates concurrent callers already (SPEC-028), and the README says so.
- **The factory wrapper's re-read cap.** A setter on the built sink that changes the wrapper's
  signal on every hand can leave the built sink one signal behind after `_SETTLE_ROUNDS`; the next
  signal handed to the wrapper is forwarded to it as usual.

## History

### 2026-11-13

- **Refined** by SPEC-083: the mark also reads the offer record, so a setter that
  `Worker.retarget` runs is not handed its signal again inside itself. At `53b68203` such a setter
  that logged ran twice, at depth two. In a forked child, the mark also reads the offers the
  forking thread inherited, from every thread. The stop-end payment keeps the `handing` mark
  alone.

### 2026-11-12

- **Refined**: the fence now names the one section that publishes with no token, a worker build
  that during a stop defers its hand to a close of its sink in flight, and why; the window it
  leaves is under *What is left*, beside the no-worker swap's hand, which a cross-thread
  `shutdown()` now meets by design. To fit the fence's length cap, its clause that any thread
  finding the factory's built sink pending hands it before use moved to the body's factory bullet,
  unchanged in meaning.
- **Refined** the same day: the factory wrapper no longer holds its build lock across the factory
  call (*A sink factory runs under no lock, and a build that calls back releases its waiters*).
- **Settled**, as a fix after SPEC-079.
