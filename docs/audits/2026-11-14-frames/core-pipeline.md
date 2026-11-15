> **Frame report: core pipeline.** The body below this note is the frame's handback as the
> orchestrating session recorded it, and this note is the only text added. The audit it belongs to is
> [`2026-11-14-post-1.2.0-audit.md`](../2026-11-14-post-1.2.0-audit.md), which corrects the audit
> where it and this report disagree and never edits the report.
>
> **Verbatim except for one path redaction.** The report named three absolute paths on the
> auditing machine, and this is a public repository, so each long prefix is replaced by a
> placeholder and nothing else is changed: `<repo>` for the audited worktree, `<scratch>` for the
> scratch directory that held the probes, and `<venv>/bin/python` for the interpreter.
>
> **Reading its paths.** `SRC` is `src/log_foundry/` and `DOC` is the repository root, both in the
> audited tree: this repository at `a0f44b5f` (tag `v1.2.0`). So `SRC/worker.py:669-682` is
> `src/log_foundry/worker.py` lines 669 to 682 at that commit, `DOC/README.md:1374` is `README.md`
> there, and `<repo>` and `<wt-audit>` are the same tree. `SC` and `<scratch>/scout-core` are
> `docs/audits/2026-11-14-probes/core-pipeline/`, so `SC/p09_flush_barrier.py` is
> `docs/audits/2026-11-14-probes/core-pipeline/p09_flush_barrier.py`. The probes' output files
> (`p04_out.txt`, `*.jsonl`, `*.ready`) were not brought in; the probes rewrite them beside
> themselves when run. `docs/audits/2026-11-14-probes/README.md` gives the command that runs a
> probe from the repository root.

## Core pipeline loss audit (a0f44b5f): the accounting identity fails in four places and `flush()` is not a durability barrier

Roots: `SRC` = `<repo>/src/log_foundry`, `DOC` = the same worktree's top level, and `SC` = `<scratch>/scout-core`.

Every probe was run as `cd SC && PYTHONPATH=<wt-audit>/src <venv>/bin/python -B <script>`. No git command that changes state was run, nothing in the audit tree was edited, and no process was left running.

**Not read:** the bodies of `sanitize.py` and `context.py`, the close registry, and most `_lifecycle.py` docstrings. Stage 2 below is therefore from `model.py` and invariant 8 only. The rankings in the avoidable-loss section are judgement, not measurement.

### 1. Stages (read)

1. **Span open.** `SRC/decorator.py:594-636` appends `span.start` to `span.events` (`:90`). The sync and async wrappers are twins (`:1053-1087`, `:1092-1123`). Generators are refused at decoration (`:808-813`).
2. **Assembly and bounding.** `SRC/model.py:190-215` calls `sanitize_fields`; ceilings are in `SRC/config.py:53-56`.
3. **In-span log.** `SRC/api.py:134-140` appends to `span.events`. Nothing leaves the span until it closes.
4. **Orphan log** (no span, or span closed). `SRC/api.py:141-167` runs the engine or one `sink.emit([event])` on the caller's thread.
5. **Span close.** `SRC/decorator.py:721-724` then `:581-584` call `worker.submit(events)`. A span's whole list is one submission.
6. **Enqueue.** `SRC/worker.py:669-682` uses `put_nowait` on a `Queue(maxsize=10_000)` of submissions (`:363`, `:438`). Overflow drops the newest.
7. **Dequeue and batch.** `SRC/worker.py:1719-1744` emits at 10 submissions or 1 s (`:361-362`). `configure()` exposes none of these knobs (`SRC/config.py:272-284`; the worker is built with defaults at `SRC/_lifecycle.py:3796`).
8. **Hand to the sink.** `SRC/worker.py:1925-1987`. A `deliver` sink goes through the engine (`:1933`). An emit-only sink gets 4 attempts with waits of 0.01, 0.02 and 0.04 s (`:1958-1975`; `SRC/sinks/_engine.py:34-37`, `:150`), then the batch is abandoned. There is no requeue.
9. **Verdict.** A run counts as failed only if nothing was accepted and nothing was unknown (`SRC/sinks/_engine.py:1873`).
10. **`flush()`.** `SRC/_lifecycle.py:5754-5778` sweeps open spans (`SRC/decorator.py:448-465`) and queues a marker. The marker is judged on `failed_batches` not having moved since it was stamped (`SRC/worker.py:910`, `:1784`).
11. **`shutdown()` and atexit.** `SRC/_lifecycle.py:5270-5328`, default 30 s (`:27`), registered at `:3833`. The final drain sends everything queued as one batch (`SRC/worker.py:1804-1817`).
12. **Thread death.** `SRC/worker.py:1535-1536`, `:1679-1690`. The worker is never rebuilt; `SRC/_lifecycle.py:3779` keeps returning the dead one.
13. **Swap.** `SRC/worker.py:1110-1138`. **Fork child:** the queue is replaced and counters zeroed (`SRC/worker.py:584-592`).

### 2. Loss sites

| Site | file:line | Counted | Announced | Once | Mark |
|---|---|---|---|---|---|
| Span open fails | `decorator.py:630-635` | no | yes | — | **SILENT in counters** (P11a) |
| Span close fails | `decorator.py:686-688` | `in_span_lost`, minus the unbuilt `span.end` | yes | undercount by 1 | defect (P11b) |
| In-span build fails | `api.py:138-140` | `in_span_lost` | yes | yes | VISIBLE |
| Append racing a close or sweep | `DOC/docs/architecture.md:1651-1665` | no | no | — | SILENT, recorded |
| Orphan emit fails | `api.py:158-167` | `orphan_lost` | yes | yes | VISIBLE, no retry |
| Queue full | `worker.py:674-680` | `dropped`, in submissions | 1st and every 1000th | yes | VISIBLE, under-reads events 3x in P1 |
| Submit after shutdown | `worker.py:670-671`, `:702-711` | `submitted_after_shutdown` and `queued` | yes | no | **DOUBLE-COUNTED** with `dropped` (P3) |
| Batch abandoned or retry declined | `worker.py:1941-1956`, `:1966-1984` | `failed_batches`, in batches | yes, with event count | yes | VISIBLE; event count on stderr only |
| Sink partial, dropped or unknown | `_engine.py:1776-1792` | `health().sink` | yes | yes | VISIBLE, but `flush()` is truthy (P9) |
| Shutdown expiry, batch in flight | `worker.py:1438-1455` | `queued` for the rest; in-flight no | "counted when the emit returns" | — | never counted if the process then exits (P4) |
| Thread dies holding a batch | `worker.py:1679-1690` | held lists no; `stopped_reason` set | yes, in lists | — | **SILENT in counters** (P8) |
| SIGKILL, SIGTERM, `os._exit`, kill mid-handler | — | impossible | no | — | UNAVOIDABLE-BUT-UNSTATED |
| Lambda freeze with no `flush()` | `DOC/README.md:1458-1462` | impossible | no | — | unavoidable, stated |
| Fork | `worker.py:584-592` | — | — | — | no loss; duplicates (P7) |
| Swap | `worker.py:1110-1138` | `incomplete_swaps` | yes | yes | no loss (P6) |

The grep for the unstated row was `grep -n -i 'sigkill\|sigterm\|os\._exit' README.md docs/architecture.md`; it returns only fork-test notes.

### 3. Probes (all run)

- **P1 `SC/p01_overflow.py`** (stalled sink, 12,000 spans of 3 events). Delivered 10,010 spans, `dropped=1990`. Spans balance: 12000 == 10010 + 1990. Events do not: 36,000 against 30,030 + 1,990, because 5,970 events are missing and the counter is in submissions. `flush()` returned `ok=True`.
- **P2 `SC/p02_shutdown_stalled.py`** (`shutdown(0.5)` on a stalled sink, 500 spans). `queued=491`, `ShutdownTimeout`, delivered 0. The gap is the 10 spans in flight, which are announced and in no counter. After the sink recovered all 500 arrived; `sink.closed=0`.
- **P3 `SC/p03_shutdown_race.py 400`** (8 threads racing `shutdown()`). Accepted 3,200, delivered 818, `queued=2382`, no duplicates. `submitted_after_shutdown=2393`, an over-count of 11 that the final drain delivered. This is documented (`SRC/worker.py:162-167`).
- **P3 `... 2000`**. Accepted 16,000, delivered 4,006, `queued=10000`, `dropped=1994`, `submitted_after_shutdown=11994`. The 1,994 are in both loss terms and announced on both lines.
- **P4 `SC/p04_exit.py 0.02 10000`** (atexit, 20 ms per 10 events). The process took 30.3 s to exit. The sink received 11,630 of 20,000 events in 2 emit calls, the larger one 19,980 events. 8,370 were lost and `close()` never ran. The only trace was "19980 event(s) in flight, counted when the emit returns".
- **P5 `SC/p05_raise_nth.py`**. With every third batch failing: 900 accepted, 600 delivered, `failed_batches=10`, stderr lines summing to 300. The identity holds only through stderr. With the first attempt of each batch failing: 300 of 300, no duplicates. On the orphan path one transient failure gave 2 accepted, 1 delivered, `orphan_lost=1`, with no retry.
- **P6 `SC/p06_swap.py`**. With the old sink healthy: A 2,018 + B 3,982 = 6,000, no duplicates. With the old sink stalled: A 10 + B 5,990 = 6,000, `incomplete_swaps=1`, and the swap took 5.01 s.
- **P7 `SC/p07_fork.py`**. The pre-fork queue arrived 50 of 50, from the parent only. Two `log_id`s were delivered by both processes (`span.start` and the pre-fork in-span log of the open span), and each process emitted its own `span.end` for that span. I found no statement of this by grep in `DOC/docs/invariants.md`.
- **P8 `SC/p08_worker_death.py`** (sink raises `SystemExit` once). 50 accepted, 10 delivered, `queued=31` (30 spans plus a stranded flush marker), `dropped=0`, `failed_batches=0`. The 10 held spans are in no counter. The 30 later spans were never delivered although the sink was healthy again, and `shutdown()` did not drain them.
- **P9 `SC/p09_flush_barrier.py`** (verdict sink, 30 events per case):

  | Sink outcome | Lost | `flush()` | `failed_batches` | `health().sink` |
  |---|---|---|---|---|
  | 1 accepted, 29 retryable forever | 29 | `ok=True` | +0 | `failed=29` |
  | 1 accepted, 29 rejected | 29 | `ok=True` | +0 | `failed=29` |
  | all dropped | 30 | `ok=True` | +0 | `dropped=30` |
  | all unknown | 30 | `ok=True` | +0 | `failed=30` |
  | all rejected | 30 | `abandoned` | +1 | `failed=30` |

  A batch abandoned before the `flush()` call also gave `ok=True` (33 accepted, 3 delivered); that one is documented at `DOC/README.md:1440-1444`.
- **P11 `SC/p11_assembly.py`**. (a) With an injected fault in `start_event`, the call returned, 0 of 2 boundary events were built, and `in_span_lost` stayed 0. (b) With an injected fault in `end_event`, 3 events were lost and `in_span_lost` moved by 2. (c) Not injected: a traced recursion into `RecursionError` entered 199 bodies, delivered 195 + 195, moved `in_span_lost` by 2, and left 6 of 398 events unaccounted.
- **P12 `SC/p12_kill.py`**. SIGKILL 3.5 s into an open root span: 0 of 51 root-span events delivered, 9 of 9 from closed child spans. SIGTERM right after 5 spans closed: 0 of 15, with no stderr. Normal-exit control: 15 of 15.
- **P13 `SC/p13_burst.py`** (30,000 spans in 0.42 s, 20 ms per emit call). 10,180 delivered, `dropped=19820`. The backlog went out in 1,018 calls of exactly 20 events over 23.5 s. An orphan log after `shutdown()` was delivered (+1).

### 4. Accounting defects (invariant 2)

1. **Post-shutdown overflow is counted twice.** `SRC/worker.py:670-671` counts before the put and `:674-680` counts again on `Full`. The same method's docstring says this must not happen (`SRC/worker.py:657-658`). P3: 1,994 in both terms.
2. **A dead drain thread's held batch is in no counter.** `SRC/worker.py:1679-1690` announces it in lists, not events. P8: 10 spans.
3. **A span that fails to open loses its boundary events uncounted.** `SRC/decorator.py:630-635` has no `_note_in_span_loss`, against `DOC/docs/invariants.md:60-61`. A failed close undercounts by the `span.end` it could not build (`SRC/decorator.py:687`). P11c shows a non-injected route.
4. **`flush()` is truthy over total loss of the flushed batch.** `SRC/sinks/_engine.py:1873` and `SRC/worker.py:1941` drive it. `SRC/results.py:61` ("the sink took them") and `DOC/README.md:1374`, `:1414-1417` ("evidence of delivery") say otherwise, while `DOC/README.md:767-769` says the opposite. The Lambda handler idiom omits `h.sink` (`DOC/README.md:1501-1502`), though the general idiom has it (`:1258`), so P9's first four rows pass it silently.
5. **Health has no event unit.** `dropped` is in submissions and `failed_batches` in batches (recorded at `DOC/docs/invariants.md:62`). The balance in events exists only by summing stderr lines (P1, P5).
6. **In-flight events at an expired exit are promised a count that never comes.** `SRC/worker.py:1449-1450`; P4.

### 5. Avoidable loss, ranked for a Lambda fleet shipping to SQS

1. **Events stay in the open span until it closes.** A timeout or out-of-memory kill mid-handler loses the whole root span, which is the invocation most worth having (P12: 0 of 51). The README's Lambda section covers freeze, not kill (`DOC/README.md:1455-1512`).
2. **`flush()` and the documented handler check miss sink-side partial loss** (defect 4). SQS per-entry failures and oversize drops land exactly there.
3. **No `flush()` before the handler returns.** Up to 9 pending spans, or under 1 s of events, wait for the next thaw and are lost on reap. This is stated and uncountable.
4. **Thin worker-level retry.** An emit-only sink gets 70 ms in total, and that includes `MultiSink`, which offers no `deliver` (`DOC/docs/invariants.md:157`). The orphan path gets none for such sinks (`SRC/api.py:156-157`). A `deliver` sink gets `(sink.max_retries+1) x 4` rounds (`SRC/sinks/_engine.py:1619-1620`). Nothing is requeued.
5. **Fixed batch of 10 submissions and a fixed 10,000-submission queue**, neither configurable (P13: 66% dropped although the sink takes 19,980 events in one call).
6. **Exit.** The 30 s budget carries one giant final batch (P4), and SIGTERM has no handler (P12).
7. **Thread death is permanent** and strands later events (P8).
8. **No spill to disk.** The contract is at-most-once in memory; `DOC/docs/architecture.md:778-800` calls SQS the durable stage but nothing says a crash loses the queue.

### 6. One structural mechanism

A single event-unit ledger on the runtime would make the identity checkable by construction. Every accept site credits `accepted` (`SRC/decorator.py:90`, `SRC/api.py:137`, `:150`, `SRC/decorator.py:721`). Every exit debits exactly one of: delivered, one named loss term, or a pending gauge (open spans, queued, in flight). `health()` would expose the terms, and `shutdown()` plus an autouse test fixture would assert `accepted == delivered + losses + pending`.

It needs event counts at `SRC/worker.py:676`, `:703`, `:1943`, `:1968` and `:1679`, and a debit at `SRC/decorator.py:631`. Defects 1, 2, 3, 5 and 6 then become assertion failures. Defect 4 needs `flush()` to compare the ledger's loss total across its window rather than `failed_batches`.

## Detail cut for length

Everything above this heading is the report as handed back, unchanged. What follows was measured or read during the same run and left out for length. Probe outputs are the outputs of the single run each number in the report came from; P3, P6, P7 and P13 involve thread scheduling or wall-clock time, so a re-run reproduces the shape and not the exact figures. There is no P10 script: the "keep submitting after shutdown" case numbered P10 in my notes is the second P3 run (`p03_shutdown_race.py 2000`).

### A. Exact probe output

Command for each: `cd SC && PYTHONPATH=<wt-audit>/src <venv>/bin/python -B <script> [args]`. Every run exited 0.

#### P1 `p01_overflow.py`

```
stalled: queued=10000 dropped=1990 failed_batches=0 stopped_reason=None retired=False submitted_after_shutdown=0 orphan_lost=0 in_span_lost=0 incomplete_swaps=0 sink=None
after flush: queued=0 dropped=1990 failed_batches=0 stopped_reason=None retired=False submitted_after_shutdown=0 orphan_lost=0 in_span_lost=0 incomplete_swaps=0 sink=None
flush: FlushResult(ok=True, reason=None)
accepted spans=12000 events=36000
delivered spans=10010 events=30030
health.dropped (submissions)=1990; events actually missing=5970
IDENTITY spans: 12000 == 10010 + 1990 -> True
IDENTITY events using health alone: 36000 == 30030 + 1990 -> False
  stderr: log-foundry: lost 1 submission(s); log queue full; count is cumulative
  stderr: log-foundry: lost 1000 submission(s); log queue full; count is cumulative
```

#### P2 `p02_shutdown_stalled.py`

```
shutdown returned after 0.51s; sink.closed=0
after expired shutdown: queued=491 dropped=0 failed_batches=0 stopped_reason=ShutdownTimeout retired=True submitted_after_shutdown=0 orphan_lost=0 in_span_lost=0 incomplete_swaps=0 sink=None
delivered so far spans=0
IDENTITY (spans) at this instant: 500 == delivered 0 + queued 491 + dropped 0 -> False  (gap = batch in flight, in no counter)
flush after expired shutdown: FlushResult(ok=False, reason='retired')
after sink recovered: queued=0 dropped=0 failed_batches=0 stopped_reason=ShutdownTimeout retired=True submitted_after_shutdown=0 orphan_lost=0 in_span_lost=0 incomplete_swaps=0 sink=None
delivered spans=500 of 500; sink.closed=0; thread alive=False
  stderr: log-foundry: shutdown timed out after 0.5s; the sink is left open because the worker thread is still using it; 20 event(s) in flight, counted when the emit returns; 490 submission(s) still queued
```

Reading note: `queued=491` against the line's "490 submission(s) still queued" is the `_SHUTDOWN` sentinel, which `Health.queued` counts (`SRC/worker.py:795`) and `_queued_submissions` leaves out (`SRC/worker.py:1482-1488`).

#### P3 `p03_shutdown_race.py 400`

```
after: queued=2382 dropped=0 failed_batches=0 stopped_reason=None retired=True submitted_after_shutdown=2393 orphan_lost=0 in_span_lost=0 incomplete_swaps=0 sink=None
accepted spans=3200 (about 837 when shutdown() returned); delivered span.end=818 span.start=818; sink.closed=1
IDENTITY spans: accepted 3200 == delivered 818 + queued 2382 + dropped 0 -> True
undelivered = 2382; submitted_after_shutdown = 2393; over-count = 11
sum of loss terms queued+dropped+submitted_after_shutdown = 4775 vs undelivered 2382
duplicates delivered: 0
  stderr: log-foundry: lost 1 submission(s); logged after shutdown(), which is terminal; nothing will drain these. Use flush() in a process that logs again. Count is cumulative
  stderr: log-foundry: lost 1000 submission(s); logged after shutdown(), which is terminal; nothing will drain these. Use flush() in a process that logs again. Count is cumulative
  stderr: log-foundry: lost 2000 submission(s); logged after shutdown(), which is terminal; nothing will drain these. Use flush() in a process that logs again. Count is cumulative
```

#### P3 `p03_shutdown_race.py 2000` (the post-shutdown overflow case)

```
after: queued=10000 dropped=1994 failed_batches=0 stopped_reason=None retired=True submitted_after_shutdown=11994 orphan_lost=0 in_span_lost=0 incomplete_swaps=0 sink=None
accepted spans=16000 (about 4021 when shutdown() returned); delivered span.end=4006 span.start=4006; sink.closed=1
IDENTITY spans: accepted 16000 == delivered 4006 + queued 10000 + dropped 1994 -> True
undelivered = 11994; submitted_after_shutdown = 11994; over-count = 0
sum of loss terms queued+dropped+submitted_after_shutdown = 23988 vs undelivered 11994
duplicates delivered: 0
  stderr: log-foundry: lost 1 submission(s); logged after shutdown(), which is terminal; nothing will drain these. Use flush() in a process that logs again. Count is cumulative
  stderr: log-foundry: lost 1000 submission(s); logged after shutdown(), which is terminal; nothing will drain these. Use flush() in a process that logs again. Count is cumulative
  stderr: log-foundry: lost 2000 submission(s); logged after shutdown(), which is terminal; nothing will drain these. Use flush() in a process that logs again. Count is cumulative
  stderr: log-foundry: lost 3000 submission(s); logged after shutdown(), which is terminal; nothing will drain these. Use flush() in a process that logs again. Count is cumulative
  stderr: log-foundry: lost 4000 submission(s); logged after shutdown(), which is terminal; nothing will drain these. Use flush() in a process that logs again. Count is cumulative
  stderr: log-foundry: lost 5000 submission(s); logged after shutdown(), which is terminal; nothing will drain these. Use flush() in a process that logs again. Count is cumulative
  stderr: log-foundry: lost 6000 submission(s); logged after shutdown(), which is terminal; nothing will drain these. Use flush() in a process that logs again. Count is cumulative
  stderr: log-foundry: lost 7000 submission(s); logged after shutdown(), which is terminal; nothing will drain these. Use flush() in a process that logs again. Count is cumulative
  stderr: log-foundry: lost 8000 submission(s); logged after shutdown(), which is terminal; nothing will drain these. Use flush() in a process that logs again. Count is cumulative
  stderr: log-foundry: lost 9000 submission(s); logged after shutdown(), which is terminal; nothing will drain these. Use flush() in a process that logs again. Count is cumulative
  stderr: log-foundry: lost 10000 submission(s); logged after shutdown(), which is terminal; nothing will drain these. Use flush() in a process that logs again. Count is cumulative
  stderr: log-foundry: lost 1 submission(s); log queue full; count is cumulative
  stderr: log-foundry: lost 11000 submission(s); logged after shutdown(), which is terminal; nothing will drain these. Use flush() in a process that logs again. Count is cumulative
  stderr: log-foundry: lost 1000 submission(s); log queue full; count is cumulative
```

Reading note: the spans identity balances here because it uses `queued` and `dropped` only. The double count is `dropped=1994` sitting inside `submitted_after_shutdown=11994`: the three loss terms sum to 23,988 against 11,994 undelivered, and each of the 1,994 was announced on both the "logged after shutdown()" line and the "log queue full" line.

#### P4 `p04_exit.py 0.02 10000`

```
child: accepted spans=10000 events=20000 queued=9990 dropped=0
exit code=0; process took 30.3s to exit; sink received 11630 of 20000 events in 2 emit call(s), largest batch 19980 events; sink.close() ran=False
lost at exit = 8370 events; no counter survives the process
  stderr: log-foundry: shutdown timed out after 30s; the sink is left open because the worker thread is still using it; 19980 event(s) in flight, counted when the emit returns
```

Reading note: the fake sink sleeps 20 ms per 10 events and records each chunk as it lands, so the 11,630 is a per-chunk sink's result. A sink that sends the batch as one request would have delivered 20 of 20,000 (the first batch of 10 spans) and lost the single 19,980-event call whole. The measured rate was about 388 events/s, slower than the nominal 500, because of `time.sleep` granularity on this machine.

#### P5 `p05_raise_nth.py`

```
every 3rd batch fails for good: queued=0 dropped=0 failed_batches=10 stopped_reason=None retired=False submitted_after_shutdown=0 orphan_lost=0 in_span_lost=0 incomplete_swaps=0 sink=None
accepted events=900; delivered=600; failed_batches=10; sum of 'lost N event(s)' stderr lines=300; emit attempts on failing batches=40
IDENTITY events via stderr: 900 == 600 + 300 -> True
IDENTITY events via health() alone: no event-count term for an abandoned batch (failed_batches=10 batches, sink=None)
last flush(): FlushResult(ok=False, reason='abandoned')
  stderr: log-foundry: lost 30 event(s); batch abandoned after 4 emit attempts
  stderr: log-foundry: lost 30 event(s); batch abandoned after 4 emit attempts
transient (first attempt of each batch fails): queued=0 dropped=0 failed_batches=10 stopped_reason=None retired=False submitted_after_shutdown=0 orphan_lost=0 in_span_lost=0 incomplete_swaps=0 sink=None
accepted events=300; delivered=300; duplicates=0; flush=FlushResult(ok=True, reason=None); stderr lines=0
orphan path, first emit fails once: queued=0 dropped=0 failed_batches=10 stopped_reason=None retired=False submitted_after_shutdown=0 orphan_lost=1 in_span_lost=0 incomplete_swaps=0 sink=None
orphan accepted=2 delivered=1 emit calls=2 orphan_lost=1
  stderr: log-foundry: absorbed a failure while emitting an orphan log (ConnectionError); the event was lost
```

Reading note: `failed_batches=10` on the second and third lines is carried over from the first part of the same process (the worker survives a sink swap); neither later part added to it. The script prints only the first two of the ten "lost 30 event(s)" lines.

#### P6 `p06_swap.py`

```
[old sink healthy] accepted spans=6000 A=2018 B=3982 A+B=6000 dup=0 dropped +0 failed_batches +0 incomplete_swaps +0 swap took 0.00s A.closed=1
    flush while A stalled=FlushResult(ok=True, reason=None); flush after A recovered=FlushResult(ok=True, reason=None)
    IDENTITY: 6000 == 6000 + 0 -> True
[old sink stalled in emit] accepted spans=6000 A=10 B=5990 A+B=6000 dup=0 dropped +0 failed_batches +0 incomplete_swaps +1 swap took 5.01s A.closed=0
    flush while A stalled=FlushResult(ok=False, reason='timed-out'); flush after A recovered=FlushResult(ok=True, reason=None)
    IDENTITY: 6000 == 6000 + 0 -> True
    stderr: log-foundry: the previous sink could not be confirmed drained within 4.99996s; it is left open until a shutdown() finds the drain thread ended; 5990 submission(s) still queued, which may reach the new sink
```

Reading note: in the stalled case the 5,990 spans submitted before and during the swap went to B, the new sink, which is the misrouting `incomplete_swaps` documents; nothing was lost or duplicated. `configure(sink=...)` blocked the caller for the full 5 s swap budget.

#### P7 `p07_fork.py`

```
parent before fork: queued=15
.../scout-core/p07_fork.py:39: DeprecationWarning: This process (pid=11300) is multi-threaded, use of fork() may lead to deadlocks in the child.
  pid = os.fork()
child:  flush=FlushResult(ok=True, reason=None) queued=0 dropped=0 failed_batches=0 stopped_reason=None
parent: flush=FlushResult(ok=True, reason=None) queued=0 dropped=0 failed_batches=0
   ('child', 'buffered-in-open-span-before-fork') 1
   ('child', 'child-after-fork') 1
   ('child', 'span.end') 1
   ('child', 'span.start') 1
   ('parent', 'buffered-in-open-span-before-fork') 1
   ('parent', 'parent-after-fork') 1
   ('parent', 'queued_before_fork:span.end') 25
   ('parent', 'queued_before_fork:span.start') 25
   ('parent', 'span.end') 1
   ('parent', 'span.start') 1
rows=58 distinct log_ids=56 log_ids delivered twice=2
   duplicated: [('child', 'span.start'), ('parent', 'span.start')]
   duplicated: [('child', 'buffered-in-open-span-before-fork'), ('parent', 'buffered-in-open-span-before-fork')]
pre-fork queue: accepted events=50 delivered rows=50
```

(The warning line's path is shortened here; it printed the full `SC` path.)

#### P8 `p08_worker_death.py`

```
after death: queued=31 dropped=0 failed_batches=0 stopped_reason=SystemExit retired=False submitted_after_shutdown=0 orphan_lost=0 in_span_lost=0 incomplete_swaps=0 sink=None
flushes: FlushResult(ok=True, reason=None) | FlushResult(ok=False, reason='abandoned') | FlushResult(ok=False, reason='thread-died'); thread alive=False
accepted spans=50 delivered=10 queued=31 dropped=0 failed_batches=0
IDENTITY spans: 50 == 10 + 31 + 0 -> False (gap 9 = the batch the thread died holding)
after shutdown: queued=31 dropped=0 failed_batches=0 stopped_reason=SystemExit retired=True submitted_after_shutdown=0 orphan_lost=0 in_span_lost=0 incomplete_swaps=0 sink=None
delivered after shutdown=10 sink.closed=1
  stderr: log-foundry: absorbed a failure while draining the log queue (SystemExit); worker thread stopped; 10 undrained event-list(s) held and 1 queued item(s) undelivered, nothing further will be delivered
```

Reading note: the script's own "gap 9" is arithmetic on `queued=31`, which includes one stranded flush marker. The true split is 50 accepted = 10 delivered + 10 held by the dead thread (in no counter) + 30 queued; the report states it that way. The stderr line's "1 queued item(s)" was true when the thread died (the marker of the third flush had not yet been queued; the item is the marker of the second flush or a submission in passing) and it is written once, so it does not cover the 30 spans accepted afterwards. The flush that killed the thread reported `abandoned`, not `thread-died`.

#### P9 `p09_flush_barrier.py`

```
[1 accepted, 29 never land (RETRYABLE every round)] accepted events=30 sink took=1 lost=29 deliver calls=4
    flush()=FlushResult(ok=True, reason=None)  failed_batches +0  health.sink=SinkLosses(dropped=0, failed=29)
    stderr: log-foundry: lost 29 event(s); rejected 0, unresolved 29, raised 0, unknown 0, dropped 0 after 4 delivery round(s) to DeliverSink
[1 accepted, 29 REJECTED] accepted events=30 sink took=1 lost=29 deliver calls=1
    flush()=FlushResult(ok=True, reason=None)  failed_batches +0  health.sink=SinkLosses(dropped=0, failed=29)
    stderr: log-foundry: lost 29 event(s); rejected 29, unresolved 0, raised 0, unknown 0, dropped 0 after 1 delivery round(s) to DeliverSink
[all 30 DROPPED by the sink] accepted events=30 sink took=0 lost=30 deliver calls=1
    flush()=FlushResult(ok=True, reason=None)  failed_batches +0  health.sink=SinkLosses(dropped=30, failed=0)
    stderr: log-foundry: lost 30 event(s); rejected 0, unresolved 0, raised 0, unknown 0, dropped 30 after 1 delivery round(s) to DeliverSink
[all 30 UNKNOWN] accepted events=30 sink took=0 lost=30 deliver calls=1
    flush()=FlushResult(ok=True, reason=None)  failed_batches +0  health.sink=SinkLosses(dropped=0, failed=30)
    stderr: log-foundry: lost 30 event(s); rejected 0, unresolved 0, raised 0, unknown 30, dropped 0 after 1 delivery round(s) to DeliverSink
[all 30 REJECTED] accepted events=30 sink took=0 lost=30 deliver calls=1
    flush()=FlushResult(ok=False, reason='abandoned')  failed_batches +1  health.sink=SinkLosses(dropped=0, failed=30)
    stderr: log-foundry: lost 30 event(s); batch abandoned after 1 emit attempts
[earlier batch abandoned, then flush] accepted events=33 delivered=3 flush()=FlushResult(ok=True, reason=None) failed_batches +1
    stderr: log-foundry: lost 30 event(s); batch abandoned after 4 emit attempts
```

Reading notes. The fake sink declares `max_retries = 0`, so the engine allowed `(0+1) x (3+1) = 4` rounds (`SRC/sinks/_engine.py:1619-1620`), which is the "deliver calls=4" of the first case. In the "all 30 REJECTED" case the same 30 events appear in two places, `failed_batches +1` and `health().sink.failed=30`; that is the recorded upper bound on a sink's `failed` (`DOC/docs/invariants.md:126-128`), not a new finding. Every loss in this probe was announced on stderr exactly once.

#### P11 `p11_assembly.py`

```
(a) span open failed [injected]: queued=0 dropped=0 failed_batches=0 stopped_reason=None retired=False submitted_after_shutdown=0 orphan_lost=0 in_span_lost=0 incomplete_swaps=0 sink=None
    call returned 1; sink has start=0 end=0 in-span=1
    stderr: log-foundry: absorbed a failure while opening a span (MemoryError); this call runs untraced
(b) span close failed [injected]: queued=0 dropped=0 failed_batches=0 stopped_reason=None retired=False submitted_after_shutdown=0 orphan_lost=0 in_span_lost=2 incomplete_swaps=0 sink=None
    events the span would have carried=3 (start, in-span, end); in_span_lost moved by 2; delivered=0
    stderr: log-foundry: absorbed a failure while closing a span (MemoryError); the span's events were lost
(c) RecursionError inside a traced recursion [not injected]: queued=0 dropped=0 failed_batches=0 stopped_reason=None retired=False submitted_after_shutdown=0 orphan_lost=0 in_span_lost=4 incomplete_swaps=0 sink=None
    function bodies entered=199; delivered span.start=195 span.end=195; in_span_lost +2; dropped +0
    events expected if every entered call were traced=398; delivered+counted=392; unaccounted=6
    stderr x1: log-foundry: absorbed a failure while opening a span (RecursionError); this call runs untraced
    stderr x1: log-foundry: absorbed a failure while opening a span (RecursionError)
    stderr x1: log-foundry: absorbed a failure while closing a span (RecursionError); the span's events were lost
```

Reading notes. In (a) the in-span `info()` was delivered through the orphan path, with a fresh `trace_id`, because no span was open (`SRC/api.py:141-157`). In (c) the second "opening a span" line has no detail suffix: `_diag._suffix` swallowed a `RecursionError` of its own (`SRC/_diag.py:307-310`). Three stderr lines account for three of the four spans that did not deliver; I did not establish which site lost the fourth, and at the recursion limit any call inside the library can itself raise, so I report the 6-of-398 figure as measured and its exact split as not determined.

#### P12 `p12_kill.py`

```
[SIGKILL 3.5 s into an open root span] rc=-9; root span accepted events=51 (span.start + 50 logs) delivered=0; 3 closed child spans accepted events=9 delivered=9; stderr lines=0
[SIGTERM straight after 5 spans closed] rc=-15; accepted events=15 delivered=0; stderr lines=0
[control: same 5 spans, normal exit] rc=0; accepted events=15 delivered=15
```

Reading note: the SIGTERM case depends on the signal arriving inside the worker's 1 s time trigger, with fewer than 10 submissions pending; it arrived within milliseconds of the child's ready file.

#### P13 `p13_burst.py` (with the post-shutdown orphan check at its end)

```
burst produced: queued=10000 dropped=19820 failed_batches=0 stopped_reason=None retired=False submitted_after_shutdown=0 orphan_lost=0 in_span_lost=0 incomplete_swaps=0 sink=None
after flush: queued=0 dropped=19820 failed_batches=0 stopped_reason=None retired=False submitted_after_shutdown=0 orphan_lost=0 in_span_lost=0 incomplete_swaps=0 sink=None
accepted spans=30000 in 0.42s (72,001 spans/s); delivered events=20360 (spans=10180); dropped=19820
IDENTITY spans: 30000 == 10180 + 19820 -> True
emit calls by batch size (events): {20: 1018}; drain of the backlog took 23.5s; flush=FlushResult(ok=True, reason=None)
orphan log after shutdown: queued=0 dropped=19820 failed_batches=0 stopped_reason=None retired=True submitted_after_shutdown=0 orphan_lost=0 in_span_lost=0 incomplete_swaps=0 sink=None
orphan log after shutdown: sink received +1
```

### B. Every site a single event ledger would credit or debit

All `file:line` are under `SRC`. "Today" says what the site does now. This list is from reading the files named in the report's stage list; it does not cover the individual sinks, and inside the engine it covers only the commit, not each sink's own registration sites.

**Accept sites (credit `accepted`, one per event built)**

| Site | file:line | Today |
|---|---|---|
| `span.start` built and buffered | `decorator.py:90` (built by `model.py:230`) | no count |
| In-span level event built and buffered | `api.py:136-137` | no count |
| Orphan level event built | `api.py:150` | no count |
| `span.end` built and buffered | `decorator.py:721` (built by `model.py:341-350`) | no count |

There is no accepted-events counter anywhere today, which is why every probe had to count acceptances itself.

**Sites where an event the caller asked for is never built (credit and debit together, or a separate "not built" term)**

| Site | file:line | Today |
|---|---|---|
| Span open fails: no `span.start`, and no `span.end` later | `decorator.py:630-635` | announced, not counted (P11a) |
| In-span event build fails | `api.py:138-140` | `in_span_lost` +1, announced |
| Orphan event build, sink construction or emit fails | `api.py:165-167` | `orphan_lost` +1, announced |
| `span.end` build fails at close | `decorator.py:686-688` (raise from `:721`) | counts `len(span.events)`, which omits the unbuilt `span.end` (P11b) |

**Pending gauges (an event is in exactly one until it leaves)**

| Where the event sits | file:line | Today |
|---|---|---|
| On an open span's buffer | `model.py:69`; detached at close `decorator.py:583`, at sweep `decorator.py:461` | not visible in `health()` |
| In the worker queue | put at `worker.py:673`; read by `health()` at `worker.py:795` | `queued`, in submissions, and it also counts flush markers and the `_SHUTDOWN` sentinel |
| In the drain thread's `pending` list | `worker.py:1739`, `:1815` | not visible |
| In flight inside `emit` or the engine | `worker.py:1931`, cleared at `:1987` | `_in_flight`, read only by the shutdown-timeout line (`worker.py:1440`, `:1449-1450`) |

**Delivered (debit to `delivered`)**

| Site | file:line | Today |
|---|---|---|
| Worker, emit-only sink: `emit` returned | `worker.py:1962-1963` | no count |
| Worker, engine sink: run not failed | `worker.py:1940-1957`; per-event fates in `sinks/_engine.py:1726-1733` (`Delivery.accepted`) | no count kept; `Delivery` is discarded |
| Orphan, emit-only sink: `emit` returned | `api.py:156-157` | no count |
| Orphan, engine sink: run not failed | `api.py:153-155`, `:158` | no count |

**Loss sites (debit to exactly one loss term)**

| Site | file:line | Today's counter | Unit today |
|---|---|---|---|
| Queue full | `worker.py:674-680` | `dropped` | submissions |
| Submitted after `shutdown()` began, first read | `worker.py:669-671` -> `:702-711` | `submitted_after_shutdown` | submissions; counted before the put, so a `Full` at `:674` counts it again |
| Submitted after `shutdown()` began, second read | `worker.py:681-682` | `submitted_after_shutdown` | submissions; counted even when the final drain delivers it (P3, 11 of 2,393) |
| Engine run failed or declined | `worker.py:1941-1956` | `failed_batches` | batches; event count on stderr only |
| Emit-only batch abandoned | `worker.py:1966-1974` | `failed_batches` | batches |
| Emit-only retry declined at shutdown | `worker.py:1976-1985` | `failed_batches` | batches |
| Sink-side loss of a run (rejected, unresolved, raised, unknown, dropped) | `sinks/_engine.py:1776-1792`, other owners at `:1795` | sink's `failed_events` / `dropped_events` | events |
| Orphan engine run failed | `api.py:158-164` | `orphan_lost` | events |
| Orphan `emit` raised | `api.py:165-167` | `orphan_lost` | events |
| Span close: worker could not be resolved | `decorator.py:581` raising into `:686-688` | `in_span_lost` | events |
| Drain thread died holding `pending` | `worker.py:1535-1536` -> `:1679-1690` | none | announced in event-lists |
| `shutdown()` expired with a batch in flight | `worker.py:1438-1455` | none for the batch | announced in events; counted later only if `emit` returns and fails |
| Append racing a detach | `api.py:134-137` against `decorator.py:583` and `:461` | none | not announced (recorded, `DOC/docs/architecture.md:1651-1665`) |

**Sites that move events between gauges or reset the ledger (no debit, but the ledger must follow them)**

| Site | file:line | What the ledger must do |
|---|---|---|
| Span close hands the buffer over | `decorator.py:581-584` | open-span gauge to queued |
| Flush sweep hands a buffer over | `decorator.py:459-462` | open-span gauge to queued |
| Drain takes a submission | `worker.py:1723`, `:1739`; final drain `:1807`, `:1815` | queued to pending |
| Batch flattened and sent | `worker.py:1925-1931` | pending to in flight |
| Sink swap | `worker.py:1123-1124` | nothing; the in-flight batch stays with the sink it was read for (`:1960`) |
| Fork child rebuilds the worker | `worker.py:584-592` | zero the child's ledger; the parent still owns the queued events (P7) |
| Fork child keeps the forking thread's open spans | not handled anywhere I read | the child's copy of each open-span buffer is a second acceptance of the same `log_id`s (P7: 2 delivered twice) |

### C. What `flush()` sweeps from open spans today, and what stays on the span

Read, not probed, except where a probe is named. Source: `SRC/decorator.py:448-465` and its docstring (`:371-447`), `SRC/context.py:33-35`, `:75-93`, and `SRC/_lifecycle.py:5754-5778`.

**What the sweep takes.** `_flush_worker` calls `_sweep_open_spans()` first (`_lifecycle.py:5759`), before the queue marker (`:5768`). The sweep walks `context._live_span_stack()` (`decorator.py:452`), which returns the `_span_stack` context variable of the calling context (`context.py:93`): every span open on the calling thread or task, outermost first. For each one that has buffered events it backfills baggage onto the boundary events (`decorator.py:459`), marks the span swept (`:460`), detaches the buffer by swap (`:461`) and submits it as one submission (`:462`). So a sweep sends everything buffered so far on the caller's own stack: `span.start` and every level event logged up to that moment, for the current span and each of its open ancestors.

**What stays on the span, or never leaves without a close.**

1. **`span.end`.** It is built only when the function returns or raises (`decorator.py:721`), so no flush can send it. A process killed after a mid-handler `flush()` has delivered `span.start` and the logs so far, with no end event, duration or status.
2. **Everything logged after the sweep.** It accumulates on the fresh buffer until the next `flush()` or the close.
3. **Spans open in any other context.** The sweep "reaches only the calling context's spans" (`decorator.py:427-429`): a span open on another thread, or in another asyncio task, is not touched, so a handler that fanned out and then calls `flush()` leaves those buffers where they are.
4. **Everything, when nothing calls `flush()`.** The sweep runs from `flush()` alone. The worker's 1 s time trigger and its count trigger read only the queue (`worker.py:1719-1744`) and never see a span's buffer, and `shutdown()` does not sweep (`_lifecycle.py:5270-5328` has no call to it). P12 is this case measured: 3.5 s into an open root span, 0 of its 51 events had been delivered while all 9 events of its three closed child spans had.
5. **An event appended while the detach runs.** The recorded window (`DOC/docs/architecture.md:1660-1665`): an append racing the sweep's swap can land on the list already handed to the worker or on the new one, and is delivered only if the span is swept or closed again.

**Side effects of a sweep worth knowing for the ledger.**

- Each swept span is one queue submission, so it can be dropped whole at a full queue and counted as 1 (`worker.py:674-676`).
- The boundary events carry the baggage as of the flush, not as of the close (`decorator.py:385-387`).
- A swept root span refuses a later `continue_trace()` (`decorator.py:197-203`, `:303-304`).
- If the worker cannot be built, the sweep stops before detaching anything (`decorator.py:456-458`, `:465`), `flush()` returns `abandoned` (`_lifecycle.py:5760-5762`, `:5772-5773`), and the events stay on the span to be tried again at its close.
- An in-span `flush()` builds the worker if there is something to sweep, including on a cold start where no span has closed yet (`decorator.py:398-401`).

**What this means for the Lambda finding (report section 5, item 1).** The only way today to get an open root span's events out before it closes is an explicit `flush()` from inside the traced code, on the same thread or task. Nothing does it on a timer, and the README recipe places `flush()` outside the traced function (`DOC/README.md:1489-1499`), where it runs only if the handler returns or raises.
