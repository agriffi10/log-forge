# Probes of the 2026-11-14 audit

The scripts the five frames of [`2026-11-14-post-1.2.0-audit.md`](../2026-11-14-post-1.2.0-audit.md)
ran against this repository at `a0f44b5f` (tag `v1.2.0`), kept byte for byte as they were run so
that a later session can prove a finding closed by re-running the probe that found it. Each
directory is named for its frame report.

**Seventy-six files here are not the audit's.** Each was added after the audit was brought in
(`5cb0f449`) and names in its first lines the spec it was written for. The list below was derived
from `git log --diff-filter=A` per file; rederive it the same way when adding one.

- `lifecycle/p8_second_close_after_a_rearm.py`, `p9_second_stop_in_fork_repair.py` and
  `p10_fork_with_stderr_lock_held.py`, written while authoring SPEC-086 against `5447d26a`. `p9`
  and `p10` are the probes behind two open items in `docs/architecture.md` §12 that the audit did
  not find. `p8` is behind no open item: it traces a sink closed twice with nothing written between
  the closes to a re-arm between two takes, which invariant 5 records as correct, and is kept so
  the next reader of that symptom finds the trace.
- `lifecycle/p11_swap_arms_a_sink_a_shutdown_closed.py`, written while tracing SPEC-086's soak
  findings against `9d70f782`: the probe behind §12's item for a sink a swap installs that a
  concurrent `shutdown()` took before the swap decided. Its park point exists in every tree from
  `31dda162`, the parent of #339, to `9d70f782`, so it runs against older trees as this file says
  below.
- `lifecycle/released_sink_handed_back.py`, written while authoring SPEC-087, and
  `lifecycle/leaves_remaining_holder.py`, written while building it: the probe behind §12's item
  for a wrapper that leaves its member alone, which is then closed by nobody.
- `core-pipeline/p14_end_races_sweep.py` to `p17_log_after_an_interrupted_close.py`, written
  while authoring and revising SPEC-088, and `p18_nats_deadline_leaves_events_unattempted.py`,
  filed from SPEC-082's build session.
- `core-pipeline/spec090_*.py`, fourteen: twelve written while authoring SPEC-090 and two while building it.
- `core-pipeline/spec091_*.py`, seven, written while authoring SPEC-091.
- `sinks/spec092_*.py`, five, SPEC-092's prototype conformance corpus. Unlike the probes they
  import one another, so `sinks/` and `tests/` go on `PYTHONPATH` as their docstrings say.
- `core-pipeline/spec088_cost.py`, written while building SPEC-088: it measures what the event
  ledger costs, by the spec's Evidence method, for the delivery doc.
- `sinks/spec093_socket_options.py` and `sinks/spec093_rabbitmq_confirm_cost.py`, written while
  authoring SPEC-093 against `33582484`, and `sinks/spec094_report_wait_cost.py`, written while
  authoring SPEC-094. `sinks/spec093_close_corpus.py`, written while revising SPEC-093 against
  `97a03f45` and rebuilt after its execution-frame review, drives the real `SocketTransport` and is
  run on macOS and in Docker `python:3.12-slim` under added delay, as its docstring says;
  `sinks/spec093_fin_after_rst.py` imports nothing from the library, and
  `sinks/spec093_unread_buffer.py` reads the send queue through the patched transport.
  `sinks/spec094_stuck_partition.py` and `sinks/spec094_dead_broker_flush.py` are adapted from
  SPEC-094's reviewer's probes, `sinks/spec094_first_slice_and_pause.py` from its execution-frame
  reviewer's, and `sinks/spec094_real_broker_linger.py` runs against the Kafka image
  `tests/integration/docker-compose.yml` pins. Each is run with `PYTHONPATH=<tree>/src` as its
  docstring says, against two trees to compare. `sinks/spec093_prototype.patch` and `sinks/spec094_prototype.patch` are not
  probes: they are the two specs' prototypes, applied in that order with `patch -p1` to
  `git archive 97a03f45 src tests docs/audits/2026-11-14-probes`, and SPEC-093's and SPEC-094's
  *Evidence* sections say what each measured.
- `sinks/spec095_compare.py`, `sinks/spec095_mutants.py`, `sinks/spec095_cost.py` and
  `sinks/spec095_fifo_window.py`, written while authoring SPEC-095 against `97a03f45`. The first
  runs SPEC-092's prototype corpus against two trees and prints the rows that move; the second
  needs a tree carrying SPEC-095's prototype and is run with `python -B -P`, as its docstring says.
  `sinks/spec095_prototype.patch` and `sinks/spec095_prototype_after_094.patch` are not probes:
  they are SPEC-095's prototype, the first applied with `patch -p1` to `git archive 97a03f45 src
  tests docs/audits/2026-11-14-probes`, the second to that tree after SPEC-093's and SPEC-094's
  prototype patches, `sinks/spec093_prototype.patch` and `sinks/spec094_prototype.patch`.
  SPEC-095's *Evidence* section says what each measured.
- `core-pipeline/spec097_signal_in_section.py`, `spec097_stable_signal.py` and `spec097_cost.py`,
  written while authoring SPEC-097 and SPEC-098 against `ead40798`. The first raises SIGTERM while
  the main thread holds a library lock, as `lifecycle/p1a_signal_shutdown_injected.py` does, and
  reads the lock's owner where the lock has one, so it runs against a tree whose lifecycle locks are
  `RLock`s, which `p1a` cannot; the second reads which stop signal a sink holds and how often its
  setter runs, in a parent and a forked child; the third measures what the prototype costs. Each is
  run with `PYTHONPATH=<tree>/src` against two trees to compare. `core-pipeline/spec097_held_sweep.py`
  and `spec097_reentry_sweep.py` were added after SPEC-097's spec review and read
  `_lifecycle.in_section()`, so they run against a tree carrying the prototype only: the first raises
  SIGTERM inside every public call of `log_foundry.__all__` and reads whether the handler's held
  `shutdown()` ran, and the second interrupts every library lock section a workload reaches with a
  handler that logs and makes a traced call. `core-pipeline/spec097_pubsub_after_shutdown.py` was
  added after SPEC-098's spec review: a `GooglePubSubSink` configured again after `shutdown()`, with
  a fake publisher whose futures confirm after a given delay, which measured the losses behind
  Andrew's option 2; SPEC-098's *Evidence* gives its readings. `core-pipeline/spec097_prototype.patch`
  is not a probe: it is the two specs' prototype, applied with `patch -p1` to `git archive ead40798
  src tests docs/audits/2026-11-14-probes`, and their *Evidence* sections say what it measured, each
  part alone and both together.

- `sinks/spec096_limits_today.py`, `spec096_destination_below_limit.py` and
  `spec096_corpus_rows.py`, written while authoring SPEC-096 against `10ba21ee`: how `SQSSink` and
  `SNSSink` apply their byte limit today, what each counts when the destination's limit is below
  the sink's, and whether any SPEC-092 row depends on the limit. The third imports SPEC-092's
  prototype, so it needs `sinks/` and `tests/` on `PYTHONPATH` as its docstring says.
- `lifecycle/spec099_rule_corpus.py`, written while authoring SPEC-099 against `eda19f32`: it runs
  `p11_swap_arms_a_sink_a_shutdown_closed.py`'s `forced` choreography, records the sink's events as
  the soak records them, and judges each record, with rows written from §12's text, by three soak
  rules and by the known shapes. It imports the prototype's `tests/model_oracles.py` and
  `tests/soak_known.py`, so it runs only against a tree carrying the prototype, with `src` and
  `tests` on `PYTHONPATH` as its docstring says. `lifecycle/spec099_prototype.patch` is not a
  probe: it is SPEC-099's prototype, applied with `patch -p1` to `git archive eda19f32`, and
  SPEC-099's *Evidence* says what it measured. It also changes `scripts/pr-queue/queue.sh` and
  `pre-push` and adds `scripts/pr-queue-soak-test.sh`, the corpus grown from the execution-frame
  reviewer's harness. It changes nothing installed: the queue in use runs from the copy
  `install.sh` wrote.
- `core-pipeline/spec104_nats_exit_latch.py`, written for SPEC-104 against `03dbf502`: a JetStream
  `NATSSink`'s rounds against its own abandoned-publish latch, over an injected client.
- `sinks/spec100_moto_shapes.py`, `spec100_ids_once.py`, `spec100_ordering.py`,
  `spec100_costing.py` and `spec100_corpus_rows.py`, written while authoring SPEC-100 against
  `03dbf502`. The first needs `boto3` and `moto` in a scratch virtualenv, since moto is not a
  repository dependency: it reads the shape moto gives each SNS FIFO refusal, and shows that moto's
  SNS does not de-duplicate, so a FIFO SQS subscriber's single copy is the queue's doing. The next
  three need no AWS library and read, on `SQSSink` and on a tree whose `SNSSink` takes `fifo`, the
  ids a resend carries, the order a group lands in, and what an entry's ids cost. The last imports
  SPEC-092's prototype, which it loads with one adaptation for SPEC-090's `exit_batch` keyword,
  applied only where the prototype lacks it, as its docstring describes, so it needs `sinks/` and `tests/` on `PYTHONPATH`.
  `sinks/spec100_prototype.patch` is not a probe: it is SPEC-100's prototype, applied with
  `patch -p1` to `git archive 03dbf502 src tests docs/audits/2026-11-14-probes`, and SPEC-100's
  *Evidence* says what it measured.
- `sinks/spec100_refusals.py`, written while revising SPEC-100 against `03dbf502`. It needs no AWS
  library and prints what each combination of `fifo`, `message_group_id` and
  `message_deduplication_id` does at construction on `SQSSink`, and on `SNSSink` when the tree's
  `SNSSink` takes `fifo`, and what a FIFO batch of one id failure and one oversized event returns.

**They are evidence, not tests.** An audit wrote them quickly to answer one question each; they
assert nothing, they print what they measured, and a reader compares that with the output recorded
in the frame report under `../2026-11-14-frames/`. They are exempt from the linters for that
reason, and editing one to satisfy a linter would edit the record: `pyproject.toml` lists this
directory in ruff's `extend-exclude`, while `mypy` and `docstring-lint.py` read `src/` only and
`pytest` collects `tests/` only.

## Running one

From the repository root, against the working tree:

```sh
poetry run python -B docs/audits/2026-11-14-probes/<frame>/<script> [arguments]
```

No `PYTHONPATH` is needed. Python puts the script's own directory first on `sys.path`, which is how
the `core-pipeline` probes import their sibling `fakes.py`, and `poetry install --with dev` makes
`log_foundry` importable from `src/`. In a worktree with no environment of its own, any Python 3.12
or newer does the same with `PYTHONPATH=src <python> -B docs/audits/2026-11-14-probes/<frame>/<script>`.
To run a probe against an older tree, extract it with `git archive <ref> src | tar -x -C <dir>` and
point `PYTHONPATH` at `<dir>/src`.

A probe that measures a race or a clock (`core-pipeline/p03`, `p06`, `p07`, `p13`,
`sinks/probe_socket.py`,
`lifecycle/p1b_run.py`) reproduces the shape of its recorded output and not the exact figures. A
probe exits 0 when it ran, whatever it found; `lifecycle/p1a_signal_shutdown_injected.py` is the
exception, because the hang it demonstrates ends in the watchdog's exit code 1.

## What is here

| Frame report | Directory | Scratch name in the report | Arguments, where a script takes any |
|---|---|---|---|
| `core-pipeline.md` | `core-pipeline/` | `SC`, `scout-core` | `p03_shutdown_race.py 400` and `2000`; `p04_exit.py 0.02 10000`. `p04_exit_child.py` and `p12_kill_child.py` are run by their parents. |
| `lifecycle.md` | `lifecycle/` | `D`, `scout-lifecycle` | `p1b_run.py <path to p1b_child.py> 200`; `p1b_where.py <path to p1b_child_verbose.py> 120`. `p4b_forward.py` is not the frame's: see below. |
| `sinks.md` | `sinks/` | `scout-sinks` | none |
| `verification.md` | `verification/` | `scout-tests` | `detail.py <repo root> ast` and `sleep`; `ratio.py <repo root>`; `timing.sh <repo root>`; `classify.py <repo root>/tests` |
| `history.md` | `history/` | `scout-history` | `classify.py <repo root> <output tsv>`, which rewrites the table it is given |

`nodoc.py` and the two `strip.py` files are the readers the frames viewed modules through, not
probes. The `.tsv` files are the tables the reports cite.

**One probe was added when the audit was brought in.** `lifecycle/p4b_forward.py` is
`p4b_shared_child_shipped_sink.py` with its two `configure` lines exchanged, so that the outgoing
sink becomes a child of the incoming graph. The lifecycle frame probed only the other direction,
and `docs/architecture.md` §12 leads with this one. Both lose the same share of the file's lines
at `a0f44b5f` and against the trees at `v1.0.0`, `v1.1.0` and `3835a30e`.

**One probe was added after the audit, and is not the audit's.**
`lifecycle/released_sink_handed_back.py` was written while SPEC-087 was authored, against
`5cb0f449`. It hands back a sink an earlier swap released, with no sink shared between the graphs
at either swap, which is a different defect from finding 4. It is the evidence for the open item
in `docs/architecture.md` §12 that begins *A sink the library has released loses every later
event when it is handed back*, which records its output.

**Three probes were added while SPEC-088 was authored, and are not the audit's.** They were
written on 2026-11-15 against `ae0b7674`, and each says so in its first line.
`core-pipeline/p14_end_races_sweep.py` forces a `flush()` on a thread sharing a span to land while
the close builds `span.end`. `core-pipeline/p15_fork_child_counters.py` prints what a forked
child's `health()` starts as. `core-pipeline/p16_interrupt_through_a_guard.py` raises a
`KeyboardInterrupt` through each guard. They are the evidence for the four open items in
`docs/architecture.md` §12 that begin *A `span.end` built while another thread sweeps the span*,
*A forked child's `health()` reports*, *The events of a call a `BaseException` cuts short* and
*A `BaseException` raised while a span opens or closes*.

**One probe was added while SPEC-088 was revised after its second review, and is not the
audit's.** `core-pipeline/p17_log_after_an_interrupted_close.py`, written on 2026-11-15 against
`ae0b7674`, prints which trace and span ids a log made after an interrupted close ships on, from
the caller's `except` handler and from a task created inside the span, against a task that
outlives a span that closed normally. SPEC-088 FR-002 cites it.

**Twelve scripts were added while SPEC-090 was authored, and are not the audit's.** They were
written on 2026-11-15 against `3e27a0a6`, each says so in its first line, and each is named
`core-pipeline/spec090_*.py`. Eleven are probes, runnable against the tree with `PYTHONPATH=src`:
they measure batch sizes under a backlog and at exit, the exit drain's budget and lines, a frozen
clock, a per-event sink, a JetStream `NATSSink` behind a slow ack, the caller's burst, a full
queue's memory and the delay with no backlog. `spec090_exit_budget.py` adapts SPEC-090's spec
review's probe. Two more, `spec090_dead_http_exit.py` and `spec090_slow_per_call.py`, were added by
SPEC-090's build (`14711e7e`) from its diff reviews' probes, and each says so in its first line.
`spec090_prototype.py` is not a probe: it rewrites a scratch copy of `worker.py`
into the prototype the spec's "rule" numbers come from, and must never be run on a checkout's own
`src/`. SPEC-090's Evidence section records their output.

**Not brought in:** the probes' own output files, the `*.stripped.txt` module views, the pytest
collection listing and the commit bodies. So `verification/cats.py` does not run as it stands: it
reads a `perfile.txt` of collected tests per file that was derived from that listing.

**Four probes write beside themselves** when run: `core-pipeline/p04_exit.py`, `p07_fork.py` and
`p12_kill.py`, and `sinks/probe_file.py`. The `.gitignore` here keeps that output out of a
commit.

Run at `a0f44b5f` as the table says, `verification/classify.py`, `ratio.py` and `detail.py`, and
`history/classify.py` each reproduce their committed table byte for byte, which is the check that
the scripts and the tables arrived intact.
