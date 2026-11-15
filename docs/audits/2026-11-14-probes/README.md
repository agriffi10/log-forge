# Probes of the 2026-11-14 audit

The scripts the five frames of [`2026-11-14-post-1.2.0-audit.md`](../2026-11-14-post-1.2.0-audit.md)
ran against this repository at `a0f44b5f` (tag `v1.2.0`), kept byte for byte as they were run so
that a later session can prove a finding closed by re-running the probe that found it. Each
directory is named for its frame report.

**Four scripts here are not the audit's.** Three were written on 2026-11-15 while authoring
SPEC-086, against `5447d26a`, and the fourth while building SPEC-087; each says so in its first
line. `lifecycle/leaves_remaining_holder.py` was written for SPEC-087: it is the probe behind the
open item in `docs/architecture.md` §12 for a wrapper that leaves its member alone, which is then
closed by nobody.
`lifecycle/p9_second_stop_in_fork_repair.py` and `lifecycle/p10_fork_with_stderr_lock_held.py`
are the probes behind two open items in `docs/architecture.md` §12 that the audit did not find.
`lifecycle/p8_second_close_after_a_rearm.py` is behind no open item: it traces a sink closed
twice with nothing written between the closes to a re-arm between two takes, which invariant 5
records as correct, and is kept so the next reader of that symptom finds the trace.

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
review's probe. `spec090_prototype.py` is not a probe: it rewrites a scratch copy of `worker.py`
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
