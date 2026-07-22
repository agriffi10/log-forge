# Completed Spec — SPEC-013: AWS Lambda Compatibility

## What was completed?

The library can now be installed *and* correctly drained from an AWS Lambda function. Two changes
with one cause: the library assumed a process that starts, runs and exits, and Lambda gives it one
that is frozen, thawed and killed without warning.

- **Python floor lowered to 3.12** — `requires-python = ">=3.12"`, mypy `python_version = "3.12"`
  (it must track the *lowest* supported version or a 3.13-only API type-checks clean here and
  fails in a consumer's runtime), ruff left without `target-version` so it cannot drift from the
  floor. `ci.yml` runs the whole gate as a 3.12 + 3.13 matrix; `release.yml` builds on the floor
  only, since the wheel is pure-Python `py3-none-any`. `poetry.lock` regenerated — it pinned both
  the old range and a content-hash of `pyproject.toml`.
- **`lf.flush(timeout=5.0) -> bool`** — a drain that does *not* retire the worker. Backed by
  `Worker.flush` + `worker._FlushMarker`, a marker carrying a `threading.Event` that rides the
  FIFO queue: everything submitted before the call is necessarily ahead of it, so when the worker
  dequeues the marker it emits `pending` immediately (ignoring both batching triggers), sets the
  event, and carries on. The `_stop` event is not set, the thread is not joined, `sink.close()` is
  not called, and the once-only shutdown flag is not consumed.
- The marker is excluded from `pending` in **both** `_run` and `_final_drain` — each had its own
  copy of the append guard. Without either exclusion the marker is handed to `sink.emit` and kills
  the worker thread; both exclusions are covered by tests confirmed to fail against the unfixed
  code.
- `flush()` cannot hang and cannot resurrect a dead worker: it returns `True` immediately when no
  worker was ever created (without calling `_get_worker`, which would start a thread and register
  `atexit` to drain nothing), returns `False` promptly after `shutdown()`, honours its timeout on
  a wedged sink, and never raises.

Deviation from the spec, deliberate: `flush()` derives one **deadline** shared by the marker's
blocking put and the subsequent wait, rather than passing `timeout` to each independently. Same
"blocking put with the same timeout" requirement (FR-002), but the whole call is bounded by
`timeout` instead of `2 * timeout` — which matters for the caller with an execution deadline this
exists for.

## What changed from earlier specs?

`shutdown()` (SPEC-004) is behaviourally **unchanged** — the existing SPEC-004 tests pass
unmodified. Only its docstring gained a pointer to `flush()`, naming the warm-container failure
mode. The README's *Flushing and shutdown* section previously recommended `shutdown()` "at the end
of ... an AWS Lambda handler"; that advice was the silent-data-loss bug this spec fixes, and has
been replaced with the `finally`-placed `flush()` pattern.

## Verification

Full gate green locally on **both** 3.12 and 3.13 (ruff, mypy, pytest — 289 tests) before the
floor was declared, per FR-001; CI re-proves both legs on every push. The two marker-exclusion
guards were mutation-tested: removing the `_run` branch hangs every flush, and removing the
`_final_drain` branch raises `TypeError: '_FlushMarker' object is not iterable` inside `_emit` —
both caught by the new tests. Behaviour under a real Lambda freeze/thaw cycle is not exercised
here and was not simulated; the failure it guards is a lifecycle property of the platform.
