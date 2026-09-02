# Completed Spec — SPEC-051: API Freeze Tidy

## What was completed?

The six shapes the 2026-09-02 pre-1.0 audit found free to change today and breaking to change
after the tag.

- **Keyword-only public dataclasses.** `Health`, `Config`, `SinkLosses` and `_Result` are
  `kw_only=True`, so field order stops being part of the frozen contract.
- **`Mapping` on both `defaults=` parameters** (`configure`, `trace`) — `dict` is invariant, so a
  caller's `dict[str, str]` was refused. Deliberate deviation from "no runtime change": `trace`
  now copies at **decoration**. It bound the caller's object to every span and read it live, which
  is tolerable for a `dict` and not for a `Mapping` whose `keys()` is user code on the per-event
  path.
- **`context.__all__` trimmed to the six names the package re-exports.** The five withdrawn stay
  importable; the claim went, not the symbol.
- **Names a public signature uses are now exported:** `GroupIdSource`/`DedupIdSource` (`sinks.sqs`),
  `Backend` (`sinks.sentry`), and `flush_sink`/`DEFAULT_SWAP_TIMEOUT` at the top level.
- **`Unpack[TypedDict]` on the seven HTTP platform sinks**, via five shapes in `sinks/http.py`
  composed by inheritance. `DatadogSink("k", timeout="not-a-float")` is now a `mypy` error rather
  than a first-request failure.
- **A typed-consumer probe** — `tests/typed_consumer/{accepts,rejects}.py` run under
  `mypy --strict` in a subprocess by `tests/test_typed_consumer.py`, because `files = ["src"]`
  means the repo's own gate cannot see a caller.
- Two register entries: the frozen surface (with the worker tunables as a "do NOT build"), and the
  sink constructor surface (names frozen as the vendors spell them).

Deferred, said out loud: the `configure()` worker tunables (additive after `1.0`, and the
lazy-worker semantics need designing); the README's own defects, which belong to the release
surface — including `README.md:1019`'s `SinkLosses(dropped, failed)`, which reads positionally
after this spec.

## What changed from earlier specs?

- SPEC-034's `Health`/`Config`/`SinkLosses`/`FlushResult`/`ContinueResult` no longer accept
  positional arguments. Nothing in `src/` or `tests/` constructed them that way.
- SPEC-034 FR-004 widened `fields=` to `Mapping`; the same fix reaches `defaults=` here.
- SPEC-009's seven platform sinks lose `# type: ignore[arg-type]` on their forwarding call; four
  keep a `[misc]` covering only the `headers` key `merge_headers` pops. `merge_headers` takes
  `Any`, because it mutates by popping and a `TypedDict` is not a `MutableMapping`.
- The `Health` docstring's index-access claim, inherited from the `NamedTuple` SPEC-034 replaced,
  is gone; `health()`'s `Returns:` now names all twelve fields.

## Verification

`ruff`, `mypy`, `pytest`, `spec-lint`, `docs-lint`, `docs-lint-test` all exit 0 locally. Every new
guard was mutation-tested: three in Phase 1, four in Phase 2, six against the HTTP roster, four
against the probe. Two findings came out of that rather than out of review — a subset assertion
over the TypedDict family that inheritance made unkillable (removed, with the reason in the file),
and a `# want:` regex that collected two sentences of its own docstring (now `tokenize`).

`MYPYPATH` in the probe runner is proved the only way it can be: pointed at a library without this
spec's additions, the consumer must fail and name one. Dropping it is undetectable from a worktree
whose own install is current, which is exactly the case where it is not needed.
