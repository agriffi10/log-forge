# Changelog

Every released version, newest first. This file is an **index, not a copy** — where a release has
notes, they are the authority and this page links them rather than restating them, so the two
cannot drift apart. Versions follow [semantic versioning](https://semver.org/) from `1.0.0`
onward; the `0.x` line predates that promise and broke compatibility freely.

Each version is a Git tag, and the tag is what was published: the version is derived from it at
build time, never hand-edited. [GitHub Releases](https://github.com/agriffi10/log-forge/releases)
carry the built artifacts and, from `0.10.1`, a CycloneDX SBOM.

## 1.0.0 — 2026-09-08

**The first release under semantic versioning, and the API freeze.** Everything in
`log_foundry.__all__`, the `Sink` protocol, and every shipped sink class at its documented import
path is fixed for the whole of `1.x`. Not a feature release but a reliability one: three audit
arcs worked through the paths where the library could lose an event, fail its caller, block
forever, or report health it could not back up.

**Upgrading from any `0.x` — read the notes first, because several of the changes are silent.**
[**Full release notes**](https://github.com/agriffi10/log-forge/blob/v1.0.0/docs/release-notes/v1.0.0.md)
— twenty-two upgrade items, ordered by how likely each is to reach you. The four most likely to
break a working `0.10.x` app:

- `Health` and `SinkLosses` are frozen keyword-only dataclasses, not `NamedTuple`s. `len(h)`,
  `h[0]` and `queued, dropped, failed_batches, stopped_reason = health()` now raise `TypeError`.
  Read them by attribute.
- `log_foundry.sinks.util` is deleted with no alias. `MemorySink`, `NullSink` and `StderrSink`
  now live at `log_foundry.sinks.memory`, `…null` and `…stdout`.
- `@trace` refuses a generator function at decoration, so an upgrade can fail at import rather
  than at runtime. Trace the consumer that iterates it instead.
- `continue_trace()` is one-shot: it applies to the next root span and does not survive it. This
  one is silent — no exception and no counter, just shorter traces than you expected.

`flush()` and `continue_trace()` also return result objects rather than booleans (`if flush():`
is unchanged; `flush() is True` is not), and a `flush()` inside an open span now sweeps it —
before `1.0.0` that delivered nothing, with every counter reading clean.

## 0.10.1 — 2026-08-02

The first release carrying a CycloneDX SBOM as a release asset.

## 0.10.0 — 2026-08-02

Supply-chain transparency, shipped **without** its SBOM. The wheel is still on PyPI, but its
GitHub Release no longer exists, and releases here are immutable so it cannot be re-cut under the
same tag. **Use `0.10.1` instead** — same contents, with the SBOM attached.

## 0.9.0 — 2026-07-31

Continuous security scanning, plus a raise to the optional extras' version floors.

## 0.8.0 — 2026-07-30

Cleanup of items left open by the preceding releases.

## 0.7.1 — 2026-07-30

Bounds on integer field values, so an integer too long to render can no longer break the record.

## 0.7.0 — 2026-07-29

Batch-response adjudication — a partially-failed batch is counted rather than blindly retried —
and worker liveness, so a dead drain thread is reported instead of showing up as a rising drop
count.

## 0.6.0 — 2026-07-27

Payload and failure safety: values are coerced and bounded once at assembly, so no sink can be
handed a payload JSON refuses.

## 0.5.0 — 2026-07-22

SQS FIFO support — message group and deduplication ids.

## 0.4.0 — 2026-07-22

Baggage on boundary events, so trace-scoped context reaches `span.start` and `span.end`.

## 0.3.0 — 2026-07-22

AWS Lambda compatibility, and cross-process trace continuation: `current_traceparent()` on the
producer, `continue_trace()` on the consumer.

## 0.2.0 — 2026-07-22

**Renamed `log_forge` → `log_foundry`**, so the import package matches the distribution name.
No compatibility shim; migrating from `0.1.x` is a find-and-replace on the import. PyPI rejects
`log-forge` as too similar to the unrelated, pre-existing
[`logforge`](https://pypi.org/project/logforge/) — its similarity check collapses separators.

## 0.1.0 — 2026-07-22

The first stable release, and a large one: the span pipeline, the logging API and console echo,
async `@trace`, the background worker, and the whole sink family — stdout, file, SQLite, the
stdlib-`logging` bridge, composition and adapter sinks, every HTTP/socket platform sink, the
queue and stream sinks, and the databases. Published as `log-forge`, importing `log_forge`.

## 0.0.1 — 2026-07-22

The first tag, cut before the package was published under this name.
