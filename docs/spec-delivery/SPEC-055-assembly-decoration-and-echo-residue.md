# Completed Spec — SPEC-055: Assembly, Decoration and Echo Residue

## What was completed?

The five runtime findings of the round-two pre-1.0 audit (`docs/audits/2026-09-04-pre-1.0-audit-round-2.md`),
at the two edges the library promises most about: what leaves `build_event` and what `@trace` accepts.

- **No string leaves assembly unencodable** (FR-001). `sanitize.truncate_str`/`truncate_tail` return an
  exact `str`, measured through `str.__str__`, with every lone surrogate replaced by U+FFFD and
  `truncated` set; undecodable `bytes` are marked; `configure()` refuses a `service`/`version`/`env`
  that is not a `str` or cannot encode, before the ownership stamp. `SQLiteSink` no longer loses the batch.
- **`@trace` names once and refuses at decoration** (FR-002): `partial` and callable instances trace
  under their type name; a misordered `@classmethod`/`@staticmethod`, a bare string, a non-callable and a
  non-`str` `name=` raise `TypeError` where they are written. The async dispatch consults the type's
  `__call__`. (New helpers: `_refuse_unusable`, `_span_name`, `_is_async`.)
- **A generator function is refused at decoration** (FR-003, Option A, decided 2026-09-05): `TypeError`
  for a sync or async generator function, a `partial` of one, or an instance whose `__call__` is one; a
  function that merely returns a generator object is a stated limit. Wrapping the iteration is deferred, not
  rejected, and the reasoning is in the register.
- **A hostile key costs itself** (FR-004): `_Coercer.key()` is total, the placeholder is
  `<unserializable key: T>` and marked; both placeholders go through `text()`.
- **Echo owns its stream faults** (FR-005): `ConsoleWriter` disables echo after one line on a broken pipe
  or closed file and throttles anything else on `_diag.WARN_EVERY`, the one period the worker reads too.
- **The assembly corpus** (FR-006): `tests/test_sanitize_corpus.py`, 52 rows through both delivery paths.

Deviations, one line each: a `UnicodeEncodeError` is a `ValueError` and is throttled, not latched (found
by the first diff review); `@trace(name=1)` is refused too (found by the second); the accepted limit that two
hostile keys of one type collide on one marked placeholder is pinned by a test rather than numbered.

## What changed from earlier specs?

- SPEC-017's clippers' second element now means "altered", not only "the ceiling fired".
- SPEC-048's `_partition_key` docstring, spec and delivery doc no longer claim the pass-through; the guard stays.
- SPEC-017 FR-005's throttle constant moved from `worker._DROP_WARN_EVERY` to `_diag.WARN_EVERY`.
- SPEC-025's echo guard in `api._log` is now the total guard only; the writer reports stream faults.
- The three `architecture.md` §12 entries filed by #222 are Resolved, struck in place.

## Verification

All six local gates green on every push; every reverting mutation the spec names was planted and killed,
the corpus's under the default `-n 12`; `pytest --collect-only` diffed against the base (one named test
replaced, all else additions). Two spec frames, one plan review, two diff frames per PR before each push.
