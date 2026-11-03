# Completed Spec — SPEC-065: Integer Arguments Are Exact: configure() Ceilings and the Sink Validators

## What was completed?

Four FRs, built as `6905e91..da4765a`, in `config.py`, `sinks/_retry.py`, `sinks/_chunk.py`
and `sinks/file.py`. The principle: **an integer argument is judged and stored as the exact number
it holds**, so a float is refused at the door and a subclass cannot lie about a zero.

- **A non-`int` ceiling is refused** (FR-001): `config._require_positive` raises a `TypeError`
  naming the ceiling and the type for `max_value_bytes`, `max_stack_bytes`, `max_keys` and
  `max_depth` when the value is neither `None` nor an `int` instance. A float is refused, never
  floored.
- **An accepted int is stored exact** (FR-002): the helper now returns `int.__index__(value)`, so
  `True` is stored as `1` and a subclass whose `__lt__` lied about `0` is rejected as the `0` it
  holds. The `ValueError` message is built from the exact `int`.
- **A refused call changes nothing** (FR-003): `configure()` assigns the four results to locals
  ahead of the stamps, before any assignment and before the sink is stamped into the ownership
  record.
- **The sink validators return the exact builtin** (FR-004): `_retry.require_positive`,
  `require_timeout` and `usable_timeout` test and return the value through the new private
  `_exact_number` (`int.__index__` / `float.__float__`), and `chunk_list` and `RotatingFileSink`'s
  interval use `require_positive`'s return, which they used to discard.

Deviation, one line: the source diff also carries four formatting-only reflows (a `raise` in
`config._require_text`, a condition and a blank line in `file.py`, a `raise` in `require_timeout`).

## What changed from earlier specs?

- SPEC-049's validators (`require_positive`, `require_timeout`, `usable_timeout`) keep every verdict
  on the declared types and now return the exact builtin, not "the value unchanged". The decision
  `a-bad-argument-is-floored-if-it-works-and-refused-if-it-is-already-broken-a-new-one-is-refused`
  is refined on one point and carries a dated History entry.
- `configure()` now refuses a float ceiling it used to store. Only a caller already failing `mypy`
  can notice, and `docs/release-notes/v1.1.0.md` says so.
- `architecture.md` §12: the open item *`configure()` accepts a ceiling that is not an `int`* moved
  to Resolved. Residue left open by the spec: `Config(max_keys=2.5)` still constructs, since nothing
  in the library accepts a hand-built `Config`.

## Verification

`tests/test_config.py` (the four-ceiling matrix, storage pins, atomicity pins) and
`tests/test_sink_retry.py` (the spoofed-subclass pins at every door); the gate results are
recorded in the PR.
