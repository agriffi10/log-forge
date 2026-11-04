# Completed Spec — SPEC-080: @trace Intake by Real Type — a Lying Argument Is Refused or Traced at Decoration, Never Absorbed on Every Call

## What was completed?

Four FRs, built in `31f5b64`, in `decorator.py` and `_diag.py`'s `_echo` docstring. The principle:
**`@trace` decides what a value is by asking `type`, never the value**, so of the decorated object
only a callable runs its own code at decoration, and no argument decorates cleanly and then fails
every call. It closes the `architecture.md` §12 item on `@trace(name=...)`'s `isinstance`, and the
sibling sites in the decorator the item did not list.

- **`name=` is routed at the `trace(...)` call** (FR-001): judged in `trace`'s own body by
  `issubclass(type(name), str)`. A `str` subclass is kept as the exact `str` it holds through
  `str.__str__`; anything else not `None` raises the `TypeError` there, naming the type's stored
  name, before any decorator is applied. `decorate` reads the exact name from the closure.
- **The span-name fallback** (FR-002): `_span_name` keeps a `__qualname__` only by real type, as an
  exact `str`, and otherwise names the type through the new private `decorator._stored_name`, which
  reads `type`'s own `__name__` descriptor (`_diag._STORED_NAME`, `_diag._STR_STR`).
- **The `str` slip** (FR-003): tested first in `_refuse_unusable`, by real type, its hint echoed
  through `_diag._echo`, which gains its second caller; a failure computing the echo propagates at
  decoration, where `rejected` catches it. A callable that claims `str` is traced.
- **The descriptor and non-callable refusals** (FR-004): `_refuse_unusable` runs real-type `str`,
  real-type descriptor, `callable`, then the one kept `isinstance` for a callable proxy, then the
  generator check. Both descriptor messages are built by the new private `_misordered`, which reads
  `__func__` and its `__qualname__` through `getattr` and keeps the latter only by real type.

Guarded by the SPEC-080 tests appended to `tests/test_decorator_sync.py`, whose hostile hooks
record or raise only inside SPEC-067's `conftest.armed()`, so pytest's own reporting can still read
them; no existing test was edited, so the SPEC-055 refusal tests pass unchanged.

Decision: `docs/decisions/trace-model/trace-routes-what-it-is-handed-on-its-real-type-and-only-a-callable-runs-its-own-code-at-decoration.md`,
with its rejected alternatives and the check order's one trade-off; a pointer from *Unit of work =
a decorated call*.

## Measured base figures

From the spec, measured at `253432f` through the public `log_foundry.trace`: a `name=` whose
`__class__` property returned `str` decorated and then wrote the absorbed line on every call with
no `span.start`; a callable instance whose metaclass `__name__` property returned `1` did the same;
a 10,000-character `str` slip produced a 10,069-character message; an object claiming
`classmethod` with no `__func__` raised `AttributeError`.

## What changed from earlier specs?

- **SPEC-055**: a non-`str` `name=` is refused at the `trace(...)` call rather than when the
  decorator is applied, and is reported ahead of a bad positional argument; the `str` hint is cut
  at 64 characters; a callable claiming `str` is traced rather than refused; a callable descriptor
  proxy is told the kind it claims, and a non-callable one that it needs a callable.
- **SPEC-067**: `_diag._echo` has a second caller, which lets a failure propagate.

## What changed for a caller

In `docs/release-notes/v1.1.0.md`, *Fixed*: the refusals and messages above. No public name or
signature changed, and no caller must change code.

## Follow-ups

- **Owed until the PR's CI is green:** the `isinstance` fallback to `__class__`, the
  `type.__dict__["__name__"]` descriptor and `callable`'s slot read are CPython behaviour; this
  worktree's interpreter is 3.13, so CI's 3.12 leg settles them on 3.12.

## Verification

The tests named above; the gate results are recorded in the PR.
