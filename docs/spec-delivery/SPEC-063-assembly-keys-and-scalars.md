# Completed Spec — SPEC-063: Assembly Keys and Scalars — Container Keys Under the Pass Budgets, and No Type Routing the Caller's Object Controls

## What was completed?

The two `architecture.md` §12 items SPEC-057 left open, and three siblings of the same shape. No
config key and no public name was added; `sanitize.__all__` and `log_foundry.__all__` are unchanged.

- **Container keys** (FR-001): a key whose real type subclasses `tuple`, `list`, `dict`, `set` or
  `frozenset`, and not `Enum`, is walked by `_Coercer._container_key` through `mapping()` or
  `members()`, never `_dispatch`, spending its own node and its members' nodes and bytes. Its text is
  the JSON of the coerced form, clipped, not charged again. A key that stops the pass writes the stop
  placeholder for key and value both. A `bytes` or `bytearray` key is priced at `4 * len + 3` by
  `_bytes_key_fits` before it is rendered.
- **Exact-type routes** (FR-002): `kind is bool`, `_INT_INT` and `_FLOAT_FLOAT` at every route into
  the number rules, builtin `str`, `bytes`, `dict` and container subclasses routed by
  `issubclass(type(value), …)`, the `Enum` branch entered only by a real `Enum` or a non-builtin
  (`_REAL_ROUTED`), `_key`'s container test ahead of its claim-based scalar tests and its
  `type(key) is not bool`, a `(float, Enum)` key kept on `str()`, an `Enum` member whose real type
  is a builtin container or bytes written as `Type.NAME` by `_enum_text` (closing the
  `(tuple, ReprEnum)` limit), and `_type_name` reading the stored
  name through `str.__str__`. `isinstance` stays only on the render-through routes, for proxies.
- **Truthiness inside the guard** (FR-003): `_merge`'s `fields=` test and `_log`'s `echo` test.
- **Hostile class and colliding key** (FR-004): `model._stored_module`, `sanitize._type_name` reused
  by `model`, `_diag`'s own stored-name read, and `model._merged`, the highest-precedence-first rebuild entered only
  when the unpack raises.
- **The record** (FR-005): both §12 items resolved, `docs/invariants.md` §8, the decision
  "A route that trusts the caller's object admits it by exact type or an unbound accessor", and the
  v1.1.0 notes.

Deviations from the spec: `_merged` takes its sources positionally, highest first, and a
`bytearray` key's repr carries its type name, so the `4 * len + 3` bound is exact for `bytes` and
short by that name for `bytearray`, which the actual charge then counts.

## What changed from earlier specs?

- SPEC-057's "mapping keys are not values and are not counted" is superseded for container keys, with
  a pointer at its description, its Accepted Limits entry and its charge-table row.
- SPEC-055's "a hostile key costs only itself" now holds in the defaults/baggage/fields merge.
- The text of every container key changed, from `str(key)` to JSON, a caller-visible change recorded
  in `docs/release-notes/v1.1.0.md`, whose headline is qualified for it.

## Verification

Every new criterion was run against `71e42dd` or the plan's prototype and fails there by its asserted
reason. The doc half was checked by `spec-lint`, `docs-lint`, `docstring-lint`, the invariants and
public-surface tests, and a whitespace-collapsed search for the superseded claim, which finds it
only under a strike-through or a pointer.
