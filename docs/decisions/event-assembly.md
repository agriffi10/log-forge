# Event assembly: safety and bounds — decisions

The settled decisions for `build_event` and `sanitize` — what a value may become, and what it may
cost. Read the fences; pull an entry only when you need the reasoning.

## Contents

- [Fences](#fences)
- [A reserved word needs exactly one route through, including its own name](#a-reserved-word-needs-exactly-one-route-through-including-its-own-name)
- [An event is safe by construction — coerced and bounded once at assembly, not per sink](#an-event-is-safe-by-construction--coerced-and-bounded-once-at-assembly-not-per-sink)
- [A value too large to *render* is replaced, never clipped](#a-value-too-large-to-render-is-replaced-never-clipped)

## Fences

- **A reserved word needs exactly one route through, including its own name** — `fields` is the third reserved word and `fields={"fields": …}` must work. The keyword form wins a collision, and the merge **absorbs** a non-mapping rather than raising. (SPEC-025, SPEC-034)
- **An event is safe by construction — coerced and bounded once at assembly, not per sink** — `build_event` runs every value through `sanitize.py`, so the bare `json.dumps` calls in `sinks/` are correct by consequence. The unserializable fallback is a type name, never `repr()`. Ceilings bound per *value*. A surrogate becomes U+FFFD, marked; a hostile key costs only itself. (SPEC-017, SPEC-055)
- **A value too large to *render* is replaced, never clipped** — an over-long int becomes `<int: ~N digits>`; a truncated number is silently wrong. Detection is `bit_length()`, never `len(str(v))` — the obvious check raises the very error it prevents. (SPEC-020)

---

### A reserved word needs exactly one route through, including its own name

**A reserved word needs exactly one route through, including its own name** — `echo` and `message` were parameters stealing ordinary words from the field namespace, and `fields=` is the escape hatch, so `fields` becomes the third reserved word and `fields={"fields": …}` must work. The keyword form wins a collision (`{**base, **overrides}`), and the merge **absorbs** a non-mapping rather than raising: it runs in the emitter, outside `api._log`'s orphan guard, so an unguarded merge broke SPEC-025's promise on all four paths. (SPEC-034 FR-004)


### An event is safe by construction — coerced and bounded once at assembly, not per sink

**An event is safe by construction — coerced and bounded once at assembly, not per sink** — `build_event` runs every value through `sanitize.py`, so all 40+ bare `json.dumps` calls in `sinks/` are correct by consequence, it costs one pass per event rather than one per destination (`MultiSink`), and the guarantee reaches the non-JSON sinks too. The unserializable fallback is a type-name placeholder, never `repr()`, so the fix cannot widen the PII exposure arch §6 prevents. Ceilings bound per *value*, not per event. (SPEC-017) **No string leaves assembly that cannot encode, and a stamp that cannot is refused at `configure()`** (SPEC-055 FR-001, FR-004): the clippers return an exact `str` — measured through `str.__str__`, so a subclass's `encode` or `__str__` cannot divert it — with every lone surrogate replaced by U+FFFD and `truncated` set, on the rule `real()` applies to a non-finite float: a substitution nobody can see is a silent change to the data. One U+FFFD per surrogate, because `errors="replace"` on encode writes `?` and a `surrogatepass` round trip writes three. Undecodable `bytes` are marked the same way. The three config stamps bypass assembly on purpose (the hottest path), so `configure()` refuses a non-`str` or non-UTF-8 stamp before the ownership stamp and stores `str.__str__` of it; an over-long stamp is recorded on `invariants.md` §8 rather than clipped. A mapping key whose rendering raises is confined to its own marked `<unserializable key: T>` placeholder — the guard is in `key()`, not in `mapping()`'s loop, whose other two failures already have owners — and two hostile keys of one type colliding on one placeholder is accepted over a numbered placeholder, the first step toward one carrying the key's identity. Both placeholders go through `text()`, since `type.__name__` is writable.


### A value too large to *render* is replaced, never clipped

**A value too large to *render* is replaced, never clipped** — `int` is the one type with no natural ceiling, and CPython refuses to render one past `sys.get_int_max_str_digits()`, so an over-long integer becomes `<int: ~N digits>`. Truncating digits would silently change the number, and a wrong number is worse than a visibly elided one. Detection is `bit_length()`, never `len(str(v))` — the obvious check raises the very error being prevented — with the ratio rounded so it errs toward replacing. (SPEC-020)


