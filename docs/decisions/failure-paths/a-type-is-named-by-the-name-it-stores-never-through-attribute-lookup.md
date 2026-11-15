---
type: Decision
title: "A type is named by the name it stores, never through attribute lookup"
description: "every type name in `src/` is read through the name the type stores — `_diag.type_name(value)`, or `_diag._STR_STR(_diag._STORED_NAME(cls))` for a class in hand — never `type(x).__name__`, `x.__class__.__name__` or `getattr(..., \"__name__\")`, because a metaclass property runs on those; `tests/test_stored_name_roster.py` refuses any read of either name through attribute lookup, with three exemptions held exactly. (No spec — the stored-name sweep)"
---

**A type is named by the name it stores, never through attribute lookup.** `type(err).__name__` is
resolved through the metaclass, so an exception class whose metaclass defines a raising `__name__`
property made the path that named it raise, and one returning another string renamed the type in
the library's own lines. SPEC-063 moved the sanitizer and `_diag.absorbed` onto `type`'s own
`__name__` descriptor, SPEC-071 the argument doors, and SPEC-084 the stream and file sinks. Each
of those fixed the sites it was looking at, and the raw read kept turning up at the next sink
over. So the rule is now held for the whole of `src/` by a roster rather than site by site.

Measured with `a2cd4b36`'s `src/` under `tests/test_stored_name_sinks.py`, the raw reads still left
in the sinks had three effects. The property's `RuntimeError` escaped Kafka's produce, encode and
delivery-callback paths, `GooglePubSubSink` and the HTTP `auth` unpack. The delivery engine's guard
absorbed it on the engine sinks, so a refused round was reported as unconfirmed and its loss line
was never written. The file sinks could not build their dual-typed `SinkDeliveryError` and raised
the bare original. The same rule covers the rest of what `sinks/_driver_error.py` copies from a
driver's class, which since the driver-error rework reads nothing through attribute lookup at all:
*The dual type reads only what the driver stores, and only Python's own subclassing can refuse it*
(this area) has the mechanism and the hostile-class corpus that holds it.

The roster reports a `.__name__` or `.__qualname__` read on **any** receiver, not only on
`type(...)`, because a class held in a local is as much the caller's class as `type(x)` is and no
syntax tells the two apart. It also reports `getattr`, `hasattr`, `operator.attrgetter` and
`__getattribute__` given either name as a literal, or a dotted literal ending in one; a
`str.format` or `format_map` literal whose field reads either name; and a class pattern matching
on either as a keyword. Its exemptions are held by function and exact
expression. The two `decorator` reads name a callable rather than a type and are documented to
raise at decoration. `_otel._type_name` is guarded, so it cannot raise, and SPEC-056 pins its
`<unserializable: ?>` placeholder. Moving it onto the stored name was considered and rejected: it
fixes no defect and would change an OTLP record a caller can see. Reopening it means revising
SPEC-056's mapping criteria, not adding to this roster.

**Do NOT** add a new per-module copy of the accessor where `_diag` can be imported; `_otel` and
`sanitize` bind their own only because they import nothing from the package, and
`decorator._stored_name` predates this rule and wraps the class form. `sinks/http.py`'s
byte-identical `_type_name` was folded into `_diag.type_name` when this was settled. **Do NOT**
answer a new roster report with an exemption unless the read is not a type's name or a spec pins
its output.

## History

### 2026-11-15

- **Revised by the driver-error rework.** The entry said `_driver_error` read its `__module__` and,
  in `_reduce`, its `__bases__` through `type`'s descriptors and its `__doc__` from the class
  namespace, and that the other ways in stayed open in `docs/architecture.md` §12. The rework
  replaced that module's reads wholesale and closed the item; the entry now points at the decision
  that records it.
- **Settled.** The sweep moved every remaining read in `src/` onto `_diag.type_name` and added the
  roster, its fixture corpus and the behavioural tests.
