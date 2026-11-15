---
type: Decision
title: "A type test reads the real type, never the value's __class__"
description: "every type test in `src/` is `_diag.is_a(value, X)`, or `issubclass(type(value), X)` in a leaf module, with `X` concrete (`dict`, not `Mapping`), never `isinstance`, because `isinstance` reads the value's `__class__` and an abstract `X` hashes the value's class; `tests/test_class_check_roster.py` refuses `isinstance`, a `__class__` read and an abstract target, its exemptions — each a guarded site that keeps a claim or an ABC on purpose — held exactly by function and expression. (No spec — the class-claim sweep)"
---

**A type test reads the real type, never the value's `__class__`.** `isinstance(value, X)` reads
`value.__class__` through attribute lookup whenever the real type is not `X` or a subclass of it, so
a `__class__` property or `__getattribute__` of a caller's object ran inside the library. Measured at
`6bd0b66e` with the cases of `tests/test_class_claim.py`: a client exception whose `__class__`
property raises ran it in 15 of the 20 record-building sinks' retry predicates and loss paths, under
their guards, and in the stdout sinks' `_lose`, which then raised a plain `SinkDeliveryError` where
the dual-typed one is the contract. The property escaped as itself from `ConsoleWriter.write`,
documented to absorb every stream fault; from `FilteringSink`'s level test; from `sink_factory`'s
result check, past the `SinkDeliveryError` it documents; from `health()`'s `read_losses`; and from
eleven functions documented `Raises: None` when handed one directly, among them `_otel.log_record`
and `resource_key`, the sinks' timestamp readers and the AWS retry predicate; the HTTP response
readers among them are only ever handed `json.loads` output in production. A claim of `OSError`
could also make an impostor retryable.

**The helper is `_diag.is_a`,** which tests `type(value)`'s MRO through `type.__subclasscheck__`
unbound: the test an `except` clause makes. It asks no metaclass of the value's class and none of
the target's, so neither `__class__` nor an ABC's cache runs. `issubclass(type(value), X)` is the
same test for a concrete `X` and is the spelling in the leaf modules (`_otel`, `sanitize`,
`sinks/_classify`), which import nothing from the package. A single class narrows like
`isinstance` (`TypeIs`); a tuple narrows only the true branch (`TypeGuard`), because `TypeIs` over
`tuple[type[T], ...]` made the rest of five retry predicates unreachable to `mypy --strict`.

**An abstract target is refused by the roster**: `Mapping`'s `__subclasscheck__` hashes the class
into its cache, which runs a metaclass `__hash__` of the caller's, and through `is_a` an ABC answers
by MRO alone, so `dict` is not a `Mapping`. Where a site needs the ABC's answer, it asks under a guard
that reads a raise as the refusal it was reaching for, and is exempt. `HTTPSink`'s `auth` is the case:
its `Mapping` test raised a metaclass `__hash__` out of the constructor, and moving it to the MRO was
tried and rejected in review, because a map `Mapping` knows only by `register` (`pyrsistent.pmap`,
`immutables.Map`) would then be unpacked and send its two keys as the credentials. `sink_factory`'s
result check keeps `isinstance(result, Sink)` under the same kind of guard, rather than
`_fork._sink_shaped`, which was also tried: the static test refuses a `MagicMock(spec=…)` that
`configure(sink=…)` accepts, and accepts a weakref proxy of a non-sink.

**What it costs:** a transparent proxy that only claims `X` through `__class__` no longer passes
outside the exempt sites. Review drove proxies, `MagicMock(spec=…)` and honest subclasses through
every public entry point and every sink's `emit` and `deliver`, old tree against new, and found
three sites where 1.x had accepted a proxy. `TransformSink`'s result check is one a proxy
legitimately reaches, so it keeps `isinstance` under a guard and is exempt. The other two stay
converted, as stated costs: `read_losses` reads a sink's proxied `SinkLosses` as none, which is
what its docstring already promised for anything that is not one, and a direct `LoggingSink.emit`
reads a proxied `level` as unknown and a proxied `fields` as absent, which no pipeline event can
carry. `tests/test_class_claim.py` pins all three.

**Exempt, held exactly by function and expression in the roster:** `sanitize._Coercer._dispatch`
and `_key` keep `isinstance` for the render-through routes and the claim-then-convert routes, by
SPEC-063's decision *A route that trusts the caller's object admits it by exact type or an unbound
accessor*, under the coercer's guard; `decorator._refuse_unusable` reads a decorated object's
`classmethod` claim at decoration, documented to raise in the caller's frame; `sinks._batch.
adjudicate_keyed` asks a driver item whether it is a `Mapping` under a guard and calls its own
`__contains__` next anyway; `sinks.http._copied_pair`, `sinks._factory._FactorySink._checked` and
`sinks.transform.TransformSink._apply` ask `Mapping`, the `Sink` protocol and `dict` under a guard
that reads a raise as a refusal, as above; and
`sinks.http._sent_value` reproduces `putheader`'s own `isinstance(value, int)`
once at construction, so a proxied `int` header is still sent as its digits.

**Rejected:** routing an unencodable direct-`emit` event's value away from `json.dumps`, whose own
`default` names it through `o.__class__.__name__`. It is the standard library's code under the
per-event encode guard, so the event costs only itself, which is the settled direct-`emit` contract;
`tests/test_class_claim.py` asserts on the frame that asks, so a library frame asking still reddens.

**Do NOT** write `isinstance` in `src/` outside the roster's exemptions, and **do NOT** answer a
roster report with an exemption unless the site admits a claim on purpose under a guard or a
decision. Serves invariant 1.

## History

### 2026-11-15

- **Settled.** The sweep moved every other `isinstance` in `src/` onto `_diag.is_a` or the inline
  real-type test and added the roster, its fixture corpus and the behavioural tests.
