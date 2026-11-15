---
type: Decision
title: "The dual type reads only what the driver stores, and only Python's own subclassing can refuse it"
description: "`sinks/_driver_error.py` reads a driver's exception class and instance only through `BaseException`'s and `type`'s own descriptors, a slot's own member descriptor, or a walk of a stored `dict` comparing exact `str` keys, keys its cache by `id` with the original held in the entry, checks the derived class's stored MRO holds both bases, and copies onto it only an exact `str` or `None`, so no hook of the driver's runs outside the one guard around subclassing and construction; `tests/test_driver_error_corpus.py` holds every way in as a hostile fixture. (No spec — the driver-error rework)"
---

**The dual type reads only what the driver stores, and only Python's own subclassing can refuse
it.** `delivery_error` builds the exception a sink raises for its driver's error as an instance of
a class derived from both `SinkDeliveryError` and the driver's own class
(*A sink's own driver error is counted, announced and raised as both types*,
`docs/decisions/sink-delivery/`). The driver's class is the caller's whenever a file or stream
sink writes to a handle the caller supplied, so any hook on it is the caller's code. Three review
rounds running found a read in `_driver_error` that ran one: a metaclass name, module, doc or
bases property, then a cache key's `__hash__`, a `__cause__` setter, an `args` property, attribute
properties and a stored `__module__`'s `__set_name__`. Each fix closed one read and left the next.
So the mechanism was replaced rather than patched again, with a corpus of hostile classes in place
of a list of sites.

**Two zones.** Deriving from the original and calling the derived class run the driver's code by
Python's design: its metaclass, its `__init_subclass__`, its `__new__` and `__init__`, and the
lookups subclassing makes in its namespace. That work, and only that work, sits inside one
`except Exception` guard, and a class that refuses there is raised unchanged, as `Returns:` says.
Everything else reads and writes without consulting the driver. `args` and the cause go through
`BaseException`'s getset descriptors. A copied attribute kept in a slot, as an `OSError`'s two
path attributes and any `__slots__` entry are, goes through that slot's own member descriptor,
found in the stored MRO's namespaces; every other copied attribute, and a class's `__module__`
and `__doc__`, are found by walking a stored `dict`'s entries and keeping only a key whose type is
exactly `str`, so no key the driver planted is hashed or compared through its own `__eq__`. The
instance's `__dict__` is walked by `dict.items` itself, so a `dict` subclass's methods never run.
The cache is keyed by `id`, and its entry holds the original, which is the only thing that keeps
the id valid: an `__init_subclass__` can rewrite the derived class's bases and delete its
`__reduce__`, after which the original was collected and an ordinary class built at the same
address was handed the derived class of the first. And because `type()` returns whatever the
metaclass makes, and a metaclass `mro` or a later rewrite of the bases decides what an `except`
matches, every call checks by identity that the derived class's stored MRO holds both the
original and `SinkDeliveryError`, and refuses otherwise. Every value copied onto the derived
class is an exact `str` or `None`, so `type()` finds no `__set_name__` on it and a traceback line
compares no `str` subclass. A stored value is checked with `issubclass(type(v), str)`, never
`isinstance`, which reads the value's own `__class__`. The derived class's `__reduce__` closes
over the original rather than reading it back from the bases, and a constructor that returns
anything but an instance of exactly the derived class is a refusal, never raised as though it
were the dual type.

**Two readers stay outside the library's reach.** `pickle` resolves the original class by its own
attribute reads, and `traceback` reads the class's `__module__` and the instance's `__cause__` the
same way; the cause it then formats is the original, whose class is the hostile one. A hook
reached that way fails those readers on the driver's own exception as much as on the dual type,
so the corpus asserts each of them only on the fixtures they do not reach, and says which.

Rejected: patching each read as it is found, which is what recurred three times. Building the
instance without calling the constructor, through the solid base's `__new__` and slot writes:
it skips a driver `__init__` that sets attributes, has to locate that base, and the constructor is
already a documented refusal. Copying the whole instance `__dict__`: it would hand a caller
attributes the dual type did not carry before, a change nobody asked for. Leaving `__module__`
and `__doc__` off the derived class: `__module__` is what makes its traceback line read
`sqlite3.OperationalError` rather than this module's name, and a caller sees that.

**Do NOT** read a driver's class or instance in `_driver_error` through attribute lookup,
`getattr`, `hasattr`, `isinstance` or a `dict` lookup. **Do NOT** key the cache by the class.
**Do NOT** put a value the driver stores into the derived namespace unless it is an exact `str`
copied with `str.__str__`, or `None`. **Do NOT** answer a new way in with a guard at its site
alone: add it to the corpus as a fixture carrying the failure it guards, route the read through
the descriptors, and mutation-test the guard against the fixture.

## History

### 2026-11-15

- **Settled.** The rework replaced attribute reads in `sinks/_driver_error.py` with the
  descriptors and the exact-key walk, and added `tests/test_driver_error_corpus.py`; it closed
  the `docs/architecture.md` §12 item the stored-name sweep had recorded.
